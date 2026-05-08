/**
 * Backfill missing LinkedIn URLs and optional emails using ReverseContact.
 *
 * Default is dry-run. Use --live to write updates. Email discovery costs more,
 * so it only runs with --find-emails.
 *
 * Usage:
 *   node scripts/backfillReverseContact.js --limit 25
 *   node scripts/backfillReverseContact.js --live --limit 50
 *   node scripts/backfillReverseContact.js --live --find-emails --limit 10
 *   node scripts/backfillReverseContact.js --live --find-emails --mark-inactive-no-contact --limit 500
 */

import dotenv from 'dotenv';
dotenv.config();

process.env.REVERSECONTACT_ASYNC_MAX_POLLS ||= '2';
process.env.REVERSECONTACT_ASYNC_POLL_MS ||= '6000';

import { createClient } from '@supabase/supabase-js';
import { enrichByEmail, searchPerson, findEmail } from '../enrichment/reverseContactEnricher.js';

const LIVE = process.argv.includes('--live');
const FIND_EMAILS = process.argv.includes('--find-emails');
const EMAILS_ONLY = process.argv.includes('--emails-only');
const LINKEDIN_FROM_EMAIL = process.argv.includes('--linkedin-from-email');
const MISSING_BOTH_ONLY = process.argv.includes('--missing-both-only');
const MARK_INACTIVE_NO_CONTACT = process.argv.includes('--mark-inactive-no-contact');
const SHOULD_FIND_EMAILS = FIND_EMAILS || EMAILS_ONLY;
const limitIdx = process.argv.indexOf('--limit');
const dealIdx = process.argv.indexOf('--deal');
const LIMIT = limitIdx >= 0 ? Math.max(1, Number(process.argv[limitIdx + 1] || 25)) : 25;
const DEAL = dealIdx >= 0 ? String(process.argv[dealIdx + 1] || '').trim() : '';

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

function normalizeName(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/\b(cfa|cpa|mba|phd|md|jr|sr|ii|iii|iv)\b/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeFirm(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\b(inc|llc|ltd|lp|llp|corp|corporation|partners|partner|capital|holdings|group|ventures|management|advisors)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function profileMatches(contact, result) {
  if (!result) return false;
  const expectedName = normalizeName(contact.name);
  const actualName = normalizeName(result.fullName || [result.firstName, result.lastName].filter(Boolean).join(' '));
  if (expectedName && actualName) {
    const expected = expectedName.split(/\s+/).filter(Boolean);
    const actual = new Set(actualName.split(/\s+/).filter(Boolean));
    const shared = expected.filter(token => actual.has(token)).length;
    if (shared < Math.min(2, expected.length)) return false;
  }
  const expectedFirm = normalizeFirm(contact.company_name);
  const actualFirm = normalizeFirm(result.company);
  if (!expectedFirm || !actualFirm) return true;
  const actualTokens = new Set(actualFirm.split(/\s+/).filter(Boolean));
  return expectedFirm.split(/\s+/).filter(Boolean).some(token => actualTokens.has(token));
}

function hasAnyContactMethod(contact, patch = {}) {
  return Boolean(contact.email || contact.linkedin_url || patch.email || patch.linkedin_url);
}

async function resolveDealId() {
  if (!DEAL) return null;
  const isUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(DEAL);
  let query = sb.from('deals')
    .select('id, name')
    .limit(1);
  query = isUuid
    ? query.eq('id', DEAL)
    : query.ilike('name', `%${DEAL}%`);
  const { data, error } = await query.maybeSingle();
  if (error) throw new Error(error.message);
  if (!data) throw new Error(`Deal not found: ${DEAL}`);
  return data.id;
}

async function main() {
  if (!process.env.REVERSECONTACT_API_KEY && !process.env.REVERSE_CONTACT_API_KEY) {
    throw new Error('Missing REVERSECONTACT_API_KEY');
  }

  const dealId = await resolveDealId();
  let query = sb.from('contacts')
    .select('id, deal_id, name, company_name, job_title, email, linkedin_url, pipeline_stage, notes')
    .not('name', 'is', null)
    .not('pipeline_stage', 'in', '("Archived","Inactive","Deleted — Do Not Contact","Suppressed — Opt Out")');

  if (MISSING_BOTH_ONLY) {
    query = query.is('email', null).is('linkedin_url', null);
  } else if (EMAILS_ONLY) {
    query = query.is('email', null).not('linkedin_url', 'is', null);
  } else if (LINKEDIN_FROM_EMAIL) {
    query = query.not('email', 'is', null).is('linkedin_url', null);
  } else {
    query = query.or(SHOULD_FIND_EMAILS
      ? 'linkedin_url.is.null,email.is.null'
      : 'linkedin_url.is.null');
  }

  query = query
    .order('updated_at', { ascending: true })
    .limit(LIMIT);
  if (dealId) query = query.eq('deal_id', dealId);

  const { data: contacts, error } = await query;
  if (error) throw new Error(error.message);

  console.log(`ReverseContact backfill: ${LIVE ? 'LIVE' : 'DRY RUN'} · contacts=${contacts?.length || 0} · findEmails=${SHOULD_FIND_EMAILS} · emailsOnly=${EMAILS_ONLY} · linkedinFromEmail=${LINKEDIN_FROM_EMAIL} · missingBothOnly=${MISSING_BOTH_ONLY} · markInactiveNoContact=${MARK_INACTIVE_NO_CONTACT}`);

  let linkedinFound = 0;
  let emailsFound = 0;
  let skippedMismatch = 0;
  let markedInactive = 0;
  let providerErrors = 0;

  for (const contact of contacts || []) {
    const patch = {};
    let rc = null;
    let lookupHadProviderError = false;

    if (!contact.linkedin_url) {
      try {
        rc = contact.email
          ? await enrichByEmail(contact.email)
          : await searchPerson({ name: contact.name, companyName: contact.company_name, title: contact.job_title, perPage: 3, throwOnProviderError: true });
      } catch (err) {
        lookupHadProviderError = true;
        providerErrors++;
        console.warn(`PROVIDER ERROR ${contact.name} @ ${contact.company_name || '-'}: ${String(err.message || err).slice(0, 160)}`);
      }
      if (rc?.linkedInUrl && profileMatches(contact, rc)) {
        patch.linkedin_url = rc.linkedInUrl;
        patch.enrichment_source = 'reversecontact';
        linkedinFound++;
      } else if (rc?.linkedInUrl) {
        skippedMismatch++;
      }
    }

    if (SHOULD_FIND_EMAILS && !contact.email) {
      const emailResult = await findEmail({
        linkedInUrl: patch.linkedin_url || contact.linkedin_url,
        fullName: contact.name,
        companyName: contact.company_name,
      });
      if (emailResult?.email && profileMatches(contact, emailResult)) {
        patch.email = emailResult.email;
        patch.enrichment_source = 'reversecontact';
        emailsFound++;
      } else if (emailResult?.email) {
        skippedMismatch++;
      }
    }

    if (Object.keys(patch).length) {
      patch.updated_at = new Date().toISOString();
      console.log(`${LIVE ? 'UPDATE' : 'WOULD UPDATE'} ${contact.name} @ ${contact.company_name || '-'}: ${JSON.stringify(patch)}`);
      if (LIVE) await sb.from('contacts').update(patch).eq('id', contact.id);
    } else if (MARK_INACTIVE_NO_CONTACT && !lookupHadProviderError && !hasAnyContactMethod(contact, patch)) {
      const marker = `[REVERSECONTACT_NO_CONTACT ${new Date().toISOString()}] no email or LinkedIn found`;
      const notes = `${String(contact.notes || '').trim()}\n${marker}`.trim().slice(0, 4000);
      const inactivePatch = {
        pipeline_stage: 'Inactive',
        enrichment_status: 'no_contact_found',
        notes,
        updated_at: new Date().toISOString(),
      };
      console.log(`${LIVE ? 'MARK INACTIVE' : 'WOULD MARK INACTIVE'} ${contact.name} @ ${contact.company_name || '-'}: no email or LinkedIn found`);
      if (LIVE) {
        const { error: updateError } = await sb.from('contacts').update(inactivePatch).eq('id', contact.id);
        if (updateError) {
          console.warn(`MARK INACTIVE FAILED ${contact.name}: ${updateError.message}`);
        } else {
          markedInactive++;
        }
      } else {
        markedInactive++;
      }
    }
  }

  console.log(`Done. linkedinFound=${linkedinFound} emailsFound=${emailsFound} skippedMismatch=${skippedMismatch} markedInactive=${markedInactive} providerErrors=${providerErrors}`);
}

main().catch(err => {
  console.error(err.message);
  process.exit(1);
});
