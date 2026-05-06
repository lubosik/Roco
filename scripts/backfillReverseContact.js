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
 */

import dotenv from 'dotenv';
dotenv.config();

import { createClient } from '@supabase/supabase-js';
import { enrichByEmail, searchPerson, findEmail } from '../enrichment/reverseContactEnricher.js';

const LIVE = process.argv.includes('--live');
const FIND_EMAILS = process.argv.includes('--find-emails');
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

async function resolveDealId() {
  if (!DEAL) return null;
  const { data, error } = await sb.from('deals')
    .select('id, name')
    .or(`id.eq.${DEAL},name.ilike.%${DEAL}%`)
    .limit(1)
    .maybeSingle();
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
    .select('id, deal_id, name, company_name, job_title, email, linkedin_url, pipeline_stage')
    .not('name', 'is', null)
    .not('pipeline_stage', 'in', '("Archived","Inactive","Deleted — Do Not Contact","Suppressed — Opt Out")')
    .or(FIND_EMAILS
      ? 'linkedin_url.is.null,email.is.null'
      : 'linkedin_url.is.null')
    .order('updated_at', { ascending: true })
    .limit(LIMIT);
  if (dealId) query = query.eq('deal_id', dealId);

  const { data: contacts, error } = await query;
  if (error) throw new Error(error.message);

  console.log(`ReverseContact backfill: ${LIVE ? 'LIVE' : 'DRY RUN'} · contacts=${contacts?.length || 0} · findEmails=${FIND_EMAILS}`);

  let linkedinFound = 0;
  let emailsFound = 0;
  let skippedMismatch = 0;

  for (const contact of contacts || []) {
    const patch = {};
    let rc = null;

    if (!contact.linkedin_url) {
      rc = contact.email
        ? await enrichByEmail(contact.email)
        : await searchPerson({ name: contact.name, companyName: contact.company_name, title: contact.job_title, perPage: 3 });
      if (rc?.linkedInUrl && profileMatches(contact, rc)) {
        patch.linkedin_url = rc.linkedInUrl;
        patch.enrichment_source = 'reversecontact';
        linkedinFound++;
      } else if (rc?.linkedInUrl) {
        skippedMismatch++;
      }
    }

    if (FIND_EMAILS && !contact.email) {
      const emailResult = await findEmail({
        linkedInUrl: patch.linkedin_url || contact.linkedin_url,
        fullName: contact.name,
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
    }
  }

  console.log(`Done. linkedinFound=${linkedinFound} emailsFound=${emailsFound} skippedMismatch=${skippedMismatch}`);
}

main().catch(err => {
  console.error(err.message);
  process.exit(1);
});
