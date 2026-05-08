/**
 * Find emails for contacts that have a LinkedIn URL but no email.
 * Uses ReverseContact V2 findEmail with the LinkedIn URL — most accurate path.
 *
 * Usage:
 *   node scripts/backfillEmailFromLinkedIn.js --limit 50
 *   node scripts/backfillEmailFromLinkedIn.js --live --limit 250
 *   node scripts/backfillEmailFromLinkedIn.js --live --include-archived --limit 500
 */

import dotenv from 'dotenv';
dotenv.config();

process.env.REVERSECONTACT_ASYNC_MAX_POLLS ||= '3';
process.env.REVERSECONTACT_ASYNC_POLL_MS ||= '5000';
process.env.REVERSECONTACT_MIN_INTERVAL_MS ||= '1500';

import { createClient } from '@supabase/supabase-js';
import { findEmail } from '../enrichment/reverseContactEnricher.js';

const LIVE = process.argv.includes('--live');
const INCLUDE_ARCHIVED = process.argv.includes('--include-archived');
const limitIdx = process.argv.indexOf('--limit');
const dealIdx = process.argv.indexOf('--deal');
const LIMIT = limitIdx >= 0 ? Math.max(1, Number(process.argv[limitIdx + 1] || 50)) : 50;
const DEAL = dealIdx >= 0 ? String(process.argv[dealIdx + 1] || '').trim() : '';

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

function normalizeName(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/\b(cfa|cpa|mba|phd|md|jr|sr|ii|iii|iv)\b/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function profileMatches(contact, result) {
  if (!result?.email) return false;
  const expectedName = normalizeName(contact.name);
  const actualName = normalizeName(result.fullName || [result.firstName, result.lastName].filter(Boolean).join(' '));
  if (!expectedName || !actualName) return true;
  const expected = expectedName.split(/\s+/).filter(Boolean);
  const actual = new Set(actualName.split(/\s+/).filter(Boolean));
  const shared = expected.filter(t => actual.has(t)).length;
  return shared >= Math.min(2, expected.length);
}

async function resolveDealId() {
  if (!DEAL) return null;
  const isUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(DEAL);
  const q = sb.from('deals').select('id, name').limit(1);
  const { data, error } = await (isUuid ? q.eq('id', DEAL) : q.ilike('name', `%${DEAL}%`)).maybeSingle();
  if (error) throw new Error(error.message);
  if (!data) throw new Error(`Deal not found: ${DEAL}`);
  return data.id;
}

async function main() {
  if (!process.env.REVERSECONTACT_API_KEY && !process.env.REVERSE_CONTACT_API_KEY) {
    throw new Error('Missing REVERSECONTACT_API_KEY');
  }

  const dealId = await resolveDealId();

  const excludeStages = INCLUDE_ARCHIVED
    ? '("Deleted — Do Not Contact","Suppressed — Opt Out","Email Sent","In Conversation","Replied","Bounced","Unsubscribed","DM Sent")'
    : '("Archived","Inactive","Deleted — Do Not Contact","Suppressed — Opt Out","Email Sent","In Conversation","Replied","Bounced","Unsubscribed","DM Sent")';

  let query = sb.from('contacts')
    .select('id, deal_id, name, company_name, job_title, email, linkedin_url, pipeline_stage, enrichment_status')
    .is('email', null)
    .not('linkedin_url', 'is', null)
    .not('name', 'is', null)
    .not('pipeline_stage', 'in', excludeStages)
    .order('updated_at', { ascending: true })
    .limit(LIMIT);

  if (dealId) query = query.eq('deal_id', dealId);

  const { data: contacts, error } = await query;
  if (error) throw new Error(error.message);

  console.log(`findEmail-via-LinkedIn: ${LIVE ? 'LIVE' : 'DRY RUN'} · contacts=${contacts?.length || 0} · includeArchived=${INCLUDE_ARCHIVED}`);

  let found = 0;
  let skippedMismatch = 0;
  let noResult = 0;
  let errors = 0;

  for (const contact of contacts || []) {
    let result = null;
    try {
      result = await findEmail({
        linkedInUrl: contact.linkedin_url,
        fullName: contact.name,
        companyName: contact.company_name,
      });
    } catch (err) {
      errors++;
      console.warn(`ERROR ${contact.name} @ ${contact.company_name || '-'}: ${String(err.message).slice(0, 120)}`);
      continue;
    }

    if (!result?.email) {
      noResult++;
      console.log(`NO EMAIL   ${contact.name} @ ${contact.company_name || '-'}`);
      continue;
    }

    if (!profileMatches(contact, result)) {
      skippedMismatch++;
      console.log(`MISMATCH   ${contact.name} @ ${contact.company_name || '-'} → got ${result.fullName} <${result.email}>`);
      continue;
    }

    found++;
    const patch = {
      email: result.email,
      enrichment_status: 'enriched',
      enrichment_source: 'reversecontact',
      pipeline_stage: 'Enriched',
      updated_at: new Date().toISOString(),
    };

    console.log(`${LIVE ? 'UPDATE' : 'WOULD UPDATE'} ${contact.name} @ ${contact.company_name || '-'} → ${result.email}`);

    if (LIVE) {
      const { error: updateError } = await sb.from('contacts').update(patch).eq('id', contact.id);
      if (updateError) console.warn(`UPDATE FAILED ${contact.name}: ${updateError.message}`);
    }
  }

  console.log(`\nDone. found=${found} skippedMismatch=${skippedMismatch} noResult=${noResult} errors=${errors}`);
  console.log(`Credits used ≈ ${found * 4}`);
}

main().catch(err => {
  console.error(err.message);
  process.exit(1);
});
