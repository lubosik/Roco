/**
 * Backfill emails for contacts with enrichment_status='linkedin_only'.
 * These contacts had Apify run but no email was found, and no linkedin_url is stored.
 * Uses ReverseContact findEmail V2 (companyDomain + name) or legacy (companyName + name).
 *
 * Usage:
 *   node scripts/backfillEmailLinkedInOnly.js --limit 50
 *   node scripts/backfillEmailLinkedInOnly.js --live --limit 170
 *   node scripts/backfillEmailLinkedInOnly.js --live --deal "Project Electrify" --limit 200
 */

import dotenv from 'dotenv';
dotenv.config();

process.env.REVERSECONTACT_ASYNC_MAX_POLLS ||= '3';
process.env.REVERSECONTACT_ASYNC_POLL_MS ||= '5000';
process.env.REVERSECONTACT_MIN_INTERVAL_MS ||= '1500';

import { createClient } from '@supabase/supabase-js';
import { findEmail } from '../enrichment/reverseContactEnricher.js';

const LIVE = process.argv.includes('--live');
const limitIdx = process.argv.indexOf('--limit');
const dealIdx = process.argv.indexOf('--deal');
const LIMIT = limitIdx >= 0 ? Math.max(1, Number(process.argv[limitIdx + 1] || 50)) : 50;
const DEAL = dealIdx >= 0 ? String(process.argv[dealIdx + 1] || '').trim() : '';

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

// Known domains for PE/VC/family office firms — avoids guessing + improves V2 hit rate
const FIRM_DOMAIN_MAP = {
  'Arsenal Capital Partners': 'arsenalcapital.com',
  'Centerbridge Partners': 'centerbridge.com',
  'GI Partners': 'gipartners.com',
  'HPS Investment Partners': 'hpspartners.com',
  'Huron Capital': 'huroncapital.com',
  'Incline Equity Partners': 'inclineequity.com',
  'Main Street Capital': 'mainstcapital.com',
  'Nautic Partners': 'nautic.com',
  'Permanent Equity': 'permanentequity.com',
  'Peterson Partners': 'petersonpartners.com',
  'Renovus Capital': 'renovuscapital.com',
  'Revelstoke Capital': 'revelstokecap.com',
  'Shore Capital Partners': 'shorecappartners.com',
  'Sheridan Capital Partners': 'sheridancap.com',
  'Quad-C Management': 'quadc.com',
  'Cortec Group': 'cortecgroup.com',
  'Kingswood Capital Management': 'kingswoodcap.com',
  'Azalea Capital': 'azaleacapital.com',
  'Plexus Capital': 'plexuscapital.com',
  'CapX Partners': 'capxpartners.com',
  'Mainsail Partners': 'mainsailpartners.com',
  'Tecum Capital': 'tecumcapital.com',
  'Kidd & Company': 'kiddandco.com',
  'Ampersand Capital Partners': 'ampersandcapital.com',
  'Tarsadia Investments': 'tarsadia.com',
  'Beamonte Investments': 'beamonte.com',
  'Equable Capital': 'equablecapital.com',
  'Bridge Point Capital': 'bridgepointcap.com',
  'ValorBridge Partners': 'valorbridge.com',
  'Quad-C': 'quadc.com',
};

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

function parseName(fullName) {
  const parts = String(fullName || '').trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return { firstName: null, lastName: null };
  if (parts.length === 1) return { firstName: parts[0], lastName: null };
  return { firstName: parts[0], lastName: parts[parts.length - 1] };
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

  let query = sb.from('contacts')
    .select('id, deal_id, name, company_name, job_title, email, linkedin_url, pipeline_stage, enrichment_status')
    .is('email', null)
    .eq('enrichment_status', 'linkedin_only')
    .not('name', 'is', null)
    .not('pipeline_stage', 'in', '("Archived","Inactive","Deleted — Do Not Contact","Suppressed — Opt Out")')
    .order('updated_at', { ascending: true })
    .limit(LIMIT);

  if (dealId) query = query.eq('deal_id', dealId);

  const { data: contacts, error } = await query;
  if (error) throw new Error(error.message);

  console.log(`Email backfill (linkedin_only): ${LIVE ? 'LIVE' : 'DRY RUN'} · contacts=${contacts?.length || 0}`);

  let found = 0;
  let skippedMismatch = 0;
  let noResult = 0;
  let errors = 0;

  for (const contact of contacts || []) {
    const { firstName, lastName } = parseName(contact.name);
    const companyDomain = FIRM_DOMAIN_MAP[contact.company_name] || null;

    let result = null;
    try {
      result = await findEmail({
        fullName: contact.name,
        firstName,
        lastName,
        companyDomain,
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
    if (result.linkedInUrl && !contact.linkedin_url) patch.linkedin_url = result.linkedInUrl;

    console.log(`${LIVE ? 'UPDATE' : 'WOULD UPDATE'} ${contact.name} @ ${contact.company_name || '-'} → ${result.email}${patch.linkedin_url ? ` + ${patch.linkedin_url}` : ''}`);

    if (LIVE) {
      const { error: updateError } = await sb.from('contacts').update(patch).eq('id', contact.id);
      if (updateError) console.warn(`UPDATE FAILED ${contact.name}: ${updateError.message}`);
    }
  }

  console.log(`\nDone. found=${found} skippedMismatch=${skippedMismatch} noResult=${noResult} errors=${errors}`);
  console.log(`Credits used ≈ ${found * 4} (at 4/call for matched lookups)`);
}

main().catch(err => {
  console.error(err.message);
  process.exit(1);
});
