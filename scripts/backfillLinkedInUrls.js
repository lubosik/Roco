/**
 * scripts/backfillLinkedInUrls.js
 *
 * One-time (re-runnable) backfill: for every contact that has a name but no
 * clean LinkedIn URL, search Unipile by "name + company" and store the result.
 *
 * Respects the shared linkedin_ratelimit.json cooldown file — if LinkedIn is
 * currently throttled the script exits immediately and tells you when to retry.
 *
 * Safe to re-run: skips contacts that already have a clean URL + provider_id.
 *
 * Usage:
 *   node scripts/backfillLinkedInUrls.js              # process up to 200
 *   node scripts/backfillLinkedInUrls.js --dry-run    # preview, no writes
 *   node scripts/backfillLinkedInUrls.js --limit 50   # process at most N contacts
 *   node scripts/backfillLinkedInUrls.js --batch 8    # contacts per run (default 8)
 */

import dotenv from 'dotenv';
dotenv.config();

import { createClient } from '@supabase/supabase-js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RATE_LIMIT_FILE = path.join(__dirname, '..', 'linkedin_ratelimit.json');

const DRY_RUN   = process.argv.includes('--dry-run');
const limitIdx  = process.argv.findIndex(a => a === '--limit');
const batchIdx  = process.argv.findIndex(a => a === '--batch');
const MAX_TOTAL = limitIdx !== -1 ? (Number(process.argv[limitIdx + 1]) || 200) : 200;
const BATCH     = batchIdx !== -1 ? (Number(process.argv[batchIdx + 1]) || 8)   : 8;
const DELAY_MS  = 3000; // 3 s between searches — conservative to avoid 429

const UNIPILE_DSN                = process.env.UNIPILE_DSN;
const UNIPILE_API_KEY            = process.env.UNIPILE_API_KEY;
const UNIPILE_LINKEDIN_ACCOUNT_ID = process.env.UNIPILE_LINKEDIN_ACCOUNT_ID;

const sb = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_ANON_KEY,
);

if (!UNIPILE_API_KEY || !UNIPILE_LINKEDIN_ACCOUNT_ID) {
  console.error('❌  Missing UNIPILE_API_KEY or UNIPILE_LINKEDIN_ACCOUNT_ID');
  process.exit(1);
}

// ── Rate-limit helpers (mirrors core/linkedInRateLimit.js) ──────────────────

function isRateLimited() {
  try {
    const state = JSON.parse(fs.readFileSync(RATE_LIMIT_FILE, 'utf8'));
    if (!state.rateLimitedUntil) return false;
    if (Date.now() >= state.rateLimitedUntil) return false;
    return state.rateLimitedUntil;
  } catch { return false; }
}

function markRateLimited() {
  const until = Date.now() + 45 * 60 * 1000;
  try {
    const state = fs.existsSync(RATE_LIMIT_FILE)
      ? JSON.parse(fs.readFileSync(RATE_LIMIT_FILE, 'utf8'))
      : {};
    state.rateLimitedUntil = until;
    fs.writeFileSync(RATE_LIMIT_FILE, JSON.stringify(state, null, 2));
  } catch {}
  return until;
}

// ── URL / name helpers ───────────────────────────────────────────────────────

function cleanUrl(raw) {
  if (!raw) return null;
  const m = String(raw).match(/^https?:\/\/([a-z]{2,3}\.)?linkedin\.com\/in\/([^?#\s/]+)/i);
  return m ? `https://www.linkedin.com/in/${m[2]}` : null;
}

function normName(v) {
  return String(v || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function namesMatch(a, b) {
  const ta = normName(a).split(/\s+/).filter(Boolean);
  const tb = normName(b).split(/\s+/).filter(Boolean);
  if (!ta.length || !tb.length) return false;
  return ta.filter(t => tb.includes(t)).length >= Math.min(2, ta.length, tb.length);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── LinkedIn search ──────────────────────────────────────────────────────────

async function searchByName(name, company) {
  const query = company ? `${name} ${company}` : name;
  const url   = `${UNIPILE_DSN}/api/v1/linkedin/search` +
                `?account_id=${UNIPILE_LINKEDIN_ACCOUNT_ID}&limit=5`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'X-API-KEY': UNIPILE_API_KEY, 'Content-Type': 'application/json', accept: 'application/json' },
    body: JSON.stringify({ api: 'classic', category: 'people', keywords: query }),
  });
  if (res.status === 429) throw Object.assign(new Error('429'), { status: 429 });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data  = await res.json();
  const hits  = data.items || [];
  return hits.find(h => {
    const hn = String(h.name || [h.first_name, h.last_name].filter(Boolean).join(' ')).trim();
    return namesMatch(name, hn);
  }) || null;
}

// ── main ─────────────────────────────────────────────────────────────────────

async function main() {
  // Check shared rate-limit file
  const limitUntil = isRateLimited();
  if (limitUntil) {
    const mins = Math.ceil((limitUntil - Date.now()) / 60000);
    console.log(`⏳  LinkedIn rate-limited for ~${mins} more minute(s). Re-run after ${new Date(limitUntil).toLocaleTimeString()}.`);
    return;
  }

  console.log(`\n🔍 Backfill LinkedIn URLs — ${DRY_RUN ? 'DRY RUN' : 'LIVE'} — batch ${BATCH}, max ${MAX_TOTAL}\n`);

  // Contacts missing a clean URL: no URL at all, OR URL has query-string junk
  const { data: contacts, error } = await sb
    .from('contacts')
    .select('id, name, company_name, linkedin_url, linkedin_provider_id, pipeline_stage')
    .not('name', 'is', null)
    .neq('name', '')
    .or('linkedin_url.is.null,linkedin_url.like.%?%,linkedin_url.like.%miniProfile%,linkedin_provider_id.is.null')
    .not('pipeline_stage', 'in', '("Inactive","do_not_contact","skipped_no_name","skipped_wrong_profile","profile_mismatch","linkedin_weekly_limit")')
    .order('created_at', { ascending: false })
    .limit(MAX_TOTAL);

  if (error) { console.error('DB error:', error.message); process.exit(1); }

  const todo = contacts.filter(c => {
    const existing = cleanUrl(c.linkedin_url);
    // Skip if already clean URL + provider_id
    if (existing && c.linkedin_url === existing && c.linkedin_provider_id) return false;
    return true;
  });

  console.log(`${todo.length} contacts need enrichment (capped at batch size ${BATCH} this run)\n`);

  const slice   = todo.slice(0, BATCH);
  let updated   = 0;
  let notFound  = 0;

  for (let i = 0; i < slice.length; i++) {
    const c    = slice[i];
    const name = String(c.name || '').trim();
    const co   = String(c.company_name || '').trim();

    process.stdout.write(`[${i + 1}/${slice.length}] ${name} @ ${co || '—'} … `);

    try {
      const hit = await searchByName(name, co);
      if (!hit) {
        notFound++;
        console.log('not found');
      } else {
        const url = cleanUrl(
          hit.public_profile_url || hit.profile_url ||
          (hit.public_identifier ? `https://www.linkedin.com/in/${hit.public_identifier}` : null),
        );
        const pid = hit.id || null;
        console.log(`✓  ${url || 'no clean URL'}`);
        if (url && !DRY_RUN) {
          await sb.from('contacts').update({
            linkedin_url:         url,
            linkedin_provider_id: pid || c.linkedin_provider_id || null,
            updated_at:           new Date().toISOString(),
          }).eq('id', c.id);
        }
        updated++;
      }
    } catch (err) {
      if (err.status === 429 || String(err.message).includes('429')) {
        const until = markRateLimited();
        console.log(`\n⚠️  Rate-limited by LinkedIn — paused for 45 min (retry after ${new Date(until).toLocaleTimeString()})`);
        break;
      }
      notFound++;
      console.log(`error: ${err.message?.slice(0, 60)}`);
    }

    if (i < slice.length - 1) await sleep(DELAY_MS);
  }

  const remaining = todo.length - slice.length;
  console.log(`\n✅  Done.`);
  console.log(`   Updated:   ${updated}`);
  console.log(`   Not found: ${notFound}`);
  if (remaining > 0) {
    console.log(`   Remaining: ${remaining} more contacts — re-run to continue`);
  } else {
    console.log(`   All contacts processed.`);
  }
}

main().catch(err => { console.error(err); process.exit(1); });
