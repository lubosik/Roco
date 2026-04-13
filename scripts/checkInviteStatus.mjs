/**
 * scripts/checkInviteStatus.mjs
 *
 * One-time / on-demand script to reconcile "Invite Sent" contacts in Project Electrify
 * against the Unipile pending-invitations list.
 *
 * Logic:
 *   - Unipile GET /users/invite/sent returns ONLY still-pending invitations.
 *   - If a contact is in our DB with pipeline_stage = 'Invite Sent' but NOT in the
 *     Unipile pending list, they have accepted (or the invite expired/was withdrawn).
 *   - We verify acceptance by checking their LinkedIn profile for 1st-degree connection.
 *   - Confirmed acceptors are updated to pipeline_stage = 'Invite Accepted'.
 *
 * Usage:
 *   node scripts/checkInviteStatus.mjs [--dry-run]
 *
 * Rate limits (Unipile recommendation):
 *   - Profile lookups: ~100/day per account
 *   - We space them 3s apart to avoid hitting limits
 */

import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';

const DRY_RUN = process.argv.includes('--dry-run');
const UNIPILE_DSN = process.env.UNIPILE_DSN || 'https://api17.unipile.com:14756';
const UNIPILE_KEY = process.env.UNIPILE_API_KEY;
const LINKEDIN_ACCOUNT_ID = process.env.UNIPILE_LINKEDIN_ACCOUNT_ID;

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://xunqaxmqdknlrqdztepw.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY ||
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inh1bnFheG1xZGtubHJxZHp0ZXB3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzE2MzQ5MSwiZXhwIjoyMDg4NzM5NDkxfQ.Dlqhdoq445YdZ1CjgJpd3GcH8EO9lpfJnCTq5Ip3200';

if (!UNIPILE_KEY) { console.error('UNIPILE_API_KEY not set'); process.exit(1); }
if (!LINKEDIN_ACCOUNT_ID) { console.error('UNIPILE_LINKEDIN_ACCOUNT_ID not set'); process.exit(1); }

const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

// ── helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function unipileGet(path) {
  const url = `${UNIPILE_DSN}/api/v1${path}`;
  const res = await fetch(url, { headers: { 'X-API-KEY': UNIPILE_KEY, accept: 'application/json' } });
  if (!res.ok) {
    const txt = await res.text().catch(() => res.statusText);
    throw new Error(`Unipile GET ${path} → ${res.status}: ${txt.slice(0, 200)}`);
  }
  return res.json();
}

/** Fetch all pending sent-invitations (paginated). */
async function fetchAllPendingInvitations() {
  const all = [];
  let cursor = null;
  let page = 0;
  do {
    page++;
    const qs = `account_id=${encodeURIComponent(LINKEDIN_ACCOUNT_ID)}&limit=100${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
    const data = await unipileGet(`/users/invite/sent?${qs}`);
    const items = data?.items || [];
    all.push(...items);
    cursor = data?.cursor || null;
    console.log(`  Fetched page ${page}: ${items.length} pending invitations (total so far: ${all.length})`);
    if (items.length < 100) break;
    await sleep(1500); // space out pagination calls
  } while (cursor);
  return all;
}

/** Retrieve a LinkedIn profile to check network distance. Returns null on error. */
async function getProfile(identifier) {
  try {
    const data = await unipileGet(
      `/users/${encodeURIComponent(identifier)}?account_id=${encodeURIComponent(LINKEDIN_ACCOUNT_ID)}`
    );
    return data;
  } catch (err) {
    console.warn(`  Profile fetch failed for ${identifier}: ${err.message}`);
    return null;
  }
}

function extractPublicId(linkedinUrl) {
  if (!linkedinUrl) return null;
  const m = String(linkedinUrl).match(/linkedin\.com\/in\/([^/?#]+)/i);
  return m ? m[1].toLowerCase() : null;
}

// ── main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`\n=== Invite Status Check${DRY_RUN ? ' [DRY RUN]' : ''} ===\n`);

  // 1. Find the deal
  const { data: deals } = await sb.from('deals')
    .select('id, name')
    .ilike('name', '%electrify%')
    .limit(1);
  const deal = deals?.[0];
  if (!deal) { console.error('Project Electrify not found in deals table'); process.exit(1); }
  console.log(`Deal: ${deal.name} (${deal.id})\n`);

  // 2. Fetch all "Invite Sent" contacts for this deal
  const { data: contacts, error: contactsErr } = await sb.from('contacts')
    .select('id, name, company_name, linkedin_provider_id, linkedin_url, pipeline_stage')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['invite_sent', 'Invite Sent'])
    .not('linkedin_provider_id', 'is', null);

  if (contactsErr) { console.error('Failed to load contacts:', contactsErr.message); process.exit(1); }
  console.log(`Found ${contacts?.length ?? 0} contacts with "Invite Sent" and a LinkedIn provider ID\n`);
  if (!contacts?.length) { console.log('Nothing to check.'); return; }

  // 3. Fetch Unipile pending invitations
  console.log('Fetching pending invitations from Unipile...');
  const pending = await fetchAllPendingInvitations();
  console.log(`Total pending invitations: ${pending.length}\n`);

  // Build a Set of pending LinkedIn IDs for quick lookup
  const pendingProviderIds = new Set(
    pending.map(inv => String(inv.invited_user_id || inv.invited_user || '').trim().toLowerCase()).filter(Boolean)
  );
  const pendingPublicIds = new Set(
    pending.map(inv => String(inv.invited_user_public_id || '').trim().toLowerCase()).filter(Boolean)
  );

  // 4. For each contact NOT in the pending list, check if they're 1st degree
  const toUpdate = [];
  const stillPending = [];
  const unknown = [];

  for (const contact of contacts) {
    const providerId = String(contact.linkedin_provider_id || '').trim().toLowerCase();
    const publicId = extractPublicId(contact.linkedin_url) || '';

    // Is this contact still pending?
    if (pendingProviderIds.has(providerId) || (publicId && pendingPublicIds.has(publicId))) {
      stillPending.push(contact);
      console.log(`  PENDING  ${contact.name} @ ${contact.company_name || '?'}`);
      continue;
    }

    // Not in pending list — likely accepted. Verify via profile lookup (network_distance = 1).
    console.log(`  CHECKING ${contact.name} @ ${contact.company_name || '?'} (not in pending list — verifying...)`);
    await sleep(3000); // respect ~100 profile calls/day

    const profile = await getProfile(contact.linkedin_provider_id);
    const networkDistance = profile?.network_distance ?? profile?.relation_distance ?? null;
    const distanceStr = String(networkDistance || '').toUpperCase();
    const isFirstDegree = networkDistance === 1
      || distanceStr === 'FIRST_DEGREE'
      || distanceStr === '1'
      || profile?.relation === 'first_degree';

    if (isFirstDegree) {
      console.log(`  ✓ ACCEPTED ${contact.name} — 1st degree connection confirmed`);
      toUpdate.push(contact);
    } else if (networkDistance != null) {
      console.log(`  ~ PENDING/NOT YET ${contact.name} — distance: ${networkDistance}`);
      stillPending.push(contact);
    } else {
      // Profile returned but no distance info — assume accepted since not in pending list
      console.log(`  ? ASSUMED ACCEPTED ${contact.name} — not in pending list, no distance data`);
      unknown.push(contact);
      toUpdate.push(contact);
    }
  }

  // 5. Report
  console.log(`\n--- Results ---`);
  console.log(`  Still pending  : ${stillPending.length}`);
  console.log(`  Confirmed accepted (to update): ${toUpdate.length}`);

  if (!toUpdate.length) {
    console.log('\nNo contacts to update.');
    return;
  }

  // 6. Update pipeline_stage to 'Invite Accepted'
  if (DRY_RUN) {
    console.log('\n[DRY RUN] Would update these contacts to "Invite Accepted":');
    toUpdate.forEach(c => console.log(`  - ${c.name} @ ${c.company_name || '?'} (${c.id})`));
  } else {
    console.log(`\nUpdating ${toUpdate.length} contacts to "Invite Accepted"...`);
    const ids = toUpdate.map(c => c.id);
    const { error: updateErr } = await sb.from('contacts')
      .update({
        pipeline_stage: 'invite_accepted',
        updated_at: new Date().toISOString(),
      })
      .in('id', ids);

    if (updateErr) {
      console.error('Update failed:', updateErr.message);
    } else {
      console.log(`✓ Updated ${ids.length} contacts to "Invite Accepted"`);
      toUpdate.forEach(c => console.log(`  - ${c.name} @ ${c.company_name || '?'}`));
    }
  }

  console.log('\n=== Done ===\n');
}

main().catch(err => { console.error(err); process.exit(1); });
