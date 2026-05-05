/**
 * scripts/auditOutreach.js
 * Full audit of Project Electrify outreach state.
 *
 * Checks:
 *   1. Real LinkedIn invitations sent via Unipile vs what DB says
 *   2. Real connections (accepted invites) via Unipile
 *   3. Contacts marked invite_sent/invite_accepted in DB but never actually sent
 *   4. Email stats — sent emails from outreach_events vs emails table
 *   5. True contact count in Supabase for the deal
 *   6. Which contacts are stuck in research loop (have person_researched but keep re-queuing)
 *
 * Usage:
 *   node scripts/auditOutreach.js [--fix] [--deal <deal-name-or-id>]
 *
 *   --fix   Write corrections back to Supabase (reset fake invite_sent statuses)
 *   --deal  Target a specific deal (default: first active deal)
 */

import { config as dotenvConfig } from 'dotenv';
dotenvConfig({ path: new URL('../.env', import.meta.url).pathname });

import { createClient } from '@supabase/supabase-js';

const SHOULD_FIX = process.argv.includes('--fix');
const DEAL_ARG   = (() => { const i = process.argv.indexOf('--deal'); return i !== -1 ? process.argv[i + 1] : null; })();

const DSN     = process.env.UNIPILE_DSN || 'https://api34.unipile.com:16411';
const API_KEY = process.env.UNIPILE_API_KEY;
const LI_ACCT = process.env.UNIPILE_LINKEDIN_ACCOUNT_ID;

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

// ── Unipile helpers ───────────────────────────────────────────────────────────

async function unipileGet(path) {
  const base = DSN.startsWith('http') ? DSN : `https://${DSN}`;
  const url  = `${base}/api/v1${path}`;
  const res  = await fetch(url, { headers: { 'X-API-KEY': API_KEY, accept: 'application/json' } });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Unipile GET ${path} → ${res.status}: ${body.slice(0, 200)}`);
  }
  return res.json();
}

async function getLinkedInAccounts() {
  const data = await unipileGet('/accounts?limit=50');
  const accounts = data?.items || data?.accounts || [];
  return accounts.filter(a => String(a.type || a.provider || '').toLowerCase().includes('linkedin'));
}

async function getSentInvitations(accountId, limit = 100) {
  const items = [];
  let cursor = null;
  let fetched = 0;
  do {
    const qs = `account_id=${encodeURIComponent(accountId)}&limit=100${cursor ? `&cursor=${cursor}` : ''}`;
    const data = await unipileGet(`/users/invite/sent?${qs}`).catch(e => {
      console.warn(`  [!] getSentInvitations page failed: ${e.message}`);
      return null;
    });
    if (!data) break;
    const page = data?.items || [];
    items.push(...page);
    cursor = data?.cursor || data?.next_cursor || null;
    fetched += page.length;
  } while (cursor && fetched < limit);
  return items;
}

async function getRelations(accountId) {
  // Unipile relations endpoint — lists 1st-degree connections
  const items = [];
  let cursor = null;
  let fetched = 0;
  do {
    const qs = `account_id=${encodeURIComponent(accountId)}&limit=100${cursor ? `&cursor=${cursor}` : ''}`;
    const data = await unipileGet(`/users/relations?${qs}`).catch(e => {
      console.warn(`  [!] getRelations page failed: ${e.message}`);
      return null;
    });
    if (!data) break;
    const page = data?.items || [];
    items.push(...page);
    cursor = data?.cursor || data?.next_cursor || null;
    fetched += page.length;
  } while (cursor && fetched < 2000);
  return items;
}

// ── Supabase helpers ──────────────────────────────────────────────────────────

async function getActiveDeal() {
  if (DEAL_ARG) {
    const { data } = await sb.from('deals')
      .select('*')
      .or(`name.ilike.%${DEAL_ARG}%,id.eq.${DEAL_ARG}`)
      .limit(1)
      .maybeSingle();
    if (!data) throw new Error(`Deal not found: ${DEAL_ARG}`);
    return data;
  }
  const { data } = await sb.from('deals').select('*').eq('status', 'ACTIVE').order('created_at').limit(1).maybeSingle();
  if (!data) throw new Error('No active deal found');
  return data;
}

async function getAllContacts(dealId) {
  const PAGE = 1000;
  const all = [];
  let from = 0;
  while (true) {
    const { data, error } = await sb.from('contacts')
      .select('id, name, company_name, pipeline_stage, invite_sent_at, invite_accepted_at, last_email_sent_at, dm_sent_at, linkedin_provider_id, linkedin_url, email, enrichment_status, person_researched, updated_at, sector_focus, past_investments, investment_thesis, notes, investor_score')
      .eq('deal_id', dealId)
      .order('investor_score', { ascending: false })
      .range(from, from + PAGE - 1);
    if (error) throw new Error(`contacts query failed: ${error.message}`);
    if (!data?.length) break;
    all.push(...data);
    if (data.length < PAGE) break;
    from += PAGE;
  }
  return all;
}

async function getOutreachEvents(dealId) {
  const { data } = await sb.from('outreach_events')
    .select('contact_id, event_type, status, created_at')
    .eq('deal_id', dealId)
    .in('event_type', ['EMAIL_SENT', 'LINKEDIN_INVITE_SENT', 'LINKEDIN_DM_SENT'])
    .limit(10000);
  return data || [];
}

// ── Main audit ────────────────────────────────────────────────────────────────

async function main() {
  console.log('\n══════════════════════════════════════════════════════');
  console.log('  ROCO OUTREACH AUDIT');
  console.log(`  Mode: ${SHOULD_FIX ? 'FIX (will write to DB)' : 'DRY RUN (read only)'}`);
  console.log('══════════════════════════════════════════════════════\n');

  // 1. Resolve deal
  const deal = await getActiveDeal();
  console.log(`Deal: ${deal.name} (${deal.id})`);
  console.log(`Status: ${deal.status}`);
  console.log();

  // 2. Check Unipile LinkedIn accounts
  console.log('── LinkedIn Account Health ─────────────────────────────');
  let liAccounts = [];
  try {
    liAccounts = await getLinkedInAccounts();
    if (!liAccounts.length) {
      console.log('  ⚠  No LinkedIn accounts found in Unipile');
    } else {
      for (const acc of liAccounts) {
        const status = acc.connection_status || acc.status || 'unknown';
        const ok = ['ok', 'connected', 'valid'].includes(status.toLowerCase());
        console.log(`  ${ok ? '✓' : '✗'} Account: ${acc.id}`);
        console.log(`     Status: ${status}`);
        console.log(`     Name:   ${acc.name || acc.display_name || '(unknown)'}`);
        if (acc.id !== LI_ACCT) {
          console.log(`  ⚠  This ID (${acc.id}) differs from UNIPILE_LINKEDIN_ACCOUNT_ID (${LI_ACCT})`);
          console.log(`     → Update Railway env var: UNIPILE_LINKEDIN_ACCOUNT_ID=${acc.id}`);
        }
      }
    }
  } catch (err) {
    console.log(`  ✗ Could not list accounts: ${err.message}`);
  }
  console.log();

  // 3. Pull all contacts from DB
  console.log('── Supabase Contact Count ──────────────────────────────');
  const contacts = await getAllContacts(deal.id);
  console.log(`  Total contacts in DB: ${contacts.length}`);

  const byStage = {};
  for (const c of contacts) {
    byStage[c.pipeline_stage || 'null'] = (byStage[c.pipeline_stage || 'null'] || 0) + 1;
  }
  const stageOrder = ['Ranked', 'ranked', 'Enriched', 'enriched', 'Researched', 'researched',
    'invite_sent', 'invite_accepted', 'pending_dm_approval', 'pending_email_approval',
    'Email Sent', 'email_sent', 'DM Sent', 'dm_sent', 'Replied', 'In Conversation',
    'Meeting Booked', 'Archived', 'archived', 'Inactive', 'inactive', 'Skipped', 'skipped'];
  for (const stage of stageOrder) {
    if (byStage[stage]) console.log(`    ${byStage[stage].toString().padStart(4)}  ${stage}`);
  }
  for (const [stage, count] of Object.entries(byStage)) {
    if (!stageOrder.includes(stage)) console.log(`    ${count.toString().padStart(4)}  ${stage}`);
  }
  console.log();

  // 4. Research loop analysis
  console.log('── Research Loop Analysis ──────────────────────────────');
  const LIVE_STAGES = ['Ranked', 'ranked', 'Enriched', 'enriched', 'Researched', 'researched', 'invite_sent'];
  const liveContacts = contacts.filter(c => LIVE_STAGES.includes(c.pipeline_stage));
  const researchedButMissingSector = liveContacts.filter(c =>
    c.person_researched && !c.sector_focus
  );
  const RESEARCH_MARKERS = ['[PERSON_RESEARCH_VERIFIED', '[PERSON_RESEARCH_PARTIAL', '[PERSON_RESEARCHED'];
  const recentlyResearched = liveContacts.filter(c => {
    if (!c.person_researched) return false;
    const notes = String(c.notes || '');
    if (!RESEARCH_MARKERS.some(m => notes.includes(m))) return false;
    const last = Date.parse(c.updated_at || '');
    if (Number.isNaN(last)) return true;
    return (Date.now() - last) < 90 * 24 * 3600 * 1000;
  });
  console.log(`  Live contacts (pre-outreach stages): ${liveContacts.length}`);
  console.log(`  Researched but missing sector_focus: ${researchedButMissingSector.length}  ← caused the loop`);
  console.log(`  Have fresh research (<90 days): ${recentlyResearched.length}`);

  if (SHOULD_FIX && researchedButMissingSector.length > 0) {
    console.log(`\n  Fixing: writing fallback sector_focus to ${researchedButMissingSector.length} contacts...`);
    const ids = researchedButMissingSector.map(c => c.id);
    for (let i = 0; i < ids.length; i += 100) {
      await sb.from('contacts')
        .update({ sector_focus: 'General / Various' })
        .in('id', ids.slice(i, i + 100));
    }
    console.log(`  ✓ Done`);
  }
  console.log();

  // 5. Outreach events audit
  console.log('── Outreach Events (confirmed sends) ───────────────────');
  const events = await getOutreachEvents(deal.id);
  const confirmedEmails   = events.filter(e => e.event_type === 'EMAIL_SENT'          && e.status !== 'failed').length;
  const confirmedInvites  = events.filter(e => e.event_type === 'LINKEDIN_INVITE_SENT' && e.status !== 'failed').length;
  const confirmedDMs      = events.filter(e => e.event_type === 'LINKEDIN_DM_SENT'    && e.status !== 'failed').length;
  console.log(`  Confirmed emails sent:           ${confirmedEmails}`);
  console.log(`  Confirmed LinkedIn invites sent: ${confirmedInvites}`);
  console.log(`  Confirmed LinkedIn DMs sent:     ${confirmedDMs}`);

  const dbInviteSent  = contacts.filter(c => c.invite_sent_at || c.pipeline_stage === 'invite_sent' || c.pipeline_stage === 'invite_accepted').length;
  const dbEmailSent   = contacts.filter(c => c.last_email_sent_at).length;
  console.log();
  console.log(`  DB pipeline (invite_sent + invite_accepted): ${dbInviteSent}`);
  console.log(`  DB pipeline (has last_email_sent_at):        ${dbEmailSent}`);

  if (confirmedInvites < dbInviteSent) {
    const gap = dbInviteSent - confirmedInvites;
    console.log(`\n  ⚠  GAP: ${gap} contacts marked as invite_sent in DB but no confirmed outreach_event`);
    console.log(`     These were likely set during the 401 error period and never actually sent.`);
  }
  console.log();

  // 6. LinkedIn invitations via Unipile
  console.log('── LinkedIn Invitations (via Unipile API) ──────────────');
  const effectiveLiAcct = liAccounts[0]?.id || LI_ACCT;
  if (!effectiveLiAcct) {
    console.log('  ✗ No LinkedIn account ID available — skipping');
  } else {
    let sentInvites = [];
    try {
      console.log(`  Fetching sent invitations from Unipile (account: ${effectiveLiAcct})...`);
      sentInvites = await getSentInvitations(effectiveLiAcct, 1000);
      console.log(`  Unipile reports ${sentInvites.length} pending sent invitations`);
    } catch (err) {
      console.log(`  ✗ Could not fetch sent invitations: ${err.message}`);
    }

    // Build set of provider IDs that Unipile actually shows as sent
    const sentProviderIds = new Set(
      sentInvites
        .map(inv => inv.provider_id || inv.attendee_provider_id || inv.recipient_id || inv.id)
        .filter(Boolean)
        .map(String)
    );

    // Cross-reference with DB contacts marked as invite_sent
    const dbInviteSentContacts = contacts.filter(c =>
      (c.pipeline_stage === 'invite_sent' || c.invite_sent_at) && c.linkedin_provider_id
    );

    let unconfirmed = 0;
    const unconfirmedContacts = [];
    for (const c of dbInviteSentContacts) {
      if (!sentProviderIds.has(c.linkedin_provider_id)) {
        unconfirmed++;
        unconfirmedContacts.push(c);
      }
    }

    if (unconfirmed > 0) {
      console.log(`\n  ⚠  ${unconfirmed} contacts marked invite_sent in DB but NOT in Unipile's pending list`);
      console.log(`     (Could mean: accepted, withdrawn, or never sent)`);
      console.log(`\n  Sample unconfirmed (first 10):`);
      for (const c of unconfirmedContacts.slice(0, 10)) {
        console.log(`    - ${c.name} @ ${c.company_name || '?'}  [stage: ${c.pipeline_stage}]  [provider_id: ${c.linkedin_provider_id}]`);
      }

      if (SHOULD_FIX) {
        // Only reset contacts where invite_sent_at is set but no outreach_event confirms the send
        const confirmedInviteContactIds = new Set(
          events
            .filter(e => e.event_type === 'LINKEDIN_INVITE_SENT' && e.status !== 'failed')
            .map(e => String(e.contact_id))
            .filter(Boolean)
        );
        const toReset = unconfirmedContacts.filter(c => !confirmedInviteContactIds.has(String(c.id)));
        if (toReset.length) {
          console.log(`\n  Fixing: resetting ${toReset.length} unconfirmed invite_sent contacts to "Enriched"...`);
          const resetIds = toReset.map(c => c.id);
          for (let i = 0; i < resetIds.length; i += 100) {
            await sb.from('contacts')
              .update({
                pipeline_stage: 'Enriched',
                invite_sent_at: null,
              })
              .in('id', resetIds.slice(i, i + 100));
          }
          console.log(`  ✓ Reset ${toReset.length} contacts — they will be re-queued for outreach`);
        } else {
          console.log(`  All unconfirmed contacts have matching outreach_events — no reset needed`);
        }
      }
    } else {
      console.log(`  ✓ All invite_sent contacts in DB match Unipile's sent list`);
    }

    // Check connections (accepted)
    console.log();
    let relations = [];
    try {
      console.log(`  Fetching 1st-degree connections from Unipile...`);
      relations = await getRelations(effectiveLiAcct);
      console.log(`  Unipile reports ${relations.length} connections`);
    } catch (err) {
      console.log(`  ✗ Could not fetch connections: ${err.message}`);
    }

    const connectedProviderIds = new Set(
      relations
        .map(r => r.provider_id || r.attendee_provider_id || r.id)
        .filter(Boolean)
        .map(String)
    );

    const dbAccepted = contacts.filter(c => c.pipeline_stage === 'invite_accepted');
    let acceptedNotConnected = 0;
    const notConnectedContacts = [];
    for (const c of dbAccepted) {
      if (c.linkedin_provider_id && !connectedProviderIds.has(c.linkedin_provider_id)) {
        acceptedNotConnected++;
        notConnectedContacts.push(c);
      }
    }

    if (acceptedNotConnected > 0) {
      console.log(`\n  ⚠  ${acceptedNotConnected} contacts marked invite_accepted in DB but NOT in connections list`);
      for (const c of notConnectedContacts.slice(0, 5)) {
        console.log(`    - ${c.name} @ ${c.company_name || '?'}`);
      }
    } else if (dbAccepted.length > 0) {
      console.log(`  ✓ ${dbAccepted.length} invite_accepted contacts all verified as connections`);
    }
  }
  console.log();

  // 7. Email stats
  console.log('── Email Audit ─────────────────────────────────────────');
  const { data: emailRows } = await sb.from('emails')
    .select('id, deal_id, status, contact_id, created_at')
    .eq('deal_id', deal.id)
    .limit(5000);
  const emailsByStatus = {};
  for (const e of emailRows || []) {
    emailsByStatus[e.status || 'unknown'] = (emailsByStatus[e.status || 'unknown'] || 0) + 1;
  }
  console.log(`  Emails table totals:`);
  for (const [s, n] of Object.entries(emailsByStatus)) {
    console.log(`    ${n.toString().padStart(4)}  ${s}`);
  }
  const sentEmails   = (emailRows || []).filter(e => e.status === 'sent').length;
  const pendingEmails = (emailRows || []).filter(e => e.status === 'pending' || e.status === 'queued').length;
  if (pendingEmails > 0) {
    console.log(`\n  ⚠  ${pendingEmails} emails still in pending/queued state`);
  }
  console.log();

  // 8. Summary + next steps
  console.log('── Summary ─────────────────────────────────────────────');
  console.log(`  Real contacts in DB:        ${contacts.length}`);
  console.log(`  Confirmed emails sent:       ${confirmedEmails}`);
  console.log(`  Confirmed LI invites sent:   ${confirmedInvites}`);
  console.log(`  Confirmed LI DMs sent:       ${confirmedDMs}`);
  console.log();
  if (!SHOULD_FIX) {
    console.log('  To apply fixes, re-run with: node scripts/auditOutreach.js --fix');
  } else {
    console.log('  Fixes applied. Deploy to Railway and monitor the next orchestrator cycle.');
    console.log('  Contacts reset to Enriched will be re-queued for LinkedIn/email outreach.');
  }
  console.log('══════════════════════════════════════════════════════\n');
}

main().catch(err => {
  console.error('Audit failed:', err.message);
  console.error(err.stack);
  process.exit(1);
});
