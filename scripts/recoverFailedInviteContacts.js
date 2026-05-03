/**
 * scripts/recoverFailedInviteContacts.js
 *
 * Recovery script for contacts stuck in Archived/skipped_no_linkedin state due to:
 * 1. resolveLinkedInProfile returning null (slug-only URL) causing markMissingLinkedIn
 *    to fire — but the update failing because archive_reason column doesn't exist,
 *    leaving linkedin_url intact but pipeline_stage potentially unchanged.
 * 2. Contacts that still have a linkedin_url but ended up Archived+skipped_no_linkedin.
 *
 * Strategy:
 * - Find contacts in this deal that are Archived+skipped_no_linkedin AND still have
 *   a linkedin_url (the update failed, URL survived — restore them to Ranked).
 * - Also scan activity_log for LINKEDIN_INVITE_SKIPPED_NO_PROFILE events that had
 *   a linkedin_url, check if those contacts lost their URL, and restore if needed.
 * - Report archived contacts with no URL and no email (no recovery path).
 * - Report failed email events from activity_log.
 *
 * Run: node scripts/recoverFailedInviteContacts.js
 */

import { readFileSync } from 'fs';
import { createClient } from '@supabase/supabase-js';

// ── Load .env manually ────────────────────────────────────────────────────────
const envPath = '/root/roco/.env';
try {
  const envFile = readFileSync(envPath, 'utf8');
  for (const line of envFile.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx < 0) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    const val = trimmed.slice(eqIdx + 1).trim();
    if (!process.env[key]) process.env[key] = val;
  }
} catch (err) {
  console.warn('Could not load .env file:', err.message);
}

const { SUPABASE_URL, SUPABASE_SERVICE_KEY } = process.env;
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.');
  process.exit(1);
}

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ── Helpers ───────────────────────────────────────────────────────────────────

function isValidLinkedInUrl(url) {
  return typeof url === 'string' && url.includes('linkedin.com/in/');
}

function sinceDate(days) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString();
}

async function hasArchiveReasonColumn() {
  const { error } = await sb.from('contacts').select('archive_reason').limit(1);
  if (error) {
    console.log('NOTE: archive_reason column not found — skipping archive_reason clear step.');
    return false;
  }
  return true;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log('=== Roco — LinkedIn invite contact recovery ===\n');

  // 1. Find the deal
  const { data: deals, error: dealErr } = await sb
    .from('deals')
    .select('id, name')
    .ilike('name', '%electrify%');

  if (dealErr) {
    console.error('ERROR fetching deals:', dealErr.message);
    process.exit(1);
  }
  if (!deals || deals.length === 0) {
    console.error('ERROR: No deal matching "%electrify%" found in deals table.');
    process.exit(1);
  }

  const deal = deals[0];
  console.log(`Deal: "${deal.name}" (id=${deal.id})\n`);

  const canClearArchiveReason = await hasArchiveReasonColumn();

  // ── PATH A: Contacts stuck Archived+skipped_no_linkedin but still have a URL ──
  // These are contacts where the markMissingLinkedIn update failed (due to missing
  // archive_reason column), so linkedin_url survived but stage is still wrong.
  console.log('\n--- PATH A: Archived+skipped_no_linkedin contacts with linkedin_url still intact ---');

  let pageCursor = 0;
  const PAGE = 200;
  let pathARecovered = 0;
  let pathAChecked = 0;
  let pathAAlreadyOk = 0;
  let pathANoUrl = 0;

  while (true) {
    const { data: archivedContacts, error: archErr } = await sb
      .from('contacts')
      .select('id, name, company_name, linkedin_url, pipeline_stage, enrichment_status, last_email_sent_at, email')
      .eq('deal_id', deal.id)
      .eq('pipeline_stage', 'Archived')
      .eq('enrichment_status', 'skipped_no_linkedin')
      .range(pageCursor, pageCursor + PAGE - 1);

    if (archErr) {
      console.error('ERROR querying contacts:', archErr.message);
      break;
    }
    if (!archivedContacts || archivedContacts.length === 0) break;

    for (const contact of archivedContacts) {
      pathAChecked++;

      if (!isValidLinkedInUrl(contact.linkedin_url)) {
        // No URL — truly stuck, cannot recover via this path
        pathANoUrl++;
        continue;
      }

      // Has a URL — restore to Ranked so the orchestrator retries the invite
      const updates = {
        pipeline_stage: 'Ranked',
        enrichment_status: 'enriched',
      };
      if (canClearArchiveReason) updates.archive_reason = null;

      const { error: updateErr } = await sb.from('contacts').update(updates).eq('id', contact.id);
      if (updateErr) {
        console.error(`  ERROR updating ${contact.name} (${contact.id}):`, updateErr.message);
      } else {
        console.log(`  RECOVERED (PATH A): ${contact.name} @ ${contact.company_name} — linkedin_url="${contact.linkedin_url?.substring(0, 60)}", stage→Ranked`);
        pathARecovered++;
      }
    }

    if (archivedContacts.length < PAGE) break;
    pageCursor += PAGE;
  }

  console.log(`PATH A: Checked ${pathAChecked}, recovered ${pathARecovered}, no URL (truly stuck) ${pathANoUrl}`);

  // ── PATH B: activity_log scan for SKIPPED_NO_PROFILE events with a URL ──────
  // These cover cases where the contact may have had their linkedin_url wiped.
  console.log('\n--- PATH B: activity_log LINKEDIN_INVITE_SKIPPED_NO_PROFILE scan (last 60 days) ---');

  const since = sinceDate(60);
  const { data: events, error: eventsErr } = await sb
    .from('activity_log')
    .select('id, contact_id, detail, created_at')
    .eq('deal_id', deal.id)
    .eq('event_type', 'LINKEDIN_INVITE_SKIPPED_NO_PROFILE')
    .gte('created_at', since);

  if (eventsErr) {
    console.error('ERROR querying activity_log:', eventsErr.message);
  } else {
    console.log(`Total LINKEDIN_INVITE_SKIPPED_NO_PROFILE events: ${events?.length ?? 0}`);

    // Filter to events that had a linkedin_url
    const withUrl = (events || []).filter(ev => isValidLinkedInUrl(ev.detail?.linkedin_url));
    console.log(`Events with a linkedin_url in detail: ${withUrl.length}`);

    // Deduplicate by contact_id — keep most recent event
    const byContact = new Map();
    for (const ev of withUrl) {
      if (!ev.contact_id) continue;
      const existing = byContact.get(ev.contact_id);
      if (!existing || ev.created_at > existing.created_at) {
        byContact.set(ev.contact_id, ev);
      }
    }
    console.log(`Unique contacts to check: ${byContact.size}`);

    let pathBRecovered = 0;
    let pathBAlreadyOk = 0;
    let pathBSkipped = 0;

    for (const [contactId, ev] of byContact.entries()) {
      const originalUrl = ev.detail?.linkedin_url;

      const { data: contact, error: contactErr } = await sb
        .from('contacts')
        .select('id, name, company_name, linkedin_url, pipeline_stage, enrichment_status')
        .eq('id', contactId)
        .single();

      if (contactErr || !contact) {
        console.log(`  SKIP (not found): contact_id=${contactId}`);
        pathBSkipped++;
        continue;
      }

      if (contact.linkedin_url) {
        // URL still present — already handled by PATH A or was never wiped
        pathBAlreadyOk++;
        continue;
      }

      // linkedin_url is null — restore it
      if (!isValidLinkedInUrl(originalUrl)) {
        console.log(`  SKIP (bad original url): ${contact.name} — original="${originalUrl}"`);
        pathBSkipped++;
        continue;
      }

      const updates = {
        linkedin_url: originalUrl,
        pipeline_stage: 'Ranked',
        enrichment_status: 'enriched',
      };
      if (canClearArchiveReason) updates.archive_reason = null;

      const { error: updateErr } = await sb.from('contacts').update(updates).eq('id', contactId);
      if (updateErr) {
        console.error(`  ERROR updating ${contact.name}:`, updateErr.message);
        pathBSkipped++;
      } else {
        console.log(`  RECOVERED (PATH B): ${contact.name} @ ${contact.company_name} — restored linkedin_url, stage→Ranked`);
        pathBRecovered++;
      }
    }

    console.log(`PATH B: Unique contacts checked ${byContact.size}, recovered ${pathBRecovered}, already OK ${pathBAlreadyOk}, skipped ${pathBSkipped}`);
  }

  // ── Truly archived: no URL, no email ─────────────────────────────────────────
  console.log('\n--- ARCHIVED_NO_RECOVERY: Archived+skipped_no_linkedin contacts with no URL and no email ---');

  const { data: trueArchived, error: taErr } = await sb
    .from('contacts')
    .select('id, name, company_name, linkedin_url, email, pipeline_stage')
    .eq('deal_id', deal.id)
    .eq('pipeline_stage', 'Archived')
    .eq('enrichment_status', 'skipped_no_linkedin')
    .is('last_email_sent_at', null);

  if (taErr) {
    console.log('Could not query truly archived contacts:', taErr.message);
  } else {
    const noUrlNoEmail = (trueArchived || []).filter(c => !c.linkedin_url && !c.email);
    console.log(`Found ${noUrlNoEmail.length} contacts with no URL and no email (no recovery path):`);
    for (const c of noUrlNoEmail.slice(0, 30)) {
      console.log(`  ARCHIVED_NO_RECOVERY: ${c.name} @ ${c.company_name}`);
    }
    if (noUrlNoEmail.length > 30) {
      console.log(`  ... and ${noUrlNoEmail.length - 30} more`);
    }
  }

  // ── Failed email events from activity_log ─────────────────────────────────
  console.log('\n--- Failed email sends for this deal (activity_log) ---');
  const { data: emailEvents, error: emailEvErr } = await sb
    .from('activity_log')
    .select('id, contact_id, event_type, detail, created_at')
    .eq('deal_id', deal.id)
    .in('event_type', ['EMAIL_FAILED', 'EMAIL_SEND_FAILED', 'EMAIL_ERROR'])
    .order('created_at', { ascending: false });

  if (emailEvErr) {
    console.log('Could not query email failure events:', emailEvErr.message);
  } else if (!emailEvents || emailEvents.length === 0) {
    console.log('No failed email events found in activity_log.');
  } else {
    for (const ev of emailEvents) {
      console.log(`  FAILED_EMAIL: contact_id=${ev.contact_id} error="${ev.detail?.error || 'n/a'}" at=${ev.created_at}`);
    }
  }

  // ── Summary ───────────────────────────────────────────────────────────────
  console.log('\n=== Summary ===');
  console.log(`PATH A — Archived+skipped_no_linkedin contacts checked: ${pathAChecked}`);
  console.log(`PATH A — Contacts recovered (URL restored to Ranked):   ${pathARecovered}`);
  console.log(`PATH A — Contacts with no URL (no recovery possible):   ${pathANoUrl}`);
  const noUrlNoEmailCount = (trueArchived || []).filter(c => !c.linkedin_url && !c.email).length;
  console.log(`Truly stuck contacts (no URL, no email):                 ${noUrlNoEmailCount}`);
  console.log(`Failed email events in activity_log:                     ${emailEvents?.length ?? 'query failed'}`);
  console.log('\nDone.');
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
