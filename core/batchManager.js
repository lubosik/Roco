/**
 * core/batchManager.js — Rolling batch cadence system
 *
 * Based on 16.5M email study data:
 * - Batch size: 15–25 contacts per wave
 * - 3-7-14 day follow-up cadence captures 93% of replies
 * - Rolling: new batch launches when previous batch reaches Day 3
 * - Max 3 follow-ups — beyond Day 14, returns drop 55%
 */

import { getSupabase } from './supabase.js';
import { getBatches, createBatch, updateBatch, getSentCountToday, getSentCountThisHour, logActivity } from './supabaseSync.js';
import { isWithinSendingWindow } from './scheduleChecker.js';
import { updateContact } from '../crm/notionContacts.js';

/**
 * Get all active batches for a deal, create first batch if none exist.
 */
export async function getOrCreateActiveBatches(deal) {
  const batches = await getBatches(deal.id);
  const active = batches.filter(b => b.status === 'ACTIVE');

  if (active.length === 0 && batches.length === 0) {
    // Bootstrap: create the first batch
    const contacts = await getNextBatchContacts(deal, 1);
    if (contacts.length > 0) {
      const batch = await createBatch({
        deal_id: deal.id,
        batch_number: 1,
        status: 'ACTIVE',
        contact_ids: contacts.map(c => c.id),
        contacts_total: contacts.length,
      });
      if (batch) {
        await logActivity({
          dealId: deal.id,
          eventType: 'BATCH_LAUNCHED',
          summary: `Batch 1 launched — ${contacts.length} contacts`,
          detail: { batchNumber: 1, contactCount: contacts.length },
        });
      }
      return batch ? [batch] : [];
    }
  }

  return active;
}

/**
 * Check if a new batch should launch based on current batch state.
 */
export async function shouldLaunchNewBatch(deal) {
  const batches = await getBatches(deal.id);
  const active = batches.filter(b => b.status === 'ACTIVE');
  const batchSize = deal.batch_size || 15;

  if (active.length === 0) return true;

  // Launch a new batch when the oldest active batch has progressed to follow-up stage
  // AND total active contacts are below 2x batch size
  const oldestBatch = active[0];
  const daysSinceLaunch = Math.floor(
    (Date.now() - new Date(oldestBatch.launched_at).getTime()) / (1000 * 60 * 60 * 24)
  );

  const followupDay1 = (deal.followup_cadence_days && deal.followup_cadence_days[0]) || 3;
  const shouldLaunch = daysSinceLaunch >= followupDay1 && active.length < 3;

  return shouldLaunch;
}

/**
 * Get the next N contacts to add to a new batch.
 * Ordered by: investor_score desc, then enrichment date.
 * Only enriched contacts with emails, not already in an active batch.
 */
async function getNextBatchContacts(deal, batchNumber) {
  const sb = getSupabase();
  if (!sb) return [];

  const batchSize = deal.batch_size || 15;
  const minScore = deal.min_investor_score || 60;

  try {
    // Get contacts already in active batches for THIS deal
    const existingBatches = await getBatches(deal.id);
    const usedContactIds = existingBatches
      .filter(b => b.status === 'ACTIVE')
      .flatMap(b => b.contact_ids || []);

    // Get contacts already assigned to OTHER active deals — never double-book an investor
    const { data: assignedElsewhere } = await sb
      .from('deal_contacts')
      .select('contact_id')
      .neq('deal_id', deal.id);
    const crossDealExcludes = (assignedElsewhere || []).map(r => r.contact_id);

    const excludeIds = [...new Set([...usedContactIds, ...crossDealExcludes])];

    // Get enriched contacts with emails
    const { data } = await sb
      .from('contacts')
      .select('*')
      .not('email', 'is', null)
      .eq('enrichment_status', 'Complete')
      .gte('investor_score', minScore)
      .not('id', 'in', excludeIds.length > 0 ? `(${excludeIds.join(',')})` : '(null)')
      .order('investor_score', { ascending: false })
      .limit(batchSize);

    const contacts = data || [];

    // Assign contacts to this deal — populate deal_contacts + tag in Notion
    for (const contact of contacts) {
      // Supabase deal_contacts record
      await sb.from('deal_contacts').upsert({
        deal_id: deal.id,
        contact_id: contact.id,
        stage: 'Prospecting',
        investor_score: contact.investor_score,
        company_name: contact.company_name,
      }, { onConflict: 'deal_id,contact_id' }).then(null, () => {});

      // Write Deal Name to Notion so the pipeline table shows it
      if (contact.notion_page_id) {
        updateContact(contact.notion_page_id, { dealName: deal.name }).catch(() => {});
      }
    }

    return contacts;
  } catch (err) {
    console.warn('[batchManager] Could not fetch batch contacts:', err.message);
    return [];
  }
}

/**
 * Advance batch states — check follow-up timing and trigger next stages.
 * Called every orchestrator cycle.
 */
export async function advanceBatches(deal) {
  const sb = getSupabase();
  if (!sb) return;

  const batches = await getBatches(deal.id);
  const followupDays = deal.followup_cadence_days || [3, 7, 14];
  const [day1, day2, day3] = followupDays;

  for (const batch of batches.filter(b => b.status === 'ACTIVE')) {
    const daysSinceLaunch = Math.floor(
      (Date.now() - new Date(batch.launched_at).getTime()) / (1000 * 60 * 60 * 24)
    );

    // Track batch progression milestones
    if (daysSinceLaunch >= day1 && !batch.followup1_sent_at) {
      await updateBatch(batch.id, { followup1_sent_at: new Date().toISOString() });
      await logActivity({
        dealId: deal.id,
        eventType: 'BATCH_FOLLOWUP_1',
        summary: `Batch ${batch.batch_number} — Day ${day1} follow-ups triggered`,
      });

      // Launch next batch if appropriate
      if (await shouldLaunchNewBatch(deal)) {
        await launchNextBatch(deal, batches.length + 1);
      }
    }

    if (daysSinceLaunch >= day2 && !batch.followup2_sent_at) {
      await updateBatch(batch.id, { followup2_sent_at: new Date().toISOString() });
    }

    if (daysSinceLaunch >= day3 && !batch.followup3_sent_at) {
      await updateBatch(batch.id, { followup3_sent_at: new Date().toISOString() });
    }

    // Complete batch if Day 14+ and all follow-ups sent
    if (daysSinceLaunch >= day3 + 7 && batch.followup3_sent_at) {
      await updateBatch(batch.id, {
        status: 'COMPLETE',
        completed_at: new Date().toISOString(),
      });
      await logActivity({
        dealId: deal.id,
        eventType: 'BATCH_COMPLETE',
        summary: `Batch ${batch.batch_number} complete — sequence exhausted`,
      });
    }
  }
}

async function launchNextBatch(deal, batchNumber) {
  const contacts = await getNextBatchContacts(deal, batchNumber);
  if (contacts.length === 0) return null;

  const batch = await createBatch({
    deal_id: deal.id,
    batch_number: batchNumber,
    status: 'ACTIVE',
    contact_ids: contacts.map(c => c.id),
    contacts_total: contacts.length,
  });

  if (batch) {
    await logActivity({
      dealId: deal.id,
      eventType: 'BATCH_LAUNCHED',
      summary: `Batch ${batchNumber} launched — ${contacts.length} contacts`,
      detail: { batchNumber, contactCount: contacts.length },
    });
  }

  return batch;
}

/**
 * Check if a contact is within the firm contact limit for this deal.
 * Max N simultaneous contacts per firm before a response is received.
 */
export async function checkFirmContactLimit(contact, deal) {
  const sb = getSupabase();
  if (!sb) return true; // No limit if Supabase unavailable

  const maxPerFirm = deal.max_contacts_per_firm || 3;
  const companyName = contact.company_name || contact.firm;
  if (!companyName) return true;

  try {
    const { count } = await sb
      .from('deal_contacts')
      .select('id', { count: 'exact', head: true })
      .eq('deal_id', deal.id)
      .eq('company_name', companyName)
      .in('stage', ['Intro Sent', 'Follow-up 1 Sent', 'Follow-up 2 Sent', 'Follow-up 3 Sent']);

    return (count || 0) < maxPerFirm;
  } catch {
    return true;
  }
}

/**
 * Check cross-deal conflict: has this contact been emailed in the last 5 business days
 * from a different deal?
 */
export async function checkCrossDealConflict(contactId) {
  const sb = getSupabase();
  if (!sb) return null;

  try {
    const fiveDaysAgo = new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString();
    const { data } = await sb
      .from('emails')
      .select('deal_id, sent_at')
      .eq('contact_id', contactId)
      .eq('status', 'sent')
      .gte('sent_at', fiveDaysAgo);
    return data && data.length > 0 ? data : null;
  } catch {
    return null;
  }
}

/**
 * Flush the approved email queue for a deal, respecting rate limits.
 * Only called when within sending window.
 */
export async function flushApprovedEmailQueue(deal, sendEmailFn) {
  const sentToday = await getSentCountToday(deal.id);
  const sentThisHour = await getSentCountThisHour(deal.id);
  const maxPerDay = deal.max_emails_per_day || 20;
  const maxPerHour = deal.max_emails_per_hour || 5;

  if (sentToday >= maxPerDay) {
    console.info(`[batchManager] [${deal.name}] Daily limit reached (${maxPerDay})`);
    return 0;
  }

  if (sentThisHour >= maxPerHour) {
    console.info(`[batchManager] [${deal.name}] Hourly limit reached (${maxPerHour})`);
    return 0;
  }

  const { getApprovedQueue, updateEmail } = await import('./supabaseSync.js');
  const queue = await getApprovedQueue(deal.id);
  const slots = Math.min(maxPerHour - sentThisHour, maxPerDay - sentToday, queue.length);

  let sent = 0;
  for (let i = 0; i < slots; i++) {
    const emailRecord = queue[i];
    try {
      const result = await sendEmailFn({
        to:      emailRecord.contact_email,
        toName:  emailRecord.contact_name || '',
        subject: emailRecord.subject_used,
        body:    emailRecord.body,
      });
      if (result) {
        await updateEmail(emailRecord.id, {
          status: 'sent',
          sent_at: new Date().toISOString(),
          gmail_message_id: result.messageId,
          gmail_thread_id:  result.threadId,
        });
        sent++;

        // Natural spacing: 1.5–3 minutes between sends
        if (i < slots - 1) {
          const delay = 90000 + Math.random() * 90000;
          await new Promise(r => setTimeout(r, delay));
        }
      }
    } catch (err) {
      console.error(`[batchManager] Failed to send queued email ${emailRecord.id}:`, err.message);
    }
  }

  return sent;
}

/**
 * Get batch summary for the dashboard.
 */
export async function getBatchSummary(dealId) {
  const batches = await getBatches(dealId);
  return batches.map(b => ({
    id: b.id,
    number: b.batch_number,
    status: b.status,
    contactsTotal: b.contacts_total || 0,
    contactsReplied: b.contacts_replied || 0,
    contactsInactive: b.contacts_inactive || 0,
    launchedAt: b.launched_at,
    completedAt: b.completed_at,
    followup1SentAt: b.followup1_sent_at,
    followup2SentAt: b.followup2_sent_at,
    followup3SentAt: b.followup3_sent_at,
    daysSinceLaunch: b.launched_at
      ? Math.floor((Date.now() - new Date(b.launched_at).getTime()) / (1000 * 60 * 60 * 24))
      : 0,
  }));
}
