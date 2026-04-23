/**
 * core/jarvisApprovalBridge.js
 * Thin bridge so jarvisTools.js can execute approvals without importing
 * telegramBot.js directly (which would create a circular dependency).
 *
 * Dynamically imports dashboard/server.js at call time — same pattern
 * the rest of the codebase uses.
 */

import { getSupabase } from './supabase.js';

/**
 * Execute an approval queue item — same logic as executeReloadedApproval
 * in telegramBot.js but called from the JARVIS tool layer.
 */
export async function executeApprovalById(item) {
  const sb = getSupabase();
  if (!sb) throw new Error('Database unavailable');

  const { sendApprovedLinkedInDM, sendApprovedReply } = await import('../dashboard/server.js');

  const isLinkedIn = /linkedin/i.test(item.stage || '') || /linkedin/i.test(item.channel || '');
  const isReply    = item.channel === 'linkedin_reply' || item.outreach_mode === 'reply';

  if (isReply) {
    await sendApprovedReply({ queueId: item.id, queueItem: item });
    return;
  }

  if (isLinkedIn) {
    await sendApprovedLinkedInDM({
      contactId: item.contact_id,
      text:      item.edited_body || item.body || '',
      queueId:   item.id,
      queueItem: item,
    });
    return;
  }

  // Email path
  const { sendEmail } = await import('../approval/unipileClient.js');

  let toEmail = item.contact_email || null;
  let dealId  = item.deal_id || null;

  if (item.contact_id) {
    const { data: contact } = await sb.from('contacts')
      .select('email, deal_id').eq('id', item.contact_id).single().catch(() => ({ data: null }));
    toEmail = toEmail || contact?.email || null;
    dealId  = dealId  || contact?.deal_id || null;
  }

  if (!toEmail) throw new Error('No email address for this approval item');

  const subject    = item.subject_a || item.subject || '';
  const body       = item.edited_body || item.body || '';

  await sendEmail({ to: toEmail, toName: item.contact_name || '', subject, body });

  await sb.from('approval_queue').update({
    status:           'sent',
    sent_at:          new Date().toISOString(),
    approved_subject: subject || null,
  }).eq('id', item.id);

  if (item.contact_id) {
    await sb.from('contacts').update({
      pipeline_stage:   'Email Sent',
      last_email_sent_at: new Date().toISOString(),
      outreach_channel:   'email',
      last_outreach_at:   new Date().toISOString(),
    }).eq('id', item.contact_id);
  }

  const { notifyQueueUpdated } = await import('../dashboard/server.js');
  notifyQueueUpdated();
}
