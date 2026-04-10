// core/emailSender.js
// All email sending goes through Unipile's email API — no Maton.

import { getLiveCredentials, request } from './unipile.js';
import { getSupabase } from './supabase.js';

export async function sendDealEmail(contact, deal, subject, body) {
  const sb = getSupabase();
  const creds = await getLiveCredentials();

  // Use deal-specific account ID, fall back to configured Gmail account
  const accountId = deal.sending_account_id || creds.gmailAccountId || creds.linkedinAccountId;

  if (!accountId) {
    throw new Error('No email account configured for this deal');
  }

  const emailPayload = {
    account_id: accountId,
    to: [{ email: contact.email }],
    subject,
    body,
    body_type: 'text/plain',
  };

  const result = await request('POST', '/emails', { body: emailPayload });

  if (!result) {
    throw new Error('Unipile email send returned no response');
  }

  console.log(`[EMAIL] Sent to ${contact.email} via account ${accountId}`);

  if (sb) {
    try { await sb.from('contacts').update({ last_email_sent_at: new Date().toISOString() }).eq('id', contact.id); } catch {}

    try {
      await sb.from('conversation_messages').insert({
        contact_id: contact.id,
        deal_id: contact.deal_id,
        direction: 'outbound',
        channel: 'email',
        body,
        subject,
        sent_at: new Date().toISOString(),
      });
    } catch {}
  }

  return result;
}
