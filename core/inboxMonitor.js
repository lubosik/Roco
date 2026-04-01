// core/inboxMonitor.js
// Polling fallback — catches messages that arrived while webhooks were down.
// Runs every 60 seconds, checks all contacts with a known unipile_chat_id.

import { getSupabase } from './supabase.js';
import { getChatMessages } from './unipile.js';

export async function runInboxMonitor(handleLinkedInMessage, pushActivity, conversationManager) {
  const sb = getSupabase();
  if (!sb) return;

  let contacts = [];
  try {
    const { data } = await sb.from('contacts')
      .select('*, deals!contacts_deal_id_fkey(*)')
      .not('unipile_chat_id', 'is', null)
      .not('conversation_state', 'in', '("do_not_contact","ghosted","conversation_ended_negative","conversation_ended_positive")');
    contacts = data || [];
  } catch {
    return;
  }

  for (const contact of contacts) {
    const dealStatus = contact.deals?.status || '';
    if (dealStatus.toUpperCase() !== 'ACTIVE') continue;

    try {
      const messages = await getChatMessages(contact.unipile_chat_id, 10);
      const inbound  = messages.filter(m => !m.is_sender && !m.is_self);

      for (const msg of inbound) {
        const msgId = msg.id || msg.message_id;
        if (!msgId) continue;

        let exists = null;
        try {
          const { data } = await sb.from('conversation_messages')
            .select('id').eq('unipile_message_id', msgId)
            .limit(1).single();
          exists = data;
        } catch {}

        if (!exists) {
          console.log(`[INBOX MONITOR] Recovering missed message from ${contact.name}`);
          await handleLinkedInMessage({
            type:         'message_received',
            id:           msgId,
            chat_id:      contact.unipile_chat_id,
            text:         msg.text || msg.body || '',
            sender:       { attendee_provider_id: contact.linkedin_provider_id },
            account_info: { user_id: process.env.UNIPILE_LINKEDIN_ACCOUNT_ID },
          }, pushActivity, conversationManager);
        }
      }
    } catch (err) {
      console.warn(`[INBOX MONITOR] Error for ${contact.name}:`, err.message);
    }
  }
}

export function startInboxMonitor(handleLinkedInMessage, pushActivity, conversationManager) {
  console.log('[INBOX MONITOR] Starting — polling every 60 seconds');
  // Run immediately, then on interval
  runInboxMonitor(handleLinkedInMessage, pushActivity, conversationManager).catch(() => {});
  setInterval(() => {
    runInboxMonitor(handleLinkedInMessage, pushActivity, conversationManager).catch(() => {});
  }, 60_000);
}
