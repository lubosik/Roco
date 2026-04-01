// core/unipileWebhooks.js
// Handles inbound events from the consolidated /webhooks/unipile/messages endpoint.
// Two event types: message_received (LinkedIn DM) and new_relation (connection accepted).

import { getSupabase } from './supabase.js';
import { getLiveCredentials, isWithinSendingWindow } from './unipile.js';

// In-memory dedupe cache — cleared every 5 minutes
const recentlyProcessed = new Map();
setInterval(() => {
  const cutoff = Date.now() - 5 * 60_000;
  for (const [k, ts] of recentlyProcessed) if (ts < cutoff) recentlyProcessed.delete(k);
}, 60_000);

// 90-second per-contact message batching
const messageBatches = new Map();

// ── NORMALISATION ─────────────────────────────────────────────────────────────

function normalizeMessage(raw) {
  const data = raw?.data || raw;
  return {
    event_type:         raw.type || raw.event_type || 'unknown',
    message_id:         data.id || data.message_id || raw.id || raw.message_id,
    chat_id:            data.chat_id || data.thread_id || data.conversation_id || raw.chat_id,
    text:               data.message || data.text || data.body || data.content || raw.message || raw.text || '',
    sender_provider_id:
      data.sender?.attendee_provider_id ||
      data.sender?.provider_id ||
      data.attendee_provider_id ||
      data.sender_id ||
      data.attendees?.find(a => !a.is_self)?.provider_id ||
      raw.sender?.attendee_provider_id,
    account_user_id:    data.account_info?.user_id || data.account_id || raw.account_info?.user_id,
    raw,
  };
}

function normalizeRelation(raw) {
  const data = raw?.data || raw;
  return {
    provider_id:
      data.user_provider_id || data.provider_id || data.attendee?.provider_id ||
      data.relation?.provider_id || data.sender_id || raw.provider_id,
    name: data.user_full_name || data.name || data.full_name || data.display_name || '',
    public_identifier: data.user_public_identifier || '',
    profile_url:       data.user_profile_url || '',
  };
}

// ── CONTACT MATCHING ──────────────────────────────────────────────────────────

async function findContact(sb, payload) {
  // 1. Match by unipile_chat_id (most reliable for ongoing convos)
  if (payload.chat_id) {
    try {
      const { data } = await sb.from('contacts')
        .select('*, deals!contacts_deal_id_fkey(*)')
        .eq('unipile_chat_id', payload.chat_id)
        .limit(1).single();
      if (data) return data;
    } catch {}
  }
  // 2. Match by linkedin_provider_id
  if (payload.sender_provider_id) {
    try {
      const { data } = await sb.from('contacts')
        .select('*, deals!contacts_deal_id_fkey(*)')
        .eq('linkedin_provider_id', payload.sender_provider_id)
        .limit(1).single();
      if (data) return data;
    } catch {}
  }
  return null;
}

// ── DEDUPLICATION ──────────────────────────────────────────────────────────────

async function isDuplicate(sb, messageId) {
  if (!messageId) return false;
  if (recentlyProcessed.has(messageId)) return true;
  try {
    const { data } = await sb.from('conversation_messages')
      .select('id').eq('unipile_message_id', messageId)
      .limit(1).single();
    return !!data;
  } catch {
    return false;
  }
}

// ── HANDLER: LINKEDIN DM RECEIVED ────────────────────────────────────────────

export async function handleLinkedInMessage(raw, pushActivity, conversationManager) {
  const sb      = getSupabase();
  if (!sb) return;

  const payload = normalizeMessage(raw);
  const creds   = await getLiveCredentials();

  // Self-message filter
  if (payload.sender_provider_id && payload.account_user_id &&
      payload.sender_provider_id === payload.account_user_id) {
    console.log('[UNIPILE/MSG] Self-message — ignoring');
    return;
  }

  // Dedupe
  if (await isDuplicate(sb, payload.message_id)) {
    console.log('[UNIPILE/MSG] Duplicate message_id — ignoring:', payload.message_id);
    return;
  }
  if (payload.message_id) recentlyProcessed.set(payload.message_id, Date.now());

  const contact = await findContact(sb, payload);
  if (!contact) {
    console.log('[UNIPILE/MSG] No matching contact — ignoring');
    return;
  }

  // Only process contacts linked to active deals
  const dealStatus = contact.deals?.status || contact.deal_status;
  if (dealStatus && dealStatus.toUpperCase() !== 'ACTIVE') return;

  // Batch within 90-second window per contact
  const batchKey = contact.id;
  if (messageBatches.has(batchKey)) {
    clearTimeout(messageBatches.get(batchKey).timer);
    messageBatches.get(batchKey).messages.push(payload);
  } else {
    messageBatches.set(batchKey, { messages: [payload], timer: null });
  }

  const batch = messageBatches.get(batchKey);
  batch.timer = setTimeout(async () => {
    messageBatches.delete(batchKey);
    const combinedText = batch.messages.map(m => m.text).filter(Boolean).join('\n\n');
    if (!combinedText) return;

    // Store all inbound messages
    for (const msg of batch.messages) {
      try {
        await sb.from('conversation_messages').insert({
          contact_id:          contact.id,
          deal_id:             contact.deal_id || null,
          direction:           'inbound',
          channel:             'linkedin_dm',
          body:                msg.text || '',
          unipile_message_id:  msg.message_id || null,
          unipile_chat_id:     msg.chat_id || null,
          received_at:         new Date().toISOString(),
        });
      } catch (err) {
        console.error('[UNIPILE/MSG] Store error:', err.message);
      }
    }

    // Update contact state
    try {
      await sb.from('contacts').update({
        conversation_state: 'replied',
        reply_channel:      'linkedin',
        last_reply_at:      new Date().toISOString(),
        pipeline_stage:     'Replied',
        unipile_chat_id:    payload.chat_id || contact.unipile_chat_id || null,
      }).eq('id', contact.id);
    } catch {}

    // Suppress other contacts from the same firm on same deal
    if (contact.company_name && contact.deal_id) {
      try {
        await sb.from('contacts').update({
          pipeline_stage:     'Inactive',
          conversation_state: 'do_not_contact',
        }).eq('deal_id', contact.deal_id)
          .eq('company_name', contact.company_name)
          .neq('id', contact.id);
      } catch {}
    }

    pushActivity({
      type:   'reply',
      action: `LinkedIn reply: ${contact.name} at ${contact.company_name || ''}`,
      note:   combinedText.slice(0, 80),
    });

    console.log(`[UNIPILE/MSG] Processing reply from ${contact.name} — drafting response`);

    // Draft contextual reply via conversationManager
    try {
      await conversationManager.draftContextualReply({
        contact: { ...contact, unipile_chat_id: payload.chat_id || contact.unipile_chat_id },
        deal:    contact.deals || { id: contact.deal_id },
        channel: 'linkedin',
        inboundMessage: combinedText,
      });
    } catch (err) {
      console.error('[UNIPILE/MSG] Draft reply error:', err.message);
    }
  }, 90_000);
}

// ── HANDLER: CONNECTION ACCEPTED ──────────────────────────────────────────────

export async function handleLinkedInRelation(raw, pushActivity, queueForApproval) {
  const sb      = getSupabase();
  if (!sb) return;

  const payload = normalizeRelation(raw);

  if (!payload.provider_id) {
    console.log('[UNIPILE/REL] Missing provider_id — ignoring');
    return;
  }

  // Look up contact by linkedin_provider_id
  let contact = null;
  {
    const orClauses = [`linkedin_provider_id.eq.${payload.provider_id}`];
    if (payload.profile_url) orClauses.push(`linkedin_url.eq.${payload.profile_url}`);
    else if (payload.public_identifier) orClauses.push(`linkedin_url.ilike.%${payload.public_identifier}%`);

    try {
      const { data } = await sb.from('contacts')
        .select('*, deals!contacts_deal_id_fkey(*)')
        .or(orClauses.join(','))
        .limit(1).single();
      contact = data;
    } catch {}
  }

  if (!contact) {
    console.log('[UNIPILE/REL] Contact not found for provider_id:', payload.provider_id);
    return;
  }

  const dealStatus = contact.deals?.status || contact.deal_status;
  if (dealStatus && dealStatus.toUpperCase() !== 'ACTIVE') return;

  // Mark connection accepted
  try {
    await sb.from('contacts').update({
      linkedin_connected:  true,
      invite_accepted_at:  new Date().toISOString(),
      pipeline_stage:      'LinkedIn Connected',
    }).eq('id', contact.id);
  } catch {}

  // Log the acceptance event
  try {
    await sb.from('conversation_messages').insert({
      contact_id:  contact.id,
      deal_id:     contact.deal_id || null,
      direction:   'inbound',
      channel:     'linkedin_invite',
      body:        'LinkedIn connection request accepted',
      received_at: new Date().toISOString(),
    });
  } catch {}

  pushActivity({
    type:   'linkedin',
    action: `LinkedIn accepted: ${contact.name} at ${contact.company_name || ''}`,
    note:   contact.deals?.name || '',
  });

  console.log(`[UNIPILE/REL] ${contact.name} accepted LinkedIn invite`);

  // Skip DM if they've already replied via another channel
  if (contact.reply_channel) {
    console.log(`[UNIPILE/REL] ${contact.name} already replied via ${contact.reply_channel} — skipping DM`);
    return;
  }

  // Check sending window
  const within = await isWithinSendingWindow().catch(() => true);
  if (!within) {
    try {
      await sb.from('contacts').update({ pending_linkedin_dm: true }).eq('id', contact.id);
    } catch {}
    console.log(`[UNIPILE/REL] Outside sending window — DM flagged for next cycle for ${contact.name}`);
    return;
  }

  // Queue DM for approval
  let template = null;
  try {
    const { data } = await sb.from('email_templates')
      .select('*')
      .eq('sequence_step', 'linkedin_dm_1')
      .eq('is_primary', true)
      .eq('is_active', true)
      .limit(1).single();
    template = data;
  } catch {}

  if (!template) {
    console.warn('[UNIPILE/REL] No primary linkedin_dm_1 template — cannot queue DM');
    return;
  }

  try {
    await queueForApproval({ contact, template, channel: 'linkedin', action: 'send_dm' });
    pushActivity({
      type:   'outreach',
      action: `LinkedIn DM queued: ${contact.name} at ${contact.company_name || ''}`,
      note:   'Connection accepted → DM queued for approval',
    });
  } catch (err) {
    console.error('[UNIPILE/REL] Queue DM error:', err.message);
  }
}
