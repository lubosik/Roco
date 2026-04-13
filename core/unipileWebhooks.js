// core/unipileWebhooks.js
// Handles inbound events from the consolidated /webhooks/unipile/messages endpoint.
// Two event types: message_received (LinkedIn DM) and new_relation (connection accepted).

import { getSupabase } from './supabase.js';
import { getLiveCredentials, isWithinSendingWindow, getExistingChatWithContact, getChatMessages } from './unipile.js';
import { aiComplete } from './aiClient.js';

// In-memory dedupe cache — cleared every 5 minutes
const recentlyProcessed = new Map();
setInterval(() => {
  const cutoff = Date.now() - 5 * 60_000;
  for (const [k, ts] of recentlyProcessed) if (ts < cutoff) recentlyProcessed.delete(k);
}, 60_000);

// Per-contact batching window for multi-part inbound replies
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
      data.user_provider_id ||
      data.provider_id ||
      data.attendee?.provider_id ||
      data.attendee_provider_id ||
      data.user?.provider_id ||
      data.user?.id ||
      data.relation?.provider_id ||
      data.relation?.user_provider_id ||
      data.sender_id ||
      raw.provider_id,
    name:
      data.user_full_name ||
      data.name ||
      data.full_name ||
      data.display_name ||
      data.user?.full_name ||
      data.user?.name ||
      data.attendee?.display_name ||
      '',
    public_identifier:
      data.user_public_identifier ||
      data.public_identifier ||
      data.user?.public_identifier ||
      data.relation?.public_identifier ||
      '',
    profile_url:
      data.user_profile_url ||
      data.profile_url ||
      data.linkedin_url ||
      data.user?.profile_url ||
      data.user?.linkedin_url ||
      data.relation?.profile_url ||
      '',
    headline:
      data.headline ||
      data.user_headline ||
      data.job_title ||
      data.occupation ||
      data.user?.headline ||
      data.user?.job_title ||
      data.relation?.headline ||
      '',
  };
}

function getDealName(contact) {
  return contact?.deals?.name || contact?.deal_name || 'Unknown deal';
}

function buildLinkedInIdentityClauses(payload) {
  const clauses = [];
  if (payload?.provider_id) clauses.push(`linkedin_provider_id.eq.${payload.provider_id}`);
  if (payload?.profile_url) clauses.push(`linkedin_url.eq.${payload.profile_url}`);
  if (payload?.public_identifier) clauses.push(`linkedin_url.ilike.%${payload.public_identifier}%`);
  return [...new Set(clauses)].filter(Boolean);
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
    pushActivity({
      type: 'excluded',
      action: `LinkedIn reply received: ${payload.sender_provider_id || payload.chat_id || 'unknown sender'}`,
      note: 'Sender did not match any active deals',
    });
    return;
  }

  // Only process contacts linked to active deals
  const dealStatus = contact.deals?.status || contact.deal_status;
  if (dealStatus && dealStatus.toUpperCase() !== 'ACTIVE') {
    console.log(`[UNIPILE/MSG] Deal is ${dealStatus} for ${contact.name} — ignoring event`);
    pushActivity({
      type: 'system',
      action: `LinkedIn reply received: ${contact.name}`,
      note: `${contact.name} matched deal ${getDealName(contact)}, but that deal is ${dealStatus}`,
      dealId: contact.deal_id,
      deal_name: getDealName(contact),
    });
    return;
  }

  pushActivity({
    type: 'reply',
    activity_badge: 'linkedin_reply',
    action: `LinkedIn reply received: ${contact.name}`,
    note: `${String(payload.text || '').trim().slice(0, 100)}${getDealName(contact) ? ` · ${getDealName(contact)}` : ''}`,
    dealId: contact.deal_id,
    deal_name: getDealName(contact),
  });

  // Batch within a short window per contact
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
        pipeline_stage:     'In Conversation',
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

    console.log(`[UNIPILE/MSG] Processing reply from ${contact.name} — drafting response`);

    // Draft contextual reply via conversationManager
    try {
      await conversationManager.draftContextualReply({
        contact: { ...contact, unipile_chat_id: payload.chat_id || contact.unipile_chat_id },
        deal:    contact.deals || { id: contact.deal_id },
        channel: 'linkedin',
        inboundMessage: combinedText,
      });
      pushActivity({
        type: 'approval',
        action: `Next action: draft LinkedIn reply for ${contact.name}`,
        note: `Matched to deal ${getDealName(contact)} · contextual reply workflow queued`,
        dealId: contact.deal_id,
        deal_name: getDealName(contact),
      });
    } catch (err) {
      console.error('[UNIPILE/MSG] Draft reply error:', err.message);
    }
  }, 90_000);
}

// ── PRIOR CHAT HELPERS ────────────────────────────────────────────────────────

async function summarisePriorChat(messages, contact, deal) {
  const lines = messages
    .filter(m => m.text || m.message || m.body)
    .slice(0, 20)
    .map(m => {
      const who = m.is_self ? 'Roco' : (contact.name || 'Them');
      const text = m.text || m.message || m.body || '';
      return `${who}: ${text.slice(0, 300)}`;
    })
    .join('\n');

  const prompt = `You are summarising a prior LinkedIn conversation for a fundraising AI.\n\nContact: ${contact.name || 'Unknown'} at ${contact.company_name || 'Unknown'}\nDeal: ${deal?.name || 'Unknown'}\n\nConversation:\n${lines}\n\nSummarise in 2-3 sentences: the relationship context, any prior interest or concerns expressed, and how the next message should be positioned. Be concise.`;

  try {
    const summary = await aiComplete(prompt, { maxTokens: 200, task: 'prior_chat_summary', model: 'claude-haiku-4-5-20251001' });
    return summary?.trim() || 'Prior conversation found but could not be summarised.';
  } catch (err) {
    console.warn('[UNIPILE/PRIOR_CHAT] Summarise failed:', err.message);
    return `Prior conversation found (${messages.length} message(s)) — review recommended before sending.`;
  }
}

// Set stage to invite_accepted so phaseOutreach picks up the DM in next cycle
async function queueLinkedInDM(contactId) {
  const sb = getSupabase();
  if (!sb) return;
  try {
    await sb.from('contacts').update({
      pipeline_stage: 'invite_accepted',
    }).eq('id', contactId);
  } catch (err) {
    console.warn('[UNIPILE/REL] queueLinkedInDM failed:', err.message);
  }
}

// Full acceptance flow — checks for prior chat and routes accordingly
export async function handleLinkedInAcceptance(contact, deal, pushActivity, queueForApproval = null) {
  const sb    = getSupabase();
  if (!sb) return;

  // Already replied on another channel — no DM needed
  if (contact.reply_channel) {
    console.log(`[UNIPILE/REL] ${contact.name} already replied via ${contact.reply_channel} — skipping DM`);
    return;
  }

  const within = await isWithinSendingWindow().catch(() => true);

  // Check for existing chat history
  const creds = await getLiveCredentials();
  let existingChat = null;
  if (contact.linkedin_provider_id && creds.linkedinAccountId) {
    existingChat = await getExistingChatWithContact(contact.linkedin_provider_id, creds.linkedinAccountId);
  }

  if (existingChat?.id) {
    const messages = await getChatMessages(existingChat.id, 20).catch(() => []);
    const priorMessages = messages.filter(m => String(m.text || m.message || m.body || '').trim());

    if (priorMessages.length > 0) {
      // Prior conversation exists — summarise and route to approval
      const summary = await summarisePriorChat(messages, contact, deal);

      let approvalId = null;
      try {
        const { data: queueRow } = await sb.from('approval_queue').insert({
          contact_id:   contact.id,
          contact_name: contact.name,
          firm:         contact.company_name || null,
          deal_id:      deal?.id || contact.deal_id || null,
          message_type: 'prior_chat_review',
          message_text: summary,
          stage:        'prior_chat_review',
          status:       'pending',
          metadata:     JSON.stringify({ messageCount: priorMessages.length, chatId: existingChat.id }),
          created_at:   new Date().toISOString(),
        }).select('id').single();
        approvalId = queueRow?.id;
      } catch (err) {
        console.warn('[UNIPILE/REL] Could not store prior_chat_review:', err.message);
      }

      try {
        await sb.from('contacts').update({
          pipeline_stage:  'prior_chat_review',
          unipile_chat_id: existingChat.id,
        }).eq('id', contact.id);
      } catch {}

      // Notify via Telegram (lazy import avoids circular dep at module load time)
      if (approvalId) {
        try {
          const { sendPriorChatForApproval } = await import('../approval/telegramBot.js');
          await sendPriorChatForApproval({
            contactName:  contact.name,
            firm:         contact.company_name || 'Unknown',
            dealName:     deal?.name || 'Unknown',
            summary,
            messageCount: priorMessages.length,
            approvalId,
          });
        } catch (err) {
          console.warn('[UNIPILE/REL] Telegram prior-chat notify failed:', err.message);
        }
      }

      pushActivity({
        type:   'linkedin',
        action: `Existing LinkedIn conversation found: ${contact.name}`,
        note:   `${priorMessages.length} prior message(s) found · awaiting proceed/decline`,
        deal_name: deal?.name || null,
        dealId: deal?.id || contact.deal_id || null,
      });
      return;
    }
  }

  pushActivity({
    type:   'linkedin',
    action: `No existing LinkedIn conversation: ${contact.name}`,
    note:   'Connection accepted · triggering opening LinkedIn DM workflow',
    deal_name: deal?.name || null,
    dealId: deal?.id || contact.deal_id || null,
  });

  // No prior chat — trigger the opening DM workflow immediately.
  if (queueForApproval) {
    try {
      const workflowResult = await queueForApproval({
        contact,
        deal,
        channel: 'linkedin_dm',
        action: 'draft_for_approval',
        reason: within ? 'accepted_in_window' : 'accepted_outside_window',
      });

      if (workflowResult?.deferred) {
        await queueLinkedInDM(contact.id);
      }

      pushActivity({
        type:   'linkedin',
        action: `DM drafted for approval: ${contact.name}`,
        note:   within
          ? 'Connection accepted → opening LinkedIn DM queued for approval'
          : 'Connection accepted outside window → DM queued for approval and will wait for the DM window after approval',
        deal_name: deal?.name || null,
        dealId: deal?.id || contact.deal_id || null,
      });
      return;
    } catch (err) {
      console.warn('[UNIPILE/REL] queueForApproval failed:', err.message);
    }
  }

  await queueLinkedInDM(contact.id);
  try { await sb.from('contacts').update({ pending_linkedin_dm: true }).eq('id', contact.id); } catch {}
  pushActivity({
    type:   'linkedin',
    action: `DM queued: ${contact.name}`,
    note:   'Connection accepted → draft will be created next cycle',
    deal_name: deal?.name || null,
    dealId: deal?.id || contact.deal_id || null,
  });
}

// ── HANDLER: CONNECTION ACCEPTED ──────────────────────────────────────────────

export async function handleLinkedInRelation(raw, pushActivity, queueForApproval) {
  const sb      = getSupabase();
  if (!sb) return;

  const payload = normalizeRelation(raw);

  if (!payload.provider_id && !payload.profile_url && !payload.public_identifier && !payload.name) {
    console.log('[UNIPILE/REL] Missing usable identifiers — ignoring');
    pushActivity({
      type: 'error',
      action: 'LinkedIn acceptance received',
      note: 'Payload did not include a provider ID, profile URL, public identifier, or name, so no active deal could be matched',
    });
    return;
  }

  // Look up contact by linkedin_provider_id
  let contact = null;
  {
    const orClauses = buildLinkedInIdentityClauses(payload);

    if (orClauses.length) {
      try {
        const { data } = await sb.from('contacts')
          .select('*, deals!contacts_deal_id_fkey(*)')
          .or(orClauses.join(','))
          .limit(10);
        const candidates = data || [];
        const activeCandidates = candidates.filter(row => String(row?.deals?.status || row?.deal_status || '').toUpperCase() === 'ACTIVE');
        contact = activeCandidates[0] || candidates[0] || null;
      } catch {}
    }
  }

  if (!contact) {
    console.log('[UNIPILE/REL] Contact not found for identifiers:', payload.provider_id || payload.public_identifier || payload.profile_url || payload.name);
    pushActivity({
      type: 'excluded',
      action: `LinkedIn acceptance received: ${payload.name || payload.public_identifier || payload.provider_id}`,
      note: 'Person did not match any active deals',
    });
    return;
  }

  const dealStatus = contact.deals?.status || contact.deal_status;
  if (dealStatus && dealStatus.toUpperCase() !== 'ACTIVE') {
    console.log(`[UNIPILE/REL] Deal is ${dealStatus} for ${contact.name} — ignoring event`);
    pushActivity({
      type: 'system',
      action: `LinkedIn acceptance received: ${contact.name}`,
      note: `${contact.name} matched deal ${getDealName(contact)}, but that deal is ${dealStatus}`,
      dealId: contact.deal_id,
      deal_name: getDealName(contact),
    });
    return;
  }

  // Mark connection accepted
  try {
    await sb.from('contacts').update({
      linkedin_connected:  true,
      invite_accepted_at:  new Date().toISOString(),
      pipeline_stage:      'invite_accepted',
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
    activity_badge: 'relation',
    action: `New relation: ${contact.name}`,
    note:   [
      getDealName(contact),
      payload.headline || contact.job_title || '',
    ].filter(Boolean).join(' · '),
    dealId: contact.deal_id,
    deal_name: getDealName(contact),
  });

  try {
    const { sendTelegram } = await import('../approval/telegramBot.js');
    await sendTelegram(
      `🟧 *New relation*\n\n${contact.name}${contact.company_name ? ` @ ${contact.company_name}` : ''}\nDeal: ${getDealName(contact)}${(payload.headline || contact.job_title) ? `\n${payload.headline || contact.job_title}` : ''}`
    );
  } catch {}

  console.log(`[UNIPILE/REL] ${contact.name} accepted LinkedIn invite — running acceptance flow`);

  const deal = contact.deals || { id: contact.deal_id, name: null };
  await handleLinkedInAcceptance(contact, deal, pushActivity, queueForApproval);
}
