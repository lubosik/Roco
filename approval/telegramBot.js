import TelegramBot from 'node-telegram-bot-api';
import { info, warn, error } from '../core/logger.js';
import {
  addApprovalToQueue, getResolvedApprovals, markApprovalProcessing, updateApprovalStatus,
  loadSessionState, saveSessionState, getActiveDeals, createDeal, logActivity as sbLogActivity,
} from '../core/supabaseSync.js';
import { getSupabase } from '../core/supabase.js';
import { aiComplete } from '../core/aiClient.js';

let bot;
let rocoState; // injected from orchestrator

const pendingApprovals  = new Map(); // messageId -> { contactPage, emailDraft, resolve, ... }
const editLoops         = new Map(); // contactPageId -> count
const pendingEditReqs   = new Map(); // chatId -> msgId  (waiting for edit instructions after button press)

export function initTelegramBot(state) {
  rocoState = state;

  bot = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: true });
  info('Telegram bot started');

  bot.on('message',        handleMessage);
  bot.on('callback_query', handleCallbackQuery);
  bot.on('polling_error',  (err) => error('Telegram polling error', { err: err.message }));

  registerCommands();
  return bot;
}

export async function sendTelegram(text) {
  if (!bot) return null;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!chatId) return null;
  try {
    return await bot.sendMessage(chatId, text, { parse_mode: 'Markdown' });
  } catch (err) {
    error('Telegram send failed', { err: err.message });
    return null;
  }
}

export async function sendEmailForApproval(contactPage, emailDraft, researchSummary, score, stage, dealId = null) {
  const { getContactProp } = await import('../crm/notionContacts.js');
  const name = getContactProp(contactPage, 'Name');
  const firm = getContactProp(contactPage, 'Company Name') || 'Unknown Firm';

  // GATE: never send to Telegram if name is null/invalid
  if (!name || name.trim() === '' || name.toLowerCase() === 'null') {
    warn(`[TELEGRAM] Refusing to send approval for contact with invalid name: "${name}"`);
    return { action: 'skip' };
  }

  const isLinkedIn = stage === 'LinkedIn DM' || stage === 'linkedin_dm' || stage === 'linkedin_follow_up';
  const stageLabel = isLinkedIn ? 'LINKEDIN DM' : stage?.includes('FOLLOW') ? 'FOLLOW-UP EMAIL' : 'EMAIL';
  const researchBasis = (researchSummary || '').substring(0, 200) || 'No specific research basis';

  const hasSubjects = !!emailDraft.subject;
  const subjectBlock = hasSubjects
    ? `📧 *Subject A:* _${emailDraft.subject}_\n📧 *Subject B:* _${emailDraft.alternativeSubject || 'N/A'}_\n`
    : '';

  // Format body for Telegram — trim to 800 chars to avoid truncation
  const bodyPreview = (emailDraft.body || '').length > 800
    ? emailDraft.body.substring(0, 800) + '…'
    : emailDraft.body;

  const msg = [
    `*ROCO — ${stageLabel} Ready for Approval*`,
    ``,
    `👤 *${name}* · ${firm}`,
    `📊 Score: ${score || '—'}/100  |  Stage: ${stageLabel}`,
    `🔍 _${researchBasis}_`,
    ``,
    subjectBlock,
    `\`\`\``,
    bodyPreview,
    `\`\`\``,
    ``,
    hasSubjects
      ? `Reply: *APPROVE A* | *APPROVE B* | *EDIT [instructions]* | *SKIP* | *STOP*`
      : `Reply: *APPROVE* | *EDIT [instructions]* | *SKIP* | *STOP*`,
  ].join('\n');

  return new Promise(async (resolve) => {
    try {
      const chatId = process.env.TELEGRAM_CHAT_ID;
      const sent = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
      const entry = { contactPage, emailDraft, resolve, score, stage, firm, name, dealId, queuedAt: new Date().toISOString(), queueId: null };
      pendingApprovals.set(sent.message_id, entry);
      info(`Email draft sent to Telegram for approval: ${name}`);

      // Attach action buttons (done after send so we have the message_id)
      bot.editMessageReplyMarkup(buildKeyboard(sent.message_id), {
        chat_id: chatId,
        message_id: sent.message_id,
      }).catch(() => {});

      // Mirror to Supabase so the dashboard can see the queue
      addApprovalToQueue({
        telegramMsgId: sent.message_id,
        contactId: contactPage?.id,
        contactName: name,
        firm,
        stage,
        score,
        subjectA: emailDraft.subject,
        subjectB: emailDraft.alternativeSubject,
        body: emailDraft.body,
        researchSummary: researchSummary || null,
      }).then(row => {
        if (row) {
          const existing = pendingApprovals.get(sent.message_id);
          if (existing) pendingApprovals.set(sent.message_id, { ...existing, queueId: row.id });
        }
      }).catch(() => {});
    } catch (err) {
      error('Failed to send draft to Telegram', { err: err.message });
      resolve({ action: 'error' });
    }
  });
}

/**
 * Send a LinkedIn DM draft to Telegram for approval.
 * Returns a Promise resolving to { action: 'approve'|'skip'|'error', body }.
 */
export async function sendLinkedInDMForApproval(contact, body, dealId = null) {
  if (!bot) return { action: 'error' };
  const name = contact.name || 'Contact';
  const firm = contact.company_name || 'Unknown Firm';
  const score = contact.investor_score || 0;

  const contactType = contact.contact_type === 'individual' ? '👤 Individual' : '🏢 Firm';
  const bodyPreview = (body || '').length > 600 ? body.substring(0, 600) + '…' : body;

  const msg = [
    `*ROCO — LinkedIn DM Ready for Approval*`,
    ``,
    `👤 *${name}* · ${firm}`,
    `📊 Score: ${score}/100  |  ${contactType}  |  LinkedIn DM`,
    ``,
    '```',
    bodyPreview,
    '```',
    ``,
    `Reply: *APPROVE* | *EDIT [instructions]* | *SKIP*`,
  ].join('\n');

  return new Promise(async (resolve) => {
    try {
      const chatId = process.env.TELEGRAM_CHAT_ID;
      const sent = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
      pendingApprovals.set(sent.message_id, {
        name, firm, score, dealId,
        isLinkedInDM: true,
        emailDraft: { body, subject: null, alternativeSubject: null },
        resolve,
        contactPage: null,
        queuedAt: new Date().toISOString(),
        queueId: null,
      });
      bot.editMessageReplyMarkup(buildLinkedInDMKeyboard(sent.message_id), {
        chat_id: chatId,
        message_id: sent.message_id,
      }).catch(() => {});
      info(`LinkedIn DM draft sent to Telegram for approval: ${name}`);
    } catch (err) {
      error('Failed to send LinkedIn DM draft to Telegram', { err: err.message });
      resolve({ action: 'error' });
    }
  });
}

function buildKeyboard(msgId) {
  return {
    inline_keyboard: [
      [
        { text: '✓ Send A', callback_data: `aa:${msgId}` },
        { text: '✓ Send B', callback_data: `ab:${msgId}` },
      ],
      [
        { text: '✏ Edit',   callback_data: `ed:${msgId}` },
        { text: '🗑 Delete', callback_data: `sk:${msgId}` },
      ],
    ],
  };
}

function buildLinkedInDMKeyboard(msgId) {
  return {
    inline_keyboard: [
      [{ text: '✓ Approve & Send', callback_data: `sa:${msgId}` }],
      [
        { text: '✏ Edit', callback_data: `ed:${msgId}` },
        { text: '✗ Skip', callback_data: `sk:${msgId}` },
      ],
    ],
  };
}

function buildSourcingKeyboard(msgId, isLinkedIn) {
  const approveRow = isLinkedIn
    ? [{ text: '✓ Approve', callback_data: `sa:${msgId}` }]
    : [
        { text: '✓ Send A', callback_data: `aa:${msgId}` },
        { text: '✓ Send B', callback_data: `ab:${msgId}` },
      ];
  return {
    inline_keyboard: [
      approveRow,
      [
        { text: '✏ Edit',      callback_data: `ed:${msgId}` },
        { text: '✗ Skip',      callback_data: `sk:${msgId}` },
        { text: '🚫 End Chat', callback_data: `ec:${msgId}` },
      ],
    ],
  };
}

function buildReplyKeyboard(msgId) {
  return {
    inline_keyboard: [
      [
        { text: '✓ Approve & Send', callback_data: `ra:${msgId}` },
        { text: '✏ Edit',           callback_data: `re:${msgId}` },
      ],
      [
        { text: '✗ Skip', callback_data: `rs:${msgId}` },
      ],
    ],
  };
}

/**
 * Send a batched reply draft to Telegram for approval with inline buttons.
 * When approved, sends via Unipile automatically.
 */
export async function sendReplyForApproval(queueItemId, contact, replyBody, contextName, channel, replyToId, emailAccountId) {
  if (!bot) return;
  const name        = contact?.name || 'Contact';
  const company     = contact?.company_name || '';
  const channelLbl  = channel === 'linkedin' ? 'LinkedIn' : 'Email';

  const msg = [
    `💬 *Reply Queued — ${channelLbl}*`,
    ``,
    `To: *${name}*${company ? ` (${company})` : ''}`,
    `[${contextName}]`,
    ``,
    '```',
    String(replyBody || '').substring(0, 600),
    '```',
  ].join('\n');

  try {
    const chatId = process.env.TELEGRAM_CHAT_ID;
    const sent   = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });

    pendingApprovals.set(sent.message_id, {
      name,
      firm: company,
      isReply: true,
      queueId: queueItemId,
      replyApproval: {
        queueItemId,
        channel,
        replyToId,
        contactId:       contact?.id,
        contactEmail:    contact?.email,
        replyBody:       replyBody,
        subject:         null,
        emailAccountId:  emailAccountId || null,
      },
    });

    await bot.editMessageReplyMarkup(buildReplyKeyboard(sent.message_id), {
      chat_id: chatId, message_id: sent.message_id,
    }).catch(() => {});

    info(`Reply draft sent to Telegram for approval: ${name}`);
  } catch (err) {
    error('sendReplyForApproval failed', { err: err.message });
  }
}

async function handleReplyEdit(chatId, oldMsgId, approval, instructions) {
  try {
    const ra = approval.replyApproval;
    const revised = await aiComplete(
      `Revise this reply draft based on the instructions below.\n\nOriginal:\n${ra.replyBody}\n\nInstructions: ${instructions}\n\nReturn ONLY the revised message body. No subject line. No markdown.`,
      { maxTokens: 600, task: 'reply_edit' }
    );
    if (!revised?.trim()) throw new Error('Empty revision from AI');

    ra.replyBody = revised.trim();

    const sb = getSupabase();
    if (sb && ra.queueItemId) {
      await sb.from('approval_queue').update({ message_text: ra.replyBody }).eq('id', ra.queueItemId).catch(() => {});
    }

    const channelLbl = ra.channel === 'linkedin' ? 'LinkedIn' : 'Email';
    const revisedMsg = [
      `💬 *Revised Reply — ${channelLbl}*`,
      ``,
      `To: *${approval.name}*${approval.firm ? ` (${approval.firm})` : ''}`,
      ``,
      '```',
      ra.replyBody.substring(0, 600),
      '```',
    ].join('\n');

    pendingApprovals.delete(oldMsgId);
    const newSent = await bot.sendMessage(chatId, revisedMsg, { parse_mode: 'Markdown' });
    pendingApprovals.set(newSent.message_id, { ...approval, replyApproval: ra });
    await bot.editMessageReplyMarkup(buildReplyKeyboard(newSent.message_id), {
      chat_id: chatId, message_id: newSent.message_id,
    }).catch(() => {});

    await bot.sendMessage(chatId, `✅ Revised — approve or edit using the buttons above.`);
  } catch (err) {
    error('handleReplyEdit failed', { err: err.message });
    await bot.sendMessage(chatId, `⚠ Edit failed: ${err.message}`);
  }
}

/**
 * In-place AI edit for LinkedIn DM drafts — revises body without going back to orchestrator.
 */
async function handleLinkedInDMEdit(chatId, oldMsgId, approval, instructions) {
  try {
    const revised = await aiComplete(
      `Revise this LinkedIn DM based on the instructions below.\n\nOriginal:\n${approval.emailDraft.body}\n\nInstructions: ${instructions}\n\nReturn ONLY the revised message body. Keep it conversational and concise. No subject line.`,
      { maxTokens: 600, task: 'linkedin_dm_edit' }
    );
    if (!revised?.trim()) throw new Error('Empty revision from AI');
    approval.emailDraft.body = revised.trim();
    pendingApprovals.delete(oldMsgId);
    const msg = [
      `*ROCO — LINKEDIN DM Revised*`,
      ``,
      `To: *${approval.name}* (${approval.firm})`,
      ``,
      '```',
      approval.emailDraft.body,
      '```',
    ].join('\n');
    const newSent = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
    pendingApprovals.set(newSent.message_id, { ...approval });
    await bot.editMessageReplyMarkup(buildLinkedInDMKeyboard(newSent.message_id), {
      chat_id: chatId, message_id: newSent.message_id,
    }).catch(() => {});
    await bot.sendMessage(chatId, `✅ Revised — approve or edit using the buttons above.`);
  } catch (err) {
    error('handleLinkedInDMEdit failed', { err: err.message });
    await bot.sendMessage(chatId, `⚠ Edit failed: ${err.message}`);
  }
}

/**
 * Re-sends an updated draft to Telegram (for direct BODY:/SUBJECT: edits).
 * Keeps the original Promise active under the new message_id.
 */
async function resendUpdatedApproval(chatId, oldMsgId, approval) {
  pendingApprovals.delete(oldMsgId);
  const draft = approval.emailDraft;
  const isLinkedIn = approval.isLinkedInDM;
  const stageLabel = isLinkedIn ? 'LINKEDIN DM' : 'EMAIL (Edited)';
  const subjectBlock = (!isLinkedIn && draft.subject)
    ? `*Subject A:* ${draft.subject}\n*Subject B:* ${draft.alternativeSubject || 'N/A'}\n`
    : '';
  const msg = [
    `*ROCO — ${stageLabel} Draft Ready*`,
    ``,
    `To: *${approval.name}* (${approval.firm})`,
    ``,
    subjectBlock,
    '```',
    draft.body,
    '```',
  ].join('\n');
  const newSent = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
  pendingApprovals.set(newSent.message_id, { ...approval });
  const keyboard = isLinkedIn
    ? buildLinkedInDMKeyboard(newSent.message_id)
    : buildKeyboard(newSent.message_id);
  await bot.editMessageReplyMarkup(keyboard, {
    chat_id: chatId, message_id: newSent.message_id,
  }).catch(() => {});
  await bot.sendMessage(chatId, `✅ Updated — approve or edit using the buttons above.`);
}

async function handleMessage(msg) {
  const rawText = (msg.text || '').trim();
  const chatId = String(msg.chat.id);
  const authorisedId = String(process.env.TELEGRAM_CHAT_ID);

  if (chatId !== authorisedId) return;

  // Strip @botname suffix — Telegram adds this in groups and sometimes DMs
  const text = rawText.replace(/@\w+$/, '').trim();

  // Handle commands
  if (text.startsWith('/')) {
    await handleCommand(text, chatId);
    return;
  }

  // Route non-command text to approval handler
  await handleApprovalResponse(text, chatId);
}

// ─────────────────────────────────────────────
// INLINE BUTTON HANDLER
// ─────────────────────────────────────────────

async function handleCallbackQuery(query) {
  const chatId      = String(query.message?.chat?.id);
  const authorised  = String(process.env.TELEGRAM_CHAT_ID);
  if (chatId !== authorised) return;

  // Always dismiss the spinner
  await bot.answerCallbackQuery(query.id).catch(() => {});

  const data   = query.data || '';
  const colon  = data.indexOf(':');
  if (colon === -1) return;
  const action = data.slice(0, colon);
  const msgId  = Number(data.slice(colon + 1));

  const approval = pendingApprovals.get(msgId);
  if (!approval) {
    await bot.sendMessage(chatId, '⚠ This draft is no longer in the queue — it may have already been handled.');
    return;
  }

  if (action === 'aa' || action === 'ab') {
    const variant = action === 'aa' ? 'A' : 'B';
    const subject = variant === 'A' ? approval.emailDraft.subject : approval.emailDraft.alternativeSubject;
    resolveApproval(msgId, approval, 'approve', { variant, subject });
    await bot.sendMessage(chatId, `✅ Sending to *${approval.name}* with Subject ${variant}...`, { parse_mode: 'Markdown' });

  } else if (action === 'sa') {
    // Sourcing LinkedIn DM — single approve button
    resolveApproval(msgId, approval, 'approve', { variant: 'A', subject: approval.emailDraft.subject });
    await bot.sendMessage(chatId, `✅ LinkedIn DM approved for *${approval.name}*...`, { parse_mode: 'Markdown' });

  } else if (action === 'sk') {
    resolveApproval(msgId, approval, 'skip');
    await bot.sendMessage(chatId, `🗑 Deleted — *${approval.name}* skipped from Roco outreach. Handle manually.`, { parse_mode: 'Markdown' });

  } else if (action === 'ec') {
    // End Chat — archive contact permanently, Roco will not contact again
    if (approval.isSourcing && approval.contactId) {
      try {
        const sb = getSupabase();
        if (sb) {
          await sb.from('company_contacts').update({
            pipeline_stage: 'archived',
            updated_at: new Date().toISOString(),
          }).eq('id', approval.contactId).then(null, () => {});
        }
      } catch {}
    }
    resolveApproval(msgId, approval, 'skip');
    await bot.sendMessage(chatId, `🚫 *${approval.name}* archived — Roco will not contact them again.`, { parse_mode: 'Markdown' });

  } else if (action === 'ed') {
    pendingEditReqs.set(chatId, msgId);
    await bot.sendMessage(chatId,
      `✏ *Edit draft for ${approval.name}*\n\nType your instructions and I'll redraft:`,
      { parse_mode: 'Markdown' }
    );

  } else if (action === 'ra') {
    // Reply approve — send via Unipile
    const ra = approval.replyApproval;
    if (!ra) { await bot.sendMessage(chatId, '⚠ Missing reply data.'); return; }
    try {
      const { sendEmailReply, sendLinkedInReply } = await import('../integrations/unipileClient.js');
      let sent = null;
      if (ra.channel === 'linkedin') {
        sent = await sendLinkedInReply({ chatId: ra.replyToId, message: ra.replyBody });
      } else {
        sent = await sendEmailReply({
          to:                ra.contactEmail,
          toName:            approval.name,
          subject:           `Re: our conversation`,
          body:              ra.replyBody,
          replyToProviderId: ra.replyToId || null,
          accountId:         ra.emailAccountId || null,
        });
      }
      const sb = getSupabase();
      if (sb && ra.queueItemId) {
        await sb.from('approval_queue').update({
          status:  'sent',
          sent_at: new Date().toISOString(),
        }).eq('id', ra.queueItemId).catch(() => {});
      }
      pendingApprovals.delete(msgId);
      const ch = ra.channel === 'linkedin' ? 'LinkedIn' : 'Email';
      await bot.sendMessage(chatId, `✅ Reply sent to *${approval.name}* via ${ch}${sent ? '' : ' (send may have failed — check logs)'}`, { parse_mode: 'Markdown' });
    } catch (err) {
      await bot.sendMessage(chatId, `⚠ Send failed: ${err.message}`);
    }

  } else if (action === 're') {
    // Reply edit — wait for instructions
    pendingEditReqs.set(chatId, msgId);
    await bot.sendMessage(chatId,
      `✏ *Edit reply for ${approval.name}*\n\nType your edit instructions:`,
      { parse_mode: 'Markdown' }
    );

  } else if (action === 'rs') {
    // Reply skip
    const ra = approval.replyApproval;
    const sb = getSupabase();
    if (sb && ra?.queueItemId) {
      await sb.from('approval_queue').update({ status: 'skipped' }).eq('id', ra.queueItemId).catch(() => {});
    }
    pendingApprovals.delete(msgId);
    await bot.sendMessage(chatId, `✗ Reply to *${approval.name}* skipped.`, { parse_mode: 'Markdown' });
  }
}

// ─────────────────────────────────────────────
// SHARED RESOLVE HELPER
// ─────────────────────────────────────────────

function resolveApproval(msgId, approval, action, extra = {}) {
  pendingApprovals.delete(msgId);
  editLoops.delete(approval.contactPage?.id);
  if (approval.queueId) {
    const status = action === 'approve' ? 'telegram_approved' : 'telegram_skipped';
    const subject = action === 'approve' ? (extra.subject || null) : null;
    updateApprovalStatus(approval.queueId, status, subject).catch(() => {});
  }
  if (action === 'approve') {
    approval.resolve({ action: 'approve', variant: extra.variant, subject: extra.subject, body: extra.body || approval.emailDraft?.body });
  } else if (action === 'edit') {
    approval.resolve({ action: 'edit', instructions: extra.instructions });
  } else {
    approval.resolve({ action: 'skip' });
  }
}

// ─────────────────────────────────────────────
// TEXT APPROVAL HANDLER
// ─────────────────────────────────────────────

async function handleApprovalResponse(text, chatId) {
  const upper = text.toUpperCase().trim();

  // ── If we're waiting for edit instructions after a button press ──
  const editMsgId = pendingEditReqs.get(chatId);
  if (editMsgId !== undefined) {
    pendingEditReqs.delete(chatId);
    const approval = pendingApprovals.get(editMsgId);
    if (!approval) {
      await bot.sendMessage(chatId, 'That draft is no longer in the queue.');
      return;
    }
    // Reply edit — handle differently (re-draft in-place, resend to Telegram)
    if (approval.isReply) {
      await handleReplyEdit(chatId, editMsgId, approval, text);
      return;
    }
    // Direct body edit — "BODY: <text>" replaces body without AI redraft
    if (text.toUpperCase().startsWith('BODY:')) {
      const newBody = text.slice(5).trim();
      if (newBody) {
        approval.emailDraft.body = newBody;
        await resendUpdatedApproval(chatId, editMsgId, approval);
        return;
      }
    }
    // Direct subject edit — "SUBJECT: <text>" replaces subject without AI redraft (emails only)
    if (text.toUpperCase().startsWith('SUBJECT:') && !approval.isLinkedInDM) {
      const newSubject = text.slice(8).trim();
      if (newSubject) {
        approval.emailDraft.subject = newSubject;
        await resendUpdatedApproval(chatId, editMsgId, approval);
        return;
      }
    }
    // LinkedIn DM edit — AI revises body in-place, no round-trip to orchestrator
    if (approval.isLinkedInDM) {
      await handleLinkedInDMEdit(chatId, editMsgId, approval, text);
      return;
    }
    // Normal email edit — resolve and let orchestrator redraft with AI
    const loops = (editLoops.get(approval.contactPage?.id) || 0) + 1;
    if (loops > 3) {
      await bot.sendMessage(chatId, `Max edits reached for ${approval.name}. Skipping.`);
      resolveApproval(editMsgId, approval, 'skip');
      return;
    }
    editLoops.set(approval.contactPage?.id, loops);
    resolveApproval(editMsgId, approval, 'edit', { instructions: text });
    await bot.sendMessage(chatId, `Got it — redrafting for *${approval.name}* (edit ${loops}/3)...`, { parse_mode: 'Markdown' });
    return;
  }

  // ── STOP ──
  if (upper === 'STOP') {
    if (rocoState) rocoState.status = 'PAUSED';
    try {
      const state = await loadSessionState();
      state.rocoStatus = 'PAUSED';
      state.outreachEnabled = false;
      await saveSessionState(state);
    } catch {}
    await bot.sendMessage(chatId, `⏸ *Roco paused.* No emails or DMs will send.\n\nType /resume to restart.`, { parse_mode: 'Markdown' });
    for (const [id, ap] of pendingApprovals) {
      if (ap.queueId) updateApprovalStatus(ap.queueId, 'telegram_skipped').catch(() => {});
      ap.resolve({ action: 'skip' });
    }
    pendingApprovals.clear();
    return;
  }

  if (pendingApprovals.size === 0) {
    await bot.sendMessage(chatId, 'No emails waiting for approval.\n\nUse /queue to see the queue or /status for system status.');
    return;
  }

  const entries = [...pendingApprovals.entries()]; // ordered oldest-first

  // ── Numbered commands: APPROVE 2 A / SKIP 1 / EDIT 1 instructions ──
  const approveNum = upper.match(/^APPROVE\s+(\d+)\s+(A|B)$/);
  const skipNum    = upper.match(/^SKIP\s+(\d+)$/);
  const editNum    = text.match(/^EDIT\s+(\d+)\s+(.+)$/i);

  if (approveNum) {
    const idx = parseInt(approveNum[1]) - 1;
    if (idx < 0 || idx >= entries.length) {
      await bot.sendMessage(chatId, `No draft #${idx + 1}. Queue has ${entries.length} item(s). Use /queue to see them.`);
      return;
    }
    const [msgId, approval] = entries[idx];
    const variant = approveNum[2];
    const subject = variant === 'A' ? approval.emailDraft.subject : approval.emailDraft.alternativeSubject;
    resolveApproval(msgId, approval, 'approve', { variant, subject });
    await bot.sendMessage(chatId, `✅ Sending to *${approval.name}* with Subject ${variant}...`, { parse_mode: 'Markdown' });
    return;
  }

  if (skipNum) {
    const idx = parseInt(skipNum[1]) - 1;
    if (idx < 0 || idx >= entries.length) {
      await bot.sendMessage(chatId, `No draft #${idx + 1}. Queue has ${entries.length} item(s). Use /queue to see them.`);
      return;
    }
    const [msgId, approval] = entries[idx];
    resolveApproval(msgId, approval, 'skip');
    await bot.sendMessage(chatId, `✗ Skipped — draft for *${approval.name}* deleted.`, { parse_mode: 'Markdown' });
    return;
  }

  if (editNum) {
    const idx = parseInt(editNum[1]) - 1;
    const instructions = editNum[2].trim();
    if (idx < 0 || idx >= entries.length) {
      await bot.sendMessage(chatId, `No draft #${idx + 1}. Queue has ${entries.length} item(s). Use /queue to see them.`);
      return;
    }
    const [msgId, approval] = entries[idx];
    const loops = (editLoops.get(approval.contactPage?.id) || 0) + 1;
    if (loops > 3) {
      await bot.sendMessage(chatId, `Max edits reached for ${approval.name}. Skipping.`);
      resolveApproval(msgId, approval, 'skip');
      return;
    }
    editLoops.set(approval.contactPage?.id, loops);
    resolveApproval(msgId, approval, 'edit', { instructions });
    await bot.sendMessage(chatId, `Got it — redrafting for *${approval.name}* (edit ${loops}/3)...`, { parse_mode: 'Markdown' });
    return;
  }

  // ── Simple commands — act on the most recently queued draft ──
  const [lastMsgId, approval] = entries[entries.length - 1];

  if (upper === 'APPROVE A' || upper === 'APPROVE B') {
    const variant = upper.endsWith('A') ? 'A' : 'B';
    const subject = variant === 'A' ? approval.emailDraft.subject : approval.emailDraft.alternativeSubject;
    resolveApproval(lastMsgId, approval, 'approve', { variant, subject });
    await bot.sendMessage(chatId, `✅ Sending to *${approval.name}* with Subject ${variant}...`, { parse_mode: 'Markdown' });
    return;
  }

  if (upper === 'SKIP') {
    resolveApproval(lastMsgId, approval, 'skip');
    await bot.sendMessage(chatId, `✗ Skipped — draft for *${approval.name}* deleted.`, { parse_mode: 'Markdown' });
    return;
  }

  if (upper.startsWith('EDIT ')) {
    const instructions = text.slice(5).trim();
    const loops = (editLoops.get(approval.contactPage?.id) || 0) + 1;
    if (loops > 3) {
      await bot.sendMessage(chatId, `Max edits reached for ${approval.name}. Skipping.`);
      resolveApproval(lastMsgId, approval, 'skip');
      return;
    }
    editLoops.set(approval.contactPage?.id, loops);
    resolveApproval(lastMsgId, approval, 'edit', { instructions });
    await bot.sendMessage(chatId, `Got it — redrafting for *${approval.name}* (edit ${loops}/3)...`, { parse_mode: 'Markdown' });
    return;
  }

  // ── Unrecognised ──
  const queueSize = entries.length;
  await bot.sendMessage(chatId,
    `${queueSize} draft${queueSize > 1 ? 's' : ''} waiting.\n\n` +
    `*Tap the buttons* on any draft message, or type:\n\n` +
    `APPROVE [n] A/B — e.g. \`APPROVE 1 A\`\n` +
    `SKIP [n] — e.g. \`SKIP 2\`\n` +
    `EDIT [n] [changes] — e.g. \`EDIT 1 make it shorter\`\n\n` +
    `Or /queue to see the full list.`,
    { parse_mode: 'Markdown' }
  );
}

async function handleCommand(text, chatId) {
  const command = text.split(' ')[0].toLowerCase();
  const args = text.split(' ').slice(1).join(' ').trim();

  switch (command) {
    case '/status':
      await handleStatus(chatId);
      break;
    case '/pause':
      await handlePause(chatId);
      break;
    case '/resume':
      await handleResume(chatId);
      break;
    case '/pipeline':
      await handlePipeline(chatId);
      break;
    case '/queue':
      await handleQueue(chatId);
      break;
    case '/newdeal':
      await handleNewDeal(chatId, args);
      break;
    case '/stop':
      await handlePause(chatId);
      break;
    case '/sourcing':
      await handleSourcingStatus(chatId);
      break;
    case '/pausecampaigns':
      await handlePauseCampaigns(chatId);
      break;
    case '/resumecampaigns':
      await handleResumeCampaigns(chatId);
      break;
    default:
      await bot.sendMessage(chatId,
        'Commands available:\n\n' +
        '/status — Roco status and stats\n' +
        '/pause — Pause all outreach\n' +
        '/resume — Resume outreach\n' +
        '/pipeline — Top 10 active prospects\n' +
        '/queue — Emails waiting for approval\n' +
        '/newdeal [name] — Start a new deal\n' +
        '/sourcing — Sourcing campaigns status\n' +
        '/pausecampaigns — Pause all sourcing campaigns\n' +
        '/resumecampaigns — Resume all sourcing campaigns\n' +
        '/stop — Emergency stop'
      );
  }
}

async function handleStatus(chatId) {
  try {
    const state = await loadSessionState();
    const deals = await getActiveDeals().catch(() => []);
    const sb = getSupabase();

    let emailsSent = 0;
    let queueCount = 0;
    if (sb) {
      const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
      const { count: sent } = await sb.from('emails').select('id', { count: 'exact', head: true })
        .eq('status', 'sent').gte('sent_at', since).catch(() => ({ count: 0 }));
      emailsSent = sent || 0;

      const { count: q } = await sb.from('emails').select('id', { count: 'exact', head: true })
        .eq('status', 'pending_approval').catch(() => ({ count: 0 }));
      queueCount = q || pendingApprovals.size;
    } else {
      queueCount = pendingApprovals.size;
    }

    const dealLines = deals?.map(d =>
      `• ${d.name} — £${(d.committed_amount || 0).toLocaleString()} / £${(d.target_amount || 0).toLocaleString()}`
    ).join('\n') || '• No active deals';

    const isActive = (state.rocoStatus === 'ACTIVE') || (rocoState?.status === 'ACTIVE');

    await bot.sendMessage(chatId,
      `*ROCO STATUS*\n\n` +
      `System: ${isActive ? '🟢 ACTIVE' : '🔴 PAUSED'}\n` +
      `Outreach: ${state.outreachEnabled !== false ? '✅ ON' : '⛔ OFF'}\n` +
      `Follow-ups: ${state.followupEnabled !== false ? '✅ ON' : '⛔ OFF'}\n\n` +
      `*Active Deals:*\n${dealLines}\n\n` +
      `Emails sent (7 days): ${emailsSent}\n` +
      `Approval queue: ${queueCount} waiting`,
      { parse_mode: 'Markdown' }
    );
  } catch (err) {
    error('handleStatus failed', { err: err.message });
    await bot.sendMessage(chatId, `Status: ${rocoState?.status || 'UNKNOWN'}\nEmails sent this session: ${rocoState?.emailsSent || 0}`);
  }
}

async function handlePause(chatId) {
  if (rocoState) rocoState.status = 'PAUSED';
  try {
    const state = await loadSessionState();
    state.rocoStatus = 'PAUSED';
    state.outreachEnabled = false;
    await saveSessionState(state);
  } catch {}
  await bot.sendMessage(chatId,
    '⏸ *Roco paused.* No emails or DMs will send.\n\nType /resume to restart.',
    { parse_mode: 'Markdown' }
  );
}

async function handleResume(chatId) {
  if (rocoState) rocoState.status = 'ACTIVE';
  try {
    const state = await loadSessionState();
    state.rocoStatus = 'ACTIVE';
    state.outreachEnabled = true;
    await saveSessionState(state);
  } catch {}
  await bot.sendMessage(chatId,
    '▶️ *Roco resumed.* Outreach is live again.',
    { parse_mode: 'Markdown' }
  );
}

async function handlePipeline(chatId) {
  const { getAllActiveContacts, getContactProp } = await import('../crm/notionContacts.js');
  try {
    const contacts = await getAllActiveContacts();
    if (!contacts?.length) {
      await bot.sendMessage(chatId, 'No active prospects in pipeline yet.');
      return;
    }
    const top10 = contacts
      .sort((a, b) => (getContactProp(b, 'Investor Score (0-100)') || 0) - (getContactProp(a, 'Investor Score (0-100)') || 0))
      .slice(0, 10);
    const lines = top10.map((c, i) =>
      `${i + 1}. *${getContactProp(c, 'Name')}* (${getContactProp(c, 'Company Name') || 'Unknown'})\n` +
      `   Score: ${getContactProp(c, 'Investor Score (0-100)') || '—'} | Stage: ${getContactProp(c, 'Pipeline Stage') || '—'}`
    ).join('\n\n');
    await bot.sendMessage(chatId, `*TOP PIPELINE PROSPECTS*\n\n${lines}`, { parse_mode: 'Markdown' });
  } catch (err) {
    error('handlePipeline failed', { err: err.message });
    await bot.sendMessage(chatId, 'Could not fetch pipeline.');
  }
}

async function handleQueue(chatId) {
  if (pendingApprovals.size === 0) {
    await bot.sendMessage(chatId, 'No emails waiting for approval. ✅');
    return;
  }

  const entries = [...pendingApprovals.entries()];

  await bot.sendMessage(chatId,
    `*APPROVAL QUEUE — ${entries.length} waiting*\n\n` +
    `Tap a button on any draft below, or type:\n` +
    `\`APPROVE [n] A\` • \`SKIP [n]\` • \`EDIT [n] [instructions]\``,
    { parse_mode: 'Markdown' }
  );

  for (let i = 0; i < entries.length; i++) {
    const [msgId, a] = entries[i];
    const bodyPreview = (a.emailDraft.body || '').slice(0, 220).replace(/\n+/g, ' ');
    const preview = bodyPreview.length < (a.emailDraft.body || '').length ? bodyPreview + '…' : bodyPreview;

    const text = [
      `*#${i + 1} of ${entries.length} — ${a.name}*`,
      `${a.firm} | Score: ${a.score || '—'} | Stage: ${a.stage || '—'}`,
      ``,
      `*Subject A:* ${a.emailDraft.subject}`,
      `*Subject B:* ${a.emailDraft.alternativeSubject || '—'}`,
      ``,
      preview,
    ].join('\n');

    await bot.sendMessage(chatId, text, {
      parse_mode: 'Markdown',
      reply_markup: buildKeyboard(msgId),
    });
  }
}

async function handleNewDeal(chatId, args) {
  if (!args) {
    await bot.sendMessage(chatId,
      'To start a new deal, go to Mission Control:\n' +
      'http://76.13.44.185:3000\n\n' +
      'Or send: /newdeal [deal name] and I will create a basic deal record.'
    );
    return;
  }
  try {
    const deal = await createDeal({ name: args, status: 'ACTIVE' });
    await bot.sendMessage(chatId,
      `✅ *Deal created: ${args}*\n\n` +
      `Go to Mission Control to fill in the full details:\n` +
      `http://76.13.44.185:3000\n\n` +
      `Roco will start researching investors once the deal details are complete.`,
      { parse_mode: 'Markdown' }
    );
  } catch (err) {
    await bot.sendMessage(chatId, `Could not create deal: ${err.message}`);
  }
}

async function handleSourcingStatus(chatId) {
  try {
    const sb = getSupabase();
    if (!sb) { await bot.sendMessage(chatId, 'Database unavailable.'); return; }

    const { data: campaigns } = await sb.from('sourcing_campaigns')
      .select('*').eq('status', 'active');

    if (!campaigns?.length) {
      await bot.sendMessage(chatId, 'No active sourcing campaigns.\n\nLaunch one from Mission Control: http://76.13.44.185:3000 → Sourcing Campaigns');
      return;
    }

    const lines = [];
    for (const c of campaigns) {
      const { count: companies } = await sb.from('target_companies').select('id', { count: 'exact', head: true }).eq('campaign_id', c.id);
      const { count: meetings } = await sb.from('target_companies').select('id', { count: 'exact', head: true }).eq('campaign_id', c.id).eq('meeting_booked', true);
      const { count: contacted } = await sb.from('company_contacts').select('id', { count: 'exact', head: true }).eq('campaign_id', c.id).eq('pipeline_stage', 'contacted');
      lines.push(`*${c.name}*\nFirm: ${c.firm_name || '—'} | Sector: ${c.target_sector}\nCompanies: ${companies || 0} | Contacted: ${contacted || 0} | Meetings: ${meetings || 0}`);
    }

    await bot.sendMessage(chatId,
      `*SOURCING CAMPAIGNS (${campaigns.length} active)*\n\n${lines.join('\n\n')}`,
      { parse_mode: 'Markdown' }
    );
  } catch (err) {
    await bot.sendMessage(chatId, `Could not fetch sourcing status: ${err.message}`);
  }
}

async function handlePauseCampaigns(chatId) {
  try {
    const sb = getSupabase();
    if (!sb) { await bot.sendMessage(chatId, 'Database unavailable.'); return; }
    const { error } = await sb.from('sourcing_campaigns')
      .update({ status: 'paused', updated_at: new Date().toISOString() })
      .eq('status', 'active');
    if (error) throw new Error(error.message);
    await bot.sendMessage(chatId, '⏸ All sourcing campaigns paused.\n\nType /resumecampaigns to restart them.', { parse_mode: 'Markdown' });
  } catch (err) {
    await bot.sendMessage(chatId, `Could not pause campaigns: ${err.message}`);
  }
}

async function handleResumeCampaigns(chatId) {
  try {
    const sb = getSupabase();
    if (!sb) { await bot.sendMessage(chatId, 'Database unavailable.'); return; }
    const { error } = await sb.from('sourcing_campaigns')
      .update({ status: 'active', updated_at: new Date().toISOString() })
      .eq('status', 'paused');
    if (error) throw new Error(error.message);
    await bot.sendMessage(chatId, '▶️ All sourcing campaigns resumed.', { parse_mode: 'Markdown' });
  } catch (err) {
    await bot.sendMessage(chatId, `Could not resume campaigns: ${err.message}`);
  }
}

function registerCommands() {
  if (!bot) return;
  bot.setMyCommands([
    { command: 'status', description: 'Roco status and stats' },
    { command: 'pipeline', description: 'Top 10 active prospects' },
    { command: 'queue', description: 'Emails waiting for approval' },
    { command: 'pause', description: 'Pause all outreach' },
    { command: 'resume', description: 'Resume outreach' },
    { command: 'newdeal', description: 'Create a new deal' },
    { command: 'sourcing', description: 'Sourcing campaigns status' },
    { command: 'pausecampaigns', description: 'Pause all sourcing campaigns' },
    { command: 'resumecampaigns', description: 'Resume all sourcing campaigns' },
    { command: 'stop', description: 'Emergency stop' },
  ]).catch(() => {});
}

/**
 * Register a sourcing approval in the in-memory queue.
 * Called after sending the Telegram message for a sourcing draft.
 * When the user replies APPROVE/SKIP, resolveApproval() updates approval_queue in Supabase.
 */
export function registerSourcingApproval(telegramMsgId, { queueId, contactId, contactName, companyName, score, draft }) {
  const entry = {
    contactPage:  null,
    emailDraft:   { subject: draft?.subject_a || '', alternativeSubject: draft?.subject_b || '', body: draft?.body || '' },
    resolve:      () => {},  // no-op — execution handled by sourcingOrchestrator's execute phase
    score:        score || 0,
    stage:        draft?.subject_a ? 'Email' : 'LinkedIn DM',
    firm:         companyName || '',
    name:         contactName || '',
    contactId:    contactId || null,
    queuedAt:     new Date().toISOString(),
    queueId:      queueId || null,
    isSourcing:   true,
  };
  pendingApprovals.set(telegramMsgId, entry);
}

/**
 * Send a sourcing draft to Telegram with inline keyboard buttons.
 * Handles both email (A/B subjects) and LinkedIn DM drafts.
 */
export async function sendSourcingDraftToTelegram(contact, company, campaign, draft, researchBasis, queueId) {
  if (!bot) return;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!chatId) return;

  const isLinkedIn = !draft.subject_a;
  const channelLabel = isLinkedIn ? 'LINKEDIN DM' : 'EMAIL';
  const tier = company.match_tier?.toUpperCase() || 'WARM';
  const score = company.match_score || 0;

  const subjectBlock = draft.subject_a
    ? `*Subject A:* ${draft.subject_a}\n*Subject B:* ${draft.subject_b || 'N/A'}\n`
    : '';

  const msg = [
    `*ROCO — ${channelLabel} Draft Ready*`,
    `Mode: COMPANY SOURCING`,
    ``,
    `Campaign: ${campaign.name}`,
    `To: *${contact.name}* — ${contact.title || 'Founder/CEO'} at *${company.company_name}*`,
    `Company Score: ${score}/100 | Tier: ${tier} | Stage: INITIAL`,
    `Research basis: ${(researchBasis || '').substring(0, 200)}`,
    ``,
    subjectBlock,
    `\`\`\``,
    draft.body,
    `\`\`\``,
  ].join('\n');

  try {
    const sent = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
    if (!sent?.message_id) return;

    // Attach inline keyboard
    await bot.editMessageReplyMarkup(buildSourcingKeyboard(sent.message_id, isLinkedIn), {
      chat_id: chatId,
      message_id: sent.message_id,
    }).catch(() => {});

    // Register in in-memory approval queue
    registerSourcingApproval(sent.message_id, {
      queueId,
      contactId: contact.id,
      contactName: contact.name,
      companyName: company.company_name,
      score,
      draft,
    });
  } catch (err) {
    error('Failed to send sourcing draft to Telegram', { err: err.message });
  }
}

export function getPendingApprovals() {
  return [...pendingApprovals.entries()].map(([id, a]) => ({
    id,
    name: a.name,
    firm: a.firm,
    score: a.score,
    stage: a.stage,
    subject: a.emailDraft?.subject,
    alternativeSubject: a.emailDraft?.alternativeSubject,
    body: a.emailDraft?.body,
    contactPageId: a.contactPage?.id,
    queuedAt: a.queuedAt || new Date().toISOString(),
  }));
}

/**
 * Wipe all pending approvals for a specific deal from the in-memory queue.
 * Called when a deal is closed so no stale outreach fires.
 */
export function clearApprovalsForDeal(dealId) {
  let cleared = 0;
  for (const [msgId, entry] of pendingApprovals.entries()) {
    if (String(entry.dealId) === String(dealId)) {
      // Resolve as 'skip' so any awaiting Promise chains exit cleanly
      try { entry.resolve?.({ action: 'skip' }); } catch {}
      pendingApprovals.delete(msgId);
      cleared++;
    }
  }
  if (cleared > 0) info(`[CLOSE] Cleared ${cleared} pending approval(s) for deal ${dealId}`);
  return cleared;
}

/**
 * Poll Supabase every 15s for approvals resolved via the Vercel dashboard.
 * When found, resolves the matching in-memory Promise so the orchestrator fires the email.
 */
/**
 * On startup, reload any pending sourcing approvals from Supabase and resend to Telegram.
 * This ensures keyboard callbacks work after a process restart.
 */
export async function reloadPendingSourcingApprovals() {
  const { getSupabase } = await import('../core/supabase.js');
  const sb = getSupabase();
  if (!sb || !bot) return;

  const { data: pending } = await sb.from('approval_queue')
    .select('id, contact_name, firm, score, stage, subject_a, subject_b, body, company_contact_id, campaign_id')
    .eq('outreach_mode', 'company_sourcing')
    .eq('status', 'pending')
    .order('created_at', { ascending: true })
    .limit(20);

  if (!pending?.length) return;

  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!chatId) return;

  info(`Reloading ${pending.length} pending sourcing approvals`);

  for (const item of pending) {
    try {
      const isLinkedIn = item.stage === 'LinkedIn DM';
      const subjectBlock = !isLinkedIn && item.subject_a
        ? `*Subject A:* ${item.subject_a}\n*Subject B:* ${item.subject_b || 'N/A'}\n` : '';
      const msg = [
        `*ROCO — ${item.stage} Draft Ready* _(reloaded)_`,
        `Mode: COMPANY SOURCING`,
        ``,
        `To: *${item.contact_name}* at *${item.firm}*`,
        `Score: ${item.score || 0}/100`,
        ``,
        subjectBlock,
        `\`\`\``,
        item.body || '',
        `\`\`\``,
        ``,
        `Reply: *APPROVE A* | *APPROVE B* | *SKIP*`,
      ].join('\n');

      const sent = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
      const isLinkedInItem = item.stage === 'LinkedIn DM';
      await bot.editMessageReplyMarkup(buildSourcingKeyboard(sent.message_id, isLinkedInItem), {
        chat_id: chatId,
        message_id: sent.message_id,
      }).catch(() => {});
      registerSourcingApproval(sent.message_id, {
        queueId: item.id,
        contactId: item.company_contact_id || null,
        contactName: item.contact_name,
        companyName: item.firm,
        score: item.score || 0,
        draft: { subject_a: item.subject_a, subject_b: item.subject_b, body: item.body },
      });
      await new Promise(r => setTimeout(r, 500));
    } catch (err) {
      error('Failed to reload sourcing approval', { err: err.message });
    }
  }
}

export function startSupabaseApprovalPoller() {
  setInterval(async () => {
    if (pendingApprovals.size === 0) return;
    try {
      const resolved = await getResolvedApprovals();
      for (const item of resolved) {
        for (const [msgId, approval] of pendingApprovals) {
          const matchById = approval.queueId && String(approval.queueId) === String(item.id);
          const matchByTg = item.telegram_msg_id && String(msgId) === String(item.telegram_msg_id);
          if (!matchById && !matchByTg) continue;

          pendingApprovals.delete(msgId);
          editLoops.delete(approval.contactPage?.id);
          await markApprovalProcessing(item.id);

          if (item.status === 'approved') {
            approval.resolve({
              action: 'approve',
              variant: 'A',
              subject: item.approved_subject || approval.emailDraft.subject,
            });
          } else if (item.status === 'edit') {
            approval.resolve({ action: 'edit', instructions: item.edit_instructions || '' });
          } else {
            approval.resolve({ action: 'skip' });
          }
          break;
        }
      }
    } catch {}
  }, 15_000);
}

/**
 * Resolve a pending approval from the dashboard.
 * @param {number|string} id - The Telegram message ID (key in pendingApprovals)
 * @param {string} action - 'approve', 'skip', or 'edit'
 * @param {string} subject - Subject line chosen
 * @param {string} editedBody - Edit instructions (if action is 'edit')
 * @returns {boolean} Whether the approval was found and resolved
 */
/**
 * Approve the latest pending approval — resolves immediately, bypassing the normal window check.
 * Used when the user taps "Approve & Send" directly from the dashboard or Telegram.
 */
export function approveLatestPending({ subject } = {}) {
  if (pendingApprovals.size === 0) return false;
  // Get the most recently added approval (last entry in Map insertion order)
  const entries = [...pendingApprovals.entries()];
  const [msgId, approval] = entries[entries.length - 1];
  pendingApprovals.delete(msgId);
  editLoops.delete(approval.contactPage?.id);
  approval.resolve({
    action: 'approve',
    variant: 'A',
    subject: subject || approval.emailDraft?.subject,
  });
  return true;
}

export function resolveApprovalFromDashboard(id, action, subject, editedBody) {
  const numId = Number(id);
  const approval = pendingApprovals.get(numId) || pendingApprovals.get(String(id));
  if (!approval) return false;

  const key = pendingApprovals.has(numId) ? numId : String(id);
  pendingApprovals.delete(key);

  if (action === 'approve') {
    editLoops.delete(approval.contactPage?.id);
    approval.resolve({
      action: 'approve',
      variant: 'A',
      subject: subject || approval.emailDraft.subject,
    });
  } else if (action === 'skip') {
    editLoops.delete(approval.contactPage?.id);
    approval.resolve({ action: 'skip' });
  } else if (action === 'edit') {
    approval.resolve({ action: 'edit', instructions: editedBody || '' });
  } else {
    approval.resolve({ action: 'skip' });
  }

  return true;
}
