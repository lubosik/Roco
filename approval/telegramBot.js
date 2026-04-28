import TelegramBot from 'node-telegram-bot-api';
import { info, warn, error } from '../core/logger.js';
import {
  addApprovalToQueue, getResolvedApprovals, markApprovalProcessing, updateApprovalStatus,
  loadSessionState, saveSessionState, getActiveDeals, getAllDeals, updateDeal, createDeal, logActivity as sbLogActivity,
} from '../core/supabaseSync.js';
import { getSupabase } from '../core/supabase.js';
import { aiComplete } from '../core/aiClient.js';
import { readGlobalRuntimeSetting, writeGlobalRuntimeSetting } from '../core/runtimeCoordination.js';

let bot;
let rocoState; // injected from orchestrator

export async function clearTelegramApprovalControls(messageId) {
  if (!bot || !messageId) return;
  try {
    await bot.editMessageReplyMarkup({ inline_keyboard: [] }, {
      chat_id: process.env.TELEGRAM_CHAT_ID,
      message_id: Number(messageId),
    });
  } catch {}
}

function sanitizeApprovalText(text) {
  return String(text || '')
    .replace(/\u2014/g, '-')
    .replace(/\u2013/g, '-')
    .replace(/\u2018|\u2019/g, "'")
    .replace(/\u201C|\u201D/g, '"')
    .trim();
}

function isLinkedInStageLabel(stage) {
  const value = String(stage || '').trim().toLowerCase();
  return value === 'linkedin dm'
    || value === 'linkedin_dm'
    || value === 'linkedin_follow_up'
    || value.startsWith('linkedin follow-up')
    || value.startsWith('linkedin follow up');
}

const CURRENCY_SYMBOLS = {
  USD: '$',
  GBP: '£',
  EUR: '€',
  CAD: 'CA$',
  AUD: 'A$',
  CHF: 'Fr',
  SGD: 'S$',
};

function formatCurrencyAmount(amount, currency = 'USD') {
  const symbol = CURRENCY_SYMBOLS[(currency || 'USD').toUpperCase()] || '$';
  return `${symbol}${Number(amount || 0).toLocaleString()}`;
}

function truncatePreview(value, max = 180) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '—';
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

async function resolveDealArgument(args, statuses = null) {
  const deals = await getAllDeals().catch(() => []);
  const pool = Array.isArray(statuses) && statuses.length
    ? deals.filter(deal => statuses.includes(deal.status))
    : deals;
  const needle = String(args || '').trim().toLowerCase();

  if (!needle) return { pool, deal: null, matches: [] };

  const exact = pool.find(deal =>
    String(deal.id || '').toLowerCase() === needle ||
    String(deal.name || '').trim().toLowerCase() === needle
  );
  if (exact) return { pool, deal: exact, matches: [exact] };

  const matches = pool.filter(deal =>
    String(deal.name || '').trim().toLowerCase().includes(needle)
  );
  if (matches.length === 1) return { pool, deal: matches[0], matches };
  return { pool, deal: null, matches };
}

function formatDealChoices(deals = []) {
  if (!deals.length) return 'No matching deals.';
  return deals.map(deal => `- ${deal.name} (${deal.status || 'UNKNOWN'})`).join('\n');
}

function buildCommandGuide() {
  return [
    '*Telegram Command Guide*',
    '',
    'Use the project name after the command when you want to target one deal.',
    '',
    '*Examples:*',
    '`/status Project Electrify`',
    '`/pipeline Project Electrify`',
    '`/campaignstatus Project Electrify`',
    '`/emails Project Electrify`',
    '`/linkedindms Project Electrify`',
    '`/queue Project Electrify`',
    '`/pause Project Electrify`',
    '`/resume Project Electrify`',
    '`/close Project Electrify`',
    '',
    '*Global controls:*',
    '`/pause all`',
    '`/resume all`',
    '`/stop`',
    '',
    'If the deal name is missing or matches more than one project, Roco will ask you to specify it.',
  ].join('\n');
}

async function resolveRequiredDeal(chatId, args, {
  statuses = ['ACTIVE', 'PAUSED', 'CLOSED'],
  actionLabel = 'use',
  commandExample = '/status [deal name]',
} = {}) {
  const input = String(args || '').trim();
  const { pool, deal, matches } = await resolveDealArgument(input, statuses);
  if (deal) return deal;

  const scopedPool = pool.filter(row => statuses.includes(row.status));
  const choices = input ? (matches.length ? matches : scopedPool) : scopedPool;
  await bot.sendMessage(chatId,
    `Specify which deal to ${actionLabel}.\n\nUse: ${commandExample}\n\nExample: ${commandExample.replace('[deal name]', 'Project Electrify')}\n\n${formatDealChoices(choices.slice(0, 12))}`
  );
  return null;
}

const pendingApprovals  = new Map(); // messageId -> { contactPage, emailDraft, resolve, ... }
const editLoops         = new Map(); // contactPageId -> count
const pendingEditReqs   = new Map(); // chatId -> msgId  (waiting for edit instructions after button press)
const processingApprovals     = new Set(); // msgIds currently being handled (dedup guard)
const recentlyResolvedQueueIds = new Set(); // queue IDs resolved via Telegram, awaiting DB commit
const EDIT_REQUESTS_KEY = 'GLOBAL_TELEGRAM_EDIT_REQUESTS';
const EDIT_LOOPS_KEY = 'GLOBAL_TELEGRAM_EDIT_LOOPS';

function getApprovalEditKey(approval) {
  return String(approval?.contactPage?.id || approval?.contactId || approval?.queueId || '');
}

async function readSharedMap(key) {
  const value = await readGlobalRuntimeSetting(key).catch(() => null);
  return value && typeof value === 'object' ? value : {};
}

function persistSharedMap(key, map) {
  const payload = Object.fromEntries([...map.entries()].map(([entryKey, value]) => [String(entryKey), value]));
  return writeGlobalRuntimeSetting(key, payload).catch(() => false);
}

async function getPendingEditRequest(chatId) {
  const local = pendingEditReqs.get(String(chatId));
  if (local !== undefined) return local;
  const shared = await readSharedMap(EDIT_REQUESTS_KEY);
  const sharedValue = shared[String(chatId)];
  if (sharedValue !== undefined) pendingEditReqs.set(String(chatId), sharedValue);
  return sharedValue;
}

function setPendingEditRequest(chatId, msgId) {
  pendingEditReqs.set(String(chatId), msgId);
  persistSharedMap(EDIT_REQUESTS_KEY, pendingEditReqs);
}

function clearPendingEditRequest(chatId) {
  pendingEditReqs.delete(String(chatId));
  persistSharedMap(EDIT_REQUESTS_KEY, pendingEditReqs);
}

function clearPendingEditRequestForMsg(msgId) {
  let changed = false;
  for (const [chatId, value] of pendingEditReqs.entries()) {
    if (String(value) === String(msgId)) {
      pendingEditReqs.delete(chatId);
      changed = true;
    }
  }
  if (changed) persistSharedMap(EDIT_REQUESTS_KEY, pendingEditReqs);
}

async function getEditLoopCount(approval) {
  const key = getApprovalEditKey(approval);
  if (!key) return 0;
  if (editLoops.has(key)) return Number(editLoops.get(key) || 0);
  const shared = await readSharedMap(EDIT_LOOPS_KEY);
  const count = Number(shared[key] || 0);
  editLoops.set(key, count);
  return count;
}

function setEditLoopCount(approval, count) {
  const key = getApprovalEditKey(approval);
  if (!key) return;
  editLoops.set(key, Number(count || 0));
  persistSharedMap(EDIT_LOOPS_KEY, editLoops);
}

function clearEditLoopCount(approval) {
  const key = getApprovalEditKey(approval);
  if (!key) return;
  editLoops.delete(key);
  persistSharedMap(EDIT_LOOPS_KEY, editLoops);
}

/** Called by /api/queue to filter out items whose Telegram approval is still committing. */
export function getRecentlyResolvedQueueIds() {
  return recentlyResolvedQueueIds;
}

export function getTelegramTransport() {
  const value = String(process.env.TELEGRAM_TRANSPORT || 'polling').trim().toLowerCase();
  if (['off', 'disabled', 'none'].includes(value)) return 'off';
  if (['webhook', 'hook'].includes(value)) return 'webhook';
  return 'polling';
}

export function initTelegramBot(state) {
  rocoState = state;
  const transport = getTelegramTransport();

  if (transport === 'off') {
    info('Telegram bot disabled by TELEGRAM_TRANSPORT=off');
    return null;
  }

  bot = new TelegramBot(
    process.env.TELEGRAM_BOT_TOKEN,
    transport === 'polling' ? { polling: true } : { polling: false },
  );
  info(`Telegram bot started (${transport})`);

  bot.on('message',        handleMessage);
  bot.on('callback_query', handleCallbackQuery);

  if (transport === 'polling') {
    // Suppress repeated 409 Conflict errors (two instances competing for same token)
    // to once per 60 seconds — the bot still works for outgoing messages even when 409 fires.
    let last409LogTime = 0;
    bot.on('polling_error', (err) => {
      if (String(err?.message || '').includes('409 Conflict')) {
        const now = Date.now();
        if (now - last409LogTime > 60000) {
          error('Telegram polling conflict (409) — another bot instance may be running on Railway', { err: err.message });
          last409LogTime = now;
        }
        return;
      }
      error('Telegram polling error', { err: err.message });
    });
  }

  registerCommands();
  return bot;
}

export async function processTelegramUpdate(update) {
  if (!bot || typeof bot.processUpdate !== 'function' || !update) return false;
  try {
    bot.processUpdate(update);
    return true;
  } catch (err) {
    error('Telegram webhook update handling failed', { err: err.message });
    return false;
  }
}

export async function sendTelegram(text) {
  if (!bot) return null;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!chatId) return null;
  try {
    return await bot.sendMessage(chatId, text, { parse_mode: 'Markdown' });
  } catch (err) {
    if (String(err?.message || '').includes("can't parse entities")) {
      try {
        const plainText = String(text || '')
          .replace(/```/g, '')
          .replace(/[_*`]/g, '')
          .replace(/\[(.*?)\]\((.*?)\)/g, '$1 ($2)');
        return await bot.sendMessage(chatId, plainText);
      } catch {}
    }
    error('Telegram send failed', { err: err.message });
    return null;
  }
}

export async function sendTelegramVoiceNote(voiceInput, options = {}) {
  if (!bot || !voiceInput) return null;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!chatId) return null;
  try {
    return await bot.sendVoice(chatId, voiceInput, options);
  } catch (err) {
    warn('Telegram voice note send failed, retrying as audio', { err: err.message });
    try {
      return await bot.sendAudio(chatId, voiceInput, options);
    } catch (audioErr) {
      error('Telegram voice note send failed', { err: audioErr.message });
      return null;
    }
  }
}

export async function sendEmailForApproval(contactPage, emailDraft, researchSummary, score, stage, dealId = null) {
  const { getContactProp } = await import('../crm/notionContacts.js');
  const name = getContactProp(contactPage, 'Name');
  const firm = getContactProp(contactPage, 'Company Name') || 'Unknown Firm';
  const rawEmail = getContactProp(contactPage, 'Email') || null;
  const contactEmail = String(rawEmail || '').trim() || null;
  const sb = getSupabase();

  // GATE: never send to Telegram if name is null/invalid
  if (!name || name.trim() === '' || name.toLowerCase() === 'null') {
    warn(`[TELEGRAM] Refusing to send approval for contact with invalid name: "${name}"`);
    return { action: 'skip' };
  }

  if (!isLinkedInStageLabel(stage) && !contactEmail) {
    warn(`[TELEGRAM] Refusing to queue email approval for ${name} — no usable email address`);
    return { action: 'missing_email' };
  }

  const isLinkedIn = isLinkedInStageLabel(stage);
  const stageLabel = isLinkedIn ? 'LINKEDIN DM' : stage?.includes('FOLLOW') ? 'FOLLOW-UP EMAIL' : 'EMAIL';
  const researchBasis = (researchSummary || '').substring(0, 200) || 'No specific research basis';

  emailDraft.body = sanitizeApprovalText(emailDraft.body);
  emailDraft.subject = sanitizeApprovalText(emailDraft.subject);
  emailDraft.alternativeSubject = sanitizeApprovalText(emailDraft.alternativeSubject);

  const hasSubjects = !!emailDraft.subject;
  const subjectBlock = hasSubjects
    ? `📧 *Subject A:* _${emailDraft.subject}_\n📧 *Subject B:* _${emailDraft.alternativeSubject || 'N/A'}_\n`
    : '';

  // Format body for Telegram — trim to 800 chars to avoid truncation
  const bodyPreview = (emailDraft.body || '').length > 800
    ? emailDraft.body.substring(0, 800) + '…'
    : emailDraft.body;

  // Hyperlink the contact name: LinkedIn URL first, website fallback, else plain bold
  const emailContactLinkedIn = getContactProp(contactPage, 'LinkedIn URL') || contactPage?.linkedin_url || null;
  const emailContactWebsite = contactPage?.website || null;
  const emailContactProfileUrl = emailContactLinkedIn || emailContactWebsite || null;
  const emailNameDisplay = emailContactProfileUrl
    ? `[${name}](${emailContactProfileUrl})`
    : `*${name}*`;

  const msg = [
    `*ROCO — ${stageLabel} Ready for Approval*`,
    ``,
    `👤 ${emailNameDisplay} · ${firm}`,
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
      let queueRow = null;
      queueRow = await addApprovalToQueue({
        telegramMsgId: null,
        dealId,
        contactId: contactPage?.id,
        contactName: name,
        contactEmail,
        firm,
        stage,
        score,
        subjectA: emailDraft.subject,
        subjectB: emailDraft.alternativeSubject,
        body: emailDraft.body,
        researchSummary: researchSummary || null,
      }).catch(() => null);

      const chatId = process.env.TELEGRAM_CHAT_ID;
      const sent = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
      const entry = { contactPage, contactEmail, emailDraft, resolve, score, stage, firm, name, dealId, queuedAt: new Date().toISOString(), queueId: queueRow?.id || null, contactId: contactPage?.id || null };
      pendingApprovals.set(sent.message_id, entry);
      info(`Email draft sent to Telegram for approval: ${name}`);

      // Attach action buttons (done after send so we have the message_id)
      bot.editMessageReplyMarkup(buildKeyboard(sent.message_id, queueRow?.id || null), {
        chat_id: chatId,
        message_id: sent.message_id,
      }).catch(() => {});

      if (sb && queueRow?.id) {
        await sb.from('approval_queue').update({
          telegram_msg_id: sent.message_id,
        }).eq('id', queueRow.id);
      }
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
export async function sendLinkedInDMForApproval(contact, body, dealId = null, options = {}) {
  if (!bot) return { action: 'error' };
  const name = contact.name || 'Contact';
  const firm = contact.company_name || 'Unknown Firm';
  const score = contact.investor_score ?? null;
  const scoreLabel = score == null ? 'unscored' : `${score}/100`;
  const stage = options.stage || 'LinkedIn DM';
  const researchSummary = options.researchSummary || null;
  let queueId = options.queueId || null;

  const contactType = contact.contact_type === 'individual' ? '👤 Individual' : '🏢 Firm';
  body = sanitizeApprovalText(body);
  const bodyPreview = (body || '').length > 600 ? body.substring(0, 600) + '…' : body;

  // Hyperlink the contact name: LinkedIn URL first, website fallback, else plain bold
  const dmContactProfileUrl = contact.linkedin_url || contact.website || null;
  const dmNameDisplay = dmContactProfileUrl
    ? `[${name}](${dmContactProfileUrl})`
    : `*${name}*`;

  const msg = [
    `*ROCO — LinkedIn DM Ready for Approval*`,
    ``,
    `👤 ${dmNameDisplay} · ${firm}`,
    `📊 Score: ${scoreLabel}  |  ${contactType}  |  LinkedIn DM`,
    researchSummary ? `🔍 _${String(researchSummary).substring(0, 220)}_` : null,
    ``,
    '```',
    bodyPreview,
    '```',
    ``,
    `Reply: *APPROVE* | *EDIT [instructions]* | *MANUAL* | *CLOSE*`,
  ].filter(Boolean).join('\n');

  return new Promise(async (resolve) => {
    try {
      const sb = getSupabase();
      if (!queueId && contact?.id) {
        try {
          const queueRow = await addApprovalToQueue({
            dealId: dealId || null,
            contactId: contact.id,
            contactName: name,
            contactEmail: contact.email || null,
            firm,
            stage,
            body: body || '',
            score,
            researchSummary,
            outreachMode: 'investor_outreach',
          });
          queueId = queueRow?.id || null;
        } catch (err) {
          warn('Could not create LinkedIn approval queue row', { err: err.message, contactId: contact.id });
        }
      }

      const chatId = process.env.TELEGRAM_CHAT_ID;
      const sent = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
      pendingApprovals.set(sent.message_id, {
        name, firm, score, dealId,
        isLinkedInDM: true,
        contactId: contact.id || null,
        stage,
        emailDraft: { body, subject: null, alternativeSubject: null },
        resolve,
        contactPage: null,
        queuedAt: new Date().toISOString(),
        queueId,
      });
      bot.editMessageReplyMarkup(buildLinkedInDMKeyboard(sent.message_id, queueId), {
        chat_id: chatId,
        message_id: sent.message_id,
      }).catch(() => {});
      // Persist the Telegram message ID so that after a server restart the item
      // can be found in the in-memory map (via reloadPendingInvestorApprovals) and
      // reloadApprovalForTelegramMessage can match button presses on old messages.
      if (sb && queueId) {
        sb.from('approval_queue').update({ telegram_msg_id: sent.message_id }).eq('id', queueId).then(null, () => {});
      }
      info(`LinkedIn DM draft sent to Telegram for approval: ${name}`);
    } catch (err) {
      error('Failed to send LinkedIn DM draft to Telegram', { err: err.message });
      resolve({ action: 'error' });
    }
  });
}

function buildPriorChatKeyboard(approvalId) {
  return {
    inline_keyboard: [
      [
        { text: '✓ Proceed — Send DM', callback_data: `prior_chat:proceed:${approvalId}` },
        { text: '✗ Skip Contact',       callback_data: `prior_chat:skip:${approvalId}` },
      ],
    ],
  };
}

/**
 * Notify Telegram about a prior LinkedIn chat found during connection acceptance.
 * Decision is stored in DB; no Promise needed — handled via callback or dashboard.
 */
export async function sendPriorChatForApproval({ contactName, firm, dealName, summary, messageCount, approvalId }) {
  if (!bot) return;
  const msg = [
    `🔁 *Prior LinkedIn Chat Found*`,
    ``,
    `👤 *${contactName}* · ${firm}`,
    `📁 Deal: ${dealName}`,
    `💬 ${messageCount} prior message(s) found`,
    ``,
    `_${summary}_`,
    ``,
    `Proceed with new DM, or skip this contact?`,
  ].join('\n');

  try {
    const chatId = process.env.TELEGRAM_CHAT_ID;
    const sent   = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
    const sb = getSupabase();
    if (sb && approvalId) {
      try {
        await sb.from('approval_queue').update({
          telegram_msg_id: sent.message_id,
        }).eq('id', approvalId);
      } catch {}
    }
    await bot.editMessageReplyMarkup(buildPriorChatKeyboard(approvalId), {
      chat_id: chatId, message_id: sent.message_id,
    }).catch(() => {});
    info(`Prior chat review sent to Telegram for ${contactName} (approvalId: ${approvalId})`);
  } catch (err) {
    error('sendPriorChatForApproval failed', { err: err.message });
  }
}

function callbackPayload(action, msgId, queueId = null) {
  return queueId ? `${action}:${msgId}:${queueId}` : `${action}:${msgId}`;
}

function buildKeyboard(msgId, queueId = null) {
  return {
    inline_keyboard: [
      [
        { text: '✓ Send A', callback_data: callbackPayload('aa', msgId, queueId) },
        { text: '✓ Send B', callback_data: callbackPayload('ab', msgId, queueId) },
      ],
      [
        { text: '✏ Edit',   callback_data: callbackPayload('ed', msgId, queueId) },
        { text: '🗑 Delete', callback_data: callbackPayload('sk', msgId, queueId) },
      ],
    ],
  };
}

function buildLinkedInDMKeyboard(msgId, queueId = null) {
  return {
    inline_keyboard: [
      [{ text: '✓ Approve', callback_data: callbackPayload('sa', msgId, queueId) }],
      [
        { text: '✏ Edit', callback_data: callbackPayload('ed', msgId, queueId) },
        { text: 'Manual', callback_data: callbackPayload('lm', msgId, queueId) },
        { text: 'Close',  callback_data: callbackPayload('lc', msgId, queueId) },
      ],
    ],
  };
}

function buildReloadedApprovalKeyboard(msgId, hasSubjects = false, queueId = null) {
  return {
    inline_keyboard: hasSubjects
      ? [
          [
            { text: '✓ Send A', callback_data: callbackPayload('aa', msgId, queueId) },
            { text: '✓ Send B', callback_data: callbackPayload('ab', msgId, queueId) },
          ],
          [{ text: '✗ Skip', callback_data: callbackPayload('sk', msgId, queueId) }],
        ]
      : [
          [{ text: '✓ Approve', callback_data: callbackPayload('sa', msgId, queueId) }],
          [
            { text: 'Manual', callback_data: callbackPayload('lm', msgId, queueId) },
            { text: 'Close',  callback_data: callbackPayload('lc', msgId, queueId) },
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
        { text: '✓ Approve', callback_data: `rq:${msgId}` },
        { text: '⚡ Send Now', callback_data: `ra:${msgId}` },
        { text: '✏ Edit',           callback_data: `re:${msgId}` },
      ],
      [
        { text: '✗ Skip',    callback_data: `rs:${msgId}` },
        { text: 'Manual',    callback_data: `rm:${msgId}` },
        { text: 'Close',     callback_data: `rc:${msgId}` },
      ],
    ],
  };
}

/**
 * Send a batched reply draft to Telegram for approval with inline buttons.
 * When approved, sends via Unipile automatically.
 */
export async function sendReplyForApproval(queueItemId, contact, replyBody, contextName, channel, replyToId, emailAccountId, options = {}) {
  if (!bot) return;
  const name        = contact?.name || 'Contact';
  const company     = contact?.company_name || '';
  const channelLbl  = channel === 'linkedin' ? 'LinkedIn' : 'Email';
  const replyLabel  = String(options.replyLabel || '').trim();
  const inboundSubject = String(options.inboundSubject || '').trim();
  const inboundBody = String(options.inboundBody || '').trim();
  const quotePreview = String(options.quotePreview || inboundBody.slice(0, 220)).trim();
  const intent = String(options.intent || '').trim();
  const sentiment = String(options.sentiment || '').trim();

  const msg = [
    `💬 *Reply Queued — ${channelLbl}*`,
    ``,
    `To: *${name}*${company ? ` (${company})` : ''}`,
    `[${contextName}]`,
    inboundSubject ? `Subject: _${inboundSubject.substring(0, 140)}_` : null,
    intent || sentiment ? `Intent: ${intent || 'unknown'}${sentiment ? ` | Sentiment: ${sentiment}` : ''}` : null,
    replyLabel ? `Replying to: ${replyLabel}` : null,
    quotePreview ? `They said: _${quotePreview.substring(0, 360)}_` : null,
    ``,
    '```',
    String(replyBody || '').substring(0, 600),
    '```',
    ``,
    `_Approve = respect sending window. Send Now = bypass window._`,
  ].filter(Boolean).join('\n');

  try {
    const chatId = process.env.TELEGRAM_CHAT_ID;
    const sent   = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
    if (queueItemId) {
      const sb = getSupabase();
      await sb?.from('approval_queue').update({ telegram_msg_id: sent.message_id }).eq('id', queueItemId).then(null, () => {});
    }

    pendingApprovals.set(sent.message_id, {
      name,
      firm: company,
      isReply: true,
      queueId: queueItemId,
      replyApproval: {
        queueItemId,
        channel,
        replyToId,
        replyLabel,
        quotePreview,
        inboundSubject,
        inboundBody,
        intent,
        sentiment,
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
      await sb.from('approval_queue').update({ message_text: ra.replyBody }).eq('id', ra.queueItemId);
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

    clearPendingEditRequestForMsg(oldMsgId);
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
    approval.emailDraft.body = sanitizeApprovalText(revised);
    clearPendingEditRequestForMsg(oldMsgId);
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
    await bot.editMessageReplyMarkup(buildLinkedInDMKeyboard(newSent.message_id, approval.queueId || null), {
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
  clearPendingEditRequestForMsg(oldMsgId);
  pendingApprovals.delete(oldMsgId);
  const draft = approval.emailDraft;
  draft.body = sanitizeApprovalText(draft.body);
  draft.subject = sanitizeApprovalText(draft.subject);
  draft.alternativeSubject = sanitizeApprovalText(draft.alternativeSubject);
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
    ? buildLinkedInDMKeyboard(newSent.message_id, approval.queueId || null)
    : buildKeyboard(newSent.message_id, approval.queueId || null);
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
  if (!text) return;

  // Slash commands
  if (text.startsWith('/')) {
    await handleCommand(text, chatId);
    return;
  }

  // Route to approval handler if: waiting for edit input OR message is an
  // explicit approval command (STOP / APPROVE N A / SKIP N / EDIT N ...)
  const upper = text.toUpperCase().trim();
  const isApprovalCommand = pendingEditReqs.has(chatId)
    || upper === 'STOP'
    || upper === 'APPROVE A'
    || upper === 'APPROVE B'
    || upper === 'SKIP'
    || /^APPROVE\s+\d+/.test(upper)
    || /^SKIP\s+\d+/.test(upper)
    || /^EDIT\s+\d+/.test(upper);

  if (isApprovalCommand) {
    await handleApprovalResponse(text, chatId);
    return;
  }

  // Everything else → JARVIS
  await routeToJarvis(chatId, text);
}

async function routeToJarvis(chatId, text) {
  try {
    const { handleMessage: jarvisHandle } = await import('../core/jarvis.js');
    await bot.sendChatAction(chatId, 'typing').catch(() => {});
    const reply = await jarvisHandle(chatId, text);
    if (!reply) return;
    await bot.sendMessage(chatId, reply, { parse_mode: 'Markdown' }).catch(async () => {
      // Fallback: send without markdown if parse fails
      await bot.sendMessage(chatId, reply.replace(/[*_`[\]()~>#+=|{}.!-]/g, '\\$&')).catch(() => {});
    });
  } catch (err) {
    console.error('[JARVIS] Telegram handler error:', err.message);
    await bot.sendMessage(chatId, `Something went wrong — ${err.message.slice(0, 80)}. Try again.`).catch(() => {});
  }
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

  // prior_chat decisions — format: prior_chat:proceed:<approvalId> or prior_chat:skip:<approvalId>
  if (data.startsWith('prior_chat:')) {
    const parts     = data.split(':');
    const decision  = parts[1]; // 'proceed' or 'skip'
    const approvalId = parts[2];
    await handlePriorChatCallback(chatId, decision, approvalId);
    return;
  }

  const colon  = data.indexOf(':');
  if (colon === -1) return;
  const parts = data.split(':');
  const action = parts[0];
  const msgId  = Number(parts[1]);
  const queueId = parts[2] || null;
  if (!Number.isFinite(msgId)) {
    await bot.sendMessage(chatId, '⚠ This approval button is malformed. Open the queue from the dashboard or ask Roco to resend it.');
    return;
  }

  // Dedup guard — ignore duplicate button presses while one is in flight
  if (processingApprovals.has(msgId)) return;
  processingApprovals.add(msgId);

  let approval = pendingApprovals.get(msgId) || pendingApprovals.get(String(msgId));
  if (!approval) {
    approval = await reloadApprovalForTelegramMessage(msgId, queueId);
  }
  if (!approval) {
    processingApprovals.delete(msgId);
    await bot.sendMessage(chatId, '⚠ This draft is no longer in the queue — it may have already been handled.');
    return;
  }

  if (approval.__alreadyHandled) {
    processingApprovals.delete(msgId);
    const who = [approval.contactName, approval.firm].filter(Boolean).join(' @ ');
    if (approval.status === 'approved_waiting_for_window') {
      await bot.sendMessage(chatId, `✅ *Already approved* — ${who}\n\nEmail is queued and will send when the sending window opens.`, { parse_mode: 'Markdown' });
    } else if (approval.status === 'sent') {
      await bot.sendMessage(chatId, `✅ *Already sent* — ${who}`, { parse_mode: 'Markdown' });
    } else if (approval.status === 'telegram_skipped') {
      await bot.sendMessage(chatId, `⏭ *Already skipped* — ${who}`, { parse_mode: 'Markdown' });
    } else {
      await bot.sendMessage(chatId, `✅ *Already handled* (${approval.status}) — ${who}`, { parse_mode: 'Markdown' });
    }
    return;
  }

  if (action === 'aa' || action === 'ab') {
    const variant = action === 'aa' ? 'A' : 'B';
    const subject = variant === 'A' ? approval.emailDraft.subject : approval.emailDraft.alternativeSubject;
    resolveApproval(msgId, approval, 'approve', { variant, subject });
    processingApprovals.delete(msgId);
    await bot.sendMessage(chatId, `✅ Email approved for *${approval.name}* with Subject ${variant}. Roco will send it now if the window is open, otherwise it will wait for the next sending window.`, { parse_mode: 'Markdown' });

  } else if (action === 'sa') {
    // LinkedIn DM — single approve button
    resolveApproval(msgId, approval, 'approve', { variant: 'A', subject: approval.emailDraft.subject });
    processingApprovals.delete(msgId);
    const dmWindowHint = `Roco will send the DM during the LinkedIn DM window (8pm–11pm). If the window is already open, it will fire immediately.`;
    await bot.sendMessage(chatId, `✅ *LinkedIn DM approved* for *${approval.name}*\n\n${dmWindowHint}`, { parse_mode: 'Markdown' });

  } else if (action === 'lm') {
    const sb = getSupabase();
    if (sb && approval.contactId) {
      await sb.from('contacts').update({
        conversation_state: 'manual',
        follow_up_due_at: null,
        pending_linkedin_dm: false,
        updated_at: new Date().toISOString(),
      }).eq('id', approval.contactId);
    }
    if (sb && approval.queueId) {
      await sb.from('approval_queue').update({
        status: 'manual',
        resolved_at: new Date().toISOString(),
        edit_instructions: 'Manual takeover by Dom',
      }).eq('id', approval.queueId);
    }
    await clearTelegramApprovalControls(msgId);
    clearPendingEditRequestForMsg(msgId);
    pendingApprovals.delete(msgId);
    clearEditLoopCount(approval);
    processingApprovals.delete(msgId);
    try { approval.resolve?.({ action: 'skip' }); } catch {}
    await bot.sendMessage(chatId, `📝 *Manual* — *${approval.name}* will be ignored for this step. If they reply later, Roco can still pick that up.`, { parse_mode: 'Markdown' });

  } else if (action === 'lc') {
    const sb = getSupabase();
    if (sb && approval.contactId) {
      await sb.from('contacts').update({
        pipeline_stage: 'Inactive',
        conversation_state: 'do_not_contact',
        conversation_ended_at: new Date().toISOString(),
        conversation_ended_reason: 'Closed by Dom via Telegram',
        follow_up_due_at: null,
        pending_linkedin_dm: false,
        updated_at: new Date().toISOString(),
      }).eq('id', approval.contactId);
    }
    if (sb && approval.queueId) {
      await sb.from('approval_queue').update({
        status: 'closed',
        resolved_at: new Date().toISOString(),
        edit_instructions: 'Contact permanently closed by Dom',
      }).eq('id', approval.queueId);
    }
    await clearTelegramApprovalControls(msgId);
    clearPendingEditRequestForMsg(msgId);
    pendingApprovals.delete(msgId);
    clearEditLoopCount(approval);
    processingApprovals.delete(msgId);
    try { approval.resolve?.({ action: 'skip' }); } catch {}
    await bot.sendMessage(chatId, `🔚 *Closed* — *${approval.name}* will not be picked up by Roco again.`, { parse_mode: 'Markdown' });

  } else if (action === 'sk') {
    resolveApproval(msgId, approval, 'skip');
    processingApprovals.delete(msgId);
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
    processingApprovals.delete(msgId);
    await bot.sendMessage(chatId, `🚫 *${approval.name}* archived — Roco will not contact them again.`, { parse_mode: 'Markdown' });

  } else if (action === 'ed') {
    processingApprovals.delete(msgId);
    setPendingEditRequest(chatId, msgId);
    await bot.sendMessage(chatId,
      `✏ *Edit draft for ${approval.name}*\n\nType your instructions and I'll redraft:`,
      { parse_mode: 'Markdown' }
    );

  } else if (action === 'rq' || action === 'ra') {
    // Reply approve — either queue for window or bypass it immediately
    const ra = approval.replyApproval;
    if (!ra) { processingApprovals.delete(msgId); await bot.sendMessage(chatId, '⚠ Missing reply data.'); return; }
    try {
      const { sendApprovedReply } = await import('../dashboard/server.js');
      const sendNow = action === 'ra';
      const result = await sendApprovedReply({
        queueId: ra.queueItemId || null,
        queueItem: {
          id: ra.queueItemId || null,
          contact_id: ra.contactId || null,
          contact_name: approval.name,
          contact_email: ra.contactEmail || null,
          firm: approval.firm || null,
          body: ra.replyBody,
          edited_body: ra.replyBody,
          channel: ra.channel,
          message_type: ra.channel === 'linkedin' ? 'linkedin_reply' : 'email_reply',
          reply_to_id: ra.replyToId || null,
        },
        forceSend: sendNow,
        bodyOverride: ra.replyBody,
      });
      await clearTelegramApprovalControls(msgId);
      clearPendingEditRequestForMsg(msgId);
      pendingApprovals.delete(msgId);
      processingApprovals.delete(msgId);
      if (result?.deferred) {
        await bot.sendMessage(chatId, `✅ Reply approved for *${approval.name}* and queued for the next sending window${result?.nextOpen ? ` (${result.nextOpen})` : ''}.`, { parse_mode: 'Markdown' });
      } else {
        const ch = ra.channel === 'linkedin' ? 'LinkedIn' : 'Email';
        await bot.sendMessage(chatId, `✅ Reply sent to *${approval.name}* via ${ch}.`, { parse_mode: 'Markdown' });
      }
    } catch (err) {
      processingApprovals.delete(msgId);
      await bot.sendMessage(chatId, `⚠ Send failed: ${err.message}`);
    }

  } else if (action === 're') {
    // Reply edit — wait for instructions
    processingApprovals.delete(msgId);
    setPendingEditRequest(chatId, msgId);
    await bot.sendMessage(chatId,
      `✏ *Edit reply for ${approval.name}*\n\nType your edit instructions:`,
      { parse_mode: 'Markdown' }
    );

  } else if (action === 'rs') {
    // Reply skip
    const ra = approval.replyApproval;
    const sb = getSupabase();
    if (sb && ra?.queueItemId) {
      await sb.from('approval_queue').update({ status: 'skipped' }).eq('id', ra.queueItemId);
    }
    await clearTelegramApprovalControls(msgId);
    clearPendingEditRequestForMsg(msgId);
    pendingApprovals.delete(msgId);
    processingApprovals.delete(msgId);
    await bot.sendMessage(chatId, `✗ Reply to *${approval.name}* skipped.`, { parse_mode: 'Markdown' });

  } else if (action === 'rm') {
    // Manual takeover — dismiss the queued auto-reply but keep the contact live for future inbound tracking
    const ra = approval.replyApproval;
    const sb = getSupabase();
    if (sb && ra?.queueItemId) {
      await sb.from('approval_queue').update({
        status: 'manual',
        resolved_at: new Date().toISOString(),
        edit_instructions: 'Manual takeover by Dom',
      }).eq('id', ra.queueItemId);
    }
    await clearTelegramApprovalControls(msgId);
    clearPendingEditRequestForMsg(msgId);
    pendingApprovals.delete(msgId);
    processingApprovals.delete(msgId);
    await bot.sendMessage(chatId, `📝 *Manual mode* — auto-reply dismissed for *${approval.name}*. Future replies will still be tracked and re-queued.`, { parse_mode: 'Markdown' });

  } else if (action === 'rc') {
    // Manual close-out — end the conversation and stop further outreach
    const ra = approval.replyApproval;
    const sb = getSupabase();
    if (sb && ra?.contactId) {
      await sb.from('contacts').update({
        pipeline_stage: 'Inactive',
        conversation_state: 'conversation_ended_negative',
        conversation_ended_at: new Date().toISOString(),
        conversation_ended_reason: 'Manually closed by Dom',
        follow_up_due_at: null,
      }).eq('id', ra.contactId);

      await sb.from('activity_log').insert({
        contact_id: ra.contactId,
        event_type: 'CONVERSATION_CLOSED',
        summary: `Conversation manually closed for ${approval.name}`,
        detail: { channel: ra.channel, source: 'telegram_reply_approval' },
        created_at: new Date().toISOString(),
      }).then(() => {}, () => {});
    }
    if (sb && ra?.queueItemId) {
      await sb.from('approval_queue').update({
        status: 'closed',
        resolved_at: new Date().toISOString(),
        edit_instructions: 'Conversation manually closed by Dom',
      }).eq('id', ra.queueItemId);
    }
    await clearTelegramApprovalControls(msgId);
    clearPendingEditRequestForMsg(msgId);
    pendingApprovals.delete(msgId);
    processingApprovals.delete(msgId);
    await bot.sendMessage(chatId, `🔚 *Closed* — *${approval.name}* moved to Inactive and this conversation is finished.`, { parse_mode: 'Markdown' });
  }
}

// ─────────────────────────────────────────────
// SHARED RESOLVE HELPER
// ─────────────────────────────────────────────

function resolveApproval(msgId, approval, action, extra = {}) {
  clearTelegramApprovalControls(msgId).catch(() => {});
  clearPendingEditRequestForMsg(msgId);
  pendingApprovals.delete(msgId);
  clearEditLoopCount(approval);
  if (approval.queueId) {
    const status = action === 'approve' ? 'approved' : 'telegram_skipped';
    const subject = action === 'approve' ? (extra.subject || null) : null;
    // Track as recently resolved so /api/queue doesn't flash stale data while DB commits
    recentlyResolvedQueueIds.add(String(approval.queueId));
    setTimeout(() => recentlyResolvedQueueIds.delete(String(approval.queueId)), 12_000);
    updateApprovalStatus(approval.queueId, status, subject).catch(() => {});
  }
  if (action !== 'approve' && approval.isLinkedInDM && approval.contactId) {
    const sb = getSupabase();
    if (sb) {
      sb.from('contacts').update({
        pipeline_stage: 'Skipped',
        pending_linkedin_dm: false,
        follow_up_due_at: null,
        updated_at: new Date().toISOString(),
      }).eq('id', approval.contactId).then(null, () => {});
    }
  }
  import('../dashboard/server.js').then(({ pushActivity, notifyQueueUpdated }) => {
    pushActivity({
      type: 'APPROVAL',
      action: action === 'approve' ? 'Approved via Telegram' : action === 'edit' ? 'Edited via Telegram' : 'Skipped via Telegram',
      note: approval?.name && approval?.firm ? `${approval.name} @ ${approval.firm}` : (approval?.name || approval?.firm || ''),
      dealId: approval?.dealId || null,
    });
    notifyQueueUpdated();
  }).catch(() => {});
  if (action === 'approve') {
    const sb = getSupabase();
    const contactId = approval.contactId || approval.contactPage?.id || null;
    const approvedStage = approval.isLinkedInDM ? 'DM Approved' : 'Email Approved';
    if (sb && contactId) {
      sb.from('contacts').update({
        pipeline_stage: approvedStage,
        updated_at: new Date().toISOString(),
      }).eq('id', contactId);
    }
    approval.resolve({ action: 'approve', queueId: approval.queueId || null, variant: extra.variant, subject: extra.subject, body: extra.body || approval.emailDraft?.body });
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
  const editMsgId = await getPendingEditRequest(chatId);
  if (editMsgId !== undefined) {
    clearPendingEditRequest(chatId);
    let approval = pendingApprovals.get(editMsgId);
    if (!approval && editMsgId !== undefined) {
      approval = await reloadApprovalForTelegramMessage(editMsgId);
    }
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
      const newBody = sanitizeApprovalText(text.slice(5));
      if (newBody) {
        approval.emailDraft.body = newBody;
        await resendUpdatedApproval(chatId, editMsgId, approval);
        return;
      }
    }
    // Direct subject edit — "SUBJECT: <text>" replaces subject without AI redraft (emails only)
    if (text.toUpperCase().startsWith('SUBJECT:') && !approval.isLinkedInDM) {
      const newSubject = sanitizeApprovalText(text.slice(8));
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
    const loops = (await getEditLoopCount(approval)) + 1;
    if (loops > 3) {
      await bot.sendMessage(chatId, `Max edits reached for ${approval.name}. Skipping.`);
      resolveApproval(editMsgId, approval, 'skip');
      return;
    }
    setEditLoopCount(approval, loops);
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
    await bot.sendMessage(chatId, `✅ Email approved for *${approval.name}* with Subject ${variant}. Roco will send it now if the window is open, otherwise it will wait for the next sending window.`, { parse_mode: 'Markdown' });
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
    const loops = (await getEditLoopCount(approval)) + 1;
    if (loops > 3) {
      await bot.sendMessage(chatId, `Max edits reached for ${approval.name}. Skipping.`);
      resolveApproval(msgId, approval, 'skip');
      return;
    }
    setEditLoopCount(approval, loops);
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
    await bot.sendMessage(chatId, `✅ Email approved for *${approval.name}* with Subject ${variant}. Roco will send it now if the window is open, otherwise it will wait for the next sending window.`, { parse_mode: 'Markdown' });
    return;
  }

  if (upper === 'SKIP') {
    resolveApproval(lastMsgId, approval, 'skip');
    await bot.sendMessage(chatId, `✗ Skipped — draft for *${approval.name}* deleted.`, { parse_mode: 'Markdown' });
    return;
  }

  if (upper.startsWith('EDIT ')) {
    const instructions = text.slice(5).trim();
    const loops = (await getEditLoopCount(approval)) + 1;
    if (loops > 3) {
      await bot.sendMessage(chatId, `Max edits reached for ${approval.name}. Skipping.`);
      resolveApproval(lastMsgId, approval, 'skip');
      return;
    }
    setEditLoopCount(approval, loops);
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
      await handleStatus(chatId, args);
      break;
    case '/help':
      await handleHelp(chatId);
      break;
    case '/pause':
      await handlePause(chatId, args);
      break;
    case '/resume':
      await handleResume(chatId, args);
      break;
    case '/pipeline':
      await handlePipeline(chatId, args);
      break;
    case '/campaignstatus':
      await handleCampaignStatus(chatId, args);
      break;
    case '/emails':
      await handleOutboundMessages(chatId, 'email', args);
      break;
    case '/linkedindms':
      await handleOutboundMessages(chatId, 'linkedin_dm', args);
      break;
    case '/queue':
      await handleQueue(chatId, args);
      break;
    case '/newdeal':
      await handleNewDeal(chatId, args);
      break;
    case '/stop':
      await handlePause(chatId, 'all');
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
    case '/close':
      await handleCloseDeal(chatId, args);
      break;
    default:
      await bot.sendMessage(chatId,
        'Commands available:\n\n' +
        '/help — How to target a specific deal\n' +
        '/status [deal] — Roco status and deal stats\n' +
        '/pause [deal|all] — Pause a deal or all outreach\n' +
        '/resume [deal|all] — Resume a deal or all outreach\n' +
        '/pipeline [deal] — Top prospects for a deal\n' +
        '/campaignstatus [deal] — Current outreach by deal\n' +
        '/emails [deal] — Recent sent emails\n' +
        '/linkedindms [deal] — Recent sent LinkedIn DMs\n' +
        '/queue [deal] — Approval queue for a deal\n' +
        '/close [deal] — Close a deal\n' +
        '/newdeal [name] — Start a new deal\n' +
        '/sourcing — Sourcing campaigns status\n' +
        '/pausecampaigns — Pause all sourcing campaigns\n' +
        '/resumecampaigns — Resume all sourcing campaigns\n' +
        '/stop — Emergency stop\n\n' +
        buildCommandGuide(),
        { parse_mode: 'Markdown' }
      );
  }
}

async function handleHelp(chatId) {
  await bot.sendMessage(chatId, buildCommandGuide(), { parse_mode: 'Markdown' });
}

async function handleStatus(chatId, args = '') {
  try {
    const state = await loadSessionState();
    const deals = await getAllDeals().catch(() => []);
    const sb = getSupabase();

    let emailsSent = 0;
    let queueCount = 0;
    if (sb) {
      const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
      let sent = 0;
      try {
        ({ count: sent } = await sb.from('emails').select('id', { count: 'exact', head: true })
          .eq('status', 'sent').gte('sent_at', since));
      } catch {}
      emailsSent = sent || 0;

      let q = 0;
      try {
        ({ count: q } = await sb.from('emails').select('id', { count: 'exact', head: true })
          .eq('status', 'pending_approval'));
      } catch {}
      queueCount = q || pendingApprovals.size;
    } else {
      queueCount = pendingApprovals.size;
    }

    const isActive = (state.rocoStatus === 'ACTIVE') || (rocoState?.status === 'ACTIVE');

    const input = String(args || '').trim();
    if (input) {
      const deal = await resolveRequiredDeal(chatId, input, {
        statuses: ['ACTIVE', 'PAUSED', 'CLOSED'],
        actionLabel: 'view',
        commandExample: '/status [deal name]',
      });
      if (!deal) return;

      let prospectCount = 0;
      let contactedCount = 0;
      let repliedCount = 0;
      if (sb) {
        let prospects = 0;
        try {
          ({ count: prospects } = await sb.from('contacts').select('id', { count: 'exact', head: true })
            .eq('deal_id', deal.id)
            .not('pipeline_stage', 'in', '("Inactive","Archived","archived")'));
        } catch {}
        prospectCount = prospects || 0;

        let contactedRows = [];
        try {
          ({ data: contactedRows } = await sb.from('contacts')
            .select('id, invite_sent_at, invite_accepted_at, last_email_sent_at, last_outreach_at, pipeline_stage')
            .eq('deal_id', deal.id)
            .limit(1000));
        } catch {}
        const contacted = (contactedRows || []).filter(contact =>
          contact.invite_sent_at || contact.invite_accepted_at || contact.last_email_sent_at || contact.last_outreach_at ||
          ['DM Approved', 'Email Approved', 'DM Sent', 'Email Sent', 'In Conversation', 'Meeting Booked', 'invite_sent', 'invite_accepted'].includes(contact.pipeline_stage)
        );
        contactedCount = contacted.length;

        let replied = 0;
        try {
          ({ count: replied } = await sb.from('contacts').select('id', { count: 'exact', head: true })
            .eq('deal_id', deal.id)
            .or('response_received.eq.true,pipeline_stage.eq.In Conversation,pipeline_stage.eq.Meeting Booked'));
        } catch {}
        repliedCount = replied || 0;
      }

      await bot.sendMessage(chatId,
        `*DEAL STATUS — ${deal.name}*\n\n` +
        `System: ${isActive ? '🟢 ACTIVE' : '🔴 PAUSED'}\n` +
        `Deal: ${deal.status}${deal.paused ? ' (paused)' : ''}\n` +
        `Progress: ${formatCurrencyAmount(deal.committed_amount, deal.currency)} / ${formatCurrencyAmount(deal.target_amount, deal.currency)}\n` +
        `Prospects: ${prospectCount}\n` +
        `Contacted: ${contactedCount}\n` +
        `Replies: ${repliedCount}\n` +
        `Sector: ${deal.sector || '—'}`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    const dealLines = deals
      ?.filter(d => ['ACTIVE', 'PAUSED'].includes(d.status))
      .map(d =>
        `• ${d.name} [${d.status}] — ${formatCurrencyAmount(d.committed_amount, d.currency)} / ${formatCurrencyAmount(d.target_amount, d.currency)}`
    ).join('\n') || '• No active deals';

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

async function handlePause(chatId, args = '') {
  const input = String(args || '').trim();
  if (input && input.toLowerCase() !== 'all') {
    try {
      const { pool, deal, matches } = await resolveDealArgument(input, ['ACTIVE']);
      if (!deal) {
        const choices = matches.length ? matches : pool;
        await bot.sendMessage(chatId,
          `Specify which active deal to pause.\n\nUse: /pause [deal name]\n\n${formatDealChoices(choices.slice(0, 12))}`
        );
        return;
      }

      const updated = await updateDeal(deal.id, {
        status: 'PAUSED',
        paused: true,
        paused_at: new Date().toISOString(),
      });
      await sbLogActivity({ dealId: updated.id, eventType: 'DEAL_PAUSED', summary: `Deal "${updated.name}" paused from Telegram` });
      await bot.sendMessage(chatId, `Deal paused: ${updated.name}\nAll outreach for this deal is now paused in the database.`);
      return;
    } catch (err) {
      await bot.sendMessage(chatId, `Could not pause deal: ${err.message}`);
      return;
    }
  }

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

async function handleResume(chatId, args = '') {
  const input = String(args || '').trim();
  if (input && input.toLowerCase() !== 'all') {
    try {
      const { pool, deal, matches } = await resolveDealArgument(input, ['PAUSED', 'ACTIVE']);
      if (!deal) {
        const choices = matches.length ? matches : pool.filter(d => ['PAUSED', 'ACTIVE'].includes(d.status));
        await bot.sendMessage(chatId,
          `Specify which deal to resume.\n\nUse: /resume [deal name]\n\n${formatDealChoices(choices.slice(0, 12))}`
        );
        return;
      }

      const updated = await updateDeal(deal.id, {
        status: 'ACTIVE',
        paused: false,
        paused_at: null,
      });
      await sbLogActivity({ dealId: updated.id, eventType: 'DEAL_RESUMED', summary: `Deal "${updated.name}" resumed from Telegram` });
      await bot.sendMessage(chatId, `Deal resumed: ${updated.name}\nRoco will pick it up again on the next cycle.`);
      return;
    } catch (err) {
      await bot.sendMessage(chatId, `Could not resume deal: ${err.message}`);
      return;
    }
  }

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

async function handlePipeline(chatId, args = '') {
  try {
    const sb = getSupabase();
    const deal = await resolveRequiredDeal(chatId, args, {
      statuses: ['ACTIVE', 'PAUSED'],
      actionLabel: 'inspect in pipeline',
      commandExample: '/pipeline [deal name]',
    });
    if (!deal) return;

    const { data: contacts, error: dbErr } = await sb.from('contacts')
      .select('name, company_name, investor_score, pipeline_stage, tier')
      .eq('deal_id', deal.id)
      .not('pipeline_stage', 'in', '("Inactive","Archived","archived")')
      .order('investor_score', { ascending: false })
      .limit(10);

    if (dbErr) throw dbErr;
    if (!contacts?.length) {
      await bot.sendMessage(chatId, 'No active prospects in pipeline yet.');
      return;
    }
    const lines = contacts.map((c, i) =>
      `${i + 1}. *${c.name || 'Unknown'}* (${c.company_name || 'Unknown'})\n` +
      `   Score: ${c.investor_score ?? '—'} | Stage: ${c.pipeline_stage || '—'} | Tier: ${c.tier || '—'}`
    ).join('\n\n');
    await bot.sendMessage(chatId, `*TOP PIPELINE PROSPECTS — ${deal.name}*\n\n${lines}`, { parse_mode: 'Markdown' });
  } catch (err) {
    error('handlePipeline failed', { err: err.message });
    await bot.sendMessage(chatId, 'Could not fetch pipeline.');
  }
}

async function handleCampaignStatus(chatId, args) {
  try {
    const sb = getSupabase();
    if (!sb) {
      await bot.sendMessage(chatId, 'Database unavailable.');
      return;
    }

    const deal = await resolveRequiredDeal(chatId, args, {
      statuses: ['ACTIVE', 'PAUSED'],
      actionLabel: 'inspect for campaign status',
      commandExample: '/campaignstatus [deal name]',
    });
    if (!deal) return;

    const { data: contacts } = await sb.from('contacts')
      .select('deal_id, name, company_name, pipeline_stage, invite_sent_at, invite_accepted_at, last_email_sent_at, last_outreach_at, updated_at')
      .eq('deal_id', deal.id)
      .order('updated_at', { ascending: false })
      .limit(250);

    const activeContacts = (contacts || []).filter(contact => !['Inactive', 'Archived', 'archived'].includes(contact.pipeline_stage));
    const contacted = activeContacts.filter(contact =>
      contact.invite_sent_at || contact.invite_accepted_at || contact.last_email_sent_at || contact.last_outreach_at ||
      ['DM Approved', 'Email Approved', 'DM Sent', 'Email Sent', 'In Conversation', 'Meeting Booked', 'invite_sent', 'invite_accepted'].includes(contact.pipeline_stage)
    );
    const firms = new Set(contacted.map(contact => String(contact.company_name || '').trim()).filter(Boolean));
    const recent = contacted.slice(0, 6).map(contact =>
      `${contact.name || 'Unknown'} @ ${contact.company_name || 'Unknown'} — ${contact.pipeline_stage || '—'}`
    );

    await bot.sendMessage(chatId,
      `${deal.name} [${deal.status}]\n` +
      `Prospects in play: ${activeContacts.length} | Firms in outreach: ${firms.size} | Contacted: ${contacted.length}\n` +
      `${recent.length ? `Recent: ${recent.join(' | ')}` : 'Recent: none yet'}`
    );
  } catch (err) {
    error('handleCampaignStatus failed', { err: err.message });
    await bot.sendMessage(chatId, `Could not fetch campaign status: ${err.message}`);
  }
}

async function handleOutboundMessages(chatId, channel, args) {
  try {
    const sb = getSupabase();
    if (!sb) {
      await bot.sendMessage(chatId, 'Database unavailable.');
      return;
    }

    const deal = await resolveRequiredDeal(chatId, args, {
      statuses: ['ACTIVE', 'PAUSED', 'CLOSED'],
      actionLabel: `view ${channel === 'email' ? 'emails' : 'LinkedIn DMs'} for`,
      commandExample: `${channel === 'email' ? '/emails' : '/linkedindms'} [deal name]`,
    });
    if (!deal) return;

    let query = sb.from('conversation_messages')
      .select('contact_id, deal_id, subject, body, created_at')
      .eq('direction', 'outbound')
      .eq('channel', channel)
      .order('created_at', { ascending: false })
      .limit(12);
    query = query.eq('deal_id', deal.id);

    const { data: messages, error: msgErr } = await query;
    if (msgErr) throw new Error(msgErr.message);
    if (!messages?.length) {
      await bot.sendMessage(chatId, channel === 'email' ? 'No sent emails found.' : 'No sent LinkedIn DMs found.');
      return;
    }

    const contactIds = [...new Set(messages.map(message => message.contact_id).filter(Boolean))];
    const { data: contacts } = contactIds.length
      ? await sb.from('contacts').select('id, name, company_name').in('id', contactIds)
      : { data: [] };
    const contactMap = Object.fromEntries((contacts || []).map(contact => [String(contact.id), contact]));

    const title = `${channel === 'email' ? 'Recent Sent Emails' : 'Recent Sent LinkedIn DMs'} — ${deal.name}`;
    const lines = messages.map((message, index) => {
      const contact = contactMap[String(message.contact_id)] || {};
      const sentAt = new Date(message.created_at).toLocaleString('en-GB', { timeZone: 'UTC' });
      const subject = channel === 'email' ? `\nSubject: ${truncatePreview(message.subject || '—', 90)}` : '';
      return `${index + 1}. ${contact.name || 'Unknown'} @ ${contact.company_name || 'Unknown'}\nSent: ${sentAt} UTC${subject}\n${truncatePreview(message.body, 240)}`;
    });

    await bot.sendMessage(chatId, `${title}\n\n${lines.join('\n\n')}`);
  } catch (err) {
    error('handleOutboundMessages failed', { err: err.message, channel });
    await bot.sendMessage(chatId, `Could not fetch ${channel === 'email' ? 'emails' : 'LinkedIn DMs'}: ${err.message}`);
  }
}

async function handleQueue(chatId, args = '') {
  if (pendingApprovals.size === 0) {
    await bot.sendMessage(chatId, 'No emails waiting for approval. ✅');
    return;
  }

  let entries = [...pendingApprovals.entries()];
  const input = String(args || '').trim();
  if (input) {
    const deal = await resolveRequiredDeal(chatId, input, {
      statuses: ['ACTIVE', 'PAUSED', 'CLOSED'],
      actionLabel: 'view the queue for',
      commandExample: '/queue [deal name]',
    });
    if (!deal) return;
    entries = entries.filter(([, approval]) => String(approval.dealId || '') === String(deal.id));
    if (!entries.length) {
      await bot.sendMessage(chatId, `No approval items waiting for ${deal.name}.`);
      return;
    }
  }

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
      reply_markup: buildKeyboard(msgId, a.queueId || null),
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

async function handleCloseDeal(chatId, args) {
  try {
    const input = String(args || '').trim();
    const { pool, deal, matches } = await resolveDealArgument(input, ['ACTIVE', 'PAUSED']);
    if (!deal) {
      const choices = matches.length ? matches : pool;
      await bot.sendMessage(chatId,
        `Specify which deal to close.\n\nUse: /close [deal name]\n\n${formatDealChoices(choices.slice(0, 12))}`
      );
      return;
    }

    const closedAt = new Date().toISOString();
    const updated = await updateDeal(deal.id, {
      status: 'CLOSED',
      paused: false,
      closed_at: closedAt,
      archived_at: closedAt,
      archived_reason: 'closed',
    });

    const sb = getSupabase();
    if (sb) {
      await sb.from('contacts').update({ pipeline_stage: 'Inactive' }).eq('deal_id', deal.id);
      let dealContacts = [];
      try {
        ({ data: dealContacts } = await sb.from('contacts').select('id').eq('deal_id', deal.id));
      } catch {}
      const contactIds = (dealContacts || []).map(contact => contact.id).filter(Boolean);
      if (contactIds.length) {
        await sb.from('approval_queue').delete().in('contact_id', contactIds).eq('status', 'pending');
      }
    }

    clearApprovalsForDeal(deal.id);
    await sbLogActivity({ dealId: updated.id, eventType: 'DEAL_CLOSED', summary: `Deal "${updated.name}" closed from Telegram` });
    await bot.sendMessage(chatId, `Deal closed: ${updated.name}\nAll outreach has been stopped and the deal is now closed in the database.`);
  } catch (err) {
    error('handleCloseDeal failed', { err: err.message });
    await bot.sendMessage(chatId, `Could not close deal: ${err.message}`);
  }
}

function registerCommands() {
  if (!bot) return;
  bot.setMyCommands([
    { command: 'help', description: 'How to target a specific deal' },
    { command: 'status', description: 'Roco status and stats' },
    { command: 'pipeline', description: 'Top 10 active prospects' },
    { command: 'campaignstatus', description: 'Current outreach by deal' },
    { command: 'emails', description: 'Recent sent emails' },
    { command: 'linkedindms', description: 'Recent sent LinkedIn DMs' },
    { command: 'queue', description: 'Emails waiting for approval' },
    { command: 'pause', description: 'Pause a deal or all outreach' },
    { command: 'resume', description: 'Resume a deal or all outreach' },
    { command: 'close', description: 'Close a deal' },
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
  const looksLikeEmail = (value) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || '').trim());
  return [...pendingApprovals.entries()].flatMap(([id, a]) => {
    const isLinkedIn = a.isLinkedInDM || isLinkedInStageLabel(a.stage);
    const contactEmail = a.contactPage?.properties?.Email?.email
      || a.contactPage?.email
      || a.contactEmail
      || null;
    if (!isLinkedIn && !looksLikeEmail(contactEmail)) return [];
    return [{
      id: a.queueId || id,
      telegramMsgId: id,
      queueId: a.queueId || null,
      name: a.name,
      firm: a.firm,
      score: a.score,
      stage: a.stage,
      channel: isLinkedIn ? 'linkedin' : 'email',
      message_type: a.isReply ? (isLinkedIn ? 'linkedin_reply' : 'email_reply') : null,
      isReply: !!a.isReply,
      subject: a.emailDraft?.subject,
      alternativeSubject: a.emailDraft?.alternativeSubject,
      body: a.emailDraft?.body,
      contactPageId: a.contactPage?.id,
      contactEmail,
      queuedAt: a.queuedAt || new Date().toISOString(),
    }];
  });
}

export async function dismissPendingApproval(id) {
  const numId = Number(id);
  let matchedKey = pendingApprovals.has(numId) ? numId : (pendingApprovals.has(String(id)) ? String(id) : null);

  if (matchedKey == null) {
    for (const [key, approval] of pendingApprovals.entries()) {
      if (String(approval.queueId || '') === String(id)) {
        matchedKey = key;
        break;
      }
    }
  }

  if (matchedKey == null) return null;
  const approval = pendingApprovals.get(matchedKey);
  await clearTelegramApprovalControls(matchedKey).catch(() => {});
  clearPendingEditRequestForMsg(matchedKey);
  pendingApprovals.delete(matchedKey);
  clearEditLoopCount(approval);
  return { key: matchedKey, approval };
}

export async function updateApprovalDraftFromDashboard(id, { body = null, subject = null } = {}) {
  const numId = Number(id);
  // Search by Telegram msgId (primary key) first, then fall back to searching by queueId
  let approval = pendingApprovals.get(numId) || pendingApprovals.get(String(id));
  if (!approval) {
    for (const [, a] of pendingApprovals) {
      if (String(a.queueId || '') === String(id)) { approval = a; break; }
    }
  }
  if (!approval) return false;

  if (body !== null) approval.emailDraft.body = sanitizeApprovalText(body);
  if (subject !== null && !approval.isLinkedInDM) approval.emailDraft.subject = sanitizeApprovalText(subject);
  return true;
}

async function executeReloadedApproval(item, decision) {
  const sb = getSupabase();
  if (!sb) throw new Error('Supabase unavailable');

  if (decision.action === 'skip') {
    await sb.from('approval_queue').update({
      status: 'skipped',
      resolved_at: new Date().toISOString(),
    }).eq('id', item.id);
    return;
  }

  if (decision.action === 'edit') {
    const instructions = decision.instructions || '';
    const originalBody = item.edited_body || item.body || '';

    // Direct SUBJECT: override — no AI redraft needed
    if (instructions.toUpperCase().startsWith('SUBJECT:') && !isLinkedInStageLabel(item.stage)) {
      const newSubject = instructions.slice(8).trim();
      await sb.from('approval_queue').update({ approved_subject: newSubject }).eq('id', item.id).then(null, () => {});
      await bot.sendMessage(
        process.env.TELEGRAM_CHAT_ID,
        `✅ Subject updated to: "${newSubject}"`,
      ).catch(() => {});
      const { notifyQueueUpdated } = await import('../dashboard/server.js');
      notifyQueueUpdated();
      return;
    }

    // Direct BODY: override — skip AI redraft
    let newBody = originalBody;
    if (instructions.toUpperCase().startsWith('BODY:')) {
      newBody = instructions.slice(5).trim();
    } else {
      // AI redraft
      try {
        const { orComplete } = await import('../core/openRouterClient.js');
        const prompt = `You are editing a fundraising outreach email on behalf of Dom.

ORIGINAL EMAIL:
${originalBody}

EDIT INSTRUCTIONS:
${instructions}

Return ONLY the revised email body. No subject line. No labels. No explanation. Sign off as: Dom`;
        const draft = await orComplete(prompt, { tier: 'conversation', maxTokens: 600 });
        if (draft?.trim()) newBody = draft.trim();
      } catch (err) {
        console.warn('[TELEGRAM EDIT] AI redraft failed:', err.message);
      }
    }

    // Save updated body to DB
    await sb.from('approval_queue').update({
      edited_body: newBody,
      edit_instructions: instructions,
    }).eq('id', item.id).then(null, () => {});

    // Send a new Telegram message with the redrafted email and re-approval buttons
    const subject = item.approved_subject || item.subject_a || item.subject || '';
    const altSubject = item.subject_b || null;
    const contactName = item.contact_name || 'contact';
    const preview = [
      `✏ *Redrafted for ${contactName}*`,
      subject ? `Subject: _${subject}_` : null,
      '',
      newBody.length > 800 ? `${newBody.slice(0, 800)}…` : newBody,
    ].filter(s => s !== null).join('\n');

    const inlineKeyboard = [[
      { text: altSubject ? '✅ Approve A' : '✅ Approve', callback_data: `aa:${item.telegram_msg_id || 0}:${item.id}` },
      ...(altSubject ? [{ text: '✅ Approve B', callback_data: `ab:${item.telegram_msg_id || 0}:${item.id}` }] : []),
      { text: '✏ Edit again', callback_data: `ed:${item.telegram_msg_id || 0}:${item.id}` },
      { text: '✗ Skip', callback_data: `sk:${item.telegram_msg_id || 0}:${item.id}` },
    ]];

    try {
      const sent = await bot.sendMessage(
        process.env.TELEGRAM_CHAT_ID,
        preview,
        {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: inlineKeyboard },
        },
      );
      // Register the new message in pendingApprovals so buttons work
      if (sent?.message_id) {
        const updatedItem = { ...item, edited_body: newBody, telegram_msg_id: sent.message_id };
        const entry = buildReloadedApprovalEntry(updatedItem);
        pendingApprovals.set(sent.message_id, entry);
        pendingApprovals.set(String(sent.message_id), entry);
        // Update DB with new telegram_msg_id
        await sb.from('approval_queue').update({ telegram_msg_id: sent.message_id }).eq('id', item.id).then(null, () => {});
      }
    } catch (err) {
      console.warn('[TELEGRAM EDIT] Failed to send redrafted message:', err.message);
    }

    const { notifyQueueUpdated } = await import('../dashboard/server.js');
    notifyQueueUpdated();
    return;
  }

  if (isLinkedInStageLabel(item.stage) && item.contact_id) {
    const { sendApprovedLinkedInDM } = await import('../dashboard/server.js');
    const { notifyQueueUpdated } = await import('../dashboard/server.js');
    const text = decision.body || item.body || '';
    await sb.from('contacts').update({
      pipeline_stage: 'DM Approved',
      updated_at: new Date().toISOString(),
    }).eq('id', item.contact_id);
    const dmResult = await sendApprovedLinkedInDM({
      contactId: item.contact_id,
      text,
      queueId: item.id,
      queueItem: item,
    });
    if (dmResult?.deferred) {
      await sb.from('approval_queue').update({
        status: 'approved_waiting_for_window',
        edited_body: text || null,
        resolved_at: new Date().toISOString(),
      }).eq('id', item.id);
      notifyQueueUpdated();
      return;
    }

    notifyQueueUpdated();
    return;
  }

  const { sendEmail } = await import('../integrations/unipileClient.js');
  const { pushActivity, notifyQueueUpdated } = await import('../dashboard/server.js');
  let toEmail = item.contact_email || null;
  let dealId = null;
  if (item.contact_id) {
    try {
      const { data: contact } = await sb.from('contacts').select('email, deal_id').eq('id', item.contact_id).single();
      toEmail = toEmail || contact?.email || null;
      dealId = contact?.deal_id || null;
    } catch {}
  }
  if (!toEmail) throw new Error('No email address found for queued approval');

  const approvedSubject = decision.subject || item.subject_a || item.subject || '';
  const bodyToSend = decision.body || item.body || '';
  let deal = null;
  try {
    if (dealId) {
      const { getDeal } = await import('../core/supabaseSync.js');
      deal = await getDeal(dealId);
    }
  } catch {}

  if (item.contact_id) {
    await sb.from('contacts').update({
      pipeline_stage: 'Email Approved',
      updated_at: new Date().toISOString(),
    }).eq('id', item.contact_id);
  }

  try {
    const { isWithinChannelWindow } = await import('../core/scheduleChecker.js');
    if (deal && !isWithinChannelWindow(deal, 'email')) {
      await sb.from('approval_queue').update({
        status: 'approved_waiting_for_window',
        approved_subject: approvedSubject || null,
        edited_body: bodyToSend || null,
        resolved_at: new Date().toISOString(),
      }).eq('id', item.id);
      notifyQueueUpdated();
      return;
    }
  } catch {}

  const sendResult = await sendEmail({
    to: toEmail,
    toName: item.contact_name || '',
    subject: approvedSubject,
    body: bodyToSend,
    trackingLabel: `deal:${dealId || 'none'}|contact:${item.contact_id || 'none'}|stage:${String(item.stage || 'email').toLowerCase().replace(/\s+/g, '_')}`,
  });

  await sb.from('approval_queue').update({
    status: 'sent',
    sent_at: new Date().toISOString(),
    approved_subject: approvedSubject || null,
  }).eq('id', item.id);

  if (item.contact_id) {
    await sb.from('contacts').update({
      pipeline_stage: 'Email Sent',
      last_email_sent_at: new Date().toISOString(),
      outreach_channel: 'email',
      last_outreach_at: new Date().toISOString(),
    }).eq('id', item.contact_id);

    try {
      await sb.from('conversation_messages').insert({
        contact_id: item.contact_id,
        deal_id: dealId,
        direction: 'outbound',
        channel: 'email',
        body: bodyToSend,
        subject: approvedSubject,
        sent_at: new Date().toISOString(),
      });
    } catch {}
  }

  const emailSentAt = new Date().toISOString();
  const emailSentActivityKey = sendResult?.emailId
    ? `email_sent:${sendResult.emailId}`
    : `email_sent:${item.contact_id || item.contact_email || item.contact_name || 'unknown'}:${emailSentAt}`;
  if (dealId) {
    try {
      await sb.from('activity_log').insert({
        deal_id: dealId,
        event_type: 'EMAIL_SENT',
        summary: `Email sent: ${item.contact_name || 'contact'}`,
        detail: {
          activity_key: emailSentActivityKey,
          message: [item.firm || '', approvedSubject || ''].filter(Boolean).join(' · '),
          channel: 'email',
          account_id: sendResult?.accountId || null,
          provider_id: sendResult?.providerId || null,
          message_id: sendResult?.emailId || null,
          thread_id: sendResult?.threadId || null,
          to: toEmail,
        },
        created_at: emailSentAt,
      });
    } catch {}
  }

  pushActivity({
    type: 'email',
    activity_key: emailSentActivityKey,
    action: `Email sent: ${item.contact_name || ''}`,
    note: `${item.firm || ''} · ${approvedSubject}`,
    persist: false,
  });
  await sendTelegram(
    `✅ *Email sent* → *${item.contact_name || 'contact'}* (${item.firm || 'unknown firm'})${approvedSubject ? `\nSubject: _${sanitizeApprovalText(approvedSubject)}_` : ''}`
  ).catch(() => {});
  notifyQueueUpdated();
}

function buildReloadedApprovalEntry(item) {
  const messageType = String(item.message_type || '').toLowerCase();
  const channel = String(item.channel || (messageType.includes('linkedin') ? 'linkedin' : 'email')).toLowerCase();
  if (messageType === 'email_reply' || messageType === 'linkedin_reply' || /reply/i.test(item.stage || '')) {
    const body = item.edited_body || item.message_text || item.body || '';
    return {
      name: item.contact_name || 'Unknown',
      firm: item.firm || '',
      isReply: true,
      queueId: item.id,
      replyApproval: {
        queueItemId: item.id,
        channel,
        replyToId: item.reply_to_id || null,
        contactId: item.contact_id || null,
        contactEmail: item.contact_email || null,
        replyBody: body,
        subject: item.subject_a || item.subject || null,
      },
      replayed: true,
    };
  }
  return {
    name: item.contact_name || 'Unknown',
    firm: item.firm || '',
    stage: item.stage || 'EMAIL',
    isLinkedInDM: isLinkedInStageLabel(item.stage),
    contactId: item.contact_id || null,
    emailDraft: {
      subject: item.subject_a || item.subject || null,
      alternativeSubject: item.subject_b || null,
      body: item.edited_body || item.body || '',
    },
    queuedAt: new Date().toISOString(),
    queueId: item.id,
    replayed: true,
    resolve: async (decision) => executeReloadedApproval(item, decision),
  };
}

async function reloadApprovalForTelegramMessage(msgId, queueId = null) {
  const sb = getSupabase();
  if (!sb || (!msgId && !queueId)) return null;
  try {
    let item = null;
    if (queueId) {
      const byQueue = await sb.from('approval_queue')
        .select('id, contact_id, contact_name, contact_email, firm, stage, subject_a, subject_b, subject, body, edited_body, message_text, message_type, channel, reply_to_id, outreach_mode, telegram_msg_id, status')
        .eq('id', queueId)
        .maybeSingle();
      if (byQueue.error) throw byQueue.error;
      item = byQueue.data || null;
    }
    if (!item && msgId) {
      const byTelegram = await sb.from('approval_queue')
        .select('id, contact_id, contact_name, contact_email, firm, stage, subject_a, subject_b, subject, body, edited_body, message_text, message_type, channel, reply_to_id, outreach_mode, telegram_msg_id, status')
        .eq('telegram_msg_id', msgId)
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (byTelegram.error) throw byTelegram.error;
      item = byTelegram.data || null;
    }
    if (!item || item.outreach_mode === 'company_sourcing') return null;
    // Already handled by another instance (Railway/VPS both running) or window-queued
    if (['approved', 'approved_waiting_for_window', 'sending', 'sent', 'telegram_skipped', 'skipped', 'manual', 'failed'].includes(item.status)) {
      return { __alreadyHandled: true, status: item.status, contactName: item.contact_name, firm: item.firm };
    }
    if (item.status !== 'pending') return null;
    if (msgId && !item.telegram_msg_id) {
      await sb.from('approval_queue').update({ telegram_msg_id: msgId }).eq('id', item.id).then(null, () => {});
      item.telegram_msg_id = msgId;
    }
    const entry = buildReloadedApprovalEntry(item);
    pendingApprovals.set(msgId, entry);
    pendingApprovals.set(String(msgId), entry);
    return entry;
  } catch (err) {
    error('Failed to reload Telegram approval', { err: err.message, msgId, queueId });
    return null;
  }
}

export async function reloadPendingInvestorApprovals() {
  const { getSupabase } = await import('../core/supabase.js');
  const sb = getSupabase();
  if (!sb || !bot) return;

  const existingQueueIds = new Set(
    [...pendingApprovals.values()].map(entry => String(entry.queueId || '')).filter(Boolean)
  );

  const { data: pending } = await sb.from('approval_queue')
    .select('id, contact_id, contact_name, contact_email, firm, stage, subject_a, subject_b, subject, body, edited_body, message_text, message_type, channel, reply_to_id, outreach_mode, telegram_msg_id, score, research_summary')
    .eq('status', 'pending')
    .order('created_at', { ascending: true })
    .limit(30);

  const investorPending = (pending || [])
    .filter(item => item.outreach_mode !== 'company_sourcing')
    .filter(item => !existingQueueIds.has(String(item.id)));

  // Items that already had a Telegram message: restore them to the in-memory map
  // silently instead of re-sending (avoids duplicate Telegram messages on restart).
  for (const item of investorPending.filter(i => i.telegram_msg_id)) {
    pendingApprovals.set(String(item.telegram_msg_id), {
      ...buildReloadedApprovalEntry(item),
      emailDraft: {
        subject: item.subject_a || item.subject || null,
        alternativeSubject: item.subject_b || null,
        body: item.body || '',
      },
    });
  }

  const unsent = investorPending.filter(item => !item.telegram_msg_id);
  if (!unsent.length) return;

  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!chatId) return;

  info(`Reloading ${unsent.length} pending investor approvals`);

  for (const item of unsent) {
    try {
      const hasSubjects = !!(item.subject_a || item.subject);
      const bodyPreview = ((item.body || '').length > 800 ? `${item.body.slice(0, 800)}…` : (item.body || ''));
      const msg = [
        `*ROCO — ${item.stage || 'EMAIL'} Ready for Approval*`,
        ``,
        `👤 *${item.contact_name || 'Unknown'}* · ${item.firm || 'Unknown Firm'}`,
        `📊 Score: ${item.score || '—'}/100`,
        item.research_summary ? `🔍 _${String(item.research_summary).slice(0, 220)}_` : null,
        item.contact_email ? `📧 ${item.contact_email}` : null,
        ``,
        hasSubjects ? `📧 *Subject A:* _${item.subject_a || item.subject || ''}_` : null,
        hasSubjects && item.subject_b ? `📧 *Subject B:* _${item.subject_b}_` : null,
        `\`\`\``,
        bodyPreview,
        `\`\`\``,
      ].filter(Boolean).join('\n');

      const sent = await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
      await sb.from('approval_queue').update({
        telegram_msg_id: sent.message_id,
      }).eq('id', item.id).then(() => {}, () => {});
      await bot.editMessageReplyMarkup(buildReloadedApprovalKeyboard(sent.message_id, hasSubjects, item.id), {
        chat_id: chatId,
        message_id: sent.message_id,
      }).catch(() => {});

      pendingApprovals.set(sent.message_id, {
        ...buildReloadedApprovalEntry(item),
        emailDraft: {
          subject: item.subject_a || item.subject || null,
          alternativeSubject: item.subject_b || null,
          body: item.body || '',
        },
      });
      await new Promise(r => setTimeout(r, 400));
    } catch (err) {
      error('Failed to reload investor approval', { err: err.message, approvalId: item.id });
    }
  }
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
      clearPendingEditRequestForMsg(msgId);
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
      const isLinkedIn = isLinkedInStageLabel(item.stage);
      const subjectBlock = !isLinkedIn && item.subject_a
        ? `*Subject A:* ${item.subject_a}\n*Subject B:* ${item.subject_b || 'N/A'}\n` : '';
      const msg = [
        `*ROCO — ${item.stage} Draft Ready*`,
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
      const isLinkedInItem = isLinkedInStageLabel(item.stage);
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

          clearPendingEditRequestForMsg(msgId);
          pendingApprovals.delete(msgId);
          clearEditLoopCount(approval);
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
  clearPendingEditRequestForMsg(msgId);
  pendingApprovals.delete(msgId);
  clearEditLoopCount(approval);
  approval.resolve({
    action: 'approve',
    variant: 'A',
    subject: subject || approval.emailDraft?.subject,
  });
  return true;
}

async function handlePriorChatCallback(chatId, decision, approvalId) {
  if (!approvalId) return;
  const sb = getSupabase();
  try {
    let row = null;
    if (sb) {
      try {
        const result = await sb.from('approval_queue')
          .select('id, contact_id, contact_name, firm, deal_id, telegram_msg_id')
          .eq('id', approvalId)
          .single();
        row = result?.data || null;
      } catch {
        row = null;
      }
    }
    if (row?.telegram_msg_id) await clearTelegramApprovalControls(row.telegram_msg_id);

    if (decision === 'proceed') {
      // Mark approval as approved, restore invite_accepted, and queue the DM approval immediately.
      if (sb) {
        await sb.from('approval_queue').update({
          status: 'telegram_approved', resolved_at: new Date().toISOString(),
        }).eq('id', approvalId);

        if (row?.contact_id) {
          await sb.from('contacts').update({
            pipeline_stage: 'invite_accepted',
          }).eq('id', row.contact_id);
        }
      }
      if (row?.contact_id) {
        const { queueLinkedInDmApproval } = await import('../dashboard/server.js');
        await queueLinkedInDmApproval(row.contact_id, { reason: 'prior_chat_approved' }).catch(() => {});
      }
      await bot.sendMessage(chatId, `✅ *Proceeding* — LinkedIn DM approval queued.`, { parse_mode: 'Markdown' });
    } else {
      // Skip — archive the contact
      if (sb) {
        await sb.from('approval_queue').update({
          status: 'telegram_skipped', resolved_at: new Date().toISOString(),
        }).eq('id', approvalId);

        if (row?.contact_id) {
          await sb.from('contacts').update({
            pipeline_stage: 'Inactive',
            conversation_state: 'do_not_contact',
            conversation_ended_at: new Date().toISOString(),
            conversation_ended_reason: 'Prior LinkedIn chat declined via Telegram',
          }).eq('id', row.contact_id);
        }
      }
      await bot.sendMessage(chatId, `✗ *Skipped* — contact will not receive a DM.`, { parse_mode: 'Markdown' });
    }
    import('../dashboard/server.js').then(({ pushActivity, notifyQueueUpdated }) => {
      pushActivity({
        type: 'linkedin',
        action: decision === 'proceed' ? 'Prior chat approved via Telegram' : 'Prior chat skipped via Telegram',
        note: row?.contact_name
          ? `${row.contact_name}${row.firm ? ` @ ${row.firm}` : ''}`
          : (row?.contact_id || approvalId),
        dealId: row?.deal_id || null,
      });
      notifyQueueUpdated();
    }).catch(() => {});
  } catch (err) {
    error('handlePriorChatCallback failed', { err: err.message });
    await bot.sendMessage(chatId, `⚠ Could not process decision: ${err.message}`);
  }
}

export function resolveApprovalFromDashboard(id, action, subject, editedBody) {
  const numId = Number(id);
  let approval = pendingApprovals.get(numId) || pendingApprovals.get(String(id));
  let key = pendingApprovals.has(numId) ? numId : (pendingApprovals.has(String(id)) ? String(id) : null);
  if (!approval) {
    for (const [entryKey, entry] of pendingApprovals.entries()) {
      if (String(entry.queueId || '') === String(id)) {
        approval = entry;
        key = entryKey;
        break;
      }
    }
  }
  if (!approval) return false;
  clearTelegramApprovalControls(key).catch(() => {});
  clearPendingEditRequestForMsg(key);
  pendingApprovals.delete(key);

  if (approval.queueId) {
    if (action === 'approve') {
      updateApprovalStatus(approval.queueId, 'approved', subject || approval.emailDraft.subject).catch(() => {});
    } else if (action === 'skip') {
      updateApprovalStatus(approval.queueId, 'skipped').catch(() => {});
    }
  }

  if (action === 'approve') {
    clearEditLoopCount(approval);
    approval.resolve({
      action: 'approve',
      queueId: approval.queueId || null,
      variant: 'A',
      subject: sanitizeApprovalText(subject || approval.emailDraft.subject),
      body: sanitizeApprovalText(editedBody || approval.emailDraft?.body),
    });
  } else if (action === 'skip') {
    clearEditLoopCount(approval);
    approval.resolve({ action: 'skip' });
  } else if (action === 'edit') {
    approval.resolve({ action: 'edit', instructions: editedBody || '' });
  } else {
    approval.resolve({ action: 'skip' });
  }

  return true;
}
