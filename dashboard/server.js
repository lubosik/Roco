import express from 'express';
import session from 'express-session';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import { Readable } from 'stream';
import multer from 'multer';
import * as XLSX from 'xlsx';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { DateTime } from 'luxon';
import { getPendingApprovals, resolveApprovalFromDashboard, updateApprovalDraftFromDashboard, clearApprovalsForDeal, clearTelegramApprovalControls, sendTelegram, sendSourcingDraftToTelegram, sendReplyForApproval, reloadPendingInvestorApprovals, dismissPendingApproval, getRecentlyResolvedQueueIds } from '../approval/telegramBot.js';
import { getInvestorGuidance, getSourcingGuidance, saveInvestorGuidance, saveSourcingGuidance, buildGuidanceBlock } from '../services/guidanceService.js';
import { invalidateCache as invalidateAgentContext } from '../core/agentContext.js';
import { getSupabase } from '../core/supabase.js';
import {
  getConversationHistory,
  logConversationMessage,
  classifyIntent,
  draftContextualReply,
  setConversationState,
  appendIntentHistory,
  checkTempClosedContacts,
  draftTempCloseFollowUp,
} from '../core/conversationManager.js';
import { sendEmailReply, sendLinkedInReply, sendEmail, listEmails, listWebhooks, listSentInvitations } from '../integrations/unipileClient.js';
import { retrieveEmail } from '../integrations/unipileClient.js';
import { getApiHealth, startHealthChecks } from '../core/apiFallback.js';
import { info, error } from '../core/logger.js';
import { aiComplete, haikuComplete, claudeWebSearch } from '../core/aiClient.js';
import { normalizeComparableName } from '../core/hardeningHelpers.js';
import {
  loadSessionState, saveSessionState,
  getAllDeals, getActiveDeals, getDeal, createDeal, updateDeal,
  getTemplates, getTemplate, updateTemplate, seedDefaultTemplates,
  getActivityLog, logActivity as sbLogActivity,
  getBatches, deleteApprovalFromQueue, addApprovalToQueue,
} from '../core/supabaseSync.js';
import { getWindowStatus, getWindowVisualization, isWithinChannelWindow, getNextWindowOpenForChannel } from '../core/scheduleChecker.js';
import { getBatchSummary } from '../core/batchManager.js';
import { recreateLinkedInWebhooks, startLinkedInDM, sendLinkedInDM as sendLinkedInDMReply, getConnectedEmailAccounts, getExistingChatWithContact, getChatMessages, processLinkedInInvite } from '../core/unipile.js';
import { handleLinkedInMessage as handleLiMsg, handleLinkedInRelation as handleLiRelation } from '../core/unipileWebhooks.js';
import { startInboxMonitor } from '../core/inboxMonitor.js';
import { draftLinkedInDM } from '../outreach/linkedinDrafter.js';
import { listDailyActivityReports } from '../core/analyticsEngine.js';
import {
  researchPerson,
  classifyPersonResearch,
  hasVerifiedPersonResearch,
} from '../research/personResearcher.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const STATE_FILE = path.join(__dirname, '../state.json');

let rocoState;
let wss;
let app;
let jarvisVoiceStatus = {
  ok: null,
  provider: 'elevenlabs',
  configured: false,
  voice_id: null,
  model_id: 'eleven_flash_v2_5',
  checked_at: null,
  error: null,
  upstream_status: null,
};

const activityFeed = [];
const MAX_FEED = 200;
const webhookReceiptDedupe = new Map();
const WEBHOOK_RECEIPT_DEDUPE_MS = 10 * 60 * 1000;
const campaignFirmLinkCache = new Map();
const CAMPAIGN_FIRM_LINK_TTL_MS = 6 * 60 * 60 * 1000;
const INVESTOR_DB_SUMMARY_TTL_MS = 5 * 60 * 1000;
let investorDbSummaryCache = {
  expiresAt: 0,
  value: null,
};

function invalidateInvestorDbSummaryCache() {
  investorDbSummaryCache = {
    expiresAt: 0,
    value: null,
  };
}

function getActivityTimestamp(entry) {
  const raw = entry?.created_at || entry?.timestamp || entry?.createdAt || null;
  const millis = raw ? new Date(raw).getTime() : 0;
  return Number.isFinite(millis) ? millis : 0;
}

function buildActivityFingerprint(entry) {
  const explicitKey = entry?.activity_key || entry?.activityKey || entry?.detail?.activity_key || entry?.meta?.activity_key;
  if (explicitKey) return `activity_key:${explicitKey}`;
  if (entry?.id) return `id:${entry.id}`;
  const ts = entry?.created_at || entry?.timestamp || entry?.createdAt || '';
  return [
    entry?.deal_id || entry?.dealId || '',
    entry?.type || entry?.event_type || '',
    entry?.action || entry?.summary || '',
    entry?.note || entry?.detail || '',
    entry?.full_content || '',
    String(ts).slice(0, 19),
  ].join('|');
}

function mergeActivityEntries(dbEntries = [], liveEntries = []) {
  const merged = [];
  const seen = new Set();

  for (const entry of [...dbEntries, ...liveEntries]) {
    const key = buildActivityFingerprint(entry);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(entry);
  }

  return merged.sort((a, b) => getActivityTimestamp(b) - getActivityTimestamp(a));
}

function updateJarvisVoiceStatus(next = {}) {
  jarvisVoiceStatus = {
    ...jarvisVoiceStatus,
    ...next,
    checked_at: new Date().toISOString(),
  };
}

async function hydrateActivityFeed(limit = 100) {
  const sb = getSupabase();
  if (!sb) return activityFeed;
  try {
    const { data } = await sb.from('activity_log')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(limit);
    const merged = mergeActivityEntries(data || [], activityFeed.slice().reverse()).slice(0, limit);
    activityFeed.length = 0;
    for (const entry of merged.slice().reverse()) activityFeed.push(entry);
  } catch (err) {
    console.warn('[ACTIVITY] hydrate failed:', err.message);
  }
  return activityFeed;
}

function normalizeFirmLinkName(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\b(inc|llc|ltd|lp|llp|plc|corp|corporation|partners|partner|capital|holdings|group|ventures|management|advisors)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function isVerifiedCompanyProfileMatch(firmName, profile) {
  const expected = normalizeFirmLinkName(firmName);
  const actual = normalizeFirmLinkName(profile?.name || '');
  if (!expected || !actual) return false;
  if (expected === actual) return true;

  const expectedTokens = expected.split(' ').filter(Boolean);
  const actualTokens = actual.split(' ').filter(Boolean);
  if (!expectedTokens.length || !actualTokens.length) return false;

  const overlap = expectedTokens.filter(token => actualTokens.includes(token));
  return overlap.length >= Math.min(expectedTokens.length, actualTokens.length)
    && overlap.length >= 2;
}

async function resolveCampaignFirmLink(firmName) {
  const cacheKey = normalizeFirmLinkName(firmName);
  if (!cacheKey) return { url: null, type: null };

  const cached = campaignFirmLinkCache.get(cacheKey);
  if (cached && (Date.now() - cached.cachedAt) < CAMPAIGN_FIRM_LINK_TTL_MS) {
    return cached.value;
  }

  let value = { url: null, type: null };
  try {
    const { getLinkedInCompanyProfile } = await import('../core/unipile.js');
    const profile = await getLinkedInCompanyProfile(firmName, null, null).catch(() => null);
    if (profile && isVerifiedCompanyProfileMatch(firmName, profile)) {
      value = {
        url: profile.profile_url || profile.website || null,
        type: profile.profile_url ? 'linkedin' : (profile.website ? 'website' : null),
      };
    } else if (profile?.website) {
      value = { url: profile.website, type: 'website' };
    }
  } catch {}

  campaignFirmLinkCache.set(cacheKey, { cachedAt: Date.now(), value });
  return value;
}

async function enrichCampaignFirmLinks(rows = []) {
  const enriched = [];
  const concurrency = 4;

  for (let i = 0; i < rows.length; i += concurrency) {
    const chunk = rows.slice(i, i + concurrency);
    const resolved = await Promise.all(chunk.map(async row => {
      const link = await resolveCampaignFirmLink(row.firm_name);
      return {
        ...row,
        firm_link_url: link.url || null,
        firm_link_type: link.type || null,
      };
    }));
    enriched.push(...resolved);
  }

  return enriched;
}

function sanitizeApprovalText(text) {
  return String(text || '')
    .replace(/\u2014/g, '-')
    .replace(/\u2013/g, '-')
    .replace(/\u2018|\u2019/g, "'")
    .replace(/\u201C|\u201D/g, '"')
    .trim();
}

function buildInboundMessageNote(parts = []) {
  return parts
    .map(part => truncateInline(part, 120))
    .filter(Boolean)
    .join(' · ');
}

function buildWebhookReceiptActivity(eventType, payload, source = 'unipile') {
  const normalizedType = String(
    eventType || payload?.type || payload?.event_type || payload?.event || 'unknown'
  ).toLowerCase();
  const event = payload?.data || payload || {};
  const sourceLabel = String(source || 'unipile').replace(/[_-]+/g, ' ').trim() || 'unipile';

  if (['mail_received', 'email.received', 'email_received'].includes(normalizedType)) {
    const fromEmail = event?.from_attendee?.identifier || event?.from_email || event?.from?.email || event?.from || '';
    const fromName = event?.from_attendee?.display_name || event?.from_name || '';
    const subject = event?.subject || '';
    const body = extractUnipileEmailBody(event);
    const sender = fromName || fromEmail || 'unknown sender';
    return {
      type: 'email',
      activity_badge: 'email',
      action: `Inbound ${sourceLabel} email received: ${sender}`,
      note: buildInboundMessageNote([
        subject ? `Subject: "${subject}"` : null,
        body || null,
      ]),
      full_content: body || null,
      meta: {
        event_type: normalizedType,
        source: sourceLabel,
        sender,
        from_email: fromEmail || null,
        subject: subject || null,
        matched_active_deal: false,
      },
    };
  }

  if (['message_received', 'message.created'].includes(normalizedType)) {
    const fromName = event?.sender?.attendee_name || event?.sender_name || event?.attendee_name || '';
    const fromProviderId = event?.sender?.attendee_provider_id || event?.sender_id || event?.attendee_id || '';
    const body = extractUnipileMessageText(event);
    const sender = fromName || fromProviderId || 'unknown sender';
    return {
      type: 'linkedin',
      activity_badge: 'linkedin',
      action: `Inbound LinkedIn DM received: ${sender}`,
      note: buildInboundMessageNote([body || null]),
      full_content: body || null,
      meta: {
        event_type: normalizedType,
        source: sourceLabel,
        sender,
        provider_id: fromProviderId || null,
        matched_active_deal: false,
      },
    };
  }

  if (['new_relation', 'connection_request_accepted'].includes(normalizedType)) {
    const name = event?.user_full_name || event?.attendee?.name || '';
    const publicId = event?.user_public_identifier || '';
    const profileUrl = event?.user_profile_url || '';
    return {
      type: 'webhook',
      action: `Webhook received: LinkedIn acceptance`,
      note: [
        name || publicId || profileUrl || 'unknown person',
      ].filter(Boolean).join(' · '),
      meta: { event_type: normalizedType, source: sourceLabel },
    };
  }

  if (['mail_opened', 'mail_link_clicked'].includes(normalizedType)) {
    const label = event?.label || '';
    const detail = event?.url ? truncateInline(event.url, 80) : null;
    return {
      type: 'webhook',
      action: `Webhook received: ${normalizedType === 'mail_link_clicked' ? 'email click' : 'email open'}`,
      note: [
        label || event?.tracking_id || 'unlabeled tracking event',
        detail,
      ].filter(Boolean).join(' · '),
      meta: { event_type: normalizedType, source: sourceLabel },
    };
  }

  return {
    type: 'webhook',
    action: `Webhook received: ${normalizedType || 'unknown'}`,
    note: [
      `source ${sourceLabel}`,
      event?.account_id || payload?.account_id || null,
    ].filter(Boolean).join(' · '),
    meta: { event_type: normalizedType, source: sourceLabel },
  };
}

async function logWebhookReceipt(eventType, payload, source = 'unipile', options = {}) {
  const dedupeKey = buildWebhookReceiptDedupeKey(eventType, payload, source);
  if (isDuplicateWebhookReceipt(dedupeKey)) {
    return false;
  }

  await insertWebhookLogRecord({
    event_type: eventType || 'unknown',
    payload: {
      ...(payload || {}),
      __roco_meta: {
        ...((payload && payload.__roco_meta) || {}),
        source,
      },
    },
  });
  if (options.emitActivity !== false) {
    pushActivity(buildWebhookReceiptActivity(eventType, payload, source));
  }
  return true;
}

function buildWebhookReceiptDedupeKey(eventType, payload, source = 'unipile') {
  const event = payload || {};
  const data = event?.data || event || {};
  const sender = data?.sender || data?.from_attendee || data?.from || {};
  const stableId = [
    data.id,
    data.message_id,
    data.email_id,
    data.event_id,
    data.tracking_id,
    data.thread_id,
    data.conversation_id,
    data.chat_id,
    event.id,
    event.message_id,
  ].find(Boolean);
  if (stableId) return `${source}:${eventType || 'unknown'}:${stableId}`;

  const actor = [
    data.from_email,
    data.from,
    sender.identifier,
    sender.email,
    sender.attendee_provider_id,
    data.sender_id,
    data.user_provider_id,
    data.user_public_identifier,
    data.user_full_name,
  ].find(Boolean) || 'unknown';
  const subject = data.subject || '';
  const text = stripHtml(data.body_plain || data.body || data.text || data.message || '').slice(0, 160);
  return `${source}:${eventType || 'unknown'}:${actor}:${subject}:${text}`;
}

function isDuplicateWebhookReceipt(key) {
  const now = Date.now();
  for (const [cachedKey, ts] of webhookReceiptDedupe) {
    if (now - ts > WEBHOOK_RECEIPT_DEDUPE_MS) webhookReceiptDedupe.delete(cachedKey);
  }
  if (!key) return false;
  const last = webhookReceiptDedupe.get(key);
  if (last && now - last < WEBHOOK_RECEIPT_DEDUPE_MS) return true;
  webhookReceiptDedupe.set(key, now);
  return false;
}

function getTelegramBotId() {
  return String(process.env.TELEGRAM_BOT_TOKEN || '').split(':')[0].trim();
}

function looksLikeOperationalEcho(text) {
  const value = String(text || '').toLowerCase();
  if (!value) return false;
  return value.includes('roco - email ready for approval')
    || value.includes('roco — email ready for approval')
    || value.includes('roco - linkedin dm ready for approval')
    || value.includes('roco — linkedin dm ready for approval')
    || value.includes('linkedin message received from:')
    || value.includes('instant reply sent')
    || value.includes('approved via dashboard');
}

function shouldSuppressInboundWebhookMessage({ fromName, fromUrn, bodyText, payload }) {
  const botId = getTelegramBotId();
  const senderName = String(fromName || '').trim();
  const senderUrn = String(fromUrn || '').trim();
  const text = String(bodyText || '').trim();
  const raw = payload || {};

  if (!text) return true;
  if (raw?.is_sender === true || raw?.is_self === true || raw?.sender?.is_self === true) return true;
  if (botId && (senderName === botId || senderUrn === botId)) return true;
  if (looksLikeOperationalEcho(text)) return true;
  return false;
}

async function insertWebhookLogRecord(record = {}) {
  try {
    const sb = getSupabase();
    if (!sb) return false;
    const payload = {
      event_type: record.event_type || 'unknown',
      payload: record.payload || {},
      received_at: new Date().toISOString(),
    };
    const { error: insertErr } = await sb.from('webhook_logs').insert(payload);
    if (!insertErr) return true;
    if (!isMissingColumnError(insertErr, 'received_at')) return false;
    const { error: fallbackErr } = await sb.from('webhook_logs').insert({
      event_type: payload.event_type,
      payload: payload.payload,
    });
    return !fallbackErr;
  } catch {}
  return false;
}

async function listRecentWebhookLogs(limit = 200) {
  const sb = getSupabase();
  if (!sb) return [];

  try {
    const { data, error: primaryErr } = await sb
      .from('webhook_logs')
      .select('event_type, payload, received_at')
      .order('received_at', { ascending: false })
      .limit(limit);
    if (!primaryErr) return data || [];
    if (!isMissingColumnError(primaryErr, 'received_at')) return [];
  } catch {}

  try {
    const { data, error: fallbackErr } = await sb
      .from('webhook_logs')
      .select('event_type, payload, created_at')
      .order('created_at', { ascending: false })
      .limit(limit);
    if (fallbackErr) return [];
    return (data || []).map(row => ({
      ...row,
      received_at: row.received_at || row.created_at || null,
    }));
  } catch {
    return [];
  }
}

function isTruthyEnvFlag(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase());
}

function getConfiguredUnipileAccountIds() {
  return {
    gmail: String(process.env.UNIPILE_GMAIL_ACCOUNT_ID || '').trim(),
    outlook: String(process.env.UNIPILE_OUTLOOK_ACCOUNT_ID || '').trim(),
    linkedin: String(process.env.UNIPILE_LINKEDIN_ACCOUNT_ID || '').trim(),
  };
}

function matchesConfiguredAccount(payload, expectedAccountId) {
  const actual = String(payload?.account_id || payload?.data?.account_id || '').trim();
  if (!expectedAccountId) return true;
  return !!actual && actual === String(expectedAccountId).trim();
}

function isReplyMessageType(value) {
  return ['email_reply', 'linkedin_reply'].includes(String(value || '').trim().toLowerCase());
}

function getReplyActivityBadge(channel) {
  return channel === 'linkedin' ? 'linkedin_reply' : 'email_reply';
}

function buildEmailTrackingLabel({ dealId = null, contactId = null, stage = 'email' } = {}) {
  const normalizedStage = String(stage || 'email').trim().toLowerCase().replace(/\s+/g, '_');
  return `deal:${dealId || 'none'}|contact:${contactId || 'none'}|stage:${normalizedStage}`;
}

function parseEmailTrackingLabel(label) {
  const result = {};
  for (const part of String(label || '').split('|')) {
    const [key, value] = part.split(':');
    if (key && value) result[key] = value;
  }
  return result;
}

function getEmailTrackingBadge(eventType) {
  return eventType === 'mail_link_clicked' ? 'email_clicked' : 'email_opened';
}

function getEmailTrackingAction(eventType, contactName) {
  return `${eventType === 'mail_link_clicked' ? 'Email clicked' : 'Email opened'}: ${contactName || 'Contact'}`;
}

async function findTrackedEmailRecord(sb, payload) {
  const labelBits = parseEmailTrackingLabel(payload?.label || '');
  if (labelBits.deal && labelBits.contact) {
    try {
      const { data } = await sb.from('emails')
        .select('id, deal_id, contact_id, to_email, subject, status, message_id, provider_id, metadata')
        .eq('deal_id', labelBits.deal)
        .eq('contact_id', labelBits.contact)
        .eq('status', 'sent')
        .order('sent_at', { ascending: false })
        .limit(10);
      const rows = data || [];
      const matched = rows.find(row =>
        String(row.message_id || '') === String(payload?.email_id || '') ||
        String(row.provider_id || '') === String(payload?.email_id || '')
      );
      if (matched) return matched;
      if (rows.length) return rows[0];
    } catch {}
  }

  for (const field of ['message_id', 'provider_id']) {
    const value = String(payload?.email_id || '').trim();
    if (!value) continue;
    try {
      const { data } = await sb.from('emails')
        .select('id, deal_id, contact_id, to_email, subject, status, message_id, provider_id, metadata')
        .eq(field, value)
        .eq('status', 'sent')
        .limit(1)
        .maybeSingle();
      if (data) return data;
    } catch {}
  }

  return null;
}

async function findActiveTrackedDealContext(sb, trackedEmail) {
  if (!sb || !trackedEmail?.deal_id) return null;

  let { data: contact } = await sb.from('contacts')
    .select('id, name, email, company_name, job_title, deal_id, deals!contacts_deal_id_fkey(id, name, status)')
    .eq('id', trackedEmail.contact_id)
    .maybeSingle()
    .then(result => result, () => ({ data: null }));

  const deal = contact?.deals || null;
  if (!contact?.id || String(deal?.status || '').toUpperCase() !== 'ACTIVE') return null;
  return { contact, deal };
}

function mergeTrackedEmailMetadata(existingMetadata, payload, trackedEmail) {
  const metadata = {
    ...(existingMetadata && typeof existingMetadata === 'object' ? existingMetadata : {}),
  };
  const eventType = String(payload?.event || '').trim().toLowerCase();
  const now = payload?.date || new Date().toISOString();
  const alreadyOpened = Number(metadata.opens_count || 0) > 0;
  const alreadyClicked = Number(metadata.clicks_count || 0) > 0;

  metadata.tracking_label = metadata.tracking_label || payload?.label || null;
  metadata.last_tracking_event = eventType || null;
  metadata.last_tracking_event_at = now;
  metadata.last_tracking_id = payload?.tracking_id || metadata.last_tracking_id || null;
  metadata.last_tracking_event_id = payload?.event_id || metadata.last_tracking_event_id || null;
  metadata.last_tracking_email_id = payload?.email_id || trackedEmail?.message_id || metadata.last_tracking_email_id || null;

  if (eventType === 'mail_link_clicked') {
    metadata.clicks_count = Number(metadata.clicks_count || 0) + 1;
    metadata.first_clicked_at = metadata.first_clicked_at || now;
    metadata.last_clicked_at = now;
    metadata.last_clicked_url = payload?.url || metadata.last_clicked_url || null;
  } else {
    metadata.opens_count = Number(metadata.opens_count || 0) + 1;
    metadata.first_opened_at = metadata.first_opened_at || now;
    metadata.last_opened_at = now;
  }

  return {
    metadata,
    isFirstEvent: eventType === 'mail_link_clicked' ? !alreadyClicked : !alreadyOpened,
  };
}

function isEmptyWebhookPayload(event) {
  if (event == null) return true;
  if (typeof event === 'string') return !event.trim();
  if (Array.isArray(event)) return event.length === 0;
  if (typeof event === 'object') return Object.keys(event).length === 0;
  return false;
}

function getDefaultSequenceStepsForChannel(channel) {
  if (channel === 'linkedin_dm') {
    return [
      { step: 1, type: 'linkedin_dm', label: 'linkedin_dm_1', delay_days: 0 },
      { step: 2, type: 'linkedin_dm', label: 'linkedin_dm_2', delay_days: 7 },
    ];
  }
  return [
    { step: 1, type: 'email', label: 'email_intro', delay_days: 0 },
    { step: 2, type: 'email', label: 'email_followup_1', delay_days: 7 },
    { step: 3, type: 'email', label: 'email_followup_2', delay_days: 14 },
  ];
}

function normaliseSequenceDelay(step, fallback = null) {
  const candidates = [
    step?.delay_days,
    step?.delayDays,
    step?.wait_days,
    step?.waitDays,
    step?.days,
  ];
  for (const candidate of candidates) {
    const value = Number(candidate);
    if (Number.isFinite(value)) return value;
  }
  return fallback;
}

function getChannelSequenceSteps(sequence, channel) {
  const fallback = getDefaultSequenceStepsForChannel(channel);
  const typeMatches = channel === 'linkedin_dm'
    ? new Set(['linkedin_dm'])
    : new Set(['email']);
  const steps = (sequence?.steps || [])
    .filter(step => typeMatches.has(String(step?.type || '').toLowerCase()))
    .sort((a, b) => Number(a?.step || 0) - Number(b?.step || 0));
  return steps.length ? steps : fallback;
}

async function getSequenceForDealFromServer(sb, dealId) {
  if (!sb) return null;

  if (dealId) {
    try {
      const { data } = await sb.from('deal_sequence').select('steps').eq('deal_id', dealId).limit(1).single();
      if (data?.steps?.length) return data;
    } catch {}
  }

  try {
    const { data } = await sb.from('outreach_sequence').select('steps').limit(1).single();
    return data || null;
  } catch {
    return null;
  }
}

function getNextFollowUpPlanForChannel(sequence, deal, channel, sentFollowUpNumber) {
  const steps = getChannelSequenceSteps(sequence, channel);
  const currentStep = steps[sentFollowUpNumber] || null;
  const nextStep = steps[sentFollowUpNumber + 1] || null;
  const defaultGap = channel === 'linkedin_dm'
    ? (Number(deal?.followup_days_li) || 7)
    : (Number(deal?.followup_days_email) || 7);

  if (!nextStep) return { delayDays: null, nextStep: null };

  const currentDelay = normaliseSequenceDelay(currentStep, 0);
  const nextDelay = normaliseSequenceDelay(nextStep, defaultGap);
  const delta = Number(nextDelay) - Number(currentDelay);
  return {
    delayDays: Number.isFinite(delta) && delta > 0 ? delta : defaultGap,
    nextStep,
  };
}

function computeBackfilledScheduledFollowUpAt(contact, deal, sequence) {
  if (contact?.response_received === true || contact?.last_reply_at) return null;

  const stage = String(contact?.pipeline_stage || '').trim().toLowerCase().replace(/\s+/g, '_');
  const isEmailSent = stage === 'email_sent';
  const isDmSent = stage === 'dm_sent';
  if (!isEmailSent && !isDmSent) return null;

  const sentAt = isDmSent
    ? (contact?.dm_sent_at || contact?.last_outreach_at)
    : (contact?.last_email_sent_at || contact?.last_outreach_at);
  if (!sentAt) return null;

  const dueAt = new Date(new Date(sentAt).getTime() + 7 * 24 * 60 * 60 * 1000);
  if (Number.isNaN(dueAt.getTime())) return null;
  return dueAt.toISOString();
}

async function processUnipileMessageEvent(event) {
  const payload = event?.data || event || {};
  let eventType = (event?.type || event?.event_type || '').toLowerCase();

  if (!eventType) {
    const hasRelationShape = Boolean(
      payload?.user_provider_id ||
      payload?.user_public_identifier ||
      payload?.user_profile_url ||
      payload?.relation?.provider_id ||
      payload?.attendee?.provider_id
    );
    const hasMessageShape = Boolean(
      payload?.chat_id ||
      payload?.conversation_id ||
      payload?.message ||
      payload?.text ||
      payload?.sender?.attendee_provider_id
    );

    if (hasRelationShape) eventType = 'new_relation';
    else if (hasMessageShape) eventType = 'message_received';
  }

  console.log('[WEBHOOKS/UNIPILE] Received event:', eventType);

  if (['message_received', 'message.created'].includes(eventType)) {
    const message = payload?.data || payload;
    const fromProvId = message?.sender?.attendee_provider_id || message?.sender_id || message?.attendee_id || '';
    const fromName = message?.sender?.attendee_name || message?.sender_name || message?.attendee_name || '';
    const bodyText = message?.message || message?.text || message?.body || '';
    const chatId = message?.chat_id || message?.conversation_id || '';
    const messageId = message?.id || message?.message_id || '';

    if (shouldSuppressInboundWebhookMessage({ fromName, fromUrn: fromProvId, bodyText, payload: message })) {
      await insertWebhookLogRecord({
        event_type: eventType || 'message_received',
        payload: event || {},
      }).then(null, () => {});
      return 'suppressed';
    }

    await logWebhookReceipt(eventType || 'message_received', event, 'unipile_messages', { emitActivity: false }).catch(() => {});

    await queueInboundWithDebounce({
      fromUrn: fromProvId,
      fromName,
      bodyText,
      chatId,
      messageId,
      channel: 'linkedin',
      raw: message,
    });
    return 'linkedin';
  }

  if (['new_relation', 'connection_request_accepted'].includes(eventType)) {
    await logWebhookReceipt(eventType, event, 'unipile_messages').catch(() => {});
    const queueForApproval = async ({ contact, reason }) => {
      if (!contact?.id) return null;
      return queueLinkedInDmApproval(contact.id, { reason });
    };
    const relationResult = await handleLiRelation(event, pushActivity, queueForApproval);
    if (relationResult?.matchStatus && relationResult.matchStatus !== 'matched') {
      await insertWebhookLogRecord({
        event_type: eventType,
        payload: {
          ...(event || {}),
          __roco_meta: {
            ...((event && event.__roco_meta) || {}),
            source: 'unipile_messages',
            match_status: relationResult.matchStatus,
            match_note: relationResult?.contactId
              ? `contact ${relationResult.contactId}`
              : 'acceptance webhook did not match an active contact',
          },
        },
      }).then(null, () => {});
    }
    return;
  }

  if (['mail_received', 'email.received', 'email_received'].includes(eventType)) {
    await logWebhookReceipt(eventType, event, 'unipile_messages', { emitActivity: false }).catch(() => {});
    return 'email';
  }

  await insertWebhookLogRecord({
    event_type: eventType || 'unknown',
    payload: event || {},
  }).catch(() => {});
  console.log('[WEBHOOKS/UNIPILE] Unhandled event type:', eventType);
  return null;
}

function getConfiguredServerBaseUrl() {
  const railwayDomain = String(process.env.RAILWAY_PUBLIC_DOMAIN || '').trim();
  if (railwayDomain) return `https://${railwayDomain.replace(/^https?:\/\//, '').replace(/\/+$/, '')}`;

  const explicit = String(process.env.PUBLIC_URL || process.env.SERVER_BASE_URL || '').trim();
  if (explicit) {
    const normalized = explicit.replace(/\/+$/, '');
    return /^https?:\/\//i.test(normalized) ? normalized : `https://${normalized.replace(/^\/+/, '')}`;
  }

  return '';
}

function requireAuth(req, res, next) {
  const publicPaths = ['/login', '/welcome.html', '/favicon.ico', '/audio/'];
  if (publicPaths.some(p => req.path.startsWith(p))) return next();
  if (req.session?.authenticated) return next();
  if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Unauthorized' });
  return res.redirect('/welcome.html');
}

function isMissingColumnError(err, columnName) {
  const msg = String(err?.message || err || '');
  return msg.includes(`Could not find the '${columnName}' column`) ||
    msg.includes(`column ${columnName} does not exist`);
}

function normalizeMetadataArray(value) {
  if (Array.isArray(value)) return value.map(v => String(v || '').trim()).filter(Boolean);
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return [];
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) return parsed.map(v => String(v || '').trim()).filter(Boolean);
    } catch {}
    return trimmed.split(',').map(v => v.trim()).filter(Boolean);
  }
  return [];
}

function parseLooseJsonObject(text) {
  const match = String(text || '').match(/\{[\s\S]*\}/);
  if (!match) return null;
  try {
    return JSON.parse(match[0]);
  } catch {
    return null;
  }
}

function asJsonArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return [];
    try {
      const parsed = JSON.parse(trimmed);
      return Array.isArray(parsed) ? parsed : [trimmed];
    } catch {
      return trimmed.split(',').map(item => item.trim()).filter(Boolean);
    }
  }
  return value ? [value] : [];
}

function normaliseTranscriptAnalysis(parsed = {}) {
  const interests = asJsonArray(parsed.investment_interests || parsed.sectors_of_interest || parsed.interests);
  const intentSignals = asJsonArray(parsed.intent_signals || parsed.positive_intent_signals);
  const objections = asJsonArray(parsed.negative_signals || parsed.objections);
  const followUps = asJsonArray(parsed.follow_up_actions || parsed.recommended_follow_up_actions);
  return {
    summary: String(parsed.summary || '').trim(),
    investor_name: String(parsed.investor_name || parsed.name || '').trim(),
    firm_name: String(parsed.firm_name || parsed.firm || parsed.company_name || '').trim(),
    investor_type: String(parsed.investor_type || parsed.contact_type || '').trim(),
    location: String(parsed.location || parsed.where_they_live || parsed.hq_location || '').trim(),
    role_title: String(parsed.role_title || parsed.title || '').trim(),
    investment_thesis: String(parsed.investment_thesis || parsed.investor_thesis || '').trim(),
    sectors_of_interest: interests,
    cheque_size_range: String(parsed.cheque_size_range || parsed.cheque_size || '').trim(),
    aum: String(parsed.aum || parsed.fund_size || '').trim(),
    past_investments: asJsonArray(parsed.past_investments || parsed.portfolio_companies),
    investment_interests: interests,
    intent_signals: [...intentSignals, ...objections.map(item => `Objection: ${item}`)].filter(Boolean),
    positive_intent_signals: intentSignals,
    negative_signals: objections,
    sentiment_score: Math.max(1, Math.min(10, Number(parsed.sentiment_score || parsed.sentiment || 5) || 5)),
    follow_up_actions: followUps,
    raw: parsed,
  };
}

async function buildTranscriptAnalysisContext(sb, linkedContact = null) {
  const [dealsRes, activityRes] = await Promise.all([
    sb.from('deals').select('id, name, sector, geography, investor_profile, status').order('created_at', { ascending: false }).limit(12).then(result => result).catch(() => ({ data: [] })),
    sb.from('activity_log').select('deal_id, event_type, summary, created_at').order('created_at', { ascending: false }).limit(40).then(result => result).catch(() => ({ data: [] })),
  ]);
  return {
    active_deals: (dealsRes.data || []).map(deal => ({
      id: deal.id,
      name: deal.name,
      sector: deal.sector,
      geography: deal.geography,
      investor_profile: deal.investor_profile || null,
      status: deal.status,
    })),
    recent_activity: (activityRes.data || []).map(entry => ({
      deal_id: entry.deal_id || null,
      event_type: entry.event_type || null,
      summary: entry.summary || null,
      created_at: entry.created_at || null,
    })),
    existing_investor_record: linkedContact ? {
      name: linkedContact.name || null,
      firm_name: linkedContact.company_name || null,
      title: linkedContact.job_title || null,
      email: linkedContact.email || null,
      linkedin_url: linkedContact.linkedin_url || null,
      notes: linkedContact.notes || null,
      investment_thesis: linkedContact.investment_thesis || null,
      past_investments: linkedContact.past_investments || null,
      intent_signals: linkedContact.intent_signals || null,
    } : null,
  };
}

async function analyzeMeetingTranscript({ transcriptText, investorName, linkedContact = null }) {
  const sb = getSupabase();
  const context = sb ? await buildTranscriptAnalysisContext(sb, linkedContact).catch(() => ({})) : {};
  const prompt = `You are Roco. Analyse this meeting transcript in the context of the full fundraising dashboard.

GLOBAL CONTEXT
${JSON.stringify(context, null, 2)}

TASK
1. Identify the investor's full name, firm name, title, location, and whether they appear to be an angel/UHNW, athlete, family office, VC, PE investor, or other investor type.
2. Extract their investment preferences and thesis.
3. Extract sectors, stages, cheque size, AUM or fund size, geography, lifestyle/personal details if clearly useful, and any past investments or portfolio companies mentioned.
4. Extract positive intent signals, negative signals or objections, overall sentiment score 1 to 10, and concrete follow-up actions.
5. If an existing investor record is present in context, reconcile against it and include any new information that should be merged in.
6. Return structured JSON only.

KNOWN INVESTOR LABEL
${investorName || linkedContact?.name || 'Unknown investor'}

TRANSCRIPT
${transcriptText}`;

  const rawText = await haikuComplete(prompt, { maxTokens: 1200 });
  const parsed = parseLooseJsonObject(rawText);
  if (!parsed) throw new Error('Transcript analysis JSON parse failed');
  return normaliseTranscriptAnalysis(parsed);
}

function buildConversationHistoryEntry({ type, date, summary, sentiment = null }) {
  return {
    type,
    date,
    summary,
    sentiment,
  };
}

async function upsertTranscriptInvestorDatabaseRecord(sb, { analysis, contact, investorEmail, investorName, firmName }) {
  if (!sb) return null;
  const lookupEmail = String(investorEmail || contact?.email || '').trim();
  const lookupName = String(investorName || analysis.investor_name || contact?.name || '').trim();
  let existing = null;

  if (lookupEmail) {
    const { data } = await sb.from('investors_db')
      .select('*')
      .or(`email.eq.${lookupEmail},primary_contact_email.eq.${lookupEmail}`)
      .limit(1)
      .maybeSingle()
      .then(result => result, () => ({ data: null }));
    existing = data || null;
  }
  if (!existing && lookupName) {
    const { data } = await sb.from('investors_db')
      .select('*')
      .ilike('name', lookupName)
      .limit(1)
      .maybeSingle()
      .then(result => result, () => ({ data: null }));
    existing = data || null;
  }

  const payload = {
    name: lookupName || analysis.firm_name || firmName || 'Unknown investor',
    email: lookupEmail || null,
    primary_contact_email: lookupEmail || null,
    investor_type: analysis.investor_type || existing?.investor_type || null,
    contact_type: (analysis.investor_type || '').toLowerCase().includes('angel') || (analysis.investor_type || '').toLowerCase().includes('uhnw')
      ? 'angel'
      : existing?.contact_type || 'individual_at_firm',
    is_angel: (analysis.investor_type || '').toLowerCase().includes('angel') || (analysis.investor_type || '').toLowerCase().includes('uhnw'),
    preferred_industries: analysis.sectors_of_interest.join(', ') || existing?.preferred_industries || null,
    description: [
      analysis.summary,
      analysis.firm_name ? `Firm: ${analysis.firm_name}` : (firmName ? `Firm: ${firmName}` : null),
      analysis.role_title ? `Title: ${analysis.role_title}` : null,
      analysis.location ? `Location: ${analysis.location}` : null,
      analysis.investment_thesis ? `Thesis: ${analysis.investment_thesis}` : null,
      analysis.cheque_size_range ? `Cheque size: ${analysis.cheque_size_range}` : null,
      analysis.aum ? `AUM/Fund size: ${analysis.aum}` : null,
      analysis.follow_up_actions.length ? `Follow-up: ${analysis.follow_up_actions.join('; ')}` : null,
    ].filter(Boolean).join(' | ').slice(0, 2000),
    enrichment_status: 'transcript_enriched',
    hq_location: analysis.location || existing?.hq_location || null,
    hq_country: analysis.location || existing?.hq_country || null,
    last_investment_company: analysis.past_investments[0] || existing?.last_investment_company || null,
  };

  let record = null;
  if (existing?.id) {
    const { data } = await sb.from('investors_db').update(payload).eq('id', existing.id).select('*').single().then(r => r, () => ({ data: existing }));
    record = data || existing;
  } else {
    const { data } = await sb.from('investors_db').insert(payload).select('*').single().then(r => r, () => ({ data: null }));
    record = data || null;
  }
  return record;
}

function mergeDealSettings(currentSettings, patch) {
  return {
    ...((currentSettings && typeof currentSettings === 'object') ? currentSettings : {}),
    ...patch,
  };
}

function mergeParsedDealInfo(currentParsed, patch) {
  return {
    ...((currentParsed && typeof currentParsed === 'object') ? currentParsed : {}),
    ...patch,
  };
}

function buildInvestorListPayload(body = {}, { requireName = false } = {}) {
  const payload = {};
  if (requireName || body.name != null) {
    const name = String(body.name || '').trim();
    if (!name && requireName) throw new Error('name is required');
    if (name) payload.name = name;
  }
  if (body.list_type != null) payload.list_type = String(body.list_type || '').trim() || null;
  if (body.description != null) payload.description = String(body.description || '').trim() || null;
  if (body.priority_order != null && body.priority_order !== '') payload.priority_order = Number(body.priority_order);
  if (body.source != null) payload.source = String(body.source || '').trim() || null;
  if (body.list_source != null) payload.source = String(body.list_source || '').trim() || null;
  if (body.deal_types != null) payload.deal_types = normalizeMetadataArray(body.deal_types);
  if (body.sectors != null) payload.sectors = normalizeMetadataArray(body.sectors);
  return payload;
}

function normalizeSheetRows(rows) {
  return (rows || []).map(row => {
    const normalized = {};
    for (const [key, value] of Object.entries(row || {})) {
      const cleanKey = String(key || '')
        .replace(/\uFEFF/g, '')
        .replace(/\s+/g, ' ')
        .trim();
      normalized[cleanKey] = value;
    }
    return normalized;
  });
}

function extractUnipileMessageText(message) {
  return [
    message?.text,
    message?.message,
    message?.body_plain,
    message?.body,
    message?.content,
  ].map(stripHtml).find(Boolean) || '';
}

function extractUnipileEmailBody(message) {
  return [
    message?.body_plain,
    message?.body_text,
    message?.text,
    message?.message,
    message?.snippet,
    message?.body,
    message?.html,
  ].map(stripHtml).find(Boolean) || '';
}

function getUnipileMessageTimestamp(message) {
  return message?.created_at
    || message?.timestamp
    || message?.sent_at
    || message?.received_at
    || null;
}

function buildConversationDedupeKey(message) {
  const body = String(message?.body || '').trim();
  const direction = String(message?.direction || '').trim();
  const unipileId = String(message?.unipile_message_id || '').trim();
  return unipileId || `${direction}|${body}`;
}

async function hydrateLinkedInConversationHistory(sb, contact, dealId = null) {
  if (!sb || !contact?.id) return [];

  let existingMessages = [];
  {
    let query = sb.from('conversation_messages')
      .select('*')
      .eq('contact_id', contact.id)
      .order('sent_at', { ascending: true });
    if (dealId) query = query.eq('deal_id', dealId);
    const { data } = await query;
    existingMessages = data || [];
  }

  const chatId = String(contact.unipile_chat_id || '').trim();
  if (!chatId) return existingMessages;

  const remoteMessages = await getChatMessages(chatId, 100).catch(() => []);
  if (!Array.isArray(remoteMessages) || remoteMessages.length === 0) return existingMessages;

  const existingKeys = new Set(existingMessages.map(buildConversationDedupeKey));
  const inserts = [];

  for (const remote of remoteMessages) {
    const body = extractUnipileMessageText(remote);
    if (!body) continue;

    const unipileMessageId = remote?.message_id || remote?.id || null;
    const timestamp = getUnipileMessageTimestamp(remote);
    const direction = (remote?.is_sender || remote?.is_self) ? 'outbound' : 'inbound';
    const mapped = {
      contact_id: contact.id,
      deal_id: dealId || contact.deal_id || null,
      direction,
      channel: 'linkedin_dm',
      body,
      unipile_message_id: unipileMessageId,
      unipile_chat_id: chatId,
      sent_at: direction === 'outbound' ? (timestamp || new Date().toISOString()) : null,
      received_at: direction === 'inbound' ? (timestamp || new Date().toISOString()) : null,
    };
    const dedupeKey = buildConversationDedupeKey(mapped);
    if (existingKeys.has(dedupeKey)) continue;
    existingKeys.add(dedupeKey);
    inserts.push(mapped);
  }

  if (inserts.length > 0) {
    try {
      await sb.from('conversation_messages').insert(inserts);
    } catch {}
    let query = sb.from('conversation_messages')
      .select('*')
      .eq('contact_id', contact.id)
      .order('sent_at', { ascending: true });
    if (dealId) query = query.eq('deal_id', dealId);
    const { data } = await query;
    existingMessages = data || existingMessages;
  }

  return existingMessages;
}

function normalizeEmailAddress(value) {
  return String(value || '').trim().toLowerCase();
}

function collectEmailAddresses(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value.flatMap(collectEmailAddresses);
  if (typeof value === 'string') {
    const match = value.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/ig);
    return match ? match.map(normalizeEmailAddress).filter(Boolean) : [];
  }
  if (typeof value === 'object') {
    return [
      value.email,
      value.address,
      value.value,
      value.name,
    ].flatMap(collectEmailAddresses);
  }
  return [];
}

function detectRemoteEmailDirection(message, contactEmail) {
  if (message?.is_sender || message?.is_self) return 'outbound';
  const normalizedContactEmail = normalizeEmailAddress(contactEmail);
  const fromEmails = collectEmailAddresses(message?.from || message?.from_attendee || message?.sender || message?.sender_info);
  const toEmails = collectEmailAddresses(message?.to || message?.to_attendees || message?.recipients);
  if (normalizedContactEmail && fromEmails.includes(normalizedContactEmail)) return 'inbound';
  if (normalizedContactEmail && toEmails.includes(normalizedContactEmail)) return 'outbound';
  return 'inbound';
}

async function hydrateEmailConversationHistory(sb, contact, dealId = null) {
  if (!sb || !contact?.id) return [];

  let existingMessages = [];
  {
    let query = sb.from('conversation_messages')
      .select('*')
      .eq('contact_id', contact.id)
      .order('sent_at', { ascending: true });
    if (dealId) query = query.eq('deal_id', dealId);
    const { data } = await query;
    existingMessages = data || [];
  }

  const threadIds = new Set();
  for (const field of ['thread_id', 'gmail_thread_id']) {
    try {
      let query = sb.from('emails')
        .select(field)
        .eq('contact_id', contact.id)
        .not(field, 'is', null);
      if (dealId) query = query.eq('deal_id', dealId);
      const { data } = await query;
      for (const row of data || []) {
        if (row?.[field]) threadIds.add(String(row[field]));
      }
    } catch {}
  }

  try {
    let query = sb.from('replies')
      .select('thread_id')
      .eq('contact_id', contact.id)
      .not('thread_id', 'is', null);
    if (dealId) query = query.eq('deal_id', dealId);
    const { data } = await query;
    for (const row of data || []) {
      if (row?.thread_id) threadIds.add(String(row.thread_id));
    }
  } catch {}

  if (!threadIds.size) return existingMessages;

  const existingKeys = new Set(existingMessages.map(buildConversationDedupeKey));
  const inserts = [];

  for (const threadId of threadIds) {
    let remoteMessages = [];
    try {
      remoteMessages = await listEmails({ threadId, limit: 100 });
    } catch {
      remoteMessages = [];
    }

    for (const remote of remoteMessages || []) {
      const body = extractUnipileEmailBody(remote);
      if (!body) continue;

      const direction = detectRemoteEmailDirection(remote, contact.email);
      const unipileMessageId = remote?.message_id || remote?.id || remote?.provider_id || null;
      const timestamp = getUnipileMessageTimestamp(remote);
      const mapped = {
        contact_id: contact.id,
        deal_id: dealId || contact.deal_id || null,
        direction,
        channel: 'email',
        subject: remote?.subject || null,
        body,
        unipile_message_id: unipileMessageId,
        sent_at: direction === 'outbound' ? (timestamp || new Date().toISOString()) : null,
        received_at: direction === 'inbound' ? (timestamp || new Date().toISOString()) : null,
      };
      const dedupeKey = buildConversationDedupeKey(mapped);
      if (existingKeys.has(dedupeKey)) continue;
      existingKeys.add(dedupeKey);
      inserts.push(mapped);
    }
  }

  if (inserts.length > 0) {
    try {
      await sb.from('conversation_messages').insert(inserts);
    } catch {}
    let query = sb.from('conversation_messages')
      .select('*')
      .eq('contact_id', contact.id)
      .order('sent_at', { ascending: true });
    if (dealId) query = query.eq('deal_id', dealId);
    const { data } = await query;
    existingMessages = data || existingMessages;
  }

  return existingMessages;
}

// ─────────────────────────────────────────────
// REPLY DEBOUNCE BATCHER
// ─────────────────────────────────────────────
const replyDebounceMap = new Map();
const inboundReplyDedupe = new Map();
const REPLY_DEBOUNCE_MS = Number(process.env.REPLY_DEBOUNCE_MS || 15_000);
const INBOUND_REPLY_DEDUPE_MS = 10 * 60 * 1000;

function isDuplicateInboundReply(key) {
  if (!key) return false;
  const now = Date.now();
  for (const [cachedKey, ts] of inboundReplyDedupe) {
    if (now - ts > INBOUND_REPLY_DEDUPE_MS) inboundReplyDedupe.delete(cachedKey);
  }
  const last = inboundReplyDedupe.get(key);
  if (last && now - last < INBOUND_REPLY_DEDUPE_MS) return true;
  inboundReplyDedupe.set(key, now);
  return false;
}

function clearDebounceForDeal(dealId) {
  let cleared = 0;
  for (const [key, batch] of replyDebounceMap.entries()) {
    if (batch.deal && String(batch.deal.id) === String(dealId)) {
      clearTimeout(batch.timer);
      replyDebounceMap.delete(key);
      cleared++;
    }
  }
  if (cleared > 0) info(`[CLOSE] Cleared ${cleared} debounce batch(es) for deal ${dealId}`);
}

// ─────────────────────────────────────────────
// STATE FILE HELPERS
// ─────────────────────────────────────────────

function readState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    }
  } catch (err) {
    error('Failed to read state.json', { err: err.message });
  }
  return {
    rocoStatus: 'ACTIVE',
    outreachEnabled: true,
    followupEnabled: true,
    enrichmentEnabled: true,
    researchEnabled: true,
    linkedinEnabled: true,
    pausedUntil: null,
    activeDeals: [],
    lastUpdated: new Date().toISOString(),
  };
}

function writeState(state) {
  state.lastUpdated = new Date().toISOString();
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
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
  const value = Number(amount || 0);
  const symbol = CURRENCY_SYMBOLS[(currency || 'USD').toUpperCase()] || '$';
  return `${symbol}${value.toLocaleString()}`;
}

async function getDealNameMap(sb, dealIds = []) {
  const ids = [...new Set((dealIds || []).filter(Boolean).map(String))];
  if (!sb || ids.length === 0) return {};
  const { data } = await sb.from('deals').select('id, name').in('id', ids);
  return Object.fromEntries((data || []).map(deal => [String(deal.id), deal.name || 'Unknown Project']));
}

async function buildInvestorDbSummary(sb) {
  const { count: total, error: totalError } = await sb.from('investors_db')
    .select('id', { count: 'exact', head: true });
  if (totalError) throw new Error(totalError.message);

  const PAGE_SIZE = 1000;
  const byCategory = {};
  const investorCountByListId = {};
  let uncategorised = 0;
  let from = 0;

  while (true) {
    const { data, error: pageError } = await sb.from('investors_db')
      .select('investor_category, list_id')
      .range(from, from + PAGE_SIZE - 1);
    if (pageError) throw new Error(pageError.message);
    if (!data?.length) break;

    for (const row of data) {
      const category = row.investor_category || 'Uncategorised';
      byCategory[category] = (byCategory[category] || 0) + 1;
      if (!row.investor_category) uncategorised += 1;
      if (row.list_id) {
        const key = String(row.list_id);
        investorCountByListId[key] = (investorCountByListId[key] || 0) + 1;
      }
    }

    if (data.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }

  return {
    total: total || 0,
    uncategorised,
    byCategory: Object.fromEntries(
      Object.entries(byCategory).sort(([, a], [, b]) => b - a)
    ),
    investorCountByListId,
    builtAt: new Date().toISOString(),
  };
}

async function getInvestorDbSummary(sb, { forceRefresh = false } = {}) {
  const now = Date.now();
  if (!forceRefresh && investorDbSummaryCache.value && investorDbSummaryCache.expiresAt > now) {
    return investorDbSummaryCache.value;
  }

  const summary = await buildInvestorDbSummary(sb);
  investorDbSummaryCache = {
    value: summary,
    expiresAt: now + INVESTOR_DB_SUMMARY_TTL_MS,
  };
  return summary;
}

function normalizeContactIdentityValue(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/^www\./, '')
    .replace(/\/+$/, '');
}

function extractLinkedInIdentityCandidates(value) {
  const normalized = normalizeContactIdentityValue(value);
  if (!normalized) return [];

  const candidates = new Set([normalized]);
  const slugMatch = normalized.match(/linkedin\.com\/in\/([^/?#]+)/i);
  if (slugMatch?.[1]) candidates.add(slugMatch[1].toLowerCase());
  const tail = normalized.split('/').filter(Boolean).pop();
  if (tail && tail !== 'in') candidates.add(tail.toLowerCase());
  return [...candidates].filter(Boolean);
}

function rankMatchedInvestorContact(contact, { requireActiveDeal = false } = {}) {
  let score = 0;
  const dealStatus = String(contact?.deals?.status || contact?.deal_status || '').toUpperCase();
  if (dealStatus === 'ACTIVE') score += 100;
  else if (requireActiveDeal) score -= 100;
  if (contact?.unipile_chat_id) score += 25;
  if (contact?.linkedin_provider_id) score += 20;
  if (contact?.email) score += 15;
  score += new Date(contact?.updated_at || contact?.last_reply_at || contact?.last_outreach_at || 0).getTime() / 1e13;
  return score;
}

function pickBestInvestorContact(candidates = [], options = {}) {
  const filtered = candidates.filter(Boolean).filter(contact => !options.requireActiveDeal || String(contact?.deals?.status || '').toUpperCase() === 'ACTIVE');
  const pool = filtered.length ? filtered : candidates.filter(Boolean);
  if (!pool.length) return null;
  return [...pool].sort((a, b) => rankMatchedInvestorContact(b, options) - rankMatchedInvestorContact(a, options))[0] || null;
}

function getContactIdentityKeys(contact) {
  const keys = [];
  const email = normalizeContactIdentityValue(contact?.email);
  const linkedin = normalizeContactIdentityValue(contact?.linkedin_url);
  const name = normalizeContactIdentityValue(contact?.name);
  const company = normalizeContactIdentityValue(contact?.company_name);
  if (email) keys.push(`email:${email}`);
  if (linkedin) keys.push(`linkedin:${linkedin}`);
  if (name && company) keys.push(`name_company:${name}|${company}`);
  return [...new Set(keys)];
}

function normalizeFirmLookupName(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

const LINKEDIN_INVITE_STAGES = new Set([
  'invite_sent',
  'invite_accepted',
  'DM Approved',
  'DM Sent',
  'dm_sent',
  'Replied',
  'In Conversation',
  'Meeting Booked',
  'Meeting Scheduled',
]);
const LINKEDIN_ACCEPTED_STAGES = new Set([
  'invite_accepted',
  'DM Approved',
  'DM Sent',
  'dm_sent',
  'Replied',
  'In Conversation',
  'Meeting Booked',
  'Meeting Scheduled',
]);
const CLOSED_CONTACT_STAGES = new Set([
  'Archived',
  'Skipped',
  'Inactive',
  'Suppressed — Opt Out',
  'Deleted — Do Not Contact',
]);

function hasLinkedInInviteHistory(contact) {
  return Boolean(
    contact?.invite_sent_at ||
    contact?.invite_accepted_at ||
    contact?.outreach_channel === 'linkedin_invite' ||
    LINKEDIN_INVITE_STAGES.has(contact?.pipeline_stage)
  );
}

function hasLinkedInAccepted(contact) {
  return Boolean(
    contact?.invite_accepted_at ||
    LINKEDIN_ACCEPTED_STAGES.has(contact?.pipeline_stage)
  );
}

function hasActivePendingLinkedInInvite(contact) {
  if (!hasLinkedInInviteHistory(contact)) return false;
  if (hasLinkedInAccepted(contact)) return false;
  if (CLOSED_CONTACT_STAGES.has(contact?.pipeline_stage)) return false;
  return true;
}

function createEmptyDealChannelMetrics() {
  return {
    contacts: 0,
    active_prospects: 0,
    emails_sent: 0,
    emails_replied: 0,
    li_invites_sent: 0,
    li_accepts: 0,
    li_active_pending: 0,
    li_dms_sent: 0,
    li_dm_replies: 0,
    total_sent: 0,
    total_responses: 0,
    email_response_rate: 0,
    li_acceptance_rate: 0,
    li_dm_response_rate: 0,
    overall_response_rate: 0,
  };
}

function finalizeDealChannelMetrics(metrics) {
  const next = { ...createEmptyDealChannelMetrics(), ...(metrics || {}) };
  next.total_sent = next.emails_sent + next.li_invites_sent + next.li_dms_sent;
  next.total_responses = next.emails_replied + next.li_accepts + next.li_dm_replies;
  next.email_response_rate = next.emails_sent > 0
    ? Math.round((next.emails_replied / next.emails_sent) * 100)
    : 0;
  next.li_acceptance_rate = next.li_invites_sent > 0
    ? Math.round((next.li_accepts / next.li_invites_sent) * 100)
    : 0;
  next.li_dm_response_rate = next.li_dms_sent > 0
    ? Math.round((next.li_dm_replies / next.li_dms_sent) * 100)
    : 0;
  next.overall_response_rate = next.total_sent > 0
    ? Math.round((next.total_responses / next.total_sent) * 100)
    : 0;
  return next;
}

function upsertChannelReplyMetrics(metrics, contact, seenEmailReplies, seenLinkedInReplies) {
  if (!contact || !contact.deal_id) return;
  const replySignal = contact.response_received === true || Boolean(contact.last_reply_at);
  if (!replySignal) return;
  const channel = String(contact.reply_channel || '').trim().toLowerCase();
  const fallbackType = ''; // last_contact_type column removed — rely on reply_channel only
  const replyKey = `${contact.deal_id}:${contact.id || contact.email || contact.name || 'unknown-contact'}`;

  if ((channel === 'email' || fallbackType === 'email') && !seenEmailReplies.has(replyKey)) {
    seenEmailReplies.add(replyKey);
    metrics.emails_replied += 1;
    return;
  }

  if ((channel === 'linkedin' || fallbackType === 'linkedin') && !seenLinkedInReplies.has(replyKey)) {
    seenLinkedInReplies.add(replyKey);
    metrics.li_dm_replies += 1;
  }
}

async function computeDealChannelMetrics(sb, dealIds = []) {
  const ids = [...new Set((dealIds || []).filter(Boolean))];
  const metricsByDeal = Object.fromEntries(ids.map(id => [id, createEmptyDealChannelMetrics()]));
  if (!sb || !ids.length) return metricsByDeal;

  const safeIds = ids.length ? ids : ['00000000-0000-0000-0000-000000000000'];
  const [
    { data: contactsData },
    { data: emailsData },
    { data: repliesData },
  ] = await Promise.all([
    sb.from('contacts')
      .select('id, deal_id, email, name, pipeline_stage, response_received, last_reply_at, reply_channel, last_email_sent_at, invite_sent_at, invite_accepted_at, outreach_channel, dm_sent_at')
      .in('deal_id', safeIds),
    sb.from('emails')
      .select('id, deal_id, status')
      .eq('status', 'sent')
      .in('deal_id', safeIds),
    sb.from('replies')
      .select('deal_id, contact_id, channel')
      .in('deal_id', safeIds),
  ]);

  let outreachEvents = [];
  try {
    const { data } = await sb.from('outreach_events')
      .select('deal_id, contact_id, event_type, status')
      .in('deal_id', safeIds)
      .in('event_type', ['EMAIL_SENT', 'LINKEDIN_INVITE_SENT', 'LINKEDIN_DM_SENT']);
    outreachEvents = data || [];
  } catch {}

  const contacts = contactsData || [];
  const emails = emailsData || [];
  const replies = repliesData || [];
  const sentEventCoverage = Object.fromEntries(ids.map(id => [id, { email: false, invite: false, dm: false }]));
  const seenEmailReplies = new Set();
  const seenLinkedInReplies = new Set();

  for (const contact of contacts) {
    if (!metricsByDeal[contact.deal_id]) continue;
    const metrics = metricsByDeal[contact.deal_id];
    metrics.contacts += 1;
    if (!CLOSED_CONTACT_STAGES.has(contact.pipeline_stage)) metrics.active_prospects += 1;
    if (hasLinkedInInviteHistory(contact)) metrics.li_active_pending += hasActivePendingLinkedInInvite(contact) ? 1 : 0;
    if (hasLinkedInAccepted(contact)) metrics.li_accepts += 1;
    upsertChannelReplyMetrics(metrics, contact, seenEmailReplies, seenLinkedInReplies);
  }

  for (const reply of replies) {
    if (!reply?.deal_id || !metricsByDeal[reply.deal_id]) continue;
    const metrics = metricsByDeal[reply.deal_id];
    const channel = String(reply.channel || '').trim().toLowerCase();
    const replyKey = `${reply.deal_id}:${reply.contact_id || `reply:${channel || 'unknown'}`}`;
    if (channel === 'email') {
      if (!seenEmailReplies.has(replyKey)) {
        seenEmailReplies.add(replyKey);
        metrics.emails_replied += 1;
      }
    } else if (channel === 'linkedin') {
      if (!seenLinkedInReplies.has(replyKey)) {
        seenLinkedInReplies.add(replyKey);
        metrics.li_dm_replies += 1;
      }
    }
  }

  for (const row of outreachEvents) {
    if (!row?.deal_id || !metricsByDeal[row.deal_id]) continue;
    if (String(row.status || 'confirmed').toLowerCase() !== 'confirmed') continue;
    const metrics = metricsByDeal[row.deal_id];
    switch (row.event_type) {
      case 'EMAIL_SENT':
        sentEventCoverage[row.deal_id].email = true;
        metrics.emails_sent += 1;
        break;
      case 'LINKEDIN_INVITE_SENT':
        sentEventCoverage[row.deal_id].invite = true;
        metrics.li_invites_sent += 1;
        break;
      case 'LINKEDIN_DM_SENT':
        sentEventCoverage[row.deal_id].dm = true;
        metrics.li_dms_sent += 1;
        break;
      default:
        break;
    }
  }

  for (const dealId of ids) {
    const metrics = metricsByDeal[dealId];
    const coverage = sentEventCoverage[dealId] || {};
    if (!coverage.email) {
      metrics.emails_sent = emails.filter(row => row.deal_id === dealId).length;
    }
    if (!coverage.invite) {
      metrics.li_invites_sent = contacts.filter(contact => contact.deal_id === dealId && hasLinkedInInviteHistory(contact)).length;
    }
    if (!coverage.dm) {
      metrics.li_dms_sent = contacts.filter(contact =>
        contact.deal_id === dealId &&
        (contact.dm_sent_at || ['DM Approved', 'DM Sent', 'dm_sent', 'Replied', 'In Conversation', 'Meeting Booked', 'Meeting Scheduled'].includes(contact.pipeline_stage))
      ).length;
    }
    metricsByDeal[dealId] = finalizeDealChannelMetrics(metrics);
  }

  return metricsByDeal;
}

async function countConfirmedLinkedInInvitesForDeals(sb, dealIds = []) {
  const ids = (dealIds || []).filter(Boolean);
  if (!sb || !ids.length) return 0;

  try {
    const { count, error } = await sb.from('outreach_events')
      .select('id', { count: 'exact', head: true })
      .in('deal_id', ids)
      .eq('event_type', 'LINKEDIN_INVITE_SENT')
      .eq('status', 'confirmed');
    if (!error) return Number(count || 0);
  } catch {}

  try {
    const { count, error } = await sb.from('activity_log')
      .select('id', { count: 'exact', head: true })
      .in('deal_id', ids)
      .eq('event_type', 'LINKEDIN_INVITE_SENT');
    if (error) return 0;
    return Number(count || 0);
  } catch {
  return 0;
}

async function listUnmatchedWebhookReceipts(limit = 100) {
  const logs = await listRecentWebhookLogs(limit);
  return logs.filter(row => {
    const meta = row?.payload?.__roco_meta || {};
    return meta.match_status === 'unmatched' || meta.match_status === 'ambiguous';
  });
}

async function listLinkedInProviderLimitPauses(limit = 100) {
  const sb = getSupabase();
  if (!sb) return [];
  try {
    const { data } = await sb.from('contacts')
      .select('id, deal_id, name, company_name, notes, follow_up_due_at, deals!contacts_deal_id_fkey(name)')
      .not('notes', 'is', null)
      .order('follow_up_due_at', { ascending: true })
      .limit(limit);

    return (data || []).map(row => {
      const match = String(row?.notes || '').match(/\[LI_INVITE_LIMIT:count=(\d+)\|blocked_until=([^|\]]+)(?:\|notified_at=([^\]]+))?\]/i);
      if (!match) return null;
      return {
        id: row.id,
        deal_id: row.deal_id || null,
        deal_name: row.deals?.name || null,
        contact_name: row.name || 'Unknown contact',
        company_name: row.company_name || null,
        retry_count: Number(match[1] || 0),
        blocked_until: match[2] || row.follow_up_due_at || null,
        notified_at: match[3] || null,
      };
    }).filter(Boolean);
  } catch {
    return [];
  }
}
}

function buildFirmCampaignSummary(contacts = [], enrichmentStatus = 'pending') {
  const archivedStages = new Set(['Archived', 'Deleted — Do Not Contact', 'Suppressed — Opt Out', 'Inactive']);
  const repliedStages = new Set(['Replied', 'In Conversation', 'Meeting Booked']);
  const totalContacts = contacts.length;
  const contactedCount = contacts.filter(contact =>
    contact?.invite_sent_at ||
    contact?.last_email_sent_at ||
    contact?.last_outreach_at ||
    ['invite_sent', 'invite_accepted', 'DM Approved', 'Email Approved', 'DM Sent', 'Email Sent', 'dm_sent', 'email_sent', 'Replied', 'In Conversation', 'Meeting Booked'].includes(contact?.pipeline_stage)
  ).length;
  const inviteSentCount = contacts.filter(hasLinkedInInviteHistory).length;
  const activePendingInviteCount = contacts.filter(hasActivePendingLinkedInInvite).length;
  const inviteAcceptedCount = contacts.filter(contact =>
    contact?.invite_accepted_at || ['invite_accepted', 'DM Approved', 'DM Sent', 'dm_sent', 'Replied', 'In Conversation', 'Meeting Booked'].includes(contact?.pipeline_stage)
  ).length;
  const emailSentCount = contacts.filter(contact =>
    contact?.last_email_sent_at || ['Email Approved', 'Email Sent', 'email_sent', 'Replied', 'In Conversation', 'Meeting Booked'].includes(contact?.pipeline_stage)
  ).length;
  const repliedCount = contacts.filter(contact =>
    contact?.response_received === true ||
    contact?.last_reply_at ||
    repliedStages.has(contact?.pipeline_stage)
  ).length;
  const meetingBookedCount = contacts.filter(contact => contact?.pipeline_stage === 'Meeting Booked').length;
  const closedCount = totalContacts > 0 && contacts.every(contact => archivedStages.has(contact?.pipeline_stage)) ? totalContacts : 0;

  let firmStage = 'pending_enrichment';
  let firmStageLabel = 'Pending enrichment';

  if (closedCount === totalContacts && totalContacts > 0) {
    firmStage = 'closed';
    firmStageLabel = 'Closed';
  } else if (meetingBookedCount > 0) {
    firmStage = 'meeting_booked';
    firmStageLabel = 'Meeting booked';
  } else if (repliedCount > 0) {
    firmStage = 'replied';
    firmStageLabel = 'Replied';
  } else if (inviteAcceptedCount > 0) {
    firmStage = 'invite_accepted';
    firmStageLabel = 'Connection accepted';
  } else if (contactedCount > 0) {
    firmStage = 'outreach_started';
    firmStageLabel = 'Outreach in progress';
  } else if (enrichmentStatus === 'complete') {
    firmStage = 'ready_for_outreach';
    firmStageLabel = 'Ready for outreach';
  } else if (enrichmentStatus === 'in_progress') {
    firmStage = 'enriching';
    firmStageLabel = 'Finding decision makers';
  }

  return {
    total_contacts: totalContacts,
    contacted_count: contactedCount,
    invite_sent_count: inviteSentCount,
    active_pending_invite_count: activePendingInviteCount,
    invite_accepted_count: inviteAcceptedCount,
    email_sent_count: emailSentCount,
    replied_count: repliedCount,
    meeting_booked_count: meetingBookedCount,
    closed_count: closedCount,
    firm_stage: firmStage,
    firm_stage_label: firmStageLabel,
  };
}

async function buildContactDealContextMap(sb) {
  if (!sb) return new Map();
  const { data: allContacts } = await sb.from('contacts')
    .select('id, name, company_name, email, linkedin_url, deal_id, updated_at')
    .limit(5000);
  const contacts = allContacts || [];
  const dealIds = [...new Set(contacts.map(c => c.deal_id).filter(Boolean).map(String))];
  const { data: dealRows } = dealIds.length
    ? await sb.from('deals').select('id, name, status').in('id', dealIds)
    : { data: [] };
  const dealsById = new Map((dealRows || []).map(deal => [String(deal.id), deal]));

  const idsByKey = new Map();
  for (const contact of contacts) {
    for (const key of getContactIdentityKeys(contact)) {
      if (!idsByKey.has(key)) idsByKey.set(key, new Set());
      idsByKey.get(key).add(contact.id);
    }
  }

  const contactById = new Map(contacts.map(contact => [contact.id, contact]));
  const contextMap = new Map();

  for (const contact of contacts) {
    const relatedIds = new Set([contact.id]);
    for (const key of getContactIdentityKeys(contact)) {
      for (const relatedId of (idsByKey.get(key) || [])) relatedIds.add(relatedId);
    }

    const relatedContacts = [...relatedIds]
      .map(id => contactById.get(id))
      .filter(Boolean);

    const relatedDeals = [...new Map(
      relatedContacts
        .filter(row => row.deal_id)
        .map(row => {
          const deal = dealsById.get(String(row.deal_id));
          return [String(row.deal_id), {
            dealId: row.deal_id,
            dealName: deal?.name || 'Unknown Project',
            status: deal?.status || '',
          }];
        })
    ).values()];

    const currentDeal = contact.deal_id ? dealsById.get(String(contact.deal_id)) : null;
    const activeDeal = (currentDeal?.status === 'ACTIVE'
      ? { dealId: contact.deal_id, dealName: currentDeal.name || 'Unknown Project', status: currentDeal.status }
      : relatedDeals.find(deal => deal.status === 'ACTIVE')) || null;

    contextMap.set(contact.id, {
      projectName: currentDeal?.name || (relatedDeals[0]?.dealName || ''),
      currentDealStatus: currentDeal?.status || '',
      activeDealName: activeDeal?.dealName || '',
      activeDealId: activeDeal?.dealId || null,
      deals: relatedDeals,
      dealNamesText: relatedDeals.map(deal => deal.dealName).join(', '),
    });
  }

  return contextMap;
}

function formatConversationHistoryForProject(messages, contact, dealName = null) {
  const projectLabel = dealName || 'Unknown Project';
  const projectHeader = `Project: ${projectLabel}`;
  const history = (messages || []).map(message => {
    const role = message.direction === 'outbound' ? 'ROCO' : (contact?.name || 'INVESTOR');
    const channel = message.channel ? ` via ${message.channel}` : '';
    const timestamp = message.sent_at || message.received_at;
    const dateLabel = timestamp ? ` on ${new Date(timestamp).toLocaleDateString('en-GB')}` : '';
    return `[${projectLabel}] ${role}${channel}${dateLabel}\n${message.body || ''}`;
  }).join('\n\n---\n\n');
  return history ? `${projectHeader}\n${history}` : projectHeader;
}

// ─────────────────────────────────────────────
// LINKEDIN DM SEND HELPER (approval flow)
// ─────────────────────────────────────────────

function buildLinkedInDraftContactPage(contact) {
  const whyThisFirm = contact.why_this_firm
    || contact.match_rationale
    || contact.justification
    || '';
  return {
    id: contact.id,
    properties: {
      'Name':                   { type: 'title',     title:     [{ plain_text: contact.name || '' }] },
      'Email':                  { type: 'email',     email:     contact.email || null },
      'Company Name':           { type: 'rich_text', rich_text: [{ plain_text: contact.company_name || '' }] },
      'LinkedIn URL':           { type: 'url',       url:       contact.linkedin_url || null },
      'Job Title':              { type: 'rich_text', rich_text: [{ plain_text: contact.job_title || '' }] },
      'Investor Score (0-100)': { type: 'number',    number:    contact.investor_score || null },
      'Notes':                  { type: 'rich_text', rich_text: [{ plain_text: contact.notes || '' }] },
      'Sector Focus':           { type: 'rich_text', rich_text: [{ plain_text: contact.sector_focus || '' }] },
      'Geography':              { type: 'rich_text', rich_text: [{ plain_text: contact.geography || '' }] },
      'Typical Cheque Size':    { type: 'rich_text', rich_text: [{ plain_text: contact.typical_cheque_size || '' }] },
      'Past Investments':       { type: 'rich_text', rich_text: [{ plain_text: contact.past_investments || '' }] },
      'AUM':                    { type: 'rich_text', rich_text: [{ plain_text: contact.aum_fund_size || '' }] },
      'Investment Thesis':      { type: 'rich_text', rich_text: [{ plain_text: contact.investment_thesis || '' }] },
      'Why This Firm':          { type: 'rich_text', rich_text: [{ plain_text: whyThisFirm }] },
    },
    name: contact.name,
    email: contact.email,
    company_name: contact.company_name,
    linkedin_url: contact.linkedin_url,
    investor_score: contact.investor_score,
    job_title: contact.job_title,
    notes: contact.notes,
    sector_focus: contact.sector_focus,
    geography: contact.geography,
    typical_cheque_size: contact.typical_cheque_size,
    past_investments: Array.isArray(contact.past_investments) ? contact.past_investments.join(', ') : contact.past_investments,
    aum_fund_size: contact.aum_fund_size,
    investment_thesis: contact.investment_thesis,
    why_this_firm: whyThisFirm,
  };
}

async function loadLinkedInConversationContext(contact) {
  if (!contact?.linkedin_provider_id && !contact?.unipile_chat_id) return [];
  try {
    const chatId = contact.unipile_chat_id
      || (await getExistingChatWithContact(contact.linkedin_provider_id, process.env.UNIPILE_LINKEDIN_ACCOUNT_ID))?.id
      || null;
    if (!chatId) return [];
    const messages = await getChatMessages(chatId, 25).catch(() => []);
    return Array.isArray(messages) ? messages : [];
  } catch {
    return [];
  }
}

function truncateInline(value, max = 140) {
  const text = stripHtml(value).replace(/\s+/g, ' ').trim();
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

function preservesAcceptedProgress(stage) {
  return [
    'pending_dm_approval',
    'dm approved',
    'dm sent',
    'in conversation',
    'prior_chat_review',
    'replied',
    'meeting booked',
    'meeting scheduled',
  ].includes(String(stage || '').trim().toLowerCase());
}

function normalizeReplyClassification(parsed = {}) {
  const rawIntent = String(parsed?.intent || '').trim();
  const rawSentiment = String(parsed?.sentiment || '').trim().toLowerCase();
  const intentKey = rawIntent.toLowerCase().replace(/\s+/g, '_');
  const intentMap = {
    interested: 'interested',
    wants_more_info: 'asking_question',
    meeting_request: 'interested',
    positive: 'interested',
    not_interested: 'not_interested',
    opt_out: 'not_interested',
    asking_question: 'asking_question',
    providing_info: 'providing_info',
    considering: 'considering',
    neutral: 'neutral',
    conversation_end: 'conversation_end',
  };
  const sentimentMap = {
    positive: 'positive',
    negative: 'negative',
    neutral: 'neutral',
  };
  return {
    ...parsed,
    intent: intentMap[intentKey] || intentKey || 'neutral',
    sentiment: sentimentMap[rawSentiment] || 'neutral',
  };
}

async function collapseDuplicateLinkedInDmQueueRows(sb, contactId, canonicalRowId) {
  if (!sb || !contactId || !canonicalRowId) return;
  try {
    const { data: duplicates } = await sb.from('approval_queue')
      .select('id')
      .eq('contact_id', contactId)
      .eq('stage', 'LinkedIn DM')
      .in('status', ['pending', 'approved', 'approved_waiting_for_window', 'sending'])
      .neq('id', canonicalRowId);
    const duplicateIds = (duplicates || []).map(row => row.id).filter(Boolean);
    if (!duplicateIds.length) return;
    await sb.from('approval_queue').update({
      status: 'skipped',
      resolved_at: new Date().toISOString(),
      edit_instructions: 'Auto-skipped duplicate LinkedIn DM approval',
    }).in('id', duplicateIds);
  } catch {}
}

async function suppressSkippedLinkedInDmContact(sb, contactId, reason = 'LinkedIn DM skipped') {
  if (!sb || !contactId) return;
  await sb.from('contacts').update({
    pipeline_stage: 'Skipped',
    pending_linkedin_dm: false,
    follow_up_due_at: null,
    updated_at: new Date().toISOString(),
  }).eq('id', contactId).then(null, () => {});
}

function hasActiveEmailConversation(contact = {}) {
  return !!contact.last_email_sent_at
    || ['intro_sent', 'follow_up_sent', 'awaiting_response', 'temp_closed'].includes(contact.conversation_state);
}

function stripPersonResearchMarkers(notes) {
  return String(notes || '')
    .replace(/\n?\[PERSON_RESEARCHED[^\n]*\]/g, '')
    .replace(/\n?\[PERSON_RESEARCHED_FOR_RANKING[^\n]*\]/g, '')
    .replace(/\n?\[PERSON_RESEARCH_PARTIAL[^\n]*\]/g, '')
    .replace(/\n?\[PERSON_RESEARCH_VERIFIED[^\n]*\]/g, '')
    .trim();
}

function buildPersonResearchPatch(contact, research) {
  const status = classifyPersonResearch({ ...contact, ...research });
  const marker = status === 'verified' ? 'PERSON_RESEARCH_VERIFIED' : 'PERSON_RESEARCH_PARTIAL';
  const notesBase = stripPersonResearchMarkers(contact.notes);
  const summaryLines = [
    research.firm_description ? `Profile: ${research.firm_description}` : null,
    research.investment_thesis ? `Thesis: ${research.investment_thesis}` : null,
    research.past_investments ? `Past: ${research.past_investments}` : null,
    research.recent_news ? `Recent: ${research.recent_news}` : null,
  ].filter(Boolean);

  const patch = {
    person_researched: status === 'verified',
    notes: [notesBase, `[${marker} ${new Date().toISOString()}]`, `Research status: ${status}`, ...summaryLines].filter(Boolean).join('\n').slice(0, 4000),
  };
  if (research.job_title && !contact.job_title) patch.job_title = research.job_title;
  if (research.company_name && !contact.company_name) patch.company_name = research.company_name;
  if (research.linkedin_url && !contact.linkedin_url) patch.linkedin_url = research.linkedin_url;
  if (research.sector_focus) patch.sector_focus = research.sector_focus;
  if (research.geography) patch.geography = research.geography;
  if (research.typical_cheque) patch.typical_cheque_size = research.typical_cheque;
  if (research.firm_aum) patch.aum_fund_size = research.firm_aum;
  if (research.past_investments) patch.past_investments = research.past_investments;
  if (research.investment_thesis) patch.investment_thesis = research.investment_thesis;
  if (research.contact_type_confirmed) patch.contact_type = research.contact_type_confirmed;
  return patch;
}

function hasRecentResearchVerificationFailure(contact, hours = 6) {
  const matches = [...String(contact?.notes || '').matchAll(/\[PERSON_RESEARCH_VERIFICATION_FAILED ([^\]]+)\]/g)];
  if (!matches.length) return false;
  const latest = Date.parse(matches[matches.length - 1][1]);
  return Number.isFinite(latest) && Date.now() - latest < hours * 60 * 60 * 1000;
}

async function ensureVerifiedResearchForLinkedInDraft(contact, sb) {
  if (hasVerifiedPersonResearch(contact)) return contact;
  if (!sb || !contact?.id || !contact?.deals?.id) return null;
  if (hasRecentResearchVerificationFailure(contact)) return null;

  pushActivity({
    type: 'research',
    action: `Verifying research before LinkedIn DM: ${contact.name}`,
    note: contact.company_name || 'unknown firm',
    deal_name: contact.deals?.name || null,
    dealId: contact.deals?.id || contact.deal_id || null,
  });

  try {
    const research = await researchPerson({ contact, deal: contact.deals });
    if (!research) return null;
    const patch = buildPersonResearchPatch(contact, research);
    const updated = { ...contact, ...patch };
    const status = classifyPersonResearch(updated);
    await sb.from('contacts').update(patch).eq('id', contact.id);
    await sbLogActivity({
      dealId: contact.deals?.id || contact.deal_id || null,
      contactId: contact.id,
      eventType: status === 'verified' ? 'PERSON_RESEARCH_VERIFIED_BEFORE_DM_DRAFT' : 'PERSON_RESEARCH_PARTIAL_BEFORE_DM_DRAFT',
      summary: status === 'verified'
        ? `${contact.name} verified researched before LinkedIn DM draft`
        : `${contact.name} still only partially researched before LinkedIn DM draft`,
      detail: {
        research_status: status,
        company_name: research.company_name || contact.company_name || null,
        past_investments: research.past_investments || null,
        investment_thesis: research.investment_thesis || null,
      },
    }).catch(() => {});
    return status === 'verified' ? updated : null;
  } catch (err) {
    const marker = `[PERSON_RESEARCH_VERIFICATION_FAILED ${new Date().toISOString()}] ${String(err.message || err).slice(0, 180)}`;
    const notes = `${String(contact.notes || '').trim()}\n${marker}`.trim().slice(0, 4000);
    await sb.from('contacts').update({
      notes,
      person_researched: false,
      pipeline_stage: 'invite_accepted',
      pending_linkedin_dm: true,
      updated_at: new Date().toISOString(),
    }).eq('id', contact.id).then(null, () => {});
    await sbLogActivity({
      dealId: contact.deals?.id || contact.deal_id || null,
      contactId: contact.id,
      eventType: 'PERSON_RESEARCH_VERIFICATION_FAILED',
      summary: `${contact.name} could not be verified before LinkedIn DM draft`,
      detail: { error: String(err.message || err).slice(0, 500) },
    }).catch(() => {});
    return null;
  }
}

export async function queueLinkedInDmApproval(contactId, { reason = 'acceptance', body = null } = {}) {
  const sb = getSupabase();
  if (!sb || !contactId) return null;

  // Check 1: do not re-draft a DM that Dom already skipped/closed.
  const { data: blockedRows } = await sb.from('approval_queue')
    .select('id, status, created_at, edit_instructions')
    .eq('contact_id', contactId)
    .eq('stage', 'LinkedIn DM')
    .in('status', ['skipped', 'telegram_skipped', 'closed', 'manual'])
    .order('created_at', { ascending: false })
    .limit(5);
  const manuallyBlockedRow = (blockedRows || []).find(row => {
    if (row.status !== 'skipped') return true;
    const instructions = String(row.edit_instructions || '').toLowerCase();
    return !instructions.includes('auto-requeued')
      && !instructions.includes('missing score')
      && !instructions.includes('missing research')
      && !instructions.includes('stale processing');
  });
  if (manuallyBlockedRow) {
    await suppressSkippedLinkedInDmContact(sb, contactId, `Prior LinkedIn DM approval was ${manuallyBlockedRow.status}`);
    return { deferred: true, reason: `previous_dm_${manuallyBlockedRow.status}` };
  }

  // Check 2: do not open a LinkedIn DM lane for a contact already in an email sequence.
  const { data: currentContact } = await sb.from('contacts')
    .select('id, name, pipeline_stage, investor_score, last_email_sent_at, conversation_state, pending_linkedin_dm')
    .eq('id', contactId)
    .maybeSingle()
    .then(result => result, () => ({ data: null }));
  if (hasActiveEmailConversation(currentContact)) {
    await suppressSkippedLinkedInDmContact(sb, contactId, 'Already contacted by email; LinkedIn DM lane suppressed');
    return { deferred: true, reason: 'already_contacted_by_email' };
  }
  const score = Number(currentContact?.investor_score);
  if (!Number.isFinite(score) || score <= 0) {
    await sb.from('contacts').update({
      pipeline_stage: 'Researched',
      pending_linkedin_dm: false,
      updated_at: new Date().toISOString(),
    }).eq('id', contactId).then(null, () => {});
    return { deferred: true, reason: 'missing_score' };
  }

  // Check 3: existing pending queue row
  const { data: existingRows } = await sb.from('approval_queue')
    .select('id, status, created_at')
    .eq('contact_id', contactId)
    .eq('stage', 'LinkedIn DM')
    .in('status', ['pending', 'approved', 'approved_waiting_for_window', 'sending'])
    .order('created_at', { ascending: false })
    .limit(1);
  if (existingRows?.length) {
    await reloadPendingInvestorApprovals().catch(() => {});
    return existingRows[0];
  }

  // Check 2: Optimistic lock — atomically claim the contact before building the draft.
  // This prevents concurrent calls from both proceeding when the DB check finds no existing row.
  const LOCKABLE_STAGES = ['invite_accepted', 'Invite Accepted', 'Enriched', 'enriched', 'Ranked', 'ranked', 'pending_linkedin_dm'];
  const { data: locked } = await sb.from('contacts')
    .update({ pipeline_stage: 'pending_dm_approval' })
    .eq('id', contactId)
    .in('pipeline_stage', LOCKABLE_STAGES)
    .select('id');

  if (!locked?.length) {
    // Another webhook/cycle may have claimed the row and be drafting right now.
    // Do not arm the noisy fallback unless we know there is no active queue row
    // and the contact is not already in the pending approval state.
    const { data: current } = await sb.from('contacts')
      .select('id, name, pipeline_stage, pending_linkedin_dm')
      .eq('id', contactId)
      .maybeSingle()
      .then(result => result, () => ({ data: null }));
    const currentStage = String(current?.pipeline_stage || '').trim().toLowerCase();
    if (currentStage === 'pending_dm_approval') {
      const { data: pendingRows } = await sb.from('approval_queue')
        .select('id, status, created_at')
        .eq('contact_id', contactId)
        .eq('stage', 'LinkedIn DM')
        .in('status', ['pending', 'approved', 'approved_waiting_for_window', 'sending'])
        .order('created_at', { ascending: false })
        .limit(1)
        .then(result => result, () => ({ data: null }));
      if (pendingRows?.length) {
        await reloadPendingInvestorApprovals().catch(() => {});
        return pendingRows[0];
      }
      await sb.from('contacts').update({
        pipeline_stage: 'invite_accepted',
        pending_linkedin_dm: true,
      }).eq('id', contactId).then(null, () => {});
      return { deferred: true, reason: 'draft_claim_in_progress_or_stale' };
    }
    return { deferred: true, reason: `stage_not_lockable:${current?.pipeline_stage || 'unknown'}` };
  }

  const payload = await buildLinkedInDmDraftPayload(contactId, { body });
  if (!payload) {
    await sb.from('contacts').update({
      pipeline_stage: 'invite_accepted',
      pending_linkedin_dm: true,
    }).eq('id', contactId).then(null, () => {});
    pushActivity({
      type: 'warning',
      action: 'LinkedIn DM queue stalled',
      note: `No draft payload could be built for contact ${contactId}, flagged for retry`,
    });
    return { deferred: true, reason: 'draft_payload_empty' };
  }

  const { contact, messageBody, researchSummary } = payload;

  const row = await addApprovalToQueue({
    contactId: contact.id,
    contactName: contact.name || '',
    contactEmail: contact.email || null,
    firm: contact.company_name || '',
    dealId: contact.deal_id || null,
    dealName: contact.deals?.name || null,
    stage: 'LinkedIn DM',
    body: messageBody,
    score: contact.investor_score ?? null,
    researchSummary,
    outreachMode: 'investor_outreach',
  });
  if (!row?.id) {
    await sb.from('contacts').update({
      pipeline_stage: 'invite_accepted',
      pending_linkedin_dm: true,
    }).eq('id', contact.id).then(null, () => {});
    throw new Error(`Failed to create LinkedIn approval queue row for ${contact.name || contact.id}`);
  }

  await collapseDuplicateLinkedInDmQueueRows(sb, contact.id, row.id);

  // Mark as pending_dm_approval so the orchestrator doesn't re-draft on the next cycle.
  // Stage advances to 'DM Approved' only after Dom presses approve in Telegram/dashboard.
  await sb.from('contacts').update({
    pending_linkedin_dm: false,
    pipeline_stage: 'pending_dm_approval',
  }).eq('id', contact.id);
  await reloadPendingInvestorApprovals().catch(() => {});
  pushActivity({
    type: 'linkedin',
    action: `LinkedIn DM drafted for approval: ${contact.name || ''}`,
    note: `${contact.company_name || ''}${reason ? ` · ${reason}` : ''}`,
    deal_name: contact.deals?.name || null,
    dealId: contact.deal_id || null,
  });

  // Fire Telegram notification in background (non-blocking)
  const _contact = contact;
  const _messageBody = messageBody;
  const _rowId = row.id;
  const _researchSummary = researchSummary;
  import('../approval/telegramBot.js').then(({ sendLinkedInDMForApproval }) => {
    sendLinkedInDMForApproval(
      _contact,
      _messageBody,
      _contact.deal_id || null,
      { stage: 'LinkedIn DM', researchSummary: _researchSummary, queueId: _rowId }
    ).catch(() => {});
  }).catch(() => {});

  notifyQueueUpdated();
  return row;
}

async function buildLinkedInDmDraftPayload(contactId, { body = null } = {}) {
  const sb = getSupabase();
  if (!sb || !contactId) return null;

  const { data: fetchedContact } = await sb.from('contacts')
    .select('*, deals!contacts_deal_id_fkey(*)')
    .eq('id', contactId)
    .single();
  let contact = fetchedContact;
  if (!contact) return null;

  // If contact lacks LinkedIn identifiers, try to find them via Unipile search before drafting
  if (!contact.linkedin_provider_id && !contact.unipile_chat_id && contact.name) {
    try {
      const { searchLinkedInPeople } = await import('../integrations/unipileClient.js');
      const nameParts = contact.name.trim().split(/\s+/);
      const firstName = nameParts[0] || '';
      const lastName = nameParts.slice(1).join(' ') || '';
      const searchResults = await searchLinkedInPeople({
        keywords: `${firstName} ${lastName} ${contact.company_name || ''}`.trim(),
        limit: 5,
      });
      const match = (searchResults || []).find(r => {
        const rName = String(r.name || r.full_name || '').toLowerCase();
        const contactName = contact.name.toLowerCase();
        return rName.includes(firstName.toLowerCase()) ||
          contactName.includes((r.name || '').split(' ')[0]?.toLowerCase() || '');
      });
      if (match) {
        const providerId = match.provider_id || match.providerId || null;
        const profileUrl = match.profile_url || match.linkedinUrl || match.linkedin_url || null;
        if (providerId || profileUrl) {
          await sb.from('contacts').update({
            linkedin_provider_id: providerId || contact.linkedin_provider_id,
            linkedin_url: profileUrl || contact.linkedin_url,
          }).eq('id', contact.id).then(null, () => {});
          contact = {
            ...contact,
            linkedin_provider_id: providerId || contact.linkedin_provider_id,
            linkedin_url: profileUrl || contact.linkedin_url,
          };
          console.log(`[DM DRAFT] Found LinkedIn profile for ${contact.name} via search: ${profileUrl || providerId}`);
        }
      }
    } catch (searchErr) {
      console.warn(`[DM DRAFT] LinkedIn profile search failed for ${contact.name}:`, searchErr.message);
    }
  }

  // Log if still no LinkedIn identifiers after search
  if (!contact.linkedin_provider_id && !contact.unipile_chat_id) {
    console.warn(`[buildLinkedInDmDraftPayload] contact ${contact.id} has no linkedin_provider_id or unipile_chat_id — will draft but DM send may require retry`);
  }

  let firmResearch = null;
  if (contact.firm_id) {
    const { data } = await sb.from('firms')
      .select('aum, past_investments, investment_thesis, thesis, match_rationale, justification')
      .eq('id', contact.firm_id)
      .maybeSingle()
      .then(result => result, () => ({ data: null }));
    firmResearch = data || null;
  }

  let enrichedContact = {
    ...contact,
    past_investments: contact.past_investments || firmResearch?.past_investments || '',
    investment_thesis: contact.investment_thesis || firmResearch?.investment_thesis || firmResearch?.thesis || '',
    aum_fund_size: contact.aum_fund_size || firmResearch?.aum || '',
    why_this_firm: firmResearch?.match_rationale || firmResearch?.justification || '',
    match_rationale: firmResearch?.match_rationale || '',
    justification: firmResearch?.justification || '',
  };

  const verifiedContact = await ensureVerifiedResearchForLinkedInDraft(enrichedContact, sb);
  if (!verifiedContact) {
    console.warn(`[buildLinkedInDmDraftPayload] contact ${contact.id} has only partial/missing research — deferring LinkedIn DM draft`);
    return null;
  }
  contact = verifiedContact;
  enrichedContact = verifiedContact;

  const conversationHistory = await loadLinkedInConversationContext(contact);

  const draft = body
    ? { body }
    : await draftLinkedInDM(buildLinkedInDraftContactPage(enrichedContact), null, 'intro', {
        deal: contact.deals || null,
        conversationHistory,
      });
  const messageBody = String(draft?.body || '').trim();
  if (!messageBody) return null;

  const researchSummary = [
    conversationHistory.length ? `${conversationHistory.length} prior LinkedIn message(s) loaded` : null,
    contact.notes ? truncateInline(contact.notes, 140) : null,
    enrichedContact.why_this_firm ? truncateInline(enrichedContact.why_this_firm, 180) : null,
  ].filter(Boolean).join(' · ') || null;

  return {
    contact,
    messageBody,
    researchSummary,
  };
}

export async function sendApprovedLinkedInDM({ contactId, text, queueId = null, queueItem = null }) {
  const sb = getSupabase();
  if (!sb) throw new Error('Database unavailable');

  if (queueId) {
    let claimed = null;
    try {
      ({ data: claimed } = await sb.from('approval_queue').update({
        status: 'sending',
        resolved_at: new Date().toISOString(),
      }).eq('id', queueId)
        // Accept 'pending' too — dashboard approvals fire before the async
        // updateApprovalStatus('approved') call has completed.
        .in('status', ['pending', 'approved', 'approved_waiting_for_window'])
        .select('id,status')
        .maybeSingle());
    } catch {}

    if (!claimed?.id) {
      return { skipped: true, reason: 'queue_not_claimed' };
    }
  }

  let contact = null;
  try {
    const { data } = await sb.from('contacts')
      .select('id, name, company_name, deal_id, linkedin_provider_id, unipile_chat_id, pipeline_stage, dm_sent_at')
      .eq('id', contactId).single();
    contact = data;
  } catch { /* not found */ }
  if (!contact) throw new Error('Contact not found: ' + contactId);
  if (!contact.linkedin_provider_id && !contact.unipile_chat_id) {
    throw new Error('No LinkedIn provider ID or chat ID on contact ' + contactId);
  }

  let deal = null;
  if (contact.deal_id) {
    try {
      const { data } = await sb.from('deals').select('*').eq('id', contact.deal_id).single();
      deal = data || null;
    } catch {}
  }

  let existingOutbound = null;
  try {
    ({ data: existingOutbound } = await sb.from('conversation_messages')
      .select('id, sent_at, body')
      .eq('contact_id', contact.id)
      .eq('direction', 'outbound')
      .eq('channel', 'linkedin_dm')
      .order('sent_at', { ascending: false })
      .limit(1)
      .maybeSingle());
  } catch {}

  if (contact.dm_sent_at || contact.pipeline_stage === 'DM Sent' || existingOutbound?.id) {
    const sentAt = contact.dm_sent_at || existingOutbound?.sent_at || new Date().toISOString();
    await sb.from('contacts').update({
      pipeline_stage: 'DM Sent',
      outreach_channel: 'linkedin_dm',
      dm_sent_at: sentAt,
      last_outreach_at: sentAt,
      updated_at: new Date().toISOString(),
    }).eq('id', contact.id);
    if (queueId) {
      await sb.from('approval_queue').update({
        status: 'sent',
        sent_at: sentAt,
      }).eq('id', queueId);
    }
    pushActivity({
      type: 'warning',
      action: `Duplicate LinkedIn DM suppressed: ${contact.name || 'contact'}`,
      note: 'Existing outbound LinkedIn DM already recorded',
      deal_name: deal?.name || null,
      dealId: contact.deal_id || null,
    });
    notifyQueueUpdated();
    return { skipped: true, duplicate: true, sentAt };
  }

  if (deal && !isWithinChannelWindow(deal, 'linkedin_dm')) {
    const deferredBody = sanitizeApprovalText(text || queueItem?.edited_body || queueItem?.body || '') || null;
    await sb.from('contacts').update({
      pipeline_stage: 'DM Approved',
      updated_at: new Date().toISOString(),
    }).eq('id', contact.id);
    if (queueId) {
      await sb.from('approval_queue').update({
        status: 'approved_waiting_for_window',
        edited_body: deferredBody,
        resolved_at: new Date().toISOString(),
      }).eq('id', queueId);
    }
    pushActivity({
      type: 'linkedin',
      action: `LinkedIn DM approved: ${contact.name || 'contact'}`,
      note: deal?.name
        ? `${contact.company_name || queueItem?.firm || ''} · Waiting for DM window${deal ? ` (${getNextWindowOpenForChannel(deal, 'linkedin_dm')})` : ''}`
        : `Waiting for DM window${deal ? ` (${getNextWindowOpenForChannel(deal, 'linkedin_dm')})` : ''}`,
      deal_name: deal?.name || queueItem?.deal_name || null,
      dealId: contact.deal_id || queueItem?.deal_id || null,
    });
    notifyQueueUpdated();
    return { deferred: true, nextOpen: deal ? getNextWindowOpenForChannel(deal, 'linkedin_dm') : null };
  }

  const bodyToSend = sanitizeApprovalText(text || queueItem?.edited_body || queueItem?.body || '');
  if (!bodyToSend) {
    throw new Error('LinkedIn DM body is empty');
  }

  let result;
  const sentAt = new Date().toISOString();
  try {
    if (contact.unipile_chat_id) {
      result = await sendLinkedInDMReply(contact.unipile_chat_id, bodyToSend);
    } else {
      result = await startLinkedInDM(contact.linkedin_provider_id, bodyToSend);
    }
  } catch (err) {
    if (queueId) {
      await sb.from('approval_queue').update({
        status: 'failed',
        resolved_at: new Date().toISOString(),
        edit_instructions: String(err.message || 'LinkedIn DM send failed').slice(0, 300),
      }).eq('id', queueId);
    }
    pushActivity({
      type: 'error',
      action: `LinkedIn DM failed: ${contact.name || 'contact'}`,
      note: String(err.message || 'Unknown send failure').slice(0, 200),
      deal_name: deal?.name || null,
      dealId: contact.deal_id || null,
    });
    if (contact.deal_id) {
      await sb.from('activity_log').insert({
        deal_id: contact.deal_id,
        event_type: 'LINKEDIN_DM_FAILED',
        summary: `LinkedIn DM failed for ${contact.name || 'contact'}`,
        detail: { error: String(err.message || 'Unknown send failure').slice(0, 500) },
        created_at: new Date().toISOString(),
      }).then(null, () => {});
    }
    sendTelegram(`❌ *LinkedIn DM failed* → *${contact.name || 'contact'}* (${contact.company_name || 'unknown firm'})${deal?.name ? ` · *${deal.name}*` : ''}\n_${String(err.message || 'Unknown failure').slice(0, 160)}_`).catch(() => {});
    notifyQueueUpdated();
    throw err;
  }

  const stageLabel = String(queueItem?.stage || '').toLowerCase();
  const followUpMatch = stageLabel.match(/follow[- ]up\s*(\d+)/i);
  const followUpNumber = followUpMatch ? Number(followUpMatch[1] || 1) : 0;
  // Use the deal's configured follow-up days, fall back to 7-day default
  const liFollowUpDays = Number(deal?.followup_days_li) || 7;
  const nextFollowUpDueAt = followUpNumber >= 1
    ? null
    : new Date(Date.now() + liFollowUpDays * 24 * 60 * 60 * 1000).toISOString();

  const updates = {
    pipeline_stage: 'DM Sent',
    outreach_channel: 'linkedin_dm',
    last_outreach_at: sentAt,
    dm_sent_at: sentAt,
    follow_up_count: followUpNumber,
    follow_up_due_at: nextFollowUpDueAt,
  };
  if (result?.chat_id && !contact.unipile_chat_id) updates.unipile_chat_id = result.chat_id;
  await sb.from('contacts').update(updates).eq('id', contact.id);

  await sb.from('conversation_messages').insert({
    contact_id: contact.id,
    deal_id:    contact.deal_id || null,
    direction:  'outbound',
    channel:    'linkedin_dm',
    body:       bodyToSend,
    sent_at:    sentAt,
  }).then(null, err => console.warn('[LI DM] log error:', err.message));

  if (contact.deal_id) {
    await sb.from('activity_log').insert({
      deal_id:    contact.deal_id,
      event_type: 'LINKEDIN_DM_SENT',
      summary:    `LinkedIn DM sent to ${contact.name || 'contact'}`,
      detail:     {
        channel: 'linkedin_dm',
        chat_id: result?.chat_id || contact.unipile_chat_id || null,
        message_id: result?.message_id || null,
        queue_id: queueId || queueItem?.id || null,
        firm: contact.company_name || queueItem?.firm || null,
      },
      created_at: sentAt,
    }).catch(err => console.warn('[LI DM] activity_log insert error:', err.message));
  }

  pushActivity({
    type: 'dm',
    action: `LinkedIn DM sent`,
    note: `${contact.name || 'contact'}${contact.company_name ? ` @ ${contact.company_name}` : ''}${deal?.name ? ` · ${deal.name}` : ''}`,
    deal_name: deal?.name || queueItem?.deal_name || null,
    dealId: contact.deal_id || queueItem?.deal_id || null,
  });

  if (queueId) {
    await sb.from('approval_queue').update({
      status: 'sent',
      sent_at: sentAt,
      edited_body: bodyToSend !== queueItem?.body ? bodyToSend : (queueItem?.edited_body || null),
    }).eq('id', queueId).then(null, err => console.warn('[LI DM] approval_queue update error:', err.message));
  }

  notifyQueueUpdated();
  const _dmPreview = bodyToSend ? `\n\n_${bodyToSend.slice(0, 120).replace(/[\n\r]+/g, ' ')}${bodyToSend.length > 120 ? '…' : ''}_` : '';
  const _dmDealLabel = (deal?.name || queueItem?.deal_name) ? ` · *${deal?.name || queueItem?.deal_name}*` : '';
  await sendTelegram(
    `💬 *LinkedIn DM sent* → *${contact.name || 'contact'}* (${contact.company_name || queueItem?.firm || 'unknown firm'})${_dmDealLabel}${_dmPreview}`
  ).catch(() => {});

  return result;
}

export async function sendApprovedReply({ queueId = null, queueItem = null, forceSend = false, bodyOverride = null } = {}) {
  const sb = getSupabase();
  if (!sb) throw new Error('Database unavailable');

  let item = queueItem || null;
  if (!item && queueId) {
    const { data } = await sb.from('approval_queue')
      .select('id, deal_id, contact_id, candidate_id, contact_name, contact_email, firm, body, edited_body, approved_subject, subject_a, subject, stage, channel, message_type, reply_to_id, resolved_at')
      .eq('id', queueId)
      .single();
    item = data || null;
  }
  if (!item) throw new Error('Reply approval not found');

  const contactId = item.contact_id || item.candidate_id || null;
  const channel = String(item.channel || (String(item.message_type || '').includes('linkedin') ? 'linkedin' : 'email')).toLowerCase();
  const isLinkedInReply = channel === 'linkedin';
  const bodyToSend = sanitizeApprovalText(bodyOverride || item.edited_body || item.body || '');
  if (!bodyToSend) throw new Error('Reply body is empty');

  let contact = null;
  if (contactId) {
    try {
      const { data } = await sb.from('contacts')
        .select('id, name, company_name, email, deal_id, unipile_chat_id')
        .eq('id', contactId)
        .single();
      contact = data || null;
    } catch {}
  }

  const dealId = item.deal_id || contact?.deal_id || null;
  let deal = null;
  if (dealId) {
    try {
      const { data } = await sb.from('deals').select('*').eq('id', dealId).single();
      deal = data || null;
    } catch {}
  }

  if (!forceSend && deal && !isWithinChannelWindow(deal, isLinkedInReply ? 'linkedin_dm' : 'email')) {
    if (queueId) {
      await sb.from('approval_queue').update({
        status: 'approved_waiting_for_window',
        edited_body: bodyToSend || null,
        approved_subject: !isLinkedInReply ? sanitizeApprovalText(item.approved_subject || item.subject_a || item.subject || 'our conversation') : null,
        resolved_at: new Date().toISOString(),
      }).eq('id', queueId);
    }
    pushActivity({
      type: isLinkedInReply ? 'linkedin' : 'email',
      activity_badge: getReplyActivityBadge(channel),
      action: `${isLinkedInReply ? 'LinkedIn reply received' : 'Email reply received'}: ${contact?.name || item.contact_name || 'Contact'}`,
      note: `${truncateInline(bodyToSend, 120)} · waiting for ${isLinkedInReply ? 'LinkedIn' : 'email'} window${deal ? (getWindowStatus(deal).nextOpen ? ` (${getWindowStatus(deal).nextOpen})` : '') : ''}`,
      deal_name: deal?.name || null,
      dealId,
    });
    notifyQueueUpdated();
    return { deferred: true, nextOpen: deal ? (getWindowStatus(deal).nextOpen || null) : null };
  }

  if (queueId) {
    const { data: claimed } = await sb.from('approval_queue').update({
      status: 'sending',
      edited_body: bodyToSend || null,
      approved_subject: !isLinkedInReply ? sanitizeApprovalText(item.approved_subject || item.subject_a || item.subject || 'our conversation') : null,
      resolved_at: new Date().toISOString(),
    }).eq('id', queueId)
      .in('status', ['pending', 'approved', 'approved_waiting_for_window'])
      .select('id')
      .maybeSingle();
    if (!claimed?.id) return { skipped: true, reason: 'queue_not_claimed' };
  }

  const sentAt = new Date().toISOString();
  if (isLinkedInReply) {
    const chatId = contact?.unipile_chat_id || item.reply_to_id || null;
    if (!chatId) throw new Error('No LinkedIn chat available for reply');
    const quoteId = contact?.unipile_chat_id ? (item.reply_to_id || null) : null;
    const result = await sendLinkedInReply({ chatId, message: bodyToSend, quoteId });

    if (queueId) {
      await sb.from('approval_queue').update({
        status: 'sent',
        sent_at: sentAt,
        edited_body: bodyToSend || null,
      }).eq('id', queueId);
    }

    if (contactId) {
      await sb.from('contacts').update({
        pipeline_stage: 'In Conversation',
        last_outreach_at: sentAt,
        updated_at: sentAt,
      }).eq('id', contactId);
      await sb.from('conversation_messages').insert({
        contact_id: contactId,
        deal_id: dealId,
        direction: 'outbound',
        channel: 'linkedin_dm',
        body: bodyToSend,
        sent_at: sentAt,
      }).then(null, () => {});
    }

    pushActivity({
      type: 'linkedin',
      action: `LinkedIn reply sent: ${contact?.name || item.contact_name || 'Contact'}`,
      note: `${truncateInline(bodyToSend, 120)}${contact?.company_name || item.firm ? ` · ${contact?.company_name || item.firm}` : ''}`,
      deal_name: deal?.name || null,
      dealId,
    });
    notifyQueueUpdated();
    return { sent: true, sentAt, messageId: result?.messageId || null, chatId };
  }

  const toEmail = contact?.email || item.contact_email || null;
  if (!toEmail) throw new Error('No email address found for reply');
  const subject = sanitizeApprovalText(item.approved_subject || item.subject_a || item.subject || 'our conversation');
  const inferReplyAccountId = async () => {
    const candidates = [
      process.env.UNIPILE_OUTLOOK_ACCOUNT_ID,
      process.env.UNIPILE_GMAIL_ACCOUNT_ID,
    ].filter(Boolean);
    for (const accountId of candidates) {
      try {
        const mail = await retrieveEmail(item.reply_to_id, accountId).catch(() => null);
        if (mail?.account_id || mail?.id) return mail.account_id || accountId;
      } catch {}
      try {
        const threadItems = await listEmails({ threadId: item.reply_to_id, accountId, limit: 1, metaOnly: true }).catch(() => []);
        if (threadItems?.[0]?.account_id || threadItems?.length) return threadItems[0]?.account_id || accountId;
      } catch {}
    }
    return null;
  };
  const replyAccountId = await inferReplyAccountId();
  const result = await sendEmailReply({
    to: toEmail,
    toName: contact?.name || item.contact_name || '',
    subject,
    body: bodyToSend,
    replyToProviderId: item.reply_to_id || null,
    accountId: replyAccountId,
    trackingLabel: buildEmailTrackingLabel({
      dealId,
      contactId,
      stage: item.stage || item.message_type || 'email_reply',
    }),
  });

  if (queueId) {
    await sb.from('approval_queue').update({
      status: 'sent',
      sent_at: sentAt,
      approved_subject: subject || null,
      edited_body: bodyToSend || null,
    }).eq('id', queueId);
  }

  if (contactId) {
    await sb.from('contacts').update({
      pipeline_stage: 'In Conversation',
      last_outreach_at: sentAt,
      last_email_sent_at: sentAt,
      updated_at: sentAt,
    }).eq('id', contactId);
    await sb.from('conversation_messages').insert({
      contact_id: contactId,
      deal_id: dealId,
      direction: 'outbound',
      channel: 'email',
      body: bodyToSend,
      subject,
      sent_at: sentAt,
    }).then(null, () => {});
  }

  pushActivity({
    type: 'email',
    action: `Email reply sent: ${contact?.name || item.contact_name || 'Contact'}`,
    note: `${truncateInline(bodyToSend, 120)}${contact?.company_name || item.firm ? ` · ${contact?.company_name || item.firm}` : ''}`,
    deal_name: deal?.name || null,
    dealId,
  });
  notifyQueueUpdated();
  return { sent: true, sentAt, threadId: result?.threadId || null, messageId: result?.emailId || null };
}

// ─────────────────────────────────────────────
// INIT
// ─────────────────────────────────────────────

export function initDashboard(state) {
  rocoState = state;

  app = express();
  const server = createServer(app);
  wss = new WebSocketServer({ server, path: '/ws' });

  // CORS — must be before auth so preflight OPTIONS requests pass through
  app.use((req, res, next) => {
    const origin = req.headers.origin;
    // Allow any origin (dashboard may be served from Vercel, localhost, etc.)
    res.header('Access-Control-Allow-Origin', origin || '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.header('Access-Control-Allow-Credentials', 'true');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
  });

  // Health check — no auth required (before auth middleware)
  app.get('/health', (req, res) => {
    res.json({ ok: true, status: 'running', ts: new Date().toISOString() });
  });

  // Session middleware
  app.use(session({
    secret: process.env.SESSION_SECRET || 'roco-mission-control-2026',
    resave: false,
    saveUninitialized: false,
    cookie: { secure: false, maxAge: 24 * 60 * 60 * 1000 },
  }));

  app.get('/api/admin/webhook-status', requireAuth, async (req, res) => {
    try {
      const hooks = await listWebhooks(100).catch(() => []);
      const recentLogs = await listRecentWebhookLogs(200);

      const gmailId = process.env.UNIPILE_GMAIL_ACCOUNT_ID || null;
      const outlookId = process.env.UNIPILE_OUTLOOK_ACCOUNT_ID || null;

      const latestFor = (predicate) => (recentLogs || []).find(predicate) || null;

      const gmailLog = latestFor(log => {
        const payload = log.payload || {};
        return ['mail_received', 'email.received', 'email_received'].includes(String(log.event_type || '').toLowerCase())
          && String(payload.account_id || payload.data?.account_id || '') === String(gmailId || '');
      });
      const outlookLog = latestFor(log => {
        const payload = log.payload || {};
        return ['mail_received', 'email.received', 'email_received'].includes(String(log.event_type || '').toLowerCase())
          && String(payload.account_id || payload.data?.account_id || '') === String(outlookId || '');
      });
      const liAcceptLog = latestFor(log => ['new_relation', 'connection_request_accepted'].includes(String(log.event_type || '').toLowerCase()));
      const liMsgLog = latestFor(log => ['message_received', 'message.created'].includes(String(log.event_type || '').toLowerCase()));

      res.json({
        hooks,
        latest: {
          gmail: gmailLog ? { received_at: gmailLog.received_at, event_type: gmailLog.event_type } : null,
          outlook: outlookLog ? { received_at: outlookLog.received_at, event_type: outlookLog.event_type } : null,
          linkedin_acceptance: liAcceptLog ? { received_at: liAcceptLog.received_at, event_type: liAcceptLog.event_type } : null,
          linkedin_dm: liMsgLog ? { received_at: liMsgLog.received_at, event_type: liMsgLog.event_type } : null,
        },
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/admin/webhook-logs', requireAuth, async (req, res) => {
    try {
      const limit = Math.min(Math.max(Number(req.query.limit || 100), 1), 500);
      const logs = await listRecentWebhookLogs(limit);
      const items = logs.map(log => {
        const payload = log.payload || {};
        const event = payload.data || payload;
        return {
          received_at: log.received_at || null,
          event_type: log.event_type || null,
          account_id: payload.account_id || event.account_id || null,
          source: payload.__roco_meta?.source || null,
          webhook_name: payload.webhook_name || event.webhook_name || null,
          message_id: payload.message_id || event.message_id || event.id || null,
          chat_id: payload.chat_id || event.chat_id || event.conversation_id || null,
          subject: event.subject || null,
          preview: truncateInline(
            extractUnipileMessageText(event) ||
            extractUnipileEmailBody(event) ||
            event.user_full_name ||
            '',
            200,
          ),
          payload,
        };
      });
      res.json(items);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ─── Unipile webhooks — no Basic Auth (Unipile calls these from their servers) ───

  // POST /webhook/unipile/gmail — inbound Gmail replies via Unipile
  app.post('/webhook/unipile/gmail', express.json(), async (req, res) => {
    res.json({ ok: true }); // Acknowledge immediately
    try {
      const event = req.body;
      await logWebhookReceipt(event?.type || 'mail_received', event, 'gmail', { emitActivity: false });
      console.log('[WEBHOOK/GMAIL] Received event:', event?.type || 'unknown');

      // Unipile sends a wrapper with type + payload
      const payload = event?.data || event;
      const { gmail: gmailAccountId } = getConfiguredUnipileAccountIds();
      if (!matchesConfiguredAccount(payload, gmailAccountId)) {
        console.warn('[WEBHOOK/GMAIL] Ignoring event for unexpected account_id:', payload?.account_id || event?.account_id || 'unknown');
        return;
      }
      const fromEmail   = payload?.from_attendee?.identifier || payload?.from_email || '';
      const fromName    = payload?.from_attendee?.display_name || payload?.from_name || '';
      const subject     = payload?.subject || '';
      const bodyText    = payload?.body_plain || payload?.body_text || payload?.text || payload?.message || payload?.snippet || payload?.body || payload?.html || '';
      const threadId    = payload?.thread_id || payload?.in_reply_to?.id || payload?.conversation_id || '';
      const messageId   = payload?.id || payload?.message_id || '';

      if (!fromEmail) return;

      // Skip Drafts/Sent copies and MAILER-DAEMON bounces
      const gmailRole    = String(payload?.role || '').toLowerCase();
      const gmailFolders = (payload?.folders || []).map(f => String(f).toLowerCase());
      if (gmailRole === 'drafts' || gmailRole === 'sent' || gmailFolders.includes('drafts') || gmailFolders.includes('[gmail]/sent mail')) {
        console.log('[WEBHOOK/GMAIL] Ignoring own outbound copy (role:', gmailRole || gmailFolders.join(','), ')');
        return;
      }
      const fromLowerGmail = fromEmail.toLowerCase();
      if (fromLowerGmail.startsWith('mailer-daemon') || fromLowerGmail.startsWith('postmaster')) {
        console.log('[WEBHOOK/GMAIL] Ignoring bounce/system email from:', fromEmail);
        return;
      }

      await queueInboundWithDebounce({
        fromEmail, fromName, subject, bodyText, threadId, messageId, channel: 'email', raw: payload,
      });
    } catch (err) {
      console.error('[WEBHOOK/GMAIL] Error:', err.message);
    }
  });

  // POST /webhook/unipile/outlook — inbound Outlook replies via Unipile
  app.post('/webhook/unipile/outlook', express.json(), async (req, res) => {
    res.json({ ok: true }); // Acknowledge immediately
    try {
      const event = req.body;
      await logWebhookReceipt(event?.event || event?.type || 'mail_received', event, 'outlook', { emitActivity: false });
      console.log('[WEBHOOK/OUTLOOK] Received event:', event?.event || event?.type || 'unknown');

      const payload   = event?.data || event;
      const eventType = payload?.event || event?.event || '';
      const { outlook: configuredOutlookAccountId } = getConfiguredUnipileAccountIds();
      if (!matchesConfiguredAccount(payload, configuredOutlookAccountId)) {
        console.warn('[WEBHOOK/OUTLOOK] Ignoring event for unexpected account_id:', payload?.account_id || event?.account_id || 'unknown');
        return;
      }

      // Only process inbound emails (ignore sent/moved)
      if (eventType && eventType !== 'mail_received') return;

      // Ignore Drafts/Sent folder events (our own outbound emails echo back via webhook)
      const role    = String(payload?.role || '').toLowerCase();
      const folders = (payload?.folders || []).map(f => String(f).toLowerCase());
      if (role === 'drafts' || role === 'sent' || folders.includes('drafts') || folders.includes('sent items')) {
        console.log('[WEBHOOK/OUTLOOK] Ignoring own outbound copy (role:', role || folders.join(','), ')');
        return;
      }

      const fromEmail = payload?.from_attendee?.identifier || payload?.from_email || '';
      const fromName  = payload?.from_attendee?.display_name || payload?.from_name || '';
      const subject   = payload?.subject || '';
      const bodyText  = payload?.body_plain || payload?.body_text || payload?.text || payload?.message || payload?.snippet || payload?.body || payload?.html || '';
      const threadId  = payload?.thread_id || payload?.in_reply_to?.id || payload?.conversation_id || '';
      const messageId = payload?.id || payload?.message_id || payload?.email_id || '';

      if (!fromEmail) {
        console.log('[WEBHOOK/OUTLOOK] No from_email in payload — skipping');
        return;
      }

      // Skip MAILER-DAEMON bounces and our own email address
      const { outlook: ownOutlookId } = getConfiguredUnipileAccountIds();
      const fromLower = fromEmail.toLowerCase();
      if (fromLower.startsWith('mailer-daemon') || fromLower.startsWith('microsoftexchange') || fromEmail === ownOutlookId) {
        console.log('[WEBHOOK/OUTLOOK] Ignoring bounce/system email from:', fromEmail);
        return;
      }

      console.log(`[WEBHOOK/OUTLOOK] From: ${fromName} <${fromEmail}> | Subject: ${subject}`);

      const outlookAccountId = payload?.account_id || process.env.UNIPILE_OUTLOOK_ACCOUNT_ID;
      await queueInboundWithDebounce({
        fromEmail, fromName, subject, bodyText, threadId, messageId,
        channel: 'email', emailAccountId: outlookAccountId, raw: payload,
      });
    } catch (err) {
      console.error('[WEBHOOK/OUTLOOK] Error:', err.message);
    }
  });

  // POST /webhook/unipile/email-tracking — outbound email opens/clicks via Unipile
  app.post('/webhook/unipile/email-tracking', express.json(), async (req, res) => {
    res.json({ ok: true });
    try {
      const event = req.body;
      const payload = event?.data || event || {};
      const eventType = String(payload?.event || event?.event || event?.type || '').trim().toLowerCase();
      const { gmail: gmailAccountId, outlook: outlookAccountId } = getConfiguredUnipileAccountIds();

      await insertWebhookLogRecord({
        event_type: eventType || 'mail_opened',
        payload: {
          ...(event || {}),
          __roco_meta: {
            ...((event && event.__roco_meta) || {}),
            source: 'email_tracking',
          },
        },
      }).catch(() => {});

      if (!['mail_opened', 'mail_link_clicked'].includes(eventType)) return;
      if (!matchesConfiguredAccount(payload, gmailAccountId) && !matchesConfiguredAccount(payload, outlookAccountId)) {
        console.warn('[WEBHOOK/EMAIL-TRACKING] Ignoring event for unexpected account_id:', payload?.account_id || event?.account_id || 'unknown');
        return;
      }

      const sb = getSupabase();
      if (!sb) return;

      const trackedEmail = await findTrackedEmailRecord(sb, payload);
      if (!trackedEmail) {
        console.log('[WEBHOOK/EMAIL-TRACKING] No outbound email match for tracking event:', payload?.tracking_id || payload?.email_id || 'unknown');
        return;
      }

      const context = await findActiveTrackedDealContext(sb, trackedEmail);
      if (!context?.deal?.id || !context?.contact?.id) {
        console.log('[WEBHOOK/EMAIL-TRACKING] Matched email is not tied to an active deal:', trackedEmail.id);
        return;
      }

      const { metadata, isFirstEvent } = mergeTrackedEmailMetadata(trackedEmail.metadata, payload, trackedEmail);
      await sb.from('emails').update({ metadata }).eq('id', trackedEmail.id);

      if (!isFirstEvent) return;

      const badge = getEmailTrackingBadge(eventType);
      const contactName = context.contact.name || trackedEmail.to_email || 'Contact';
      const snippet = eventType === 'mail_link_clicked'
        ? truncateInline(payload?.url || payload?.label || 'Tracked link clicked', 120)
        : truncateInline(trackedEmail.subject || payload?.label || trackedEmail.to_email || 'Tracked email opened', 120);

      pushActivity({
        type: 'email',
        activity_badge: badge,
        action: getEmailTrackingAction(eventType, contactName),
        note: [
          context.deal.name || null,
          context.contact.company_name || null,
          snippet || null,
        ].filter(Boolean).join(' · '),
        dealId: context.deal.id,
        deal_name: context.deal.name || null,
      });

      await sbLogActivity({
        dealId: context.deal.id,
        contactId: context.contact.id,
        eventType: eventType === 'mail_link_clicked' ? 'EMAIL_CLICKED' : 'EMAIL_OPENED',
        summary: `${contactName} ${eventType === 'mail_link_clicked' ? 'clicked a tracked link' : 'opened a tracked email'}`,
        detail: {
          email_id: payload?.email_id || null,
          tracking_id: payload?.tracking_id || null,
          event_id: payload?.event_id || null,
          url: payload?.url || null,
          label: payload?.label || null,
        },
      }).catch(() => {});
    } catch (err) {
      console.error('[WEBHOOK/EMAIL-TRACKING] Error:', err.message);
    }
  });

  // POST /webhook/unipile/linkedin — inbound LinkedIn DMs via Unipile (legacy route)
  app.post('/webhook/unipile/linkedin', express.json(), async (req, res) => {
    res.json({ ok: true }); // Acknowledge immediately
    try {
      const event = req.body;
      await logWebhookReceipt(event?.type || 'message_received', event);
      console.log('[WEBHOOK/LINKEDIN] Received event:', event?.type || 'unknown');

      const payload   = event?.data || event;
      const fromUrn   = payload?.sender?.attendee_provider_id || payload?.sender_id || payload?.attendee_id || '';
      const fromName  = payload?.sender?.attendee_name || payload?.sender_name || payload?.attendee_name || '';
      const bodyText  = payload?.message || payload?.text || payload?.body_plain || payload?.body || payload?.content || '';
      const chatId    = payload?.chat_id || payload?.conversation_id || '';
      const messageId = payload?.id || payload?.message_id || '';

      if (!fromUrn && !fromName) return;

      await queueInboundWithDebounce({
        fromUrn, fromName, bodyText, chatId, messageId, channel: 'linkedin', raw: payload,
      });
    } catch (err) {
      console.error('[WEBHOOK/LINKEDIN] Error:', err.message);
    }
  });

  // POST /webhook/unipile/linkedin/messages — inbound LinkedIn messages (registered in unipileSetup.js)
  app.post('/webhook/unipile/linkedin/messages', express.json(), async (req, res) => {
    res.json({ ok: true });
    try {
      const event   = req.body;
      await logWebhookReceipt(event?.type || 'message_received', event);
      const payload = event?.data || event;

      // Unipile message_received payload: sender.attendee_provider_id, sender.attendee_name, message (top-level)
      const fromProvId   = payload?.sender?.attendee_provider_id || payload?.sender_id || payload?.attendee_id || '';
      const fromName     = payload?.sender?.attendee_name || payload?.sender_name || payload?.attendee_name || '';
      const fromLinkedin = fromProvId ? `https://linkedin.com/in/${fromProvId}` : null;
      const messageText  = payload?.message || payload?.text || payload?.body_plain || payload?.body || payload?.content || '';
      const chatId       = payload?.chat_id || payload?.conversation_id || '';
      const messageId    = payload?.id || payload?.message_id || '';

      console.log(`[WEBHOOK/LINKEDIN/MSG] From: ${fromName} | Chat: ${chatId}`);

      // Persist to linkedin_messages table
      const sb = getSupabase();
      if (sb && messageText) {
        await sb.from('linkedin_messages').insert({
          chat_id:             chatId || null,
          message_text:        messageText,
          from_name:           fromName || null,
          from_linkedin_url:   fromLinkedin || null,
          from_provider_id:    fromProvId || null,
          created_at:          new Date().toISOString(),
        }).then(null, e => console.warn('[WEBHOOK/LINKEDIN/MSG] DB insert failed:', e.message));
      }

      // Route through debounce batcher to draft response
      if (fromName || fromProvId) {
        await queueInboundWithDebounce({
          fromUrn: fromProvId, fromName, bodyText: messageText, chatId, messageId, channel: 'linkedin', raw: payload,
        });
      }
    } catch (err) {
      console.error('[WEBHOOK/LINKEDIN/MSG] Error:', err.message);
    }
  });

  // POST /webhook/unipile/linkedin/relations — connection accepted notification
  app.post('/webhook/unipile/linkedin/relations', express.json(), async (req, res) => {
    res.json({ ok: true });
    try {
      const event   = req.body;
      if (isEmptyWebhookPayload(event)) {
        console.warn('[WEBHOOK/LINKEDIN/REL] Empty request body — likely provider validation ping');
        return;
      }
      await logWebhookReceipt(event?.type || 'new_relation', event);
      // Log full raw payload to confirm Unipile field structure
      console.log('[WEBHOOK/LINKEDIN/REL] Raw payload:', JSON.stringify(event));

      // Unipile new_relation payload is flat: user_provider_id, user_full_name, user_public_identifier, user_profile_url
      const payload    = event?.data || event;
      const providerId = payload?.user_provider_id || payload?.provider_id || payload?.attendee?.provider_id || '';
      const name       = payload?.user_full_name || payload?.display_name || payload?.attendee?.display_name || '';
      const publicId   = payload?.user_public_identifier || '';
      const profileUrl = payload?.user_profile_url || '';
      const contactLabel = name || publicId || providerId || 'Unknown contact';

      console.log(`[WEBHOOK/LINKEDIN/REL] Connection accepted: ${name} (${providerId})`);

      if (!providerId && !name) {
        console.warn('[WEBHOOK/LINKEDIN/REL] Empty payload — cannot identify person, skipping');
        pushActivity({
          type: 'error',
          action: 'LinkedIn acceptance received',
          note: 'Payload was empty, so no contact or deal could be matched',
        });
        return;
      }

      const sb = getSupabase();
      if (!sb) return;

      // Look up the contact in our pipeline — include paused deals so we still record the accepted connection.
      // We check paused state later and hold the DM until resume, but always update the stage.
      let contactQuery = sb.from('contacts')
        .select('id, name, company_name, deal_id, pipeline_stage, response_received, linkedin_provider_id, linkedin_url, updated_at, deals!inner(id, name, status, paused)')
        .eq('deals.status', 'ACTIVE');

      // Match by provider_id first (most reliable), then URL slug, then name as last resort.
      // Never use a wildcard-only pattern — if we have nothing specific, skip entirely.
      if (providerId) {
        // Use provider_id exact match OR full profile URL match — never a partial wildcard on provider_id
        const orClauses = [`linkedin_provider_id.eq.${providerId}`];
        if (profileUrl) orClauses.push(`linkedin_url.eq.${profileUrl}`);
        else if (publicId) orClauses.push(`linkedin_url.ilike.%${publicId}%`);
        contactQuery = contactQuery.or(orClauses.join(','));
      } else if (publicId) {
        // Slug-only match — specific enough to be safe
        contactQuery = contactQuery.ilike('linkedin_url', `%${publicId}%`);
      } else if (name) {
        // Name only as absolute last resort
        contactQuery = contactQuery.ilike('name', `%${name}%`);
      } else {
        // Nothing to match on — skip entirely
        console.warn('[WEBHOOK/LINKEDIN/REL] No usable identifiers to look up contact — skipping');
        return;
      }

      const { data: contacts } = await contactQuery.limit(10);
      const contact = pickBestInvestorContact(contacts || [], { requireActiveDeal: true });

      if (contact) {
        // ── Investor pipeline contact ──────────────────────────────────────────
        const dealStatus = contact.deals?.status;
        const dealPaused = contact.deals?.paused;

        const ALREADY_RESPONDED = ['In Conversation', 'Replied', 'Meeting Booked', 'Meeting Scheduled'];
        const alreadyResponded = ALREADY_RESPONDED.includes(contact.pipeline_stage) || contact.response_received;

        // Always record the accepted connection — even if deal is paused.
        // The stage update ensures the orchestrator picks it up on resume.
        if (!alreadyResponded) {
          const patch = preservesAcceptedProgress(contact.pipeline_stage)
            ? {}
            : { pipeline_stage: 'invite_accepted' };
          if (Object.keys(patch).length) {
            await sb.from('contacts').update(patch).eq('id', contact.id);
          }
        } else {
          console.log(`[WEBHOOK/LINKEDIN/REL] ${contact.name} already responded (${contact.pipeline_stage}) — not overwriting stage`);
        }

        const pausedNote = dealPaused ? ' (deal paused — DM will be queued on resume)' : '';
        pushActivity({
          type: 'linkedin',
          activity_badge: 'accepted',
          action: `${contact.name} accepted your connection request`,
          note: alreadyResponded
            ? `Matched to deal ${contact.deals?.name || 'Unknown deal'} · already in conversation, no DM will be sent.`
            : `Matched to deal ${contact.deals?.name || 'Unknown deal'} · pipeline stage -> invite_accepted.${pausedNote}`,
          dealId: contact.deal_id,
          deal_name: contact.deals?.name || null,
        });
        await sbLogActivity({
          dealId: contact.deal_id,
          contactId: contact.id,
          eventType: 'INVITE_ACCEPTED',
          summary: `${contact.name} accepted connection request${dealPaused ? ' (deal paused)' : ''}`,
        }).catch(() => {});
        console.log(`[WEBHOOK/LINKEDIN/REL] ${contact.name} advanced to invite_accepted (investor pipeline)${dealPaused ? ' — deal paused, DM held for resume' : ''}`);

        if (!alreadyResponded) {
          queueLinkedInDmApproval(contact.id, {
            reason: dealPaused ? 'accepted_while_paused' : 'accepted_via_legacy_route',
          }).catch(err => console.warn('[WEBHOOK/LINKEDIN/REL] LinkedIn DM draft queue failed:', err.message));
        }

      } else {
        // ── Sourcing pipeline — check company_contacts ─────────────────────────
        let sourcingContactQuery = sb.from('company_contacts')
          .select('*, target_companies(company_name, match_score, match_tier, product_description, intent_signals_found, why_matches, sector, geography), sourcing_campaigns(name, firm_name, firm_type, investment_thesis, deal_type, investment_size)')
          .eq('pipeline_stage', 'invite_sent')
          .eq('linkedin_invite_accepted', false);

        if (providerId) {
          const orClauses = [`linkedin_provider_id.eq.${providerId}`];
          if (profileUrl) orClauses.push(`linkedin_url.eq.${profileUrl}`);
          else if (publicId) orClauses.push(`linkedin_url.ilike.%${publicId}%`);
          sourcingContactQuery = sourcingContactQuery.or(orClauses.join(','));
        } else if (publicId) {
          sourcingContactQuery = sourcingContactQuery.ilike('linkedin_url', `%${publicId}%`);
        } else if (name) {
          sourcingContactQuery = sourcingContactQuery.ilike('name', `%${name}%`);
        } else {
          console.log(`[WEBHOOK/LINKEDIN/REL] No identifiers — cannot look up sourcing contact`);
          return;
        }

        const { data: sourcingContacts } = await sourcingContactQuery.limit(1);
        const sc = sourcingContacts?.[0];

        if (!sc) {
          console.log(`[WEBHOOK/LINKEDIN/REL] ${name || providerId} not in any active pipeline — ignoring`);
          pushActivity({
            type: 'excluded',
            action: `LinkedIn acceptance received: ${contactLabel}`,
            note: 'Did not match any active deals or sourcing campaigns',
          });
          return;
        }

        // Mark invite accepted
        await sb.from('company_contacts').update({
          linkedin_invite_accepted: true,
          pipeline_stage: 'invite_accepted',
          updated_at: new Date().toISOString(),
        }).eq('id', sc.id).then(null, () => {});

        console.log(`[WEBHOOK/LINKEDIN/REL] Sourcing contact ${sc.name} accepted invite — queuing LinkedIn DM`);
        pushActivity({
          type: 'linkedin',
          activity_badge: 'accepted',
          action: `${sc.name} accepted connection request (sourcing)`,
          note: `Campaign: ${sc.sourcing_campaigns?.name || 'Unknown'} — queuing LinkedIn DM draft`,
        });

        // Queue LinkedIn DM draft for Telegram approval
        try {
          const { constructCompanySourcingMessage } = await import('../sourcing/messageConstructorSourcing.js');
          const company = sc.target_companies;
          const campaign = sc.sourcing_campaigns;
          if (!company || !campaign) {
            console.warn(`[WEBHOOK/LINKEDIN/REL] Missing company/campaign data for ${sc.name} — skipping DM queue`);
            return;
          }

          const draft = await constructCompanySourcingMessage(sc, company, sc.campaign_id, 'linkedin_dm', null);
          if (!draft) return;

          const researchBasis = company.intent_signals_found || company.why_matches || `${company.sector} in ${company.geography}`;

          // Check for duplicate pending queue entry
          const { data: existingDm } = await sb.from('approval_queue')
            .select('id').eq('company_contact_id', sc.id).eq('status', 'pending').limit(1);
          if (existingDm?.length > 0) {
            console.log(`[WEBHOOK/LINKEDIN/REL] DM already queued for ${sc.name} — skipping duplicate`);
            return;
          }

          const { data: queueRow } = await sb.from('approval_queue').insert([{
            contact_id:         sc.id,
            contact_name:       sc.name,
            firm:               company.company_name,
            stage:              'LinkedIn DM',
            score:              company.match_score || 0,
            subject_a:          null,
            subject_b:          null,
            body:               draft.body,
            research_summary:   researchBasis,
            status:             'pending',
            campaign_id:        sc.campaign_id,
            company_contact_id: sc.id,
            outreach_mode:      'company_sourcing',
            created_at:         new Date().toISOString(),
          }]).select().single().then(r => r, () => ({ data: null }));

          const companyObj = { ...company, id: sc.company_id };
          const campaignObj = { ...campaign, id: sc.campaign_id };
          await sendSourcingDraftToTelegram(sc, companyObj, campaignObj, draft, researchBasis, queueRow?.id);
        } catch (dmErr) {
          console.error(`[WEBHOOK/LINKEDIN/REL] Failed to queue LinkedIn DM for ${sc.name}:`, dmErr.message);
        }
      }
    } catch (err) {
      console.error('[WEBHOOK/LINKEDIN/REL] Error:', err.message);
    }
  });

  // POST /webhooks/unipile/messages — consolidated LinkedIn events (messages + relation accepted)
  // No auth — Unipile calls this from their servers. Always 200 immediately.
  const unipileMessageWebhookHandler = async (req, res) => {
    res.status(200).json({ ok: true });

    const event = req.body;

    try {
      if (isEmptyWebhookPayload(event)) {
        console.warn('[WEBHOOKS/UNIPILE] Empty request body — likely provider validation ping');
        return;
      }
      const payload = event?.data || event || {};
      const eventType = String(event?.type || event?.event_type || event?.event || '').toLowerCase();
      const { linkedin: linkedinAccountId } = getConfiguredUnipileAccountIds();
      const isLinkedInEvent =
        ['message_received', 'message.created', 'new_relation', 'connection_request_accepted'].includes(eventType) ||
        Boolean(payload?.sender?.attendee_provider_id || payload?.user_provider_id || payload?.user_public_identifier);

      if (isLinkedInEvent && !matchesConfiguredAccount(payload, linkedinAccountId)) {
        await logWebhookReceipt(eventType || 'unknown', {
          ...event,
          __roco_meta: {
            ...(event?.__roco_meta || {}),
            ignored_reason: 'unexpected_linkedin_account',
          },
        }, 'unipile_messages').catch(() => {});
        console.warn('[WEBHOOKS/UNIPILE] Ignoring LinkedIn event for unexpected account_id:', payload?.account_id || event?.account_id || 'unknown');
        return;
      }
      const mode = await processUnipileMessageEvent(event);
      if (mode === 'email') {
        const payload = event?.data || event || {};
        const fromEmail = payload?.from_attendee?.identifier || payload?.from?.email || payload?.from_email || payload?.from || '';
        const fromName  = payload?.from_attendee?.display_name || payload?.from_name || '';
        const subject   = payload?.subject || '';
        const rawBody   = payload?.body_plain || payload?.body_text || payload?.text || payload?.message || payload?.snippet || payload?.body || payload?.html || '';
        const body      = stripHtml(rawBody);
        const threadId  = payload?.thread_id || payload?.threadId || payload?.conversation_id || payload?.in_reply_to?.id || '';
        const messageId = payload?.id || payload?.message_id || payload?.email_id || '';
        console.log(`[WEBHOOKS/UNIPILE] mail_received from: ${fromEmail} subject: ${subject}`);

        if (fromEmail) {
          await queueInboundWithDebounce({
            fromEmail,
            fromName,
            subject,
            bodyText: body,
            threadId,
            messageId,
            channel: 'email',
            raw: payload,
          });
        }
      }
    } catch (err) {
      console.error('[WEBHOOKS/UNIPILE] Handler error:', err.message);
    }
  };
  app.post('/webhooks/unipile/messages', express.json(), unipileMessageWebhookHandler);
  // Backward-compatibility aliases for older Unipile webhook registrations.
  app.post('/webhook/unipile/linkedin/messages', express.json(), unipileMessageWebhookHandler);
  app.post('/webhook/unipile/linkedin/relations', express.json(), unipileMessageWebhookHandler);

  // Auth middleware
  const dashboardUser = (process.env.DASHBOARD_USER || 'admin').trim();
  const dashboardPass = (process.env.DASHBOARD_PASS || 'roco2026').trim();
  const dashboardDisplayName = (process.env.DASHBOARD_DISPLAY_NAME || dashboardUser).trim();

  app.use(requireAuth);

  // Login endpoint
  app.post('/login', express.json(), (req, res) => {
    const { username, password } = req.body || {};
    if (username === dashboardUser && password === dashboardPass) {
      req.session.authenticated = true;
      req.session.username = username;
      req.session.displayName = dashboardDisplayName;
      const sanitised = dashboardDisplayName.toLowerCase().replace(/[^a-z0-9]/g, '_');
      generateWelcomeAudio(dashboardDisplayName).catch(console.error);
      res.json({
        success: true,
        displayName: dashboardDisplayName,
        welcomeAudioPath: `/audio/welcome_${sanitised}.mp3`,
      });
    } else {
      res.status(401).json({ error: 'Invalid credentials' });
    }
  });

  // Get current user
  app.get('/api/me', (req, res) => {
    if (!req.session?.authenticated) return res.status(401).json({ error: 'Unauthorized' });
    res.json({ username: req.session.username, displayName: req.session.displayName });
  });

  // Logout
  app.post('/logout', (req, res) => {
    req.session.destroy(() => {});
    res.json({ success: true });
  });

  // Serve welcome page at root (no auth)
  app.get('/', (req, res) => {
    if (req.session?.authenticated) return res.redirect('/dashboard');
    res.sendFile('welcome.html', { root: path.join(__dirname, 'public') });
  });

  // Main dashboard (requires auth)
  app.get('/dashboard', (req, res) => {
    res.sendFile('index.html', { root: path.join(__dirname, 'public') });
  });

  app.use(express.json());
  app.use(express.static(path.join(__dirname, '../public')));
  app.use(express.static(path.join(__dirname, 'public')));
  registerRoutes(app);

  // WebSocket — push live activity
  wss.on('connection', (ws) => {
    info('Dashboard WebSocket client connected');
    const sendInit = async () => {
      if (!activityFeed.length) {
        await hydrateActivityFeed(100).catch(() => {});
      }
      ws.send(JSON.stringify({ type: 'init', feed: activityFeed.slice(-100) }));
    };
    sendInit().catch(() => {
      ws.send(JSON.stringify({ type: 'init', feed: activityFeed.slice(-100) }));
    });
    ws.on('error', () => {});
  });

  const port = process.env.PORT || 3000;
  server.listen(port, '0.0.0.0', () => {
    info(`Mission Control dashboard running at http://0.0.0.0:${port}`);
  });

  // Start API health checks
  startHealthChecks();
  hydrateActivityFeed(100).catch(() => {});

  // Recreate LinkedIn webhooks — auto-discover Cloudflare tunnel URL if running
  (async () => {
    let serverBaseUrl = getConfiguredServerBaseUrl();
    if (!serverBaseUrl) {
      try {
        const r = await fetch('http://localhost:20241/quicktunnel');
        const d = await r.json();
        if (d?.hostname) {
          serverBaseUrl = `https://${d.hostname}`;
          console.log('[BOOT] Cloudflare tunnel URL auto-discovered:', serverBaseUrl);
        }
      } catch {}
    }
    if (!serverBaseUrl) {
      console.warn('[BOOT] LinkedIn webhook recreation skipped: no Railway/public base URL configured');
      return;
    }
    recreateLinkedInWebhooks(serverBaseUrl).catch(err =>
      console.warn('[BOOT] LinkedIn webhook recreation failed:', err.message)
    );
  })();

  // Default to webhook-only mode to avoid resurfacing historical chat messages as new replies.
  if (isTruthyEnvFlag(process.env.ENABLE_UNIPILE_INBOX_MONITOR)) {
    startInboxMonitor(handleLiMsg, pushActivity, { draftContextualReply });
  } else {
    console.log('[INBOX MONITOR] Disabled — webhook-only mode is active');
  }

  // Register Telegram webhook for inline button callbacks
  import('../core/telegram.js').then(m => m.registerTelegramWebhook()).catch(e => console.warn('[TELEGRAM] Webhook reg error:', e.message));

  // Clear old cached welcome audio so it regenerates if the voice ID changed
  const audioDir = path.join(__dirname, 'public', 'audio');
  try {
    if (fs.existsSync(audioDir)) {
      fs.readdirSync(audioDir).filter(f => f.startsWith('welcome_')).forEach(f => {
        try { fs.unlinkSync(path.join(audioDir, f)); } catch {}
      });
    }
  } catch {}

  // Pre-generate ElevenLabs welcome audio for the configured display name
  generateWelcomeAudio(dashboardDisplayName).catch(console.error);

  // Backfill existing investors into named lists (one-time, skips if lists already exist)
  backfillInvestorLists().catch(e => console.warn('[LISTS] Backfill error:', e.message));

  // Backfill contact_type classification for investors_db rows that predate this feature
  import('../core/investorDatabaseImporter.js')
    .then(m => m.backfillContactTypes())
    .catch(e => console.warn('[BACKFILL] contact_type backfill error:', e.message));

  return { app, server, wss };
}

async function backfillInvestorLists() {
  const sb = getSupabase();
  if (!sb) return;

  const { count } = await sb.from('investor_lists').select('*', { count: 'exact', head: true });
  if (count > 0) return; // already done

  console.log('[LISTS] Backfilling investor lists from source files...');

  const { data: investors } = await sb.from('investors_db')
    .select('source_file, investor_category')
    .not('source_file', 'is', null);

  const fileGroups = {};
  (investors || []).forEach(inv => {
    const key = inv.source_file || 'Unknown';
    if (!fileGroups[key]) fileGroups[key] = 0;
    fileGroups[key]++;
  });

  for (const [sourceFile, cnt] of Object.entries(fileGroups)) {
    const listName = (sourceFile.replace(/\.[^/.]+$/, '').replace(/_/g, ' ').replace(/-/g, ' ').trim()) || sourceFile;
    try {
      const { data: newList } = await sb.from('investor_lists').insert({
        name: listName, list_type: 'standard', source: 'pitchbook',
      }).select().single();
      if (newList?.id) {
        await sb.from('investors_db').update({ list_id: newList.id, list_name: newList.name }).eq('source_file', sourceFile);
        console.log(`[LISTS] Created list "${newList.name}" (${cnt} investors)`);
      }
    } catch (e) { console.warn(`[LISTS] Skipped "${listName}":`, e.message); }
  }

  // Catch-all for investors with no source_file
  try {
    const { data: generalList } = await sb.from('investor_lists').insert({
      name: 'General Database', list_type: 'standard', source: 'pitchbook',
    }).select().single();
    if (generalList?.id) {
      await sb.from('investors_db').update({ list_id: generalList.id, list_name: 'General Database' }).is('list_id', null);
    }
  } catch (e) { console.warn('[LISTS] General Database list error:', e.message); }

  console.log('[LISTS] Backfill complete');
}

// ─────────────────────────────────────────────
// ELEVENLABS WELCOME AUDIO
// ─────────────────────────────────────────────

async function generateWelcomeAudio(displayName) {
  const apiKey = process.env.ELEVENLABS_API_KEY;
  if (!apiKey) {
    info('[VOICE] ELEVENLABS_API_KEY not set — skipping welcome audio generation');
    return null;
  }

  const sanitised = displayName.toLowerCase().replace(/[^a-z0-9]/g, '_');
  const outputPath = path.join(__dirname, 'public', 'audio', `welcome_${sanitised}.mp3`);

  if (fs.existsSync(outputPath)) {
    info(`[VOICE] Welcome audio already cached for ${displayName}`);
    return `/audio/welcome_${sanitised}.mp3`;
  }

  info(`[VOICE] Generating welcome audio for ${displayName}...`);

  try {
    const response = await fetch('https://api.elevenlabs.io/v1/text-to-speech/0pa5K4pOrbnP5VS5eH6k', {
      method: 'POST',
      headers: {
        'xi-api-key': apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        text: `Welcome back, ${displayName}.`,
        model_id: 'eleven_flash_v2_5',
        voice_settings: {
          stability: 0.4,
          similarity_boost: 0.85,
          style: 0.35,
          use_speaker_boost: true,
        },
      }),
    });

    if (!response.ok) {
      error(`[VOICE] ElevenLabs error: ${response.status} ${await response.text()}`);
      return null;
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    fs.mkdirSync(path.join(__dirname, 'public', 'audio'), { recursive: true });
    fs.writeFileSync(outputPath, buffer);
    info(`[VOICE] Welcome audio cached at ${outputPath}`);
    return `/audio/welcome_${sanitised}.mp3`;
  } catch (err) {
    error(`[VOICE] Failed to generate welcome audio: ${err.message}`);
    return null;
  }
}

// ─────────────────────────────────────────────
// ROUTES
// ─────────────────────────────────────────────

function registerRoutes(app) {

  // GET /api/state — full system state
  app.get('/api/state', (req, res) => {
    const state = readState();
    // Also merge live rocoState values
    if (rocoState) {
      state.emailsSent = rocoState.emailsSent || 0;
      state.startedAt = rocoState.startedAt;
      state.deal = rocoState.deal;
    }
    res.json(state);
  });

  // POST /api/toggle — toggle a system switch
  app.post('/api/toggle', async (req, res) => {
    const { key, value } = req.body;
    // Accept both camelCase variants (followUpEnabled from HTML, followupEnabled legacy)
    const allowed = ['outreachEnabled', 'followupEnabled', 'followUpEnabled',
                     'enrichmentEnabled', 'researchEnabled', 'linkedinEnabled', 'rocoStatus'];
    if (!allowed.includes(key)) {
      return res.status(400).json({ error: 'Invalid toggle key' });
    }

    // Normalise followUpEnabled → followupEnabled internally
    const normKey = key === 'followUpEnabled' ? 'followupEnabled' : key;

    const state = readState();

    if (normKey === 'rocoStatus') {
      // Accept explicit value or flip
      if (value !== undefined) {
        state.rocoStatus = value === true || value === 'ACTIVE' ? 'ACTIVE' : 'PAUSED';
      } else {
        state.rocoStatus = state.rocoStatus === 'ACTIVE' ? 'PAUSED' : 'ACTIVE';
      }
      if (rocoState) rocoState.status = state.rocoStatus;
    } else {
      // Use explicit value if provided, otherwise flip
      state[normKey] = value !== undefined ? !!value : !state[normKey];
    }

    writeState(state);

    // Persist to Supabase so the orchestrator picks it up
    saveSessionState(state).catch(() => {});

    broadcastToAll({ type: 'STATE_UPDATE', state });
    pushActivity({ type: 'SYSTEM', action: 'Setting changed', note: `${normKey} → ${state[normKey] ?? state.rocoStatus}` });

    res.json({ key: normKey, newValue: state[normKey] ?? state.rocoStatus, timestamp: new Date().toISOString() });
  });

  // Legacy pause/resume — kept for backward compat
  app.post('/api/pause', (req, res) => {
    const state = readState();
    state.rocoStatus = 'PAUSED';
    writeState(state);
    if (rocoState) rocoState.status = 'PAUSED';
    broadcastToAll({ type: 'STATE_UPDATE', state });
    res.json({ ok: true, status: 'PAUSED' });
  });

  app.post('/api/resume', (req, res) => {
    const state = readState();
    state.rocoStatus = 'ACTIVE';
    writeState(state);
    if (rocoState) rocoState.status = 'ACTIVE';
    broadcastToAll({ type: 'STATE_UPDATE', state });
    res.json({ ok: true, status: 'ACTIVE' });
  });

  // Legacy status endpoint
  app.get('/api/status', (req, res) => {
    res.json({
      status: rocoState?.status || 'UNKNOWN',
      deal: rocoState?.deal || null,
      emailsSent: rocoState?.emailsSent || 0,
      startedAt: rocoState?.startedAt || null,
    });
  });

  // GET /api/activity — paginated, persistent activity from DB
  app.get('/api/activity', requireAuth, async (req, res) => {
    try {
      const sb     = getSupabase();
      const page   = Math.max(1, parseInt(req.query.page || '1'));
      const limit  = 50;
      const offset = (page - 1) * limit;
      const dealId = req.query.deal_id || req.query.dealId || null;

      if (!sb) {
        // Fallback to in-memory feed
        const items = activityFeed.slice().reverse();
        return res.json({ events: items.slice(offset, offset + limit), total: items.length, page, pages: Math.ceil(items.length / limit), has_more: offset + limit < items.length });
      }

      let query = sb.from('activity_log')
        .select('*', { count: 'exact' })
        .order('created_at', { ascending: false })
        .range(offset, offset + limit - 1);
      if (dealId) query = query.eq('deal_id', dealId);

      const { data, count, error: dbErr } = await query;
      if (dbErr) {
        // DB error — fall back to in-memory feed
        const items = activityFeed.slice().reverse();
        return res.json({ events: items.slice(offset, offset + limit), total: items.length, page, pages: Math.ceil(items.length / limit) || 1, has_more: offset + limit < items.length });
      }

      const liveItems = activityFeed
        .filter(item => !dealId || String(item.deal_id || item.dealId || '') === String(dealId))
        .slice()
        .reverse();
      const merged = offset === 0
        ? mergeActivityEntries(data || [], liveItems)
        : (data || []);
      const mergedTotal = offset === 0
        ? Math.max(Number(count || 0), merged.length)
        : Number(count || 0);

      res.json({
        events:   merged.slice(0, limit),
        total:    mergedTotal,
        page,
        pages:    Math.ceil(mergedTotal / limit) || 1,
        has_more: offset + limit < mergedTotal,
      });
    } catch (err) {
      // Any error — return in-memory feed
      const items = activityFeed.slice().reverse();
      res.json({ events: items.slice(0, 50), total: items.length, page: 1, pages: 1, has_more: false });
    }
  });

  // GET /api/activity/recent — last 20 events, no pagination (for overview widget)
  app.get('/api/activity/recent', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.json(activityFeed.slice(-20).reverse());
      const { data } = await sb.from('activity_log')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(20);
      const merged = mergeActivityEntries(data || [], activityFeed.slice().reverse());
      res.json(merged.slice(0, 20));
    } catch {
      res.json(activityFeed.slice(-20).reverse());
    }
  });

  app.get('/api/ops/alerts', requireAuth, async (req, res) => {
    try {
      const [webhookIssues, providerLimitPauses] = await Promise.all([
        listUnmatchedWebhookReceipts(100),
        listLinkedInProviderLimitPauses(100),
      ]);

      res.json({
        webhook_issues: webhookIssues.slice(0, 12).map(row => ({
          event_type: row.event_type || 'unknown',
          received_at: row.received_at || row.created_at || null,
          source: row?.payload?.__roco_meta?.source || null,
          match_status: row?.payload?.__roco_meta?.match_status || null,
          note: row?.payload?.__roco_meta?.match_note || null,
          payload: row.payload || {},
        })),
        provider_limit_pauses: providerLimitPauses.slice(0, 12),
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/pipeline — active pipeline contacts from Supabase, filtered by deal
  app.get('/api/pipeline', async (req, res) => {
    try {
      const { dealId } = req.query;
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });

      const EXCLUDED = dealId
        ? ['Deleted — Do Not Contact', 'Suppressed — Opt Out']
        : ['Archived', 'Skipped', 'Deleted — Do Not Contact', 'Suppressed — Opt Out'];

      const baseSelect = 'id, name, company_name, job_title, linkedin_url, investor_score, pipeline_stage, enrichment_status, email, phone, notes, updated_at, invite_sent_at, last_email_sent_at, dm_sent_at, last_outreach_at, last_reply_at, deal_id, conversation_state, intent_history, follow_up_due_at, follow_up_count, response_received';
      const richSelect = baseSelect + ', last_intent, last_intent_label';

      const buildQuery = (select) => {
        let q = sb.from('contacts')
          .select(select)
          .not('pipeline_stage', 'in', `(${EXCLUDED.map(s => `"${s}"`).join(',')})`)
          .order('investor_score', { ascending: false })
          .limit(500);
        if (dealId) q = q.eq('deal_id', dealId);
        return q;
      };

      let { data, error: dbErr } = await buildQuery(richSelect);
      // Fall back if sentiment columns not yet added via SQL migration
      if (dbErr?.code === '42703') {
        ({ data, error: dbErr } = await buildQuery(baseSelect));
      }
      if (dbErr) throw new Error(dbErr.message);

      // Build deal name lookup
      const dealIds = [...new Set((data || []).map(c => c.deal_id).filter(Boolean))];
      const dealNames = {};
      const dealRowsById = {};
      if (dealIds.length) {
        const { data: dealRows } = await sb.from('deals').select('id, name, followup_days_li, followup_days_email').in('id', dealIds);
        (dealRows || []).forEach(d => {
          dealNames[d.id] = d.name;
          dealRowsById[d.id] = d;
        });
      }

      let globalSequence = null;
      try {
        const { data } = await sb.from('outreach_sequence').select('steps').limit(1).single();
        globalSequence = data || null;
      } catch {}

      const dealSequences = {};
      if (dealIds.length) {
        const { data: seqRows } = await sb.from('deal_sequence').select('deal_id, steps').in('deal_id', dealIds);
        (seqRows || []).forEach(row => { dealSequences[row.deal_id] = row; });
      }

      const backfills = [];

      const contactDealContext = await buildContactDealContextMap(sb);
      const mapped = (data || []).map(c => {
        const dealContext = contactDealContext.get(c.id) || {};
        const dealRow = dealRowsById[c.deal_id] || null;
        const sequence = dealSequences[c.deal_id] || globalSequence;
        const scheduledFollowUpAt = computeBackfilledScheduledFollowUpAt(c, dealRow, sequence);
        if (scheduledFollowUpAt && c.follow_up_due_at !== scheduledFollowUpAt) {
          backfills.push({ id: c.id, follow_up_due_at: scheduledFollowUpAt });
        }
        return {
        id: c.id,
        name: c.name,
        firm: c.company_name || '',
        jobTitle: c.job_title || '',
        score: c.investor_score,
        stage: (c.pipeline_stage === 'Inactive' && String(c.conversation_state || '').startsWith('conversation_ended'))
          ? 'Closed'
          : c.pipeline_stage,
        lastContacted: c.last_outreach_at || c.last_email_sent_at || c.invite_sent_at || c.updated_at,
        lastReplyAt: c.last_reply_at,
        scheduledFollowUpAt,
        followUpCount: c.follow_up_count || 0,
        enrichmentStatus: c.enrichment_status,
        email: c.email,
        phone: c.phone,
        linkedinUrl: c.linkedin_url,
        notes: c.notes,
        deal_id: c.deal_id,
        dealName: dealNames[c.deal_id] || '',
        projectName: dealContext.projectName || dealNames[c.deal_id] || '',
        activeDealName: dealContext.activeDealName || '',
        activeDealId: dealContext.activeDealId || null,
        deals: dealContext.deals || [],
        dealNamesText: dealContext.dealNamesText || '',
        conversationState: c.conversation_state,
        lastIntent: c.last_intent,
        lastIntentLabel: c.last_intent_label,
        intentHistory: c.intent_history || [],
      };
      });

      for (const backfill of backfills.slice(0, 100)) {
        await sb.from('contacts').update({ follow_up_due_at: backfill.follow_up_due_at }).eq('id', backfill.id);
      }
      res.json(mapped);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/contacts/:id/conversation — messages + intent history for a contact
  app.get('/api/contacts/:id/conversation', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { id } = req.params;
      const requestedDealId = String(req.query.dealId || '').trim() || null;

      const { data: contact } = await sb.from('contacts')
        .select('id, name, company_name, job_title, email, phone, linkedin_url, pipeline_stage, conversation_state, last_intent, last_intent_label, intent_history, last_outreach_at, last_reply_at, investor_score, notes, deal_id, batch_id')
        .eq('id', id)
        .maybeSingle();

      if (contact) {
        await hydrateLinkedInConversationHistory(sb, contact, requestedDealId);
        await hydrateEmailConversationHistory(sb, contact, requestedDealId);
      }

      let query = sb.from('conversation_messages')
        .select('id, deal_id, direction, channel, body, subject, sent_at, received_at, intent, intent_confidence, action_taken')
        .eq('contact_id', id)
        .limit(250);
      if (requestedDealId) query = query.eq('deal_id', requestedDealId);
      const { data: rawMessages } = await query;
      const messages = (rawMessages || []).slice().sort((a, b) => {
        const aTs = new Date(a.sent_at || a.received_at || 0).getTime();
        const bTs = new Date(b.sent_at || b.received_at || 0).getTime();
        return aTs - bTs;
      });

      const selectedDealId = requestedDealId || (messages?.length === 1 ? messages[0]?.deal_id : null) || contact?.deal_id || null;
      const contactDealContext = await buildContactDealContextMap(sb);
      const dealNameMap = await getDealNameMap(sb, [
        requestedDealId,
        selectedDealId,
        contact?.deal_id,
        ...(messages || []).map(m => m.deal_id),
      ]);
      const selectedDealName = selectedDealId ? (dealNameMap[selectedDealId] || 'Unknown Project') : null;

      // Fetch firm research from batch_firms (AUM, thesis, justification, past investments)
      let firmResearch = null;
      if (contact?.batch_id && contact?.company_name) {
        try {
          const { data: firmRow } = await sb.from('batch_firms')
            .select('firm_name, score, aum, thesis, justification, past_investments, contacts_found, enrichment_status')
            .eq('batch_id', contact.batch_id)
            .eq('firm_name', contact.company_name)
            .maybeSingle();
          firmResearch = firmRow || null;
        } catch {
          firmResearch = null;
        }
      }

      res.json({
        contact: contact || null,
        firmResearch,
        messages: (messages || []).map(m => ({
          id: m.id,
          dealId: m.deal_id || null,
          dealName: m.deal_id ? (dealNameMap[String(m.deal_id)] || 'Unknown Project') : null,
          direction: m.direction,
          channel: m.channel,
          body: m.body,
          subject: m.subject,
          timestamp: m.sent_at || m.received_at,
          intent: m.intent,
          intentConfidence: m.intent_confidence,
          actionTaken: m.action_taken,
        })),
        selectedDealId,
        selectedDealName,
        deals: contactDealContext.get(id)?.deals || [],
        activeDealName: contactDealContext.get(id)?.activeDealName || '',
        dealNamesText: contactDealContext.get(id)?.dealNamesText || '',
        intentHistory: contact?.intent_history || [],
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/queue — approval queue with IDs (in-memory + Supabase LinkedIn DMs)
  app.get('/api/queue', async (req, res) => {
    const inMemory = getPendingApprovals();
    try {
      const sb = getSupabase();
      if (sb) {
        const { data: sbItems, error: sbErr } = await sb.from('approval_queue')
          .select('id, contact_id, candidate_id, contact_name, contact_email, firm, stage, body, message_type, channel, reply_to_id, created_at, subject_a, subject_b, approved_subject, score, research_summary, edited_body, edit_instructions, deal_name, outreach_mode, status')
          .in('status', ['pending', 'approved_waiting_for_window'])
          .order('created_at', { ascending: true });
        if (sbErr) console.warn('[/api/queue] Supabase query error:', sbErr.message);
        const LINKEDIN_STAGES = ['LinkedIn DM', 'LinkedIn Reply', 'prior_chat_review'];
        const looksLikeEmail = (value) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || '').trim());
        const missingEmailIds = [];
        const contactIdsToHydrate = [...new Set((sbItems || [])
          .filter(r => r.contact_id)
          .map(r => r.contact_id))];
        const contactEmailMap = {};
        const contactScoreMap = {};
        if (contactIdsToHydrate.length) {
          try {
            const { data: contactRows } = await sb.from('contacts').select('id, email, investor_score').in('id', contactIdsToHydrate);
            for (const row of contactRows || []) {
              contactEmailMap[row.id] = row.email || null;
              contactScoreMap[row.id] = row.investor_score ?? null;
            }
          } catch {}
        }

        const sbMapped = (sbItems || []).flatMap(r => {
          const isReply = isReplyMessageType(r.message_type);
          const isLinkedIn = LINKEDIN_STAGES.includes(r.stage) || r.message_type === 'prior_chat_review' || r.message_type === 'linkedin_reply' || r.channel === 'linkedin';
          const resolvedEmail = r.contact_email || contactEmailMap[r.contact_id] || null;
          if (!isLinkedIn && !looksLikeEmail(resolvedEmail)) {
            missingEmailIds.push(r.id);
            return [];
          }
          return {
            id:              r.id,
            name:            r.contact_name || '',
            firm:            r.firm || '',
            stage:           r.stage,
            body:            r.edited_body || r.body || '',
            emailBody:       r.edited_body || r.body || '',
            message_type:    r.message_type || null,
            isReply,
            channel:         r.channel || (isLinkedIn ? 'linkedin' : 'email'),
            queuedAt:        r.created_at,
            contact_id:      r.contact_id || r.candidate_id,
            subjectA:        (r.status === 'approved_waiting_for_window' ? (r.approved_subject || r.subject_a) : r.subject_a) || null,
            subjectB:        r.subject_b || null,
            score:           r.score ?? contactScoreMap[r.contact_id] ?? null,
            researchSummary: r.research_summary || null,
            contactEmail:    resolvedEmail,
            dealName:        r.deal_name || null,
            editInstructions: r.edit_instructions || null,
            status:          r.status || 'pending',
            waitingForWindow: r.status === 'approved_waiting_for_window',
            _supabaseOnly:   true,
          };
        });
        if (missingEmailIds.length) {
          sb.from('approval_queue').update({
            status: 'skipped',
            resolved_at: new Date().toISOString(),
            edit_instructions: 'Auto-skipped: missing usable email address',
          }).in('id', missingEmailIds).then(null, () => {});
        }
        const merged = [];
        const seenQueueIds = new Set();
        const seenLocalIds = new Set();

        for (const item of inMemory) {
          const queueKey = item.queueId ? `q:${item.queueId}` : null;
          const localKey = item.telegramMsgId ? `i:${item.telegramMsgId}` : null;
          if (queueKey) seenQueueIds.add(queueKey);
          if (localKey) seenLocalIds.add(localKey);
          merged.push(item);
        }

        // Items resolved via Telegram but whose DB update may still be in flight
        const recentlyResolved = getRecentlyResolvedQueueIds();

        for (const item of sbMapped) {
          const queueKey = item.id ? `q:${item.id}` : null;
          if (queueKey && seenQueueIds.has(queueKey)) continue;
          const localKey = item.telegramMsgId ? `i:${item.telegramMsgId}` : null;
          if (localKey && seenLocalIds.has(localKey)) continue;
          // Filter out *pending* items that were just approved/skipped via Telegram
          // but whose DB commit is still in flight.  Never filter approved_waiting_for_window —
          // those should show on the dashboard immediately after approval.
          if (item.id && item.status === 'pending' && recentlyResolved.has(String(item.id))) continue;
          merged.push(item);
        }

        return res.json(merged);
      }
    } catch {}
    res.json(inMemory);
  });

  // Alias for backward compat
  app.get('/api/approvals', (req, res) => {
    res.json(getPendingApprovals());
  });

  // POST /api/approvals/:id/prior-chat — dashboard decision on prior-chat review items
  app.post('/api/approvals/:id/prior-chat', async (req, res) => {
    const { id }       = req.params;
    const { decision } = req.body; // 'proceed' or 'skip'
    if (!decision || !['proceed', 'skip'].includes(decision)) {
      return res.status(400).json({ error: 'decision must be proceed or skip' });
    }
    const sb = getSupabase();
    if (!sb) return res.status(500).json({ error: 'DB unavailable' });
    try {
      const { data: row } = await sb.from('approval_queue')
        .select('id, contact_id, contact_name, firm, deal_id').eq('id', id).single();
      if (!row) return res.status(404).json({ error: 'Queue item not found' });

      const contactPatch = decision === 'proceed'
        ? {
            pipeline_stage: 'invite_accepted',
            updated_at: new Date().toISOString(),
          }
        : {
            pipeline_stage: 'Inactive',
            conversation_state: 'do_not_contact',
            conversation_ended_at: new Date().toISOString(),
            conversation_ended_reason: 'Prior LinkedIn chat declined via dashboard',
            updated_at: new Date().toISOString(),
          };
      await sb.from('contacts').update(contactPatch).eq('id', row.contact_id);
      await sb.from('approval_queue').update({
        status:       decision === 'proceed' ? 'approved' : 'skipped',
        resolved_at:  new Date().toISOString(),
      }).eq('id', id);

      if (decision === 'proceed') {
        await queueLinkedInDmApproval(row.contact_id, { reason: 'prior_chat_approved_dashboard' }).catch(() => {});
      }

      pushActivity({
        type:   'linkedin',
        action: decision === 'proceed'
          ? `Prior chat approved — DM approval queued: ${row.contact_name || ''}`
          : `Prior chat skipped: ${row.contact_name || ''}`,
        note: row.firm || '',
        dealId: row.deal_id || null,
      });
      res.json({ success: true });
    } catch (err) {
      console.error('[/api/approvals/prior-chat]', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/approve — approve an email from dashboard
  app.post('/api/approve', async (req, res) => {
    const { id, subjectChoice, variant, editedBody, subject, sendNow = false } = req.body;
    if (!id) return res.status(400).json({ error: 'id required' });

    const sb = getSupabase();

    let sbItem = null;
    if (sb) {
      try {
        const { data } = await sb.from('approval_queue')
          .select('id, deal_id, contact_id, candidate_id, contact_name, contact_email, firm, body, edited_body, stage, subject_a, subject_b, subject, approved_subject, message_type, channel, reply_to_id, telegram_msg_id')
          .eq('id', id)
          .in('status', ['pending', 'approved_waiting_for_window'])
          .maybeSingle();
        sbItem = data || null;
      } catch {}
    }

    if (sbItem && isReplyMessageType(sbItem.message_type)) {
      try {
        await dismissPendingApproval(id).catch(() => {});
        if (sbItem.telegram_msg_id) await clearTelegramApprovalControls(sbItem.telegram_msg_id).catch(() => {});
        const result = await sendApprovedReply({
          queueId: sbItem.id,
          queueItem: sbItem,
          forceSend: !!sendNow,
          bodyOverride: editedBody || sbItem.edited_body || sbItem.body || '',
        });
        return res.json({
          success: true,
          deferred: !!result?.deferred,
          message: result?.deferred
            ? (result?.nextOpen
                ? `Reply approved and waiting for the sending window (${result.nextOpen})`
                : 'Reply approved and waiting for the sending window')
            : 'Reply sent',
        });
      } catch (err) {
        console.error('[/api/approve] Reply send error:', err.message);
        return res.status(500).json({ error: 'Send failed: ' + err.message });
      }
    }

    // Find the approval to get contact info before resolving
    const pending = getPendingApprovals();
    const item = pending.find(p => String(p.id) === String(id) || String(p.queueId || '') === String(id));

    const resolved = resolveApprovalFromDashboard(
      id,
      'approve',
      subject || item?.subject,
      editedBody
    );

    if (!resolved) {
      // Fall back: check Supabase for a LinkedIn DM queue item (webhook-triggered, not in-memory)
      if (sb) {
        let queueItem = sbItem;
        try {
          if (!queueItem) {
            const { data } = await sb.from('approval_queue')
              .select('id, deal_id, contact_id, candidate_id, contact_name, contact_email, firm, body, edited_body, stage, subject_a, subject_b, subject, approved_subject, message_type, channel, reply_to_id, telegram_msg_id')
              .eq('id', id)
              .eq('status', 'pending')
              .single();
            queueItem = data;
          }
        } catch { /* not found or not pending */ }

        if (queueItem?.contact_id && queueItem?.stage === 'LinkedIn DM') {
          try {
            if (queueItem.telegram_msg_id) await clearTelegramApprovalControls(queueItem.telegram_msg_id).catch(() => {});
            const text = sanitizeApprovalText(editedBody || queueItem.edited_body || queueItem.body || '');
            await sb.from('contacts').update({
              pipeline_stage: 'DM Approved',
              updated_at: new Date().toISOString(),
            }).eq('id', queueItem.contact_id);
            const sendResult = await sendApprovedLinkedInDM({
              contactId: queueItem.contact_id,
              text,
              queueId: queueItem.id,
              queueItem,
            });
            if (sendResult?.skipped) {
              // Item was already claimed by the orchestrator — treat as if sent
              notifyQueueUpdated();
              return res.json({ success: true, message: 'LinkedIn DM already being processed' });
            }
            if (sendResult?.deferred) {
              return res.json({
                success: true,
                deferred: true,
                message: sendResult.nextOpen
                  ? `LinkedIn DM approved and waiting for the DM window (${sendResult.nextOpen})`
                  : 'LinkedIn DM approved and waiting for the DM window',
              });
            }
            return res.json({ success: true, message: 'LinkedIn DM sent' });
          } catch (err) {
            console.error('[/api/approve] LinkedIn DM send error:', err.message);
            return res.status(500).json({ error: 'Send failed: ' + err.message });
          }
        }

        // Email approval (INTRO or other email stages) — send via Unipile Gmail
        if (queueItem?.contact_id) {
          try {
            if (queueItem.telegram_msg_id) await clearTelegramApprovalControls(queueItem.telegram_msg_id).catch(() => {});
            // Look up contact email if not stored directly on the queue item
            let toEmail = queueItem.contact_email;
            let contactRow = null;
            if (!toEmail) {
              try {
                const result = await sb.from('contacts').select('email, name, deal_id, company_name').eq('id', queueItem.contact_id).single();
                contactRow = result?.data || null;
              } catch {
                contactRow = null;
              }
              toEmail = contactRow?.email;
            } else {
              try {
                const result = await sb.from('contacts').select('email, name, deal_id, company_name').eq('id', queueItem.contact_id).single();
                contactRow = result?.data || null;
              } catch {
                contactRow = null;
              }
            }
            if (!toEmail) {
              return res.status(400).json({ error: 'No email address found for this contact — cannot send' });
            }

            const queueDealId = queueItem.deal_id || contactRow?.deal_id || null;
            const activeVariant = variant || subjectChoice;
            const chosenSubject = subject || (activeVariant === 'b' ? queueItem.subject_b : '') || queueItem.subject_a || queueItem.subject || '';
            const finalSubject  = editedBody ? chosenSubject : (chosenSubject || '');
            const bodyToSend    = sanitizeApprovalText(editedBody || queueItem.edited_body || queueItem.body || '');
            await sb.from('contacts').update({
              pipeline_stage: 'Email Approved',
              updated_at: new Date().toISOString(),
            }).eq('id', queueItem.contact_id);

            let deal = null;
            if (queueDealId) {
              try {
                const { data } = await sb.from('deals').select('*').eq('id', queueDealId).single();
                deal = data || null;
              } catch {}
            }

            if (!sendNow && deal && !isWithinChannelWindow(deal, 'email')) {
              await sb.from('approval_queue').update({
                status: 'approved_waiting_for_window',
                resolved_at: new Date().toISOString(),
                approved_subject: finalSubject || null,
                edited_body: bodyToSend || null,
                deal_id: queueDealId,
              }).eq('id', queueItem.id);
              pushActivity({
                type: 'email',
                action: `Email approved: ${queueItem.contact_name || ''}`,
                note: deal?.name
                  ? `${queueItem.firm || contactRow?.company_name || ''} · Waiting for email window${getWindowStatus(deal).nextOpen ? ` (${getWindowStatus(deal).nextOpen})` : ''}`
                  : 'Waiting for email window',
                deal_name: deal?.name || null,
                dealId: queueDealId,
              });
              notifyQueueUpdated();
              return res.json({
                success: true,
                deferred: true,
                message: getWindowStatus(deal).nextOpen
                  ? `Email approved and waiting for the sending window (${getWindowStatus(deal).nextOpen})`
                  : 'Email approved and waiting for the sending window',
              });
            }

            const sendResult = await sendEmail({
              to: toEmail,
              toName: queueItem.contact_name || '',
              subject: finalSubject,
              body: bodyToSend,
              trackingLabel: buildEmailTrackingLabel({
                dealId: queueDealId,
                contactId: queueItem.contact_id || null,
                stage: queueItem.stage || 'email',
              }),
            });

            try {
              await sb.from('approval_queue').update({
                status:           'sent',
                sent_at:          new Date().toISOString(),
                approved_subject: finalSubject,
                deal_id:          queueDealId,
              }).eq('id', queueItem.id);
            } catch {}

            try {
              await sb.from('contacts').update({
                pipeline_stage:     'Email Sent',
                last_email_sent_at: new Date().toISOString(),
              }).eq('id', queueItem.contact_id);
            } catch {}

            try {
              await sb.from('conversation_messages').insert({
                contact_id:  queueItem.contact_id,
                direction:   'outbound',
                channel:     'email',
                body:        bodyToSend,
                subject:     finalSubject,
                sent_at:     new Date().toISOString(),
              });
            } catch {}

            // Log to activity_log so email sent metrics increment
            let contactForLog = null;
            const emailSentActivityAt = new Date().toISOString();
            const emailSentActivityKey = sendResult?.emailId
              ? `email_sent:${sendResult.emailId}`
              : `email_sent:${queueItem.contact_id || queueItem.contact_email || queueItem.contact_name || 'unknown'}:${emailSentActivityAt}`;
            try {
              const result = await sb.from('contacts').select('deal_id').eq('id', queueItem.contact_id).single();
              contactForLog = result?.data || null;
            } catch {
              contactForLog = null;
            }
            if (contactForLog?.deal_id) {
              try {
                const { data: dealRow } = await sb.from('deals')
                  .select('id, followup_days_li, followup_days_email')
                  .eq('id', contactForLog.deal_id)
                  .maybeSingle();
                const sequence = await getSequenceForDealFromServer(sb, contactForLog.deal_id);
                const stageLabel = String(queueItem?.stage || '').toLowerCase();
                const followUpMatch = stageLabel.match(/follow[- ]up\s*(\d+)/i);
                const followUpNumber = followUpMatch ? Number(followUpMatch[1] || 1) : 0;
                const nextFollowUpPlan = getNextFollowUpPlanForChannel(sequence, dealRow || null, 'email', followUpNumber);
                const sentAt = emailSentActivityAt;
                const nextFollowUpDueAt = nextFollowUpPlan?.delayDays
                  ? new Date(Date.now() + nextFollowUpPlan.delayDays * 24 * 60 * 60 * 1000).toISOString()
                  : null;

                await sb.from('contacts').update({
                  pipeline_stage:     'Email Sent',
                  outreach_channel:   'email',
                  last_email_sent_at: sentAt,
                  last_outreach_at:   sentAt,
                  follow_up_count:    followUpNumber,
                  follow_up_due_at:   nextFollowUpDueAt,
                }).eq('id', queueItem.contact_id);

                await sb.from('activity_log').insert({
                  deal_id:    contactForLog.deal_id,
                  event_type: 'EMAIL_SENT',
                  summary:    `Email sent: ${queueItem.contact_name || 'contact'}`,
                  detail:     {
                    activity_key: emailSentActivityKey,
                    message: [queueItem.firm || '', finalSubject || ''].filter(Boolean).join(' · '),
                    channel: 'email',
                    account_id: sendResult?.accountId || null,
                    provider_id: sendResult?.providerId || null,
                    message_id: sendResult?.emailId || null,
                    thread_id: sendResult?.threadId || null,
                    to: toEmail,
                  },
                  created_at: new Date().toISOString(),
                });
              } catch {}
            }

            pushActivity({
              type:   'email',
              activity_key: emailSentActivityKey,
              action: `Email sent: ${queueItem.contact_name || ''}`,
              note:   `${queueItem.firm || ''} · ${finalSubject}`,
              persist: false,
            });
            await sendTelegram(
              `✅ *Email sent* → *${queueItem.contact_name || 'contact'}* (${queueItem.firm || contactRow?.company_name || 'unknown firm'})${finalSubject ? `\nSubject: _${sanitizeApprovalText(finalSubject)}_` : ''}`
            ).catch(() => {});
            return res.json({ success: true, message: 'Email sent' });
          } catch (err) {
            console.error('[/api/approve] Email send error:', err.message);
            return res.status(500).json({ error: 'Send failed: ' + err.message });
          }
        }
      }
      return res.status(404).json({ error: 'Approval not found — may have already been handled' });
    }

    pushActivity({
      type: 'APPROVAL',
      action: 'Approved via Dashboard',
      note: item
        ? `${item.name} @ ${item.firm}`
        : (sbItem ? `${sbItem.contact_name || 'Unknown'} @ ${sbItem.firm || 'Unknown firm'}` : 'Unknown contact'),
    });

    const isLinkedIn = String(item?.stage || '').toLowerCase().includes('linkedin');
    res.json({
      success: true,
      message: isLinkedIn
        ? 'LinkedIn DM approved'
        : 'Approval sent — orchestrator will fire the email',
    });
  });

  // POST /api/skip-approval — skip an approval from dashboard (hard delete)
  app.post('/api/skip-approval', async (req, res) => {
    const { id } = req.body;
    if (!id) return res.status(400).json({ error: 'id required' });

    // Resolve in-memory (id is the Telegram message ID)
    resolveApprovalFromDashboard(id, 'skip');

    // Hard delete from Supabase approval_queue by id (handles both telegram_msg_id and UUID items)
    await deleteApprovalFromQueue(id).catch(() => {});
    const sb = getSupabase();
    if (sb) {
      try {
        const { data: skippedRow } = await sb.from('approval_queue')
          .update({ status: 'skipped', resolved_at: new Date().toISOString() })
          .eq('id', id)
          .select('telegram_msg_id, contact_id, stage')
          .maybeSingle();
        if (skippedRow?.telegram_msg_id) await clearTelegramApprovalControls(skippedRow.telegram_msg_id).catch(() => {});
        if (skippedRow?.contact_id && String(skippedRow.stage || '').toLowerCase().includes('linkedin')) {
          await suppressSkippedLinkedInDmContact(sb, skippedRow.contact_id, 'LinkedIn DM approval skipped from dashboard');
        }
      } catch {
        // best effort
      }
    }

    res.json({ success: true });
  });

  // POST /api/edit-approval — send edit instructions
  app.post('/api/edit-approval', async (req, res) => {
    const { id, body, subject } = req.body;
    if (!id) return res.status(400).json({ error: 'id required' });

    const sanitizedBody = body == null ? null : sanitizeApprovalText(body);
    const sanitizedSubject = subject == null ? null : sanitizeApprovalText(subject);
    if (sanitizedBody !== null && !sanitizedBody) {
      return res.status(400).json({ error: 'Edited body cannot be empty' });
    }

    const updatedMemory = await updateApprovalDraftFromDashboard(id, {
      body: sanitizedBody,
      subject: sanitizedSubject,
    }).catch(() => false);

    const sb = getSupabase();
    let updatedDb = false;
    if (sb) {
      try {
        const patch = {};
        if (sanitizedBody !== null) patch.edited_body = sanitizedBody;
        if (sanitizedSubject !== null) patch.approved_subject = sanitizedSubject;
        if (Object.keys(patch).length) {
          const { data } = await sb.from('approval_queue').update(patch)
            .eq('id', id)
            .in('status', ['pending', 'approved', 'approved_waiting_for_window'])
            .select('id')
            .maybeSingle();
          updatedDb = !!data?.id;
        }
      } catch {}
    }

    if (!updatedMemory && !updatedDb) {
      return res.status(404).json({ error: 'Approval not found' });
    }

    notifyQueueUpdated();
    res.json({ success: true, message: 'Draft updated' });
  });

  // GET /api/health — API health status + process diagnostics
  app.get('/api/health', (req, res) => {
    try {
      const apiHealth = getApiHealth();
      // Flatten each service to a string status for the dashboard frontend
      const serviceStatuses = {};
      for (const [svc, val] of Object.entries(apiHealth)) {
        serviceStatuses[svc] = typeof val === 'object' && val !== null
          ? String(val.status ?? 'unknown')
          : String(val ?? 'unknown');
      }
      res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        uptime_seconds: Math.floor(process.uptime()),
        memory_mb: Math.floor(process.memoryUsage().heapUsed / 1024 / 1024),
        websocket_clients: wss ? wss.clients.size : 0,
        node_version: process.version,
        elevenlabs_tts: jarvisVoiceStatus.ok === false
          ? 'error'
          : (jarvisVoiceStatus.configured ? 'ok' : 'unconfigured'),
        jarvis_voice: jarvisVoiceStatus,
        ...serviceStatuses,
      });
    } catch (err) {
      res.status(500).json({ status: 'error', error: err.message });
    }
  });

  // GET /api/stats — stats summary from Supabase
  app.get('/api/stats', async (req, res) => {
    const stats = {
      emails_sent: 0,
      response_rate: 0,
      active_prospects: 0,
      approval_queue: getPendingApprovals().length,
      capital_committed: 0,
      active_deals: 0,
      // Legacy field names for dashboard compat
      emailsSent: 0,
      activeProspects: 0,
      queueCount: getPendingApprovals().length,
      committed: 0,
      targetAmount: 0,
      dealName: '—',
    };

    try {
      const sb = getSupabase();
      if (sb) {
        // Resolve active deal IDs once — all per-deal stats use this
        let activeDealIds = [];     // non-paused (for orchestrator-relevant stats)
        let allActiveDealIds = [];  // all active incl. paused (for counting totals)
        let allDealsData  = [];
        try {
          const { data: allDeals } = await sb.from('deals').select('id, name, committed_amount, target_amount, status, paused');
          allDealsData      = allDeals || [];
          activeDealIds     = allDealsData.filter(d => d.status === 'ACTIVE' && !d.paused).map(d => d.id);
          allActiveDealIds  = allDealsData.filter(d => d.status === 'ACTIVE').map(d => d.id);
        } catch (e) { console.warn('/api/stats deal ids:', e.message); }
        const safeActiveIds    = activeDealIds.length > 0    ? activeDealIds    : ['00000000-0000-0000-0000-000000000000'];
        const safeAllActiveIds = allActiveDealIds.length > 0 ? allActiveDealIds : ['00000000-0000-0000-0000-000000000000'];

        try {
          const channelMetricsByDeal = await computeDealChannelMetrics(sb, safeAllActiveIds);
          const totals = Object.values(channelMetricsByDeal).reduce((acc, metrics) => {
            acc.emails_sent += metrics.emails_sent || 0;
            acc.emails_replied += metrics.emails_replied || 0;
            acc.li_invites_sent += metrics.li_invites_sent || 0;
            acc.li_accepts += metrics.li_accepts || 0;
            acc.li_active_pending += metrics.li_active_pending || 0;
            acc.li_dms_sent += metrics.li_dms_sent || 0;
            acc.li_dm_replies += metrics.li_dm_replies || 0;
            return acc;
          }, {
            emails_sent: 0,
            emails_replied: 0,
            li_invites_sent: 0,
            li_accepts: 0,
            li_active_pending: 0,
            li_dms_sent: 0,
            li_dm_replies: 0,
          });

          stats.emails_sent = totals.emails_sent;
          stats.emailsSent = totals.emails_sent;
          stats.emails_replied = totals.emails_replied;
          stats.response_rate = totals.emails_sent > 0
            ? Math.round((totals.emails_replied / totals.emails_sent) * 100)
            : 0;
          stats.responseRate = stats.response_rate;
          stats.li_invites_sent = totals.li_invites_sent;
          stats.li_accepts = totals.li_accepts;
          stats.li_active_pending = totals.li_active_pending;
          stats.li_acceptance_rate = totals.li_invites_sent > 0
            ? Math.round((totals.li_accepts / totals.li_invites_sent) * 100)
            : 0;
          stats.li_dms_sent = totals.li_dms_sent;
          stats.li_dm_response_rate = totals.li_dms_sent > 0
            ? Math.round((totals.li_dm_replies / totals.li_dms_sent) * 100)
            : 0;
          stats.total_responses = totals.emails_replied + totals.li_accepts + totals.li_dm_replies;
          stats.overall_outreach_sent = totals.emails_sent + totals.li_invites_sent + totals.li_dms_sent;
          stats.overall_response_rate = stats.overall_outreach_sent > 0
            ? Math.round((stats.total_responses / stats.overall_outreach_sent) * 100)
            : 0;
        } catch (e) { console.warn('/api/stats outreach metrics:', e.message); }

        // Pending approvals
        try {
          const { count } = await sb.from('emails').select('id', { count: 'exact', head: true })
            .eq('status', 'pending_approval')
            .in('deal_id', safeActiveIds);
          stats.approval_queue = (count || 0) + getPendingApprovals().length;
          stats.queueCount     = stats.approval_queue;
        } catch (e) { console.warn('/api/stats approval_queue:', e.message); }

        // Active prospects — all contacts in active deals (incl. paused) that are not archived/skipped
        try {
          const { count } = await sb.from('contacts').select('id', { count: 'exact', head: true })
            .in('deal_id', safeAllActiveIds)
            .not('pipeline_stage', 'in', '("Archived","ARCHIVED","archived","Skipped","skipped_no_name","skipped_no_linkedin","skipped_duplicate_email","Inactive","Suppressed — Opt Out","Deleted — Do Not Contact")');
          stats.active_prospects = count || 0;
          stats.activeProspects  = count || 0;
        } catch (e) { console.warn('/api/stats prospects:', e.message); }

        // Positive replies — contacts that replied and are not in a negative/dead state
        try {
          const { count: positiveReplies } = await sb.from('contacts')
            .select('id', { count: 'exact', head: true })
            .in('deal_id', safeAllActiveIds)
            .not('last_reply_at', 'is', null)
            .not('pipeline_stage', 'in', '("Inactive","Archived","ARCHIVED","archived","Suppressed — Opt Out","Deleted — Do Not Contact")')
            .not('conversation_state', 'in', '("conversation_ended_negative","do_not_contact")');
          stats.positive_replies = positiveReplies || 0;
          stats.positive_reply_rate = stats.overall_outreach_sent > 0
            ? Math.round(((positiveReplies || 0) / stats.overall_outreach_sent) * 100)
            : 0;
        } catch (e) { console.warn('/api/stats positive_replies:', e.message); }

        // Deal counts + capital
        try {
          const activeDealsData = allDealsData.filter(d => d.status === 'ACTIVE');
          stats.active_deals         = activeDealsData.length;
          stats.total_deals_launched = allDealsData.length;
          stats.total_funds_raised   = allDealsData.reduce((s, d) => s + (d.committed_amount || 0), 0);
          stats.capital_committed    = stats.total_funds_raised;
          stats.committed            = stats.total_funds_raised;
          stats.active_committed     = activeDealsData.reduce((s, d) => s + (d.committed_amount || 0), 0);
          if (activeDealsData.length) {
            stats.targetAmount = activeDealsData[0].target_amount || 0;
            stats.dealName     = activeDealsData[0].name || '—';
          }
        } catch (e) { console.warn('/api/stats deals:', e.message); }

      }
    } catch (err) {
      console.error('/api/stats outer error:', err.message);
    }

    res.json(stats);
  });

  // POST /api/action — manual triggers
  app.post('/api/action', async (req, res) => {
    const { action } = req.body;

    switch (action) {
      case 'pause_all': {
        const state = readState();
        state.rocoStatus = 'PAUSED';
        state.pausedUntil = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
        writeState(state);
        if (rocoState) rocoState.status = 'PAUSED';
        broadcastToAll({ type: 'STATE_UPDATE', state });
        pushActivity({ type: 'SYSTEM', action: 'Paused All', note: 'Manual 24h pause via dashboard' });
        return res.json({ success: true, message: 'Roco paused for 24 hours' });
      }

      case 'resume_all': {
        const state = readState();
        state.rocoStatus = 'ACTIVE';
        state.pausedUntil = null;
        writeState(state);
        if (rocoState) rocoState.status = 'ACTIVE';
        broadcastToAll({ type: 'STATE_UPDATE', state });
        pushActivity({ type: 'SYSTEM', action: 'Resumed', note: 'Manual resume via dashboard' });
        return res.json({ success: true, message: 'Roco resumed' });
      }

      case 'flush_queue': {
        const pending = getPendingApprovals();
        for (const item of pending) {
          resolveApprovalFromDashboard(item.id, 'skip');
        }
        pushActivity({ type: 'SYSTEM', action: 'Queue Flushed', note: `${pending.length} items skipped` });
        return res.json({ success: true, message: `Flushed ${pending.length} pending approvals` });
      }

      case 'run_research': {
        try {
          const { getActiveDeals, getDeal } = await import('../core/supabaseSync.js');
          const { runFirmResearch } = await import('../research/firmResearcher.js');
          const { dealId } = req.body;
          let deals;
          if (dealId) {
            const d = await getDeal(dealId);
            if (!d) return res.status(404).json({ error: 'Deal not found' });
            deals = [d];
          } else {
            deals = await getActiveDeals();
            if (!deals.length) return res.json({ success: false, message: 'No active deals found' });
          }
          pushActivity({ type: 'RESEARCH', action: 'Research Started', note: `Firm-first research running for: ${deals.map(d => d.name).join(', ')}` });
          Promise.allSettled(deals.map(d => runFirmResearch(d))).then(results => {
            const total = results.reduce((sum, r) => sum + (typeof r.value === 'number' ? r.value : 0), 0);
            pushActivity({ type: 'RESEARCH', action: 'Research Complete', note: `Found ${total} new contacts for ${deals.map(d => d.name).join(', ')}` });
          });
          return res.json({ success: true, message: `Firm research started for ${deals.map(d => d.name).join(', ')} — firms and contacts will appear shortly` });
        } catch (e) {
          return res.status(500).json({ error: e.message });
        }
      }

      case 'run_enrichment': {
        try {
          const { runManualEnrich } = await import('../core/orchestrator.js');
          const { dealId } = req.body;
          let deals;
          if (dealId) {
            const sb = getSupabase();
            const { data } = await sb.from('deals').select('*').eq('id', dealId).single();
            if (!data) return res.json({ success: false, message: 'Deal not found' });
            deals = [data];
          } else {
            const { getActiveDeals } = await import('../core/supabaseSync.js');
            deals = await getActiveDeals();
            if (!deals.length) return res.json({ success: false, message: 'No active deals found' });
          }
          const state = readState();
          // Run deals sequentially so activity feed is readable contact-by-contact
          (async () => {
            for (const d of deals) {
              await runManualEnrich(d, state);
            }
          })();
          return res.json({ success: true, message: `Manual enrichment started for ${deals.map(d => d.name).join(', ')} — watch the activity feed for live updates` });
        } catch (e) {
          return res.status(500).json({ error: e.message });
        }
      }

      case 'suppress_firm': {
        const { firm } = req.body;
        if (!firm) return res.status(400).json({ error: 'firm is required' });
        try {
          const sb = getSupabase();
          // Mark all contacts from this firm as Archived across all deals
          await sb.from('contacts').update({ pipeline_stage: 'Archived' })
            .ilike('company_name', firm);
          pushActivity({ type: 'SYSTEM', action: 'Firm Suppressed', note: `All contacts from "${firm}" archived` });
          return res.json({ success: true, message: `Firm "${firm}" suppressed` });
        } catch (e) {
          return res.status(500).json({ error: e.message });
        }
      }

      default:
        return res.status(400).json({ error: `Unknown action: ${req.body?.action}` });
    }
  });

  // POST /api/deal — update active deal
  app.post('/api/deal', (req, res) => {
    const { dealName, targetAmount, currentCommitted, sector, geography, description } = req.body;

    const state = readState();
    if (!state.activeDeals || state.activeDeals.length === 0) {
      state.activeDeals = [{ id: 'deal_001' }];
    }

    const deal = state.activeDeals[0];
    if (dealName !== undefined) deal.name = dealName;
    if (targetAmount !== undefined) deal.targetAmount = Number(targetAmount);
    if (currentCommitted !== undefined) deal.currentCommitted = Number(currentCommitted);
    if (sector !== undefined) deal.sector = sector;
    if (geography !== undefined) deal.geography = geography;
    if (description !== undefined) deal.description = description;

    writeState(state);
    broadcastToAll({ type: 'DEAL_UPDATE', deal });

    pushActivity({ type: 'SYSTEM', action: 'Deal Updated', note: deal.name || 'Deal' });
    res.json({ success: true, deal });
  });

  // POST /api/contact/:id/stage — update a contact's pipeline stage
  app.post('/api/contact/:id/stage', async (req, res) => {
    const { id } = req.params;
    const { stage } = req.body;
    if (!stage) return res.status(400).json({ error: 'stage required' });
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { error: updateErr } = await sb.from('contacts').update({ pipeline_stage: stage }).eq('id', id);
      if (updateErr) throw updateErr;
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/contacts/:id/reactivate — move an archived contact back into the pipeline
  app.post('/api/contacts/:id/reactivate', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: contact } = await sb.from('contacts')
        .select('id, name, deal_id, pipeline_stage')
        .eq('id', req.params.id).single();
      if (!contact) return res.status(404).json({ error: 'Contact not found' });
      if (contact.pipeline_stage !== 'Archived') return res.status(400).json({ error: 'Contact is not archived' });
      await sb.from('contacts')
        .update({ pipeline_stage: 'Ranked', enrichment_status: 'Pending' })
        .eq('id', req.params.id);
      pushActivity({ type: 'PIPELINE', action: 'Contact Reactivated', note: `${contact.name} moved back into pipeline` });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/contacts/:id — fetch full contact record for prospect drawer
  app.get('/api/contacts/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('contacts')
        .select('*').eq('id', req.params.id).single();
      if (dbErr) throw new Error(dbErr.message);
      if (!data) return res.status(404).json({ error: 'Contact not found' });
      res.json(data);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/contacts/:id/investor-card', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const requestedDealId = String(req.query.dealId || '').trim() || null;

      const { data: contact, error: contactErr } = await sb.from('contacts')
        .select('*')
        .eq('id', req.params.id)
        .single();
      if (contactErr) throw new Error(contactErr.message);

      const [dealMap, messagesRes, emailsRes, transcriptsRes] = await Promise.all([
        getDealNameMap(sb, [requestedDealId, contact.deal_id]),
        sb.from('conversation_messages')
          .select('direction, channel, body, subject, sent_at, received_at, intent')
          .eq('contact_id', req.params.id)
          .order('sent_at', { ascending: true })
          .then(result => result)
          .catch(() => ({ data: [] })),
        sb.from('emails')
          .select('*')
          .eq('contact_id', req.params.id)
          .order('created_at', { ascending: true })
          .then(result => result)
          .catch(() => ({ data: [] })),
        sb.from('meeting_transcripts')
          .select('*')
          .eq('contact_id', req.params.id)
          .order('created_at', { ascending: true })
          .then(result => result)
          .catch(() => ({ data: [] })),
      ]);

      const transcripts = transcriptsRes.data || [];
      const transcriptSentiment = transcripts.length ? transcripts[transcripts.length - 1].sentiment_score : null;
      const history = [];

      for (const msg of messagesRes.data || []) {
        history.push({
          type: String(msg.channel || 'email').includes('linkedin') ? 'LinkedIn' : 'Email',
          date: msg.sent_at || msg.received_at || null,
          summary: String(msg.subject || msg.body || '').replace(/\s+/g, ' ').trim().slice(0, 220),
        });
      }

      for (const email of emailsRes.data || []) {
        history.push({
          type: 'Email',
          date: email.sent_at || email.created_at || null,
          summary: String(email.subject || email.body || email.content || '').replace(/\s+/g, ' ').trim().slice(0, 220),
        });
      }

      for (const transcript of transcripts) {
        history.push({
          type: 'Meeting',
          date: transcript.created_at || null,
          summary: String(transcript.summary || transcript.transcript_text || '').replace(/\s+/g, ' ').trim().slice(0, 220),
          sentiment: transcript.sentiment_score || null,
        });
      }

      history.sort((a, b) => new Date(a.date || 0) - new Date(b.date || 0));

      res.json({
        contact: {
          ...contact,
          deal_name: dealMap[String(requestedDealId || contact.deal_id || '')] || null,
          transcript_sentiment: transcriptSentiment,
          sectors_of_interest: asJsonArray(contact.sector_focus),
          cheque_size_range: contact.typical_cheque_size || null,
          aum_display: contact.aum || contact.aum_fund_size || null,
          past_investments_list: asJsonArray(contact.past_investments),
          intent_signals_list: asJsonArray(contact.intent_signals),
        },
        history,
        transcripts,
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/contacts/:id/conversation — full conversation history for prospect drawer
  app.get('/api/contacts/:id/conversation', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const requestedDealId = String(req.query.dealId || '').trim() || null;

      const { data: contact, error: contactErr } = await sb.from('contacts')
        .select('id, deal_id, name, unipile_chat_id')
        .eq('id', req.params.id)
        .single();
      if (contactErr) throw new Error(contactErr.message);

      const messages = await hydrateLinkedInConversationHistory(sb, contact, requestedDealId);
      res.json({ messages: messages || [] });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/contacts/:id — remove contact from pipeline permanently
  app.delete('/api/contacts/:id', async (req, res) => {
    const { id } = req.params;
    try {
      const sb = getSupabase();

      // 1. Delete from Supabase contacts table
      if (sb) await sb.from('contacts').delete().eq('id', id);

      // 2. Delete from deal_contacts join table
      if (sb) await sb.from('deal_contacts').delete().eq('contact_id', id);

      // 3. Delete any pending draft emails for this contact
      if (sb) await sb.from('emails')
        .delete()
        .eq('contact_id', id)
        .in('status', ['draft', 'pending_approval']);

      // 4. Log
      pushActivity({ type: 'SYSTEM', action: 'Contact Deleted', note: `Contact ${id} removed from pipeline` });
      await sbLogActivity({
        contactId: id,
        eventType: 'CONTACT_DELETED',
        summary: 'Contact deleted from pipeline — will not be contacted',
      }).catch(() => {});

      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/meeting-transcripts', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('meeting_transcripts')
        .select('id, created_at, deal_id, contact_id, investor_name, investor_email, investor_phone, investor_linkedin, summary, transcript_text, sentiment_score, follow_up_actions, is_new_investor')
        .order('created_at', { ascending: false })
        .limit(200);
      if (dbErr) throw new Error(dbErr.message);
      const dealMap = await getDealNameMap(sb, (data || []).map(row => row.deal_id));
      res.json((data || []).map(row => ({
        ...row,
        deal_name: dealMap[String(row.deal_id || '')] || null,
      })));
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/meeting-transcripts/search-existing', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const search = String(req.query.search || '').trim();
      if (!search) return res.json([]);
      const { data, error: dbErr } = await sb.from('contacts')
        .select('id, name, email, company_name, job_title, linkedin_url, investment_thesis, past_investments, intent_signals')
        .or(`name.ilike.%${search}%,email.ilike.%${search}%,company_name.ilike.%${search}%`)
        .order('updated_at', { ascending: false })
        .limit(20);
      if (dbErr) throw new Error(dbErr.message);
      res.json(data || []);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/meeting-transcripts', (req, res, next) => {
    fileUpload(req, res, (err) => {
      if (err) return res.status(400).json({ error: err.message });
      next();
    });
  }, async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const {
        investor_mode,
        contact_id,
        investor_name,
        investor_email,
        investor_phone,
        investor_linkedin,
        transcript_text,
        investor_firm,
        investor_category,
      } = req.body || {};
      let transcriptBody = String(transcript_text || '').trim();
      if (!transcriptBody && req.file?.path) {
        const { extractDocumentText } = await import('../core/dealDocumentParser.js');
        transcriptBody = String(await extractDocumentText(req.file.path, req.file.originalname)).trim();
      }
      if (!(contact_id || investor_name) || !transcriptBody) {
        if (req.file?.path) try { fs.unlinkSync(req.file.path); } catch {}
        return res.status(400).json({ error: 'Investor identity and transcript content are required' });
      }

      const isExistingInvestor = investor_mode !== 'new';
      let contact = null;
      if (isExistingInvestor && contact_id) {
        const { data } = await sb.from('contacts').select('*').eq('id', contact_id).single();
        contact = data || null;
      }

      const baseTranscript = {
        deal_id: contact?.deal_id || null,
        contact_id: contact?.id || null,
        investor_name: contact?.name || investor_name,
        investor_email: contact?.email || investor_email || null,
        investor_phone: contact?.phone || investor_phone || null,
        investor_linkedin: contact?.linkedin_url || investor_linkedin || null,
        is_new_investor: !isExistingInvestor,
        transcript_text: transcriptBody,
      };
      const { data: insertedTranscript, error: insertErr } = await sb.from('meeting_transcripts').insert(baseTranscript).select('*').single();
      if (insertErr) throw new Error(insertErr.message);

      const analysis = await analyzeMeetingTranscript({
        transcriptText: transcriptBody,
        investorName: baseTranscript.investor_name,
        linkedContact: contact,
      });
      if (req.file?.path) try { fs.unlinkSync(req.file.path); } catch {}

      const historyEntry = buildConversationHistoryEntry({
        type: 'meeting',
        date: new Date().toISOString(),
        summary: analysis.summary || analysis.follow_up_actions[0] || 'Meeting transcript processed',
        sentiment: analysis.sentiment_score,
      });

      if (contact) {
        const existingHistory = asJsonArray(contact.conversation_history);
        const updatePayload = {
          aum: analysis.aum || contact.aum || null,
          investment_thesis: analysis.investment_thesis || contact.investment_thesis || null,
          past_investments: analysis.past_investments,
          intent_signals: analysis.intent_signals,
          meeting_count: Number(contact.meeting_count || 0) + 1,
          last_meeting_date: new Date().toISOString(),
          conversation_history: [...existingHistory, historyEntry],
          company_name: analysis.firm_name || contact.company_name || investor_firm || null,
          job_title: analysis.role_title || contact.job_title || null,
          sector_focus: analysis.sectors_of_interest.join(', ') || contact.sector_focus || null,
          typical_cheque_size: analysis.cheque_size_range || contact.typical_cheque_size || null,
          pipeline_stage: 'Meeting Held',
        };
        const { data } = await sb.from('contacts').update(updatePayload).eq('id', contact.id).select('*').single();
        contact = data || contact;
      } else {
        const { data } = await sb.from('contacts').insert({
          deal_id: null,
          name: investor_name,
          email: investor_email || null,
          phone: investor_phone || null,
          linkedin_url: investor_linkedin || null,
          company_name: analysis.firm_name || investor_firm || null,
          job_title: analysis.role_title || null,
          aum: analysis.aum || null,
          investment_thesis: analysis.investment_thesis || null,
          past_investments: analysis.past_investments,
          intent_signals: analysis.intent_signals,
          meeting_count: 1,
          last_meeting_date: new Date().toISOString(),
          conversation_history: [historyEntry],
          sector_focus: analysis.sectors_of_interest.join(', ') || null,
          typical_cheque_size: analysis.cheque_size_range || null,
          notes: analysis.summary || null,
          pipeline_stage: 'Meeting Held',
        }).select('*').single();
        contact = data || null;
      }

      const investorDbRecord = await upsertTranscriptInvestorDatabaseRecord(sb, {
        analysis: {
          ...analysis,
          investor_type: analysis.investor_type || investor_category || null,
        },
        contact,
        investorEmail: baseTranscript.investor_email,
        investorName: analysis.investor_name || baseTranscript.investor_name,
        firmName: analysis.firm_name || investor_firm || null,
      });

      await sb.from('meeting_transcripts').update({
        contact_id: contact?.id || insertedTranscript.contact_id || null,
        summary: analysis.summary || null,
        investment_interests: analysis.investment_interests,
        intent_signals: analysis.intent_signals,
        sentiment_score: analysis.sentiment_score,
        follow_up_actions: analysis.follow_up_actions,
        raw_analysis: analysis.raw,
      }).eq('id', insertedTranscript.id);

      await sendTelegram(
        `Transcript processed for ${baseTranscript.investor_name}. Sentiment: ${analysis.sentiment_score}/10. ${analysis.positive_intent_signals.length} intent signals extracted. ${analysis.follow_up_actions.length} follow-up actions identified.`
      ).catch(() => {});

      res.json({
        success: true,
        transcript_id: insertedTranscript.id,
        contact_id: contact?.id || null,
        investors_db_id: investorDbRecord?.id || null,
        analysis,
      });
    } catch (err) {
      if (req.file?.path) try { fs.unlinkSync(req.file.path); } catch {}
      res.status(500).json({ error: err.message });
    }
  });

  // Legacy raise-progress endpoint
  app.post('/api/raise-progress', (req, res) => {
    const { committed, target } = req.body;
    if (rocoState) {
      rocoState.committed = committed;
      rocoState.target = target || rocoState?.deal?.raiseAmount;
    }
    const state = readState();
    if (state.activeDeals?.[0]) {
      if (committed !== undefined) state.activeDeals[0].currentCommitted = Number(committed);
      writeState(state);
    }
    broadcastToAll({ type: 'raise_update', committed, target });
    res.json({ ok: true });
  });

  // ─── DEALS ────────────────────────────────────────────────────────────────

  // GET /api/deals — all deals with live contact/email/response counts
  app.get('/api/deals', async (req, res) => {
    try {
      const deals = await getAllDeals();
      const sb = getSupabase();
      if (!sb || !deals?.length) return res.json(deals || []);

      const dealIds = deals.map(d => d.id);
      const channelMetricsByDeal = await computeDealChannelMetrics(sb, dealIds);

      // Fetch current campaign batch status for all deals
      const { data: campaignBatches } = await sb.from('campaign_batches')
        .select('deal_id, id, batch_number, status, ranked_firms, target_firms')
        .in('deal_id', dealIds)
        .in('status', ['researching', 'pending_approval', 'approved'])
        .order('created_at', { ascending: false });

      // Map to most-recent batch per deal
      const batchByDeal = {};
      for (const b of (campaignBatches || [])) {
        if (!batchByDeal[b.deal_id]) batchByDeal[b.deal_id] = b;
      }

      const batchIds = Object.values(batchByDeal).map(b => b.id).filter(Boolean);
      const firmCountByBatch = {};
      if (batchIds.length) {
        const { data: batchFirmRows } = await sb.from('batch_firms')
          .select('batch_id')
          .in('batch_id', batchIds);
        for (const row of (batchFirmRows || [])) {
          firmCountByBatch[row.batch_id] = (firmCountByBatch[row.batch_id] || 0) + 1;
        }
      }

      const enriched = deals.map(deal => {
        const metrics = channelMetricsByDeal[deal.id] || createEmptyDealChannelMetrics();
        const batch = batchByDeal[deal.id] || null;
        const liveFirmCount = batch?.id ? (firmCountByBatch[batch.id] || 0) : 0;

        return {
          ...deal,
          contacts: metrics.contacts,
          live_firms: liveFirmCount,
          firms: liveFirmCount,
          active_prospects: metrics.active_prospects,
          emails_sent: metrics.emails_sent,
          emails_replied: metrics.emails_replied,
          li_invites_sent: metrics.li_invites_sent,
          li_accepts: metrics.li_accepts,
          li_dms_sent: metrics.li_dms_sent,
          li_dm_replies: metrics.li_dm_replies,
          response_rate: metrics.overall_response_rate,
          email_response_rate: metrics.email_response_rate,
          li_acceptance_rate: metrics.li_acceptance_rate,
          li_dm_response_rate: metrics.li_dm_response_rate,
          live_contacts: metrics.contacts,
          live_active_prospects: metrics.active_prospects,
          live_emails_sent: metrics.emails_sent,
          live_response_rate: metrics.overall_response_rate,
          live_responses: metrics.total_responses,
          current_batch_status: batch?.status || null,
          current_batch_number: batch?.batch_number || null,
          current_batch_id: batch?.id || null,
          current_batch_ranked_firms: liveFirmCount,
          current_batch_target_firms: batch?.target_firms || 100,
        };
      });

      res.json(enriched);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/history — archived/closed deals
  app.get('/api/deals/history', async (req, res) => {
    try {
      const deals = await getAllDeals();
      res.json(deals.filter(d => d.status !== 'ACTIVE'));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id — single deal with batches + schedule status
  app.get('/api/deals/:id', async (req, res) => {
    try {
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });
      const [batches, windowStatus, windowViz] = await Promise.all([
        getBatchSummary(deal.id),
        Promise.resolve(getWindowStatus(deal, null)),
        Promise.resolve(getWindowVisualization(deal)),
      ]);

      // Fetch parsed_deal_info and saved PitchBook metadata if available
      let parsed_deal_info = null;
      let pitchbook = {
        investor_universe_lists: [],
        comparable_deals_count: 0,
        priority_lists: [],
      };
      try {
        const sb = getSupabase();
        if (sb) {
          const [
            { data: doc },
            linkedListsResult,
            { count: comparablesCount },
          ] = await Promise.all([
            sb.from('deal_documents')
              .select('parsed_deal_info')
              .eq('deal_id', deal.id)
              .order('created_at', { ascending: false })
              .limit(1)
              .maybeSingle(),
            sb.from('deal_list_priorities')
              .select('list_id, list_name, priority_order, source, status')
              .eq('deal_id', deal.id)
              .order('priority_order', { ascending: true }),
            sb.from('deal_intelligence')
              .select('id', { count: 'exact', head: true })
              .eq('deal_id', deal.id),
          ]);
          let linkedLists = linkedListsResult?.data || [];
          if (linkedListsResult?.error) {
            const fallback = await sb.from('deal_list_priorities')
              .select('list_id, list_name, priority_order, status')
              .eq('deal_id', deal.id)
              .order('priority_order', { ascending: true });
            linkedLists = fallback?.data || [];
            if (fallback?.error) {
              const minimal = await sb.from('deal_list_priorities')
                .select('list_id, list_name, priority_order')
                .eq('deal_id', deal.id)
                .order('priority_order', { ascending: true });
              linkedLists = minimal?.data || [];
            }
          }
          parsed_deal_info = doc?.parsed_deal_info || null;
          const kbListId   = deal.knowledge_base_list_id   || deal.settings?.knowledge_base_list_id   || null;
          const kbListName = deal.knowledge_base_list_name || deal.settings?.knowledge_base_list_name || null;
          // Fetch KB list record for live count
          let kbList = null;
          if (kbListId && sb) {
            const { data: kbRec } = await sb.from('investor_lists').select('id, name, list_type').eq('id', kbListId).maybeSingle();
            const { count: kbCount } = await sb.from('investors_db').select('*', { count: 'exact', head: true }).eq('list_id', kbListId);
            if (kbRec) kbList = { ...kbRec, investor_count: kbCount || 0 };
          }
          pitchbook = {
            investor_universe_lists: (linkedLists || []).filter(list => !list.source || list.source === 'pitchbook'),
            comparable_deals_count: comparablesCount || 0,
            priority_lists: linkedLists || [],
            kb_list: kbList || (kbListId ? { id: kbListId, name: kbListName, investor_count: 0 } : null),
          };
        }
      } catch {}

      res.json({ ...deal, batches, windowStatus, windowVisualization: windowViz, parsed_deal_info, pitchbook });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/create — create new deal
  // multer handles multipart/form-data (CSV upload path); JSON path uses express.json()
  const dealUpload = multer({ storage: multer.memoryStorage() }).single('csvFile');

  app.post('/api/deals/create', (req, res, next) => {
    dealUpload(req, res, (err) => {
      if (err) console.warn('[DEAL LAUNCH] multer error (non-fatal):', err.message);
      next();
    });
  }, async (req, res) => {
    try {
      console.log('[DEAL LAUNCH] Content-Type:', req.headers['content-type']);
      console.log('[DEAL LAUNCH] Body keys:', Object.keys(req.body || {}));
      console.log('[DEAL LAUNCH] Body:', JSON.stringify(req.body));

      // Auto-detect currency from deal brief text
      const detectCurrency = (texts) => {
        const combined = texts.filter(Boolean).join(' ');
        // Check for explicit currency field first
        if (req.body.currency) return req.body.currency.toUpperCase();
        // Count occurrences of each symbol — most frequent wins
        const gbp = (combined.match(/£/g) || []).length + (combined.match(/\bpound[s]?\b|\bGBP\b/gi) || []).length;
        const usd = (combined.match(/\$/g) || []).length + (combined.match(/\bdollar[s]?\b|\bUSD\b/gi) || []).length;
        const eur = (combined.match(/€/g) || []).length + (combined.match(/\beuro[s]?\b|\bEUR\b/gi) || []).length;
        if (gbp > usd && gbp > eur) return 'GBP';
        if (eur > usd && eur > gbp) return 'EUR';
        return 'USD'; // default
      };

      // Strip currency symbols, commas, spaces before parsing monetary values
      const parseAmount = (val) => {
        if (!val) return 0;
        const cleaned = String(val).replace(/[£$€,\s]/g, '');
        // Handle shorthand: 5m = 5000000, 500k = 500000
        const shorthand = cleaned.match(/^([\d.]+)\s*([kmb]?)$/i);
        if (shorthand) {
          const num = parseFloat(shorthand[1]);
          const suffix = (shorthand[2] || '').toLowerCase();
          if (suffix === 'k') return num * 1_000;
          if (suffix === 'm') return num * 1_000_000;
          if (suffix === 'b') return num * 1_000_000_000;
          return num || 0;
        }
        return parseFloat(cleaned) || 0;
      };

      const name = (
        req.body?.name ||
        req.body?.dealName ||
        req.body?.deal_name ||
        req.body?.['deal-name'] ||
        ''
      ).toString().trim();

      if (!name) {
        console.error('[DEAL LAUNCH] Name missing. Full body:', JSON.stringify(req.body));
        return res.status(400).json({
          error: 'Deal name is required',
          received_keys: Object.keys(req.body || {}),
        });
      }

      // geography arrives as JSON string when sent via FormData
      let geography = req.body.geography || '';
      if (typeof geography === 'string' && geography.startsWith('[')) {
        try { geography = JSON.parse(geography); } catch {}
      }
      if (Array.isArray(geography)) geography = geography.join(', ');

      const descriptionText = req.body.description || '';
      const keyMetricsText = req.body.keyMetrics || req.body.key_metrics || '';
      const investorText = req.body.investorProfile || req.body.investor_profile || '';
      const dealCurrency = (req.body.currency || 'USD').toString().trim().toUpperCase() || 'USD';
      const priorityLists = (() => {
        try { return JSON.parse(req.body.priority_lists || '[]'); } catch { return []; }
      })();
      const primaryPriorityList = [...priorityLists]
        .sort((a, b) => Number(a.priority_order || 999) - Number(b.priority_order || 999))[0] || null;
      const knowledgeBaseListId   = req.body.knowledge_base_list_id   || null;
      const knowledgeBaseListName = req.body.knowledge_base_list_name || null;

      const deal = {
        name,
        currency:              dealCurrency,
        raise_type:            req.body.raiseType || req.body.raise_type || 'Equity',
        target_amount:         parseAmount(req.body.targetAmount || req.body.target_amount),
        min_cheque:            parseAmount(req.body.minCheque || req.body.min_cheque),
        max_cheque:            parseAmount(req.body.maxCheque || req.body.max_cheque),
        sector:                req.body.sector || '',
        geography,
        target_geography:      req.body.target_geography || (Array.isArray(geography) ? geography.join(', ') : geography) || 'Global',
        description:           descriptionText,
        key_metrics:           keyMetricsText,
        investor_profile:      investorText,
        deck_url:              req.body.deckUrl || req.body.deck_url || '',
        linkedin_seeds:        req.body.linkedinSeeds || req.body.linkedin_seeds || '',
        batch_size:            parseInt(req.body.batchSize || 15),
        max_emails_per_day:    parseInt(req.body.maxEmailsPerDay || 20),
        max_emails_per_hour:   parseInt(req.body.maxEmailsPerHour || 5),
        max_contacts_per_firm: parseInt(req.body.maxContactsPerFirm || 3),
        max_total_outreach:    parseInt(req.body.maxTotalOutreach || 200),
        send_from:        req.body.emailFrom  || req.body.sendFrom  || req.body.send_from  || '06:00',
        send_until:       req.body.emailUntil || req.body.sendUntil || req.body.send_until || '18:00',
        li_connect_from:  req.body.liConnectFrom  || null,
        li_connect_until: req.body.liConnectUntil || null,
        li_dm_from:       req.body.liDmFrom  || '20:00',
        li_dm_until:      req.body.liDmUntil || '23:00',
        timezone:              req.body.timezone || 'America/New_York',
        active_days:           req.body.activeDays || 'Mon,Tue,Wed,Thu,Fri',
        status:                'ACTIVE',
        committed_amount:      0,
        emails_sent:           0,
        response_rate:         0,
        active_prospects:      0,
        created_by:            'dashboard',
        sending_account_id:    req.body.sending_account_id || null,
        sending_email:         req.body.sending_email || null,
        priority_list_id:        primaryPriorityList?.list_id || null,
        priority_list_name:      primaryPriorityList?.list_name || null,
        knowledge_base_list_id:  knowledgeBaseListId,
        knowledge_base_list_name: knowledgeBaseListName,
      };
      console.log(`[DEAL LAUNCH] Using currency: ${dealCurrency} for "${name}"`);

      const supabase = getSupabase();
      let insertResult = await supabase
        .from('deals')
        .insert(deal)
        .select()
        .single();

      if (insertResult.error && isMissingColumnError(insertResult.error, 'target_geography')) {
        const fallbackDeal = { ...deal };
        delete fallbackDeal.target_geography;
        insertResult = await supabase
          .from('deals')
          .insert(fallbackDeal)
          .select()
          .single();
      }

      // KB columns may not exist in all deployments — store in settings JSONB as fallback
      if (insertResult.error && (
        isMissingColumnError(insertResult.error, 'knowledge_base_list_id') ||
        isMissingColumnError(insertResult.error, 'knowledge_base_list_name')
      )) {
        const kbId   = deal.knowledge_base_list_id;
        const kbName = deal.knowledge_base_list_name;
        const fallbackDeal = { ...deal };
        delete fallbackDeal.knowledge_base_list_id;
        delete fallbackDeal.knowledge_base_list_name;
        if (kbId || kbName) {
          fallbackDeal.settings = {
            ...(fallbackDeal.settings || {}),
            knowledge_base_list_id:   kbId,
            knowledge_base_list_name: kbName,
          };
        }
        insertResult = await supabase.from('deals').insert(fallbackDeal).select().single();
      }

      const { data: savedDeal, error: supabaseError } = insertResult;

      if (supabaseError) {
        error('Supabase deal insert error: ' + supabaseError.message);
        return res.status(500).json({ error: supabaseError.message });
      }

      // Save priority lists if provided
      if (priorityLists.length) {
        try {
          await supabase.from('deal_list_priorities').insert(
            priorityLists.map(l => ({
              deal_id: savedDeal.id,
              list_id: l.list_id,
              list_name: l.list_name,
              priority_order: l.priority_order,
              status: 'pending',
            }))
          );
        } catch (e) {
          console.warn('[DEAL] priority lists insert:', e.message);
        }
      }

      // Save exclusion list if provided
      const exclusions = (() => {
        try { return JSON.parse(req.body.exclusions || '[]'); } catch { return []; }
      })();
      if (exclusions?.length > 0) {
        const exclusionRows = exclusions.map(e => ({
          deal_id:     savedDeal.id,
          firm_name:   e.firm_name   ? e.firm_name.toLowerCase().trim()   : null,
          person_name: e.person_name ? e.person_name.toLowerCase().trim() : null,
          email:       e.email       ? e.email.toLowerCase().trim()       : null,
          added_by:    req.session?.displayName || 'Dom',
        }));
        try {
          await getSupabase().from('deal_exclusions').insert(exclusionRows);
        } catch (e) {
          console.warn('[DEAL] exclusions insert:', e.message);
        }
        pushActivity({ type: 'system', action: `Exclusion list loaded: ${exclusions.length} entries`, note: name });
      }

      pushActivity({ type: 'SYSTEM', action: 'Deal Launched', note: name });
      await sbLogActivity({ dealId: savedDeal.id, eventType: 'DEAL_CREATED', summary: `Deal "${name}" created` }).catch(() => {});
      const tgTarget = savedDeal.target_amount > 0
        ? formatCurrencyAmount(savedDeal.target_amount, savedDeal.currency)
        : '—';
      await sendTelegram(`New deal launched: ${name}\nTarget: ${tgTarget}\nSector: ${savedDeal.sector || '—'}\nRaise Type: ${savedDeal.raise_type || '—'}\n\nRoco is on it.`).catch(() => {});

      // Auto-insert deck_url into deal_assets
      const deckUrl = savedDeal.deck_url || '';
      const calendlyUrl = req.body.calendlyUrl || req.body.calendly_url || '';
      let launchAssets = [];
      try { launchAssets = JSON.parse(req.body.launchAssets || '[]'); } catch {}

      const assetRows = [];
      if (deckUrl) assetRows.push({ deal_id: savedDeal.id, name: 'Pitch Deck', asset_type: 'deck', url: deckUrl });
      if (calendlyUrl) assetRows.push({ deal_id: savedDeal.id, name: 'Book a Call', asset_type: 'calendly', url: calendlyUrl });
      for (const a of launchAssets) {
        if (a.name && a.url && a.asset_type) assetRows.push({ deal_id: savedDeal.id, name: a.name, asset_type: a.asset_type, url: a.url });
      }
      if (assetRows.length) {
        const sbAssets = getSupabase();
        if (sbAssets) {
          try { await sbAssets.from('deal_assets').insert(assetRows); }
          catch(e) { console.warn('[DEAL] asset insert:', e.message); }
        }
      }

      // Link previously-parsed document to this deal
      const documentId = req.body.document_id || req.body.documentId;
      if (documentId) {
        try {
          await getSupabase()?.from('deal_documents')
            .update({ deal_id: savedDeal.id })
            .eq('id', documentId);
        } catch (e) { console.warn('[DEAL] document link failed:', e.message); }
      }

      broadcastToAll({ type: 'DEAL_CREATED', deal: savedDeal });
      info(`Deal created: ${name} (id: ${savedDeal.id})`);
      res.json({ success: true, deal: savedDeal });

      const pendingIntelligenceUploads = String(req.body.pending_intelligence_uploads || '').toLowerCase() === 'true';

      // Fire post-launch setup immediately — non-blocking.
      // Templates and sequence should always be generated on launch.
      // Only research waits for PitchBook uploads to finish.
      setImmediate(async () => {
        const broadcast = (msg, type = 'research') => pushActivity({
          type, action: msg, note: '', dealId: savedDeal.id, deal_name: savedDeal.name,
        });

        try {
          broadcast(`Deal launched: ${savedDeal.name}`, 'system');

          // Auto-generate deal-specific templates — fire in background, never block launch
          (async () => {
            try {
              const { generateDealTemplates } = await import('../core/templateGenerator.js');
              await generateDealTemplates(savedDeal, req.session?.displayName || 'Dom');
              broadcast('Templates generated for ' + savedDeal.name, 'system');
              pushActivity({ type: 'system', action: `Templates ready: ${savedDeal.name}`, note: 'AI email + LinkedIn templates generated', dealId: savedDeal.id });
            } catch (e) {
              console.warn('[DEAL LAUNCH] Template generation failed:', e.message);
              pushActivity({ type: 'error', action: `Template generation failed: ${savedDeal.name}`, note: e.message?.slice(0, 80), dealId: savedDeal.id });
            }
          })();

          // Seed default outreach sequence for this deal
          try {
            const sb = getSupabase();
            if (sb) {
              await sb.from('deal_sequence').insert({
                deal_id: savedDeal.id,
                steps: [
                  { step: 1, type: 'email',           label: 'email_intro',      delay_days: 0,  description: 'Cold intro email' },
                  { step: 2, type: 'linkedin_invite', label: 'linkedin_invite',  delay_days: 0,  description: 'LinkedIn connection request' },
                  { step: 3, type: 'linkedin_dm',     label: 'linkedin_dm_1',    delay_days: 0,  description: 'LinkedIn DM after connection accepted' },
                  { step: 4, type: 'email',           label: 'email_followup_1', delay_days: 7,  description: 'Follow-up email if no reply' },
                  { step: 5, type: 'linkedin_dm',     label: 'linkedin_dm_2',    delay_days: 0,  description: 'LinkedIn follow-up DM' },
                  { step: 6, type: 'email',           label: 'email_followup_2', delay_days: 14, description: 'Second follow-up if no reply' },
                ],
                sending_window: { start_hour: 8, end_hour: 18, days: [1,2,3,4,5] },
                updated_at: new Date().toISOString(),
              });
            }
          } catch (e) {
            console.warn('[DEAL LAUNCH] Sequence seed failed:', e.message);
          }

          if (pendingIntelligenceUploads) {
            broadcast('PitchBook uploads pending — research will start after imports complete', 'system');
            return;
          }

          // Step 1: CSV ingest if file was uploaded
          const csvContent = req.body?.csvData ||
            (req.file?.buffer ? req.file.buffer.toString('utf-8') : null);

          if (csvContent) {
            broadcast('Importing CSV investors...');
            try {
              const { ingestCSV } = await import('../research/csvIngestor.js');
              const count = await ingestCSV({
                csvContent,
                dealId: savedDeal.id,
                dealName: savedDeal.name,
                broadcastFn: (msg) => broadcast(msg),
              });
              broadcast(`CSV: ${count} contacts imported`, 'research');
            } catch (e) {
              console.error('[DEAL LAUNCH] CSV ingest failed:', e.message);
              broadcast(`CSV ingest error: ${e.message}`, 'error');
            }
          }

          // Step 2: Firm-first research (Grok/Gemini finds firms, then contacts at each firm)
          broadcast(`Starting firm research for ${savedDeal.name}...`);
          try {
            const { runFirmResearch } = await import('../research/firmResearcher.js');
            await runFirmResearch(savedDeal);
          } catch (e) {
            console.error('[DEAL LAUNCH] Firm research failed:', e.message);
            broadcast(`Research error: ${e.message}`, 'error');
            // Fallback to legacy research
            try {
              const { runDealResearch } = await import('../research/dealResearcher.js');
              await runDealResearch(savedDeal);
            } catch (e2) {
              broadcast(`Legacy research also failed: ${e2.message}`, 'error');
            }
          }

          // Step 3: Full orchestrator cycle (rank, enrich, sync, outreach)
          try {
            const { triggerImmediateRun } = await import('../core/orchestrator.js');
            await triggerImmediateRun(savedDeal.id);
          } catch (e) {
            info(`Orchestrator immediate trigger failed (will pick up next cycle): ${e.message}`);
          }

        } catch (err) {
          console.error('[DEAL LAUNCH] Research sequence failed:', err.message);
          broadcast(`Research error: ${err.message}`, 'error');
        }
      });
    } catch (err) {
      error('Deal launch error: ' + err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // PATCH /api/deals/:id — update deal settings
  app.patch('/api/deals/:id', async (req, res) => {
    try {
      const updates = {};
      const allowed = [
        // Core
        'name', 'status', 'sector', 'geography', 'description', 'raise_type', 'currency',
        'target_geography',
        'target_amount', 'committed_amount', 'emails_sent', 'response_rate', 'active_prospects',
        'key_metrics', 'deck_url', 'investor_profile', 'linkedin_seeds', 'notes',
        // Cheque sizes — new column names
        'min_cheque', 'max_cheque',
        // Legacy column names (kept for backward compat)
        'minimum_cheque', 'maximum_cheque',
        // Schedule — new column names
        'send_from', 'send_until', 'timezone', 'active_days',
        // Schedule — legacy column names
        'sending_days', 'sending_start', 'sending_end', 'sending_timezone',
        // Per-channel windows (flat columns)
        'li_connect_from', 'li_connect_until', 'li_dm_from', 'li_dm_until',
        // Rate limits
        'max_emails_per_day', 'max_emails_per_hour', 'batch_size',
        'followup_cadence_days', 'max_contacts_per_firm', 'max_total_outreach',
        'min_investor_score', 'prioritise_hot_leads', 'include_unscored',
        'linkedin_daily_limit', 'followup_days_li', 'followup_days_email', 'no_follow_ups',
        // Pipeline cap
        'pipeline_max', 'pipeline_refill_threshold',
        // Pause / archive
        'paused', 'paused_at', 'outreach_paused_until',
        'archived_at', 'archived_reason',
        // Email account + priority list
        'sending_account_id', 'sending_email',
        'priority_list_id', 'priority_list_name',
      ];
      for (const key of allowed) {
        if (req.body[key] !== undefined) {
          // Guard: don't store empty strings for timezone (would cause fallback to Europe/London)
          if (key === 'timezone' && !req.body[key]) continue;
          updates[key] = req.body[key];
        }
      }
      let deal;
      try {
        deal = await updateDeal(req.params.id, updates);
      } catch (err) {
        const fallbackUpdates = { ...updates };
        const existingDeal = await getDeal(req.params.id);

        if (fallbackUpdates.target_geography && isMissingColumnError(err, 'target_geography')) {
          delete fallbackUpdates.target_geography;
        }
        if (fallbackUpdates.no_follow_ups !== undefined && (
          isMissingColumnError(err, 'no_follow_ups')
          || isMissingColumnError(err, 'settings')
        )) {
          fallbackUpdates.parsed_deal_info = mergeParsedDealInfo(existingDeal?.parsed_deal_info, {
            no_follow_ups: !!fallbackUpdates.no_follow_ups,
          });
          delete fallbackUpdates.no_follow_ups;
        }

        deal = await updateDeal(req.params.id, fallbackUpdates);
      }
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      res.json({ success: true, deal });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PATCH /api/deals/:id/settings — update deal per-channel windows (flat columns, no JSONB needed)
  app.patch('/api/deals/:id/settings', async (req, res) => {
    try {
      const { timezone, currency, email_send_window, linkedin_connection_window, linkedin_dm_window } = req.body;

      const updates = {};
      if (timezone) updates.timezone = timezone;
      if (currency) updates.currency = currency.toUpperCase();
      // Map window objects to flat columns
      if (email_send_window?.start) updates.send_from = email_send_window.start;
      if (email_send_window?.end)   updates.send_until = email_send_window.end;
      if (linkedin_connection_window?.start) updates.li_connect_from  = linkedin_connection_window.start;
      if (linkedin_connection_window?.end)   updates.li_connect_until = linkedin_connection_window.end;
      if (linkedin_dm_window?.start) updates.li_dm_from  = linkedin_dm_window.start;
      if (linkedin_dm_window?.end)   updates.li_dm_until = linkedin_dm_window.end;

      if (!Object.keys(updates).length) return res.json({ success: true });

      const deal = await updateDeal(req.params.id, updates);
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/pause
  app.post('/api/deals/:id/pause', async (req, res) => {
    try {
      const deal = await updateDeal(req.params.id, {
        status: 'PAUSED',
        paused: true,
        paused_at: new Date().toISOString(),
      });
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      pushActivity({ type: 'system', action: 'Deal Paused', note: `${deal.name} — paused by user`, dealId: deal.id, deal_name: deal.name });
      res.json({ success: true, deal });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/resume
  app.post('/api/deals/:id/resume', async (req, res) => {
    try {
      const deal = await updateDeal(req.params.id, {
        status: 'ACTIVE',
        paused: false,
        paused_at: null,
      });
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      pushActivity({ type: 'system', action: 'Deal Resumed', note: `${deal.name} — resumed by user`, dealId: deal.id, deal_name: deal.name });
      const { sendTelegram } = await import('../approval/telegramBot.js');
      await sendTelegram(`▶️ *Deal Resumed: ${deal.name}*\n\nRoco is back online for this deal.\nWebhooks active. Outreach resuming on next cycle.`).catch(() => {});
      res.json({ success: true, deal });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/close
  app.post('/api/deals/:id/close', async (req, res) => {
    try {
      const dealId = req.params.id;
      const deal = await updateDeal(dealId, {
        status: 'CLOSED',
        paused: false,
        closed_at: new Date().toISOString(),
        archived_at: new Date().toISOString(),
        archived_reason: 'closed',
      });

      // ── Mark all contacts Inactive so investors become available for future deals ──
      try {
        const sb = getSupabase();
        if (sb) {
          await sb.from('contacts').update({ pipeline_stage: 'Inactive' }).eq('deal_id', dealId);
          info(`[CLOSE] Set all contacts for deal ${dealId} to Inactive`);
        }
      } catch (e) { console.warn('[CLOSE] Failed to set contacts Inactive:', e.message); }

      // ── Immediate cleanup: wipe all pending activity for this deal ──────────

      // 1. Clear in-memory Telegram approval queue for this deal
      const clearedApprovals = clearApprovalsForDeal(dealId);

      // 2. Clear in-memory debounce batches for this deal (inbound reply handling)
      clearDebounceForDeal(dealId);

      // 3. Hard-delete from Supabase approval_queue table
      try {
        const sb = getSupabase();
        if (sb) {
          // approval_queue rows link via contact_id → contacts.deal_id
          const { data: dealContacts } = await sb.from('contacts')
            .select('id').eq('deal_id', dealId);
          const contactIds = (dealContacts || []).map(c => c.id);
          if (contactIds.length) {
            await sb.from('approval_queue')
              .delete()
              .in('company_contact_id', contactIds)
              .eq('status', 'pending');
          }
          info(`[CLOSE] Wiped approval_queue rows for deal ${dealId}`);
        }
      } catch (e) { console.warn('[CLOSE] approval_queue wipe failed:', e.message); }

      // 4. Signal orchestrator to skip this deal
      const { clearDealFromFlight } = await import('../core/orchestrator.js');
      clearDealFromFlight(dealId);

      info(`[CLOSE] Deal "${deal.name}" closed — cleared ${clearedApprovals} approval(s) from queue`);
      await sbLogActivity({ dealId, eventType: 'DEAL_CLOSED', summary: `Deal "${deal.name}" closed` });
      await sendTelegram(`Deal closed: ${deal.name}\nFinal committed: ${formatCurrencyAmount(deal.committed_amount, deal.currency)} of ${formatCurrencyAmount(deal.target_amount, deal.currency)} target\n\nAll outreach stopped. Deal archived.`).catch(() => {});
      pushActivity({ type: 'system', action: `Deal closed: ${deal.name}`, note: `All outreach stopped — ${clearedApprovals} pending approval(s) cleared`, dealId, deal_name: deal.name });
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      broadcastToAll({ type: 'DEAL_CLOSED', dealId });

      // Release any contacts held in other deals because of this deal closing
      try {
        const sb = getSupabase();
        const holdPattern = `%[CROSS_DEAL_HOLD:${req.params.id}|%`;
        const { data: heldContacts } = await sb.from('contacts')
          .select('id, name, deal_id, notes')
          .eq('pipeline_stage', 'Skipped')
          .ilike('notes', holdPattern);

        if (heldContacts?.length) {
          const when = new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
          const priorNote = `[PRIOR_DEAL:${deal.name}|contacted:${when}]`;
          const holdRegex = new RegExp(`\\n?\\[CROSS_DEAL_HOLD:${req.params.id}\\|[^\\]]*\\]`, 'g');

          for (const c of heldContacts) {
            const cleanedNotes = (c.notes || '').replace(holdRegex, '').trim();
            await sb.from('contacts').update({
              pipeline_stage: 'Researched',
              notes: cleanedNotes ? `${cleanedNotes}\n${priorNote}` : priorNote,
            }).eq('id', c.id);
          }
          pushActivity({ type: 'PIPELINE', action: 'Contacts Released', note: `${heldContacts.length} cross-deal held contacts released into pipeline after "${deal.name}" closed` });
          info(`[CLOSE] Released ${heldContacts.length} cross-deal held contacts`);
        }
      } catch (e) {
        console.warn('[CLOSE] Failed to release cross-deal holds:', e.message);
      }

      res.json({ success: true, deal });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/trigger-invites — manually send LinkedIn invites to Ranked/Enriched contacts
  app.post('/api/deals/:id/trigger-invites', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });

      const { data: contacts } = await sb.from('contacts')
        .select('*')
        .eq('deal_id', req.params.id)
        .in('pipeline_stage', ['Ranked', 'Enriched'])
        .not('linkedin_url', 'is', null)
        .is('invite_sent_at', null)
        .limit(10);

      if (!contacts?.length) return res.json({ sent: 0, message: 'No contacts ready for invites' });

      let pendingInvites = [];
      try {
        pendingInvites = await listSentInvitations(100);
      } catch {}

      let sent = 0;
      const results = [];
      for (const contact of contacts) {
        try {
          const outcome = await processLinkedInInvite({
            sb,
            deal,
            contact,
            pushActivity,
            logActivity: sbLogActivity,
            pendingInvites,
            source: 'dashboard_manual_trigger',
          });
          if (['sent', 'already_pending', 'already_connected'].includes(outcome.status)) sent++;
          results.push({ name: contact.name, status: outcome.status });
          await new Promise(r => setTimeout(r, 2000));
        } catch (e) {
          results.push({ name: contact.name, status: 'failed', error: e.message });
        }
      }
      pushActivity({ type: 'LINKEDIN', action: 'Invites Sent', note: `${sent} LinkedIn invites sent manually` });
      res.json({ sent, results });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.delete('/api/deals/:id', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      const id = req.params.id;

      const childTables = [
        'activity_log',
        'conversation_messages',
        'approval_queue',
        'contacts',
        'batch_firms',
        'campaign_batches',
        'deal_intelligence',
        'deal_investor_scores',
        'deal_templates',
        'deal_sequence',
        'deal_exclusions',
        'deal_assets',
        'deal_documents',
        'emails',
        'replies',
        'linkedin_messages',
        'deal_contacts',
        'schedule_log',
        'weekly_reports',
        'investor_deal_history',
      ];

      for (const table of childTables) {
        try {
          await sb.from(table).delete().eq('deal_id', id);
        } catch (err) {
          console.warn(`[DELETE DEAL] ${table}:`, err.message);
        }
      }

      const { error: deleteError } = await sb.from('deals').delete().eq('id', id);
      if (deleteError) throw deleteError;

      broadcastToAll({ type: 'DEAL_DELETED', dealId: id });
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: 'Failed to delete deal: ' + err.message });
    }
  });

  // POST /api/deals/:id/clear-pipeline — delete ALL contacts for this deal
  app.post('/api/deals/:id/clear-pipeline', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });

      const { data: contacts } = await sb.from('contacts')
        .select('id, name')
        .eq('deal_id', req.params.id);

      const count = contacts?.length || 0;

      const did = req.params.id;
      // Full memory wipe — contacts + all pipeline state for this deal
      await sb.from('contacts').delete().eq('deal_id', did);
      const pipelineTables = [
        'emails', 'replies', 'linkedin_messages', 'firms',
        'firm_suppressions', 'firm_responses', 'schedule_log', 'deal_contacts', 'batches',
      ];
      for (const t of pipelineTables) {
        try { await sb.from(t).delete().eq('deal_id', did); } catch {}
      }

      pushActivity({ type: 'SYSTEM', action: 'Pipeline Cleared', note: `${count} contacts cleared from "${deal.name}"` });
      broadcastToAll({ type: 'PIPELINE_CLEARED', dealId: did });
      await sbLogActivity({ dealId: did, eventType: 'PIPELINE_CLEARED', summary: `Pipeline cleared — ${count} contacts removed` }).catch(() => {});

      res.json({ success: true, cleared: count });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ── Deal Assets ─────────────────────────────────────────────────────────────

  // GET /api/deals/:id/assets
  app.get('/api/deals/:id/assets', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error } = await sb.from('deal_assets')
        .select('*')
        .eq('deal_id', req.params.id)
        .order('created_at', { ascending: true });
      if (error) return res.status(500).json({ error: error.message });
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/assets — add an asset
  app.post('/api/deals/:id/assets', async (req, res) => {
    const { name, asset_type, url, description } = req.body;
    if (!name || !asset_type || !url) return res.status(400).json({ error: 'name, asset_type, url required' });
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error } = await sb.from('deal_assets')
        .insert({ deal_id: req.params.id, name, asset_type, url, description: description || null })
        .select().single();
      if (error) return res.status(500).json({ error: error.message });
      pushActivity({ type: 'system', action: `Asset added: ${name}`, note: asset_type });
      res.json(data);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/deals/:id/assets/:assetId
  app.delete('/api/deals/:id/assets/:assetId', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { error } = await sb.from('deal_assets')
        .delete()
        .eq('id', req.params.assetId)
        .eq('deal_id', req.params.id); // safety: scope to this deal
      if (error) return res.status(500).json({ error: error.message });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/capital — update capital committed (Dom's manual input)
  app.post('/api/deals/:id/capital', async (req, res) => {
    const { amount } = req.body;
    if (amount == null || isNaN(Number(amount))) return res.status(400).json({ error: 'amount required' });
    try {
      const deal = await updateDeal(req.params.id, { committed_amount: Number(amount) });
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      pushActivity({
        type: 'system',
        action: `Capital updated: ${deal.name}`,
        note: `${formatCurrencyAmount(amount, deal.currency)} committed`,
      });
      res.json({ success: true, committed_amount: Number(amount) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ────────────────────────────────────────────────────────────────────────────

  // GET /api/deals/:id/metrics — live metrics from contacts table
  app.get('/api/deals/:id/metrics', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });

      const channelMetricsByDeal = await computeDealChannelMetrics(sb, [req.params.id]);
      const channelMetrics = channelMetricsByDeal[req.params.id] || createEmptyDealChannelMetrics();
      const [
        { data: emailRows },
        { count: firmsCount },
      ] = await Promise.all([
        sb.from('emails').select('id, metadata').eq('deal_id', req.params.id).eq('status', 'sent'),
        sb.from('batch_firms').select('id', { count: 'exact', head: true }).eq('deal_id', req.params.id),
      ]);
      const outboundEmails = emailRows || [];

      const emailsSent      = channelMetrics.emails_sent;
      const emailReplies    = channelMetrics.emails_replied;
      const emailsOpened    = outboundEmails.reduce((sum, row) => sum + (Number(row?.metadata?.opens_count || 0) > 0 ? 1 : 0), 0);
      const emailsClicked   = outboundEmails.reduce((sum, row) => sum + (Number(row?.metadata?.clicks_count || 0) > 0 ? 1 : 0), 0);
      const invitesSent     = channelMetrics.li_invites_sent;
      const activePendingInvites = channelMetrics.li_active_pending;
      const invitesAccepted = channelMetrics.li_accepts;
      const dmsSent         = channelMetrics.li_dms_sent;
      const dmResponses     = channelMetrics.li_dm_replies;
      const activeProspects = channelMetrics.active_prospects;

      res.json({
        totalContacts:      channelMetrics.contacts,
        activeProspects,
        invitesSent,
        activePendingInvites,
        invitesAccepted,
        acceptanceRate:     channelMetrics.li_acceptance_rate,
        dmsSent,
        dmResponses,
        dmResponseRate:     channelMetrics.li_dm_response_rate,
        emailsSent,
        emailReplies,
        emailsOpened,
        emailOpenRate:      emailsSent > 0 ? Math.round((emailsOpened / emailsSent) * 100) : 0,
        emailsClicked,
        emailClickRate:     emailsSent > 0 ? Math.round((emailsClicked / emailsSent) * 100) : 0,
        emailResponseRate:  channelMetrics.email_response_rate,
        totalResponses:     channelMetrics.total_responses,
        overallResponseRate: channelMetrics.overall_response_rate,
        firms:              firmsCount || 0,
        capitalCommitted:   deal.committed_amount || 0,
        targetAmount:       deal.target_amount || 0,
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/archived — archived contacts with reason
  app.get('/api/deals/:id/archived', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('contacts')
        .select('id, name, company_name, job_title, linkedin_url, investor_score, notes, created_at')
        .eq('deal_id', req.params.id)
        .eq('pipeline_stage', 'Archived')
        .order('investor_score', { ascending: false })
        .limit(500);
      if (dbErr) return res.status(500).json({ error: dbErr.message });

      const mapped = (data || []).map(c => {
        // Extract last score rationale from notes: "[SCORE: X — Grade] reason text"
        const matches = [...(c.notes || '').matchAll(/\[SCORE:\s*(\d+)\s*—\s*(\w+)\]\s*([^\n\[]+)/g)];
        const last = matches[matches.length - 1];
        return {
          id: c.id,
          name: c.name,
          firm: c.company_name || '',
          jobTitle: c.job_title || '',
          score: c.investor_score,
          archiveReason: last ? last[3].trim() : 'Score below threshold',
          grade: last ? last[2] : 'Archive',
        };
      });
      res.json(mapped);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/rankings — ranked contacts for a deal (sorted by investor_score desc)
  app.get('/api/deals/:id/rankings', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { page = 1, limit = 50 } = req.query;
      const offset = (Number(page) - 1) * Number(limit);
      const { data, error: dbErr, count } = await sb.from('contacts')
        .select('id, name, company_name, job_title, linkedin_url, investor_score, pipeline_stage, sector_focus, geography, typical_cheque_size, notes, source, enrichment_status, email, phone, last_email_sent_at, invite_sent_at, created_at, past_investments, person_researched, investors_db_id', { count: 'exact' })
        .eq('deal_id', req.params.id)
        .not('investor_score', 'is', null)
        .order('investor_score', { ascending: false })
        .range(offset, offset + Number(limit) - 1);
      if (dbErr) return res.status(500).json({ error: dbErr.message });
      res.json({
        contacts: data || [],
        total: count || 0,
        page: Number(page),
        limit: Number(limit),
        pages: Math.ceil((count || 0) / Number(limit)),
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/batches — campaign batch list for a deal
  app.get('/api/deals/:id/batches', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      const { data } = await sb.from('campaign_batches')
        .select('*')
        .eq('deal_id', req.params.id)
        .order('batch_number', { ascending: true });
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── CAMPAIGN BATCHES (single batch firm-first flow) ──────────────────────

  app.get('/api/deals/:id/batch/current', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      const { data } = await sb.from('campaign_batches')
        .select('*')
        .eq('deal_id', req.params.id)
        .not('status', 'in', '("completed","skipped")')
        .order('batch_number', { ascending: false })
        .limit(1).maybeSingle();
      if (!data) return res.json(null);
      const { data: firmRows, count } = await sb.from('batch_firms')
        .select('id', { count: 'exact' })
        .eq('batch_id', data.id);
      const liveFirmCount = count ?? firmRows?.length ?? 0;
      res.json({
        ...data,
        firms_target: data.firms_target || data.target_firms || 100,
        firms_researched: liveFirmCount,
        ranked_firms: liveFirmCount,
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/kanban — firms for current batch grouped by kanban stage
  app.get('/api/deals/:id/kanban', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      const dealId = req.params.id;

      // Match the same batch query as batch/current — exclude only completed/skipped
      const { data: batch } = await sb.from('campaign_batches')
        .select('*')
        .eq('deal_id', dealId)
        .not('status', 'in', '("completed","skipped")')
        .order('batch_number', { ascending: false })
        .limit(1).maybeSingle();

      if (!batch) {
        info(`[kanban] no active batch found for deal ${dealId}`);
        return res.json({ batch: null, columns: {}, _debug: { reason: 'no_batch', deal_id: dealId } });
      }

      // Get all firms for this batch
      // Note: select('*') avoids the `rank` reserved-word clash in PostgREST
      const { data: firmRows, error: firmErr } = await sb.from('batch_firms')
        .select('*')
        .eq('batch_id', batch.id)
        .order('score', { ascending: false });

      if (firmErr) throw firmErr;

      info(`[kanban] deal=${dealId} batch=${batch.id} status=${batch.status} firms=${firmRows?.length ?? 0}`);

      // Try contacts filtered by batch_id first; fall back to deal_id only
      // (contacts pre-dating batch tracking may lack batch_id)
      let contacts = [];
      const { data: batchContacts } = await sb.from('contacts')
        .select('id, name, job_title, company_name, pipeline_stage, response_received, last_reply_at, last_email_sent_at, last_outreach_at, invite_sent_at, invite_accepted_at, investor_score')
        .eq('deal_id', dealId)
        .eq('batch_id', batch.id);

      if (batchContacts?.length) {
        contacts = batchContacts;
      } else {
        // Fallback: all contacts for this deal (ignore batch_id)
        const { data: dealContacts } = await sb.from('contacts')
          .select('id, name, job_title, company_name, pipeline_stage, response_received, last_reply_at, last_email_sent_at, last_outreach_at, invite_sent_at, invite_accepted_at, investor_score')
          .eq('deal_id', dealId);
        contacts = dealContacts || [];
        info(`[kanban] batch_id contact lookup returned 0 — fell back to deal-level contacts (${contacts.length})`);
      }

      // Group contacts by normalised firm name
      const contactsByFirm = new Map();
      for (const c of contacts) {
        const key = normalizeFirmLookupName(c.company_name);
        if (!key) continue;
        if (!contactsByFirm.has(key)) contactsByFirm.set(key, []);
        contactsByFirm.get(key).push(c);
      }

      const columns = { queued: [], contacted: [], connected: [], engaged: [], meeting_booked: [], passed: [], exhausted: [] };

      for (const row of (firmRows || [])) {
        const firmContacts = contactsByFirm.get(normalizeFirmLookupName(row.firm_name)) || [];
        const summary = buildFirmCampaignSummary(firmContacts, row.enrichment_status || 'pending');

        const sorted = [...firmContacts].sort((a, b) => Number(b.investor_score || 0) - Number(a.investor_score || 0));
        const top = sorted[0];

        const firmData = {
          id: row.id,
          firm_name: row.firm_name,
          score: row.score || 0,
          firm_stage: summary.firm_stage,
          firm_stage_label: summary.firm_stage_label,
          total_contacts: summary.total_contacts,
          top_contact: top ? { name: top.name, title: top.job_title } : null,
        };

        switch (summary.firm_stage) {
          case 'meeting_booked':   columns.meeting_booked.push(firmData); break;
          case 'replied':          columns.engaged.push(firmData); break;
          case 'invite_accepted':  columns.connected.push(firmData); break;
          case 'outreach_started': columns.contacted.push(firmData); break;
          case 'closed':           columns.passed.push(firmData); break;
          default:                 columns.queued.push(firmData);
        }
      }

      res.json({
        batch: { id: batch.id, status: batch.status, batch_number: batch.batch_number },
        columns,
        _debug: { firms: firmRows?.length ?? 0, contacts: contacts.length },
      });
    } catch (err) {
      error(`[kanban] ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/deals/:id/kanban/debug — raw diagnostics (no auth needed in dev, requireAuth in prod)
  app.get('/api/deals/:id/kanban/debug', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      const dealId = req.params.id;

      const { data: allBatches } = await sb.from('campaign_batches')
        .select('id, status, batch_number, created_at')
        .eq('deal_id', dealId)
        .order('batch_number', { ascending: false });

      const { count: totalFirms } = await sb.from('batch_firms')
        .select('id', { count: 'exact', head: true })
        .eq('deal_id', dealId);

      const { count: contactsWithBatch } = await sb.from('contacts')
        .select('id', { count: 'exact', head: true })
        .eq('deal_id', dealId)
        .not('batch_id', 'is', null);

      const { count: contactsNoBatch } = await sb.from('contacts')
        .select('id', { count: 'exact', head: true })
        .eq('deal_id', dealId)
        .is('batch_id', null);

      res.json({ deal_id: dealId, batches: allBatches, totalFirms, contactsWithBatch, contactsNoBatch });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/deals/:id/batch/:batchId/firms', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      const wantsPagination = req.query.page != null || req.query.limit != null;
      const page = Math.max(1, parseInt(req.query.page || '1', 10));
      const limit = Math.min(20, Math.max(1, parseInt(req.query.limit || '20', 10)));
      const offset = (page - 1) * limit;

      let firmQuery = sb.from('batch_firms')
        .select('*', wantsPagination ? { count: 'exact' } : undefined)
        .eq('batch_id', req.params.batchId)
        .order('score', { ascending: false });

      if (wantsPagination) {
        firmQuery = firmQuery.range(offset, offset + limit - 1);
      }

      const { data, error, count } = await firmQuery;
      if (error) return res.status(500).json({ error: error.message });

      const { data: contacts, error: contactsError } = await sb.from('contacts')
        .select('id, company_name, pipeline_stage, response_received, last_reply_at, last_email_sent_at, last_outreach_at, invite_sent_at, invite_accepted_at')
        .eq('deal_id', req.params.id)
        .eq('batch_id', req.params.batchId);
      if (contactsError) return res.status(500).json({ error: contactsError.message });

      const contactsByFirm = new Map();
      for (const contact of (contacts || [])) {
        const key = normalizeFirmLookupName(contact.company_name);
        if (!key) continue;
        if (!contactsByFirm.has(key)) contactsByFirm.set(key, []);
        contactsByFirm.get(key).push(contact);
      }

      const rows = [...(data || [])].sort((a, b) => {
        const rankDiff = Number(a.rank || 9999) - Number(b.rank || 9999);
        if (rankDiff !== 0) return rankDiff;
        return Number(b.score || 0) - Number(a.score || 0);
      });
      const baseSummarized = rows.map(row => ({
        ...buildFirmCampaignSummary(
          contactsByFirm.get(normalizeFirmLookupName(row.firm_name)) || [],
          row.enrichment_status || 'pending'
        ),
        id: row.id,
        firm_name: row.firm_name,
        score: row.score || 0,
        rank: row.rank || null,
        justification: row.justification || null,
        thesis: row.thesis || null,
        past_investments: Array.isArray(row.past_investments) ? row.past_investments : [],
        aum: row.aum || null,
        contact_type: row.contact_type || 'individual_at_firm',
        status: row.status || 'pending',
        enrichment_status: row.enrichment_status || 'pending',
        contacts_found: row.contacts_found || 0,
      }));
      const summarized = await enrichCampaignFirmLinks(baseSummarized);

      if (!wantsPagination) {
        return res.json(summarized);
      }

      return res.json({
        firms: summarized,
        total: count || 0,
        page,
        pages: Math.ceil((count || 0) / limit) || 1,
        per_page: limit,
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.post('/api/deals/:id/batch/:batchId/approve', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();

      const { data: activeBatch } = await sb.from('campaign_batches')
        .select('id, batch_number')
        .eq('deal_id', req.params.id)
        .in('status', ['approved'])
        .neq('id', req.params.batchId)
        .limit(1).maybeSingle();

      if (activeBatch) {
        return res.status(400).json({
          error: `Batch ${activeBatch.batch_number} is already active. Close it first.`
        });
      }

      await sb.from('campaign_batches').update({
        status: 'approved',
        approved_at: new Date().toISOString(),
        approved_by: req.session?.displayName || 'Dom',
        updated_at: new Date().toISOString(),
      }).eq('id', req.params.batchId);

      // Normalize legacy/mis-cased firm states so the approved batch can enter enrichment.
      await sb.from('batch_firms')
        .update({ enrichment_status: 'pending' })
        .eq('batch_id', req.params.batchId)
        .in('enrichment_status', ['Pending', 'PENDING']);
      await sb.from('batch_firms')
        .update({ enrichment_status: 'pending' })
        .eq('batch_id', req.params.batchId)
        .is('enrichment_status', null);

      pushActivity({
        type: 'system',
        action: `Campaign approved — batch ${req.params.batchId.slice(0, 6)}`,
        note: 'Contact enrichment starting now, in rank order',
        dealId: req.params.id,
      });

      res.json({ success: true });

      setImmediate(async () => {
        try {
          const { triggerImmediateRun } = await import('../core/orchestrator.js');
          await triggerImmediateRun(req.params.id);
        } catch (err) {
          console.error('[CAMPAIGN APPROVAL] Immediate run failed:', err.message);
        }
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/campaign-reviews — all batches awaiting approval, with their firms
  app.get('/api/campaign-reviews', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: batches } = await sb.from('campaign_batches')
        .select('id, deal_id, batch_number, status, ranked_firms, created_at, updated_at')
        .eq('status', 'pending_approval')
        .order('updated_at', { ascending: false });
      if (!batches?.length) return res.json([]);
      // Enrich each batch with deal name and top firms
      const dealIds = [...new Set(batches.map(b => b.deal_id))];
      const { data: deals } = await sb.from('deals').select('id, name, sector, raise_type').in('id', dealIds);
      const dealMap = Object.fromEntries((deals || []).map(d => [d.id, d]));
      const reviews = await Promise.all(batches.map(async (batch) => {
        const { data: firms } = await sb.from('batch_firms')
          .select('id, firm_name, score, justification, contact_type')
          .eq('batch_id', batch.id)
          .order('score', { ascending: false })
          .limit(20);
        return {
          ...batch,
          deal_name: dealMap[batch.deal_id]?.name || 'Unknown Deal',
          deal_sector: dealMap[batch.deal_id]?.sector || '',
          deal_raise_type: dealMap[batch.deal_id]?.raise_type || '',
          firms: firms || [],
        };
      }));
      res.json(reviews);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.post('/api/deals/:id/campaign/:batchId/firms', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      const { firm_name, investors_db_id } = req.body || {};
      if (!firm_name?.trim()) return res.status(400).json({ error: 'firm_name required' });
      const dealId = req.params.id;
      const normalizedFirm = normalizeFirmLookupName(firm_name);

      let { data: batch } = await sb.from('campaign_batches')
        .select('id, status, batch_number')
        .eq('id', req.params.batchId)
        .eq('deal_id', dealId)
        .maybeSingle();

      if (!batch || !['pending_approval', 'approved'].includes(batch.status)) {
        const { data: fallbackBatch } = await sb.from('campaign_batches')
          .select('id, status, batch_number')
          .eq('deal_id', dealId)
          .in('status', ['pending_approval', 'approved'])
          .order('updated_at', { ascending: false })
          .limit(1)
          .maybeSingle();
        batch = fallbackBatch || null;
      }

      if (!batch) {
        const { data: maxBatch } = await sb.from('campaign_batches')
          .select('batch_number')
          .eq('deal_id', dealId)
          .order('batch_number', { ascending: false })
          .limit(1)
          .maybeSingle();
        const { data: createdBatch, error: batchError } = await sb.from('campaign_batches')
          .insert({
            deal_id: dealId,
            batch_number: (maxBatch?.batch_number || 0) + 1,
            status: 'pending_approval',
            target_firms: 1,
            ranked_firms: 0,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
          })
          .select('id, status, batch_number')
          .single();
        if (batchError) throw batchError;
        batch = createdBatch;
      }

      const { data: existingBatchFirm } = await sb.from('batch_firms')
        .select('id, firm_name')
        .eq('deal_id', dealId)
        .eq('batch_id', batch.id);
      const firmAlreadyQueued = (existingBatchFirm || []).some(row =>
        normalizeFirmLookupName(row.firm_name) === normalizedFirm
      );
      if (firmAlreadyQueued) {
        return res.json({ success: true, batch_id: batch.id, duplicate: true, researched: !!investors_db_id });
      }

      const { data: existingContact } = await sb.from('contacts')
        .select('id, pipeline_stage')
        .eq('deal_id', dealId)
        .ilike('company_name', firm_name.trim())
        .limit(1)
        .maybeSingle();
      if (existingContact) {
        return res.json({
          success: true,
          batch_id: batch.id,
          duplicate: true,
          existing_contact: true,
          pipeline_stage: existingContact.pipeline_stage || null,
          researched: !!investors_db_id,
        });
      }

      const { data: investor } = investors_db_id
        ? await sb.from('investors_db').select('*').eq('id', investors_db_id).maybeSingle()
        : { data: null };

      const score = Number(investor?.investor_score || investor?.score || 50);
      const contactType = investor?.is_angel ? 'angel' : (investor?.contact_type || 'individual_at_firm');
      const insertPayload = {
        batch_id: batch.id,
        deal_id: dealId,
        investor_id: investor?.id || null,
        firm_name: firm_name.trim(),
        contact_type: contactType,
        score,
        thesis: investor?.description?.slice(0, 500) || null,
        past_investments: [],
        aum: investor?.aum_millions ? `$${investor.aum_millions}M` : null,
        justification: 'Manually added by reviewer',
        firm_researched: !!investor,
        enrichment_status: 'pending',
        status: 'pending',
        created_at: new Date().toISOString(),
      };

      const { data: inserted, error } = await sb.from('batch_firms')
        .insert(insertPayload)
        .select('id')
        .single();
      if (error) throw error;

      const { count } = await sb.from('batch_firms')
        .select('id', { count: 'exact', head: true })
        .eq('batch_id', batch.id);
      await sb.from('campaign_batches')
        .update({ ranked_firms: count || 0, updated_at: new Date().toISOString() })
        .eq('id', batch.id);

      pushActivity({
        type: 'system',
        action: `Firm manually added: ${firm_name.trim()}`,
        note: `Batch ${batch.batch_number || 'manual'} · ${batch.status}`,
        dealId,
      });

      if (batch.status === 'approved') {
        setImmediate(async () => {
          try {
            const { triggerImmediateRun } = await import('../core/orchestrator.js');
            await triggerImmediateRun(dealId);
          } catch (err) {
            console.error('[MANUAL FIRM ADD] Immediate run failed:', err.message);
          }
        });
      }

      res.json({ success: true, id: inserted?.id, batch_id: batch.id, researched: !!investor });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.delete('/api/deals/:id/batch/:batchId/firms/:firmId', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      await sb.from('batch_firms')
        .delete()
        .eq('id', req.params.firmId)
        .eq('batch_id', req.params.batchId);
      const { count } = await sb.from('batch_firms')
        .select('id', { count: 'exact', head: true })
        .eq('batch_id', req.params.batchId);
      await sb.from('campaign_batches')
        .update({ ranked_firms: count || 0, updated_at: new Date().toISOString() })
        .eq('id', req.params.batchId);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/batch/:batchId/firms/:firmId/contacts — contacts found for a specific firm
  app.get('/api/deals/:id/batch/:batchId/firms/:firmId/contacts', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      const { data: firm } = await sb.from('batch_firms')
        .select('firm_name')
        .eq('id', req.params.firmId)
        .maybeSingle();
      if (!firm) return res.json([]);
      const { data, error } = await sb.from('contacts')
        .select('id, name, job_title, email, linkedin_url, pipeline_stage, enrichment_status, investor_score, response_received, last_reply_at, last_email_sent_at, last_outreach_at, invite_sent_at, invite_accepted_at')
        .eq('deal_id', req.params.id)
        .eq('batch_id', req.params.batchId)
        .ilike('company_name', firm.firm_name)
        .order('investor_score', { ascending: false });
      if (error) throw error;
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.post('/api/deals/:id/batch/:batchId/close', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();

      await sb.from('campaign_batches').update({
        status: 'completed',
        closed_at: new Date().toISOString(),
        closed_by: req.session?.displayName || 'Dom',
        updated_at: new Date().toISOString(),
      }).eq('id', req.params.batchId).eq('deal_id', req.params.id);

      await sb.from('batch_firms')
        .update({ status: 'completed' })
        .eq('batch_id', req.params.batchId);

      pushActivity({
        type: 'system',
        action: 'Batch closed — next batch will begin on next cycle',
        note: 'Research will start for the next 20 firms automatically',
        dealId: req.params.id,
      });

      await sendTelegram(
        `✅ *Batch Closed*\n\nNext batch will begin researching automatically.\nRoco will exclude all firms from previous batches.`
      ).catch(() => {});

      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/deals/:id/batch/:batchId/skip — skip a batch that hasn't started outreach
  app.post('/api/deals/:id/batch/:batchId/skip', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });

      const { data: batch } = await sb.from('campaign_batches')
        .select('status, batch_number').eq('id', req.params.batchId).single();
      if (!batch) return res.status(404).json({ error: 'Batch not found' });

      if (batch.status === 'active') {
        return res.status(400).json({ error: 'Cannot skip an active batch. Close it first.' });
      }

      await sb.from('campaign_batches').update({
        status: 'skipped',
        closed_at: new Date().toISOString(),
        closed_by: req.session?.displayName || 'Dom',
        skip_reason: req.body?.reason || 'Manually skipped',
        updated_at: new Date().toISOString(),
      }).eq('id', req.params.batchId);

      await sb.from('batch_firms')
        .update({ status: 'skipped' })
        .eq('batch_id', req.params.batchId);

      pushActivity({
        type: 'system',
        action: `Batch ${batch.batch_number} skipped`,
        note: req.body?.reason || 'Manually skipped by user',
        dealId: req.params.id,
      });

      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.post('/api/deals/:id/trigger-research', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      const { data: deal } = await sb.from('deals').select('*').eq('id', req.params.id).maybeSingle();
      if (!deal) return res.status(404).json({ error: 'Deal not found' });
      const { triggerImmediateRun } = await import('../core/orchestrator.js');
      await triggerImmediateRun(deal.id);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/email-accounts — list connected email accounts via Unipile
  app.get('/api/email-accounts', async (req, res) => {
    try {
      const accounts = await getConnectedEmailAccounts();
      res.json(accounts);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  const intelligenceUpload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 25 * 1024 * 1024 },
  });

  // POST /api/deals/:id/import-intelligence — import PitchBook XLSX (investor list or deal comparables)
  app.post('/api/deals/:id/import-intelligence', requireAuth, intelligenceUpload.single('file'), async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

      const buffer = req.file.buffer;
      const filename = req.file.originalname;
      const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
      const ws = wb.Sheets[wb.SheetNames[0]];
      let rows = XLSX.utils.sheet_to_json(ws, { range: 7, defval: null });
      if (!rows.length) rows = XLSX.utils.sheet_to_json(ws, { defval: null });
      rows = normalizeSheetRows(rows);
      if (!rows.length) return res.status(400).json({ error: 'No data rows found in file' });

      const firstRowKeys = Object.keys(rows[0] || {});
      const isInvestorFile = firstRowKeys.includes('Investor ID') && firstRowKeys.includes('Investors');
      const isDealFile = firstRowKeys.includes('Deal ID') && firstRowKeys.includes('Companies');

      const clientHint = String(req.body?.fileType || '').toLowerCase().trim();
      const useInvestorParser = clientHint === 'investors' || (!clientHint && isInvestorFile);
      const useDealParser = clientHint === 'intelligence' || clientHint === 'deals' || (!clientHint && isDealFile);

      if (!useInvestorParser && !useDealParser) {
        return res.status(400).json({
          error: 'Cannot detect file type. Expected PitchBook investor export (has "Investor ID") or deals export (has "Deal ID").',
          detected_columns: firstRowKeys.slice(0, 10),
        });
      }

      if (useInvestorParser) {
        const result = await importInvestorUniverse(sb, rows, req.params.id, filename);
        return res.json({ success: true, type: 'investors', ...result });
      }

      if (useDealParser) {
        const result = await importComparableDeals(sb, rows, req.params.id);
        return res.json({ success: true, type: 'intelligence', ...result });
      }
    } catch (err) {
      console.error('[IMPORT INTELLIGENCE]', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/investors/search — search investors DB for manual firm add
  app.get('/api/investors/search', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      const { q } = req.query;
      if (!q || q.length < 2) return res.json([]);
      const { data } = await sb.from('firms')
        .select('id, name, sector, hq_location, website')
        .ilike('name', `%${q}%`)
        .limit(20);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/schedule — schedule status for a deal
  app.get('/api/deals/:id/schedule', async (req, res) => {
    try {
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });
      const state = readState();
      const status = getWindowStatus(deal, state.outreachPausedUntil);
      const visualization = getWindowVisualization(deal);
      res.json({ status, visualization });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/pause-outreach — pause outreach for a specific deal until datetime
  app.post('/api/deals/:id/pause-outreach', async (req, res) => {
    try {
      const { until } = req.body;
      const deal = await updateDeal(req.params.id, { outreach_paused_until: until || null });
      res.json({ success: true, deal });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── EXCLUSION LIST ───────────────────────────────────────────────────────

  // GET /api/deals/:id/exclusions
  app.get('/api/deals/:id/exclusions', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data, error } = await sb.from('deal_exclusions')
        .select('*')
        .eq('deal_id', req.params.id)
        .order('added_at', { ascending: false });
      if (error) throw error;
      res.json(data || []);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/deals/:id/exclusions — add single entry manually
  app.post('/api/deals/:id/exclusions', async (req, res) => {
    try {
      const sb = getSupabase();
      const { firm_name, person_name, email } = req.body;
      if (!firm_name && !person_name && !email) {
        return res.status(400).json({ error: 'At least one identifier required' });
      }
      const { data, error } = await sb.from('deal_exclusions').insert({
        deal_id:     req.params.id,
        firm_name:   firm_name   ? firm_name.toLowerCase().trim()   : null,
        person_name: person_name ? person_name.toLowerCase().trim() : null,
        email:       email       ? email.toLowerCase().trim()       : null,
        added_by:    req.session?.displayName || 'Dom',
      }).select().single();
      if (error) throw error;
      pushActivity({ type: 'system', action: `Exclusion added: ${firm_name || person_name || email}`, note: '' });

      // Invalidate cache so orchestrator picks up new entry immediately
      try {
        const { invalidateExclusionCache } = await import('../core/exclusionCheck.js');
        invalidateExclusionCache(req.params.id);
      } catch (_) {}

      res.json({ success: true, exclusion: data });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // DELETE /api/deals/:id/exclusions/:exclusionId
  app.delete('/api/deals/:id/exclusions/:exclusionId', async (req, res) => {
    try {
      const sb = getSupabase();
      await sb.from('deal_exclusions')
        .delete()
        .eq('id', req.params.exclusionId)
        .eq('deal_id', req.params.id);

      try {
        const { invalidateExclusionCache } = await import('../core/exclusionCheck.js');
        invalidateExclusionCache(req.params.id);
      } catch (_) {}

      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/deals/:id/exclusions/upload — bulk upload XLSX or CSV
  const exclUpload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } }).single('file');

  const FIRM_INDICATORS = [
    'llc', 'lp', 'llp', 'inc', 'ltd', 'corp', 'capital', 'partners', 'partner',
    'management', 'fund', 'group', 'ventures', 'venture', 'equity', 'investments',
    'investment', 'advisors', 'advisory', 'associates', 'holdings', 'trust',
    'asset', 'assets', 'financial', 'finance', 'securities', 'family office',
    'family', 'solutions', 'services', 'consulting',
  ];

  function classifyAccountName(accountName, firstName, lastName) {
    if (!accountName) return { firm_name: null, person_name: null };
    const lower = accountName.toLowerCase();
    if (FIRM_INDICATORS.some(ind => lower.includes(ind))) {
      return { firm_name: accountName, person_name: [firstName, lastName].filter(Boolean).join(' ') || null };
    }
    const fullNameFromCols = [firstName, lastName].filter(Boolean).join(' ').toLowerCase();
    if (fullNameFromCols && lower === fullNameFromCols) {
      return { firm_name: null, person_name: accountName };
    }
    const words = accountName.trim().split(/\s+/);
    if (words.length <= 2) {
      return { firm_name: null, person_name: accountName };
    }
    return { firm_name: accountName, person_name: [firstName, lastName].filter(Boolean).join(' ') || null };
  }

  function parseExclusionFile(buffer, originalname) {
    const wb = XLSX.read(buffer, { type: 'buffer' });
    const ws = wb.Sheets[wb.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(ws, { defval: null });
    return rows.map(row => {
      const norm = {};
      Object.entries(row).forEach(([k, v]) => { norm[k.toLowerCase().replace(/\s+/g, '_')] = v; });
      const accountName = norm.account_name || norm.firm || norm.company || norm.organization || norm.name || null;
      const firstName   = norm.first_name || norm.firstname || null;
      const lastName    = norm.last_name  || norm.lastname  || null;
      const email       = norm.contact_email || norm.email || norm.email_address || null;
      const { firm_name, person_name } = classifyAccountName(
        accountName ? String(accountName).trim() : null,
        firstName   ? String(firstName).trim()   : null,
        lastName    ? String(lastName).trim()     : null,
      );
      return {
        firm_name:   firm_name   ? firm_name.toLowerCase().trim()   : null,
        person_name: person_name ? person_name.toLowerCase().trim() : null,
        email:       email       ? String(email).toLowerCase().trim() : null,
      };
    }).filter(r => r.firm_name || r.person_name || r.email);
  }

  app.post('/api/deals/:id/exclusions/upload', (req, res) => {
    exclUpload(req, res, async (err) => {
      if (err) return res.status(400).json({ error: err.message });
      try {
        const sb = getSupabase();
        if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
        const parsed = parseExclusionFile(req.file.buffer, req.file.originalname);
        if (!parsed.length) return res.status(400).json({ error: 'No valid rows found in file' });

        const rows = parsed.map(p => ({
          deal_id:     req.params.id,
          firm_name:   p.firm_name,
          person_name: p.person_name,
          email:       p.email,
          added_by:    req.session?.displayName || 'Dom',
        }));

        let imported = 0;
        for (let i = 0; i < rows.length; i += 100) {
          const batch = rows.slice(i, i + 100);
          const { error: bErr } = await sb.from('deal_exclusions').insert(batch);
          if (!bErr) imported += batch.length;
        }

        try {
          const { invalidateExclusionCache } = await import('../core/exclusionCheck.js');
          invalidateExclusionCache(req.params.id);
        } catch (_) {}

        pushActivity({ type: 'system', action: `Exclusion list imported: ${imported} entries`, note: req.file.originalname });
        res.json({ success: true, imported });
      } catch (e) {
        console.error('[EXCLUSIONS UPLOAD]', e.message);
        res.status(500).json({ error: e.message });
      }
    });
  });

  // ─── DEAL PRIORITY LISTS & KB ─────────────────────────────────────────────

  // POST /api/deals/:id/priority-lists — attach an existing investor list to this deal
  app.post('/api/deals/:id/priority-lists', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { list_id, list_name, priority_order } = req.body;
      if (!list_id) return res.status(400).json({ error: 'list_id required' });
      // Get current max priority_order for this deal
      const { data: existing } = await sb.from('deal_list_priorities')
        .select('priority_order').eq('deal_id', req.params.id).order('priority_order', { ascending: false }).limit(1);
      const nextOrder = ((existing?.[0]?.priority_order ?? -1)) + 1;
      const { error } = await sb.from('deal_list_priorities').upsert({
        deal_id: req.params.id,
        list_id,
        list_name: list_name || list_id,
        priority_order: priority_order ?? nextOrder,
        status: 'pending',
      }, { onConflict: 'deal_id,list_id' });
      if (error) return res.status(500).json({ error: error.message });
      const { data: primaryList } = await sb.from('deal_list_priorities')
        .select('list_id, list_name')
        .eq('deal_id', req.params.id)
        .order('priority_order', { ascending: true })
        .limit(1)
        .maybeSingle();
      await sb.from('deals').update({
        priority_list_id: primaryList?.list_id || null,
        priority_list_name: primaryList?.list_name || null,
        updated_at: new Date().toISOString(),
      }).eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/deals/:id/priority-lists/:listId — detach a priority list from this deal
  app.delete('/api/deals/:id/priority-lists/:listId', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      await sb.from('deal_list_priorities').delete().eq('deal_id', req.params.id).eq('list_id', req.params.listId);
      const { data: primaryList } = await sb.from('deal_list_priorities')
        .select('list_id, list_name')
        .eq('deal_id', req.params.id)
        .order('priority_order', { ascending: true })
        .limit(1)
        .maybeSingle();
      await sb.from('deals').update({
        priority_list_id: primaryList?.list_id || null,
        priority_list_name: primaryList?.list_name || null,
        updated_at: new Date().toISOString(),
      }).eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PATCH /api/deals/:id/kb — update the knowledge base for this deal
  app.patch('/api/deals/:id/kb', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { kb_list_id, kb_list_name } = req.body;
      const updates = { updated_at: new Date().toISOString() };
      // Try dedicated columns first; fall back to settings JSONB if columns don't exist
      try {
        await sb.from('deals').update({
          ...updates,
          knowledge_base_list_id:   kb_list_id   || null,
          knowledge_base_list_name: kb_list_name || null,
        }).eq('id', req.params.id);
      } catch {
        const { data: current } = await sb.from('deals').select('settings').eq('id', req.params.id).single();
        await sb.from('deals').update({
          ...updates,
          settings: { ...(current?.settings || {}), knowledge_base_list_id: kb_list_id || null, knowledge_base_list_name: kb_list_name || null },
        }).eq('id', req.params.id);
      }
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── DEAL TEMPLATES ────────────────────────────────────────────────────────

  // GET /api/deals/:id/templates
  app.get('/api/deals/:id/templates', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data: dealTemplates } = await sb.from('deal_templates')
        .select('*').eq('deal_id', req.params.id).order('created_at');
      const { data: globalTemplates } = await sb.from('email_templates')
        .select('*').eq('is_active', true).order('created_at');
      res.json({ deal: dealTemplates || [], global: globalTemplates || [] });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/templates
  app.post('/api/deals/:id/templates', async (req, res) => {
    try {
      const sb = getSupabase();
      const { name, type, sequence_step, subject_a, subject_b, body, is_primary, notes } = req.body;
      if (!name || !body) return res.status(400).json({ error: 'name and body required' });
      if (is_primary && sequence_step) {
        await sb.from('deal_templates').update({ is_primary: false })
          .eq('deal_id', req.params.id).eq('sequence_step', sequence_step);
      }
      const { data, error: insErr } = await sb.from('deal_templates').insert({
        deal_id: req.params.id, name, type: type || 'email',
        sequence_step: sequence_step || null,
        subject_a: subject_a || null, subject_b: subject_b || null,
        body, is_primary: !!is_primary, notes: notes || null,
      }).select().single();
      if (insErr) throw insErr;
      res.json({ success: true, template: data });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PUT /api/deals/:id/templates/:tmplId
  app.put('/api/deals/:id/templates/:tmplId', async (req, res) => {
    try {
      const sb = getSupabase();
      const { name, subject_a, subject_b, body, is_primary, notes, sequence_step } = req.body;
      if (is_primary && sequence_step) {
        await sb.from('deal_templates').update({ is_primary: false })
          .eq('deal_id', req.params.id).eq('sequence_step', sequence_step)
          .neq('id', req.params.tmplId);
      }
      const { data, error: updErr } = await sb.from('deal_templates').update({
        name, subject_a, subject_b, body, is_primary: !!is_primary,
        notes, sequence_step, updated_at: new Date().toISOString(),
      }).eq('id', req.params.tmplId).eq('deal_id', req.params.id).select().single();
      if (updErr) throw updErr;
      res.json({ success: true, template: data });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/deals/:id/templates/:tmplId
  app.delete('/api/deals/:id/templates/:tmplId', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data: t } = await sb.from('deal_templates').select('is_primary')
        .eq('id', req.params.tmplId).single();
      if (t?.is_primary) return res.status(400).json({ error: 'Cannot delete a primary template' });
      await sb.from('deal_templates').delete().eq('id', req.params.tmplId);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/templates/regenerate — regenerate AI templates using latest deal data
  app.post('/api/deals/:id/templates/regenerate', requireAuth, async (req, res) => {
    try {
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });
      const { generateDealTemplates } = await import('../core/templateGenerator.js');
      const rows = await generateDealTemplates(deal, req.session?.displayName || 'Dom');
      pushActivity({ type: 'system', action: `Templates regenerated for ${deal.name}`, note: `${rows.length} templates updated with latest deal data`, dealId: deal.id });
      res.json({ success: true, count: rows.length });
    } catch (err) {
      console.error('[TEMPLATE REGEN]', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/deals/:id/sequence
  app.get('/api/deals/:id/sequence', async (req, res) => {
    try {
      const sb = getSupabase();
      let data = null;
      try {
        const r = await sb.from('deal_sequence').select('*').eq('deal_id', req.params.id).limit(1).single();
        data = r.data;
      } catch (_) {}
      res.json(data || { steps: null });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PUT /api/deals/:id/sequence
  app.put('/api/deals/:id/sequence', async (req, res) => {
    try {
      const sb = getSupabase();
      const { steps, sending_window } = req.body;
      const renumbered = (steps || []).map((s, i) => ({ ...s, step: i + 1 }));
      let existing = null;
      try {
        const r = await sb.from('deal_sequence').select('id').eq('deal_id', req.params.id).limit(1).single();
        existing = r.data;
      } catch (_) {}
      const payload = { steps: renumbered, updated_at: new Date().toISOString(),
        ...(sending_window ? { sending_window } : {}) };
      if (existing?.id) {
        await sb.from('deal_sequence').update(payload).eq('id', existing.id);
      } else {
        await sb.from('deal_sequence').insert({ deal_id: req.params.id, ...payload });
      }
      pushActivity({ type: 'system', action: 'Deal sequence updated', note: '' });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/archive — firms that have been exhausted for this deal
  app.get('/api/deals/:id/archive', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data } = await sb.from('deal_archive')
        .select('*')
        .eq('deal_id', req.params.id)
        .order('archived_at', { ascending: false });
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── TELEGRAM WEBHOOK (inline button callbacks) ────────────────────────────

  app.post('/api/telegram/webhook', async (req, res) => {
    try {
      const { processTelegramUpdate, getTelegramTransport } = await import('../approval/telegramBot.js');
      if (getTelegramTransport() === 'webhook') {
        await processTelegramUpdate(req.body);
        return res.json({ ok: true });
      }

      const cb = req.body?.callback_query;
      if (!cb) return res.json({ ok: true });

      const [action, approvalId] = (cb.data || '').split(':');
      const sb = getSupabase();
      if (!sb || !approvalId) return res.json({ ok: true });

      const { data: item } = await sb.from('approval_queue')
        .select('*').eq('id', approvalId).maybeSingle();

      if (!item) return res.json({ ok: true });
      const meta = (() => { try { return JSON.parse(item.metadata || '{}'); } catch { return {}; } })();

      const { editTelegramMessage, answerCallbackQuery } = await import('../core/telegram.js');
      const chatId    = cb.message?.chat?.id;
      const messageId = cb.message?.message_id;

      if (action === 'proceed') {
        await sb.from('approval_queue').update({ status: 'prior_chat_proceed', resolved_at: new Date().toISOString() }).eq('id', approvalId);
        pushActivity({ type: 'linkedin', action: `Prior chat: proceeding with ${meta.contact_name || 'contact'}`, note: meta.firm_name || '', deal_id: item.deal_id });
        await editTelegramMessage(chatId, messageId, `✅ *Proceeding with ${meta.contact_name || 'contact'}*\nDM queued for your approval.`);
      } else if (action === 'find_other') {
        await sb.from('approval_queue').update({ status: 'prior_chat_skip', resolved_at: new Date().toISOString() }).eq('id', approvalId);
        pushActivity({ type: 'linkedin', action: `Prior chat: skipping ${meta.contact_name || 'contact'}, finding next`, note: meta.firm_name || '', deal_id: item.deal_id });
        await editTelegramMessage(chatId, messageId, `↩️ *Skipping ${meta.contact_name || 'contact'}*\nMoving to next contact at ${meta.firm_name || 'firm'}.`);
      } else if (action === 'suppress') {
        if (meta.firm_name) {
          await sb.from('contacts').update({ pipeline_stage: 'Archived' }).ilike('company_name', `%${meta.firm_name}%`);
        }
        await sb.from('approval_queue').update({ status: 'prior_chat_suppress', resolved_at: new Date().toISOString() }).eq('id', approvalId);
        pushActivity({ type: 'system', action: `Firm suppressed via Telegram: ${meta.firm_name || 'firm'}`, deal_id: item.deal_id });
        await editTelegramMessage(chatId, messageId, `🚫 *${meta.firm_name || 'Firm'} suppressed*\nAll outreach paused.`);
      }

      await answerCallbackQuery(cb.id, 'Done');
      res.json({ ok: true });
    } catch (err) {
      console.error('[TG WEBHOOK]', err.message);
      res.json({ ok: true });
    }
  });

  // ─── ENV VARS ─────────────────────────────────────────────────────────────

  const ENV_PATH = path.join(__dirname, '../.env');

  // GET /api/env — list all .env vars with masked values
  app.get('/api/env', (req, res) => {
    try {
      const content = fs.existsSync(ENV_PATH) ? fs.readFileSync(ENV_PATH, 'utf8') : '';
      const vars = [];
      for (const line of content.split('\n')) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        const eqIdx = trimmed.indexOf('=');
        if (eqIdx === -1) continue;
        const key   = trimmed.slice(0, eqIdx).trim();
        const value = trimmed.slice(eqIdx + 1).trim();
        const masked = value.length === 0 ? '(empty)'
          : value.length <= 8 ? '•'.repeat(value.length)
          : value.slice(0, 6) + '•'.repeat(Math.min(value.length - 6, 24));
        vars.push({ key, masked, set: value.length > 0 });
      }
      res.json(vars);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/env — update or add a single .env variable
  app.post('/api/env', (req, res) => {
    try {
      const { key, value } = req.body;
      if (!key || !/^[A-Z0-9_]+$/.test(key)) return res.status(400).json({ error: 'Invalid key' });
      if (value === undefined || value === null) return res.status(400).json({ error: 'value required' });

      const content = fs.existsSync(ENV_PATH) ? fs.readFileSync(ENV_PATH, 'utf8') : '';
      const lines = content.split('\n');
      let found = false;
      const newLines = lines.map(line => {
        const t = line.trim();
        if (!t || t.startsWith('#') || !t.includes('=')) return line;
        const eqIdx = t.indexOf('=');
        if (t.slice(0, eqIdx).trim() === key) {
          found = true;
          // Quote values containing special chars that dotenv would misparse (#, spaces, quotes)
          const needsQuotes = /[#\s"'\\]/.test(value);
          const safe = needsQuotes ? `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"` : value;
          return `${key}=${safe}`;
        }
        return line;
      });
      if (!found) {
        const needsQuotes = /[#\s"'\\]/.test(value);
        const safe = needsQuotes ? `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"` : value;
        newLines.push(`${key}=${safe}`);
      }
      fs.writeFileSync(ENV_PATH, newLines.join('\n'));
      // Apply immediately to running process
      process.env[key] = value;
      info(`[ENV] Updated: ${key}`);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── SEQUENCE ─────────────────────────────────────────────────────────────

  // GET /api/sequence
  app.get('/api/sequence', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.json({ steps: [] });
      const { data } = await sb.from('outreach_sequence').select('*').limit(1).single();
      res.json(data || { steps: [] });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PUT /api/sequence
  app.put('/api/sequence', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { steps, sending_window } = req.body;
      if (!Array.isArray(steps)) return res.status(400).json({ error: 'steps must be array' });

      // Renumber sequentially regardless of what was sent
      const renumbered = steps.map((s, i) => ({ ...s, step: i + 1 }));

      // Check for duplicate labels (not step numbers)
      const labels = renumbered.map(s => s.label).filter(Boolean);
      if (new Set(labels).size !== labels.length) {
        return res.status(400).json({ error: 'Each step label must be unique' });
      }

      let existing = null;
      try {
        const { data } = await sb.from('outreach_sequence').select('id').limit(1).single();
        existing = data;
      } catch { /* no row yet */ }

      const payload = {
        steps:      renumbered,
        updated_at: new Date().toISOString(),
        updated_by: req.session?.displayName || 'Dom',
        ...(sending_window ? { sending_window } : {}),
      };

      let result;
      if (existing?.id) {
        const { data } = await sb.from('outreach_sequence').update(payload).eq('id', existing.id).select().single();
        result = data;
      } else {
        const { data } = await sb.from('outreach_sequence').insert(payload).select().single();
        result = data;
      }
      pushActivity({ type: 'system', action: 'Outreach sequence updated', note: `${renumbered.length} steps` });
      res.json({ success: true, sequence: result });
    } catch (err) {
      console.error('[SEQUENCE]', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // ─── TEMPLATES ────────────────────────────────────────────────────────────

  // GET /api/templates — supports ?sequence_step= filter
  app.get('/api/templates', async (req, res) => {
    try {
      const { sequence_step } = req.query;
      if (sequence_step) {
        const sb = getSupabase();
        if (!sb) return res.json([]);
        const { data } = await sb.from('email_templates').select('*').eq('sequence_step', sequence_step).eq('is_active', true);
        return res.json(data || []);
      }
      const templates = await getTemplates();
      res.json(templates);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/templates/:id
  app.get('/api/templates/:id', async (req, res) => {
    try {
      const template = await getTemplate(req.params.id);
      if (!template) return res.status(404).json({ error: 'Template not found' });
      res.json(template);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/templates — create a new template
  app.post('/api/templates', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });

      const { name, type, subject_a, subject_b, body, is_active, ab_test_enabled, sequence_step, is_primary } = req.body;
      if (!name?.trim() || !body?.trim()) return res.status(400).json({ error: 'name and body required' });

      const { data, error } = await sb.from('email_templates').insert({
        name:            name.trim(),
        type:            type || 'email',
        subject_a:       subject_a || null,
        subject_b:       subject_b || null,
        body:            body.trim(),
        is_active:       is_active !== false,
        ab_test_enabled: !!ab_test_enabled,
        sequence_step:   sequence_step || null,
        is_primary:      !!is_primary,
        updated_at:      new Date().toISOString(),
      }).select().single();

      if (error) throw error;

      pushActivity({
        type: 'system',
        action: `Template created: "${name.trim()}"`,
        note: type || 'email',
      });

      res.json({ success: true, template: data });
    } catch (err) {
      console.error('[TEMPLATES POST]', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // PATCH /api/templates/:id — update template, sync to local file
  app.patch('/api/templates/:id', async (req, res) => {
    try {
      const { subject_a, subject_b, body, notes, is_active, ab_test_enabled } = req.body;
      const updates = {};
      if (subject_a !== undefined) updates.subject_a = subject_a;
      if (subject_b !== undefined) updates.subject_b = subject_b;
      if (body !== undefined) updates.body = body;
      if (notes !== undefined) updates.notes = notes;
      if (is_active !== undefined) updates.is_active = is_active;
      if (ab_test_enabled !== undefined) updates.ab_test_enabled = ab_test_enabled;
      const template = await updateTemplate(req.params.id, updates);
      pushActivity({
        type: 'system',
        action: `Template updated: ${template?.name || req.params.id}`,
        note: template?.type || '',
      });
      res.json({ success: true, template });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PATCH /api/templates/:id/primary — set as primary for its sequence_step
  app.patch('/api/templates/:id/primary', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: tmpl } = await sb.from('email_templates').select('*').eq('id', req.params.id).single();
      if (!tmpl) return res.status(404).json({ error: 'Not found' });
      if (!req.body.keep_ab) {
        await sb.from('email_templates').update({ is_primary: false }).eq('sequence_step', tmpl.sequence_step).neq('id', req.params.id);
      }
      await sb.from('email_templates').update({ is_primary: true }).eq('id', req.params.id);
      pushActivity({ type: 'system', action: `Primary template set: "${tmpl.name}"`, note: tmpl.sequence_step || '' });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PATCH /api/templates/:id/ab — set A/B pair
  app.patch('/api/templates/:id/ab', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { ab_pair_id, ab_test_enabled } = req.body;
      await sb.from('email_templates').update({ ab_pair_id: ab_pair_id || null, ab_test_enabled: !!ab_test_enabled }).eq('id', req.params.id);
      // If pairing, update partner too
      if (ab_pair_id) {
        await sb.from('email_templates').update({ ab_pair_id: req.params.id, ab_test_enabled: true }).eq('id', ab_pair_id);
      }
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PATCH /api/templates/:id/notes — save notes
  app.patch('/api/templates/:id/notes', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { notes } = req.body;
      await sb.from('email_templates').update({ notes, updated_at: new Date().toISOString() }).eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/templates/:id
  app.delete('/api/templates/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: tmpl } = await sb.from('email_templates').select('name, is_primary').eq('id', req.params.id).single();
      if (!tmpl) return res.status(404).json({ error: 'Not found' });
      if (tmpl.is_primary) return res.status(400).json({ error: 'Cannot delete a primary template. Set another template as primary first.' });
      await sb.from('email_templates').delete().eq('id', req.params.id);
      pushActivity({ type: 'system', action: `Template deleted: "${tmpl.name}"`, note: '' });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/templates/:id/reset — reset to system default
  app.post('/api/templates/:id/reset', async (req, res) => {
    try {
      await seedDefaultTemplates(); // re-seeds if needed
      const template = await getTemplate(req.params.id);
      res.json({ success: true, template });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/templates/preview — render template with sample data
  app.post('/api/templates/preview', (req, res) => {
    const { body, subject_a, subject_b } = req.body;
    const sample = {
      firstName: 'James', lastName: 'Richardson', fullName: 'James Richardson',
      firm: 'Blackstone Real Estate', company: 'Blackstone Real Estate',
      title: 'Managing Director', jobTitle: 'Managing Director',
      dealName: 'Meridian Industrial Portfolio',
      dealBrief: 'a £12m industrial portfolio in the East Midlands, yielding 7.8%',
      sector: 'Industrial Real Estate', geography: 'UK',
      targetAmount: '£12m', keyMetrics: '7.8% yield, 95% occupancy, 8-year WAULT',
      minCheque: '£500k', maxCheque: '£5M',
      investorProfile: 'UK-focused real estate investors seeking 6%+ yield',
      comparableDeal: 'their 2023 UK logistics park acquisition',
      deckUrl: 'https://docsend.com/view/example',
      callLink: 'https://calendly.com/dom/30min',
      senderName: 'Dom', senderTitle: 'Partner',
    };
    const render = (text) => {
      if (!text) return '';
      return text.replace(/\{\{(\w+)\}\}/g, (_, key) => sample[key] || `{{${key}}}`);
    };
    res.json({
      subjectA: render(subject_a),
      subjectB: render(subject_b),
      body: render(body),
      sampleData: sample,
    });
  });

  // ─── ACTIVITY (SUPABASE) ──────────────────────────────────────────────────

  // GET /api/activity/log — persistent activity from Supabase
  app.get('/api/activity/log', async (req, res) => {
    try {
      const { dealId, limit = 200, offset = 0, eventType } = req.query;
      const logs = await getActivityLog({
        dealId: dealId || null,
        limit: Number(limit),
        offset: Number(offset),
        eventType: eventType || null,
      });
      res.json(logs);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── GLOBAL SCHEDULE CONTROLS ─────────────────────────────────────────────

  // POST /api/pause-outreach — global outreach pause until datetime
  app.post('/api/pause-outreach', async (req, res) => {
    const { until } = req.body;
    const state = readState();
    state.outreach_paused_until = until || null;  // key the orchestrator reads
    state.outreachPausedUntil   = until || null;  // key the dashboard status reads
    writeState(state);
    await saveSessionState(state).catch(() => {});
    broadcastToAll({ type: 'STATE_UPDATE', state });
    pushActivity({
      type: 'SYSTEM',
      action: until ? 'Outreach Paused' : 'Outreach Resumed',
      note: until ? `Until ${new Date(until).toLocaleString('en-GB')}` : 'Pause lifted',
    });
    res.json({ success: true, outreachPausedUntil: until });
  });

  // ─── COMPANY SOURCING ROUTES ──────────────────────────────────────────────

  // GET /api/sourcing/campaigns — list all sourcing campaigns
  app.get('/api/sourcing/campaigns', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('sourcing_campaigns')
        .select('*')
        .order('created_at', { ascending: false });
      if (dbErr) throw new Error(dbErr.message);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns — create new sourcing campaign
  app.post('/api/sourcing/campaigns', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });

      const body = req.body;
      if (!body.name)          return res.status(400).json({ error: 'Campaign name is required' });
      if (!body.target_sector) return res.status(400).json({ error: 'Target sector is required' });
      if (!body.target_geography) return res.status(400).json({ error: 'Target geography is required' });

      // Handle geography as array or string
      let geography = body.target_geography;
      if (Array.isArray(geography)) geography = geography.join(', ');

      const campaign = {
        name:               body.name,
        firm_name:          body.firm_name || null,
        firm_type:          body.firm_type || null,
        investment_thesis:  body.investment_thesis || null,
        target_sector:      body.target_sector,
        target_geography:   geography,
        company_stage:      body.company_stage || null,
        min_revenue:        body.min_revenue || null,
        max_revenue:        body.max_revenue || null,
        min_ebitda:         body.min_ebitda || null,
        max_ebitda:         body.max_ebitda || null,
        min_company_age_months: parseInt(body.min_company_age_months) || null,
        max_company_age_months: parseInt(body.max_company_age_months) || null,
        business_model:     body.business_model || null,
        ownership_type:     body.ownership_type || null,
        headcount_min:      parseInt(body.headcount_min) || null,
        headcount_max:      parseInt(body.headcount_max) || null,
        deal_type:          body.deal_type || null,
        investment_size:    body.investment_size || null,
        intent_signals:     body.intent_signals || null,
        keywords_include:   body.keywords_include || null,
        keywords_exclude:   body.keywords_exclude || null,
        timezone:           body.timezone || 'America/New_York',
        batch_size:         parseInt(body.batch_size) || 5,
        max_companies_per_campaign: parseInt(body.max_companies_per_campaign) || 200,
        linkedin_connection_window: body.linkedin_connection_window || { start: '09:00', end: '18:00' },
        linkedin_dm_window: body.linkedin_dm_window || { start: '20:00', end: '23:00' },
        email_send_window:  body.email_send_window || { start: '09:00', end: '18:00' },
        status:             'active',
      };

      const { data: saved, error: dbErr } = await sb.from('sourcing_campaigns')
        .insert(campaign).select().single();
      if (dbErr) throw new Error(dbErr.message);

      pushActivity({ type: 'system', action: 'Sourcing campaign launched', note: saved.name });
      await sbLogActivity({ eventType: 'CAMPAIGN_CREATED', summary: `Sourcing campaign "${saved.name}" created` }).catch(() => {});
      await sendTelegram(`New sourcing campaign launched: ${saved.name}\nFirm: ${saved.firm_name || '—'}\nSector: ${saved.target_sector}\nGeography: ${saved.target_geography}\n\nResearch starting now.`).catch(() => {});

      broadcastToAll({ type: 'CAMPAIGN_CREATED', campaign: saved });
      res.json({ success: true, campaign: saved });

      // Fire research immediately (non-blocking)
      setImmediate(async () => {
        try {
          const { researchCompaniesForCampaign } = await import('../sourcing/companyResearcher.js');
          await researchCompaniesForCampaign(saved);
        } catch (e) {
          console.error('[CAMPAIGN LAUNCH] Research failed:', e.message);
        }
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/sourcing/campaigns/:id — single campaign with stats
  app.get('/api/sourcing/campaigns/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });

      const { data: campaign, error: dbErr } = await sb.from('sourcing_campaigns')
        .select('*').eq('id', req.params.id).single();
      if (dbErr || !campaign) return res.status(404).json({ error: 'Campaign not found' });

      // Gather stats
      const [
        { count: companiesFound },
        { count: hotLeads },
        { count: meetingsBooked },
        { count: contactsEnriched },
        { count: outreachSent },
      ] = await Promise.all([
        sb.from('target_companies').select('id', { count: 'exact', head: true }).eq('campaign_id', req.params.id),
        sb.from('target_companies').select('id', { count: 'exact', head: true }).eq('campaign_id', req.params.id).in('match_tier', ['hot', 'warm']),
        sb.from('target_companies').select('id', { count: 'exact', head: true }).eq('campaign_id', req.params.id).eq('meeting_booked', true),
        sb.from('company_contacts').select('id', { count: 'exact', head: true }).eq('campaign_id', req.params.id).in('enrichment_status', ['enriched', 'enriched_apify']),
        sb.from('company_contacts').select('id', { count: 'exact', head: true }).eq('campaign_id', req.params.id).in('pipeline_stage', ['contacted', 'meeting_booked', 'exhausted']),
      ]);

      res.json({
        ...campaign,
        stats: {
          companies_found:   companiesFound || 0,
          hot_leads:         hotLeads || 0,
          meetings_booked:   meetingsBooked || 0,
          contacts_enriched: contactsEnriched || 0,
          outreach_sent:     outreachSent || 0,
        },
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PATCH /api/sourcing/campaigns/:id — update campaign settings
  app.patch('/api/sourcing/campaigns/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const allowed = [
        'name', 'firm_name', 'firm_type', 'investment_thesis',
        'target_sector', 'target_geography', 'company_stage', 'business_model',
        'ownership_type', 'deal_type', 'investment_size',
        'min_revenue', 'max_revenue', 'min_ebitda', 'max_ebitda',
        'min_company_age_months', 'max_company_age_months',
        'headcount_min', 'headcount_max',
        'intent_signals', 'keywords_include', 'keywords_exclude',
        'linkedin_connection_window', 'linkedin_dm_window', 'email_send_window',
        'timezone', 'batch_size', 'max_companies_per_campaign', 'status',
      ];
      const updates = { updated_at: new Date().toISOString() };
      for (const key of allowed) {
        if (req.body[key] !== undefined) updates[key] = req.body[key];
      }
      const { data, error: dbErr } = await sb.from('sourcing_campaigns')
        .update(updates).eq('id', req.params.id).select().single();
      if (dbErr) throw new Error(dbErr.message);
      res.json({ success: true, campaign: data });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns/:id/pause
  app.post('/api/sourcing/campaigns/:id/pause', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data } = await sb.from('sourcing_campaigns')
        .update({ status: 'paused', updated_at: new Date().toISOString() })
        .eq('id', req.params.id).select().single();
      pushActivity({ type: 'system', action: 'Campaign paused', note: data?.name || req.params.id });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns/:id/resume
  app.post('/api/sourcing/campaigns/:id/resume', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data } = await sb.from('sourcing_campaigns')
        .update({ status: 'active', updated_at: new Date().toISOString() })
        .eq('id', req.params.id).select().single();
      pushActivity({ type: 'system', action: 'Campaign resumed', note: data?.name || req.params.id });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns/:id/close
  app.post('/api/sourcing/campaigns/:id/close', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data } = await sb.from('sourcing_campaigns')
        .update({ status: 'closed', updated_at: new Date().toISOString() })
        .eq('id', req.params.id).select().single();
      pushActivity({ type: 'system', action: 'Campaign closed', note: data?.name || req.params.id });
      await sendTelegram(`Sourcing campaign closed: ${data?.name || req.params.id}`).catch(() => {});
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns/:id/reopen
  app.post('/api/sourcing/campaigns/:id/reopen', async (req, res) => {
    try {
      const sb = getSupabase();
      await sb.from('sourcing_campaigns')
        .update({ status: 'active', updated_at: new Date().toISOString() })
        .eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/sourcing/campaigns/:id — permanently delete campaign and all related data
  app.delete('/api/sourcing/campaigns/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const id = req.params.id;

      // Delete in dependency order: approval_queue → company_contacts → target_companies → campaign
      await sb.from('approval_queue').delete().eq('campaign_id', id).then(null, () => {});
      await sb.from('company_contacts').delete().eq('campaign_id', id).then(null, () => {});
      await sb.from('target_companies').delete().eq('campaign_id', id).then(null, () => {});
      const { error: delErr } = await sb.from('sourcing_campaigns').delete().eq('id', id);
      if (delErr) throw new Error(delErr.message);

      pushActivity({ type: 'system', action: 'Campaign deleted', note: id });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/sourcing/campaigns/:id/companies — list target companies
  app.get('/api/sourcing/campaigns/:id/companies', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { tier } = req.query;

      let query = sb.from('target_companies')
        .select('*')
        .eq('campaign_id', req.params.id)
        .order('created_at', { ascending: true })
        .limit(200);

      if (tier) query = query.eq('match_tier', tier);

      const { data, error: dbErr } = await query;
      if (dbErr) throw new Error(dbErr.message);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/sourcing/companies/:id — single company with contacts
  app.get('/api/sourcing/companies/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: company, error: dbErr } = await sb.from('target_companies')
        .select('*').eq('id', req.params.id).single();
      if (dbErr || !company) return res.status(404).json({ error: 'Company not found' });
      const { data: contacts } = await sb.from('company_contacts')
        .select('*').eq('company_id', req.params.id).order('created_at');
      res.json({ ...company, contacts: contacts || [] });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/companies/:id/archive
  app.post('/api/sourcing/companies/:id/archive', async (req, res) => {
    try {
      const sb = getSupabase();
      await sb.from('target_companies').update({ match_tier: 'archive', outreach_status: 'declined', updated_at: new Date().toISOString() }).eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/companies/:id/reinstate
  app.post('/api/sourcing/companies/:id/reinstate', async (req, res) => {
    try {
      const sb = getSupabase();
      await sb.from('target_companies').update({ match_tier: 'warm', outreach_status: 'pending', updated_at: new Date().toISOString() }).eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/sourcing/campaigns/:id/contacts — all company contacts for a campaign
  app.get('/api/sourcing/campaigns/:id/contacts', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('company_contacts')
        .select('*, target_companies(company_name, match_tier, match_score)')
        .eq('campaign_id', req.params.id)
        .order('created_at', { ascending: true })
        .limit(500);
      if (dbErr) throw new Error(dbErr.message);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/sourcing/campaigns/:id/meetings — meetings booked
  app.get('/api/sourcing/campaigns/:id/meetings', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('target_companies')
        .select('*, company_contacts(name, title, pipeline_stage)')
        .eq('campaign_id', req.params.id)
        .eq('meeting_booked', true)
        .order('meeting_booked_at', { ascending: false });
      if (dbErr) throw new Error(dbErr.message);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/contacts/:id/skip
  app.post('/api/sourcing/contacts/:id/skip', async (req, res) => {
    try {
      const sb = getSupabase();
      await sb.from('company_contacts').update({ pipeline_stage: 'skipped', updated_at: new Date().toISOString() }).eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns/:id/trigger-research — manually trigger research
  app.post('/api/sourcing/campaigns/:id/trigger-research', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: campaign } = await sb.from('sourcing_campaigns').select('*').eq('id', req.params.id).single();
      if (!campaign) return res.status(404).json({ error: 'Campaign not found' });

      pushActivity({ type: 'research', action: 'Manual research triggered', note: `[${campaign.name}]` });
      res.json({ success: true, message: `Research triggered for ${campaign.name}` });

      setImmediate(async () => {
        try {
          const { researchCompaniesForCampaign } = await import('../sourcing/companyResearcher.js');
          await researchCompaniesForCampaign(campaign);
        } catch (e) {
          console.error('[SOURCING] Manual research failed:', e.message);
        }
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── INVESTOR DATABASE ────────────────────────────────────────────────────

  // Multer with disk storage for large file uploads (xlsx, pdf, docx)
  const fileUpload = multer({
    storage: multer.diskStorage({
      destination: (req, file, cb) => cb(null, '/tmp'),
      filename:    (req, file, cb) => cb(null, `roco-${Date.now()}-${file.originalname}`),
    }),
  }).single('file');

  // GET /api/investors-db/stats
  app.get('/api/investors-db/stats', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const summary = await getInvestorDbSummary(sb);
      res.json({
        total: summary.total || 0,
        by_category: summary.byCategory || {},
        uncategorised: summary.uncategorised || 0,
        generated_at: summary.builtAt,
        cached: true,
      });
    } catch (err) {
      const stale = investorDbSummaryCache.value;
      if (stale) {
        return res.json({
          total: stale.total || 0,
          by_category: stale.byCategory || {},
          uncategorised: stale.uncategorised || 0,
          generated_at: stale.builtAt,
          cached: true,
          stale: true,
        });
      }
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/investors-db/import — upload XLSX and import to investors_db
  app.post('/api/investors-db/import', (req, res, next) => {
    fileUpload(req, res, (err) => {
      if (err) return res.status(400).json({ error: err.message });
      next();
    });
  }, async (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
      pushActivity({ type: 'research', action: `Database import started: ${req.file.originalname}`, note: '' });

      // Create or find the named list record
      const listName = (req.body.list_name || req.file.originalname.replace(/\.[^/.]+$/, '')).trim();
      const listType = req.body.list_type || 'standing';
      const listSource = req.body.list_source || 'pitchbook';
      const listPayload = buildInvestorListPayload(req.body);
      let listId = null;
      const sbList = getSupabase();
      if (sbList) {
        const { data: existingList } = await sbList.from('investor_lists')
          .select('id').eq('name', listName).maybeSingle();
        if (existingList) {
          listId = existingList.id;
        } else {
          const { data: newList } = await sbList.from('investor_lists').insert({
            name: listName, list_type: listType, source: listSource, ...listPayload,
          }).select().single();
          listId = newList?.id;
        }
      }

      const { importXLSXToDatabase } = await import('../core/investorDatabaseImporter.js');
      const result = await importXLSXToDatabase({
        filePath: req.file.path,
        filename: req.file.originalname,
        listId,
        listName,
        listType,
        broadcastFn: (msg) => pushActivity({ type: 'research', action: msg, note: '' }),
      });
      if (sbList && listId) {
        const { count: listCount } = await sbList.from('investors_db')
          .select('id', { count: 'exact', head: true })
          .eq('list_id', listId);
        await sbList.from('investor_lists').update({
          investor_count: listCount || 0,
          updated_at: new Date().toISOString(),
        }).eq('id', listId);
      }
      fs.unlinkSync(req.file.path);
      invalidateInvestorDbSummaryCache();
      res.json({ success: true, list_id: listId, list_name: listName, ...result });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  const updateInvestorListHandler = async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: existing, error: existingError } = await sb.from('investor_lists')
        .select('*')
        .eq('id', req.params.id)
        .single();
      if (existingError || !existing) return res.status(404).json({ error: 'List not found' });
      const updates = buildInvestorListPayload(req.body, { requireName: false });
      if (req.body.name != null && !updates.name) return res.status(400).json({ error: 'name is required' });
      updates.updated_at = new Date().toISOString();
      const { data, error } = await sb.from('investor_lists').update(updates).eq('id', req.params.id).select().single();
      if (error) return res.status(500).json({ error: error.message });
      // Keep investors_db rows in sync
      if (updates.name && updates.name !== existing.name) {
        await sb.from('investors_db').update({ list_name: updates.name }).eq('list_id', req.params.id);
      }
      pushActivity({
        type: 'system',
        action: `List updated: "${data?.name || existing.name}"`,
        note: `${data?.investor_count || ''} investors updated`,
      });
      res.json({ success: true, list: data });
    } catch (err) { res.status(500).json({ error: err.message }); }
  };
  app.put('/api/investor-lists/:id', updateInvestorListHandler);
  app.put('/api/lists/:id', updateInvestorListHandler);

  const getInvestorListsHandler = async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      let query = sb.from('investor_lists').select('*').order('created_at', { ascending: false });
      if (req.query.type === 'knowledge_base') {
        query = query.eq('list_type', 'knowledge_base');
      } else if (req.query.type === 'investors') {
        query = query.neq('list_type', 'knowledge_base');
      }
      const { data: lists, error: listsError } = await query;
      if (listsError) throw new Error(listsError.message);
      const summary = await getInvestorDbSummary(sb);
      const listsWithCounts = (lists || []).map(list => ({
        ...list,
        investor_count: summary.investorCountByListId?.[String(list.id)] ?? Number(list.investor_count || 0),
      }));
      res.json(listsWithCounts);
    } catch (err) { res.status(500).json({ error: err.message }); }
  };
  app.get('/api/investor-lists', getInvestorListsHandler);
  app.get('/api/lists', getInvestorListsHandler);

  // GET /api/investors-db/search
  app.get('/api/investors-db/search', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { search, type, country, enrichment, contact_type, page = 1, limit = 50 } = req.query;
      const offset = (Number(page) - 1) * Number(limit);

      let query = sb.from('investors_db').select(
        'id, name, hq_country, hq_location, investor_type, contact_type, is_angel, preferred_industries, ' +
        'aum_millions, preferred_deal_size_min, preferred_deal_size_max, preferred_ebitda_min, preferred_ebitda_max, ' +
        'last_investment_date, last_investment_company, investments_last_12m, email, primary_contact_email, ' +
        'investor_category, enrichment_status, description',
        { count: 'planned' }
      );
      if (search) query = query.or(`name.ilike.%${search}%,description.ilike.%${search}%,preferred_industries.ilike.%${search}%,hq_city.ilike.%${search}%`);
      // Type filter: check both investor_category (e.g. "IndependentSponsor") and investor_type (e.g. "Family Office")
      if (type) query = query.or(`investor_category.ilike.%${type}%,investor_type.ilike.%${type}%`);
      if (country) query = query.ilike('hq_country', `%${country}%`);
      if (enrichment) query = query.ilike('enrichment_status', `%${enrichment}%`);
      if (contact_type === 'angel') query = query.eq('is_angel', true);
      else if (contact_type) query = query.eq('contact_type', contact_type);

      query = query
        .order('investments_last_12m', { ascending: false, nullsFirst: false })
        .range(offset, offset + Number(limit) - 1);

      const { data, error: dbErr, count } = await query;
      if (dbErr) throw new Error(dbErr.message);

      const investors = data || [];
      const contactLinks = new Map();
      const emailsToMatch = investors
        .map(row => String(row.email || row.primary_contact_email || '').trim())
        .filter(Boolean);
      if (emailsToMatch.length) {
        const { data: linkedContacts } = await sb.from('contacts')
          .select('id, email')
          .in('email', emailsToMatch)
          .then(result => result)
          .catch(() => ({ data: [] }));
        for (const row of linkedContacts || []) {
          if (row.email) contactLinks.set(String(row.email).trim().toLowerCase(), row.id);
        }
      }

      res.json({
        investors: investors.map(row => ({
          ...row,
          linked_contact_id: contactLinks.get(String(row.email || row.primary_contact_email || '').trim().toLowerCase()) || null,
        })),
        total: count || 0,
        page: Number(page),
        limit: Number(limit),
        pages: Math.ceil((count || 0) / Number(limit)),
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/contacts-db/search — enriched individual contacts
  app.get('/api/contacts-db/search', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { search, firm, page = 1, limit = 50 } = req.query;
      const offset = (Number(page) - 1) * Number(limit);

      let query = sb.from('enriched_contacts').select('*', { count: 'exact' });
      if (search) query = query.or(`name.ilike.%${search}%,email.ilike.%${search}%,firm_name.ilike.%${search}%`);
      if (firm) query = query.eq('firm_name', firm);

      query = query.order('updated_at', { ascending: false })
        .range(offset, offset + Number(limit) - 1);

      const { data, error: dbErr, count } = await query;
      if (dbErr) throw new Error(dbErr.message);
      res.json({
        contacts: data || [],
        total: count || 0,
        page: Number(page),
        pages: Math.ceil((count || 0) / Number(limit)),
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/contacts-db/researched — deal-linked contacts with relationship context
  app.get('/api/contacts-db/researched', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { search, page = 1, limit = 50 } = req.query;
      const offset = (Number(page) - 1) * Number(limit);

      let query = sb.from('contacts')
        .select(
          'id, name, company_name, job_title, email, phone, linkedin_url, ' +
          'sector_focus, geography, typical_cheque_size, aum_fund_size, ' +
          'past_investments, notes, source, enrichment_status, pipeline_stage, ' +
          'person_researched, investor_score, created_at, updated_at, deal_id, conversation_state, last_intent, last_intent_label',
          { count: 'exact' }
        )
        .not('pipeline_stage', 'in', `("Deleted — Do Not Contact","Suppressed — Opt Out")`);

      if (search) query = query.or(
        `name.ilike.%${search}%,company_name.ilike.%${search}%,email.ilike.%${search}%`
      );

      query = query.order('created_at', { ascending: false })
        .range(offset, offset + Number(limit) - 1);

      const { data, error: dbErr, count } = await query;
      if (dbErr) throw new Error(dbErr.message);
      const dealNameMap = await getDealNameMap(sb, (data || []).map(row => row.deal_id));
      const contactDealContext = await buildContactDealContextMap(sb);
      res.json({
        contacts: (data || []).map(row => {
          const dealContext = contactDealContext.get(row.id) || {};
          return {
            ...row,
            dealName: dealNameMap[row.deal_id] || '',
            projectName: dealContext.projectName || dealNameMap[row.deal_id] || '',
            activeDealName: dealContext.activeDealName || '',
            activeDealId: dealContext.activeDealId || null,
            deals: dealContext.deals || [],
            dealNamesText: dealContext.dealNamesText || '',
          };
        }),
        total: count || 0,
        page: Number(page),
        pages: Math.ceil((count || 0) / Number(limit)),
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/investors-db/:id/deals — deal history for one investor
  app.get('/api/investors-db/:id/deals', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('investor_deal_history')
        .select('*')
        .eq('investors_db_id', req.params.id)
        .order('added_at', { ascending: false });
      if (dbErr) throw new Error(dbErr.message);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/investors-db/:id/profile', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });

      const { data: investor, error: investorErr } = await sb.from('investors_db')
        .select('*')
        .eq('id', req.params.id)
        .single();
      if (investorErr) throw new Error(investorErr.message);

      const emailCandidates = [...new Set([
        String(investor.email || '').trim().toLowerCase(),
        String(investor.primary_contact_email || '').trim().toLowerCase(),
      ].filter(Boolean))];

      let linkedContacts = [];
      if (emailCandidates.length) {
        const { data } = await sb.from('contacts')
          .select('*, deals(*)')
          .or(emailCandidates.map(email => `email.ilike.${email}`).join(','))
          .limit(20)
          .then(result => result)
          .catch(() => ({ data: [] }));
        linkedContacts = data || [];
      }

      if (!linkedContacts.length && investor.name) {
        const { data } = await sb.from('contacts')
          .select('*, deals(*)')
          .ilike('name', investor.name)
          .limit(10)
          .then(result => result)
          .catch(() => ({ data: [] }));
        linkedContacts = (data || []).filter(row => {
          if (!investor.name) return false;
          const sameFirm = investor.name && investor.name && (
            !investor.firm_name ||
            String(row.company_name || '').toLowerCase().includes(String(investor.firm_name || '').toLowerCase()) ||
            String(investor.firm_name || '').toLowerCase().includes(String(row.company_name || '').toLowerCase())
          );
          return sameFirm;
        });
      }

      const transcriptLookupClauses = [
        emailCandidates.map(email => `investor_email.ilike.${email}`).join(','),
        investor.name ? `investor_name.ilike.${investor.name}` : '',
      ].filter(Boolean).join(',');
      if (transcriptLookupClauses) {
        const { data } = await sb.from('meeting_transcripts')
          .select('contact_id')
          .or(transcriptLookupClauses)
          .not('contact_id', 'is', null)
          .limit(100)
          .then(result => result)
          .catch(() => ({ data: [] }));
        const transcriptContactIds = [...new Set((data || []).map(row => row.contact_id).filter(Boolean))];
        if (transcriptContactIds.length) {
          const { data: transcriptContacts } = await sb.from('contacts')
            .select('*, deals(*)')
            .in('id', transcriptContactIds)
            .limit(20)
            .then(result => result)
            .catch(() => ({ data: [] }));
          linkedContacts = [...linkedContacts, ...(transcriptContacts || [])];
        }
      }

      const uniqueLinkedContacts = [];
      const seenLinkedContactIds = new Set();
      for (const row of linkedContacts) {
        if (!row?.id || seenLinkedContactIds.has(row.id)) continue;
        seenLinkedContactIds.add(row.id);
        uniqueLinkedContacts.push(row);
      }

      const primaryContact = uniqueLinkedContacts[0] || null;
      const linkedContactIds = uniqueLinkedContacts.map(row => row.id).filter(Boolean);

      const [dealHistoryRes, messagesRes, emailsRes, transcriptsRes] = await Promise.all([
        sb.from('investor_deal_history')
          .select('*')
          .eq('investors_db_id', req.params.id)
          .order('added_at', { ascending: false })
          .then(result => result)
          .catch(() => ({ data: [] })),
        linkedContactIds.length
          ? sb.from('conversation_messages')
              .select('contact_id, deal_id, direction, channel, body, subject, sent_at, received_at, intent')
              .in('contact_id', linkedContactIds)
              .limit(250)
              .then(result => result)
              .catch(() => ({ data: [] }))
          : Promise.resolve({ data: [] }),
        linkedContactIds.length
          ? sb.from('emails')
              .select('contact_id, deal_id, subject, body, content, sent_at, created_at')
              .in('contact_id', linkedContactIds)
              .limit(250)
              .then(result => result)
              .catch(() => ({ data: [] }))
          : Promise.resolve({ data: [] }),
        sb.from('meeting_transcripts')
          .select('*')
          .or([
            emailCandidates.map(email => `investor_email.ilike.${email}`).join(','),
            investor.name ? `investor_name.ilike.${investor.name}` : '',
            linkedContactIds.length ? `contact_id.in.(${linkedContactIds.join(',')})` : '',
          ].filter(Boolean).join(','))
          .limit(100)
          .then(result => result)
          .catch(() => ({ data: [] })),
      ]);

      const dealHistory = dealHistoryRes.data || [];
      const dealMap = await getDealNameMap(sb, [
        ...dealHistory.map(row => row.deal_id),
        ...uniqueLinkedContacts.map(row => row.deal_id),
        ...((messagesRes.data || []).map(row => row.deal_id)),
        ...((emailsRes.data || []).map(row => row.deal_id)),
        ...((transcriptsRes.data || []).map(row => row.deal_id)),
      ]);

      const transcripts = transcriptsRes.data || [];
      const history = [];

      for (const item of dealHistory) {
        history.push({
          type: 'Deal',
          date: item.added_at || null,
          summary: `${dealMap[String(item.deal_id || '')] || 'Unknown deal'} · ${item.status || 'added to history'}`,
        });
      }
      for (const msg of messagesRes.data || []) {
        history.push({
          type: String(msg.channel || 'email').includes('linkedin') ? 'LinkedIn' : 'Email',
          date: msg.sent_at || msg.received_at || null,
          summary: String(msg.subject || msg.body || '').replace(/\s+/g, ' ').trim().slice(0, 220),
        });
      }
      for (const email of emailsRes.data || []) {
        history.push({
          type: 'Email',
          date: email.sent_at || email.created_at || null,
          summary: String(email.subject || email.body || email.content || '').replace(/\s+/g, ' ').trim().slice(0, 220),
        });
      }
      for (const transcript of transcripts) {
        history.push({
          type: 'Meeting',
          date: transcript.created_at || null,
          summary: String(transcript.summary || transcript.transcript_text || '').replace(/\s+/g, ' ').trim().slice(0, 220),
          sentiment: transcript.sentiment_score || null,
        });
      }

      history.sort((a, b) => new Date(a.date || 0) - new Date(b.date || 0));

      res.json({
        contact: {
          id: investor.id,
          name: investor.name || primaryContact?.name || 'Unknown investor',
          linked_contact_id: primaryContact?.id || null,
          is_database_record: true,
          company_name: investor.firm_name || primaryContact?.company_name || null,
          job_title: primaryContact?.job_title || investor.role_title || null,
          linkedin_url: primaryContact?.linkedin_url || investor.linkedin_url || null,
          email: investor.email || investor.primary_contact_email || primaryContact?.email || null,
          phone: primaryContact?.phone || null,
          pipeline_stage: primaryContact?.pipeline_stage || 'Database Record',
          conversation_state: primaryContact?.conversation_state || null,
          transcript_sentiment: transcripts.length ? transcripts[transcripts.length - 1].sentiment_score : null,
          sectors_of_interest: asJsonArray(investor.preferred_industries),
          cheque_size_range: [
            investor.preferred_deal_size_min != null ? `$${investor.preferred_deal_size_min}M` : null,
            investor.preferred_deal_size_max != null ? `$${investor.preferred_deal_size_max}M` : null,
          ].filter(Boolean).join(' - ') || (primaryContact?.typical_cheque_size || null),
          aum_display: investor.aum_millions ? `$${Number(investor.aum_millions).toLocaleString()}M` : (primaryContact?.aum || primaryContact?.aum_fund_size || null),
          past_investments_list: asJsonArray(primaryContact?.past_investments || investor.last_investment_company),
          investment_thesis: primaryContact?.investment_thesis || investor.description || null,
          notes: primaryContact?.notes || investor.description || null,
          source: 'Database',
          created_at: investor.created_at || primaryContact?.created_at || null,
          deal_name: dealHistory.length ? (dealMap[String(dealHistory[0].deal_id || '')] || null) : null,
        },
        history,
        transcripts,
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ─── DEAL DOCUMENT PARSING ────────────────────────────────────────────────

  // POST /api/deals/parse-document — upload PDF/DOCX, parse with Kimi, return structured info
  app.post('/api/deals/parse-document', (req, res, next) => {
    fileUpload(req, res, (err) => {
      if (err) return res.status(400).json({ error: err.message });
      next();
    });
  }, async (req, res) => {
    let clientDisconnected = false;
    req.on('close', () => {
      if (!res.writableEnded) {
        clientDisconnected = true;
        pushActivity({ type: 'research', action: 'Document upload cancelled', note: '' });
        if (req.file?.path) try { fs.unlinkSync(req.file.path); } catch {}
      }
    });

    try {
      if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
      pushActivity({ type: 'research', action: `Analysing document: ${req.file.originalname}`, note: '' });
      const { parseDealDocument } = await import('../core/dealDocumentParser.js');
      const { parsed, extractedText } = await parseDealDocument(
        req.file.path,
        req.file.originalname,
        (msg) => { if (!clientDisconnected) pushActivity({ type: 'research', action: msg, note: '' }); }
      );

      if (clientDisconnected) return;

      const sb = getSupabase();
      let documentId = null;
      if (sb) {
        const { data: docRecord } = await sb.from('deal_documents').insert({
          filename:         req.file.originalname,
          file_type:        req.file.originalname.split('.').pop(),
          extracted_text:   extractedText.substring(0, 50000),
          parsed_deal_info: parsed,
        }).select().single();
        documentId = docRecord?.id || null;
      }

      try { fs.unlinkSync(req.file.path); } catch {}
      res.json({ success: true, parsed, document_id: documentId });
    } catch (err) {
      if (!clientDisconnected) res.status(500).json({ error: err.message });
    }
  });

  // ─── TRAIN YOUR AGENT — GUIDANCE ENDPOINTS ───────────────────────────────

  app.get('/api/guidance/investor', async (req, res) => {
    try {
      const guidance = await getInvestorGuidance();
      res.json({ success: true, data: guidance });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
  });

  app.get('/api/guidance/sourcing', async (req, res) => {
    try {
      const guidance = await getSourcingGuidance();
      res.json({ success: true, data: guidance });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
  });

  app.post('/api/guidance/investor', async (req, res) => {
    try {
      const saved = await saveInvestorGuidance(req.body);
      invalidateAgentContext();
      pushActivity('Agent configuration updated', 'system');
      res.json({ success: true, data: saved, message: 'Investor guidance saved. Active on next cycle.' });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
  });

  app.post('/api/guidance/sourcing', async (req, res) => {
    try {
      const saved = await saveSourcingGuidance(req.body);
      invalidateAgentContext();
      pushActivity('Agent sourcing configuration updated', 'system');
      res.json({ success: true, data: saved, message: 'Sourcing guidance saved. Active on next cycle.' });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
  });

  // DELETE /api/wipe-everything — total reset: wipe ALL data, Roco starts fresh
  app.delete('/api/wipe-everything', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });

      const tables = [
        'investors_db', 'contacts', 'deals', 'approval_queue', 'activity_log',
        'enriched_contacts', 'firm_outreach_state', 'deal_documents', 'deal_assets',
        'batches', 'firms', 'emails', 'replies', 'linkedin_messages',
      ];

      const results = {};
      for (const table of tables) {
        const { error: e } = await sb.from(table).delete().neq('id', '00000000-0000-0000-0000-000000000000');
        results[table] = e ? `error: ${e.message}` : 'wiped';
      }

      // Reset state.json
      const state = readState();
      state.activeDeals = [];
      state.rocoStatus = 'ACTIVE';
      writeState(state);

      pushActivity({ type: 'system', action: 'FULL RESET — All data wiped. Roco has zero knowledge.', note: '' });
      broadcast({ type: 'WIPE_COMPLETE' });

      info('[WIPE] Full system reset complete');
      res.json({ success: true, results });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── ERROR HANDLERS ───────────────────────────────────────────────────────

  // JSON error handler — must come after all routes, catches anything that calls next(err)
  app.use((err, req, res, next) => { // eslint-disable-line no-unused-vars
    console.error(`[ROCO] Unhandled route error ${req.method} ${req.path}:`, err.message);
    res.status(err.status || 500).json({ error: err.message || 'Internal server error' });
  });

  // ─── ANALYTICS ────────────────────────────────────────────────────────────

  app.get('/api/analytics/weeks', requireAuth, async (req, res) => {
    try {
      const sb = getSupabase();
      const { data } = await sb.from('weekly_intelligence').select('*')
        .order('week_start', { ascending: false });
      res.json(data || []);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/analytics/daily-logs', requireAuth, async (req, res) => {
    try {
      const rows = await listDailyActivityReports();
      res.json(rows || []);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ─── JARVIS ORB ──────────────────────────────────────────────────────────────

  app.post('/api/jarvis', requireAuth, async (req, res) => {
    try {
      const { message, chatId, dealId } = req.body || {};
      if (!message || typeof message !== 'string' || !message.trim()) {
        return res.status(400).json({ error: 'message required' });
      }
      const { handleMessage } = await import('../core/jarvis.js');
      const effectiveChatId = chatId || 'dashboard-orb';
      const reply = await handleMessage(effectiveChatId, message.trim(), dealId || null);
      const action = await resolveJarvisDashboardAction(message, dealId || null).catch(() => null);
      res.json({ reply: reply || '', action });
    } catch (err) {
      console.error('[JARVIS ORB]', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/jarvis/speak', requireAuth, async (req, res) => {
    const { text } = req.body || {};
    if (!text || typeof text !== 'string') return res.status(400).json({ error: 'text required' });
    const apiKey = process.env.ELEVENLABS_API_KEY;
    const configuredVoiceId = process.env.ELEVENLABS_VOICE_ID || '0pa5K4pOrbnP5VS5eH6k';
    const modelId = 'eleven_flash_v2_5';
    if (!apiKey) {
      updateJarvisVoiceStatus({
        ok: false,
        configured: false,
        voice_id: configuredVoiceId,
        model_id: modelId,
        upstream_status: 503,
        error: 'ElevenLabs not configured on this service',
      });
      return res.status(503).json({ error: 'ElevenLabs not configured on this service' });
    }
    try {
      const voiceId = configuredVoiceId;
      // eleven_flash_v2_5 = ~75ms latency (vs 2000ms+ for multilingual_v2)
      // optimize_streaming_latency=3 removes audio normalizers for faster first byte
      const elevenRes = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId}/stream?optimize_streaming_latency=3`, {
        method: 'POST',
        headers: {
          'xi-api-key': apiKey,
          'Content-Type': 'application/json',
          'Accept': 'audio/mpeg',
        },
        body: JSON.stringify({
          text: text.slice(0, 2500),
          model_id: modelId,
          voice_settings: { stability: 0.4, similarity_boost: 0.85, style: 0.35, use_speaker_boost: true },
        }),
      });
      if (!elevenRes.ok) {
        const msg = await elevenRes.text().catch(() => '');
        const compactMsg = msg.slice(0, 220);
        const paymentIssue = compactMsg.toLowerCase().includes('payment_issue');
        const clientError = paymentIssue
          ? 'ElevenLabs billing issue: complete the latest invoice to restore Jarvis voice'
          : `ElevenLabs error ${elevenRes.status}: ${compactMsg}`;
        updateJarvisVoiceStatus({
          ok: false,
          configured: true,
          voice_id: voiceId,
          model_id: modelId,
          upstream_status: elevenRes.status,
          error: clientError,
        });
        return res.status(paymentIssue ? 402 : 502).json({ error: clientError, upstream_status: elevenRes.status });
      }
      updateJarvisVoiceStatus({
        ok: true,
        configured: true,
        voice_id: voiceId,
        model_id: modelId,
        upstream_status: 200,
        error: null,
      });
      res.set('Content-Type', 'audio/mpeg');
      Readable.fromWeb(elevenRes.body).pipe(res);
    } catch (err) {
      updateJarvisVoiceStatus({
        ok: false,
        configured: true,
        voice_id: configuredVoiceId,
        model_id: modelId,
        upstream_status: 500,
        error: err.message,
      });
      res.status(500).json({ error: `ElevenLabs request failed: ${err.message}` });
    }
  });

  app.get('/api/jarvis/voice-status', requireAuth, async (req, res) => {
    const configured = !!process.env.ELEVENLABS_API_KEY;
    const voiceId = process.env.ELEVENLABS_VOICE_ID || '0pa5K4pOrbnP5VS5eH6k';
    res.json({
      ...jarvisVoiceStatus,
      configured,
      voice_id: voiceId,
      model_id: 'eleven_flash_v2_5',
    });
  });

  // 404 handler — JSON for API/webhook paths, index.html for everything else
  app.use((req, res) => {
    if (req.path.startsWith('/api') || req.path.startsWith('/webhook')) {
      return res.status(404).json({ error: `Not found: ${req.method} ${req.path}` });
    }
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
  });
}

// ─────────────────────────────────────────────
// ACTIVITY PUSH
// ─────────────────────────────────────────────

export function pushActivity(entry, legacyType) {
  // Support legacy string call: pushActivity('message', 'type')
  if (typeof entry === 'string') {
    entry = { action: entry, type: legacyType || 'system' };
  }

  // Normalise: preserve the full human-readable activity line when both action and note exist.
  const action = String(entry.action || '').trim();
  const note = String(entry.note || '').trim();
  const summary = String(entry.summary || '').trim();
  const type = String(entry.type || 'system').toLowerCase();
  const full_content = entry.full_content || null;
  const message = entry.message
    || (action && note ? `${action} · ${note}` : '')
    || action
    || note
    || summary
    || 'System event';
  const deal_id = entry.deal_id || entry.dealId || null;
  const enriched = { ...entry, type, action, note, message, deal_id, timestamp: new Date().toISOString() };

  const fingerprint = buildActivityFingerprint(enriched);
  const now = Date.now();
  const isRecentDuplicate = activityFeed.some(existing => {
    if (buildActivityFingerprint(existing) !== fingerprint) return false;
    const ts = getActivityTimestamp(existing);
    return ts && now - ts < 10 * 60 * 1000;
  });
  if (isRecentDuplicate) return;

  activityFeed.push(enriched);
  if (activityFeed.length > MAX_FEED) activityFeed.shift();

  // Broadcast to all WebSocket clients
  if (wss) broadcastToAll({ type: 'activity', entry: enriched, feed: activityFeed.slice(-100) });

  // Console log for PM2 visibility
  console.log(`[${type.toUpperCase()}] ${action}${note ? ' — ' + note : ''}`);

  // Persist to Supabase activity_log — try new schema, fall back to old
  const sb = getSupabase();
  if (sb && entry.persist !== false) {
    sb.from('activity_log').insert({
      deal_id,
      type,
      action:       action || message,
      note:         note || null,
      full_content: full_content,
    }).then(() => {}).catch(() => {
      // Fall back to old schema (event_type, summary, detail)
      sb.from('activity_log').insert({
        deal_id,
        event_type: type.toUpperCase(),
        summary:    message,
        detail:     note ? { note } : null,
      }).then(() => {}).catch(err => console.warn('[pushActivity] activity_log fallback error:', err.message));
    });
  }
}

export function notifyQueueUpdated(count = null) {
  if (wss) broadcastToAll({ type: 'QUEUE_UPDATED', count });
}

async function resolveJarvisDashboardAction(message, dealId = null) {
  const text = String(message || '').trim();
  const lower = text.toLowerCase();
  if (!text) return null;

  if (/\b(approval|approve|queue|drafts?)\b/.test(lower)) return { type: 'open_view', view: 'queue' };
  if (/\b(analytics|weekly report|daily report|reports?)\b/.test(lower)) return { type: 'open_view', view: 'analytics' };
  if (/\b(transcript|meeting notes?|meetings?)\b/.test(lower)) return { type: 'open_view', view: 'transcripts' };
  if (/\b(activity|live log|activity log)\b/.test(lower)) return { type: 'open_view', view: 'activity' };

  const sb = getSupabase();
  if (!sb) {
    if (/\b(database|investor database)\b/.test(lower)) return { type: 'open_view', view: 'database' };
    if (/\b(pipeline|record|responder|responded|replied)\b/.test(lower)) return { type: 'open_view', view: 'pipeline' };
    return null;
  }

  if (/\b(last|latest|most recent)\b/.test(lower) && /\b(responder|responded|replied|reply|response)\b/.test(lower)) {
    let latest = null;
    try {
      let query = sb.from('conversation_messages')
        .select('contact_id, deal_id, contact_name, received_at, sent_at, created_at')
        .eq('direction', 'inbound')
        .not('contact_id', 'is', null)
        .order('created_at', { ascending: false })
        .limit(20);
      if (dealId) query = query.eq('deal_id', dealId);
      const { data } = await query;
      latest = (data || [])
        .sort((a, b) => new Date(b.received_at || b.sent_at || b.created_at || 0) - new Date(a.received_at || a.sent_at || a.created_at || 0))[0] || null;
    } catch {}

    if (!latest) {
      try {
        let query = sb.from('contacts')
          .select('id, deal_id, name, last_reply_at')
          .not('last_reply_at', 'is', null)
          .order('last_reply_at', { ascending: false })
          .limit(1);
        if (dealId) query = query.eq('deal_id', dealId);
        const { data } = await query;
        const contact = data?.[0] || null;
        if (contact) latest = { contact_id: contact.id, deal_id: contact.deal_id, contact_name: contact.name };
      } catch {}
    }

    if (latest?.contact_id) {
      return { type: 'open_contact', view: 'pipeline', contactId: latest.contact_id, dealId: latest.deal_id || dealId || null, label: latest.contact_name || 'Last responder' };
    }
    return { type: 'open_view', view: 'pipeline' };
  }

  const wantsRecord = /\b(open|show|pull up|go to|find|view)\b/.test(lower)
    && /\b(record|profile|pipeline|database|investor|contact|thing)\b/.test(lower);
  if (wantsRecord) {
    const cleaned = lower
      .replace(/\b(open|show|pull|up|go|to|find|view|please|can|you|me|the|inside|in|dashboard|record|profile|pipeline|database|investor|contact|thing|section|for|of)\b/g, ' ')
      .replace(/[^a-z0-9\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    const terms = cleaned.split(/\s+/).filter(term => term.length > 1).slice(0, 4);

    if (terms.length) {
      const nameProbe = terms.join(' ');
      try {
        let query = sb.from('contacts')
          .select('id, deal_id, name, company_name, updated_at')
          .ilike('name', `%${terms[0]}%`)
          .order('updated_at', { ascending: false })
          .limit(20);
        if (dealId) query = query.eq('deal_id', dealId);
        const { data } = await query;
        const matches = (data || []).filter(row => {
          const haystack = `${row.name || ''} ${row.company_name || ''}`.toLowerCase();
          return terms.every(term => haystack.includes(term)) || haystack.includes(nameProbe);
        });
        const contact = matches[0] || data?.[0] || null;
        if (contact) return { type: 'open_contact', view: 'pipeline', contactId: contact.id, dealId: contact.deal_id || dealId || null, label: contact.name || 'Contact' };
      } catch {}

      if (/\b(database|investor database|investor)\b/.test(lower)) {
        try {
          const { data } = await sb.from('investors_db')
            .select('id, name, company_name, updated_at')
            .or(`name.ilike.%${terms[0]}%,company_name.ilike.%${terms[0]}%`)
            .order('updated_at', { ascending: false })
            .limit(10);
          const match = (data || []).find(row => {
            const haystack = `${row.name || ''} ${row.company_name || ''}`.toLowerCase();
            return terms.every(term => haystack.includes(term));
          }) || data?.[0] || null;
          if (match) return { type: 'open_investor_db', view: 'database', investorId: match.id, label: match.name || match.company_name || 'Investor' };
        } catch {}
      }
    }
  }

  if (/\b(database|investor database)\b/.test(lower)) return { type: 'open_view', view: 'database' };
  if (/\b(pipeline)\b/.test(lower)) return { type: 'open_view', view: 'pipeline' };
  return null;
}

// ─────────────────────────────────────────────
// PITCHBOOK IMPORT HELPERS
// ─────────────────────────────────────────────

async function importInvestorUniverse(sb, rows, dealId, filename) {
  const listName = filename.replace(/\.xlsx?$/i, '').replace(/_/g, ' ').trim();

  const { data: existingList } = await sb.from('investor_lists')
    .select('id')
    .eq('name', listName)
    .limit(1)
    .maybeSingle();

  let listId;
  if (existingList?.id) {
    listId = existingList.id;
  } else {
    const { data: newList, error: newListError } = await sb.from('investor_lists').insert({
      name: listName,
    }).select('id').single();
    if (newListError) throw new Error(newListError.message);
    listId = newList.id;
  }

  const investors = rows
    .filter(r => (r['Investor ID'] || r['Limited Partner ID']) && (r['Investors'] || r['Limited Partners']))
    .map(r => ({
      pitchbook_id:                    String(r['Investor ID'] || r['Limited Partner ID'] || '').trim(),
      name:                            String(r['Investors'] || r['Limited Partners'] || '').trim(),
      legal_name:                      String(r['Investor Legal Name'] || '').trim() || null,
      description:                     String(r['Description'] || '').slice(0, 2000) || null,
      investor_type:                   String(r['Primary Investor Type'] || r['Limited Partner Type'] || '').trim() || null,
      aum_millions:                    r['AUM'] != null ? parseFloat(String(r['AUM']).replace(/[^0-9.-]/g, '')) || null : null,
      dry_powder_millions:             r['Dry Powder'] != null ? parseFloat(String(r['Dry Powder']).replace(/[^0-9.-]/g, '')) || null : null,
      hq_city:                         String(r['HQ City'] || '').trim() || null,
      hq_state:                        String(r['HQ State/Province'] || '').trim() || null,
      hq_country:                      String(r['HQ Country/Territory/Region'] || '').trim() || null,
      hq_region:                       String(r['HQ Global Region'] || '').trim() || null,
      hq_email:                        String(r['HQ Email'] || '').trim().toLowerCase() || null,
      website:                         String(r['Website'] || '').trim() || null,
      preferred_industries:            String(r['Preferred Industry'] || '').trim() || null,
      preferred_geographies:           String(r['Preferred Geography'] || '').trim() || null,
      preferred_investment_types:      String(r['Preferred Investment Types'] || '').trim() || null,
      preferred_investment_amount_min: r['Preferred Investment Amount Min'] ? String(r['Preferred Investment Amount Min']).trim() || null : null,
      preferred_investment_amount_max: r['Preferred Investment Amount Max'] ? String(r['Preferred Investment Amount Max']).trim() || null : null,
      preferred_deal_size_min:         r['Preferred Deal Size Min'] ? String(r['Preferred Deal Size Min']).trim() || null : null,
      preferred_deal_size_max:         r['Preferred Deal Size Max'] ? String(r['Preferred Deal Size Max']).trim() || null : null,
      preferred_ebitda_min:            r['Preferred EBITDA Min'] ? String(r['Preferred EBITDA Min']).trim() || null : null,
      preferred_ebitda_max:            r['Preferred EBITDA Max'] ? String(r['Preferred EBITDA Max']).trim() || null : null,
      investments_last_12m:            r['Investments in the last 12 months'] ? Number(r['Investments in the last 12 months']) || null : null,
      last_investment_date:            r['Last Investment Date'] != null ? String(r['Last Investment Date']) : null,
      last_investment_company:         String(r['Last Investment Company'] || '').trim() || null,
      list_id:                         listId,
      list_name:                       listName,
    }))
    .filter(i => i.name && i.name.length > 1 && i.pitchbook_id);

  if (!investors.length) {
    throw new Error(`Investor universe parser found 0 investors in "${filename}". Expected "Investor ID" and "Investors" columns on the PitchBook header row.`);
  }

  let imported = 0;
  for (let i = 0; i < investors.length; i += 100) {
    const batch = investors.slice(i, i + 100);
    const { error } = await sb.from('investors_db').upsert(batch, {
      onConflict: 'pitchbook_id',
      ignoreDuplicates: false,
    });
    if (!error) imported += batch.length;
    else console.warn('[IMPORT INVESTOR] Batch error:', error.message);
  }

  await sb.from('investor_lists').update({ investor_count: imported }).eq('id', listId);
  await sb.from('deals').update({
    priority_list_id: listId,
    priority_list_name: listName,
    updated_at: new Date().toISOString(),
  }).eq('id', dealId);
  // Insert into deal_list_priorities — try with optional columns first, fall back to minimal row
  const listPriorityRowFull = { deal_id: dealId, list_id: listId, list_name: listName, priority_order: -1, status: 'pending', source: 'pitchbook' };
  const listPriorityRowMin  = { deal_id: dealId, list_id: listId, list_name: listName, priority_order: -1 };
  const { error: lpErr } = await sb.from('deal_list_priorities')
    .upsert(listPriorityRowFull, { onConflict: 'deal_id,list_id', ignoreDuplicates: false });
  if (lpErr) {
    await sb.from('deal_list_priorities').delete().eq('deal_id', dealId).eq('list_id', listId);
    const { error: lpInsertErr } = await sb.from('deal_list_priorities').insert(listPriorityRowFull);
    if (lpInsertErr) {
      // source/status columns may not exist — try minimal row
      await sb.from('deal_list_priorities').delete().eq('deal_id', dealId).eq('list_id', listId);
      const { error: lpMinErr } = await sb.from('deal_list_priorities').insert(listPriorityRowMin);
      if (lpMinErr) console.warn('[IMPORT INVESTOR] deal_list_priorities insert error:', lpMinErr.message);
    }
  }

  pushActivity({
    type: 'research',
    action: `Investor universe imported: ${imported} investors added to database`,
    note: `${listName} — set as priority source for this deal`,
    dealId,
  });

  return { imported, listId, listName };
}

async function importComparableDeals(sb, rows, dealId) {
  const records = rows
    .filter(r => r['Deal ID'] && (r['Description'] || r['Financing Status Note']))
    .map(r => ({
      deal_id: dealId,
      source_deal_id: String(r['Deal ID'] || '').trim(),
      source_company: String(r['Companies'] || '').trim(),
      description: String(r['Description'] || '').slice(0, 3000) || null,
      financing_note: String(r['Financing Status Note'] || '').slice(0, 2000) || null,
      deal_date: r['Deal Date'] ? String(r['Deal Date']) : null,
      deal_size: r['Deal Size'] ? String(r['Deal Size']) : null,
      deal_type: String(r['Deal Type'] || '').trim() || null,
      primary_sector: String(r['Primary Industry Sector'] || '').trim() || null,
      all_industries: String(r['All Industries'] || '').trim() || null,
      hq_country: String(r['Company Country/Territory/Region'] || '').trim() || null,
      hq_region: String(r['HQ Global Region'] || '').trim() || null,
      ebitda: r['EBITDA'] ? String(r['EBITDA']) : null,
      investors_raw: String(r['Investors'] || '').trim() || null,
      lead_investors: String(r['Lead/Sole Investors'] || '').trim() || null,
      sponsor: String(r['Sponsor'] || '').trim() || null,
    }))
    .filter(r => r.source_company && r.source_deal_id);

  if (!records.length) {
    throw new Error('Comparable deals parser found 0 records. Expected "Deal ID", "Companies", and "Description" or "Financing Status Note" columns.');
  }

  const seen = new Set();
  const unique = records.filter(r => {
    if (seen.has(r.source_deal_id)) return false;
    seen.add(r.source_deal_id);
    return true;
  });

  let imported = 0;
  for (let i = 0; i < unique.length; i += 100) {
    const batch = unique.slice(i, i + 100);
    const { error } = await sb.from('deal_intelligence').upsert(batch, {
      onConflict: 'deal_id,source_deal_id',
      ignoreDuplicates: true,
    });
    if (!error) imported += batch.length;
    else console.warn('[IMPORT DEALS] Batch error:', error.message);
  }

  pushActivity({
    type: 'research',
    action: `Comparable deals imported: ${imported} records`,
    note: 'AI similarity analysis will run in background',
    dealId,
  });

  import('../core/dealIntelligence.js').then(({ analyzeDealIntelligence }) => {
    analyzeDealIntelligence(dealId, pushActivity).catch(err =>
      console.error('[DEAL INTEL ANALYSIS]', err.message)
    );
  }).catch(() => {});

  return { imported };
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────

function broadcastToAll(data) {
  if (!wss) return;
  const msg = JSON.stringify(data);
  wss.clients.forEach(ws => {
    if (ws.readyState === 1) ws.send(msg);
  });
}

// ─────────────────────────────────────────────
// DEBOUNCE BATCHER — 90-second multi-reply handler
// ─────────────────────────────────────────────

function decodeHtmlEntities(value) {
  return String(value || '')
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => {
      const code = parseInt(hex, 16);
      return Number.isFinite(code) && code >= 0 && code <= 0x10FFFF ? String.fromCodePoint(code) : '';
    })
    .replace(/&#(\d+);/g, (_, num) => {
      const code = parseInt(num, 10);
      return Number.isFinite(code) && code >= 0 && code <= 0x10FFFF ? String.fromCodePoint(code) : '';
    })
    .replace(/&nbsp;|&ensp;|&emsp;|&thinsp;/gi, ' ')
    .replace(/&zwnj;|&zwj;|&lrm;|&rlm;/gi, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;|&#39;/g, "'");
}

function stripHtml(html) {
  if (!html) return '';
  let text = String(html)
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<\/div>/gi, '\n')
    .replace(/<\/li>/gi, '\n')
    .replace(/<[^>]+>/g, '');
  text = decodeHtmlEntities(text);
  text = text
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>|<\/div>|<\/li>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/[\u200B-\u200F\u202A-\u202E\u2060\uFEFF]/g, '')
    .replace(/[\u00A0\u2000-\u200A\u202F\u205F\u3000]/g, ' ')
    .replace(/\r\n/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n[ \t]+/g, '\n')
    .replace(/[ \t]{2,}/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  return text;
}

function normalizeInboundEmail(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';
  const bracketMatch = raw.match(/<([^>]+)>/);
  const candidate = bracketMatch?.[1] || raw;
  return candidate.replace(/^mailto:/, '').trim();
}

async function fetchInvestorContactById(contactId) {
  const sb = getSupabase();
  if (!sb || !contactId) return null;
  const { data } = await sb.from('contacts').select('*, deals(*)').eq('id', contactId).limit(1).maybeSingle();
  return data || null;
}

async function fetchSourcingContactById(contactId) {
  const sb = getSupabase();
  if (!sb || !contactId) return null;
  const { data } = await sb.from('company_contacts').select('*, sourcing_campaigns(*), target_companies(*)').eq('id', contactId).limit(1).maybeSingle();
  return data || null;
}

async function findInvestorContactByEmail(sb, email, { requireActiveDeal = true } = {}) {
  const normalized = normalizeInboundEmail(email);
  if (!sb || !normalized) return null;
  const { data } = await sb.from('contacts')
    .select('*, deals(*)')
    .ilike('email', normalized)
    .limit(10);
  return pickBestInvestorContact(data || [], { requireActiveDeal });
}

async function findInvestorContactByEmailThread(sb, threadId, { requireActiveDeal = true } = {}) {
  if (!sb || !threadId) return null;
  const fields = ['thread_id', 'gmail_thread_id'];
  for (const field of fields) {
    try {
      const { data } = await sb.from('emails')
        .select('contact_id')
        .eq(field, threadId)
        .not('contact_id', 'is', null)
        .order('sent_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (data?.contact_id) {
        const contact = await fetchInvestorContactById(data.contact_id);
        if (contact && (!requireActiveDeal || String(contact?.deals?.status || '').toUpperCase() === 'ACTIVE')) return contact;
      }
    } catch {}
  }
  try {
    const { data } = await sb.from('replies')
      .select('contact_id')
      .eq('thread_id', threadId)
      .not('contact_id', 'is', null)
      .order('received_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (data?.contact_id) {
      const contact = await fetchInvestorContactById(data.contact_id);
      if (contact && (!requireActiveDeal || String(contact?.deals?.status || '').toUpperCase() === 'ACTIVE')) return contact;
    }
  } catch {}
  return null;
}

async function findInvestorContactByDisplayName(sb, fromName, { requireActiveDeal = true } = {}) {
  const normalizedTarget = normalizeComparableName(fromName);
  const first = normalizedTarget.split(/\s+/).filter(Boolean)[0];
  if (!sb || !normalizedTarget || !first) return null;
  try {
    const { data } = await sb.from('contacts')
      .select('*, deals(*)')
      .ilike('name', `%${first}%`)
      .limit(25);
    const candidates = (data || []).filter(row => {
      const rowName = normalizeComparableName(row?.name);
      return rowName === normalizedTarget || (normalizedTarget.length >= 6 && rowName.includes(normalizedTarget));
    });
    return pickBestInvestorContact(candidates, { requireActiveDeal });
  } catch {
    return null;
  }
}

async function findOriginalEmailByThread(sb, threadId) {
  if (!sb || !threadId) return null;
  for (const field of ['thread_id', 'gmail_thread_id']) {
    try {
      const { data } = await sb.from('emails')
        .select('*, deals(name, description, sector, status)')
        .eq(field, threadId)
        .order('sent_at', { ascending: true })
        .limit(1)
        .maybeSingle();
      if (data) return data;
    } catch {}
  }
  return null;
}

async function queueInboundWithDebounce({ fromEmail, fromUrn, fromName, subject, bodyText, threadId, chatId, messageId, channel, emailAccountId, raw }) {
  bodyText = stripHtml(bodyText);
  if (!bodyText?.trim()) return;

  const normalizedEmail = normalizeInboundEmail(fromEmail);
  const contactKey = channel === 'linkedin'
    ? (chatId || fromUrn || fromName)
    : (normalizedEmail || threadId || fromName);
  if (!contactKey) return;

  const batchKey  = `${channel}_${contactKey}`;
  const msgEntry  = {
    content: bodyText.trim(),
    received_at: new Date(),
    channel,
    subject: subject || null,
    threadId: threadId || chatId || null,
    messageId,
    emailAccountId: emailAccountId || null,
    fromEmail: normalizedEmail || null,
    fromUrn: fromUrn || null,
    fromName: fromName || null,
    chatId: chatId || null,
    raw,
  };
  const messageDedupeKey = messageId
    ? `${channel}:message:${messageId}`
    : `${channel}:${contactKey}:${subject || ''}:${bodyText.trim().slice(0, 240)}`;
  if (isDuplicateInboundReply(messageDedupeKey)) {
    console.log(`[REPLY] Duplicate inbound ${channel} message suppressed for ${contactKey}`);
    return;
  }

  if (replyDebounceMap.has(batchKey)) {
    const existing = replyDebounceMap.get(batchKey);
    if (messageId && existing.messages.some(message => String(message.messageId || '') === String(messageId))) {
      console.log(`[REPLY] Duplicate batched message suppressed for ${contactKey}`);
      return;
    }
    clearTimeout(existing.timer);
    existing.messages.push(msgEntry);
    existing.timer = setTimeout(() => flushReplyBatch(batchKey), REPLY_DEBOUNCE_MS);
    replyDebounceMap.set(batchKey, existing);
    console.log(`[REPLY] Batched message ${existing.messages.length} from ${contactKey}`);
  } else {
    // Resolve contact + context before batching
    const ctx = await resolveContactAndContext({
      channel,
      contactKey,
      fromEmail: normalizedEmail,
      fromUrn,
      fromName,
      threadId,
      chatId,
    });

    if (!ctx.contact) {
      console.log(`[REPLY] No active deal/contact context for ${contactKey} — logged receipt only`);
      pushActivity({
        type: channel === 'linkedin' ? 'linkedin' : 'email',
        activity_badge: channel === 'linkedin' ? 'linkedin' : 'email',
        activity_key: messageId ? `unmatched_inbound:${channel}:${messageId}` : null,
        action: `${channel === 'linkedin' ? 'Inbound LinkedIn DM received' : 'Inbound email received'}: ${fromName || normalizedEmail || fromUrn || contactKey}`,
        note: buildInboundMessageNote([
          subject ? `Subject: "${subject}"` : null,
          bodyText || null,
        ]),
        full_content: bodyText || null,
        meta: {
          matched_active_deal: false,
          from_email: normalizedEmail || null,
          provider_id: fromUrn || null,
          subject: subject || null,
        },
      });
      return;
    }

    const timer = setTimeout(() => flushReplyBatch(batchKey), REPLY_DEBOUNCE_MS);
    const contextLabel = ctx.deal?.name || ctx.campaign?.name || null;
    pushActivity({
      type: 'reply',
      activity_badge: getReplyActivityBadge(channel),
      action: `${channel === 'linkedin' ? 'LinkedIn reply received' : 'Email reply received'}: ${ctx.contact.name}`,
      note: `${truncateInline(bodyText, 120)}${contextLabel ? ` · ${contextLabel}` : ''}${subject ? ` · "${truncateInline(subject, 80)}"` : ''}`,
      full_content: bodyText,
      dealId: ctx.deal?.id || null,
      deal_name: ctx.deal?.name || null,
    });
    replyDebounceMap.set(batchKey, {
      timer,
      contact:   ctx.contact,
      deal:      ctx.deal || null,
      campaign:  ctx.campaign || null,
      mode:      ctx.mode,
      threadId:  threadId || chatId || null,
      messages:  [msgEntry],
    });
    console.log(`[REPLY] Started batch for ${ctx.contact.name} (${ctx.mode})`);
  }
}

async function flushReplyBatch(batchKey) {
  const batch = replyDebounceMap.get(batchKey);
  replyDebounceMap.delete(batchKey);
  if (!batch?.messages?.length) return;
  console.log(`[REPLY] Flushing ${batch.messages.length} message(s) from ${batch.contact.name}`);
  await processRocoBatchedReply(batch).catch(err => {
    console.error(`[REPLY] processRocoBatchedReply failed for ${batch.contact.name}:`, err.message);
  });
}

async function resolveContactAndContext({ contactKey, channel, fromEmail, fromUrn, fromName, threadId, chatId }) {
  const sb = getSupabase();
  if (!sb) return { contact: null, deal: null, campaign: null, mode: null };

  // Investor outreach — check contacts table
  try {
    if (channel === 'linkedin' && chatId) {
      const { data } = await sb.from('contacts').select('*, deals(*)').eq('unipile_chat_id', chatId).limit(5);
      const contact = pickBestInvestorContact(data || [], { requireActiveDeal: true });
      if (contact) return { contact, deal: contact.deals || null, campaign: null, mode: 'investor_outreach' };
    }
    if (channel === 'linkedin' && (fromUrn || contactKey)) {
      const providerOrIdentity = fromUrn || contactKey;
      const { data: providerMatches } = await sb.from('contacts').select('*, deals(*)').eq('linkedin_provider_id', providerOrIdentity).limit(5);
      const providerContact = pickBestInvestorContact(providerMatches || [], { requireActiveDeal: true });
      if (providerContact) return { contact: providerContact, deal: providerContact.deals || null, campaign: null, mode: 'investor_outreach' };

      const identityCandidates = extractLinkedInIdentityCandidates(providerOrIdentity);
      for (const identity of identityCandidates) {
        const { data: linkedinMatches } = await sb.from('contacts')
          .select('*, deals(*)')
          .ilike('linkedin_url', `%${identity}%`)
          .limit(10);
        const linkedinContact = pickBestInvestorContact(linkedinMatches || [], { requireActiveDeal: true });
        if (linkedinContact) return { contact: linkedinContact, deal: linkedinContact.deals || null, campaign: null, mode: 'investor_outreach' };
      }
    }
    if (channel === 'linkedin' && fromName && fromName.trim().split(/\s+/).length >= 2) {
      const contact = await findInvestorContactByDisplayName(sb, fromName, { requireActiveDeal: true });
      if (contact) {
        console.log(`[RESOLVE] Matched LinkedIn sender by name "${fromName}" → ${contact.name}`);
        return { contact, deal: contact.deals || null, campaign: null, mode: 'investor_outreach' };
      }
    }
    if (channel === 'email' && fromEmail) {
      const contact = await findInvestorContactByEmail(sb, fromEmail, { requireActiveDeal: true });
      if (contact) return { contact, deal: contact.deals || null, campaign: null, mode: 'investor_outreach' };
    }
    if (channel === 'email' && threadId) {
      const contact = await findInvestorContactByEmailThread(sb, threadId, { requireActiveDeal: true });
      if (contact) return { contact, deal: contact.deals || null, campaign: null, mode: 'investor_outreach' };
    }
    // Name-based fallback — catches replies from alternate email addresses (e.g. gcampbell@ vs bcampbell@)
    if (channel === 'email' && fromName && fromName.trim().split(/\s+/).length >= 2) {
      const contact = await findInvestorContactByDisplayName(sb, fromName, { requireActiveDeal: true });
      if (contact) {
        console.log(`[RESOLVE] Matched by name "${fromName}" → ${contact.name} (email mismatch fallback)`);
        return { contact, deal: contact.deals || null, campaign: null, mode: 'investor_outreach' };
      }
    }
  } catch {}

  // Company sourcing — check company_contacts table
  try {
    if (channel === 'linkedin' && (fromUrn || contactKey)) {
      const { data } = await sb.from('company_contacts').select('*, sourcing_campaigns(*), target_companies(*)').eq('linkedin_provider_id', fromUrn || contactKey).limit(1).maybeSingle();
      if (data) return { contact: data, deal: null, campaign: data.sourcing_campaigns || null, mode: 'company_sourcing' };
    }
    if (channel === 'email' && fromEmail) {
      const { data } = await sb.from('company_contacts').select('*, sourcing_campaigns(*), target_companies(*)').ilike('email', fromEmail).limit(1).maybeSingle();
      if (data) return { contact: data, deal: null, campaign: data.sourcing_campaigns || null, mode: 'company_sourcing' };
    }
    if (channel === 'email' && threadId) {
      const { data: priorReply } = await sb.from('replies')
        .select('contact_id')
        .eq('thread_id', threadId)
        .not('contact_id', 'is', null)
        .order('received_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (priorReply?.contact_id) {
        const contact = await fetchSourcingContactById(priorReply.contact_id);
        if (contact) return { contact, deal: null, campaign: contact.sourcing_campaigns || null, mode: 'company_sourcing' };
      }
    }
  } catch {}

  return { contact: null, deal: null, campaign: null, mode: null };
}

function validateMatchedReplyContext({ contact, deal, campaign, mode, channel, messages = [] }) {
  const reasons = [];
  const hasPerson = !!(contact?.id && String(contact?.name || '').trim());
  const activeDealVerified = mode === 'investor_outreach'
    ? !!(deal?.id && String(deal?.status || '').toUpperCase() === 'ACTIVE')
    : !!(campaign?.id && !['closed', 'archived', 'paused'].includes(String(campaign?.status || '').toLowerCase()));
  const firmVerified = !!(
    contact?.firm_id ||
    String(contact?.company_name || '').trim() ||
    String(campaign?.firm_name || '').trim() ||
    String(contact?.target_companies?.company_name || '').trim()
  );
  const validContactDetails = channel === 'email'
    ? !!(
        normalizeInboundEmail(contact?.email) ||
        messages.some(msg => normalizeInboundEmail(msg?.fromEmail || msg?.raw?.from_email || msg?.raw?.from_attendee?.identifier))
      )
    : !!(
        contact?.linkedin_provider_id ||
        contact?.unipile_chat_id ||
        messages.some(msg => msg?.chatId || msg?.fromUrn || msg?.raw?.chat_id || msg?.raw?.sender?.attendee_provider_id)
      );

  if (!hasPerson) reasons.push('missing_person_match');
  if (!activeDealVerified) reasons.push('not_verified_active_deal');
  if (!firmVerified) reasons.push('missing_verified_firm');
  if (!validContactDetails) reasons.push('missing_valid_contact_details');

  return { ok: reasons.length === 0, reasons };
}

async function processRocoBatchedReply(batch) {
  const { contact, deal, campaign, mode, messages, threadId } = batch;
  const emailAccountId = messages.find(m => m.emailAccountId)?.emailAccountId || null;
  const sb = getSupabase();
  const combinedContent = messages.map(m => m.content).join('\n\n');
  const channel = messages[0].channel;
  const contextName = deal?.name || campaign?.name || 'Unknown';
  const contactTable = mode === 'investor_outreach' ? 'contacts' : 'company_contacts';

  // Log received messages
  for (const msg of messages) {
    await sb?.from('activity_log').insert({
      deal_id:    deal?.id || null,
      contact_id: contact.id,
      event_type: 'REPLY_RECEIVED',
      summary:    `[${contextName}]: ${contact.name} sent a message via ${channel}`,
      detail:     { content: msg.content, channel },
    }).then(null, () => {});
  }

  // Update contact state immediately so the pipeline/campaign board reflects that
  // this person is no longer in a cold outbound stage.
  const statePatch = {
    response_received: true,
    last_reply_at:     new Date().toISOString(),
    response_summary:  combinedContent.slice(0, 200),
    ...(contactTable === 'contacts'
      ? { pipeline_stage: 'In Conversation', follow_up_due_at: null }
      : { pipeline_stage: 'contacted' }),
  };
  await sb?.from(contactTable).update(statePatch).eq('id', contact.id);

  // Record which channel the reply came in on (for channel loyalty in responses)
  await sb?.from(contactTable).update({ reply_channel: channel })
    .eq('id', contact.id);

  // Log every inbound turn individually so the per-contact thread stays exact.
  if (mode === 'investor_outreach' && contact?.id) {
    for (const msg of messages) {
      await logConversationMessage({
        contactId:        contact.id,
        dealId:           deal?.id || null,
        direction:        'inbound',
        channel,
        subject:          msg?.subject || null,
        body:             msg.content,
        unipileMessageId: msg?.messageId || null,
      }).catch(() => {});
    }
  }

  const replyContextValidation = validateMatchedReplyContext({ contact, deal, campaign, mode, channel, messages });
  if (!replyContextValidation.ok) {
    const reasonText = replyContextValidation.reasons.join(', ');
    await sb?.from('activity_log').insert({
      deal_id:    deal?.id || null,
      contact_id: contact?.id || null,
      event_type: 'REPLY_REVIEW_SKIPPED',
      summary:    `[${contextName}]: Reply automation skipped for ${contact?.name || 'unknown contact'}`,
      detail:     {
        reasons: replyContextValidation.reasons,
        channel,
        content_preview: combinedContent.slice(0, 240),
      },
    }).catch(() => {});
    pushActivity({
      type: 'system',
      action: `Reply automation skipped: ${contact?.name || 'unknown contact'}`,
      note: `${contextName} · ${reasonText}`,
      dealId: deal?.id || null,
      deal_name: deal?.name || null,
      persist: false,
    });
    return;
  }

  // Load conversation history — prefer conversation_messages table, fall back to activity_log
  let convMessages = [];
  if (mode === 'investor_outreach' && contact?.id) {
    convMessages = await getConversationHistory(contact.id, deal?.id || contact?.deal_id || null).catch(() => []);
  }

  let emailThreadHistory = [];
  if (channel === 'email' && threadId) {
    try {
      emailThreadHistory = await listEmails({
        threadId,
        accountId: emailAccountId || process.env.UNIPILE_OUTLOOK_ACCOUNT_ID || process.env.UNIPILE_GMAIL_ACCOUNT_ID,
        limit: 50,
      });
    } catch (err) {
      console.warn('[REPLY] Unipile email thread fetch failed:', err.message);
    }
  }

  // Build plain-text history for existing classifyAndDraftRocoReply prompt format
  const { data: history } = await sb?.from('activity_log')
    .select('*')
    .eq('contact_id', contact.id)
    .in('event_type', ['REPLY_RECEIVED', 'EMAIL_SENT', 'LINKEDIN_DM_SENT', 'MESSAGE_SENT', 'OUTREACH_SENT'])
    .order('created_at', { ascending: true }) || { data: [] };

  const conversationHistory = convMessages.length > 0
    ? formatConversationHistoryForProject(convMessages, contact, deal?.name || null)
    : (history || []).map(h => {
      const isInbound = h.event_type === 'REPLY_RECEIVED';
      const prefix = deal?.name ? `[${deal.name}] ` : '';
      return `${prefix}${isInbound ? contact.name : 'Roco'}: ${h.detail?.content || h.summary}`;
    }).join('\n');

  const remoteEmailThread = (emailThreadHistory || []).map(mail => {
    const isInboundMail = !mail.is_sender;
    const fromName = mail.from_attendee?.display_name || mail.from?.display_name || mail.from?.identifier || contact.name;
    const body = mail.body_plain || mail.body || mail.snippet || '';
    const prefix = deal?.name ? `[${deal.name}] ` : '';
    return `${prefix}${isInboundMail ? fromName : 'Roco'}: ${String(body).replace(/<[^>]+>/g, ' ').trim()}`;
  }).filter(Boolean).join('\n');

  const effectiveConversationHistory = [conversationHistory, remoteEmailThread].filter(Boolean).join('\n');

  // Mid-conversation research if triggered
  let researchContext = '';
  if (detectResearchNeeded(combinedContent)) {
    researchContext = await conductMidConversationResearch(combinedContent, contact, deal, campaign, mode, contextName);
  }

  // Load guidance fresh
  const guidanceBlock = await buildGuidanceBlock(mode);

  // Classify and draft
  const classification = await classifyAndDraftRocoReply(
    contact, combinedContent, effectiveConversationHistory,
    deal, campaign, mode, contextName,
    researchContext, guidanceBlock, messages.length
  );

  if (!classification) {
    await sb?.from('activity_log').insert({
      deal_id:    deal?.id || null,
      contact_id: contact.id,
      event_type: 'REPLY_CLASSIFICATION_ERROR',
      summary:    `[${contextName}]: Failed to classify reply from ${contact.name}`,
    }).then(null, () => {});
    return;
  }

  await notifyInboundReplyClassified({
    contact,
    deal,
    campaign,
    contextName,
    channel,
    message: combinedContent,
    classification,
  });

  // ── CONVERSATION STATE MACHINE (investor_outreach only) ─────────────────────
  if (mode === 'investor_outreach' && contact?.id) {
    try {
      // Run rich intent classification via conversationManager
      const intent = await classifyIntent(combinedContent, convMessages, contact, deal).catch(() => null);

      if (intent) {
        console.log(`[INTENT] ${contact.name} @ ${contact.company_name}: ${intent.intent_key} (${intent.category}, confidence: ${intent.confidence})`);

        // Append to intent_history on contact
        await appendIntentHistory(contact.id, {
          channel,
          message_preview:  combinedContent.substring(0, 100),
          intent:           intent.intent_key,
          intent_category:  classification.sentiment || intent.category,
          sentiment:        classification.sentiment || intent.category,
          action_taken:     intent.suggested_action,
        }).catch(() => {});

        // Stamp the latest intent + sentiment for pipeline display.
        if (sb) {
          await sb.from('contacts').update({
            last_intent:       intent.intent_key,
            last_intent_label: classification.sentiment || intent.category,
          }).eq('id', contact.id);
        }

        // Update intent on the conversation_messages record just inserted
        if (sb) {
          await sb.from('conversation_messages')
            .update({ intent: intent.intent_key, intent_confidence: intent.confidence, action_taken: intent.suggested_action })
            .eq('contact_id', contact.id)
            .eq('direction', 'inbound')
            .order('created_at', { ascending: false })
            .limit(1)
            .then(null, () => {});
        }

        // Apply conversation state
        if (intent.conversation_state === 'do_not_contact') {
          await setConversationState(contact.id, 'do_not_contact', {
            conversation_ended_reason: 'Requested removal',
            pipeline_stage:            'Inactive',
          }).catch(() => {});
          console.log(`[CONV] ${contact.name} marked Do Not Contact`);

        } else if (intent.is_temp_close) {
          await setConversationState(contact.id, 'temp_closed', {
            temp_closed_reason: combinedContent.substring(0, 200),
          }).catch(() => {});
          console.log(`[CONV] ${contact.name} temp_closed — will re-engage in 5 days`);

        } else if (intent.is_conversation_ended) {
          await setConversationState(contact.id, intent.conversation_state, {
            conversation_ended_reason: intent.conversation_ended_reason || intent.intent_key,
            pipeline_stage:            intent.category === 'positive' ? 'Meeting Booked' : 'Inactive',
          }).catch(() => {});

        } else {
          await setConversationState(contact.id, intent.conversation_state || 'awaiting_response').catch(() => {});
        }

        // Flag to Dom if required
        if (intent.requires_dom) {
          pushActivity({
            type:      'alert',
            action:    `DOM ACTION NEEDED`,
            note:      `${contact.name} @ ${contact.company_name} — ${intent.dom_flag_reason || intent.intent_key}`,
            dealId:    deal?.id,
            deal_name: deal?.name,
          });
          await sendTelegram(
            `⚠️ *Dom action needed*\n${contact.name} at ${contact.company_name}\nIntent: ${intent.intent_key}\n${intent.dom_flag_reason || ''}`
          ).catch(() => {});
        }
      }
    } catch (intentErr) {
      console.warn('[CONV] Intent classification error:', intentErr.message);
    }
  }
  // ────────────────────────────────────────────────────────────────────────────

  if (classification.intent === 'not_interested') {
    await handleNegativeReplyResponse(contact, deal, campaign, mode, contextName);
    return;
  }

  if (classification.intent === 'conversation_end') {
    // Conversation reached natural close (deal closed, they got what they needed, etc.) — archive without suppression
    await sb?.from('contacts').update({
      pipeline_stage:    'Archived',
      response_received: true,
      response_summary:  'Conversation ended',
      follow_up_due_at:  null,
    }).eq('id', contact.id);
    await sb?.from('activity_log').insert({
      deal_id:    deal?.id || null,
      contact_id: contact.id,
      event_type: 'CONVERSATION_ENDED',
      summary:    `[${contextName}]: Conversation with ${contact.name} ended naturally`,
    }).then(null, () => {});
    await sendTelegram(
      `🔚 *Conversation ended* — ${contact.name} (${contact.company_name || 'unknown'})\n[${contextName}] — archived.`
    ).catch(() => {});
    return;
  }

  // Queue one reply per inbound message so each approval can target the exact message it answers.
  for (let i = 0; i < messages.length; i++) {
    const inbound = messages[i];
    if (!inbound?.content?.trim()) continue;

    const perMessageDraft = await classifyAndDraftRocoReply(
      contact,
      inbound.content,
      effectiveConversationHistory,
      deal,
      campaign,
      mode,
      contextName,
      researchContext,
      guidanceBlock,
      1
    );
    const reply = perMessageDraft?.messages_to_send?.[0] || null;
    if (!reply?.body?.trim()) continue;

    if (/\[link\]|\[placeholder\]|\[calendar\]/i.test(reply.body)) {
      console.warn(`[REPLY] Placeholder detected in reply to ${contact.name} — skipping`);
      continue;
    }

    const replyTargetId = inbound.messageId || threadId || null;
    const replyLabel = channel === 'linkedin'
      ? `LinkedIn message ${i + 1}/${messages.length}`
      : `Email message ${i + 1}/${messages.length}`;
    const quotePreview = truncateInline(inbound.content, 160);
    const inboundSubject = inbound.subject || null;

    const { data: queued } = await sb?.from('approval_queue').insert({
      deal_id:            deal?.id || null,
      deal_name:          deal?.name || campaign?.name || null,
      contact_id:         contact.id,
      candidate_id:       contact.id,
      contact_name:       contact.name || '',
      contact_email:      contact.email || null,
      firm:               contact.company_name || '',
      campaign_id:        campaign?.id || null,
      company_contact_id: mode === 'company_sourcing' ? contact.id : null,
      message_type:       channel === 'linkedin' ? 'linkedin_reply' : 'email_reply',
      outreach_mode:      mode === 'investor_outreach' ? 'investor_outreach' : 'company_sourcing',
      channel,
      stage:              channel === 'linkedin' ? 'LinkedIn Reply' : 'Email Reply',
      message_text:       reply.body,
      body:               reply.body,
      subject_a:          reply.subject || null,
      reply_to_id:        replyTargetId,
      status:             'pending',
    }).select().single() || { data: null };

    if (!queued?.id) continue;

    await sendReplyForApproval(
      queued.id,
      contact,
      reply.body,
      contextName,
      channel,
      replyTargetId,
      emailAccountId,
      {
        replyLabel,
        quotePreview,
        inboundSubject,
        inboundBody: inbound.content,
        intent: classification.intent,
        sentiment: classification.sentiment,
      }
    ).catch(() => {});

    await sb?.from('activity_log').insert({
      deal_id:    deal?.id || null,
      contact_id: contact.id,
      event_type: 'REPLY_QUEUED',
      summary:    `[${contextName}]: Reply drafted for ${contact.name} — awaiting approval`,
      detail:     {
        label: reply.reply_to_context || replyLabel,
        intent: classification.intent,
        sentiment: classification.sentiment,
        quote_preview: quotePreview,
      },
    }).catch(() => {});

    pushActivity({
      type: 'approval',
      action: `Reply drafted for approval: ${contact.name}`,
      note: `${channel === 'linkedin' ? 'LinkedIn' : 'Email'} · ${classification.intent} · ${contextName}`,
      full_content: inbound.content,
      dealId: deal?.id || null,
      deal_name: deal?.name || null,
    });
  }
}

async function classifyAndDraftRocoReply(contact, combinedContent, conversationHistory, deal, campaign, mode, contextName, researchContext, guidanceBlock, messageCount) {
  const modeFrame = mode === 'investor_outreach'
    ? `You are responding on behalf of the deal principal. The contact is a potential investor. Goal: build interest and get them on a call or moving toward committing capital.`
    : `You are responding on behalf of the investment firm. The contact is a decision maker at a company the firm wants to invest in. Goal: build interest in the investment opportunity and get them on a call.`;

  const contextBlock = mode === 'investor_outreach' && deal
    ? `DEAL: ${deal.name} | ${deal.raise_type || deal.type || 'Investment'} | ${deal.sector} | Target: ${deal.target_amount || 'TBD'} | Min cheque: ${deal.min_cheque || deal.cheque_min || '—'}\nCONTACT: ${contact.name} at ${contact.company_name || 'unknown'} | Score: ${contact.investor_score || '—'}/100`
    : `CAMPAIGN: ${campaign?.name || 'Unknown'} | Firm: ${campaign?.firm_name || '—'} (${campaign?.firm_type || '—'})\nTHESIS: ${campaign?.investment_thesis || '—'}\nCONTACT: ${contact.name}, ${contact.title || '—'} at ${contact.company_name || 'unknown'}`;

  const researchBlock = researchContext
    ? `\nRESEARCH RESULTS (incorporate naturally — do not quote verbatim):\n${researchContext}`
    : '';

  const firstName = contact.name?.split(' ')[0] || 'there';

  const prompt = `${guidanceBlock}

${modeFrame}

${contextBlock}

FULL CONVERSATION:
${conversationHistory || '(no prior history)'}

LATEST (${messageCount} message${messageCount > 1 ? 's' : ''} received together):
${combinedContent}
${researchBlock}

Instructions:
1. Read all messages as one coherent turn
2. Determine intent, what they are asking/saying, and what the ideal response is
3. One reply handles everything unless they asked genuinely separate questions requiring distinct answers
4. Always move toward the next concrete step — a call, more info, or a soft close
5. Use first name: "${firstName}"
6. No em-dashes, no bullet points, no corporate language, 4-6 sentences max per reply
7. Sound like a knowledgeable human counterpart, not an AI agent
8. If the latest message is only an acknowledgement, sign-off, "okay", "thank you", "see you later", or otherwise does not require a useful response, set intent to conversation_end, next_action to continue or archive, and return messages_to_send as an empty array

Return ONLY valid JSON (no markdown):
{
  "intent": "interested|not_interested|asking_question|providing_info|considering|neutral|conversation_end",
  "sentiment": "positive|negative|neutral",
  "key_points": ["point1"],
  "ready_for_call": true,
  "messages_to_send": [
    {
      "reply_to_context": "what this reply addresses",
      "body": "full reply body",
      "subject": "email subject if email channel, null otherwise"
    }
  ],
  "next_action": "push_for_call|answer_question|provide_info|archive|continue",
  "internal_note": "one sentence on where this conversation stands"
}`;

  try {
    const text = await aiComplete(prompt, { maxTokens: 1000, task: `reply_classify:${contact.id}` });
    const parsed = JSON.parse(text.replace(/```json|```/g, '').trim());
    return normalizeReplyClassification(parsed);
  } catch (err) {
    console.error(`[REPLY] classifyAndDraftRocoReply failed for ${contact.name}:`, err.message);
    return null;
  }
}

async function notifyInboundReplyClassified({ contact, deal, campaign, contextName, channel, message, classification }) {
  const sentiment = String(classification?.sentiment || 'neutral').toLowerCase();
  const intent = String(classification?.intent || 'unknown').toLowerCase();
  const isPositive = sentiment === 'positive' || ['interested', 'asking_question', 'providing_info', 'considering'].includes(intent);
  const isNegative = sentiment === 'negative' || ['not_interested', 'conversation_end'].includes(intent);
  const marker = isPositive ? '🟩 Positive response' : isNegative ? '🟥 Negative response' : '🟪 Neutral response';
  const quote = sanitizeApprovalText(message).slice(0, 900);
  const firm = contact?.company_name || campaign?.firm_name || 'unknown firm';
  const dealName = deal?.name || contextName || campaign?.name || 'Unknown deal';
  const lines = [
    `*${marker}*`,
    `Deal: *${sanitizeApprovalText(dealName)}*`,
    `Person: *${sanitizeApprovalText(contact?.name || 'Unknown')}*`,
    `Firm: *${sanitizeApprovalText(firm)}*`,
    `Channel: ${channel === 'linkedin' ? 'LinkedIn' : 'Email'}`,
    `Sentiment: ${sentiment} | Intent: ${intent}`,
    '',
    `They said: _${quote || 'No message body captured'}_`,
  ];
  if (classification?.hasDraft !== false) {
    lines.push('');
    lines.push('_Draft reply queued for approval ✓_');
  }
  await sendTelegram(lines.join('\n')).catch(() => {});
}

function detectResearchNeeded(content) {
  const triggers = [
    'what other deals', 'track record', 'market size', 'current market',
    'comparable', 'similar deals', 'who else', 'other investors', 'proof',
    'how does this compare', 'valuation', 'recent news', 'traction',
    'revenue', 'growth rate', 'what stage', 'what round', 'case study',
  ];
  const lower = content.toLowerCase();
  return triggers.some(t => lower.includes(t));
}

async function conductMidConversationResearch(content, contact, deal, campaign, mode, contextName) {
  if (!process.env.ANTHROPIC_API_KEY) return '';
  try {
    const sector = deal?.sector || campaign?.target_sector || 'the relevant sector';
    const ctxDesc = mode === 'investor_outreach'
      ? `Fundraising for ${deal?.name} in ${sector}`
      : `Sourcing companies for ${campaign?.firm_name} in ${sector}`;

    const prompt = `A contact named ${contact.name} has asked this during an investment conversation:\n"${content}"\n\nContext: ${ctxDesc}\n\nUse web search to find 3-5 specific, accurate, current facts that would help answer this question in a confident, informed way. Be specific — cite real market data, comparable deals, or recent news where possible.\n\nReturn a concise factual paragraph only — no preamble, no explanation.`;

    const text = await claudeWebSearch(prompt, {
      maxTokens: 500,
      maxUses: 2,
      systemPrompt: 'Use web search when needed. Return a concise factual paragraph only with concrete, current information.',
      userLocation: {
        type: 'approximate',
        city: 'New York',
        region: 'New York',
        country: 'US',
        timezone: 'America/New_York',
      },
    });

    const sb = getSupabase();
    await sb?.from('activity_log').insert({
      contact_id: contact.id,
      event_type: 'MID_CONVERSATION_RESEARCH',
      summary:    `[${contextName}]: Mid-conversation research for ${contact.name}`,
      detail:     { question_preview: content.slice(0, 100) },
    }).then(null, () => {});

    return text;
  } catch (err) {
    console.warn(`[REPLY] Mid-conversation research failed: ${err.message}`);
    return '';
  }
}

async function handleNegativeReplyResponse(contact, deal, campaign, mode, contextName) {
  const table = mode === 'investor_outreach' ? 'contacts' : 'company_contacts';
  const sb = getSupabase();
  const now = new Date().toISOString();
  await sb?.from(table).update({
    pipeline_stage:   mode === 'investor_outreach' ? 'Inactive' : 'declined',
    conversation_state: mode === 'investor_outreach' ? 'conversation_ended_negative' : undefined,
    conversation_ended_at: mode === 'investor_outreach' ? now : undefined,
    conversation_ended_reason: mode === 'investor_outreach' ? 'Not interested' : undefined,
    response_received: true,
    response_summary:  'Not interested',
    ...(table === 'contacts' ? { last_intent: 'not_interested', last_intent_label: 'negative' } : {}),
    ...(table === 'contacts' ? { follow_up_due_at: null } : {}),
  }).eq('id', contact.id);

  const isAngel = contact.is_angel || contact.contact_type === 'angel';

  if (mode === 'investor_outreach' && !isAngel && contact.firm_id && deal?.id) {
    // Institutional investor — suppress the whole firm
    await sb?.from('firm_outreach_state').upsert({
      firm_id:               contact.firm_id,
      deal_id:               deal.id,
      response_received:     true,
      responding_contact_id: contact.id,
      status:                'declined',
    }).then(null, () => {});

    await sb?.from('firm_suppressions').upsert({
      firm_id:    contact.firm_id,
      deal_id:    deal.id,
      reason:     'declined',
      suppressed_at: now,
    }, { onConflict: 'firm_id,deal_id' }).then(null, () => {});
  } else if (mode === 'investor_outreach' && isAngel) {
    // Angel — they represent only themselves, no firm suppression
    console.log(`[SUPPRESSION] ${contact.name} is angel — individual suppression only, no firm-wide suppression`);
  } else if (mode === 'company_sourcing' && contact.company_id) {
    await sb?.from('target_companies').update({
      firm_responded:  true,
      outreach_status: 'declined',
    }).eq('id', contact.company_id);
  }

  if (mode === 'investor_outreach' && deal?.id && contact.company_name) {
    const peerUpdate = {
      pipeline_stage: 'Inactive',
      response_received: true,
      follow_up_due_at: null,
      last_intent: 'not_interested',
      last_intent_label: 'negative',
      conversation_state: 'conversation_ended_negative',
      conversation_ended_at: now,
      conversation_ended_reason: `Firm suppressed after ${contact.name} passed`,
    };
    let peerQuery = sb?.from('contacts').update(peerUpdate).eq('deal_id', deal.id).neq('id', contact.id);
    if (contact.firm_id) {
      peerQuery = peerQuery.eq('firm_id', contact.firm_id);
    } else {
      peerQuery = peerQuery.eq('company_name', contact.company_name);
    }
    await peerQuery;
    await sb?.from('batch_firms').update({ status: 'suppressed' })
      .eq('deal_id', deal.id)
      .ilike('firm_name', contact.company_name);
  }

  const suppressionScope = (mode === 'investor_outreach' && isAngel)
    ? 'individual suppressed'
    : mode === 'investor_outreach'
      ? 'firm suppressed'
      : 'company suppressed';
  await sendTelegram(
    `⛔ *Not interested* — ${contact.name} (${contact.company_name || 'unknown'})\n` +
    `Intent: not_interested\n` +
    `[${contextName}] — ${suppressionScope}.`
  ).catch(() => {});

  pushActivity({
    type: 'warning',
    action: `Firm suppressed after pass: ${contact.company_name || contact.name || 'contact'}`,
    note: `${contact.name || 'Contact'} classified as not interested · ${contextName}`,
    dealId: deal?.id || null,
    deal_name: deal?.name || null,
  });

  await sb?.from('activity_log').insert({
    deal_id:    deal?.id || null,
    contact_id: contact.id,
    event_type: 'OUTREACH_SUPPRESSED',
    summary:    `[${contextName}]: ${contact.name} declined — suppressed from further outreach`,
    detail:     {
      classification: 'not_interested',
      suppression_scope: suppressionScope,
      company_name: contact.company_name || null,
    },
  }).catch(() => {});
}

async function isDuplicateReplyApproval(candidateId, messageType, withinSeconds = 300) {
  try {
    const sb = getSupabase();
    if (!sb) return false;
    const since = new Date(Date.now() - withinSeconds * 1000).toISOString();
    const { data } = await sb.from('approval_queue')
      .select('id')
      .eq('candidate_id', candidateId)
      .eq('message_type', messageType)
      .eq('status', 'pending')
      .gte('created_at', since)
      .limit(1);
    return (data?.length || 0) > 0;
  } catch { return false; }
}

// ─────────────────────────────────────────────
// INBOUND REPLY HANDLERS (called by Unipile webhooks)
// ─────────────────────────────────────────────

async function classifyWithGpt({ body, fromEmail, fromName, dealName, dealDescription }) {
  const prompt =
    `Classify this investor reply for deal: "${dealName}".\n\n` +
    `From: ${fromName || fromEmail}\n` +
    `Message:\n"""\n${body.slice(0, 800)}\n"""\n\n` +
    `Return JSON: { "intent": "INTERESTED|WANTS_MORE_INFO|MEETING_REQUEST|NOT_INTERESTED|OPT_OUT|AUTO_REPLY|CONVERSATION_END|POSITIVE|NEUTRAL", "notes": "...", "sentiment": "positive|neutral|negative" }\n\nUse CONVERSATION_END when the investor is clearly wrapping up (e.g. "Thanks, I'll be in touch", "Not for us right now", "Let's revisit next year", "Good luck with the raise").`;
  try {
    const text = await aiComplete(prompt, {
      reasoning: 'low',
      maxTokens: 200,
      task: `classify:${fromName || fromEmail}`,
    });
    const parsed = JSON.parse(text.replace(/```json|```/g, '').trim());
    return { intent: parsed.intent || 'NEUTRAL', notes: parsed.notes || '', sentiment: parsed.sentiment || 'neutral' };
  } catch (err) {
    console.warn('[INBOUND] classify failed:', err.message);
    return { intent: 'NEUTRAL', notes: '', sentiment: 'neutral' };
  }
}

// Close a conversation: update stage in Supabase, generate summary, notify Dom
async function closeConversation({ contact, sb, outcome, newStage, summary }) {
  if (!contact) return;

  // Update Supabase stage
  try { await sb.from('contacts').update({ pipeline_stage: newStage }).eq('id', contact.id); } catch { /* non-fatal */ }

  // Generate a concise AI summary of the whole conversation thread
  let conversationSummary = summary;
  try {
    const { data: replies } = await sb.from('replies')
      .select('body, received_at, classification')
      .eq('contact_id', contact.id)
      .order('received_at', { ascending: true });

    if (replies?.length) {
      const thread = replies.map(r => `[${r.classification}] ${r.body}`).join('\n\n---\n\n');
      conversationSummary = await aiComplete(
        `Summarise this fundraising conversation with ${contact.name} in 3–5 sentences. ` +
        `State the outcome clearly (${outcome}), key points raised, and any action items for Dom.\n\n${thread.slice(0, 2000)}`,
        { reasoning: 'low', maxTokens: 200, task: 'conversation_summary' }
      ).catch(() => summary);
    }
  } catch { /* use fallback summary */ }

  // Notify Dom
  await sendTelegram(
    `🔚 *Conversation Closed — ${outcome}*\n\n` +
    `Contact: ${contact.name}\n\n` +
    `Summary:\n${conversationSummary}\n\n` +
    `Stage updated to: ${newStage}`
  ).catch(() => {});

  pushActivity({
    type: 'linkedin',
    action: `Conversation closed: ${contact.name} — ${outcome}`,
    note: conversationSummary.substring(0, 150),
  });

  if (contact.deal_id && contact.company_name && ['Not Interested', 'Suppressed — Opt Out'].includes(newStage)) {
    try {
      await sb.from('contacts')
        .update({ pipeline_stage: 'Inactive', response_received: true, follow_up_due_at: null })
        .eq('deal_id', contact.deal_id)
        .eq('company_name', contact.company_name)
        .neq('id', contact.id);
      await sb.from('batch_firms')
        .update({ status: 'suppressed' })
        .eq('deal_id', contact.deal_id)
        .ilike('firm_name', contact.company_name);
    } catch {}
  }

  console.log(`[CLOSE] ${contact.name} → ${newStage}. Summary logged to Supabase activity.`);
}

async function fetchDealAssets(dealId) {
  if (!dealId) return [];
  try {
    const sb = getSupabase();
    if (!sb) return [];
    const { data } = await sb.from('deal_assets').select('*').eq('deal_id', dealId).order('created_at', { ascending: true });
    return data || [];
  } catch { return []; }
}

function buildAssetContext(assets, intent) {
  if (!assets.length) return '';
  // For meeting requests: prioritise Calendly. For more info: prioritise deck then others.
  const relevant = intent === 'MEETING_REQUEST'
    ? assets.filter(a => a.asset_type === 'calendly').concat(assets.filter(a => a.asset_type !== 'calendly'))
    : assets.filter(a => a.asset_type === 'deck').concat(assets.filter(a => a.asset_type !== 'deck'));

  return relevant.slice(0, 4).map(a => {
    const label = { calendly: 'Booking link', deck: 'Pitch deck', image: 'Image', video: 'Video', link: 'Link', other: 'Resource' }[a.asset_type] || 'Resource';
    return `- ${a.name} (${label}): ${a.url}${a.description ? ` — ${a.description}` : ''}`;
  }).join('\n');
}

async function draftInstantReply({ contact, originalEmail, inboundBody, classification, deal, sb }) {
  const contactName = contact?.name || contact?.email || 'Investor';
  const dealId = originalEmail?.deal_id || deal?.id || contact?.deal_id;

  // 1 — Fetch deal fresh for full context
  let freshDeal = deal;
  if (dealId && sb) {
    const { data } = await sb.from('deals').select('*').eq('id', dealId).single();
    if (data) freshDeal = data;
  }

  // 2 — Fetch firm research context
  let firmContext = '';
  if (sb && contact?.company_name && dealId) {
    const { data: firms } = await sb.from('firms')
      .select('firm_name, firm_type, aum, focus_areas, geography, notes')
      .eq('deal_id', dealId)
      .ilike('firm_name', `%${contact.company_name}%`)
      .limit(1);
    const firm = firms?.[0];
    if (firm) {
      firmContext =
        `\nFirm research on ${firm.firm_name}:\n` +
        `Type: ${firm.firm_type || 'Unknown'} | AUM: ${firm.aum || 'Unknown'} | Geography: ${firm.geography || 'Unknown'}\n` +
        `Focus: ${(firm.focus_areas || '').substring(0, 200)}\n` +
        `Notes: ${(firm.notes || '').substring(0, 300)}\n`;
    }
  }

  // 3 — Fetch full conversation thread (sent emails + received replies, chronological)
  let threadTranscript = '';
  if (sb && contact?.id) {
    const [{ data: sentEmails }, { data: receivedReplies }] = await Promise.all([
      sb.from('emails').select('body, subject_used, sent_at, stage').eq('contact_id', contact.id).order('sent_at', { ascending: true }),
      sb.from('replies').select('body, received_at, classification').eq('contact_id', contact.id).order('received_at', { ascending: true }),
    ]);
    const events = [
      ...(sentEmails || []).map(e => ({ ts: e.sent_at, role: 'Dom', text: e.body })),
      ...(receivedReplies || []).map(r => ({ ts: r.received_at, role: contactName, text: r.body })),
    ].sort((a, b) => new Date(a.ts) - new Date(b.ts));
    if (events.length) {
      threadTranscript = '\n\nFULL CONVERSATION THREAD:\n' +
        events.map(e => `[${e.role}]: ${(e.text || '').substring(0, 400)}`).join('\n\n---\n\n');
    }
  }

  // 4 — Deal assets (only if available AND intent requires them)
  const assets = await fetchDealAssets(dealId);
  const hasCalendly = assets.some(a => a.asset_type === 'calendly');
  const hasDeck     = assets.some(a => a.asset_type === 'deck');
  const needsMeeting = ['MEETING_REQUEST'].includes(classification.intent);
  const needsInfo    = ['WANTS_MORE_INFO'].includes(classification.intent);

  // Asset context: only inject real URLs. Never inject placeholders.
  let assetInstruction = '';
  if (needsMeeting && hasCalendly) {
    const link = assets.find(a => a.asset_type === 'calendly');
    assetInstruction = `\nBooking link available — share this URL directly in your reply: ${link.url}\n`;
  } else if (needsInfo && hasDeck) {
    const link = assets.find(a => a.asset_type === 'deck');
    assetInstruction = `\nPitch deck available — share this URL directly in your reply: ${link.url}\n`;
  }

  // Dom action note for Telegram (what Dom needs to do manually if no asset)
  let domNote = '';
  if (needsMeeting && !hasCalendly) domNote = '📅 *Action needed:* Book this person in your calendar — no Calendly link set up yet.';
  if (needsInfo && !hasDeck) domNote = '📄 *Action needed:* Send them your deck — no deck asset uploaded yet.';

  // Load guidance and inject at top of prompt
  const guidanceBlock = await buildGuidanceBlock('investor_outreach').catch(() => '');

  const prompt =
    `${guidanceBlock}You are Dom, a fundraising professional. Write an instant, warm reply to this investor message.\n\n` +
    `DEAL: ${freshDeal?.name || 'our current deal'}\n` +
    `Description: ${(freshDeal?.description || '').substring(0, 300)}\n` +
    `Sector: ${freshDeal?.sector || ''} | Geography: ${freshDeal?.geography || 'UK'} | Raise: £${Number(freshDeal?.target_amount || 0).toLocaleString()}\n` +
    firmContext +
    `\nINVESTOR: ${contactName} | Company: ${contact?.company_name || ''} | Title: ${contact?.job_title || ''}\n` +
    `Their latest message:\n"""\n${inboundBody.slice(0, 600)}\n"""\n` +
    `Intent: ${classification.intent} | Sentiment: ${classification.sentiment}\n` +
    threadTranscript +
    assetInstruction +
    `\nRULES — follow these strictly:\n` +
    `- NEVER use placeholders like [link], [name], [calendar], [CALENDLY_LINK], etc. If you don't have a URL, don't reference it.\n` +
    `- If they want to meet and no booking link is provided above: ask for their email so Dom can get them booked in.\n` +
    `- If they want more info and no deck link is provided above: say Dom will get the relevant info sent over to them.\n` +
    `- Write as if Dom typed this himself — natural, warm, brief (3–5 sentences max).\n` +
    `- Continue naturally from the thread. Do NOT re-introduce yourself if you've already spoken.\n` +
    `- Do NOT use corporate jargon. Sign off as Dom.\n` +
    `Return ONLY the message body. No subject line. No markdown.`;

  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key':         process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Type':      'application/json',
      },
      body: JSON.stringify({
        model:      'claude-sonnet-4-6',
        max_tokens: 500,
        messages:   [{ role: 'user', content: prompt }],
      }),
    });
    if (!res.ok) {
      const t = await res.text();
      throw new Error(`Sonnet ${res.status}: ${t.substring(0, 200)}`);
    }
    const data = await res.json();
    const text = data.content?.[0]?.text || '';
    console.log(`[INBOUND] Sonnet drafted instant reply for ${contactName}`);
    return { body: text.trim(), domNote };
  } catch (err) {
    console.warn('[INBOUND] Sonnet reply draft failed:', err.message);
    return { body: '', domNote: '' };
  }
}

async function handleInboundReply({ fromEmail, fromName, fromUrn, subject, bodyText: rawBodyText, threadId, messageId, channel, threadField, emailAccountId }) {
  // Strip HTML tags so Telegram notifications and activity feed only show plain text.
  const bodyText = stripHtml(rawBodyText);
  if (!bodyText && !fromEmail && !fromUrn) return;
  const normalizedEmail = normalizeInboundEmail(fromEmail);
  if (shouldSuppressInboundWebhookMessage({
    fromName,
    fromUrn,
    bodyText,
    payload: {
      from_email: normalizedEmail,
      sender_id: fromUrn,
      sender_name: fromName,
    },
  })) {
    return;
  }
  console.log(`[INBOUND/${channel.toUpperCase()}] Reply from ${normalizedEmail || fromName} on thread ${threadId}`);

  const sb = getSupabase();

  // Find contact — try email first, then LinkedIn chat_id, then provider_id
  let contact = null;
  if (sb) {
    if (normalizedEmail) {
      contact = await findInvestorContactByEmail(sb, normalizedEmail, { requireActiveDeal: true });
    }
    if (!contact && threadId) {
      contact = await findInvestorContactByEmailThread(sb, threadId, { requireActiveDeal: true });
    }
    if (!contact && fromName && fromName.trim().split(/\s+/).length >= 2) {
      contact = await findInvestorContactByDisplayName(sb, fromName, { requireActiveDeal: true });
    }
    if (!contact && fromUrn) {
      // Fall back to LinkedIn provider_id
      const { data } = await sb.from('contacts').select('*, deals(*)').eq('linkedin_provider_id', fromUrn).limit(1).maybeSingle();
      contact = data || null;
    }
  }

  // Find original email by thread_id
  let originalEmail = null;
  if (sb && threadId) {
    originalEmail = await findOriginalEmailByThread(sb, threadId);
  }

  // Log the inbound reply
  let replyRecord = null;
  if (sb) {
    const { data } = await sb.from('replies').insert({
      body:           bodyText,
      thread_id:      threadId || null,
      message_id:     messageId || null,
      contact_id:     contact?.id || null,
      deal_id:        originalEmail?.deal_id || contact?.deal_id || null,
      channel,
      received_at:    new Date().toISOString(),
      classification: 'PENDING',
    }).select().single();
    replyRecord = data || null;
  }

  // Update contact to "In Conversation" as soon as they reply
  if (contact && sb) {
    await sb.from('contacts').update({
      pipeline_stage:    'In Conversation',
      response_received: true,
      last_reply_at:     new Date().toISOString(),
      follow_up_due_at:  null,
    }).eq('id', contact.id);
  }

  let deal = originalEmail?.deals || contact?.deals || null;
  if (!deal && (originalEmail?.deal_id || contact?.deal_id) && sb) {
    try {
      const { data } = await sb.from('deals').select('*').eq('id', originalEmail?.deal_id || contact?.deal_id).maybeSingle();
      deal = data || null;
    } catch {}
  }
  const classification = await classifyWithGpt({
    body: bodyText, fromEmail, fromName,
    dealName: deal?.name || 'Unknown Deal',
    dealDescription: deal?.description || '',
  });

  if (sb && replyRecord) {
    await sb.from('replies').update({
      classification:       classification.intent,
      classification_notes: classification.notes,
      sentiment:            classification.sentiment,
    }).eq('id', replyRecord.id);
  }

  console.log(`[INBOUND/${channel.toUpperCase()}] Classified as: ${classification.intent}`);

  if (!contact?.id) {
    console.log(`[INBOUND/${channel.toUpperCase()}] Inbound ignored because no active contact context was resolved`);
    return;
  }

  if (classification.intent === 'AUTO_REPLY') {
    return;
  }

  if (['INTERESTED', 'WANTS_MORE_INFO', 'MEETING_REQUEST', 'POSITIVE'].includes(classification.intent)) {
    const replyContact = contact || { email: fromEmail, name: fromName };
    const { body: draftBody, domNote } = await draftInstantReply({
      contact: replyContact, originalEmail, inboundBody: bodyText, classification, deal, sb,
    });

    if (draftBody) {
      const replySubject = `Re: ${subject || originalEmail?.subject_used || originalEmail?.subject_a || 'our conversation'}`;
      const activeDealId = originalEmail?.deal_id || deal?.id || contact?.deal_id || null;
      if (!contact?.id || !activeDealId) {
        console.log(`[INBOUND/${channel.toUpperCase()}] Positive-looking inbound ignored because no active deal/contact context was resolved`);
        return;
      }

      const activeDeal = await getDeal(activeDealId).catch(() => null);
      if (activeDeal && String(activeDeal.status || '').toUpperCase() !== 'ACTIVE') {
        pushActivity({
          type: 'system',
          action: `Inbound reply ignored for inactive deal: ${replyContact.name || fromName}`,
          note: `${channel} reply matched deal ${activeDeal.name || activeDealId}, but deal is ${activeDeal.status || 'inactive'}`,
          dealId: activeDealId,
        });
        return;
      }

      const { data: queued } = await sb?.from('approval_queue').insert({
        deal_id:            activeDealId,
        deal_name:          activeDeal?.name || deal?.name || null,
        contact_id:         contact.id,
        candidate_id:       contact.id,
        contact_name:       contact.name || replyContact.name || fromName || '',
        contact_email:      contact.email || replyContact.email || fromEmail || null,
        firm:               contact.company_name || '',
        message_type:       channel === 'linkedin' ? 'linkedin_reply' : 'email_reply',
        outreach_mode:      'investor_outreach',
        channel,
        stage:              channel === 'linkedin' ? 'LinkedIn Reply' : 'Email Reply',
        message_text:       draftBody,
        body:               draftBody,
        subject_a:          channel === 'email' ? replySubject : null,
        reply_to_id:        threadId || null,
        status:             'pending',
      }).select().single() || { data: null };

      if (queued?.id) {
        await sendReplyForApproval(
          queued.id,
          contact,
          draftBody,
          activeDeal?.name || deal?.name || 'Unknown',
          channel,
          threadId,
          emailAccountId,
          {
            inboundSubject: subject || originalEmail?.subject_used || originalEmail?.subject_a || null,
            inboundBody: bodyText,
            quotePreview: truncateInline(bodyText, 160),
            intent: classification.intent,
            sentiment: classification.sentiment,
          }
        ).catch(() => {});
      }

      pushActivity({
        type: 'approval',
        action: `Reply drafted for approval: ${replyContact.name || fromName}`,
        note: `${channel === 'linkedin' ? 'LinkedIn' : 'Email'} · ${classification.intent}`,
        full_content: bodyText,
        dealId: activeDealId,
      });

      console.log(`[INBOUND/${channel.toUpperCase()}] Reply queued for approval for ${replyContact.name || fromEmail}`);
    }
  } else if (classification.intent === 'NOT_INTERESTED') {
    // Conversation over — close it out
    await closeConversation({
      contact, sb, outcome: 'Not Interested',
      newStage: 'Not Interested',
      summary: `${fromName || 'Contact'} replied indicating they are not interested. Message: "${bodyText.substring(0, 300)}"`,
    });
  } else if (classification.intent === 'CONVERSATION_END') {
    await closeConversation({
      contact, sb, outcome: 'Conversation Ended',
      newStage: 'Not Interested',
      summary: `${fromName || 'Contact'} wrapped up the conversation. Message: "${bodyText.substring(0, 300)}"`,
    });
  } else if (classification.intent === 'OPT_OUT') {
    // Hard opt-out — suppress and log
    await closeConversation({
      contact, sb, outcome: 'Opt Out',
      newStage: 'Suppressed — Opt Out',
      summary: `${fromName || 'Contact'} requested to opt out. Message: "${bodyText.substring(0, 300)}"`,
    });
  }

  broadcastToAll({
    type: 'REPLY_RECEIVED',
    reply: { from: fromName || fromEmail, subject, classification: classification.intent, thread_id: threadId, channel },
  });

  pushActivity({
    type: 'reply',
    action: `${fromName || fromEmail || 'Contact'} replied`,
    note: `${channel === 'linkedin' ? 'LinkedIn' : 'Email'} · ${classification.intent} · "${bodyText.substring(0, 80)}"`,
    dealId: originalEmail?.deal_id || contact?.deal_id,
  });

  await sendTelegram(
    `📩 *${channel === 'linkedin' ? 'LinkedIn Message' : 'Email Reply'} Received*\n\n` +
    `From: ${fromName || fromEmail}\n` +
    `Intent: ${classification.intent} | Sentiment: ${classification.sentiment}\n\n` +
    `${bodyText.substring(0, 300)}${bodyText.length > 300 ? '...' : ''}`
  );
}

async function handleInboundLinkedInMessage({ fromUrn, fromName, bodyText, chatId, messageId }) {
  await handleInboundReply({
    fromEmail:   '',
    fromName:    fromName || fromUrn,
    fromUrn:     fromUrn || '',
    subject:     'LinkedIn message',
    bodyText,
    threadId:    chatId,
    messageId,
    channel:     'linkedin',
    threadField: null,
  });
}

function extractResearchSummary(notes) {
  if (!notes.includes('[RESEARCHED:')) return null;
  return {
    comparableDeals: (notes.match(/Comparable Deals: (.+)/)?.[1] || '').split(', ').filter(Boolean),
    investmentCriteria: notes.match(/Criteria: (.+)/)?.[1] || '',
    approachAngle: notes.match(/Approach: (.+)/)?.[1] || '',
    confidenceScore: parseInt(notes.match(/Confidence: (\d+)/)?.[1] || '0'),
  };
}
