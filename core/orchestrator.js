/**
 * core/orchestrator.js
 * Supabase-native pipeline orchestrator.
 * Phases (per deal per cycle): phaseRank → phaseArchive → phaseEnrich → phaseOutreach → phaseFollowUps
 * Research is triggered immediately on deal creation (see dashboard/server.js).
 */

import fs from 'fs';
import { getSupabase } from './supabase.js';
import { loadState, saveState } from './state.js';
import { getActiveDeals, logActivity as sbLogActivity, loadSessionState, saveSessionState } from './supabaseSync.js';
import { DateTime } from 'luxon';
import {
  getConversationHistory,
  logConversationMessage,
  setConversationState,
  checkTempClosedContacts,
  draftTempCloseFollowUp,
} from './conversationManager.js';
import { isWithinSendingWindow, isGloballyPaused, getNextWindowOpen, isWithinChannelWindow } from './scheduleChecker.js';
import { isWithinEmailWindow, describeNextEmailWindow, isActiveOutreachDay } from './sendingWindow.js';
import { rankInvestor } from '../research/investorRanker.js';
import { enrichWithKaspr } from '../enrichment/kaspEnricher.js';
import { enrichWithApify } from '../enrichment/apifyEnricher.js';
import { findLinkedInUrl } from '../enrichment/linkedinFinder.js';
import {
  sendLinkedInDM,
  sendEmail as unipileSendEmail,
  listSentInvitations,
  canonicalizeLinkedInProfileUrl,
  retrieveLinkedInProfile,
} from '../integrations/unipileClient.js';
import { enrichFirmViaLinkedIn, processLinkedInInvite } from './unipile.js';
import { sendEmailForApproval, sendLinkedInDMForApproval, sendTelegram, sendTelegramVoiceNote } from '../approval/telegramBot.js';
import { draftEmail } from '../outreach/emailDrafter.js';
import { draftLinkedInDM } from '../outreach/linkedinDrafter.js';
import { isExcluded } from './exclusionCheck.js';
import {
  researchPerson,
  isResearched,
  hasCoreResearchFields,
  hasFreshResearch,
} from '../research/personResearcher.js';
import { researchFirmOnly, findDecisionMakers } from '../research/firmResearcher.js';
import { runDealResearch } from '../research/dealResearcher.js'; // legacy fallback
import { queryInvestorDatabase, batchScoreInvestors as batchScoreInvestors } from './investorDatabaseQuery.js';
import { haikuComplete } from './aiClient.js';
import { pushActivity, notifyQueueUpdated, queueLinkedInDmApproval, sendApprovedLinkedInDM, sendApprovedReply } from '../dashboard/server.js';
import { info, warn, error } from './logger.js';
import { ORCHESTRATOR_INTERVAL_MS } from '../config/constants.js';
import { runFundraiserReasoning, gatherCurrentMetrics } from './fundraiserBrain.js';
import { writeMemory } from './rocoMemory.js';
import { searchInvestorsWithGrok, scanInvestorNewsForDeal, saveDealNewsLeads, scanGeneralInvestorSignals, storeGeneralInvestorSignals, buildDealNewsScan, summarizePortfolioNewsDigest, scrapePublicInvestorDirectories } from './newsScanner.js';
import { buildDailyActivityReport, persistDailyActivityReport, renderDailyVoiceNoteFromText } from './analyticsEngine.js';

export const rocoState = {
  status: 'ACTIVE',
  deal: null,
  emailsSent: 0,
  startedAt: new Date().toISOString(),
};

/** Strip em-dashes (and similar fancy punctuation) from outreach text. */
function sanitizeOutreach(text) {
  if (!text) return text;
  return text
    .replace(/\u2014/g, '-')   // em-dash —
    .replace(/\u2013/g, '-')   // en-dash –
    .replace(/\u2018|\u2019/g, "'")  // curly single quotes
    .replace(/\u201C|\u201D/g, '"'); // curly double quotes
}

function hasUsableEmail(email) {
  const value = String(email || '').trim();
  return !!value && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

async function persistOutboundEmailRecord({ sb, deal, contact, subject, result, stage, status = 'sent', errorMessage = null }) {
  if (!sb || !deal?.id || !contact?.id || !hasUsableEmail(contact?.email)) return;
  try {
    await sb.from('emails').insert({
      deal_id: deal.id,
      contact_id: contact.id,
      to_email: contact.email,
      subject: subject || null,
      status,
      direction: 'outbound',
      sent_at: status === 'sent' ? new Date().toISOString() : null,
      error_message: errorMessage || null,
      provider_id: result?.providerId || null,
      thread_id: result?.threadId || null,
      message_id: result?.messageId || result?.emailId || null,
      metadata: {
        stage: stage || null,
        account_id: result?.accountId || null,
        channel: 'email',
        tracking_label: result?.trackingLabel || null,
        opens_count: 0,
        clicks_count: 0,
      },
    });
  } catch {}
}

function isLinkedInStageLabel(stage) {
  const value = String(stage || '').trim().toLowerCase();
  return value === 'linkedin dm'
    || value === 'linkedin_dm'
    || value === 'linkedin_follow_up'
    || value.startsWith('linkedin follow-up')
    || value.startsWith('linkedin follow up');
}

function normalizeFirmIdentity(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\b(inc|llc|ltd|lp|llp|plc|corp|corporation|partners|partner|capital|holdings|group|ventures|management|advisors)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenizeContactName(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .split(' ')
    .filter(Boolean);
}

function parseContactIdentityName(name) {
  const tokens = tokenizeContactName(name);
  if (!tokens.length) return null;
  const suffixes = new Set(['phd', 'md']);
  const parts = tokens.filter(part => !suffixes.has(part));
  if (!parts.length) return null;
  const first = parts[0] || '';
  const last = parts.length >= 2 ? parts[parts.length - 1] : '';
  const middle = parts.slice(1, -1);
  return {
    first,
    last,
    middleInitials: middle.map(part => part[0]).join(''),
    hasNamedMiddle: middle.some(part => part.length > 1),
    normalized: [first, last].filter(Boolean).join(' '),
    tokens: parts,
  };
}

function arePrefixNameVariants(leftName, rightName) {
  const left = tokenizeContactName(leftName);
  const right = tokenizeContactName(rightName);
  if (left.length < 2 || right.length < 2) return false;
  if (left[0] !== right[0]) return false;

  const shorter = left.length <= right.length ? left : right;
  const longer = shorter === left ? right : left;
  return shorter.every((token, index) => longer[index] === token);
}

function areLikelySameNamedContact(leftName, rightName) {
  const left = parseContactIdentityName(leftName);
  const right = parseContactIdentityName(rightName);
  if (!left || !right) return false;
  if (left.first === right.first && left.last === right.last) {
    if (!left.middleInitials || !right.middleInitials) return true;
    if (left.middleInitials === right.middleInitials) return true;
    if (left.hasNamedMiddle && right.hasNamedMiddle) return false;
    return false;
  }
  return arePrefixNameVariants(leftName, rightName);
}

function normalizeLinkedInIdentity(url) {
  const raw = String(url || '').trim();
  if (!raw) return '';
  try {
    return canonicalizeLinkedInProfileUrl(raw).toLowerCase();
  } catch {
    return raw.toLowerCase();
  }
}

const LINKEDIN_NO_MATCH_NOTE_PATTERN = /\[LI_NO_MATCH:checked_at=([^\]|]+)(?:\|reason=([^\]]+))?\]/i;

function hasRecentLinkedInNoMatchSuppression(notes, hours = 72) {
  const match = String(notes || '').match(LINKEDIN_NO_MATCH_NOTE_PATTERN);
  if (!match?.[1]) return false;
  const checkedAt = new Date(match[1]).getTime();
  return Number.isFinite(checkedAt) && (Date.now() - checkedAt) < (hours * 60 * 60 * 1000);
}

function buildContactIdentityKey(contact, firmName = '') {
  const providerId = String(contact?.linkedin_provider_id || '').trim().toLowerCase();
  if (providerId) return `provider:${providerId}`;
  const linkedinUrl = normalizeLinkedInIdentity(contact?.linkedin_url);
  if (linkedinUrl) return `linkedin:${linkedinUrl}`;
  const email = String(contact?.email || '').trim().toLowerCase();
  if (hasUsableEmail(email)) return `email:${email}`;
  const parsedName = parseContactIdentityName(contact?.name);
  if (!parsedName?.normalized) return '';
  const normalizedFirm = normalizeFirmIdentity(firmName || contact?.company_name || '');
  return `name:${parsedName.normalized}:${normalizedFirm}`;
}

function scoreContactMergeCandidate(contact) {
  let score = 0;
  if (hasUsableEmail(contact?.email)) score += 5;
  if (contact?.linkedin_provider_id) score += 4;
  if (contact?.linkedin_url) score += 3;
  if (contact?.job_title) score += 2;
  score += Math.min(String(contact?.name || '').trim().length / 100, 0.5);
  return score;
}

function mergeContactRecords(primary, incoming) {
  const primaryScore = scoreContactMergeCandidate(primary);
  const incomingScore = scoreContactMergeCandidate(incoming);
  const winner = incomingScore > primaryScore ? incoming : primary;
  const loser = winner === incoming ? primary : incoming;
  return {
    ...loser,
    ...winner,
    name: winner.name || loser.name || null,
    email: winner.email || loser.email || null,
    linkedin_url: winner.linkedin_url || loser.linkedin_url || null,
    linkedin_provider_id: winner.linkedin_provider_id || loser.linkedin_provider_id || null,
    job_title: winner.job_title || loser.job_title || null,
  };
}

function buildContactMergePatch(existing, incoming, firmName = '') {
  const merged = mergeContactRecords(
    { ...existing, company_name: existing.company_name || firmName || null },
    { ...incoming, company_name: incoming.company_name || firmName || null }
  );
  const patch = {};
  for (const field of ['name', 'company_name', 'email', 'linkedin_url', 'linkedin_provider_id', 'job_title']) {
    const nextValue = merged[field] ?? null;
    const currentValue = existing[field] ?? null;
    if (nextValue && nextValue !== currentValue) patch[field] = nextValue;
  }
  return patch;
}

function findMatchingExistingContact(existingContacts, incoming, firmName = '') {
  const incomingKey = buildContactIdentityKey(incoming, firmName);
  const normalizedFirm = normalizeFirmIdentity(firmName || incoming?.company_name || '');
  let best = null;
  let bestScore = -1;

  for (const existing of existingContacts || []) {
    const existingFirm = normalizeFirmIdentity(existing?.company_name || '');
    if (normalizedFirm && existingFirm && existingFirm !== normalizedFirm) continue;

    const existingKey = buildContactIdentityKey(existing, firmName || existing?.company_name || '');
    const exactIdentityMatch = !!incomingKey && incomingKey === existingKey;
    const sameNameVariant = areLikelySameNamedContact(existing?.name, incoming?.name);
    if (exactIdentityMatch && existing?.name && incoming?.name && !sameNameVariant) continue;
    if (!exactIdentityMatch && !sameNameVariant) continue;

    const score = scoreContactMergeCandidate(existing);
    if (score > bestScore) {
      best = existing;
      bestScore = score;
    }
  }

  return best;
}

const STALE_BATCH_FIRM_ENRICHMENT_MS = 20 * 60 * 1000;
const MAX_FIRM_ENRICHMENT_ATTEMPTS = 3;
const ACTIVE_MONITORING_STAGES = ['pending_email_approval', 'pending_dm_approval', 'Email Approved', 'DM Approved', 'Email Sent', 'DM Sent', 'email_sent', 'dm_sent', 'intro_sent', 'follow_up_sent', 'awaiting_response', 'temp_closed'];
const lastDealStatusActivity = new Map();
const dailyNewsScanState = new Map();
const dailyActivityDigestState = new Map();
const dailyLogRecommendationState = new Map();
const approvedBatchTopUpState = new Map();
const dealCycleLocks = new Set();
const reasoningTelegramState = new Map();
const decisionTelegramState = new Map();

// Contacts currently in the approval flow — prevents duplicate approval drafts per cycle
const contactsInFlight = new Set();

function normalizeActionLabel(value) {
  return String(value || '').toLowerCase().replace(/\s+/g, ' ').trim();
}

async function implementPendingDailyLogActions(deals = []) {
  const sb = getSupabase();
  if (!sb || !Array.isArray(deals) || !deals.length) return;

  const todayEt = DateTime.now().setZone('America/New_York').toISODate();
  if (!todayEt || dailyLogRecommendationState.get(todayEt)) return;

  const yesterdayEt = DateTime.now().setZone('America/New_York').minus({ days: 1 }).toISODate();
  const { data: rows, error } = await sb.from('daily_logs')
    .select('id, deal_id, deal_name, recommended_actions, actions_implemented')
    .eq('log_date', yesterdayEt)
    .eq('actions_implemented', false);
  if (error || !rows?.length) {
    dailyLogRecommendationState.set(todayEt, true);
    return;
  }

  const dealMap = new Map(deals.map(deal => [String(deal.id), deal]));

  for (const row of rows) {
    const deal = dealMap.get(String(row.deal_id || ''));
    if (!deal) continue;
    const actions = Array.isArray(row.recommended_actions) ? row.recommended_actions.filter(Boolean) : [];
    if (!actions.length) {
      await sb.from('daily_logs').update({ actions_implemented: true }).eq('id', row.id).then(null, () => {});
      continue;
    }

    for (const action of actions) {
      await implementRecommendedActionForDeal(deal, action).catch(err => {
        pushActivity({
          type: 'warning',
          action: 'Daily log recommendation failed',
          note: `${deal.name} · ${String(action || '').slice(0, 120)} · ${err.message?.slice(0, 100) || 'unknown error'}`,
          deal_name: deal.name,
          dealId: deal.id,
        });
      });
    }

    await sb.from('daily_logs').update({ actions_implemented: true }).eq('id', row.id).then(null, () => {});
  }

  dailyLogRecommendationState.set(todayEt, true);
}

async function implementRecommendedActionForDeal(deal, action) {
  const normalized = normalizeActionLabel(action);
  if (!normalized) return;

  if (normalized.startsWith('research ') || normalized.includes('top of funnel') || normalized.includes('expand linkedin search')) {
    await triggerAutoFeedForDeal(deal, { reason: 'daily_log_recommendation', requestedCount: 24 });
    return;
  }

  if (normalized.startsWith('follow up with')) {
    pushActivity({
      type: 'system',
      action: 'Daily log recommendation queued warm follow-ups',
      note: `${deal.name} · ${String(action).slice(0, 160)}`,
      deal_name: deal.name,
      dealId: deal.id,
    });
    return;
  }

  if (normalized.startsWith('clear ') && normalized.includes('pending approvals')) {
    pushActivity({
      type: 'system',
      action: 'Daily log recommendation noted approval backlog',
      note: `${deal.name} · ${String(action).slice(0, 160)}`,
      deal_name: deal.name,
      dealId: deal.id,
    });
    return;
  }

  pushActivity({
    type: 'system',
    action: 'Daily log recommendation acknowledged',
    note: `${deal.name} · ${String(action).slice(0, 160)}`,
    deal_name: deal.name,
    dealId: deal.id,
  });
}

/** Remove all contacts belonging to a deal from the in-flight set on deal close. */
export function clearDealFromFlight(dealId) {
  // contactsInFlight stores contact IDs (not deal IDs), so we can't filter by deal directly.
  // The safe approach: we accept a small window where a cycle might skip the check —
  // since getActiveDeals() excludes CLOSED deals, the orchestrator won't pick up the deal
  // in the next cycle regardless. This is a best-effort cleanup for the current cycle.
  // (If needed in future, store { contactId, dealId } objects instead of bare IDs.)
  info(`[CLOSE] Deal ${dealId} closed — orchestrator will skip it from next cycle`);
}

// Generic labels that describe a role, not a real firm — exclude from firm-level suppression
const GENERIC_FIRM_NAMES = new Set([
  'angel investor', 'angel investors', 'independent investor', 'independent',
  'self-employed', 'self employed', 'freelance', 'freelancer', 'consultant',
  'private investor', 'individual investor', 'personal investment',
  'n/a', 'na', 'none', 'unknown',
]);

function normalizeBatchFirmEnrichmentStatus(status) {
  const value = String(status || '').trim().toLowerCase();
  if (!value) return 'pending';
  if (['pending', 'in_progress', 'complete', 'failed'].includes(value)) return value;
  if (['approved for outreach', 'ranked'].includes(value)) return 'pending';
  return value;
}

function parseBatchFirmEnrichmentMeta(firm) {
  const raw = String(firm?.status_reason || '').trim();
  if (!raw.startsWith('{')) return {};
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

function buildBatchFirmEnrichmentMeta(firm, patch = {}) {
  return {
    ...parseBatchFirmEnrichmentMeta(firm),
    ...patch,
  };
}

function stringifyBatchFirmEnrichmentMeta(meta) {
  try {
    return JSON.stringify(meta);
  } catch {
    return null;
  }
}

function pushDealStatusOnce(deal, statusKey, entry) {
  if (!deal?.id || !statusKey) {
    pushActivity(entry);
    return;
  }
  const previous = lastDealStatusActivity.get(String(deal.id));
  if (previous === statusKey) return;
  lastDealStatusActivity.set(String(deal.id), statusKey);
  pushActivity(entry);
}

async function sendDecisionTelegramOnce(deal, decisionKey, text) {
  if (!deal?.id || !decisionKey || !text) return;
  const key = `${deal.id}:${decisionKey}`;
  if (decisionTelegramState.get(key)) return;
  decisionTelegramState.set(key, true);
  await sendTelegram(text).catch(() => {
    decisionTelegramState.delete(key);
  });
}

function describeWindowState(deal, state) {
  const globallyPaused = state.outreach_paused_until && isGloballyPaused(state.outreach_paused_until);
  if (globallyPaused) {
    const pausedUntil = new Date(state.outreach_paused_until).toLocaleString('en-GB');
    return `outreach paused until ${pausedUntil}`;
  }

  const emailOpen = isWithinEmailWindow(deal);
  const inviteOpen = isWithinChannelWindow(deal, 'linkedin_invite');
  const dmOpen = isWithinChannelWindow(deal, 'linkedin_dm');

  const parts = [
    emailOpen ? 'email window open' : `email window closed (${describeNextEmailWindow(deal)})`,
    inviteOpen ? 'LinkedIn invites open' : 'LinkedIn invites closed',
    dmOpen ? 'LinkedIn DMs open' : 'LinkedIn DMs closed',
  ];

  return parts.join(' · ');
}

async function logDealIdleStatus(deal, batch, state) {
  const sb = getSupabase();
  if (!sb || !deal?.id || !batch?.id) return;

  let batchFirms = [];
  try {
    const { data } = await sb.from('batch_firms')
      .select('enrichment_status')
      .eq('batch_id', batch.id);
    batchFirms = data || [];
  } catch {
    return;
  }

  const remainingFirms = batchFirms.filter(firm =>
    normalizeBatchFirmEnrichmentStatus(firm.enrichment_status) !== 'complete'
  ).length;

  if (remainingFirms > 0) return;

  const [{ count: pendingApprovals }, { count: pendingInvites }, { count: pendingLinkedInDms }, { count: activeConversations }] = await Promise.all([
    sb.from('approval_queue').select('id', { count: 'exact', head: true }).eq('deal_id', deal.id).eq('status', 'pending'),
    sb.from('contacts').select('id', { count: 'exact', head: true }).eq('deal_id', deal.id).not('invite_sent_at', 'is', null).is('invite_accepted_at', null),
    sb.from('contacts').select('id', { count: 'exact', head: true }).eq('deal_id', deal.id).eq('pipeline_stage', 'invite_accepted'),
    sb.from('contacts').select('id', { count: 'exact', head: true }).eq('deal_id', deal.id).in('pipeline_stage', ACTIVE_MONITORING_STAGES),
  ]);

  const monitoring = [];
  if (pendingApprovals) monitoring.push(`${pendingApprovals} awaiting approval`);
  if (pendingInvites) monitoring.push(`${pendingInvites} pending LinkedIn acceptance`);
  if (pendingLinkedInDms) monitoring.push(`${pendingLinkedInDms} accepted LinkedIn contact${pendingLinkedInDms === 1 ? '' : 's'} waiting for DM`);
  if (activeConversations) monitoring.push(`${activeConversations} active conversation${activeConversations === 1 ? '' : 's'}`);
  if (!monitoring.length) monitoring.push('no pending outreach tasks');

  const windowState = describeWindowState(deal, state);
  const action = monitoring.length === 1 && monitoring[0] === 'no pending outreach tasks'
    ? 'Idle — monitoring for replies and LinkedIn acceptances'
    : 'Monitoring outreach state';
  const note = `${deal.name} · Batch ${batch.batch_number} · ${windowState} · ${monitoring.join(' · ')}`;
  const statusKey = [
    batch.id,
    batch.batch_number,
    windowState,
    pendingApprovals || 0,
    pendingInvites || 0,
    pendingLinkedInDms || 0,
    activeConversations || 0,
  ].join('|');

  pushDealStatusOnce(deal, statusKey, {
    type: 'system',
    action,
    note,
    deal_name: deal.name,
    dealId: deal.id,
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// STARTUP + LOOP
// ─────────────────────────────────────────────────────────────────────────────

export async function startOrchestrator() {
  console.log('[ORCHESTRATOR] ================================');
  console.log('[ORCHESTRATOR] Starting (Supabase-native)...');

  try {
    const deals = await getActiveDeals();
    console.log(`[ORCHESTRATOR] Active deals: ${deals.length}`);
    deals.forEach(d => console.log(`  - ${d.name} (${d.status})`));
    if (deals.length === 0) {
      console.warn('[ORCHESTRATOR] No active deals — waiting for deal creation via Mission Control');
    }
  } catch (err) {
    console.error('[ORCHESTRATOR] Could not load active deals on startup:', err.message);
  }

  // Heartbeat every 5 min
  setInterval(() => {
    const now = new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' });
    console.log(`[ORCHESTRATOR] Heartbeat — ${now} — status: ${rocoState.status}`);
  }, 5 * 60 * 1000);

  // One-time backfill: copy past_investments + person_researched from investors_db → contacts
  backfillContactsFromInvestorsDb().catch(e =>
    console.warn('[BACKFILL] past_investments backfill failed:', e.message)
  );

  console.log('[ORCHESTRATOR] ================================');
  runLoop();
}

/**
 * Trigger an immediate orchestrator cycle for a specific deal.
 * Called by dashboard/server.js after a new deal is created.
 */
export async function triggerImmediateRun(dealId) {
  console.log(`[ORCHESTRATOR] Immediate run triggered for deal: ${dealId}`);
  try {
    const deals = await getActiveDeals();
    const deal = dealId ? deals.find(d => String(d.id) === String(dealId)) : deals[0];
    if (!deal) {
      warn(`[ORCHESTRATOR] Immediate run: deal ${dealId} not found`);
      return;
    }
    const state = await loadState();
    await runDealCycle(deal, state);
    console.log(`[ORCHESTRATOR] Immediate run complete for: ${deal.name}`);
  } catch (err) {
    error('[ORCHESTRATOR] Immediate run error', { err: err.message });
  }
}

async function runLoop() {
  while (true) {
    try {
      const state = await loadState();
      if (state.roco_status === 'ACTIVE') {
        await runCycle(state);
      } else {
        info(`Orchestrator ${state.roco_status} — waiting...`);
      }
    } catch (err) {
      error('Orchestrator cycle threw unexpectedly', { err: err.message, stack: err.stack });
    }
    await sleep(ORCHESTRATOR_INTERVAL_MS);
  }
}

async function runCycle(state) {
  info('--- Orchestrator cycle starting ---');

  if (state.outreach_paused_until && isGloballyPaused(state.outreach_paused_until)) {
    const next = new Date(state.outreach_paused_until).toLocaleString('en-GB');
    info(`Outreach globally paused until ${next}`);
  }

  // Phase A: Investor outreach deals (existing)
  try {
    const deals = await getActiveDeals();
    await implementPendingDailyLogActions(deals);
    await runDailyNewsScanCycle(deals);
    await runDailyActivityDigestCycle(deals);

    if (deals.length > 0) {
      const ordered = [...deals].sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
      info(`Processing ${ordered.length} deal(s) sequentially: ${ordered.map(d => d.name).join(' → ')}`);

      for (const deal of ordered) {
        try {
          await runDealCycle(deal, state);
        } catch (err) {
          error(`Deal cycle failed: ${deal.name}`, { err: err.message });
        }
        if (ordered.indexOf(deal) < ordered.length - 1) await sleep(2000);
      }
    } else {
      info('No active investor deals — checking sourcing campaigns');
    }
  } catch (err) {
    error('Orchestrator investor cycle error', { err: err.message });
  }

  // Phase B: Company sourcing campaigns (new — runs independently of investor deals)
  try {
    const sb = getSupabase();
    if (sb) {
      const { data: activeCampaigns } = await sb.from('sourcing_campaigns')
        .select('*')
        .eq('status', 'active')
        .order('created_at', { ascending: true });

      if (activeCampaigns?.length) {
        info(`Processing ${activeCampaigns.length} sourcing campaign(s): ${activeCampaigns.map(c => c.name).join(' → ')}`);
        const { runCompanySourcingCycle } = await import('../sourcing/sourcingOrchestrator.js');

        for (const campaign of activeCampaigns) {
          try {
            await runCompanySourcingCycle(campaign);
          } catch (err) {
            error(`Sourcing cycle failed: ${campaign.name}`, { err: err.message });
          }
          await sleep(2000);
        }
      }
    }
  } catch (err) {
    error('Orchestrator sourcing cycle error', { err: err.message });
  }

  // Weekly intelligence (non-blocking) — runs autonomously Monday 9am EST
  checkMondayIntelligence().catch(err =>
    console.error('[WEEKLY INTEL] Check failed:', err.message)
  );

  info('--- Orchestrator cycle complete ---');
}

async function checkMondayIntelligence() {
  const now = new Date();
  const estNow = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const sb = getSupabase();
  if (!sb) return;

  // ── CATCH-UP: generate any past weeks stuck in 'pending' ─────────────────
  // These accumulate when: (a) a Monday run failed, or (b) pre-created placeholders
  // that were never generated because the server was down during the window.
  // Run on every cycle (not just Monday) so missed reports always self-heal.
  const todayISO = estNow.toISOString().split('T')[0];

  // Reset any 'generating' records for past weeks — these are zombie runs where
  // the process crashed mid-generation. Current week can legitimately be 'generating'
  // (active run), but a past week's 'generating' is definitely stuck.
  try {
    await sb
      .from('weekly_intelligence')
      .update({ status: 'failed', raw_recommendations: { error: 'Zombie generating reset', step: 'watchdog' } })
      .eq('status', 'generating')
      .lt('week_end', todayISO);
  } catch { /* non-fatal */ }

  let stuckWeeks = null;
  try {
    const { data } = await sb
      .from('weekly_intelligence')
      .select('id, week_start, week_end, week_number, status')
      .in('status', ['pending', 'failed'])  // retry both stuck and failed
      .lt('week_end', todayISO)             // only weeks that have fully ended
      .order('week_start', { ascending: true })
      .limit(3);
    stuckWeeks = data;
  } catch { /* non-fatal */ }

  if (stuckWeeks?.length) {
    console.log(`[WEEKLY INTEL] Catch-up: ${stuckWeeks.length} past week(s) need generating (statuses: ${stuckWeeks.map(w => w.status).join(', ')})`);
    for (const week of stuckWeeks) {
      await runWeeklyIntelligence(week.week_start, week.week_end, week.week_number, sb);
    }
  }

  // ── SCHEDULED: Monday 9am EST — generate last week's report ──────────────
  const isMonday = estNow.getDay() === 1;
  const isNineAm = estNow.getHours() === 9 && estNow.getMinutes() < 10;

  if (!isMonday || !isNineAm) return;

  // Calculate last week (Mon to Sun)
  const lastMonday = new Date(estNow);
  lastMonday.setDate(estNow.getDate() - 7);
  const weekStart = lastMonday.toISOString().split('T')[0];

  const lastSunday = new Date(estNow);
  lastSunday.setDate(estNow.getDate() - 1);
  const weekEnd = lastSunday.toISOString().split('T')[0];

  const { data: existing } = await sb
    .from('weekly_intelligence')
    .select('id, status, week_number')
    .eq('week_start', weekStart)
    .limit(1).single().then(r => r, () => ({ data: null }));

  if (existing?.status === 'generated') {
    console.log('[WEEKLY INTEL] Already generated for', weekStart);
  } else if (existing?.status === 'generating') {
    console.log('[WEEKLY INTEL] Already generating for', weekStart);
  } else {
    const { data: latest } = await sb
      .from('weekly_intelligence')
      .select('week_number')
      .order('week_number', { ascending: false })
      .limit(1).single().then(r => r, () => ({ data: null }));

    const weekNum = existing?.week_number || (latest?.week_number || 0) + 1;

    if (!existing) {
      await sb.from('weekly_intelligence').insert({
        week_number: weekNum,
        week_start: weekStart,
        week_end: weekEnd,
        status: 'pending',
      });
    }

    await runWeeklyIntelligence(weekStart, weekEnd, weekNum, sb);

    // Pre-create placeholder for the current week (will be generated next Monday)
    const thisMonday = estNow.toISOString().split('T')[0];
    const thisSunday = new Date(estNow);
    thisSunday.setDate(estNow.getDate() + 6);

    await sb.from('weekly_intelligence').upsert({
      week_number: weekNum + 1,
      week_start: thisMonday,
      week_end: thisSunday.toISOString().split('T')[0],
      status: 'pending',
    }, { onConflict: 'week_start', ignoreDuplicates: true });
  }
}

async function gatherWeekMetrics(weekStart, weekEnd, sb) {
  const startISO = new Date(weekStart + 'T00:00:00Z').toISOString();
  const endISO   = new Date(weekEnd   + 'T23:59:59Z').toISOString();

  const { data: dealRows } = await sb
    .from('deal_analytics')
    .select('*')
    .gte('week_starting', weekStart)
    .lte('week_starting', weekEnd);

  const fromDealAnalytics = (dealRows || []).reduce((acc, row) => ({
    emails_sent:           (acc.emails_sent           || 0) + (row.emails_sent           || 0),
    linkedin_invites_sent: (acc.linkedin_invites_sent || 0) + (row.linkedin_invites_sent || 0),
    linkedin_dms_sent:     (acc.linkedin_dms_sent     || 0) + (row.linkedin_dms_sent     || 0),
    email_replies:         (acc.email_replies         || 0) + (row.email_replies         || 0),
    linkedin_replies:      (acc.linkedin_replies      || 0) + (row.linkedin_replies      || 0),
    meetings_booked:       (acc.meetings_booked       || 0) + (row.meetings_booked       || 0),
    positive_responses:    (acc.positive_responses    || 0) + (row.positive_responses    || 0),
    negative_responses:    (acc.negative_responses    || 0) + (row.negative_responses    || 0),
  }), {});

  const { data: inboundMsgs } = await sb
    .from('conversation_messages')
    .select('id, content, channel, contact_id')
    .eq('direction', 'inbound')
    .gte('sent_at', startISO)
    .lte('sent_at', endISO);

  const allInbound = inboundMsgs || [];

  const { count: meetingsFromIntent } = await sb
    .from('approval_queue')
    .select('id', { count: 'exact', head: true })
    .contains('metadata', { intent: 'scheduling' })
    .gte('created_at', startISO)
    .lte('created_at', endISO);

  const { count: firmsResearched } = await sb
    .from('batch_firms')
    .select('id', { count: 'exact', head: true })
    .eq('firm_researched', true)
    .gte('created_at', startISO)
    .lte('created_at', endISO);

  const { count: contactsEnriched } = await sb
    .from('contacts')
    .select('id', { count: 'exact', head: true })
    .eq('enrichment_status', 'complete')
    .gte('updated_at', startISO)
    .lte('updated_at', endISO);

  const { count: ghosted } = await sb
    .from('contacts')
    .select('id', { count: 'exact', head: true })
    .eq('conversation_state', 'ghosted');

  const { count: activeDeals } = await sb
    .from('deals')
    .select('id', { count: 'exact', head: true })
    .eq('status', 'active');

  const { count: liAccepted } = await sb
    .from('contacts')
    .select('id', { count: 'exact', head: true })
    .eq('linkedin_connected', true)
    .gte('updated_at', startISO)
    .lte('updated_at', endISO);

  const sentimentResult = await analyseSentimentBatch(allInbound);

  const es  = fromDealAnalytics.emails_sent           || 0;
  const er  = fromDealAnalytics.email_replies         || allInbound.filter(m => m.channel === 'email').length;
  const li  = fromDealAnalytics.linkedin_invites_sent || 0;
  const la  = liAccepted  || 0;
  const lds = fromDealAnalytics.linkedin_dms_sent     || 0;
  const ldr = fromDealAnalytics.linkedin_replies      || allInbound.filter(m => m.channel === 'linkedin').length;
  const mtg = Math.max(fromDealAnalytics.meetings_booked || 0, meetingsFromIntent || 0);

  return {
    emails_sent:              es,
    email_reply_rate:         es  > 0 ? +((er  / es)  * 100).toFixed(1) : 0,
    linkedin_invites_sent:    li,
    linkedin_accepted:        la,
    linkedin_acceptance_rate: li  > 0 ? +((la  / li)  * 100).toFixed(1) : 0,
    linkedin_dms_sent:        lds,
    linkedin_dm_reply_rate:   lds > 0 ? +((ldr / lds) * 100).toFixed(1) : 0,
    meetings_booked:          mtg,
    active_deals_count:       activeDeals      || 0,
    firms_researched:         firmsResearched  || 0,
    contacts_enriched:        contactsEnriched || 0,
    ghosted_contacts:         ghosted          || 0,
    total_replies:            er + ldr,
    avg_sentiment:            sentimentResult.average,
    sentiment_breakdown:      sentimentResult.breakdown,
  };
}

async function analyseSentimentBatch(messages) {
  if (!messages.length) {
    return { average: 0, breakdown: { positive: 0, neutral: 0, negative: 0 } };
  }

  const sample = messages.slice(0, 50).map((m, i) =>
    `[${i + 1}] "${(m.content || '').slice(0, 150).replace(/"/g, "'")}"`
  ).join('\n');

  try {
    const Anthropic = (await import('@anthropic-ai/sdk')).default;
    const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

    const response = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 400,
      messages: [{
        role: 'user',
        content: `Classify investor reply sentiment. Context: PE fundraising outreach.
Positive (+0.2 to +1.0): interested, open to call, requesting materials.
Neutral (-0.2 to +0.2): asking questions, non-committal, out of office.
Negative (-1.0 to -0.2): declined, wrong mandate, unsubscribe.

REPLIES:
${sample}

Return ONLY valid JSON — no other text:
{"scores":[${messages.slice(0, 50).map(() => '0').join(',')}],"summary":"..."}`,
      }],
    });

    const raw = response.content[0]?.text || '';
    const result = extractJSON(raw);   // robust extraction
    const scores = (result.scores || []).filter(s => typeof s === 'number');
    if (!scores.length) throw new Error('no scores in sentiment response');

    const avg = scores.reduce((a, b) => a + b, 0) / scores.length;
    const pos = scores.filter(s => s > 0.2).length;
    const neg = scores.filter(s => s < -0.2).length;
    const neu = scores.length - pos - neg;
    const tot = scores.length;

    return {
      average: +avg.toFixed(2),
      breakdown: {
        positive: tot > 0 ? Math.round((pos / tot) * 100) : 0,
        neutral:  tot > 0 ? Math.round((neu / tot) * 100) : 0,
        negative: tot > 0 ? Math.round((neg / tot) * 100) : 0,
      },
    };
  } catch (err) {
    console.error('[SENTIMENT]', err.message);
    return { average: 0, breakdown: { positive: 0, neutral: 0, negative: 0 } };
  }
}

// Safely extract JSON from Claude output — handles markdown fences, leading text, truncation
function extractJSON(text) {
  if (!text) throw new Error('Empty Claude response');
  // Strip markdown fences
  let cleaned = text.replace(/```json\s*/gi, '').replace(/```\s*/g, '').trim();
  // Find the first { and last } to handle any leading/trailing text
  const start = cleaned.indexOf('{');
  const end   = cleaned.lastIndexOf('}');
  if (start === -1 || end === -1 || end <= start) throw new Error(`No JSON object found in: ${cleaned.slice(0, 100)}`);
  return JSON.parse(cleaned.slice(start, end + 1));
}

export async function runWeeklyIntelligence(weekStart, weekEnd, weekNum, sb) {
  const label = `Week ${weekNum} (${weekStart} — ${weekEnd})`;
  let currentStep = 'init';

  pushActivity({ type: 'analysis', action: `Intelligence generation started: ${label}`, note: 'SAGE gathering metrics across all active deals' });

  const { error: generatingErr } = await sb.from('weekly_intelligence')
    .upsert({
      week_start: weekStart,
      week_end: weekEnd,
      week_number: weekNum,
      status: 'generating',
    }, { onConflict: 'week_start' });
  if (generatingErr) throw generatingErr;

  try {
    // ── Step 1: Gather metrics ────────────────────────────────────────────────
    currentStep = 'gathering metrics';
    pushActivity({ type: 'analysis', action: `Gathering metrics: ${label}`, note: 'Reading deal_analytics, conversation_messages, batch_firms' });
    const metrics = await gatherWeekMetrics(weekStart, weekEnd, sb);
    pushActivity({ type: 'analysis', action: `Metrics collected: ${label}`, note: `${metrics.emails_sent} emails · ${metrics.total_replies} replies · ${metrics.meetings_booked} meetings` });

    // ── Step 2: Load historical context ──────────────────────────────────────
    currentStep = 'loading history';
    let prevWeeks = [];
    try {
      const { data } = await sb
        .from('weekly_intelligence')
        .select('week_start, headline, what_worked, what_didnt_work, best_investor_profile, investor_matching_insights, template_generation_insights')
        .eq('status', 'generated')
        .lt('week_start', weekStart)
        .order('week_start', { ascending: false })
        .limit(3);
      prevWeeks = data || [];
    } catch { /* no history is fine */ }

    const historyContext = prevWeeks.length
      ? prevWeeks.map(w =>
          `Week of ${w.week_start}: ${w.headline || 'No headline'}\n` +
          `What worked: ${w.what_worked || 'N/A'}\n` +
          `Best investor profile: ${w.best_investor_profile || 'N/A'}\n` +
          `Investor insight: ${w.investor_matching_insights || 'N/A'}`
        ).join('\n\n')
      : 'No prior weeks — this is the first report.';

    let dailyLogs = [];
    try {
      const { data } = await sb
        .from('daily_activity_reports')
        .select('report_date, headline, executive_summary, deal_sections')
        .gte('report_date', weekStart)
        .lte('report_date', weekEnd)
        .order('report_date', { ascending: true });
      dailyLogs = data || [];
    } catch {}

    const dailyLogContext = dailyLogs.length
      ? dailyLogs.map(log =>
          `${log.report_date}: ${log.headline || 'Daily log'}\n` +
          `Summary: ${log.executive_summary || 'No executive summary saved.'}\n` +
          `Deals: ${(log.deal_sections || []).map(section => `${section.deal_name}: ${section.target_status || section.progress_status || 'Status unavailable'}`).join(' | ') || 'No deal breakdown saved.'}`
        ).join('\n\n')
      : 'No daily logs were available for this week.';

    // ── Step 3: Call Claude SAGE ──────────────────────────────────────────────
    currentStep = 'calling Claude SAGE';
    pushActivity({ type: 'analysis', action: `SAGE generating report: ${label}`, note: 'Analysing patterns, generating recommendations' });

    const Anthropic = (await import('@anthropic-ai/sdk')).default;
    const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

    const response = await anthropic.messages.create({
      model:      'claude-haiku-4-5-20251001', // Haiku is faster + cheaper for structured output
      max_tokens: 1500,  // bumped to avoid JSON truncation
      messages: [{
        role: 'user',
        content: `You are SAGE, the intelligence engine for ROCO — an autonomous PE fundraising agent.
Generate a weekly intelligence report. Be specific and data-driven.
Sound like a McKinsey analyst briefing a PE partner: precise, direct, no fluff. No em dashes.

WEEK: ${weekStart} to ${weekEnd}

THIS WEEK'S METRICS:
Emails sent: ${metrics.emails_sent} | Email reply rate: ${metrics.email_reply_rate}%
LinkedIn invites: ${metrics.linkedin_invites_sent} | Acceptance rate: ${metrics.linkedin_acceptance_rate}%
LinkedIn DMs: ${metrics.linkedin_dms_sent} | DM reply rate: ${metrics.linkedin_dm_reply_rate}%
Total replies (all channels): ${metrics.total_replies}
Meetings booked: ${metrics.meetings_booked}
Active deals: ${metrics.active_deals_count}
Firms researched: ${metrics.firms_researched}
Contacts enriched: ${metrics.contacts_enriched}
Ghosted (no reply after full sequence): ${metrics.ghosted_contacts}

SENTIMENT (${metrics.total_replies} replies):
Average: ${metrics.avg_sentiment} | Positive: ${metrics.sentiment_breakdown?.positive || 0}% | Neutral: ${metrics.sentiment_breakdown?.neutral || 0}% | Negative: ${metrics.sentiment_breakdown?.negative || 0}%

HISTORICAL CONTEXT:
${historyContext}

DAILY LOG CONTEXT FOR THIS WEEK:
${dailyLogContext}

Return ONLY a raw JSON object — no markdown, no preamble, no explanation:
{"headline":"...","what_worked":"...","what_didnt_work":"...","best_investor_profile":"...","best_sending_time":"...","template_recommendations":"...","investor_matching_insights":"...","template_generation_insights":"...","trend_vs_last_week":"...","three_actions":["...","...","..."]}`,
      }],
    });

    // ── Step 4: Parse response ────────────────────────────────────────────────
    currentStep = 'parsing Claude response';
    const raw = response.content[0]?.text || '';
    const sageResult = extractJSON(raw);  // robust extraction, handles any format

    // Validate required fields exist
    if (!sageResult.headline) throw new Error('SAGE response missing headline field');

    pushActivity({ type: 'analysis', action: `SAGE report complete: ${label}`, note: sageResult.headline });

    // ── Step 5: Save to DB — explicit columns only, no spread ─────────────────
    currentStep = 'saving to database';
    const { error: saveErr } = await sb.from('weekly_intelligence').upsert({
      week_start:                   weekStart,
      week_end:                     weekEnd,
      week_number:                  weekNum,
      // Metrics — explicit list avoids spreading unknown columns
      emails_sent:              metrics.emails_sent,
      email_reply_rate:         metrics.email_reply_rate,
      linkedin_invites_sent:    metrics.linkedin_invites_sent,
      linkedin_acceptance_rate: metrics.linkedin_acceptance_rate,
      linkedin_dms_sent:        metrics.linkedin_dms_sent,
      linkedin_dm_reply_rate:   metrics.linkedin_dm_reply_rate,
      meetings_booked:          metrics.meetings_booked,
      active_deals_count:       metrics.active_deals_count,
      firms_researched:         metrics.firms_researched,
      contacts_enriched:        metrics.contacts_enriched,
      ghosted_contacts:         metrics.ghosted_contacts,
      total_replies:            metrics.total_replies,
      avg_sentiment:            metrics.avg_sentiment,
      // SAGE output
      headline:                     sageResult.headline,
      what_worked:                  sageResult.what_worked,
      what_didnt_work:              sageResult.what_didnt_work,
      best_investor_profile:        sageResult.best_investor_profile,
      best_sending_time:            sageResult.best_sending_time,
      template_recommendations:     sageResult.template_recommendations,
      investor_matching_insights:   sageResult.investor_matching_insights,
      template_generation_insights: sageResult.template_generation_insights,
      raw_recommendations:          sageResult,
      status:                       'generated',
      generated_at:                 new Date().toISOString(),
    }, { onConflict: 'week_start' });
    if (saveErr) throw saveErr;

    await refreshInvestorListSuccessRates(sb).catch(err => {
      console.warn('[SAGE] list success rate refresh failed:', err.message);
    });
    await syncListLearningsToDealMemory(sb).catch(err => {
      console.warn('[SAGE] list learnings sync failed:', err.message);
    });

    // ── Step 6: Notify Dom ────────────────────────────────────────────────────
    currentStep = 'sending Telegram notification';
    const weekLabel = `${new Date(weekStart).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })} — ${new Date(weekEnd).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })}`;

    await sendTelegram(
      `📊 *Weekly Intelligence Report Ready*\n\n` +
      `Week ${weekNum}: ${weekLabel}\n\n` +
      `${sageResult.headline}\n\n` +
      `*3 Actions:*\n` +
      (sageResult.three_actions || []).map((a, i) => `${i + 1}. ${a}`).join('\n') +
      `\n\nOpen the Intelligence tab in the dashboard to review the full report.`
    ).catch(e => console.warn('[WEEKLY INTEL] Telegram notify failed:', e.message));

    pushActivity({ type: 'analysis', action: `Analysis complete: ${label}`, note: 'Telegram sent · Open Intelligence tab to review' });

  } catch (err) {
    console.error(`[WEEKLY INTEL] Failed at step "${currentStep}": ${err.message}`);

    // Set to 'failed' (not 'pending') so catch-up doesn't immediately retry a broken run
    // and so we can distinguish "never ran" from "ran and broke"
    try {
      await sb.from('weekly_intelligence')
        .upsert({
          week_start: weekStart,
          week_end: weekEnd,
          week_number: weekNum,
          status: 'failed',
          raw_recommendations: { error: err.message, step: currentStep },
        }, { onConflict: 'week_start' });
    } catch {}

    pushActivity({
      type: 'error',
      action: `Intelligence generation failed: ${label}`,
      note: `Step: ${currentStep} — ${err.message?.slice(0, 120)}`,
    });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STARTUP BACKFILL — copy past_investments + person_researched from investors_db
// Runs once at startup to fix contacts promoted before these fields were tracked.
// ─────────────────────────────────────────────────────────────────────────────

async function backfillContactsFromInvestorsDb() {
  const sb = getSupabase();
  if (!sb) return;

  // Find contacts with an investors_db_id but missing past_investments
  const { data: contacts } = await sb.from('contacts')
    .select('id, investors_db_id, notes, person_researched, past_investments')
    .not('investors_db_id', 'is', null)
    .limit(500);

  if (!contacts?.length) return;

  let pastInvFilled = 0;
  let personResearchedFilled = 0;

  for (const contact of contacts) {
    const updates = {};

    // Backfill person_researched if notes contain the marker but column is false
    if (!contact.person_researched && contact.notes?.includes('[PERSON_RESEARCHED]')) {
      updates.person_researched = true;
      personResearchedFilled++;
    }

    // Only fetch from investors_db / parse notes if we're missing past_investments
    if (!contact.past_investments) {
      // 1. Try investors_db
      const { data: inv } = await sb.from('investors_db')
        .select('past_investments')
        .eq('id', contact.investors_db_id)
        .single();
      if (inv?.past_investments) {
        updates.past_investments = inv.past_investments;
        pastInvFilled++;
      } else if (contact.notes) {
        // 2. Parse from research notes — patterns: "Portfolio: X, Y" or "Past: X, Y"
        const portfolioMatch = contact.notes.match(/(?:Portfolio|Past(?:\s+Investments)?)\s*:\s*([^\n|[\]]{3,200})/i);
        if (portfolioMatch?.[1]) {
          updates.past_investments = portfolioMatch[1].trim();
          pastInvFilled++;
        }
      }
    }

    if (Object.keys(updates).length) {
      await sb.from('contacts').update(updates).eq('id', contact.id);
    }
  }

  if (pastInvFilled || personResearchedFilled) {
    console.log(`[BACKFILL] Contacts updated — past_investments: ${pastInvFilled}, person_researched: ${personResearchedFilled}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// CAMPAIGN BATCH MANAGEMENT
// ─────────────────────────────────────────────────────────────────────────────

/** How many ranked firms trigger a campaign review */
const BATCH_FIRM_TARGET = 100;

function classifyBatchEntity({ companyName, isAngel }) {
  const companyLower = (companyName || '').toLowerCase().trim();
  const contactType = (!companyName || GENERIC_FIRM_NAMES.has(companyLower) || isAngel)
    ? 'individual'
    : 'institutional';
  return {
    contactType,
    entityKey: contactType === 'institutional' ? companyLower : null,
  };
}

async function getBatchEntitySnapshot(dealId, batchStart) {
  const sb = getSupabase();
  if (!sb) return { entityCount: 0, firmKeys: new Set() };

  const ACTIVE_STAGES = ['Ranked', 'ranked', 'Enriched', 'enriched', 'pending_email_approval', 'pending_dm_approval', 'Email Approved', 'DM Approved', 'Email Sent', 'DM Sent', 'email_sent', 'dm_sent',
    'invite_sent', 'invite_accepted', 'Replied', 'In Conversation'];

  const { data: firmContacts } = await sb.from('contacts')
    .select('company_name')
    .eq('deal_id', dealId)
    .in('contact_type', ['institutional', 'individual_at_firm'])
    .gte('created_at', batchStart)
    .in('pipeline_stage', ACTIVE_STAGES)
    .not('pipeline_stage', 'eq', 'Archived');

  const firmKeys = new Set(
    (firmContacts || []).map(c => (c.company_name || '').toLowerCase().trim()).filter(Boolean)
  );

  const { count: individualCount } = await sb.from('contacts')
    .select('id', { count: 'exact', head: true })
    .eq('deal_id', dealId)
    .eq('contact_type', 'individual')
    .gte('created_at', batchStart)
    .in('pipeline_stage', ACTIVE_STAGES)
    .not('pipeline_stage', 'eq', 'Archived');

  return {
    entityCount: firmKeys.size + (individualCount || 0),
    firmKeys,
  };
}

async function backfillBatchContactTypes(deal, batch) {
  if (!batch) return;
  const sb = getSupabase();
  if (!sb) return;

  const { data: contacts } = await sb.from('contacts')
    .select('id, company_name, is_angel')
    .eq('deal_id', deal.id)
    .gte('created_at', batch.created_at)
    .is('contact_type', null)
    .not('pipeline_stage', 'eq', 'Archived')
    .limit(200);

  for (const contact of contacts || []) {
    const { contactType } = classifyBatchEntity({
      companyName: contact.company_name,
      isAngel: contact.is_angel,
    });
    await sb.from('contacts').update({ contact_type: contactType }).eq('id', contact.id);
  }
}

// In-memory error tracking per deal — auto-pause after 5 consecutive errors with 5-min cooldown
const dealErrorCounts = new Map(); // dealId → { count, pausedUntil }

async function ensureBatchExists(deal) {
  const sb = getSupabase();
  if (!sb) return null;
  const { data: existing } = await sb.from('campaign_batches')
    .select('*')
    .eq('deal_id', deal.id)
    .not('status', 'in', '("completed","skipped")')
    .order('batch_number', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (existing) return existing;

  const batchNumber = await getNextBatchNumber(deal.id);
  const { data: created } = await sb.from('campaign_batches')
    .insert({
      deal_id: deal.id,
      batch_number: batchNumber,
      status: 'researching',
      target_firms: BATCH_FIRM_TARGET,
      ranked_firms: 0,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    })
    .select()
    .single();
  pushActivity({
    type: 'system',
    action: `Batch ${batchNumber} started — researching up to ${BATCH_FIRM_TARGET} firms`,
    note: `${deal.name} · Identifying and scoring best-fit investors`,
    deal_name: deal.name,
    dealId: deal.id,
  });
  return created;
}

async function getNextBatchNumber(dealId) {
  const sb = getSupabase();
  const { data } = await sb.from('campaign_batches')
    .select('batch_number')
    .eq('deal_id', dealId)
    .order('batch_number', { ascending: false })
    .limit(1)
    .single();
  return (data?.batch_number || 0) + 1;
}

async function isApprovedForOutreach(dealId) {
  const sb = getSupabase();
  if (!sb) return false;
  const { data } = await sb.from('campaign_batches')
    .select('id, status')
    .eq('deal_id', dealId)
    .eq('status', 'approved')
    .limit(1)
    .single();
  return !!data;
}

async function getExcludedFirmNames(dealId) {
  const sb = getSupabase();
  if (!sb) return new Set();

  // Exclude from: current deal's batch_firms + ALL active deals' contacts + global exclusion list
  // This prevents adding a firm to the pipeline if we're already reaching out to them on another deal
  const [batchRes, allContactsRes, exclusionRes, otherDealsRes] = await Promise.all([
    sb.from('batch_firms').select('firm_name').eq('deal_id', dealId).then(r => r).catch(() => ({ data: [] })),
    sb.from('contacts').select('company_name').eq('deal_id', dealId).then(r => r).catch(() => ({ data: [] })),
    sb.from('firm_exclusion_list').select('company_name, deal_id, deal_status').then(r => r).catch(() => ({ data: [] })),
    // Also grab all firms from OTHER active deals' contacts (cross-deal dedup)
    sb.from('contacts').select('company_name').neq('deal_id', dealId).not('company_name', 'is', null).then(r => r).catch(() => ({ data: [] })),
  ]);

  const names = new Set();
  for (const row of batchRes.data || []) {
    const name = normalizeFirmName(row?.firm_name);
    if (name) names.add(name);
  }
  for (const row of allContactsRes.data || []) {
    const name = normalizeFirmName(row?.company_name);
    if (name) names.add(name);
  }
  for (const row of exclusionRes.data || []) {
    const name = normalizeFirmName(row?.company_name);
    if (!name) continue;
    const status = normalizeActionLabel(row?.deal_status);
    if (status === 'active' || status === 'paused') names.add(name);
  }
  // Cross-deal: exclude any firm already being contacted on another deal
  for (const row of otherDealsRes.data || []) {
    const name = normalizeFirmName(row?.company_name);
    if (name) names.add(name);
  }
  return names;
}

async function updateBatchResearchCount(batchId) {
  const sb = getSupabase();
  if (!sb) return 0;
  const { data, count } = await sb.from('batch_firms')
    .select('id', { count: 'exact' })
    .eq('batch_id', batchId);
  const firmCount = count ?? data?.length ?? 0;
  await sb.from('campaign_batches')
    .update({ ranked_firms: firmCount, updated_at: new Date().toISOString() })
    .eq('id', batchId);
  return firmCount;
}

function normalizeFirmName(value) {
  return String(value || '').toLowerCase().trim();
}

function truncateForNote(value, max = 180) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  return text.length > max ? `${text.slice(0, max - 1)}...` : text;
}

async function preEnrichNewsLead(batchFirmId, firmName, deal, existingFirm = {}) {
  const sb = getSupabase();
  if (!sb || !batchFirmId || !firmName || !deal?.id) return { contactsFound: 0 };

  const unipileData = await enrichFirmViaLinkedIn(firmName, deal, pushActivity).catch(() => ({ linkedin_profile: null, contacts: [] }));
  const updates = {};
  const profile = unipileData.linkedin_profile;

  if (profile?.employee_count && !existingFirm.aum) updates.aum = `${profile.employee_count} employees`;
  if (profile?.description && !existingFirm.thesis) updates.thesis = String(profile.description).slice(0, 500);
  if (Array.isArray(unipileData.contacts) && unipileData.contacts.length) updates.contacts_found = unipileData.contacts.length;

  if (Object.keys(updates).length) {
    try { await sb.from('batch_firms').update(updates).eq('id', batchFirmId); } catch {}
  }

  return { contactsFound: Number(updates.contacts_found || 0) };
}

async function addNewsLeadsToBatch(deal, leads) {
  const sb = getSupabase();
  if (!sb || !deal?.id || !Array.isArray(leads) || !leads.length) return { added: 0, skipped: 0, batch: null };

  const batch = await ensureBatchExists(deal);
  if (!batch) return { added: 0, skipped: leads.length, batch: null };

  const strictGeo = deal.strict_geography !== false;
  const existingNames = await getExcludedFirmNames(deal.id);
  let added = 0;
  let skipped = 0;

  for (const lead of leads) {
    const firmName = String(lead?.firm_name || '').trim();
    const normalized = normalizeFirmName(firmName);
    if (!normalized || existingNames.has(normalized)) {
      skipped += 1;
      continue;
    }

    const investorLike = {
      firm_name: firmName,
      name: firmName,
      investor_type: lead.investor_type || 'PE/Buyout',
      hq_country: lead.hq_country || 'United States',
      description: `${lead.why_relevant} Recent: ${lead.news_event}`,
      _from_internet: true,
      list_name: 'Grok Daily News Scan',
    };

    const { score, scoring_breakdown, geo_match, control_action } = scoreFirmAgainstDeal(investorLike, deal, 0, null);
    if (control_action === 'archive') {
      skipped += 1;
      continue;
    }
    if (strictGeo && !geo_match && score < 40) {
      pushActivity({
        type: 'excluded',
        action: `News lead skipped (geo mismatch): ${firmName}`,
        note: lead.news_event || 'Geographic mismatch',
        deal_name: deal.name,
        dealId: deal.id,
      });
      skipped += 1;
      continue;
    }

    const floorScore = lead.urgency === 'high' ? 82 : 72;
    const finalScore = Math.max(score, floorScore);
    const { data: inserted, error: insertError } = await sb.from('batch_firms').insert({
      batch_id: batch.id,
      deal_id: deal.id,
      firm_name: firmName,
      score: finalScore,
      justification: lead.why_relevant || null,
      thesis: lead.news_event || null,
      notes: lead.source_hint ? `Source: ${lead.source_hint}` : null,
      source_list: 'Grok Daily News Scan',
      firm_researched: true,
      enrichment_status: 'pending',
      status: 'to_research',
      created_at: new Date().toISOString(),
    }).select('id, firm_name, thesis, aum').single();

    if (insertError) {
      skipped += 1;
      pushActivity({
        type: 'error',
        action: `News lead insert failed: ${firmName}`,
        note: insertError.message?.slice(0, 120),
        deal_name: deal.name,
        dealId: deal.id,
      });
      continue;
    }

    existingNames.add(normalized);
    added += 1;

    const preEnriched = await preEnrichNewsLead(inserted?.id, firmName, deal, inserted || {});
    pushActivity({
      type: 'research',
      action: `News lead added to pipeline: ${firmName} — Score ${finalScore}/100`,
      note: `${deal.name} · ${lead.news_event}${preEnriched.contactsFound ? ` · ${preEnriched.contactsFound} LinkedIn contacts pre-identified` : ''}`,
      deal_name: deal.name,
      dealId: deal.id,
    });
  }

  const firmCount = await updateBatchResearchCount(batch.id);
  if (batch.status === 'researching' && firmCount >= BATCH_FIRM_TARGET) {
    await triggerCampaignReview(deal, batch);
  } else if (batch.status === 'pending_approval') {
    await rankBatchFirms(batch.id, deal);
    await updateBatchResearchCount(batch.id);
  }

  return { added, skipped, batch };
}

async function loadDealInfoForInvestorMatching(deal) {
  const sb = getSupabase();
  if (!sb) {
    return {
      deal_name: deal.name,
      deal_type: deal.raise_type || 'Buyout',
      sector: deal.sector || 'General',
      sub_sector: null,
      geography: deal.geography || 'United States',
      hq_location: deal.geography || '',
      ebitda_usd_m: null,
      revenue_usd_m: null,
      enterprise_value_usd_m: null,
      equity_required_usd_m: deal.min_cheque ? deal.min_cheque / 1_000_000 : null,
      ideal_investor_types: ['PE/Buyout', 'Family Office'],
      ideal_investor_profile: `Investor interested in ${deal.sector || 'general'} deals`,
      disqualified_investor_types: [],
    };
  }

  const { data: docs } = await sb.from('deal_documents')
    .select('parsed_deal_info')
    .eq('deal_id', deal.id)
    .limit(1);

  return docs?.[0]?.parsed_deal_info || {
    deal_name: deal.name,
    deal_type: deal.raise_type || 'Buyout',
    sector: deal.sector || 'General',
    sub_sector: null,
    geography: deal.geography || 'United States',
    hq_location: deal.geography || '',
    ebitda_usd_m: null,
    revenue_usd_m: null,
    enterprise_value_usd_m: null,
    equity_required_usd_m: deal.min_cheque ? deal.min_cheque / 1_000_000 : null,
    ideal_investor_types: ['PE/Buyout', 'Family Office'],
    ideal_investor_profile: `Investor interested in ${deal.sector || 'general'} deals`,
    disqualified_investor_types: [],
  };
}

async function getPriorityListShortlist(deal, { limit = 20, emitActivity = false } = {}) {
  const sb = getSupabase();
  if (!sb) return { shortlisted: [], activePriorityList: null };

  const threshold = deal.min_investor_score || 60;
  const dealInfo = await loadDealInfoForInvestorMatching(deal);

  const { data: alreadyContacted } = await sb.from('contacts')
    .select('investors_db_id, company_name')
    .eq('deal_id', deal.id);
  const contactedDbIds = new Set((alreadyContacted || []).map(c => c.investors_db_id).filter(Boolean));
  const contactedFirms = new Set((alreadyContacted || []).map(c => normalizeFirmForDedup(c.company_name)).filter(Boolean));

  const { data: priorityLists } = await sb.from('deal_list_priorities')
    .select('*')
    .eq('deal_id', deal.id)
    .not('status', 'eq', 'exhausted')
    .order('priority_order', { ascending: true });

  let activePriorityList = null;
  let shortlisted = [];

  for (const pl of (priorityLists || [])) {
    const LIST_PAGE = 1000;
    let listFrom = 0;
    const listInvestors = [];
    while (true) {
      const { data: page } = await sb.from('investors_db')
        .select('*')
        .eq('list_id', pl.list_id)
        .range(listFrom, listFrom + LIST_PAGE - 1);
      if (!page?.length) break;
      listInvestors.push(...page);
      if (page.length < LIST_PAGE) break;
      listFrom += LIST_PAGE;
    }

    if (!listInvestors.length) {
      await sb.from('deal_list_priorities')
        .update({ status: 'exhausted', exhausted_at: new Date().toISOString() })
        .eq('id', pl.id);
      continue;
    }

    const fresh = listInvestors.filter(inv =>
      !contactedDbIds.has(inv.id) &&
      !contactedFirms.has(normalizeFirmForDedup(inv.name))
    );

    if (!fresh.length) {
      await sb.from('deal_list_priorities')
        .update({ status: 'exhausted', exhausted_at: new Date().toISOString() })
        .eq('id', pl.id);
      continue;
    }

    await sb.from('deal_list_priorities').update({ status: 'active' }).eq('id', pl.id);
    activePriorityList = pl;

    if (emitActivity) {
      pushActivity({
        type: 'research',
        action: `Scoring ${fresh.length} candidates from list "${pl.list_name}"`,
        deal_name: deal.name,
        dealId: deal.id,
      });
    }

    const scored = await batchScoreInvestors(fresh, dealInfo, deal);
    const disqualified = (dealInfo.disqualified_investor_types || []).map(t => t.toLowerCase());
    shortlisted = scored
      .filter(s => {
        if (s.score < threshold) return false;
        if (disqualified.some(d => (s.investor_type || '').toLowerCase().includes(d))) return false;
        return true;
      })
      .sort((a, b) => b.score - a.score)
      .slice(0, Math.max(1, limit));
    break;
  }

  return { shortlisted, activePriorityList };
}

async function addPriorityInvestorsToBatch(deal, investors, activePriorityList) {
  const sb = getSupabase();
  if (!sb || !deal?.id || !investors?.length) return { added: 0, skipped: 0, batch: null };

  const batch = await ensureBatchExists(deal);
  if (!batch) return { added: 0, skipped: investors.length, batch: null };

  const existingNames = await getExcludedFirmNames(deal.id);
  let added = 0;
  let skipped = 0;

  for (const investor of investors) {
    const firmName = String(investor?.firm_name || investor?.name || '').trim();
    const normalized = normalizeFirmName(firmName);
    if (!normalized || existingNames.has(normalized)) {
      skipped += 1;
      continue;
    }

    const { error: insertError } = await sb.from('batch_firms').insert({
      batch_id: batch.id,
      deal_id: deal.id,
      investor_id: investor.id || null,
      firm_name: firmName,
      score: Number(investor.score || investor.investor_score || 0),
      justification: investor.score_reason || investor.justification || null,
      thesis: investor.description || investor.thesis || null,
      past_investments: Array.isArray(investor.past_investments) ? investor.past_investments : [],
      aum: investor.aum_millions ? `$${investor.aum_millions}M` : (investor.aum || null),
      source_list: activePriorityList?.list_name || investor.list_name || 'Priority List',
      firm_researched: true,
      enrichment_status: 'pending',
      status: 'pending',
      created_at: new Date().toISOString(),
    });

    if (insertError) {
      skipped += 1;
      continue;
    }

    existingNames.add(normalized);
    added += 1;
  }

  await updateBatchResearchCount(batch.id);
  return { added, skipped, batch };
}

async function triggerAutoFeedForDeal(deal, { reason = 'pipeline_depth', requestedCount = 24 } = {}) {
  const sb = getSupabase();
  if (!sb || !deal?.id) return { researched: 0, added: 0, skipped: 0 };

  const autoFeedKey = `${deal.id}:${DateTime.now().setZone('America/New_York').toISODate()}:${reason}`;
  if (approvedBatchTopUpState.get(autoFeedKey)) {
    return { researched: 0, added: 0, skipped: 0 };
  }

  const batch = await ensureBatchExists(deal);
  if (!batch || batch.status !== 'researching') return { researched: 0, added: 0, skipped: 0 };

  const before = await updateBatchResearchCount(batch.id);
  const needed = Math.max(20, Math.min(30, requestedCount));
  await researchNextFirms(deal, batch, needed, before);
  const after = await updateBatchResearchCount(batch.id);
  const added = Math.max(0, after - before);
  const researched = needed;
  const skipped = Math.max(0, researched - added);

  approvedBatchTopUpState.set(autoFeedKey, true);
  await sb.from('activity_log').insert({
    deal_id: deal.id,
    event_type: 'PIPELINE_AUTO_FEED',
    summary: `Auto-feed triggered for ${deal.name}: researched ${researched} new firms, ${added} added after deduplication, ${skipped} skipped (already in pipeline).`,
    created_at: new Date().toISOString(),
  }).catch(() => {});
  pushActivity({
    type: 'pipeline',
    action: `Auto-feed triggered for ${deal.name}: researched ${researched} new firms, ${added} added after deduplication, ${skipped} skipped (already in pipeline).`,
    note: reason,
    deal_name: deal.name,
    dealId: deal.id,
  });
  return { researched, added, skipped };
}

async function runDailyNewsScanCycle(deals) {
  const estNow = DateTime.now().setZone('America/New_York');
  if (estNow.hour !== 7) return;
  const sb = getSupabase();

  const todayKey = estNow.toISODate();
  const existingState = dailyNewsScanState.get(todayKey);
  if (existingState === 'running' || existingState === 'done') return;
  dailyNewsScanState.set(todayKey, 'running');

  try {
    const digestSections = [];
    const portfolioScanRows = [];

    if (deals.length > 0) {
      for (const deal of deals) {
        const scanResult = await buildDealNewsScan(deal, deals, pushActivity).catch(() => ({ leads: [], summary: '' }));
        portfolioScanRows.push({ deal, scanResult });
        const leads = Array.isArray(scanResult?.leads) ? scanResult.leads : [];
        await saveDealNewsLeads(deal.id, leads).catch(() => {});
        const pipelineResult = await addNewsLeadsToBatch(deal, leads).catch(() => ({ added: 0, skipped: leads.length, batch: null }));
        const actionsTaken = [];
        if (pipelineResult.added > 0) {
          actionsTaken.push(`Added ${pipelineResult.added} recommended investors to research queue`);
        }
        if (sb) {
          await sb.from('news_scans').insert({
            deal_id: deal.id,
            deal_name: deal.name,
            sector: deal.sector || null,
            grok_queries: scanResult.grokQueries || [],
            grok_raw_results: scanResult.grokRawResults || [],
            claude_summary: scanResult.summary || scanResult.notes || '',
            is_relevant_to_deal: scanResult.isRelevant === true,
            recommended_new_investors: scanResult.recommendedInvestors || [],
            actions_taken: actionsTaken,
            telegram_digest: scanResult.summary || '',
          }).catch(() => {});
        }

        const allFindings = Array.isArray(scanResult?.allFindings) ? scanResult.allFindings : [];
        const rejectedFindings = Array.isArray(scanResult?.rejectedFindings) ? scanResult.rejectedFindings : [];

        if (leads.length > 0) {
          const preview = leads.slice(0, 3).map(lead =>
            `- ${lead.firm_name}: ${String(lead.why_relevant || lead.news_event || '').slice(0, 110)}`
          ).join('\n');
          digestSections.push(
            `*${deal.name}*\n${scanResult.summary || `${leads.length} relevant investor-news signal${leads.length !== 1 ? 's' : ''} identified.`}\n${pipelineResult.added} added to pipeline${pipelineResult.batch ? ` · Batch ${pipelineResult.batch.batch_number}` : ''}\n${preview}`
          );
        } else {
          // Always show sector context — even if nothing was pipeline-worthy
          const sectorItems = allFindings.length ? allFindings : rejectedFindings;
          if (sectorItems.length > 0) {
            const preview = sectorItems.slice(0, 5).map(item =>
              `- ${item.firm_name ? `*${item.firm_name}*: ` : ''}${String(item.news_event || item.why_relevant || item.reason || '').slice(0, 100)}`
            ).join('\n');
            digestSections.push(
              `*${deal.name}*\n` +
              `${scanResult.rawSummary || `Sector scan complete — ${sectorItems.length} item${sectorItems.length !== 1 ? 's' : ''} reviewed.`}\n` +
              `_No investor leads met the pipeline bar today_\n${preview}`
            );
          } else {
            digestSections.push(
              `*${deal.name}*\n${scanResult.rawSummary || scanResult.summary || 'Sector scan ran — no news items found today.'}`
            );
          }
        }
      }
    } else {
      const generalLeads = await scanGeneralInvestorSignals(pushActivity).catch(() => []);
      const stored = await storeGeneralInvestorSignals(generalLeads, pushActivity).catch(() => ({ stored: 0, skipped: generalLeads.length }));

      if (generalLeads.length > 0) {
        const preview = generalLeads.slice(0, 3).map(lead =>
          `- ${lead.firm_name}: ${String(lead.why_relevant || lead.signal || '').slice(0, 110)}`
        ).join('\n');
        digestSections.push(
          `*No active deals today*\n${generalLeads.length} market signal${generalLeads.length !== 1 ? 's' : ''} found · ${stored.stored} stored to investors_db\n${preview}`
        );
      } else {
        digestSections.push('*No active deals today*\nNo useful investor market signals found from the persona-driven scan.');
      }
    }

    const portfolioSummary = deals.length > 0
      ? await summarizePortfolioNewsDigest(portfolioScanRows).catch(() => '')
      : '';

    const digestText =
      `🗞️ *ROCO Daily News Scan Digest*\n` +
      `_7am ET · ${estNow.toFormat('d LLL yyyy')}_\n\n` +
      (portfolioSummary ? `*Portfolio View*\n${portfolioSummary}\n\n` : '') +
      digestSections.join('\n\n');

    await sendTelegram(digestText).catch(() => {});
    pushActivity({
      type: 'system',
      action: 'Daily news scan digest sent to Dom via Telegram',
      note: deals.length ? `${deals.length} active deal scan${deals.length !== 1 ? 's' : ''}` : 'Persona-driven scan with no active deals',
    });

    dailyNewsScanState.set(todayKey, 'done');
  } catch (err) {
    dailyNewsScanState.delete(todayKey);
    throw err;
  }
}

async function runDailyActivityDigestCycle(deals) {
  const estNow = DateTime.now().setZone('America/New_York');
  const digestHour = Number(process.env.DAILY_ACTIVITY_DIGEST_HOUR_ET || 20);

  // Collect dates to process: today (if at digest hour) + any missing days from the past 7 days
  const datesToProcess = [];

  // Today's digest — only fire at the scheduled hour
  if (estNow.hour === digestHour) {
    datesToProcess.push(estNow.toISODate());
  }

  // Catch-up: check the past 7 days for any missing logs and backfill them
  const sb = getSupabase();
  if (sb) {
    for (let daysBack = 1; daysBack <= 7; daysBack++) {
      const pastDate = estNow.minus({ days: daysBack }).toISODate();
      if (dailyActivityDigestState.get(pastDate) === 'done') continue;
      try {
        const { data: existingRows } = await sb.from('daily_logs')
          .select('id').eq('log_date', pastDate).limit(1);
        if (existingRows?.length) { dailyActivityDigestState.set(pastDate, 'done'); continue; }
      } catch {}
      try {
        const { data: legacyRows } = await sb.from('daily_activity_reports')
          .select('id').eq('report_date', pastDate).limit(1);
        if (legacyRows?.length) { dailyActivityDigestState.set(pastDate, 'done'); continue; }
      } catch {}
      // Missing — add to backfill queue (skip today if already queued above)
      if (!datesToProcess.includes(pastDate)) datesToProcess.push(pastDate);
    }
  }

  if (!datesToProcess.length) return;

  for (const targetDate of datesToProcess) {
    const isCatchUp = targetDate !== estNow.toISODate();

    if (sb && targetDate && !isCatchUp) {
      try {
        const { data: existingRows } = await sb.from('daily_logs')
          .select('id, log_date, deal_id, telegram_voice_script')
          .eq('log_date', targetDate)
          .limit(1);
        if (existingRows?.length) {
          dailyActivityDigestState.set(targetDate, 'done');
          continue;
        }
      } catch {}

      try {
        const { data: legacyRows } = await sb.from('daily_activity_reports')
          .select('id, report_date')
          .eq('report_date', targetDate)
          .limit(1);
        if (legacyRows?.length) {
          dailyActivityDigestState.set(targetDate, 'done');
          continue;
        }
      } catch {}
    }

    const existingState = dailyActivityDigestState.get(targetDate);
    if (existingState === 'running' || existingState === 'done') continue;
    dailyActivityDigestState.set(targetDate, 'running');

    try {
      const reportReference = DateTime.fromISO(`${targetDate}T20:05:00`, { zone: 'America/New_York' });
      const report = await buildDailyActivityReport({ deals, reference: reportReference });
      let sentVoiceAt = null;
      let voiceName = null;

      // Only send voice note for today's report — not for catch-up backfills
      if (!isCatchUp) {
        try {
          const voiceNote = await renderDailyVoiceNoteFromText(report.voice_script || report.executive_summary || '');
          if (voiceNote?.filePath) {
            const sent = await sendTelegramVoiceNote(voiceNote.filePath, {
              caption: report.telegram_caption || `Daily voice log · ${report.report_date}`,
            }).catch(() => null);
            if (sent) {
              sentVoiceAt = new Date().toISOString();
              voiceName = voiceNote.voiceName || null;
            }
            if (fs.existsSync(voiceNote.filePath)) fs.unlinkSync(voiceNote.filePath);
          } else {
            pushActivity({
              type: 'warning',
              action: 'Daily voice log skipped',
              note: 'No ElevenLabs voice note was generated for the daily report',
            });
          }
        } catch (voiceErr) {
          pushActivity({
            type: 'warning',
            action: 'Daily voice log generation failed',
            note: voiceErr.message?.slice(0, 160),
          });
        }
      }

      await persistDailyActivityReport({
        ...report,
        voice_name: voiceName,
        actions_implemented: false,
        voice_note_sent_at: sentVoiceAt || null,
      }).catch(() => {});

      pushActivity({
        type: 'system',
        action: isCatchUp ? `Daily log backfilled for ${targetDate}` : 'Daily activity log generated',
        note: `${report.deals_covered || 0} deal${report.deals_covered === 1 ? '' : 's'} · ${report.activity_count || 0} activity events`,
      });

      dailyActivityDigestState.set(targetDate, 'done');
    } catch (err) {
      dailyActivityDigestState.delete(targetDate);
      if (!isCatchUp) throw err;
      warn(`[ANALYTICS] Failed to backfill daily log for ${targetDate}: ${err.message}`);
    }
  }
}

async function maybeTopUpApprovedBatch(deal, brainDirectives) {
  if (!deal?.id || brainDirectives?.allowResearch === false) return;

  const estNow = DateTime.now().setZone('America/New_York');
  const topUpKey = `${deal.id}:${estNow.toISODate()}`;
  if (approvedBatchTopUpState.get(topUpKey)) return;

  const sb = getSupabase();
  if (!sb) return;

  // Use invite-ready count at unblocked firms as the real pipeline health signal.
  // "Total contacts" overstates health — contacts waiting for invite-accepts block their firm
  // from getting fresh invites, so the real actionable count is contacts at NEW firms.
  const { count: inviteReadyCount } = await sb.from('contacts')
    .select('id', { count: 'exact', head: true })
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Ranked', 'Enriched'])
    .not('linkedin_url', 'is', null)
    .is('invite_sent_at', null);

  // How many of those are at truly unblocked firms (no one else there already engaged)?
  // Approximate this as: if invite-ready count is < daily target, we need fresh firms.
  const dailyTarget = deal.linkedin_daily_limit || DAILY_INVITE_TARGET;
  const pipelineGap = Math.max(0, dailyTarget * 3 - Number(inviteReadyCount || 0)); // need 3x buffer
  if (pipelineGap <= 0) {
    info(`[${deal.name}] Top-up skipped — ${inviteReadyCount} invite-ready contacts (≥ ${dailyTarget * 3} needed)`);
    return;
  }
  // Don't trigger a new Grok search if there are already firms queued for enrichment.
  // The enrichment pipeline will convert those to invite-ready contacts soon enough.
  const { count: pendingFirms } = await sb.from('batch_firms')
    .select('id', { count: 'exact', head: true })
    .eq('deal_id', deal.id)
    .in('status', ['pending', 'processing', 'researched', 'enriching']);
  if (Number(pendingFirms || 0) >= 10) {
    info(`[${deal.name}] Top-up skipped — ${pendingFirms} firms already in enrichment queue`);
    approvedBatchTopUpState.set(topUpKey, true); // don't recheck today
    return;
  }

  info(`[${deal.name}] Top-up triggered — only ${inviteReadyCount} invite-ready contacts, ${pendingFirms || 0} firms in queue (gap: ${pipelineGap})`);

  // Mark as done for today BEFORE searching — prevents repeated firing on empty results or restarts
  approvedBatchTopUpState.set(topUpKey, true);

  pushActivity({
    type: 'research',
    action: `Pipeline low — checking attached priority lists for ${deal.name}`,
    note: `${inviteReadyCount} invite-ready contacts · target gap ${pipelineGap}`,
    deal_name: deal.name,
    dealId: deal.id,
  });

  const priorityTopUp = await getPriorityListShortlist(deal, {
    limit: Math.min(20, pipelineGap),
    emitActivity: true,
  }).catch(() => ({ shortlisted: [], activePriorityList: null }));

  if (priorityTopUp.shortlisted?.length) {
    const inserted = await addPriorityInvestorsToBatch(deal, priorityTopUp.shortlisted, priorityTopUp.activePriorityList)
      .catch(() => ({ added: 0, skipped: priorityTopUp.shortlisted.length }));
    if (inserted.added > 0) {
      pushActivity({
        type: 'research',
        action: 'Priority-list top-up added fresh firms',
        note: `${deal.name} · ${inserted.added} firm${inserted.added === 1 ? '' : 's'} from "${priorityTopUp.activePriorityList?.list_name || 'priority list'}"`,
        deal_name: deal.name,
        dealId: deal.id,
      });
      return;
    }
  }

  const existingNames = await getExcludedFirmNames(deal.id);
  const grokLeads = await searchInvestorsWithGrok(deal, existingNames, pushActivity).catch(() => []);
  if (!grokLeads.length) return;

  const topUpLeads = grokLeads.slice(0, Math.min(20, pipelineGap)).map(lead => ({
    firm_name: lead.firm_name || lead.name,
    news_event: truncateForNote(lead.description || 'Fresh investor activity identified via Grok top-up scan', 160),
    why_relevant: truncateForNote(lead.description || `${lead.firm_name || lead.name} appears to fit ${deal.name}`, 180),
    urgency: 'medium',
    source_hint: 'Grok approved-batch top-up',
    investor_type: lead.investor_type || null,
    hq_country: lead.hq_country || null,
  }));

  const result = await addNewsLeadsToBatch(deal, topUpLeads).catch(() => ({ added: 0 }));
  if (result.added > 0) {
    pushActivity({
      type: 'research',
      action: 'Approved campaign top-up added fresh firms',
      note: `${deal.name} · ${result.added} firm${result.added === 1 ? '' : 's'} added while staying on target`,
      deal_name: deal.name,
      dealId: deal.id,
    });

    // Notify Telegram so we know what was found and why
    const firmLines = topUpLeads.slice(0, result.added).map((lead, i) => {
      const reason = String(lead.why_relevant || lead.news_event || '').slice(0, 100);
      return `${i + 1}. *${lead.firm_name}*${reason ? `\n   _${reason}_` : ''}`;
    }).join('\n');

    const msg = [
      `🔍 *Top-up: ${result.added} new firm${result.added === 1 ? '' : 's'} added — ${deal.name}*`,
      ``,
      `Pipeline ran low (${inviteReadyCount} invite-ready contacts). Found ${grokLeads.length} matches, adding the best ${result.added}:`,
      ``,
      firmLines,
      ``,
      `Enriching now → contacts will be queued for outreach once ready.`,
    ].join('\n');

    sendTelegram(msg).catch(() => {});
  }
}

function deepResearchOnlyForBatch() {
  const value = process.env.RESEARCH_DEEP_ONLY_FOR_BATCH;
  if (value == null || value === '') return true;
  return !['0', 'false', 'no', 'off'].includes(String(value).toLowerCase());
}

function isContactInsideBatch(contact, batch) {
  if (!contact?.created_at || !batch?.created_at) return false;
  return Date.parse(contact.created_at) >= Date.parse(batch.created_at);
}

function contactNeedsCoreResearch(contact) {
  return !contact?.past_investments || !contact?.investment_thesis || !contact?.sector_focus;
}

function hasResearchFailureMarker(notes) {
  if (typeof notes !== 'string') return false;
  return notes.includes('[PERSON_RESEARCH_FAILED]') || notes.includes('Research failed');
}

function shouldResearchContact(contact, deal, batch) {
  const scoreThreshold = Number(deal.min_investor_score || 60);
  const score = Number(contact.investor_score || 0);
  const inCurrentBatch = isContactInsideBatch(contact, batch);
  const missingCore = contactNeedsCoreResearch(contact);
  const freshResearch = hasFreshResearch(contact);
  const existingResearch = !!contact.person_researched ||
    hasCoreResearchFields(contact) ||
    (isResearched(contact.notes) && !hasResearchFailureMarker(contact.notes));

  if (freshResearch && !missingCore) return false;
  if (!existingResearch) return true;
  if (missingCore) return true;
  if (inCurrentBatch) return true;
  if (!deepResearchOnlyForBatch() && score >= scoreThreshold) return true;
  return false;
}

// ── Response analytics cache (refreshed every 30 min) ─────────────────────
// Tracks which investor profiles (by type) historically respond positively to
// outreach. Feeds a small scoring bonus for high-performing profiles.
let _responseSignalCache = null;
let _responseSignalCacheTime = 0;

async function getResponseSignals(sb) {
  if (_responseSignalCache && Date.now() - _responseSignalCacheTime < 30 * 60 * 1000) {
    return _responseSignalCache;
  }
  try {
    // Pull all contacts that have reached a terminal conversation state
    const { data } = await (sb || getSupabase())?.from('contacts')
      .select('investor_type, contact_type, hq_country, conversation_state')
      .not('conversation_state', 'is', null)
      .not('conversation_state', 'eq', 'awaiting_response')
      .not('conversation_state', 'eq', 'needs_reply') || {};

    if (!data?.length) return (_responseSignalCache = new Map());

    // Bucket by investor_type → count positive vs total
    const buckets = new Map();
    const POSITIVE_STATES = new Set(['meeting_booked', 'conversation_ended_positive', 'soft_yes', 'temp_closed']);

    for (const c of data) {
      const rawType = (c.investor_type || c.contact_type || '').toLowerCase().trim();
      if (!rawType || rawType === 'unknown') continue;
      // Normalise to broad category for reliable signal
      let bucket = 'other';
      if (/family.office|ffo|mfo/.test(rawType))                bucket = 'family office';
      else if (/private.equity|pe\/buyout|buyout/.test(rawType)) bucket = 'private equity';
      else if (/independent.sponsor|fundless/.test(rawType))     bucket = 'independent sponsor';
      else if (/venture.capital|vc|seed|early/.test(rawType))    bucket = 'venture capital';
      else if (/growth.equity|growth/.test(rawType))             bucket = 'growth equity';
      else if (/real.estate/.test(rawType))                      bucket = 'real estate';

      if (!buckets.has(bucket)) buckets.set(bucket, { total: 0, positive: 0 });
      const b = buckets.get(bucket);
      b.total++;
      if (POSITIVE_STATES.has(c.conversation_state)) b.positive++;
    }

    const signals = new Map();
    for (const [key, { total, positive }] of buckets.entries()) {
      if (total < 5) continue; // not enough data yet — don't bias scoring
      const rate = positive / total;
      // Max 5pts — only meaningful once we have real data volume
      const boost = rate >= 0.25 ? 5 : rate >= 0.18 ? 4 : rate >= 0.12 ? 3 : rate >= 0.07 ? 2 : rate >= 0.03 ? 1 : 0;
      signals.set(key, { boost, rate: Math.round(rate * 100), total });
    }

    _responseSignalCache = signals;
    _responseSignalCacheTime = Date.now();
    if (signals.size > 0) {
      console.log(`[RESPONSE SIGNALS] ${signals.size} profile buckets: ${[...signals.entries()].map(([k,v]) => `${k}=${v.rate}%(${v.total})`).join(', ')}`);
    }
    return signals;
  } catch (e) {
    console.warn('[RESPONSE SIGNALS] Failed to compute:', e.message);
    return _responseSignalCache || new Map();
  }
}

async function getFirmComparableContext(dealId, firmName) {
  const sb = getSupabase();
  if (!sb || !firmName) return null;
  const { data } = await sb.from('deal_investor_scores')
    .select('intelligence_boost, times_backed_similar, backed_companies')
    .eq('deal_id', dealId)
    .ilike('investor_name', `%${firmName.split(' ')[0]}%`)
    .order('intelligence_boost', { ascending: false })
    .limit(1)
    .maybeSingle();
  return data || null;
}

function toShortList(value, limit = 4) {
  if (Array.isArray(value)) return value.filter(Boolean).slice(0, limit);
  if (typeof value === 'string') {
    return value.split(/[;,\n]+/).map(v => v.trim()).filter(Boolean).slice(0, limit);
  }
  return [];
}

function parseMoneyToMillions(str) {
  if (!str) return null;
  const clean = String(str).replace(/[$,\s]/g, '').toUpperCase();
  const match = clean.match(/([\d.]+)([MBK]?)/);
  if (!match) return null;
  const value = parseFloat(match[1]);
  if (!Number.isFinite(value)) return null;
  if (match[2] === 'B') return value * 1000;
  if (match[2] === 'K') return value / 1000;
  return value;
}

// Normalise a deal's equity/target amount to $M
function normaliseDealAmountToM(deal) {
  const raw = deal.equity_required_usd_m || deal.equity || deal.target_amount || 0;
  const val = parseFloat(String(raw).replace(/[^0-9.-]/g, '')) || 0;
  return val > 10_000 ? val / 1_000_000 : val; // >10k means it was stored in dollars
}

// ── CONTROL MATCH EXPERT SCORING ─────────────────────────────────────────────
// Produces evidence-based control rationale citing real market data.
// Score range: -25 (hard mismatch) to +20 (documented match).
function scoreControlMatchExpert(investor, deal) {
  const dealPref   = (deal.investor_control_preference || 'majority').toLowerCase();
  const invPref    = (investor.control_preference || 'unknown').toLowerCase();
  const evidence   = investor.control_evidence || '';
  const confidence = (investor.control_confidence || 'low').toLowerCase();
  const invType    = (investor.investor_type || '').toLowerCase();
  const invName    = (investor.name || investor.firm_name || '').toLowerCase();
  const ebitda     = parseFloat(deal.ebitda_usd_m || deal.ebitda || 0);

  if (dealPref === 'either') {
    return { score: 0, rationale: 'Deal open to both majority and minority structures — control scoring not applied.', action: 'proceed' };
  }

  if (invPref === dealPref && confidence === 'high') {
    const s = dealPref === 'majority' ? 18 : 16;
    return {
      score: s,
      rationale: `Control match (${s}/20): ${evidence || 'Documented history of ' + dealPref + ' investments matches deal structure.'} For a deal at $${ebitda}M EBITDA, this investor is the correct structural fit — ` +
        (dealPref === 'majority'
          ? `majority buyout investors operate in the ${ebitda < 5 ? '$2M-$5M EBITDA lower middle market where they typically acquire at 5.5x-7.0x EBITDA (GF Data H1 2025)' : '$5M-$15M core LMM range at 6.5x-8.5x EBITDA'}.`
          : `minority growth investors partner without controlling the business — suited when the seller retains operational authority post-close.`),
      action: 'proceed',
    };
  }

  if (invPref === dealPref && confidence === 'medium') {
    return {
      score: 12,
      rationale: `Probable control match (12/20): ${evidence || 'Investor type and fund characteristics suggest ' + dealPref + ' preference.'} Medium confidence — no closed deal documentation found. Worth outreach but verify mandate before sending CIM.`,
      action: 'proceed',
    };
  }

  if (invPref === dealPref && confidence === 'low') {
    return {
      score: 6,
      rationale: `Weak control signal (6/20): ${evidence || 'Limited data available.'} Cannot confirm this investor actively does ${dealPref} deals. Outreach could clarify mandate — prioritise higher-confidence firms first.`,
      action: 'proceed',
    };
  }

  if (invPref === 'both') {
    const isFO = invType.includes('family office') || invName.includes('family');
    const s = isFO ? 12 : 9;
    return {
      score: s,
      rationale: `Flexible capital (${s}/20): ` +
        (isFO
          ? `Family office — no LP reporting mandate or blind pool restrictions. Closed 30% of LMM deals on Axial in 2025, up from 16% in 2024. Decision typically sits with 1-2 principals, faster to term sheet (often 2-4 weeks) than institutional PE. Comfortable with either majority buyout or minority co-invest — deal terms negotiated on the merits.`
          : `Investor has documented both majority and minority transactions. Verify current mandate and preferred deal size before outreach — IS capital providers closed only 1.5% of deals presented in some data sets (Ocean Avenue 2025), so mandate alignment is critical.`),
      action: 'proceed',
    };
  }

  if (!invPref || invPref === 'unknown') {
    return {
      score: 0,
      rationale: `Control preference unknown (0/20): No investment history or fund documentation found to classify this investor as majority or minority. Neutral — not penalised. Roco will attempt to classify during enrichment via firm website, SEC EDGAR ADV filing, and Grok web research.`,
      action: 'proceed',
    };
  }

  const hardMismatch =
    (dealPref === 'majority' && invPref === 'minority' && confidence === 'high') ||
    (dealPref === 'minority' && invPref === 'majority' && confidence === 'high');

  if (hardMismatch) {
    return {
      score: -25,
      rationale: `Hard control mismatch (-25/20): ${evidence || 'Confirmed ' + invPref + ' investor receiving a ' + dealPref + ' deal pitch.'} ` +
        (dealPref === 'majority' && invPref === 'minority'
          ? `This investor takes minority stakes and does not acquire controlling interests. A majority buyout pitch falls completely outside their mandate. Sending this will damage credibility with this contact.`
          : `This investor acquires controlling stakes and drives operational changes post-close. A minority stake pitch where the founder retains control does not match how this firm deploys capital.`) +
        ` Archiving to avoid reputational risk.`,
      action: 'archive',
    };
  }

  // Soft mismatch
  return {
    score: -12,
    rationale: `Soft control mismatch (-12/20): ${evidence || 'Investor type suggests ' + invPref + ' preference but no hard confirmation.'} Deal requires ${dealPref} investor. Probably the wrong structural fit. Could potentially co-invest as a minority alongside a lead majority buyer — but deprioritise in favour of confirmed ${dealPref} investors. Minority investments in IS deals typically price at a 10-15% discount to equivalent majority deals (Colonnade Advisors).`,
    action: 'proceed',
  };
}

function scoreFirmAgainstDeal(investor, deal, intelligenceBoost = 0, responseSignals = null) {
  let score = 0;
  const reasons = [];

  // ── Extract deal parameters ──────────────────────────────────────────────
  const dealType    = String(deal.raise_type || deal.deal_type || '').toLowerCase();
  const dealSector  = String(deal.sector || '').toLowerCase();
  const dealEquity  = normaliseDealAmountToM(deal);
  const dealEbitda  = parseFloat(deal.ebitda_usd_m || deal.ebitda || 0);
  const dealEV      = parseFloat(deal.enterprise_value_usd_m || deal.ev || 0);
  const targetGeo   = String(deal.target_geography || deal.geography || 'Global').toLowerCase();

  // ── Extract investor parameters — correct investors_db column names ───────
  const invType         = String(investor.investor_type || '').toLowerCase();
  const invPrefTypes    = String(investor.preferred_investment_types || '').toLowerCase();
  const invIndustries   = String(investor.preferred_industries || '').toLowerCase();
  const invVerticals    = String(investor.preferred_verticals || '').toLowerCase();
  const invOtherPrefs   = String(investor.other_preferences || '').toLowerCase();
  const invDesc         = String(investor.description || '').toLowerCase();
  const invPrefGeo      = String(investor.preferred_geographies || '').toLowerCase();
  const invCountry      = String(investor.hq_country || '').toLowerCase();
  const invRegion       = String(investor.hq_region || '').toLowerCase();
  const invAUM          = parseFloat(investor.aum_millions || 0) || 0;
  const prefInvMin      = parseFloat(investor.preferred_investment_amount_min) || null;
  const prefInvMax      = parseFloat(investor.preferred_investment_amount_max) || null;
  const prefDealMin     = parseFloat(investor.preferred_deal_size_min) || null;
  const prefDealMax     = parseFloat(investor.preferred_deal_size_max) || null;
  const prefEbitdaMin   = parseFloat(investor.preferred_ebitda_min) || null;
  const prefEbitdaMax   = parseFloat(investor.preferred_ebitda_max) || null;
  const inv12mo         = Number(investor.investments_last_12m) || 0;
  const inv6mo          = Number(investor.investments_last_6m)  || 0;
  const inv7d           = Number(investor.investments_last_7d)  || 0;
  const inv2y           = Number(investor.investments_last_2y)  || 0;
  const dryPowder       = parseFloat(investor.dry_powder_millions) || 0;
  const fundVintage     = parseInt(investor.last_closed_fund_vintage) || 0;
  const fundsOpen       = parseInt(investor.num_funds_open) || 0;
  const invStatus       = String(investor.investor_status || '').toLowerCase();

  // ── HARD DISQUALIFIERS ────────────────────────────────────────────────────
  const isPEDeal = /buyout|independent.sponsor|fundless|co.invest|lbo|private.equity/i.test(dealType);

  // Inactive / dissolved investors — skip entirely
  if (/inactive|dissolved|wound.down|liquidated|closed.fund/i.test(invStatus)) {
    reasons.push(`Hard disqualifier: investor status "${investor.investor_status}" — not actively deploying`);
    return { score: 0, scoring_breakdown: reasons, geo_match: false };
  }

  // Pure VC / seed / accelerator on a PE deal — incompatible mandate
  const isVCOnly = /venture capital|seed stage|early.stage|accelerator|incubator/i.test(invType) &&
    !/private equity|buyout|growth equity|family office/i.test(invType);
  if (isPEDeal && isVCOnly) {
    reasons.push(`Hard disqualifier: VC/seed mandate (${investor.investor_type}) is incompatible with ${deal.raise_type || 'PE'} deal`);
    return { score: 5, scoring_breakdown: reasons, geo_match: false };
  }

  // Mega-PE with stated minimum deal size far above this deal
  const statedMinDeal = prefDealMin || prefInvMin;
  if (statedMinDeal && dealEquity > 0 && dealEquity < statedMinDeal * 0.1) {
    reasons.push(`Hard disqualifier: minimum deal size $${statedMinDeal}M — deal equity $${dealEquity}M is <10% of that threshold`);
    return { score: 5, scoring_breakdown: reasons, geo_match: false };
  }

  // Hard geography kill: investor has explicit geo preferences that completely exclude deal geography
  // (Only apply if both investor geo and deal geo are unambiguous and non-global)
  const dealGeoStr = String(deal.target_geography || deal.geography || '').toLowerCase();
  const isGlobalDeal = !dealGeoStr || dealGeoStr.includes('global') || dealGeoStr.includes('worldwide');
  const invExplicitGeo = invPrefGeo && !invPrefGeo.includes('global') && !invPrefGeo.includes('worldwide');
  if (!isGlobalDeal && invExplicitGeo && invPrefGeo.length > 5) {
    // Check for complete disjoint — e.g. investor is Europe-only and deal is US
    const isUSDeal = /united states|usa|north america/i.test(dealGeoStr);
    const isEUInv  = /europe|european|germany|france|dach|nordic|benelux/i.test(invPrefGeo) &&
                     !/united states|north america|global/i.test(invPrefGeo);
    if (isUSDeal && isEUInv) {
      reasons.push(`Hard disqualifier: investor is Europe-only (${(investor.preferred_geographies||'').slice(0,60)}) — deal is US-based`);
      return { score: 3, scoring_breakdown: reasons, geo_match: false };
    }
  }

  // ── 1. INVESTOR TYPE + DEAL STRUCTURE (22 pts) ───────────────────────────
  // Ideal types per deal structure
  const peIdealTypes  = ['private equity', 'pe/buyout', 'buyout', 'family office', 'independent sponsor',
    'fundless sponsor', 'holding company', 'family investment office', 'endowment', 'pension fund',
    'sovereign wealth', 'asset manager'];
  const peBroadTypes  = ['growth equity', 'mezzanine', 'credit', 'debt', 'real assets', 'infrastructure',
    'hedge fund', 'fund of funds'];
  const vcTypes       = ['venture capital', 'angel', 'seed', 'accelerator', 'incubator', 'corporate venture'];

  let typeScore = 0;
  const invTypeText = `${invType} ${invPrefTypes}`;

  if (isPEDeal) {
    const idealHit = peIdealTypes.some(t => invTypeText.includes(t));
    const broadHit = peBroadTypes.some(t => invTypeText.includes(t));
    const vcHit    = vcTypes.some(t => invTypeText.includes(t));
    if (idealHit && !vcHit) {
      typeScore = 22;
      reasons.push(`Investor type ideal for PE/IS deal: ${investor.investor_type} (+22)`);
    } else if (broadHit && !vcHit) {
      typeScore = 13;
      reasons.push(`Investor type broadly compatible: ${investor.investor_type} (+13)`);
    } else if (!invType) {
      typeScore = 9;
      reasons.push('Investor type not specified — neutral (+9)');
    } else {
      typeScore = 3;
      reasons.push(`Investor type weak fit for PE deal: ${investor.investor_type} (+3)`);
    }

    // Deal structure preference bonus (up to +3 on top of type score)
    const isCoInvestDeal = /co.invest|co-invest|fundless|independent.sponsor/i.test(dealType);
    const isBuyoutDeal   = /buyout|lbo/i.test(dealType);
    if (isCoInvestDeal && /co.invest|co-invest|direct/i.test(invPrefTypes)) {
      typeScore = Math.min(typeScore + 3, 22);
      reasons.push('Explicitly prefers co-investments / direct deals — structure match bonus (+3)');
    } else if (isBuyoutDeal && /buyout|control|acquisition/i.test(invPrefTypes)) {
      typeScore = Math.min(typeScore + 2, 22);
      reasons.push('Explicitly prefers buyout / control deals — structure match bonus (+2)');
    }
  } else {
    // For non-PE deal types, score generically
    typeScore = invType ? 12 : 8;
    reasons.push(`Investor type: ${investor.investor_type || 'Not specified'} — generic scoring (+${typeScore})`);
  }
  score += typeScore;

  // ── 2. DEAL SIZE / CHECK SIZE (25 pts) ────────────────────────────────────
  // Priority: preferred_investment_amount → preferred_deal_size → preferred_ebitda → AUM inference
  let sizeScore = 0;

  if (prefInvMin != null && prefInvMax != null && dealEquity > 0) {
    if (dealEquity >= prefInvMin && dealEquity <= prefInvMax) {
      sizeScore = 25;
      reasons.push(`Equity $${dealEquity}M within preferred investment range $${prefInvMin}M–$${prefInvMax}M (+25)`);
    } else if (dealEquity >= prefInvMin * 0.4 && dealEquity <= prefInvMax * 2.5) {
      sizeScore = 15;
      reasons.push(`Equity $${dealEquity}M near preferred investment range $${prefInvMin}M–$${prefInvMax}M (+15)`);
    } else if (dealEquity < prefInvMin * 0.15 || dealEquity > prefInvMax * 6) {
      sizeScore = 0;
      reasons.push(`Size mismatch: equity $${dealEquity}M far outside preferred range $${prefInvMin}M–$${prefInvMax}M (+0)`);
    } else {
      sizeScore = 6;
      reasons.push(`Equity $${dealEquity}M outside but not far from preferred investment range (+6)`);
    }
  } else if (prefDealMin != null && prefDealMax != null && dealEV > 0) {
    if (dealEV >= prefDealMin && dealEV <= prefDealMax) {
      sizeScore = 25;
      reasons.push(`EV $${dealEV}M within preferred deal size $${prefDealMin}M–$${prefDealMax}M (+25)`);
    } else if (dealEV >= prefDealMin * 0.4 && dealEV <= prefDealMax * 2.5) {
      sizeScore = 15;
      reasons.push(`EV $${dealEV}M near preferred deal size $${prefDealMin}M–$${prefDealMax}M (+15)`);
    } else if (dealEV < prefDealMin * 0.15 || dealEV > prefDealMax * 6) {
      sizeScore = 0;
      reasons.push(`Deal size mismatch: EV $${dealEV}M far outside preferred range $${prefDealMin}M–$${prefDealMax}M (+0)`);
    } else {
      sizeScore = 6;
      reasons.push(`EV $${dealEV}M outside but not far from preferred deal size (+6)`);
    }
  } else if (prefEbitdaMin != null && prefEbitdaMax != null && dealEbitda > 0) {
    if (dealEbitda >= prefEbitdaMin && dealEbitda <= prefEbitdaMax) {
      sizeScore = 25;
      reasons.push(`EBITDA $${dealEbitda}M within preferred EBITDA range $${prefEbitdaMin}M–$${prefEbitdaMax}M (+25)`);
    } else if (dealEbitda >= prefEbitdaMin * 0.4 && dealEbitda <= prefEbitdaMax * 2.5) {
      sizeScore = 15;
      reasons.push(`EBITDA $${dealEbitda}M near preferred EBITDA range $${prefEbitdaMin}M–$${prefEbitdaMax}M (+15)`);
    } else {
      sizeScore = 3;
      reasons.push(`EBITDA $${dealEbitda}M outside preferred EBITDA range $${prefEbitdaMin}M–$${prefEbitdaMax}M (+3)`);
    }
  } else if (invAUM > 0 && dealEquity > 0) {
    // Infer from AUM: a typical PE/FO deploys 2–20% of AUM per deal
    const estMin = invAUM * 0.01;
    const estMax = invAUM * 0.2;
    if (dealEquity >= estMin && dealEquity <= estMax) {
      sizeScore = 14;
      reasons.push(`Deal size plausible vs AUM $${invAUM}M (estimated range $${Math.round(estMin)}M–$${Math.round(estMax)}M) (+14)`);
    } else if (dealEquity < estMin * 0.1 || dealEquity > estMax * 5) {
      sizeScore = 2;
      reasons.push(`Deal size likely misaligned vs AUM $${invAUM}M — equity $${dealEquity}M vs estimated range $${Math.round(estMin)}M–$${Math.round(estMax)}M (+2)`);
    } else {
      sizeScore = 8;
      reasons.push(`Deal size loosely consistent with AUM $${invAUM}M (+8)`);
    }
  } else {
    sizeScore = 10; // no size data available — neutral
    reasons.push('No preferred size data available — neutral (+10)');
  }
  score += sizeScore;

  // ── 3. SECTOR / THESIS DEPTH (25 pts) ────────────────────────────────────
  // Sub-sector: pull from deal fields if available (enriches matching depth)
  const dealSubSector = String(deal.sub_sector || deal.settings?.sub_sector || '').toLowerCase().trim();

  // Expand deal sector to related terms
  const sectorSynonyms = {
    healthcare: ['health', 'medical', 'pharma', 'pharmaceutical', 'biotech', 'life sciences',
      'clinical', 'hospital', 'therapeutics', 'diagnostics', 'dental', 'veterinary', 'wellness'],
    technology: ['tech', 'software', 'saas', 'cloud', 'digital', 'data', 'information technology',
      'cybersecurity', 'artificial intelligence', 'fintech', 'edtech', 'proptech'],
    manufacturing: ['industrial', 'factory', 'production', 'fabrication', 'machinery', 'aerospace', 'defense'],
    distribution: ['logistics', 'supply chain', 'warehouse', 'fulfillment', 'transport', 'freight', 'shipping'],
    'business services': ['b2b', 'professional services', 'consulting', 'staffing', 'outsourcing',
      'facility', 'managed services', 'hr services', 'marketing services'],
    'financial services': ['fintech', 'insurance', 'banking', 'asset management', 'wealth management', 'lending', 'payments'],
    'real estate': ['property', 'reit', 'commercial real estate', 'residential', 'multifamily'],
    consumer: ['retail', 'ecommerce', 'brand', 'cpg', 'food', 'beverage', 'restaurant', 'hospitality'],
    energy: ['oil', 'gas', 'renewables', 'cleantech', 'utilities', 'power', 'solar', 'wind'],
    education: ['edtech', 'training', 'learning', 'higher education', 'k-12'],
  };

  const primarySector = dealSector.split(/[\/,]+/)[0].trim();
  const expandedTerms = new Set();
  expandedTerms.add(primarySector);
  dealSector.split(/[\/,;]+/).map(s => s.trim()).filter(Boolean).forEach(s => expandedTerms.add(s));
  for (const [key, synonyms] of Object.entries(sectorSynonyms)) {
    if (primarySector.includes(key) || key.includes(primarySector)) {
      synonyms.forEach(s => expandedTerms.add(s));
    }
  }
  const relevantTerms = [...expandedTerms].filter(t => t.length > 3);

  const industryHits   = relevantTerms.filter(t => invIndustries.includes(t));
  const verticalHits   = relevantTerms.filter(t => invVerticals.includes(t));
  const descHits       = relevantTerms.filter(t => invDesc.includes(t));
  const otherPrefHits  = relevantTerms.filter(t => invOtherPrefs.includes(t));
  const hasIndustriesData = !!(invIndustries || invVerticals);

  // Sub-sector specificity bonus: if investor description/verticals mention the sub-sector by name,
  // that is a strong signal of precise thesis alignment (mirrors placement agent "comp fund" approach)
  const subSectorHit = dealSubSector && dealSubSector.length > 3 && (
    invIndustries.includes(dealSubSector) ||
    invVerticals.includes(dealSubSector) ||
    invDesc.includes(dealSubSector)
  );

  let sectorScore = 0;
  if (industryHits.length >= 2 || (industryHits.length >= 1 && verticalHits.length >= 1)) {
    sectorScore = 25;
    const matched = [...new Set([...industryHits, ...verticalHits])].slice(0, 3);
    reasons.push(`Strong sector match: ${matched.join(', ')} (+25)`);
  } else if (industryHits.length === 1) {
    sectorScore = 18;
    reasons.push(`Sector match in preferred industries: ${industryHits[0]} (+18)`);
  } else if (verticalHits.length >= 1) {
    sectorScore = 15;
    reasons.push(`Sector match in preferred verticals: ${verticalHits[0]} (+15)`);
  } else if (descHits.length >= 2) {
    sectorScore = 12;
    reasons.push(`Sector mentioned in description: ${descHits.slice(0, 2).join(', ')} (+12)`);
  } else if (descHits.length === 1 || otherPrefHits.length >= 1) {
    sectorScore = 7;
    reasons.push(`Weak sector signal in investor profile: ${(descHits[0] || otherPrefHits[0])} (+7)`);
  } else if (!hasIndustriesData && !invDesc) {
    sectorScore = 8;
    reasons.push('No sector preference data — neutral (+8)');
  } else {
    sectorScore = 0;
    reasons.push(`No ${primarySector} sector alignment found in investor data (+0)`);
  }

  // Sub-sector precision bonus: investor's focus explicitly matches the sub-sector (up to +3)
  if (subSectorHit && sectorScore > 0 && sectorScore < 25) {
    sectorScore = Math.min(sectorScore + 3, 25);
    reasons.push(`Sub-sector precision match: "${dealSubSector}" explicitly in investor focus (+3)`);
  }

  score += sectorScore;

  // ── 4. GEOGRAPHY (13 pts) ─────────────────────────────────────────────────
  const geoAliases = {
    'united states': ['united states', 'north america', 'americas', 'us', 'usa', 'u.s.'],
    us:              ['united states', 'north america', 'americas', 'us', 'usa', 'u.s.'],
    usa:             ['united states', 'north america', 'americas', 'us', 'usa'],
    'north america': ['north america', 'united states', 'canada', 'americas', 'us', 'usa'],
    'united kingdom':['united kingdom', 'uk', 'great britain', 'europe', 'western europe'],
    uk:              ['united kingdom', 'uk', 'great britain', 'europe', 'western europe'],
    europe:          ['europe', 'western europe', 'european union', 'uk', 'dach', 'benelux'],
    uae:             ['uae', 'united arab emirates', 'middle east', 'mena', 'gcc'],
    'middle east':   ['middle east', 'mena', 'gcc', 'uae', 'saudi', 'gulf'],
    global:          [],
  };

  let geoMatch = false;
  let geoScore = 0;
  const targetGeos = targetGeo.split(',').map(v => v.trim()).filter(Boolean);

  if (targetGeos.includes('global') || !targetGeos.length) {
    geoScore = 10; geoMatch = true;
    reasons.push('Global mandate — any geography accepted (+10)');
  } else {
    const matchTerms = new Set(targetGeos.flatMap(g => geoAliases[g] || [g]));
    const hqMatch   = invCountry && [...matchTerms].some(t => invCountry.includes(t));
    const regMatch  = invRegion  && [...matchTerms].some(t => invRegion.includes(t));
    const prefMatch = invPrefGeo && ([...matchTerms].some(t => invPrefGeo.includes(t)) ||
      invPrefGeo.includes('global') || invPrefGeo.includes('worldwide'));

    geoMatch = hqMatch || regMatch || prefMatch;

    if (hqMatch) {
      geoScore = 13;
      reasons.push(`HQ in target region: ${investor.hq_country} (+13)`);
    } else if (prefMatch) {
      geoScore = 10;
      reasons.push(`Preferred geography covers target: ${(investor.preferred_geographies || '').slice(0, 60)} (+10)`);
    } else if (regMatch) {
      geoScore = 7;
      reasons.push(`Region overlaps target: ${investor.hq_region} (+7)`);
    } else if (!invCountry && !invPrefGeo) {
      geoScore = 5;
      reasons.push('Geography not specified — neutral (+5)');
    } else {
      geoScore = 0;
      reasons.push(`Geographic mismatch: ${investor.hq_country || 'Unknown HQ'} vs target ${deal.target_geography || deal.geography || 'Unknown'} (+0)`);
    }
  }
  score += geoScore;

  // ── 5. ACTIVITY VELOCITY (8 pts) — is this investor actively deploying? ───
  // Professional signal: not just how many deals, but whether pace is accelerating.
  // Recent 7-day deal = at peak deployment. Velocity = 12m vs prior-year annualised rate.
  const annualised2y  = inv2y > 0 ? inv2y / 2 : 0;
  const velocity      = inv12mo > 0 && annualised2y > 0 ? inv12mo / annualised2y : null;

  let activityScore = 0;
  if (inv7d > 0) {
    activityScore = 8;
    reasons.push(`Just invested (${inv7d} deal(s) in last 7 days) — at peak deployment velocity (+8)`);
  } else if (inv12mo >= 5 && velocity && velocity >= 1.2) {
    activityScore = 8;
    reasons.push(`Very active and accelerating: ${inv12mo} deals/12m (${velocity.toFixed(1)}x pace vs prior year) (+8)`);
  } else if (inv12mo >= 5) {
    activityScore = 6;
    reasons.push(`Very active: ${inv12mo} investments in last 12 months (+6)`);
  } else if (inv12mo >= 2 && velocity && velocity >= 1.0) {
    activityScore = 5;
    reasons.push(`Active at steady pace: ${inv12mo} deals/12m (+5)`);
  } else if (inv12mo >= 2) {
    activityScore = 3;
    reasons.push(`Active: ${inv12mo} investments in last 12 months (+3)`);
  } else if (inv12mo >= 1) {
    activityScore = 2;
    reasons.push(`Some recent activity: ${inv12mo} deal(s) in last 12 months (+2)`);
  } else if (inv6mo >= 1) {
    activityScore = 3;
    reasons.push(`Recent deal in last 6 months — currently deploying (+3)`);
  } else {
    reasons.push('No recent investment activity data available (+0)');
  }
  score += activityScore;

  // ── 6. FUND TIMING & DRY POWDER (7 pts) — the pacing cycle signal ─────────
  // Professional context: an LP who just closed a new fund is actively deploying.
  // Dry powder = capital available now. Fund vintage age = deployment cycle stage.
  // (Source: professional placement agents prioritise recently-closed funds heavily)
  const currentYear   = new Date().getFullYear();
  const vintageAge    = fundVintage > 1990 ? currentYear - fundVintage : null;

  let timingScore = 0;
  if (dryPowder > 0 && vintageAge !== null && vintageAge <= 3) {
    timingScore = 7;
    reasons.push(`Recent fund (${fundVintage}) with $${Math.round(dryPowder)}M dry powder — prime deployment window (+7)`);
  } else if (dryPowder > 0 && vintageAge !== null && vintageAge <= 5) {
    timingScore = 6;
    reasons.push(`Fund (${fundVintage}) with $${Math.round(dryPowder)}M dry powder still deploying (+6)`);
  } else if (dryPowder > 0) {
    timingScore = 5;
    reasons.push(`Stated dry powder $${Math.round(dryPowder)}M — capital available to deploy (+5)`);
  } else if (vintageAge !== null && vintageAge <= 2) {
    timingScore = 6;
    reasons.push(`Very recent fund close (${fundVintage}) — in active early deployment window (+6)`);
  } else if (vintageAge !== null && vintageAge <= 4) {
    timingScore = 4;
    reasons.push(`Recent fund vintage (${fundVintage}) — likely still within deployment period (+4)`);
  } else if (fundsOpen > 0) {
    timingScore = 4;
    reasons.push(`${fundsOpen} open fund(s) — currently in active fundraising/deployment (+4)`);
  } else if (vintageAge !== null && vintageAge <= 7) {
    timingScore = 1;
    reasons.push(`Fund vintage ${fundVintage} — likely in late deployment or harvesting stage (+1)`);
  } else {
    reasons.push('No fund timing data available — cannot assess deployment cycle (+0)');
  }
  score += timingScore;

  // ── Historical response signal (from live outreach data) ─────────────────
  // As Roco accumulates outreach history, this tracks which investor profile types
  // actually respond positively. Adds up to 5pts for high-performing profile types.
  if (responseSignals?.size > 0) {
    let responseBucket = null;
    if (/family.office|ffo|mfo/.test(invType))                responseBucket = 'family office';
    else if (/private.equity|pe\/buyout|buyout/.test(invType)) responseBucket = 'private equity';
    else if (/independent.sponsor|fundless/.test(invType))     responseBucket = 'independent sponsor';
    else if (/venture.capital|vc|seed|early/.test(invType))    responseBucket = 'venture capital';
    else if (/growth.equity|growth/.test(invType))             responseBucket = 'growth equity';
    else if (/real.estate/.test(invType))                      responseBucket = 'real estate';

    const sig = responseBucket ? responseSignals.get(responseBucket) : null;
    if (sig && sig.boost > 0) {
      score = Math.min(score + sig.boost, 100);
      reasons.push(`Response analytics: ${sig.rate}% positive rate for ${responseBucket} (${sig.total} data points) (+${sig.boost})`);
    }
  }

  // ── Intelligence boost from comparable deal history ───────────────────────
  // This is the strongest signal: an investor who has backed comparable deals
  // has already demonstrated thesis alignment and appetite for this sector/structure.
  if (intelligenceBoost > 0) {
    score = Math.min(score + intelligenceBoost, 100);
    reasons.push(`Comparable deal intelligence boost: +${intelligenceBoost} (backed similar deals in history)`);
  }

  // ── 7. CONTROL MATCH (expert scoring) ────────────────────────────────────
  const controlMatch = scoreControlMatchExpert(investor, deal);
  const controlScore = Number(controlMatch.score) || 0;
  if (controlScore !== 0) {
    score = Math.max(0, Math.min(100, score + controlScore));
    reasons.push(`Control: ${controlMatch.rationale.slice(0, 200)}`);
  }

  return {
    score: Math.round(Math.min(score, 100)),
    scoring_breakdown: reasons,
    geo_match: geoMatch,
    control_action: controlMatch.action || 'proceed',
    control_rationale: controlMatch.rationale || null,
  };
}

async function generateJustification(firm, deal, scoringBreakdown = []) {
  try {
    const comparable = await getFirmComparableContext(deal.id, firm.firm_name);
    const rankValue = Number(firm.rank || 0) || null;
    const rankContext = rankValue ? `Current batch rank: #${rankValue} out of 100.` : 'Current batch rank is not assigned yet.';

    // Build fund timing context from investor data
    const currentYear = new Date().getFullYear();
    const fundVintage = parseInt(firm.last_closed_fund_vintage) || 0;
    const vintageAge  = fundVintage > 1990 ? currentYear - fundVintage : null;
    const dryPowder   = parseFloat(firm.dry_powder_millions) || 0;
    const inv12mo     = Number(firm.investments_last_12m) || 0;
    const inv2y       = Number(firm.investments_last_2y) || 0;
    const inv7d       = Number(firm.investments_last_7d) || 0;
    const velocity    = inv12mo > 0 && inv2y > 0 ? (inv12mo / (inv2y / 2)).toFixed(1) : null;

    // Extract the top-scoring signals from breakdown for the ranked batch narrative
    const positiveSignals = scoringBreakdown.filter(r =>
      !r.toLowerCase().includes('mismatch') &&
      !r.toLowerCase().includes('disqualifier') &&
      !r.toLowerCase().includes('+0)') &&
      !r.toLowerCase().includes('neutral')
    );
    const concerns = scoringBreakdown.filter(r =>
      r.toLowerCase().includes('mismatch') ||
      r.toLowerCase().includes('+0)') ||
      r.toLowerCase().includes('outside') ||
      r.toLowerCase().includes('no ') ||
      r.toLowerCase().includes('weak')
    );
    // Extract response analytics line if present
    const responseLine = scoringBreakdown.find(r => r.toLowerCase().includes('response analytics:'));

    const prompt = `You are a senior placement agent writing an evidence-based investor fit assessment for a campaign approval package. Your job is to explain WHY this firm earned its place in the ranked top-100 batch, citing specific evidence from the data. Be objective. State mismatches and data gaps as clearly as strengths. Do not sell or persuade. No em dashes. No superlatives.

DEAL:
Type: ${deal.raise_type || deal.deal_type || 'Unknown'}
Sector: ${deal.sector || 'Unknown'}
EBITDA: $${deal.ebitda_usd_m || deal.ebitda || 'Unknown'}M  |  EV: $${deal.enterprise_value_usd_m || deal.ev || 'Unknown'}M
Equity Required: $${deal.equity_required_usd_m || deal.equity || deal.target_amount || 'Unknown'}${deal.target_amount ? '' : 'M'}
Geography: ${deal.target_geography || deal.geography || 'Not specified'}

INVESTOR PROFILE:
Type: ${firm.investor_type || firm.contact_type || 'Unknown'}
HQ: ${[firm.hq_city, firm.hq_state, firm.hq_country].filter(Boolean).join(', ') || 'Not specified'}
AUM: ${firm.aum || firm.aum_millions ? `$${firm.aum || firm.aum_millions}M` : 'Not disclosed'}
Preferred Investment Size: ${firm.preferred_investment_amount_min && firm.preferred_investment_amount_max ? `$${firm.preferred_investment_amount_min}M - $${firm.preferred_investment_amount_max}M` : firm.preferred_deal_size_min ? `$${firm.preferred_deal_size_min}M - $${firm.preferred_deal_size_max}M (deal size)` : 'Not specified'}
Preferred Sectors: ${firm.preferred_industries || 'Not specified'}${firm.preferred_verticals ? ` | Verticals: ${firm.preferred_verticals}` : ''}
Preferred Geography: ${firm.preferred_geographies || 'Not specified'}
Recent Activity: ${inv7d > 0 ? `${inv7d} deal(s) in last 7 days` : `${inv12mo} deals in last 12 months`}${velocity ? ` (${velocity}x pace vs prior year)` : ''}
Fund Timing: ${dryPowder > 0 ? `$${Math.round(dryPowder)}M dry powder confirmed` : vintageAge !== null ? `Last fund vintage ${fundVintage} (${vintageAge} years ago)` : 'No fund timing data'}
Investment Thesis: ${(firm.thesis || '').slice(0, 500)}
Past Investments: ${toShortList(firm.past_investments, 5).join(', ') || 'None recorded'}
Comparable-Deal History: ${comparable?.times_backed_similar ? `BACKED ${comparable.times_backed_similar} comparable deal(s): ${toShortList(comparable?.backed_companies, 4).join(', ')}` : 'No comparable-deal history in database'}
${responseLine ? `Historical Response Data: ${responseLine.replace(/^response analytics:\s*/i, '')}` : ''}
RANK CONTEXT: ${rankContext}
SCORING SIGNALS FIRED (Score: ${firm.score}/100):
Positive: ${positiveSignals.slice(0, 6).join(' | ') || 'None'}
Concerns: ${concerns.slice(0, 3).join(' | ') || 'None identified'}

Write 5-6 sentences structured as follows:
1. Score, current batch rank if available, and the primary reason this firm made the ranked top-100 list.
2. Investor type and mandate alignment — does it match or not, and what is the evidence.
3. Deal size / check size compatibility — cite the specific numbers that align or diverge.
4. Sector and thesis alignment — what in their stated strategy matches this deal's sector.
5. Activity and fund timing signal — are they actively deploying capital right now, evidence.${responseLine ? ' If historical response data is present, include the response rate as supporting context.' : ''}
6. Concerns or data gaps — be direct about what is unknown or does not align.

No bullet points. No headers. Plain prose only. No em dashes. No superlatives.`.trim();

    const text = await haikuComplete(prompt, { maxTokens: 380 });
    return text?.trim() || null;
  } catch (e) {
    return null;
  }
}

async function triggerCampaignReview(deal, batch) {
  const sb = getSupabase();
  if (!sb) return;

  const { data: claimedBatch } = await sb.from('campaign_batches')
    .update({
      status: 'pending_approval',
      updated_at: new Date().toISOString(),
    })
    .eq('id', batch.id)
    .eq('status', 'researching')
    .select('id')
    .maybeSingle();

  if (!claimedBatch) return;

  await rankBatchFirms(batch.id, deal);
  const reviewedCount = await updateBatchResearchCount(batch.id);
  await sb.from('campaign_batches')
    .update({ ranked_firms: reviewedCount, updated_at: new Date().toISOString() })
    .eq('id', batch.id);

  let { data: firms } = await sb.from('batch_firms')
    .select('id, firm_name, justification, thesis, past_investments, aum, score, contact_type, investor_id, rank')
    .eq('batch_id', batch.id)
    .order('rank', { ascending: true })
    .order('score', { ascending: false })
    .limit(BATCH_FIRM_TARGET);

  if (!firms?.length) {
    const { data: fallbackFirms } = await sb.from('batch_firms')
      .select('id, firm_name, justification, thesis, past_investments, aum, score, contact_type, investor_id, rank')
      .eq('batch_id', batch.id)
      .order('score', { ascending: false })
      .limit(BATCH_FIRM_TARGET);
    firms = fallbackFirms || [];
  }
  firms = [...(firms || [])].sort((a, b) => Number(a.rank || 9999) - Number(b.rank || 9999));

  for (const firm of (firms || [])) {
    if (firm.justification) continue;

    // Enrich with full investor DB record for richer justification
    let enrichedFirm = { ...firm };
    if (firm.investor_id) {
      const { data: invRecord } = await sb.from('investors_db')
        .select('investor_type, preferred_industries, preferred_verticals, preferred_geographies, preferred_investment_amount_min, preferred_investment_amount_max, preferred_deal_size_min, preferred_deal_size_max, preferred_ebitda_min, preferred_ebitda_max, hq_city, hq_state, hq_country, aum_millions, investments_last_12m, investments_last_2y, investments_last_7d, dry_powder_millions, last_closed_fund_vintage, num_funds_open')
        .eq('id', firm.investor_id)
        .maybeSingle();
      if (invRecord) enrichedFirm = { ...firm, ...invRecord };
    } else {
      // Try name-based lookup as fallback
      const { data: nameMatch } = await sb.from('investors_db')
        .select('investor_type, preferred_industries, preferred_verticals, preferred_geographies, preferred_investment_amount_min, preferred_investment_amount_max, preferred_deal_size_min, preferred_deal_size_max, hq_city, hq_state, hq_country, aum_millions, investments_last_12m, investments_last_2y, investments_last_7d, dry_powder_millions, last_closed_fund_vintage')
        .ilike('name', `%${(firm.firm_name || '').split(' ')[0]}%`)
        .limit(1)
        .maybeSingle();
      if (nameMatch) enrichedFirm = { ...firm, ...nameMatch };
    }

    const boost = await getIntelligenceBoostForFirm(deal.id, firm.firm_name);
    const boostValue = Number(boost || 0);
    const regenSignals = await getResponseSignals(null);
    const { scoring_breakdown: regen_breakdown } = scoreFirmAgainstDeal(enrichedFirm, deal, boostValue, regenSignals);

    const justification = await generateJustification(enrichedFirm, deal, regen_breakdown);
    await sb.from('batch_firms')
      .update({ justification: justification || `Matches ${deal.sector || 'deal'} mandate` })
      .eq('id', firm.id);
  }

  const topFirm = firms?.[0];
  pushActivity({
    type: 'system',
    action: `Batch ${batch.batch_number} complete — ${reviewedCount} firms ranked and ready for review`,
    note: `${deal.name} · Top firm: ${topFirm?.firm_name || 'Unknown'} (Score ${topFirm?.score || 0}/100) · Approval required before outreach`,
    deal_name: deal.name,
    dealId: deal.id,
  });

  console.log(`[BATCH] Batch #${batch.batch_number} for ${deal.name} → pending_approval (${reviewedCount} firms)`);
}

async function closeBatchForDeal(dealId) {
  const sb = getSupabase();
  if (!sb) return { error: 'No DB connection' };

  const { data: deal } = await sb.from('deals').select('*').eq('id', dealId).single();
  const { data: approvedBatch } = await sb.from('campaign_batches')
    .select('id, batch_number')
    .eq('deal_id', dealId)
    .eq('status', 'approved')
    .limit(1)
    .single();

  if (approvedBatch) {
    await sb.from('campaign_batches')
      .update({ status: 'completed', closed_at: new Date().toISOString(), updated_at: new Date().toISOString() })
      .eq('id', approvedBatch.id);
    await sb.from('batch_firms')
      .update({ status: 'completed' })
      .eq('batch_id', approvedBatch.id);
  }

  pushActivity({
    type: 'system',
    action: `Batch ${approvedBatch?.batch_number || ''} closed — next batch will begin automatically`,
    note: `${deal?.name} · Research will start for the next ${BATCH_FIRM_TARGET} firms on the next cycle`,
    deal_name: deal?.name,
    dealId,
  });

  return {
    closed: approvedBatch?.batch_number || null,
    promoted: null,
    building: null,
    buildingProgress: null,
  };
}

async function getDealExclusions(dealId) {
  const sb = getSupabase();
  if (!sb) return new Set();
  const excluded = new Set();

  const { data } = await sb.from('deal_exclusions')
    .select('firm_name')
    .eq('deal_id', dealId);
  for (const entry of (data || [])) {
    const firmName = (entry.firm_name || '').toLowerCase().trim();
    if (firmName) excluded.add(firmName);
  }

  try {
    const { data: exclusionLists } = await sb.from('investor_lists')
      .select('id')
      .eq('is_exclusion_list', true);
    const exclusionListIds = (exclusionLists || []).map(list => list.id).filter(Boolean);
    if (exclusionListIds.length) {
      const { data: excludedRows } = await sb.from('investors_db')
        .select('name')
        .in('list_id', exclusionListIds);
      for (const row of (excludedRows || [])) {
        const firmName = (row.name || '').toLowerCase().trim();
        if (firmName) excluded.add(firmName);
      }
    }
  } catch (err) {
    console.warn('[EXCLUSIONS] exclusion list load failed:', err.message);
  }

  return excluded;
}

function normalizeStringArray(values) {
  if (Array.isArray(values)) return values.map(v => String(v || '').trim()).filter(Boolean);
  if (!values) return [];
  return String(values).split(',').map(v => v.trim()).filter(Boolean);
}

function normalizeDealType(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function listTypeBaseScore(listType) {
  switch (String(listType || '').toLowerCase()) {
    case 'deal_specific':
      return 0;
    case 'comparable_deals':
      return 10;
    case 'standing':
      return 20;
    case 'news_research':
      return 30;
    case 'manual':
      return 35;
    default:
      return 40;
  }
}

export async function getOrderedListsForDeal(deal) {
  const sb = getSupabase();
  if (!sb) return [];

  const [
    { data: lists },
    { data: linkedPriorityLists },
  ] = await Promise.all([
    sb.from('investor_lists')
      .select('*')
      .neq('list_type', 'knowledge_base')
      .order('priority_order', { ascending: true }),
    sb.from('deal_list_priorities')
      .select('list_id, priority_order, status')
      .eq('deal_id', deal.id)
      .not('status', 'eq', 'exhausted'),
  ]);

  if (!lists?.length) return [];

  const linkedMap = new Map((linkedPriorityLists || []).map(list => [String(list.list_id), list]));
  const dealSector = String(deal.sector || '').toLowerCase().trim();
  const dealType = normalizeDealType(deal.deal_type || deal.raise_type || deal.type || '');

  const counts = await Promise.all(lists.map(async list => {
    const { count } = await sb.from('investors_db')
      .select('id', { count: 'exact', head: true })
      .eq('list_id', list.id);
    return [String(list.id), count || 0];
  }));
  const countMap = new Map(counts);

  return lists
    .filter(list => list.is_exclusion_list !== true)
    .map(list => {
      const listSectors = normalizeStringArray(list.sectors).map(s => s.toLowerCase());
      const listDealTypes = normalizeStringArray(list.deal_types).map(normalizeDealType);
      const linked = linkedMap.get(String(list.id));

      let relevanceScore = Number(list.priority_order ?? 99);
      relevanceScore += listTypeBaseScore(list.list_type);

      if (linked) {
        relevanceScore -= 50;
        relevanceScore += Number(linked.priority_order || 0);
      }

      if (dealSector && listSectors.some(s => dealSector.includes(s) || s.includes(dealSector))) {
        relevanceScore -= 15;
      }
      if (dealType && listDealTypes.some(t => dealType.includes(t) || t.includes(dealType))) {
        relevanceScore -= 10;
      }

      const successRate = Number(list.success_rate || 0);
      if (successRate > 10) relevanceScore -= 3;
      if (successRate > 20) relevanceScore -= 5;

      return {
        ...list,
        investor_count: countMap.get(String(list.id)) || 0,
        linked_priority_order: linked?.priority_order ?? null,
        relevanceScore,
      };
    })
    .sort((a, b) => a.relevanceScore - b.relevanceScore);
}

async function updateListStats(listRef, dealId, hadPositiveReply) {
  const sb = getSupabase();
  if (!sb || !listRef) return;

  let list = null;
  if (listRef.id) {
    const response = await sb.from('investor_lists')
      .select('id, name, use_count, success_rate')
      .eq('id', listRef.id)
      .maybeSingle()
      .then(r => r, () => ({ data: null }));
    list = response?.data || null;
  }

  if (!list && listRef.name) {
    const response = await sb.from('investor_lists')
      .select('id, name, use_count, success_rate')
      .ilike('name', `%${String(listRef.name).split(' ')[0]}%`)
      .limit(1)
      .maybeSingle()
      .then(r => r, () => ({ data: null }));
    list = response?.data || null;
  }

  if (!list) return;

  await sb.from('investor_lists')
    .update({ use_count: (list.use_count || 0) + 1 })
    .eq('id', list.id);

  if (hadPositiveReply !== undefined) {
    const newRate = hadPositiveReply
      ? Math.min(100, Number(list.success_rate || 0) + 1)
      : Number(list.success_rate || 0);
    await sb.from('investor_lists')
      .update({ success_rate: newRate })
      .eq('id', list.id);
  }
}

async function refreshInvestorListSuccessRates(sb) {
  if (!sb) return;
  const { data: lists } = await sb.from('investor_lists')
    .select('id, name')
    .neq('list_type', 'knowledge_base');
  if (!lists?.length) return;

  for (const list of lists) {
    const { data: firms } = await sb.from('batch_firms')
      .select('firm_name, deal_id')
      .eq('source_list', list.name);
    if (!firms?.length) {
      await sb.from('investor_lists').update({ success_rate: 0 }).eq('id', list.id);
      continue;
    }

    let positiveCount = 0;
    for (const firm of firms) {
      const firmKeyword = String(firm.firm_name || '').split(' ')[0];
      if (!firmKeyword) continue;
      const { data: contacts } = await sb.from('contacts')
        .select('pipeline_stage, response_received')
        .eq('deal_id', firm.deal_id)
        .ilike('company_name', `%${firmKeyword}%`)
        .eq('response_received', true)
        .limit(20);
      const hasPositiveReply = (contacts || []).some(contact => {
        const stage = String(contact.pipeline_stage || '').toLowerCase();
        return !['not interested', 'inactive', 'ghosted', 'suppressed — opt out', 'suppressed - opt out'].includes(stage);
      });
      if (hasPositiveReply) positiveCount += 1;
    }

    const successRate = firms.length ? Number(((positiveCount / firms.length) * 100).toFixed(1)) : 0;
    await sb.from('investor_lists').update({ success_rate: successRate }).eq('id', list.id);
  }
}

async function syncListLearningsToDealMemory(sb) {
  if (!sb) return;
  const { data: topLists } = await sb.from('investor_lists')
    .select('name, success_rate, deal_types, sectors, use_count')
    .neq('list_type', 'knowledge_base')
    .order('success_rate', { ascending: false })
    .order('use_count', { ascending: false })
    .limit(3);
  if (!topLists?.length) return;

  const learnings = topLists.map(list => ({
    list_name: list.name,
    learning: `${Number(list.success_rate || 0)}% response rate, best for ${normalizeStringArray(list.sectors).join(', ') || 'general sectors'} and ${normalizeStringArray(list.deal_types).join(', ') || 'general deal types'}`,
  }));

  const { data: activeDeals } = await sb.from('deals')
    .select('id')
    .eq('status', 'active');

  for (const deal of (activeDeals || [])) {
    await writeMemory(deal.id, {
      list_learnings: learnings,
      best_list_for_this_deal: learnings[0]?.list_name || '',
    }).catch(() => {});
  }
}

async function getIntelligenceBoostForFirm(dealId, firmName) {
  const sb = getSupabase();
  if (!sb || !firmName) return 0;
  const { data } = await sb.from('deal_investor_scores')
    .select('intelligence_boost')
    .eq('deal_id', dealId)
    .ilike('investor_name', `%${firmName.split(' ')[0]}%`)
    .order('intelligence_boost', { ascending: false })
    .limit(1)
    .maybeSingle();
  return Number(data?.intelligence_boost || 0);
}

async function getCandidateFirms(deal, existingNames, exclusions, limit = 80) {
  const sb = getSupabase();
  if (!sb) return [];
  const threshold = Number(deal.min_investor_score || 60);
  const candidates = [];
  const seenIds = new Set();
  const intelligenceBoostByName = new Map();

  // ── Load Knowledge Base enrichment map ──────────────────────────────────
  // Priority: deal-specific KB → global KB lists (all knowledge_base lists in DB).
  // KB records fill in empty/null fields in investor records before scoring.
  const kbEnrichmentMap = new Map(); // lowercased name → KB record

  const loadKBRecords = async (listIdFilter) => {
    const KB_PAGE = 1000;
    let kbFrom = 0;
    while (true) {
      let q = sb.from('investors_db')
        .select('name, investor_type, preferred_industries, preferred_verticals, description, thesis, past_investments, preferred_geographies, preferred_investment_amount_min, preferred_investment_amount_max, preferred_deal_size_min, preferred_deal_size_max, aum_millions, dry_powder_millions, last_closed_fund_vintage, investments_last_12m, investments_last_2y')
        .range(kbFrom, kbFrom + KB_PAGE - 1);
      q = listIdFilter ? q.eq('list_id', listIdFilter) : q;
      const { data: kbPage } = await q;
      if (!kbPage?.length) break;
      for (const rec of kbPage) {
        const key = (rec.name || '').toLowerCase().trim();
        const firstToken = key.split(/\s+/)[0];
        if (key && !kbEnrichmentMap.has(key)) kbEnrichmentMap.set(key, rec); // don't overwrite — first source wins
        if (firstToken && firstToken.length > 3 && !kbEnrichmentMap.has(`__tok__${firstToken}`))
          kbEnrichmentMap.set(`__tok__${firstToken}`, rec);
      }
      if (kbPage.length < KB_PAGE) break;
      kbFrom += KB_PAGE;
    }
  };

  // KB list ID may be stored in dedicated column or in settings JSONB (fallback for older schema)
  const dealKbListId   = deal.knowledge_base_list_id   || deal.settings?.knowledge_base_list_id   || null;
  const dealKbListName = deal.knowledge_base_list_name || deal.settings?.knowledge_base_list_name || null;

  try {
    if (dealKbListId) {
      // Deal-specific KB first (highest priority)
      await loadKBRecords(dealKbListId);
      console.log(`[CANDIDATES] Deal KB loaded: ${Math.floor(kbEnrichmentMap.size / 1.5)} firms from "${dealKbListName || dealKbListId}"`);
    }

    // Always supplement from global KB lists (all lists tagged knowledge_base)
    const { data: globalKBLists } = await sb.from('investor_lists')
      .select('id, name').eq('list_type', 'knowledge_base');
    for (const kbList of (globalKBLists || [])) {
      if (kbList.id === dealKbListId) continue; // already loaded
      await loadKBRecords(kbList.id);
    }
    if (kbEnrichmentMap.size > 0) {
      console.log(`[CANDIDATES] Total KB enrichment: ${Math.floor(kbEnrichmentMap.size / 1.5)} unique firms available`);
    }
  } catch (e) {
    console.warn('[CANDIDATES] KB load failed:', e.message);
  }

  // ── Load response analytics signal ──────────────────────────────────────
  // Historical outreach data: which investor types actually respond positively?
  // Builds a profile signal map: investor_type → { boost, rate%, total contacts }
  // Used as a small scoring bonus (max 5pts) for types with proven response rates.
  const responseSignals = await getResponseSignals(sb);

  // Merge KB data into an investor record — KB fills in empty/null fields only
  const mergeKB = (investor) => {
    if (!kbEnrichmentMap.size) return investor;
    const key = ((investor.firm_name || investor.name) || '').toLowerCase().trim();
    const firstToken = key.split(/\s+/)[0];
    const kb = kbEnrichmentMap.get(key) ||
               (firstToken && kbEnrichmentMap.get(`__tok__${firstToken}`));
    if (!kb) return investor;
    const merged = { ...investor };
    // Only overwrite if current value is empty/null — KB enriches but doesn't override
    for (const field of ['investor_type', 'preferred_industries', 'preferred_verticals', 'description',
      'preferred_geographies', 'preferred_investment_amount_min', 'preferred_investment_amount_max',
      'preferred_deal_size_min', 'preferred_deal_size_max', 'aum_millions', 'dry_powder_millions',
      'last_closed_fund_vintage', 'investments_last_12m', 'investments_last_2y']) {
      if ((merged[field] == null || merged[field] === '') && kb[field] != null) {
        merged[field] = kb[field];
      }
    }
    // thesis and past_investments always take KB value if richer
    if (kb.thesis && (!merged.thesis || kb.thesis.length > (merged.thesis || '').length)) merged.thesis = kb.thesis;
    if (kb.past_investments?.length) merged.past_investments = kb.past_investments;
    return merged;
  };

  const shouldSkip = (name) => {
    const key = (name || '').toLowerCase().trim();
    return !key || existingNames.has(key) || exclusions.has(key) || GENERIC_FIRM_NAMES.has(key);
  };

  const getInvestorScore = (investor) => {
    const raw = investor?.score ?? investor?.investor_score ?? investor?.overall_score;
    const value = Number(raw);
    return Number.isFinite(value) ? value : 0;
  };

  const getBoostKey = (name) => String(name || '').toLowerCase().trim();

  const getInvestorBoost = (investor) => {
    const firmName = investor?.firm_name || investor?.name || '';
    const exact = intelligenceBoostByName.get(getBoostKey(firmName));
    if (exact != null) return exact;
    const firstToken = getBoostKey(firmName).split(/\s+/)[0];
    if (!firstToken) return 0;
    for (const [key, value] of intelligenceBoostByName.entries()) {
      if (key.includes(firstToken)) return value;
    }
    return 0;
  };

  const sortPriorityCandidates = (rows) => {
    return [...(rows || [])].sort((a, b) => {
      const boostDiff = getInvestorBoost(b) - getInvestorBoost(a);
      if (boostDiff !== 0) return boostDiff;
      const scoreDiff = getInvestorScore(b) - getInvestorScore(a);
      if (scoreDiff !== 0) return scoreDiff;
      return Number(b?.investments_last_12m || 0) - Number(a?.investments_last_12m || 0);
    });
  };

  const addCandidate = (investor, candidateSource = null) => {
    const firmName = investor.firm_name || investor.name;
    if (shouldSkip(firmName)) return false;
    if (investor.id && seenIds.has(investor.id)) return false;
    if (investor.id) seenIds.add(investor.id);
    candidates.push(candidateSource ? { ...investor, candidate_source: candidateSource } : investor);
    return true;
  };

  const orderedLists = await getOrderedListsForDeal(deal);

  const dealInfo = {
    deal_name: deal.name,
    deal_type: deal.raise_type || deal.type || 'Investment',
    sector: deal.sector || 'General',
    geography: deal.geography || 'United States',
    ideal_investor_profile: deal.investor_profile || '',
    disqualified_investor_types: [],
  };

  const { data: intelligenceScores } = await sb.from('deal_investor_scores')
    .select('investor_name, intelligence_boost')
    .eq('deal_id', deal.id)
    .gt('intelligence_boost', 0);
  for (const row of (intelligenceScores || [])) {
    intelligenceBoostByName.set(getBoostKey(row.investor_name), Number(row.intelligence_boost || 0));
  }

  for (const pl of orderedLists) {
    if (candidates.length >= limit) break;

    // Paginate through the entire list — no arbitrary cap
    let allListInvestors = [];
    const PAGE = 1000;
    let from = 0;
    while (true) {
      const { data: page } = await sb.from('investors_db')
        .select('*')
        .eq('list_id', pl.id)
        .range(from, from + PAGE - 1);
      if (!page?.length) break;
      allListInvestors = allListInvestors.concat(page);
      if (page.length < PAGE) break;
      from += PAGE;
    }

    const freshListInvestors = allListInvestors.filter(investor => !shouldSkip(investor.firm_name || investor.name));
    if (!freshListInvestors.length) continue;

    // Score every investor in the list against this deal (pure JS, no AI calls — fast)
    // mergeKB enriches sparse records with data from the knowledge base before scoring
    const fullyScored = freshListInvestors.map(investor => {
      const enriched = mergeKB(investor);
      const boost = intelligenceBoostByName.get(getBoostKey(enriched.firm_name || enriched.name)) || 0;
      const { score } = scoreFirmAgainstDeal(enriched, deal, boost, responseSignals);
      return { ...enriched, _list_score: score };
    }).sort((a, b) => b._list_score - a._list_score);

    const sourceLabel = pl.list_type === 'deal_specific'
      ? `Deal-specific list: ${pl.name}`
      : pl.list_type === 'comparable_deals'
        ? `Comparable deals list: ${pl.name}`
        : `Investor list: ${pl.name}`;

    console.log(`[FIRM RESEARCH] ${pl.name || pl.id}: scored ${fullyScored.length} investors — top score: ${fullyScored[0]?._list_score ?? 'n/a'}`);

    for (const investor of fullyScored) {
      addCandidate({
        ...investor,
        list_id: investor.list_id || pl.id,
        list_name: investor.list_name || pl.name,
        list_type: investor.list_type || pl.list_type,
        relevanceScore: pl.relevanceScore,
      }, sourceLabel);
      if (candidates.length >= limit) break;
    }
  }

  if (candidates.length < limit) {
    const fullShortlisted = await queryInvestorDatabase(dealInfo, deal);
    for (const investor of (fullShortlisted || [])) {
      if (Number(investor.score || 0) < threshold) continue;
      addCandidate(investor, 'Deal-brief database query');
      if (candidates.length >= limit) break;
    }
  }

  if (candidates.length < limit) {
    const sectorKeyword = (deal.sector || '').split('/')[0].trim();
    let fallbackQuery = sb.from('investors_db')
      .select('*')
      .order('investments_last_12m', { ascending: false, nullsFirst: false })
      .limit(Math.max(limit * 3, 120));

    if (sectorKeyword) {
      fallbackQuery = fallbackQuery.or(
        `preferred_industries.ilike.%${sectorKeyword}%,description.ilike.%${sectorKeyword}%`
      );
    }

    const { data: broadFallback } = await fallbackQuery;
    for (const investor of sortPriorityCandidates(broadFallback || [])) {
      addCandidate(mergeKB(investor), 'Sector fallback database query');
      if (candidates.length >= limit) break;
    }
  }

  return candidates;
}

async function rankBatchFirms(batchId, deal) {
  const sb = getSupabase();
  if (!sb) return [];
  const { data: firms } = await sb.from('batch_firms')
    .select('id, firm_name, score')
    .eq('batch_id', batchId)
    .order('score', { ascending: false });
  if (!firms?.length) return [];

  // Assign all ranks in parallel
  await Promise.all(firms.map((f, i) =>
    sb.from('batch_firms').update({ rank: i + 1 }).eq('id', f.id)
  ));

  pushActivity({
    type: 'research',
    action: `Batch ranked — top firm: ${firms[0]?.firm_name} (Score ${firms[0]?.score}/100)`,
    note: `${deal.name} · ${firms.length} firms scored · Campaign ready for review`,
    deal_name: deal.name,
    dealId: deal.id,
  });

  return firms;
}

async function continueResearch(deal, batch) {
  const sb = getSupabase();
  if (!sb || !batch) return;
  const firmCount = await updateBatchResearchCount(batch.id);

  if (firmCount >= BATCH_FIRM_TARGET) {
    await triggerCampaignReview(deal, batch);
    return;
  }

  const needed = BATCH_FIRM_TARGET - firmCount;
  pushActivity({
    type: 'research',
    action: `Research in progress: ${firmCount}/${BATCH_FIRM_TARGET} firms identified`,
    note: `${deal.name} · Batch ${batch.batch_number} · Continuing firm research`,
    deal_name: deal.name,
    dealId: deal.id,
  });
  await announceCycleDecision(deal, batch, 'research_start', firmCount === 0
    ? 'Fresh launch or empty pipeline. Roco is filling the initial top of funnel to 100 firms.'
    : `Top of funnel still needs ${needed} more firms.`, {
    current: firmCount,
    target: BATCH_FIRM_TARGET,
  });

  await researchNextFirms(deal, batch, needed, firmCount);
}

function logBrainExecutionDecision(deal, directives = {}) {
  if (!deal?.id || !directives || !Object.keys(directives).length) return;
  const parts = [
    `research ${directives.allowResearch === false ? 'held' : 'enabled'}`,
    `outreach ${directives.allowOutreach === false ? 'held' : 'enabled'}`,
    `follow-ups ${directives.allowFollowUps === false ? 'held' : 'enabled'}`,
  ];
  const reasons = [
    directives.researchReason,
    directives.allowOutreach === false ? directives.outreachReason : null,
    directives.allowFollowUps === false ? directives.followUpReason : null,
  ].filter(Boolean);

  pushActivity({
    type: 'system',
    action: 'Brain execution policy applied',
    note: `${deal.name} · ${parts.join(' · ')}${reasons.length ? ` · ${reasons.join(' · ')}` : ''}`,
    deal_name: deal.name,
    dealId: deal.id,
  });
}

async function announceCycleDecision(deal, batch, mode, reason, extras = {}) {
  if (!deal?.id || !batch?.id) return;
  const normalizedMode = String(mode || '').trim().toLowerCase();
  const cleanedReason = normalizeWhitespace(reason || 'No reason provided');
  const key = `${batch.id}:${normalizedMode}:${cleanedReason}`;

  let text = null;
  if (normalizedMode === 'research_start') {
    const current = Number(extras.current ?? 0);
    const target = Number(extras.target ?? BATCH_FIRM_TARGET);
    text = `🔍 *Research starting now* — ${deal.name}\n\nI’m filling Batch #${batch.batch_number} to keep the top of funnel full.\nCurrent batch: ${current}/${target} firms.\nWhy I’m doing it: ${cleanedReason}`;
  } else if (normalizedMode === 'research_wait') {
    text = `⏳ *Waiting before more research* — ${deal.name}\n\nI’m holding new firm research for Batch #${batch.batch_number} this cycle.\nWhy I’m waiting: ${cleanedReason}`;
  } else if (normalizedMode === 'outreach_start') {
    text = `📤 *Outreach running now* — ${deal.name}\n\nI’m moving Batch #${batch.batch_number} forward inside the current sending windows.\nWhy I’m doing it: ${cleanedReason}`;
  } else if (normalizedMode === 'outreach_wait') {
    text = `⏳ *Waiting before outreach* — ${deal.name}\n\nI’m holding new outreach for Batch #${batch.batch_number} this cycle.\nWhy I’m waiting: ${cleanedReason}`;
  } else if (normalizedMode === 'followup_wait') {
    text = `⏳ *Waiting before follow-ups* — ${deal.name}\n\nI’m holding follow-ups for Batch #${batch.batch_number} this cycle.\nWhy I’m waiting: ${cleanedReason}`;
  } else if (normalizedMode === 'pending_approval') {
    text = `📋 *Awaiting approval* — ${deal.name}\n\nBatch #${batch.batch_number} is queued for your review.\nWhy I stopped here: ${cleanedReason}`;
  }

  if (text) {
    await sendDecisionTelegramOnce(deal, key, text);
  }
}

async function researchNextFirms(deal, batch, needed, startingCount = 0) {
  const sb = getSupabase();
  if (!sb || needed <= 0) return;
  const exclusions = await getDealExclusions(deal.id);
  const existingNames = await getExcludedFirmNames(deal.id);
  const baseCandidates = await getCandidateFirms(deal, existingNames, exclusions, needed * 4);
  const [grokLeads, directoryLeads] = await Promise.all([
    searchInvestorsWithGrok(deal, existingNames, pushActivity).catch(() => []),
    scrapePublicInvestorDirectories(deal, existingNames, pushActivity).catch(() => []),
  ]);
  const candidates = [...baseCandidates, ...grokLeads, ...directoryLeads];

  console.log(`[FIRM RESEARCH] ${candidates.length} candidates loaded for ${deal.name} — ${candidates[0]?.candidate_source || candidates[0]?.list_name || 'deal-brief database query'}`);
  pushActivity({
    type: 'research',
    action: `Candidate pool ready: ${candidates.length} firms to evaluate`,
    note: `${deal.name} · PitchBook/DB: ${baseCandidates.length} · Grok: ${grokLeads.length} · Directories: ${directoryLeads.length}`,
    deal_name: deal.name,
    dealId: deal.id,
  });

  let added = 0;
  for (const investor of candidates) {
    if (added >= needed) break;
    const firmKey = investor.firm_name || investor.name;
    const normalized = (firmKey || '').toLowerCase().trim();
    if (!normalized || existingNames.has(normalized)) continue;

    try {
      const boost = await getIntelligenceBoostForFirm(deal.id, firmKey);
      const boostValue = Number(boost || 0);

      // investor is already KB-enriched from getCandidateFirms (mergeKB applied in priority list scoring)
      let enrichedInvestor = investor;
      let { score, scoring_breakdown, geo_match, control_action, control_rationale } = scoreFirmAgainstDeal(investor, deal, boostValue, null);
      const strictGeo = deal.strict_geography !== false;

      // Data-gap rescue: if the firm scores in the borderline range AND has sparse data,
      // research it first (Gemini/Grok fills the gaps) then re-score before making the cut decision.
      // This prevents firms from being excluded purely because we don't have enough data on them.
      const richText = investor.description || investor.thesis || '';
      const isDataSparse = richText.length < 80 || !investor.investor_type;
      if (isDataSparse && score >= 18 && score < 45) {
        pushActivity({
          type: 'research',
          action: `Data-gap check: ${firmKey} — sparse profile, researching before cut decision`,
          note: `Initial score ${score}/100 — filling gaps`,
          deal_name: deal.name,
          dealId: deal.id,
        });
        const gapFill = await researchFirmOnly(investor, deal);
        if (gapFill.thesis || gapFill.past_investments?.length) {
          enrichedInvestor = {
            ...investor,
            description:      gapFill.thesis || investor.description || null,
            thesis:           gapFill.thesis || investor.thesis || null,
            past_investments: gapFill.past_investments?.length ? gapFill.past_investments : (investor.past_investments || []),
            aum:              gapFill.aum || investor.aum || null,
          };
          const rescored = scoreFirmAgainstDeal(enrichedInvestor, deal, boostValue, null);
          if (rescored.score > score) {
            score            = rescored.score;
            scoring_breakdown = rescored.scoring_breakdown;
            geo_match        = rescored.geo_match;
            control_action   = rescored.control_action;
            control_rationale = rescored.control_rationale;
            pushActivity({
              type: 'research',
              action: `Re-scored after gap fill: ${firmKey} — ${score}/100`,
              note: `Gap fill improved score`,
              deal_name: deal.name,
              dealId: deal.id,
            });
          }
        }
      }

      // Hard control mismatch — archive immediately, never reach out
      if (control_action === 'archive') {
        pushActivity({
          type: 'excluded',
          action: `Control mismatch — archived: ${firmKey}`,
          note: `${deal.name} · ${control_rationale?.slice(0, 120) || 'Hard control mismatch'}`,
          deal_name: deal.name,
          dealId: deal.id,
        });
        existingNames.add((firmKey || '').toLowerCase().trim());
        continue;
      }

      if (strictGeo && !geo_match && score < 40) {
        pushActivity({
          type: 'excluded',
          action: `Skipped (geo mismatch): ${firmKey}`,
          note: scoring_breakdown.find(r => r.toLowerCase().includes('mismatch')) || 'Geographic mismatch',
          deal_name: deal.name,
          dealId: deal.id,
        });
        continue;
      }

      if (score < 30) {
        pushActivity({
          type: 'excluded',
          action: `Scored too low: ${firmKey} — ${score}/100`,
          note: isDataSparse ? 'Below threshold even after gap-fill research' : 'Below minimum threshold (30)',
          deal_name: deal.name,
          dealId: deal.id,
        });
        continue;
      }

      const firmData = await researchFirmOnly(enrichedInvestor, deal);
      const justification = await generateJustification({
        ...enrichedInvestor,
        firm_name: firmKey,
        thesis: firmData.thesis || enrichedInvestor.description || null,
        past_investments: firmData.past_investments || [],
        aum: firmData.aum || enrichedInvestor.aum || null,
        score,
      }, deal, scoring_breakdown);

      await sb.from('batch_firms').insert({
        batch_id: batch.id,
        deal_id: deal.id,
        investor_id: enrichedInvestor.id || null,
        firm_name: firmKey,
        contact_type: enrichedInvestor.is_angel ? 'angel' : (enrichedInvestor.contact_type || 'individual_at_firm'),
        score,
        thesis: firmData.thesis || null,
        past_investments: firmData.past_investments || [],
        aum: firmData.aum || enrichedInvestor.aum || null,
        justification: justification || firmData.justification || null,
        source_list: enrichedInvestor.list_name || enrichedInvestor.candidate_source || (enrichedInvestor._from_internet ? 'Grok Web Research' : 'database'),
        firm_researched: true,
        enrichment_status: 'pending',
        status: 'pending',
        created_at: new Date().toISOString(),
      });

      if (enrichedInvestor.list_id || enrichedInvestor.list_name) {
        await updateListStats({
          id: enrichedInvestor.list_id,
          name: enrichedInvestor.list_name,
        }, deal.id).catch(() => {});
      }

      existingNames.add(normalized);
      added++;
      const totalFirmsForDeal = await getDealFirmTotal(deal.id);
      pushActivity({
        type: 'research',
        action: `Ranked: ${firmKey} — Score ${score}/100 (${startingCount + added}/${totalFirmsForDeal})`,
        note: `${deal.name} · Batch ${batch.batch_number} · Source: ${enrichedInvestor.list_name || enrichedInvestor.candidate_source || (enrichedInvestor._from_internet ? 'Grok Web Research' : 'database')}${boostValue > 0 ? ` · +${boostValue}pts from deal comparables` : ''}`,
        deal_name: deal.name,
        dealId: deal.id,
      });

      await sleep(1500);
    } catch (err) {
      pushActivity({
        type: 'error',
        action: `Research failed: ${firmKey}`,
        note: err.message?.slice(0, 80),
        deal_name: deal.name,
        dealId: deal.id,
      });
    }
  }

  const newCount = startingCount + added;
  await sb.from('campaign_batches')
    .update({ ranked_firms: newCount, updated_at: new Date().toISOString() })
    .eq('id', batch.id);

  if (newCount >= BATCH_FIRM_TARGET) {
    await triggerCampaignReview(deal, batch);
  }
}

async function createContactFromAngel(firm, deal, batch) {
  const sb = getSupabase();
  if (!sb) return;
  const { data: investor } = firm.investor_id
    ? await sb.from('investors_db').select('*').eq('id', firm.investor_id).maybeSingle()
    : { data: null };

  const incoming = {
    deal_id: deal.id,
    batch_id: batch.id,
    name: investor?.decision_maker_name || investor?.primary_contact_name || investor?.name || firm.firm_name,
    company_name: firm.firm_name,
    email: investor?.email || investor?.primary_contact_email || null,
    linkedin_url: null,
    job_title: investor?.primary_contact_title || 'Angel Investor',
    contact_type: 'angel',
    is_angel: true,
    pipeline_stage: 'Approved for Outreach',
    investor_score: firm.score,
    enrichment_status: investor?.email ? 'enriched' : 'pending',
    enrichment_source: 'pitchbook',
    notes: firm.thesis || firm.justification || null,
    created_at: new Date().toISOString(),
  };

  const { data: existingDealContacts } = await sb.from('contacts')
    .select('id, batch_id, name, email, linkedin_url, linkedin_provider_id, job_title, company_name, pipeline_stage, enrichment_status')
    .eq('deal_id', deal.id);

  const existing = findMatchingExistingContact(existingDealContacts || [], incoming, firm.firm_name);
  if (existing) {
    const patch = buildContactMergePatch(existing, incoming, firm.firm_name);
    if (!existing.batch_id && batch?.id) patch.batch_id = batch.id;
    if (incoming.enrichment_status === 'enriched' && existing.enrichment_status !== 'enriched') patch.enrichment_status = 'enriched';
    if (existing.pipeline_stage === 'Ranked' || existing.pipeline_stage === 'Researched') patch.pipeline_stage = 'Approved for Outreach';
    if (Object.keys(patch).length) {
      const { error } = await sb.from('contacts').update(patch).eq('id', existing.id);
      if (error) {
        console.error(`[ENRICH] Failed to merge angel contact for ${firm.firm_name}:`, error.message);
      }
    }
  } else {
    const { error } = await sb.from('contacts').insert(incoming);
    if (error && error.code !== '23505') {
      console.error(`[ENRICH] Failed to insert angel contact for ${firm.firm_name}:`, error.message);
    }
  }

  try { await sb.from('batch_firms').update({ contacts_found: 1 }).eq('id', firm.id); } catch {}
}

function isVerifiedLinkedInSource(source) {
  const normalized = String(source || '').toLowerCase();
  return normalized.startsWith('unipile');
}

function sanitizeProspectLinkedInIdentity(contact = {}) {
  if (isVerifiedLinkedInSource(contact.source)) return contact;
  return {
    ...contact,
    linkedin_url: null,
    linkedin_provider_id: null,
  };
}

async function enrichFirmContacts(firm, deal, batch, state) {
  const sb = getSupabase();
  if (!sb) return;
  const firmName = firm.firm_name || firm.name;
  const unipileData = await enrichFirmViaLinkedIn(firmName, deal, pushActivity).catch(() => ({ linkedin_profile: null, contacts: [] }));

  if (unipileData.linkedin_profile) {
    const profile = unipileData.linkedin_profile;
    const batchFirmUpdates = {};
    if (profile.employee_count && !firm.aum) batchFirmUpdates.aum = `${profile.employee_count} employees`;
    if (profile.description && !firm.thesis) batchFirmUpdates.thesis = String(profile.description).slice(0, 500);
    if (Object.keys(batchFirmUpdates).length) {
      try { await sb.from('batch_firms').update(batchFirmUpdates).eq('id', firm.id); } catch {}
    }

    const investorUpdates = {};
    if (profile.description) investorUpdates.description = String(profile.description).slice(0, 1000);
    if (profile.website) investorUpdates.website = profile.website;
    if (profile.locations?.[0]?.country) investorUpdates.hq_country = profile.locations[0].country;
    if (Object.keys(investorUpdates).length) {
      try {
        await sb.from('investors_db')
          .update(investorUpdates)
          .ilike('name', `%${String(firmName || '').split(' ')[0]}%`);
      } catch {}
    }
  }

  let contacts = (unipileData.contacts || []).map(sanitizeProspectLinkedInIdentity);
  if (!contacts.length) {
    pushActivity({
      type: 'research',
      action: `LinkedIn search returned no results for ${firmName} — using Gemini/Grok fallback`,
      note: deal.name,
      deal_id: deal.id,
    });
    contacts = (await findDecisionMakers(firm, deal)).map(sanitizeProspectLinkedInIdentity);
  }

  // ── Within-batch dedup: prefer entries with email or linkedin_url ──────────
  const seenNames = new Map(); // identityKey → index in deduped array
  const deduped = [];
  for (const c of contacts) {
    if (!c.name) continue;
    const identityKey = buildContactIdentityKey(c, firm.firm_name);
    if (!identityKey) continue;
    const existing = seenNames.get(identityKey);
    if (existing === undefined) {
      seenNames.set(identityKey, deduped.length);
      deduped.push({ ...c });
    } else {
      const existingContact = deduped[existing];
      const sameNameVariant = areLikelySameNamedContact(existingContact.name, c.name);
      if (!sameNameVariant) {
        seenNames.set(`${identityKey}:${deduped.length}`, deduped.length);
        deduped.push({ ...c });
      } else {
        deduped[existing] = mergeContactRecords(existingContact, c);
      }
    }
  }

  // ── Fetch existing contacts for this deal so retries merge onto the same row ─
  const { data: existingDealContacts } = await sb.from('contacts')
    .select('id, batch_id, name, email, linkedin_url, linkedin_provider_id, job_title, company_name, pipeline_stage, enrichment_status')
    .eq('deal_id', deal.id);

  const existingContacts = (existingDealContacts || []).filter(existing => {
    const existingFirm = normalizeFirmIdentity(existing.company_name || '');
    const targetFirm = normalizeFirmIdentity(firm.firm_name || firmName || '');
    return !targetFirm || !existingFirm || existingFirm === targetFirm;
  });

  let inserted = 0;
  for (const contact of deduped) {
    const sanitizedContact = sanitizeProspectLinkedInIdentity(contact);
    const identityKey = buildContactIdentityKey(sanitizedContact, firm.firm_name);
    if (!identityKey) continue;
    const existing = findMatchingExistingContact(existingContacts, {
      ...sanitizedContact,
      company_name: firm.firm_name,
    }, firm.firm_name);

    if (existing) {
      const patch = buildContactMergePatch(existing, {
        ...sanitizedContact,
        company_name: firm.firm_name,
      }, firm.firm_name);
      if (!existing.batch_id && batch?.id) patch.batch_id = batch.id;

      if (Object.keys(patch).length) {
        try {
          await sb.from('contacts').update(patch).eq('id', existing.id);
          Object.assign(existing, patch);
        } catch {}
      }

      const mergedContact = { ...existing, ...patch };
      const shouldReEnrich = !!(patch.linkedin_url || patch.linkedin_provider_id) && !mergedContact.email;
      if (shouldReEnrich) {
        await enrichSingleContact(sb, mergedContact, deal, state || {});
      }
      continue;
    }

    const { data: inserted_row, error } = await sb.from('contacts').insert({
      deal_id: deal.id,
      batch_id: batch.id,
      name: sanitizedContact.name,
      company_name: firm.firm_name,
      email: sanitizedContact.email || null,
      linkedin_url: sanitizedContact.linkedin_url || null,
      linkedin_provider_id: sanitizedContact.linkedin_provider_id || null,
      job_title: sanitizedContact.job_title || null,
      contact_type: 'individual_at_firm',
      is_angel: false,
      pipeline_stage: 'Ranked',
      investor_score: firm.score,
      enrichment_status: 'pending',
      enrichment_source: sanitizedContact.source || (unipileData.contacts?.length ? 'unipile' : 'gemini'),
      notes: firm.thesis || firm.justification || null,
      created_at: new Date().toISOString(),
    }).select('*').single();
    if (error && error.code !== '23505') {
      console.error(`[ENRICH] Failed to insert contact ${contact.name} @ ${firm.firm_name}:`, error.message);
    } else if (!error && inserted_row) {
      inserted++;
      existingContacts.push({
        id: inserted_row.id,
        batch_id: inserted_row.batch_id || batch?.id || null,
        name: sanitizedContact.name,
        email: sanitizedContact.email,
        linkedin_url: sanitizedContact.linkedin_url,
        linkedin_provider_id: sanitizedContact.linkedin_provider_id,
        job_title: sanitizedContact.job_title,
        company_name: firm.firm_name,
      });
      // ── Full enrichment inline: LinkedIn find + KASPR/Apify — complete this contact before moving on ──
      await enrichSingleContact(sb, inserted_row, deal, state || {});
    }
  }

  try { await sb.from('batch_firms').update({ contacts_found: deduped.length }).eq('id', firm.id); } catch {}
}

async function runApprovedEnrichment(deal, batch, state) {
  const sb = getSupabase();
  if (!sb) return;

  let { data: batchFirms } = await sb.from('batch_firms')
    .select('*')
    .eq('batch_id', batch.id);

  const now = Date.now();
  const staleInProgress = (batchFirms || []).filter(firm => {
    if (normalizeBatchFirmEnrichmentStatus(firm.enrichment_status) !== 'in_progress') return false;
    const touchedAt = firm.created_at || null;
    if (!touchedAt) return true;
    const age = now - new Date(touchedAt).getTime();
    return Number.isFinite(age) && age > STALE_BATCH_FIRM_ENRICHMENT_MS;
  });

  if (staleInProgress.length) {
    const staleIds = staleInProgress.map(f => f.id);
    await sb.from('batch_firms').update({
      enrichment_status: 'pending',
    }).in('id', staleIds);

    pushActivity({
      type: 'system',
      action: `Recovered ${staleIds.length} stalled firm enrichment job${staleIds.length !== 1 ? 's' : ''}`,
      note: `${deal.name} · ${staleInProgress.map(f => f.firm_name).join(', ')}`,
      deal_name: deal.name,
      dealId: deal.id,
    });

    const refreshed = await sb.from('batch_firms')
      .select('*')
      .eq('batch_id', batch.id);
    batchFirms = refreshed.data || [];
  }

  // Prefer explicit rank ordering for approved campaigns; fall back to score when rank is absent.
  const sortedFirms = (batchFirms || [])
    .sort((a, b) => {
      const rankDiff = Number(a.rank || 9999) - Number(b.rank || 9999);
      if (rankDiff !== 0) return rankDiff;
      return Number(b.score ?? 0) - Number(a.score ?? 0);
    })
    .map((f, i) => ({ ...f, _rank: i + 1 }));

  const pendingFirms = sortedFirms.filter(firm => {
    const status = normalizeBatchFirmEnrichmentStatus(firm.enrichment_status);
    if (status === 'pending') return true;
    if (status !== 'failed') return false;
    const attempts = Number(parseBatchFirmEnrichmentMeta(firm).retry_count || 0);
    return attempts < MAX_FIRM_ENRICHMENT_ATTEMPTS;
  });
  const remaining = pendingFirms.length;

  if (!remaining) {
    pushDealStatusOnce(deal, `firms-enriched:${batch.id}`, {
      type: 'system',
      action: `All firms enriched — outreach running within sending windows`,
      note: `${deal.name} · Batch ${batch.batch_number}`,
      deal_name: deal.name,
      dealId: deal.id,
    });
    return;
  }

  // Process up to 5 firms per cycle with a 5-second rest between each.
  // Outreach runs in the same cycle for all already-enriched contacts.
  const FIRMS_PER_CYCLE = 5;
  const firmsToProcess = pendingFirms.slice(0, FIRMS_PER_CYCLE);
  const total = batch.firms_target || BATCH_FIRM_TARGET;

  pushActivity({
    type: 'research',
    action: `Enriching ${firmsToProcess.length} firm${firmsToProcess.length !== 1 ? 's' : ''}: ${firmsToProcess.map(f => f.firm_name).join(', ')}`,
    note: `${deal.name} · Batch ${batch.batch_number} · ${remaining} pending · ${remaining - firmsToProcess.length} after this batch`,
    deal_name: deal.name,
    dealId: deal.id,
  });

  for (let i = 0; i < firmsToProcess.length; i++) {
    await enrichSingleFirm(firmsToProcess[i], deal, batch, state);
    if (i < firmsToProcess.length - 1) {
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

async function enrichSingleFirm(firm, deal, batch, state) {
  const sb = getSupabase();
  if (!sb) return;

  const priorMeta = parseBatchFirmEnrichmentMeta(firm);
  const attemptNumber = Number(priorMeta.retry_count || 0) + 1;
  const isRetry = attemptNumber > 1;

  await sb.from('batch_firms')
    .update({
      enrichment_status: 'in_progress',
      status_reason: stringifyBatchFirmEnrichmentMeta(buildBatchFirmEnrichmentMeta(firm, {
        retry_count: Number(priorMeta.retry_count || 0),
        last_attempt_started_at: new Date().toISOString(),
        final_failure: false,
        manual_review_required: false,
      })),
    })
    .eq('id', firm.id);

  const total = await getDealFirmTotal(deal.id);
  const firmLabel = `${firm.firm_name} (Rank ${firm._rank || '?'}/${total})`;
  const attemptSuffix = isRetry ? ` · Retry ${attemptNumber}/${MAX_FIRM_ENRICHMENT_ATTEMPTS}` : '';

  pushActivity({
    type: 'research',
    action: `${isRetry ? 'Retrying decision makers' : 'Finding decision makers'}: ${firmLabel}`,
    note: firm.contact_type === 'angel'
      ? `${deal.name} · Angel investor — using existing profile${attemptSuffix}`
      : `${deal.name} · Searching LinkedIn and database${attemptSuffix}`,
    deal_name: deal.name,
    dealId: deal.id,
  });

  try {
    if (firm.contact_type === 'angel') {
      await createContactFromAngel(firm, deal, batch);
    } else {
      await enrichFirmContacts(firm, deal, batch, state);
    }

    // Read contacts_found that enrichFirmContacts/createContactFromAngel set
    const { data: updatedFirm } = await sb.from('batch_firms')
      .select('contacts_found')
      .eq('id', firm.id)
      .maybeSingle();
    const found = updatedFirm?.contacts_found ?? (firm.contact_type === 'angel' ? 1 : 0);

    await sb.from('batch_firms')
      .update({
        enrichment_status: 'complete',
        status_reason: stringifyBatchFirmEnrichmentMeta(buildBatchFirmEnrichmentMeta(firm, {
          retry_count: attemptNumber,
          last_success_at: new Date().toISOString(),
          last_error: null,
          final_failure: false,
          manual_review_required: false,
        })),
      })
      .eq('id', firm.id);

    pushActivity({
      type: 'research',
      action: `Contacts confirmed: ${firmLabel} — ${found} decision maker${found !== 1 ? 's' : ''} found`,
      note: `${deal.name} · Queued for outreach`,
      deal_name: deal.name,
      dealId: deal.id,
    });

  } catch (err) {
    const errorMessage = String(err?.message || 'Unknown enrichment error').slice(0, 220);
    const finalFailure = attemptNumber >= MAX_FIRM_ENRICHMENT_ATTEMPTS;
    const failureMeta = buildBatchFirmEnrichmentMeta(firm, {
      retry_count: attemptNumber,
      last_error: errorMessage,
      last_attempt_failed_at: new Date().toISOString(),
      final_failure: finalFailure,
      manual_review_required: finalFailure,
    });
    await sb.from('batch_firms')
      .update({
        enrichment_status: 'failed',
        status_reason: stringifyBatchFirmEnrichmentMeta(failureMeta),
        notes: finalFailure
          ? appendNote(firm.notes, `Manual review required after ${MAX_FIRM_ENRICHMENT_ATTEMPTS} failed enrichment attempts`)
          : firm.notes || null,
      })
      .eq('id', firm.id);

    pushActivity({
      type: 'error',
      action: finalFailure
        ? `Enrichment failed permanently: ${firm.firm_name}`
        : `Enrichment failed: ${firm.firm_name}`,
      note: finalFailure
        ? `${deal.name} · Attempt ${attemptNumber}/${MAX_FIRM_ENRICHMENT_ATTEMPTS} failed — manual review required`
        : `${deal.name} · Attempt ${attemptNumber}/${MAX_FIRM_ENRICHMENT_ATTEMPTS} failed — ${errorMessage.slice(0, 100)}`,
      deal_name: deal.name,
      dealId: deal.id,
    });

    if (attemptNumber === 2) {
      await sendTelegram(
        `⚠️ *Firm enrichment retry warning*\n\n*Deal:* ${deal.name}\n*Firm:* ${firm.firm_name}\n*Attempt:* 2/${MAX_FIRM_ENRICHMENT_ATTEMPTS}\n*Status:* Failed twice, Roco will try one final time automatically.\n*Error:* ${errorMessage}`
      ).catch(() => {});
    }

    if (finalFailure) {
      await sendTelegram(
        `⚠️ *Firm enrichment manual review needed*\n\n*Deal:* ${deal.name}\n*Firm:* ${firm.firm_name}\n*Attempts:* ${MAX_FIRM_ENRICHMENT_ATTEMPTS}/${MAX_FIRM_ENRICHMENT_ATTEMPTS}\n*Outcome:* Roco could not find decision makers after three attempts and is skipping this firm.\n*Action:* Please review this firm manually.\n*Error:* ${errorMessage}`
      ).catch(() => {});
    }
  }
}

async function runApprovedOutreach(deal, batch, state, directives = null) {
  const sb = getSupabase();
  if (!sb) return;
  const { data: approvedContacts } = await sb.from('contacts')
    .select('id, email')
    .eq('deal_id', deal.id)
    .eq('batch_id', batch.id)
    .eq('pipeline_stage', 'Approved for Outreach')
    .limit(10);

  const emailIds   = (approvedContacts || []).filter(c =>  c.email).map(c => c.id);
  const noEmailIds = (approvedContacts || []).filter(c => !c.email).map(c => c.id);
  if (emailIds.length) {
    await sb.from('contacts').update({ pipeline_stage: 'Enriched', enrichment_status: 'enriched' }).in('id', emailIds);
  }
  // No email yet — hand off to phaseEnrich (KASPR) by moving to Ranked
  if (noEmailIds.length) {
    await sb.from('contacts').update({ pipeline_stage: 'Ranked', enrichment_status: 'pending' }).in('id', noEmailIds);
  }

  if (state.enrichment_enabled !== false) {
    await phaseEnrich(deal, state);
  }

  await phaseTempCloseCheck(deal, state);
  if (directives?.allowOutreach === false) {
    pushDealStatusOnce(deal, `brain-outreach-hold:${batch.id}:${directives.outreachReason || ''}`, {
      type: 'system',
      action: 'Brain hold: outreach paused this cycle',
      note: `${deal.name} · ${directives.outreachReason || 'Reasoning directed Roco to wait before sending new outreach'}`,
      deal_name: deal.name,
      dealId: deal.id,
    });
    await announceCycleDecision(deal, batch, 'outreach_wait', directives.outreachReason || 'Reasoning directed Roco to wait before sending new outreach');
  } else if (state.outreach_enabled !== false) {
    await announceCycleDecision(deal, batch, 'outreach_start', directives?.outreachReason || 'It is time to move the batch forward within the current sending windows');
    await phaseLinkedInInvites(deal, state);
  }

  const globallyPaused = state.outreach_paused_until && isGloballyPaused(state.outreach_paused_until);
  const onActiveOutreachDay = isActiveOutreachDay(deal) && !globallyPaused;
  if (onActiveOutreachDay) {
    if (directives?.allowOutreach !== false && state.outreach_enabled !== false) await phaseOutreach(deal, state);
    if (directives?.allowFollowUps !== false && state.followup_enabled !== false) {
      await phaseFollowUps(deal, state);
    } else if (directives?.allowFollowUps === false) {
      pushDealStatusOnce(deal, `brain-followup-hold:${batch.id}:${directives.followUpReason || ''}`, {
        type: 'system',
        action: 'Brain hold: follow-ups paused this cycle',
        note: `${deal.name} · ${directives.followUpReason || 'Reasoning directed Roco to wait before following up'}`,
        deal_name: deal.name,
        dealId: deal.id,
      });
      await announceCycleDecision(deal, batch, 'followup_wait', directives.followUpReason || 'Reasoning directed Roco to wait before following up');
    }
  }
}

async function maybeStartNextBatch(deal) {
  await ensureBatchExists(deal);
}

async function getDealFirmTotal(dealId) {
  const sb = getSupabase();
  if (!sb || !dealId) return BATCH_FIRM_TARGET;
  try {
    const { count } = await sb.from('batch_firms')
      .select('id', { count: 'exact', head: true })
      .eq('deal_id', dealId);
    return Number(count) || BATCH_FIRM_TARGET;
  } catch {
    return BATCH_FIRM_TARGET;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PATIENCE RULES — think in days, not minutes
// ─────────────────────────────────────────────────────────────────────────────

function shouldAddFirms(metrics) {
  const pipelineGap          = Math.max(0, 100 - (metrics.firms_in_pipeline || 0));
  const hoursSinceInvite     = metrics.hours_since_last_li_invite;
  if (pipelineGap <= 0) return false;                             // already at 100
  if (hoursSinceInvite !== null && hoursSinceInvite < 48) return false; // sent invites recently — wait
  return true;
}

function shouldFollowUpDM(contact) {
  if (!contact.dm_sent || contact.dm_replied || contact.dm_followup_sent) return false;
  const dmSentAt = contact.dm_sent_at || contact.last_outreach_at;
  if (!dmSentAt) return false;
  const days = Math.floor((Date.now() - new Date(dmSentAt)) / 86400000);
  return days >= 3;  // minimum 3 days
}

function shouldFollowUpEmail(contact) {
  if (!contact.email_sent || contact.email_replied || contact.email_followup_sent) return false;
  const emailSentAt = contact.email_sent_at || contact.last_email_sent_at;
  if (!emailSentAt) return false;
  const days = Math.floor((Date.now() - new Date(emailSentAt)) / 86400000);
  return days >= 7;  // minimum 7 days
}

// ─────────────────────────────────────────────────────────────────────────────
// DEAL ARCHIVE — move exhausted firms out of pipeline
// ─────────────────────────────────────────────────────────────────────────────

async function archiveFirm(firmId, dealId, reason, note = '') {
  const sb = getSupabase();
  if (!sb) return;
  try {
    const { data: firm } = await sb.from('batch_firms').select('*').eq('id', firmId).single().then(r => r, () => ({ data: null }));
    if (!firm) return;

    const firmKeyword = (firm.firm_name || '').split(' ')[0];
    const { count: contacted } = await sb.from('contacts')
      .select('id', { count: 'exact', head: true })
      .eq('deal_id', dealId)
      .ilike('company_name', `%${firmKeyword}%`)
      .not('pipeline_stage', 'eq', 'Identified')
      .then(r => r, () => ({ count: 0 }));

    try {
      await sb.from('deal_archive').insert({
        deal_id: dealId,
        firm_name: firm.firm_name,
        original_rank: firm.rank,
        score: firm.score,
        source_list: firm.source_list,
        archive_reason: reason,
        archive_note: note,
        contacts_reached: contacted || 0,
        original_batch_firm_id: firmId,
      });
    } catch {}

    await sb.from('batch_firms').update({ status: 'exhausted', notes: `Archived: ${reason}` }).eq('id', firmId);

    if (firm.source_list) {
      const { data: positiveReply } = await sb.from('contacts')
        .select('id')
        .eq('deal_id', dealId)
        .ilike('company_name', `%${String(firm.firm_name || '').split(' ')[0]}%`)
        .eq('response_received', true)
        .limit(1)
        .maybeSingle()
        .catch(() => ({ data: null }));
      await updateListStats({ name: firm.source_list }, dealId, !!positiveReply).catch(() => {});
    }

    pushActivity({
      type: 'system',
      action: `Archived: ${firm.firm_name} — ${reason}`,
      note: `${contacted || 0} contacts reached`,
      deal_id: dealId,
    });
  } catch (err) {
    console.warn('[ARCHIVE FIRM]', err.message);
  }
}

async function archiveExhaustedFirms(dealId) {
  const sb = getSupabase();
  if (!sb || !dealId) return;
  try {
    // Find firms where all contacts have been through the full sequence (2+ follow-ups, no reply)
    const { data: candidates } = await sb.from('batch_firms')
      .select('id, firm_name, deal_id')
      .eq('deal_id', dealId)
      .eq('enrichment_status', 'complete')
      .not('status', 'in', '("exhausted","suppressed")')
      .limit(20);

    if (!candidates?.length) return;

    for (const firm of candidates) {
      // Check if all contacts from this firm are inactive/ghosted
      const firmKeyword = (firm.firm_name || '').split(' ')[0];
      const { data: contacts } = await sb.from('contacts')
        .select('pipeline_stage, follow_up_count, response_received')
        .eq('deal_id', dealId)
        .ilike('company_name', `%${firmKeyword}%`)
        .not('pipeline_stage', 'in', '("Identified","Archived")');

      if (!contacts?.length) continue;

      const allExhausted = contacts.every(c =>
        ['Inactive', 'Ghosted', 'Not Interested', 'Suppressed — Opt Out'].includes(c.pipeline_stage) ||
        (c.follow_up_count >= 2 && !c.response_received)
      );

      if (allExhausted) {
        await archiveFirm(firm.id, dealId, 'sequence_exhausted', `All ${contacts.length} contacts through full sequence`);
      }
    }
  } catch (err) {
    console.warn('[ARCHIVE EXHAUSTED]', err.message);
  }
}

function normalizeWhitespace(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function extractActionPlanSection(actionPlan, label) {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = String(actionPlan || '').match(new RegExp(`${escaped}[:\\s]+([\\s\\S]*?)(?:\\n\\n|\\n[0-9A-Z][^\\n]*:|$)`, 'i'));
  return normalizeWhitespace(match?.[1] || '');
}

async function maybeSendPreResearchThinkingTelegram(deal, batch, brainResult) {
  if (!deal?.id || !batch?.id || !brainResult?.actionPlan) return;
  if (batch.status !== 'researching') return;

  const key = `${deal.id}:${batch.id}:${batch.status}`;
  if (reasoningTelegramState.get(key)) return;

  const assessment = extractActionPlanSection(brainResult.actionPlan, 'HONEST ASSESSMENT');
  const priorities = extractActionPlanSection(brainResult.actionPlan, "TODAY'S 3 PRIORITIES");
  const patience = extractActionPlanSection(brainResult.actionPlan, 'PATIENCE CHECK');
  const domNote = extractActionPlanSection(brainResult.actionPlan, 'WHAT DOM SHOULD KNOW');
  const directives = brainResult.directives || {};

  const summary = [
    `🧠 *Pre-Research Reasoning* — ${deal.name}`,
    '',
    `Batch #${batch.batch_number} is still in research mode, so Roco is reasoning before querying more firms.`,
    '',
    `*Assessment:* ${assessment || normalizeWhitespace(brainResult.goalAnalysis?.status) || 'Assessment generated in dashboard reasoning feed.'}`,
    `*Priorities:* ${priorities || 'See dashboard live activity for the full action plan.'}`,
    `*Patience Check:* ${patience || 'No explicit patience guidance returned this cycle.'}`,
    `*Execution:* Research ${directives.allowResearch ? 'ON' : 'HOLD'}${directives.researchReason ? ` (${directives.researchReason})` : ''}${directives.allowFollowUps === false ? ` · Follow-ups HOLD (${directives.followUpReason || 'brain-directed wait'})` : ''}${directives.allowOutreach === false ? ` · Outreach HOLD (${directives.outreachReason || 'brain-directed wait'})` : ''}`,
    domNote ? `*What Dom Should Know:* ${domNote}` : '',
  ].filter(Boolean).join('\n');

  reasoningTelegramState.set(key, true);
  await sendTelegram(summary).catch(() => {
    reasoningTelegramState.delete(key);
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// DEAL CYCLE
// ─────────────────────────────────────────────────────────────────────────────

async function runDealCycle(deal, state) {
  if (dealCycleLocks.has(deal.id)) {
    info(`[${deal.name}] Cycle already in progress — skipping overlapping run`);
    return;
  }
  dealCycleLocks.add(deal.id);

  // ── Error cooldown gate ───────────────────────────────────────────────────
  const errTrack = dealErrorCounts.get(deal.id) || { count: 0, pausedUntil: null };
  if (errTrack.pausedUntil) {
    if (Date.now() < errTrack.pausedUntil) {
      const remaining = Math.ceil((errTrack.pausedUntil - Date.now()) / 60_000);
      info(`[${deal.name}] Error cooldown — skipping (${remaining}min left)`);
      dealCycleLocks.delete(deal.id);
      return;
    }
    // Cooldown expired — reset and resume
    errTrack.count = 0;
    errTrack.pausedUntil = null;
    dealErrorCounts.set(deal.id, errTrack);
    info(`[${deal.name}] Error cooldown expired — resuming`);
    pushActivity({ type: 'system', action: 'Auto-resumed', note: `${deal.name} — error cooldown expired` });
    sendTelegram(`✅ *${deal.name}* auto-resumed after error cooldown.`).catch(() => {});
  }

  console.log(`[ORCHESTRATOR] ---- Cycle: ${deal.name} ----`);

  try {

  // ── 1. FUNDRAISER BRAIN — reason first, then act ──────────────────────────
  let brainResult = null;
  let brainDirectives = null;
  try {
    const metrics = await gatherCurrentMetrics(deal.id);
    const orderedLists = await getOrderedListsForDeal(deal);
    // Build agentConfig from deal fields (sender info etc.)
    const agentConfig = {
      sender_name:   deal.sender_name || process.env.SENDER_NAME || 'Dom',
      sender_title:  deal.sender_title || 'Principal',
      custom_rules:  deal.custom_rules || deal.settings?.custom_rules || null,
      prioritise:    deal.prioritise   || deal.settings?.prioritise   || null,
    };
    brainResult = await runFundraiserReasoning(deal, { metrics, agentConfig, orderedLists }, pushActivity);
    if (brainResult) brainResult._metrics = metrics;  // stash for patience check
    brainDirectives = brainResult?.directives || null;
    if (brainDirectives) {
      logBrainExecutionDecision(deal, brainDirectives);
    }

    if (Number(metrics?.firms_in_pipeline || 0) < 25) {
      await triggerAutoFeedForDeal(deal, { reason: 'pipeline_depth', requestedCount: 24 }).catch(() => {});
    }
  } catch (brainErr) {
    console.warn('[FUNDRAISER BRAIN] Cycle reasoning failed:', brainErr.message);
  }

  // ── 2. ARCHIVE exhausted firms ───────────────────────────────────────────
  await archiveExhaustedFirms(deal.id).catch(() => {});

  // ── 2b. PATIENCE CHECK — log if we should wait before adding firms ────────
  if (brainResult) {
    const { metrics: brainMetrics } = brainResult.goalAnalysis ? { metrics: brainResult.goalAnalysis } : { metrics: {} };
    // Re-use the metrics we gathered in step 1
    const metricsForPatienceCheck = brainResult._metrics;
    if (metricsForPatienceCheck && !shouldAddFirms(metricsForPatienceCheck)) {
      info(`[${deal.name}] Patience: pipeline at ${metricsForPatienceCheck.firms_in_pipeline}/100 — LI invites sent ${metricsForPatienceCheck.hours_since_last_li_invite}h ago, waiting`);
    }
  }

  // ── 3. MAIN BATCH CYCLE ──────────────────────────────────────────────────
  const batch = await ensureBatchExists(deal);
  if (!batch) return;
  await maybeSendPreResearchThinkingTelegram(deal, batch, brainResult);

  switch (batch.status) {
    case 'researching':
      if (brainDirectives?.allowResearch === false) {
        pushDealStatusOnce(deal, `brain-research-hold:${batch.id}:${brainDirectives.researchReason || ''}`, {
          type: 'system',
          action: 'Brain hold: research paused this cycle',
          note: `${deal.name} · ${brainDirectives.researchReason || 'Reasoning directed Roco to wait before adding more firms'}`,
          deal_name: deal.name,
          dealId: deal.id,
        });
        await announceCycleDecision(deal, batch, 'research_wait', brainDirectives.researchReason || 'Reasoning directed Roco to wait before adding more firms');
      } else {
        await continueResearch(deal, batch);
      }
      break;
    case 'pending_approval':
      console.log(`[ORCHESTRATOR] ${deal.name} batch ${batch.batch_number} awaiting approval`);
      pushDealStatusOnce(deal, `pending-approval:${batch.id}:${batch.batch_number}`, {
        type: 'system',
        action: 'Batch awaiting approval',
        note: `${deal.name} · Batch ${batch.batch_number}`,
        deal_name: deal.name,
        dealId: deal.id,
      });
      await announceCycleDecision(deal, batch, 'pending_approval', 'Research target reached and the batch is ready for your approval');
      break;
    case 'approved':
      await runApprovedEnrichment(deal, batch, state);
      await runApprovedOutreach(deal, batch, state, brainDirectives);
      await maybeTopUpApprovedBatch(deal, brainDirectives);
      await phaseTopUpPipeline(deal, state);  // promote archived contacts + trigger research when pipeline low
      await logDealIdleStatus(deal, batch, state);
      break;
    case 'completed':
    case 'skipped':
      await maybeStartNextBatch(deal);
      break;
    default:
      console.log(`[ORCHESTRATOR] ${deal.name} batch status: ${batch.status} — no action`);
      break;
  }

  console.log(`[ORCHESTRATOR] ---- Cycle complete: ${deal.name} ----`);

  // Success — reset error counter
  errTrack.count = 0;
  dealErrorCounts.set(deal.id, errTrack);

  } catch (cycleErr) {
    errTrack.count = (errTrack.count || 0) + 1;
    dealErrorCounts.set(deal.id, errTrack);
    console.error(`[${deal.name}] Cycle error (${errTrack.count}/5): ${cycleErr.message}`);
    pushActivity({ type: 'error', action: 'Cycle Error', note: `${deal.name} — ${cycleErr.message.substring(0, 150)}` });

    if (errTrack.count >= 5) {
      errTrack.pausedUntil = Date.now() + 5 * 60_000;
      dealErrorCounts.set(deal.id, errTrack);
      pushActivity({ type: 'error', action: 'Deal Auto-Paused', note: `${deal.name} — 5 consecutive errors, 5min cooldown` });
      sendTelegram(`⚠️ *${deal.name} auto-paused*\n5 consecutive cycle errors. Cooling down for 5 minutes.\n\nLast error: ${cycleErr.message.substring(0, 200)}`).catch(() => {});
    }
  } finally {
    dealCycleLocks.delete(deal.id);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE DB QUERY — Pull investors from investors_db, score with Haiku 4.5, promote
// Only runs when pipeline is below 100 contacts
// ─────────────────────────────────────────────────────────────────────────────

async function phaseDatabaseQuery(deal, batch) {
  const sb = getSupabase();
  if (!sb) return;

  try {
    const MAX_PIPELINE      = deal.pipeline_max || 100;
    const MIN_PIPELINE      = deal.pipeline_refill_threshold || 30;
    const threshold         = deal.min_investor_score || 60;

    // Count active ranked pipeline
    const { count: currentPipeline } = await sb.from('contacts')
      .select('*', { count: 'exact', head: true })
      .eq('deal_id', deal.id)
      .gte('investor_score', threshold)
      .not('pipeline_stage', 'eq', 'Inactive');

    if ((currentPipeline || 0) >= MAX_PIPELINE) {
      info(`[${deal.name}] DB QUERY: pipeline full (${currentPipeline}/${MAX_PIPELINE}) — skipping`);
      return;
    }

    if ((currentPipeline || 0) >= MIN_PIPELINE) {
      info(`[${deal.name}] DB QUERY: pipeline healthy (${currentPipeline} ranked) — skipping`);
      return;
    }

    info(`[${deal.name}] DB QUERY: pipeline at ${currentPipeline} — querying investor database...`);

    // Load parsed deal info
    const { data: docs } = await sb.from('deal_documents')
      .select('parsed_deal_info').eq('deal_id', deal.id).limit(1);

    const dealInfo = docs?.[0]?.parsed_deal_info || {
      deal_name:                deal.name,
      deal_type:                deal.raise_type || 'Buyout',
      sector:                   deal.sector || 'General',
      sub_sector:               null,
      geography:                deal.geography || 'United States',
      hq_location:              deal.geography || '',
      ebitda_usd_m:             null,
      revenue_usd_m:            null,
      enterprise_value_usd_m:   null,
      equity_required_usd_m:    deal.min_cheque ? deal.min_cheque / 1_000_000 : null,
      ideal_investor_types:     ['PE/Buyout', 'Family Office'],
      ideal_investor_profile:   `Investor interested in ${deal.sector || 'general'} deals`,
      disqualified_investor_types: [],
    };

    logStep('Querying investor database', deal.name, 'research', deal);
    pushActivity({ type: 'research', action: `Querying investor database for ${deal.name}…`, deal_name: deal.name, dealId: deal.id });

    // Fetch already-contacted investors_db_id values
    const { data: alreadyContacted } = await sb.from('contacts')
      .select('investors_db_id, company_name').eq('deal_id', deal.id);
    const contactedDbIds = new Set((alreadyContacted || []).map(c => c.investors_db_id).filter(Boolean));
    // Normalize firm names to catch "XYZ Capital, LLC" vs "XYZ Capital" mismatches
    const contactedFirms = new Set((alreadyContacted || []).map(c => normalizeFirmForDedup(c.company_name)).filter(Boolean));

    let shortlisted = [];

    // ── Priority lists path ──────────────────────────────────────────────────
    const { data: priorityLists } = await sb.from('deal_list_priorities')
      .select('*')
      .eq('deal_id', deal.id)
      .not('status', 'eq', 'exhausted')
      .order('priority_order', { ascending: true });

    let activePriorityList = null;

    if (priorityLists?.length) {
      for (const pl of priorityLists) {
        info(`[${deal.name}] DB QUERY: checking priority list "${pl.list_name}" (order ${pl.priority_order})`);

        // Load ALL firms from this priority list — paginated so no 500-row cap
        const LIST_PAGE = 1000;
        let listFrom = 0;
        const allListInvestors = [];
        while (true) {
          const { data: page } = await sb.from('investors_db')
            .select('*').eq('list_id', pl.list_id)
            .range(listFrom, listFrom + LIST_PAGE - 1);
          if (!page?.length) break;
          allListInvestors.push(...page);
          if (page.length < LIST_PAGE) break;
          listFrom += LIST_PAGE;
        }
        const listInvestors = allListInvestors;

        if (!listInvestors.length) {
          await sb.from('deal_list_priorities')
            .update({ status: 'exhausted', exhausted_at: new Date().toISOString() }).eq('id', pl.id);
          continue;
        }

        const fresh = listInvestors.filter(inv =>
          !contactedDbIds.has(inv.id) &&
          !contactedFirms.has(normalizeFirmForDedup(inv.name))
        );

        if (!fresh.length) {
          await sb.from('deal_list_priorities')
            .update({ status: 'exhausted', exhausted_at: new Date().toISOString() }).eq('id', pl.id);
          info(`[${deal.name}] DB QUERY: list exhausted — "${pl.list_name}"`);
          continue;
        }

        await sb.from('deal_list_priorities').update({ status: 'active' }).eq('id', pl.id);
        activePriorityList = pl;

        // Score this list's candidates with Haiku 4.5 (gpt-5.4-mini fallback)
        info(`[${deal.name}] DB QUERY: scoring ${fresh.length} candidates from "${pl.list_name}"`);
        pushActivity({ type: 'research', action: `Scoring ${fresh.length} candidates from list "${pl.list_name}"`, deal_name: deal.name, dealId: deal.id });
        const scored = await batchScoreInvestors(fresh, dealInfo, deal);
        const disqualified = (dealInfo.disqualified_investor_types || []).map(t => t.toLowerCase());
        shortlisted = scored
          .filter(s => {
            if (s.score < threshold) return false;
            if (disqualified.some(d => (s.investor_type || '').toLowerCase().includes(d))) return false;
            return true;
          })
          .sort((a, b) => b.score - a.score)
          .slice(0, 150);

        info(`[${deal.name}] DB QUERY: ${shortlisted.length} shortlisted from "${pl.list_name}"`);
        break; // Use first non-exhausted list
      }
    }

    // ── Full database fallback ───────────────────────────────────────────────
    if (!shortlisted.length && !activePriorityList) {
      pushActivity({ type: 'research', action: `Searching full investor database for ${deal.name}…`, deal_name: deal.name, dealId: deal.id });
      const fullShortlisted = await queryInvestorDatabase(dealInfo, deal);
      // Filter out already-contacted
      shortlisted = fullShortlisted.filter(inv =>
        !contactedDbIds.has(inv.id) &&
        !contactedFirms.has(normalizeFirmForDedup(inv.name))
      );
    }

    pushActivity({ type: 'research', action: `Shortlisted ${shortlisted.length} investors above score threshold`, deal_name: deal.name, dealId: deal.id });

    if (!shortlisted.length) {
      info(`[${deal.name}] DB QUERY: no new investors to promote`);
      return;
    }

    // ── Promote to contacts ──────────────────────────────────────────────────
    const isWarmList  = activePriorityList?.list_type === 'warm';
    const sourceLabel = activePriorityList
      ? (isWarmList ? `Warm — ${activePriorityList.list_name}` : `List — ${activePriorityList.list_name}`)
      : `Database (${shortlisted[0]?.investor_category || 'General'})`;

    let promoted = 0;
    const batchSnapshot = batch
      ? await getBatchEntitySnapshot(deal.id, batch.created_at)
      : { entityCount: 0, firmKeys: new Set() };
    let remainingEntitySlots = Math.max(0, BATCH_FIRM_TARGET - batchSnapshot.entityCount);
    const batchFirmKeys = new Set(batchSnapshot.firmKeys);

    if (batch && remainingEntitySlots === 0) {
      info(`[${deal.name}] DB QUERY: current batch already full (${BATCH_FIRM_TARGET}/${BATCH_FIRM_TARGET}) — skipping new entities`);
      return;
    }

    for (const investor of shortlisted) {
      // Final dup guard (by linkedin_url)
      if (investor.linkedin_url) {
        const { data: byLi } = await sb.from('contacts').select('id')
          .eq('deal_id', deal.id).eq('linkedin_url', investor.linkedin_url).limit(1);
        if (byLi?.length > 0) continue;
      }

      // Exclusion list check — skip contacts on the per-deal exclusion list
      const contactCandidate = {
        company_name: investor.name,
        name:         investor.decision_maker_name || investor.primary_contact_name || investor.name,
        email:        investor.email || investor.primary_contact_email || null,
      };
      if (await isExcluded(deal.id, contactCandidate)) {
        pushActivity({
          type:      'excluded',
          action:    `EXCLUDED: ${contactCandidate.name || investor.name} at ${investor.name}`,
          note:      'On exclusion list for this deal',
          deal_name: deal.name,
          dealId:    deal.id,
        });
        console.log(`[PIPELINE] Excluded: ${contactCandidate.name} at ${investor.name}`);
        continue;
      }

      // Cross-deal dedup — skip if investor is already an active contact in another deal
      if (investor.id) {
        const { data: activeElsewhere } = await sb.from('contacts')
          .select('deal_id')
          .eq('investors_db_id', investor.id)
          .not('deal_id', 'eq', deal.id)
          .not('pipeline_stage', 'eq', 'Inactive')
          .not('conversation_state', 'in', '("conversation_ended_positive","conversation_ended_negative","meeting_booked","ghosted","do_not_contact")')
          .limit(1);
        if (activeElsewhere?.length) continue;
      }

      // Contacts from investors_db already have structured data (description, thesis, AUM, etc.)
      // — mark as researched so the batch gate doesn't stall waiting for per-person Grok calls
      const alreadyResearched = !!(investor.person_researched || investor.last_researched_at || investor.description || investor.investment_thesis);
      const existingEmail     = investor.email || investor.primary_contact_email || null;
      const existingLinkedin  = null;
      const existingName      = investor.decision_maker_name || investor.primary_contact_name || investor.name;
      const existingTitle     = investor.primary_contact_title || null;

      const notesParts = [];
      if (investor.description)       notesParts.push(investor.description);
      if (investor.research_notes)    notesParts.push(investor.research_notes);
      if (investor.investment_thesis) notesParts.push(`Thesis: ${investor.investment_thesis}`);
      if (investor.past_investments)  notesParts.push(`Past: ${investor.past_investments}`);

      // If investor already has email, skip enrichment entirely — go straight to Enriched
      const skipEnrichment = !!existingEmail;
      const promoteStage   = skipEnrichment ? 'Enriched' : 'Researched';
      const promoteEnrich  = skipEnrichment ? 'enriched'  : 'Pending';
      const { contactType, entityKey } = classifyBatchEntity({
        companyName: investor.name,
        isAngel: investor.is_angel || false,
      });
      const consumesNewEntity = contactType === 'individual' || (entityKey && !batchFirmKeys.has(entityKey));

      if (batch && consumesNewEntity && remainingEntitySlots <= 0) break;

      await sb.from('contacts').insert({
        deal_id:              deal.id,
        name:                 existingName,
        company_name:         investor.name,
        job_title:            existingTitle,
        email:                existingEmail,
        phone:                investor.primary_contact_phone || null,
        linkedin_url:         existingLinkedin,
        geography:            investor.hq_country || investor.hq_location || null,
        aum_fund_size:        investor.aum_millions ? `$${investor.aum_millions}M` : null,
        sector_focus:         (investor.preferred_industries || '').substring(0, 500) || null,
        typical_cheque_size:  investor.typical_cheque_size ||
          (investor.preferred_deal_size_min && investor.preferred_deal_size_max
            ? `$${investor.preferred_deal_size_min}M - $${investor.preferred_deal_size_max}M` : null),
        investor_score:       investor.score,
        investment_thesis:    investor.investment_thesis || null,
        past_investments:     investor.past_investments || null,
        pipeline_stage:       promoteStage,
        enrichment_status:    promoteEnrich,
        source:               sourceLabel,
        notes:                notesParts.join(' | ').substring(0, 1000) || null,
        investors_db_id:      investor.id,
        person_researched:    alreadyResearched,
        is_warm_contact:      isWarmList,
        conversation_state:   isWarmList ? 'warm_not_started' : 'not_started',
        contact_type:         contactType,
        is_angel:             investor.is_angel || false,
        created_at:           new Date().toISOString(),
      });

      pushActivity({ type: 'research', action: `Promoted: ${investor.name} (score: ${investor.score})`, deal_name: deal.name, dealId: deal.id });
      promoted++;
      if (batch && consumesNewEntity) {
        remainingEntitySlots--;
        if (entityKey) batchFirmKeys.add(entityKey);
      }

      if (investor.id) {
        try {
          await sb.from('investor_deal_history').upsert({
            investors_db_id: investor.id,
            deal_id:         deal.id,
            deal_name:       deal.name,
            investor_score:  investor.score,
            grade:           investor.grade,
            pipeline_stage:  'Researched',
            outcome:         'active',
          }, { onConflict: 'investors_db_id,deal_id' });
        } catch (_) {}
      }
    }

    pushActivity({
      type: 'research',
      action: 'Database Query Complete',
      note: `${promoted} investors promoted to pipeline for ${deal.name}`,
      deal_name: deal.name,
      dealId: deal.id,
    });
    info(`[${deal.name}] DB QUERY: ${promoted} investors promoted from ${shortlisted.length} shortlisted`);

  } catch (err) {
    warn(`[${deal.name}] phaseDatabaseQuery failed: ${err.message}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 0c — PENDING FIRM RESEARCH
// Process any firms with status 'pending_research' (e.g. manually added via Campaign tab)
// Uses Grok/Gemini to find decision-makers, then promotes them to contacts.
// ─────────────────────────────────────────────────────────────────────────────

async function phasePendingFirmResearch(deal) {
  const sb = getSupabase();
  if (!sb) return;
  try {
    const { count } = await sb.from('firms')
      .select('id', { count: 'exact', head: true })
      .eq('deal_id', deal.id)
      .eq('status', 'pending_research');
    if (!count) return;
    info(`[${deal.name}] phasePendingFirmResearch: ${count} firm(s) need contact discovery`);
    pushActivity({ type: 'research', action: `Finding decision-makers for ${count} firm(s)`, deal_name: deal.name, dealId: deal.id });
    await runFirmEnrichmentLoop(deal);
  } catch (err) {
    warn(`[${deal.name}] phasePendingFirmResearch failed: ${err.message}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 1 — RANK
// Score all unranked 'Researched' contacts for this deal
// ─────────────────────────────────────────────────────────────────────────────

async function phaseRank(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  // Rank all unscored contacts immediately — investors_db data (thesis, AUM, sector, geography)
  // is already populated at promotion time, so we don't need to wait for person research.
  // Person research (phasePersonResearch) will enrich notes further, and contacts can be
  // re-ranked in a later cycle if the score needs updating after deeper research.
  const { data: contacts, error: err } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Researched', 'RESEARCHED', 'researched', 'Enriched', 'ENRICHED', 'enriched'])
    .is('investor_score', null)
    .order('created_at', { ascending: true })
    .limit(50);

  if (err) { console.warn(`[RANK] Query error: ${err.message}`); return; }
  if (!contacts?.length) { info(`[${deal.name}] phaseRank: nothing to score`); return; }

  console.log(`[ORCHESTRATOR] phaseRank: scoring ${contacts.length} contacts for ${deal.name}`);
  logStep(`Ranking ${contacts.length} investor(s)`, deal.name, 'research', deal);

  for (const contact of contacts) {
    try {
      const result = await rankInvestor({ investor: contact, deal });
      // Map grade to tier
      const tierMap = { 'Hot': 'hot', 'Warm': 'warm', 'Possible': 'possible', 'Archive': 'archive' };
      const tier = tierMap[result.grade] || (result.score >= 75 ? 'hot' : result.score >= 50 ? 'warm' : result.score >= 30 ? 'possible' : 'archive');
      const isArchived = result.grade === 'Archive' || tier === 'archive';
      // If enrichment already ran (any non-pending status), skip straight to Enriched
      const ENRICHMENT_DONE = ['enriched','enriched_apify','linkedin_only','email_invalid_linkedin_only','skipped_no_linkedin','email_only'];
      const enrichmentAlreadyDone = ENRICHMENT_DONE.includes(contact.enrichment_status);
      const newStage = isArchived ? 'Archived' : (enrichmentAlreadyDone || contact.pipeline_stage === 'Enriched') ? 'Enriched' : 'Ranked';
      // Build the update object with score, stage, and tier
      // Classify as individual or institutional based on company_name
      const companyLower = (contact.company_name || '').toLowerCase().trim();
      const isIndividual = !contact.company_name || GENERIC_FIRM_NAMES.has(companyLower) || contact.is_angel;

      const rankUpdate = {
        investor_score: result.score,
        pipeline_stage: newStage,
        tier,
        contact_type: isIndividual ? 'individual' : 'institutional',
      };
      // If fresh contact (never enriched), mark pending so phaseEnrich picks it up
      if (!isArchived && !enrichmentAlreadyDone && !contact.enrichment_status) {
        rankUpdate.enrichment_status = 'pending';
      }
      // Apply intelligence boost from comparable deal analysis
      const boost = await getIntelligenceBoost(contact, deal, sb);
      if (boost.delta > 0) {
        rankUpdate.investor_score = Math.min((result.score || 0) + boost.delta, 100);
      }

      await sb.from('contacts').update({
        ...rankUpdate,
        notes: (() => {
          const clean = (contact.notes || '').replace(/\n?\[SCORE:[^\n]*\]/g, '').trim();
          const baseEntry = `[SCORE: ${result.score} — ${result.grade}] ${result.rationale}`;
          const boostEntry = boost.delta > 0
            ? `\n[INTELLIGENCE BOOST +${boost.delta}] Backed ${boost.times} comparable deal(s): ${boost.companies}`
            : '';
          return clean ? `${clean}\n${baseEntry}${boostEntry}` : `${baseEntry}${boostEntry}`;
        })(),
      }).eq('id', contact.id);

      logStep(`Ranked: ${contact.name}`, `${result.grade} (${result.score})`, 'research', deal);
      await sbLogActivity({
        dealId: deal.id,
        contactId: contact.id,
        eventType: 'RANKED',
        summary: `${contact.name} scored ${result.score} — ${result.grade}`,
        detail: { score: result.score, grade: result.grade, rationale: result.rationale },
      });

      // If enrichment was already done in a prior pass, log it so the feed shows the full flow
      if (!isArchived && enrichmentAlreadyDone) {
        const enrichLabel = contact.enrichment_status === 'enriched' || contact.enrichment_status === 'enriched_apify'
          ? `Email on file (${contact.enrichment_status})`
          : `No email found (${contact.enrichment_status})`;
        pushActivity({ type: 'enrichment', action: `Enriched`, note: `${contact.name} — ${enrichLabel}`, deal_name: deal.name, dealId: deal.id });
      }
    } catch (e) {
      console.warn(`[RANK] Failed for ${contact.name}:`, e.message);
    }
    await sleep(1000);
  }
}

async function getIntelligenceBoost(contact, deal, sb) {
  const none = { delta: 0, times: 0, companies: '' };
  try {
    const searchName = contact.company_name || contact.name;
    if (!searchName) return none;
    const firstName = searchName.split(' ')[0];
    const { data } = await sb.from('deal_investor_scores')
      .select('intelligence_boost, times_backed_similar, backed_companies')
      .eq('deal_id', deal.id)
      .ilike('investor_name', `%${firstName}%`)
      .order('intelligence_boost', { ascending: false })
      .limit(1).maybeSingle();
    if (!data?.intelligence_boost) return none;
    return {
      delta:     data.intelligence_boost,
      times:     data.times_backed_similar,
      companies: (data.backed_companies || []).slice(0, 3).join(', '),
    };
  } catch {
    return none;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 2 — ARCHIVE
// Move low-scoring contacts (Archive grade) out of the active pipeline
// ─────────────────────────────────────────────────────────────────────────────

async function phaseArchive(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  const { data: contacts } = await sb.from('contacts')
    .select('id, name, investor_score')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Ranked', 'RANKED', 'ranked'])
    .lt('investor_score', 45)
    .limit(50);

  if (!contacts?.length) return;

  const ids = contacts.map(c => c.id);
  await sb.from('contacts').update({ pipeline_stage: 'Archived' }).in('id', ids);
  logStep(`Archived ${contacts.length} low-score contacts`, deal.name, 'system', deal);
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 2b — PERSON RESEARCH
// Deep-research each shortlisted contact using Gemini grounded web search.
// Fills firm details, thesis, cheque size, past investments, geography.
// Tracked via [PERSON_RESEARCHED] marker in notes — no extra DB column needed.
// ─────────────────────────────────────────────────────────────────────────────

async function phasePersonResearch(deal, batch) {
  info(`[${deal.name}] phasePersonResearch: skipped by policy — use firm research + LinkedIn profile retrieval instead`);
}

// ─────────────────────────────────────────────────────────────────────────────
// EMAIL VERIFICATION — MillionVerifier
// Called after KASPR/Apify finds an email — validates before saving
// ─────────────────────────────────────────────────────────────────────────────

async function verifyEmailWithMillionVerifier(email) {
  const apiKey = process.env.MILLION_VERIFIER_API_KEY;
  if (!apiKey) {
    console.warn('[ENRICH] MILLION_VERIFIER_API_KEY not set — skipping verification, treating as valid');
    return { valid: true, skipVerification: true };
  }
  try {
    const url = `https://api.millionverifier.com/api/v3/?api=${apiKey}&email=${encodeURIComponent(email)}&timeout=10`;
    const res = await fetch(url, { signal: AbortSignal.timeout(12000) });
    if (!res.ok) {
      console.warn(`[ENRICH] MillionVerifier returned ${res.status} for ${email} — treating as valid`);
      return { valid: true, apiError: true };
    }
    const data = await res.json();
    console.log(`[ENRICH] MillionVerifier: ${email} → result=${data.result}, quality=${data.quality}`);
    const isInvalid = data.result === 'invalid' || data.result === 'disposable';
    const isBadQuality = data.quality === 'bad';
    if (isInvalid || isBadQuality) {
      return { valid: false, result: data.result, quality: data.quality, suggestion: data.didyoumean || null };
    }
    return { valid: true, result: data.result, quality: data.quality };
  } catch (err) {
    console.warn(`[ENRICH] MillionVerifier error for ${email}: ${err.message} — treating as valid`);
    return { valid: true, apiError: true };
  }
}

async function buildDraftLinkedInProfileContext(contactPage) {
  const identifier = contactPage?.linkedin_provider_id || contactPage?.linkedin_url || null;
  if (!identifier) return '';

  const profile = await retrieveLinkedInProfile(identifier).catch(() => null);
  if (!profile) return '';

  const experience = Array.isArray(profile.experience)
    ? profile.experience
      .map(role => [role?.title, role?.company_name || role?.company].filter(Boolean).join(' at '))
      .filter(Boolean)
      .slice(0, 2)
      .join('; ')
    : '';
  const skills = Array.isArray(profile.skills) ? profile.skills.slice(0, 5).join(', ') : '';

  return [
    `LinkedIn headline: ${profile.headline || 'Not available'}`,
    `LinkedIn summary: ${String(profile.summary || '').slice(0, 260) || 'Not available'}`,
    `Current role: ${[profile.current_title, profile.current_company].filter(Boolean).join(' at ') || 'Not available'}`,
    `Location: ${profile.location || 'Not available'}`,
    experience ? `Recent experience: ${experience}` : null,
    skills ? `Skills: ${skills}` : null,
  ].filter(Boolean).join('\n');
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 3 — ENRICH
// Find email/phone for ranked contacts via KASPR; find LinkedIn URLs if missing
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// CORE PER-CONTACT ENRICHMENT — LinkedIn find + KASPR/Apify + MillionVerifier
// Called inline from enrichFirmContacts (firm-complete flow) and as a
// cleanup pass from phaseEnrich (catches anything that slipped through).
// ─────────────────────────────────────────────────────────────────────────────

async function enrichSingleContact(sb, contact, deal, state) {
  // ── Check enriched_contacts cache before calling external APIs ──
  try {
      const { data: cached } = await sb.from('enriched_contacts')
        .select('email, phone')
        .eq('firm_name', contact.company_name || contact.name)
        .not('email', 'is', null)
        .limit(1);
      if (cached?.length > 0) {
        const c = cached[0];
        // Duplicate email guard — check before assigning cached email
        const { data: cacheDupe } = await sb.from('contacts')
          .select('id, name').eq('deal_id', deal.id).eq('email', c.email).neq('id', contact.id).limit(1);
        if (cacheDupe?.length) {
          warn(`[ENRICH] Cached email ${c.email} already assigned to ${cacheDupe[0].name} — using linkedin_only for ${contact.name}`);
          await sb.from('contacts').update({ enrichment_status: 'linkedin_only', pipeline_stage: 'Enriched' }).eq('id', contact.id);
        } else {
          await sb.from('contacts').update({
            email:            c.email,
            phone:            c.phone || null,
            enrichment_status: 'enriched',
            pipeline_stage:   'Enriched',
          }).eq('id', contact.id);
          pushActivity({ type: 'enrichment', action: `Enriched (cached): ${contact.name}`, note: `${c.email}`, deal_name: deal.name, dealId: deal.id });
        }
        return;
      }
    } catch (_) {}
    try {
      // Step 1: ensure LinkedIn URL exists
      let linkedinUrl = contact.linkedin_url;
      if (linkedinUrl && !contact.linkedin_provider_id && !isVerifiedLinkedInSource(contact.enrichment_source || contact.source)) {
        linkedinUrl = null;
      }
      if (!linkedinUrl && state.linkedin_enabled !== false && !hasRecentLinkedInNoMatchSuppression(contact.notes)) {
        linkedinUrl = await findLinkedInUrl({
          name: contact.name,
          company: contact.company_name,
          title: contact.job_title,
        });
        if (linkedinUrl) {
          await sb.from('contacts').update({ linkedin_url: linkedinUrl }).eq('id', contact.id);
        }
      }

      // Pre-enrichment validation: skip contacts with no valid name
      if (!contact.name || contact.name.trim() === '' || contact.name.toLowerCase() === 'null') {
        warn(`[ENRICH] Skipping contact ${contact.id} — no valid name`);
        await sb.from('contacts').update({ enrichment_status: 'skipped_no_name', pipeline_stage: 'Archived' }).eq('id', contact.id);
        return;
      }

      // If contact already has an email, skip KASPR/Apify and advance directly
      if (contact.email) {
        await sb.from('contacts').update({ enrichment_status: 'enriched', pipeline_stage: 'Enriched' }).eq('id', contact.id);
        logStep(`Already has email: ${contact.name}`, deal.name, 'enrichment', deal);
        pushActivity({ type: 'enrichment', action: `Email already on file`, note: `${contact.name} — advanced to Enriched`, deal_name: deal.name, dealId: deal.id });
        return;
      }

      // No LinkedIn URL and no email — completely unreachable, archive immediately
      if (!linkedinUrl) {
        warn(`[ENRICH] No LinkedIn or email for ${contact.name} — archiving`);
        await sb.from('contacts').update({ enrichment_status: 'skipped_no_linkedin', pipeline_stage: 'Archived' }).eq('id', contact.id);
        pushActivity({ type: 'enrichment', action: `Archived (no contact info)`, note: `${contact.name} — no email or LinkedIn found`, deal_name: deal.name, dealId: deal.id });
        return;
      }

      pushActivity({ type: 'enrichment', action: `Enriching: ${contact.name}`, note: `${contact.company_name || ''} — trying KASPR...`, deal_name: deal.name, dealId: deal.id });

      // Step 2: enrich email/phone via KASPR first
      const kasprResult = await enrichWithKaspr({ linkedinUrl, fullName: contact.name });

      if (kasprResult === 'RATE_LIMITED') {
        warn('[ENRICH] KASPR rate limited — skipping enrichment for this contact');
        return;
      }

      let enrichResult = kasprResult;
      let enrichSource = 'kaspr';

      if (!kasprResult?.email && !kasprResult?.phone) {
        // KASPR returned nothing — try Apify
        logStep(`KASPR no data for ${contact.name} — trying Apify...`, deal.name, 'enrichment', deal);
        pushActivity({ type: 'enrichment', action: `KASPR returned no data`, note: `${contact.name} — trying Apify...`, deal_name: deal.name, dealId: deal.id });

        try {
          const apifyResult = await enrichWithApify({ linkedin_url: linkedinUrl, name: contact.name });
          if (apifyResult?.email || apifyResult?.phone) {
            enrichResult = apifyResult;
            enrichSource = 'apify';
            logStep(`Apify found email for ${contact.name}`, deal.name, 'enrichment', deal);
            pushActivity({ type: 'enrichment', action: `Apify found email`, note: `${contact.name} — ${apifyResult?.email || ''}`, deal_name: deal.name, dealId: deal.id });
          } else {
            // No email/phone — save any profile fields Apify returned and advance to Enriched (linkedin_only)
            const profileUpdates = { enrichment_status: 'linkedin_only', enrichment_source: null, pipeline_stage: 'Enriched' };
            if (apifyResult?.headline && !contact.job_title)     profileUpdates.job_title = apifyResult.headline;
            if (apifyResult?.company_name && !contact.company_name) profileUpdates.company_name = apifyResult.company_name;
            if (apifyResult?.linkedin_provider_id && !contact.linkedin_provider_id) profileUpdates.linkedin_provider_id = apifyResult.linkedin_provider_id;
            await sb.from('contacts').update(profileUpdates).eq('id', contact.id);
            logStep(`No email found for ${contact.name} — LinkedIn-only outreach`, deal.name, 'enrichment', deal);
            pushActivity({ type: 'enrichment', action: `No email found`, note: `${contact.name} at ${contact.company_name || ''} — queued for LinkedIn-only outreach`, deal_name: deal.name, dealId: deal.id });
            return;
          }
        } catch (apifyErr) {
          warn(`[ENRICH] Apify error for ${contact.name}: ${apifyErr.message}`);
          await sb.from('contacts').update({ enrichment_status: 'linkedin_only', pipeline_stage: 'Enriched' }).eq('id', contact.id);
          return;
        }
      } else {
        logStep(`KASPR found data for ${contact.name}`, deal.name, 'enrichment', deal);
        pushActivity({ type: 'enrichment', action: `KASPR found email`, note: `${contact.name} — ${kasprResult?.email || ''}`, deal_name: deal.name, dealId: deal.id });
      }

      // Step: MillionVerifier — validate email before saving
      if (enrichResult?.email) {
        pushActivity({ type: 'enrichment', action: `Verifying email`, note: `${contact.name} — MillionVerifier...`, deal_name: deal.name, dealId: deal.id });
        const verification = await verifyEmailWithMillionVerifier(enrichResult.email);
        if (!verification.valid) {
          logStep(`Email invalid for ${contact.name}`, `${verification.result}, ${verification.quality} — LinkedIn-only`, 'enrichment', deal);
          pushActivity({ type: 'enrichment', action: `Email failed verification`, note: `${contact.name} — ${verification.result}/${verification.quality} — LinkedIn-only outreach`, deal_name: deal.name, dealId: deal.id });
          await sbLogActivity({
            dealId: deal.id, contactId: contact.id,
            eventType: 'EMAIL_INVALID',
            summary: `Email for ${contact.name} failed MillionVerifier (${verification.result}, quality: ${verification.quality}) — LinkedIn-only`,
            detail: { email: enrichResult.email, result: verification.result, quality: verification.quality, suggestion: verification.suggestion },
          });
          await sb.from('contacts').update({
            email: null,
            phone: enrichResult?.phone || null,
            enrichment_status: 'email_invalid_linkedin_only',
            pipeline_stage: 'Enriched',
            enrichment_source: enrichSource,
          }).eq('id', contact.id);
          return; // handled — pick up in outreach queue as linkedin_only type
        }
        // Email verified OK — proceed to save below
        logStep(`Email verified for ${contact.name}`, `${verification.result || 'ok'} quality: ${verification.quality || 'ok'}`, 'enrichment', deal);
      }

      const updates = { enrichment_status: enrichResult?.email ? 'enriched' : 'linkedin_only', enrichment_source: enrichSource };
      if (enrichResult?.email) {
        // Duplicate email guard — no two contacts in the same deal may share an email
        const { data: emailDupe } = await sb.from('contacts')
          .select('id, name')
          .eq('deal_id', deal.id)
          .eq('email', enrichResult.email)
          .neq('id', contact.id)
          .limit(1);
        if (emailDupe?.length) {
          warn(`[ENRICH] Duplicate email ${enrichResult.email} — already assigned to ${emailDupe[0].name}. Skipping email for ${contact.name}`);
          pushActivity({ type: 'enrichment', action: `Duplicate email skipped`, note: `${contact.name} — ${enrichResult.email} already assigned to ${emailDupe[0].name}`, deal_name: deal.name, dealId: deal.id });
          updates.enrichment_status = 'linkedin_only';
        } else {
          updates.email = enrichResult.email;
          updates.enrichment_status = enrichSource === 'apify' ? 'enriched_apify' : 'enriched';
        }
      }
      if (enrichResult?.phone) updates.phone = enrichResult.phone;
      // Extra profile data Apify may return
      if (enrichResult?.source === 'apify') {
        if (enrichResult.headline && !contact.job_title)            updates.job_title = enrichResult.headline;
        if (enrichResult.linkedin_provider_id && !contact.linkedin_provider_id) updates.linkedin_provider_id = enrichResult.linkedin_provider_id;

        // Company mismatch check: Apify returned a current_company that doesn't
        // match the firm this contact is assigned to.  Log it but do NOT block —
        // the investor may work across multiple firms or Apify may be slightly off.
        if (enrichResult.company_name && contact.company_name) {
          const enrichFirmNorm = normalizeFirmIdentity(enrichResult.company_name);
          const assignedFirmNorm = normalizeFirmIdentity(contact.company_name);
          if (enrichFirmNorm && assignedFirmNorm && enrichFirmNorm !== assignedFirmNorm) {
            const enrichTokens = new Set(enrichFirmNorm.split(' ').filter(Boolean));
            const assignedTokens = new Set(assignedFirmNorm.split(' ').filter(Boolean));
            let inter = 0;
            for (const t of enrichTokens) if (assignedTokens.has(t)) inter++;
            const union = enrichTokens.size + assignedTokens.size - inter;
            const similarity = union > 0 ? inter / union : 0;
            if (similarity < 0.5) {
              pushActivity({
                type: 'warning',
                action: '[MATCH] Company mismatch — contact may be at a different firm',
                note: `${contact.name} — Apify shows "${enrichResult.company_name}", assigned to "${contact.company_name}" (${Math.round(similarity * 100)}% match)`,
                deal_name: deal.name,
                dealId: deal.id,
              });
            }
          }
        } else if (enrichResult.company_name && !contact.company_name) {
          updates.company_name = enrichResult.company_name;
        }
      }

      updates.pipeline_stage = 'Enriched'; // Always advance to Enriched after enrichment attempt
      await sb.from('contacts').update(updates).eq('id', contact.id);

      // ── Write to enriched_contacts cache ──
      const enrichedEmail = enrichResult?.email || null;
      const enrichedPhone = enrichResult?.phone || null;
      if (enrichedEmail || linkedinUrl) {
        try {
          await sb.from('enriched_contacts').upsert({
            investors_db_id: contact.investors_db_id || null,
            firm_name:  contact.company_name || contact.name,
            name:       contact.name,
            title:      contact.job_title || null,
            email:      enrichedEmail,
            phone:      enrichedPhone,
            linkedin_url: linkedinUrl || null,
            source:     enrichSource,
            verified:   !!(enrichedEmail && updates.enrichment_status !== 'email_invalid_linkedin_only'),
            updated_at: new Date().toISOString(),
          }, { onConflict: 'firm_name,email', ignoreDuplicates: false });
        } catch (e) { /* non-fatal */ }
      }

      // ── Write enrichment back to investors_db so future deals reuse email/LinkedIn ──
      if (contact.investors_db_id) {
        try {
          const enrichDbUpdates = { enrichment_status: updates.enrichment_status };
          if (enrichResult?.email)    enrichDbUpdates.email = enrichResult.email;
          if (linkedinUrl)            enrichDbUpdates.decision_maker_linkedin = linkedinUrl;
          if (contact.name && contact.name !== contact.company_name) {
            enrichDbUpdates.decision_maker_name = contact.name;
          }
          await sb.from('investors_db').update(enrichDbUpdates).eq('id', contact.investors_db_id);
          console.log(`[ENRICH PERSIST] Saved enrichment for ${contact.name} → investors_db`);
        } catch (e) { console.warn(`[ENRICH PERSIST] investors_db write-back failed: ${e.message}`); }
      }

      logStep(`Enriched: ${contact.name}`, enrichResult?.email ? `email found via ${enrichSource}` : 'no data', 'enrichment', deal);
      await sbLogActivity({
        dealId: deal.id,
        contactId: contact.id,
        eventType: 'ENRICHED',
        summary: `${contact.name} enriched via ${enrichSource} — ${enrichResult?.email ? enrichResult.email : 'no email found'}`,
        detail: { source: enrichSource, email: enrichResult?.email || null },
      });
    } catch (e) {
      console.warn(`[ENRICH] Failed for ${contact.name}:`, e.message);
    }
    await sleep(1500);
}

export async function phaseEnrich(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  // Cleanup pass: catch any contacts that missed inline enrichment (CSV imports, legacy, etc.)
  const { data: contacts } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Ranked', 'RANKED', 'ranked', 'Researched', 'RESEARCHED', 'researched', 'Approved for Outreach'])
    .in('enrichment_status', ['Pending', 'pending', 'Partial', 'partial'])
    .limit(10);

  if (contacts?.length) {
    console.log(`[ORCHESTRATOR] phaseEnrich: cleanup-enriching ${contacts.length} contact(s) for ${deal.name}`);
    logStep(`Enriching ${contacts.length} contact(s)`, deal.name, 'enrichment', deal);
    for (const contact of contacts) {
      await enrichSingleContact(sb, contact, deal, state);
    }
  }

  // Secondary pass: find LinkedIn for contacts that have email (e.g. from CSV) but no linkedin_url
  // phaseLinkedInInvites requires linkedin_url — without this, CSV contacts never get invites
  // Process in batches of 5 to avoid LinkedIn 429 rate limits
  if (state.linkedin_enabled !== false) {
    const { data: needLinkedIn } = await sb.from('contacts')
      .select('id, name, company_name, job_title, email, enrichment_status')
      .eq('deal_id', deal.id)
      .in('pipeline_stage', ['Ranked', 'RANKED', 'ranked', 'Enriched', 'ENRICHED', 'enriched'])
      .is('linkedin_url', null)
      .limit(5); // batch of 5 per cycle to stay well under LinkedIn rate limits

    for (const contact of (needLinkedIn || [])) {
      try {
        if (hasRecentLinkedInNoMatchSuppression(contact.notes)) {
          continue;
        }
        const url = await findLinkedInUrl({ name: contact.name, company: contact.company_name, title: contact.job_title });
        if (url) {
          await sb.from('contacts').update({ linkedin_url: url }).eq('id', contact.id);
          logStep(`LinkedIn found: ${contact.name}`, deal.name, 'enrichment', deal);
        }
      } catch (e) {
        console.warn(`[ENRICH] LinkedIn find failed for ${contact.name}:`, e.message);
      }
      await sleep(2000); // 2s between calls — gentler on LinkedIn API
    }
  }

  // Email-only advancement: Ranked contacts with email but still no linkedin_url → advance to 'Enriched'
  // so phaseOutreach can send them emails directly (LinkedIn invite will be skipped)
  const { data: emailOnly } = await sb.from('contacts')
    .select('id, name')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Ranked', 'RANKED', 'ranked'])
    .not('email', 'is', null)
    .is('linkedin_url', null);

  if (emailOnly?.length) {
    const ids = emailOnly.map(c => c.id);
    await sb.from('contacts').update({ pipeline_stage: 'Enriched' }).in('id', ids);
    logStep(`${emailOnly.length} email-only contact(s) advanced to outreach (no LinkedIn found)`, deal.name, 'enrichment', deal);
    await sbLogActivity({
      dealId: deal.id,
      eventType: 'EMAIL_ONLY_ADVANCE',
      summary: `${emailOnly.length} contact(s) with email but no LinkedIn advanced to email outreach`,
    });
  }

  // No-contact archive: Ranked contacts with no email AND no linkedin_url after enrichment attempts → Archive
  const { data: noContact } = await sb.from('contacts')
    .select('id, name')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Ranked', 'RANKED', 'ranked'])
    .in('enrichment_status', ['No Data', 'Complete'])
    .is('email', null)
    .is('linkedin_url', null);

  if (noContact?.length) {
    const ids = noContact.map(c => c.id);
    await sb.from('contacts').update({ pipeline_stage: 'Archived', archive_reason: 'No email or LinkedIn found' }).in('id', ids);
    logStep(`Archived ${noContact.length} contact(s) — no email or LinkedIn found`, deal.name, 'system', deal);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// MANUAL ENRICHMENT — triggered from dashboard
// Like phaseEnrich but:
//  - targets ALL Ranked contacts regardless of prior enrichment_status (retries)
//  - resets enrichment_status to 'Pending' before attempting so the main flow picks up fresh
//  - higher batch limit (20 contacts) to do a full sweep in one go
// ─────────────────────────────────────────────────────────────────────────────

export async function runManualEnrich(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  // Find all Ranked contacts — skip only ones already successfully Enriched (full stage)
  const { data: candidates } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .eq('pipeline_stage', 'Ranked')
    .limit(20);

  if (!candidates?.length) {
    info(`[${deal.name}] runManualEnrich: no Ranked contacts to enrich`);
    pushActivity({ type: 'enrichment', action: 'Manual Enrichment', note: `No Ranked contacts found for ${deal.name}`, deal_name: deal.name });
    return;
  }

  // Reset enrichment_status to Pending so the logic below re-attempts all
  const resetIds = candidates.map(c => c.id);
  await sb.from('contacts').update({ enrichment_status: 'Pending' }).in('id', resetIds);

  pushActivity({
    type: 'enrichment',
    action: 'Manual Enrichment Running',
    note: `${candidates.length} Ranked contact(s) queued for KASPR → Apify — ${deal.name}`,
    deal_name: deal.name,
  });
  console.log(`[MANUAL ENRICH] Processing ${candidates.length} Ranked contacts for ${deal.name}`);

  for (const contact of candidates) {
    try {
      // Step 1: find LinkedIn URL if missing
      let linkedinUrl = contact.linkedin_url;
      if (linkedinUrl && !contact.linkedin_provider_id && !isVerifiedLinkedInSource(contact.enrichment_source || contact.source)) {
        linkedinUrl = null;
      }
      if (!linkedinUrl && state.linkedin_enabled !== false && !hasRecentLinkedInNoMatchSuppression(contact.notes)) {
        linkedinUrl = await findLinkedInUrl({ name: contact.name, company: contact.company_name, title: contact.job_title });
        if (linkedinUrl) await sb.from('contacts').update({ linkedin_url: linkedinUrl }).eq('id', contact.id);
      }

      if (!contact.name || contact.name.trim() === '' || contact.name.toLowerCase() === 'null') {
        await sb.from('contacts').update({ enrichment_status: 'skipped_no_name', pipeline_stage: 'Archived' }).eq('id', contact.id);
        continue;
      }

      if (!linkedinUrl && !contact.email) {
        await sb.from('contacts').update({ enrichment_status: 'skipped_no_linkedin' }).eq('id', contact.id);
        pushActivity({ type: 'enrichment', action: `No LinkedIn or email`, note: `${contact.name} — skipped`, deal_name: deal.name });
        continue;
      }

      if (!linkedinUrl && contact.email) {
        await sb.from('contacts').update({ enrichment_status: 'email_only', pipeline_stage: 'Enriched' }).eq('id', contact.id);
        pushActivity({ type: 'enrichment', action: `Email-only`, note: `${contact.name} — no LinkedIn, advancing via email`, deal_name: deal.name });
        continue;
      }

      pushActivity({ type: 'enrichment', action: `Enriching: ${contact.name}`, note: `${contact.company_name || ''} — trying KASPR...`, deal_name: deal.name });

      // Step 2: KASPR first
      const kasprResult = await enrichWithKaspr({ linkedinUrl, fullName: contact.name });

      if (kasprResult === 'RATE_LIMITED') {
        warn('[MANUAL ENRICH] KASPR rate limited — stopping this batch');
        pushActivity({ type: 'enrichment', action: 'KASPR Rate Limited', note: 'Manual enrichment paused — retry later', deal_name: deal.name });
        break;
      }

      let enrichResult = kasprResult;
      let enrichSource = 'kaspr';

      if (!kasprResult?.email && !kasprResult?.phone) {
        // Step 3: Apify fallback
        pushActivity({ type: 'enrichment', action: `KASPR: no data`, note: `${contact.name} — trying Apify...`, deal_name: deal.name });
        try {
          const apifyResult = await enrichWithApify({ linkedin_url: linkedinUrl, name: contact.name });
          if (apifyResult?.email || apifyResult?.phone) {
            enrichResult = apifyResult;
            enrichSource = 'apify';
            pushActivity({ type: 'enrichment', action: `Apify: email found`, note: `${contact.name}`, deal_name: deal.name });
          } else {
            const profileUpdates = { enrichment_status: 'linkedin_only', pipeline_stage: 'Enriched' };
            if (apifyResult?.headline && !contact.job_title)             profileUpdates.job_title = apifyResult.headline;
            if (apifyResult?.company_name && !contact.company_name)      profileUpdates.company_name = apifyResult.company_name;
            if (apifyResult?.linkedin_provider_id && !contact.linkedin_provider_id) profileUpdates.linkedin_provider_id = apifyResult.linkedin_provider_id;
            await sb.from('contacts').update(profileUpdates).eq('id', contact.id);
            pushActivity({ type: 'enrichment', action: `No email found`, note: `${contact.name} — LinkedIn-only outreach`, deal_name: deal.name });
            continue;
          }
        } catch (apifyErr) {
          warn(`[MANUAL ENRICH] Apify error for ${contact.name}: ${apifyErr.message}`);
          await sb.from('contacts').update({ enrichment_status: 'linkedin_only', pipeline_stage: 'Enriched' }).eq('id', contact.id);
          continue;
        }
      } else {
        pushActivity({ type: 'enrichment', action: `KASPR: email found`, note: `${contact.name}`, deal_name: deal.name });
      }

      const updates = { enrichment_status: enrichResult?.email ? (enrichSource === 'apify' ? 'enriched_apify' : 'enriched') : 'linkedin_only', enrichment_source: enrichSource };
      if (enrichResult?.email) updates.email = enrichResult.email;
      if (enrichResult?.phone) updates.phone = enrichResult.phone;
      if (enrichResult?.source === 'apify') {
        if (enrichResult.headline && !contact.job_title)             updates.job_title = enrichResult.headline;
        if (enrichResult.company_name && !contact.company_name)      updates.company_name = enrichResult.company_name;
        if (enrichResult.linkedin_provider_id && !contact.linkedin_provider_id) updates.linkedin_provider_id = enrichResult.linkedin_provider_id;
      }
      updates.pipeline_stage = 'Enriched'; // Always advance to Enriched after enrichment attempt
      await sb.from('contacts').update(updates).eq('id', contact.id);

      await sbLogActivity({
        dealId: deal.id, contactId: contact.id, eventType: 'ENRICHED',
        summary: `${contact.name} enriched via ${enrichSource} — ${enrichResult?.email ? enrichResult.email : 'no email found'}`,
        detail: { source: enrichSource, email: enrichResult?.email || null },
      }).catch(() => {});
    } catch (e) {
      console.warn(`[MANUAL ENRICH] Failed for ${contact.name}:`, e.message);
    }
    await sleep(1500);
  }

  pushActivity({ type: 'enrichment', action: 'Manual Enrichment Complete', note: deal.name, deal_name: deal.name });
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 0b — CROSS-DEAL CHECK
// Before ranking new contacts, check if they're already in another deal's pipeline.
// Active deal → hold them (Skipped) until that deal closes.
// Closed deal → add prior outreach context to notes so AI doesn't act like a stranger.
// ─────────────────────────────────────────────────────────────────────────────

async function phaseCrossDealCheck(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  // Only check new unranked contacts that haven't been cross-deal checked yet
  const { data: candidates } = await sb.from('contacts')
    .select('id, name, linkedin_url, company_name, notes')
    .eq('deal_id', deal.id)
    .eq('pipeline_stage', 'Researched')
    .not('linkedin_url', 'is', null)
    .not('notes', 'like', '%[CROSS_DEAL%')
    .limit(30);

  if (!candidates?.length) return;

  const urls = candidates.map(c => c.linkedin_url).filter(Boolean);

  // Find these LinkedIn URLs in any OTHER deal
  const { data: matches } = await sb.from('contacts')
    .select('linkedin_url, deal_id, pipeline_stage, notes, invite_sent_at, name')
    .in('linkedin_url', urls)
    .neq('deal_id', deal.id);

  if (!matches?.length) return;

  // Fetch statuses for the matched deals
  const matchDealIds = [...new Set(matches.map(m => m.deal_id))];
  const { data: matchDeals } = await sb.from('deals').select('id, name, status').in('id', matchDealIds);
  const dealMap = Object.fromEntries((matchDeals || []).map(d => [d.id, d]));

  // Build lookup: linkedin_url → most relevant match (prefer ACTIVE over CLOSED)
  const urlToMatch = {};
  for (const m of matches) {
    const d = dealMap[m.deal_id];
    if (!d) continue;
    const prev = urlToMatch[m.linkedin_url];
    if (!prev || d.status === 'ACTIVE') urlToMatch[m.linkedin_url] = { ...m, deal: d };
  }

  for (const contact of candidates) {
    const match = urlToMatch[contact.linkedin_url];
    if (!match) continue;

    if (match.deal.status === 'ACTIVE') {
      // Hold until that deal closes
      const holdNote = `[CROSS_DEAL_HOLD:${match.deal_id}|${match.deal.name}]`;
      const newNotes = contact.notes ? `${contact.notes}\n${holdNote}` : holdNote;
      await sb.from('contacts').update({ pipeline_stage: 'Skipped', notes: newNotes }).eq('id', contact.id);
      info(`[${deal.name}] Cross-deal hold: ${contact.name} is already in active deal "${match.deal.name}"`);
    } else {
      // Previously in a closed deal — add prior context, let them proceed normally
      const when = match.invite_sent_at
        ? new Date(match.invite_sent_at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })
        : 'previously';
      const priorNote = `[PRIOR_DEAL:${match.deal.name}|contacted:${when}]`;
      const newNotes = contact.notes ? `${contact.notes}\n${priorNote}` : priorNote;
      await sb.from('contacts').update({ notes: newNotes }).eq('id', contact.id);
      info(`[${deal.name}] Prior deal context added for ${contact.name} (closed deal: "${match.deal.name}")`);
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 4 — NOTION SYNC
// LinkedIn invite-first sequencing helper
// ─────────────────────────────────────────────────────────────────────────────

const ACTIVE_STAGES = ['Ranked', 'Enriched', 'invite_sent', 'invite_accepted', 'pending_email_approval', 'pending_dm_approval', 'Email Approved', 'DM Approved', 'Email Sent', 'DM Sent', 'email_sent', 'dm_sent', 'Replied', 'In Conversation', 'Meeting Booked', 'Meeting Scheduled'];

// Pipeline health constants
const DAILY_INVITE_TARGET = 28;        // default target LinkedIn invites per day
const REACTIVATION_MIN_SCORE = 40;     // re-promote archived contacts with score >= this
const PIPELINE_LOW_THRESHOLD = 10;     // trigger top-up when ready contacts drop below this
const RESEARCH_COOLDOWN_MS = 24 * 60 * 60 * 1000; // max once per day per deal

function getSeniorityRank(jobTitle) {
  const title = String(jobTitle || '').toLowerCase();
  if (!title) return 50;
  if (/(analyst|investment banking analyst|research analyst|intern|coordinator)/.test(title)) return 5;
  if (/(associate|senior associate)/.test(title)) return 10;
  if (/(vice president|vp|principal|investment manager|manager)/.test(title)) return 20;
  if (/(director|managing director|head|founder|co-founder)/.test(title)) return 30;
  if (/(partner|managing partner|general partner|gp|chief investment officer|cio)/.test(title)) return 40;
  return 25;
}

// Persist cooldown to DB (parsed_deal_info JSONB) so it survives PM2 restarts
async function getLastResearchTime(sb, dealId) {
  const { data } = await sb.from('deals').select('parsed_deal_info').eq('id', dealId).single();
  const ts = data?.parsed_deal_info?.last_firm_research_at;
  return ts ? new Date(ts).getTime() : 0;
}
async function setLastResearchTime(sb, dealId) {
  const { data } = await sb.from('deals').select('parsed_deal_info').eq('id', dealId).single();
  const parsed_deal_info = { ...(data?.parsed_deal_info || {}), last_firm_research_at: new Date().toISOString() };
  await sb.from('deals').update({ parsed_deal_info }).eq('id', dealId);
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 0 — TOP-UP PIPELINE
// Ensure the pipeline has enough contacts to hit the daily invite target.
// If running low: promote borderline archived contacts → then run new research.
// ─────────────────────────────────────────────────────────────────────────────

async function phaseTopUpPipeline(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  // Contacts ready for LinkedIn invites (Ranked/Enriched, has URL, not yet invited)
  const { count: pipelineReady } = await sb.from('contacts')
    .select('id', { count: 'exact', head: true })
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Ranked', 'Enriched'])
    .not('linkedin_url', 'is', null)
    .is('invite_sent_at', null);

  // Archived contacts that could be promoted (score >= threshold)
  const { count: archivedEligible } = await sb.from('contacts')
    .select('id', { count: 'exact', head: true })
    .eq('deal_id', deal.id)
    .eq('pipeline_stage', 'Archived')
    .gte('investor_score', REACTIVATION_MIN_SCORE);

  const ready = pipelineReady || 0;
  const eligible = archivedEligible || 0;

  info(`[${deal.name}] Pipeline: ${ready} ready for invite, ${eligible} eligible archived`);

  // Step 1: Promote borderline archived contacts if pipeline is running low
  if (ready < PIPELINE_LOW_THRESHOLD && eligible > 0) {
    const needed = Math.max(PIPELINE_LOW_THRESHOLD - ready, 5);
    const { data: toPromote } = await sb.from('contacts')
      .select('id, name, investor_score')
      .eq('deal_id', deal.id)
      .eq('pipeline_stage', 'Archived')
      .gte('investor_score', REACTIVATION_MIN_SCORE)
      .order('investor_score', { ascending: false })
      .limit(needed);

    if (toPromote?.length) {
      await sb.from('contacts')
        .update({ pipeline_stage: 'Ranked', enrichment_status: 'Pending' })
        .in('id', toPromote.map(c => c.id));
      info(`[${deal.name}] Auto-reactivated ${toPromote.length} archived contacts`);
      pushActivity({ type: 'PIPELINE', action: 'Pipeline Top-Up', note: `${toPromote.length} borderline contacts reactivated for ${deal.name}` });
    }
  }

  // Step 2: If pipeline still critically low (no archived eligible either), run new research
  // Cooldown persisted to DB so it survives PM2 restarts
  if (ready + eligible < PIPELINE_LOW_THRESHOLD) {
    const lastRun = await getLastResearchTime(sb, deal.id);
    const msSinceLast = Date.now() - lastRun;
    if (msSinceLast < RESEARCH_COOLDOWN_MS) {
      const hoursLeft = Math.ceil((RESEARCH_COOLDOWN_MS - msSinceLast) / 3_600_000);
      info(`[${deal.name}] Pipeline low but research cooldown active — next run in ~${hoursLeft}h`);
      return;
    }
    info(`[${deal.name}] Pipeline depleted — running daily top-up firm research`);
    await setLastResearchTime(sb, deal.id);
    try {
      const saved = await runFirmResearch(deal);
      if (saved > 0) {
        pushActivity({ type: 'RESEARCH', action: 'Auto Top-Up Research', note: `Found ${saved} new contacts for ${deal.name}` });
      }
    } catch (e) {
      warn(`[${deal.name}] Top-up research failed: ${e.message}`);
      try {
        const saved2 = await runDealResearch(deal);
        if (saved2 > 0) pushActivity({ type: 'RESEARCH', action: 'Top-Up (legacy)', note: `Found ${saved2} contacts for ${deal.name}` });
      } catch (e2) { warn(`[${deal.name}] Legacy research also failed: ${e2.message}`); }
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 5a — LINKEDIN INVITES
// ─────────────────────────────────────────────────────────────────────────────
// PHASE TEMP CLOSE CHECK — Re-engage temp_closed contacts after 5 days
// ─────────────────────────────────────────────────────────────────────────────

async function phaseTempCloseCheck(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  try {
    const allStale = await checkTempClosedContacts();
    const stale    = allStale.filter(c => c.deal_id === deal.id);

    if (!stale.length) return;

    info(`[${deal.name}] phaseTempCloseCheck: ${stale.length} temp-closed contact(s) overdue for re-engagement`);

    // Process max 3 per cycle to avoid flooding
    for (const contact of stale.slice(0, 3)) {
      try {
        const history   = await getConversationHistory(contact.id);
        const followUp  = await draftTempCloseFollowUp({ contact, deal, conversationHistory: history });

        if (!followUp) {
          info(`[TEMP_CLOSE] Could not draft re-engagement for ${contact.name}`);
          continue;
        }

        // Build a contact-page-like object for sendEmailForApproval
        const contactPage = buildContactPage(contact);
        const fakeDraft   = { body: followUp, subject: `Re: ${deal.name}`, alternativeSubject: null };

        contactsInFlight.add(contact.id);
        try {
          const decision = await sendEmailForApproval(contactPage, fakeDraft, contact.notes || '', contact.investor_score || 0, 'FOLLOW-UP', deal.id);

          if (decision.action === 'approve') {
            const channel = contact.email ? 'email' : 'linkedin_dm';
            let sent = null;

            if (channel === 'email' && contact.email) {
              sent = await unipileSendEmail({
                to:      contact.email,
                toName:  contact.name,
                subject: decision.subject || fakeDraft.subject,
                body:    followUp,
              });
            } else if (channel === 'linkedin_dm' && contact.linkedin_provider_id) {
              sent = await sendLinkedInDM({
                attendeeProviderId: contact.linkedin_provider_id,
                message: followUp,
              });
            }

            if (sent) {
              const newFollowUpCount = (contact.follow_up_count || 0) + 1;
              const followupDays     = deal?.followup_days_email || 3;

              // If they've ghosted 2+ times, move to ghosted and let phaseOutreach pick up next person
              if (newFollowUpCount >= 2) {
                await setConversationState(contact.id, 'ghosted', {
                  pipeline_stage:   'Inactive',
                  follow_up_due_at: null,
                });
                info(`[TEMP_CLOSE] ${contact.name} ghosted after ${newFollowUpCount} touches — marked Inactive`);
              } else {
                await setConversationState(contact.id, 'awaiting_response', {
                  follow_up_count:   newFollowUpCount,
                  follow_up_due_at:  new Date(Date.now() + followupDays * 24 * 60 * 60 * 1000).toISOString(),
                  temp_closed_at:    null,
                  next_follow_up_due: null,
                });
              }

              // Log to conversation_messages
              await logConversationMessage({
                contactId: contact.id,
                dealId:    deal.id,
                direction: 'outbound',
                channel,
                body:      followUp,
              }).catch(() => {});

              pushActivity({
                type:      channel === 'email' ? 'email' : 'dm',
                action:    `Re-engagement sent`,
                note:      `${contact.name} @ ${contact.company_name || ''} (temp_close follow-up)`,
                deal_name: deal.name,
                dealId:    deal.id,
              });

              await sbLogActivity({
                dealId:    deal.id,
                contactId: contact.id,
                eventType: 'TEMP_CLOSE_FOLLOWUP_SENT',
                summary:   `Re-engagement sent to ${contact.name} after temp close`,
                apiUsed:   'unipile',
              });
            }
          } else {
            await handleNonApproval(contact, decision.action);
          }
        } finally {
          contactsInFlight.delete(contact.id);
        }
      } catch (err) {
        warn(`[TEMP_CLOSE] Failed to process ${contact.name}: ${err.message}`);
      }
    }
  } catch (err) {
    warn(`[TEMP_CLOSE] phaseTempCloseCheck failed for ${deal.name}: ${err.message}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Send connection requests to Ranked/Enriched contacts with LinkedIn URLs
// ─────────────────────────────────────────────────────────────────────────────

async function phaseLinkedInInvites(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  if (!isWithinChannelWindow(deal, 'linkedin_invite')) {
    info(`[${deal.name}] phaseLinkedInInvites: outside LinkedIn connection window — skipping`);
    return;
  }

  // Reactivate any contacts deferred by weekly LinkedIn limit whose retry window has passed
  try {
    const { data: weeklyLimitExpired } = await sb.from('contacts')
      .select('id, enrichment_status')
      .eq('deal_id', deal.id)
      .eq('pipeline_stage', 'linkedin_weekly_limit')
      .lt('follow_up_due_at', new Date().toISOString())
      .not('linkedin_url', 'is', null);
    if (weeklyLimitExpired?.length) {
      for (const c of weeklyLimitExpired) {
        const stage = c.enrichment_status === 'enriched' || c.enrichment_status === 'enriched_apify' ? 'Enriched' : 'Ranked';
        await sb.from('contacts').update({ pipeline_stage: stage, follow_up_due_at: null }).eq('id', c.id);
      }
      info(`[${deal.name}] Reactivated ${weeklyLimitExpired.length} contact(s) after LinkedIn weekly limit reset`);
    }
  } catch {}

  // Check how many invites already sent today
  const todayStart = new Date();
  todayStart.setHours(0, 0, 0, 0);
  const { count: sentToday } = await sb.from('contacts')
    .select('id', { count: 'exact', head: true })
    .eq('deal_id', deal.id)
    .gte('invite_sent_at', todayStart.toISOString());

  const dailyTarget = deal.linkedin_daily_limit || DAILY_INVITE_TARGET;
  const remainingToday = dailyTarget - (sentToday || 0);

  if (remainingToday <= 0) {
    info(`[${deal.name}] Daily LinkedIn invite limit reached (${sentToday}/${dailyTarget})`);
    return;
  }

  // LinkedIn invite strategy:
  // - connection requests are passive — send to all contacts at a firm proactively
  // - only block a firm once someone there has ACTIVELY RESPONDED (replied, meeting booked, conversation)
  // - don't block just because a pending approval, DM, or email is in flight — those are still cold outreach
  const RESPONDED_STAGES = ['In Conversation', 'Replied', 'Meeting Booked', 'Meeting Scheduled'];

  const { data: respondedContacts } = await sb.from('contacts')
    .select('company_name, response_received')
    .eq('deal_id', deal.id)
    .or(`response_received.eq.true,pipeline_stage.in.(${RESPONDED_STAGES.map(s => `"${s}"`).join(',')})`)
    .not('company_name', 'is', null);

  const blockedFirms = new Set();
  for (const c of respondedContacts || []) {
    const firm = (c.company_name || '').toLowerCase().trim();
    if (!firm || GENERIC_FIRM_NAMES.has(firm)) continue;
    blockedFirms.add(firm);
  }

  const perCycleLimit = Math.min(8, remainingToday);

  const { data: candidates } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Ranked', 'RANKED', 'ranked', 'Enriched', 'ENRICHED', 'enriched'])  // both tracks get LinkedIn invites
    .not('linkedin_url', 'is', null)
    .is('invite_sent_at', null)
    .neq('pipeline_stage', 'invite_sent')  // extra guard: never re-send if stage already advanced
    .order('investor_score', { ascending: false })
    .limit(50); // fetch more so we can filter firm-by-firm

  if (!candidates?.length) { info(`[${deal.name}] phaseLinkedInInvites: nothing to invite (${sentToday}/${dailyTarget} sent today)`); return; }

  const contacts = [];
  for (const c of candidates) {
    if (contacts.length >= perCycleLimit) break;
    if (c.follow_up_due_at && new Date(c.follow_up_due_at).getTime() > Date.now()) continue;
    const firm = (c.company_name || '').toLowerCase().trim();
    const isRealFirm = firm && !GENERIC_FIRM_NAMES.has(firm);
    if (isRealFirm && blockedFirms.has(firm)) continue;
    contacts.push(c);
  }

  if (!contacts.length) {
    info(`[${deal.name}] phaseLinkedInInvites: all firms already engaged or no LinkedIn candidates remain (${blockedFirms.size} blocked)`);
    return;
  }

  console.log(`[ORCHESTRATOR] phaseLinkedInInvites: ${contacts.length} contacts for ${deal.name} (${blockedFirms.size} firms blocked, ${remainingToday} invite slots left today)`);

  let pendingInvites = [];
  try {
    pendingInvites = await listSentInvitations(100);
  } catch (err) {
    warn(`[${deal.name}] Could not fetch pending LinkedIn invitations: ${err.message}`);
  }
  const canQueueOutboundEmailFallback = isWithinEmailWindow(deal)
    && !(state?.outreach_paused_until && isGloballyPaused(state.outreach_paused_until));

  for (const contact of contacts) {
    if (contactsInFlight.has(contact.id)) continue;
    try {
      const outcome = await processLinkedInInvite({
        sb,
        deal,
        contact,
        pushActivity,
        logActivity: sbLogActivity,
        pendingInvites,
        source: 'orchestrator',
      });
      if (outcome.status === 'sent') {
        logStep('LinkedIn invite sent', contact.name, 'linkedin', deal);
      } else if (outcome.status === 'already_pending') {
        info(`[${deal.name}] ${contact.name} already has a pending LinkedIn invite`);
      } else if (outcome.status === 'already_connected') {
        info(`[${deal.name}] ${contact.name} already connected — moved to DM queue`);
      } else if (outcome.status === 'missing_profile') {
        info(`[${deal.name}] ${contact.name} has no usable LinkedIn profile — logged and removed from invite queue`);
        if (canQueueOutboundEmailFallback && hasUsableEmail(contact.email) && !contact.last_email_sent_at) {
          await handleOutreachApproval(contact, 'INTRO', 0, deal, { forceChannel: 'email' }).catch(err => {
            warn(`[${deal.name}] Email fallback queue failed for ${contact.name}: ${err.message}`);
          });
        }
      } else if (outcome.status === 'deferred_provider_limit') {
        info(`[${deal.name}] ${contact.name} invite deferred after LinkedIn provider limit (${outcome.retryCount || 1}/3) until ${outcome.retryAt || 'later'}`);
        if (canQueueOutboundEmailFallback && hasUsableEmail(contact.email) && !contact.last_email_sent_at) {
          await handleOutreachApproval(contact, 'INTRO', 0, deal, { forceChannel: 'email' }).catch(err => {
            warn(`[${deal.name}] Provider-limit email fallback queue failed for ${contact.name}: ${err.message}`);
          });
        }
        if (outcome.weeklyLimitHit) {
          info(`[${deal.name}] LinkedIn weekly quota confirmed — stopping invite loop until ${outcome.retryAt}`);
          break;
        }
        // Even on first/second retry, stop the loop — provider limit means LinkedIn is throttling this session
        break;
      } else if (outcome.status === 'suppressed_no_match') {
        info(`[${deal.name}] ${contact.name} invite retry suppressed after recent LinkedIn mismatch until ${outcome.retryAt || 'later'}`);
      } else if (outcome.status === 'routed_to_email') {
        info(`[${deal.name}] ${contact.name} — low LinkedIn activity (score ${outcome.score ?? '?'}), routing to email`);
        pushActivity({
          type: 'outreach',
          action: '[ACTIVITY] Low LinkedIn activity — sending email instead',
          note: `${contact.name}${contact.company_name ? ` @ ${contact.company_name}` : ''} · score ${outcome.score ?? '?'}/100`,
          deal_name: deal.name,
          dealId: deal.id,
        });
        if (canQueueOutboundEmailFallback && hasUsableEmail(contact.email) && !contact.last_email_sent_at) {
          await handleOutreachApproval(contact, 'INTRO', 0, deal, { forceChannel: 'email' }).catch(err => {
            warn(`[${deal.name}] Email queue failed after low-activity routing for ${contact.name}: ${err.message}`);
          });
        }
      } else if (outcome.status === 'failed_lookup' || outcome.status === 'failed_send') {
        warn(`[${deal.name}] LinkedIn invite path failed for ${contact.name}: ${outcome.error?.message || 'unknown error'}`);
        // Defer this contact for 4 hours so it doesn't re-enter the queue every cycle
        await sb.from('contacts').update({
          follow_up_due_at: new Date(Date.now() + 4 * 60 * 60 * 1000).toISOString(),
        }).eq('id', contact.id).catch(() => {});
      }
    } catch (e) {
      warn(`[${deal.name}] Unexpected LinkedIn invite processing error for ${contact.name}: ${e.message}`);
    }
    await sleep(3000);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 5b — OUTREACH
// Email for enriched contacts; DM for invite-accepted contacts
// ─────────────────────────────────────────────────────────────────────────────

async function phaseOutreach(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  // Firm gates:
  // 1. respondedFirms — firm has someone who replied → block all others entirely
  // 2. activeFirms    — firm has someone already in substantive outreach → hold new outreach
  const RESPONDED_STAGES = ['In Conversation', 'Replied', 'Meeting Booked', 'Meeting Scheduled'];
  const ACTIVE_PIPELINE_STAGES = ['invite_accepted', 'pending_email_approval', 'pending_dm_approval', 'Email Approved', 'DM Approved', 'Email Sent', 'DM Sent', 'email_sent', 'dm_sent', 'intro_sent', 'follow_up_sent', 'awaiting_response', 'temp_closed'];
  const ACTIVE_CONVERSATION_STATES = ['intro_sent', 'follow_up_sent', 'awaiting_response', 'temp_closed'];

  const { data: firmGateContacts } = await sb.from('contacts')
    .select('id, company_name, pipeline_stage, response_received, conversation_state, last_email_sent_at')
    .eq('deal_id', deal.id)
    .not('company_name', 'is', null);

  const respondedFirms = new Set();
  const activeFirms    = new Set();
  const outreachedContacts = new Set();

  for (const c of firmGateContacts || []) {
    const firm = (c.company_name || '').toLowerCase().trim();
    const hasExistingOutreach = !!c.last_email_sent_at
      || ACTIVE_PIPELINE_STAGES.includes(c.pipeline_stage)
      || ACTIVE_CONVERSATION_STATES.includes(c.conversation_state);

    if (hasExistingOutreach && c.id) {
      outreachedContacts.add(String(c.id));
    }
    if (!firm || GENERIC_FIRM_NAMES.has(firm)) continue;
    if (c.response_received || RESPONDED_STAGES.includes(c.pipeline_stage)) {
      respondedFirms.add(firm);
    } else if (hasExistingOutreach) {
      activeFirms.add(firm);
    }
  }

  const isFirmBlocked = (c) => {
    const firm = (c.company_name || '').toLowerCase().trim();
    if (!firm || GENERIC_FIRM_NAMES.has(firm)) return false;
    return respondedFirms.has(firm) || activeFirms.has(firm);
  };

  const hasExistingOutreach = (c) => (
    !!c?.last_email_sent_at
    || ACTIVE_PIPELINE_STAGES.includes(c?.pipeline_stage)
    || ACTIVE_CONVERSATION_STATES.includes(c?.conversation_state)
    || outreachedContacts.has(String(c?.id || ''))
  );

  // Waterfall selector:
  // - strict score ordering
  // - one untouched contact per firm at a time
  // - only open the next highest-ranked firm for each channel per cycle
  const pickNextWaterfallContacts = (rows, limitPerCycle = 1) => {
    const seenFirms = new Set();
    const selected = [];
    const ordered = [...(rows || [])].sort((a, b) => {
      const scoreDiff = Number(b.investor_score || 0) - Number(a.investor_score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      return new Date(a.created_at || 0).getTime() - new Date(b.created_at || 0).getTime();
    });

    for (const contact of ordered) {
      if (selected.length >= limitPerCycle) break;
      if (hasExistingOutreach(contact)) continue;
      if (isFirmBlocked(contact) || !isResearchReady(contact)) continue;

      const firm = (contact.company_name || '').toLowerCase().trim();
      const isRealFirm = firm && !GENERIC_FIRM_NAMES.has(firm);
      if (isRealFirm && seenFirms.has(firm)) continue;
      if (isRealFirm) seenFirms.add(firm);

      selected.push(contact);
    }
    return selected;
  };

  // Research gate: only outreach contacts whose research is complete.
  // A contact is research-ready if:
  //   (a) person_researched is true, OR
  //   (b) enrichment_status is not pending (research ran but may have come back empty — Sonnet fills gaps)
  // Contacts still awaiting research are skipped this cycle and picked up once phasePersonResearch completes.
  // Contacts that are permanently unreachable — no email AND no LinkedIn at all
  // Note: email_invalid_linkedin_only is NOT dead — they have a LinkedIn URL and can receive DMs
  const DEAD_STATUSES = new Set(['skipped_no_linkedin', 'skipped_no_name']);
  const RESEARCH_PENDING_STATUSES = ['pending', 'Pending'];
  const isResearchReady = (c) => {
    if (DEAD_STATUSES.has(c.enrichment_status)) return false; // no contact method
    if (c.person_researched) return true;
    if (RESEARCH_PENDING_STATUSES.includes(c.enrichment_status)) {
      info(`[${deal.name}] phaseOutreach: ${c.name} — research not yet complete, deferring`);
      return false;
    }
    return true; // enrichment done but research data may be sparse — Sonnet fills from own knowledge
  };

  const cleanupPendingFirmQueueConflicts = async () => {
    const { data: pendingLinkedInRows } = await sb.from('approval_queue')
      .select('id, contact_id, contact_name, firm, created_at, status')
      .eq('deal_id', deal.id)
      .eq('stage', 'LinkedIn DM')
      .in('status', ['pending', 'approved', 'approved_waiting_for_window', 'sending'])
      .order('created_at', { ascending: true });

    if (!pendingLinkedInRows?.length) return;

    const pendingByFirm = new Map();
    for (const row of pendingLinkedInRows) {
      const firm = (row.firm || '').toLowerCase().trim();
      if (!firm || GENERIC_FIRM_NAMES.has(firm)) continue;
      if (!pendingByFirm.has(firm)) pendingByFirm.set(firm, []);
      pendingByFirm.get(firm).push(row);
    }

    for (const [firm, rows] of pendingByFirm.entries()) {
      if (rows.length <= 1) continue;
      const primary = rows[0];
      const duplicates = rows.slice(1);

      for (const duplicate of duplicates) {
        await sb.from('approval_queue').update({
          status: 'skipped',
          resolved_at: new Date().toISOString(),
          edit_instructions: `Auto-skipped: duplicate LinkedIn DM queued for firm ${firm}`,
        }).eq('id', duplicate.id);

        let contact = null;
        if (duplicate.contact_id) {
          try {
            const { data } = await sb.from('contacts').select('*').eq('id', duplicate.contact_id).single();
            contact = data || null;
          } catch {}
        }

        if (!contact) continue;

        await sbLogActivity({
          dealId: deal.id,
          contactId: contact.id,
          eventType: 'LINKEDIN_DM_QUEUE_DEDUPED',
          summary: `Removed duplicate LinkedIn DM approval for ${contact.name}`,
          detail: {
            firm,
            kept_queue_id: primary.id,
            skipped_queue_id: duplicate.id,
          },
          apiUsed: 'system',
        }).catch(() => {});

        pushActivity({
          type: 'warning',
          action: 'Duplicate LinkedIn DM removed',
          note: `${contact.name} @ ${contact.company_name || duplicate.firm || ''}`,
          deal_name: deal.name,
          dealId: deal.id,
        });

        if (hasUsableEmail(contact.email) && !contact.last_email_sent_at) {
          try {
            await handleOutreachApproval(contact, 'INTRO', 0, deal, { forceChannel: 'email' });
          } catch (err) {
            warn(`[OUTREACH] Failed to queue email replacement after LinkedIn dedupe for ${contact.name}: ${err.message}`);
          }
        }
      }
    }
  };

  const queueInviteAcceptedLinkedInDrafts = async () => {
    const { data: acceptedContacts } = await sb.from('contacts')
      .select('id, name, company_name, linkedin_provider_id, email, last_email_sent_at, response_received, conversation_state, invite_accepted_at, investor_score, created_at, unipile_chat_id, reply_channel')
      .eq('deal_id', deal.id)
      .eq('pipeline_stage', 'invite_accepted')
      .not('linkedin_provider_id', 'is', null)
      .not('response_received', 'eq', true)
      .order('invite_accepted_at', { ascending: true })
      .order('investor_score', { ascending: false })
      .order('created_at', { ascending: true })
      .limit(25);

    const linkedInFirmSlots = new Set();
    try {
      const { data: existingLinkedInQueue } = await sb.from('approval_queue')
        .select('firm')
        .eq('deal_id', deal.id)
        .eq('stage', 'LinkedIn DM')
        .in('status', ['pending', 'approved', 'approved_waiting_for_window', 'sending']);
      for (const row of existingLinkedInQueue || []) {
        const firm = (row.firm || '').toLowerCase().trim();
        if (firm && !GENERIC_FIRM_NAMES.has(firm)) linkedInFirmSlots.add(firm);
      }
    } catch {}

    for (const contact of acceptedContacts || []) {
      if (contact.conversation_state === 'manual') continue;
      const firm = (contact.company_name || '').toLowerCase().trim();
      const hasFirm = firm && !GENERIC_FIRM_NAMES.has(firm);

      if (hasFirm && linkedInFirmSlots.has(firm)) {
        continue;
      }

      try {
        if (contact.unipile_chat_id) {
          // Prior chat already detected and stored — queue DM directly into existing chat
          await queueLinkedInDmApproval(contact.id, { reason: 'invite_accepted_backfill' });
        } else {
          // No prior chat ID — run full acceptance flow which checks for prior chat history.
          // Uses dynamic import to avoid circular dep at module load time.
          const { handleLinkedInAcceptance } = await import('./unipileWebhooks.js');
          await handleLinkedInAcceptance(
            { ...contact, deal_id: deal.id },
            deal,
            pushActivity,
            async (_params) => {
              await queueLinkedInDmApproval(contact.id, { reason: 'invite_accepted_backfill' });
            }
          );
        }
        if (hasFirm) linkedInFirmSlots.add(firm);
      } catch (err) {
        warn(`[OUTREACH] Failed to queue LinkedIn DM approval for ${contact.name || contact.id}: ${err.message}`);
      }
    }
  };

  const flushApprovedLinkedInDms = async () => {
    if (!isWithinChannelWindow(deal, 'linkedin_dm')) {
      info(`[${deal.name}] phaseOutreach: outside LinkedIn DM window — approved DMs remain queued`);
      return;
    }

    const { data: approvedDms } = await sb.from('approval_queue')
      .select('id, contact_id, candidate_id, contact_name, contact_email, firm, body, edited_body, deal_id, deal_name, resolved_at, message_type, channel, reply_to_id, stage')
      .eq('deal_id', deal.id)
      .eq('stage', 'LinkedIn DM')
      .in('status', ['approved', 'approved_waiting_for_window'])
      .order('resolved_at', { ascending: true })
      .limit(10);

    const { data: approvedLinkedInReplies } = await sb.from('approval_queue')
      .select('id, contact_id, candidate_id, contact_name, contact_email, firm, body, edited_body, deal_id, deal_name, resolved_at, message_type, channel, reply_to_id, stage')
      .eq('deal_id', deal.id)
      .eq('message_type', 'linkedin_reply')
      .in('status', ['approved', 'approved_waiting_for_window'])
      .order('resolved_at', { ascending: true })
      .limit(10);

    for (const item of [...(approvedLinkedInReplies || []), ...(approvedDms || [])]) {
      try {
        if (item.message_type === 'linkedin_reply') {
          await sendApprovedReply({
            queueId: item.id,
            queueItem: item,
            forceSend: false,
          });
        } else {
          await sendApprovedLinkedInDM({
            contactId: item.contact_id,
            text: item.edited_body || item.body || '',
            queueId: item.id,
            queueItem: item,
          });
        }
      } catch (err) {
        warn(`[OUTREACH] Failed to send approved LinkedIn DM for ${item.contact_name || item.contact_id}: ${err.message}`);
      }
    }
  };

  const flushApprovedEmails = async () => {
    if (!isWithinChannelWindow(deal, 'email')) {
      info(`[${deal.name}] phaseOutreach: outside email window — approved emails remain queued`);
      return;
    }

    const { data: approvedEmails } = await sb.from('approval_queue')
      .select('id, contact_id, candidate_id, contact_name, contact_email, firm, body, edited_body, subject_a, subject, approved_subject, deal_id, resolved_at, stage, message_type, channel, reply_to_id')
      .or(`deal_id.eq.${deal.id},deal_id.is.null`)
      .in('status', ['approved', 'approved_waiting_for_window'])
      .order('resolved_at', { ascending: true })
      .limit(10);

    for (const item of approvedEmails || []) {
      if (item.message_type === 'email_reply') {
        try {
          await sendApprovedReply({
            queueId: item.id,
            queueItem: item,
            forceSend: false,
          });
        } catch (err) {
          warn(`[OUTREACH] Failed to send approved email reply for ${item.contact_name || item.contact_id}: ${err.message}`);
        }
        continue;
      }
      if (isLinkedInStageLabel(item.stage)) continue;
      if (!item.contact_id) continue;

      const { data: claimed } = await sb.from('approval_queue').update({
        status: 'sending',
      }).eq('id', item.id)
        .in('status', ['approved', 'approved_waiting_for_window'])
        .select('id')
        .maybeSingle();
      if (!claimed?.id) continue;

      try {
        const { data: contact } = await sb.from('contacts')
          .select('*')
          .eq('id', item.contact_id)
          .maybeSingle();
        if (!contact || String(contact.deal_id || '') !== String(deal.id)) continue;
        if (!contact?.email) throw new Error('Queued email has no usable recipient');

        const subject = item.approved_subject || item.subject_a || item.subject || '';
        const body = item.edited_body || item.body || '';
        const emailResult = await unipileSendEmail({
          to: contact.email,
          toName: contact.name,
          subject,
          body,
          accountId: deal?.sending_account_id || null,
          trackingLabel: `deal:${deal.id}|contact:${contact.id}|stage:${String(item.stage || 'email').toLowerCase().replace(/\s+/g, '_')}`,
        });

        const nextFollowUpPlan = await getNextFollowUpPlanForChannel(deal, 'email', /follow/i.test(String(item.stage || '')) ? 1 : 0);
        let followUpDueAt = nextFollowUpPlan.delayDays
          ? new Date(Date.now() + nextFollowUpPlan.delayDays * 24 * 60 * 60 * 1000).toISOString()
          : null;

        const noFollowUpsMode = deal?.settings?.no_follow_ups || deal?.no_follow_ups;
        if (noFollowUpsMode && !followUpDueAt) {
          const cascadeDays = Number(deal?.followup_days_email) || 3;
          followUpDueAt = new Date(Date.now() + cascadeDays * 24 * 60 * 60 * 1000).toISOString();
        }

        await sb.from('approval_queue').update({
          status: 'sent',
          sent_at: new Date().toISOString(),
          approved_subject: subject || null,
          deal_id: item.deal_id || deal.id,
        }).eq('id', item.id);

        await sb.from('contacts').update({
          pipeline_stage: 'Email Sent',
          last_email_sent_at: new Date().toISOString(),
          outreach_channel: 'email',
          last_outreach_at: new Date().toISOString(),
          follow_up_due_at: followUpDueAt,
        }).eq('id', item.contact_id);

        await logConversationMessage({
          contactId: contact.id,
          dealId: deal.id,
          direction: 'outbound',
          channel: 'email',
          subject,
          body,
          unipileMessageId: emailResult?.emailId || null,
          templateName: null,
        }).catch(() => {});

        await setConversationState(contact.id, /follow/i.test(String(item.stage || '')) ? 'follow_up_sent' : 'intro_sent').catch(() => {});
        await persistOutboundEmailRecord({
          sb,
          deal,
          contact,
          subject,
          result: emailResult,
          stage: item.stage,
          status: 'sent',
        });

        pushActivity({
          type: 'email',
          action: 'Email sent',
          note: `${contact.name}${contact.company_name ? ` @ ${contact.company_name}` : ''}${subject ? ` · "${sanitizeOutreach(subject)}"` : ''}`,
          deal_name: deal?.name,
          dealId: deal?.id,
        });

        await sbLogActivity({
          dealId: deal?.id,
          contactId: contact.id,
          eventType: 'EMAIL_SENT',
          summary: `Queued email sent to ${contact.name} @ ${contact.company_name || ''}`,
          detail: {
            subject,
            stage: item.stage,
            channel: 'email',
            account_id: emailResult?.accountId || null,
            message_id: emailResult?.emailId || null,
            thread_id: emailResult?.threadId || null,
            to: contact.email || null,
          },
          apiUsed: 'unipile',
        }).catch(() => {});

        sendTelegram(`✅ *Email sent* → *${contact.name}* (${contact.company_name || 'unknown firm'})${subject ? `\nSubject: _${sanitizeOutreach(subject)}_` : ''}`).catch(() => {});
        notifyQueueUpdated();
      } catch (err) {
        try {
          const { data: failedContact } = await sb.from('contacts').select('id, email').eq('id', item.contact_id).maybeSingle();
          await persistOutboundEmailRecord({
            sb,
            deal,
            contact: { id: item.contact_id, email: failedContact?.email || item.contact_email || null },
            subject: item.approved_subject || item.subject_a || item.subject || '',
            result: null,
            stage: item.stage,
            status: 'failed',
            errorMessage: String(err.message || err).slice(0, 500),
          });
        } catch {}
        await sb.from('approval_queue').update({
          status: 'approved_waiting_for_window',
          edit_instructions: `Send retry pending: ${String(err.message || err).slice(0, 160)}`,
        }).eq('id', item.id);
        pushActivity({
          type: 'error',
          action: 'Email send failed',
          note: `${item.contact_name || item.contact_id} · ${String(err.message || err).slice(0, 160)}`,
          deal_name: deal?.name,
          dealId: deal?.id,
        });
        await sbLogActivity({
          dealId: deal?.id,
          contactId: item.contact_id || null,
          eventType: 'EMAIL_SEND_FAILED',
          summary: `Approved email failed to send for ${item.contact_name || item.contact_id}`,
          detail: {
            error: String(err.message || err).slice(0, 500),
            queue_id: item.id,
          },
          apiUsed: 'unipile',
        }).catch(() => {});
        warn(`[OUTREACH] Failed to send approved email for ${item.contact_name || item.contact_id}: ${err.message}`);
      }
    }
  };

  await cleanupPendingFirmQueueConflicts().catch(err => warn(`[${deal.name}] cleanupPendingFirmQueueConflicts error: ${err.message}`));
  await queueInviteAcceptedLinkedInDrafts().catch(err => warn(`[${deal.name}] queueInviteAcceptedLinkedInDrafts error: ${err.message}`));
  await flushApprovedLinkedInDms().catch(err => warn(`[${deal.name}] flushApprovedLinkedInDms error: ${err.message}`));
  await flushApprovedEmails().catch(err => warn(`[${deal.name}] flushApprovedEmails error: ${err.message}`));

  // Day-1 firm lane: send one junior / mid-level email intro per firm if we have an email,
  // even while LinkedIn connection requests are going to the rest of the firm.
  let emailContacts = [];
  const { data: emailCandidates } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Enriched', 'Ranked', 'invite_sent'])
    .not('email', 'is', null)
    .is('last_email_sent_at', null)
    .order('investor_score', { ascending: false })
    .order('created_at', { ascending: true })
    .limit(100);

  const candidatesByFirm = new Map();
  for (const contact of (emailCandidates || [])) {
    if (!hasUsableEmail(contact.email)) continue;
    if (hasExistingOutreach(contact)) continue;
    if (isFirmBlocked(contact) || !isResearchReady(contact)) continue;
    const firm = (contact.company_name || '').toLowerCase().trim();
    const firmKey = firm && !GENERIC_FIRM_NAMES.has(firm) ? firm : `__solo__${contact.id}`;
    if (!candidatesByFirm.has(firmKey)) candidatesByFirm.set(firmKey, []);
    candidatesByFirm.get(firmKey).push(contact);
  }

  emailContacts = Array.from(candidatesByFirm.values())
    .map(group => group.sort((a, b) => {
      const seniorityDiff = getSeniorityRank(a.job_title) - getSeniorityRank(b.job_title);
      if (seniorityDiff !== 0) return seniorityDiff;
      const scoreDiff = Number(b.investor_score || 0) - Number(a.investor_score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      return new Date(a.created_at || 0).getTime() - new Date(b.created_at || 0).getTime();
    })[0])
    .sort((a, b) => {
      const scoreDiff = Number(b.investor_score || 0) - Number(a.investor_score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      return new Date(a.created_at || 0).getTime() - new Date(b.created_at || 0).getTime();
    })
    .slice(0, 5);

  const contacts = [...emailContacts];
  if (!contacts.length) { info(`[${deal.name}] phaseOutreach: no contacts ready (${respondedFirms.size} firms responded, ${activeFirms.size} firms active)`); return; }

  console.log(`[ORCHESTRATOR] phaseOutreach: ${contacts.length} contacts for ${deal.name} (${emailContacts.length} email, ${respondedFirms.size} responded, ${activeFirms.size} active)`);

  for (const contact of contacts) {
    if (contactsInFlight.has(contact.id)) {
      info(`${contact.name} already in approval queue — skipping`);
      continue;
    }
    const forceChannel = contact.pipeline_stage === 'invite_accepted' ? 'linkedin_dm' : 'email';
    await handleOutreachApproval(contact, 'INTRO', 0, deal, { forceChannel });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 6 — FOLLOW-UPS
// Send follow-ups for contacts due a follow-up (follow_up_due_at in the past)
// ─────────────────────────────────────────────────────────────────────────────

async function phaseFollowUps(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  // Firm response gate — don't follow up at a firm where someone has already replied
  const RESPONDED_STAGES = ['In Conversation', 'Replied', 'Meeting Booked', 'Meeting Scheduled'];
  const { data: respondedContacts } = await sb.from('contacts')
    .select('company_name')
    .eq('deal_id', deal.id)
    .or(`response_received.eq.true,pipeline_stage.in.(${RESPONDED_STAGES.map(s => `"${s}"`).join(',')})`)
    .not('company_name', 'is', null);
  const respondedFirms = new Set((respondedContacts || []).map(c => (c.company_name || '').toLowerCase().trim()).filter(Boolean));

  const ACTIVE_SUBSTANTIVE_STAGES = ['pending_email_approval', 'pending_dm_approval', 'Email Approved', 'DM Approved', 'Email Sent', 'DM Sent', 'email_sent', 'dm_sent', 'intro_sent', 'follow_up_sent', 'awaiting_response', 'temp_closed'];
  const { data: activeContacts } = await sb.from('contacts')
    .select('company_name')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ACTIVE_SUBSTANTIVE_STAGES)
    .not('company_name', 'is', null);
  const activeFirms = new Set((activeContacts || []).map(c => (c.company_name || '').toLowerCase().trim()).filter(Boolean));

  const now = new Date().toISOString();
  const { data: allDue } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .lte('follow_up_due_at', now)
    // invite_sent = LinkedIn invite sent but not yet accepted — follow up via email if available
    .in('pipeline_stage', ['Email Sent', 'DM Sent', 'email_sent', 'dm_sent', 'invite_accepted', 'invite_sent'])
    .limit(10);

  // Channel-switch policy:
  // - no_follow_ups=true  (e.g. Project Electrify) → always advance to next person, no channel switching
  // - no_follow_ups=false (default) → if LinkedIn unanswered and contact has email, try email first
  // LinkedIn (invite/DM): followup_days_li (default 2) → channel switch or next person
  // Email:                followup_days_email (default 3) → next person
  const noFollowUpsMode = deal?.no_follow_ups ?? false;
  const followUpChannelFor = (contact) => {
    if (noFollowUpsMode) return null; // always advance to next person
    const isLinkedInStage = ['DM Sent', 'dm_sent', 'invite_sent', 'invite_accepted'].includes(contact.pipeline_stage);
    if (isLinkedInStage && contact.email && hasUsableEmail(contact.email)) return 'email';
    return null;
  };
  // Filter: skip responded firms, skip manual/suppressed contacts
  const contacts = (allDue || []).sort((a, b) => {
    const scoreDiff = Number(b.investor_score || 0) - Number(a.investor_score || 0);
    if (scoreDiff !== 0) return scoreDiff;
    return new Date(a.created_at || 0).getTime() - new Date(b.created_at || 0).getTime();
  }).filter(c => {
    const firm = (c.company_name || '').toLowerCase().trim();
    if (firm && !GENERIC_FIRM_NAMES.has(firm) && respondedFirms.has(firm)) return false;
    if (c.response_received) return false;
    if (c.conversation_state === 'manual') return false;
    return true;
  }).slice(0, 5);

  if (!contacts?.length) return;

  console.log(`[ORCHESTRATOR] phaseFollowUps: ${contacts.length} contacts due follow-up for ${deal.name}`);

  for (const contact of contacts) {
    if (contactsInFlight.has(contact.id)) continue;

    const switchChannel = followUpChannelFor(contact);
    const channelLabel = ['Email Sent', 'email_sent'].includes(contact.pipeline_stage) ? 'email' : 'LinkedIn';

    if (switchChannel) {
      // Channel switch: LinkedIn unanswered → try email
      info(`[${deal.name}] ${contact.name}: LinkedIn unanswered — switching to email channel`);
      pushActivity({
        type: 'outreach',
        action: `[CHANNEL SWITCH] LinkedIn unanswered — trying email`,
        note: `${contact.name}${contact.company_name ? ` @ ${contact.company_name}` : ''}`,
        deal_name: deal.name,
        dealId: deal.id,
      });
      // Clear the LinkedIn patience timer before drafting the email
      await sb.from('contacts').update({ follow_up_due_at: null }).eq('id', contact.id).catch(() => {});
      await handleOutreachApproval(contact, 'email_intro', 0, deal, { forceChannel: 'email' }).catch(() => {});
    } else {
      // Patience window expired → mark inactive and advance waterfall to next person at the firm.
      // Channel selection for the replacement contact is handled by queueNextFirmWaterfallContact
      // using LinkedIn activity-score routing (high activity → LI, low + email → email, etc.).
      await sb.from('contacts').update({
        follow_up_due_at: null,
        pipeline_stage: 'Inactive',
      }).eq('id', contact.id);
      info(`[${deal.name}] ${contact.name}: ${channelLabel} unanswered — advancing to next contact at ${contact.company_name || 'firm'}`);
      pushActivity({
        type: 'outreach',
        action: `[WATERFALL] No response — moving to next contact at firm`,
        note: `${contact.name}${contact.company_name ? ` @ ${contact.company_name}` : ''} · ${channelLabel} unanswered`,
        deal_name: deal.name,
        dealId: deal.id,
      });
      await queueNextFirmWaterfallContact(deal, contact).catch(() => {});
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// OUTREACH APPROVAL HANDLER (shared by phaseOutreach + phaseFollowUps)
// ─────────────────────────────────────────────────────────────────────────────

async function handleOutreachApproval(contact, stage, followUpNumber, deal, options = {}) {
  if (contactsInFlight.has(contact.id)) return;
  const forceChannel = options.forceChannel || 'email';

  // DB-level dedup: if there's already an active approval for this contact+deal,
  // don't draft again. This survives restarts (contactsInFlight is in-memory only).
  {
    const activeSb = getSupabase();
    if (activeSb && contact.id) {
      const { data: existingApproval } = await activeSb.from('approval_queue')
        .select('id, status, stage')
        .eq('contact_id', contact.id)
        .eq('deal_id', deal.id)
        .in('status', ['pending', 'approved', 'approved_waiting_for_window', 'sending'])
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (existingApproval) {
        info(`[OUTREACH] Skipping ${contact.name} — active approval already in queue (${existingApproval.stage} / ${existingApproval.status})`);
        contactsInFlight.add(contact.id); // keep in-memory gate consistent
        return;
      }
    }
  }

  // Hard gate — never attempt to draft or send if the contact has no name
  if (!contact.name || contact.name.trim() === '' || contact.name.toLowerCase() === 'null') {
    warn(`[OUTREACH] Skipping contact ${contact.id} — no name, cannot draft a message`);
    return;
  }

  if (forceChannel === 'email' && !hasUsableEmail(contact.email)) {
    info(`[OUTREACH] ${contact.name} has no usable email — skipping email approval`);
    return;
  }

  if (forceChannel === 'linkedin_dm' && !contact.linkedin_provider_id) {
    info(`[OUTREACH] ${contact.name} has no LinkedIn provider ID — skipping LinkedIn DM approval`);
    return;
  }

  // Build a contact-page-like object for the drafting helpers
  const contactPage = buildContactPage(contact);

  // Detect prior deal context — tell the AI this isn't a cold contact
  const priorMatch = (contact.notes || '').match(/\[PRIOR_DEAL:([^|]+)\|contacted:([^\]]+)\]/);
  const priorInstructions = priorMatch
    ? `IMPORTANT CONTEXT: This investor was previously contacted for our deal "${priorMatch[1]}" on ${priorMatch[2]}. They already know who we are. Do NOT introduce yourself as if meeting for the first time. Reference the prior relationship naturally and briefly — e.g. acknowledge you've been in touch before and explain this is a new opportunity. Keep it warm and familiar, not cold.`
    : null;

  // Warm contact override — personal re-engagement, not cold pitch
  const warmInstructions = contact.is_warm_contact
    ? `IMPORTANT: This is a warm contact — Dom Pandolfo already knows ${contact.name} personally. Do NOT write a cold pitch. Write a short, warm re-engagement email (under 6 sentences). Reference that they know each other. This is a new deal opportunity, not an introduction. No "hope this finds you well". Sign off as Dom. Direct and warm.`
    : null;

  const effectiveInstructions = warmInstructions || priorInstructions;
  const linkedinConversationHistory = forceChannel === 'linkedin_dm'
    ? await getConversationHistory(contact.id, deal?.id || null).catch(() => [])
    : [];
  const sequenceStepLabel = await getSequenceStepLabelForChannel(deal, forceChannel, followUpNumber);
  const linkedInApprovalStage = followUpNumber > 0 ? `LinkedIn Follow-Up ${followUpNumber}` : 'LinkedIn DM';

  let draft = forceChannel === 'linkedin_dm'
    ? await draftLinkedInDM(contactPage, null, stage === 'INTRO' ? 'intro' : 'followup', {
        deal,
        conversationHistory: linkedinConversationHistory,
        sequenceStepLabel,
      })
    : await draftEmailWithTemplate(contactPage, null, stage, deal, effectiveInstructions);
  if (!draft) {
    error(`[OUTREACH] Draft generation failed for ${contact.name}`);
    return;
  }

  contactsInFlight.add(contact.id);

  // Immediately update the contact's pipeline_stage to 'pending_dm_approval' / 'pending_email_approval'
  // so that if Roco restarts before the user acts, the orchestrator's hasExistingOutreach()
  // check prevents re-selecting this contact and creating duplicate approvals.
  // The stage advances to 'DM Approved' / 'Email Approved' only after Dom presses approve.
  try {
    const pendingStage = forceChannel === 'linkedin_dm' ? 'pending_dm_approval' : 'pending_email_approval';
    await sb.from('contacts').update({
      pipeline_stage: pendingStage,
      updated_at: new Date().toISOString(),
    }).eq('id', contact.id);
  } catch {}

  pushActivity({
    type: forceChannel === 'linkedin_dm' ? 'linkedin' : 'email',
    action: forceChannel === 'linkedin_dm' ? 'LinkedIn DM queued for approval' : 'Email queued for approval',
    note: `${contact.name}${contact.company_name ? ` @ ${contact.company_name}` : ''} — awaiting Telegram`,
    deal_name: deal?.name,
    dealId: deal?.id,
  });

  try {
    let decision = forceChannel === 'linkedin_dm'
      ? await sendLinkedInDMForApproval(contact, draft.body, deal.id, {
          stage: linkedInApprovalStage,
          researchSummary: contact.notes || null,
        })
      : await sendEmailForApproval(contactPage, draft, contact.notes || '', contact.investor_score || 0, stage, deal.id);

    let editCount = 0;
    while (decision.action === 'edit' && editCount < 3) {
      editCount++;
      draft = forceChannel === 'linkedin_dm'
        ? await draftLinkedInDM(contactPage, null, stage === 'INTRO' ? 'intro' : 'followup', {
            deal,
            conversationHistory: linkedinConversationHistory,
            sequenceStepLabel,
          })
        : await draftEmail(contactPage, null, stage, decision.instructions);
      if (!draft) return;
      decision = forceChannel === 'linkedin_dm'
        ? await sendLinkedInDMForApproval(contact, draft.body, deal.id, {
            stage: linkedInApprovalStage,
            researchSummary: contact.notes || null,
          })
        : await sendEmailForApproval(contactPage, draft, contact.notes || '', contact.investor_score || 0, stage, deal.id);
    }

    if (decision.action === 'approve') {
      await executeOutreach(contact, contactPage, draft, decision, stage, followUpNumber, deal, { forceChannel });
    } else if (decision.action === 'missing_email') {
      info(`[OUTREACH] ${contact.name} email approval suppressed — no usable email on record`);
    } else {
      await handleNonApproval(contact, decision.action);
    }
  } finally {
    contactsInFlight.delete(contact.id);
  }
}

async function executeOutreach(contact, contactPage, draft, decision, stage, followUpNumber, deal, options = {}) {
  const sb = getSupabase();
  let result = null;
  let channel = options.forceChannel || 'email';

  // Strip em-dashes and fancy punctuation before any send
  if (draft) {
    draft.body    = sanitizeOutreach(draft.body    || '');
    draft.subject = sanitizeOutreach(draft.subject || '');
  }
  if (decision) {
    decision.subject = sanitizeOutreach(decision.subject || '');
  }

  // Safety net — if the AI returned a refusal or placeholder instead of a real message, abort
  const body = draft?.body || '';
  const bodyLower = body.toLowerCase();
  const looksLikeRefusal = !body
    || bodyLower.includes("i can't write")
    || bodyLower.includes("i cannot write")
    || bodyLower.includes("contact's name")
    || bodyLower.includes("firm are both missing")
    || bodyLower.includes("drop those in")
    || bodyLower.includes("missing from your request")
    || bodyLower.includes("insufficient data")
    || bodyLower.includes("cannot construct")
    || bodyLower.includes("i need more information")
    || bodyLower.includes("please provide")
    || bodyLower.includes("could you share")
    || bodyLower.includes("[name]")
    || bodyLower.includes("[firm]")
    || bodyLower.includes("{{")
    || body.trim().length < 30;
  if (looksLikeRefusal) {
    warn(`[OUTREACH] Draft body looks like a refusal/placeholder for ${contact.name} — aborting send`);
    await sbLogActivity({
      dealId: deal?.id,
      contactId: contact.id,
      eventType: 'MESSAGE_BLOCKED',
      summary: `Message blocked for ${contact.name} — draft contained LLM error or placeholder`,
      detail: { body_preview: body.slice(0, 100) },
    }).catch(() => {});
    return;
  }

  if (!isWithinChannelWindow(deal, channel)) {
    // Update queue + contact stage for both email and linkedin_dm so dashboard shows correct state
    if (sb && decision?.queueId) {
      const approvedStage = channel === 'linkedin_dm' ? 'DM Approved' : 'Email Approved';
      await sb.from('approval_queue').update({
        status: 'approved_waiting_for_window',
        approved_subject: decision?.subject || draft?.subject || null,
        edited_body: decision?.body || draft?.body || null,
        resolved_at: new Date().toISOString(),
      }).eq('id', decision.queueId).then(null, () => {});
      await sb.from('contacts').update({
        pipeline_stage: approvedStage,
        updated_at: new Date().toISOString(),
      }).eq('id', contact.id).then(null, () => {});
      notifyQueueUpdated();
    }
    info(`[${deal.name}] executeOutreach: outside ${channel} window for ${contact.name} — waiting for window`);
    return;
  }

  if (channel === 'linkedin_dm') {
    if (!contact.linkedin_provider_id) {
      warn(`[OUTREACH] ${contact.name} missing LinkedIn provider ID at send time`);
      return;
    }
    const dmResult = await sendLinkedInDM({
      attendeeProviderId: contact.linkedin_provider_id,
      message: decision.body || draft.body,
    });
    if (dmResult?.success) {
      result = dmResult;
      if (sb && dmResult.chatId) {
        await sb.from('contacts').update({ linkedin_chat_id: dmResult.chatId }).eq('id', contact.id);
      }
    }
  } else if (contact.email) {
    const emailResult = await unipileSendEmail({
      to: contact.email,
      toName: contact.name,
      subject: decision.subject || draft.subject,
      body: draft.body,
      accountId: deal?.sending_account_id || null,
    });
    if (emailResult) {
      result = emailResult;
      channel = 'email';
    }
  }

  if (!result) {
    pushActivity({
      type: 'error',
      action: channel === 'linkedin_dm' ? 'LinkedIn DM send failed' : 'Email send failed',
      note: `${contact.name}${contact.company_name ? ` @ ${contact.company_name}` : ''} · no provider confirmation returned`,
      deal_name: deal?.name,
      dealId: deal?.id,
    });
    await sbLogActivity({
      dealId: deal?.id,
      contactId: contact.id,
      eventType: channel === 'linkedin_dm' ? 'LINKEDIN_DM_SEND_FAILED' : 'EMAIL_SEND_FAILED',
      summary: `${stage} failed for ${contact.name} @ ${contact.company_name || ''} via ${channel}`,
      detail: {
        stage,
        channel,
        to: contact.email || null,
        reason: 'No provider confirmation returned',
      },
      apiUsed: 'unipile',
    }).catch(() => {});
    warn(`[OUTREACH] All send methods failed for ${contact.name}`);
    return;
  }

  rocoState.emailsSent++;

  const newStage = channel === 'linkedin_dm' ? 'DM Sent' : 'Email Sent';
  const nextFollowUpPlan = await getNextFollowUpPlanForChannel(deal, channel, followUpNumber);
  let followUpDueAt = nextFollowUpPlan.delayDays
    ? new Date(Date.now() + nextFollowUpPlan.delayDays * 24 * 60 * 60 * 1000).toISOString()
    : null;

  // Always set a patience timer so phaseFollowUps can advance to the next contact when it fires.
  // LinkedIn (invite/DM): fires after followup_days_li days → marks inactive / channel-switch
  // Email:                fires after followup_days_email days → marks inactive, tries next person
  if (!followUpDueAt) {
    const cascadeDays = channel === 'linkedin_dm'
      ? (Number(deal?.followup_days_li) || 2)
      : (Number(deal?.followup_days_email) || 3);
    followUpDueAt = new Date(Date.now() + cascadeDays * 24 * 60 * 60 * 1000).toISOString();
  }

  if (sb) {
    const contactUpdate = {
      pipeline_stage: newStage,
      follow_up_due_at: followUpDueAt,
      follow_up_count: followUpNumber,
      outreach_channel: channel,
      last_outreach_at: new Date().toISOString(),
    };
    if (channel === 'email') {
      contactUpdate.last_email_sent_at = new Date().toISOString();
    } else if (channel === 'linkedin_dm') {
      contactUpdate.dm_sent_at = new Date().toISOString();
    }
    const { error: contactUpdateError } = await sb.from('contacts').update(contactUpdate).eq('id', contact.id);
    if (contactUpdateError) {
      throw new Error(`contact stage update failed: ${contactUpdateError.message}`);
    }
    if (decision?.queueId) {
      await sb.from('approval_queue').update({
        status: 'sent',
        sent_at: new Date().toISOString(),
        approved_subject: decision?.subject || draft?.subject || null,
      }).eq('id', decision.queueId);
    }
    if (channel === 'email') {
      await persistOutboundEmailRecord({
        sb,
        deal,
        contact,
        subject: decision?.subject || draft?.subject || null,
        result,
        stage,
        status: 'sent',
      });
    }
  }

  logStep(`${stage} Sent (${channel})`, `${contact.name} @ ${contact.company_name || ''}`, channel === 'email' ? 'email' : 'linkedin', deal);
  pushActivity({
    type: channel === 'linkedin_dm' ? 'dm' : 'email',
    action: channel === 'linkedin_dm' ? `LinkedIn DM sent` : `Email sent`,
    note: `${contact.name}${contact.company_name ? ` @ ${contact.company_name}` : ''}${channel === 'email' && decision?.subject ? ` · "${sanitizeOutreach(decision.subject)}"` : ''}`,
    deal_name: deal?.name, dealId: deal?.id,
  });

  // Telegram confirmation after successful send
  const channelLabel = channel === 'linkedin_dm' ? 'LinkedIn DM' : 'Email';
  const subjectLine = channel === 'email' && decision?.subject ? `\nSubject: _${sanitizeOutreach(decision.subject)}_` : '';
  const dealLabel = deal?.name ? ` · *${deal.name}*` : '';
  const liPreview = channel === 'linkedin_dm' && draft?.body
    ? `\n\n_${draft.body.slice(0, 120).replace(/[\n\r]+/g, ' ')}${draft.body.length > 120 ? '…' : ''}_`
    : '';
  sendTelegram(`✅ *${channelLabel} sent* → *${contact.name}* (${contact.company_name || 'unknown firm'})${dealLabel}${subjectLine}${liPreview}`).catch(() => {});

  await sbLogActivity({
    dealId: deal?.id,
    contactId: contact.id,
    eventType: channel === 'linkedin_dm' ? 'LINKEDIN_DM_SENT' : 'EMAIL_SENT',
    summary: `${stage} sent to ${contact.name} @ ${contact.company_name || ''} via ${channel}`,
    detail: {
      subject: decision.subject,
      stage,
      channel,
      account_id: result?.accountId || null,
      provider_id: result?.providerId || null,
      message_id: result?.messageId || result?.emailId || null,
      thread_id: result?.threadId || null,
      chat_id: result?.chatId || null,
      attendee_provider_id: result?.attendeeProviderId || null,
      to: contact.email || null,
    },
    apiUsed: 'unipile',
  });

  // Log to conversation_messages for full conversation history (including template used)
  await logConversationMessage({
    contactId:    contact.id,
    dealId:       deal?.id || null,
    direction:    'outbound',
    channel,
    subject:      decision?.subject || draft?.subject || null,
    body:         draft?.body || '',
    unipileMessageId: result?.messageId || result?.emailId || null,
    templateName: draft?.templateName || null,
  }).catch(() => {});

  // Set conversation_state to 'intro_sent' or 'follow_up_sent'
  const newConvState = followUpNumber > 0 ? 'follow_up_sent' : 'intro_sent';
  await setConversationState(contact.id, newConvState).catch(() => {});

  if (followUpNumber > 0) {
    await queueNextFirmWaterfallContact(deal, contact).catch(err => {
      warn(`[OUTREACH] Waterfall advance failed for ${contact.name}: ${err.message}`);
    });
  }
}

async function handleNonApproval(contact, action) {
  const sb = getSupabase();
  if (action === 'skip' && sb) {
    await sb.from('contacts').update({ pipeline_stage: 'Skipped' }).eq('id', contact.id);
    info(`${contact.name} skipped`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// TEMPLATE SUPPORT
// ─────────────────────────────────────────────────────────────────────────────

function fillTemplate(template, { contact, deal }) {
  const name = contact.name || '';
  const parts = name.split(' ');
  const fmtNum = (n) => n ? String(Number(n).toLocaleString()) : '';
  const fmtAmount = (n) => n ? `£${Number(n).toLocaleString()}` : '';
  return template
    // Contact
    .replace(/\{\{firstName\}\}/gi, parts[0] || name)
    .replace(/\{\{lastName\}\}/gi, parts.slice(1).join(' ') || '')
    .replace(/\{\{fullName\}\}/gi, name)
    .replace(/\{\{company\}\}/gi, contact.company_name || '')
    .replace(/\{\{firm\}\}/gi, contact.company_name || '')
    .replace(/\{\{title\}\}/gi, contact.job_title || '')
    .replace(/\{\{jobTitle\}\}/gi, contact.job_title || '')
    // Investor research — resolved from contact record + research notes
    .replace(/\{\{pastInvestments\}\}/gi, contact.past_investments || '')
    .replace(/\{\{investmentThesis\}\}/gi, contact.investment_thesis || '')
    .replace(/\{\{sectorFocus\}\}/gi, contact.sector_focus || '')
    .replace(/\{\{investorGeography\}\}/gi, contact.geography || '')
    // Deal — standard fields
    .replace(/\{\{dealName\}\}/gi, deal?.name || '')
    .replace(/\{\{dealType\}\}/gi, deal?.raise_type || deal?.type || 'investment')
    .replace(/\{\{dealBrief\}\}/gi, (deal?.description || '').substring(0, 120))
    .replace(/\{\{sector\}\}/gi, deal?.sector || '')
    .replace(/\{\{targetAmount\}\}/gi, fmtAmount(deal?.target_amount))
    .replace(/\{\{keyMetrics\}\}/gi, deal?.key_metrics || '')
    .replace(/\{\{geography\}\}/gi, deal?.geography || '')
    .replace(/\{\{minCheque\}\}/gi, fmtAmount(deal?.min_cheque))
    .replace(/\{\{maxCheque\}\}/gi, fmtAmount(deal?.max_cheque))
    .replace(/\{\{comparableDeal\}\}/gi, deal?.comparable_deal || '')
    .replace(/\{\{investorProfile\}\}/gi, deal?.investor_profile || '')
    .replace(/\{\{investorFocus\}\}/gi, deal?.investor_profile || deal?.sector || '')
    // Deal — financial metrics (from deal settings JSONB or top-level columns)
    .replace(/\{\{ebitda\}\}/gi, fmtNum(deal?.ebitda || deal?.settings?.ebitda || deal?.ebitda_usd_m))
    .replace(/\{\{ev\}\}/gi, fmtNum(deal?.ev || deal?.settings?.ev || deal?.enterprise_value_usd_m))
    .replace(/\{\{equity\}\}/gi, fmtAmount(deal?.equity || deal?.settings?.equity || deal?.target_amount))
    .replace(/\{\{revenue\}\}/gi, fmtNum(deal?.revenue || deal?.settings?.revenue))
    // Assets
    .replace(/\{\{deckUrl\}\}/gi, deal?.deck_url || '')
    .replace(/\{\{callLink\}\}/gi, deal?.calendly_url || deal?.call_link || '')
    // Sender
    .replace(/\{\{senderName\}\}/gi, process.env.SENDER_NAME || 'Dom')
    .replace(/\{\{senderTitle\}\}/gi, process.env.SENDER_TITLE || '');
}

// Maps internal stage type keys to sequence_step labels and name patterns
const STEP_LABEL_MAP = {
  intro:      'email_intro',
  followup_1: 'email_followup_1',
  followup_2: 'email_followup_2',
  followup_3: 'email_followup_3',
};

const TEMPLATE_NAME_PATTERNS = {
  intro:      ['email intro', 'intro email', 'intro'],
  followup_1: ['email follow up', 'follow up 1', 'followup 1', 'follow-up 1', 'email follow-up'],
  followup_2: ['follow up 2', 'followup 2', 'follow-up 2'],
  followup_3: ['follow up 3', 'followup 3', 'follow-up 3'],
};

/** Fetch the active outreach sequence from DB. Returns array of step objects. */
export async function getOutreachSequence() {
  try {
    const sb = getSupabase();
    if (!sb) return null;
    const { data } = await sb.from('outreach_sequence').select('steps').limit(1).single();
    return data?.steps || null;
  } catch {
    return null;
  }
}

/**
 * Fetch the primary template for a given sequence_step label.
 * Checks deal-level overrides first, falls back to global email_templates.
 * If A/B is enabled (two primaries), picks deterministically by contactId.
 */
export async function getTemplateForStep(stepLabel, contactId, dealId = null) {
  try {
    const sb = getSupabase();
    if (!sb) return null;

    // 1. Deal-level override
    if (dealId) {
      const { data: dealTmpl } = await sb.from('deal_templates')
        .select('*')
        .eq('deal_id', dealId)
        .eq('sequence_step', stepLabel)
        .eq('is_primary', true);
      if (dealTmpl?.length > 0) {
        if (dealTmpl.length > 1 && dealTmpl[0].ab_test_enabled) {
          const hash = (contactId || '').charCodeAt(0) % 2;
          return normaliseTemplate(dealTmpl[hash] || dealTmpl[0]);
        }
        return normaliseTemplate(dealTmpl[0]);
      }
    }

    // 2. Global fallback
    const { data: templates } = await sb.from('email_templates')
      .select('*')
      .eq('sequence_step', stepLabel)
      .eq('is_primary', true)
      .eq('is_active', true);

    if (!templates?.length) return null;

    if (templates.length > 1 && templates[0].ab_test_enabled) {
      const hash = (contactId || '').charCodeAt(0) % 2;
      return normaliseTemplate(templates[hash] || templates[0]);
    }

    return normaliseTemplate(templates[0]);
  } catch {
    return null;
  }
}

/**
 * Fetch the outreach sequence for a deal.
 * Checks deal_sequence first, falls back to global outreach_sequence.
 */
export async function getSequenceForDeal(dealId) {
  try {
    const sb = getSupabase();
    if (!sb) return null;

    if (dealId) {
      let dealSeq = null;
      try {
        const r = await sb.from('deal_sequence').select('steps, sending_window').eq('deal_id', dealId).limit(1).single();
        dealSeq = r.data;
      } catch (_) {}
      if (dealSeq?.steps?.length > 0) return dealSeq;
    }

    let globalSeq = null;
    try {
      const r = await sb.from('outreach_sequence').select('steps, sending_window').limit(1).single();
      globalSeq = r.data;
    } catch (_) {}
    return globalSeq || { steps: [], sending_window: { start_hour: 8, end_hour: 18, days: [1,2,3,4,5] } };
  } catch {
    return null;
  }
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

async function getChannelSequenceSteps(deal, channel) {
  const sequence = await getSequenceForDeal(deal?.id || null).catch(() => null);
  const fallback = getDefaultSequenceStepsForChannel(channel);
  const typeMatches = channel === 'linkedin_dm'
    ? new Set(['linkedin_dm'])
    : new Set(['email']);
  const steps = (sequence?.steps || [])
    .filter(step => typeMatches.has(String(step?.type || '').toLowerCase()))
    .sort((a, b) => Number(a?.step || 0) - Number(b?.step || 0));
  return steps.length ? steps : fallback;
}

async function getMaxFollowUpsForChannel(deal, channel) {
  // Per-deal no-follow-ups gate: intro only, no follow-ups on any channel
  if (deal?.settings?.no_follow_ups || deal?.no_follow_ups) return 0;
  const steps = await getChannelSequenceSteps(deal, channel);
  return Math.max(0, steps.length - 1);
}

async function getSequenceStepLabelForChannel(deal, channel, followUpNumber) {
  const steps = await getChannelSequenceSteps(deal, channel);
  return steps[followUpNumber]?.label || steps[followUpNumber]?.sequence_step || null;
}

async function getNextFollowUpPlanForChannel(deal, channel, sentFollowUpNumber) {
  const steps = await getChannelSequenceSteps(deal, channel);
  const currentStep = steps[sentFollowUpNumber] || null;
  const nextStep = steps[sentFollowUpNumber + 1] || null;
  const defaultGap = channel === 'linkedin_dm'
    ? (Number(deal?.followup_days_li) || 2)
    : (Number(deal?.followup_days_email) || 3);

  if (!nextStep) {
    return { delayDays: null, nextStep: null };
  }

  const currentDelay = normaliseSequenceDelay(currentStep, 0);
  const nextDelay = normaliseSequenceDelay(nextStep, defaultGap);
  const delta = Number(nextDelay) - Number(currentDelay);
  return {
    delayDays: Number.isFinite(delta) && delta > 0 ? delta : defaultGap,
    nextStep,
  };
}

function formatFollowUpStage(followUpNumber) {
  return followUpNumber > 0 ? `FOLLOW-UP ${followUpNumber}` : 'INTRO';
}

async function queueNextFirmWaterfallContact(deal, sourceContact) {
  const sb = getSupabase();
  const firm = String(sourceContact?.company_name || '').trim();
  if (!sb || !firm) return;

  const firmKey = firm.toLowerCase();
  if (GENERIC_FIRM_NAMES.has(firmKey)) return;

  const respondedStages = ['In Conversation', 'Replied', 'Meeting Booked', 'Meeting Scheduled'];
  const activeApprovalStatuses = ['pending', 'approved', 'approved_waiting_for_window', 'sending'];

  const { data: responded } = await sb.from('contacts')
    .select('id')
    .eq('deal_id', deal.id)
    .eq('company_name', firm)
    .or(`response_received.eq.true,pipeline_stage.in.(${respondedStages.map(s => `"${s}"`).join(',')})`)
    .limit(1);
  if (responded?.length) return;

  const { data: queued } = await sb.from('approval_queue')
    .select('id')
    .eq('deal_id', deal.id)
    .eq('firm', firm)
    .in('status', activeApprovalStatuses)
    .limit(1);
  if (queued?.length) return;

  // Look for the next uncontacted person at this firm:
  // Priority 1: invite_accepted contacts not yet messaged (ready for DM)
  // Priority 2: Enriched contacts not yet invited (start LinkedIn invite or email)
  // Priority 3: Ranked contacts (will get invited in next phaseLinkedInInvites cycle)
  const { data: firmContacts } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .eq('company_name', firm)
    .neq('id', sourceContact.id)
    .in('pipeline_stage', ['invite_accepted', 'Enriched', 'enriched', 'Ranked', 'ranked'])
    .order('investor_score', { ascending: false })
    .order('created_at', { ascending: true })
    .limit(20);

  const nextContact = (firmContacts || []).find(contact => {
    if (contact.response_received) return false;
    if (contact.conversation_state === 'manual' || contact.conversation_state === 'do_not_contact') return false;
    if (contact.last_email_sent_at || contact.dm_sent_at || contact.last_outreach_at || contact.invite_sent_at) return false;
    if (contactsInFlight.has(contact.id)) return false;
    return true;
  });

  if (!nextContact) return;

  // Determine the right channel for the next contact using smart routing:
  // - Already connected (invite_accepted) → DM
  // - Has LinkedIn with high activity (score >= 40 or unscored) → LinkedIn invite
  // - Has LinkedIn but low activity (score < 40) + has email → email
  // - Has LinkedIn but no email → LinkedIn invite regardless
  // - No LinkedIn but has email → email
  // - Neither → nothing
  let forceChannel = null;
  const actScore = nextContact.linkedin_activity_score;
  const recChannel = nextContact.recommended_channel;
  const hasLinkedIn = !!nextContact.linkedin_url;
  const hasEmail = hasUsableEmail(nextContact.email);
  const isLowActivity = recChannel === 'email' || (actScore != null && actScore < 40);

  if (nextContact.pipeline_stage === 'invite_accepted' && nextContact.linkedin_provider_id && isWithinChannelWindow(deal, 'linkedin_dm')) {
    forceChannel = 'linkedin_dm';
  } else if (hasLinkedIn && !nextContact.invite_sent_at) {
    if (isLowActivity && hasEmail) {
      // Low LinkedIn activity AND has email → go email first
      forceChannel = 'email';
    } else {
      // High/unknown activity, OR low activity but no email → LinkedIn invite
      // Let phaseLinkedInInvites handle it naturally (picks it up next cycle)
      info(`[${deal.name}] Waterfall: ${nextContact.name} @ ${firm} queued for LinkedIn invite (score: ${actScore ?? 'unscored'})`);
      return;
    }
  } else if (!hasLinkedIn && hasEmail) {
    forceChannel = 'email';
  }

  if (!forceChannel) return;

  info(`[${deal.name}] Waterfall advancing to ${nextContact.name} @ ${firm} via ${forceChannel}`);
  pushActivity({
    type: 'outreach',
    action: `[WATERFALL] Moving to next contact at ${firm}`,
    note: `${sourceContact.name} unanswered → ${nextContact.name} @ ${firm} via ${forceChannel}`,
    deal_name: deal.name,
    dealId: deal.id,
  });
  await handleOutreachApproval(nextContact, 'INTRO', 0, deal, { forceChannel });
}

async function fetchTemplate(type, dealId, contactId) {
  try {
    const sb = getSupabase();
    if (!sb) return null;

    // Strategy 1: sequence_step column (most reliable — uses explicit is_primary flag)
    const stepLabel = STEP_LABEL_MAP[type];
    if (stepLabel) {
      const t = await getTemplateForStep(stepLabel, contactId, dealId);
      if (t) return t;
    }

    // Strategy 2: fetch all active email templates and match by type/name
    const { data: all } = await sb.from('email_templates')
      .select('*')
      .in('type', ['email', type])
      .eq('is_active', true);
    if (!all?.length) return null;

    const pool = dealId
      ? [...all.filter(t => t.deal_id === dealId), ...all.filter(t => !t.deal_id)]
      : all.filter(t => !t.deal_id);

    // Exact legacy type match
    const exactType = pool.find(t => t.type === type);
    if (exactType) return normaliseTemplate(exactType);

    // Name-pattern match ("Email Intro", "Email Follow Up", etc.)
    const patterns = TEMPLATE_NAME_PATTERNS[type] || [];
    const byName = pool.find(t =>
      patterns.some(p => (t.name || '').toLowerCase().includes(p))
    );
    if (byName) return normaliseTemplate(byName);

    return null;
  } catch {
    return null;
  }
}

// Ensure body_a is always populated (new templates use `body` column, legacy use `body_a`)
function normaliseTemplate(t) {
  return { ...t, body_a: t.body_a || t.body || null };
}

async function draftEmailWithTemplate(contactPage, research, stage, deal, editInstructions = null) {
  const typeMap = { 'INTRO': 'intro', 'FOLLOW-UP 1': 'followup_1', 'FOLLOW-UP 2': 'followup_2', 'FOLLOW-UP 3': 'followup_3' };
  const templateType = typeMap[stage] || 'intro';

  const contactForTemplate = {
    name:             contactPage.name || (typeof contactPage.getContactProp === 'function' ? contactPage.getContactProp('Name') : ''),
    company_name:     contactPage.company_name || (contactPage.properties?.['Company Name']?.relation ? '' : (contactPage.properties?.['Company Name']?.rich_text?.[0]?.plain_text || '')),
    job_title:        contactPage.job_title || contactPage.properties?.['Job Title']?.rich_text?.[0]?.plain_text || '',
    // Research fields — resolve {{pastInvestments}}, {{investmentThesis}}, {{sectorFocus}}, {{investorGeography}}
    past_investments: contactPage.past_investments || research?.comparableDeals?.slice(0,3).join(', ') || '',
    investment_thesis: contactPage.investment_thesis || research?.approachAngle || '',
    sector_focus:     contactPage.sector_focus || contactPage.preferred_industries || '',
    geography:        contactPage.geography || contactPage.hq_country || '',
  };
  const linkedinProfileContext = await buildDraftLinkedInProfileContext(contactPage);

  // Fetch template — if found, fill base variables then pass to Sonnet for personalisation
  const template = await fetchTemplate(templateType, deal?.id, contactPage.id);
  if (template?.body_a) {
    const filledSubjectA = fillTemplate(template.subject_a || '', { contact: contactForTemplate, deal });
    const filledSubjectB = template.subject_b ? fillTemplate(template.subject_b, { contact: contactForTemplate, deal }) : null;
    const filledBody     = fillTemplate(template.body_a, { contact: contactForTemplate, deal });

    // A/B testing: if enabled and subject_b exists, randomly pick which subject Sonnet refines from
    const useSubjectB = !!(template.ab_test_enabled && filledSubjectB && Math.random() < 0.5);
    const primarySubject = useSubjectB ? filledSubjectB : filledSubjectA;
    const altSubject     = useSubjectB ? filledSubjectA : filledSubjectB;

    // Pass filled template to Sonnet — Sonnet is the final editor, owns the output completely.
    // The template is a structural guide and tone reference, not a rigid script.
    const templateInstruction = editInstructions
      ? `DOM'S EDIT INSTRUCTIONS: ${editInstructions}`
      : '';

    const personalisationPrompt = `You are Dom's personal writing assistant. Dom is a senior fundraising professional. You are producing the FINAL, send-ready ${stage} email for this investor — it must read like Dom wrote it himself.

TEMPLATE (use as a structural and tonal guide — not a rigid script):
Subject: ${primarySubject}
Body:
${filledBody}

INVESTOR RESEARCH:
Name: ${contactForTemplate.name}
Firm: ${contactForTemplate.company_name || 'independent investor'}
Title: ${contactForTemplate.job_title || ''}
Past investments: ${contactForTemplate.past_investments || research?.comparableDeals?.slice(0,3).join(', ') || 'not on record'}
Investment thesis: ${contactForTemplate.investment_thesis || research?.approachAngle || 'not available'}
Sector focus: ${contactForTemplate.sector_focus || ''}
Geography: ${contactForTemplate.geography || ''}
${research?.approachAngle ? `Approach angle: ${research.approachAngle.substring(0, 200)}` : ''}
${linkedinProfileContext ? `LinkedIn profile context:\n${linkedinProfileContext}\n` : ''}

DEAL: ${deal?.name || ''} | ${deal?.sector || ''} | £${Number(deal?.target_amount || 0).toLocaleString()}
Description: ${(deal?.description || '').substring(0, 150)}
${templateInstruction}

YOUR JOB — produce the final optimised email:
- Use the template as a starting point for structure and tone
- If any part of the template doesn't flow naturally or make sense for THIS investor, rewrite that part — the final message must read coherently from first word to last
- Where you have specific research on the investor, weave it in naturally — don't force it if it doesn't fit
- If a template variable resolved to something vague or empty, replace it with something that actually makes sense in context
- Keep the same brevity and voice as the template — do NOT expand or add bullet points, em-dashes, or jargon
- The subject line should drive curiosity — not "Investment Opportunity" or similar
- FORBIDDEN: "Hope this finds you well", "I wanted to reach out", "exciting opportunity", "touching base", corporate jargon
- Sign off as Dom

Return ONLY valid JSON (no markdown, no preamble):
{
  "subject": "<final subject>",
  "body": "<final email body — plain text, no markdown>",
  "alternativeSubject": "<alternative A/B subject>"
}`;

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
          max_tokens: 600,
          messages:   [{ role: 'user', content: personalisationPrompt }],
        }),
      });
      if (res.ok) {
        const data = await res.json();
        const text = data.content?.[0]?.text || '';
        const match = text.match(/\{[\s\S]*\}/);
        if (match) {
          const parsed = JSON.parse(match[0]);
          if (parsed?.body) {
            info(`[TEMPLATE] Sonnet personalised ${stage} template for ${contactForTemplate.name}`);
            return { subject: parsed.subject || primarySubject, body: parsed.body, alternativeSubject: parsed.alternativeSubject || altSubject, templateName: template.name };
          }
        }
      }
    } catch (err) {
      warn(`[TEMPLATE] Sonnet personalisation failed for ${contactForTemplate.name}: ${err.message} — using filled template`);
    }

    // Fallback: return filled template as-is if Sonnet fails
    return { subject: primarySubject, body: filledBody, alternativeSubject: altSubject, templateName: template.name };
  }

  // No template in DB — fall through to full AI drafting
  return draftEmail(contactPage, research, stage, editInstructions);
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Build a contact-page-like object from a flat Supabase contact row.
 * draftEmail and sendEmailForApproval expect getContactProp-style access.
 */
function buildContactPage(contact) {
  const whyThisFirm = contact.why_this_firm
    || contact.match_rationale
    || contact.justification
    || contact.firms?.match_rationale
    || contact.firms?.justification
    || '';
  return {
    id: contact.id,
    // Property objects MUST include a `type` field — getContactProp() switches on p.type
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
    // Expose flat fields too for convenience
    name: contact.name,
    email: contact.email,
    company_name: contact.company_name,
    linkedin_url: contact.linkedin_url,
    website: contact.website || contact.firms?.website || null,
    investor_score: contact.investor_score,
    why_this_firm: whyThisFirm,
  };
}

// Strip common legal suffixes so "XYZ Capital Partners, LLC" matches "XYZ Capital Partners"
function normalizeFirmForDedup(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/^the\s+/i, '')
    .replace(/[,.]?\s*(llc|lp|l\.p\.|l\.l\.c\.|inc\.?|ltd\.?|corp\.?|co\.?|group|capital|partners|management|advisors|investments?|fund|funds|associates|services|solutions|holdings?)\.?\s*$/gi, '')
    .replace(/[^a-z0-9]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function logStep(action, note, type = 'General', deal = null) {
  pushActivity({
    type,
    action,
    note,
    ...(deal ? { deal_name: deal.name, dealId: deal.id } : {}),
  });
  info(`[${type}]${deal ? ` [${deal.name}]` : ''} ${action}: ${note}`);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
