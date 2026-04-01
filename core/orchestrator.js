/**
 * core/orchestrator.js
 * Supabase-native pipeline orchestrator.
 * Phases (per deal per cycle): phaseRank → phaseArchive → phaseEnrich → phaseNotionSync → phaseOutreach → phaseFollowUps
 * Research is triggered immediately on deal creation (see dashboard/server.js).
 */

import { getSupabase } from './supabase.js';
import { loadState, saveState } from './state.js';
import { getActiveDeals, logActivity as sbLogActivity } from './supabaseSync.js';
import {
  getConversationHistory,
  logConversationMessage,
  setConversationState,
  checkTempClosedContacts,
  draftTempCloseFollowUp,
} from './conversationManager.js';
import { isWithinSendingWindow, isGloballyPaused, getNextWindowOpen, isWithinChannelWindow } from './scheduleChecker.js';
import { isWithinEmailWindow, describeNextEmailWindow } from './sendingWindow.js';
import { rankInvestor } from '../research/investorRanker.js';
import { enrichWithKaspr } from '../enrichment/kaspEnricher.js';
import { enrichWithApify } from '../enrichment/apifyEnricher.js';
import { findLinkedInUrl } from '../enrichment/linkedinFinder.js';
import {
  sendLinkedInInvite,
  sendLinkedInDM,
  sendEmail as unipileSendEmail,
} from '../integrations/unipileClient.js';
import { sendEmailForApproval, sendLinkedInDMForApproval, sendTelegram } from '../approval/telegramBot.js';
import { draftEmail } from '../outreach/emailDrafter.js';
import { draftLinkedInDM } from '../outreach/linkedinDrafter.js';
import { isExcluded } from './exclusionCheck.js';
import {
  researchPerson,
  isResearched,
  hasCoreResearchFields,
  hasFreshResearch,
} from '../research/personResearcher.js';
import { runFirmResearch } from '../research/firmResearcher.js';
import { runDealResearch } from '../research/dealResearcher.js'; // legacy fallback
import { queryInvestorDatabase, batchScoreInvestors as batchScoreInvestors } from './investorDatabaseQuery.js';
import { pushActivity } from '../dashboard/server.js';
import { info, warn, error } from './logger.js';
import { ORCHESTRATOR_INTERVAL_MS } from '../config/constants.js';

// Alias to match prompts that refer to createNotionContact
import { createContact as createNotionContact, updateContact as updateNotionContact } from '../crm/notionContacts.js';

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

// Contacts currently in the approval flow — prevents duplicate approval drafts per cycle
const contactsInFlight = new Set();

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

  // Weekly analytics (non-blocking)
  checkAndRunAnalytics().catch(err =>
    console.error('[ANALYTICS] Weekly check failed:', err.message)
  );

  info('--- Orchestrator cycle complete ---');
}

async function checkAndRunAnalytics() {
  const sb = getSupabase();
  if (!sb) return;
  const { data: lastRun } = await sb.from('deal_analytics')
    .select('created_at').order('created_at', { ascending: false }).limit(1);
  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
  const lastRunDate  = lastRun?.[0]?.created_at ? new Date(lastRun[0].created_at) : null;
  if (!lastRunDate || lastRunDate < sevenDaysAgo) {
    const { runWeeklyAnalytics } = await import('./analyticsEngine.js');
    await runWeeklyAnalytics().catch(err =>
      console.error('[ANALYTICS] Weekly run failed:', err.message)
    );
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
const BATCH_FIRM_TARGET = 20;

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

  const ACTIVE_STAGES = ['Ranked', 'ranked', 'Enriched', 'enriched', 'email_sent', 'dm_sent',
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


async function getOrCreateBatch(deal) {
  const sb = getSupabase();
  if (!sb) return null;
  // Always return the current RESEARCHING batch (background builder).
  // pending_approval / approved / ready / completed batches are tracked separately.
  const { data: existing } = await sb.from('campaign_batches')
    .select('*')
    .eq('deal_id', deal.id)
    .eq('status', 'researching')
    .order('created_at', { ascending: false })
    .limit(1)
    .single();
  if (existing) return existing;

  // Cap at 3 active batches before creating a new researching one
  const { count: activeCount } = await sb.from('campaign_batches')
    .select('id', { count: 'exact', head: true })
    .eq('deal_id', deal.id)
    .in('status', ['researching', 'ready', 'pending_approval', 'approved']);
  if ((activeCount || 0) >= 3) {
    console.log(`[BATCH] Max 3 batches active for ${deal.name} — skipping new batch creation`);
    return null;
  }

  const batchNumber = await getNextBatchNumber(deal.id);
  const { data: created } = await sb.from('campaign_batches')
    .insert({
      deal_id: deal.id,
      batch_number: batchNumber,
      status: 'researching',
      target_firms: BATCH_FIRM_TARGET,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    })
    .select()
    .single();
  console.log(`[BATCH] Created batch #${batchNumber} for ${deal.name}`);
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

/**
 * Sync ranked entities into the current researching batch.
 * Only counts contacts promoted during THIS batch's time window (created_at >= batch.created_at)
 * so that prior batch contacts don't inflate the count.
 *
 * Unique entities: each institutional firm = 1 (by company_name), each individual contact = 1.
 *
 * When 20 reached:
 *  - If an active batch (pending_approval or approved) already exists → status 'ready', silently build next batch
 *  - Otherwise → triggerCampaignReview (pending_approval + Telegram notification)
 *
 * Returns true if target was hit (caller should know not to proceed with outreach this cycle).
 */
async function updateBatchFirms(deal, batch) {
  if (!batch) return false;
  const sb = getSupabase();
  if (!sb) return false;
  const batchStart = batch.created_at;
  await backfillBatchContactTypes(deal, batch);
  const snapshot = await getBatchEntitySnapshot(deal.id, batchStart);
  const entityCount = Math.min(snapshot.entityCount, BATCH_FIRM_TARGET);
  console.log(`[BATCH] ${deal.name} batch #${batch.batch_number}: ${snapshot.firmKeys.size} firms + ${Math.max(0, snapshot.entityCount - snapshot.firmKeys.size)} individuals = ${entityCount}/${BATCH_FIRM_TARGET}`);

  // Update batch with current entity count
  await sb.from('campaign_batches')
    .update({ ranked_firms: entityCount, updated_at: new Date().toISOString() })
    .eq('id', batch.id);

  if (entityCount >= BATCH_FIRM_TARGET && batch.status === 'researching') {
    // Verify that contacts actually queued for outreach (Ranked/Enriched) are fully researched.
    // We only gate on active outreach candidates — not every contact ever created for this deal.
    const { count: unresearchedCount } = await sb.from('contacts')
      .select('id', { count: 'exact', head: true })
      .eq('deal_id', deal.id)
      .gte('created_at', batchStart)
      .in('pipeline_stage', ['Ranked', 'ranked', 'Enriched', 'enriched'])
      .eq('person_researched', false);

    if ((unresearchedCount || 0) > 0) {
      console.log(`[BATCH] ${deal.name} batch #${batch.batch_number}: 20 firms ranked but ${unresearchedCount} outreach-ready contacts still need person research — holding`);
      return false;
    }

    // Check if there's already an active batch (pending_approval or approved)
    const { data: activeBatch } = await sb.from('campaign_batches')
      .select('id, batch_number, status')
      .eq('deal_id', deal.id)
      .in('status', ['pending_approval', 'approved'])
      .limit(1)
      .single();

    if (activeBatch) {
      // Another batch is already active — mark this one ready, silently build next
      await sb.from('campaign_batches')
        .update({ status: 'ready', updated_at: new Date().toISOString() })
        .eq('id', batch.id);
      console.log(`[BATCH] Batch #${batch.batch_number} for ${deal.name} → ready (batch #${activeBatch.batch_number} still ${activeBatch.status})`);
      pushActivity({
        type: 'system',
        action: 'Next batch ready',
        note: `${deal.name} — Batch #${batch.batch_number} is built and waiting. Available after batch #${activeBatch.batch_number} is closed.`,
        deal_name: deal.name,
        dealId: deal.id,
      });
      // Cap at 3 active batches (researching + ready + pending_approval + approved)
      const { count: activeCount } = await sb.from('campaign_batches')
        .select('id', { count: 'exact', head: true })
        .eq('deal_id', deal.id)
        .in('status', ['researching', 'ready', 'pending_approval', 'approved']);
      if ((activeCount || 0) < 3) {
        const nextNumber = await getNextBatchNumber(deal.id);
        await sb.from('campaign_batches').insert({
          deal_id: deal.id,
          batch_number: nextNumber,
          status: 'researching',
          target_firms: BATCH_FIRM_TARGET,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        });
        console.log(`[BATCH] Auto-created batch #${nextNumber} for ${deal.name} (background research continues)`);
      } else {
        console.log(`[BATCH] Max 3 batches reached for ${deal.name} — not creating more until one completes`);
      }
    } else {
      // No active batch — this is the first batch up for approval
      await triggerCampaignReview(deal, batch);
    }
    return true;
  }
  return false;
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

async function generateJustification(firmName, deal) {
  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 100,
        messages: [{
          role: 'user',
          content: `In one sentence (max 20 words), explain why "${firmName}" is a good fit for this deal: ${deal.name} — ${deal.sector || ''} ${deal.raise_type || ''} $${deal.target_amount || ''}M. Be specific and direct. No em-dashes.`,
        }],
      }),
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data.content?.[0]?.text?.trim() || null;
  } catch (e) {
    return null;
  }
}

async function triggerCampaignReview(deal, batch) {
  const sb = getSupabase();
  if (!sb) return;

  // Flip batch to pending_approval
  await sb.from('campaign_batches')
    .update({ status: 'pending_approval', updated_at: new Date().toISOString() })
    .eq('id', batch.id);

  // Generate justifications for the top firms
  const { data: firms } = await sb.from('firm_outreach_state')
    .select('id, firm_id, firms(name, sector, hq_location)')
    .eq('deal_id', deal.id)
    .in('status', ['ranked', 'pending_outreach'])
    .order('rank_score', { ascending: false })
    .limit(BATCH_FIRM_TARGET);

  if (firms?.length) {
    for (const f of firms) {
      const firmName = f.firms?.name || 'Unknown';
      const justification = await generateJustification(firmName, deal);
      await sb.from('campaign_batch_firms')
        .upsert({
          batch_id: batch.id,
          firm_outreach_state_id: f.id,
          firm_name: firmName,
          justification: justification || `Matches ${deal.sector || 'deal'} mandate`,
          created_at: new Date().toISOString(),
        }, { onConflict: 'batch_id,firm_outreach_state_id' });
    }
  }

  // Telegram notification
  const msg = `📋 *Campaign Review Ready* — ${deal.name}\n\nBatch #${batch.batch_number} has ${firms?.length || 0} firms ready for your approval.\n\nReview in the dashboard → Campaign tab.`;
  await sendTelegram(msg).catch(() => {});

  pushActivity({
    type: 'system',
    action: 'Campaign review ready',
    note: `${deal.name} — Batch #${batch.batch_number} (${firms?.length || 0} firms). Awaiting approval.`,
    deal_name: deal.name,
    dealId: deal.id,
  });

  console.log(`[BATCH] Batch #${batch.batch_number} for ${deal.name} → pending_approval (${firms?.length || 0} firms)`);
}

/**
 * Close the current approved batch and promote the next ready batch for review.
 * Called from the dashboard "Close Batch" button.
 *
 * @returns {{ closed: number|null, promoted: number|null, building: number|null }}
 */
async function closeBatchForDeal(dealId) {
  const sb = getSupabase();
  if (!sb) return { error: 'No DB connection' };

  // Find the deal for notifications
  const { data: deal } = await sb.from('deals').select('*').eq('id', dealId).single();

  // Mark the approved batch as completed
  const { data: approvedBatch } = await sb.from('campaign_batches')
    .select('id, batch_number')
    .eq('deal_id', dealId)
    .eq('status', 'approved')
    .limit(1)
    .single();

  if (approvedBatch) {
    await sb.from('campaign_batches')
      .update({ status: 'completed', updated_at: new Date().toISOString() })
      .eq('id', approvedBatch.id);
    console.log(`[BATCH] Batch #${approvedBatch.batch_number} for deal ${dealId} → completed`);
  }

  // Promote the next ready batch to pending_approval
  const { data: readyBatch } = await sb.from('campaign_batches')
    .select('id, batch_number')
    .eq('deal_id', dealId)
    .eq('status', 'ready')
    .order('batch_number', { ascending: true })
    .limit(1)
    .single();

  if (readyBatch) {
    await sb.from('campaign_batches')
      .update({ status: 'pending_approval', updated_at: new Date().toISOString() })
      .eq('id', readyBatch.id);

    // Send Telegram notification
    const msg = `📋 *Campaign Review Ready* — ${deal?.name || 'Deal'}\n\nBatch #${readyBatch.batch_number} is up for review. Open the dashboard → Campaign tab.`;
    await sendTelegram(msg).catch(() => {});

    pushActivity({
      type: 'system',
      action: 'Next batch ready for review',
      note: `${deal?.name || ''} — Batch #${readyBatch.batch_number} promoted to review after batch #${approvedBatch?.batch_number || '?'} closed.`,
      deal_name: deal?.name,
      dealId,
    });

    console.log(`[BATCH] Batch #${readyBatch.batch_number} for deal ${dealId} → pending_approval`);
    return { closed: approvedBatch?.batch_number || null, promoted: readyBatch.batch_number };
  }

  // No ready batch yet — check if one is still being built
  const { data: buildingBatch } = await sb.from('campaign_batches')
    .select('batch_number, ranked_firms, target_firms')
    .eq('deal_id', dealId)
    .eq('status', 'researching')
    .limit(1)
    .single();

  return {
    closed: approvedBatch?.batch_number || null,
    promoted: null,
    building: buildingBatch?.batch_number || null,
    buildingProgress: buildingBatch ? `${buildingBatch.ranked_firms || 0}/${buildingBatch.target_firms || BATCH_FIRM_TARGET}` : null,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// DEAL CYCLE
// ─────────────────────────────────────────────────────────────────────────────

async function runDealCycle(deal, state) {
  console.log(`[ORCHESTRATOR] ---- Cycle: ${deal.name} ----`);
  logStep('Cycle starting', deal.name, 'system', deal);

  // ── Batch gate ────────────────────────────────────────────────────────────
  // Get or create the current research batch for this deal.
  const batch = await getOrCreateBatch(deal);

  // Phase 0: keep pipeline topped up — promote archived borderline contacts + re-research if depleted
  await phaseTopUpPipeline(deal, state);

  // Phase 0b: cross-deal deduplication — hold contacts already in another active deal
  await phaseCrossDealCheck(deal, state);

  // All research/enrichment phases run 24/7
  if (state.research_enabled !== false) {
    // Skip DB promotion if the current researching batch already hit its firm target
    const batchFull = batch?.status === 'researching' && (batch?.ranked_firms || 0) >= BATCH_FIRM_TARGET;
    if (!batchFull) {
      await phaseDatabaseQuery(deal, batch);      // query investor DB, score with Haiku 4.5, promote shortlist
    } else {
      info(`[${deal.name}] DB QUERY: batch at ${batch.ranked_firms}/${BATCH_FIRM_TARGET} — skipping promotion`);
    }
    await phasePersonResearch(deal, batch); // research person+firm FIRST so ranker has full data
    await phaseRank(deal, state);    // rank only contacts that have been researched
    await phaseArchive(deal, state);
    await phaseNotionSync(deal, state); // sync immediately after ranking
  }
  // Check if this batch just hit the firm target — if so, trigger review and stop outreach
  if (batch?.status === 'researching') {
    const hitTarget = await updateBatchFirms(deal, batch);
    if (hitTarget) {
      logStep('Batch target reached — campaign review triggered', `Batch #${batch.batch_number}`, 'system', deal);
      console.log(`[ORCHESTRATOR] ---- Cycle complete: ${deal.name} (review triggered) ----`);
      return;
    }
  }

  // ── Outreach gate — must have an approved batch ───────────────────────────
  const approved = await isApprovedForOutreach(deal.id);
  if (!approved) {
    console.log(`[BATCH][GATE] ${deal.name} — no approved batch yet. Research only.`);
    logStep('Cycle complete (no approved batch)', '', 'system', deal);
    console.log(`[ORCHESTRATOR] ---- Cycle complete: ${deal.name} ----`);
    return;
  }

  // ── Sending window enforcement (EST) ─────────────────────────────────────
  const globallyPaused = state.outreach_paused_until && isGloballyPaused(state.outreach_paused_until);
  const inEmailWindow = isWithinEmailWindow(deal) && !globallyPaused;

  if (!inEmailWindow) {
    const next = describeNextEmailWindow(deal);
    info(`[${deal.name}] Outside sending window — next: ${next}. LinkedIn invites only.`);
  }

  // Temp close monitoring (always runs when approved)
  await phaseTempCloseCheck(deal, state);

  // Enrichment runs after approval — find emails/LinkedIn for the approved batch contacts
  if (state.enrichment_enabled !== false) {
    await phaseEnrich(deal, state);
    await phaseNotionSync(deal, state);
  }

  // LinkedIn invites run anytime (approved + enabled)
  if (state.outreach_enabled !== false) {
    await phaseLinkedInInvites(deal, state);
  }

  // Email + LinkedIn DMs only within 6am-8am or 8pm-11pm EST window
  if (inEmailWindow) {
    if (state.outreach_enabled !== false) {
      await phaseOutreach(deal, state);
    }
    if (state.followup_enabled !== false) {
      await phaseFollowUps(deal, state);
    }
  } else {
    info(`[${deal.name}] Email/DM outreach held — outside EST window`);
  }

  logStep('Cycle complete', '', 'system', deal);
  console.log(`[ORCHESTRATOR] ---- Cycle complete: ${deal.name} ----`);
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
    const contactedFirms = new Set((alreadyContacted || []).map(c => (c.company_name || '').toLowerCase().trim()).filter(Boolean));

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

        const { data: listInvestors } = await sb.from('investors_db')
          .select('*').eq('list_id', pl.list_id).limit(500);

        if (!listInvestors?.length) {
          await sb.from('deal_list_priorities')
            .update({ status: 'exhausted', exhausted_at: new Date().toISOString() }).eq('id', pl.id);
          continue;
        }

        const fresh = listInvestors.filter(inv =>
          !contactedDbIds.has(inv.id) &&
          !contactedFirms.has((inv.name || '').toLowerCase().trim())
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
        const scored = await batchScoreInvestors(fresh, dealInfo);
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
        !contactedFirms.has((inv.name || '').toLowerCase().trim())
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
      const existingLinkedin  = investor.decision_maker_linkedin || null;
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
      await sb.from('contacts').update({
        ...rankUpdate,
        notes: (() => {
          const clean = (contact.notes || '').replace(/\n?\[SCORE:[^\n]*\]/g, '').trim();
          const entry = `[SCORE: ${result.score} — ${result.grade}] ${result.rationale}`;
          return clean ? `${clean}\n${entry}` : entry;
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
  const sb = getSupabase();
  if (!sb) return;

  // Fetch unresearched contacts — NULL notes are excluded by SQL NOT LIKE (NULL != pattern = NULL = falsy),
  // so we fetch a wider batch without the notes filter and apply it in JS instead.
  // Include all case variants — DB may have 'RESEARCHED' (uppercase) from bulk imports
  const { data: allCandidates } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Researched', 'RESEARCHED', 'researched', 'Enriched', 'ENRICHED', 'enriched', 'Ranked', 'RANKED', 'ranked'])
    .order('created_at', { ascending: true })
    .limit(80);

  // Filter and prioritize in JS so we only spend live-search budget on contacts
  // that are missing core fields, inside the current batch, or otherwise eligible.
  const candidates = (allCandidates || [])
    .filter(c => shouldResearchContact(c, deal, batch))
    .sort((a, b) => {
      const aBatch = isContactInsideBatch(a, batch) ? 1 : 0;
      const bBatch = isContactInsideBatch(b, batch) ? 1 : 0;
      if (aBatch !== bBatch) return bBatch - aBatch;
      const aMissing = contactNeedsCoreResearch(a) ? 1 : 0;
      const bMissing = contactNeedsCoreResearch(b) ? 1 : 0;
      if (aMissing !== bMissing) return bMissing - aMissing;
      return Number(b.investor_score || 0) - Number(a.investor_score || 0);
    })
    .slice(0, 5);

  if (!candidates.length) {
    info(`[${deal.name}] phasePersonResearch: no eligible candidates`);
    return;
  }

  for (const contact of candidates) {
    pushActivity({
      type: 'research',
      action: 'Researching',
      note: `${contact.name}${contact.company_name ? ` — ${contact.company_name}` : ''}`,
      deal_name: deal.name,
      dealId: deal.id,
    });
    info(`[research] Person research: ${contact.name}: ${deal.name}`);

    let result = null;
    try {
      result = await researchPerson({ contact, deal });
    } catch (err) {
      const msg = err.isQuota
        ? `⚠ Quota exhausted — Grok & Gemini keys rate-limited or out of credits. Check billing.`
        : `⚠ Research API error: ${err.message}`;
      pushActivity({ type: 'error', action: `Research failed: ${contact.name}`, note: msg, deal_name: deal.name, dealId: deal.id });
      warn(`[PERSON RESEARCH] Error for ${contact.name}: ${err.message}`);
      await sb.from('contacts').update({
        notes: (contact.notes ? contact.notes + '\n' : '') + '[PERSON_RESEARCH_FAILED] API error',
      }).eq('id', contact.id);
      continue; // move to next contact, don't abort the whole batch
    }

    const updates = {};
    if (result) {
      updates.person_researched = true;
      if (result.job_title)    updates.job_title    = result.job_title;
      if (result.company_name && !contact.company_name) updates.company_name = result.company_name;
      if (result.firm_aum)     updates.aum_fund_size = result.firm_aum;
      if (result.typical_cheque) updates.typical_cheque_size = result.typical_cheque;
      if (result.sector_focus) updates.sector_focus = result.sector_focus;
      if (result.geography)    updates.geography    = result.geography;
      if (result.past_investments) updates.past_investments = result.past_investments;
      if (result.investment_thesis) updates.investment_thesis = result.investment_thesis;
      if (result.linkedin_url && !contact.linkedin_url) updates.linkedin_url = result.linkedin_url;
      // Update contact_type if research confirms a different classification
      if (result.contact_type_confirmed) {
        updates.contact_type = result.contact_type_confirmed === 'angel' ? 'individual' : result.contact_type_confirmed;
        updates.is_angel     = result.contact_type_confirmed === 'angel';
      }

      const parts = [];
      if (result.firm_description)  parts.push(result.firm_description);
      if (result.investment_thesis) parts.push(`Thesis: ${result.investment_thesis}`);
      if (result.investment_stage)  parts.push(`Stage: ${result.investment_stage}`);
      if (result.typical_cheque)    parts.push(`Cheque: ${result.typical_cheque}`);
      if (result.firm_aum)          parts.push(`AUM: ${result.firm_aum}`);
      if (result.geography)         parts.push(`Geography: ${result.geography}`);
      if (result.sector_focus)      parts.push(`Focus: ${result.sector_focus}`);
      if (result.past_investments)  parts.push(`Portfolio: ${result.past_investments}`);
      if (result.recent_news)       parts.push(`News: ${result.recent_news}`);

      const research = parts.length ? parts.join(' | ') : '';
      updates.notes = (contact.notes ? contact.notes + '\n' : '') +
        `[PERSON_RESEARCHED] ${research}`.substring(0, 2000);
    } else {
      updates.notes = (contact.notes ? contact.notes + '\n' : '') + '[PERSON_RESEARCHED] No data found';
    }

    await sb.from('contacts').update(updates).eq('id', contact.id);

    // ── Write research back to investors_db so future deals reuse it ──
    if (contact.investors_db_id && result) {
      try {
        const dbUpdates = { last_researched_at: new Date().toISOString(), person_researched: true };
        if (result.job_title)         dbUpdates.decision_maker_title = result.job_title;
        if (result.linkedin_url)      dbUpdates.decision_maker_linkedin = result.linkedin_url;
        if (result.past_investments)  dbUpdates.past_investments = result.past_investments;
        if (result.investment_thesis) dbUpdates.investment_thesis = result.investment_thesis;
        if (result.typical_cheque)    dbUpdates.typical_cheque_size = result.typical_cheque;
        if (result.firm_aum)          dbUpdates.aum_millions = parseFloat(result.firm_aum.replace(/[^0-9.]/g, '')) || undefined;
        if (result.geography)         dbUpdates.preferred_geographies = result.geography;
        if (result.sector_focus)      dbUpdates.preferred_industries  = result.sector_focus;
        if (result.contact_type_confirmed) {
          dbUpdates.contact_type = result.contact_type_confirmed;
          dbUpdates.is_angel = result.contact_type_confirmed === 'angel';
        }
        const noteParts = [];
        if (result.firm_description)  noteParts.push(result.firm_description);
        if (result.investment_thesis) noteParts.push(`Thesis: ${result.investment_thesis}`);
        if (result.recent_news)       noteParts.push(`News: ${result.recent_news}`);
        if (noteParts.length)         dbUpdates.research_notes = noteParts.join(' | ').substring(0, 2000);
        await sb.from('investors_db').update(dbUpdates).eq('id', contact.investors_db_id);
        console.log(`[RESEARCH PERSIST] Saved research for ${contact.company_name || contact.name} → investors_db`);
      } catch (e) { console.warn(`[RESEARCH PERSIST] investors_db write-back failed: ${e.message}`); }
    }

    if (contact.notion_page_id && result) {
      try {
        await updateNotionContact(contact.notion_page_id, {
          title: result.job_title || undefined,
          sectorFocus: result.sector_focus || undefined,
          geography: result.geography || undefined,
          chequeSize: result.typical_cheque || undefined,
          similarPastDeals: result.past_investments || undefined,
          notes: updates.notes,
          linkedinUrl: result.linkedin_url && !contact.linkedin_url ? result.linkedin_url : undefined,
        });
      } catch { /* non-fatal */ }
    }

    pushActivity({
      type: 'research',
      action: result ? `Researched: ${contact.name}` : `No data: ${contact.name}`,
      note: result
        ? `${contact.company_name || result.company_name || ''}${result.confidence ? ` — ${result.confidence} confidence` : ''}`
        : `${contact.company_name || ''} — no data found`,
      deal_name: deal.name,
      dealId: deal.id,
    });
    info(`[research] Researched: ${contact.name}${result ? ` (${result.confidence || '?'} confidence)` : ' — no data'}: ${deal.name}`);
    await sleep(2000); // brief gap between contacts to avoid Gemini rate limits
  }
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

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 3 — ENRICH
// Find email/phone for ranked contacts via KASPR; find LinkedIn URLs if missing
// ─────────────────────────────────────────────────────────────────────────────

export async function phaseEnrich(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  const { data: contacts } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Ranked', 'RANKED', 'ranked', 'Researched', 'RESEARCHED', 'researched'])
    .in('enrichment_status', ['Pending', 'pending', 'Partial', 'partial'])
    .limit(5);

  if (!contacts?.length) { info(`[${deal.name}] phaseEnrich: nothing to enrich`); return; }

  console.log(`[ORCHESTRATOR] phaseEnrich: enriching ${contacts.length} contacts for ${deal.name}`);
  logStep(`Enriching ${contacts.length} contact(s)`, deal.name, 'enrichment', deal);

  for (const contact of contacts) {
    // ── Check enriched_contacts cache before calling external APIs ──
    try {
      const { data: cached } = await sb.from('enriched_contacts')
        .select('email, phone, linkedin_url')
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
          await sb.from('contacts').update({ enrichment_status: 'linkedin_only', pipeline_stage: 'Enriched', linkedin_url: c.linkedin_url || null }).eq('id', contact.id);
        } else {
          await sb.from('contacts').update({
            email:            c.email,
            phone:            c.phone || null,
            linkedin_url:     c.linkedin_url || null,
            enrichment_status: 'enriched',
            pipeline_stage:   'Enriched',
          }).eq('id', contact.id);
          pushActivity({ type: 'enrichment', action: `Enriched (cached): ${contact.name}`, note: `${c.email}`, deal_name: deal.name, dealId: deal.id });
        }
        continue;
      }
    } catch (_) {}
    try {
      // Step 1: ensure LinkedIn URL exists
      let linkedinUrl = contact.linkedin_url;
      if (!linkedinUrl && state.linkedin_enabled !== false) {
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
        continue;
      }

      // If contact already has an email, skip KASPR/Apify and advance directly
      if (contact.email) {
        await sb.from('contacts').update({ enrichment_status: 'enriched', pipeline_stage: 'Enriched' }).eq('id', contact.id);
        logStep(`Already has email: ${contact.name}`, deal.name, 'enrichment', deal);
        pushActivity({ type: 'enrichment', action: `Email already on file`, note: `${contact.name} — advanced to Enriched`, deal_name: deal.name, dealId: deal.id });
        continue;
      }

      // No LinkedIn URL and no email — completely unreachable, archive immediately
      if (!linkedinUrl) {
        warn(`[ENRICH] No LinkedIn or email for ${contact.name} — archiving`);
        await sb.from('contacts').update({ enrichment_status: 'skipped_no_linkedin', pipeline_stage: 'Archived' }).eq('id', contact.id);
        pushActivity({ type: 'enrichment', action: `Archived (no contact info)`, note: `${contact.name} — no email or LinkedIn found`, deal_name: deal.name, dealId: deal.id });
        continue;
      }

      pushActivity({ type: 'enrichment', action: `Enriching: ${contact.name}`, note: `${contact.company_name || ''} — trying KASPR...`, deal_name: deal.name, dealId: deal.id });

      // Step 2: enrich email/phone via KASPR first
      const kasprResult = await enrichWithKaspr({ linkedinUrl, fullName: contact.name });

      if (kasprResult === 'RATE_LIMITED') {
        warn('[ENRICH] KASPR rate limited — stopping enrichment this cycle');
        break;
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
            continue;
          }
        } catch (apifyErr) {
          warn(`[ENRICH] Apify error for ${contact.name}: ${apifyErr.message}`);
          await sb.from('contacts').update({ enrichment_status: 'linkedin_only', pipeline_stage: 'Enriched' }).eq('id', contact.id);
          continue;
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
          continue; // handled — pick up in outreach queue as linkedin_only type
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
        if (enrichResult.company_name && !contact.company_name)     updates.company_name = enrichResult.company_name;
        if (enrichResult.linkedin_provider_id && !contact.linkedin_provider_id) updates.linkedin_provider_id = enrichResult.linkedin_provider_id;
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

      // Mirror to Notion if already synced
      if (contact.notion_page_id) {
        try {
          await updateNotionContact(contact.notion_page_id, {
            pipelineStage: updates.pipeline_stage,
            enrichmentStatus: updates.enrichment_status,
            ...(enrichResult?.email ? { email: enrichResult.email } : {}),
            ...(enrichResult?.phone ? { phone: enrichResult.phone } : {}),
          });
        } catch (e) { /* non-fatal */ }
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

  // Secondary pass: find LinkedIn for contacts that have email (e.g. from CSV) but no linkedin_url
  // phaseLinkedInInvites requires linkedin_url — without this, CSV contacts never get invites
  if (state.linkedin_enabled !== false) {
    const { data: needLinkedIn } = await sb.from('contacts')
      .select('id, name, company_name, job_title, email, enrichment_status')
      .eq('deal_id', deal.id)
      .in('pipeline_stage', ['Ranked', 'RANKED', 'ranked', 'Enriched', 'ENRICHED', 'enriched'])
      .is('linkedin_url', null)
      .limit(5);

    for (const contact of (needLinkedIn || [])) {
      try {
        const url = await findLinkedInUrl({ name: contact.name, company: contact.company_name, title: contact.job_title });
        if (url) {
          await sb.from('contacts').update({ linkedin_url: url }).eq('id', contact.id);
          logStep(`LinkedIn found: ${contact.name}`, deal.name, 'enrichment', deal);
        }
      } catch (e) {
        console.warn(`[ENRICH] LinkedIn find failed for ${contact.name}:`, e.message);
      }
      await sleep(1000);
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
      if (!linkedinUrl && state.linkedin_enabled !== false) {
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

      if (contact.notion_page_id) {
        try {
          await updateNotionContact(contact.notion_page_id, {
            pipelineStage: updates.pipeline_stage,
            enrichmentStatus: updates.enrichment_status,
            ...(enrichResult?.email ? { email: enrichResult.email } : {}),
            ...(enrichResult?.phone ? { phone: enrichResult.phone } : {}),
          });
        } catch (e) { /* non-fatal */ }
      }

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
// Create Notion CRM pages for contacts that don't have one yet
// ─────────────────────────────────────────────────────────────────────────────

const ACTIVE_STAGES = ['Ranked', 'Enriched', 'invite_sent', 'invite_accepted', 'email_sent', 'dm_sent', 'Replied', 'In Conversation', 'Meeting Booked', 'Meeting Scheduled'];

// Pipeline health constants
const DAILY_INVITE_TARGET = 28;        // default target LinkedIn invites per day
const REACTIVATION_MIN_SCORE = 40;     // re-promote archived contacts with score >= this
const PIPELINE_LOW_THRESHOLD = 10;     // trigger top-up when ready contacts drop below this
const RESEARCH_COOLDOWN_MS = 24 * 60 * 60 * 1000; // max once per day per deal

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

async function phaseNotionSync(deal, state) {
  const sb = getSupabase();
  if (!sb) return;

  // Create new Notion pages for contacts that haven't been synced yet
  const { data: toCreate } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .is('notion_page_id', null)
    .in('pipeline_stage', ACTIVE_STAGES)
    .limit(10);

  if (toCreate?.length) {
    console.log(`[ORCHESTRATOR] phaseNotionSync: syncing ${toCreate.length} contacts to Notion`);

    for (const contact of toCreate) {
      try {
        const notionPage = await createNotionContact({
          name: contact.name,
          title: contact.job_title || '',          // correct field name
          linkedinUrl: contact.linkedin_url || null,
          email: contact.email || null,
          phone: contact.phone || null,
          sectorFocus: contact.sector_focus || '',
          geography: contact.geography || '',
          chequeSize: contact.typical_cheque_size || '',
          score: contact.investor_score,            // correct field name
          pipelineStage: contact.pipeline_stage,
          source: contact.source || 'Roco',
          dealName: deal.name,
          notes: contact.notes || '',
          enrichmentStatus: contact.enrichment_status || 'Pending',
        });

        if (notionPage?.id) {
          await sb.from('contacts').update({ notion_page_id: notionPage.id }).eq('id', contact.id);
          logStep(`Notion: ${contact.name}`, deal.name, 'system', deal);
        }
      } catch (e) {
        console.warn(`[NOTION SYNC] Failed for ${contact.name}:`, e.message);
      }
      await sleep(500);
    }
  }

  // Update existing Notion pages — push all research fields, not just stage/score
  const { data: toUpdate } = await sb.from('contacts')
    .select('id, name, notion_page_id, pipeline_stage, email, phone, linkedin_url, enrichment_status, investor_score, notes, job_title, sector_focus, geography, typical_cheque_size, past_investments')
    .eq('deal_id', deal.id)
    .not('notion_page_id', 'is', null)
    .in('pipeline_stage', ACTIVE_STAGES)
    .limit(20);

  if (toUpdate?.length) {
    for (const contact of toUpdate) {
      try {
        await updateNotionContact(contact.notion_page_id, {
          pipelineStage: contact.pipeline_stage,
          enrichmentStatus: contact.enrichment_status || undefined,
          ...(contact.email ? { email: contact.email } : {}),
          ...(contact.phone ? { phone: contact.phone } : {}),
          ...(contact.linkedin_url ? { linkedinUrl: contact.linkedin_url } : {}),
          ...(contact.investor_score != null ? { score: contact.investor_score } : {}),
          ...(contact.notes ? { notes: contact.notes } : {}),
          ...(contact.job_title ? { title: contact.job_title } : {}),
          ...(contact.sector_focus ? { sectorFocus: contact.sector_focus } : {}),
          ...(contact.geography ? { geography: contact.geography } : {}),
          ...(contact.typical_cheque_size ? { chequeSize: contact.typical_cheque_size } : {}),
          ...(contact.past_investments ? { similarPastDeals: contact.past_investments } : {}),
        });
      } catch (e) {
        // non-fatal — Notion may have renamed/deleted properties
        console.warn(`[NOTION SYNC] Update failed for ${contact.name}:`, e.message);
      }
      await sleep(300);
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

        // Build Notion-style contact page for sendEmailForApproval
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
              const followupDays     = deal?.followup_days_email || 7;

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

  // ── Firm-level sequencing ─────────────────────────────────────────────────
  // Only one active LinkedIn contact per firm at a time.
  // Rules:
  //  1. If any contact at the firm has responded (In Conversation / Replied / Meeting) → block ALL others at that firm
  //  2. If a contact's invite was sent < 7 days ago and they haven't accepted → wait before sending to next person
  //  3. If invite is 7+ days old with no acceptance AND no response → eligible to move to next contact

  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  const ENGAGED_STAGES = ['In Conversation', 'Replied', 'Meeting Booked', 'Meeting Scheduled'];
  const ACTIVE_LI_STAGES = ['invite_sent', 'invite_accepted', 'dm_sent', 'email_sent'];

  const { data: engagedContacts } = await sb.from('contacts')
    .select('company_name, pipeline_stage, invite_sent_at, response_received')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', [...ENGAGED_STAGES, ...ACTIVE_LI_STAGES])
    .not('company_name', 'is', null);

  const respondedFirms = new Set();  // firm has a response → block entirely
  const waitingFirms   = new Set();  // firm has active invite < 7 days → wait

  for (const c of engagedContacts || []) {
    const firm = (c.company_name || '').toLowerCase().trim();
    if (!firm || GENERIC_FIRM_NAMES.has(firm)) continue; // skip non-real "firms"
    if (c.response_received || ENGAGED_STAGES.includes(c.pipeline_stage)) {
      respondedFirms.add(firm);
    } else if (ACTIVE_LI_STAGES.includes(c.pipeline_stage)) {
      // Invite sent but no response yet — only block if < 7 days old
      if (!c.invite_sent_at || c.invite_sent_at > sevenDaysAgo) {
        waitingFirms.add(firm);
      }
      // If 7+ days with no movement: fall through → next contact at that firm is eligible
    }
  }
  // ──────────────────────────────────────────────────────────────────────────

  const perCycleLimit = Math.min(5, remainingToday);

  const { data: candidates } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .in('pipeline_stage', ['Ranked', 'RANKED', 'ranked', 'Enriched', 'ENRICHED', 'enriched'])  // both tracks get LinkedIn invites
    .not('linkedin_url', 'is', null)
    .is('invite_sent_at', null)
    .order('investor_score', { ascending: false })
    .limit(50); // fetch more so we can filter firm-by-firm

  if (!candidates?.length) { info(`[${deal.name}] phaseLinkedInInvites: nothing to invite (${sentToday}/${dailyTarget} sent today)`); return; }

  // One contact per firm per cycle; skip blocked/waiting firms
  const seenFirms = new Set();
  const contacts = [];
  for (const c of candidates) {
    if (contacts.length >= perCycleLimit) break;
    const firm = (c.company_name || '').toLowerCase().trim();
    const isRealFirm = firm && !GENERIC_FIRM_NAMES.has(firm);
    if (isRealFirm && (respondedFirms.has(firm) || waitingFirms.has(firm))) continue;
    if (isRealFirm && seenFirms.has(firm)) continue; // already sending to this firm this cycle
    if (isRealFirm) seenFirms.add(firm);
    contacts.push(c);
  }

  if (!contacts.length) {
    info(`[${deal.name}] phaseLinkedInInvites: all firms waiting or blocked (${respondedFirms.size} responded, ${waitingFirms.size} waiting)`);
    return;
  }

  console.log(`[ORCHESTRATOR] phaseLinkedInInvites: ${contacts.length} contacts for ${deal.name} (${respondedFirms.size} firms responded, ${waitingFirms.size} waiting)`);

  for (const contact of contacts) {
    if (contactsInFlight.has(contact.id)) continue;
    try {
      // Blank connection request — no note. First message comes after acceptance.
      let providerId = contact.linkedin_provider_id || null;
      if (!providerId && contact.linkedin_url) {
        const urnMatch = contact.linkedin_url.match(/miniProfileUrn=urn%3Ali%3A[^&]+%3A([A-Za-z0-9_-]+)/);
        if (urnMatch) providerId = urnMatch[1];
      }
      await sendLinkedInInvite({ providerId, linkedinUrl: providerId ? null : contact.linkedin_url });

      const liFollowupDays = deal?.followup_days_li || 7;
      const hasEmail = !!contact.email;
      // Enriched contacts (have email) keep their stage so phaseOutreach can still email them.
      // Ranked contacts (no email, LinkedIn-only) move to invite_sent.
      const inviteUpdate = {
        invite_sent_at:   new Date().toISOString(),
        outreach_channel: 'linkedin_invite',
        follow_up_count:  0,
      };
      if (!hasEmail) {
        inviteUpdate.pipeline_stage   = 'invite_sent';
        inviteUpdate.follow_up_due_at = new Date(Date.now() + liFollowupDays * 24 * 60 * 60 * 1000).toISOString();
      }
      await sb.from('contacts').update(inviteUpdate).eq('id', contact.id);

      logStep(`LinkedIn invite sent`, contact.name, 'linkedin', deal);
      pushActivity({
        type: 'invite',
        action: `Connection request sent`,
        note: `${contact.name}${contact.company_name ? ` @ ${contact.company_name}` : ''}`,
        deal_name: deal.name, dealId: deal.id,
      });
      await sbLogActivity({
        dealId: deal.id, contactId: contact.id,
        eventType: 'LINKEDIN_INVITE_SENT',
        summary: `Connection request sent to ${contact.name}`,
        apiUsed: 'unipile',
      });
    } catch (e) {
      const msg = (e.message || '').toLowerCase();
      // Detect already-connected: skip the invite, queue for DM outreach immediately
      const alreadyConnected = msg.includes('already connected')
        || msg.includes('member_already_connected')
        || msg.includes('existing_relationship')
        || msg.includes('you are already connected')
        || msg.includes('connection already exists');
      // Detect pending invite: avoid retrying, just mark as invite_sent
      const pendingInvite = msg.includes('already sent')
        || msg.includes('invite_already_sent')
        || msg.includes('pending invitation')
        || msg.includes('already invited');

      if (alreadyConnected) {
        console.log(`[LINKEDIN] ${contact.name} is already connected — skipping invite, queuing for DM`);
        const liFollowupDays = deal?.followup_days_li || 7;
        await sb.from('contacts').update({
          pipeline_stage:   'invite_accepted',
          invite_sent_at:   new Date().toISOString(),
          outreach_channel: 'linkedin_dm',
          follow_up_due_at: new Date(Date.now() + liFollowupDays * 24 * 60 * 60 * 1000).toISOString(),
          notes: (contact.notes ? contact.notes + ' | ' : '') + 'Was already connected on LinkedIn — skipped invite, queued for DM',
        }).eq('id', contact.id);
        await sbLogActivity({
          dealId: deal.id, contactId: contact.id,
          eventType: 'LINKEDIN_ALREADY_CONNECTED',
          summary: `${contact.name} already connected — moved directly to DM queue`,
          apiUsed: 'unipile',
        });
      } else if (pendingInvite) {
        console.log(`[LINKEDIN] ${contact.name} already has a pending invite — marking as invite_sent`);
        const liFollowupDays = deal?.followup_days_li || 7;
        await sb.from('contacts').update({
          pipeline_stage:  'invite_sent',
          invite_sent_at:  new Date().toISOString(),
          follow_up_due_at: new Date(Date.now() + liFollowupDays * 24 * 60 * 60 * 1000).toISOString(),
        }).eq('id', contact.id);
      } else {
        console.warn(`[LINKEDIN] Invite failed for ${contact.name}: ${e.message}`);
      }
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
  // 2. activeFirms    — firm has someone actively in-pipeline (not yet replied) → hold new outreach
  const RESPONDED_STAGES = ['In Conversation', 'Replied', 'Meeting Booked', 'Meeting Scheduled'];
  const ACTIVE_PIPELINE_STAGES = ['email_sent', 'dm_sent', 'invite_accepted', 'invite_sent', 'intro_sent', 'follow_up_sent', 'awaiting_response', 'temp_closed'];

  const { data: firmGateContacts } = await sb.from('contacts')
    .select('company_name, pipeline_stage, response_received, conversation_state')
    .eq('deal_id', deal.id)
    .or(`response_received.eq.true,pipeline_stage.in.(${[...RESPONDED_STAGES, ...ACTIVE_PIPELINE_STAGES].map(s => `"${s}"`).join(',')})`)
    .not('company_name', 'is', null);

  const respondedFirms = new Set();
  const activeFirms    = new Set();

  for (const c of firmGateContacts || []) {
    const firm = (c.company_name || '').toLowerCase().trim();
    if (!firm || GENERIC_FIRM_NAMES.has(firm)) continue;
    if (c.response_received || RESPONDED_STAGES.includes(c.pipeline_stage)) {
      respondedFirms.add(firm);
    } else if (ACTIVE_PIPELINE_STAGES.includes(c.pipeline_stage) || ACTIVE_PIPELINE_STAGES.includes(c.conversation_state)) {
      activeFirms.add(firm);
    }
  }

  const isFirmBlocked = (c) => {
    const firm = (c.company_name || '').toLowerCase().trim();
    if (!firm || GENERIC_FIRM_NAMES.has(firm)) return false;
    return respondedFirms.has(firm) || activeFirms.has(firm);
  };

  // Research gate: only outreach contacts whose research is complete.
  // A contact is research-ready if:
  //   (a) person_researched is true, OR
  //   (b) enrichment_status is not pending (research ran but may have come back empty — Sonnet fills gaps)
  // Contacts still awaiting research are skipped this cycle and picked up once phasePersonResearch completes.
  const RESEARCH_PENDING_STATUSES = ['pending', 'Pending'];
  const isResearchReady = (c) => {
    if (c.person_researched) return true;
    if (RESEARCH_PENDING_STATUSES.includes(c.enrichment_status)) {
      info(`[${deal.name}] phaseOutreach: ${c.name} — research not yet complete, deferring`);
      return false;
    }
    return true; // enrichment done but research data may be sparse — Sonnet fills from own knowledge
  };

  // Email outreach: Enriched contacts with email — gated by email channel window
  let emailContacts = [];
  if (isWithinChannelWindow(deal, 'email')) {
    const { data } = await sb.from('contacts')
      .select('*')
      .eq('deal_id', deal.id)
      .eq('pipeline_stage', 'Enriched')
      .not('email', 'is', null)
      .limit(5);
    emailContacts = (data || []).filter(c => !isFirmBlocked(c) && isResearchReady(c)).slice(0, 2);
  } else {
    info(`[${deal.name}] phaseOutreach: outside email window — skipping emails`);
  }

  // DM outreach: invite-accepted contacts — gated by LinkedIn DM window
  // Exclude anyone who has already responded on any channel (e.g. replied to email after accepting invite)
  let dmContacts = [];
  if (isWithinChannelWindow(deal, 'linkedin_dm')) {
    const { data } = await sb.from('contacts')
      .select('*')
      .eq('deal_id', deal.id)
      .eq('pipeline_stage', 'invite_accepted')
      .not('response_received', 'eq', true)
      .limit(5);
    dmContacts = (data || []).filter(c => !isFirmBlocked(c) && isResearchReady(c)).slice(0, 2);
  } else {
    info(`[${deal.name}] phaseOutreach: outside LinkedIn DM window — skipping DMs`);
  }

  const contacts = [...emailContacts, ...dmContacts];
  if (!contacts.length) { info(`[${deal.name}] phaseOutreach: no contacts ready (${respondedFirms.size} firms responded, ${activeFirms.size} firms active)`); return; }

  console.log(`[ORCHESTRATOR] phaseOutreach: ${contacts.length} contacts for ${deal.name} (${emailContacts.length} email, ${dmContacts.length} DM, ${respondedFirms.size} responded, ${activeFirms.size} active)`);

  for (const contact of contacts) {
    if (contactsInFlight.has(contact.id)) {
      info(`${contact.name} already in approval queue — skipping`);
      continue;
    }
    await handleOutreachApproval(contact, 'INTRO', 0, deal);
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

  const now = new Date().toISOString();
  const { data: allDue } = await sb.from('contacts')
    .select('*')
    .eq('deal_id', deal.id)
    .lte('follow_up_due_at', now)
    // invite_sent = LinkedIn invite sent but not yet accepted — follow up via email if available
    .in('pipeline_stage', ['email_sent', 'dm_sent', 'invite_accepted', 'invite_sent'])
    .limit(10);

  // Apply firm gate + channel window gate + cross-channel response gate
  const contacts = (allDue || []).filter(c => {
    const firm = (c.company_name || '').toLowerCase().trim();
    if (firm && !GENERIC_FIRM_NAMES.has(firm) && respondedFirms.has(firm)) return false;

    // Cross-channel block: if they responded on one channel, don't follow up on the other
    if (c.response_received) return false;

    // invite_sent follow-ups go via email during email window (they haven't accepted yet, so no DM)
    // dm_sent follow-ups go during LinkedIn DM window
    // everything else (email_sent, invite_accepted) goes via email window
    const ch = c.outreach_channel === 'linkedin_dm' ? 'linkedin_dm' : 'email';
    return isWithinChannelWindow(deal, ch);
  }).slice(0, 2);

  if (!contacts?.length) return;

  console.log(`[ORCHESTRATOR] phaseFollowUps: ${contacts.length} contacts due follow-up for ${deal.name}`);

  for (const contact of contacts) {
    if (contactsInFlight.has(contact.id)) continue;
    const followUpNumber = (contact.follow_up_count || 0) + 1;
    const stage = followUpNumber === 1 ? 'FOLLOW-UP 1' : followUpNumber === 2 ? 'FOLLOW-UP 2' : 'FOLLOW-UP 3';
    if (followUpNumber > 3) {
      // Max follow-ups reached — archive
      await sb.from('contacts').update({ pipeline_stage: 'Archived', follow_up_due_at: null }).eq('id', contact.id);
      continue;
    }
    // For invite_sent contacts: force email channel for the follow-up (can't re-send LinkedIn invite)
    const contactForFollowUp = contact.pipeline_stage === 'invite_sent'
      ? { ...contact, outreach_channel: contact.email ? 'email' : null }
      : contact;
    if (contact.pipeline_stage === 'invite_sent' && !contact.email) {
      // No email and LinkedIn invite pending — nothing we can do yet, push due date forward
      const nextDays = deal?.followup_days_li || 7;
      await sb.from('contacts').update({
        follow_up_due_at: new Date(Date.now() + nextDays * 24 * 60 * 60 * 1000).toISOString(),
      }).eq('id', contact.id);
      info(`[${deal.name}] ${contact.name}: invite pending, no email — waiting another ${nextDays} days`);
      continue;
    }
    await handleOutreachApproval(contactForFollowUp, stage, followUpNumber, deal);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// OUTREACH APPROVAL HANDLER (shared by phaseOutreach + phaseFollowUps)
// ─────────────────────────────────────────────────────────────────────────────

async function handleOutreachApproval(contact, stage, followUpNumber, deal) {
  if (contactsInFlight.has(contact.id)) return;

  // Hard gate — never attempt to draft or send if the contact has no name
  if (!contact.name || contact.name.trim() === '' || contact.name.toLowerCase() === 'null') {
    warn(`[OUTREACH] Skipping contact ${contact.id} — no name, cannot draft a message`);
    return;
  }

  // Build a contact-page-like object for draftEmail (expects Notion-style page)
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

  let draft = await draftEmailWithTemplate(contactPage, null, stage, deal, effectiveInstructions);
  if (!draft) {
    error(`[OUTREACH] Draft generation failed for ${contact.name}`);
    return;
  }

  contactsInFlight.add(contact.id);

  pushActivity({
    type: 'email',
    action: `Email queued for approval`,
    note: `${contact.name}${contact.company_name ? ` @ ${contact.company_name}` : ''} — awaiting Telegram`,
    deal_name: deal?.name,
    dealId: deal?.id,
  });

  try {
    let decision = await sendEmailForApproval(contactPage, draft, contact.notes || '', contact.investor_score || 0, stage, deal.id);

    let editCount = 0;
    while (decision.action === 'edit' && editCount < 3) {
      editCount++;
      draft = await draftEmail(contactPage, null, stage, decision.instructions);
      if (!draft) return;
      decision = await sendEmailForApproval(contactPage, draft, contact.notes || '', contact.investor_score || 0, stage, deal.id);
    }

    if (decision.action === 'approve') {
      await executeOutreach(contact, contactPage, draft, decision, stage, followUpNumber, deal);
    } else {
      await handleNonApproval(contact, decision.action);
    }
  } finally {
    contactsInFlight.delete(contact.id);
  }
}

async function executeOutreach(contact, contactPage, draft, decision, stage, followUpNumber, deal) {
  const sb = getSupabase();
  let result = null;
  let channel = 'email';

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

  // Determine which channel we'll use and gate on its window BEFORE sending anything
  const preferLinkedIn = stage === 'INTRO' && !!contact.linkedin_provider_id;
  const preferredChannel = preferLinkedIn ? 'linkedin_dm' : 'email';
  const channelWindowKey = preferLinkedIn ? 'linkedin_dm' : 'email';

  if (!isWithinChannelWindow(deal, channelWindowKey)) {
    info(`[${deal.name}] executeOutreach: outside ${preferredChannel} window for ${contact.name} — skipping this cycle`);
    return;
  }

  // Try LinkedIn DM for intro if we have a provider ID — requires separate Telegram approval
  if (preferLinkedIn) {
    const dmDecision = await sendLinkedInDMForApproval(contact, draft.body, deal.id);
    if (dmDecision.action === 'approve') {
      try {
        const dmBody = dmDecision.body || draft.body;
        const dmResult = await sendLinkedInDM({
          attendeeProviderId: contact.linkedin_provider_id,
          message: dmBody,
        });
        if (dmResult?.success) {
          result = dmResult;
          channel = 'linkedin_dm';
          if (sb && dmResult.chatId) {
            await sb.from('contacts').update({ linkedin_chat_id: dmResult.chatId }).eq('id', contact.id);
          }
        }
      } catch (e) {
        console.warn(`[OUTREACH] LinkedIn DM failed for ${contact.name}: ${e.message} — falling back to email`);
      }
    } else {
      info(`[OUTREACH] LinkedIn DM skipped for ${contact.name} — falling back to email`);
    }
  }

  // Fall back to email (also check email window if falling back from LinkedIn)
  if (!result && contact.email) {
    if (!isWithinChannelWindow(deal, 'email')) {
      info(`[${deal.name}] executeOutreach: LinkedIn DM failed and outside email window for ${contact.name} — skipping`);
      return;
    }
    const emailResult = await unipileSendEmail({
      to: contact.email,
      toName: contact.name,
      subject: decision.subject || draft.subject,
      body: draft.body,
    });
    if (emailResult) {
      result = emailResult;
      channel = 'email';
    }
  }

  if (!result) {
    warn(`[OUTREACH] All send methods failed for ${contact.name}`);
    return;
  }

  rocoState.emailsSent++;

  const newStage = channel === 'linkedin_dm' ? 'dm_sent' : 'email_sent';
  const followupDays = channel === 'linkedin_dm'
    ? (deal?.followup_days_li || 7)
    : (deal?.followup_days_email || 7);
  const followUpDueAt = new Date(Date.now() + followupDays * 24 * 60 * 60 * 1000).toISOString();

  if (sb) {
    const contactUpdate = {
      pipeline_stage: newStage,
      last_contacted_at: new Date().toISOString(),
      follow_up_due_at: followUpDueAt,
      follow_up_count: followUpNumber,
    };
    if (channel === 'email') {
      contactUpdate.last_email_sent_at = new Date().toISOString();
    }
    await sb.from('contacts').update(contactUpdate).eq('id', contact.id);
  }

  // Also update Notion page if we have one
  if (contact.notion_page_id) {
    try {
      await updateNotionContact(contact.notion_page_id, {
        pipelineStage: newStage,
        lastContacted: new Date().toISOString().split('T')[0],
        domApproved: 'Yes',
      });
    } catch (e) {
      console.warn(`[OUTREACH] Notion update failed for ${contact.name}: ${e.message}`);
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
  sendTelegram(`✅ *${channelLabel} sent* → *${contact.name}* (${contact.company_name || 'unknown firm'})${subjectLine}`).catch(() => {});

  await sbLogActivity({
    dealId: deal?.id,
    contactId: contact.id,
    eventType: channel === 'linkedin_dm' ? 'LINKEDIN_DM_SENT' : 'EMAIL_SENT',
    summary: `${stage} sent to ${contact.name} @ ${contact.company_name || ''} via ${channel}`,
    detail: { subject: decision.subject, stage, channel },
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
    templateName: draft?.templateName || null,
  }).catch(() => {});

  // Set conversation_state to 'intro_sent' or 'follow_up_sent'
  const newConvState = followUpNumber > 0 ? 'follow_up_sent' : 'intro_sent';
  await setConversationState(contact.id, newConvState).catch(() => {});
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
 * Build a Notion-page-like object from a flat Supabase contact row.
 * draftEmail and sendEmailForApproval expect getContactProp-style access.
 */
function buildContactPage(contact) {
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
      'Investment Thesis':      { type: 'rich_text', rich_text: [{ plain_text: '' }] },
    },
    // Expose flat fields too for convenience
    name: contact.name,
    email: contact.email,
    company_name: contact.company_name,
    linkedin_url: contact.linkedin_url,
    investor_score: contact.investor_score,
  };
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
