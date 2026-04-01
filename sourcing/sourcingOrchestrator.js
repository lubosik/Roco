/**
 * sourcing/sourcingOrchestrator.js
 * Company sourcing campaign cycle runner.
 * Mirrors the investor outreach orchestrator architecture exactly.
 * Phase A: Research companies
 * Phase B: Find decision makers per company
 * Phase C: Rank companies
 * Phase D: Enrich company contacts
 * Phase E: Queue outreach (Telegram approval)
 * Phase F: Execute approved sends (window-gated)
 */

import { getSupabase } from '../core/supabase.js';
import { pushActivity } from '../dashboard/server.js';
import { sendTelegram, sendSourcingDraftToTelegram } from '../approval/telegramBot.js';
import { researchCompaniesForCampaign, findDecisionMakersAtCompany } from './companyResearcher.js';
import { rankUnrankedCompanies } from './companyRanker.js';
import { enrichCompanyContact } from './companyEnricher.js';
import { constructCompanySourcingMessage } from './messageConstructorSourcing.js';
import { sendLinkedInInvite, sendLinkedInDM, sendEmail as unipileSendEmail } from '../integrations/unipileClient.js';
import { DateTime } from 'luxon';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Contacts currently in the approval flow — prevents duplicate approval drafts
const sourcingContactsInFlight = new Set();

// ─────────────────────────────────────────────────────────
// WINDOW CHECK (for campaigns — uses JSONB windows)
// ─────────────────────────────────────────────────────────

function isWithinCampaignWindow(now, windowObj) {
  if (!windowObj?.start || !windowObj?.end) return true;
  const [startH, startM] = windowObj.start.split(':').map(Number);
  const [endH,   endM]   = windowObj.end.split(':').map(Number);
  const nowMinutes = now.hour * 60 + now.minute;
  return nowMinutes >= (startH * 60 + startM) && nowMinutes < (endH * 60 + endM);
}

// ─────────────────────────────────────────────────────────
// MAIN CAMPAIGN CYCLE
// ─────────────────────────────────────────────────────────

export async function runCompanySourcingCycle(campaign) {
  const sb = getSupabase();
  if (!sb) return;

  console.log(`[SOURCING] ---- Cycle: ${campaign.name} ----`);
  pushActivity({ type: 'system', action: 'Sourcing cycle starting', note: `[${campaign.name}]` });

  try {
    // Phase 1: Research — find companies (if too few or never researched)
    // Always re-fetch campaign from DB to get latest last_research_at (may have been set by setImmediate post-create)
    const { data: freshCampaign } = await sb.from('sourcing_campaigns').select('*').eq('id', campaign.id).single();
    const liveCampaign = freshCampaign || campaign;

    const { count: companyCount } = await sb.from('target_companies')
      .select('id', { count: 'exact', head: true })
      .eq('campaign_id', campaign.id);

    // Research logic: only run if we're short on companies, or it's been 24+ hours since last run
    const count = companyCount || 0;
    const researchAge = liveCampaign.last_research_at
      ? (Date.now() - new Date(liveCampaign.last_research_at).getTime()) / 3600000
      : null;
    // Run if: fewer than 10 companies, OR (fewer than 30 and 24h+ since last research)
    const needsResearch = count < 10 || (count < 30 && researchAge !== null && researchAge > 24);
    if (needsResearch) {
      await researchCompaniesForCampaign(liveCampaign);
    } else if (count >= 10) {
      console.log(`[SOURCING] Research skipped — ${count} companies already found`);
    }

    // Phase 2: Deep research — find decision makers at unresearched companies (3 per cycle, Grok is slow)
    const { data: unresearched } = await sb.from('target_companies')
      .select('*')
      .eq('campaign_id', liveCampaign.id)
      .eq('research_status', 'researched')
      .limit(3);

    for (const company of (unresearched || [])) {
      try {
        await findDecisionMakersAtCompany(company, liveCampaign);
        await sleep(2000);
      } catch (err) {
        console.warn(`[SOURCING] Decision maker research failed for ${company.company_name}:`, err.message);
      }
    }

    // Phase 3: Rank unranked companies
    await rankUnrankedCompanies(liveCampaign);

    // Phase 4: Enrich company contacts (KASPR → Apify chain)
    const { data: unenriched } = await sb.from('company_contacts')
      .select('*')
      .eq('campaign_id', liveCampaign.id)
      .eq('enrichment_status', 'pending')
      .eq('pipeline_stage', 'researched')
      .limit(10);

    for (const contact of (unenriched || [])) {
      try {
        await enrichCompanyContact(contact, liveCampaign);
        await sleep(1500);
      } catch (err) {
        console.warn(`[SOURCING] Enrichment failed for ${contact.name}:`, err.message);
      }
    }

    // Phase 5: Queue outreach — prepare messages, send to Telegram for approval
    const toQueue = await getContactsReadyForOutreach(liveCampaign.id);
    for (const contact of toQueue) {
      if (sourcingContactsInFlight.has(contact.id)) continue;

      try {
        const { data: company } = await sb.from('target_companies')
          .select('*').eq('id', contact.company_id).single();

        if (!company) continue;

        // Skip if company already has a meeting booked or responded
        if (company.meeting_booked || company.firm_responded || company.outreach_status === 'meeting_booked') continue;

        // Skip if match_tier is 'archive' or 'possible' (only contact hot/warm)
        if (company.match_tier === 'archive' || company.match_tier === 'possible') continue;

        await prepareAndQueueCompanySourcingOutreach(contact, company, liveCampaign);
        await sleep(1000);
      } catch (err) {
        console.warn(`[SOURCING] Queue outreach failed for ${contact.name}:`, err.message);
      }
    }

    // Phase 6: Execute approved sends (window-gated)
    const now = DateTime.now().setZone(liveCampaign.timezone || 'America/New_York');

    if (isWithinCampaignWindow(now, liveCampaign.linkedin_connection_window)) {
      await executeApprovedLinkedInConnections(liveCampaign.id, liveCampaign);
    }
    if (isWithinCampaignWindow(now, liveCampaign.email_send_window)) {
      await executeApprovedEmails(liveCampaign.id, liveCampaign);
    }
    if (isWithinCampaignWindow(now, liveCampaign.linkedin_dm_window)) {
      await executeApprovedLinkedInDMs(liveCampaign.id, liveCampaign);
    }

  } catch (err) {
    console.error(`[SOURCING] Cycle error for ${campaign.name}:`, err.message);
  }

  pushActivity({ type: 'system', action: 'Sourcing cycle complete', note: `[${campaign.name}]` });
  console.log(`[SOURCING] ---- Cycle complete: ${campaign.name} ----`);
}

// ─────────────────────────────────────────────────────────
// CONTACTS READY FOR OUTREACH
// ─────────────────────────────────────────────────────────

async function getContactsReadyForOutreach(campaignId) {
  const sb = getSupabase();
  if (!sb) return [];

  // Only contacts with email — Phase 5 queues email drafts for approval.
  // LinkedIn-only contacts skip Phase 5; Phase 6 sends their invite autonomously,
  // then the webhook triggers a LinkedIn DM draft after acceptance.
  // email_only = has email but no LinkedIn (still valid for email outreach)
  const { data } = await sb.from('company_contacts')
    .select('*')
    .eq('campaign_id', campaignId)
    .in('enrichment_status', ['enriched', 'enriched_apify', 'email_only'])
    .not('email', 'is', null)
    .in('pipeline_stage', ['enriched', 'researched'])
    .eq('outreach_count', 0)
    .order('created_at', { ascending: true })
    .limit(5);

  return data || [];
}

// ─────────────────────────────────────────────────────────
// PREPARE + QUEUE OUTREACH
// ─────────────────────────────────────────────────────────

async function prepareAndQueueCompanySourcingOutreach(contact, company, campaign) {
  const sb = getSupabase();
  if (!sb) return;

  // Phase 5 only queues email drafts. LinkedIn-only contacts are handled by:
  //   Phase 6 → sends LinkedIn invite autonomously
  //   Webhook  → sets linkedin_invite_accepted = true, queues LinkedIn DM draft
  if (!contact.email) {
    console.log(`[SOURCING] No email for ${contact.name} — skipping Phase 5 (invite will be sent by Phase 6)`);
    return;
  }
  const messageType = 'email_initial';

  sourcingContactsInFlight.add(contact.id);

  try {
    const draft = await constructCompanySourcingMessage(
      contact, company, campaign.id, messageType, null
    );

    if (!draft) {
      sourcingContactsInFlight.delete(contact.id);
      return;
    }

    // Check for existing pending queue entry to avoid duplicates
    const { data: existing } = await sb.from('approval_queue')
      .select('id')
      .eq('company_contact_id', contact.id)
      .eq('status', 'pending')
      .limit(1);
    if (existing?.length > 0) {
      console.log(`[SOURCING] Skipping duplicate queue entry for ${contact.name}`);
      // Still update pipeline_stage so contact isn't re-processed
      await sb.from('company_contacts').update({ pipeline_stage: 'queued', updated_at: new Date().toISOString() }).eq('id', contact.id);
      return;
    }

    // Queue in approval_queue with sourcing metadata
    const researchBasis = company.intent_signals_found || company.why_matches || `${company.sector} company in ${company.geography}`;

    const { data: queueRow, error: queueErr } = await sb.from('approval_queue').insert([{
      contact_id:         contact.id,
      contact_name:       contact.name,
      firm:               company.company_name,
      stage:              messageType === 'email_initial' ? 'Email' : 'LinkedIn DM',
      score:              company.match_score || 0,
      subject_a:          draft.subject_a || null,
      subject_b:          draft.subject_b || null,
      body:               draft.body,
      research_summary:   researchBasis,
      status:             'pending',
      campaign_id:        campaign.id,
      company_contact_id: contact.id,
      outreach_mode:      'company_sourcing',
      created_at:         new Date().toISOString(),
    }]).select().single();
    if (queueErr) {
      console.warn(`[SOURCING] approval_queue insert failed for ${contact.name}:`, queueErr.message);
    }

    // Send to Telegram for approval (with inline keyboard buttons)
    await sendSourcingDraftToTelegram(contact, company, campaign, draft, researchBasis, queueRow?.id);

    // Update contact pipeline stage
    await sb.from('company_contacts').update({
      pipeline_stage: 'queued',
      updated_at: new Date().toISOString(),
    }).eq('id', contact.id).then(null, () => {});

  } finally {
    sourcingContactsInFlight.delete(contact.id);
  }
}


// ─────────────────────────────────────────────────────────
// EXECUTE APPROVED SENDS
// ─────────────────────────────────────────────────────────

async function executeApprovedLinkedInConnections(campaignId, campaign) {
  const sb = getSupabase();
  if (!sb) return;

  // LinkedIn connection invites go out autonomously (no approval needed).
  // Pick up both 'enriched' (LinkedIn-only contacts) and 'queued' (contacts where email was
  // already queued for approval, so invite goes out simultaneously per requirement G).
  const { data: contacts } = await sb.from('company_contacts')
    .select('*')
    .eq('campaign_id', campaignId)
    .in('pipeline_stage', ['enriched', 'queued'])
    .eq('linkedin_invite_sent', false)
    .not('linkedin_url', 'is', null)
    .in('enrichment_status', ['enriched', 'enriched_apify', 'linkedin_only'])
    .order('created_at', { ascending: true })
    .limit(5);

  for (const contact of (contacts || [])) {
    const { data: company } = await sb.from('target_companies')
      .select('*').eq('id', contact.company_id).single().then(r => r, () => ({ data: null }));

    if (!company || company.meeting_booked || company.firm_responded) continue;
    if (company.match_tier === 'archive' || company.match_tier === 'possible') continue;

    try {
      await sendLinkedInInvite({
        linkedinUrl: contact.linkedin_url,
        message: `Hi ${contact.first_name || contact.name.split(' ')[0]} — I came across ${company.company_name} and wanted to connect. ${campaign.firm_name || 'Our team'} focuses on ${campaign.target_sector} companies — would love to be in your network.`.substring(0, 300),
      });

      await sb.from('company_contacts').update({
        linkedin_invite_sent:    true,
        linkedin_invite_sent_at: new Date().toISOString(),
        pipeline_stage:          'invite_sent',
        updated_at:              new Date().toISOString(),
      }).eq('id', contact.id);

      pushActivity({ type: 'linkedin', action: 'LinkedIn invite sent', note: `[${campaign.name}]: ${contact.name} @ ${company.company_name}` });
      await sleep(3000);
    } catch (err) {
      console.warn(`[SOURCING] LinkedIn invite failed for ${contact.name}:`, err.message);
    }
  }
}

async function executeApprovedEmails(campaignId, campaign) {
  const sb = getSupabase();
  if (!sb) return;

  // Fetch approved email items for this campaign
  const { data: approved } = await sb.from('approval_queue')
    .select('*')
    .eq('campaign_id', campaignId)
    .eq('outreach_mode', 'company_sourcing')
    .in('status', ['approved', 'telegram_approved'])
    .in('stage', ['Email', 'email_initial', 'Follow-Up Email'])
    .limit(10);

  for (const item of (approved || [])) {
    const { data: contact } = await sb.from('company_contacts')
      .select('*').eq('id', item.company_contact_id || item.contact_id).single().then(r => r, () => ({ data: null }));
    if (!contact?.email) continue;

    const { data: company } = await sb.from('target_companies')
      .select('*').eq('id', contact.company_id).single().then(r => r, () => ({ data: null }));
    if (!company || company.meeting_booked || company.firm_responded) continue;

    try {
      const subject = item.approved_subject || item.subject_a || 'Reaching out';
      const body = item.body;

      await unipileSendEmail({
        toEmail:  contact.email,
        subject,
        body,
        fromName: campaign.firm_name || 'Investment Team',
      });

      await sb.from('company_contacts').update({
        pipeline_stage:     'contacted',
        outreach_count:     (contact.outreach_count || 0) + 1,
        last_email_sent_at: new Date().toISOString(),
        email_status:       'sent',
        updated_at:         new Date().toISOString(),
      }).eq('id', contact.id);

      await sb.from('approval_queue').update({ status: 'sent' }).eq('id', item.id).then(null, () => {});

      pushActivity({ type: 'email', action: 'Email sent', note: `[${campaign.name}]: ${contact.name} @ ${company.company_name}` });
      await sleep(2000);
    } catch (err) {
      console.warn(`[SOURCING] Email send failed for ${contact.name}:`, err.message);
    }
  }
}

async function executeApprovedLinkedInDMs(campaignId, campaign) {
  const sb = getSupabase();
  if (!sb) return;

  const { data: approved } = await sb.from('approval_queue')
    .select('*')
    .eq('campaign_id', campaignId)
    .eq('outreach_mode', 'company_sourcing')
    .in('status', ['approved', 'telegram_approved'])
    .in('stage', ['LinkedIn DM', 'linkedin_dm'])
    .limit(5);

  for (const item of (approved || [])) {
    const { data: contact } = await sb.from('company_contacts')
      .select('*').eq('id', item.company_contact_id || item.contact_id).single().then(r => r, () => ({ data: null }));
    if (!contact?.linkedin_url) continue;

    const { data: company } = await sb.from('target_companies')
      .select('*').eq('id', contact.company_id).single().then(r => r, () => ({ data: null }));
    if (!company || company.meeting_booked || company.firm_responded) continue;

    // Only send DM if connection was accepted
    if (!contact.linkedin_invite_accepted && contact.linkedin_invite_sent) {
      continue; // Wait for connection acceptance
    }

    try {
      await sendLinkedInDM({
        attendeeProviderId: contact.linkedin_provider_id || contact.linkedin_url,
        message: item.body,
      });

      await sb.from('company_contacts').update({
        pipeline_stage:          'contacted',
        outreach_count:          (contact.outreach_count || 0) + 1,
        last_linkedin_action_at: new Date().toISOString(),
        linkedin_status:         'dm_sent',
        updated_at:              new Date().toISOString(),
      }).eq('id', contact.id);

      await sb.from('approval_queue').update({ status: 'sent' }).eq('id', item.id).then(null, () => {});

      pushActivity({ type: 'linkedin', action: 'LinkedIn DM sent', note: `[${campaign.name}]: ${contact.name} @ ${company.company_name}` });
      await sleep(3000);
    } catch (err) {
      console.warn(`[SOURCING] LinkedIn DM failed for ${contact.name}:`, err.message);
    }
  }
}

// ─────────────────────────────────────────────────────────
// MEETING BOOKED HANDLER
// Called when an inbound reply indicates a meeting was confirmed
// ─────────────────────────────────────────────────────────

export async function handleMeetingBooked(contactId, companyId, campaignId) {
  const sb = getSupabase();
  if (!sb) return;

  await sb.from('target_companies').update({
    meeting_booked:    true,
    meeting_booked_at: new Date().toISOString(),
    outreach_status:   'meeting_booked',
    updated_at:        new Date().toISOString(),
  }).eq('id', companyId).then(null, () => {});

  await sb.from('company_contacts').update({
    pipeline_stage: 'meeting_booked',
    updated_at:     new Date().toISOString(),
  }).eq('id', contactId).then(null, () => {});

  // Get campaign name for notification
  const { data: campaign } = await sb.from('sourcing_campaigns')
    .select('name').eq('id', campaignId).single().then(r => r, () => ({ data: null }));

  const { data: contact } = await sb.from('company_contacts')
    .select('name').eq('id', contactId).single().then(r => r, () => ({ data: null }));
  const { data: company } = await sb.from('target_companies')
    .select('company_name').eq('id', companyId).single().then(r => r, () => ({ data: null }));

  const campaignName = campaign?.name || 'Unknown Campaign';
  const contactName = contact?.name || 'Unknown Contact';
  const companyName = company?.company_name || 'Unknown Company';

  pushActivity({
    type: 'milestone',
    action: 'Meeting booked!',
    note: `[${campaignName}]: Meeting with ${contactName} at ${companyName}`,
  });

  await sendTelegram(
    `MEETING BOOKED\n\nCampaign: ${campaignName}\nContact: ${contactName} at ${companyName}\n\nAll outreach to ${companyName} has been halted.`
  ).then(null, () => {});

  await sb.from('activity_log').insert({
    event_type: 'MEETING_BOOKED',
    summary: `[${campaignName}]: Meeting booked with ${contactName} at ${companyName}`,
    created_at: new Date().toISOString(),
  }).then(null, () => {});
}

// ─────────────────────────────────────────────────────────
// FOLLOW-UPS
// Maximum 2 touches per channel per contact
// ─────────────────────────────────────────────────────────

export async function runCompanySourcingFollowUps(campaign) {
  const sb = getSupabase();
  if (!sb) return;

  // Find contacts that were contacted 3+ days ago with no reply, and have follow_up_count < 1
  const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString();

  const { data: contacts } = await sb.from('company_contacts')
    .select('*, target_companies!inner(meeting_booked, firm_responded, outreach_status, match_tier, company_name)')
    .eq('campaign_id', campaign.id)
    .eq('pipeline_stage', 'contacted')
    .eq('response_received', false)
    .lt('follow_up_count', 1)
    .lt('last_email_sent_at', threeDaysAgo)
    .limit(5);

  for (const contact of (contacts || [])) {
    const company = contact.target_companies;
    if (!company || company.meeting_booked || company.firm_responded) continue;
    if (company.match_tier === 'archive' || company.match_tier === 'possible') continue;

    try {
      const messageType = contact.email ? 'email_follow_up' : 'linkedin_follow_up';
      const draft = await constructCompanySourcingMessage(contact, {
        id:                  contact.company_id,
        company_name:        company.company_name,
        product_description: null,
        estimated_revenue:   null,
        intent_signals_found: null,
        why_matches:         null,
        sector:              campaign.target_sector,
        geography:           campaign.target_geography,
        match_score:         null,
      }, campaign.id, messageType, null);

      if (!draft) continue;

      const researchBasis = `Follow-up for ${company.company_name}`;

      await sb.from('approval_queue').insert([{
        contact_id:         contact.id,
        contact_name:       contact.name,
        firm:               company.company_name,
        stage:              messageType === 'email_follow_up' ? 'Follow-Up Email' : 'LinkedIn Follow-up',
        score:              0,
        subject_a:          draft.subject_a || null,
        subject_b:          draft.subject_b || null,
        body:               draft.body,
        research_summary:   researchBasis,
        status:             'pending',
        campaign_id:        campaign.id,
        company_contact_id: contact.id,
        outreach_mode:      'company_sourcing',
        created_at:         new Date().toISOString(),
      }]).then(null, () => {});

      await sendSourcingDraftToTelegram(
        contact,
        { id: contact.company_id, company_name: company.company_name, match_score: 0, match_tier: 'warm', intent_signals_found: null, why_matches: null },
        campaign,
        draft,
        researchBasis,
        null
      );

      await sb.from('company_contacts').update({
        follow_up_count: (contact.follow_up_count || 0) + 1,
        updated_at:      new Date().toISOString(),
      }).eq('id', contact.id).then(null, () => {});

      await sleep(1000);
    } catch (err) {
      console.warn(`[SOURCING] Follow-up failed for ${contact.name}:`, err.message);
    }
  }

  // Mark contacts with 2+ follow-ups and no reply as exhausted
  const { data: exhausted } = await sb.from('company_contacts')
    .select('id, company_id')
    .eq('campaign_id', campaign.id)
    .eq('pipeline_stage', 'contacted')
    .eq('response_received', false)
    .gte('follow_up_count', 1)
    .gte('outreach_count', 1)
    .limit(20);

  for (const c of (exhausted || [])) {
    await sb.from('company_contacts').update({
      pipeline_stage: 'exhausted',
      updated_at:     new Date().toISOString(),
    }).eq('id', c.id).then(null, () => {});
  }
}
