/**
 * core/jarvisTools.js
 * Tool definitions (Claude tool_use schema) + executor functions for JARVIS.
 * Each tool wraps an existing Roco module — JARVIS calls these, not raw modules.
 *
 * When OpenRouter is added, swap the model routing in jarvis.js — tools stay the same.
 */

import { getSupabase } from './supabase.js';
import { writeMemory } from './jarvisMemory.js';

// ─────────────────────────────────────────────────────────────────────────────
// ORCHESTRATOR EVENT FEED
// Orchestrator pushes events here via notifyJarvis() (dynamic import in orchestrator.js).
// JARVIS reads these in buildSystemPrompt to know what the orchestrator just did.
// ─────────────────────────────────────────────────────────────────────────────

const orchestratorEvents = [];
const MAX_EVENTS = 20;

export function pushOrchestratorEvent(event) {
  orchestratorEvents.unshift({ ...event, ts: new Date().toISOString() });
  if (orchestratorEvents.length > MAX_EVENTS) orchestratorEvents.length = MAX_EVENTS;
}

export function getOrchestratorEvents(limit = 8) {
  return orchestratorEvents.slice(0, limit);
}

// ─────────────────────────────────────────────────────────────────────────────
// TOOL SCHEMAS  (passed to Claude's `tools` array)
// ─────────────────────────────────────────────────────────────────────────────

export const JARVIS_TOOLS = [
  {
    name: 'get_status',
    description: 'Get current pipeline health and metrics for a deal. Returns emails sent, response rate, LinkedIn stats, pending approvals, active firms, and meetings booked.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string', description: 'The deal UUID' },
      },
      required: ['deal_id'],
    },
  },
  {
    name: 'get_pending_approvals',
    description: 'List all messages currently waiting for approval (emails and LinkedIn DMs). Returns contact name, firm, channel, and a message preview.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string', description: 'The deal UUID' },
      },
      required: ['deal_id'],
    },
  },
  {
    name: 'approve_message',
    description: 'Approve a pending email or LinkedIn DM by contact name. Finds the pending approval and fires it immediately.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:      { type: 'string', description: 'The deal UUID' },
        contact_name: { type: 'string', description: 'Name of the contact whose message to approve (partial match ok)' },
      },
      required: ['deal_id', 'contact_name'],
    },
  },
  {
    name: 'skip_message',
    description: 'Skip (dismiss) a pending approval for a contact.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:      { type: 'string', description: 'The deal UUID' },
        contact_name: { type: 'string', description: 'Name of the contact whose message to skip' },
      },
      required: ['deal_id', 'contact_name'],
    },
  },
  {
    name: 'get_recent_activity',
    description: 'Get the most recent activity events for a deal — emails sent, DMs sent, replies received, invites accepted, etc.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string', description: 'The deal UUID' },
        limit:   { type: 'number', description: 'How many events to return (default 15, max 40)' },
      },
      required: ['deal_id'],
    },
  },
  {
    name: 'control_module',
    description: 'Pause or resume any part of the outreach system: outreach, enrichment, research, linkedin, or followups. Also supports a timed pause until a specific ISO date.',
    input_schema: {
      type: 'object',
      properties: {
        module: {
          type: 'string',
          enum: ['outreach', 'enrichment', 'research', 'linkedin', 'followups', 'all'],
          description: 'Which module to control',
        },
        action: {
          type: 'string',
          enum: ['pause', 'resume'],
          description: 'Whether to pause or resume',
        },
        until: {
          type: 'string',
          description: 'Optional ISO datetime to pause until (e.g. "2026-04-28T09:00:00Z"). Only used with action=pause.',
        },
      },
      required: ['module', 'action'],
    },
  },
  {
    name: 'trigger_research',
    description: 'Kick off an immediate research and enrichment cycle for the deal. Finds new firms, enriches contacts, and loads them into the pipeline. The orchestrator then handles outreach on the next cycle.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string', description: 'The deal UUID' },
      },
      required: ['deal_id'],
    },
  },
  {
    name: 'search_pipeline',
    description: 'Search for a firm or person in the pipeline. Returns their current stage, contact details, and recent activity.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string', description: 'The deal UUID' },
        query:   { type: 'string', description: 'Name of the firm or person to search for' },
      },
      required: ['deal_id', 'query'],
    },
  },
  {
    name: 'get_contacts_by_stage',
    description: 'List contacts at a specific pipeline stage — e.g. "invite_sent", "invite_accepted", "Email Sent", "Replied", "Meeting Booked".',
    input_schema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string', description: 'The deal UUID' },
        stage:   { type: 'string', description: 'Pipeline stage to filter by' },
        limit:   { type: 'number', description: 'Max contacts to return (default 20)' },
      },
      required: ['deal_id', 'stage'],
    },
  },
  {
    name: 'suppress_firm',
    description: 'Suppress a firm from future outreach for one deal or all deals. Use when Dom says to stop contacting a firm, blacklist a firm, or suppress a company. If scope is unclear, ask one confirmation question before using all_deals.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:   { type: 'string', description: 'The deal UUID' },
        firm_name: { type: 'string', description: 'Name of the firm to suppress' },
        reason:    { type: 'string', description: 'Why this firm is being suppressed' },
        scope:     { type: 'string', enum: ['deal', 'all_deals'], description: 'deal suppresses only the active deal. all_deals suppresses this firm globally across current/future outreach.' },
      },
      required: ['deal_id', 'firm_name', 'reason'],
    },
  },
  {
    name: 'web_search',
    description: 'Search the web for real-time information about a firm, investor, recent fund closes, partner moves, or any topic. Use this to look up someone before outreach, check recent news about a firm, or research anything not in the pipeline database.',
    input_schema: {
      type: 'object',
      properties: {
        query:       { type: 'string', description: 'What to search for (e.g. "Greenvolt Capital fund close 2026", "Sarah Chen VC partner background")' },
        num_results: { type: 'number', description: 'Number of results (default 5, max 8)' },
      },
      required: ['query'],
    },
  },
  {
    name: 'get_contacts_breakdown',
    description: 'Get a detailed breakdown of contacts in the pipeline grouped by stage. Shows how many contacts are at each stage (invite_sent, invite_accepted, Email Sent, Replied, etc.) for a deal.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string', description: 'The deal UUID' },
      },
      required: ['deal_id'],
    },
  },
  {
    name: 'update_contact',
    description: 'Update a contact\'s pipeline/campaign stage, tier, notes, email, or LinkedIn URL. Works on investor pipeline contacts first, then sourcing campaign contacts. Use this when Dom tells you someone replied, moved Kanban stages, became exhausted, gave new info, or should be re-classified. Fuzzy name match — partial names work.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:        { type: 'string', description: 'The deal UUID' },
        contact_name:   { type: 'string', description: 'Name or partial name of the contact to update' },
        pipeline_stage: { type: 'string', description: 'New pipeline stage: Researched, invite_sent, invite_accepted, Email Sent, Replied, Meeting Booked, Archived' },
        tier:           { type: 'string', description: 'Contact tier: hot, warm, possible, archive' },
        notes:          { type: 'string', description: 'Notes to append (added to existing notes, not overwritten)' },
        email:          { type: 'string', description: 'Update or set email address' },
        linkedin_url:   { type: 'string', description: 'Update or set LinkedIn profile URL' },
      },
      required: ['deal_id', 'contact_name'],
    },
  },
  {
    name: 'record_reply',
    description: 'Record that a contact replied — what they said, their intent, and any action to take. Moves them to the Replied stage and saves the message. Use when Dom pastes or summarises a reply.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:      { type: 'string', description: 'The deal UUID' },
        contact_name: { type: 'string', description: 'Name or partial name of the contact who replied' },
        message:      { type: 'string', description: 'The reply content or summary of what they said' },
        intent:       { type: 'string', description: 'Intent classification: interested, not_interested, more_info, meeting_request, follow_up_later, unsubscribe, other' },
        action_note:  { type: 'string', description: 'Optional: what Dom wants to do next (e.g. "follow up in 2 weeks", "book a call")' },
      },
      required: ['deal_id', 'contact_name', 'message'],
    },
  },
  {
    name: 'add_intelligence',
    description: 'Store a piece of intelligence or observation Dom shares — about a firm, a contact, the market, or the deal strategy. Saved to JARVIS memory so it informs future decisions.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string', description: 'The deal UUID (optional — omit for general intelligence)' },
        subject: { type: 'string', description: 'What this intelligence is about (e.g. "Blackstone Capital", "Meeting with Sarah Chen", "Market signal")' },
        content: { type: 'string', description: 'The intelligence itself — what Dom observed, learned, or wants remembered' },
        tags:    { type: 'array', items: { type: 'string' }, description: 'Optional tags for retrieval, e.g. ["firm:blackstone", "hot_lead"]' },
      },
      required: ['subject', 'content'],
    },
  },
  {
    name: 'update_deal',
    description: 'Update deal-level information — EBITDA, equity target, sector, geography, investor profile, or any settings. Use when Dom provides new deal details.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:          { type: 'string', description: 'The deal UUID' },
        sector:           { type: 'string', description: 'Deal sector' },
        geography:        { type: 'string', description: 'Target geography' },
        target_amount:    { type: 'number', description: 'Total raise target in USD/GBP' },
        ebitda_usd_m:     { type: 'number', description: 'EBITDA in millions' },
        equity_required_usd_m: { type: 'number', description: 'Equity required in millions' },
        investor_profile: { type: 'string', description: 'Description of ideal investor' },
        description:      { type: 'string', description: 'Updated deal description' },
      },
      required: ['deal_id'],
    },
  },
  {
    name: 'enrich_contact',
    description: 'Enrich a contact with LinkedIn data via Apify. Finds their LinkedIn URL, job title, company details, and connection info.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:      { type: 'string', description: 'The deal UUID' },
        contact_name: { type: 'string', description: 'Name or partial name of the contact to enrich' },
      },
      required: ['deal_id', 'contact_name'],
    },
  },
  {
    name: 'find_decision_makers',
    description: 'Find decision makers at a specific firm using LinkedIn search via Unipile. Returns people with relevant titles (Partner, MD, Principal, etc.).',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:   { type: 'string', description: 'The deal UUID' },
        firm_name: { type: 'string', description: 'Name of the firm to find decision makers at' },
      },
      required: ['deal_id', 'firm_name'],
    },
  },
  {
    name: 'search_linkedin_people',
    description: 'Search LinkedIn for people by keyword, company, or role. Useful for finding contacts at specific firms not yet in the pipeline.',
    input_schema: {
      type: 'object',
      properties: {
        keywords: { type: 'string', description: 'Search keywords (e.g. "Partner Sequoia Capital", "Managing Director infrastructure fund")' },
        limit:    { type: 'number', description: 'Max results to return (default 10)' },
      },
      required: ['keywords'],
    },
  },
  {
    name: 'send_linkedin_dm',
    description: 'Send a LinkedIn DM to a specific contact already connected. Only use for contacts with invite_accepted_at set.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:      { type: 'string', description: 'The deal UUID' },
        contact_name: { type: 'string', description: 'Name or partial name of the connected contact' },
        message:      { type: 'string', description: 'The DM message to send' },
      },
      required: ['deal_id', 'contact_name', 'message'],
    },
  },
  {
    name: 'send_email',
    description: 'Send an email to a specific contact. Constructs and sends via Unipile Outlook account. Always show the draft to user before sending unless explicitly told to send immediately.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:      { type: 'string', description: 'The deal UUID' },
        contact_name: { type: 'string', description: 'Name or partial name of the contact to email' },
        subject:      { type: 'string', description: 'Email subject line' },
        body:         { type: 'string', description: 'Full email body text' },
      },
      required: ['deal_id', 'contact_name', 'subject', 'body'],
    },
  },
  {
    name: 'add_contact_to_pipeline',
    description: 'Add a specific person to the pipeline database so the orchestrator enriches and reaches out to them. Use after finding someone via search or when Dom mentions a specific person/firm to target. The orchestrator will automatically enrich, find their LinkedIn, and draft outreach on the next cycle.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:      { type: 'string', description: 'The deal UUID' },
        name:         { type: 'string', description: 'Full name of the person' },
        company_name: { type: 'string', description: 'Their firm or company name' },
        job_title:    { type: 'string', description: 'Their job title (Partner, MD, Principal, etc.)' },
        linkedin_url: { type: 'string', description: 'LinkedIn profile URL if known' },
        email:        { type: 'string', description: 'Email address if known' },
        notes:        { type: 'string', description: 'Why this person is a target, any intelligence about them' },
        tier:         { type: 'string', description: 'Priority tier: hot, warm, possible (default: warm)' },
      },
      required: ['deal_id', 'name', 'company_name'],
    },
  },
  {
    name: 'get_pipeline_contacts',
    description: 'Get contacts in a specific pipeline state or with specific properties. Use to find who has replied, who is warm, who needs follow-up.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string', description: 'The deal UUID' },
        filter:  {
          type: 'string',
          enum: ['replied', 'warm', 'accepted_no_dm', 'meetings', 'all_active'],
          description: 'Filter: replied=has last_reply_at, warm=hot/warm tier, accepted_no_dm=connected but no DM yet, meetings=has meeting booked, all_active=exclude archived',
        },
        limit: { type: 'number', description: 'Max contacts to return (default 20)' },
      },
      required: ['deal_id', 'filter'],
    },
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTORS
// ─────────────────────────────────────────────────────────────────────────────

async function emitJarvisActivity(action, note, dealId = null, deal_name = null) {
  try {
    const { pushActivity } = await import('../dashboard/server.js');
    pushActivity({ type: 'jarvis', action, note, dealId, deal_name });
  } catch {}
}

export async function executeTool(name, input) {
  switch (name) {
    case 'get_status':              return toolGetStatus(input);
    case 'get_pending_approvals':   return toolGetPendingApprovals(input);
    case 'approve_message':         return toolApproveMessage(input);
    case 'skip_message':            return toolSkipMessage(input);
    case 'get_recent_activity':     return toolGetRecentActivity(input);
    case 'control_module':          return toolControlModule(input);
    case 'trigger_research':        return toolTriggerResearch(input);
    case 'search_pipeline':         return toolSearchPipeline(input);
    case 'get_contacts_by_stage':   return toolGetContactsByStage(input);
    case 'suppress_firm':           return toolSuppressFirm(input);
    case 'web_search':              return toolWebSearch(input);
    case 'get_contacts_breakdown':  return toolGetContactsBreakdown(input);
    case 'update_contact':          return toolUpdateContact(input);
    case 'record_reply':            return toolRecordReply(input);
    case 'add_intelligence':        return toolAddIntelligence(input);
    case 'update_deal':             return toolUpdateDeal(input);
    case 'enrich_contact':          return toolEnrichContact(input);
    case 'find_decision_makers':    return toolFindDecisionMakers(input);
    case 'search_linkedin_people':  return toolSearchLinkedInPeople(input);
    case 'send_linkedin_dm':        return toolSendLinkedInDM(input);
    case 'send_email':              return toolSendEmail(input);
    case 'add_contact_to_pipeline': return toolAddContactToPipeline(input);
    case 'get_pipeline_contacts':   return toolGetPipelineContacts(input);
    default:
      return { error: `Unknown tool: ${name}` };
  }
}

// ── get_status ────────────────────────────────────────────────────────────────
async function toolGetStatus({ deal_id }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const safeCount = async (query) => {
      try { const r = await query; return r.count || 0; } catch { return 0; }
    };

    const [emails, dms, invites, accepted, replies, meetings, pendingApprovals, pendingDmApprovals, activeContacts] = await Promise.all([
      safeCount(sb.from('contacts').select('id', { count: 'exact', head: true }).eq('deal_id', deal_id).not('last_email_sent_at', 'is', null)),
      safeCount(sb.from('contacts').select('id', { count: 'exact', head: true }).eq('deal_id', deal_id).not('dm_sent_at', 'is', null)),
      safeCount(sb.from('contacts').select('id', { count: 'exact', head: true }).eq('deal_id', deal_id).not('invite_sent_at', 'is', null)),
      safeCount(sb.from('contacts').select('id', { count: 'exact', head: true }).eq('deal_id', deal_id).not('invite_accepted_at', 'is', null)),
      safeCount(sb.from('contacts').select('id', { count: 'exact', head: true }).eq('deal_id', deal_id).not('last_reply_at', 'is', null)),
      safeCount(sb.from('contacts').select('id', { count: 'exact', head: true }).eq('deal_id', deal_id).not('last_meeting_date', 'is', null)),
      safeCount(sb.from('approval_queue').select('id', { count: 'exact', head: true }).eq('deal_id', deal_id).eq('status', 'pending')),
      safeCount(sb.from('contacts').select('id', { count: 'exact', head: true }).eq('deal_id', deal_id).eq('pipeline_stage', 'pending_dm_approval')),
      safeCount(sb.from('contacts').select('id', { count: 'exact', head: true }).eq('deal_id', deal_id).not('pipeline_stage', 'in', '("Archived","ARCHIVED","archived","Skipped","skipped_no_name","skipped_no_linkedin","skipped_duplicate_email","Inactive","Suppressed — Opt Out","Deleted — Do Not Contact")')),
    ]);

    const invitesPending = invites - accepted;
    const responseRate = (emails + dms) > 0 ? Math.round((replies / (emails + dms)) * 100) : 0;

    return {
      emails_sent_total:    emails,
      dms_sent_total:       dms,
      invites_sent:         invites,
      invites_accepted:     accepted,
      invites_pending:      Math.max(0, invitesPending),
      total_replies:        replies,
      meetings_booked:      meetings,
      active_firms:         activeContacts,
      pending_approvals:    pendingApprovals,
      pending_dm_approvals: pendingDmApprovals,
      response_rate_pct:    responseRate,
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── get_pending_approvals ─────────────────────────────────────────────────────
async function toolGetPendingApprovals({ deal_id }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const { data } = await sb.from('approval_queue')
      .select('id, contact_name, firm, stage, channel, body, created_at')
      .eq('deal_id', deal_id)
      .in('status', ['pending', 'approved_waiting_for_window'])
      .order('created_at', { ascending: true })
      .limit(20);

    return {
      count: (data || []).length,
      items: (data || []).map(item => ({
        id:           item.id,
        contact:      item.contact_name || 'Unknown',
        firm:         item.firm || '',
        channel:      item.channel || item.stage || '',
        preview:      (item.body || '').slice(0, 120),
        queued_at:    item.created_at,
      })),
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── approve_message ───────────────────────────────────────────────────────────
async function toolApproveMessage({ deal_id, contact_name }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const { data: items } = await sb.from('approval_queue')
      .select('id, contact_id, contact_name, firm, stage, body, edited_body, subject_a, subject, outreach_mode, channel')
      .eq('deal_id', deal_id)
      .in('status', ['pending', 'approved_waiting_for_window'])
      .ilike('contact_name', `%${contact_name.split(' ')[0]}%`)
      .order('created_at', { ascending: true })
      .limit(5);

    if (!items?.length) {
      return { error: `No pending approval found for "${contact_name}"` };
    }

    const item = items[0];
    const { executeApprovalById } = await import('./jarvisApprovalBridge.js');
    await executeApprovalById(item);

    await writeMemory(deal_id, {
      type:    'ACTION',
      subject: `Approved message to ${item.contact_name}`,
      content: `JARVIS approved ${item.stage || item.channel || 'message'} to ${item.contact_name} at ${item.firm || 'unknown firm'}.`,
      tags:    [`contact:${(item.contact_name || '').toLowerCase().replace(/\s+/g, '_')}`, `firm:${(item.firm || '').toLowerCase().replace(/\s+/g, '_')}`],
    });

    emitJarvisActivity(
      `Approved message to ${item.contact_name}`,
      `${item.firm || ''} · ${item.stage || item.channel || 'message'}`,
      deal_id
    ).catch(() => {});
    return { approved: true, contact: item.contact_name, firm: item.firm, channel: item.stage || item.channel };
  } catch (err) {
    return { error: err.message };
  }
}

// ── skip_message ──────────────────────────────────────────────────────────────
async function toolSkipMessage({ deal_id, contact_name }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const { data: items } = await sb.from('approval_queue')
      .select('id, contact_name, firm, stage')
      .eq('deal_id', deal_id)
      .in('status', ['pending', 'approved_waiting_for_window'])
      .ilike('contact_name', `%${contact_name.split(' ')[0]}%`)
      .limit(3);

    if (!items?.length) {
      return { error: `No pending approval found for "${contact_name}"` };
    }

    const item = items[0];
    await sb.from('approval_queue').update({
      status:      'skipped',
      resolved_at: new Date().toISOString(),
    }).eq('id', item.id);

    return { skipped: true, contact: item.contact_name, firm: item.firm };
  } catch (err) {
    return { error: err.message };
  }
}

// ── get_recent_activity ───────────────────────────────────────────────────────
async function toolGetRecentActivity({ deal_id, limit = 15 }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  const cap = Math.min(limit, 40);
  const events = [];

  try {
    // 1. activity_log (may be empty — don't rely on it alone)
    const { data: logRows } = await sb.from('activity_log')
      .select('event_type, summary, detail, created_at')
      .eq('deal_id', deal_id)
      .order('created_at', { ascending: false })
      .limit(cap);
    for (const e of logRows || []) {
      events.push({ type: e.event_type, summary: e.summary, when: e.created_at, source: 'log' });
    }

    // 2. Inbound replies from conversation_messages
    const { data: replies } = await sb.from('conversation_messages')
      .select('contact_name, body, sent_at, intent, channel')
      .eq('deal_id', deal_id)
      .eq('direction', 'inbound')
      .order('sent_at', { ascending: false })
      .limit(cap);
    for (const r of replies || []) {
      events.push({
        type:    'reply_received',
        summary: `${r.contact_name || 'Unknown'} replied${r.intent && r.intent !== 'other' ? ` (${r.intent})` : ''}: "${(r.body || '').slice(0, 120)}"`,
        when:    r.sent_at,
        source:  'messages',
      });
    }

    // 3. Recent contact stage milestones (last 7 days)
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    const { data: recentContacts } = await sb.from('contacts')
      .select('name, company_name, pipeline_stage, last_reply_at, last_email_sent_at, meeting_booked_at, invite_accepted_at, dm_sent_at, updated_at')
      .eq('deal_id', deal_id)
      .gte('updated_at', sevenDaysAgo)
      .order('updated_at', { ascending: false })
      .limit(30);

    for (const c of recentContacts || []) {
      const label = `${c.name}${c.company_name ? ` at ${c.company_name}` : ''}`;
      if (c.meeting_booked_at && c.meeting_booked_at >= sevenDaysAgo) {
        events.push({ type: 'meeting_booked', summary: `Meeting booked with ${label}`, when: c.meeting_booked_at, source: 'contacts' });
      }
      if (c.last_reply_at && c.last_reply_at >= sevenDaysAgo) {
        events.push({ type: 'reply_received', summary: `${label} replied [stage: ${c.pipeline_stage || '?'}]`, when: c.last_reply_at, source: 'contacts' });
      }
      if (c.dm_sent_at && c.dm_sent_at >= sevenDaysAgo) {
        events.push({ type: 'dm_sent', summary: `LinkedIn DM sent to ${label}`, when: c.dm_sent_at, source: 'contacts' });
      }
      if (c.invite_accepted_at && c.invite_accepted_at >= sevenDaysAgo) {
        events.push({ type: 'invite_accepted', summary: `${label} accepted LinkedIn invite`, when: c.invite_accepted_at, source: 'contacts' });
      }
      if (c.last_email_sent_at && c.last_email_sent_at >= sevenDaysAgo) {
        events.push({ type: 'email_sent', summary: `Email sent to ${label}`, when: c.last_email_sent_at, source: 'contacts' });
      }
    }

    // Deduplicate and sort newest first
    const seen = new Set();
    const deduped = events
      .filter(e => {
        const key = `${e.type}:${e.summary.slice(0, 60)}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .sort((a, b) => (b.when || '').localeCompare(a.when || ''))
      .slice(0, cap);

    // Summary counts
    const repliesTotal = (replies || []).length;
    const meetingsTotal = (recentContacts || []).filter(c => c.meeting_booked_at).length;

    return {
      total_events:   deduped.length,
      replies_7d:     repliesTotal,
      meetings_7d:    meetingsTotal,
      events: deduped.map(e => ({ type: e.type, summary: e.summary, when: e.when })),
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── control_module ────────────────────────────────────────────────────────────
async function toolControlModule({ module, action, until }) {
  try {
    const { loadSessionState, saveSessionState } = await import('./supabaseSync.js');
    const state = await loadSessionState();

    const pausing = action === 'pause';

    if (module === 'all') {
      state.rocoStatus = pausing ? 'PAUSED' : 'ACTIVE';
    } else {
      const keyMap = {
        outreach:    'outreachEnabled',
        enrichment:  'enrichmentEnabled',
        research:    'researchEnabled',
        linkedin:    'linkedinEnabled',
        followups:   'followupEnabled',
      };
      const key = keyMap[module];
      if (!key) return { error: `Unknown module: ${module}` };
      state[key] = !pausing;
    }

    if (pausing && until) {
      state.outreachPausedUntil = until;
    } else if (!pausing) {
      state.outreachPausedUntil = null;
    }

    await saveSessionState(state);

    emitJarvisActivity(
      `${pausing ? 'Paused' : 'Resumed'} ${module}`,
      `JARVIS ${pausing ? 'paused' : 'resumed'} ${module}${until ? ` until ${until}` : ''}`,
      null
    ).catch(() => {});

    return {
      module,
      action,
      until: until || null,
      status: `${module} is now ${pausing ? 'paused' : 'active'}`,
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── trigger_research ──────────────────────────────────────────────────────────
async function toolTriggerResearch({ deal_id }) {
  try {
    // If no deal_id provided, find the first active deal from the database
    let resolvedDealId = deal_id;
    if (!resolvedDealId) {
      const sb = getSupabase();
      if (sb) {
        const { data } = await sb.from('deals').select('id, name').eq('status', 'ACTIVE').limit(1);
        resolvedDealId = data?.[0]?.id || null;
      }
    }
    if (!resolvedDealId) return { error: 'No active deal found. Please specify a deal.' };

    const { runJarvisResearch, triggerImmediateRun } = await import('./orchestrator.js');

    // Run direct firm discovery (bypasses batch cooldown)
    const result = await runJarvisResearch(resolvedDealId, 15);

    // Also kick off a full cycle for enrichment/outreach
    triggerImmediateRun(resolvedDealId).catch(() => {});

    const msg = result.error
      ? `Research cycle triggered (${result.error})`
      : result.added > 0
        ? `Found ${result.added} new firms, queued for enrichment and outreach.`
        : 'Research ran — no new firms found this pass. Pipeline may already be full or all candidates are excluded.';

    emitJarvisActivity('Research cycle triggered', msg, resolvedDealId).catch(() => {});
    return { triggered: true, firms_added: result.added || 0, message: msg };
  } catch (err) {
    return { error: err.message };
  }
}

// ── search_pipeline ───────────────────────────────────────────────────────────
async function toolSearchPipeline({ deal_id, query }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const q = query.trim();
    const [{ data: contacts }, { data: firms }] = await Promise.all([
      sb.from('contacts')
        .select('id, name, company_name, pipeline_stage, email, last_outreach_at, enrichment_status')
        .eq('deal_id', deal_id)
        .or(`name.ilike.%${q}%,company_name.ilike.%${q}%`)
        .limit(5),
      sb.from('firm_outreach_state')
        .select('firm_name, status, last_contacted_at, notes')
        .eq('deal_id', deal_id)
        .ilike('firm_name', `%${q}%`)
        .limit(5),
    ]);

    return {
      contacts: (contacts || []).map(c => ({
        name:          c.name,
        firm:          c.company_name,
        stage:         c.pipeline_stage,
        email:         c.email ? '✓' : '✗',
        last_outreach: c.last_outreach_at,
      })),
      firms: (firms || []).map(f => ({
        name:           f.firm_name,
        status:         f.status,
        last_contacted: f.last_contacted_at,
        notes:          f.notes,
      })),
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── get_contacts_by_stage ─────────────────────────────────────────────────────
async function toolGetContactsByStage({ deal_id, stage, limit = 20 }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const { data } = await sb.from('contacts')
      .select('name, company_name, email, pipeline_stage, last_outreach_at, linkedin_url')
      .eq('deal_id', deal_id)
      .ilike('pipeline_stage', `%${stage}%`)
      .order('last_outreach_at', { ascending: false })
      .limit(Math.min(limit, 50));

    return {
      stage,
      count: (data || []).length,
      contacts: (data || []).map(c => ({
        name:          c.name,
        firm:          c.company_name,
        has_email:     !!c.email,
        has_linkedin:  !!c.linkedin_url,
        last_outreach: c.last_outreach_at,
      })),
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── web_search ────────────────────────────────────────────────────────────────
async function toolWebSearch({ query, num_results = 5 }) {
  try {
    const { orComplete } = await import('./openRouterClient.js');
    const result = await orComplete(query, { tier: 'web', maxTokens: Math.min((num_results || 5) * 300, 1500) });
    if (!result) return { message: 'No results returned.', results: [] };
    return { results: [{ snippet: result, title: 'Web Research', source: 'perplexity', url: '' }] };
  } catch (err) {
    return { error: `Web search failed: ${err.message}` };
  }
}

// ── get_contacts_breakdown ────────────────────────────────────────────────────
async function toolGetContactsBreakdown({ deal_id }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const { data } = await sb.from('contacts')
      .select('pipeline_stage, tier')
      .eq('deal_id', deal_id);

    const stageCounts = {};
    const tierCounts = { hot: 0, warm: 0, possible: 0, archive: 0, other: 0 };

    for (const c of data || []) {
      const stage = c.pipeline_stage || 'unknown';
      stageCounts[stage] = (stageCounts[stage] || 0) + 1;
      const tier = c.tier?.toLowerCase();
      if (tier && tierCounts[tier] !== undefined) tierCounts[tier]++;
      else tierCounts.other++;
    }

    const sortedStages = Object.entries(stageCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([stage, count]) => ({ stage, count }));

    return {
      total: (data || []).length,
      by_stage: sortedStages,
      by_tier: tierCounts,
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── suppress_firm ─────────────────────────────────────────────────────────────
async function toolSuppressFirm({ deal_id, firm_name, reason, scope = 'deal' }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const now = new Date().toISOString();
    const global = scope === 'all_deals';
    const firmToken = firm_name.split(/\s+/).filter(Boolean)[0] || firm_name;

    if (!global) {
      await sb.from('firm_outreach_state').upsert({
        deal_id,
        firm_name,
        status:     'suppressed',
        notes:      reason,
        updated_at: now,
      }, { onConflict: 'deal_id,firm_name' }).catch(() => {});
    }

    let contactsQuery = sb.from('contacts')
      .update({
        tier: 'archive',
        pipeline_stage: 'Archived',
        conversation_state: 'do_not_contact',
        conversation_ended_reason: `Suppressed by JARVIS: ${reason}`,
        updated_at: now,
      })
      .ilike('company_name', `%${firmToken}%`);
    if (!global) contactsQuery = contactsQuery.eq('deal_id', deal_id);
    const contactsResult = await contactsQuery.select('id').catch(() => ({ data: [] }));
    const contactsSuppressed = contactsResult?.data?.length || 0;

    let batchQuery = sb.from('batch_firms')
      .update({ status: 'suppressed', notes: `Suppressed by JARVIS: ${reason}` })
      .ilike('firm_name', `%${firmToken}%`);
    if (!global) batchQuery = batchQuery.eq('deal_id', deal_id);
    await batchQuery.catch(() => {});

    if (!global) {
      await sb.from('firm_suppressions').insert({
        deal_id,
        company_name: firm_name,
        triggered_by_contact: 'JARVIS',
        reason,
        contacts_suppressed: contactsSuppressed,
        suppression_type: 'JARVIS',
        created_at: now,
      }).catch(() => {});

      await sb.from('deal_exclusions').insert({
        deal_id,
        firm_name: firm_name.toLowerCase().trim(),
        added_by: 'JARVIS',
      }).catch(() => {});
    } else {
      const { data: activeDeals } = await sb.from('deals').select('id').eq('status', 'ACTIVE').catch(() => ({ data: [] }));
      for (const deal of activeDeals || []) {
        await sb.from('firm_outreach_state').upsert({
          deal_id: deal.id,
          firm_name,
          status: 'suppressed',
          notes: `Global JARVIS suppression: ${reason}`,
          updated_at: now,
        }, { onConflict: 'deal_id,firm_name' }).catch(() => {});
        await sb.from('deal_exclusions').insert({
          deal_id: deal.id,
          firm_name: firm_name.toLowerCase().trim(),
          added_by: 'JARVIS',
        }).catch(() => {});
      }
    }

    await writeMemory(deal_id, {
      type:    'DECISION',
      subject: `Suppressed ${firm_name}`,
      content: `${firm_name} suppressed from ${global ? 'all deals' : 'this deal'}. Reason: ${reason}`,
      tags:    [`firm:${firm_name.toLowerCase().replace(/\s+/g, '_')}`],
      metadata: { reason, scope: global ? 'all_deals' : 'deal', suppressed_at: now },
    });

    emitJarvisActivity(
      `Suppressed firm: ${firm_name}`,
      `${global ? 'All deals' : 'Active deal'} · ${reason}`,
      deal_id
    ).catch(() => {});

    return { suppressed: true, firm: firm_name, scope: global ? 'all_deals' : 'deal', contacts_suppressed: contactsSuppressed };
  } catch (err) {
    return { error: err.message };
  }
}

// ── update_contact ────────────────────────────────────────────────────────────
async function toolUpdateContact({ deal_id, contact_name, pipeline_stage, tier, notes, email, linkedin_url }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    // Fuzzy match on name
    const { data: matches } = await sb.from('contacts')
      .select('id, name, company_name, pipeline_stage, tier, notes, email, linkedin_url')
      .eq('deal_id', deal_id)
      .ilike('name', `%${contact_name.split(' ')[0]}%`)
      .limit(5);

    if (!matches?.length) {
      const { data: campaignMatches } = await sb.from('company_contacts')
        .select('id, name, company_name, title, pipeline_stage, email, linkedin_url, notes, campaign_id, sourcing_campaigns(name)')
        .ilike('name', `%${contact_name.split(' ')[0]}%`)
        .limit(5)
        .catch(() => ({ data: [] }));

      if (!campaignMatches?.length) return { error: `No contact found matching "${contact_name}"` };

      const campaignContact = campaignMatches.find(c =>
        String(c.name || '').toLowerCase().startsWith(contact_name.toLowerCase().split(' ')[0])
      ) || campaignMatches[0];

      const campaignPatch = {};
      if (pipeline_stage) campaignPatch.pipeline_stage = pipeline_stage;
      if (email) campaignPatch.email = email;
      if (linkedin_url) campaignPatch.linkedin_url = linkedin_url;
      if (notes) {
        const existing = campaignContact.notes ? `${campaignContact.notes}\n\n` : '';
        campaignPatch.notes = `${existing}[JARVIS ${new Date().toISOString().slice(0, 10)}] ${notes}`;
      }
      campaignPatch.updated_at = new Date().toISOString();
      if (Object.keys(campaignPatch).length <= 1) return { error: 'No fields provided to update' };

      await sb.from('company_contacts').update(campaignPatch).eq('id', campaignContact.id);
      emitJarvisActivity(
        `Updated campaign contact: ${campaignContact.name}`,
        `${campaignContact.company_name || ''} · ${Object.entries(campaignPatch).filter(([k]) => k !== 'updated_at').map(([k, v]) => `${k}=${String(v).slice(0, 60)}`).join(', ')}`,
        null,
        campaignContact.sourcing_campaigns?.name || null
      ).catch(() => {});
      return {
        updated: true,
        mode: 'campaign',
        contact: campaignContact.name,
        firm: campaignContact.company_name,
        campaign: campaignContact.sourcing_campaigns?.name || null,
        changes: Object.keys(campaignPatch).filter(k => k !== 'updated_at'),
      };
    }

    // Pick best match (prefer exact name start)
    const contact = matches.find(c =>
      c.name.toLowerCase().startsWith(contact_name.toLowerCase().split(' ')[0])
    ) || matches[0];

    const patch = {};
    if (pipeline_stage) patch.pipeline_stage = pipeline_stage;
    if (tier) patch.tier = tier;
    if (email) patch.email = email;
    if (linkedin_url) patch.linkedin_url = linkedin_url;
    if (notes) {
      const existing = contact.notes ? `${contact.notes}\n\n` : '';
      patch.notes = `${existing}[JARVIS ${new Date().toISOString().slice(0, 10)}] ${notes}`;
    }
    patch.updated_at = new Date().toISOString();

    if (!Object.keys(patch).length) return { error: 'No fields provided to update' };

    await sb.from('contacts').update(patch).eq('id', contact.id);
    emitJarvisActivity(
      `Updated pipeline contact: ${contact.name}`,
      `${contact.company_name || ''} · ${Object.entries(patch).filter(([k]) => k !== 'updated_at').map(([k, v]) => `${k}=${String(v).slice(0, 60)}`).join(', ')}`,
      deal_id
    ).catch(() => {});

    await writeMemory(deal_id, {
      type:    'UPDATE',
      subject: `Updated contact: ${contact.name}`,
      content: `JARVIS updated ${contact.name} at ${contact.company_name || 'unknown firm'}. Changes: ${Object.entries(patch).filter(([k]) => k !== 'updated_at').map(([k, v]) => `${k}=${String(v).slice(0, 60)}`).join(', ')}`,
      tags:    [`contact:${(contact.name || '').toLowerCase().replace(/\s+/g, '_')}`],
    });

    return {
      updated: true,
      contact: contact.name,
      firm: contact.company_name,
      changes: Object.keys(patch).filter(k => k !== 'updated_at'),
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── record_reply ──────────────────────────────────────────────────────────────
async function toolRecordReply({ deal_id, contact_name, message, intent, action_note }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    // Find contact
    const { data: matches } = await sb.from('contacts')
      .select('id, name, company_name, pipeline_stage')
      .eq('deal_id', deal_id)
      .ilike('name', `%${contact_name.split(' ')[0]}%`)
      .limit(5);

    if (!matches?.length) return { error: `No contact found matching "${contact_name}"` };
    const contact = matches[0];

    // Save to conversation_messages
    await sb.from('conversation_messages').insert({
      deal_id,
      contact_id:   contact.id,
      contact_name: contact.name,
      direction:    'inbound',
      channel:      'manual_entry',
      body:         message,
      intent:       intent || 'other',
      sent_at:      new Date().toISOString(),
    }).then(null, () => {});

    // Move to Replied stage (don't downgrade if already at a later stage)
    const laterStages = ['Meeting Booked'];
    const newStage = laterStages.includes(contact.pipeline_stage) ? contact.pipeline_stage : 'Replied';
    const noteParts = [`[Reply recorded ${new Date().toISOString().slice(0, 10)}] "${message.slice(0, 300)}"`];
    if (action_note) noteParts.push(`Action: ${action_note}`);

    await sb.from('contacts').update({
      pipeline_stage: newStage,
      notes: noteParts.join('\n'),
      last_reply_at: new Date().toISOString(),
      response_received: true,
      reply_channel: 'manual',
      updated_at: new Date().toISOString(),
    }).eq('id', contact.id);

    await writeMemory(deal_id, {
      type:    'REPLY',
      subject: `Reply from ${contact.name} at ${contact.company_name}`,
      content: `${contact.name} (${contact.company_name}) replied. Intent: ${intent || 'unknown'}. Message: "${message.slice(0, 400)}".${action_note ? ` Planned action: ${action_note}` : ''}`,
      tags:    [`contact:${(contact.name || '').toLowerCase().replace(/\s+/g, '_')}`, `intent:${intent || 'other'}`],
    });
    emitJarvisActivity(
      `Recorded reply: ${contact.name}`,
      `${contact.company_name || ''} · ${intent || 'other'}${action_note ? ` · ${action_note}` : ''}`,
      deal_id
    ).catch(() => {});

    return {
      recorded: true,
      contact: contact.name,
      firm: contact.company_name,
      stage_now: newStage,
      intent: intent || 'other',
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── add_intelligence ──────────────────────────────────────────────────────────
async function toolAddIntelligence({ deal_id, subject, content, tags }) {
  try {
    await writeMemory(deal_id || 'global', {
      type:    'INTELLIGENCE',
      subject,
      content,
      tags:    tags || [],
    });
    return { saved: true, subject };
  } catch (err) {
    return { error: err.message };
  }
}

// ── update_deal ───────────────────────────────────────────────────────────────
async function toolUpdateDeal({ deal_id, sector, geography, target_amount, ebitda_usd_m, equity_required_usd_m, investor_profile, description }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const patch = {};
    if (sector !== undefined)                  patch.sector = sector;
    if (geography !== undefined)               patch.geography = geography;
    if (target_amount !== undefined)           patch.target_amount = target_amount;
    if (ebitda_usd_m !== undefined)            patch.ebitda_usd_m = ebitda_usd_m;
    if (equity_required_usd_m !== undefined)   patch.equity_required_usd_m = equity_required_usd_m;
    if (investor_profile !== undefined)        patch.investor_profile = investor_profile;
    if (description !== undefined)             patch.description = description;
    patch.updated_at = new Date().toISOString();

    if (Object.keys(patch).length <= 1) return { error: 'No fields provided to update' };

    await sb.from('deals').update(patch).eq('id', deal_id);

    await writeMemory(deal_id, {
      type:    'UPDATE',
      subject: 'Deal details updated',
      content: `Deal settings updated by JARVIS: ${Object.entries(patch).filter(([k]) => k !== 'updated_at').map(([k, v]) => `${k}=${String(v).slice(0, 80)}`).join(', ')}`,
      tags:    ['deal_update'],
    });

    return { updated: true, changes: Object.keys(patch).filter(k => k !== 'updated_at') };
  } catch (err) {
    return { error: err.message };
  }
}

// ── enrich_contact ────────────────────────────────────────────────────────────
async function toolEnrichContact({ deal_id, contact_name }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const { data: matches } = await sb.from('contacts')
      .select('id, name, company_name, email, linkedin_url, enrichment_status')
      .eq('deal_id', deal_id)
      .ilike('name', `%${contact_name.split(' ')[0]}%`)
      .limit(5);

    if (!matches?.length) return { error: `No contact found matching "${contact_name}"` };
    const contact = matches.find(c =>
      c.name.toLowerCase().startsWith(contact_name.toLowerCase().split(' ')[0])
    ) || matches[0];

    const { enrichWithApify } = await import('../enrichment/apifyEnricher.js');
    const result = await enrichWithApify(contact);

    return {
      contact: contact.name,
      firm: contact.company_name,
      enrichment_result: result,
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── find_decision_makers ──────────────────────────────────────────────────────
async function toolFindDecisionMakers({ deal_id, firm_name }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const [{ data: firmRows }, { data: dealRows }] = await Promise.all([
      sb.from('firms')
        .select('id, name, website, description, hq_city, hq_country, fund_size_usd_m, focus_sectors, focus_geographies')
        .ilike('name', `%${firm_name.split(' ')[0]}%`)
        .limit(3),
      sb.from('deals')
        .select('id, name, sector, geography, description, investor_profile, ebitda_usd_m, equity_required_usd_m')
        .eq('id', deal_id)
        .single(),
    ]);

    const firm = firmRows?.[0] || { name: firm_name };
    const deal = dealRows || { id: deal_id };

    const { findDecisionMakers } = await import('../research/firmResearcher.js');
    const people = await findDecisionMakers(firm, deal);

    return {
      firm: firm.name,
      people_found: Array.isArray(people) ? people.length : 0,
      people: Array.isArray(people) ? people : [],
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── search_linkedin_people ────────────────────────────────────────────────────
async function toolSearchLinkedInPeople({ keywords, limit = 10 }) {
  try {
    const { searchLinkedInPeople } = await import('../integrations/unipileClient.js');
    const results = await searchLinkedInPeople({ keywords, limit: Math.min(limit, 20) });
    return {
      count: Array.isArray(results) ? results.length : 0,
      results: Array.isArray(results) ? results : [],
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── send_linkedin_dm ──────────────────────────────────────────────────────────
async function toolSendLinkedInDM({ deal_id, contact_name, message }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const { data: matches } = await sb.from('contacts')
      .select('id, name, company_name, invite_accepted_at, linkedin_provider_id, linkedin_urn, dm_sent_at')
      .eq('deal_id', deal_id)
      .ilike('name', `%${contact_name.split(' ')[0]}%`)
      .not('invite_accepted_at', 'is', null)
      .limit(5);

    if (!matches?.length) {
      return { error: `No connected contact found matching "${contact_name}". Contact must have accepted a LinkedIn invite first.` };
    }
    const contact = matches.find(c =>
      c.name.toLowerCase().startsWith(contact_name.toLowerCase().split(' ')[0])
    ) || matches[0];

    const providerId = contact.linkedin_provider_id || contact.linkedin_urn;
    if (!providerId) {
      return { error: `Contact ${contact.name} has no LinkedIn provider ID. Cannot send DM.` };
    }

    const { sendLinkedInDM } = await import('../integrations/unipileClient.js');
    await sendLinkedInDM({ attendeeProviderId: providerId, message });

    await sb.from('contacts').update({
      dm_sent_at:     new Date().toISOString(),
      pipeline_stage: 'DM Sent',
      updated_at:     new Date().toISOString(),
    }).eq('id', contact.id);

    emitJarvisActivity(
      `Sent LinkedIn DM to ${contact.name}`,
      `${contact.company_name || ''} · "${message.slice(0, 80)}"`,
      deal_id
    ).catch(() => {});
    return {
      sent: true,
      contact_name: contact.name,
      firm: contact.company_name,
      message_preview: message.slice(0, 80),
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── send_email ────────────────────────────────────────────────────────────────
async function toolSendEmail({ deal_id, contact_name, subject, body }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    const [{ data: matches }, { data: deal }] = await Promise.all([
      sb.from('contacts')
        .select('id, name, company_name, email')
        .eq('deal_id', deal_id)
        .ilike('name', `%${contact_name.split(' ')[0]}%`)
        .not('email', 'is', null)
        .limit(5),
      sb.from('deals')
        .select('id, name, sender_name')
        .eq('id', deal_id)
        .single(),
    ]);

    if (!matches?.length) {
      return { error: `No contact with an email found matching "${contact_name}". Contact must have an email address set.` };
    }
    const contact = matches.find(c =>
      c.name.toLowerCase().startsWith(contact_name.toLowerCase().split(' ')[0])
    ) || matches[0];

    const { sendEmail } = await import('../integrations/unipileClient.js');
    await sendEmail({
      to:        contact.email,
      toName:    contact.name,
      subject,
      body,
      fromName:  deal?.sender_name || 'Dom',
      accountId: 'q0z5aYSiRG-fteyP-ZnmPA',
    });

    await sb.from('contacts').update({
      last_email_sent_at: new Date().toISOString(),
      email_sent:         true,
      updated_at:         new Date().toISOString(),
    }).eq('id', contact.id);

    emitJarvisActivity(
      `Sent email to ${contact.name}`,
      `${contact.company_name || ''} · Subject: ${subject}`,
      deal_id
    ).catch(() => {});
    return {
      sent: true,
      to:      contact.email,
      contact: contact.name,
      firm:    contact.company_name,
      subject,
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── add_contact_to_pipeline ───────────────────────────────────────────────────
async function toolAddContactToPipeline({ deal_id, name, company_name, job_title, linkedin_url, email, notes, tier = 'warm' }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    // Check if already exists (fuzzy match on first name + first word of company)
    const { data: existing } = await sb.from('contacts')
      .select('id, name, pipeline_stage')
      .eq('deal_id', deal_id)
      .ilike('name', `%${name.split(' ')[0]}%`)
      .ilike('company_name', `%${company_name.split(' ')[0]}%`)
      .limit(1);

    if (existing?.length) {
      return {
        added: false,
        message: `${name} at ${company_name} is already in the pipeline at stage: ${existing[0].pipeline_stage}`,
      };
    }

    const { data: inserted, error: insertErr } = await sb.from('contacts').insert({
      deal_id,
      name,
      company_name,
      job_title:         job_title || null,
      linkedin_url:      linkedin_url || null,
      email:             email || null,
      notes:             notes || null,
      tier:              tier || 'warm',
      pipeline_stage:    'Researched',
      enrichment_status: linkedin_url ? 'linkedin_only' : 'pending',
      source:            'jarvis',
      created_at:        new Date().toISOString(),
      updated_at:        new Date().toISOString(),
    }).select('id').single();

    if (insertErr) return { error: insertErr.message };

    emitJarvisActivity(
      `Added ${name} to pipeline`,
      `${company_name}${job_title ? ` · ${job_title}` : ''} · tier: ${tier} · will be enriched next cycle`,
      deal_id
    ).catch(() => {});

    // Kick off enrichment cycle
    const { triggerImmediateRun } = await import('./orchestrator.js');
    triggerImmediateRun(deal_id).catch(() => {});

    return {
      added: true,
      contact_id: inserted?.id,
      message: `Added ${name} at ${company_name} to the pipeline. Enrichment and LinkedIn outreach will start on the next orchestrator cycle.`,
    };
  } catch (err) {
    return { error: err.message };
  }
}

// ── get_pipeline_contacts ─────────────────────────────────────────────────────
async function toolGetPipelineContacts({ deal_id, filter, limit = 20 }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  const cap = Math.min(limit, 50);
  try {
    const base = sb.from('contacts')
      .select('id, name, company_name, pipeline_stage, tier, last_reply_at, last_email_sent_at, dm_sent_at, invite_accepted_at, email')
      .eq('deal_id', deal_id)
      .limit(cap);

    let query;
    switch (filter) {
      case 'replied':
        query = base.not('last_reply_at', 'is', null).order('last_reply_at', { ascending: false });
        break;
      case 'warm':
        query = base.in('tier', ['hot', 'warm']).order('tier', { ascending: true });
        break;
      case 'accepted_no_dm':
        query = base.not('invite_accepted_at', 'is', null).is('dm_sent_at', null).order('invite_accepted_at', { ascending: false });
        break;
      case 'meetings':
        query = base.or('pipeline_stage.ilike.%meeting%,meeting_count.gt.0').order('updated_at', { ascending: false });
        break;
      case 'all_active':
        query = base
          .not('pipeline_stage', 'in', '("Archived","ARCHIVED","archived","Skipped","Inactive","Suppressed — Opt Out","Deleted — Do Not Contact","skipped_no_name","skipped_no_linkedin","skipped_duplicate_email")')
          .order('updated_at', { ascending: false });
        break;
      default:
        return { error: `Unknown filter "${filter}". Use: replied, warm, accepted_no_dm, meetings, all_active` };
    }

    const { data, error } = await query;
    if (error) return { error: error.message };

    return {
      filter,
      count: (data || []).length,
      contacts: (data || []).map(c => ({
        name:               c.name,
        firm:               c.company_name,
        stage:              c.pipeline_stage,
        tier:               c.tier,
        has_email:          !!c.email,
        last_reply_at:      c.last_reply_at,
        last_email_sent_at: c.last_email_sent_at,
        dm_sent_at:         c.dm_sent_at,
        invite_accepted_at: c.invite_accepted_at,
      })),
    };
  } catch (err) {
    return { error: err.message };
  }
}
