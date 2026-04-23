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
    description: 'Permanently suppress a firm from all future outreach on this deal. Use when they have declined, are not a fit, or should never be contacted again.',
    input_schema: {
      type: 'object',
      properties: {
        deal_id:   { type: 'string', description: 'The deal UUID' },
        firm_name: { type: 'string', description: 'Name of the firm to suppress' },
        reason:    { type: 'string', description: 'Why this firm is being suppressed' },
      },
      required: ['deal_id', 'firm_name', 'reason'],
    },
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTORS
// ─────────────────────────────────────────────────────────────────────────────

export async function executeTool(name, input) {
  switch (name) {
    case 'get_status':             return toolGetStatus(input);
    case 'get_pending_approvals':  return toolGetPendingApprovals(input);
    case 'approve_message':        return toolApproveMessage(input);
    case 'skip_message':           return toolSkipMessage(input);
    case 'get_recent_activity':    return toolGetRecentActivity(input);
    case 'control_module':         return toolControlModule(input);
    case 'trigger_research':       return toolTriggerResearch(input);
    case 'search_pipeline':        return toolSearchPipeline(input);
    case 'get_contacts_by_stage':  return toolGetContactsByStage(input);
    case 'suppress_firm':          return toolSuppressFirm(input);
    default:
      return { error: `Unknown tool: ${name}` };
  }
}

// ── get_status ────────────────────────────────────────────────────────────────
async function toolGetStatus({ deal_id }) {
  try {
    const { gatherCurrentMetrics } = await import('./fundraiserBrain.js');
    const metrics = await gatherCurrentMetrics(deal_id);
    return {
      emails_sent_total:    metrics.emails_sent         || 0,
      emails_sent_today:    metrics.emails_sent_today   || 0,
      dms_sent_total:       metrics.dms_sent            || 0,
      invites_sent:         metrics.li_invites_sent     || 0,
      invites_pending:      metrics.li_pending          || 0,
      total_replies:        metrics.total_replies       || 0,
      meetings_booked:      metrics.meetings_booked     || 0,
      active_firms:         metrics.firms_in_pipeline   || 0,
      pending_approvals:    metrics.pending_approvals   || 0,
      response_rate_pct:    metrics.response_rate       || 0,
      goal_status:          metrics.goal_status         || 'UNKNOWN',
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
  try {
    const { data } = await sb.from('activity_log')
      .select('event_type, summary, detail, created_at')
      .eq('deal_id', deal_id)
      .order('created_at', { ascending: false })
      .limit(Math.min(limit, 40));

    return {
      events: (data || []).map(e => ({
        type:    e.event_type,
        summary: e.summary,
        when:    e.created_at,
      })),
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
    const { triggerImmediateRun } = await import('./orchestrator.js');
    triggerImmediateRun(deal_id).catch(() => {});
    return { triggered: true, message: 'Immediate research and enrichment cycle started. New firms will be queued for outreach within minutes.' };
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

// ── suppress_firm ─────────────────────────────────────────────────────────────
async function toolSuppressFirm({ deal_id, firm_name, reason }) {
  const sb = getSupabase();
  if (!sb) return { error: 'Database unavailable' };
  try {
    await sb.from('firm_outreach_state').upsert({
      deal_id,
      firm_name,
      status:     'suppressed',
      notes:      reason,
      updated_at: new Date().toISOString(),
    }, { onConflict: 'deal_id,firm_name' });

    // Archive all contacts at this firm for this deal
    await sb.from('contacts')
      .update({ tier: 'archive', pipeline_stage: 'Archived' })
      .eq('deal_id', deal_id)
      .ilike('company_name', `%${firm_name.split(' ')[0]}%`);

    await writeMemory(deal_id, {
      type:    'DECISION',
      subject: `Suppressed ${firm_name}`,
      content: `${firm_name} suppressed from all future outreach. Reason: ${reason}`,
      tags:    [`firm:${firm_name.toLowerCase().replace(/\s+/g, '_')}`],
      metadata: { reason, suppressed_at: new Date().toISOString() },
    });

    return { suppressed: true, firm: firm_name };
  } catch (err) {
    return { error: err.message };
  }
}
