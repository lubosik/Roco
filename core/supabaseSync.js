/**
 * core/supabaseSync.js — Supabase sync layer
 *
 * Source of truth for: session state, deals, emails, activity log, templates.
 * Falls back to state.json if Supabase is unavailable.
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { getSupabase } from './supabase.js';
import { deriveOutreachEventStatus } from './hardeningHelpers.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const STATE_FILE = path.join(__dirname, '../state.json');
const LOCAL_STATE_DISABLED = ['1', 'true', 'yes', 'on'].includes(String(process.env.ROCO_DISABLE_LOCAL_STATE || '').trim().toLowerCase())
  || !!process.env.RAILWAY_PUBLIC_DOMAIN
  || !!process.env.RAILWAY_STATIC_URL;

const DEFAULT_TEMPLATES = [
  {
    name: 'intro',
    type: 'email',
    subject_a: 'Thought of you on this one',
    subject_b: 'Something in your space',
    body: `{{firstName}},

Given {{comparableDeal}}, thought this one might be relevant for you.

We are finalising the cap table on {{dealName}} — {{dealBrief}}.

Let me know if you would like to explore further.

Dom`,
    variables: ['firstName', 'comparableDeal', 'dealName', 'dealBrief'],
    ab_test_enabled: true,
    is_active: true,
  },
  {
    name: 'followup_1',
    type: 'email',
    subject_a: 'Quick one',
    subject_b: 'Following up',
    body: `{{firstName}},

Wanted to make sure this did not get buried.

{{dealName}} — {{dealBrief}}. We are moving forward with the cap table this week.

Worth a conversation?

Dom`,
    variables: ['firstName', 'dealName', 'dealBrief'],
    ab_test_enabled: true,
    is_active: true,
  },
  {
    name: 'followup_2',
    type: 'email',
    subject_a: 'Still relevant?',
    subject_b: 'One more from me',
    body: `{{firstName}},

Is {{sector}} still something you are actively looking at?

Happy to send across more detail if the timing is right.

Dom`,
    variables: ['firstName', 'sector'],
    ab_test_enabled: true,
    is_active: true,
  },
  {
    name: 'followup_3',
    type: 'email',
    subject_a: 'Leaving this with you',
    subject_b: 'Last one from me',
    body: `{{firstName}},

Not going to keep following up after this — just wanted to make sure you had the chance to look.

{{dealName}}. Happy to share the full deck if useful.

Dom`,
    variables: ['firstName', 'dealName'],
    ab_test_enabled: true,
    is_active: true,
  },
  {
    name: 'linkedin_intro',
    type: 'linkedin',
    subject_a: null,
    subject_b: null,
    body: `Hi {{firstName}} — given your work at {{firmName}}, thought {{dealName}} might be interesting. Happy to share more detail. Dom`,
    variables: ['firstName', 'firmName', 'dealName'],
    ab_test_enabled: false,
    is_active: true,
  },
  {
    name: 'linkedin_followup_1',
    type: 'linkedin',
    subject_a: null,
    subject_b: null,
    body: `Hi {{firstName}} — just making sure my last message didn't get buried. Would love to share the detail on {{dealName}} if relevant. Dom`,
    variables: ['firstName', 'dealName'],
    ab_test_enabled: false,
    is_active: true,
  },
];

const OUTREACH_EVENT_TYPES = new Set([
  'LINKEDIN_INVITE_SENT',
  'LINKEDIN_INVITE_ALREADY_PENDING',
  'LINKEDIN_ALREADY_CONNECTED',
  'LINKEDIN_INVITE_FAILED',
  'LINKEDIN_INVITE_SKIPPED_NO_PROFILE',
  'LINKEDIN_INVITE_PROVIDER_LIMIT',
  'LINKEDIN_INVITE_PROVIDER_LIMIT_ESCALATED',
  'LINKEDIN_DM_SENT',
  'EMAIL_SENT',
]);

async function mirrorOutreachEvent(sb, payload = {}) {
  const eventType = String(payload?.eventType || '').toUpperCase();
  if (!OUTREACH_EVENT_TYPES.has(eventType)) return;

  const detail = payload?.detail && typeof payload.detail === 'object' ? payload.detail : {};
  const row = {
    deal_id: payload.dealId || null,
    contact_id: payload.contactId || null,
    event_type: eventType,
    channel: detail.channel || null,
    status: detail.status || deriveOutreachEventStatus(eventType),
    provider: payload.apiUsed || detail.provider || detail.api_used || 'unipile',
    provider_message_id: detail.invitation_id || detail.message_id || detail.email_id || null,
    provider_account_id: detail.account_id || null,
    metadata: {
      summary: payload.summary || null,
      fallback_used: !!payload.fallbackUsed,
      ...detail,
    },
    created_at: new Date().toISOString(),
  };

  try {
    await sb.from('outreach_events').insert([row]);
  } catch {
    // Best-effort only; deploy must not depend on this table existing yet.
  }
}

// ─────────────────────────────────────────────
// SESSION STATE
// ─────────────────────────────────────────────

export async function loadSessionState() {
  let localState = null;
  try {
    if (!LOCAL_STATE_DISABLED && fs.existsSync(STATE_FILE)) {
      localState = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    }
  } catch {}

  const sb = getSupabase();
  if (sb) {
    try {
      const { data, error } = await sb
        .from('sessions')
        .select('*')
        .eq('id', 'singleton')
        .single();
      if (!error && data) {
        // Preserve any local-only keys that are not yet modeled in Supabase.
        const mapped = {
          ...(localState || {}),
          ...mapSessionFromSupabase(data),
        };
        if (!LOCAL_STATE_DISABLED) {
          fs.writeFileSync(STATE_FILE, JSON.stringify(mapped, null, 2));
        }
        return mapped;
      }
    } catch (err) {
      console.warn('[supabaseSync] Could not load session from Supabase:', err.message);
    }
  }

  // Fall back to local state.json
  if (localState) return localState;

  return getDefaultState();
}

export async function saveSessionState(state) {
  // Always write locally first
  try {
    if (!LOCAL_STATE_DISABLED) {
      fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
    }
  } catch {}

  const sb = getSupabase();
  if (!sb) return;

  try {
    const mapped = mapStateToSupabase(state);
    await sb.from('sessions').upsert({ id: 'singleton', ...mapped, updated_at: new Date().toISOString() });
  } catch (err) {
    console.warn('[supabaseSync] Could not save session to Supabase:', err.message);
  }
}

function mapSessionFromSupabase(data) {
  return {
    rocoStatus: data.roco_status || 'ACTIVE',
    outreachEnabled: data.outreach_enabled !== false,
    followupEnabled: data.followup_enabled !== false,
    enrichmentEnabled: data.enrichment_enabled !== false,
    researchEnabled: data.research_enabled !== false,
    linkedinEnabled: data.linkedin_enabled !== false,
    activeDealIds: data.active_deal_ids || [],
    outreachPausedUntil: data.outreach_paused_until || null,
    lastUpdated: data.updated_at,
  };
}

function mapStateToSupabase(state) {
  return {
    roco_status: state.rocoStatus || 'ACTIVE',
    outreach_enabled: state.outreachEnabled !== false,
    followup_enabled: state.followupEnabled !== false,
    enrichment_enabled: state.enrichmentEnabled !== false,
    research_enabled: state.researchEnabled !== false,
    linkedin_enabled: state.linkedinEnabled !== false,
    active_deal_ids: state.activeDealIds || [],
    outreach_paused_until: state.outreachPausedUntil || null,
  };
}

function getDefaultState() {
  return {
    rocoStatus: 'ACTIVE',
    outreachEnabled: true,
    followupEnabled: true,
    enrichmentEnabled: true,
    researchEnabled: true,
    linkedinEnabled: true,
    activeDealIds: [],
    outreachPausedUntil: null,
    lastUpdated: new Date().toISOString(),
  };
}

function normalizeDealRecord(deal) {
  if (!deal) return deal;
  const parsedDealInfo = (
    deal.parsed_deal_info && typeof deal.parsed_deal_info === 'object'
      ? deal.parsed_deal_info
      : {}
  );
  const noFollowUps = deal.no_follow_ups !== undefined
    ? !!deal.no_follow_ups
    : !!parsedDealInfo.no_follow_ups;
  return {
    ...deal,
    parsed_deal_info: parsedDealInfo,
    no_follow_ups: noFollowUps,
  };
}

function isMissingColumnError(err, column) {
  const message = String(err?.message || err || '').toLowerCase();
  const needle = String(column || '').toLowerCase();
  return !!needle && (
    message.includes(`could not find the '${needle}' column`)
    || message.includes(`column "${needle}" does not exist`)
    || message.includes(`"${needle}"`)
  );
}

function mergeParsedDealInfo(existing, patch = {}) {
  const base = existing && typeof existing === 'object' ? { ...existing } : {};
  Object.entries(patch || {}).forEach(([key, value]) => {
    if (value === undefined) return;
    if (value === null) delete base[key];
    else base[key] = value;
  });
  return base;
}

// ─────────────────────────────────────────────
// DEALS
// ─────────────────────────────────────────────

export async function getActiveDeals() {
  const sb = getSupabase();
  if (!sb) return [];
  try {
    const { data, error } = await sb
      .from('deals')
      .select('*')
      .ilike('status', 'active')
      .neq('paused', true)
      .order('created_at', { ascending: false });
    if (error) throw error;
    return data || [];
  } catch (err) {
    try {
      const { data, error } = await sb
        .from('deals')
        .select('*')
        .ilike('status', 'active')
        .order('created_at', { ascending: false });
      if (error) throw error;
      return (data || []).filter(deal => deal.paused !== true);
    } catch (fallbackErr) {
      console.warn('[supabaseSync] Could not load active deals:', fallbackErr.message || err.message);
      return [];
    }
  }
}

export async function getAllDeals() {
  const sb = getSupabase();
  if (!sb) return [];
  try {
    const { data } = await sb.from('deals').select('*').order('created_at', { ascending: false });
    return (data || []).map(normalizeDealRecord);
  } catch {
    return [];
  }
}

export async function getDeal(dealId) {
  const sb = getSupabase();
  if (!sb) return null;
  try {
    const { data } = await sb.from('deals').select('*').eq('id', dealId).single();
    return normalizeDealRecord(data);
  } catch {
    return null;
  }
}

export async function createDeal(fields) {
  const sb = getSupabase();
  if (!sb) throw new Error('Supabase not configured');
  const { data, error } = await sb.from('deals').insert([fields]).select().single();
  if (error) throw error;
  return normalizeDealRecord(data);
}

export async function updateDeal(dealId, fields) {
  const sb = getSupabase();
  if (!sb) throw new Error('Supabase not configured');

  const runUpdate = async (updates) => {
    const { data, error } = await sb
      .from('deals')
      .update({ ...updates, updated_at: new Date().toISOString() })
      .eq('id', dealId)
      .select()
      .single();
    if (error) throw error;
    return normalizeDealRecord(data);
  };

  try {
    return await runUpdate(fields);
  } catch (error) {
    const fallback = { ...fields };
    let changed = false;

    if (Object.prototype.hasOwnProperty.call(fallback, 'target_geography') && isMissingColumnError(error, 'target_geography')) {
      delete fallback.target_geography;
      changed = true;
    }

    if (Object.prototype.hasOwnProperty.call(fallback, 'no_follow_ups') && (
      isMissingColumnError(error, 'no_follow_ups')
      || isMissingColumnError(error, 'parsed_deal_info')
    )) {
      const existingDeal = await getDeal(dealId).catch(() => null);
      if (existingDeal && !isMissingColumnError(error, 'parsed_deal_info')) {
        fallback.parsed_deal_info = mergeParsedDealInfo(existingDeal.parsed_deal_info, {
          no_follow_ups: !!fallback.no_follow_ups,
        });
      }
      delete fallback.no_follow_ups;
      changed = true;
    }

    if (!changed) throw error;
    return runUpdate(fallback);
  }
}

// ─────────────────────────────────────────────
// ACTIVITY LOG
// ─────────────────────────────────────────────

export async function logActivity({ dealId, contactId, eventType, summary, detail, apiUsed, fallbackUsed } = {}) {
  const sb = getSupabase();
  if (!sb) return;
  try {
    await sb.from('activity_log').insert([{
      deal_id: dealId || null,
      contact_id: contactId || null,
      event_type: eventType || 'GENERAL',
      summary: summary || '',
      detail: detail || null,
      api_used: apiUsed || null,
      fallback_used: fallbackUsed || false,
    }]);
    await mirrorOutreachEvent(sb, {
      dealId,
      contactId,
      eventType,
      summary,
      detail,
      apiUsed,
      fallbackUsed,
    });
  } catch {
    // Best-effort
  }
}

export async function getActivityLog({ dealId, limit = 200, offset = 0, eventType } = {}) {
  const sb = getSupabase();
  if (!sb) return [];
  try {
    let query = sb
      .from('activity_log')
      .select('*')
      .order('created_at', { ascending: false })
      .range(offset, offset + limit - 1);
    if (dealId) query = query.eq('deal_id', dealId);
    if (eventType) query = query.eq('event_type', eventType);
    const { data } = await query;
    return data || [];
  } catch {
    return [];
  }
}

// ─────────────────────────────────────────────
// EMAILS
// ─────────────────────────────────────────────

export async function saveEmail(fields) {
  const sb = getSupabase();
  if (!sb) return null;
  try {
    const { data } = await sb.from('emails').insert([fields]).select().single();
    return data;
  } catch {
    return null;
  }
}

export async function updateEmail(emailId, fields) {
  const sb = getSupabase();
  if (!sb) return null;
  try {
    const { data } = await sb
      .from('emails')
      .update({ ...fields, updated_at: new Date().toISOString() })
      .eq('id', emailId)
      .select()
      .single();
    return data;
  } catch {
    return null;
  }
}

export async function getApprovedQueue(dealId) {
  const sb = getSupabase();
  if (!sb) return [];
  try {
    let query = sb
      .from('emails')
      .select('*')
      .eq('status', 'approved')
      .order('queued_to_send_at', { ascending: true });
    if (dealId) query = query.eq('deal_id', dealId);
    const { data } = await query;
    return data || [];
  } catch {
    return [];
  }
}

export async function getSentCountToday(dealId) {
  const sb = getSupabase();
  if (!sb) return 0;
  try {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const { count } = await sb
      .from('emails')
      .select('id', { count: 'exact', head: true })
      .eq('deal_id', dealId)
      .eq('status', 'sent')
      .gte('sent_at', today.toISOString());
    return count || 0;
  } catch {
    return 0;
  }
}

export async function getSentCountThisHour(dealId) {
  const sb = getSupabase();
  if (!sb) return 0;
  try {
    const hourAgo = new Date(Date.now() - 3600000);
    const { count } = await sb
      .from('emails')
      .select('id', { count: 'exact', head: true })
      .eq('deal_id', dealId)
      .eq('status', 'sent')
      .gte('sent_at', hourAgo.toISOString());
    return count || 0;
  } catch {
    return 0;
  }
}

// ─────────────────────────────────────────────
// REPLIES
// ─────────────────────────────────────────────

export async function saveReply(fields) {
  const sb = getSupabase();
  if (!sb) return null;
  try {
    const { data } = await sb.from('replies').insert([fields]).select().single();
    return data;
  } catch {
    return null;
  }
}

// ─────────────────────────────────────────────
// TEMPLATES
// ─────────────────────────────────────────────

export async function seedDefaultTemplates() {
  const sb = getSupabase();
  if (!sb) return;
  try {
    const { count } = await sb
      .from('email_templates')
      .select('id', { count: 'exact', head: true });
    if (count > 0) return; // Already seeded
    await sb.from('email_templates').insert(DEFAULT_TEMPLATES);
    console.log('[supabaseSync] Default templates seeded');
  } catch (err) {
    console.warn('[supabaseSync] Could not seed templates:', err.message);
  }
}

export async function getTemplates() {
  const sb = getSupabase();
  if (!sb) return DEFAULT_TEMPLATES;
  try {
    const { data } = await sb
      .from('email_templates')
      .select('*')
      .order('created_at', { ascending: true });
    return data || DEFAULT_TEMPLATES;
  } catch {
    return DEFAULT_TEMPLATES;
  }
}

export async function getTemplate(id) {
  const sb = getSupabase();
  if (!sb) return null;
  try {
    const { data } = await sb.from('email_templates').select('*').eq('id', id).single();
    return data;
  } catch {
    return null;
  }
}

export async function updateTemplate(id, fields) {
  const sb = getSupabase();
  if (!sb) throw new Error('Supabase not configured');
  const { data, error } = await sb
    .from('email_templates')
    .update({ ...fields, updated_at: new Date().toISOString(), updated_by: 'dom' })
    .eq('id', id)
    .select()
    .single();
  if (error) throw error;

  // Also update local emailTemplates.js fallback file
  await syncTemplatesToLocal();
  return data;
}

async function syncTemplatesToLocal() {
  try {
    const templates = await getTemplates();
    const emailTemplates = templates.filter(t => t.type === 'email');
    const lines = [
      '// AUTO-GENERATED by supabaseSync.js — do not edit directly\n',
      'export const templates = {',
    ];

    const nameMap = { intro: 'intro', followup_1: 'followup1', followup_2: 'followup2', followup_3: 'followup3' };
    for (const t of emailTemplates) {
      const key = nameMap[t.name] || t.name;
      lines.push(`  ${key}: {`);
      lines.push(`    subjectA: ${JSON.stringify(t.subject_a || '')},`);
      lines.push(`    subjectB: ${JSON.stringify(t.subject_b || '')},`);
      lines.push(`    body: ${JSON.stringify(t.body || '')},`);
      lines.push(`  },`);
    }
    lines.push('};\n');
    lines.push('export function applyTemplate(templateObj, data) {');
    lines.push('  const result = {};');
    lines.push('  for (const key of Object.keys(templateObj)) {');
    lines.push('    let value = templateObj[key];');
    lines.push('    for (const [placeholder, replacement] of Object.entries(data)) {');
    lines.push('      value = value.replace(new RegExp(`\\\\{\\\\{${placeholder}\\\\}\\\\}`, \'g\'), replacement || \'\');');
    lines.push('    }');
    lines.push('    result[key] = value;');
    lines.push('  }');
    lines.push('  return result;');
    lines.push('}');

    const filePath = path.join(__dirname, '../templates/emailTemplates.js');
    fs.writeFileSync(filePath, lines.join('\n'));
  } catch {}
}

// ─────────────────────────────────────────────
// BATCHES
// ─────────────────────────────────────────────

export async function getBatches(dealId) {
  const sb = getSupabase();
  if (!sb) return [];
  try {
    const { data } = await sb
      .from('batches')
      .select('*')
      .eq('deal_id', dealId)
      .order('batch_number', { ascending: true });
    return data || [];
  } catch {
    return [];
  }
}

export async function createBatch(fields) {
  const sb = getSupabase();
  if (!sb) return null;
  try {
    const { data } = await sb.from('batches').insert([fields]).select().single();
    return data;
  } catch {
    return null;
  }
}

export async function updateBatch(batchId, fields) {
  const sb = getSupabase();
  if (!sb) return null;
  try {
    const { data } = await sb.from('batches').update(fields).eq('id', batchId).select().single();
    return data;
  } catch {
    return null;
  }
}

// ─────────────────────────────────────────────
// FIRM RESPONSES
// ─────────────────────────────────────────────

export async function recordFirmResponse(fields) {
  const sb = getSupabase();
  if (!sb) return null;
  try {
    const { data } = await sb.from('firm_responses').insert([fields]).select().single();
    return data;
  } catch {
    return null;
  }
}

export async function checkFirmResponded(companyName, dealId) {
  const sb = getSupabase();
  if (!sb) return false;
  try {
    const { count } = await sb
      .from('firm_responses')
      .select('id', { count: 'exact', head: true })
      .eq('company_name', companyName)
      .eq('deal_id', dealId);
    return (count || 0) > 0;
  } catch {
    return false;
  }
}

// ─────────────────────────────────────────────
// SCHEDULE LOG
// ─────────────────────────────────────────────

export async function logScheduleEvent(dealId, eventType, detail) {
  const sb = getSupabase();
  if (!sb) return;
  try {
    await sb.from('schedule_log').insert([{ deal_id: dealId, event_type: eventType, detail }]);
  } catch {}
}

// ─────────────────────────────────────────────
// APPROVAL QUEUE (bridges VPS ↔ Vercel dashboard)
// ─────────────────────────────────────────────

export async function addApprovalToQueue(data) {
  const sb = getSupabase();
  if (!sb) return null;
  try {
    // Dedup: never create a second active entry for the same contact+stage.
    // Covers the restart-loop case where contactsInFlight is cleared but the
    // DB row is still pending/approved/sending.
    if (data.contactId && data.stage) {
      const { data: existing } = await sb.from('approval_queue')
        .select('id, status')
        .eq('contact_id', data.contactId)
        .eq('stage', data.stage)
        .in('status', ['pending', 'approved', 'approved_waiting_for_window', 'sending'])
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (existing) {
        console.log(`[supabaseSync] addApprovalToQueue: dedup — returning existing row ${existing.id} (${existing.status}) for contact ${data.contactId}`);
        return existing;
      }
    }

    const { data: row, error } = await sb
      .from('approval_queue')
      .insert([{
        telegram_msg_id: data.telegramMsgId || null,
        deal_id: data.dealId || null,
        contact_id: data.contactId || null,
        contact_name: data.contactName || '',
        contact_email: data.contactEmail || null,
        firm: data.firm || null,
        deal_name: data.dealName || null,
        stage: data.stage || null,
        subject_a: data.subjectA || null,
        subject_b: data.subjectB || null,
        body: data.body || null,
        score: data.score ?? null,
        research_summary: data.researchSummary || null,
        outreach_mode: data.outreachMode || null,
        status: 'pending',
      }])
      .select()
      .single();
    if (error) throw error;
    return row;
  } catch (err) {
    console.warn('[supabaseSync] Could not add to approval_queue:', err.message);
    return null;
  }
}

export async function getApprovalQueue() {
  const sb = getSupabase();
  if (!sb) return [];
  try {
    const { data } = await sb
      .from('approval_queue')
      .select('*')
      .eq('status', 'pending')
      .order('created_at', { ascending: true });
    return (data || []).map(row => ({
      id: row.id,
      telegramMsgId: row.telegram_msg_id,
      name: row.contact_name,
      firm: row.firm,
      score: row.score,
      stage: row.stage,
      subject: row.subject_a,
      alternativeSubject: row.subject_b,
      body: row.body,
      contactPageId: row.contact_id,
      queuedAt: row.created_at,
    }));
  } catch {
    return [];
  }
}

export async function resolveApprovalInSupabase(id, status, approvedSubject, editInstructions) {
  const sb = getSupabase();
  if (!sb) throw new Error('Supabase not configured');
  const { error } = await sb
    .from('approval_queue')
    .update({
      status,
      approved_subject: approvedSubject || null,
      edit_instructions: editInstructions || null,
      resolved_at: new Date().toISOString(),
    })
    .eq('id', id);
  if (error) throw error;
}

export async function getResolvedApprovals() {
  const sb = getSupabase();
  if (!sb) return [];
  try {
    const { data } = await sb
      .from('approval_queue')
      .select('*')
      .in('status', ['approved', 'skipped', 'edit'])
      .not('resolved_at', 'is', null)
      .order('resolved_at', { ascending: true });
    return data || [];
  } catch {
    return [];
  }
}

export async function markApprovalProcessing(id) {
  const sb = getSupabase();
  if (!sb) return;
  try {
    await sb.from('approval_queue').update({ status: 'processing' }).eq('id', id);
  } catch {}
}

export async function updateApprovalStatus(id, status, approvedSubject = null) {
  const sb = getSupabase();
  if (!sb) return;
  try {
    const updates = { status };
    if (approvedSubject) updates.approved_subject = approvedSubject;
    // Only transition from 'pending' → approved/skipped.  Never overwrite
    // 'approved_waiting_for_window', 'sending', or 'sent' — those are
    // set by sendApprovedLinkedInDM / executeOutreach and must not be
    // clobbered by this fire-and-forget call from resolveApproval.
    await sb.from('approval_queue').update(updates)
      .eq('id', id)
      .in('status', ['pending']);
  } catch {}
}

export async function deleteApprovalFromQueue(telegramMsgId) {
  const sb = getSupabase();
  if (!sb) return;
  try {
    await sb.from('approval_queue').delete().eq('telegram_msg_id', telegramMsgId);
  } catch {}
}

// ─────────────────────────────────────────────
// STARTUP: VERIFY SUPABASE CONNECTION
// ─────────────────────────────────────────────

export async function verifySupabase() {
  const sb = getSupabase();
  if (!sb) return false;
  try {
    const { error } = await sb.from('sessions').select('id').limit(1);
    if (error) throw error;
    return true;
  } catch (err) {
    try {
      const { error } = await sb.from('deals').select('id').limit(1);
      if (error) throw error;
      console.warn(`[supabaseSync] sessions verification failed (${err.message}); deals table is reachable`);
      return true;
    } catch (fallbackErr) {
      console.warn('[supabaseSync] Supabase verification failed:', fallbackErr.message || err.message);
      return false;
    }
  }
}
