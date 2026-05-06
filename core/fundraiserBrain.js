import { DateTime } from 'luxon';
import { getSupabase } from './supabase.js';

function getDealTimezone(deal = {}) {
  return deal.timezone || deal.sending_timezone || 'America/New_York';
}

function toDayRange(timezone = 'America/New_York') {
  const now = DateTime.now().setZone(timezone);
  return {
    start: now.startOf('day').toUTC().toISO(),
    end: now.endOf('day').toUTC().toISO(),
  };
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
const ACTIVE_APPROVAL_STATUSES = ['pending', 'approved', 'approved_waiting_for_window', 'sending'];

async function fetchAllRows(buildQuery, pageSize = 1000) {
  const rows = [];
  for (let from = 0; ; from += pageSize) {
    const { data, error } = await buildQuery().range(from, from + pageSize - 1);
    if (error) throw error;
    rows.push(...(data || []));
    if (!data || data.length < pageSize) break;
  }
  return rows;
}

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

function normalizeNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function pickTargetEquity(deal = {}) {
  const candidates = [
    deal.target_equity,
    deal.raise_target,
    deal.equity_target,
    deal.target_amount,
    deal.fundraising_target,
  ];
  for (const candidate of candidates) {
    const value = normalizeNumber(candidate, NaN);
    if (Number.isFinite(value) && value > 0) return value;
  }
  return 5;
}

function pickMeetingsNeeded(deal = {}, targetEquity = 5) {
  const direct = normalizeNumber(deal.meetings_needed, NaN);
  if (Number.isFinite(direct) && direct > 0) return direct;
  return Math.max(8, Math.ceil(targetEquity * 2));
}

function getDealLaunchDate(deal = {}, timezone = 'America/New_York') {
  const candidates = [
    deal.launched_at,
    deal.launch_date,
    deal.started_at,
    deal.go_live_at,
    deal.created_at,
  ];
  for (const candidate of candidates) {
    if (!candidate) continue;
    const parsed = DateTime.fromISO(String(candidate), { zone: 'utc' }).setZone(timezone);
    if (parsed.isValid) return parsed;
  }
  return DateTime.now().setZone(timezone);
}

function getTimingContext(deal = {}, metrics = {}) {
  const timezone = getDealTimezone(deal);
  const now = DateTime.now().setZone(timezone);
  const launchDate = getDealLaunchDate(deal, timezone);
  const rawDays = Math.max(0, Math.floor(now.startOf('day').diff(launchDate.startOf('day'), 'days').days));

  let cursor = launchDate.startOf('day');
  let businessDays = 0;
  while (cursor <= now.startOf('day')) {
    if (cursor.weekday <= 5) businessDays += 1;
    cursor = cursor.plus({ days: 1 });
  }

  const totalOutboundToday = normalizeNumber(metrics.li_invites_today, 0)
    + normalizeNumber(metrics.emails_sent_today, 0)
    + normalizeNumber(metrics.dms_sent_today, 0);

  return {
    timezone,
    now,
    launchDate,
    daysSinceLaunch: rawDays,
    businessDaysSinceLaunch: Math.max(1, businessDays),
    isWeekend: now.weekday >= 6,
    totalOutboundToday,
  };
}

export async function gatherCurrentMetrics(dealId) {
  const sb = getSupabase();
  if (!sb || !dealId) {
    return {
      li_invites_today: 0,
      emails_sent_today: 0,
      dms_sent_today: 0,
      li_pending: 0,
      firms_in_pipeline: 0,
      meetings_booked: 0,
      total_replies: 0,
      pending_approvals: 0,
      hours_since_last_li_invite: null,
    };
  }

  const { data: deal } = await sb.from('deals').select('*').eq('id', dealId).limit(1).maybeSingle();
  const timezone = getDealTimezone(deal || {});
  const today = toDayRange(timezone);
  const sevenDaysAgo = DateTime.now().setZone(timezone).minus({ days: 7 }).toUTC().toISO();

  const [
    contacts,
    activityRows,
    pendingApprovalsRes,
    emailRows,
    repliesRows,
    outreachEvents,
  ] = await Promise.all([
    fetchAllRows(() => sb.from('contacts')
      .select('id, deal_id, invite_sent_at, invite_accepted_at, last_email_sent_at, dm_sent_at, last_outreach_at, outreach_channel, pipeline_stage, conversation_state, response_received, last_reply_at, reply_channel, last_meeting_date, meeting_count')
      .eq('deal_id', dealId)),
    fetchAllRows(() => sb.from('activity_log')
      .select('event_type, created_at')
      .eq('deal_id', dealId)
      .gte('created_at', today.start)
      .lte('created_at', today.end)),
    sb.from('approval_queue')
      .select('id', { count: 'exact', head: true })
      .eq('deal_id', dealId)
      .in('status', ACTIVE_APPROVAL_STATUSES),
    fetchAllRows(() => sb.from('emails')
      .select('id, deal_id, status')
      .eq('deal_id', dealId)
      .eq('status', 'sent')),
    fetchAllRows(() => sb.from('replies')
      .select('deal_id, contact_id, channel')
      .eq('deal_id', dealId)),
    fetchAllRows(() => sb.from('outreach_events')
      .select('deal_id, contact_id, event_type, status, created_at')
      .eq('deal_id', dealId)
      .in('event_type', ['EMAIL_SENT', 'LINKEDIN_INVITE_SENT', 'LINKEDIN_DM_SENT'])),
  ]);

  const activities = activityRows || [];
  const pendingApprovals = Number(pendingApprovalsRes.count || 0);

  const inRange = (value, rangeStart = today.start, rangeEnd = today.end) => {
    if (!value) return false;
    const ts = DateTime.fromISO(String(value), { zone: 'utc' }).setZone(timezone);
    return ts.isValid
      && ts >= DateTime.fromISO(rangeStart).setZone(timezone)
      && ts <= DateTime.fromISO(rangeEnd).setZone(timezone);
  };

  const confirmedEvents = (outreachEvents || []).filter(row =>
    String(row.status || 'confirmed').toLowerCase() === 'confirmed'
  );
  const hasEmailEventCoverage = confirmedEvents.some(row => row.event_type === 'EMAIL_SENT');
  const hasInviteEventCoverage = confirmedEvents.some(row => row.event_type === 'LINKEDIN_INVITE_SENT');
  const hasDmEventCoverage = confirmedEvents.some(row => row.event_type === 'LINKEDIN_DM_SENT');

  const emailsSent = hasEmailEventCoverage
    ? confirmedEvents.filter(row => row.event_type === 'EMAIL_SENT').length
    : (emailRows || []).length;
  const liInvitesSent = hasInviteEventCoverage
    ? confirmedEvents.filter(row => row.event_type === 'LINKEDIN_INVITE_SENT').length
    : contacts.filter(hasLinkedInInviteHistory).length;
  const dmsSent = hasDmEventCoverage
    ? confirmedEvents.filter(row => row.event_type === 'LINKEDIN_DM_SENT').length
    : contacts.filter(row =>
      row.dm_sent_at || ['DM Approved', 'DM Sent', 'dm_sent', 'Replied', 'In Conversation', 'Meeting Booked', 'Meeting Scheduled'].includes(row.pipeline_stage)
    ).length;

  const emailsSentToday = hasEmailEventCoverage
    ? confirmedEvents.filter(row => row.event_type === 'EMAIL_SENT' && inRange(row.created_at)).length
    : contacts.filter(row => inRange(row.last_email_sent_at)).length;
  const liInvitesToday = hasInviteEventCoverage
    ? confirmedEvents.filter(row => row.event_type === 'LINKEDIN_INVITE_SENT' && inRange(row.created_at)).length
    : contacts.filter(row => inRange(row.invite_sent_at)).length;
  const dmsSentToday = hasDmEventCoverage
    ? confirmedEvents.filter(row => row.event_type === 'LINKEDIN_DM_SENT' && inRange(row.created_at)).length
    : contacts.filter(row => inRange(row.dm_sent_at)).length;

  const emailsSentLast7 = hasEmailEventCoverage
    ? confirmedEvents.filter(row => row.event_type === 'EMAIL_SENT' && inRange(row.created_at, sevenDaysAgo, today.end)).length
    : contacts.filter(row => inRange(row.last_email_sent_at, sevenDaysAgo, today.end)).length;
  const liInvitesLast7 = hasInviteEventCoverage
    ? confirmedEvents.filter(row => row.event_type === 'LINKEDIN_INVITE_SENT' && inRange(row.created_at, sevenDaysAgo, today.end)).length
    : contacts.filter(row => inRange(row.invite_sent_at, sevenDaysAgo, today.end)).length;
  const dmsSentLast7 = hasDmEventCoverage
    ? confirmedEvents.filter(row => row.event_type === 'LINKEDIN_DM_SENT' && inRange(row.created_at, sevenDaysAgo, today.end)).length
    : contacts.filter(row => inRange(row.dm_sent_at, sevenDaysAgo, today.end)).length;

  const liPending = contacts.filter(hasActivePendingLinkedInInvite).length;
  const liAccepted = contacts.filter(hasLinkedInAccepted).length;
  const firmsInPipeline = contacts.filter(row => {
    const stage = String(row.pipeline_stage || '').toLowerCase();
    return !['inactive', 'archived', 'declined', 'suppressed - opt out', 'suppressed', 'do_not_contact'].includes(stage);
  }).length;
  const meetingsBooked = contacts.filter(row => {
    const stage = String(row.pipeline_stage || '').toLowerCase();
    return row.last_meeting_date || Number(row.meeting_count) > 0 || stage.includes('meeting');
  }).length;

  const seenEmailReplies = new Set();
  const seenLinkedInReplies = new Set();
  const seenEmailRepliesLast7 = new Set();
  const seenLinkedInRepliesLast7 = new Set();
  let emailReplies = 0;
  let liDmReplies = 0;
  let emailRepliesLast7 = 0;
  let liDmRepliesLast7 = 0;

  const addReply = (channel, key, at = null) => {
    const normalized = String(channel || '').trim().toLowerCase();
    if (!key) return;
    if (normalized === 'email') {
      if (!seenEmailReplies.has(key)) {
        seenEmailReplies.add(key);
        emailReplies += 1;
      }
      if (at && inRange(at, sevenDaysAgo, today.end) && !seenEmailRepliesLast7.has(key)) {
        seenEmailRepliesLast7.add(key);
        emailRepliesLast7 += 1;
      }
    } else if (normalized === 'linkedin') {
      if (!seenLinkedInReplies.has(key)) {
        seenLinkedInReplies.add(key);
        liDmReplies += 1;
      }
      if (at && inRange(at, sevenDaysAgo, today.end) && !seenLinkedInRepliesLast7.has(key)) {
        seenLinkedInRepliesLast7.add(key);
        liDmRepliesLast7 += 1;
      }
    }
  };

  for (const row of contacts) {
    const replySignal = row.response_received === true || Boolean(row.last_reply_at);
    if (!replySignal) continue;
    addReply(row.reply_channel, `${row.deal_id}:${row.id}`, row.last_reply_at);
  }

  for (const row of repliesRows || []) {
    addReply(row.channel, `${row.deal_id}:${row.contact_id || `reply:${row.channel || 'unknown'}`}`);
  }

  const totalReplies = emailReplies + liDmReplies;
  const repliesLast7 = emailRepliesLast7 + liDmRepliesLast7;

  // Positive replies = contacts who have replied and are NOT in a terminal negative state
  const positiveReplies = contacts.filter(row => {
    if (!row.last_reply_at && !row.reply_channel) return false;
    const stage = String(row.pipeline_stage || '').toLowerCase();
    const convState = String(row.conversation_state || '').toLowerCase();
    if (['inactive', 'declined', 'do_not_contact', 'archived', 'suppressed'].includes(stage)) return false;
    if (['conversation_ended_negative', 'do_not_contact'].includes(convState)) return false;
    return true;
  }).length;

  const lastInviteAt = contacts
    .map(row => row.invite_sent_at)
    .filter(Boolean)
    .map(value => DateTime.fromISO(value, { zone: 'utc' }))
    .filter(ts => ts.isValid)
    .sort((a, b) => b.toMillis() - a.toMillis())[0] || null;

  const hoursSinceLastLiInvite = lastInviteAt
    ? Math.max(0, Math.round(DateTime.utc().diff(lastInviteAt, 'hours').hours))
    : null;

  return {
    li_invites_today: liInvitesToday,
    emails_sent_today: emailsSentToday,
    dms_sent_today: dmsSentToday,
    li_pending: liPending,
    li_accepted: liAccepted,
    firms_in_pipeline: firmsInPipeline,
    meetings_booked: meetingsBooked,
    total_replies: totalReplies,
    email_replies: emailReplies,
    li_dm_replies: liDmReplies,
    total_responses: totalReplies + liAccepted,
    pending_approvals: pendingApprovals,
    hours_since_last_li_invite: hoursSinceLastLiInvite,
    activity_events_today: activities.length,
    emails_sent: emailsSent,
    dms_sent: dmsSent,
    li_invites_sent: liInvitesSent,
    emails_sent_last_7_days: emailsSentLast7,
    li_invites_last_7_days: liInvitesLast7,
    dms_sent_last_7_days: dmsSentLast7,
    replies_last_7_days: repliesLast7,
    email_replies_last_7_days: emailRepliesLast7,
    li_dm_replies_last_7_days: liDmRepliesLast7,
    email_response_rate: emailsSent > 0 ? Math.round((emailReplies / emailsSent) * 100) : 0,
    li_dm_response_rate: dmsSent > 0 ? Math.round((liDmReplies / dmsSent) * 100) : 0,
    overall_response_rate: (emailsSent + dmsSent) > 0 ? Math.round((totalReplies / (emailsSent + dmsSent)) * 100) : 0,
    response_rate: emailsSent > 0 ? Math.round((emailReplies / emailsSent) * 100) : 0,
    positive_replies: positiveReplies,
    positive_reply_rate: (() => {
      const sent = emailsSent + dmsSent;
      return sent > 0 ? Math.round((positiveReplies / sent) * 100) : 0;
    })(),
  };
}

export function calculateGoalTracking(deal = {}, metrics = {}) {
  const targetEquity = pickTargetEquity(deal);
  const meetingsNeeded = pickMeetingsNeeded(deal, targetEquity);
  const meetingsBooked = normalizeNumber(metrics.meetings_booked, 0);
  const firmsInPipeline = normalizeNumber(metrics.firms_in_pipeline, 0);
  const liPending = normalizeNumber(metrics.li_pending, 0);
  const totalReplies = normalizeNumber(metrics.total_replies, 0);
  const timing = getTimingContext(deal, metrics);
  const earlyLaunch = timing.businessDaysSinceLaunch <= 2;
  const isFreshWeekend = timing.isWeekend && timing.businessDaysSinceLaunch <= 2;
  const waitingForResponses = liPending >= 5 && totalReplies === 0 && timing.businessDaysSinceLaunch <= 4;

  let status = 'ON TRACK';
  let rationale = 'Pipeline and meeting pace are acceptable.';
  if (meetingsBooked >= meetingsNeeded) {
    status = 'ON TARGET';
    rationale = 'Meeting target has already been reached.';
  } else if (isFreshWeekend && firmsInPipeline >= 10) {
    status = 'BUILDING';
    rationale = 'The deal is newly live and it is the weekend, so reply latency is expected.';
  } else if (earlyLaunch && (timing.totalOutboundToday > 0 || liPending > 0 || firmsInPipeline >= 10)) {
    status = 'BUILDING';
    rationale = 'The deal is still in its first working days, so outreach is still compounding.';
  } else if (waitingForResponses) {
    status = 'WAITING';
    rationale = 'Outreach has already started and the team is still waiting for normal reply lag to clear.';
  } else if (meetingsBooked <= 0 && firmsInPipeline < 10 && timing.businessDaysSinceLaunch > 3) {
    status = 'CRITICAL';
    rationale = 'There are too few active firms after the initial launch window.';
  }
  else if (meetingsBooked < Math.max(2, Math.ceil(meetingsNeeded * 0.25))) status = 'BEHIND';
  else rationale = 'The deal needs more meetings to reach the current pace target.';

  return {
    status,
    rationale,
    target_equity: targetEquity,
    meetings_needed: meetingsNeeded,
    meetings_booked: meetingsBooked,
    firms_in_pipeline: firmsInPipeline,
    progress_ratio: meetingsNeeded > 0 ? Number((meetingsBooked / meetingsNeeded).toFixed(2)) : 0,
    launch_date: timing.launchDate.toISO(),
    days_since_launch: timing.daysSinceLaunch,
    business_days_since_launch: timing.businessDaysSinceLaunch,
    is_weekend_local: timing.isWeekend,
    total_outbound_today: timing.totalOutboundToday,
  };
}

export async function runFundraiserReasoning(deal, context = {}, pushActivity = () => {}) {
  const metrics = context.metrics || await gatherCurrentMetrics(deal?.id);
  const goalAnalysis = calculateGoalTracking(deal, metrics);
  const lowPipeline = normalizeNumber(metrics.firms_in_pipeline, 0) < 25;
  const waitingOnReplies = normalizeNumber(metrics.li_pending, 0) > 40 && normalizeNumber(metrics.firms_in_pipeline, 0) >= 100;

  const directives = {
    allowResearch: true,
    allowOutreach: !waitingOnReplies,
    allowFollowUps: true,
    researchReason: lowPipeline ? 'Pipeline needs more qualified firms' : 'Maintain research flow',
    outreachReason: waitingOnReplies ? 'Waiting on outstanding LinkedIn accepts and replies before adding more outbound' : 'Outreach can proceed',
    followUpReason: 'Follow-ups remain active',
  };

  const actionPlan = [
    `HONEST ASSESSMENT: ${deal?.name || 'Deal'} is ${goalAnalysis.status}.`,
    `TIMING CONTEXT: Day ${goalAnalysis.days_since_launch + 1} since launch, ${goalAnalysis.business_days_since_launch} business day(s) in market${goalAnalysis.is_weekend_local ? ', and it is currently the weekend locally' : ''}.`,
    `TODAY'S 3 PRIORITIES: ${lowPipeline ? 'Expand the active pipeline' : 'Work the highest-conviction firms'}; convert warm conversations; keep approvals moving.`,
    `PATIENCE CHECK: ${normalizeNumber(metrics.firms_in_pipeline, 0)} firms active and ${normalizeNumber(metrics.li_pending, 0)} pending LinkedIn connections.`,
    `WHAT DOM SHOULD KNOW: ${normalizeNumber(metrics.meetings_booked, 0)}/${goalAnalysis.meetings_needed} meetings booked against the current working target. ${goalAnalysis.rationale}`,
  ].join('\n\n');

  return { directives, goalAnalysis, actionPlan };
}
