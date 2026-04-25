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

  const [
    contactsRes,
    activityRes,
    msgRes,
    pendingApprovalsRes,
  ] = await Promise.all([
    sb.from('contacts')
      .select('id, invite_sent_at, invite_accepted_at, last_email_sent_at, dm_sent_at, last_outreach_at, pipeline_stage, last_reply_at, reply_channel, last_meeting_date, meeting_count')
      .eq('deal_id', dealId),
    sb.from('activity_log')
      .select('event_type, created_at')
      .eq('deal_id', dealId)
      .gte('created_at', today.start)
      .lte('created_at', today.end),
    sb.from('conversation_messages')
      .select('contact_id, direction, channel, received_at')
      .eq('deal_id', dealId)
      .eq('direction', 'inbound'),
    sb.from('approval_queue')
      .select('id', { count: 'exact', head: true })
      .eq('deal_id', dealId)
      .eq('status', 'pending'),
  ]);

  const contacts = contactsRes.data || [];
  const activities = activityRes.data || [];
  const inboundMessages = msgRes.data || [];
  const pendingApprovals = Number(pendingApprovalsRes.count || 0);

  const liInvitesToday = contacts.filter(row => {
    const sentAt = row.invite_sent_at || null;
    if (!sentAt) return false;
    const ts = DateTime.fromISO(sentAt, { zone: 'utc' }).setZone(timezone);
    return ts.isValid && ts >= DateTime.fromISO(today.start).setZone(timezone) && ts <= DateTime.fromISO(today.end).setZone(timezone);
  }).length;

  const emailsSentToday = contacts.filter(row => {
    const sentAt = row.last_email_sent_at || null;
    if (!sentAt) return false;
    const ts = DateTime.fromISO(sentAt, { zone: 'utc' }).setZone(timezone);
    return ts.isValid && ts >= DateTime.fromISO(today.start).setZone(timezone) && ts <= DateTime.fromISO(today.end).setZone(timezone);
  }).length;

  const dmsSentToday = contacts.filter(row => {
    const sentAt = row.dm_sent_at || null;
    if (!sentAt) return false;
    const ts = DateTime.fromISO(sentAt, { zone: 'utc' }).setZone(timezone);
    return ts.isValid && ts >= DateTime.fromISO(today.start).setZone(timezone) && ts <= DateTime.fromISO(today.end).setZone(timezone);
  }).length;

  // Use pipeline_stage as source of truth — invite_accepted_at was historically unreliable
  const liPending = contacts.filter(row => String(row.pipeline_stage || '').toLowerCase() === 'invite_sent').length;
  const liAccepted = contacts.filter(row => row.invite_accepted_at || ['dm sent','dm approved','pending_dm_approval','in conversation','invite_accepted'].includes(String(row.pipeline_stage || '').toLowerCase())).length;
  const firmsInPipeline = contacts.filter(row => {
    const stage = String(row.pipeline_stage || '').toLowerCase();
    return !['inactive', 'archived', 'declined', 'suppressed - opt out', 'suppressed', 'do_not_contact'].includes(stage);
  }).length;
  const meetingsBooked = contacts.filter(row => {
    const stage = String(row.pipeline_stage || '').toLowerCase();
    return row.last_meeting_date || Number(row.meeting_count) > 0 || stage.includes('meeting');
  }).length;
  // Deduplicated reply count: union of contacts flagged as replied + distinct contact_ids in inbound messages
  const repliedContactIds = new Set(contacts.filter(row => row.last_reply_at || row.reply_channel).map(row => row.id).filter(Boolean));
  inboundMessages.forEach(msg => { if (msg.contact_id) repliedContactIds.add(msg.contact_id); });
  const totalReplies = repliedContactIds.size;

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
    pending_approvals: pendingApprovals,
    hours_since_last_li_invite: hoursSinceLastLiInvite,
    activity_events_today: activities.length,
    emails_sent: contacts.filter(row => row.last_email_sent_at).length,
    dms_sent: contacts.filter(row => row.dm_sent_at).length,
    // Stage-based count — invite_sent_at was historically unreliable
    li_invites_sent: contacts.filter(row => {
      if (row.invite_sent_at) return true;
      const stage = String(row.pipeline_stage || '').toLowerCase();
      return ['invite_sent', 'invite_accepted', 'pending_dm_approval', 'dm approved', 'dm sent', 'in conversation'].includes(stage);
    }).length,
    response_rate: (() => {
      const replied = contacts.filter(row => row.last_reply_at || row.reply_channel).length;
      const sent = contacts.filter(row => row.last_email_sent_at || row.dm_sent_at).length;
      return sent > 0 ? Math.round((replied / sent) * 100) : 0;
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
