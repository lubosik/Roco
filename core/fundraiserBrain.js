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
  ] = await Promise.all([
    sb.from('contacts')
      .select('invite_sent_at, invite_accepted_at, last_email_sent_at, dm_sent_at, last_outreach_at, pipeline_stage, last_reply_at, reply_channel, linkedin_connected, meeting_booked_at')
      .eq('deal_id', dealId),
    sb.from('activity_log')
      .select('event_type, created_at')
      .eq('deal_id', dealId)
      .gte('created_at', today.start)
      .lte('created_at', today.end),
    sb.from('conversation_messages')
      .select('direction, channel, received_at')
      .eq('deal_id', dealId)
      .eq('direction', 'inbound'),
  ]);

  const contacts = contactsRes.data || [];
  const activities = activityRes.data || [];
  const inboundMessages = msgRes.data || [];

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

  const liPending = contacts.filter(row => row.invite_sent_at && !row.invite_accepted_at).length;
  const firmsInPipeline = contacts.filter(row => {
    const stage = String(row.pipeline_stage || '').toLowerCase();
    return !['inactive', 'archived', 'declined', 'suppressed - opt out', 'suppressed', 'do_not_contact'].includes(stage);
  }).length;
  const meetingsBooked = contacts.filter(row => {
    const stage = String(row.pipeline_stage || '').toLowerCase();
    return row.meeting_booked_at || stage.includes('meeting');
  }).length;
  const totalReplies = contacts.filter(row => row.last_reply_at || row.reply_channel).length + inboundMessages.length;

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
    firms_in_pipeline: firmsInPipeline,
    meetings_booked: meetingsBooked,
    total_replies: totalReplies,
    hours_since_last_li_invite: hoursSinceLastLiInvite,
    activity_events_today: activities.length,
  };
}

export function calculateGoalTracking(deal = {}, metrics = {}) {
  const targetEquity = pickTargetEquity(deal);
  const meetingsNeeded = pickMeetingsNeeded(deal, targetEquity);
  const meetingsBooked = normalizeNumber(metrics.meetings_booked, 0);
  const firmsInPipeline = normalizeNumber(metrics.firms_in_pipeline, 0);

  let status = 'ON TRACK';
  if (meetingsBooked <= 0 && firmsInPipeline < 10) status = 'CRITICAL';
  else if (meetingsBooked < Math.max(2, Math.ceil(meetingsNeeded * 0.25))) status = 'BEHIND';
  else if (meetingsBooked >= meetingsNeeded) status = 'ON TARGET';

  return {
    status,
    target_equity: targetEquity,
    meetings_needed: meetingsNeeded,
    meetings_booked: meetingsBooked,
    firms_in_pipeline: firmsInPipeline,
    progress_ratio: meetingsNeeded > 0 ? Number((meetingsBooked / meetingsNeeded).toFixed(2)) : 0,
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
    `TODAY'S 3 PRIORITIES: ${lowPipeline ? 'Expand the active pipeline' : 'Work the highest-conviction firms'}; convert warm conversations; keep approvals moving.`,
    `PATIENCE CHECK: ${normalizeNumber(metrics.firms_in_pipeline, 0)} firms active and ${normalizeNumber(metrics.li_pending, 0)} pending LinkedIn connections.`,
    `WHAT DOM SHOULD KNOW: ${normalizeNumber(metrics.meetings_booked, 0)}/${goalAnalysis.meetings_needed} meetings booked against the current working target.`,
  ].join('\n\n');

  pushActivity({
    type: 'analysis',
    action: `Fundraiser reasoning: ${deal?.name || 'Deal'}`,
    note: `${goalAnalysis.status} · ${normalizeNumber(metrics.firms_in_pipeline, 0)} firms active · ${normalizeNumber(metrics.meetings_booked, 0)} meetings booked`,
    deal_id: deal?.id || null,
  });

  return { directives, goalAnalysis, actionPlan };
}

export async function sendMorningBrief(deal, actionPlan, goalAnalysis, metrics, sendTelegram) {
  if (typeof sendTelegram !== 'function') return;
  const message = [
    `*Morning Brief — ${deal?.name || 'Deal'}*`,
    `Status: ${goalAnalysis?.status || 'IN PROGRESS'}`,
    `Pipeline: ${normalizeNumber(metrics?.firms_in_pipeline, 0)} firms active`,
    `Meetings: ${normalizeNumber(metrics?.meetings_booked, 0)}/${normalizeNumber(goalAnalysis?.meetings_needed, 0)}`,
    '',
    String(actionPlan || '').slice(0, 1200),
  ].join('\n');
  await sendTelegram(message);
}
