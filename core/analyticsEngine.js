// core/analyticsEngine.js
// Weekly analytics computation + recommendation generation.
// Accepted recommendations update roco_learned_settings to influence agent behaviour.

import Anthropic from '@anthropic-ai/sdk';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { DateTime } from 'luxon';
import { getSupabase } from './supabase.js';
import { gatherCurrentMetrics } from './fundraiserBrain.js';

const ANALYTICS_TIMEZONE = 'America/New_York';
const ELEVENLABS_BASE_URL = 'https://api.elevenlabs.io/v1';

function getAnthropicClient() {
  return process.env.ANTHROPIC_API_KEY
    ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
    : null;
}

function getElevenLabsConfig() {
  return {
    apiKey: process.env.ELEVENLABS_API_KEY || process.env.XI_API_KEY || null,
    modelId: process.env.ELEVENLABS_MODEL_ID || 'eleven_multilingual_v2',
    defaultVoiceId: process.env.ELEVENLABS_VOICE_ID || 'JBFqnCBsd6RMkjVDRZzb',
    voiceName: process.env.ELEVENLABS_VOICE_NAME || 'Configured voice',
  };
}

export function getAnalyticsWindow(reference = DateTime.now().setZone(ANALYTICS_TIMEZONE)) {
  const end = reference.endOf('minute');
  const start = end.minus({ days: 7 }).startOf('day');
  return {
    start,
    end,
    weekStarting: start.toISODate(),
    timezone: ANALYTICS_TIMEZONE,
  };
}

function getDealAnalyticsTimezone(deal) {
  return deal?.timezone || deal?.sending_timezone || ANALYTICS_TIMEZONE;
}

function safeIso(value) {
  if (!value) return null;
  if (DateTime.isDateTime(value)) return value.toUTC().toISO();
  return DateTime.fromJSDate(new Date(value)).toUTC().toISO();
}

function sortByCountDesc(entries = []) {
  return [...entries].sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0));
}

function deriveBestValue(counts) {
  return sortByCountDesc(Object.entries(counts))[0]?.[0] ?? null;
}

function parseClaudeJson(text) {
  const cleaned = String(text || '').replace(/```json|```/g, '').trim();
  const match = cleaned.match(/\[[\s\S]*\]/);
  if (!match) return null;
  return JSON.parse(match[0]);
}

function extractJSONObject(text) {
  const cleaned = String(text || '').replace(/```json|```/g, '').trim();
  const match = cleaned.match(/\{[\s\S]*\}/);
  return match ? JSON.parse(match[0]) : null;
}

function normalizeWhitespace(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function sanitizeNarrationText(value) {
  return normalizeWhitespace(value)
    .replace(/https?:\/\/\S+/gi, '')
    .replace(/\b(ACo|ACw|AE)[A-Za-z0-9_-]+\b/g, '')
    .replace(/\bcannot_resend_yet\b/gi, 'temporary LinkedIn limit')
    .replace(/\busers\/invite\b/gi, 'LinkedIn invite send')
    .replace(/\bapi\/v1\/\S+\b/gi, 'API call')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function truncate(value, max = 200) {
  const text = normalizeWhitespace(value);
  if (!text) return '';
  return text.length > max ? `${text.slice(0, max - 1)}...` : text;
}

function buildTimingRecommendation(analyticsData) {
  const hourCounts = {};
  const dayCounts = {};

  analyticsData.forEach(row => {
    if (row.best_response_hour != null) {
      const key = String(row.best_response_hour);
      hourCounts[key] = (hourCounts[key] || 0) + 1;
    }
    if (row.best_response_day != null) {
      const key = String(row.best_response_day);
      dayCounts[key] = (dayCounts[key] || 0) + 1;
    }
  });

  const bestHour = deriveBestValue(hourCounts);
  const bestDay = deriveBestValue(dayCounts);
  if (bestHour == null && bestDay == null) return null;

  const dayLabel = bestDay != null
    ? ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][Number(bestDay) - 1] || bestDay
    : null;
  const parts = [];
  if (dayLabel) parts.push(dayLabel);
  if (bestHour != null) parts.push(`${bestHour}:00`);

  return {
    category: 'timing',
    title: 'Lean Into Best Window',
    insight: `Replies cluster most often around ${parts.join(' at ') || 'the same repeat window'} in America/New_York time.`,
    recommendation: `Bias new outreach toward ${parts.join(' at ') || 'that window'} and avoid spreading sends evenly across lower-response periods.`,
    supporting_data: {
      best_day: dayLabel,
      best_hour: bestHour != null ? Number(bestHour) : null,
    },
    suggested_setting_change: bestHour != null
      ? { key: 'preferred_outreach_hour_est', value: String(bestHour) }
      : null,
  };
}

function buildChannelRecommendation(analyticsData) {
  const totals = analyticsData.reduce((acc, row) => {
    acc.emailSent += Number(row.emails_sent || 0);
    acc.emailReplies += Number(row.email_replies || 0);
    acc.linkedinSent += Number(row.linkedin_invites_sent || 0) + Number(row.linkedin_dms_sent || 0);
    acc.linkedinReplies += Number(row.linkedin_replies || 0);
    return acc;
  }, {
    emailSent: 0,
    emailReplies: 0,
    linkedinSent: 0,
    linkedinReplies: 0,
  });

  if (!totals.emailSent && !totals.linkedinSent) return null;

  const emailRate = totals.emailSent ? totals.emailReplies / totals.emailSent : 0;
  const linkedinRate = totals.linkedinSent ? totals.linkedinReplies / totals.linkedinSent : 0;
  const winner = emailRate >= linkedinRate ? 'email' : 'LinkedIn';
  const winnerRate = winner === 'email' ? emailRate : linkedinRate;
  const loserRate = winner === 'email' ? linkedinRate : emailRate;

  return {
    category: 'channel',
    title: 'Shift Toward Winning Channel',
    insight: `${winner} is converting better this week (${(winnerRate * 100).toFixed(1)}% vs ${(loserRate * 100).toFixed(1)}%).`,
    recommendation: `Prioritize ${winner} earlier in the sequence for similar deals until the weaker channel improves.`,
    supporting_data: {
      email_rate: Number(emailRate.toFixed(4)),
      linkedin_rate: Number(linkedinRate.toFixed(4)),
    },
    suggested_setting_change: {
      key: 'preferred_primary_channel',
      value: winner === 'email' ? 'email' : 'linkedin',
    },
  };
}

function buildConversionRecommendation(analyticsData) {
  const totals = analyticsData.reduce((acc, row) => {
    acc.outreach += Number(row.total_outreach || 0);
    acc.replies += Number(row.email_replies || 0) + Number(row.linkedin_replies || 0);
    acc.meetings += Number(row.meetings_booked || 0);
    acc.negative += Number(row.negative_responses || 0);
    return acc;
  }, {
    outreach: 0,
    replies: 0,
    meetings: 0,
    negative: 0,
  });

  if (!totals.outreach) return null;

  const replyRate = totals.replies / totals.outreach;
  const meetingRate = totals.replies ? totals.meetings / totals.replies : 0;
  const negativeShare = totals.replies ? totals.negative / totals.replies : 0;

  return {
    category: negativeShare > 0.35 ? 'targeting' : 'sequence',
    title: negativeShare > 0.35 ? 'Tighten Investor Targeting' : 'Improve Meeting Conversion',
    insight: negativeShare > 0.35
      ? `${(negativeShare * 100).toFixed(1)}% of replies are negative, which suggests the list quality is drifting.`
      : `Reply rate is ${(replyRate * 100).toFixed(1)}%, but only ${(meetingRate * 100).toFixed(1)}% of replies become meetings.`,
    recommendation: negativeShare > 0.35
      ? 'Raise the investor quality bar and lean harder on firms already showing positive signals in this sector.'
      : 'Tighten the follow-up CTA and move qualified respondents to call-booking faster while interest is warm.',
    supporting_data: {
      overall_reply_rate: Number(replyRate.toFixed(4)),
      meeting_conversion_rate: Number(meetingRate.toFixed(4)),
      negative_reply_share: Number(negativeShare.toFixed(4)),
    },
    suggested_setting_change: negativeShare > 0.35
      ? { key: 'minimum_investor_score', value: '70' }
      : null,
  };
}

export function buildHeuristicRecommendations(analyticsData = []) {
  return [
    buildTimingRecommendation(analyticsData),
    buildChannelRecommendation(analyticsData),
    buildConversionRecommendation(analyticsData),
  ].filter(Boolean).slice(0, 6);
}

async function fetchActiveDeals(supabase) {
  const { data, error } = await supabase.from('deals').select('*').eq('status', 'ACTIVE');
  if (error) throw error;
  return data || [];
}

export async function computeDealSnapshot(deal, window = getAnalyticsWindow()) {
  const supabase = getSupabase();
  if (!supabase || !deal?.id) return null;

  let query = supabase.from('conversation_messages')
    .select('*')
    .eq('deal_id', deal.id)
    .gte('created_at', safeIso(window.start));

  if (window.end) {
    query = query.lte('created_at', safeIso(window.end));
  }

  const { data: messages, error: messageError } = await query;
  if (messageError) throw messageError;

  const outbound = (messages || []).filter(message => message.direction === 'outbound');
  const inbound = (messages || []).filter(message => message.direction === 'inbound');

  const emailsSent = outbound.filter(message => message.channel === 'email').length;
  const liInvites = outbound.filter(message => message.channel === 'linkedin_invite').length;
  const liDms = outbound.filter(message => message.channel === 'linkedin_dm').length;
  const emailReplies = inbound.filter(message => message.channel === 'email').length;
  const liReplies = inbound.filter(message => message.channel === 'linkedin_dm').length;

  const positiveReplies = inbound.filter(message =>
    ['interested_send_materials', 'interested_schedule_call', 'meeting_booked_confirmed'].includes(message.intent)
  ).length;
  const negativeReplies = inbound.filter(message =>
    ['not_right_fit', 'remove_unsubscribe'].includes(message.intent)
  ).length;
  const tempCloses = inbound.filter(message =>
    ['will_review_get_back', 'hold_period', 'out_of_office'].includes(message.intent)
  ).length;

  const meetingQuery = supabase.from('contacts')
    .select('*', { count: 'exact', head: true })
    .eq('deal_id', deal.id)
    .eq('conversation_state', 'meeting_booked')
    .gte('updated_at', safeIso(window.start));
  const { count: meetings, error: meetingError } = await meetingQuery;
  if (meetingError) throw meetingError;

  const dealTz = getDealAnalyticsTimezone(deal);
  const hourCounts = {};
  const dayCounts = {};
  for (const message of inbound) {
    const timestamp = message.received_at || message.created_at;
    if (!timestamp) continue;
    const localized = DateTime.fromISO(timestamp, { zone: 'utc' }).setZone(dealTz);
    if (!localized.isValid) continue;
    hourCounts[localized.hour] = (hourCounts[localized.hour] || 0) + 1;
    dayCounts[localized.weekday] = (dayCounts[localized.weekday] || 0) + 1;
  }

  const templatePerf = {};
  for (const message of outbound) {
    if (!message.template_name) continue;
    if (!templatePerf[message.template_name]) {
      templatePerf[message.template_name] = { sent: 0, replies: 0 };
    }
    templatePerf[message.template_name].sent += 1;
  }

  const totalOutreach = emailsSent + liInvites + liDms;
  const totalReplies = emailReplies + liReplies;
  const bestHour = deriveBestValue(hourCounts);
  const bestDay = deriveBestValue(dayCounts);

  return {
    deal_id: deal.id,
    deal_name: deal.name,
    timezone: dealTz,
    week_starting: window.weekStarting,
    emails_sent: emailsSent,
    linkedin_invites_sent: liInvites,
    linkedin_dms_sent: liDms,
    total_outreach: totalOutreach,
    email_replies: emailReplies,
    linkedin_replies: liReplies,
    positive_responses: positiveReplies,
    negative_responses: negativeReplies,
    temp_closes: tempCloses,
    meetings_booked: meetings || 0,
    email_response_rate: emailsSent > 0 ? emailReplies / emailsSent : 0,
    linkedin_response_rate: (liInvites + liDms) > 0 ? liReplies / (liInvites + liDms) : 0,
    overall_response_rate: totalOutreach > 0 ? totalReplies / totalOutreach : 0,
    meeting_conversion_rate: totalReplies > 0 ? (meetings || 0) / totalReplies : 0,
    best_response_hour: bestHour != null ? Number(bestHour) : null,
    best_response_day: bestDay != null ? Number(bestDay) : null,
    sector: deal.sector || null,
    deal_type: deal.raise_type || null,
    template_performance: templatePerf,
    source: 'live',
  };
}

export async function getLiveAnalyticsSummary(options = {}) {
  const supabase = getSupabase();
  if (!supabase) return [];

  const window = options.window || getAnalyticsWindow();
  const deals = options.deals || await fetchActiveDeals(supabase);
  const rows = [];

  for (const deal of deals) {
    try {
      const snapshot = await computeDealSnapshot(deal, window);
      if (snapshot && snapshot.total_outreach > 0) rows.push(snapshot);
    } catch (err) {
      console.warn(`[ANALYTICS] Live summary failed for ${deal?.name || deal?.id || 'deal'}:`, err.message);
    }
  }

  return rows.sort((a, b) => Number(b.total_outreach || 0) - Number(a.total_outreach || 0));
}

function toPersistedAnalyticsRow(snapshot) {
  return {
    deal_id: snapshot.deal_id,
    week_starting: snapshot.week_starting,
    emails_sent: snapshot.emails_sent,
    linkedin_invites_sent: snapshot.linkedin_invites_sent,
    linkedin_dms_sent: snapshot.linkedin_dms_sent,
    total_outreach: snapshot.total_outreach,
    email_replies: snapshot.email_replies,
    linkedin_replies: snapshot.linkedin_replies,
    positive_responses: snapshot.positive_responses,
    negative_responses: snapshot.negative_responses,
    temp_closes: snapshot.temp_closes,
    meetings_booked: snapshot.meetings_booked,
    email_response_rate: snapshot.email_response_rate,
    linkedin_response_rate: snapshot.linkedin_response_rate,
    overall_response_rate: snapshot.overall_response_rate,
    meeting_conversion_rate: snapshot.meeting_conversion_rate,
    best_response_hour: snapshot.best_response_hour,
    best_response_day: snapshot.best_response_day,
    sector: snapshot.sector,
    deal_type: snapshot.deal_type,
    timezone: snapshot.timezone,
    template_performance: snapshot.template_performance,
  };
}

async function requestAiRecommendations(analyticsData) {
  const anthropic = getAnthropicClient();
  if (!anthropic) return null;

  const dataStr = JSON.stringify(analyticsData, null, 2);
  const templateData = JSON.stringify(analyticsData.map(row => ({
    deal: row.deal_name || row.deal || 'Unknown deal',
    templates: row.template_performance || {},
  })), null, 2);

  const prompt = `You are analysing outreach performance data for ROCO, an autonomous fundraising agent.

WEEKLY DATA ACROSS ALL ACTIVE DEALS:
${dataStr}

TEMPLATE PERFORMANCE BREAKDOWN:
${templateData}

Analyse this data and generate 3-6 specific, actionable recommendations.
Focus on patterns that would genuinely improve response rates.

Look for:
1. Best sending times (hours/days with highest response rates)
2. Channel performance (email vs LinkedIn — which gets more replies per deal type/sector)
3. Sequence patterns (which follow-up number gets the most responses)
4. Template performance — which templates get the highest reply rates, which are being ignored
5. Subject line patterns (A vs B variants — which drives more opens/replies)
6. Copy patterns in messages that preceded positive responses
7. Sector patterns (do healthcare investors respond differently than PE generalists?)
8. Response intent patterns (what intents are most common — are we attracting the right signals?)

Return ONLY valid JSON array:
[
  {
    "category": "timing|copy|targeting|sequence|channel",
    "title": "Short title (max 8 words)",
    "insight": "What the data shows (1-2 sentences)",
    "recommendation": "Specific action to take (1-2 sentences)",
    "supporting_data": { "key_metric": "value" },
    "suggested_setting_change": { "key": "setting_key", "value": "new_value" }
  }
]
Use null for suggested_setting_change if no direct setting maps to this recommendation.`;

  const response = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 2000,
    messages: [{ role: 'user', content: prompt }],
  });

  return parseClaudeJson(response.content?.[0]?.text || '');
}

async function persistRecommendations(supabase, recommendations, weekStarting, dealsAnalysed) {
  try {
    await supabase.from('roco_recommendations')
      .delete()
      .eq('week_starting', weekStarting)
      .eq('status', 'pending');
  } catch (err) {
    console.warn('[ANALYTICS] Could not clear stale pending recommendations:', err.message);
  }

  let persisted = 0;
  for (const recommendation of recommendations) {
    try {
      const { error } = await supabase.from('roco_recommendations').insert({
        category: recommendation.category,
        title: recommendation.title,
        insight: recommendation.insight,
        recommendation: recommendation.recommendation,
        supporting_data: recommendation.supporting_data || {},
        suggested_setting_change: recommendation.suggested_setting_change || null,
        week_starting: weekStarting,
        deals_analysed: dealsAnalysed,
        status: 'pending',
      });
      if (!error) persisted += 1;
      else console.warn('[ANALYTICS] Recommendation insert failed:', error.message);
    } catch (err) {
      console.warn('[ANALYTICS] Recommendation insert failed:', err.message);
    }
  }

  return persisted;
}

async function generateRecommendations(analyticsData, weekStarting) {
  let recommendations = null;

  try {
    recommendations = await requestAiRecommendations(analyticsData);
  } catch (err) {
    console.warn('[ANALYTICS] AI recommendation generation failed:', err.message);
  }

  if (!Array.isArray(recommendations) || !recommendations.length) {
    recommendations = buildHeuristicRecommendations(analyticsData);
  }

  if (!recommendations.length) {
    return {
      count: 0,
      recommendations: [],
      persisted: 0,
    };
  }

  const supabase = getSupabase();
  const persisted = supabase
    ? await persistRecommendations(supabase, recommendations, weekStarting, analyticsData.length)
    : 0;

  return {
    count: recommendations.length,
    recommendations,
    persisted,
  };
}

export async function getAnalyticsRecommendationsPreview(options = {}) {
  const rows = options.analyticsData?.length
    ? options.analyticsData
    : await getLiveAnalyticsSummary(options);
  return buildHeuristicRecommendations(rows).map((recommendation, index) => ({
    ...recommendation,
    id: `preview-${index + 1}`,
    status: 'preview',
    is_preview: true,
  }));
}

export async function runWeeklyAnalytics(options = {}) {
  const trigger = options.trigger || 'manual';
  const supabase = getSupabase();
  if (!supabase) return { ran: false, reason: 'no_supabase' };

  const window = options.window || getAnalyticsWindow();
  console.log(`[ANALYTICS] Running weekly analysis (${trigger}) for ${window.weekStarting} (${window.timezone})...`);

  const deals = await fetchActiveDeals(supabase);
  if (!deals.length) return { ran: false, reason: 'no_active_deals' };

  const analyticsData = [];
  let analyticsPersisted = 0;

  for (const deal of deals) {
    try {
      const snapshot = await computeDealSnapshot(deal, window);
      if (!snapshot || snapshot.total_outreach === 0) continue;

      analyticsData.push(snapshot);

      try {
        const { error } = await supabase.from('deal_analytics').upsert(
          toPersistedAnalyticsRow(snapshot),
          { onConflict: 'deal_id,week_starting' }
        );
        if (error) console.warn(`[ANALYTICS] Summary upsert failed for ${deal.name}:`, error.message);
        else analyticsPersisted += 1;
      } catch (err) {
        console.warn(`[ANALYTICS] Summary upsert failed for ${deal.name}:`, err.message);
      }
    } catch (err) {
      console.warn(`[ANALYTICS] Snapshot failed for ${deal.name}:`, err.message);
    }
  }

  if (!analyticsData.length) {
    console.log('[ANALYTICS] No outreach activity in the last 7 days — skipping recommendations');
    return {
      ran: true,
      analyticsCount: 0,
      analyticsPersisted,
      recommendationsGenerated: 0,
      recommendationsPersisted: 0,
      weekStarting: window.weekStarting,
    };
  }

  const recommendationResult = await generateRecommendations(analyticsData, window.weekStarting);
  const hasDashboardData = analyticsPersisted > 0 && recommendationResult.persisted > 0;

  if (hasDashboardData) {
    try {
      const { pushActivity } = await import('../dashboard/server.js');
      pushActivity({
        type: 'system',
        action: trigger === 'manual' ? 'Analytics complete' : 'Weekly analytics complete',
        note: `Recommendations generated for ${analyticsData.length} active deal${analyticsData.length !== 1 ? 's' : ''} — check the Analytics tab`,
      });
    } catch {}

    try {
      const { sendTelegram } = await import('../approval/telegramBot.js');
      const dealNames = analyticsData.map(row => row.deal_name).join(', ');
      await sendTelegram(
        `📊 *${trigger === 'manual' ? 'Analytics Complete' : 'Weekly Analytics Complete'}*\n\nNew recommendations generated for: ${dealNames}\n\nCheck the Analytics tab in the dashboard to review and apply them.`
      );
    } catch {}
  } else {
    console.warn('[ANALYTICS] Skipping external completion notification because analytics data was not fully persisted');
  }

  console.log(`[ANALYTICS] Weekly analysis complete (${trigger})`);
  return {
    ran: true,
    analyticsCount: analyticsData.length,
    analyticsPersisted,
    recommendationsGenerated: recommendationResult.count,
    recommendationsPersisted: recommendationResult.persisted,
    weekStarting: window.weekStarting,
    hasDashboardData,
  };
}

export async function applyRecommendation(recommendationId) {
  const supabase = getSupabase();
  if (!supabase) return;

  const { data: recommendation } = await supabase.from('roco_recommendations')
    .select('*').eq('id', recommendationId).single();

  if (!recommendation || recommendation.status !== 'pending') return;

  if (recommendation.suggested_setting_change) {
    const { key, value } = recommendation.suggested_setting_change;
    await supabase.from('roco_learned_settings').upsert({
      key,
      value: String(value),
      source_recommendation_id: recommendationId,
      applied_at: new Date().toISOString(),
    }, { onConflict: 'key' });

    try {
      const { invalidateCache } = await import('./agentContext.js');
      invalidateCache();
    } catch {}
  }

  await supabase.from('roco_recommendations').update({
    status: 'applied',
    applied_at: new Date().toISOString(),
  }).eq('id', recommendationId);

  console.log(`[ANALYTICS] Applied recommendation: ${recommendation.title}`);

  try {
    const { pushActivity } = await import('../dashboard/server.js');
    pushActivity({
      type: 'system',
      action: `Recommendation applied: ${recommendation.title}`,
      note: recommendation.recommendation || '',
    });
  } catch {}
}

export function getDailyActivityWindow(reference = DateTime.now().setZone(ANALYTICS_TIMEZONE)) {
  const end = reference.endOf('minute');
  const start = reference.startOf('day');
  return {
    start,
    end,
    reportDate: reference.toISODate(),
    timezone: ANALYTICS_TIMEZONE,
  };
}

async function fetchDailyActivityEntries(window) {
  const supabase = getSupabase();
  if (!supabase) return [];

  const { data, error } = await supabase.from('activity_log')
    .select('*')
    .gte('created_at', safeIso(window.start))
    .lte('created_at', safeIso(window.end))
    .order('created_at', { ascending: false });

  if (error) throw error;
  return data || [];
}

async function fetchDealsForDailyLog(referenceDeals = [], activityEntries = []) {
  const supabase = getSupabase();
  const mapped = new Map((referenceDeals || []).filter(Boolean).map(deal => [String(deal.id), deal]));
  const activityDealIds = [...new Set((activityEntries || []).map(entry => entry.deal_id).filter(Boolean).map(String))];
  const missingIds = activityDealIds.filter(id => !mapped.has(id));

  if (!supabase || !missingIds.length) return [...mapped.values()];

  const { data } = await supabase.from('deals').select('*').in('id', missingIds);
  for (const deal of data || []) mapped.set(String(deal.id), deal);
  return [...mapped.values()];
}

function summarizeDailyActionCounts(entries = []) {
  return entries.reduce((acc, entry) => {
    const key = String(entry.type || entry.event_type || 'system').toLowerCase();
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
}

async function fetchDailyConversationEntries(window) {
  const supabase = getSupabase();
  if (!supabase) return [];

  const { data, error } = await supabase.from('conversation_messages')
    .select('*')
    .gte('created_at', safeIso(window.start))
    .lte('created_at', safeIso(window.end))
    .order('created_at', { ascending: false });

  if (error) throw error;
  return data || [];
}

async function fetchDailySentEmailEntries(window) {
  const supabase = getSupabase();
  if (!supabase) return [];

  const { data, error } = await supabase.from('emails')
    .select('id, deal_id, contact_id, sent_at, status, direction')
    .eq('status', 'sent')
    .eq('direction', 'outbound')
    .gte('sent_at', safeIso(window.start))
    .lte('sent_at', safeIso(window.end))
    .order('sent_at', { ascending: false });

  if (error) throw error;
  return data || [];
}

function countDealActivityEntries(entries = [], expectedType) {
  return entries.reduce((sum, entry) => (
    String(entry?.event_type || '').toUpperCase() === expectedType ? sum + 1 : sum
  ), 0);
}

function countDealActivityPattern(entries = [], pattern) {
  return entries.reduce((sum, entry) => (
    pattern.test(String(entry?.event_type || '').toUpperCase()) ? sum + 1 : sum
  ), 0);
}

function buildPerDealDailyMetrics(dealId, activityEntries = [], conversationEntries = [], sentEmailEntries = [], currentMetrics = {}) {
  const key = String(dealId || '');
  const dealActivity = activityEntries.filter(entry => String(entry?.deal_id || '') === key);
  const dealEmails = sentEmailEntries.filter(entry => String(entry?.deal_id || '') === key);
  const outbound = conversationEntries.filter(entry =>
    String(entry?.deal_id || '') === key && String(entry?.direction || '').toLowerCase() === 'outbound'
  );
  const inbound = conversationEntries.filter(entry =>
    String(entry?.deal_id || '') === key && String(entry?.direction || '').toLowerCase() === 'inbound'
  );

  const emailActivityCount = countDealActivityEntries(dealActivity, 'EMAIL_SENT');
  const emailConversationCount = outbound.filter(entry => String(entry?.channel || '').toLowerCase() === 'email').length;
  const emailTableCount = dealEmails.length;

  const liInviteActivityCount = countDealActivityEntries(dealActivity, 'LINKEDIN_INVITE_SENT');
  const liInviteCurrentCount = Number(currentMetrics?.li_invites_today || 0);

  const dmActivityCount = countDealActivityEntries(dealActivity, 'LINKEDIN_DM_SENT');
  const dmConversationCount = outbound.filter(entry => String(entry?.channel || '').toLowerCase() === 'linkedin_dm').length;
  const dmCurrentCount = Number(currentMetrics?.dms_sent_today || 0);

  return {
    li_invites_today: Math.max(liInviteActivityCount, liInviteCurrentCount),
    emails_sent_today: Math.max(emailActivityCount, emailConversationCount, emailTableCount, Number(currentMetrics?.emails_sent_today || 0)),
    dms_sent_today: Math.max(dmActivityCount, dmConversationCount, dmCurrentCount),
    enrichment_actions: countDealActivityPattern(dealActivity, /(ENRICH|RESEARCH_COMPLETE)/),
    invite_failures: countDealActivityPattern(dealActivity, /LINKEDIN_INVITE_FAILED/),
    missing_linkedin: countDealActivityPattern(dealActivity, /LINKEDIN_INVITE_SKIPPED_NO_PROFILE/),
    provider_limit_deferrals: countDealActivityPattern(dealActivity, /LINKEDIN_INVITE_PROVIDER_LIMIT/),
    total_replies: inbound.length,
    email_replies: inbound.filter(entry => String(entry?.channel || '').toLowerCase() === 'email').length,
    linkedin_replies: inbound.filter(entry => String(entry?.channel || '').toLowerCase() === 'linkedin_dm').length,
    positive_replies: inbound.filter(entry =>
      ['interested_send_materials', 'interested_schedule_call', 'meeting_booked_confirmed'].includes(String(entry?.intent || ''))
    ).length,
  };
}

function pickReportingTargetEquity(deal = {}) {
  const candidates = [
    deal.target_equity,
    deal.raise_target,
    deal.equity_target,
    deal.target_amount,
    deal.fundraising_target,
  ];

  for (const candidate of candidates) {
    const value = Number(candidate);
    if (!Number.isFinite(value) || value <= 0) continue;
    if (value >= 1000) return Number((value / 1_000_000).toFixed(2));
    return value;
  }

  return 2;
}

function pickReportingMeetingsNeeded(deal = {}, targetEquity = 2) {
  const direct = Number(deal.meetings_needed || deal.meeting_goal || deal.target_meetings);
  if (Number.isFinite(direct) && direct > 0 && direct < 500) return Math.round(direct);
  return Math.max(8, Math.ceil(targetEquity * 15));
}

function buildDailyReportingGoal(deal, metrics, launchContext) {
  const targetEquity = pickReportingTargetEquity(deal);
  const meetingsNeeded = pickReportingMeetingsNeeded(deal, targetEquity);
  const meetingsBooked = Number(metrics?.meetings_booked || 0);
  const firmsInPipeline = Number(metrics?.firms_in_pipeline || 0);
  const totalReplies = Number(metrics?.total_replies || 0);
  const todayTouches = Number(metrics?.li_invites_today || 0) + Number(metrics?.emails_sent_today || 0) + Number(metrics?.dms_sent_today || 0);

  let status = '🟡 BUILDING MOMENTUM';
  if (launchContext?.days_since_launch <= 4 && (todayTouches > 0 || firmsInPipeline >= 20 || totalReplies > 0)) {
    status = '🟡 EARLY LAUNCH — building pipeline';
  } else if (meetingsBooked >= meetingsNeeded) {
    status = '🟢 ON TRACK';
  } else if (firmsInPipeline < 10 && launchContext?.days_since_launch > 4) {
    status = '🔴 CRITICAL';
  } else if (totalReplies > 0 || firmsInPipeline >= 40) {
    status = '🟡 BUILDING MOMENTUM';
  } else {
    status = '🟠 BEHIND PLAN';
  }

  return {
    status,
    target_equity: targetEquity,
    meetings_needed: meetingsNeeded,
    meetings_booked: meetingsBooked,
    meetings_gap: Math.max(0, meetingsNeeded - meetingsBooked),
  };
}

function pickDailyNextMove(dealName, metrics = {}, highlights = []) {
  if (Number(metrics.provider_limit_deferrals || 0) > 0 && Number(metrics.emails_sent_today || 0) === 0) {
    return `keep outbound moving through email while LinkedIn throttling clears for ${dealName}`;
  }
  if (Number(metrics.missing_linkedin || 0) >= 3) {
    return `skip unmatched LinkedIn profiles cleanly and keep the next valid contacts moving for ${dealName}`;
  }
  return highlights[0] || `keep ${dealName} moving toward the current target`;
}

async function buildDailyDealSnapshots(deals = [], activityEntries = [], conversationEntries = [], sentEmailEntries = [], window = getDailyActivityWindow()) {
  const supabase = getSupabase();
  const activityByDeal = new Map();
  for (const entry of activityEntries) {
    const dealId = entry.deal_id ? String(entry.deal_id) : null;
    if (!dealId) continue;
    if (!activityByDeal.has(dealId)) activityByDeal.set(dealId, []);
    activityByDeal.get(dealId).push(entry);
  }

  const snapshots = [];
  for (const deal of deals) {
    if (!deal?.id) continue;
    const currentMetrics = await gatherCurrentMetrics(deal.id).catch(() => ({}));
    let firmsInPipeline = Number(currentMetrics?.firms_in_pipeline || 0);
    if (!firmsInPipeline && supabase) {
      try {
        const { count } = await supabase.from('batch_firms')
          .select('id', { count: 'exact', head: true })
          .eq('deal_id', deal.id);
        firmsInPipeline = Number(count || 0);
      } catch {}
    }
    const metrics = {
      ...currentMetrics,
      ...buildPerDealDailyMetrics(deal.id, activityEntries, conversationEntries, sentEmailEntries, currentMetrics),
      firms_in_pipeline: firmsInPipeline,
    };
    const entries = activityByDeal.get(String(deal.id)) || [];
    const highlights = entries
      .slice(0, 8)
      .map(entry => normalizeWhitespace(entry.action || entry.summary || entry.note || ''))
      .filter(Boolean);
    const launchContext = describeLaunchWindow(deal, metrics, window.end);
    const goal = buildDailyReportingGoal(deal, metrics, launchContext);

    snapshots.push({
      deal_id: deal.id,
      deal_name: deal.name,
      goal_status: goal.status,
      goal,
      metrics,
      launch_context: launchContext,
      reporting_status: deriveReportingProgressStatus({ goal_status: goal.status, metrics, goal, launch_context: launchContext }),
      highlights,
      next_move: sanitizeNarrationText(
        goal.status === 'CRITICAL'
          ? `increase outbound volume and unlock the next best contacts for ${deal.name}`
          : goal.status === 'BEHIND'
            ? `push the highest-conviction investors and convert activity into live conversations for ${deal.name}`
            : pickDailyNextMove(deal.name, metrics, highlights)
      ),
      activity_count: entries.length,
    });
  }

  return snapshots;
}

function buildFallbackDailyReport(window, activityEntries, dealSnapshots, previousReport = null) {
  const actionCounts = summarizeDailyActionCounts(activityEntries);
  const globalMetrics = buildDailyGlobalMetrics(dealSnapshots, activityEntries);
  const headline = `Daily operating log for ${DateTime.fromISO(`${window.reportDate}T12:00:00`, { zone: ANALYTICS_TIMEZONE }).toFormat('d LLL yyyy')}`;
  const thinPipeline = Number(globalMetrics.firms_in_pipeline || 0) < 20;
  const dealSections = dealSnapshots.map(snapshot => ({
    deal_name: snapshot.deal_name,
    deal_id: snapshot.deal_id,
    progress_status: snapshot.reporting_status || snapshot.goal_status,
    summary: sanitizeNarrationText(
      `${snapshot.deal_name} is ${snapshot.reporting_status || snapshot.goal_status}. `
      + `I sent ${snapshot.metrics.li_invites_today || 0} LinkedIn invites, ${snapshot.metrics.emails_sent_today || 0} emails, and ${snapshot.metrics.dms_sent_today || 0} LinkedIn DMs today. `
      + `${snapshot.launch_context?.days_since_launch ? `This is day ${snapshot.launch_context.days_since_launch} since launch. ` : ''}`
      + `I now have ${snapshot.metrics.firms_in_pipeline || 0} firms active in the pipeline${snapshot.launch_context?.pending_connections ? ` and ${snapshot.launch_context.pending_connections} LinkedIn requests still pending` : ''}.`
    ),
    key_actions: [
      `${snapshot.metrics.li_invites_today || 0} LinkedIn invites sent`,
      `${snapshot.metrics.emails_sent_today || 0} emails sent`,
      `${snapshot.metrics.dms_sent_today || 0} LinkedIn DMs sent`,
      `${snapshot.metrics.firms_in_pipeline || 0} firms currently in pipeline`,
    ].map(sanitizeNarrationText).filter(Boolean),
    target_status: buildDealStatusLine(snapshot),
    next_move: snapshot.next_move,
    recommended_actions: buildRecommendedActionsForSnapshot(snapshot),
  }));
  const previousContext = buildPreviousReportContext(previousReport);
  const executiveSummary = dealSections.length
    ? sanitizeNarrationText(
        `I sent ${globalMetrics.linkedin_invites} LinkedIn invites, ${globalMetrics.emails_sent} emails, and ${globalMetrics.linkedin_dms} LinkedIn DMs across ${dealSections.length} active deals today. `
        + `I logged ${globalMetrics.enrichment_actions} enrichment actions, kept ${globalMetrics.firms_in_pipeline} firms active in the pipeline, and have ${globalMetrics.meetings_booked} meetings booked so far. `
        + `${previousContext ? `Compared with ${previousContext.report_date}, today I built on the prior log and kept the active work moving forward. ` : ''}`
        + `${thinPipeline ? 'We need to feed more firms into the top of funnel tomorrow. ' : ''}`
        + `Tomorrow I need to keep the approved outreach moving and convert the strongest active conversations into replies.`
      )
    : 'No active deal work was logged today.';

  return {
    headline,
    executive_summary: executiveSummary,
    telegram_caption: `Daily log ready · ${activityEntries.length} activity events · ${dealSections.length} deal${dealSections.length === 1 ? '' : 's'} covered`,
    voice_script: sanitizeNarrationText(`Hey Dom, here is what I got through today. ${executiveSummary} ${dealSections.map(section => `${section.deal_name}: ${section.summary} Next I will focus on ${section.next_move}.`).join(' ')}${thinPipeline ? ' We need to feed more firms into the top of funnel tomorrow.' : ''}`).slice(0, 1100),
    deal_sections: dealSections,
    raw_context: {
      action_counts: actionCounts,
      activity_count: activityEntries.length,
      global_metrics: globalMetrics,
    },
  };
}

function buildDailyGlobalMetrics(dealSnapshots = [], activityEntries = []) {
  const totals = dealSnapshots.reduce((acc, snapshot) => {
    const metrics = snapshot.metrics || {};
    acc.linkedin_invites += Number(metrics.li_invites_today || 0);
    acc.emails_sent += Number(metrics.emails_sent_today || 0);
    acc.linkedin_dms += Number(metrics.dms_sent_today || 0);
    acc.total_replies += Number(metrics.total_replies || 0);
    acc.positive_replies += Number(metrics.positive_replies || 0);
    acc.meetings_booked += Number(metrics.meetings_booked || 0);
    acc.firms_in_pipeline += Number(metrics.firms_in_pipeline || 0);
    return acc;
  }, {
    linkedin_invites: 0,
    emails_sent: 0,
    linkedin_dms: 0,
    total_replies: 0,
    positive_replies: 0,
    meetings_booked: 0,
    firms_in_pipeline: 0,
  });

  totals.enrichment_actions = dealSnapshots.reduce((sum, snapshot) => sum + Number(snapshot?.metrics?.enrichment_actions || 0), 0);
  totals.invite_failures = dealSnapshots.reduce((sum, snapshot) => sum + Number(snapshot?.metrics?.invite_failures || 0), 0);
  totals.missing_linkedin = dealSnapshots.reduce((sum, snapshot) => sum + Number(snapshot?.metrics?.missing_linkedin || 0), 0);
  return totals;
}

function sanitizeDailyReport(aiReport = {}) {
  const dealSections = Array.isArray(aiReport.deal_sections) ? aiReport.deal_sections : [];
  return {
    ...aiReport,
    headline: sanitizeNarrationText(aiReport.headline || ''),
    executive_summary: sanitizeNarrationText(aiReport.executive_summary || ''),
    telegram_caption: sanitizeNarrationText(aiReport.telegram_caption || ''),
    voice_script: sanitizeNarrationText(aiReport.voice_script || '').slice(0, 1150),
    deal_sections: dealSections.map(section => ({
      ...section,
      deal_id: section.deal_id || null,
      deal_name: sanitizeNarrationText(section.deal_name || ''),
      progress_status: sanitizeNarrationText(section.progress_status || ''),
      summary: sanitizeNarrationText(section.summary || ''),
      key_actions: Array.isArray(section.key_actions) ? section.key_actions.map(item => sanitizeNarrationText(item)).filter(Boolean).slice(0, 4) : [],
      target_status: sanitizeNarrationText(section.target_status || ''),
      next_move: sanitizeNarrationText(section.next_move || ''),
      recommended_actions: Array.isArray(section.recommended_actions)
        ? section.recommended_actions.map(item => sanitizeNarrationText(item)).filter(Boolean).slice(0, 6)
        : [],
    })),
  };
}

function buildRecommendedActionsForSnapshot(snapshot = {}) {
  const actions = [];
  const metrics = snapshot.metrics || {};
  const dealName = snapshot.deal_name || 'this deal';

  if (Number(metrics.firms_in_pipeline || 0) < 20) {
    actions.push(`Research 20 more firms for ${dealName}`);
  }
  if (Number(metrics.positive_replies || 0) > 0) {
    actions.push(`Follow up with ${Math.min(5, Number(metrics.positive_replies || 0))} warm contacts in ${dealName}`);
  }
  if (Number(metrics.total_replies || 0) === 0 && Number(metrics.emails_sent_today || 0) + Number(metrics.li_invites_today || 0) > 0) {
    actions.push(`Expand LinkedIn search for ${dealName} and add a fresh investor angle tomorrow`);
  }
  if (Number(metrics.pending_approvals || 0) > 0) {
    actions.push(`Clear ${metrics.pending_approvals} pending approvals for ${dealName}`);
  }
  if (!actions.length && snapshot.next_move) {
    actions.push(snapshot.next_move);
  }
  return actions.slice(0, 6);
}

function describeLaunchWindow(deal, metrics = {}, reference = DateTime.now().setZone(ANALYTICS_TIMEZONE)) {
  const timezone = getDealAnalyticsTimezone(deal);
  const createdAt = deal?.created_at ? DateTime.fromISO(deal.created_at, { zone: 'utc' }) : null;
  const launchDate = createdAt?.isValid ? createdAt.setZone(timezone) : null;
  const daysSinceLaunch = launchDate
    ? Math.max(1, Math.ceil(reference.setZone(timezone).diff(launchDate, 'days').days))
    : null;
  const todayTouches = Number(metrics.li_invites_today || 0) + Number(metrics.emails_sent_today || 0) + Number(metrics.dms_sent_today || 0);
  const activePipeline = Number(metrics.firms_in_pipeline || 0);
  const pendingConnections = Number(metrics.li_pending || 0);
  const totalReplies = Number(metrics.total_replies || 0);
  const earlyOutreach = daysSinceLaunch != null && daysSinceLaunch <= 3;
  const activeBuildout = earlyOutreach && (todayTouches > 0 || activePipeline > 0 || pendingConnections > 0);

  return {
    launch_date: launchDate?.toISODate() || null,
    launch_label: launchDate?.toFormat('d LLL yyyy') || null,
    days_since_launch: daysSinceLaunch,
    early_outreach: earlyOutreach,
    active_buildout: activeBuildout,
    today_touches: todayTouches,
    total_replies: totalReplies,
    pending_connections: pendingConnections,
    active_pipeline: activePipeline,
  };
}

function deriveReportingProgressStatus(snapshot) {
  const rawStatus = String(snapshot?.goal_status || '').toUpperCase();
  const launch = snapshot?.launch_context || {};

  if (launch.active_buildout && Number(launch.total_replies || 0) === 0) {
    return 'ON TRACK - EARLY OUTREACH';
  }
  if (rawStatus.includes('ON TARGET') || rawStatus.includes('ON TRACK')) return 'ON TRACK';
  if (rawStatus.includes('BEHIND')) return 'BEHIND';
  if (rawStatus.includes('CRITICAL')) return 'CRITICAL';
  if (rawStatus.includes('DEADLINE PASSED')) return 'DEADLINE PASSED';
  return sanitizeNarrationText(snapshot?.goal_status || 'IN PROGRESS');
}

function buildDealStatusLine(snapshot) {
  const launch = snapshot?.launch_context || {};
  const dayLabel = launch.days_since_launch ? `Day ${launch.days_since_launch} since launch` : null;
  const meetings = `${snapshot?.metrics?.meetings_booked || 0}/${snapshot?.goal?.meetings_needed || 0} meetings`;
  const pipeline = `${snapshot?.metrics?.firms_in_pipeline || 0} firms active`;
  return [dayLabel, meetings, pipeline].filter(Boolean).join(' | ');
}

async function fetchPreviousDailyActivityReport(reportDate) {
  const supabase = getSupabase();
  if (!supabase || !reportDate) return null;
  const { data, error } = await supabase.from('daily_logs')
    .select('log_date, deal_name, telegram_voice_script, recommended_actions')
    .lt('log_date', reportDate)
    .order('log_date', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  if (!data) return null;
  return {
    report_date: data.log_date || null,
    executive_summary: data.telegram_voice_script || '',
    deal_sections: [{
      deal_name: data.deal_name || '',
      next_move: Array.isArray(data.recommended_actions) ? data.recommended_actions[0] || '' : '',
    }],
  };
}

function buildPreviousReportContext(previousReport) {
  if (!previousReport) return null;
  return {
    report_date: previousReport.report_date || null,
    headline: sanitizeNarrationText(previousReport.headline || ''),
    executive_summary: sanitizeNarrationText(previousReport.executive_summary || ''),
    deal_sections: Array.isArray(previousReport.deal_sections)
      ? previousReport.deal_sections.slice(0, 8).map(section => ({
          deal_name: sanitizeNarrationText(section.deal_name || ''),
          progress_status: sanitizeNarrationText(section.progress_status || ''),
          summary: sanitizeNarrationText(section.summary || ''),
          target_status: sanitizeNarrationText(section.target_status || ''),
          next_move: sanitizeNarrationText(section.next_move || ''),
        }))
      : [],
  };
}

async function buildAiDailyReport(window, activityEntries, dealSnapshots, previousReport = null) {
  const anthropic = getAnthropicClient();
  if (!anthropic) throw new Error('Anthropic unavailable');
  const globalMetrics = buildDailyGlobalMetrics(dealSnapshots, activityEntries);
  const previousContext = buildPreviousReportContext(previousReport);
  const prompt = `You are writing ROCO's end-of-day operating log for Dom.

DATE
${window.reportDate} (${window.timezone})

PREVIOUS DAILY LOG
${previousContext ? JSON.stringify(previousContext, null, 2) : 'None. This is the first available daily log in context.'}

ACTIVITY COUNTS
${JSON.stringify(summarizeDailyActionCounts(activityEntries), null, 2)}

DEAL SNAPSHOTS
${JSON.stringify(dealSnapshots, null, 2)}

GLOBAL METRICS FOR THE DAY
${JSON.stringify(globalMetrics, null, 2)}

RECENT ACTIVITY ENTRIES
${JSON.stringify(activityEntries.slice(0, 80).map(entry => ({
    deal_id: entry.deal_id || null,
    type: entry.type || entry.event_type || 'system',
    action: sanitizeNarrationText(entry.action || entry.summary || ''),
    note: sanitizeNarrationText(truncate(entry.note || entry.detail || entry.summary || '', 180)),
    created_at: entry.created_at || null,
  })), null, 2)}

TASK
Write a concise, conversational end-of-day update for Dom.
Speak like an operator giving a clean verbal debrief, not like a log parser.
Write entirely in first person singular. Use "I" and "my". Never say "Roco", "ROCO", or "the agent" when describing today's work.
Be specific about what I actually did today: LinkedIn invites sent, emails sent, DMs sent, enrichment progress, pipeline depth by deal, replies, meetings, and whether each deal is on track.
Every deal snapshot includes launch timing. Use it. If a deal launched only 1-3 days ago and outreach has only just started, do not frame zero meetings or zero replies as failure by itself. Pending LinkedIn requests, first emails going out, and pipeline build are signs of normal early momentum.
Only call a deal behind or critical if the operating facts support that judgment beyond normal early outreach lag.
Compare today against the previous daily log when one exists. Reference what changed since yesterday in natural language.
State what matters tomorrow: the next focus, the blocker, and what action will help the deal catch up if it is behind.
If the combined active pipeline across all deals is below 20 firms, include this exact sentence in the voice script: "we need to feed more firms into the top of funnel tomorrow."
Never include URLs, provider IDs, LinkedIn slugs, raw API paths, or literal raw error payloads.
Never read out contact profile links or opaque IDs.
Do not enumerate long lists of contact names or firms. Use at most two concrete examples total, and only if they materially help the summary.
Keep the voice script natural and short enough for a 60-90 second voice note.

Return ONLY valid JSON:
{
  "headline": "short title",
  "executive_summary": "2-4 sentences",
  "telegram_caption": "1 sentence",
  "voice_script": "spoken script under 1200 characters",
  "deal_sections": [
    {
      "deal_name": "Project name",
      "progress_status": "ON TRACK / BEHIND / CRITICAL",
      "summary": "2-3 sentence summary",
      "key_actions": ["...", "..."],
      "target_status": "short target line",
      "next_move": "next best action",
      "recommended_actions": ["specific action for tomorrow"]
    }
  ]
}`;

  const response = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 1600,
    messages: [{ role: 'user', content: prompt }],
  });
  const parsed = extractJSONObject(response.content?.[0]?.text || '');
  if (!parsed) throw new Error('Daily activity report JSON parse failed');
  return sanitizeDailyReport(parsed);
}

export async function buildDailyActivityReport({ deals = [], reference = DateTime.now().setZone(ANALYTICS_TIMEZONE) } = {}) {
  const window = getDailyActivityWindow(reference);
  const activityEntries = await fetchDailyActivityEntries(window);
  const conversationEntries = await fetchDailyConversationEntries(window).catch(() => []);
  const sentEmailEntries = await fetchDailySentEmailEntries(window).catch(() => []);
  const reportDeals = await fetchDealsForDailyLog(deals, activityEntries);
  const dealSnapshots = await buildDailyDealSnapshots(reportDeals, activityEntries, conversationEntries, sentEmailEntries, window);
  const previousReport = await fetchPreviousDailyActivityReport(window.reportDate).catch(() => null);

  let aiReport = null;
  try {
    aiReport = await buildAiDailyReport(window, activityEntries, dealSnapshots, previousReport);
  } catch {
    aiReport = buildFallbackDailyReport(window, activityEntries, dealSnapshots, previousReport);
  }
  aiReport = sanitizeDailyReport(aiReport);

  return {
    report_date: window.reportDate,
    log_date: window.reportDate,
    timezone: window.timezone,
    headline: aiReport.headline || `Daily operating log · ${window.reportDate}`,
    executive_summary: aiReport.executive_summary || null,
    voice_script: aiReport.voice_script || null,
    telegram_caption: aiReport.telegram_caption || null,
    deal_sections: Array.isArray(aiReport.deal_sections) ? aiReport.deal_sections : [],
    raw_payload: {
      ai_report: aiReport,
      deal_snapshots: dealSnapshots,
      action_counts: summarizeDailyActionCounts(activityEntries),
      activity_entries: activityEntries,
      sent_email_entries: sentEmailEntries,
      conversation_count: conversationEntries.length,
      previous_report: buildPreviousReportContext(previousReport),
    },
    activity_count: activityEntries.length,
    deals_covered: dealSnapshots.length,
    status: 'generated',
  };
}

export async function persistDailyActivityReport(report) {
  const supabase = getSupabase();
  if (!supabase || !report?.report_date) return null;
  const reportDate = report.log_date || report.report_date;
  const sections = Array.isArray(report.deal_sections) ? report.deal_sections : [];
  const snapshotsByDealId = new Map(
    (report.raw_payload?.deal_snapshots || [])
      .filter(snapshot => snapshot?.deal_id)
      .map(snapshot => [String(snapshot.deal_id), snapshot])
  );
  const activityByDealId = new Map();
  for (const entry of report.raw_payload?.activity_entries || []) {
    const key = String(entry?.deal_id || '');
    if (!key) continue;
    if (!activityByDealId.has(key)) activityByDealId.set(key, []);
    activityByDealId.get(key).push(entry);
  }

  const rows = sections
    .filter(section => section?.deal_id)
    .map(section => {
      const snapshot = snapshotsByDealId.get(String(section.deal_id)) || {};
      const metrics = snapshot.metrics || {};
      return {
        log_date: reportDate,
        deal_id: section.deal_id,
        deal_name: section.deal_name || snapshot.deal_name || null,
        total_firms_researched: Number(snapshot.activity_count || 0),
        total_firms_active: Number(metrics.firms_in_pipeline || 0),
        firms_added_today: Number(metrics.enrichment_actions || 0),
        emails_sent_today: Number(metrics.emails_sent_today || 0),
        linkedin_sent_today: Number(metrics.li_invites_today || 0) + Number(metrics.dms_sent_today || 0),
        replies_received_today: Number(metrics.total_replies || 0),
        positive_replies_today: Number(metrics.positive_replies || 0),
        meetings_booked_today: Number(metrics.meetings_booked || 0),
        pending_approvals: Number(metrics.pending_approvals || 0),
        recommended_actions: section.recommended_actions || buildRecommendedActionsForSnapshot(snapshot),
        actions_implemented: report.actions_implemented === true,
        telegram_voice_script: report.voice_script || report.executive_summary || null,
        activity_entries: activityByDealId.get(String(section.deal_id)) || [],
      };
    });

  if (!rows.length) return [];
  const { data, error } = await supabase.from('daily_logs')
    .upsert(rows, { onConflict: 'log_date,deal_id' })
    .select('*');
  if (error) throw error;
  return data || [];
}

export async function listDailyActivityReports(limit = 90) {
  const supabase = getSupabase();
  if (!supabase) return [];

  const mapDailyLogRowToAggregate = row => ({
    report_date: row.log_date,
    headline: `Daily log · ${row.log_date}`,
    executive_summary: row.telegram_voice_script || null,
    voice_script: row.telegram_voice_script || null,
    deals_covered: 1,
    activity_count: Array.isArray(row.activity_entries) ? row.activity_entries.length : 0,
    status: 'generated',
    deal_sections: [{
      deal_id: row.deal_id,
      deal_name: row.deal_name,
      progress_status: null,
      summary: [
        `${row.total_firms_active || 0} active firms`,
        `${row.emails_sent_today || 0} emails`,
        `${row.linkedin_sent_today || 0} LinkedIn sends`,
        `${row.replies_received_today || 0} replies`,
        `${row.meetings_booked_today || 0} meetings`,
      ].join(' · '),
      key_actions: row.recommended_actions || [],
      target_status: `${row.pending_approvals || 0} pending approvals`,
      next_move: Array.isArray(row.recommended_actions) ? row.recommended_actions[0] || '' : '',
      recommended_actions: row.recommended_actions || [],
    }],
  });

  const mergeAggregates = (target, source) => ({
    ...target,
    headline: target.headline || source.headline || null,
    executive_summary: target.executive_summary || source.executive_summary || null,
    voice_script: target.voice_script || source.voice_script || null,
    voice_name: target.voice_name || source.voice_name || null,
    voice_note_sent_at: target.voice_note_sent_at || source.voice_note_sent_at || null,
    raw_payload: target.raw_payload || source.raw_payload || null,
    status: target.status === 'generated' ? 'generated' : source.status || target.status || 'pending',
    deals_covered: Math.max(Number(target.deals_covered || 0), Number(source.deals_covered || 0)),
    activity_count: Math.max(Number(target.activity_count || 0), Number(source.activity_count || 0)),
    deal_sections: Array.isArray(target.deal_sections) && target.deal_sections.length
      ? target.deal_sections
      : (source.deal_sections || []),
  });

  const { data, error } = await supabase.from('daily_logs')
    .select('*')
    .order('log_date', { ascending: false })
    .limit(limit * 12);
  if (error) throw error;
  const grouped = new Map();
  for (const row of data || []) {
    const key = row.log_date;
    if (!grouped.has(key)) grouped.set(key, mapDailyLogRowToAggregate(row));
    else {
      const agg = grouped.get(key);
      agg.deals_covered += 1;
      agg.activity_count += Array.isArray(row.activity_entries) ? row.activity_entries.length : 0;
      agg.deal_sections.push(mapDailyLogRowToAggregate(row).deal_sections[0]);
    }
  }

  try {
    const { data: legacyRows, error: legacyError } = await supabase.from('daily_activity_reports')
      .select('*')
      .order('report_date', { ascending: false })
      .limit(limit);
    if (legacyError) throw legacyError;
    for (const row of legacyRows || []) {
      const key = row.report_date;
      if (!key) continue;
      const legacyAggregate = {
        report_date: key,
        headline: row.headline || `Daily log · ${key}`,
        executive_summary: row.executive_summary || row.voice_script || null,
        voice_script: row.voice_script || row.executive_summary || null,
        voice_name: row.voice_name || null,
        voice_note_sent_at: row.voice_note_sent_at || null,
        deals_covered: Number(row.deals_covered || (Array.isArray(row.deal_sections) ? row.deal_sections.length : 0) || 0),
        activity_count: Number(row.activity_count || 0),
        status: row.status || 'generated',
        raw_payload: row.raw_payload || null,
        deal_sections: Array.isArray(row.deal_sections) ? row.deal_sections : [],
      };
      grouped.set(key, grouped.has(key) ? mergeAggregates(grouped.get(key), legacyAggregate) : legacyAggregate);
    }
  } catch {}

  const todayEt = DateTime.now().setZone(ANALYTICS_TIMEZONE).toISODate();
  const digestHour = Number(process.env.DAILY_ACTIVITY_DIGEST_HOUR_ET || 20);
  if (todayEt && !grouped.has(todayEt)) {
    grouped.set(todayEt, {
      report_date: todayEt,
      headline: `Daily log · ${todayEt}`,
      executive_summary: null,
      voice_script: null,
      deals_covered: 0,
      activity_count: 0,
      status: 'pending',
      raw_payload: {
        message: `This daily log will be generated automatically at ${digestHour}:00 PM ET.`,
      },
      deal_sections: [],
    });
  }

  return [...grouped.values()]
    .sort((a, b) => new Date(b.report_date) - new Date(a.report_date))
    .slice(0, limit);
}

async function fetchElevenLabsVoices() {
  const { apiKey } = getElevenLabsConfig();
  if (!apiKey) return [];
  const response = await fetch(`${ELEVENLABS_BASE_URL}/voices`, {
    headers: { 'xi-api-key': apiKey },
  });
  if (!response.ok) {
    const err = await response.text().catch(() => '');
    throw new Error(`ElevenLabs voices failed: ${response.status} ${truncate(err, 120)}`);
  }
  const data = await response.json();
  return data.voices || [];
}

function choosePreferredElevenLabsVoice(voices = []) {
  const preferredNames = ['darian', 'roger', 'adam'];
  const ranked = [...voices].sort((left, right) => {
    const score = voice => {
      const name = String(voice?.name || '').toLowerCase();
      const labels = voice?.labels || {};
      let total = 0;
      if (String(labels.gender || '').toLowerCase() === 'male') total += 50;
      if (String(labels.category || '').toLowerCase().includes('default')) total += 15;
      if (String(labels.accent || '').toLowerCase().includes('american')) total += 10;
      preferredNames.forEach((preferred, index) => {
        if (name.includes(preferred)) total += 30 - (index * 5);
      });
      return total;
    };
    return score(right) - score(left);
  });
  return ranked[0] || null;
}

async function synthesizeElevenLabsVoice(voiceId, text) {
  const { apiKey, modelId } = getElevenLabsConfig();
  const response = await fetch(`${ELEVENLABS_BASE_URL}/text-to-speech/${voiceId}?output_format=mp3_44100_128`, {
    method: 'POST',
    headers: {
      'xi-api-key': apiKey,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model_id: modelId,
      text: normalizeWhitespace(text).slice(0, 1150),
      voice_settings: {
        stability: 0.45,
        similarity_boost: 0.8,
        style: 0.15,
        use_speaker_boost: true,
      },
    }),
  });

  if (!response.ok) {
    const err = await response.text().catch(() => '');
    throw new Error(`ElevenLabs TTS failed: ${response.status} ${truncate(err, 120)}`);
  }

  const buffer = Buffer.from(await response.arrayBuffer());
  const filePath = path.join(os.tmpdir(), `roco-daily-log-${Date.now()}.mp3`);
  fs.writeFileSync(filePath, buffer);
  return filePath;
}

export async function renderDailyVoiceNoteFromText(text) {
  const config = getElevenLabsConfig();
  if (!config.apiKey || !normalizeWhitespace(text)) return null;

  const attempts = [];

  if (config.defaultVoiceId) {
    attempts.push({
      voiceId: config.defaultVoiceId,
      voiceName: config.voiceName,
    });
  }

  try {
    const voices = await fetchElevenLabsVoices();
    const preferred = choosePreferredElevenLabsVoice(voices);
    if (preferred?.voice_id && !attempts.some(voice => voice.voiceId === preferred.voice_id)) {
      attempts.unshift({
        voiceId: preferred.voice_id,
        voiceName: preferred.name || null,
      });
    }
  } catch (err) {
    // Free-tier API access does not expose the voice library; fall back to configured/default voice IDs.
    if (!attempts.length) {
      throw new Error(`ElevenLabs voice lookup failed and no fallback voice ID is configured: ${err.message}`);
    }
  }

  let lastError = null;
  for (const attempt of attempts) {
    try {
      const filePath = await synthesizeElevenLabsVoice(attempt.voiceId, text);
      return {
        filePath,
        voiceName: attempt.voiceName || null,
      };
    } catch (err) {
      lastError = err;
    }
  }

  throw lastError || new Error('ElevenLabs TTS failed with all configured voice attempts');
}
