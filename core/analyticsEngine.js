// core/analyticsEngine.js
// Weekly analytics computation + AI recommendation generation.
// Accepted recommendations update roco_learned_settings to influence agent behaviour.

import Anthropic from '@anthropic-ai/sdk';
import { getSupabase } from './supabase.js';

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

export async function runWeeklyAnalytics() {
  console.log('[ANALYTICS] Running weekly analysis...');
  const supabase = getSupabase();
  if (!supabase) return;

  const weekStart = new Date();
  weekStart.setDate(weekStart.getDate() - 7);
  weekStart.setHours(0, 0, 0, 0);

  const { data: deals } = await supabase.from('deals').select('*').eq('status', 'ACTIVE');
  if (!deals?.length) return;

  const analyticsData = [];

  for (const deal of deals) {
    try {
      const snapshot = await computeDealSnapshot(deal, weekStart);
      if (snapshot.total_outreach === 0) continue;

      await supabase.from('deal_analytics').upsert({
        ...snapshot,
        deal_id: deal.id,
        week_starting: weekStart.toISOString().split('T')[0],
      }, { onConflict: 'deal_id,week_starting' });

      analyticsData.push({ deal: deal.name, ...snapshot });
    } catch (err) {
      console.warn(`[ANALYTICS] Snapshot failed for ${deal.name}:`, err.message);
    }
  }

  if (!analyticsData.length) {
    console.log('[ANALYTICS] No outreach activity this week — skipping recommendations');
    return;
  }

  await generateRecommendations(analyticsData, weekStart);
  console.log('[ANALYTICS] Weekly analysis complete');
}

async function computeDealSnapshot(deal, weekStart) {
  const supabase = getSupabase();

  const { data: messages } = await supabase.from('conversation_messages')
    .select('*')
    .eq('deal_id', deal.id)
    .gte('created_at', weekStart.toISOString());

  const outbound = (messages || []).filter(m => m.direction === 'outbound');
  const inbound  = (messages || []).filter(m => m.direction === 'inbound');

  const emailsSent  = outbound.filter(m => m.channel === 'email').length;
  const liInvites   = outbound.filter(m => m.channel === 'linkedin_invite').length;
  const liDms       = outbound.filter(m => m.channel === 'linkedin_dm').length;
  const emailReplies = inbound.filter(m => m.channel === 'email').length;
  const liReplies    = inbound.filter(m => m.channel === 'linkedin_dm').length;

  const positiveReplies = inbound.filter(m =>
    ['interested_send_materials','interested_schedule_call','meeting_booked_confirmed'].includes(m.intent)
  ).length;
  const negativeReplies = inbound.filter(m =>
    ['not_right_fit','remove_unsubscribe'].includes(m.intent)
  ).length;
  const tempCloses = inbound.filter(m =>
    ['will_review_get_back','hold_period','out_of_office'].includes(m.intent)
  ).length;

  const { count: meetings } = await supabase.from('contacts')
    .select('*', { count: 'exact', head: true })
    .eq('deal_id', deal.id)
    .eq('conversation_state', 'meeting_booked')
    .gte('updated_at', weekStart.toISOString());

  // Best response time
  const replyHours = inbound.map(m => new Date(m.received_at || m.created_at).getHours());
  const hourCounts = {};
  replyHours.forEach(h => { hourCounts[h] = (hourCounts[h] || 0) + 1; });
  const bestHour = Object.entries(hourCounts).sort(([,a],[,b]) => b - a)[0]?.[0];

  const replyDays = inbound.map(m => new Date(m.received_at || m.created_at).getDay());
  const dayCounts = {};
  replyDays.forEach(d => { dayCounts[d] = (dayCounts[d] || 0) + 1; });
  const bestDay = Object.entries(dayCounts).sort(([,a],[,b]) => b - a)[0]?.[0];

  // Template performance
  const templatePerf = {};
  outbound.forEach(m => {
    if (!m.template_name) return;
    if (!templatePerf[m.template_name]) templatePerf[m.template_name] = { sent: 0, replies: 0 };
    templatePerf[m.template_name].sent++;
  });

  const totalOutreach = emailsSent + liInvites + liDms;
  const totalReplies  = emailReplies + liReplies;

  return {
    emails_sent:          emailsSent,
    linkedin_invites_sent: liInvites,
    linkedin_dms_sent:    liDms,
    total_outreach:       totalOutreach,
    email_replies:        emailReplies,
    linkedin_replies:     liReplies,
    positive_responses:   positiveReplies,
    negative_responses:   negativeReplies,
    temp_closes:          tempCloses,
    meetings_booked:      meetings || 0,
    email_response_rate:  emailsSent > 0 ? emailReplies / emailsSent : 0,
    linkedin_response_rate: (liInvites + liDms) > 0 ? liReplies / (liInvites + liDms) : 0,
    overall_response_rate: totalOutreach > 0 ? totalReplies / totalOutreach : 0,
    meeting_conversion_rate: totalReplies > 0 ? (meetings || 0) / totalReplies : 0,
    best_response_hour:   bestHour != null ? parseInt(bestHour) : null,
    best_response_day:    bestDay  != null ? parseInt(bestDay)  : null,
    sector:               deal.sector,
    deal_type:            deal.raise_type,
    template_performance: templatePerf,
  };
}

async function generateRecommendations(analyticsData, weekStart) {
  const dataStr = JSON.stringify(analyticsData, null, 2);

  const templateData = JSON.stringify(analyticsData.map(d => ({
    deal: d.deal,
    templates: d.template_performance || {},
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

  const text = response.content[0]?.text || '';
  const match = text.replace(/```json|```/g, '').trim().match(/\[[\s\S]*\]/);
  if (!match) {
    console.warn('[ANALYTICS] Could not parse recommendations from Claude response');
    return;
  }

  const recs = JSON.parse(match[0]);
  const weekStr = weekStart.toISOString().split('T')[0];
  const supabase = getSupabase();

  for (const rec of recs) {
    await supabase.from('roco_recommendations').insert({
      category:               rec.category,
      title:                  rec.title,
      insight:                rec.insight,
      recommendation:         rec.recommendation,
      supporting_data:        rec.supporting_data || {},
      suggested_setting_change: rec.suggested_setting_change || null,
      week_starting:          weekStr,
      deals_analysed:         analyticsData.length,
    });
  }

  console.log(`[ANALYTICS] Generated ${recs.length} recommendations`);
}

export async function applyRecommendation(recommendationId) {
  const supabase = getSupabase();
  if (!supabase) return;

  const { data: rec } = await supabase.from('roco_recommendations')
    .select('*').eq('id', recommendationId).single();

  if (!rec || rec.status !== 'pending') return;

  if (rec.suggested_setting_change) {
    const { key, value } = rec.suggested_setting_change;
    await supabase.from('roco_learned_settings').upsert({
      key,
      value: String(value),
      source_recommendation_id: recommendationId,
      applied_at: new Date().toISOString(),
    }, { onConflict: 'key' });

    // Invalidate agent context cache if available
    try {
      const { invalidateCache } = await import('./agentContext.js');
      invalidateCache();
    } catch {}
  }

  await supabase.from('roco_recommendations').update({
    status: 'applied',
    applied_at: new Date().toISOString(),
  }).eq('id', recommendationId);

  console.log(`[ANALYTICS] Applied recommendation: ${rec.title}`);
}
