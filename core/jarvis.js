/**
 * core/jarvis.js
 * JARVIS — the cognitive agent powering Roco.
 *
 * JARVIS is not a chatbot layered on top of the system.
 * JARVIS IS the intelligence running the system.
 *
 * Three entry points:
 *   handleMessage(chatId, text)      — you spoke to it via Telegram
 *   runAutonomousCheck(deals, state) — post-orchestrator-cycle health check
 *   sendMorningBrief(deal)           — 08:00 daily briefing
 *
 * Model: claude-sonnet-4-6 now. Swap to Opus 4.7 via OpenRouter when key is set.
 * The OPENROUTER_API_KEY env var enables OpenRouter. If absent, falls back to
 * ANTHROPIC_API_KEY direct.
 */

import Anthropic from '@anthropic-ai/sdk';
import { getSupabase }        from './supabase.js';
import { getActiveDeals }     from './supabaseSync.js';
import { buildMemoryContext, writeMemory } from './jarvisMemory.js';
import { JARVIS_TOOLS, executeTool }       from './jarvisTools.js';
import { gatherCurrentMetrics }            from './fundraiserBrain.js';

// ─────────────────────────────────────────────────────────────────────────────
// CLIENT
// When OPENROUTER_API_KEY is set, all calls route through OpenRouter so you can
// use any model (Opus 4.7, Kimi K2.5, etc.) by changing MODEL_BRAIN below.
// ─────────────────────────────────────────────────────────────────────────────

const MODEL_BRAIN = process.env.JARVIS_BRAIN_MODEL
  || (process.env.OPENROUTER_API_KEY ? 'anthropic/claude-opus-4-7' : 'claude-sonnet-4-6');

function getClient() {
  if (process.env.OPENROUTER_API_KEY) {
    return new Anthropic({
      apiKey:  process.env.OPENROUTER_API_KEY,
      // Anthropic SDK appends /v1/messages itself. OpenRouter's Anthropic-
      // compatible root is /api, otherwise requests become /api/v1/v1/messages.
      baseURL: 'https://openrouter.ai/api',
    });
  }
  return new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
}

// ─────────────────────────────────────────────────────────────────────────────
// SESSION STORE  (in-memory, keyed by chatId — survives within one process)
// ─────────────────────────────────────────────────────────────────────────────

const SESSION_TTL_MS = 30 * 60 * 1000; // 30 min inactivity → fresh session

const sessions = new Map();
// { chatId: { messages: [], dealId: string|null, lastAt: number } }

function getSession(chatId) {
  const s = sessions.get(chatId);
  if (s && Date.now() - s.lastAt < SESSION_TTL_MS) return s;
  const fresh = { messages: [], dealId: null, lastAt: Date.now() };
  sessions.set(chatId, fresh);
  return fresh;
}

function touchSession(chatId) {
  const s = sessions.get(chatId);
  if (s) s.lastAt = Date.now();
}

// ─────────────────────────────────────────────────────────────────────────────
// PIPELINE HIGHLIGHTS  (rich live context injected into every system prompt)
// ─────────────────────────────────────────────────────────────────────────────

async function buildPipelineHighlights(dealId) {
  const sb = getSupabase();
  if (!sb || !dealId) return '';
  try {
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    const [repliesRes, acceptedRes, meetingsRes, emailCountRes, dmCountRes, inviteCountRes] = await Promise.all([
      sb.from('contacts').select('name, company_name, last_reply_at, pipeline_stage')
        .eq('deal_id', dealId).not('last_reply_at', 'is', null)
        .gte('last_reply_at', sevenDaysAgo).order('last_reply_at', { ascending: false }).limit(5),
      // Use pipeline_stage — invite_accepted_at was historically unreliable
      sb.from('contacts').select('name, company_name, pipeline_stage, invite_accepted_at')
        .eq('deal_id', dealId)
        .in('pipeline_stage', ['invite_accepted', 'pending_dm_approval'])
        .limit(12),
      sb.from('contacts').select('name, company_name, meeting_booked_at')
        .eq('deal_id', dealId).not('meeting_booked_at', 'is', null).limit(10),
      sb.from('contacts').select('id', { count: 'exact', head: true })
        .eq('deal_id', dealId).not('last_email_sent_at', 'is', null),
      sb.from('contacts').select('id', { count: 'exact', head: true })
        .eq('deal_id', dealId).not('dm_sent_at', 'is', null),
      // Count by stage — invite_sent_at was historically unreliable
      sb.from('contacts').select('id', { count: 'exact', head: true })
        .eq('deal_id', dealId)
        .in('pipeline_stage', ['invite_sent', 'invite_accepted', 'pending_dm_approval', 'DM Approved', 'DM Sent', 'In Conversation']),
    ]);

    const lines = [];
    const replies = repliesRes.data || [];
    if (replies.length) {
      lines.push(`Recent replies (7 days): ${replies.map(c => `${c.name} at ${c.company_name} [${c.pipeline_stage || '?'}]`).join(', ')}`);
    }
    const accepted = acceptedRes.data || [];
    if (accepted.length) {
      lines.push(`LinkedIn accepted — DM not yet sent (${accepted.length}): ${accepted.slice(0, 6).map(c => c.name).join(', ')}${accepted.length > 6 ? ` +${accepted.length - 6} more` : ''}`);
    }
    const meetings = meetingsRes.data || [];
    if (meetings.length) {
      lines.push(`Meetings booked (${meetings.length}): ${meetings.map(c => `${c.name} at ${c.company_name}`).join(', ')}`);
    }
    lines.push(`Emails sent total: ${emailCountRes.count || 0}`);
    lines.push(`LinkedIn DMs sent total: ${dmCountRes.count || 0}`);
    lines.push(`LinkedIn invites sent total: ${inviteCountRes.count || 0}`);
    return lines.join('\n');
  } catch {
    return '';
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// DEAL TIMELINE & VELOCITY CONTEXT
// ─────────────────────────────────────────────────────────────────────────────

async function buildDealTimeline(deal) {
  if (!deal) return '';
  const sb = getSupabase();
  const now = new Date();
  const nowDay = now.toLocaleDateString('en-US', { weekday: 'long' }).toLowerCase();
  const sendingDays = Array.isArray(deal.sending_days) && deal.sending_days.length
    ? deal.sending_days.map(d => d.toLowerCase())
    : ['monday','tuesday','wednesday','thursday','friday'];
  const isOutreachDay = sendingDays.includes(nowDay);
  const isWeekend = ['saturday','sunday'].includes(nowDay);

  // Days since launch
  const launchDate = new Date(deal.created_at);
  const daysSinceLaunch = Math.max(1, Math.floor((now - launchDate) / 86400000));

  // Outreach days elapsed (rough: calendar days × active_days_ratio)
  const outreachDaysElapsed = Math.max(1, Math.round(daysSinceLaunch * (sendingDays.length / 7)));

  // Days until end of month (working deadline if no close date)
  const closeDateStr = deal.target_close_date || null;
  const deadline = closeDateStr ? new Date(closeDateStr) : new Date(now.getFullYear(), now.getMonth() + 1, 0);
  const daysUntilDeadline = Math.max(0, Math.ceil((deadline - now) / 86400000));
  const deadlineLabel = closeDateStr
    ? new Date(closeDateStr).toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' })
    : `end of ${now.toLocaleDateString('en-US', { month: 'long' })} (no close date set)`;

  // Working days remaining until deadline
  let workingDaysLeft = 0;
  const cursor = new Date(now);
  cursor.setHours(0,0,0,0);
  const deadlineMidnight = new Date(deadline);
  deadlineMidnight.setHours(23,59,59,999);
  while (cursor <= deadlineMidnight) {
    const dName = cursor.toLocaleDateString('en-US', { weekday: 'long' }).toLowerCase();
    if (sendingDays.includes(dName)) workingDaysLeft++;
    cursor.setDate(cursor.getDate() + 1);
  }

  // Activity queries
  const since7d = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  const KEY_EVENTS = ['EMAIL_SENT','LINKEDIN_INVITE_SENT','LINKEDIN_DM_SENT','REPLY_RECEIVED','LINKEDIN_ACCEPTANCE'];

  let last7 = {};
  let lifetime = {};
  if (sb) {
    try {
      const [r7, rAll] = await Promise.all([
        sb.from('activity_log').select('event_type').eq('deal_id', deal.id)
          .gte('created_at', since7d).in('event_type', KEY_EVENTS),
        sb.from('activity_log').select('event_type').eq('deal_id', deal.id)
          .gte('created_at', launchDate.toISOString()).in('event_type', KEY_EVENTS),
      ]);
      (r7.data || []).forEach(a => { last7[a.event_type] = (last7[a.event_type] || 0) + 1; });
      (rAll.data || []).forEach(a => { lifetime[a.event_type] = (lifetime[a.event_type] || 0) + 1; });
    } catch {}
  }

  const totalEmails   = lifetime['EMAIL_SENT'] || 0;
  const totalInvites  = lifetime['LINKEDIN_INVITE_SENT'] || 0;
  const totalDMs      = lifetime['LINKEDIN_DM_SENT'] || 0;
  const totalReplies  = lifetime['REPLY_RECEIVED'] || 0;

  const emailsPerDay  = (totalEmails  / outreachDaysElapsed).toFixed(1);
  const invitesPerDay = (totalInvites / outreachDaysElapsed).toFixed(1);

  const projEmails  = Math.round(parseFloat(emailsPerDay)  * workingDaysLeft);
  const projInvites = Math.round(parseFloat(invitesPerDay) * workingDaysLeft);

  const todayMode = isWeekend
    ? `RESEARCH MODE — weekend, no outreach sending. Use today to find new firms, enrich pipeline, and review strategy.`
    : isOutreachDay
      ? `OUTREACH MODE — weekday, sending is active.`
      : `MONITORING — today is not a configured sending day.`;

  const targetAmount = deal.target_amount
    ? `$${(deal.target_amount / 1_000_000).toFixed(1)}M`
    : (deal.parsed_deal_info?.raise_amount || 'not set');

  return `
DEAL TIMELINE & PACE:
  Launch date: ${launchDate.toLocaleDateString('en-GB', { day:'numeric', month:'long', year:'numeric' })} (${daysSinceLaunch} days ago, ~${outreachDaysElapsed} outreach days)
  Target raise: ${targetAmount}
  Working deadline: ${deadlineLabel} (${daysUntilDeadline} calendar days, ${workingDaysLeft} outreach days remaining)
  Today (${now.toLocaleDateString('en-US', { weekday:'long', month:'short', day:'numeric' })}): ${todayMode}
  Configured outreach days: ${sendingDays.map(d => d.slice(0,3)).join(', ')}

LIFETIME ACTIVITY (since launch, from activity log):
  Emails sent: ${totalEmails}
  LinkedIn invites sent: ${totalInvites}
  LinkedIn DMs sent: ${totalDMs}
  Replies received: ${totalReplies}
  Avg emails/outreach day: ${emailsPerDay}
  Avg invites/outreach day: ${invitesPerDay}

LAST 7 DAYS:
  Emails: ${last7['EMAIL_SENT'] || 0}, LI invites: ${last7['LINKEDIN_INVITE_SENT'] || 0}, DMs: ${last7['LINKEDIN_DM_SENT'] || 0}, Replies: ${last7['REPLY_RECEIVED'] || 0}

PACE PROJECTION (at current rate, ${workingDaysLeft} outreach days left):
  Additional emails: ~${projEmails} -> total ~${totalEmails + projEmails}
  Additional invites: ~${projInvites} -> total ~${totalInvites + projInvites}
`;
}

// ─────────────────────────────────────────────────────────────────────────────
// SYSTEM PROMPT BUILDER
// ─────────────────────────────────────────────────────────────────────────────

async function buildSystemPrompt(deal, metrics) {
  const memoryCtx    = deal ? await buildMemoryContext(deal.id) : 'No deal active.';
  const highlights   = deal ? await buildPipelineHighlights(deal.id) : '';
  const timelineSection = deal ? await buildDealTimeline(deal) : '';

  const dealSection = deal ? `
ACTIVE DEAL: ${deal.name}
DEAL ID (use this exact UUID in all tool calls): ${deal.id}
What's being raised: ${deal.description || deal.parsed_deal_info?.description || 'Not specified'}
Target investors: ${deal.parsed_deal_info?.target_investor_profile || 'Not specified'}
Target raise: ${deal.parsed_deal_info?.raise_amount || deal.target_raise || 'Not specified'}
` : 'NO ACTIVE DEAL — ask the user to set one up.';

  const metricsSection = metrics ? `
LIVE PIPELINE:
  Emails sent: ${metrics.emails_sent || 0} total (${metrics.emails_sent_today || 0} today) — includes contacts now inactive/archived after no reply
  LinkedIn invites: ${metrics.li_invites_sent || 0} sent · ${metrics.li_accepted || 0} accepted · ${metrics.li_pending || 0} still pending
  LinkedIn DMs: ${metrics.dms_sent || 0} sent
  Total unique replies (email + LinkedIn combined): ${metrics.total_replies || 0}
  Total replies: ${metrics.total_replies || 0}
  Response rate: ${metrics.response_rate || 0}%
  Meetings booked: ${metrics.meetings_booked || 0}
  Active firms: ${metrics.firms_in_pipeline || 0}
  Pending approvals: ${metrics.pending_approvals || 0}
  Status: ${metrics.goal_status || 'UNKNOWN'}
` : '';

  const { getOrchestratorEvents } = await import('./jarvisTools.js');
  const recentEvents = getOrchestratorEvents(8);
  const eventsSection = recentEvents.length
    ? `\nORCHESTRATOR ACTIVITY (last ${recentEvents.length} events):\n${recentEvents.map(e => `  [${new Date(e.ts).toLocaleTimeString()}] ${e.action}${e.note ? ': ' + e.note : ''}`).join('\n')}\n`
    : '';

  return `You are JARVIS — the AI agent running Roco, an autonomous PE/VC fundraising platform.

You are not a chatbot. You are an intelligent co-worker who controls the entire fundraising operation. You have full visibility into the pipeline, memory of everything done previously, and tools to act on anything.

${dealSection}
${metricsSection}${timelineSection}${highlights ? `PIPELINE HIGHLIGHTS (live from database):\n${highlights}\n` : ''}${eventsSection}
MEMORY (what you've done and learned on this deal):
${memoryCtx}

TODAY: ${new Date().toLocaleDateString('en-GB', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' })}

PERSONALITY:
- Direct and sharp. Like a senior analyst who knows this deal inside out.
- Short confident sentences. No fluff, no filler.
- Always reference specific names, numbers, and dates from context.
- When you take an action via a tool, confirm it concisely — one sentence.
- When you see something worth flagging, flag it clearly with a recommendation.
- When asked to do something, do it via tools — don't just describe what you'd do.
- If you don't have enough information to act, ask one focused question.
- Keep every response to one paragraph maximum. Conversational, brief, like a trusted colleague — not a written report.

FORMATTING — STRICT RULES:
- Plain text only. No markdown whatsoever.
- No asterisks (* or **). No underscores for emphasis. No backticks.
- No em dashes (—). Use a comma or period instead.
- No bullet points or numbered lists unless absolutely necessary, and if you must list things, use plain line breaks with a simple dash prefix, never asterisks.
- No headers or bold text.
- No excessive punctuation. Write like a person, not a document.

IMPORTANT:
- You control the orchestration cycle — you can pause, resume, or redirect it at any time.
- When you use a tool and get a result, respond naturally based on the result.
- One paragraph maximum per response. No lists, no headers, no structured summaries unless the user explicitly asks for a breakdown.
- Never say "I'll need to" — either do it now with a tool or say why you can't.
- When asked to "do research" or "find more investors", use trigger_research — this fires firm discovery immediately in the background and returns at once. When the tool returns, confirm briefly that research has been triggered and they can watch it in the live activity log. Do not wait or say you'll report back later.
- When asked to "find someone" or "add [name] at [firm]" to the pipeline, use add_contact_to_pipeline to save them to the database. The orchestrator will enrich them automatically.
- search_linkedin_people and find_decision_makers call Unipile directly — use them to find people, then add_contact_to_pipeline to save the best matches.
- You have full read/write access to the pipeline database via tools. When you take an action, the orchestrator picks it up on the next cycle.
- You understand the deal timeline: how many days since launch, working days remaining, and current velocity vs pace needed. When asked "are we on track?" or "what should we focus on?", synthesise the timeline and pipeline data to give a direct, specific answer with concrete numbers.
- On weekends: recommend research activities (finding firms, enriching contacts, reviewing strategy). On weekdays: focus on outreach actions and approvals.
- Always speak in terms of concrete numbers: "you've sent X emails in Y days, at this pace you'll hit Z by end of month."`;
}

// ─────────────────────────────────────────────────────────────────────────────
// AGENTIC LOOP  (tool_use cycle)
// ─────────────────────────────────────────────────────────────────────────────

async function runAgentLoop(systemPrompt, messages, dealId) {
  const client  = getClient();
  const maxIter = 6; // safety cap — JARVIS won't call tools more than 6 times per turn

  let loopMessages = [...messages];

  for (let i = 0; i < maxIter; i++) {
    const response = await client.messages.create({
      model:      MODEL_BRAIN,
      max_tokens: 1024,
      system:     systemPrompt,
      tools:      JARVIS_TOOLS,
      messages:   loopMessages,
    });

    // Always add the assistant turn
    loopMessages.push({ role: 'assistant', content: response.content });

    if (response.stop_reason !== 'tool_use') {
      // Final text response
      const text = response.content.find(b => b.type === 'text')?.text || '';
      return { text, finalMessages: loopMessages };
    }

    // Execute all tool calls in this response
    const toolResults = [];
    for (const block of response.content) {
      if (block.type !== 'tool_use') continue;
      console.log(`[JARVIS] Tool call: ${block.name}`, JSON.stringify(block.input));
      let result;
      try {
        result = await executeTool(block.name, { ...block.input, deal_id: block.input.deal_id || dealId });
      } catch (err) {
        result = { error: err.message };
      }
      console.log(`[JARVIS] Tool result: ${block.name}`, JSON.stringify(result).slice(0, 200));
      toolResults.push({
        type:        'tool_result',
        tool_use_id: block.id,
        content:     typeof result === 'string' ? result : JSON.stringify(result),
      });
    }

    loopMessages.push({ role: 'user', content: toolResults });
  }

  return { text: 'I hit my action limit on this turn. Let me know if you need me to continue.', finalMessages: loopMessages };
}

// ─────────────────────────────────────────────────────────────────────────────
// PUBLIC: handleMessage  — called from Telegram
// ─────────────────────────────────────────────────────────────────────────────

export async function handleMessage(chatId, text, overrideDealId = null) {
  const session = getSession(chatId);

  // Resolve active deal — let caller override (e.g. dashboard orb with a specific deal selected)
  if (overrideDealId) {
    session.dealId = overrideDealId;
  } else if (!session.dealId) {
    const deals = await getActiveDeals().catch(() => []);
    session.dealId = deals[0]?.id || null;
  }

  let deal = session.dealId ? await getDeal(session.dealId) : null;
  // If the stored dealId no longer resolves (deleted/closed), fall back to first active deal
  if (!deal && !overrideDealId) {
    const deals = await getActiveDeals().catch(() => []);
    session.dealId = deals[0]?.id || null;
    deal = session.dealId ? await getDeal(session.dealId) : null;
  }
  const metrics = deal ? await gatherCurrentMetrics(deal.id).catch(() => null) : null;

  const systemPrompt = await buildSystemPrompt(deal, metrics);

  // Add user message to session history
  session.messages.push({ role: 'user', content: text });

  let responseText = '';
  try {
    const { text: reply, finalMessages } = await runAgentLoop(
      systemPrompt,
      session.messages,
      session.dealId,
    );
    responseText = reply;

    // Persist conversation history (cap at last 20 turns to avoid token bloat)
    session.messages = finalMessages.slice(-20);
    touchSession(chatId);

    // Write conversation to persistent memory (async, non-blocking)
    if (session.dealId) {
      writeMemory(session.dealId, {
        type:    'ACTION',
        subject: `Conversation: ${text.slice(0, 60)}`,
        content: `User: ${text.slice(0, 200)} → JARVIS: ${responseText.slice(0, 200)}`,
        tags:    ['conversation'],
        metadata: { chatId },
      }).catch(() => {});
    }
  } catch (err) {
    console.error('[JARVIS] handleMessage error:', err.message);
    responseText = `Something went wrong on my end — ${err.message.slice(0, 100)}. Try again.`;
  }

  return responseText;
}

// ─────────────────────────────────────────────────────────────────────────────
// PUBLIC: runAutonomousCheck  — called post-orchestrator-cycle
// ─────────────────────────────────────────────────────────────────────────────

const VOLUME_THRESHOLDS = {
  min_pending_approvals:    3,
  min_firms_in_pipeline:    10,
  response_rate_drop_pct:   30,
};

// Prevent the same alert type from firing more than once every 2 hours per deal
const _alertCooldowns = new Map();
function _canAlert(dealId, type) {
  const key = `${dealId}:${type}`;
  const last = _alertCooldowns.get(key) || 0;
  if (Date.now() - last < 2 * 60 * 60 * 1000) return false;
  _alertCooldowns.set(key, Date.now());
  return true;
}

export async function runAutonomousCheck(deals) {
  if (!deals?.length) return;

  for (const deal of deals) {
    try {
      await runDealAutonomousCheck(deal);
    } catch (err) {
      console.warn(`[JARVIS] Autonomous check failed for ${deal.name}:`, err.message);
    }
  }
}

async function runDealAutonomousCheck(deal) {
  const metrics = await gatherCurrentMetrics(deal.id).catch(() => null);
  if (!metrics) return;

  const alerts = [];

  // Rule 1: Pipeline running thin
  if ((metrics.firms_in_pipeline || 0) < VOLUME_THRESHOLDS.min_firms_in_pipeline && _canAlert(deal.id, 'volume')) {
    alerts.push({
      type:   'volume',
      msg:    `Pipeline is thin — ${metrics.firms_in_pipeline} active contacts. I've triggered a research cycle to refill it.`,
      action: async () => {
        const { triggerImmediateRun } = await import('./orchestrator.js');
        triggerImmediateRun(deal.id).catch(() => {});
        await writeMemory(deal.id, {
          type:    'ACTION',
          subject: 'Auto-triggered research (thin pipeline)',
          content: `Pipeline dropped to ${metrics.firms_in_pipeline} active contacts — triggered immediate research cycle.`,
          tags:    ['autonomous', 'research'],
        });
      },
    });
  }

  if (!alerts.length) return;

  // Only send Telegram if actionable
  const actionableAlerts = alerts.filter(a => a.action);
  if (!actionableAlerts.length) return;

  for (const alert of actionableAlerts) {
    if (alert.action) await alert.action().catch(() => {});
  }

  const { sendTelegram } = await import('../approval/telegramBot.js');
  const lines = actionableAlerts.map(a => `• ${a.msg}`).join('\n');
  await sendTelegram(`🤖 *JARVIS — ${deal.name}*\n${lines}`).catch(() => {});
}

// ─────────────────────────────────────────────────────────────────────────────
// PUBLIC: sendMorningBrief  — 08:00 daily
// ─────────────────────────────────────────────────────────────────────────────

export async function sendMorningBrief(deal) {
  try {
    const metrics      = await gatherCurrentMetrics(deal.id);
    const systemPrompt = await buildSystemPrompt(deal, metrics);

    const prompt = `Generate a morning brief for the team. Cover:
1. Key numbers (what happened yesterday / overnight)
2. Top 2-3 priorities for today
3. Anything needing immediate attention
4. One observation or recommendation

Keep it tight — this is read on a phone. Use *bold* for key names/numbers. Max 200 words.`;

    const { text } = await runAgentLoop(
      systemPrompt,
      [{ role: 'user', content: prompt }],
      deal.id,
    );

    const { sendTelegram } = await import('../approval/telegramBot.js');
    await sendTelegram(`🌅 *JARVIS Morning Brief — ${deal.name}*\n\n${text}`);

    await writeMemory(deal.id, {
      type:    'ACTION',
      subject: 'Morning brief sent',
      content: text.slice(0, 300),
      tags:    ['morning_brief'],
    });
  } catch (err) {
    console.error('[JARVIS] Morning brief failed:', err.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

async function getDeal(dealId) {
  const sb = getSupabase();
  if (!sb || !dealId) return null;
  try {
    const { data } = await sb.from('deals').select('*').eq('id', dealId).single();
    return data || null;
  } catch {
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// MORNING BRIEF TIMER  — checks once per cycle if it's brief-time
// ─────────────────────────────────────────────────────────────────────────────

const BRIEF_HOUR_EST = Number(process.env.JARVIS_BRIEF_HOUR_EST || 8);
const briefSentToday = new Set(); // dealId → sent today flag

export function checkMorningBriefTimer(deals) {
  if (!deals?.length) return;
  const nowEst  = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const hour    = nowEst.getHours();
  const dateKey = nowEst.toDateString();

  if (hour !== BRIEF_HOUR_EST) return;

  for (const deal of deals) {
    const key = `${deal.id}:${dateKey}`;
    if (briefSentToday.has(key)) continue;
    briefSentToday.add(key);
    sendMorningBrief(deal).catch(err => console.warn('[JARVIS] Brief failed:', err.message));
  }
}
