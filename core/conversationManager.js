/**
 * core/conversationManager.js
 * Manages conversation state, intent classification, temp closes, and waterfall progression.
 */

import Anthropic from '@anthropic-ai/sdk';
import { getSupabase } from './supabase.js';
import { getIntentContext, getReplyContext, getTempCloseContext } from './agentContext.js';

let _anthropic;
function getAnthropic() {
  if (!_anthropic) _anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  return _anthropic;
}

// ── CONVERSATION HISTORY ──────────────────────────────────────────────────────

export async function getConversationHistory(contactId) {
  const sb = getSupabase();
  if (!sb) return [];
  const { data } = await sb
    .from('conversation_messages')
    .select('*')
    .eq('contact_id', contactId)
    .order('sent_at', { ascending: true });
  return data || [];
}

export async function logConversationMessage({ contactId, dealId, direction, channel, subject, body, unipileMessageId, templateName }) {
  const sb = getSupabase();
  if (!sb) return null;

  const { data } = await sb.from('conversation_messages').insert({
    contact_id:          contactId,
    deal_id:             dealId || null,
    direction,
    channel,
    subject:             subject || null,
    body:                body || '',
    sent_at:             direction === 'outbound' ? new Date().toISOString() : null,
    received_at:         direction === 'inbound'  ? new Date().toISOString() : null,
    unipile_message_id:  unipileMessageId || null,
    template_name:       templateName || null,
  }).select().single();

  // Mirror timestamps on contacts row
  if (direction === 'outbound') {
    await sb.from('contacts').update({ last_outreach_at: new Date().toISOString() }).eq('id', contactId).catch(() => {});
  } else {
    await sb.from('contacts').update({ last_reply_at: new Date().toISOString() }).eq('id', contactId).catch(() => {});
  }

  return data;
}

// ── INTENT CLASSIFICATION ─────────────────────────────────────────────────────

export async function classifyIntent(inboundMessage, conversationHistory, contact, deal) {
  const sb = getSupabase();
  const { data: knownIntents } = sb
    ? await sb.from('conversation_intents').select('*')
    : { data: [] };

  const historyText = (conversationHistory || []).slice(-10).map(m => {
    const ts   = new Date(m.sent_at || m.received_at || Date.now()).toLocaleDateString('en-GB');
    const role = m.direction === 'outbound' ? 'ROCO' : (contact?.name || 'INVESTOR');
    return `[${role} via ${m.channel} on ${ts}]\n${m.body}`;
  }).join('\n\n---\n\n');

  const intentList = (knownIntents || []).map(i =>
    `${i.intent_key}: ${i.description} (category: ${i.category})`
  ).join('\n');

  const agentCtx = await getIntentContext();
  const prompt = `${agentCtx}You are an expert at classifying investor responses in private equity fundraising conversations.

DEAL CONTEXT:
Deal: ${deal?.name || 'Unknown'}
Sector: ${deal?.sector || 'Unknown'}
Investor: ${contact?.name || 'Unknown'} at ${contact?.company_name || 'Unknown'}

FULL CONVERSATION HISTORY:
${historyText || 'No prior messages.'}

LATEST INBOUND MESSAGE:
"${inboundMessage}"

KNOWN INTENTS:
${intentList || '(none loaded — use your judgement)'}

TASK:
1. Classify the latest message into one of the known intents above.
   If none fit well, create a new intent_key in snake_case and describe it.
2. Determine the conversation_state this message creates.
3. Determine what Roco should do next.
4. Assess if Dom needs to be notified.

Return ONLY valid JSON:
{
  "intent_key": "string",
  "intent_description": "string",
  "category": "positive|soft|negative|question|unknown",
  "is_new_intent": boolean,
  "confidence": 0.0,
  "conversation_state": "temp_closed|awaiting_response|conversation_ended_positive|conversation_ended_negative|meeting_booked|do_not_contact|needs_reply",
  "is_temp_close": boolean,
  "temp_close_days": null,
  "is_conversation_ended": boolean,
  "conversation_ended_reason": null,
  "requires_dom": boolean,
  "dom_flag_reason": null,
  "suggested_action": "string",
  "suggested_reply": null,
  "tone_notes": "string"
}`;

  const response = await getAnthropic().messages.create({
    model:      'claude-sonnet-4-6',
    max_tokens: 1500,
    messages:   [{ role: 'user', content: prompt }],
  });

  const text  = response.content[0]?.text || '';
  const match = text.replace(/```json|```/g, '').trim().match(/\{[\s\S]*\}/);
  if (!match) throw new Error('No JSON from intent classifier');
  const result = JSON.parse(match[0]);

  // Auto-register newly discovered intents
  if (result.is_new_intent && sb) {
    await registerNewIntent(result, inboundMessage, contact, sb);
  } else if (!result.is_new_intent && sb) {
    // Update last_seen_at on known intent
    await sb.from('conversation_intents')
      .update({ last_seen_at: new Date().toISOString() })
      .eq('intent_key', result.intent_key)
      .catch(() => {});
  }

  return result;
}

async function registerNewIntent(result, exampleMessage, contact, sb) {
  console.log(`[INTENT] New intent discovered: ${result.intent_key} — auto-registering`);
  await sb.from('conversation_intents').upsert({
    intent_key:      result.intent_key,
    description:     result.intent_description,
    category:        result.category,
    standard_action: result.suggested_action,
    requires_dom:    result.requires_dom,
    example_phrases: [exampleMessage.substring(0, 200)],
    auto_discovered: true,
    last_seen_at:    new Date().toISOString(),
  }, { onConflict: 'intent_key' });
  console.log(`[INTENT] ⚠️  New intent registered (auto_discovered=true): ${result.intent_key} — Dom should review`);
}

// ── CONTEXTUAL REPLY DRAFTING ─────────────────────────────────────────────────

export async function draftContextualReply({ contact, deal, conversationHistory, inboundMessage, intent, agentPersona }) {
  const historyText = (conversationHistory || []).slice(-8).map(m => {
    const role = m.direction === 'outbound' ? 'ROCO' : (contact?.name || 'INVESTOR');
    return `[${role} via ${m.channel}]\n${m.body}`;
  }).join('\n\n---\n\n');

  const agentCtx = await getReplyContext();
  const prompt = `${agentCtx}You are drafting a reply on behalf of Dom, a senior fundraising professional.

AGENT PERSONA:
${agentPersona || 'Senior placement advisor. Direct, peer-level, no fluff.'}

VOICE RULES:
- No "hope this finds you well", "just following up", "circle back", "touch base"
- No em dashes. No exclamation marks.
- Short. Confident. Specific.
- Never apologise for reaching out.
- Lead with substance, not pleasantries.
- Match the energy and length of their message.
- Never reference their silence or slowness to reply.
- Sign off as: Dom

DEAL: ${deal?.name || 'the deal'}
INVESTOR: ${contact?.name} at ${contact?.company_name}
INTENT CLASSIFIED: ${intent?.intent_key} — ${intent?.intent_description}
CONVERSATION STATE AFTER THIS REPLY: ${intent?.conversation_state}

FULL CONVERSATION HISTORY:
${historyText}

THEIR LATEST MESSAGE:
"${inboundMessage}"

SUGGESTED ACTION: ${intent?.suggested_action}
TONE NOTES: ${intent?.tone_notes}

${intent?.is_temp_close ? `NOTE: Acknowledge their response naturally, then close the loop gracefully. Do NOT mention any follow-up timeline.` : ''}

${intent?.is_conversation_ended
  ? `NOTE: This is the final message in the sequence. ${intent.category === 'negative'
      ? 'Close gracefully. Thank them. Naturally ask if they know anyone else who might be interested (one sentence max).'
      : 'Confirm next steps clearly. Keep it brief.'}`
  : ''}

Draft the reply now. Reply text only — no subject line, no labels, no explanation:`;

  const response = await getAnthropic().messages.create({
    model:      'claude-sonnet-4-6',
    max_tokens: 400,
    messages:   [{ role: 'user', content: prompt }],
  });

  return response.content[0]?.text?.trim() || null;
}

// ── CONVERSATION STATE ────────────────────────────────────────────────────────

export async function setConversationState(contactId, state, extras = {}) {
  const sb = getSupabase();
  if (!sb) return;

  const updates = { conversation_state: state, ...extras };

  if (state === 'temp_closed') {
    updates.temp_closed_at      = new Date().toISOString();
    updates.next_follow_up_due  = new Date(Date.now() + 5 * 24 * 60 * 60 * 1000).toISOString();
  }

  if (['conversation_ended_positive', 'conversation_ended_negative', 'meeting_booked', 'do_not_contact'].includes(state)) {
    updates.conversation_ended_at = new Date().toISOString();
  }

  await sb.from('contacts').update(updates).eq('id', contactId);
}

export async function appendIntentHistory(contactId, intentRecord) {
  const sb = getSupabase();
  if (!sb) return;

  const { data: contact } = await sb.from('contacts')
    .select('intent_history').eq('id', contactId).single();

  const history = contact?.intent_history || [];
  history.push({ timestamp: new Date().toISOString(), ...intentRecord });

  await sb.from('contacts')
    .update({ intent_history: history.slice(-50) })
    .eq('id', contactId);
}

// ── TEMP CLOSE MONITORING ─────────────────────────────────────────────────────

export async function checkTempClosedContacts() {
  const sb = getSupabase();
  if (!sb) return [];

  const fiveDaysAgo = new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString();
  const TERMINAL = ['conversation_ended_positive', 'conversation_ended_negative', 'meeting_booked', 'do_not_contact', 'ghosted'];

  // No reply at all since temp close
  const { data: stale1 } = await sb
    .from('contacts')
    .select('*')
    .eq('conversation_state', 'temp_closed')
    .lt('temp_closed_at', fiveDaysAgo)
    .is('last_reply_at', null)
    .not('conversation_state', 'in', `(${TERMINAL.map(s => `"${s}"`).join(',')})`)
    .not('pipeline_stage', 'eq', 'Inactive');

  // Replied, but before the temp close (nothing since)
  const { data: stale2 } = await sb
    .from('contacts')
    .select('*')
    .eq('conversation_state', 'temp_closed')
    .lt('temp_closed_at', fiveDaysAgo)
    .not('last_reply_at', 'is', null)
    .not('conversation_state', 'in', `(${TERMINAL.map(s => `"${s}"`).join(',')})`)
    .not('pipeline_stage', 'eq', 'Inactive');

  const combined = [...(stale1 || []), ...(stale2 || [])];
  // Deduplicate by id
  const seen = new Set();
  return combined.filter(c => {
    if (seen.has(c.id)) return false;
    // For stale2: ensure last_reply_at was before temp_closed_at
    if (c.last_reply_at && c.temp_closed_at && c.last_reply_at >= c.temp_closed_at) return false;
    seen.add(c.id);
    return true;
  });
}

export async function draftTempCloseFollowUp({ contact, deal, conversationHistory, agentPersona }) {
  const historyText = (conversationHistory || []).slice(-6).map(m => {
    const role = m.direction === 'outbound' ? 'ROCO' : (contact?.name || 'INVESTOR');
    return `[${role} via ${m.channel}]\n${m.body}`;
  }).join('\n\n---\n\n');

  const daysSince = contact.temp_closed_at
    ? Math.floor((Date.now() - new Date(contact.temp_closed_at).getTime()) / 86400000)
    : 5;

  const agentCtx = await getTempCloseContext();
  const prompt = `${agentCtx}You are drafting a re-engagement message on behalf of Dom, a senior fundraising professional.

This investor indicated they would get back to us. They have not responded.
Do NOT reference the time elapsed. Do NOT guilt them.
Do NOT say "just checking in", "following up", or "circling back".

Write a short, natural message that re-ignites the conversation based on where it left off.
Reference the deal or something specific from the conversation. Under 4 sentences.
One clear, low-pressure ask at the end. Sign off as: Dom.

VOICE RULES:
- No em dashes. No exclamation marks. No "hope you are well".
- Peer-level. Direct. Specific.
- Do not mention their lack of response.

DEAL: ${deal?.name || 'the deal'}
INVESTOR: ${contact?.name} at ${contact?.company_name}
TEMP CLOSE REASON: ${contact?.temp_closed_reason || 'investor said they would review and get back'}

CONVERSATION:
${historyText || 'No history available.'}

Draft the re-engagement message now (reply text only):`;

  const response = await getAnthropic().messages.create({
    model:      'claude-sonnet-4-6',
    max_tokens: 200,
    messages:   [{ role: 'user', content: prompt }],
  });

  return response.content[0]?.text?.trim() || null;
}
