/**
 * outreach/linkedinDrafter.js — LinkedIn DM drafting
 *
 * Same Claude/GPT/template fallback chain as emailDrafter.js
 * LinkedIn-specific constraints:
 * - Connection request notes: max 300 characters
 * - DMs: max 1000 characters
 * - Ultra-conversational, no formal sign-off
 */

import Anthropic from '@anthropic-ai/sdk';
import OpenAI from 'openai';
import { getDeal } from '../core/dealContext.js';
import { getContactProp } from '../crm/notionContacts.js';
import { info, warn, error } from '../core/logger.js';
import { getTemplates } from '../core/supabaseSync.js';
import { buildGuidanceBlock } from '../services/guidanceService.js';

const LINKEDIN_SYSTEM_PROMPT = `You are Dom's writing assistant for LinkedIn messages. Dom raises capital for private deals. You produce the FINAL, send-ready message — it must read exactly like Dom typed it himself.

LINKEDIN RULES — non-negotiable:
- Maximum 1000 characters for DMs. Stay comfortably under.
- Ultra-conversational — like a text from a smart contact, not an email.
- No greeting fluff. Start with their name or jump straight in.
- No formal sign-off. End after the ask — nothing more.
- One sentence on the deal, one question. Do not pad it out.
- Reference something specific about their work or portfolio if research is available. If not, write something that flows naturally — don't force a reference that doesn't make sense.
- Every word must make sense. If something in the brief doesn't fit, rewrite it so the message flows from start to finish.
- FORBIDDEN: "reaching out", "hope you're well", "exciting opportunity", "just wanted to", "touching base", placeholder text like [name] or [company], corporate jargon of any kind.`;

let anthropicClient;
let openaiClient;

function getAnthropic() {
  if (!anthropicClient) anthropicClient = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  return anthropicClient;
}

function getOpenAI() {
  if (!openaiClient) openaiClient = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  return openaiClient;
}

export async function draftLinkedInDM(contactPage, researchData, type = 'intro') {
  const name = getContactProp(contactPage, 'Name');
  const firstName = name?.split(' ')[0] || name;
  const firm = getContactProp(contactPage, 'Company Name') || 'their firm';
  const deal = getDeal();

  const maxChars = type === 'connection_request' ? 300 : 1000;

  // Try to get template from Supabase
  const templateBody = await getLinkedInTemplate(type);

  const guidanceBlock = await buildGuidanceBlock('investor_outreach').catch(() => '');

  const userPrompt = `${guidanceBlock}Write a LinkedIn ${type === 'connection_request' ? 'connection request note' : 'DM'} from Dom to ${firstName} at ${firm}.

Deal: ${deal.name} — ${deal.description || deal.sector}
Research: ${researchData?.approachAngle || 'General interest in ' + deal.sector}
Comparable deals they have done: ${(researchData?.comparableDeals || []).join(', ') || 'None on record'}

Maximum ${maxChars} characters. Count carefully.
${type === 'connection_request' ? 'This is a connection request note — 1-2 sentences max.' : ''}

Return only the message text. No JSON, no labels, no formatting. Just the message.`;

  // Try Claude first
  try {
    const response = await Promise.race([
      getAnthropic().messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 512,
        system: LINKEDIN_SYSTEM_PROMPT,
        messages: [{ role: 'user', content: userPrompt }],
      }),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout')), 10000)),
    ]);
    const text = response.content[0].text.trim();
    if (text.length <= maxChars + 20) { // Small tolerance
      info(`LinkedIn DM drafted via Claude for ${firstName}`);
      return { body: text.slice(0, maxChars), type };
    }
    throw new Error('Response too long');
  } catch (err) {
    warn(`Claude failed for LinkedIn DM (${firstName}) — trying GPT`);
  }

  // GPT fallback
  try {
    const response = await getOpenAI().chat.completions.create({
      model: 'gpt-5.2',
      messages: [
        { role: 'system', content: LINKEDIN_SYSTEM_PROMPT },
        { role: 'user', content: userPrompt },
      ],
      max_completion_tokens: 256,
    });
    const text = response.choices[0].message.content.trim();
    info(`LinkedIn DM drafted via GPT for ${firstName}`);
    return { body: text.slice(0, maxChars), type };
  } catch (err) {
    warn(`GPT also failed for LinkedIn DM (${firstName}) — using template`);
  }

  // Template fallback
  if (templateBody) {
    const body = templateBody
      .replace(/{{firstName}}/g, firstName)
      .replace(/{{firmName}}/g, firm)
      .replace(/{{dealName}}/g, deal.name || '')
      .replace(/{{dealBrief}}/g, deal.description?.slice(0, 100) || '');
    return { body: body.slice(0, maxChars), type };
  }

  // Last resort
  return {
    body: `Hi ${firstName} — thought ${deal.name || 'this deal'} might be relevant given your work at ${firm}. Happy to share more detail. Dom`,
    type,
  };
}

async function getLinkedInTemplate(type) {
  try {
    const templates = await getTemplates();
    const linkedIn = templates.filter(t => t.type === 'linkedin' && t.is_active !== false);
    if (!linkedIn.length) return null;

    // For connection_request / intro: prefer templates with 'intro' in name
    // For follow-up DMs: prefer templates with 'follow' in name
    const isIntro = type === 'connection_request' || type === 'intro';
    const match = isIntro
      ? (linkedIn.find(t => (t.name || '').toLowerCase().includes('intro')) || linkedIn[0])
      : (linkedIn.find(t => (t.name || '').toLowerCase().includes('follow')) || linkedIn[0]);

    return match?.body || match?.body_a || null;
  } catch {
    return null;
  }
}

/**
 * Log a LinkedIn message to Supabase.
 */
export async function logLinkedInMessage({ dealId, contactId, direction, body, status }) {
  const { getSupabase } = await import('../core/supabase.js');
  const sb = getSupabase();
  if (!sb) return null;
  try {
    const { data } = await sb.from('linkedin_messages').insert([{
      deal_id: dealId,
      contact_id: contactId,
      direction,
      body,
      status,
      ...(direction === 'inbound' ? { received_at: new Date().toISOString() } : { sent_at: new Date().toISOString() }),
    }]).select().single();
    return data;
  } catch {
    return null;
  }
}
