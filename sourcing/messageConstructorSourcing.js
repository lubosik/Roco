/**
 * sourcing/messageConstructorSourcing.js
 * Deal-isolated message construction for company sourcing outreach.
 * Investor firm → Company (CEO/Founder/MD).
 * Mirrors messageConstructor.js architecture exactly.
 */

import Anthropic from '@anthropic-ai/sdk';
import OpenAI from 'openai';
import { getSupabase } from '../core/supabase.js';
import { buildGuidanceBlock } from '../services/guidanceService.js';

let _anthropic, _openai;
function getAnthropic() { if (!_anthropic) _anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY }); return _anthropic; }
function getOpenAI()    { if (!_openai)    _openai    = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });    return _openai; }

const SYSTEM_PROMPT = `You are writing on behalf of an investment firm reaching out to company founders and executives.
You are approaching a company to explore whether there could be a fit for investment, acquisition, or partnership.

Writing style rules:
- Write as if the firm already knows about this company. Warm and direct.
- Never use em-dashes. Never use hashtags. Never bullet points in copy.
- Short sentences. Confident tone. Reference specific company details.
- This is an investor approaching a company — not a fundraiser pitching to investors.
- Never be generic. Reference their actual business, product, or signals.
- One clear ask at the end: a brief call.
- Subject lines: curiosity-driven, under 8 words.
- Email body: 4-6 sentences max. LinkedIn DMs: 3-4 sentences max.

FORBIDDEN: "Hope this finds you well", "I wanted to reach out", "synergy", "exciting opportunity", "impressive company"`;

/**
 * Construct a personalised outreach message for company sourcing.
 * @param {object} contact - company_contacts row
 * @param {object} company - target_companies row
 * @param {string} campaignId - sourcing campaign ID
 * @param {string} messageType - 'email_initial' | 'email_follow_up' | 'linkedin_dm' | 'linkedin_follow_up'
 * @param {string|null} editInstructions
 * @returns {object|null} { subject_a, subject_b, body, campaign_id } or null
 */
export async function constructCompanySourcingMessage(contact, company, campaignId, messageType, editInstructions = null) {
  const sb = getSupabase();

  // GATE 1: Validate contact name
  if (!contact.name || contact.name.trim() === '' || contact.name.toLowerCase() === 'null') {
    console.error(`[SOURCING MSG] SKIP: Contact ${contact.id} has no valid name`);
    await sb.from('company_contacts').update({
      pipeline_stage: 'skipped_no_name',
      notes: (contact.notes || '') + '\n[SKIPPED: no valid name]',
      updated_at: new Date().toISOString(),
    }).eq('id', contact.id).then(null, () => {});
    return null;
  }

  const firstName = contact.name.trim().split(' ')[0];
  if (!firstName || firstName.toLowerCase() === 'null') {
    console.error(`[SOURCING MSG] SKIP: Cannot extract first name for contact ${contact.id}`);
    return null;
  }

  // GATE 2: Load campaign fresh from Supabase — never use stale reference
  const { data: freshCampaign, error: campaignErr } = await sb
    .from('sourcing_campaigns')
    .select('*')
    .eq('id', campaignId)
    .single();

  if (campaignErr || !freshCampaign) {
    console.error(`[SOURCING MSG] SKIP: Could not load campaign ${campaignId}: ${campaignErr?.message}`);
    return null;
  }

  const isLinkedIn = messageType === 'linkedin_dm' || messageType === 'linkedin_follow_up';

  // Load guidance fresh — injected at top of prompt
  const guidanceBlock = await buildGuidanceBlock('company_sourcing');

  let editNote = '';
  if (editInstructions) {
    editNote = `\n\nEdit instructions: ${editInstructions}\nApply these changes. Keep everything else the same.`;
  }

  const userPrompt = `${guidanceBlock}You are crafting a highly personalised ${isLinkedIn ? 'LinkedIn DM' : 'email'} on behalf of ${freshCampaign.firm_name || 'an investment firm'}, a ${freshCampaign.firm_type || 'PE'} firm.

IMPORTANT: This message is from an INVESTOR approaching a COMPANY. You are reaching out to a founder or executive to explore whether their company could be an investment opportunity. Do NOT reference any other campaigns or companies beyond what is provided.

RECIPIENT:
- First name: ${firstName}
- Full name: ${contact.name}
- Title: ${contact.title || 'Founder/CEO'}
- Company: ${company.company_name}

COMPANY RESEARCH:
- What they do: ${company.product_description || 'Not available'}
- Revenue estimate: ${company.estimated_revenue || 'not publicly available'}
- Intent signals found: ${company.intent_signals_found || 'strong sector alignment'}
- Why they match our thesis: ${company.why_matches || ''}
- Geography: ${company.geography || freshCampaign.target_geography}
- Sector: ${company.sector || freshCampaign.target_sector}

INVESTMENT FIRM:
- Firm: ${freshCampaign.firm_name || 'Our firm'}
- Type: ${freshCampaign.firm_type || 'Investment Firm'}
- Thesis: ${freshCampaign.investment_thesis || 'Not specified'}
- Deal type: ${freshCampaign.deal_type || 'Investment'}
- Investment size: ${freshCampaign.investment_size || 'flexible'}

RULES — FOLLOW EXACTLY:
1. Address them by first name: "${firstName}" — never "null", never "there"
2. Reference something SPECIFIC about their company from the research above — their product, a recent signal, or their sector position
3. Position the firm as an interested investor exploring a potential fit — not a cold sales pitch
4. Keep it conversational and confident — as if the firm already knows about their business
5. No em-dashes anywhere
6. No hashtags, symbols, or bullet points
7. ${isLinkedIn ? 'LinkedIn DM: 3-4 sentences max. No subject lines.' : 'Email: 4-6 sentences. Generate TWO subject line options (A and B), curiosity-driven, under 8 words.'}
8. Sign off with the firm name: "${freshCampaign.firm_name || 'Our Team'}"
9. Return ONLY valid JSON: ${isLinkedIn ? '{ "subject_a": null, "subject_b": null, "body": "..." }' : '{ "subject_a": "...", "subject_b": "...", "body": "..." }'}

Example tone: "We have been watching companies like ${company.company_name} in [sector]. Given [specific signal], we think there could be an interesting conversation worth having. We are [firm type] with [deal type] focus in this space — would you be open to a brief call?"

IF YOU CANNOT CONSTRUCT A PROPER MESSAGE — return: { "error": "insufficient_data", "reason": "brief explanation" }
DO NOT fabricate company details. DO NOT reference other campaigns or companies.
${editNote}`;

  // Try Claude first
  let parsed = null;
  try {
    const response = await Promise.race([
      getAnthropic().messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 1024,
        system: SYSTEM_PROMPT,
        messages: [{ role: 'user', content: userPrompt }],
      }),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Claude timeout')), 15000)),
    ]);
    const text = response.content[0].text;
    parsed = extractJSON(text);
  } catch (err) {
    console.warn(`[SOURCING MSG] Claude failed for ${contact.name} — trying GPT:`, err.message);
  }

  // GPT fallback
  if (!parsed) {
    try {
      const response = await getOpenAI().chat.completions.create({
        model: 'gpt-4o',
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: userPrompt },
        ],
        max_tokens: 1024,
      });
      parsed = extractJSON(response.choices[0].message.content);
    } catch (err) {
      console.error(`[SOURCING MSG] Both AI models failed for ${contact.name}:`, err.message);
      return null;
    }
  }

  if (!parsed) {
    console.warn(`[SOURCING MSG] Could not parse JSON for ${contact.name}`);
    return null;
  }

  if (parsed.error) {
    console.warn(`[SOURCING MSG] LLM returned error for ${contact.name}: ${parsed.reason}`);
    await sb.from('company_contacts').update({
      pipeline_stage: 'skipped_insufficient_data',
      notes: (contact.notes || '') + `\n[SKIPPED: ${parsed.reason}]`,
      updated_at: new Date().toISOString(),
    }).eq('id', contact.id).then(null, () => {});
    return null;
  }

  if (!parsed.body) {
    console.warn(`[SOURCING MSG] Empty body for ${contact.name}`);
    return null;
  }

  // GATE 3: Post-generation sanity check — reject if other campaign names appear
  const bodyLower = parsed.body.toLowerCase();
  try {
    const { data: otherCampaigns } = await sb.from('sourcing_campaigns')
      .select('name')
      .neq('id', freshCampaign.id);

    for (const other of (otherCampaigns || [])) {
      if (other.name && bodyLower.includes(other.name.toLowerCase())) {
        console.error(`[SOURCING MSG] Cross-campaign contamination detected for ${contact.name} — blocked`);
        return null;
      }
    }
  } catch {}

  return {
    subject_a:   parsed.subject_a || null,
    subject_b:   parsed.subject_b || null,
    body:        parsed.body,
    campaign_id: freshCampaign.id,
  };
}

function extractJSON(text) {
  try {
    const match = (text || '').match(/\{[\s\S]*\}/);
    if (match) return JSON.parse(match[0]);
  } catch {}
  return null;
}
