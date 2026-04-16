/**
 * outreach/messageConstructor.js
 * Deal-isolated, null-safe message construction.
 * Every call loads the deal fresh from Supabase.
 * Never references a wrong deal. Never sends null names.
 */

import Anthropic from '@anthropic-ai/sdk';
import OpenAI from 'openai';
import { getSupabase } from '../core/supabase.js';
import { buildGuidanceBlock } from '../services/guidanceService.js';
import { getOutreachContext } from '../core/agentContext.js';
import { retrieveLinkedInProfile } from '../integrations/unipileClient.js';

let _anthropic, _openai;
function getAnthropic() { if (!_anthropic) _anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY }); return _anthropic; }
function getOpenAI() { if (!_openai) _openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY }); return _openai; }

const SYSTEM_PROMPT = `You are Dom's personal writing assistant. Dom is a senior fundraising professional. You write outreach messages on his behalf to potential investors.

Writing style rules:
- Write as if Dom already knows this person. Warm and familiar.
- Never use em-dashes. Never use hashtags. Never bullet points in copy.
- Proper grammar, ultra-conversational. Short sentences.
- Never be salesy. Reference specific past investments from their portfolio.
- One clear ask at the end.
- Subject lines: curiosity-driven, under 8 words.
- Maximum 150 words for email body. LinkedIn DMs: 3-5 sentences.
- ALWAYS write with FULL CONFIDENCE. Never express uncertainty about the prospect's focus, background, or interest. Use whatever research you have and write as if you did your homework. If specific thesis data is unavailable, write "given your focus at [Firm]" or "I was looking at [Firm] and thought this was worth a conversation" — never say "I'm not sure of your focus" or "I'm not certain you cover this".

FORBIDDEN PHRASES (never use any of these):
"Hope this finds you well", "I wanted to reach out", "synergy", "exciting opportunity", "impressive portfolio",
"I'm not sure of your", "I'm not certain", "I don't know if you", "I wasn't sure if",
"not sure if this is your focus", "not sure if this fits", "may or may not be",
"might not be relevant", "this may not be in your wheelhouse", "apologies if this isn't relevant"`;

/**
 * Construct a personalised outreach message with full deal isolation.
 * @param {object} contact - Contact record from Supabase
 * @param {object} firm - Firm record from Supabase
 * @param {string} dealId - Deal ID to load fresh from Supabase
 * @param {string} messageType - 'email_initial' | 'email_follow_up' | 'linkedin_dm' | 'linkedin_follow_up' | 'email_reply'
 * @param {string|null} editInstructions - Optional edit instructions
 * @returns {object|null} { subject_a, subject_b, body, deal_id } or null if rejected
 */
export async function constructOutreachMessage(contact, firm, dealId, messageType, editInstructions = null, mode = 'investor_outreach') {
  const sb = getSupabase();

  // GATE 1: Validate contact name
  if (!contact.name || contact.name.trim() === '' || contact.name.toLowerCase() === 'null') {
    console.error(`[MSG] SKIP: Contact ${contact.id} has no valid name`);
    await sb.from('contacts').update({
      pipeline_stage: 'skipped_no_name',
      notes: (contact.notes || '') + '\n[SKIPPED: no valid name]',
    }).eq('id', contact.id).catch(() => {});
    return null;
  }

  const firstName = contact.name.trim().split(' ')[0];
  if (!firstName || firstName.toLowerCase() === 'null') {
    console.error(`[MSG] SKIP: Cannot extract first name for contact ${contact.id} (name: "${contact.name}")`);
    return null;
  }

  // GATE 2: Load deal fresh from Supabase — never use stale or global reference
  const { data: freshDeal, error: dealErr } = await sb
    .from('deals')
    .select('*')
    .eq('id', dealId)
    .single();

  if (dealErr || !freshDeal) {
    console.error(`[MSG] SKIP: Could not load deal ${dealId} from Supabase: ${dealErr?.message}`);
    return null;
  }

  const isLinkedIn = messageType === 'linkedin_dm' || messageType === 'linkedin_follow_up';
  const stageLabel = {
    email_initial: 'INTRO',
    email_follow_up: 'FOLLOW-UP',
    linkedin_dm: 'LinkedIn DM',
    linkedin_follow_up: 'LinkedIn Follow-up',
    email_reply: 'Reply',
  }[messageType] || 'INTRO';

  const GENERIC_FIRMS = new Set([
    'angel investor', 'angel investors', 'independent investor', 'independent',
    'self-employed', 'self employed', 'freelance', 'consultant', 'private investor',
  ]);
  const isAngel = contact.is_angel || contact.contact_type === 'angel';
  const firmName = firm?.name || contact.company_name;
  const isGenericFirm = isAngel || !firmName || GENERIC_FIRMS.has(firmName.toLowerCase().trim());
  const resolvedFirmName = isGenericFirm ? null : firmName;
  const contactTypeLabel = isAngel ? 'angel investor' : 'investor';
  const firmLine = resolvedFirmName ? `at ${resolvedFirmName}` : '';

  const pastInvestmentsArr = Array.isArray(firm?.past_investments)
    ? firm.past_investments.filter(Boolean)
    : (firm?.past_investments ? [firm.past_investments] : []);
  const pastInvestments = pastInvestmentsArr.length
    ? pastInvestmentsArr.slice(0, 8).join(', ')
    : 'Not on record';
  const investmentThesis = firm?.investment_thesis || firm?.thesis || 'not available';
  const whyThisFirm = firm?.match_rationale || firm?.justification || contact?.why_this_firm || 'strong sector and geography alignment';
  const aumLine = firm?.aum || firm?.aum_fund_size || firm?.fund_size ? `- Fund size / AUM: ${firm?.aum || firm?.aum_fund_size || firm?.fund_size}` : null;
  const stageOfInvestment = firm?.stage_focus || firm?.investment_stage || null;

  // Load phase-specific guidance — investor outreach gets only identity + voice + outreach rules
  const guidanceBlock = mode === 'investor_outreach'
    ? await getOutreachContext()
    : await buildGuidanceBlock(mode);

  // Best-effort LinkedIn profile fetch for hyper-personalisation.
  // Uses the contact's stored provider_id or LinkedIn URL. Never blocks — failure = skip.
  let linkedInProfileContext = '';
  const liIdentifier = contact.linkedin_provider_id || contact.linkedin_url || null;
  if (liIdentifier) {
    try {
      const profile = await Promise.race([
        retrieveLinkedInProfile(liIdentifier),
        new Promise(resolve => setTimeout(() => resolve(null), 5000)),
      ]);
      if (profile) {
        const parts = [];
        if (profile.headline) parts.push(`- Headline: ${profile.headline}`);
        if (profile.current_title) parts.push(`- Current role: ${profile.current_title}${profile.current_company ? ` at ${profile.current_company}` : ''}`);
        if (profile.summary) parts.push(`- Summary: ${profile.summary.slice(0, 300)}`);
        if (profile.experience?.length) {
          const recent = profile.experience.slice(0, 2).map(e =>
            [e.title, e.company_name || e.company].filter(Boolean).join(' @ ')
          ).filter(Boolean);
          if (recent.length) parts.push(`- Recent roles: ${recent.join('; ')}`);
        }
        if (parts.length) linkedInProfileContext = '\nLINKEDIN PROFILE (live data):\n' + parts.join('\n');
      }
    } catch {}
  }

  let editNote = '';
  if (editInstructions) {
    editNote = `\n\nDom's edit instructions: ${editInstructions}\nApply these changes. Keep everything else the same.`;
  }

  const userPrompt = `${guidanceBlock}You are crafting a highly personalised ${stageLabel} on behalf of Dom, a professional fundraiser.

IMPORTANT: You are working EXCLUSIVELY on the deal described below. Do not reference any other deals, products, companies, or sectors not mentioned in this deal brief.

DEAL (this is the ONLY deal you are working on):
- Deal Name: ${freshDeal.name}
- Type: ${freshDeal.type || freshDeal.raise_type || 'Investment'}
- Sector: ${freshDeal.sector || 'Technology'}
- Raise Target: ${freshDeal.target_amount ? `£${Number(freshDeal.target_amount).toLocaleString()}` : 'Not specified'}
- Geography: ${freshDeal.geography || 'UK'}
- Description: ${freshDeal.description || ''}

PROSPECT:
- First name: ${firstName}
- Full name: ${contact.name}
- Title: ${contact.job_title || contact.title || 'investor'}
- Type: ${contactTypeLabel}${firmLine ? ` (${firmLine})` : ''}
- LinkedIn: ${contact.linkedin_url || 'unknown'}${linkedInProfileContext}

FIRM RESEARCH:
${resolvedFirmName
  ? `- Firm: ${resolvedFirmName}
- Firm type: ${firm?.firm_type || 'investment firm'}${stageOfInvestment ? `\n- Investment stage focus: ${stageOfInvestment}` : ''}${aumLine ? `\n${aumLine}` : ''}
- Investment thesis: ${investmentThesis}
- Past investments (portfolio): ${pastInvestments}
- Why this deal matches them: ${whyThisFirm}`
  : `- ${firstName} is an independent/angel investor — do NOT reference a firm. Reference their investing background directly.`
}

PERSONALISATION RULES (follow strictly):
1. Use "${firstName}" as salutation — never "null", never "there"
2. MUST reference at least one SPECIFIC named company from their portfolio/past investments — make it feel like you've done your homework
3. Connect that portfolio company to why ${freshDeal.name} is a natural next bet for them
4. Message must be about "${freshDeal.name}" ONLY
5. Ultra-conversational — as if Dom has known this person for years
6. No em-dashes. No hashtags. No bullet points in body.
7. Sign off as "Dom"
8. ${isLinkedIn ? 'LinkedIn DM: 3-5 sentences max. No subject lines needed.' : 'Email: 5-8 sentences. Generate TWO subject line options (A and B), curiosity-driven, under 8 words.'}
9. Return ONLY valid JSON: ${isLinkedIn ? '{ "subject_a": null, "subject_b": null, "body": "..." }' : '{ "subject_a": "...", "subject_b": "...", "body": "..." }'}

IF you cannot construct a proper message, return: { "error": "insufficient_data", "reason": "brief explanation" }
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
    console.warn(`[MSG] Claude failed for ${contact.name} — trying GPT:`, err.message);
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
      console.error(`[MSG] Both AI models failed for ${contact.name}:`, err.message);
      return null;
    }
  }

  if (!parsed) {
    console.warn(`[MSG] Could not parse JSON response for ${contact.name}`);
    return null;
  }

  if (parsed.error) {
    console.warn(`[MSG] LLM returned error for ${contact.name}: ${parsed.reason}`);
    await sb.from('contacts').update({
      pipeline_stage: 'skipped_insufficient_data',
      notes: (contact.notes || '') + `\n[SKIPPED: ${parsed.reason}]`,
    }).eq('id', contact.id).catch(() => {});
    return null;
  }

  if (!parsed.body) {
    console.warn(`[MSG] Empty body for ${contact.name}`);
    return null;
  }

  // GATE 3: Post-generation sanity check — reject if wrong deal name appears
  const bodyLower = parsed.body.toLowerCase();
  let otherDeals = [];
  try { const r = await sb.from('deals').select('name').neq('id', freshDeal.id); otherDeals = r.data || []; } catch {}

  for (const other of (otherDeals || [])) {
    if (other.name && bodyLower.includes(other.name.toLowerCase())) {
      console.error(`[MSG] CRITICAL: Message for "${freshDeal.name}" referenced wrong deal "${other.name}" — rejected`);
      await sb.from('contacts').update({
        pipeline_stage: 'skipped_wrong_deal_context',
        notes: (contact.notes || '') + `\n[SKIPPED: LLM referenced wrong deal "${other.name}"]`,
      }).eq('id', contact.id).catch(() => {});
      return null;
    }
  }

  return {
    subject_a: parsed.subject_a || null,
    subject_b: parsed.subject_b || null,
    body: parsed.body,
    deal_id: freshDeal.id,
  };
}

function extractJSON(text) {
  try {
    const match = text?.match(/\{[\s\S]*\}/);
    if (match) return JSON.parse(match[0]);
  } catch {}
  return null;
}
