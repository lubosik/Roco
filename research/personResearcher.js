/**
 * research/personResearcher.js
 * Deep-researches individual investors using Gemini (primary) or Grok (fallback).
 * Both use real-time web search grounding.
 * Tracks completion via a [PERSON_RESEARCHED] marker in the notes field.
 */

import { getResearchContext } from '../core/agentContext.js';

const GEMINI_MODELS = [
  'gemini-2.5-flash',
  'gemini-1.5-flash',
  'gemini-1.5-pro',
];

const GROK_MODEL = 'grok-4-latest';

function buildPrompt(contact, deal) {
  const firm    = contact.company_name || '';
  const isAngel = contact.is_angel || contact.contact_type === 'angel';

  const investorContext = isAngel
    ? `${contact.name} is an ANGEL INVESTOR — an individual investing personal capital, not on behalf of a fund.
Research their personal investment history, typical personal cheque sizes (usually $25K-$500K),
sectors they have backed personally, and any public statements about their investing.
Do NOT look for a fund mandate or institutional AUM — they invest their own money.`
    : `${contact.name} is a ${contact.job_title || 'decision-maker'} at ${firm || 'an investment firm'}.
Research the firm's investment mandate, fund size, typical cheque sizes, sectors,
and any recent portfolio companies relevant to ${deal.sector || 'the deal sector'}.
Also research ${contact.name}'s specific role and any public statements they have made.`;

  return `Research this investor thoroughly for a fundraising deal.

INVESTOR:
Name: ${contact.name}
${firm ? `Firm: ${firm}` : 'No firm affiliation — independent/angel investor'}
Title: ${contact.job_title || 'Unknown'}
Type: ${isAngel ? 'Angel Investor (personal capital)' : 'Institutional Investor'}
${contact.linkedin_url ? `LinkedIn: ${contact.linkedin_url}` : ''}

DEAL CONTEXT:
Sector: ${deal.sector || 'AI / SaaS'}
Stage: ${deal.raise_type || 'Pre-Seed/Seed'}
Geography: ${deal.geography || 'UK, Europe'}

RESEARCH FOCUS:
${investorContext}

Using web search, find and return:
1. Their exact current title and seniority
2. ${isAngel ? 'Their personal investment activity and background' : 'The firm they work at and its investment focus'}
3. ${isAngel ? 'Typical personal cheque size and sectors backed' : "The firm's investment focus, stage preference, typical cheque size, AUM"}
4. 3-5 specific companies they have backed (personally for angels, via firm for institutional)
5. Their investment thesis or stated focus
6. Geographies they invest in
7. Any recent news about them${isAngel ? '' : ' or their firm'} (last 12 months)
8. Their LinkedIn URL if findable

Return ONLY this JSON (no markdown, no other text):
{
  "job_title": "<exact current title or null>",
  "company_name": "<firm name — null if angel with no firm>",
  "firm_description": "<2-3 sentence overview — firm for institutional, personal bio for angel>",
  "firm_aum": "<AUM e.g. $500m or null — null for angels>",
  "investment_stage": "<e.g. Pre-seed, Seed, Series A>",
  "typical_cheque": "<e.g. £100k-£500k or null>",
  "sector_focus": "<their actual sectors, comma-separated>",
  "geography": "<geographies they invest in>",
  "past_investments": "<comma-separated portfolio companies, 3-5>",
  "investment_thesis": "<1-2 sentence thesis>",
  "linkedin_url": "<LinkedIn URL or null>",
  "recent_news": "<relevant recent news or null>",
  "contact_type_confirmed": "<angel|individual_at_firm|firm — your assessment based on research>",
  "confidence": "high|medium|low"
}`;
}

function parseJsonFromText(text) {
  const match = text.replace(/```json\n?|```/g, '').trim().match(/\{[\s\S]*\}/);
  if (!match) throw new Error('No JSON in response');
  return JSON.parse(match[0]);
}

/** Try Gemini with all models and both keys. Returns result or null. Pushes errors into `errors` array. */
async function tryGemini(prompt, contactName, errors) {
  const primaryKey = process.env.GEMINI_API_KEY;
  if (!primaryKey) return null;

  const keys = [primaryKey, process.env.GEMINI_API_KEY_FALLBACK].filter(Boolean);

  for (const key of keys) {
    for (const model of GEMINI_MODELS) {
      try {
        const body = {
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: { temperature: 0.1, maxOutputTokens: 1024 },
          tools: [{ google_search: {} }],
        };

        const res = await fetch(
          `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`,
          { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }
        );

        if (!res.ok) {
          const errText = await res.text();
          const err = new Error(`${model} ${res.status}: ${errText.substring(0, 150)}`);
          err.status = res.status;
          throw err;
        }

        const data = await res.json();
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '';
        const result = parseJsonFromText(text);
        console.log(`[PERSON RESEARCH] ${contactName}: Gemini ${model} success (${result.confidence || '?'} confidence)`);
        return result;
      } catch (err) {
        errors.push({ api: 'gemini', model, status: err.status, message: err.message });
        console.warn(`[PERSON RESEARCH] Gemini ${model} failed for ${contactName}: ${err.message}`);
      }
    }
    if (key !== keys[keys.length - 1]) {
      console.warn(`[PERSON RESEARCH] Primary Gemini key exhausted for ${contactName} — trying fallback key`);
    }
  }

  return null;
}

/** Try Grok with web search via Responses API. Returns result or null. Pushes errors into `errors` array. */
async function tryGrok(prompt, contactName, errors) {
  const key = process.env.XAI_API_KEY || process.env.GROK_API_KEY;
  if (!key) return null;

  try {
    const res = await fetch('https://api.x.ai/v1/responses', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${key}`,
      },
      body: JSON.stringify({
        model: GROK_MODEL,
        input: [{ role: 'user', content: prompt }],
        tools: [{ type: 'web_search' }],
      }),
    });

    if (!res.ok) {
      const errText = await res.text();
      const err = new Error(`Grok ${GROK_MODEL} ${res.status}: ${errText.substring(0, 150)}`);
      err.status = res.status;
      throw err;
    }

    const data = await res.json();
    // Responses API format: output[] contains message objects
    const outputMsg = (data.output || []).find(o => o.type === 'message');
    const text = outputMsg?.content?.find(c => c.type === 'output_text')?.text || '';
    if (!text) throw new Error('Empty response from Grok');

    const result = parseJsonFromText(text);
    console.log(`[PERSON RESEARCH] ${contactName}: Grok success (${result.confidence || '?'} confidence)`);
    return result;
  } catch (err) {
    errors.push({ api: 'grok', model: GROK_MODEL, status: err.status, message: err.message });
    console.warn(`[PERSON RESEARCH] Grok failed for ${contactName}: ${err.message}`);
    return null;
  }
}

/**
 * Research one investor using web-grounded AI search.
 * Tries Gemini first (all models, both keys), then Grok as final fallback.
 *
 * @param {{ contact: object, deal: object }} params
 * @returns {Promise<object|null>}
 * @throws {Error} with .isQuota=true if all failures were quota/rate-limit errors
 */
export async function researchPerson({ contact, deal }) {
  if (!contact.name) return null;

  const hasGemini = !!process.env.GEMINI_API_KEY;
  const hasGrok = !!(process.env.XAI_API_KEY || process.env.GROK_API_KEY);

  if (!hasGemini && !hasGrok) {
    console.warn('[PERSON RESEARCH] No API keys configured (XAI_API_KEY, GROK_API_KEY, or GEMINI_API_KEY) — skipping');
    return null;
  }

  const firm = contact.company_name || '';
  console.log(`[PERSON RESEARCH] ${contact.name}${firm ? ` at ${firm}` : ''}`);

  const agentCtx = await getResearchContext();
  const basePrompt = buildPrompt(contact, deal);
  const prompt = agentCtx ? `${agentCtx}${basePrompt}` : basePrompt;
  const errors = [];

  // 1. Try Grok (primary)
  const grokResult = await tryGrok(prompt, contact.name, errors);
  if (grokResult) return grokResult;

  // 2. Grok failed — fall back to Gemini
  if (errors.length > 0) {
    console.warn(`[PERSON RESEARCH] Grok failed for ${contact.name} — falling back to Gemini`);
  }

  const geminiResult = await tryGemini(prompt, contact.name, errors);
  if (geminiResult) return geminiResult;

  // All APIs failed — classify the error type so orchestrator can surface it
  const quotaStatuses = [429, 503];
  const isQuota = errors.some(e => quotaStatuses.includes(e.status) || e.message.includes('429') || e.message.toLowerCase().includes('quota') || e.message.toLowerCase().includes('rate limit'));
  const isGrokQuota = errors.filter(e => e.api === 'grok').some(e => quotaStatuses.includes(e.status) || e.message.includes('429'));

  const apisSummary = [...new Set(errors.map(e => e.api))].join(' + ');
  const err = new Error(`All research APIs failed for ${contact.name} (tried: ${apisSummary})`);
  err.isQuota = isQuota;
  err.isGrokFailed = errors.some(e => e.api === 'grok');
  err.allErrors = errors;
  throw err;
}

/** Returns true if a contact's notes contain the research marker */
export function isResearched(notes) {
  return typeof notes === 'string' && notes.includes('[PERSON_RESEARCHED]');
}
