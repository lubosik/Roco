/**
 * research/personResearcher.js
 * Investor research router:
 * - Reuse/canonicalize existing structured data first (zero-cost path)
 * - Gemini grounded search is the default live-research provider
 * - Grok is optional fallback
 * Tracks completion via a [PERSON_RESEARCHED] marker in the notes field.
 */

import { getResearchContext } from '../core/agentContext.js';

const DEFAULT_GEMINI_MODELS = [
  'gemini-2.5-flash',
  'gemini-2.5-pro',
];

const DEFAULT_GROK_MODEL = 'grok-4.1-fast';

function boolFromEnv(value, fallback = false) {
  if (value == null || value === '') return fallback;
  return !['0', 'false', 'no', 'off'].includes(String(value).toLowerCase());
}

function getResearchConfig() {
  return {
    primaryProvider: (process.env.RESEARCH_PRIMARY_PROVIDER || 'gemini').toLowerCase(),
    enableGrokFallback: boolFromEnv(process.env.RESEARCH_ENABLE_GROK_FALLBACK, true),
    cacheTtlDays: Number(process.env.RESEARCH_CACHE_TTL_DAYS || 90),
    geminiModels: (process.env.RESEARCH_GEMINI_MODELS || '')
      .split(',')
      .map(m => m.trim())
      .filter(Boolean)
      .slice(0, 5)
      .concat(DEFAULT_GEMINI_MODELS)
      .filter((model, index, all) => all.indexOf(model) === index),
    grokModel: process.env.RESEARCH_GROK_MODEL || DEFAULT_GROK_MODEL,
  };
}

function inferContactType(contact) {
  if (contact.contact_type) return contact.contact_type;
  const company = (contact.company_name || '').trim().toLowerCase();
  if (contact.is_angel || !company) return 'individual';
  return company.includes('angel investor') ? 'angel' : 'institutional';
}

export function hasCoreResearchFields(record) {
  return !!(record?.past_investments && record?.investment_thesis && record?.sector_focus);
}

export function hasFreshResearch(record, ttlDays = getResearchConfig().cacheTtlDays) {
  if (!record?.last_researched_at) return false;
  const last = Date.parse(record.last_researched_at);
  if (Number.isNaN(last)) return false;
  return (Date.now() - last) < ttlDays * 24 * 60 * 60 * 1000;
}

export function normalizePersonResearch(record = {}) {
  const notes = typeof record.notes === 'string' ? record.notes : '';
  const type = inferContactType(record);
  const description = record.firm_description || record.description || record.research_notes || notes || null;
  return {
    job_title: record.job_title || record.decision_maker_title || record.primary_contact_title || null,
    company_name: record.company_name || record.firm_name || null,
    firm_description: description ? String(description).substring(0, 600) : null,
    firm_aum: record.firm_aum || record.aum_fund_size || (record.aum_millions ? `$${record.aum_millions}M` : null),
    investment_stage: record.investment_stage || record.preferred_stage || null,
    typical_cheque: record.typical_cheque || record.typical_cheque_size || null,
    sector_focus: record.sector_focus || record.preferred_industries || null,
    geography: record.geography || record.preferred_geographies || record.hq_country || record.hq_location || null,
    past_investments: record.past_investments || null,
    investment_thesis: record.investment_thesis || null,
    linkedin_url: record.linkedin_url || record.decision_maker_linkedin || null,
    recent_news: record.recent_news || null,
    contact_type_confirmed: type,
    confidence: hasCoreResearchFields(record) ? 'high' : 'medium',
  };
}

function buildPrompt(contact, deal) {
  const firm    = contact.company_name || '';
  const isAngel = contact.is_angel || contact.contact_type === 'angel';
  const isIndividual = contact.contact_type === 'individual';

  const investorContext = (isAngel || isIndividual)
    ? `${contact.name} is an individual investor — investing personal or family capital, not on behalf of a fund.
Research their personal investment history, typical personal cheque sizes,
sectors they have backed personally, and any public statements about their investing.
Do NOT look for a fund mandate or institutional AUM — they invest their own money.`
    : `${contact.name} is a ${contact.job_title || 'decision-maker'} at ${firm || 'an investment firm'}.
Research the firm's investment mandate, fund size, typical cheque sizes, sectors,
and any recent portfolio companies relevant to ${deal.sector || 'the deal sector'}.
Also research ${contact.name}'s specific role and any public statements they have made.`;

  return `Research this investor thoroughly for a fundraising deal.

INVESTOR:
Name: ${contact.name}
${firm ? `Firm: ${firm}` : 'No firm affiliation — independent investor'}
Title: ${contact.job_title || 'Unknown'}
Type: ${(isAngel || isIndividual) ? 'Individual Investor (personal capital)' : 'Institutional Investor'}

DEAL CONTEXT:
Sector: ${deal.sector || 'AI / SaaS'}
Stage: ${deal.raise_type || 'Pre-Seed/Seed'}
Geography: ${deal.geography || 'UK, Europe'}

RESEARCH FOCUS:
${investorContext}

Using web search, find and return:
1. Their exact current title and seniority
2. ${(isAngel || isIndividual) ? 'Their personal investment activity and background' : 'The firm they work at and its investment focus'}
3. ${(isAngel || isIndividual) ? 'Typical personal cheque size and sectors backed' : "The firm's investment focus, stage preference, typical cheque size, AUM"}
4. 3-5 specific companies they have backed (personally for individuals/angels, via firm for institutional)
5. Their investment thesis or stated focus
6. Geographies they invest in
7. Any recent news about them${(isAngel || isIndividual) ? '' : ' or their firm'} (last 12 months)
8. Leave linkedin_url as null unless the profile was already verified elsewhere

CONTACT TYPE CLASSIFICATION RULES (for contact_type_confirmed field):
- "institutional" = operates a formal fund, VC, PE firm, family office, or invests on behalf of others/LPs
- "angel" = explicitly self-identifies as an angel investor, backs early startups with personal capital, often listed on AngelList or similar
- "individual" = high net worth individual or family wealth — invests personally but does not typically call themselves an angel (e.g. HNWI, family wealth, private investor, exec with personal portfolio)

Return ONLY this JSON (no markdown, no other text):
{
  "job_title": "<exact current title or null>",
  "company_name": "<firm name — null if individual/angel with no firm>",
  "firm_description": "<2-3 sentence overview — firm for institutional, personal bio for individual/angel>",
  "firm_aum": "<AUM e.g. $500m or null — null for individuals/angels>",
  "investment_stage": "<e.g. Pre-seed, Seed, Series A>",
  "typical_cheque": "<e.g. £100k-£500k or null>",
  "sector_focus": "<their actual sectors, comma-separated>",
  "geography": "<geographies they invest in>",
  "past_investments": "<comma-separated portfolio companies, 3-5>",
  "investment_thesis": "<1-2 sentence thesis>",
  "linkedin_url": null,
  "recent_news": "<relevant recent news or null>",
  "contact_type_confirmed": "<institutional|angel|individual — your assessment based on research>",
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
  const { geminiModels } = getResearchConfig();
  const keys = [process.env.GEMINI_API_KEY, process.env.GEMINI_API_KEY_FALLBACK].filter(Boolean);
  if (!keys.length) return null;

  for (const key of keys) {
    for (const model of geminiModels) {
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
  const { grokModel } = getResearchConfig();
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
        model: grokModel,
        input: [{ role: 'user', content: prompt }],
        tools: [{ type: 'web_search' }],
      }),
    });

    if (!res.ok) {
      const errText = await res.text();
      const err = new Error(`Grok ${grokModel} ${res.status}: ${errText.substring(0, 150)}`);
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
    errors.push({ api: 'grok', model: grokModel, status: err.status, message: err.message });
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
  const config = getResearchConfig();
  const normalized = normalizePersonResearch(contact);
  const canReuseStoredResearch = hasCoreResearchFields(contact) || hasFreshResearch(contact, config.cacheTtlDays);

  const hasGemini = !!(process.env.GEMINI_API_KEY || process.env.GEMINI_API_KEY_FALLBACK);
  const hasGrok = !!(process.env.XAI_API_KEY || process.env.GROK_API_KEY);

  if (canReuseStoredResearch) {
    console.log(`[PERSON RESEARCH] ${contact.name}: using cached/structured data`);
    return normalized;
  }

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
  const providers = config.primaryProvider === 'grok'
    ? ['grok', 'gemini']
    : ['gemini', 'grok'];

  for (const provider of providers) {
    if (provider === 'grok' && !config.enableGrokFallback && config.primaryProvider !== 'grok') continue;
    const result = provider === 'gemini'
      ? await tryGemini(prompt, contact.name, errors)
      : await tryGrok(prompt, contact.name, errors);
    if (result) return result;
    if (errors.length > 0) {
      console.warn(`[PERSON RESEARCH] ${provider} failed for ${contact.name}`);
    }
  }

  if (hasCoreResearchFields(contact)) {
    console.warn(`[PERSON RESEARCH] Live research failed for ${contact.name} — reusing stored fields`);
    return normalized;
  }

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
