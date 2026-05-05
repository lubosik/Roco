/**
 * research/personResearcher.js
 * Investor research router:
 * - Reuse/canonicalize existing structured data first (zero-cost path)
 * - Try configured grounded-search providers before falling back to synthesis
 * Tracks completion via research markers in the notes field.
 */

import { getResearchContext } from '../core/agentContext.js';

const DEFAULT_GEMINI_MODELS = [
  'gemini-2.5-flash',
  'gemini-2.5-pro',
];

const DEFAULT_GROK_MODEL = process.env.RESEARCH_GROK_MODEL || 'grok-3-fast';

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

function normalizeOpenRouterModel(model) {
  if (!model) return null;
  if (model.includes('/')) return model;
  if (model.startsWith('gemini-')) return `google/${model}`;
  return model;
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

const RESEARCH_MARKERS = [
  '[PERSON_RESEARCH_VERIFIED',
  '[PERSON_RESEARCH_PARTIAL',
  '[PERSON_RESEARCHED',
  '[PERSON_RESEARCHED_FOR_RANKING',
  '[LINKEDIN_PROFILE_RESEARCHED',
];

function hasText(value, minLength = 2) {
  return typeof value === 'string' && value.trim().length >= minLength;
}

function notesInclude(notes, pattern) {
  return typeof notes === 'string' && notes.toLowerCase().includes(pattern);
}

export function classifyPersonResearch(record = {}) {
  const notes = typeof record.notes === 'string' ? record.notes : '';
  if (notes.includes('[PERSON_RESEARCH_VERIFIED')) return 'verified';

  const past = record.past_investments;
  const thesis = record.investment_thesis || record.thesis;
  const sectors = record.sector_focus || record.preferred_industries;
  const geography = record.geography || record.preferred_geographies || record.hq_country || record.hq_location;
  const cheque = record.typical_cheque || record.typical_cheque_size;
  const aum = record.firm_aum || record.aum_fund_size;
  const profile = record.firm_description || record.description || record.research_notes || record.recent_news;

  if (hasCoreResearchFields(record)) return 'verified';

  const evidence = [
    past,
    thesis,
    sectors,
    geography,
    cheque,
    aum,
    profile,
  ].filter(v => hasText(String(v || ''), 4)).length;

  const hasInvestmentSpecifics = hasText(String(past || ''), 4)
    || hasText(String(thesis || ''), 20)
    || hasText(String(sectors || ''), 4);

  if (hasInvestmentSpecifics && evidence >= 3) return 'verified';

  if (
    notes.includes('[PERSON_RESEARCH_PARTIAL') ||
    RESEARCH_MARKERS.some(marker => notes.includes(marker)) ||
    notesInclude(notes, 'linkedin headline:') ||
    notesInclude(notes, 'linkedin summary:') ||
    notesInclude(notes, 'profile:') ||
    evidence > 0 ||
    hasText(record.company_name || record.job_title || record.linkedin_url || '', 3)
  ) {
    return 'partial';
  }

  return 'missing';
}

export function hasVerifiedPersonResearch(record = {}) {
  return classifyPersonResearch(record) === 'verified';
}

export function hasPartialPersonResearch(record = {}) {
  return classifyPersonResearch(record) === 'partial';
}

export function hasFreshResearch(record, ttlDays = getResearchConfig().cacheTtlDays) {
  // last_researched_at doesn't exist as a DB column — use person_researched + updated_at
  // A contact is "fresh" if it has been successfully researched and updated recently
  if (!record?.person_researched) return false;
  // If notes contain a research marker, treat as fresh within TTL
  const notes = typeof record.notes === 'string' ? record.notes : '';
  const hasMarker = RESEARCH_MARKERS.some(m => notes.includes(m));
  if (!hasMarker) return false;
  const last = Date.parse(record.updated_at || record.created_at || '');
  if (Number.isNaN(last)) return true; // has marker but no timestamp → assume fresh
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
    confidence: hasVerifiedPersonResearch(record) ? 'high' : 'medium',
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
  const match = String(text || '').replace(/```json\n?|```/g, '').trim().match(/\{[\s\S]*\}/);
  if (!match) throw new Error('No JSON in response');
  return JSON.parse(match[0]);
}

function summarizeResearchErrors(errors) {
  return errors
    .map(e => `${e.api}/${e.model}: ${e.status || 'no-status'} ${String(e.message || '').replace(/\s+/g, ' ').slice(0, 140)}`)
    .join('; ');
}

function resultHasResearchSignal(result) {
  if (!result || typeof result !== 'object') return false;
  return [
    result.firm_description,
    result.sector_focus,
    result.past_investments,
    result.investment_thesis,
    result.recent_news,
    result.job_title,
    result.company_name,
  ].some(value => hasText(String(value || ''), 4));
}

async function tryOpenRouterResearch(prompt, contactName, errors, {
  api,
  tier = 'web',
  model = null,
  label = null,
  maxTokens = 1536,
}) {
  const resolvedModel = normalizeOpenRouterModel(model);
  try {
    const { orComplete, getModelForTier } = await import('../core/openRouterClient.js');
    const modelName = resolvedModel || getModelForTier(tier);
    const text = await orComplete(prompt, { tier, model: resolvedModel, maxTokens });
    const result = parseJsonFromText(text);
    if (!resultHasResearchSignal(result)) throw new Error('Research response contained no usable fields');
    console.log(`[PERSON RESEARCH] ${contactName}: ${label || modelName} success (${result.confidence || '?'} confidence)`);
    return result;
  } catch (err) {
    errors.push({
      api,
      model: resolvedModel || tier,
      status: err.status || err.code || null,
      message: err.message,
    });
    console.warn(`[PERSON RESEARCH] ${api} failed for ${contactName}: ${err.message}`);
    return null;
  }
}

function buildResearchAttempts(config) {
  const geminiAttempts = config.geminiModels.map(model => ({
    api: 'gemini',
    tier: 'web',
    model,
    label: normalizeOpenRouterModel(model),
  }));

  const perplexityAttempt = {
    api: 'perplexity',
    tier: 'web',
    model: process.env.OR_WEB_MODEL || null,
    label: process.env.OR_WEB_MODEL || 'perplexity/sonar-pro',
  };

  return config.primaryProvider === 'gemini'
    ? [...geminiAttempts, perplexityAttempt]
    : [perplexityAttempt, ...geminiAttempts];
}

async function tryResearchSynthesis(prompt, contactName, errors) {
  const synthesisPrompt = `${prompt}

Live web search failed or returned no usable fields. Make one final best-effort pass using only reliable knowledge in the supplied context and the model's existing knowledge. Do not invent portfolio companies, cheque size, AUM, recent news, or LinkedIn URLs. Use null for anything uncertain and set confidence to "low" unless the supplied context clearly supports the answer.`;

  return tryOpenRouterResearch(synthesisPrompt, contactName, errors, {
    api: 'openrouter_research',
    tier: 'research',
    label: process.env.OR_RESEARCH_MODEL || 'research tier',
    maxTokens: 1536,
  });
}

/**
 * Research one investor using web-grounded AI search.
 * Tries configured grounded providers first, then best-effort research synthesis.
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

  if (canReuseStoredResearch) {
    console.log(`[PERSON RESEARCH] ${contact.name}: using cached/structured data`);
    return normalized;
  }

  const firm = contact.company_name || '';
  console.log(`[PERSON RESEARCH] ${contact.name}${firm ? ` at ${firm}` : ''}`);

  const agentCtx = await getResearchContext();
  const basePrompt = buildPrompt(contact, deal);
  const prompt = agentCtx ? `${agentCtx}${basePrompt}` : basePrompt;
  const errors = [];

  for (const attempt of buildResearchAttempts(config)) {
    const result = await tryOpenRouterResearch(prompt, contact.name, errors, attempt);
    if (result) return result;
  }

  {
    const result = await tryResearchSynthesis(prompt, contact.name, errors);
    if (result) return result;
  }

  if (hasCoreResearchFields(contact)) {
    console.warn(`[PERSON RESEARCH] Live research failed for ${contact.name} — reusing stored fields`);
    return normalized;
  }

  // All APIs failed — classify the error type so orchestrator can surface it
  const quotaStatuses = [429, 503];
  const isQuota = errors.some(e => {
    const msg = String(e.message || '').toLowerCase();
    return quotaStatuses.includes(e.status) || msg.includes('429') || msg.includes('quota') || msg.includes('rate limit');
  });

  const apisSummary = [...new Set(errors.map(e => e.api))].join(' + ');
  const detail = summarizeResearchErrors(errors);
  const err = new Error(`All research APIs failed for ${contact.name} (tried: ${apisSummary}; ${detail})`);
  err.isQuota = isQuota;
  err.isGrokFailed = false;
  err.allErrors = errors;
  throw err;
}

/** Returns true if a contact's notes contain the research marker */
export function isResearched(notes) {
  return typeof notes === 'string' && RESEARCH_MARKERS.some(marker => notes.includes(marker));
}
