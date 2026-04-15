/**
 * enrichment/linkedinFinder.js
 * Find a LinkedIn URL for a contact using strict search + verification.
 * Returns a canonical URL string or null.
 */

import {
  canonicalizeLinkedInProfileUrl,
  resolveLinkedInProfile,
  searchLinkedInPeople,
  searchLinkedInPeopleSalesNavigator,
} from '../integrations/unipileClient.js';

// Module-level rate limit cooldown — when LinkedIn returns 429, pause all searches for 45 minutes
const LINKEDIN_COOLDOWN_MS = 45 * 60 * 1000;
let linkedInRateLimitedUntil = null;

function markLinkedInRateLimited() {
  linkedInRateLimitedUntil = Date.now() + LINKEDIN_COOLDOWN_MS;
  console.warn(`[LINKEDIN FINDER] Rate limited by LinkedIn — pausing all searches for 45 minutes (until ${new Date(linkedInRateLimitedUntil).toISOTimeString?.() || new Date(linkedInRateLimitedUntil).toISOString()})`);
}

function isLinkedInRateLimited() {
  if (!linkedInRateLimitedUntil) return false;
  if (Date.now() >= linkedInRateLimitedUntil) {
    linkedInRateLimitedUntil = null;
    console.info('[LINKEDIN FINDER] Rate limit cooldown expired — resuming searches');
    return false;
  }
  return true;
}

function is429Error(err) {
  const msg = String(err?.message || err || '');
  return msg.includes('429') || msg.toLowerCase().includes('too_many_requests') || msg.toLowerCase().includes('too many requests');
}

function normalizeText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function tokenize(value) {
  return normalizeText(value).split(/\s+/).filter(Boolean);
}

function tokensOverlap(left, right) {
  if (!left.length || !right.length) return 0;
  const rightSet = new Set(right);
  return left.filter(token => rightSet.has(token)).length;
}

function buildCanonicalLinkedInUrl(person) {
  if (person?.public_profile_url) return canonicalizeLinkedInProfileUrl(person.public_profile_url);
  if (person?.public_identifier) return `https://www.linkedin.com/in/${person.public_identifier}`;
  if (person?.profile_url && /linkedin\.com\/in\//i.test(person.profile_url)) {
    return canonicalizeLinkedInProfileUrl(person.profile_url);
  }
  return null;
}

function getCandidateName(person) {
  return person?.name
    || [person?.first_name, person?.last_name].filter(Boolean).join(' ').trim()
    || null;
}

function getCandidateCompanyText(person) {
  const currentPositions = Array.isArray(person?.current_positions) ? person.current_positions : [];
  return [
    person?.current_company?.name,
    person?.company,
    person?.company_name,
    currentPositions.map(position => position?.company).filter(Boolean).join(' '),
    person?.headline,
  ].filter(Boolean).join(' ');
}

function scoreCandidate(person, expected) {
  const candidateName = getCandidateName(person);
  const candidateNameTokens = tokenize(candidateName);
  const expectedNameTokens = tokenize(expected.name);
  const expectedCompanyTokens = tokenize(expected.company);
  const expectedTitleTokens = tokenize(expected.title);
  const companyTextTokens = tokenize(getCandidateCompanyText(person));
  const headlineTokens = tokenize(person?.headline);

  if (!candidateNameTokens.length) return -100;

  const firstExpected = expectedNameTokens[0];
  const lastExpected = expectedNameTokens[expectedNameTokens.length - 1];
  const nameOverlap = tokensOverlap(expectedNameTokens, candidateNameTokens);
  const companyOverlap = tokensOverlap(expectedCompanyTokens, companyTextTokens);
  const titleOverlap = tokensOverlap(expectedTitleTokens, headlineTokens);

  let score = 0;
  if (normalizeText(candidateName) === normalizeText(expected.name)) score += 12;
  score += nameOverlap * 4;
  if (firstExpected && candidateNameTokens.includes(firstExpected)) score += 3;
  if (lastExpected && candidateNameTokens.includes(lastExpected)) score += 5;
  if (expectedCompanyTokens.length) score += companyOverlap * 3;
  if (expectedTitleTokens.length) score += titleOverlap * 1.5;
  if (person?.public_profile_url || person?.public_identifier) score += 1;

  if (lastExpected && !candidateNameTokens.includes(lastExpected)) score -= 8;
  if (firstExpected && !candidateNameTokens.includes(firstExpected)) score -= 4;
  if (expectedCompanyTokens.length >= 2 && companyOverlap === 0) score -= 5;

  return score;
}

function isStrongNameMatch(expectedName, actualName) {
  const expectedTokens = tokenize(expectedName);
  const actualTokens = tokenize(actualName);
  if (!expectedTokens.length || !actualTokens.length) return false;
  const firstExpected = expectedTokens[0];
  const lastExpected = expectedTokens[expectedTokens.length - 1];
  const overlap = tokensOverlap(expectedTokens, actualTokens);
  return overlap >= Math.min(2, expectedTokens.length, actualTokens.length)
    && (!firstExpected || actualTokens.includes(firstExpected))
    && (!lastExpected || actualTokens.includes(lastExpected));
}

function isLikelyCompanyMatch(expectedCompany, values) {
  const expectedTokens = tokenize(expectedCompany);
  if (!expectedTokens.length) return true;
  const actualTokens = tokenize(values.filter(Boolean).join(' '));
  if (!actualTokens.length) return true;
  return tokensOverlap(expectedTokens, actualTokens) >= Math.min(2, expectedTokens.length);
}

function validateLinkedInProfileUrl(url) {
  const canonical = canonicalizeLinkedInProfileUrl(url);
  if (!canonical || !/linkedin\.com\/in\//i.test(canonical)) return null;
  const slug = canonical.match(/linkedin\.com\/in\/([^/?#\s]+)/i)?.[1];
  if (!slug || slug.length < 3) return null;
  return canonical;
}

function hasEnoughIdentityContext(expected = {}) {
  const nameTokens = tokenize(expected.name);
  if (nameTokens.length < 2) return false;
  return tokenize(expected.company).length > 0 || tokenize(expected.title).length > 0;
}

function buildFallbackPrompt({ name, company, title }) {
  return `Find the exact LinkedIn profile URL for this person.

Person:
- Name: ${name}
- Company: ${company || 'Unknown'}
- Title: ${title || 'Unknown'}

Rules:
- Search the public web and identify the single best matching LinkedIn personal profile.
- Only return a linkedin.com/in/ profile URL.
- Do not return company pages, search pages, or guessed URLs.
- If you are not confident the profile belongs to the exact same person, return null.

Return ONLY valid JSON:
{
  "linkedin_url": "<exact LinkedIn profile URL or null>",
  "confidence": "high|medium|low",
  "reason": "<one short sentence>"
}`;
}

function parseFallbackCandidateUrl(text) {
  const cleaned = String(text || '').replace(/```json|```/g, '').trim();
  const objectMatch = cleaned.match(/\{[\s\S]*\}/);
  if (objectMatch) {
    try {
      const parsed = JSON.parse(objectMatch[0]);
      return validateLinkedInProfileUrl(parsed?.linkedin_url || null);
    } catch {}
  }

  const directUrl = cleaned.match(/https?:\/\/(?:[a-z]{2,3}\.)?linkedin\.com\/in\/[^?\s"']+/i)?.[0];
  return validateLinkedInProfileUrl(directUrl);
}

async function tryGeminiFallback(prompt) {
  const models = ['gemini-2.5-flash', 'gemini-2.5-pro'];
  const keys = [process.env.GEMINI_API_KEY, process.env.GEMINI_API_KEY_FALLBACK].filter(Boolean);
  for (const key of keys) {
    for (const model of models) {
      try {
        const res = await fetch(
          `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              contents: [{ parts: [{ text: prompt }] }],
              tools: [{ google_search: {} }],
              generationConfig: { temperature: 0.1, maxOutputTokens: 300 },
            }),
          },
        );
        if (!res.ok) continue;
        const data = await res.json();
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '';
        const url = parseFallbackCandidateUrl(text);
        if (url) return url;
      } catch {}
    }
  }
  return null;
}

async function tryGrokFallback(prompt) {
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
        model: process.env.RESEARCH_GROK_MODEL || 'grok-3-fast',
        input: [{ role: 'user', content: prompt }],
        tools: [{ type: 'web_search' }],
      }),
    });
    if (!res.ok) return null;
    const data = await res.json();
    const outputMsg = (data.output || []).find(item => item.type === 'message');
    const text = outputMsg?.content?.find(item => item.type === 'output_text')?.text || '';
    return parseFallbackCandidateUrl(text);
  } catch {
    return null;
  }
}

async function searchCandidates({ name, company, title }) {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  const firstName = parts[0] || '';
  const lastName = parts.slice(1).join(' ') || parts[0] || '';
  const keywordSets = [
    [name, company, title].filter(Boolean).join(' '),
    [name, company].filter(Boolean).join(' '),
    [name, title].filter(Boolean).join(' '),
    name,
  ].filter(Boolean);

  const seen = new Set();
  const candidates = [];
  const pushCandidate = (person) => {
    const key = String(
      person?.id
      || person?.public_identifier
      || person?.public_profile_url
      || person?.profile_url
      || '',
    ).trim();
    if (!key || seen.has(key)) return;
    seen.add(key);
    candidates.push(person);
  };

  try {
    const salesResults = await searchLinkedInPeopleSalesNavigator({
      firstName,
      lastName,
      keywords: [company, title].filter(Boolean).join(' '),
      limit: 10,
    });
    salesResults.forEach(pushCandidate);
  } catch (err) {
    if (is429Error(err)) { markLinkedInRateLimited(); return candidates; }
    const message = String(err?.message || '');
    if (!/403|501|feature_not_subscribed|subscription_required|not implemented/i.test(message)) {
      console.warn(`[LINKEDIN FINDER] Sales Navigator search failed for "${name}": ${message}`);
    }
  }

  // Bail out immediately if rate limited during Sales Nav attempt
  if (isLinkedInRateLimited()) return candidates;

  for (const keywords of keywordSets) {
    try {
      const results = await searchLinkedInPeople({ keywords, limit: 10 });
      results.forEach(pushCandidate);
    } catch (err) {
      if (is429Error(err)) { markLinkedInRateLimited(); break; }
      console.warn(`[LINKEDIN FINDER] Classic search failed for "${name}" with "${keywords}": ${err.message}`);
    }
    if (candidates.length >= 15) break;
    if (isLinkedInRateLimited()) break;
  }

  return candidates;
}

async function verifyCandidate(person, expected) {
  const candidateName = getCandidateName(person);
  if (!candidateName || !isStrongNameMatch(expected.name, candidateName)) return null;

  const candidateUrl = buildCanonicalLinkedInUrl(person);
  const identifier = candidateUrl || person?.public_identifier || person?.id;
  if (!identifier) return null;

  try {
    const resolved = await resolveLinkedInProfile(identifier);
    if (!resolved?.name || !isStrongNameMatch(expected.name, resolved.name)) return null;

    const resolvedCompanyValues = [
      resolved?.headline,
      resolved?.raw?.company,
      resolved?.raw?.company_name,
      resolved?.raw?.current_company?.name,
      ...(Array.isArray(resolved?.raw?.current_positions)
        ? resolved.raw.current_positions.map(position => position?.company)
        : []),
      ...(Array.isArray(resolved?.raw?.positions)
        ? resolved.raw.positions.map(position => position?.company_name || position?.company)
        : []),
    ];

    if (!isLikelyCompanyMatch(expected.company, resolvedCompanyValues)) return null;

    return canonicalizeLinkedInProfileUrl(resolved?.linkedinUrl || candidateUrl);
  } catch (err) {
    console.warn(`[LINKEDIN FINDER] Candidate verification failed for "${expected.name}": ${err.message}`);
    return null;
  }
}

async function verifyFallbackUrl(url, expected) {
  const candidateUrl = validateLinkedInProfileUrl(url);
  if (!candidateUrl) return null;

  try {
    const resolved = await resolveLinkedInProfile(candidateUrl);
    if (!resolved?.name || !isStrongNameMatch(expected.name, resolved.name)) return null;

    const resolvedCompanyValues = [
      resolved?.headline,
      resolved?.raw?.company,
      resolved?.raw?.company_name,
      resolved?.raw?.current_company?.name,
      ...(Array.isArray(resolved?.raw?.current_positions)
        ? resolved.raw.current_positions.map(position => position?.company)
        : []),
      ...(Array.isArray(resolved?.raw?.positions)
        ? resolved.raw.positions.map(position => position?.company_name || position?.company)
        : []),
    ];

    if (!isLikelyCompanyMatch(expected.company, resolvedCompanyValues)) return null;
    return canonicalizeLinkedInProfileUrl(resolved?.linkedinUrl || candidateUrl);
  } catch (err) {
    console.warn(`[LINKEDIN FINDER] Fallback verification failed for "${expected.name}": ${err.message}`);
    return null;
  }
}

export async function findLinkedInUrl({ name, company, title }) {
  if (!name || !String(name).trim()) return null;

  // Respect rate limit cooldown — don't hammer LinkedIn when we're already being throttled
  if (isLinkedInRateLimited()) return null;

  const expected = { name, company, title };
  if (!hasEnoughIdentityContext(expected)) return null;

  const candidates = await searchCandidates(expected);
  const ranked = candidates
    .map(person => ({ person, score: scoreCandidate(person, expected) }))
    .filter(entry => entry.score >= 12)
    .sort((left, right) => right.score - left.score);

  if (ranked.length >= 2 && (ranked[0].score - ranked[1].score) < 3) {
    return null;
  }

  for (const { person } of ranked.slice(0, 5)) {
    const verifiedUrl = await verifyCandidate(person, expected);
    if (verifiedUrl) return verifiedUrl;
  }

  const fallbackPrompt = buildFallbackPrompt(expected);
  const fallbackUrl = await tryGeminiFallback(fallbackPrompt) || await tryGrokFallback(fallbackPrompt);
  if (!fallbackUrl) return null;

  return verifyFallbackUrl(fallbackUrl, expected);
}

export function getLinkedInFallbackPrompt({ name, company, title }) {
  return buildFallbackPrompt({ name, company, title });
}

export async function findLinkedInUrlWithDiagnostics({ name, company, title }) {
  const prompt = buildFallbackPrompt({ name, company, title });
  const url = await findLinkedInUrl({ name, company, title });
  return { url, prompt };
}
