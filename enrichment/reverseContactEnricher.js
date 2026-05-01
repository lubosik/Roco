/**
 * enrichment/reverseContactEnricher.js
 * ReverseContact API integration — reverse email lookup and LinkedIn profile extraction.
 *
 * API base: https://api.reversecontact.com/enrichment
 * Auth: query param apikey=<REVERSECONTACT_API_KEY>
 * Rate limit: 15 req/s — we cap at 1 req/s to be conservative.
 *
 * Exports:
 *   enrichByEmail(email)      → { linkedInUrl, firstName, lastName, headline, company } | null
 *   enrichByLinkedIn(linkedInUrl) → same shape | null
 */

import fetch from 'node-fetch';

const BASE_URL = 'https://api.reversecontact.com/enrichment';
const MIN_INTERVAL_MS = 1000; // 1 req/s conservative cap

let _lastCallAt = 0;

async function _rateLimit() {
  const now = Date.now();
  const elapsed = now - _lastCallAt;
  if (elapsed < MIN_INTERVAL_MS) {
    await new Promise(r => setTimeout(r, MIN_INTERVAL_MS - elapsed));
  }
  _lastCallAt = Date.now();
}

async function _fetchWithRetry(url, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const res = await fetch(url, { method: 'GET', headers: { Accept: 'application/json' } });
    if (res.status !== 503 && res.status !== 504) return res;
    if (attempt < retries) await new Promise(r => setTimeout(r, 2000 * (attempt + 1)));
  }
  return await fetch(url, { method: 'GET', headers: { Accept: 'application/json' } });
}

/**
 * Normalise raw API response into a consistent shape.
 * Returns null if the response does not contain useful data.
 */
function _normalise(raw) {
  if (!raw || raw.success === false) return null;

  const person = raw.person || {};
  const company = raw.company || {};

  const linkedInUrl = person.linkedInUrl || null;
  const firstName = person.firstName || null;
  const lastName = person.lastName || null;
  const headline = person.headline || null;
  const companyName = company.name || null;

  // Require at least a LinkedIn URL or a name to be useful
  if (!linkedInUrl && !firstName && !lastName) return null;

  return { linkedInUrl, firstName, lastName, headline, company: companyName };
}

/**
 * Lookup a person by their email address.
 * Returns enrichment data or null.
 */
export async function enrichByEmail(email) {
  const apiKey = process.env.REVERSECONTACT_API_KEY;
  if (!apiKey) {
    console.warn('[ReverseContact] REVERSECONTACT_API_KEY not set — skipping enrichByEmail');
    return null;
  }
  if (!email || typeof email !== 'string') return null;

  await _rateLimit();

  try {
    const url = `${BASE_URL}?apikey=${encodeURIComponent(apiKey)}&mail=${encodeURIComponent(email.trim())}`;
    const res = await _fetchWithRetry(url);

    if (!res.ok) {
      console.warn(`[ReverseContact] enrichByEmail HTTP ${res.status} for ${email}`);
      return null;
    }

    const raw = await res.json();
    return _normalise(raw);
  } catch (err) {
    console.warn(`[ReverseContact] enrichByEmail error for ${email}:`, err.message);
    return null;
  }
}

/**
 * Lookup a person by their LinkedIn profile URL.
 * Returns enrichment data or null.
 */
export async function enrichByLinkedIn(linkedInUrl) {
  const apiKey = process.env.REVERSECONTACT_API_KEY;
  if (!apiKey) {
    console.warn('[ReverseContact] REVERSECONTACT_API_KEY not set — skipping enrichByLinkedIn');
    return null;
  }
  if (!linkedInUrl || typeof linkedInUrl !== 'string') return null;

  await _rateLimit();

  try {
    // LinkedIn-by-URL requires paid PAYG plan — trial key returns 403
    const url = `${BASE_URL}/profile?apikey=${encodeURIComponent(apiKey)}&url=${encodeURIComponent(linkedInUrl.trim())}`;
    const res = await _fetchWithRetry(url);

    if (res.status === 403) {
      console.info('[ReverseContact] enrichByLinkedIn: trial plan does not support LinkedIn URL lookup — upgrade to PAYG');
      return null;
    }
    if (!res.ok) {
      console.warn(`[ReverseContact] enrichByLinkedIn HTTP ${res.status} for ${linkedInUrl}`);
      return null;
    }

    const raw = await res.json();
    return _normalise(raw);
  } catch (err) {
    console.warn(`[ReverseContact] enrichByLinkedIn error for ${linkedInUrl}:`, err.message);
    return null;
  }
}
