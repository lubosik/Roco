/**
 * enrichment/reverseContactEnricher.js
 * ReverseContact integration for profile enrichment and contact discovery.
 *
 * V2 uses Authorization: Bearer rc_* and JSON POST endpoints. The legacy
 * enrichment endpoints are kept as a fallback because older accounts/docs still
 * expose them.
 */

import fetch from 'node-fetch';

const V2_BASE_URL = 'https://api.reversecontact.com/v2';
const LEGACY_BASE_URL = 'https://api.reversecontact.com/enrichment';
const MIN_INTERVAL_MS = Math.max(1000, Number(process.env.REVERSECONTACT_MIN_INTERVAL_MS || 6500));
const ASYNC_POLL_MS = 10_000;
const ASYNC_MAX_POLLS = 6;

let lastCallAt = 0;
let legacyReverseEmailRestricted = false;
let v2EmailFinderRestricted = false;
let legacyEmailFinderRestricted = false;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function getApiKey() {
  return process.env.REVERSECONTACT_API_KEY || process.env.REVERSE_CONTACT_API_KEY || null;
}

async function rateLimit() {
  const elapsed = Date.now() - lastCallAt;
  if (elapsed < MIN_INTERVAL_MS) await sleep(MIN_INTERVAL_MS - elapsed);
  lastCallAt = Date.now();
}

function normalizeLinkedInUrl(value) {
  const match = String(value || '').trim().match(/^https?:\/\/([a-z]{2,3}\.)?linkedin\.com\/in\/([^/?#\s]+)/i);
  return match ? `https://${match[1] || 'www.'}linkedin.com/in/${match[2].replace(/\/+$/, '')}` : null;
}

function pick(...values) {
  return values.find(value => value !== undefined && value !== null && String(value).trim() !== '') || null;
}

function unwrapPayload(raw) {
  if (!raw || raw.success === false) return null;
  if (raw.data?.data && Array.isArray(raw.data.data)) return raw.data.data[0] || null;
  if (Array.isArray(raw.data)) return raw.data[0] || null;
  if (raw.data && typeof raw.data === 'object' && !Array.isArray(raw.data)) return raw.data;
  return raw;
}

function normalizePerson(raw) {
  const payload = unwrapPayload(raw);
  if (!payload) return null;

  const person = payload.person || payload.profile || payload;
  const company = payload.company || person.company || person.currentCompany || {};
  const linkedInUrl = normalizeLinkedInUrl(pick(
    person.linkedinUrl,
    person.linkedInUrl,
    person.linkedin_url,
    person.public_profile_url,
    person.profile_url,
    person.publicId ? `https://www.linkedin.com/in/${person.publicId}` : null,
    person.publicIdentifier ? `https://www.linkedin.com/in/${person.publicIdentifier}` : null,
  ));

  const firstName = pick(person.firstName, person.first_name);
  const lastName = pick(person.lastName, person.last_name);
  const fullName = pick(
    person.fullName,
    person.name,
    payload.results?.full_name,
    [firstName, lastName].filter(Boolean).join(' ').trim(),
  );
  const email = pick(
    payload.email,
    person.email,
    person.emailAddress,
    payload.results?.email,
    Array.isArray(payload.data) ? payload.data[0]?.email : null,
    Array.isArray(payload.emails) ? payload.emails[0]?.email || payload.emails[0]?.value : null,
  );
  const headline = pick(person.headline, person.currentPositionTitle, person.job_title, person.title);
  const companyName = pick(
    company.name,
    company.companyName,
    person.currentCompanyName,
    person.companyName,
  );

  if (!linkedInUrl && !email && !fullName) return null;

  return {
    linkedInUrl,
    email: email || null,
    firstName: firstName || null,
    lastName: lastName || null,
    fullName: fullName || null,
    headline: headline || null,
    company: companyName || null,
    raw,
  };
}

async function requestV2(path, body = null, { method = 'POST', allow404 = true } = {}) {
  const apiKey = getApiKey();
  if (!apiKey) return null;
  await rateLimit();

  const res = await fetch(`${V2_BASE_URL}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      Accept: 'application/json',
      ...(body ? { 'Content-Type': 'application/json' } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (allow404 && res.status === 404) return null;
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    const err = new Error(`ReverseContact ${method} ${path} -> ${res.status}: ${text.slice(0, 200)}`);
    err.status = res.status;
    throw err;
  }
  return res.json();
}

async function requestLegacy(params, endpoint = '') {
  const apiKey = getApiKey();
  if (!apiKey) return null;
  await rateLimit();

  const query = new URLSearchParams({ apikey: apiKey, ...params });
  const res = await fetch(`${LEGACY_BASE_URL}${endpoint}?${query.toString()}`, {
    headers: { Accept: 'application/json' },
  });
  if (res.status === 404) return null;
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    const err = new Error(`ReverseContact legacy${endpoint} -> ${res.status}: ${text.slice(0, 200)}`);
    err.status = res.status;
    throw err;
  }
  return res.json();
}

async function pollWebhook(webhookId) {
  if (!webhookId) return null;
  for (let i = 0; i < ASYNC_MAX_POLLS; i++) {
    await sleep(i === 0 ? ASYNC_POLL_MS : Math.min(ASYNC_POLL_MS * 1.5, 20_000));
    const raw = await requestV2(`/webhooks/${encodeURIComponent(webhookId)}`, null, { method: 'GET' }).catch(() => null);
    const status = String(raw?.status || raw?.data?.status || '').toLowerCase();
    if (['succeeded', 'success', 'completed', 'done'].includes(status)) return raw;
    if (['errored', 'error', 'failed'].includes(status)) return raw;
  }
  return null;
}

export async function enrichByEmail(email) {
  const value = String(email || '').trim();
  if (!getApiKey() || !value) return null;

  try {
    const resolved = await requestV2('/resolve/persons/email', { email: value }).catch(err => {
      if ([400, 403, 404, 422].includes(err.status)) return null;
      throw err;
    });
    const webhookId = resolved?.data?.webhookId || resolved?.webhookId || null;
    const normalized = normalizePerson(webhookId ? await pollWebhook(webhookId) : resolved);
    if (normalized) return normalized;
  } catch (err) {
    console.warn(`[ReverseContact] V2 email lookup failed for ${value}: ${err.message}`);
  }

  try {
    if (legacyReverseEmailRestricted) return null;
    const legacy = await requestLegacy({ email: value }).catch(err => {
      if (err.status === 403) legacyReverseEmailRestricted = true;
      return null;
    }) || await requestLegacy({ mail: value }).catch(err => {
      if (err.status === 403) legacyReverseEmailRestricted = true;
      throw err;
    });
    return normalizePerson(legacy);
  } catch (err) {
    console.warn(`[ReverseContact] legacy email lookup failed for ${value}: ${err.message}`);
    return null;
  }
}

export async function enrichByLinkedIn(linkedInUrl) {
  const url = normalizeLinkedInUrl(linkedInUrl);
  if (!getApiKey() || !url) return null;

  try {
    return normalizePerson(await requestV2('/fetch/persons', { url }));
  } catch (err) {
    console.warn(`[ReverseContact] V2 LinkedIn lookup failed for ${url}: ${err.message}`);
  }

  try {
    return normalizePerson(await requestLegacy({ url }, '/profile'));
  } catch (err) {
    console.warn(`[ReverseContact] legacy LinkedIn lookup failed for ${url}: ${err.message}`);
    return null;
  }
}

export async function searchPerson({ name, companyName, title, perPage = 5 } = {}) {
  if (!getApiKey()) return null;
  const [firstName, ...rest] = String(name || '').trim().split(/\s+/).filter(Boolean);
  const lastName = rest.length ? rest[rest.length - 1] : null;
  if (!firstName && !companyName && !title) return null;

  const body = {
    firstName: firstName || undefined,
    lastName: lastName || undefined,
    currentCompanyName: companyName || undefined,
    currentPositionTitle: title || undefined,
    perPage: Math.min(Math.max(Number(perPage) || 5, 1), 10),
    page: 1,
  };
  const raw = await requestV2('/search/persons', body).catch(err => {
    console.warn(`[ReverseContact] person search failed for ${name || companyName}: ${err.message}`);
    return null;
  });
  const normalized = normalizePerson(raw);
  if (normalized) return normalized;

  return normalizePerson(await requestLegacy({
    ...(firstName ? { firstName } : {}),
    ...(lastName ? { lastName } : {}),
    ...(companyName ? { companyName } : {}),
  }).catch(err => {
    console.warn(`[ReverseContact] legacy person search failed for ${name || companyName}: ${err.message}`);
    return null;
  }));
}

export async function findEmail({ linkedInUrl, fullName, firstName, lastName, companyDomain, companyName } = {}) {
  if (!getApiKey()) return null;
  const body = {};
  const url = normalizeLinkedInUrl(linkedInUrl);
  if (url) body.url = url;
  else {
    if (fullName) body.fullName = fullName;
    if (firstName) body.firstName = firstName;
    if (lastName) body.lastName = lastName;
    if (companyDomain) body.companyDomain = companyDomain;
  }
  if (!body.url && !(body.companyDomain && (body.fullName || (body.firstName && body.lastName)))) return null;

  const created = v2EmailFinderRestricted ? null : await requestV2('/contact/email', body).catch(err => {
    if (err.status === 403) v2EmailFinderRestricted = true;
    console.warn(`[ReverseContact] find email failed for ${fullName || linkedInUrl}: ${err.message}`);
    return null;
  });
  const webhookId = created?.data?.webhookId || created?.webhookId || null;
  const result = webhookId ? await pollWebhook(webhookId) : created;
  const normalized = normalizePerson(result);
  if (normalized?.email) return normalized;

  const legacyParams = {};
  if (fullName) legacyParams.full_name = fullName;
  if (firstName) legacyParams.first_name = firstName;
  if (lastName) legacyParams.last_name = lastName;
  if (companyDomain) legacyParams.domain = companyDomain;
  if (companyName) legacyParams.company_name = companyName;
  if (!(legacyParams.full_name || (legacyParams.first_name && legacyParams.last_name)) || !(legacyParams.domain || legacyParams.company_name)) {
    return normalized;
  }
  if (legacyEmailFinderRestricted) return normalized;
  return normalizePerson(await requestLegacy(legacyParams, '/email-finder').catch(err => {
    if (err.status === 403) legacyEmailFinderRestricted = true;
    console.warn(`[ReverseContact] legacy email finder failed for ${fullName || linkedInUrl}: ${err.message}`);
    return null;
  }));
}
