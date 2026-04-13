// core/unipile.js
// Single wrapper for all Unipile API calls.
// Live credentials: Supabase first, env fallback, cached 30s.

import { getSupabase } from './supabase.js';
import {
  listSentInvitations,
  resolveLinkedInProfile,
  sendLinkedInInvite,
  searchLinkedInPeople,
  searchLinkedInPeopleSalesNavigator,
} from '../integrations/unipileClient.js';

let _cachedCreds = null;
let _credsCachedAt = 0;
const CRED_TTL = 30_000;
const LINKEDIN_PROVIDER_LIMIT_RETRY_HOURS = [4, 8, 24];
const PROVIDER_LIMIT_NOTE_PATTERN = /\[LI_INVITE_LIMIT:count=(\d+)\|blocked_until=([^|\]]+)(?:\|notified_at=([^\]]+))?\]/i;

export async function getLiveCredentials() {
  if (_cachedCreds && Date.now() - _credsCachedAt < CRED_TTL) return _cachedCreds;

  let creds = {
    dsn:               process.env.UNIPILE_DSN || 'https://api34.unipile.com:16411',
    apiKey:            process.env.UNIPILE_API_KEY,
    linkedinAccountId: process.env.UNIPILE_LINKEDIN_ACCOUNT_ID,
    gmailAccountId:    process.env.UNIPILE_GMAIL_ACCOUNT_ID,
    outlookAccountId:  process.env.UNIPILE_OUTLOOK_ACCOUNT_ID,
  };

  try {
    const sb = getSupabase();
    if (sb) {
      const { data } = await sb.from('deal_settings').select('key, value')
        .in('key', ['UNIPILE_DSN', 'UNIPILE_API_KEY', 'UNIPILE_LINKEDIN_ACCOUNT_ID']);
      (data || []).forEach(r => {
        if (r.key === 'UNIPILE_DSN')                 creds.dsn               = r.value;
        if (r.key === 'UNIPILE_API_KEY')             creds.apiKey            = r.value;
        if (r.key === 'UNIPILE_LINKEDIN_ACCOUNT_ID') creds.linkedinAccountId = r.value;
      });
    }
  } catch (err) {
    console.warn('[UNIPILE] Live credential load failed, using env:', err.message);
  }

  _cachedCreds     = creds;
  _credsCachedAt   = Date.now();
  return creds;
}

function getBaseUrl(dsn) {
  const clean = (dsn || '').replace(/^https?:\/\//, '').replace(/\/+$/, '');
  return `https://${clean}/api/v1`;
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// Strip parenthetical location qualifiers and legal suffixes before LinkedIn lookups.
// "Beverly Capital (Illinois)" → "Beverly Capital"
// "Acme Partners LLC"          → "Acme Partners"
function cleanFirmNameForLinkedIn(name) {
  if (!name) return name;
  let s = String(name)
    .replace(/\s*\([^)]*\)\s*$/g, '')   // trailing (Illinois), (Chicago, IL), etc.
    .replace(/\s+(?:LLC|LP|LLP|Inc\.?|Ltd\.?|Co\.?|Corp\.?|PLLC|PC|Fund|Ventures?)\.?\s*$/i, '')
    .trim();
  return s || String(name).trim();
}

async function request(method, path, { body, query, allowErrorResponse = false } = {}) {
  const creds = await getLiveCredentials();
  let url = `${getBaseUrl(creds.dsn)}${path}`;
  if (query) url += '?' + new URLSearchParams(query).toString();

  const headers = { 'X-API-KEY': creds.apiKey, accept: 'application/json' };
  let bodyContent;
  if (body) {
    headers['Content-Type'] = 'application/json';
    bodyContent = JSON.stringify(body);
  }

  const res = await fetch(url, { method, headers, body: bodyContent });

  if (!res.ok && !allowErrorResponse) {
    const text = await res.text().catch(() => '');
    console.error(`[UNIPILE] ${method} ${path} → ${res.status}: ${text.slice(0, 200)}`);
    throw new Error(`Unipile ${res.status}: ${text.slice(0, 100)}`);
  }

  return res.json().catch(() => null);
}

export async function getLinkedInCompanyProfile(firmName, pushActivity = null, dealId = null) {
  if (!firmName) return null;

  const creds = await getLiveCredentials();
  const accountId = creds.linkedinAccountId;
  const cleaned   = cleanFirmNameForLinkedIn(firmName);

  // Build candidate lookup strings: cleaned slug, cleaned raw, 2-word shortened, original raw
  const candidates = [...new Set([
    cleaned.toLowerCase().replace(/\s+/g, '-'),
    cleaned,
    cleaned.split(/\s+/).slice(0, 2).join('-').toLowerCase(),
  ])].filter(Boolean);

  let profile = null;
  for (const candidate of candidates) {
    const slug = encodeURIComponent(candidate);
    const res = await request('GET', `/linkedin/company/${slug}`, {
      query: { account_id: accountId },
      allowErrorResponse: true,
    }).catch(() => null);
    if (res && !res.error) { profile = res; break; }
  }

  if (!profile || profile.error) {
    pushActivity?.({
      type: 'research',
      action: `LinkedIn profile not found: ${firmName}`,
      note: 'Using existing database data only',
      deal_id: dealId,
    });
    return null;
  }

  const industry = Array.isArray(profile.industry) ? profile.industry.join(', ') : (profile.industry || 'industry unknown');
  const location = profile.locations?.[0]?.country || profile.locations?.[0]?.name || 'location unknown';
  pushActivity?.({
    type: 'research',
    action: `LinkedIn profile found: ${profile.name || firmName}`,
    note: `${profile.employee_count || '?'} employees · ${industry} · ${location}`,
    deal_id: dealId,
  });
  return profile;
}

async function resolveLinkedInCompanyId(firmName) {
  const creds   = await getLiveCredentials();
  const cleaned = cleanFirmNameForLinkedIn(firmName);

  async function trySearch(keywords) {
    return request('GET', '/linkedin/search/parameters', {
      query: { type: 'COMPANY', keywords, account_id: creds.linkedinAccountId, limit: 5 },
      allowErrorResponse: true,
    }).catch(() => null);
  }

  // Try cleaned full name first, then first two words as a fallback
  const shortName  = cleaned.split(/\s+/).slice(0, 2).join(' ');
  const attempts   = cleaned === shortName ? [cleaned] : [cleaned, shortName];

  for (const attempt of attempts) {
    const result = await trySearch(attempt);
    const items  = result?.items || [];
    const firstToken = cleaned.toLowerCase().split(/\s+/)[0];
    const match = items.find(item =>
      String(item.title || item.name || '').toLowerCase().includes(firstToken)
    );
    const id = match?.id || items[0]?.id || null;
    if (id) return id;
  }
  return null;
}

export async function searchDecisionMakersByCompany(firmName, pushActivity = null, dealId = null) {
  if (!firmName) return [];

  const creds    = await getLiveCredentials();
  const cleaned  = cleanFirmNameForLinkedIn(firmName);
  const companyId = await resolveLinkedInCompanyId(firmName);
  const body = {
    api: 'classic',
    category: 'people',
    keywords: 'Managing Partner OR General Partner OR Partner OR Principal OR "Managing Director" OR Director OR Vice President',
  };
  if (companyId) body.company = [companyId];
  if (!companyId) body.keywords += ` ${cleaned}`;

  const result = await request('POST', '/linkedin/search', {
    query: { account_id: creds.linkedinAccountId, limit: 10 },
    body,
    allowErrorResponse: true,
  }).catch(() => null);

  const people = (result?.items || [])
    .filter(person => person?.name || person?.headline)
    .map(person => ({
      name: person.name || '',
      headline: person.headline || '',
      linkedin_url: person.profile_url || '',
      linkedin_urn: person.entity_urn || '',
      network_distance: person.network_distance || 3,
      source: 'linkedin_search',
    }));

  if (people.length > 0) {
    pushActivity?.({
      type: 'research',
      action: `LinkedIn people search: ${people.length} contacts found at ${firmName}`,
      note: `Company ID: ${companyId || 'not resolved'} · ${people.slice(0, 3).map(person => `${person.name || 'Unknown'} (${String(person.headline || 'no title').slice(0, 40)})`).join(' · ')}`,
      deal_id: dealId,
    });
  } else {
    pushActivity?.({
      type: 'research',
      action: `LinkedIn people search: no decision makers found for ${firmName}`,
      note: `Company ID: ${companyId || 'not resolved'} · company matched, but Unipile returned no people results · using fallback research`,
      deal_id: dealId,
    });
  }

  await sleep(1500);
  return people;
}

export async function enrichFirmViaLinkedIn(firmName, deal, pushActivity = null) {
  pushActivity?.({
    type: 'research',
    action: `Unipile enriching: ${firmName}`,
    note: `${deal.name} · Pulling LinkedIn company profile + decision makers`,
    deal_id: deal.id,
  });

  const [linkedin_profile, contacts] = await Promise.all([
    getLinkedInCompanyProfile(firmName, pushActivity, deal.id),
    searchDecisionMakersByCompany(firmName, pushActivity, deal.id),
  ]);

  await sleep(1000);
  return { linkedin_profile, contacts, enrichment_source: 'unipile' };
}

// Boot-time: delete existing LinkedIn webhooks and recreate with consolidated endpoint.
// Gmail/Outlook webhooks are managed separately by unipileSetup.js — not touched here.
export async function recreateLinkedInWebhooks(serverBaseUrl) {
  const base = (serverBaseUrl || process.env.SERVER_BASE_URL || '').replace(/\/+$/, '');
  if (!base) {
    console.warn('[UNIPILE] No base URL provided — skipping webhook recreation');
    return;
  }

  console.log('[UNIPILE] Recreating LinkedIn webhooks →', base);

  const creds = await getLiveCredentials();
  if (!creds.apiKey) {
    console.warn('[UNIPILE] No API key — skipping webhook recreation');
    return;
  }

  // Fetch existing webhooks and delete LinkedIn-specific ones
  const existing = await request('GET', '/webhooks').catch(() => ({ items: [] }));
  for (const wh of existing?.items || []) {
    const isLinkedIn = wh.account_id === creds.linkedinAccountId ||
      (wh.request_url || '').includes('linkedin') ||
      (wh.request_url || '').includes('unipile/messages');
    if (isLinkedIn) {
      await request('DELETE', `/webhooks/${wh.id}`).catch(() => {});
    }
  }

  const targetUrl = `${base}/webhooks/unipile/messages`;

  // LinkedIn message events
  await request('POST', '/webhooks', { body: {
    name:        'roco_linkedin_messages',
    source:      'messaging',
    events:      ['message_received'],
    request_url: targetUrl,
    format:      'json',
    enabled:     true,
    account_ids: [creds.linkedinAccountId].filter(Boolean),
    headers:     [{ key: 'Content-Type', value: 'application/json' }],
  }});

  // LinkedIn connection accepted events
  await request('POST', '/webhooks', { body: {
    name:        'roco_linkedin_relations',
    source:      'users',
    events:      ['new_relation'],
    request_url: targetUrl,
    format:      'json',
    enabled:     true,
    account_ids: [creds.linkedinAccountId].filter(Boolean),
    headers:     [{ key: 'Content-Type', value: 'application/json' }],
  }});

  console.log('[UNIPILE] LinkedIn webhooks recreated →', targetUrl);
}

// Start new DM thread (contact not yet connected, or no prior chat)
export async function startLinkedInDM(providerUserId, text) {
  const creds  = await getLiveCredentials();
  const result = await request('POST', '/chats', { body: {
    account_id:     creds.linkedinAccountId,
    attendees_ids:  [providerUserId],
    text,
  }});
  return { chat_id: result?.id || result?.chat_id, message_id: result?.message_id };
}

// Reply into existing chat thread
export async function sendLinkedInDM(chatId, text) {
  const creds  = await getLiveCredentials();
  const result = await request('POST', `/chats/${chatId}/messages`, { body: {
    text,
    account_id: creds.linkedinAccountId,
  }});
  return { chat_id: chatId, message_id: result?.id || result?.message_id };
}

// Fetch recent messages from a chat (for inbox polling)
export async function getChatMessages(chatId, limit = 20) {
  const result = await request('GET', `/chats/${chatId}/messages`, { query: { limit } });
  return result?.items || result?.messages || [];
}

// Send LinkedIn connection request
export async function sendConnectionRequest(providerUserId, message = '') {
  return request('POST', '/linkedin/invitations', {
    body: { provider_id: providerUserId, message: (message || '').slice(0, 300) },
    allowErrorResponse: true,
  });
}

// Check current relationship status with a LinkedIn user
// Returns: 'connected' | 'pending' | 'none' | 'blocked' | 'unknown'
export async function getLinkedInRelationship(providerUserId) {
  try {
    const result = await request('GET', `/linkedin/relations/${providerUserId}`, { allowErrorResponse: true })
      .catch(() => null);
    return result?.status || result?.relation_status || 'none';
  } catch (err) {
    console.warn('[UNIPILE] Could not check relationship for', providerUserId, err.message);
    return 'unknown';
  }
}

// Find an existing chat with a contact by their provider user ID
// Returns the chat object or null
export async function getExistingChatWithContact(providerUserId, accountId) {
  try {
    const result = await request('GET', '/chats', {
      query: { account_id: accountId, limit: 50 },
    }).catch(() => null);
    const chats = result?.items || result?.chats || [];
    return chats.find(chat =>
      (chat.attendees || []).some(a =>
        a.provider_id === providerUserId || a.attendee_provider_id === providerUserId
      )
    ) || null;
  } catch (err) {
    console.warn('[UNIPILE] Chat lookup failed:', err.message);
    return null;
  }
}

// List connected email accounts (Gmail, Outlook, etc.) via Unipile
export async function getConnectedEmailAccounts() {
  const creds = await getLiveCredentials();
  // Build fallback list from known env account IDs
  const fallbacks = [
    creds.gmailAccountId && {
      connection_id: creds.gmailAccountId,
      email: process.env.GMAIL_EMAIL || 'Gmail',
      provider: 'GOOGLE',
      label: `Gmail (${process.env.GMAIL_EMAIL || 'connected'})`,
    },
    creds.outlookAccountId && {
      connection_id: creds.outlookAccountId,
      email: process.env.OUTLOOK_EMAIL || 'Outlook',
      provider: 'MICROSOFT',
      label: `Outlook (${process.env.OUTLOOK_EMAIL || 'connected'})`,
    },
  ].filter(Boolean);

  try {
    const result = await request('GET', '/accounts').catch(() => null);
    const accounts = result?.items || result?.accounts || [];
    const emailAccounts = accounts
      .filter(a => {
        const type = (a.type || a.account_type || '').toUpperCase();
        return type.includes('GOOGLE') || type.includes('MICROSOFT') ||
               type.includes('GMAIL') || type.includes('OUTLOOK') ||
               type.includes('EMAIL');
      })
      .map(a => ({
        connection_id: a.id,
        email: a.email || a.username || a.name || 'Unknown',
        provider: a.type || a.account_type || 'Email',
        label: `${a.email || a.username || a.name} (${a.type || 'Email'})`,
      }));
    return emailAccounts.length ? emailAccounts : fallbacks;
  } catch {
    return fallbacks;
  }
}

// Check if current time is within the configured sending window
export async function isWithinSendingWindow() {
  const sb = getSupabase();
  let w = { start_hour: 8, end_hour: 18, days: [1, 2, 3, 4, 5] };

  if (sb) {
    try {
      const { data } = await sb.from('outreach_sequence')
        .select('sending_window').limit(1).single();
      if (data?.sending_window) w = data.sending_window;
    } catch {}
  }

  const now  = new Date();
  const hour = now.getHours();
  const day  = now.getDay() || 7; // getDay() returns 0 for Sun; map to 7 for Mon=1..Sun=7

  const within = hour >= w.start_hour && hour < w.end_hour && (w.days || []).includes(day);
  console.log(`[SENDING WINDOW] ${hour}:xx day=${day} → ${within ? 'WITHIN' : 'OUTSIDE'} window (${w.start_hour}-${w.end_hour}, days: ${JSON.stringify(w.days)})`);
  return within;
}

function hasUsableEmail(email) {
  const value = String(email || '').trim();
  return !!value && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function appendNote(existing, note) {
  const current = String(existing || '').trim();
  if (!current) return note;
  if (current.includes(note)) return current;
  return `${current} | ${note}`;
}

function normalizeName(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function namesLikelyMatch(left, right) {
  const a = normalizeName(left).split(/\s+/).filter(Boolean);
  const b = normalizeName(right).split(/\s+/).filter(Boolean);
  if (!a.length || !b.length) return true;
  const shared = a.filter(token => b.includes(token));
  return shared.length >= Math.min(2, a.length, b.length);
}

function splitNameParts(value) {
  const tokens = normalizeName(value).split(/\s+/).filter(Boolean);
  if (!tokens.length) return { firstName: null, lastName: null };
  return {
    firstName: tokens[0] || null,
    lastName: tokens.length > 1 ? tokens[tokens.length - 1] : null,
  };
}

function companyLikelyMatches(contact, person) {
  const expected = normalizeName(contact?.company_name || '');
  if (!expected) return true;
  const actual = normalizeName(
    person?.company_name
    || person?.company
    || person?.companyName
    || person?.organization
    || person?.current_company
    || ''
  );
  if (!actual) return true;
  const expectedTokens = expected.split(/\s+/).filter(Boolean);
  const actualTokens = actual.split(/\s+/).filter(Boolean);
  return expectedTokens.some(token => actualTokens.includes(token));
}

async function findMatchingLinkedInProfileForContact(contact) {
  const name = String(contact?.name || '').trim();
  if (!name) return null;

  const { firstName, lastName } = splitNameParts(name);
  const companyName = String(contact?.company_name || '').trim();
  const queries = [
    [name, companyName].filter(Boolean).join(' '),
    name,
  ].filter(Boolean);

  const candidates = [];
  const seen = new Set();
  const pushCandidate = (person, source) => {
    const fullName = [person?.first_name, person?.last_name].filter(Boolean).join(' ').trim()
      || String(person?.name || '').trim();
    if (!fullName || !namesLikelyMatch(name, fullName) || !companyLikelyMatches(contact, person)) return;
    const providerId = String(person?.provider_id || '').trim() || null;
    const publicId = String(person?.public_identifier || person?.provider_public_id || '').trim() || null;
    const linkedinUrl = normalizeLinkedInProfileUrl(
      person?.public_profile_url
      || person?.profile_url
      || (publicId ? `https://www.linkedin.com/in/${publicId}` : '')
    );
    const key = providerId || publicId || linkedinUrl || fullName.toLowerCase();
    if (!key || seen.has(key)) return;
    seen.add(key);
    candidates.push({
      providerId,
      publicId: publicId || extractLinkedInPublicId(linkedinUrl),
      linkedinUrl: isLikelyLinkedInProfileUrl(linkedinUrl) ? linkedinUrl : null,
      name: fullName,
      source,
    });
  };

  for (const query of queries) {
    try {
      const sales = await searchLinkedInPeopleSalesNavigator({
        firstName,
        lastName,
        keywords: companyName || query,
        limit: 5,
      });
      (sales || []).forEach(person => pushCandidate(person, 'unipile_sales_navigator'));
    } catch {}

    try {
      const classic = await searchLinkedInPeople({
        keywords: query,
        limit: 10,
      });
      (classic || []).forEach(person => pushCandidate(person, 'unipile_search'));
    } catch {}

    if (candidates.length) break;
  }

  return candidates.find(candidate => candidate.providerId || candidate.linkedinUrl) || null;
}

function normalizeLinkedInProfileUrl(value) {
  return String(value || '')
    .trim()
    .replace(/[?#].*$/, '')
    .replace(/\/+$/, '');
}

function isLikelyLinkedInProfileUrl(value) {
  return /^https?:\/\/([a-z]{2,3}\.)?linkedin\.com\/in\/[^/?#\s]+$/i.test(normalizeLinkedInProfileUrl(value));
}

function isLikelyLinkedInProviderId(value) {
  return /^(ACo|ACw|AE)[A-Za-z0-9_-]+$/.test(String(value || '').trim());
}

function extractLinkedInPublicId(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (isLikelyLinkedInProfileUrl(raw)) {
    const match = normalizeLinkedInProfileUrl(raw).match(/linkedin\.com\/in\/([^/?#\s]+)/i);
    return match?.[1] || null;
  }
  if (/^[a-z0-9][a-z0-9-]{2,}$/i.test(raw) && !raw.includes(' ')) return raw;
  return null;
}

function summarizeInviteError(err) {
  return String(err?.message || err || 'Unknown LinkedIn invite error').slice(0, 300);
}

function isAlreadyConnectedError(message) {
  return [
    'already connected',
    'member_already_connected',
    'existing_relationship',
    'you are already connected',
    'connection already exists',
  ].some(token => message.includes(token));
}

function isPendingInviteError(message) {
  return [
    'already sent',
    'invite_already_sent',
    'pending invitation',
    'already invited',
    'already_invited',
  ].some(token => message.includes(token));
}

function isProviderLimitError(message) {
  return [
    'cannot_resend_yet',
    'temporary provider limit',
    'provider limit',
    'cannot resend yet',
  ].some(token => message.includes(token));
}

function parseProviderLimitState(notes) {
  const match = String(notes || '').match(PROVIDER_LIMIT_NOTE_PATTERN);
  if (!match) return null;
  return {
    count: Number(match[1] || 0),
    blockedUntil: match[2] || null,
    notifiedAt: match[3] || null,
  };
}

function stripProviderLimitState(notes) {
  return String(notes || '')
    .replace(PROVIDER_LIMIT_NOTE_PATTERN, '')
    .replace(/\s+\|\s+/g, ' | ')
    .replace(/\s{2,}/g, ' ')
    .trim()
    .replace(/\|\s*$/, '')
    .trim();
}

function appendProviderLimitState(notes, state) {
  const cleaned = stripProviderLimitState(notes);
  const marker = `[LI_INVITE_LIMIT:count=${Math.max(1, Number(state?.count || 0))}|blocked_until=${state?.blockedUntil || ''}${state?.notifiedAt ? `|notified_at=${state.notifiedAt}` : ''}]`;
  return cleaned ? `${cleaned} | ${marker}` : marker;
}

function isInvalidProfileError(err, linkedinUrl) {
  if (!linkedinUrl || !isLikelyLinkedInProfileUrl(linkedinUrl)) return true;
  const message = String(err?.message || '').toLowerCase();
  return err?.status === 404
    || message.includes('user not found')
    || message.includes('resource not found')
    || message.includes('invalid linkedin providerid')
    || message.includes('identifier');
}

async function recordInviteActivity({ pushActivity, logActivity, deal, contact, type, action, note, eventType, detail }) {
  pushActivity?.({
    type,
    action,
    note,
    deal_name: deal?.name || null,
    dealId: deal?.id || null,
  });
  await logActivity?.({
    dealId: deal?.id || null,
    contactId: contact?.id || null,
    eventType,
    summary: `${action}${note ? `: ${note}` : ''}`.slice(0, 300),
    detail,
    apiUsed: 'unipile',
  }).catch(() => {});
}

async function markMissingLinkedIn({ sb, deal, contact, pushActivity, logActivity, linkedinUrl, reason }) {
  const emailUsable = hasUsableEmail(contact?.email);
  const note = reason || 'Could not find LinkedIn';
  const updates = {
    linkedin_url: null,
    linkedin_provider_id: null,
    linkedin_public_id: null,
    linkedin_connected: false,
    pending_linkedin_dm: false,
    invite_sent_at: null,
    invite_accepted_at: null,
    notes: appendNote(contact?.notes, note),
  };

  if (emailUsable) {
    updates.pipeline_stage = 'Enriched';
    updates.enrichment_status = 'email_only';
  } else {
    updates.pipeline_stage = 'Archived';
    updates.enrichment_status = 'skipped_no_linkedin';
    updates.archive_reason = 'Could not find LinkedIn';
  }

  await sb.from('contacts').update(updates).eq('id', contact.id);

  await recordInviteActivity({
    pushActivity,
    logActivity,
    deal,
    contact,
    type: 'warning',
    action: 'Could not find LinkedIn',
    note: `${contact.name || 'Contact'}${note ? ` · ${note}` : ''}`,
    eventType: 'LINKEDIN_INVITE_SKIPPED_NO_PROFILE',
    detail: {
      channel: 'linkedin_invite',
      linkedin_url: linkedinUrl || null,
      email_fallback: emailUsable,
      reason: note,
    },
  });

  return { status: 'missing_profile', emailFallback: emailUsable };
}

export async function fetchPendingLinkedInInvitesSafe() {
  try {
    return await listSentInvitations(100);
  } catch {
    return [];
  }
}

export async function processLinkedInInvite({
  sb,
  deal,
  contact,
  pushActivity,
  logActivity,
  pendingInvites = null,
  source = 'orchestrator',
}) {
  const rawLinkedInValue = String(contact?.linkedin_url || '').trim();
  const linkedinUrl = isLikelyLinkedInProfileUrl(rawLinkedInValue) ? normalizeLinkedInProfileUrl(rawLinkedInValue) : null;
  let providerId = String(contact?.linkedin_provider_id || '').trim() || null;
  let publicId = String(contact?.linkedin_public_id || '').trim() || null;
  if (!providerId && isLikelyLinkedInProviderId(rawLinkedInValue)) providerId = rawLinkedInValue;
  if (!publicId) publicId = extractLinkedInPublicId(rawLinkedInValue);
  const invites = Array.isArray(pendingInvites) ? pendingInvites : await fetchPendingLinkedInInvitesSafe();
  const providerLimitState = parseProviderLimitState(contact?.notes);
  if (providerLimitState?.blockedUntil && new Date(providerLimitState.blockedUntil).getTime() > Date.now()) {
    return {
      status: 'deferred_provider_limit',
      providerId,
      publicId,
      retryAt: providerLimitState.blockedUntil,
      retryCount: providerLimitState.count || 0,
    };
  }

  if (!providerId || !publicId) {
    if (!linkedinUrl && !providerId && !publicId) {
      const discovered = await findMatchingLinkedInProfileForContact(contact);
      if (discovered) {
        providerId = discovered.providerId || providerId;
        publicId = discovered.publicId || publicId;
        if (discovered.linkedinUrl) {
          await sb.from('contacts').update({
            linkedin_url: discovered.linkedinUrl,
            linkedin_provider_id: discovered.providerId || null,
            linkedin_public_id: discovered.publicId || null,
          }).eq('id', contact.id);
        }
      } else {
        return markMissingLinkedIn({ sb, deal, contact, pushActivity, logActivity, linkedinUrl, reason: 'Could not find LinkedIn profile URL' });
      }
    }

    let resolved = null;
    try {
      resolved = await resolveLinkedInProfile(providerId || publicId || linkedinUrl);
      if (resolved?.name && contact?.name && !namesLikelyMatch(contact.name, resolved.name)) {
        const discovered = await findMatchingLinkedInProfileForContact(contact);
        if (!discovered) {
          return markMissingLinkedIn({
            sb,
            deal,
            contact,
            pushActivity,
            logActivity,
            linkedinUrl,
            reason: `Resolved LinkedIn profile does not match contact name (${resolved.name})`,
          });
        }
        resolved = {
          ...resolved,
          providerId: discovered.providerId || resolved.providerId,
          publicId: discovered.publicId || resolved.publicId,
          linkedinUrl: discovered.linkedinUrl || resolved.linkedinUrl,
          name: discovered.name || resolved.name,
        };
      }
      if (resolved?.providerId) providerId = resolved.providerId;
      if (resolved?.publicId) publicId = resolved.publicId;

      const updates = {};
      if (resolved?.providerId && resolved.providerId !== contact.linkedin_provider_id) updates.linkedin_provider_id = resolved.providerId;
      if (resolved?.publicId && resolved.publicId !== contact.linkedin_public_id) updates.linkedin_public_id = resolved.publicId;
      if (resolved?.linkedinUrl && resolved.linkedinUrl !== contact.linkedin_url) updates.linkedin_url = resolved.linkedinUrl;
      if (Object.keys(updates).length) {
        await sb.from('contacts').update(updates).eq('id', contact.id);
      }
    } catch (err) {
      if (isInvalidProfileError(err, linkedinUrl)) {
        return markMissingLinkedIn({
          sb,
          deal,
          contact,
          pushActivity,
          logActivity,
          linkedinUrl,
          reason: `Could not find LinkedIn profile: ${summarizeInviteError(err)}`,
        });
      }

      await recordInviteActivity({
        pushActivity,
        logActivity,
        deal,
        contact,
        type: 'error',
        action: 'LinkedIn profile lookup failed',
        note: `${contact.name || 'Contact'} · ${summarizeInviteError(err)}`,
        eventType: 'LINKEDIN_PROFILE_LOOKUP_FAILED',
        detail: {
          channel: 'linkedin_invite',
          linkedin_url: linkedinUrl || null,
          error: summarizeInviteError(err),
          source,
        },
      });
      return { status: 'failed_lookup', error: err };
    }
  }

  if (!providerId) {
    return markMissingLinkedIn({ sb, deal, contact, pushActivity, logActivity, linkedinUrl, reason: 'Could not find LinkedIn provider ID' });
  }

  const inviteAlreadyPending = invites.some(inv => {
    const invitedProviderId = String(inv?.invited_user_id || '').trim();
    const invitedPublicId = String(inv?.invited_user_public_id || '').trim().toLowerCase();
    return invitedProviderId === providerId || (!!publicId && invitedPublicId === publicId.toLowerCase());
  });

  const liFollowupDays = deal?.followup_days_li || 7;
  const followUpDueAt = new Date(Date.now() + liFollowupDays * 24 * 60 * 60 * 1000).toISOString();

  if (inviteAlreadyPending) {
    await sb.from('contacts').update({
      pipeline_stage: 'invite_sent',
      outreach_channel: 'linkedin_invite',
      follow_up_due_at: followUpDueAt,
    }).eq('id', contact.id);

    await recordInviteActivity({
      pushActivity,
      logActivity,
      deal,
      contact,
      type: 'invite',
      action: 'Already pending',
      note: `${contact.name || 'Contact'} already has a pending LinkedIn invite`,
      eventType: 'LINKEDIN_INVITE_ALREADY_PENDING',
      detail: { channel: 'linkedin_invite', provider_id: providerId, public_id: publicId, source },
    });
    return { status: 'already_pending', providerId, publicId };
  }

  const relationship = await getLinkedInRelationship(providerId).catch(() => 'unknown');
  if (relationship === 'connected') {
    await sb.from('contacts').update({
      pipeline_stage: 'invite_accepted',
      linkedin_connected: true,
      invite_accepted_at: new Date().toISOString(),
      outreach_channel: 'linkedin_dm',
    }).eq('id', contact.id);

    await recordInviteActivity({
      pushActivity,
      logActivity,
      deal,
      contact,
      type: 'invite',
      action: 'Already connected',
      note: `${contact.name || 'Contact'} moved directly to DM queue`,
      eventType: 'LINKEDIN_ALREADY_CONNECTED',
      detail: { channel: 'linkedin_invite', provider_id: providerId, public_id: publicId, source },
    });
    return { status: 'already_connected', providerId, publicId };
  }

  if (relationship === 'pending') {
    await sb.from('contacts').update({
      pipeline_stage: 'invite_sent',
      outreach_channel: 'linkedin_invite',
      follow_up_due_at: followUpDueAt,
    }).eq('id', contact.id);

    await recordInviteActivity({
      pushActivity,
      logActivity,
      deal,
      contact,
      type: 'invite',
      action: 'Already pending',
      note: `${contact.name || 'Contact'} already has a pending LinkedIn invite`,
      eventType: 'LINKEDIN_INVITE_ALREADY_PENDING',
      detail: { channel: 'linkedin_invite', provider_id: providerId, public_id: publicId, source, relationship },
    });
    return { status: 'already_pending', providerId, publicId };
  }

  if (relationship === 'blocked') {
    await sb.from('contacts').update({ pipeline_stage: 'Archived' }).eq('id', contact.id);
    await recordInviteActivity({
      pushActivity,
      logActivity,
      deal,
      contact,
      type: 'warning',
      action: 'LinkedIn blocked',
      note: `${contact.name || 'Contact'} could not be invited`,
      eventType: 'LINKEDIN_INVITE_BLOCKED',
      detail: { channel: 'linkedin_invite', provider_id: providerId, public_id: publicId, source },
    });
    return { status: 'blocked', providerId, publicId };
  }

  try {
    const inviteResult = await sendLinkedInInvite({ providerId });
    const cleanedNotes = stripProviderLimitState(contact?.notes);
    await sb.from('contacts').update({
      pipeline_stage: 'invite_sent',
      invite_sent_at: new Date().toISOString(),
      outreach_channel: 'linkedin_invite',
      follow_up_count: 0,
      follow_up_due_at: followUpDueAt,
      notes: cleanedNotes || null,
      linkedin_provider_id: providerId || contact.linkedin_provider_id || null,
      linkedin_public_id: publicId || contact.linkedin_public_id || null,
    }).eq('id', contact.id);

    await recordInviteActivity({
      pushActivity,
      logActivity,
      deal,
      contact,
      type: 'invite',
      action: 'Connection request sent',
      note: `${contact.name || 'Contact'}${contact.company_name ? ` @ ${contact.company_name}` : ''}`,
      eventType: 'LINKEDIN_INVITE_SENT',
      detail: {
        channel: 'linkedin_invite',
        provider_id: inviteResult?.providerId || providerId,
        public_id: publicId,
        invitation_id: inviteResult?.invitationId || null,
        account_id: inviteResult?.accountId || null,
        usage: inviteResult?.usage || null,
        source,
      },
    });

    return { status: 'sent', providerId, publicId, inviteResult };
  } catch (err) {
    const message = String(err?.message || '').toLowerCase();
    if (isAlreadyConnectedError(message)) {
      await sb.from('contacts').update({
        pipeline_stage: 'invite_accepted',
        linkedin_connected: true,
        invite_accepted_at: new Date().toISOString(),
        outreach_channel: 'linkedin_dm',
        notes: appendNote(contact?.notes, 'Was already connected on LinkedIn — skipped invite, queued for DM'),
      }).eq('id', contact.id);

      await recordInviteActivity({
        pushActivity,
        logActivity,
        deal,
        contact,
        type: 'invite',
        action: 'Already connected',
        note: `${contact.name || 'Contact'} moved directly to DM queue`,
        eventType: 'LINKEDIN_ALREADY_CONNECTED',
        detail: { channel: 'linkedin_invite', provider_id: providerId, public_id: publicId, source, error: summarizeInviteError(err) },
      });
      return { status: 'already_connected', providerId, publicId };
    }

    if (isPendingInviteError(message)) {
      // Stage is already invite_sent from the pre-call stamp — just log it
      await recordInviteActivity({
        pushActivity,
        logActivity,
        deal,
        contact,
        type: 'invite',
        action: 'Already pending',
        note: `${contact.name || 'Contact'} already has a pending LinkedIn invite`,
        eventType: 'LINKEDIN_INVITE_ALREADY_PENDING',
        detail: { channel: 'linkedin_invite', provider_id: providerId, public_id: publicId, source, error: summarizeInviteError(err) },
      });
      return { status: 'already_pending', providerId, publicId };
    }

    if (isProviderLimitError(message)) {
      const retryCount = Math.max(0, Number(providerLimitState?.count || 0)) + 1;
      const cooldownHours = LINKEDIN_PROVIDER_LIMIT_RETRY_HOURS[Math.min(retryCount - 1, LINKEDIN_PROVIDER_LIMIT_RETRY_HOURS.length - 1)];
      const retryAt = new Date(Date.now() + cooldownHours * 60 * 60 * 1000).toISOString();
      const shouldNotify = retryCount >= 3 && !providerLimitState?.notifiedAt;
      const nextNotes = appendProviderLimitState(contact?.notes, {
        count: retryCount,
        blockedUntil: retryAt,
        notifiedAt: shouldNotify ? new Date().toISOString() : providerLimitState?.notifiedAt || null,
      });

      try {
        await sb.from('contacts').update({
          pipeline_stage: contact.pipeline_stage,
          invite_sent_at: null,
          outreach_channel: null,
          follow_up_due_at: retryAt,
          notes: nextNotes,
        }).eq('id', contact.id);
      } catch {}

      await recordInviteActivity({
        pushActivity,
        logActivity,
        deal,
        contact,
        type: retryCount >= 3 ? 'warning' : 'invite',
        action: retryCount >= 3 ? 'LinkedIn invite paused for manual review' : 'LinkedIn invite deferred by provider limit',
        note: `${contact.name || 'Contact'} · retry ${retryCount}/3 · next attempt ${retryAt}`,
        eventType: retryCount >= 3 ? 'LINKEDIN_INVITE_PROVIDER_LIMIT_ESCALATED' : 'LINKEDIN_INVITE_PROVIDER_LIMIT',
        detail: {
          channel: 'linkedin_invite',
          provider_id: providerId,
          public_id: publicId,
          linkedin_url: linkedinUrl || rawLinkedInValue || null,
          source,
          error: summarizeInviteError(err),
          retry_count: retryCount,
          retry_at: retryAt,
        },
      });

      if (shouldNotify) {
        try {
          const { sendTelegram } = await import('../approval/telegramBot.js');
          await sendTelegram(
            `⚠️ *LinkedIn invite limit reached*\n\n` +
            `Unipile/LinkedIn returned \`cannot_resend_yet\` for *${contact.name || 'Unknown contact'}* at *${contact.company_name || 'Unknown firm'}*.\n` +
            `Roco retried 3 times and paused further invite attempts until manual review.\n` +
            `${linkedinUrl || rawLinkedInValue ? `LinkedIn: ${linkedinUrl || rawLinkedInValue}\n` : ''}` +
            `${contact.email ? `Email on file: ${contact.email}\n` : ''}` +
            `Next automatic retry not before: ${retryAt}`
          );
        } catch {}
      }

      return { status: 'deferred_provider_limit', providerId, publicId, error: err, retryAt, retryCount };
    }

    await recordInviteActivity({
      pushActivity,
      logActivity,
      deal,
      contact,
      type: 'error',
      action: 'LinkedIn invite failed',
      note: `${contact.name || 'Contact'} · ${summarizeInviteError(err)}`,
      eventType: 'LINKEDIN_INVITE_FAILED',
      detail: {
        channel: 'linkedin_invite',
        provider_id: providerId,
        public_id: publicId,
        linkedin_url: linkedinUrl || null,
        source,
        error: summarizeInviteError(err),
        status: err?.status || null,
      },
    });
    return { status: 'failed_send', providerId, publicId, error: err };
  }
}
