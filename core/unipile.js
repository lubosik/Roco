// core/unipile.js
// Single wrapper for all Unipile API calls.
// Live credentials: Supabase first, env fallback, cached 30s.

import { getSupabase } from './supabase.js';

let _cachedCreds = null;
let _credsCachedAt = 0;
const CRED_TTL = 30_000;

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
    source:      'messaging',
    events:      ['message_received'],
    request_url: targetUrl,
    account_id:  creds.linkedinAccountId,
  }});

  // LinkedIn connection accepted events
  await request('POST', '/webhooks', { body: {
    source:      'users',
    events:      ['new_relation'],
    request_url: targetUrl,
    account_id:  creds.linkedinAccountId,
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
