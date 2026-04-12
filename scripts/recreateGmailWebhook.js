/**
 * scripts/recreateGmailWebhook.js
 * Resets the Roco Unipile webhooks to the exact account-scoped set used in production.
 * Run: node scripts/recreateGmailWebhook.js
 */

import dotenv from 'dotenv';
dotenv.config({ path: '/root/roco/.env' });

const rawDsn = process.env.UNIPILE_DSN || 'https://api34.unipile.com:16411';
const DSN = rawDsn.startsWith('http') ? rawDsn.replace(/\/+$/, '') : `https://${rawDsn.replace(/\/+$/, '')}`;
const KEY = process.env.UNIPILE_API_KEY;
const BASE_URL = String(
  process.env.PUBLIC_URL ||
  process.env.SERVER_BASE_URL ||
  process.env.RAILWAY_STATIC_URL ||
  process.env.RAILWAY_PUBLIC_DOMAIN ||
  'https://roco-production.up.railway.app'
).trim().replace(/\/+$/, '');

const ACCOUNT_IDS = {
  linkedin: process.env.UNIPILE_LINKEDIN_ACCOUNT_ID || '',
  gmail: process.env.UNIPILE_GMAIL_ACCOUNT_ID || '',
  outlook: process.env.UNIPILE_OUTLOOK_ACCOUNT_ID || '',
};

function headers() {
  return { 'X-API-KEY': KEY, 'Content-Type': 'application/json', accept: 'application/json' };
}

function normalizeWebhookList(data) {
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.items)) return data.items;
  if (Array.isArray(data?.results)) return data.results;
  if (Array.isArray(data?.webhooks)) return data.webhooks;
  return [];
}

function normalizeAccountIds(webhook) {
  if (Array.isArray(webhook?.account_ids)) return webhook.account_ids.map(String);
  if (webhook?.account_id) return [String(webhook.account_id)];
  return [];
}

function isRocoWebhook(webhook) {
  const name = String(webhook?.name || '');
  const url = String(webhook?.request_url || '');
  return name.startsWith('roco_')
    || url.includes('/webhooks/unipile/messages')
    || url.includes('/webhook/unipile/gmail')
    || url.includes('/webhook/unipile/outlook')
    || url.includes('/webhook/unipile/linkedin');
}

async function api(path, options = {}) {
  const res = await fetch(`${DSN}${path}`, {
    ...options,
    headers: {
      ...headers(),
      ...(options.headers || {}),
    },
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${options.method || 'GET'} ${path} failed: ${res.status} ${text.slice(0, 300)}`);
  return text ? JSON.parse(text) : null;
}

const desiredWebhooks = [
  {
    name: 'roco_linkedin_messages',
    request_url: `${BASE_URL}/webhooks/unipile/messages`,
    source: 'messaging',
    events: ['message_received'],
    format: 'json',
    enabled: true,
    account_ids: [ACCOUNT_IDS.linkedin].filter(Boolean),
  },
  {
    name: 'roco_linkedin_relations',
    request_url: `${BASE_URL}/webhooks/unipile/messages`,
    source: 'users',
    events: ['new_relation'],
    format: 'json',
    enabled: true,
    account_ids: [ACCOUNT_IDS.linkedin].filter(Boolean),
  },
  {
    name: 'roco_gmail_inbound',
    request_url: `${BASE_URL}/webhook/unipile/gmail`,
    source: 'email',
    events: ['mail_received'],
    format: 'json',
    enabled: true,
    account_ids: [ACCOUNT_IDS.gmail].filter(Boolean),
  },
  {
    name: 'roco_outlook_inbound',
    request_url: `${BASE_URL}/webhook/unipile/outlook`,
    source: 'email',
    events: ['mail_received'],
    format: 'json',
    enabled: true,
    account_ids: [ACCOUNT_IDS.outlook].filter(Boolean),
  },
].map(webhook => ({
  ...webhook,
  headers: [{ key: 'Content-Type', value: 'application/json' }],
}));

async function resetRocoWebhooks() {
  console.log('Resetting Roco Unipile webhooks');
  console.log(`DSN: ${DSN}`);
  console.log(`Base URL: ${BASE_URL}`);
  console.log('Account IDs:', JSON.stringify(ACCOUNT_IDS, null, 2));

  const existing = normalizeWebhookList(await api('/api/v1/webhooks'));
  console.log(`Found ${existing.length} existing webhook(s)`);

  for (const webhook of existing.filter(isRocoWebhook)) {
    if (!webhook.id) continue;
    console.log(`Deleting ${webhook.name || webhook.id} -> ${webhook.request_url} [${normalizeAccountIds(webhook).join(',') || 'all accounts'}]`);
    await api(`/api/v1/webhooks/${webhook.id}`, { method: 'DELETE' });
  }

  for (const webhook of desiredWebhooks) {
    console.log(`Creating ${webhook.name} -> ${webhook.request_url} [${webhook.account_ids.join(',') || 'all accounts'}]`);
    await api('/api/v1/webhooks', {
      method: 'POST',
      body: JSON.stringify(webhook),
    });
  }

  const refreshed = normalizeWebhookList(await api('/api/v1/webhooks'));
  const summary = refreshed
    .filter(isRocoWebhook)
    .map(webhook => ({
      id: webhook.id || webhook.webhook_id || null,
      name: webhook.name || null,
      source: webhook.source || null,
      events: webhook.events || [],
      request_url: webhook.request_url || null,
      account_ids: normalizeAccountIds(webhook),
    }));

  console.log('\nCurrent Roco webhooks:');
  console.log(JSON.stringify(summary, null, 2));
}

resetRocoWebhooks().then(() => {
  console.log('\nDone.');
  process.exit(0);
}).catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
