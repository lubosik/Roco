/**
 * apiFallback.js — Comprehensive API fallback system for Roco
 *
 * Every external API call flows through withFallback().
 * Health state is tracked live and exposed to the dashboard.
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

// ─────────────────────────────────────────────
// HEALTH STATE
// ─────────────────────────────────────────────

export const apiHealth = {
  anthropic:         { status: 'ok', lastCheck: null, lastError: null, fallbackActive: false },
  openai:            { status: 'ok', lastCheck: null, lastError: null, fallbackActive: false },
  gemini:            { status: 'ok', lastCheck: null, lastError: null, fallbackActive: false },
  kaspr:             { status: 'ok', lastCheck: null, lastError: null, fallbackActive: false },
  notion:            { status: 'ok', lastCheck: null, lastError: null, fallbackActive: false },
  gmail:             { status: 'ok', lastCheck: null, lastError: null, fallbackActive: false },
  telegram:          { status: 'ok', lastCheck: null, lastError: null, fallbackActive: false },
  serpapi:           { status: 'ok', lastCheck: null, lastError: null, fallbackActive: false },
  apify:             { status: 'ok', lastCheck: null, lastError: null, fallbackActive: false },
  grok:              { status: 'ok', lastCheck: null, lastError: null, fallbackActive: false },
  millionverifier:   { status: 'ok', lastCheck: null, lastError: null, fallbackActive: false },
};

export function getApiHealth() {
  return { ...apiHealth };
}

function updateHealth(service, status, errorMsg = null) {
  if (apiHealth[service]) {
    apiHealth[service].status = status;
    apiHealth[service].lastCheck = new Date().toISOString();
    if (errorMsg) apiHealth[service].lastError = errorMsg;
  }
}

// ─────────────────────────────────────────────
// TELEGRAM ALERT (direct HTTP — no circular deps)
// ─────────────────────────────────────────────

async function sendFallbackAlert(message) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return;
  try {
    await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text: message }),
    });
  } catch {
    // Best-effort
  }
}

// ─────────────────────────────────────────────
// CORE withFallback WRAPPER
// ─────────────────────────────────────────────

/**
 * withFallback(name, attempts, context)
 *
 * @param {string} name - Service name (e.g. 'email_generation')
 * @param {Function[]} attempts - Array of async functions to try in order
 * @param {object} context - Optional context for logging (contactId, dealId, etc.)
 * @returns {*} Result from first successful attempt
 */
export async function withFallback(name, attempts, context = {}) {
  const providerNames = [
    'primary', 'fallback-1', 'fallback-2', 'fallback-3', 'fallback-4'
  ];

  for (let i = 0; i < attempts.length; i++) {
    const label = providerNames[i] || `fallback-${i}`;
    try {
      const result = await attempts[i]();
      if (result !== null && result !== undefined) {
        if (i > 0) {
          console.log(`[apiFallback] ${name} succeeded via ${label}`);
          updateHealthFromName(name, i);
        }
        return result;
      }
      throw new Error('Empty result');
    } catch (err) {
      const errMsg = err?.message || String(err);
      console.warn(`[apiFallback] ${name} ${label} failed: ${errMsg}`);

      updateHealthFromName(name, i, errMsg);

      if (i === 0 && attempts.length > 1) {
        await sendFallbackAlert(
          `ROCO FALLBACK ALERT\n\n${name} primary failed.\nSwitching to fallback. Error: ${errMsg}`
        );
      }

      if (i < attempts.length - 1) {
        await sleep(2000);
        continue;
      }

      // All attempts failed
      console.error(`[apiFallback] ${name} — ALL attempts failed`);
      return null;
    }
  }
}

function updateHealthFromName(name, failedIndex, errorMsg = null) {
  // Map operation names to API service names
  const serviceMap = {
    'email_generation': failedIndex === 0 ? 'anthropic' : failedIndex === 1 ? 'openai' : 'gemini',
    'research': failedIndex === 0 ? 'gemini' : failedIndex === 1 ? 'serpapi' : 'apify',
    'enrichment': failedIndex === 0 ? 'kaspr' : null,
    'email_send': failedIndex === 0 ? 'gmail' : null,
    'notion': 'notion',
    'telegram': 'telegram',
  };

  const service = serviceMap[name];
  if (!service) return;

  if (errorMsg) {
    updateHealth(service, 'degraded', errorMsg);
    apiHealth[service].fallbackActive = failedIndex > 0;
  } else {
    updateHealth(service, 'ok');
    apiHealth[service].fallbackActive = false;
  }
}

// ─────────────────────────────────────────────
// NOTION FALLBACK DATABASE (SQLite)
// ─────────────────────────────────────────────

let db = null;
const FALLBACK_DB = path.join(__dirname, '../fallback.db');
const FALLBACK_LOG = path.join(__dirname, '../fallback-log.json');

function getDb() {
  if (db) return db;
  try {
    const Database = require('better-sqlite3');
    db = new Database(FALLBACK_DB);
    db.exec(`
      CREATE TABLE IF NOT EXISTS fallback_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL,
        record TEXT NOT NULL,
        synced INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `);
    return db;
  } catch (err) {
    console.warn('[apiFallback] better-sqlite3 not available:', err.message);
    return null;
  }
}

export function writeFallbackRecord(type, record) {
  // Try SQLite first
  const database = getDb();
  if (database) {
    try {
      database.prepare('INSERT INTO fallback_records (type, record) VALUES (?, ?)')
        .run(type, JSON.stringify(record));
      return;
    } catch (err) {
      console.warn('[apiFallback] SQLite write failed:', err.message);
    }
  }

  // Fall back to JSON log
  try {
    const entry = { type, record, timestamp: new Date().toISOString(), synced: false };
    const existing = fs.existsSync(FALLBACK_LOG)
      ? JSON.parse(fs.readFileSync(FALLBACK_LOG, 'utf8'))
      : [];
    existing.push(entry);
    fs.writeFileSync(FALLBACK_LOG, JSON.stringify(existing, null, 2));
  } catch (err) {
    console.error('[apiFallback] Fallback log write failed:', err.message);
  }
}

export function getUnsyncedRecords() {
  const database = getDb();
  if (database) {
    try {
      return database.prepare('SELECT * FROM fallback_records WHERE synced = 0').all()
        .map(r => ({ ...r, record: JSON.parse(r.record) }));
    } catch {}
  }

  try {
    if (fs.existsSync(FALLBACK_LOG)) {
      return JSON.parse(fs.readFileSync(FALLBACK_LOG, 'utf8')).filter(r => !r.synced);
    }
  } catch {}
  return [];
}

export function markRecordSynced(id) {
  const database = getDb();
  if (database) {
    try {
      database.prepare('UPDATE fallback_records SET synced = 1 WHERE id = ?').run(id);
      return;
    } catch {}
  }

  try {
    if (fs.existsSync(FALLBACK_LOG)) {
      const records = JSON.parse(fs.readFileSync(FALLBACK_LOG, 'utf8'));
      const idx = records.findIndex(r => r.id === id);
      if (idx >= 0) records[idx].synced = true;
      fs.writeFileSync(FALLBACK_LOG, JSON.stringify(records, null, 2));
    }
  } catch {}
}

// ─────────────────────────────────────────────
// HEALTH CHECK PINGS (every 60 seconds)
// ─────────────────────────────────────────────

async function pingService(name, checkFn) {
  try {
    await checkFn();
    updateHealth(name, 'ok');
  } catch (err) {
    updateHealth(name, 'degraded', err.message);
  }
}

async function runHealthChecks() {
  const checks = [];

  // Anthropic — check API key + lightweight reach
  if (process.env.ANTHROPIC_API_KEY) {
    checks.push(pingService('anthropic', async () => {
      const res = await fetchWithTimeout('https://api.anthropic.com/v1/models', {
        headers: {
          'x-api-key': process.env.ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
        },
      }, 8000);
      if (!res.ok && res.status !== 200) throw new Error(`HTTP ${res.status}`);
    }));
  } else {
    updateHealth('anthropic', 'unconfigured', 'ANTHROPIC_API_KEY not set');
  }

  // OpenAI
  if (process.env.OPENAI_API_KEY) {
    checks.push(pingService('openai', async () => {
      const res = await fetchWithTimeout('https://api.openai.com/v1/models', {
        headers: { 'Authorization': `Bearer ${process.env.OPENAI_API_KEY}` },
      }, 8000);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    }));
  } else {
    updateHealth('openai', 'unconfigured', 'OPENAI_API_KEY not set');
  }

  // Gemini — ping models list with the key
  if (process.env.GEMINI_API_KEY) {
    checks.push(pingService('gemini', async () => {
      const res = await fetchWithTimeout(
        `https://generativelanguage.googleapis.com/v1beta/models?key=${process.env.GEMINI_API_KEY}`,
        {}, 8000
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    }));
  } else {
    updateHealth('gemini', 'unconfigured', 'GEMINI_API_KEY not set');
  }

  // KASPR — ping search endpoint (401 = reachable, bad key; 200 = ok)
  if (process.env.KASPR_API_KEY) {
    checks.push(pingService('kaspr', async () => {
      const res = await fetchWithTimeout('https://api.kaspr.io/v1/linkedin/person/email-address', {
        method: 'POST',
        headers: { 'X-API-Key': process.env.KASPR_API_KEY, 'Content-Type': 'application/json' },
        body: JSON.stringify({ linkedin_url: 'https://linkedin.com/in/ping' }),
      }, 6000);
      // 400/422 = API up, key valid, bad input — fine. 401/403 = bad key. 5xx = down.
      if (res.status === 401 || res.status === 403) throw new Error(`Auth failed (HTTP ${res.status}) — check KASPR_API_KEY`);
      if (res.status >= 500) throw new Error(`HTTP ${res.status}`);
    }));
  } else {
    updateHealth('kaspr', 'unconfigured', 'KASPR_API_KEY not set');
  }

  // Notion
  if (process.env.NOTION_API_KEY) {
    checks.push(pingService('notion', async () => {
      const res = await fetchWithTimeout('https://api.notion.com/v1/users/me', {
        headers: {
          'Authorization': `Bearer ${process.env.NOTION_API_KEY}`,
          'Notion-Version': '2022-06-28',
        },
      }, 8000);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    }));
  } else {
    updateHealth('notion', 'unconfigured', 'NOTION_API_KEY not set');
  }

  // Unipile (Gmail + LinkedIn) — ping accounts list
  if (process.env.UNIPILE_API_KEY) {
    checks.push(pingService('gmail', async () => {
      const dsn = process.env.UNIPILE_DSN || 'https://api34.unipile.com:16411';
      const res = await fetchWithTimeout(`${dsn}/api/v1/accounts`, {
        headers: { 'X-API-KEY': process.env.UNIPILE_API_KEY, 'accept': 'application/json' },
      }, 8000);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    }));
  } else {
    updateHealth('gmail', 'unconfigured', 'UNIPILE_API_KEY not set');
  }

  // Telegram
  if (process.env.TELEGRAM_BOT_TOKEN) {
    checks.push(pingService('telegram', async () => {
      const res = await fetchWithTimeout(
        `https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/getMe`,
        {}, 6000
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    }));
  } else {
    updateHealth('telegram', 'unconfigured', 'TELEGRAM_BOT_TOKEN not set');
  }

  // SerpAPI — ping account info endpoint
  if (process.env.SERP_API_KEY) {
    checks.push(pingService('serpapi', async () => {
      const res = await fetchWithTimeout(
        `https://serpapi.com/account.json?api_key=${process.env.SERP_API_KEY}`,
        {}, 8000
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    }));
  } else {
    updateHealth('serpapi', 'unconfigured', 'SERP_API_KEY not set');
  }

  // Apify — ping user info endpoint
  if (process.env.APIFY_API_TOKEN) {
    checks.push(pingService('apify', async () => {
      const res = await fetchWithTimeout(
        `https://api.apify.com/v2/users/me?token=${process.env.APIFY_API_TOKEN}`,
        {}, 8000
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    }));
  } else {
    updateHealth('apify', 'unconfigured', 'APIFY_API_TOKEN not set');
  }

  // Grok (xAI) — ping models endpoint to confirm key is valid
  if (process.env.GROK_API_KEY) {
    checks.push(pingService('grok', async () => {
      const res = await fetchWithTimeout('https://api.x.ai/v1/models', {
        headers: { 'Authorization': `Bearer ${process.env.GROK_API_KEY}` },
      }, 8000);
      if (res.status === 401 || res.status === 403) throw new Error(`Auth failed (HTTP ${res.status}) — check GROK_API_KEY`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    }));
  } else {
    updateHealth('grok', 'unconfigured', 'GROK_API_KEY not set');
  }

  // MillionVerifier — verify API key with a test email
  if (process.env.MILLION_VERIFIER_API_KEY) {
    checks.push(pingService('millionverifier', async () => {
      const key = process.env.MILLION_VERIFIER_API_KEY;
      const res = await fetchWithTimeout(
        `https://api.millionverifier.com/api/v3/?api=${key}&email=test@example.com&timeout=5`,
        {}, 8000
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      if (data.error && String(data.error).toLowerCase().includes('invalid')) throw new Error('Invalid API key');
    }));
  } else {
    updateHealth('millionverifier', 'unconfigured', 'MILLION_VERIFIER_API_KEY not set');
  }

  await Promise.allSettled(checks);
}

async function fetchWithTimeout(url, options = {}, timeout = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

// ─────────────────────────────────────────────
// START HEALTH CHECK LOOP
// ─────────────────────────────────────────────

export function startHealthChecks() {
  // Initial check after 5 seconds
  setTimeout(runHealthChecks, 5000);
  // Then every 60 seconds
  setInterval(runHealthChecks, 60000);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
