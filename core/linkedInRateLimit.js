/**
 * core/linkedInRateLimit.js
 * Shared, file-persisted LinkedIn rate-limit cooldown.
 * Survives PM2 restarts — cooldown set before a restart is honoured after.
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const STATE_FILE = path.join(__dirname, '..', 'linkedin_ratelimit.json');

const COOLDOWN_MS = 45 * 60 * 1000; // 45 minutes

function readState() {
  try {
    const raw = fs.readFileSync(STATE_FILE, 'utf8');
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

function writeState(state) {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
  } catch {}
}

/**
 * Mark LinkedIn as rate-limited right now. Persists to disk.
 * @param {string} source - label for the log (e.g. 'FIRM RESEARCH', 'LINKEDIN FINDER')
 */
export function markLinkedInRateLimited(source = 'LINKEDIN') {
  const until = Date.now() + COOLDOWN_MS;
  const state = readState();
  state.rateLimitedUntil = until;
  writeState(state);
  console.warn(`[${source}] LinkedIn rate limited — pausing all searches for 45 minutes (until ${new Date(until).toISOString()})`);
}

/**
 * Returns true if LinkedIn is currently rate-limited (across all sources).
 * @param {string} source - label for the expiry log
 */
export function isLinkedInRateLimited(source = 'LINKEDIN') {
  const state = readState();
  const until = state.rateLimitedUntil;
  if (!until) return false;
  if (Date.now() >= until) {
    // Expired — clear it
    delete state.rateLimitedUntil;
    writeState(state);
    console.info(`[${source}] LinkedIn rate limit cooldown expired — resuming searches`);
    return false;
  }
  return true;
}

export function is429Error(err) {
  const msg = String(err?.message || err || '');
  return msg.includes('429') || msg.toLowerCase().includes('too_many_requests') || msg.toLowerCase().includes('too many requests');
}
