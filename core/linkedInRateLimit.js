/**
 * core/linkedInRateLimit.js
 * Shared, file-persisted LinkedIn rate-limit cooldown.
 * Survives PM2 restarts — cooldown set before a restart is honoured after.
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { readGlobalRuntimeSetting, writeGlobalRuntimeSetting } from './runtimeCoordination.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const STATE_FILE = path.join(__dirname, '..', 'linkedin_ratelimit.json');
const GLOBAL_KEY = 'GLOBAL_LINKEDIN_RATE_LIMIT';

const COOLDOWN_MS = 15 * 60 * 1000; // 15 minutes
const SHARED_REFRESH_MS = 30_000;
let sharedState = null;
let lastSharedSyncAt = 0;
let sharedSyncInFlight = null;

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

function mergeStates(localState, remoteState) {
  const localUntil = Number(localState?.rateLimitedUntil || 0);
  const remoteUntil = Number(remoteState?.rateLimitedUntil || 0);
  return {
    rateLimitedUntil: Math.max(localUntil, remoteUntil) || null,
  };
}

async function refreshSharedState(force = false) {
  if (!force && Date.now() - lastSharedSyncAt < SHARED_REFRESH_MS) return sharedState;
  if (sharedSyncInFlight) return sharedSyncInFlight;

  sharedSyncInFlight = (async () => {
    try {
      const remote = await readGlobalRuntimeSetting(GLOBAL_KEY);
      sharedState = normalizeSharedState(remote);
      lastSharedSyncAt = Date.now();
      return sharedState;
    } finally {
      sharedSyncInFlight = null;
    }
  })();

  return sharedSyncInFlight;
}

function normalizeSharedState(value) {
  if (!value || typeof value !== 'object') return {};
  const until = Number(value.rateLimitedUntil || 0);
  return until ? { rateLimitedUntil: until } : {};
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
  sharedState = { rateLimitedUntil: until };
  lastSharedSyncAt = Date.now();
  writeGlobalRuntimeSetting(GLOBAL_KEY, { rateLimitedUntil: until }).catch(() => {});
  console.warn(`[${source}] LinkedIn rate limited — pausing all searches for 15 minutes (until ${new Date(until).toISOString()})`);
}

/**
 * Returns true if LinkedIn is currently rate-limited (across all sources).
 * @param {string} source - label for the expiry log
 */
export function isLinkedInRateLimited(source = 'LINKEDIN') {
  refreshSharedState().catch(() => {});
  const state = mergeStates(readState(), sharedState);
  const until = state.rateLimitedUntil;
  if (!until) return false;
  if (Date.now() >= until) {
    // Expired — clear it
    delete state.rateLimitedUntil;
    writeState(state);
    sharedState = {};
    lastSharedSyncAt = Date.now();
    writeGlobalRuntimeSetting(GLOBAL_KEY, {}).catch(() => {});
    console.info(`[${source}] LinkedIn rate limit cooldown expired — resuming searches`);
    return false;
  }
  return true;
}

export function is429Error(err) {
  const msg = String(err?.message || err || '');
  return msg.includes('429') || msg.toLowerCase().includes('too_many_requests') || msg.toLowerCase().includes('too many requests');
}
