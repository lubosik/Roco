import { info, warn, error } from '../core/logger.js';

const KASPR_ENDPOINT   = 'https://api.developers.kaspr.io/profile/linkedin';
const KASPR_RATE_URL   = 'https://api.developers.kaspr.io/keys/rateLimits';

// Simple minutely call counter — resets every 60 s
let _minuteCount = 0;
let _minuteReset = Date.now() + 60_000;
let _rateLimits  = null; // cached from API

/** Extract LinkedIn slug from a full URL or return as-is if already a slug. */
function extractSlug(linkedinUrl) {
  if (!linkedinUrl) return null;
  const m = linkedinUrl.match(/linkedin\.com\/in\/([^/?#]+)/i);
  return m ? m[1] : linkedinUrl.replace(/\/$/, '');
}

/**
 * Fetch and cache KASPR rate limits.
 * Returns { dailyLimit, hourlyLimit, minutelyLimit } or null on failure.
 */
async function fetchRateLimits() {
  const key = process.env.KASPR_API_KEY;
  if (!key) return null;
  try {
    const res = await fetch(KASPR_RATE_URL, {
      headers: { authorization: `Bearer ${key}`, Accept: 'application/json' },
    });
    if (res.ok) {
      _rateLimits = await res.json();
      info(`[KASPR] Rate limits — daily:${_rateLimits.dailyLimit} hourly:${_rateLimits.hourlyLimit} minutely:${_rateLimits.minutelyLimit}`);
    }
  } catch {}
  return _rateLimits;
}

/**
 * Returns true if we're safe to make a KASPR call this minute.
 * Tracks locally and checks against cached limits.
 */
function isWithinRateLimit() {
  const now = Date.now();
  if (now > _minuteReset) {
    _minuteCount = 0;
    _minuteReset = now + 60_000;
  }
  const minuteLimit = _rateLimits?.minutelyLimit ?? 10; // default safe assumption
  return _minuteCount < minuteLimit;
}

/**
 * enrichWithKaspr — enrich a contact by LinkedIn URL.
 * @returns {{ email, phone } | null | 'RATE_LIMITED'}
 */
export async function enrichWithKaspr({ linkedinUrl, fullName }) {
  if (!linkedinUrl) {
    warn(`[KASPR] enrichWithKaspr called without linkedinUrl for ${fullName}`);
    return null;
  }

  const key = process.env.KASPR_API_KEY;
  if (!key) {
    warn('[KASPR] KASPR_API_KEY not set — skipping');
    return null;
  }

  // Lazy-load rate limits on first call
  if (!_rateLimits) await fetchRateLimits();

  if (!isWithinRateLimit()) {
    warn(`[KASPR] Minutely rate limit hit — pausing enrichment`);
    return 'RATE_LIMITED';
  }

  // KASPR accepts the full LinkedIn URL or just the slug
  const id = extractSlug(linkedinUrl) || linkedinUrl;

  try {
    _minuteCount++;
    const res = await fetch(KASPR_ENDPOINT, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${key}`,
        'accept-version': 'v2.0',
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      body: JSON.stringify({
        id,
        name: fullName || '',
        dataToGet: ['workEmail', 'directEmail'],
      }),
    });

    if (res.status === 429) {
      warn(`[KASPR] Rate limited (429) for ${fullName}`);
      return 'RATE_LIMITED';
    }

    if (res.status === 402) {
      warn(`[KASPR] Payment required (402) — credits exhausted for ${fullName}`);
      return null;
    }

    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`KASPR HTTP ${res.status}: ${body.substring(0, 100)}`);
    }

    const data = await res.json();

    // KASPR returns 200 with body {"message":"No credits left"} when credits exhausted
    if (data.message && !data.profile) {
      warn(`[KASPR] No credits left for ${fullName} — credits exhausted`);
      return null;
    }

    // Response is wrapped: { profile: { professionalEmails, starryProfessionalEmail, personalEmails, phones, starryPhone, ... } }
    const profile = data.profile || data;

    const email = extractEmail(profile);
    const phone = extractPhone(profile);

    info(`[KASPR] ${fullName} — email:${email ? 'found' : 'none'} phone:${phone ? 'found' : 'none'}`);
    return { email, phone };

  } catch (err) {
    error(`[KASPR] enrichWithKaspr failed for ${fullName}`, { err: err.message });
    return null;
  }
}

function extractEmail(profile) {
  // Prefer the "starry" (best-match) professional email, then array, then personal
  if (profile.starryProfessionalEmail) return profile.starryProfessionalEmail;
  if (Array.isArray(profile.professionalEmails) && profile.professionalEmails.length) return profile.professionalEmails[0];
  if (profile.starryPersonalEmail) return profile.starryPersonalEmail;
  if (Array.isArray(profile.personalEmails) && profile.personalEmails.length) return profile.personalEmails[0];
  // Legacy / flat field fallbacks
  return profile.workEmail || profile.directEmail || profile.email || profile.emailAddress || null;
}

function extractPhone(profile) {
  if (profile.starryPhone) return profile.starryPhone;
  if (Array.isArray(profile.phones) && profile.phones.length) return profile.phones[0];
  return profile.phone || profile.phoneNumber || null;
}
