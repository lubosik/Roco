// core/sendingWindow.js
// Sending window helpers — all respect deal-level overrides, defaulting to Dom's preferred times.
//
// Defaults (Dom's preferences, EST):
//   Email + LinkedIn DMs : 6am–8am  OR  8pm–11pm  (active deal days)
//   LinkedIn connections  : anytime
//   Research + enrichment: anytime
//
// Deal columns used (all stored as "HH:MM" strings or null):
//   send_from / send_until          — morning email/DM window start/end
//   li_dm_from / li_dm_until        — evening email/DM window start/end
//   li_connect_from / li_connect_until — LinkedIn connection window (null = anytime)
//   timezone                        — defaults to 'America/New_York'

const DEFAULTS = {
  morningStart:  6,
  morningEnd:    8,
  eveningStart:  20,
  eveningEnd:    23,
  timezone:      'America/New_York',
};

const DEFAULT_ACTIVE_DAYS = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday'];
const SHORT_TO_FULL_DAY = {
  mon: 'monday',
  tue: 'tuesday',
  wed: 'wednesday',
  thu: 'thursday',
  fri: 'friday',
  sat: 'saturday',
  sun: 'sunday',
};

function resolveActiveDays(deal) {
  if (deal?.active_days) {
    const days = String(deal.active_days)
      .split(',')
      .map(day => SHORT_TO_FULL_DAY[String(day || '').trim().toLowerCase()] || String(day || '').trim().toLowerCase())
      .filter(Boolean);
    if (days.length) return days;
  }

  if (Array.isArray(deal?.sending_days) && deal.sending_days.length) {
    return deal.sending_days
      .map(day => String(day || '').trim().toLowerCase())
      .filter(Boolean);
  }

  return DEFAULT_ACTIVE_DAYS;
}

/** Parse "HH:MM" or integer hour → integer hour. Returns null if value is null/undefined. */
function parseHour(val) {
  if (val == null) return null;
  if (typeof val === 'number') return val;
  const s = String(val).trim();
  if (!s) return null;
  return parseInt(s.split(':')[0], 10);
}

/** Get current hour in target timezone (0–23). */
function currentHourIn(timezone) {
  const now = new Date();
  return parseInt(
    new Intl.DateTimeFormat('en-US', { timeZone: timezone, hour: 'numeric', hour12: false }).format(now),
    10
  );
}

/** Return the lowercase weekday name in the target timezone. */
function dayNameIn(timezone) {
  const now = new Date();
  return new Intl.DateTimeFormat('en-US', { timeZone: timezone, weekday: 'long' }).format(now).toLowerCase();
}

/**
 * Is now within the email + LinkedIn DM sending window?
 *
 * Two windows (on the deal's active days):
 *   Morning : send_from  → send_until   (default 6–8am)
 *   Evening : li_dm_from → li_dm_until  (default 8–11pm)
 *
 * Pass a deal object to use deal-level overrides.
 */
export function isWithinEmailWindow(deal) {
  const tz = deal?.timezone || DEFAULTS.timezone;
  const hour = currentHourIn(tz);
  const dayName = dayNameIn(tz);
  const activeDays = resolveActiveDays(deal);

  const morningStart = parseHour(deal?.send_from)    ?? DEFAULTS.morningStart;
  const morningEnd   = parseHour(deal?.send_until)   ?? DEFAULTS.morningEnd;
  const eveningStart = parseHour(deal?.li_dm_from)   ?? DEFAULTS.eveningStart;
  const eveningEnd   = parseHour(deal?.li_dm_until)  ?? DEFAULTS.eveningEnd;

  const inMorning = hour >= morningStart && hour < morningEnd;
  const inEvening = hour >= eveningStart && hour < eveningEnd;
  const within = activeDays.includes(dayName) && (inMorning || inEvening);

  console.log(`[SENDING WINDOW] ${hour}:xx ${tz} day=${dayName} → email ${within ? 'WITHIN' : 'OUTSIDE'} (${morningStart}-${morningEnd} or ${eveningStart}-${eveningEnd})`);
  return within;
}

/**
 * Is now within the LinkedIn connection request window?
 *
 * Default: anytime (returns true always).
 * If deal has li_connect_from/until set, restricts to that range (any day).
 */
export function isWithinConnectionWindow(deal) {
  const from  = parseHour(deal?.li_connect_from);
  const until = parseHour(deal?.li_connect_until);
  if (from == null || until == null) return true; // anytime
  const tz   = deal?.timezone || DEFAULTS.timezone;
  const hour = currentHourIn(tz);
  return hour >= from && hour < until;
}

/**
 * Is now within the research/enrichment window? Always true.
 */
export function isWithinResearchWindow() {
  return true;
}

/**
 * Human-readable description of when the next email window opens.
 */
export function describeNextEmailWindow(deal) {
  const tz = deal?.timezone || DEFAULTS.timezone;
  const hour = currentHourIn(tz);
  const activeDays = resolveActiveDays(deal);

  const morningStart = parseHour(deal?.send_from)  ?? DEFAULTS.morningStart;
  const morningEnd   = parseHour(deal?.send_until) ?? DEFAULTS.morningEnd;
  const eveningStart = parseHour(deal?.li_dm_from) ?? DEFAULTS.eveningStart;
  const eveningEnd   = parseHour(deal?.li_dm_until)?? DEFAULTS.eveningEnd;
  const tzLabel      = tz === 'America/New_York' ? 'EST' : tz;
  const now = new Date();

  for (let daysAhead = 0; daysAhead <= 7; daysAhead++) {
    const candidate = new Date(now.getTime() + daysAhead * 24 * 60 * 60 * 1000);
    const dayName = new Intl.DateTimeFormat('en-US', { timeZone: tz, weekday: 'long' }).format(candidate).toLowerCase();
    if (!activeDays.includes(dayName)) continue;

    if (daysAhead === 0) {
      if (hour < morningStart) return `today ${morningStart}am ${tzLabel}`;
      if (hour < morningEnd)   return `now (morning window open, ${morningStart}-${morningEnd}am)`;
      if (hour < eveningStart) return `tonight ${eveningStart === 20 ? '8pm' : eveningStart + ':00'} ${tzLabel}`;
      if (hour < eveningEnd)   return `now (evening window open, ${eveningStart}-${eveningEnd})`;
      continue;
    }

    const dayLabel = new Intl.DateTimeFormat('en-US', { timeZone: tz, weekday: 'long' }).format(candidate);
    if (daysAhead === 1) return `tomorrow ${morningStart}am ${tzLabel}`;
    return `${dayLabel} ${morningStart}am ${tzLabel}`;
  }

  return `${morningStart}am ${tzLabel}`;
}
