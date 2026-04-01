// core/sendingWindow.js
// Sending window helpers — all respect deal-level overrides, defaulting to Dom's preferred times.
//
// Defaults (Dom's preferences, EST):
//   Email + LinkedIn DMs : 6am–8am  OR  8pm–11pm  (weekdays only)
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

/** Is it currently a weekday in target timezone? */
function isWeekdayIn(timezone) {
  const now = new Date();
  const tzDate = new Date(now.toLocaleString('en-US', { timeZone: timezone }));
  const day = tzDate.getDay(); // 0=Sun, 6=Sat
  return day >= 1 && day <= 5;
}

/**
 * Is now within the email + LinkedIn DM sending window?
 *
 * Two windows (weekdays only):
 *   Morning : send_from  → send_until   (default 6–8am)
 *   Evening : li_dm_from → li_dm_until  (default 8–11pm)
 *
 * Pass a deal object to use deal-level overrides.
 */
export function isWithinEmailWindow(deal) {
  const tz = deal?.timezone || DEFAULTS.timezone;
  const hour = currentHourIn(tz);
  const weekday = isWeekdayIn(tz);

  const morningStart = parseHour(deal?.send_from)    ?? DEFAULTS.morningStart;
  const morningEnd   = parseHour(deal?.send_until)   ?? DEFAULTS.morningEnd;
  const eveningStart = parseHour(deal?.li_dm_from)   ?? DEFAULTS.eveningStart;
  const eveningEnd   = parseHour(deal?.li_dm_until)  ?? DEFAULTS.eveningEnd;

  const inMorning = hour >= morningStart && hour < morningEnd;
  const inEvening = hour >= eveningStart && hour < eveningEnd;
  const within = weekday && (inMorning || inEvening);

  console.log(`[SENDING WINDOW] ${hour}:xx ${tz} day=${new Date(new Date().toLocaleString('en-US', { timeZone: tz })).getDay()} → email ${within ? 'WITHIN' : 'OUTSIDE'} (${morningStart}-${morningEnd} or ${eveningStart}-${eveningEnd})`);
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
  const weekday = isWeekdayIn(tz);

  const morningStart = parseHour(deal?.send_from)  ?? DEFAULTS.morningStart;
  const morningEnd   = parseHour(deal?.send_until) ?? DEFAULTS.morningEnd;
  const eveningStart = parseHour(deal?.li_dm_from) ?? DEFAULTS.eveningStart;
  const eveningEnd   = parseHour(deal?.li_dm_until)?? DEFAULTS.eveningEnd;
  const tzLabel      = tz === 'America/New_York' ? 'EST' : tz;

  if (!weekday) return `Monday ${morningStart}am ${tzLabel}`;
  if (hour < morningStart) return `today ${morningStart}am ${tzLabel}`;
  if (hour < morningEnd)   return `now (morning window open, ${morningStart}–${morningEnd}am)`;
  if (hour < eveningStart) return `tonight ${eveningStart === 20 ? '8pm' : eveningStart + ':00'} ${tzLabel}`;
  if (hour < eveningEnd)   return `now (evening window open, ${eveningStart}–${eveningEnd})`;
  return `tomorrow ${morningStart}am ${tzLabel}`;
}
