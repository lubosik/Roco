/**
 * core/scheduleChecker.js — Sending window checker
 *
 * Gates email/LinkedIn sends to configured working hours.
 * Research, enrichment, and scoring run any time.
 * Only the SEND step is gated.
 */

import { DateTime } from 'luxon';

const DAY_NAMES = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'];

/**
 * Check if current time is within the deal's configured sending window.
 * @param {object} deal - Deal record from Supabase (with schedule fields)
 * @returns {boolean}
 */
// Resolve schedule fields — new column names take priority over legacy ones
function resolveSchedule(deal) {
  const tz = deal.timezone || deal.sending_timezone || 'Europe/London';
  const startStr = deal.send_from || (deal.sending_start ? deal.sending_start.slice(0, 5) : '06:00');
  const endStr   = deal.send_until || (deal.sending_end   ? deal.sending_end.slice(0, 5)   : '18:00');

  // active_days is a comma-separated string ('Mon,Tue,Wed,Thu,Fri')
  // sending_days is an array of full lowercase names (['monday','tuesday',...])
  let sendingDays;
  if (deal.active_days) {
    const shortToFull = { mon:'monday', tue:'tuesday', wed:'wednesday', thu:'thursday', fri:'friday', sat:'saturday', sun:'sunday' };
    sendingDays = deal.active_days.split(',').map(d => shortToFull[d.trim().toLowerCase()] || d.trim().toLowerCase());
  } else {
    sendingDays = deal.sending_days || ['monday','tuesday','wednesday','thursday','friday'];
  }

  return { tz, startStr, endStr, sendingDays };
}

export function isWithinSendingWindow(deal) {
  if (!deal) return true; // No deal = no restriction

  // Paused deals never send
  if (deal.paused === true || deal.status === 'PAUSED') return false;

  const { tz, startStr, endStr, sendingDays } = resolveSchedule(deal);
  const now = DateTime.now().setZone(tz);
  const dayName = now.weekdayLong.toLowerCase();

  // Check sending days
  if (!sendingDays.includes(dayName)) return false;

  // Check if there's a global pause active
  if (deal.outreach_paused_until) {
    const pausedUntil = DateTime.fromISO(deal.outreach_paused_until).setZone(tz);
    if (now < pausedUntil) return false;
  }

  // Check time window
  const [startH, startM] = startStr.split(':').map(Number);
  const [endH, endM] = endStr.split(':').map(Number);

  const startMinutes = startH * 60 + startM;
  const endMinutes = endH * 60 + endM;
  const nowMinutes = now.hour * 60 + now.minute;

  return nowMinutes >= startMinutes && nowMinutes < endMinutes;
}


/**
 * Check if a global outreach pause is active (from sessions table).
 * @param {string|null} pausedUntil - ISO string from session state
 * @returns {boolean} true if currently paused
 */
export function isGloballyPaused(pausedUntil) {
  if (!pausedUntil) return false;
  return DateTime.now() < DateTime.fromISO(pausedUntil);
}

/**
 * Get a human-readable description of when the next sending window opens.
 * @param {object} deal
 * @returns {string}
 */
export function getNextWindowOpen(deal) {
  if (!deal) return 'Now';

  const { tz, startStr, sendingDays } = resolveSchedule(deal);
  const now = DateTime.now().setZone(tz);

  // Check today first, then upcoming days
  for (let daysAhead = 0; daysAhead <= 7; daysAhead++) {
    const candidate = now.plus({ days: daysAhead });
    const dayName = candidate.weekdayLong.toLowerCase();

    if (!sendingDays.includes(dayName)) continue;

    const [startH, startM] = startStr.split(':').map(Number);
    const windowStart = candidate.set({ hour: startH, minute: startM, second: 0 });

    if (windowStart > now) {
      if (daysAhead === 0) return `Today at ${startStr} ${tz}`;
      if (daysAhead === 1) return `Tomorrow at ${startStr} ${tz}`;
      return `${candidate.weekdayLong} at ${startStr} ${tz}`;
    }
  }

  return `${startStr} ${tz}`;
}

/**
 * Get human-readable next open time for a specific channel.
 * Respects per-channel windows (li_dm_from/until for linkedin_dm, etc.)
 * @param {object} deal
 * @param {'email'|'linkedin_dm'|'linkedin_invite'} channel
 * @returns {string}
 */
export function getNextWindowOpenForChannel(deal, channel) {
  if (!deal) return 'Now';

  const { tz, sendingDays } = resolveSchedule(deal);
  const now = DateTime.now().setZone(tz);
  const tzLabel = tz === 'America/New_York' ? 'EST' : tz;

  let startStr;
  if (channel === 'linkedin_dm') {
    startStr = deal.li_dm_from || null;
    if (!startStr || !deal.li_dm_until) return 'Now (any time)';
  } else if (channel === 'linkedin_invite') {
    startStr = deal.li_connect_from || null;
    if (!startStr) return 'Now (any time)';
  } else {
    // email — use email window
    startStr = deal.send_from || '06:00';
  }

  // For linkedin_dm / linkedin_invite: not day-restricted by default
  const isDayRestricted = channel === 'email';

  for (let daysAhead = 0; daysAhead <= 7; daysAhead++) {
    const candidate = now.plus({ days: daysAhead });
    const dayName = candidate.weekdayLong.toLowerCase();

    if (isDayRestricted && !sendingDays.includes(dayName)) continue;

    const [startH, startM] = startStr.split(':').map(Number);
    const windowStart = candidate.set({ hour: startH, minute: startM, second: 0 });

    if (windowStart > now) {
      if (daysAhead === 0) return `today at ${startStr} ${tzLabel}`;
      if (daysAhead === 1) return `tomorrow at ${startStr} ${tzLabel}`;
      return `${candidate.weekdayLong} at ${startStr} ${tzLabel}`;
    }
  }

  return `${startStr} ${tzLabel}`;
}

/**
 * Get detailed window status for a deal (used by dashboard).
 * @param {object} deal
 * @param {string|null} globalPausedUntil
 * @returns {object} { isOpen, status, nextOpen, timeUntilOpen, timeUntilClose }
 */
export function getWindowStatus(deal, globalPausedUntil = null) {
  const { tz, endStr: dealEndStr } = deal ? resolveSchedule(deal) : { tz: 'Europe/London', endStr: '18:00' };
  const now = DateTime.now().setZone(tz);

  // Global pause check
  if (globalPausedUntil && isGloballyPaused(globalPausedUntil)) {
    const until = DateTime.fromISO(globalPausedUntil).setZone(tz);
    const diff = until.diff(now, ['days', 'hours', 'minutes']);
    const parts = [];
    if (diff.days > 0) parts.push(`${Math.floor(diff.days)}d`);
    if (diff.hours > 0) parts.push(`${Math.floor(diff.hours)}h`);
    parts.push(`${Math.floor(diff.minutes)}m`);
    return {
      isOpen: false,
      status: 'GLOBALLY_PAUSED',
      label: `PAUSED — resumes in ${parts.join(' ')}`,
      nextOpen: until.toFormat('EEE d MMM HH:mm'),
    };
  }

  if (!deal) {
    return { isOpen: true, status: 'OPEN', label: 'SENDING OPEN', nextOpen: null };
  }

  const isOpen = isWithinSendingWindow(deal);

  if (isOpen) {
    // Calculate time until window closes
    const endStr = dealEndStr;
    const [endH, endM] = endStr.split(':').map(Number);
    const windowEnd = now.set({ hour: endH, minute: endM, second: 0 });
    const diff = windowEnd.diff(now, ['hours', 'minutes']);
    const timeUntilClose = `${Math.floor(diff.hours)}h ${Math.floor(diff.minutes)}m`;

    return {
      isOpen: true,
      status: 'OPEN',
      label: `SENDING OPEN — closes in ${timeUntilClose}`,
      timeUntilClose,
    };
  } else {
    const nextOpen = getNextWindowOpen(deal);
    return {
      isOpen: false,
      status: 'CLOSED',
      label: `WINDOW CLOSED — opens ${nextOpen}`,
      nextOpen,
    };
  }
}

/**
 * Check if current time is within the per-channel sending window for a deal.
 * Reads flat columns: li_connect_from/until, li_dm_from/until (email uses send_from/until).
 * Falls back to the unified send_from/send_until window if per-channel columns not set.
 * @param {object} deal
 * @param {'email'|'linkedin_invite'|'linkedin_dm'} channel
 */
export function isWithinChannelWindow(deal, channel) {
  if (!deal) return true;
  if (deal.paused === true || deal.status === 'PAUSED') return false;

  const tz = deal.timezone || 'Europe/London';
  const now = DateTime.now().setZone(tz);
  const nowMinutes = now.hour * 60 + now.minute;
  const { sendingDays } = resolveSchedule(deal);
  const dayName = now.weekdayLong.toLowerCase();

  function minutesInRange(startStr, endStr) {
    const [startH, startM] = startStr.split(':').map(Number);
    const [endH, endM]     = endStr.split(':').map(Number);
    return nowMinutes >= (startH * 60 + startM) && nowMinutes < (endH * 60 + endM);
  }

  if (channel === 'linkedin_invite') {
    // No window configured → anytime (connection requests are not day-restricted)
    if (!deal.li_connect_from || !deal.li_connect_until) return true;
    // Window configured → honour it, no day restriction
    return minutesInRange(deal.li_connect_from, deal.li_connect_until);
  }

  if (channel === 'linkedin_dm') {
    // No window configured → always open (send immediately after approval)
    if (!deal.li_dm_from || !deal.li_dm_until) return true;
    return minutesInRange(deal.li_dm_from, deal.li_dm_until);
  }

  // email — full window including active days check
  return isWithinSendingWindow(deal);
}

/**
 * Get the sending window visualizer data for the dashboard.
 * Returns an array of { day, active, startPct, widthPct } objects.
 */
export function getWindowVisualization(deal) {
  const { sendingDays, startStr, endStr } = deal ? resolveSchedule(deal) : {
    sendingDays: ['monday','tuesday','wednesday','thursday','friday'],
    startStr: '08:00', endStr: '18:00',
  };

  const [startH] = startStr.split(':').map(Number);
  const [endH] = endStr.split(':').map(Number);

  const startPct = (startH / 24) * 100;
  const widthPct = ((endH - startH) / 24) * 100;

  return DAY_NAMES.map(day => ({
    day: day.slice(0, 3).toUpperCase(),
    active: sendingDays.includes(day),
    startPct,
    widthPct,
    startTime: startStr,
    endTime: endStr,
  }));
}
