/**
 * core/unipileHealthCheck.js
 *
 * Checks that all connected Unipile accounts (LinkedIn, Gmail, Outlook) are
 * healthy each orchestrator cycle. Fires a Telegram alert the moment any
 * account returns expired/invalid credentials — so the issue is caught
 * immediately rather than discovered when a DM or email silently fails.
 *
 * Two credential layers to understand:
 *   1. UNIPILE_API_KEY  — authenticates to Unipile's API. Expires in ~10 years.
 *   2. Connected account sessions (LinkedIn/Gmail/Outlook stored in Unipile) —
 *      these expire when the provider invalidates the session. This checker
 *      catches that case.
 */

const ALERT_COOLDOWN_MS = 4 * 60 * 60 * 1000; // max one alert per account per 4 hours
const _lastAlerted = new Map();

function canAlert(accountId) {
  const last = _lastAlerted.get(accountId) || 0;
  if (Date.now() - last < ALERT_COOLDOWN_MS) return false;
  _lastAlerted.set(accountId, Date.now());
  return true;
}

async function fetchAccountStatus(dsn, apiKey, accountId) {
  const url = `${dsn}/api/v1/accounts/${accountId}`;
  try {
    const res = await fetch(url, {
      headers: { 'X-API-KEY': apiKey, accept: 'application/json' },
    });
    if (res.status === 401) return { id: accountId, healthy: false, reason: 'API key invalid (401)' };
    if (res.status === 404) return { id: accountId, healthy: false, reason: 'Account not found (404)' };
    if (!res.ok) return { id: accountId, healthy: false, reason: `HTTP ${res.status}` };
    const body = await res.json();
    const status = String(body?.status || body?.connection_state || '').toUpperCase();
    const healthy = !status || status === 'OK' || status === 'CONNECTED' || status === 'SYNCING';
    return { id: accountId, healthy, reason: healthy ? null : `Account status: ${status}`, raw: status };
  } catch (err) {
    return { id: accountId, healthy: false, reason: `Network error: ${err.message}` };
  }
}

export async function checkUnipileAccountHealth() {
  const dsn    = process.env.UNIPILE_DSN    || 'https://api34.unipile.com:16411';
  const apiKey = process.env.UNIPILE_API_KEY;
  if (!apiKey) return;

  const accounts = [
    { id: process.env.UNIPILE_LINKEDIN_ACCOUNT_ID, label: 'LinkedIn' },
    { id: process.env.UNIPILE_GMAIL_ACCOUNT_ID,    label: 'Gmail' },
    { id: process.env.UNIPILE_OUTLOOK_ACCOUNT_ID,  label: 'Outlook' },
  ].filter(a => a.id);

  if (!accounts.length) return;

  const results = await Promise.all(
    accounts.map(a => fetchAccountStatus(dsn, apiKey, a.id).then(r => ({ ...r, label: a.label })))
  );

  const broken = results.filter(r => !r.healthy);
  if (!broken.length) return;

  for (const acct of broken) {
    if (!canAlert(acct.id)) continue;
    try {
      const { sendTelegram } = await import('../approval/telegramBot.js');
      await sendTelegram(
        `⚠️ *Unipile account issue detected*\n\n` +
        `*${acct.label}* (${acct.id})\n` +
        `Reason: ${acct.reason}\n\n` +
        `If this is LinkedIn: ask the client to reconnect their LinkedIn profile in Unipile, then update UNIPILE_LINKEDIN_ACCOUNT_ID in the Railway environment variables.\n` +
        `If this is Gmail/Outlook: reconnect the email account in the Unipile dashboard.`
      );
    } catch {}
  }
}
