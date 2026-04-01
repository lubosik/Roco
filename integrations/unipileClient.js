/**
 * integrations/unipileClient.js
 * Single source of truth for all Unipile API calls.
 * Covers: LinkedIn invites, DMs, replies, search, chat history, Gmail send/reply.
 */

function getDSN()        { const d = process.env.UNIPILE_DSN || 'https://api34.unipile.com:16411'; return d.startsWith('http') ? d : `https://${d}`; }
function getKey()        { return process.env.UNIPILE_API_KEY; }
function getLiAcct()     { return process.env.UNIPILE_LINKEDIN_ACCOUNT_ID; }
function getGmAcct()     { return process.env.UNIPILE_GMAIL_ACCOUNT_ID; }
function getOutlookAcct(){ return process.env.UNIPILE_OUTLOOK_ACCOUNT_ID; }

async function api(method, path, body, isFormData = false) {
  const url = `${getDSN()}/api/v1${path}`;
  const headers = { 'X-API-KEY': getKey(), accept: 'application/json' };

  let fetchBody;
  if (body && isFormData) {
    const fd = new FormData();
    Object.entries(body).forEach(([k, v]) => {
      if (v !== undefined && v !== null) fd.append(k, String(v));
    });
    fetchBody = fd;
  } else if (body) {
    headers['content-type'] = 'application/json';
    fetchBody = JSON.stringify(body);
  }

  const res = await fetch(url, { method, headers, body: fetchBody });
  if (!res.ok) {
    const err = await res.text().catch(() => res.statusText);
    throw new Error(`Unipile ${method} ${path} → ${res.status}: ${err.substring(0, 300)}`);
  }
  return res.json();
}

// ── LINKEDIN INVITATIONS ──────────────────────────────────────────────

/**
 * Send a LinkedIn connection request.
 * providerId = LinkedIn member URN  e.g. ACoAAAcDMMQBODyLw...
 * OR pass linkedinUrl and we resolve the slug to use as identifier.
 */
export async function sendLinkedInInvite({ providerId, linkedinUrl, message }) {
  const id = providerId || extractSlug(linkedinUrl);
  if (!id) throw new Error('sendLinkedInInvite: need providerId or linkedinUrl');

  const result = await api('POST', '/users/invite', {
    account_id: getLiAcct(),
    provider_id: id,
    message: (message || '').substring(0, 300),
  });
  return { success: true, invitationId: result.invitation_id || result.id };
}

// ── LINKEDIN MESSAGES ─────────────────────────────────────────────────

/**
 * Start a new chat and send first DM.
 * attendeeProviderId = LinkedIn member URN (must be a 1st-degree connection).
 */
export async function sendLinkedInDM({ attendeeProviderId, message }) {
  const result = await api('POST', '/chats', {
    account_id: getLiAcct(),
    attendees_ids: attendeeProviderId,
    text: message,
    'linkedin[api]': 'classic',
  }, true);
  return { success: true, chatId: result.chat_id || result.id, messageId: result.message_id };
}

/**
 * Send a message in an existing chat.
 */
export async function sendLinkedInReply({ chatId, message }) {
  const result = await api('POST', `/chats/${chatId}/messages`, { text: message }, true);
  return { success: true, messageId: result.id };
}

/**
 * Get chat message history (oldest first).
 */
export async function getChatHistory({ chatId, limit = 20 }) {
  const result = await api('GET', `/chats/${chatId}/messages?limit=${limit}`);
  const messages = result.items || [];
  return messages.reverse().map(m => ({
    id: m.id,
    text: m.text || '',
    senderProviderId: m.sender?.attendee_provider_id,
    senderName: m.sender?.attendee_name,
    timestamp: m.timestamp,
    isFromUs: m.sender?.attendee_provider_id === m.account_info?.user_id,
  }));
}

/**
 * LinkedIn People Search — up to 50 results per call.
 */
export async function searchLinkedInPeople({ keywords, limit = 50 }) {
  const result = await api('POST',
    `/linkedin/search?account_id=${getLiAcct()}&limit=${Math.min(limit, 50)}`,
    { api: 'classic', category: 'people', keywords }
  );
  return result.items || [];
}

// ── GMAIL ─────────────────────────────────────────────────────────────

/**
 * Send a new email via Gmail through Unipile.
 */
export async function sendEmail({ to, toName, subject, body, fromName }) {
  const toPayload = JSON.stringify([{ display_name: toName || '', identifier: to }]);
  const result = await api('POST', '/emails', {
    account_id: getGmAcct(),
    subject,
    body: formatEmailHtml(body),
    to: toPayload,
    from: JSON.stringify({ display_name: fromName || 'Dom' }),
  }, true);
  return { success: true, emailId: result.id, threadId: result.thread_id };
}

/**
 * Reply on an existing Gmail thread.
 */
export async function sendEmailReply({ to, toName, subject, body, replyToProviderId, accountId }) {
  // accountId can be explicitly passed so Outlook replies go via the Outlook account
  const resolvedAccount = accountId || getGmAcct();
  const toPayload = JSON.stringify([{ display_name: toName || '', identifier: to }]);
  const result = await api('POST', '/emails', {
    account_id: resolvedAccount,
    subject: subject?.startsWith('Re:') ? subject : `Re: ${subject}`,
    body: formatEmailHtml(body),
    to: toPayload,
    reply_to: replyToProviderId,
  }, true);
  return { success: true, emailId: result.id, threadId: result.thread_id };
}

// ── ACCOUNT STATUS ────────────────────────────────────────────────────

export async function checkUnipileStatus() {
  try {
    const result = await api('GET', '/accounts');
    const accounts = result.items || [];
    const li = accounts.find(a => a.id === getLiAcct());
    const gm = accounts.find(a => a.id === getGmAcct());
    return {
      linkedin: li?.connection_status === 'OK' ? 'connected' : 'disconnected',
      gmail: gm?.connection_status === 'OK' ? 'connected' : 'disconnected',
    };
  } catch {
    return { linkedin: 'error', gmail: 'error' };
  }
}

// ── HELPERS ───────────────────────────────────────────────────────────

export function extractSlug(url) {
  const match = url?.match(/linkedin\.com\/in\/([^/?#\s]+)/i);
  return match ? match[1].replace(/\/$/, '') : null;
}

function formatEmailHtml(text) {
  if (!text) return '';
  return text.split('\n\n')
    .map(p => `<p style="font-family:Arial,sans-serif;font-size:14px;line-height:1.6;color:#333">${p.replace(/\n/g, '<br>')}</p>`)
    .join('');
}

// ── LINKEDIN PROFILE RETRIEVAL ────────────────────────────────────────

/**
 * Retrieve a LinkedIn profile by URL to get richer data for message construction.
 */
export async function retrieveLinkedInProfile(linkedinUrl) {
  if (!linkedinUrl) return null;

  const slug = extractSlug(linkedinUrl);
  if (!slug) return null;

  try {
    const result = await api('GET', `/linkedin/profiles/${slug}?account_id=${getLiAcct()}`);
    return {
      headline: result.headline || null,
      summary: result.summary || null,
      current_company: result.positions?.[0]?.company_name || null,
      current_title: result.positions?.[0]?.title || null,
      location: result.location || null,
      connections: result.connections_count || null,
    };
  } catch (err) {
    // Profile retrieval is best-effort — don't throw
    return null;
  }
}

/**
 * Retrieve full email details by ID.
 */
export async function retrieveEmail(emailId, accountId) {
  const acct = accountId || getGmAcct();
  return api('GET', `/emails/${emailId}?account_id=${acct}`);
}
