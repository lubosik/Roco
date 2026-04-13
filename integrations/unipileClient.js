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
function getEmailAcct()  { return getOutlookAcct() || getGmAcct(); }

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
    const error = new Error(`Unipile ${method} ${path} → ${res.status}: ${err.substring(0, 300)}`);
    error.status = res.status;
    error.path = path;
    error.method = method;
    error.responseText = err.substring(0, 1000);
    throw error;
  }
  return res.json();
}

// ── LINKEDIN INVITATIONS ──────────────────────────────────────────────

function isValidLinkedInProviderId(value) {
  return /^(ACo|ACw|AE)[A-Za-z0-9_-]+$/.test(String(value || '').trim());
}

function getLinkedInIdentifier(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (isValidLinkedInProviderId(raw)) return raw;
  const slug = extractSlug(raw);
  return slug || raw;
}

/**
 * Send a LinkedIn connection request.
 * Unipile requires the real LinkedIn provider_id (ACo / ACw / AE...), not a public profile slug.
 */
export async function sendLinkedInInvite({ providerId, message, userEmail }) {
  const id = String(providerId || '').trim();
  if (!isValidLinkedInProviderId(id)) {
    throw new Error(`sendLinkedInInvite: invalid LinkedIn providerId "${id || 'missing'}"`);
  }

  const result = await api('POST', '/users/invite', {
    account_id: getLiAcct(),
    provider_id: id,
    user_email: userEmail || undefined,
    message: (message || '').substring(0, 300),
  });
  return {
    success: true,
    accountId: getLiAcct(),
    providerId: id,
    invitationId: result.invitation_id || result.id || null,
    usage: result.usage || null,
    raw: result,
  };
}

export async function resolveLinkedInProfile(identifier) {
  const id = getLinkedInIdentifier(identifier);
  if (!id) return null;

  const result = await api('GET', `/users/${encodeURIComponent(id)}?account_id=${encodeURIComponent(getLiAcct())}`);
  const profile = result?.profile || result?.user || result || {};
  const providerId = profile.provider_id || profile.id || profile.providerId || null;
  const canonicalUrl = canonicalizeLinkedInProfileUrl(profile.profile_url || profile.linkedin_url || identifier);
  const publicId = profile.public_id || profile.provider_public_id || profile.username || extractSlug(canonicalUrl || identifier);

  return {
    providerId: isValidLinkedInProviderId(providerId) ? providerId : null,
    publicId: publicId || null,
    name: profile.name || profile.full_name || [profile.first_name, profile.last_name].filter(Boolean).join(' ').trim() || null,
    headline: profile.headline || null,
    linkedinUrl: canonicalUrl,
    raw: profile,
  };
}

export async function listSentInvitations(limit = 100) {
  const result = await api('GET', `/users/invite/sent?account_id=${encodeURIComponent(getLiAcct())}&limit=${Math.min(Math.max(limit, 1), 100)}`);
  return result?.items || [];
}

// ── LINKEDIN MESSAGES ─────────────────────────────────────────────────

/**
 * Start a new chat and send first DM.
 * attendeeProviderId = LinkedIn member URN (must be a 1st-degree connection).
 */
export async function sendLinkedInDM({ attendeeProviderId, message }) {
  const id = String(attendeeProviderId || '').trim();
  if (!isValidLinkedInProviderId(id)) {
    throw new Error(`sendLinkedInDM: invalid attendeeProviderId "${id || 'missing'}"`);
  }
  const result = await api('POST', '/chats', {
    account_id: getLiAcct(),
    attendees_ids: [id],
    text: message,
  });
  return {
    success: true,
    accountId: getLiAcct(),
    attendeeProviderId: id,
    chatId: result.chat_id || result.id || null,
    messageId: result.message_id || null,
    raw: result,
  };
}

/**
 * Send a message in an existing chat.
 */
export async function sendLinkedInReply({ chatId, message, quoteId = null }) {
  const result = await api('POST', `/chats/${chatId}/messages`, {
    account_id: getLiAcct(),
    text: message,
    quote_id: quoteId || undefined,
  });
  return { success: true, accountId: getLiAcct(), chatId, messageId: result.message_id || result.id || null, raw: result };
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
export async function searchLinkedInPeople({ keywords, companyIds = [], networkDistance = [], limit = 50 }) {
  const body = { api: 'classic', category: 'people' };
  if (keywords) body.keywords = keywords;
  if (Array.isArray(companyIds) && companyIds.length) body.company = companyIds.map(String);
  if (Array.isArray(networkDistance) && networkDistance.length) body.network_distance = networkDistance;
  const result = await api(
    'POST',
    `/linkedin/search?account_id=${getLiAcct()}&limit=${Math.min(limit, 50)}`,
    body,
  );
  return result.items || [];
}

/**
 * LinkedIn People Search via Sales Navigator when available.
 * Falls back at call-site if the account is not subscribed for this API.
 */
export async function searchLinkedInPeopleSalesNavigator({ firstName, lastName, keywords, companyIds = [], networkDistance = [], limit = 10 }) {
  const body = { api: 'sales_navigator', category: 'people' };
  if (firstName) body.first_name = firstName;
  if (lastName) body.last_name = lastName;
  if (keywords) body.keywords = keywords;
  if (Array.isArray(companyIds) && companyIds.length) body.company = { include: companyIds.map(String) };
  if (Array.isArray(networkDistance) && networkDistance.length) body.network_distance = networkDistance;

  const result = await api(
    'POST',
    `/linkedin/search?account_id=${getLiAcct()}&limit=${Math.min(Math.max(limit, 1), 10)}`,
    body,
  );
  return result.items || [];
}

export async function getLinkedInSearchParameters({ type, keywords, service = 'CLASSIC', limit = 10 }) {
  const query = new URLSearchParams({
    account_id: getLiAcct(),
    type,
    service,
    limit: String(Math.min(Math.max(limit, 1), 100)),
  });
  if (keywords) query.set('keywords', keywords);
  const result = await api('GET', `/linkedin/search/parameters?${query.toString()}`);
  return result.items || [];
}

// ── GMAIL ─────────────────────────────────────────────────────────────

/**
 * Send a new email via Unipile using the explicitly selected mailbox when provided.
 */
export async function sendEmail({ to, toName, subject, body, fromName, accountId, trackingLabel = null, trackOpens = true, trackLinks = true }) {
  const resolvedAccount = accountId || getEmailAcct();
  const payload = {
    account_id: resolvedAccount,
    subject,
    body: formatEmailHtml(body),
    to: [{ display_name: toName || '', identifier: to }],
    from: { display_name: fromName || 'Dom' },
  };
  if (trackOpens || trackLinks) {
    payload.tracking_options = {
      opens: !!trackOpens,
      links: !!trackLinks,
      label: trackingLabel || undefined,
    };
  }
  const result = await api('POST', '/emails', payload);
  return {
    success: true,
    accountId: resolvedAccount,
    emailId: result.id || result.provider_id || null,
    providerId: result.provider_id || null,
    threadId: result.thread_id || null,
    trackingLabel: trackingLabel || null,
    to,
    raw: result,
  };
}

/**
 * Reply on an existing Gmail thread.
 */
export async function sendEmailReply({ to, toName, subject, body, replyToProviderId, accountId, trackingLabel = null, trackOpens = true, trackLinks = true }) {
  // accountId can be explicitly passed so replies stay on the same mailbox the inbound came from.
  const resolvedAccount = accountId || getEmailAcct();
  const payload = {
    account_id: resolvedAccount,
    subject: subject?.startsWith('Re:') ? subject : `Re: ${subject}`,
    body: formatEmailHtml(body),
    to: [{ display_name: toName || '', identifier: to }],
    reply_to: replyToProviderId,
  };
  if (trackOpens || trackLinks) {
    payload.tracking_options = {
      opens: !!trackOpens,
      links: !!trackLinks,
      label: trackingLabel || undefined,
    };
  }
  const result = await api('POST', '/emails', payload);
  return {
    success: true,
    accountId: resolvedAccount,
    emailId: result.id || result.provider_id || null,
    providerId: result.provider_id || null,
    threadId: result.thread_id || null,
    trackingLabel: trackingLabel || null,
    to,
    raw: result,
  };
}

// ── ACCOUNT STATUS ────────────────────────────────────────────────────

export async function checkUnipileStatus() {
  try {
    const result = await api('GET', '/accounts');
    const accounts = result.items || [];
    const li = accounts.find(a => a.id === getLiAcct());
    const gm = accounts.find(a => a.id === getGmAcct());
    const outlook = accounts.find(a => a.id === getOutlookAcct());
    return {
      linkedin: li?.connection_status === 'OK' ? 'connected' : 'disconnected',
      gmail: gm?.connection_status === 'OK' ? 'connected' : 'disconnected',
      outlook: outlook?.connection_status === 'OK' ? 'connected' : 'disconnected',
      email_default: getOutlookAcct() ? 'outlook' : 'gmail',
    };
  } catch {
    return { linkedin: 'error', gmail: 'error', outlook: 'error', email_default: getOutlookAcct() ? 'outlook' : 'gmail' };
  }
}

// ── HELPERS ───────────────────────────────────────────────────────────

export function extractSlug(url) {
  const match = url?.match(/linkedin\.com\/in\/([^/?#\s]+)/i);
  return match ? match[1].replace(/\/$/, '') : null;
}

export function canonicalizeLinkedInProfileUrl(url) {
  const value = String(url || '').trim();
  if (!value) return null;
  const match = value.match(/^https?:\/\/([a-z]{2,3}\.)?linkedin\.com\/in\/([^?#\s]+)/i);
  if (!match) return value.replace(/[?#].*$/, '').replace(/\/+$/, '') || null;
  return `https://${match[1] || 'www.'}linkedin.com/in/${match[2].replace(/\/+$/, '')}`;
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
export async function retrieveLinkedInProfile(identifier, options = {}) {
  try {
    const id = getLinkedInIdentifier(identifier);
    if (!id) return null;
    const query = new URLSearchParams({
      account_id: options.accountId || getLiAcct(),
      notify: options.notify ? 'true' : 'false',
    });
    const sections = Array.isArray(options.linkedinSections) && options.linkedinSections.length
      ? options.linkedinSections
      : ['experience_preview', 'skills_preview', 'education_preview'];
    for (const section of sections) query.append('linkedin_sections', section);
    if (options.linkedinApi) query.set('linkedin_api', options.linkedinApi);

    const result = await api('GET', `/users/${encodeURIComponent(id)}?${query.toString()}`);
    const profile = result?.profile || result?.user || result || {};
    const canonicalUrl = canonicalizeLinkedInProfileUrl(
      profile.public_profile_url || profile.profile_url || profile.linkedin_url || identifier,
    );
    const positions = Array.isArray(profile.experience) ? profile.experience : (Array.isArray(profile.positions) ? profile.positions : []);
    const currentRole = positions[0] || null;
    const skills = Array.isArray(profile.skills)
      ? profile.skills.map(item => typeof item === 'string' ? item : item?.name).filter(Boolean)
      : [];
    const emails = Array.isArray(profile.contact_info?.emails) ? profile.contact_info.emails.filter(Boolean) : [];
    return {
      provider_id: profile.provider_id || profile.id || null,
      public_id: profile.public_identifier || profile.public_id || profile.username || extractSlug(canonicalUrl) || null,
      headline: profile.headline || null,
      summary: profile.summary || null,
      current_company: currentRole?.company_name || currentRole?.company || null,
      current_title: currentRole?.title || null,
      location: profile.location || profile.geo || null,
      connections: profile.connections_count || null,
      experience: positions.slice(0, 3),
      skills: skills.slice(0, 8),
      education: Array.isArray(profile.education) ? profile.education.slice(0, 2) : [],
      emails,
      linkedin_url: canonicalUrl,
      raw: profile,
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
  const acct = accountId || getEmailAcct();
  return api('GET', `/emails/${emailId}?account_id=${acct}`);
}

export async function listEmails({ threadId, messageId, anyEmail, from, to, accountId, limit = 50, metaOnly = false, includeHeaders = false } = {}) {
  const acct = accountId || getEmailAcct();
  const params = new URLSearchParams({ account_id: acct, limit: String(Math.min(Math.max(limit, 1), 250)) });
  if (threadId) params.set('thread_id', threadId);
  if (messageId) params.set('message_id', messageId);
  if (anyEmail) params.set('any_email', anyEmail);
  if (from) params.set('from', from);
  if (to) params.set('to', to);
  if (metaOnly) params.set('meta_only', 'true');
  if (includeHeaders) params.set('include_headers', 'true');
  const result = await api('GET', `/emails?${params.toString()}`);
  return result?.items || [];
}

export async function listWebhooks(limit = 100) {
  const result = await api('GET', `/webhooks?limit=${Math.min(Math.max(limit, 1), 250)}`);
  return result?.items || [];
}
