/**
 * integrations/unipileSender.js
 * Unified sender via Unipile — Gmail and LinkedIn in one place.
 */

// Read env vars lazily so dotenv in index.js loads first (ES module imports are hoisted)
function getDSN()     { return process.env.UNIPILE_DSN || 'https://api34.unipile.com:16411'; }
function getApiKey()  { return process.env.UNIPILE_API_KEY; }
function getGmailId() { return process.env.UNIPILE_GMAIL_ACCOUNT_ID; }
function getLiId()    { return process.env.UNIPILE_LINKEDIN_ACCOUNT_ID; }

function headers() {
  return {
    'X-API-KEY': getApiKey(),
    'Content-Type': 'application/json',
    accept: 'application/json',
  };
}

// ─────────────────────────────────────────────
// EMAIL (Gmail via Unipile)
// ─────────────────────────────────────────────

/**
 * Send a new email via Unipile Gmail account.
 * @returns {{ threadId, messageId } | null}
 */
export async function sendEmail({ to, toName, subject, body, replyToThreadId = null }) {
  if (!getApiKey()) {
    console.error('[UNIPILE] UNIPILE_API_KEY not set — cannot send email');
    return null;
  }
  if (!getGmailId()) {
    console.error('[UNIPILE] UNIPILE_GMAIL_ACCOUNT_ID not set — cannot send email');
    return null;
  }

  try {
    const payload = {
      account_id: getGmailId(),
      to: [{ display_name: toName || to, identifier: to }],
      subject,
      body,
    };
    if (replyToThreadId) {
      payload.thread_id = replyToThreadId;
    }

    const res = await fetch(`${getDSN()}/api/v1/emails`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const text = await res.text();
      console.error(`[UNIPILE] Email send failed: HTTP ${res.status} — ${text}`);
      return null;
    }

    const data = await res.json();
    console.log(`[UNIPILE] Email sent to ${to} — message_id: ${data.id}`);
    return { threadId: data.thread_id || data.id, messageId: data.id };

  } catch (err) {
    console.error('[UNIPILE] sendEmail error:', err.message);
    return null;
  }
}

/**
 * Reply to an existing Gmail thread via Unipile.
 * @returns {{ threadId, messageId } | null}
 */
export async function sendEmailReply({ to, toName, subject, body, threadId }) {
  return sendEmail({ to, toName, subject, body, replyToThreadId: threadId });
}

// ─────────────────────────────────────────────
// LINKEDIN DM (via Unipile)
// ─────────────────────────────────────────────

/**
 * Send a LinkedIn DM via Unipile.
 * @param {string} linkedinProfileUrl - e.g. https://www.linkedin.com/in/johndoe/
 * @param {string} message - message body
 * @returns {{ chatId, messageId } | null}
 */
export async function sendLinkedInMessage({ linkedinProfileUrl, linkedinUrn, message }) {
  if (!getApiKey()) {
    console.error('[UNIPILE] UNIPILE_API_KEY not set — cannot send LinkedIn DM');
    return null;
  }
  if (!getLiId()) {
    console.error('[UNIPILE] UNIPILE_LINKEDIN_ACCOUNT_ID not set — cannot send LinkedIn DM');
    return null;
  }

  try {
    // Step 1 — resolve profile URN if we only have a URL
    let attendeeUrn = linkedinUrn;
    if (!attendeeUrn && linkedinProfileUrl) {
      attendeeUrn = await resolveLinkedInUrn(linkedinProfileUrl);
      if (!attendeeUrn) {
        console.error('[UNIPILE] Could not resolve LinkedIn URN from URL:', linkedinProfileUrl);
        return null;
      }
    }

    // Step 2 — create or get chat
    const chatRes = await fetch(`${getDSN()}/api/v1/chats`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        account_id: getLiId(),
        attendees_ids: [attendeeUrn],
        text: message,
      }),
    });

    if (!chatRes.ok) {
      const text = await chatRes.text();
      console.error(`[UNIPILE] LinkedIn DM send failed: HTTP ${chatRes.status} — ${text}`);
      return null;
    }

    const data = await chatRes.json();
    const chatId = data.id || data.chat_id;
    const messageId = data.last_message_id || data.message_id;
    console.log(`[UNIPILE] LinkedIn DM sent — chat_id: ${chatId}`);
    return { chatId, messageId };

  } catch (err) {
    console.error('[UNIPILE] sendLinkedInMessage error:', err.message);
    return null;
  }
}

/**
 * Reply to an existing LinkedIn chat thread.
 */
export async function sendLinkedInReply({ chatId, message }) {
  if (!getApiKey() || !getLiId()) return null;
  try {
    const res = await fetch(`${getDSN()}/api/v1/chats/${chatId}/messages`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        account_id: getLiId(),
        text: message,
      }),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error(`[UNIPILE] LinkedIn reply failed: HTTP ${res.status} — ${text}`);
      return null;
    }
    const data = await res.json();
    return { messageId: data.id };
  } catch (err) {
    console.error('[UNIPILE] sendLinkedInReply error:', err.message);
    return null;
  }
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────

async function resolveLinkedInUrn(profileUrl) {
  try {
    // Unipile profile lookup by URL
    const res = await fetch(
      `${getDSN()}/api/v1/users/me/relations?account_id=${getLiId()}&profile_url=${encodeURIComponent(profileUrl)}`,
      { headers: headers() }
    );
    if (!res.ok) return null;
    const data = await res.json();
    return data.provider_id || data.urn || null;
  } catch {
    return null;
  }
}

// ─────────────────────────────────────────────
// NO-OP pollInbox (inbox handled by Unipile webhooks)
// ─────────────────────────────────────────────

export async function pollInbox() {
  // Inbox is now handled by POST /webhook/unipile/gmail and /webhook/unipile/linkedin
}
