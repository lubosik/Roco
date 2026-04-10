/**
 * core/unipileSetup.js
 * Auto-registers Unipile webhooks on startup so inbound messages are
 * forwarded to the ROCO server for classification and response drafting.
 */

// Read env vars lazily (inside functions) so dotenv in index.js loads first
function getDSN()        { const d = process.env.UNIPILE_DSN || 'https://api34.unipile.com:16411'; return d.startsWith('http') ? d : `https://${d}`; }
function getApiKey()     { return process.env.UNIPILE_API_KEY; }
function getGmailAcct()    { return process.env.UNIPILE_GMAIL_ACCOUNT_ID; }
function getLiAcct()      { return process.env.UNIPILE_LINKEDIN_ACCOUNT_ID; }
function getOutlookAcct() { return process.env.UNIPILE_OUTLOOK_ACCOUNT_ID; }

function headers() {
  return {
    'X-API-KEY': getApiKey(),
    'Content-Type': 'application/json',
    accept: 'application/json',
  };
}

/**
 * Register (or update) Unipile webhooks so inbound messages are
 * forwarded to this server. Called once during startup.
 *
 * @param {string} baseUrl — the public-facing base URL of this server,
 *   e.g. https://roco.yourdomain.com or http://76.13.44.185:3000
 */
export async function registerWebhooks(baseUrl) {
  if (!getApiKey()) {
    console.warn('[UNIPILE SETUP] UNIPILE_API_KEY not set — skipping webhook registration');
    return;
  }
  if (!baseUrl) {
    console.warn('[UNIPILE SETUP] No baseUrl provided — skipping webhook registration');
    return;
  }

  const webhooks = [
    {
      name: 'roco_gmail_inbound',
      request_url: `${baseUrl}/webhook/unipile/gmail`,
      source: 'email',
      events: ['mail_received'],
      account_ids: [getGmailAcct()].filter(Boolean),
      headers: [
        { key: 'Content-Type', value: 'application/json' },
      ],
    },
    {
      name: 'roco_outlook_inbound',
      request_url: `${baseUrl}/webhook/unipile/outlook`,
      source: 'email',
      events: ['mail_received'],
      account_ids: [getOutlookAcct()].filter(Boolean),
      headers: [
        { key: 'Content-Type', value: 'application/json' },
      ],
    },
    {
      name: 'roco_linkedin_messages',
      request_url: `${baseUrl}/webhooks/unipile/messages`,
      source: 'messaging',
      events: ['message_received'],
      account_id: getLiAcct(),
      headers: [
        { key: 'Content-Type', value: 'application/json' },
      ],
    },
    {
      name: 'roco_linkedin_relations',
      request_url: `${baseUrl}/webhooks/unipile/messages`,
      source: 'users',
      events: ['new_relation'],
      account_id: getLiAcct(),
      headers: [
        { key: 'Content-Type', value: 'application/json' },
      ],
    },
  ];

  // Fetch existing webhooks so we can dedup / update instead of creating duplicates
  let existing = [];
  try {
    const res = await fetch(`${getDSN()}/api/v1/webhooks`, { headers: headers() });
    if (res.ok) {
      const data = await res.json();
      existing = data.items || data || [];
    }
  } catch (err) {
    console.warn('[UNIPILE SETUP] Could not fetch existing webhooks:', err.message);
  }

  for (const wh of webhooks) {
    try {
      // Check if a webhook with the same name already exists
      const match = existing.find(e => e.name === wh.name || e.request_url === wh.request_url);

      if (match) {
        // Unipile doesn't support PATCH — if URL matches we're done; otherwise skip silently
        console.log(`[UNIPILE SETUP] Webhook exists: ${wh.name} → ${match.request_url}`);
        continue;
      }

      // Create fresh
      const createRes = await fetch(`${getDSN()}/api/v1/webhooks`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify(wh),
      });
      if (createRes.ok) {
        console.log(`[UNIPILE SETUP] Webhook registered: ${wh.name} → ${wh.request_url}`);
      } else {
        const txt = await createRes.text();
        console.warn(`[UNIPILE SETUP] Webhook creation failed for ${wh.name}: ${txt.substring(0, 200)}`);
      }
    } catch (err) {
      console.warn(`[UNIPILE SETUP] Error registering ${wh.name}:`, err.message);
    }
  }
}
