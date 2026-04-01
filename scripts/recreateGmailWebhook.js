/**
 * scripts/recreateGmailWebhook.js
 * One-time script to update the Gmail webhook account_ids to the new account ID.
 * Run: node scripts/recreateGmailWebhook.js
 */

import dotenv from 'dotenv';
dotenv.config({ path: '/root/roco/.env' });

const _dsn = process.env.UNIPILE_DSN || 'https://api34.unipile.com:16411';
const DSN = _dsn.startsWith('http') ? _dsn : `https://${_dsn}`;
const KEY = process.env.UNIPILE_API_KEY;
const GMAIL_ACCOUNT_ID = process.env.UNIPILE_GMAIL_ACCOUNT_ID;
const PUBLIC_URL = process.env.PUBLIC_URL || 'http://76.13.44.185:3000';

function headers() {
  return { 'X-API-KEY': KEY, 'Content-Type': 'application/json', accept: 'application/json' };
}

async function recreateGmailWebhook() {
  console.log('Gmail Webhook Updater');
  console.log(`New Account ID: ${GMAIL_ACCOUNT_ID}`);
  console.log(`Public URL: ${PUBLIC_URL}`);

  // Step 1: List existing webhooks
  let existing = [];
  try {
    const res = await fetch(`${DSN}/api/v1/webhooks`, { headers: headers() });
    const data = await res.json();
    existing = data.items || data || [];
    console.log(`Found ${existing.length} existing webhooks`);
    console.log('Webhooks:', JSON.stringify(existing.map(w => ({ id: w.id, name: w.name, url: w.request_url, source: w.source })), null, 2));
  } catch (err) {
    console.warn('Could not list webhooks:', err.message);
  }

  // Step 2: Find and update the Gmail webhook
  const gmailWebhook = existing.find(w =>
    w.name === 'roco_gmail_inbound' ||
    w.source === 'email' ||
    (w.request_url || '').includes('gmail') ||
    (w.request_url || '').includes('email')
  );

  const webhookUrl = `${PUBLIC_URL}/webhook/unipile/gmail`;

  if (gmailWebhook) {
    console.log(`\nUpdating Gmail webhook ${gmailWebhook.id}...`);
    try {
      const updateRes = await fetch(`${DSN}/api/v1/webhooks/${gmailWebhook.id}`, {
        method: 'PATCH',
        headers: headers(),
        body: JSON.stringify({
          request_url: webhookUrl,
          account_ids: [GMAIL_ACCOUNT_ID],
        }),
      });
      const text = await updateRes.text();
      if (updateRes.ok) {
        console.log('Gmail webhook updated successfully');
      } else {
        console.warn('PATCH failed:', text.substring(0, 300));
      }
    } catch (err) {
      console.warn('Could not update webhook:', err.message);
    }
  } else {
    console.log(`\nNo Gmail webhook found — creating new one...`);
    try {
      const createRes = await fetch(`${DSN}/api/v1/webhooks`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          name: 'roco_gmail_inbound',
          request_url: webhookUrl,
          source: 'email',
          events: ['mail_received'],
          account_ids: [GMAIL_ACCOUNT_ID],
        }),
      });
      const text = await createRes.text();
      if (createRes.ok) {
        console.log('Gmail webhook created:', text.substring(0, 200));
      } else {
        console.warn('Create failed:', text.substring(0, 300));
        console.log('\nPlease manually update the Gmail webhook in Unipile dashboard:');
        console.log(`  URL: ${webhookUrl}`);
        console.log(`  Account ID: ${GMAIL_ACCOUNT_ID}`);
      }
    } catch (err) {
      console.warn('Could not create webhook:', err.message);
    }
  }
}

recreateGmailWebhook().then(() => {
  console.log('\nDone.');
  process.exit(0);
}).catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
