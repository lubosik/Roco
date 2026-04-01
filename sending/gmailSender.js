import { google } from 'googleapis';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { updateContact, getContactByEmail, getContactProp } from '../crm/notionContacts.js';
import { classifyReply, handleFirmSuppression, isDecline } from '../outreach/firmSuppressor.js';
import { cancelFollowUps } from '../outreach/sequenceManager.js';
import { sendEmailForApproval, sendTelegram } from '../approval/telegramBot.js';
import { draftEmail } from '../outreach/emailDrafter.js';
import { logActivity } from '../crm/notionLogger.js';
import { info, warn, error } from '../core/logger.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TOKEN_PATH = path.join(__dirname, '../tokens/gmail_token.json');
const CREDS_PATH = path.join(__dirname, '../credentials.json');

let gmailClient;

export async function getGmailClient() {
  if (gmailClient) return gmailClient;

  if (!fs.existsSync(CREDS_PATH)) {
    warn('credentials.json not found — Gmail disabled');
    return null;
  }

  const credentials = JSON.parse(fs.readFileSync(CREDS_PATH, 'utf8'));
  const { client_secret, client_id, redirect_uris } = credentials.installed || credentials.web;
  const oAuth2 = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0]);

  if (!fs.existsSync(TOKEN_PATH)) {
    warn('Gmail token not found. Run OAuth setup to generate tokens/gmail_token.json');
    return null;
  }

  oAuth2.setCredentials(JSON.parse(fs.readFileSync(TOKEN_PATH, 'utf8')));
  gmailClient = google.gmail({ version: 'v1', auth: oAuth2 });
  return gmailClient;
}

export async function sendEmail(toEmail, subject, body, threadId = null) {
  const gmail = await getGmailClient();
  if (!gmail) return null;

  const raw = buildRawEmail(toEmail, subject, body);

  try {
    const params = {
      userId: 'me',
      requestBody: { raw },
    };
    if (threadId) params.requestBody.threadId = threadId;

    const res = await gmail.users.messages.send(params);
    info(`Email sent to ${toEmail}`, { messageId: res.data.id });
    return { messageId: res.data.id, threadId: res.data.threadId };
  } catch (err) {
    error(`Gmail send failed to ${toEmail}`, { err: err.message });
    return null;
  }
}

export async function pollInbox(rocoState) {
  const gmail = await getGmailClient();
  if (!gmail) return;

  info('Polling inbox for new replies...');

  try {
    const listRes = await gmail.users.messages.list({
      userId: 'me',
      q: 'is:unread',
      maxResults: 20,
    });

    const messages = listRes.data.messages || [];
    if (!messages.length) return;

    info(`Found ${messages.length} unread message(s)`);

    for (const msg of messages) {
      try {
        await processInboundEmail(gmail, msg.id, rocoState);
      } catch (err) {
        error(`Failed to process message ${msg.id}`, { err: err.message });
      }
    }
  } catch (err) {
    error('Inbox poll failed', { err: err.message });
  }
}

async function processInboundEmail(gmail, messageId, rocoState) {
  const msgRes = await gmail.users.messages.get({ userId: 'me', id: messageId, format: 'full' });
  const msg = msgRes.data;

  const headers = msg.payload?.headers || [];
  const from = headers.find(h => h.name === 'From')?.value || '';
  const subject = headers.find(h => h.name === 'Subject')?.value || '';
  const threadId = msg.threadId;

  // Extract email address from From header
  const emailMatch = from.match(/[\w.+-]+@[\w-]+\.[\w.]+/);
  if (!emailMatch) return markRead(gmail, messageId);

  const senderEmail = emailMatch[0];
  const body = extractBody(msg.payload);

  // Check if this is a known contact
  const contact = await getContactByEmail(senderEmail);
  if (!contact) {
    info(`Unknown reply from ${senderEmail} — logged for manual review`);
    await logActivity('Unknown Reply', senderEmail, 'Unknown', subject, 'Inbox');
    return markRead(gmail, messageId);
  }

  const contactName = getContactProp(contact, 'Name');
  const firmName = getContactProp(contact, 'Company Name') || 'Unknown Firm';
  info(`Reply from known contact: ${contactName} at ${firmName}`);

  // Classify the reply
  const classification = await classifyReply(body);
  info(`Reply classified as ${classification.classification} (confidence: ${classification.confidence})`);

  // Update Notion
  await updateContact(contact.id, {
    responseReceived: 'Yes',
    responseSummary: `${classification.classification}: ${classification.reason}`.slice(0, 2000),
    lastContacted: new Date().toISOString().split('T')[0],
    lastContactType: 'Email',
    emailThreadLink: `https://mail.google.com/mail/#inbox/${threadId}`,
  });

  // Cancel follow-up sequences — a reply has come in
  await cancelFollowUps(contact);

  // Handle declines
  if (isDecline(classification.classification)) {
    await handleFirmSuppression(contact, firmName, classification.reason);
    return markRead(gmail, messageId);
  }

  // For interested/needs info — draft a response
  const threadMessages = await getThreadContext(gmail, threadId);
  const emailDraft = await draftEmail(contact, null, 'INTRO');

  if (emailDraft) {
    const alert = [
      `*ROCO — Reply Received*\n`,
      `From: ${contactName} at ${firmName}`,
      `Classification: ${classification.classification}`,
      `Reason: ${classification.reason}`,
      ``,
      `Their message:`,
      body.slice(0, 500),
    ].join('\n');

    await sendTelegram(alert);

    const decision = await sendEmailForApproval(contact, emailDraft, classification.reason, '-', 'REPLY');

    if (decision.action === 'approve') {
      const sent = await sendEmail(senderEmail, decision.subject, emailDraft.body, threadId);
      if (sent) {
        await updateContact(contact.id, {
          lastContacted: new Date().toISOString().split('T')[0],
          lastContactType: 'Email',
          lastContactNotes: `Reply sent: ${decision.subject}`,
        });
      }
    }
  }

  await markRead(gmail, messageId);
  await logActivity('Reply Received', contactName, firmName, classification.classification, 'Inbox');
}

async function markRead(gmail, messageId) {
  try {
    await gmail.users.messages.modify({
      userId: 'me',
      id: messageId,
      requestBody: { removeLabelIds: ['UNREAD'] },
    });
  } catch {}
}

async function getThreadContext(gmail, threadId) {
  try {
    const thread = await gmail.users.threads.get({ userId: 'me', id: threadId });
    return thread.data.messages || [];
  } catch {
    return [];
  }
}

function buildRawEmail(to, subject, body) {
  const email = [
    `To: ${to}`,
    `Subject: ${subject}`,
    `Content-Type: text/plain; charset=utf-8`,
    `MIME-Version: 1.0`,
    ``,
    body,
  ].join('\r\n');

  return Buffer.from(email).toString('base64').replace(/\+/g, '-').replace(/\//g, '_');
}

function extractBody(payload) {
  if (!payload) return '';
  if (payload.body?.data) {
    return Buffer.from(payload.body.data, 'base64').toString('utf8');
  }
  if (payload.parts) {
    for (const part of payload.parts) {
      if (part.mimeType === 'text/plain' && part.body?.data) {
        return Buffer.from(part.body.data, 'base64').toString('utf8');
      }
    }
  }
  return '';
}
