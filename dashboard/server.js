import express from 'express';
import session from 'express-session';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import multer from 'multer';
import * as XLSX from 'xlsx';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { getPendingApprovals, resolveApprovalFromDashboard, clearApprovalsForDeal, sendTelegram, sendSourcingDraftToTelegram, sendReplyForApproval } from '../approval/telegramBot.js';
import { getInvestorGuidance, getSourcingGuidance, saveInvestorGuidance, saveSourcingGuidance, buildGuidanceBlock } from '../services/guidanceService.js';
import { invalidateCache as invalidateAgentContext } from '../core/agentContext.js';
import { getAllActiveContacts, getContactsByDeal, getContactProp, countActiveContacts, updateContact, archiveContact } from '../crm/notionContacts.js';
import { getAllCompanies, getCompanyProp } from '../crm/notionCompanies.js';
import { getSupabase } from '../core/supabase.js';
import {
  getConversationHistory,
  logConversationMessage,
  classifyIntent,
  draftContextualReply,
  setConversationState,
  appendIntentHistory,
  checkTempClosedContacts,
  draftTempCloseFollowUp,
} from '../core/conversationManager.js';
import { sendEmailReply, sendLinkedInReply } from '../integrations/unipileClient.js';
import { sendLinkedInInvite } from '../integrations/unipileClient.js';
import { logActivity } from '../crm/notionLogger.js';
import { getApiHealth, startHealthChecks } from '../core/apiFallback.js';
import { info, error } from '../core/logger.js';
import { aiComplete } from '../core/aiClient.js';
import {
  loadSessionState, saveSessionState,
  getAllDeals, getActiveDeals, getDeal, createDeal, updateDeal,
  getTemplates, getTemplate, updateTemplate, seedDefaultTemplates,
  getActivityLog, logActivity as sbLogActivity,
  getBatches, deleteApprovalFromQueue,
} from '../core/supabaseSync.js';
import { getWindowStatus, getWindowVisualization } from '../core/scheduleChecker.js';
import { getBatchSummary } from '../core/batchManager.js';
import { recreateLinkedInWebhooks, startLinkedInDM, sendLinkedInDM as sendLinkedInDMReply } from '../core/unipile.js';
import { handleLinkedInMessage as handleLiMsg, handleLinkedInRelation as handleLiRelation } from '../core/unipileWebhooks.js';
import { startInboxMonitor } from '../core/inboxMonitor.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const STATE_FILE = path.join(__dirname, '../state.json');

let rocoState;
let wss;
let app;

const activityFeed = [];
const MAX_FEED = 200;

// ─────────────────────────────────────────────
// REPLY DEBOUNCE BATCHER
// ─────────────────────────────────────────────
const replyDebounceMap = new Map();
const REPLY_DEBOUNCE_MS = 90 * 1000;

function clearDebounceForDeal(dealId) {
  let cleared = 0;
  for (const [key, batch] of replyDebounceMap.entries()) {
    if (batch.deal && String(batch.deal.id) === String(dealId)) {
      clearTimeout(batch.timer);
      replyDebounceMap.delete(key);
      cleared++;
    }
  }
  if (cleared > 0) info(`[CLOSE] Cleared ${cleared} debounce batch(es) for deal ${dealId}`);
}

// ─────────────────────────────────────────────
// STATE FILE HELPERS
// ─────────────────────────────────────────────

function readState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    }
  } catch (err) {
    error('Failed to read state.json', { err: err.message });
  }
  return {
    rocoStatus: 'ACTIVE',
    outreachEnabled: true,
    followupEnabled: true,
    enrichmentEnabled: true,
    researchEnabled: true,
    linkedinEnabled: true,
    pausedUntil: null,
    activeDeals: [],
    lastUpdated: new Date().toISOString(),
  };
}

function writeState(state) {
  state.lastUpdated = new Date().toISOString();
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

// ─────────────────────────────────────────────
// LINKEDIN DM SEND HELPER (approval flow)
// ─────────────────────────────────────────────

async function sendApprovedLinkedInDM({ contactId, text }) {
  const sb = getSupabase();
  if (!sb) throw new Error('Database unavailable');

  let contact = null;
  try {
    const { data } = await sb.from('contacts')
      .select('id, name, deal_id, linkedin_provider_id, unipile_chat_id')
      .eq('id', contactId).single();
    contact = data;
  } catch { /* not found */ }
  if (!contact) throw new Error('Contact not found: ' + contactId);
  if (!contact.linkedin_provider_id && !contact.unipile_chat_id) {
    throw new Error('No LinkedIn provider ID or chat ID on contact ' + contactId);
  }

  let result;
  if (contact.unipile_chat_id) {
    result = await sendLinkedInDMReply(contact.unipile_chat_id, text);
  } else {
    result = await startLinkedInDM(contact.linkedin_provider_id, text);
  }

  const newChatId = result?.chat_id;
  if (newChatId && !contact.unipile_chat_id) {
    await sb.from('contacts').update({
      unipile_chat_id: newChatId,
      pipeline_stage:  'dm_sent',
    }).eq('id', contact.id).catch(() => {});
  }

  await sb.from('conversation_messages').insert({
    contact_id: contact.id,
    deal_id:    contact.deal_id || null,
    direction:  'outbound',
    channel:    'linkedin_dm',
    body:       text,
    sent_at:    new Date().toISOString(),
  }).catch(err => console.warn('[LI DM] log error:', err.message));

  return result;
}

// ─────────────────────────────────────────────
// INIT
// ─────────────────────────────────────────────

export function initDashboard(state) {
  rocoState = state;

  app = express();
  const server = createServer(app);
  wss = new WebSocketServer({ server, path: '/ws' });

  // CORS — must be before auth so preflight OPTIONS requests pass through
  app.use((req, res, next) => {
    const origin = req.headers.origin;
    // Allow any origin (dashboard may be served from Vercel, localhost, etc.)
    res.header('Access-Control-Allow-Origin', origin || '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.header('Access-Control-Allow-Credentials', 'true');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
  });

  // Health check — no auth required (before auth middleware)
  app.get('/health', (req, res) => {
    res.json({ ok: true, status: 'running', ts: new Date().toISOString() });
  });

  // ─── Unipile webhooks — no Basic Auth (Unipile calls these from their servers) ───

  // POST /webhook/unipile/gmail — inbound Gmail replies via Unipile
  app.post('/webhook/unipile/gmail', express.json(), async (req, res) => {
    res.json({ ok: true }); // Acknowledge immediately
    try {
      const event = req.body;
      console.log('[WEBHOOK/GMAIL] Received event:', event?.type || 'unknown');

      // Unipile sends a wrapper with type + payload
      const payload = event?.data || event;
      const fromEmail   = payload?.from_attendee?.identifier || payload?.from_email || '';
      const fromName    = payload?.from_attendee?.display_name || payload?.from_name || '';
      const subject     = payload?.subject || '';
      const bodyText    = payload?.body || payload?.body_plain || payload?.text || '';
      const threadId    = payload?.thread_id || payload?.in_reply_to?.id || payload?.conversation_id || '';
      const messageId   = payload?.id || payload?.message_id || '';

      if (!fromEmail) return;

      await queueInboundWithDebounce({
        fromEmail, fromName, bodyText, threadId, messageId, channel: 'email', raw: payload,
      });
    } catch (err) {
      console.error('[WEBHOOK/GMAIL] Error:', err.message);
    }
  });

  // POST /webhook/unipile/outlook — inbound Outlook replies via Unipile
  app.post('/webhook/unipile/outlook', express.json(), async (req, res) => {
    res.json({ ok: true }); // Acknowledge immediately
    try {
      const event = req.body;
      console.log('[WEBHOOK/OUTLOOK] Received event:', event?.event || event?.type || 'unknown');

      const payload   = event?.data || event;
      const eventType = payload?.event || event?.event || '';

      // Only process inbound emails (ignore sent/moved)
      if (eventType && eventType !== 'mail_received') return;

      const fromEmail = payload?.from_attendee?.identifier || payload?.from_email || '';
      const fromName  = payload?.from_attendee?.display_name || payload?.from_name || '';
      const subject   = payload?.subject || '';
      const bodyText  = payload?.body_plain || payload?.body || payload?.text || '';
      const threadId  = payload?.thread_id || payload?.in_reply_to?.id || payload?.conversation_id || '';
      const messageId = payload?.id || payload?.message_id || payload?.email_id || '';

      if (!fromEmail) {
        console.log('[WEBHOOK/OUTLOOK] No from_email in payload — skipping');
        return;
      }

      console.log(`[WEBHOOK/OUTLOOK] From: ${fromName} <${fromEmail}> | Subject: ${subject}`);

      const outlookAccountId = payload?.account_id || process.env.UNIPILE_OUTLOOK_ACCOUNT_ID;
      await queueInboundWithDebounce({
        fromEmail, fromName, bodyText, threadId, messageId,
        channel: 'email', emailAccountId: outlookAccountId, raw: payload,
      });
    } catch (err) {
      console.error('[WEBHOOK/OUTLOOK] Error:', err.message);
    }
  });

  // POST /webhook/unipile/linkedin — inbound LinkedIn DMs via Unipile (legacy route)
  app.post('/webhook/unipile/linkedin', express.json(), async (req, res) => {
    res.json({ ok: true }); // Acknowledge immediately
    try {
      const event = req.body;
      console.log('[WEBHOOK/LINKEDIN] Received event:', event?.type || 'unknown');

      const payload   = event?.data || event;
      const fromUrn   = payload?.sender?.attendee_provider_id || payload?.sender_id || payload?.attendee_id || '';
      const fromName  = payload?.sender?.attendee_name || payload?.sender_name || payload?.attendee_name || '';
      const bodyText  = payload?.message || payload?.text || payload?.body || '';
      const chatId    = payload?.chat_id || payload?.conversation_id || '';
      const messageId = payload?.id || payload?.message_id || '';

      if (!fromUrn && !fromName) return;

      await queueInboundWithDebounce({
        fromUrn, fromName, bodyText, chatId, messageId, channel: 'linkedin', raw: payload,
      });
    } catch (err) {
      console.error('[WEBHOOK/LINKEDIN] Error:', err.message);
    }
  });

  // POST /webhook/unipile/linkedin/messages — inbound LinkedIn messages (registered in unipileSetup.js)
  app.post('/webhook/unipile/linkedin/messages', express.json(), async (req, res) => {
    res.json({ ok: true });
    try {
      const event   = req.body;
      const payload = event?.data || event;

      // Unipile message_received payload: sender.attendee_provider_id, sender.attendee_name, message (top-level)
      const fromProvId   = payload?.sender?.attendee_provider_id || payload?.sender_id || payload?.attendee_id || '';
      const fromName     = payload?.sender?.attendee_name || payload?.sender_name || payload?.attendee_name || '';
      const fromLinkedin = fromProvId ? `https://linkedin.com/in/${fromProvId}` : null;
      const messageText  = payload?.message || payload?.text || payload?.body || '';
      const chatId       = payload?.chat_id || payload?.conversation_id || '';
      const messageId    = payload?.id || payload?.message_id || '';

      console.log(`[WEBHOOK/LINKEDIN/MSG] From: ${fromName} | Chat: ${chatId}`);

      // Persist to linkedin_messages table
      const sb = getSupabase();
      if (sb && messageText) {
        await sb.from('linkedin_messages').insert({
          chat_id:             chatId || null,
          message_text:        messageText,
          from_name:           fromName || null,
          from_linkedin_url:   fromLinkedin || null,
          from_provider_id:    fromProvId || null,
          created_at:          new Date().toISOString(),
        }).catch(e => console.warn('[WEBHOOK/LINKEDIN/MSG] DB insert failed:', e.message));
      }

      // Route through debounce batcher to draft response
      if (fromName || fromProvId) {
        await queueInboundWithDebounce({
          fromUrn: fromProvId, fromName, bodyText: messageText, chatId, messageId, channel: 'linkedin', raw: payload,
        });
      }
    } catch (err) {
      console.error('[WEBHOOK/LINKEDIN/MSG] Error:', err.message);
    }
  });

  // POST /webhook/unipile/linkedin/relations — connection accepted notification
  app.post('/webhook/unipile/linkedin/relations', express.json(), async (req, res) => {
    res.json({ ok: true });
    try {
      const event   = req.body;
      // Log full raw payload to confirm Unipile field structure
      console.log('[WEBHOOK/LINKEDIN/REL] Raw payload:', JSON.stringify(event));

      // Unipile new_relation payload is flat: user_provider_id, user_full_name, user_public_identifier, user_profile_url
      const payload    = event?.data || event;
      const providerId = payload?.user_provider_id || payload?.provider_id || payload?.attendee?.provider_id || '';
      const name       = payload?.user_full_name || payload?.display_name || payload?.attendee?.display_name || '';
      const publicId   = payload?.user_public_identifier || '';
      const profileUrl = payload?.user_profile_url || '';

      console.log(`[WEBHOOK/LINKEDIN/REL] Connection accepted: ${name} (${providerId})`);

      if (!providerId && !name) {
        console.warn('[WEBHOOK/LINKEDIN/REL] Empty payload — cannot identify person, skipping');
        return;
      }

      const sb = getSupabase();
      if (!sb) return;

      // Look up the contact in our pipeline — include paused deals so we still record the accepted connection.
      // We check paused state later and hold the DM until resume, but always update the stage.
      let contactQuery = sb.from('contacts')
        .select('id, name, deal_id, notion_page_id, deals!inner(id, status, paused)')
        .eq('deals.status', 'ACTIVE');

      // Match by provider_id first (most reliable), then URL slug, then name as last resort.
      // Never use a wildcard-only pattern — if we have nothing specific, skip entirely.
      if (providerId) {
        // Use provider_id exact match OR full profile URL match — never a partial wildcard on provider_id
        const orClauses = [`linkedin_provider_id.eq.${providerId}`];
        if (profileUrl) orClauses.push(`linkedin_url.eq.${profileUrl}`);
        else if (publicId) orClauses.push(`linkedin_url.ilike.%${publicId}%`);
        contactQuery = contactQuery.or(orClauses.join(','));
      } else if (publicId) {
        // Slug-only match — specific enough to be safe
        contactQuery = contactQuery.ilike('linkedin_url', `%${publicId}%`);
      } else if (name) {
        // Name only as absolute last resort
        contactQuery = contactQuery.ilike('name', `%${name}%`);
      } else {
        // Nothing to match on — skip entirely
        console.warn('[WEBHOOK/LINKEDIN/REL] No usable identifiers to look up contact — skipping');
        return;
      }

      const { data: contacts } = await contactQuery.limit(1);
      const contact = contacts?.[0];

      if (contact) {
        // ── Investor pipeline contact ──────────────────────────────────────────
        const dealStatus = contact.deals?.status;
        const dealPaused = contact.deals?.paused;

        const ALREADY_RESPONDED = ['In Conversation', 'Replied', 'Meeting Booked', 'Meeting Scheduled'];
        const alreadyResponded = ALREADY_RESPONDED.includes(contact.pipeline_stage) || contact.response_received;

        // Always record the accepted connection — even if deal is paused.
        // The stage update ensures the orchestrator picks it up on resume.
        if (!alreadyResponded) {
          await sb.from('contacts').update({ pipeline_stage: 'invite_accepted' }).eq('id', contact.id);
          if (contact.notion_page_id) {
            await updateContact(contact.notion_page_id, {
              pipelineStage: 'invite_accepted',
            }).catch(e => console.warn(`[WEBHOOK/LINKEDIN/REL] Notion update failed for ${contact.name}: ${e.message}`));
          }
        } else {
          console.log(`[WEBHOOK/LINKEDIN/REL] ${contact.name} already responded (${contact.pipeline_stage}) — not overwriting stage`);
        }

        const pausedNote = dealPaused ? ' (deal paused — DM will be queued on resume)' : '';
        pushActivity({
          type: 'linkedin',
          action: `${contact.name} accepted your connection request`,
          note: alreadyResponded
            ? `Already in conversation — LinkedIn connected, no DM will be sent.`
            : `Pipeline stage → invite_accepted.${pausedNote}`,
        });
        await sbLogActivity({
          dealId: contact.deal_id,
          contactId: contact.id,
          eventType: 'INVITE_ACCEPTED',
          summary: `${contact.name} accepted connection request${dealPaused ? ' (deal paused)' : ''}`,
        }).catch(() => {});
        console.log(`[WEBHOOK/LINKEDIN/REL] ${contact.name} advanced to invite_accepted (investor pipeline)${dealPaused ? ' — deal paused, DM held for resume' : ''}`);

      } else {
        // ── Sourcing pipeline — check company_contacts ─────────────────────────
        let sourcingContactQuery = sb.from('company_contacts')
          .select('*, target_companies(company_name, match_score, match_tier, product_description, intent_signals_found, why_matches, sector, geography), sourcing_campaigns(name, firm_name, firm_type, investment_thesis, deal_type, investment_size)')
          .eq('pipeline_stage', 'invite_sent')
          .eq('linkedin_invite_accepted', false);

        if (providerId) {
          const orClauses = [`linkedin_provider_id.eq.${providerId}`];
          if (profileUrl) orClauses.push(`linkedin_url.eq.${profileUrl}`);
          else if (publicId) orClauses.push(`linkedin_url.ilike.%${publicId}%`);
          sourcingContactQuery = sourcingContactQuery.or(orClauses.join(','));
        } else if (publicId) {
          sourcingContactQuery = sourcingContactQuery.ilike('linkedin_url', `%${publicId}%`);
        } else if (name) {
          sourcingContactQuery = sourcingContactQuery.ilike('name', `%${name}%`);
        } else {
          console.log(`[WEBHOOK/LINKEDIN/REL] No identifiers — cannot look up sourcing contact`);
          return;
        }

        const { data: sourcingContacts } = await sourcingContactQuery.limit(1);
        const sc = sourcingContacts?.[0];

        if (!sc) {
          console.log(`[WEBHOOK/LINKEDIN/REL] ${name || providerId} not in any active pipeline — ignoring`);
          return;
        }

        // Mark invite accepted
        await sb.from('company_contacts').update({
          linkedin_invite_accepted: true,
          pipeline_stage: 'invite_accepted',
          updated_at: new Date().toISOString(),
        }).eq('id', sc.id).then(null, () => {});

        console.log(`[WEBHOOK/LINKEDIN/REL] Sourcing contact ${sc.name} accepted invite — queuing LinkedIn DM`);
        pushActivity({
          type: 'linkedin',
          action: `${sc.name} accepted connection request (sourcing)`,
          note: `Campaign: ${sc.sourcing_campaigns?.name || 'Unknown'} — queuing LinkedIn DM draft`,
        });

        // Queue LinkedIn DM draft for Telegram approval
        try {
          const { constructCompanySourcingMessage } = await import('../sourcing/messageConstructorSourcing.js');
          const company = sc.target_companies;
          const campaign = sc.sourcing_campaigns;
          if (!company || !campaign) {
            console.warn(`[WEBHOOK/LINKEDIN/REL] Missing company/campaign data for ${sc.name} — skipping DM queue`);
            return;
          }

          const draft = await constructCompanySourcingMessage(sc, company, sc.campaign_id, 'linkedin_dm', null);
          if (!draft) return;

          const researchBasis = company.intent_signals_found || company.why_matches || `${company.sector} in ${company.geography}`;

          // Check for duplicate pending queue entry
          const { data: existingDm } = await sb.from('approval_queue')
            .select('id').eq('company_contact_id', sc.id).eq('status', 'pending').limit(1);
          if (existingDm?.length > 0) {
            console.log(`[WEBHOOK/LINKEDIN/REL] DM already queued for ${sc.name} — skipping duplicate`);
            return;
          }

          const { data: queueRow } = await sb.from('approval_queue').insert([{
            contact_id:         sc.id,
            contact_name:       sc.name,
            firm:               company.company_name,
            stage:              'LinkedIn DM',
            score:              company.match_score || 0,
            subject_a:          null,
            subject_b:          null,
            body:               draft.body,
            research_summary:   researchBasis,
            status:             'pending',
            campaign_id:        sc.campaign_id,
            company_contact_id: sc.id,
            outreach_mode:      'company_sourcing',
            created_at:         new Date().toISOString(),
          }]).select().single().then(r => r, () => ({ data: null }));

          const companyObj = { ...company, id: sc.company_id };
          const campaignObj = { ...campaign, id: sc.campaign_id };
          await sendSourcingDraftToTelegram(sc, companyObj, campaignObj, draft, researchBasis, queueRow?.id);
        } catch (dmErr) {
          console.error(`[WEBHOOK/LINKEDIN/REL] Failed to queue LinkedIn DM for ${sc.name}:`, dmErr.message);
        }
      }
    } catch (err) {
      console.error('[WEBHOOK/LINKEDIN/REL] Error:', err.message);
    }
  });

  // POST /webhooks/unipile/messages — consolidated LinkedIn events (messages + relation accepted)
  // No auth — Unipile calls this from their servers. Always 200 immediately.
  app.post('/webhooks/unipile/messages', express.json(), async (req, res) => {
    res.status(200).json({ ok: true });

    const event = req.body;

    // Forensic trail — raw payload logged synchronously before anything else
    const sb = getSupabase();
    if (sb) {
      sb.from('webhook_logs').insert({
        event_type:  event?.type || event?.event_type || 'unknown',
        payload:     event,
        received_at: new Date().toISOString(),
      }).then(null, () => {});
    }

    const eventType = (event?.type || event?.event_type || '').toLowerCase();
    console.log('[WEBHOOKS/UNIPILE] Received event:', eventType);

    try {
      if (['message_received', 'message.created'].includes(eventType)) {
        await handleLiMsg(event, pushActivity, { draftContextualReply });
      } else if (['new_relation', 'connection_request_accepted'].includes(eventType)) {
        // queueForApproval callback: create approval_queue entry + log
        const queueForApproval = async ({ contact, template, channel, action }) => {
          if (!sb) return;
          const body = (template.body || template.body_a || '').replace(/{{firstName}}/g, contact.name?.split(' ')[0] || contact.name || '');
          await sb.from('approval_queue').insert([{
            contact_id:   contact.id,
            contact_name: contact.name,
            firm:         contact.company_name || '',
            stage:        'LinkedIn DM',
            body,
            status:       'pending',
            outreach_mode: 'investor_outreach',
            created_at:   new Date().toISOString(),
          }]).then(null, err => console.warn('[APPROVAL] Queue insert error:', err.message));
        };
        await handleLiRelation(event, pushActivity, queueForApproval);
      } else if (['mail_received', 'email.received', 'email_received'].includes(eventType)) {
        // Inbound email reply from investor — mark contact as replied + notify
        const fromEmail = event?.from?.email || event?.from_email || event?.from || '';
        const subject   = event?.subject || '';
        const body      = event?.text || event?.html || event?.body || '';
        const threadId  = event?.thread_id || event?.threadId || '';
        console.log(`[WEBHOOKS/UNIPILE] mail_received from: ${fromEmail} subject: ${subject}`);

        if (sb && fromEmail) {
          // Find contact by email
          const { data: contact } = await sb.from('contacts')
            .select('id, name, company_name, deal_id')
            .ilike('email', fromEmail)
            .limit(1)
            .single();

          if (contact) {
            // Mark as replied
            await sb.from('contacts').update({
              response_received: true,
              pipeline_stage: 'Replied',
              last_contacted_at: new Date().toISOString(),
            }).eq('id', contact.id);

            // Queue with debounce for contextual reply drafting
            queueInboundWithDebounce({ fromEmail, fromName: contact.name, bodyText: body, threadId, channel: 'email' }).catch(() => {});

            pushActivity({
              type: 'reply',
              action: 'Email reply received',
              note: `${contact.name}${contact.company_name ? ` @ ${contact.company_name}` : ''} — "${subject}"`,
              dealId: contact.deal_id,
            });

            const { sendTelegram } = await import('../approval/telegramBot.js');
            await sendTelegram(`📧 *Email reply* from *${contact.name}* (${contact.company_name || 'unknown'})\nSubject: _${subject}_`).catch(() => {});
          } else {
            console.log(`[WEBHOOKS/UNIPILE] mail_received — no contact found for ${fromEmail}`);
          }
        }
      } else {
        console.log('[WEBHOOKS/UNIPILE] Unhandled event type:', eventType);
      }
    } catch (err) {
      console.error('[WEBHOOKS/UNIPILE] Handler error:', err.message);
    }
  });

  // Session middleware
  app.use(session({
    secret: process.env.SESSION_SECRET || 'roco-mission-control-2026',
    resave: false,
    saveUninitialized: false,
    cookie: { secure: false, maxAge: 24 * 60 * 60 * 1000 },
  }));

  // Auth middleware
  const dashboardUser = (process.env.DASHBOARD_USER || 'admin').trim();
  const dashboardPass = (process.env.DASHBOARD_PASS || 'roco2026').trim();
  const dashboardDisplayName = (process.env.DASHBOARD_DISPLAY_NAME || dashboardUser).trim();

  function requireAuth(req, res, next) {
    const publicPaths = ['/login', '/welcome.html', '/favicon.ico', '/audio/'];
    if (publicPaths.some(p => req.path.startsWith(p))) return next();
    if (req.session?.authenticated) return next();
    if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Unauthorized' });
    return res.redirect('/welcome.html');
  }

  app.use(requireAuth);

  // Login endpoint
  app.post('/login', express.json(), (req, res) => {
    const { username, password } = req.body || {};
    if (username === dashboardUser && password === dashboardPass) {
      req.session.authenticated = true;
      req.session.username = username;
      req.session.displayName = dashboardDisplayName;
      const sanitised = dashboardDisplayName.toLowerCase().replace(/[^a-z0-9]/g, '_');
      generateWelcomeAudio(dashboardDisplayName).catch(console.error);
      res.json({
        success: true,
        displayName: dashboardDisplayName,
        welcomeAudioPath: `/audio/welcome_${sanitised}.mp3`,
      });
    } else {
      res.status(401).json({ error: 'Invalid credentials' });
    }
  });

  // Get current user
  app.get('/api/me', (req, res) => {
    if (!req.session?.authenticated) return res.status(401).json({ error: 'Unauthorized' });
    res.json({ username: req.session.username, displayName: req.session.displayName });
  });

  // Logout
  app.post('/logout', (req, res) => {
    req.session.destroy(() => {});
    res.json({ success: true });
  });

  // Serve welcome page at root (no auth)
  app.get('/', (req, res) => {
    if (req.session?.authenticated) return res.redirect('/dashboard');
    res.sendFile('welcome.html', { root: path.join(__dirname, 'public') });
  });

  // Main dashboard (requires auth)
  app.get('/dashboard', (req, res) => {
    res.sendFile('index.html', { root: path.join(__dirname, 'public') });
  });

  app.use(express.json());
  app.use(express.static(path.join(__dirname, 'public')));
  registerRoutes(app);

  // WebSocket — push live activity
  wss.on('connection', (ws) => {
    info('Dashboard WebSocket client connected');
    ws.send(JSON.stringify({ type: 'init', feed: activityFeed.slice(-100) }));
    ws.on('error', () => {});
  });

  const port = process.env.PORT || 3000;
  server.listen(port, '0.0.0.0', () => {
    info(`Mission Control dashboard running at http://0.0.0.0:${port}`);
  });

  // Start API health checks
  startHealthChecks();

  // Recreate LinkedIn webhooks — auto-discover Cloudflare tunnel URL if running
  (async () => {
    let serverBaseUrl = process.env.PUBLIC_URL || process.env.SERVER_BASE_URL || '';
    if (!serverBaseUrl) {
      try {
        const r = await fetch('http://localhost:20241/quicktunnel');
        const d = await r.json();
        if (d?.hostname) {
          serverBaseUrl = `https://${d.hostname}`;
          console.log('[BOOT] Cloudflare tunnel URL auto-discovered:', serverBaseUrl);
        }
      } catch {}
    }
    if (!serverBaseUrl) serverBaseUrl = `http://76.13.44.185:${port}`;
    recreateLinkedInWebhooks(serverBaseUrl).catch(err =>
      console.warn('[BOOT] LinkedIn webhook recreation failed:', err.message)
    );
  })();

  // Start inbox polling fallback (60-second interval, catches missed webhooks)
  startInboxMonitor(handleLiMsg, pushActivity, { draftContextualReply });

  // Pre-generate ElevenLabs welcome audio for the configured display name
  generateWelcomeAudio(dashboardDisplayName).catch(console.error);

  // Backfill existing investors into named lists (one-time, skips if lists already exist)
  backfillInvestorLists().catch(e => console.warn('[LISTS] Backfill error:', e.message));

  // Backfill contact_type classification for investors_db rows that predate this feature
  import('../core/investorDatabaseImporter.js')
    .then(m => m.backfillContactTypes())
    .catch(e => console.warn('[BACKFILL] contact_type backfill error:', e.message));

  return { app, server, wss };
}

async function backfillInvestorLists() {
  const sb = getSupabase();
  if (!sb) return;

  const { count } = await sb.from('investor_lists').select('*', { count: 'exact', head: true });
  if (count > 0) return; // already done

  console.log('[LISTS] Backfilling investor lists from source files...');

  const { data: investors } = await sb.from('investors_db')
    .select('source_file, investor_category')
    .not('source_file', 'is', null);

  const fileGroups = {};
  (investors || []).forEach(inv => {
    const key = inv.source_file || 'Unknown';
    if (!fileGroups[key]) fileGroups[key] = 0;
    fileGroups[key]++;
  });

  for (const [sourceFile, cnt] of Object.entries(fileGroups)) {
    const listName = (sourceFile.replace(/\.[^/.]+$/, '').replace(/_/g, ' ').replace(/-/g, ' ').trim()) || sourceFile;
    try {
      const { data: newList } = await sb.from('investor_lists').insert({
        name: listName, list_type: 'standard', source: 'pitchbook',
      }).select().single();
      if (newList?.id) {
        await sb.from('investors_db').update({ list_id: newList.id, list_name: newList.name }).eq('source_file', sourceFile);
        console.log(`[LISTS] Created list "${newList.name}" (${cnt} investors)`);
      }
    } catch (e) { console.warn(`[LISTS] Skipped "${listName}":`, e.message); }
  }

  // Catch-all for investors with no source_file
  try {
    const { data: generalList } = await sb.from('investor_lists').insert({
      name: 'General Database', list_type: 'standard', source: 'pitchbook',
    }).select().single();
    if (generalList?.id) {
      await sb.from('investors_db').update({ list_id: generalList.id, list_name: 'General Database' }).is('list_id', null);
    }
  } catch (e) { console.warn('[LISTS] General Database list error:', e.message); }

  console.log('[LISTS] Backfill complete');
}

// ─────────────────────────────────────────────
// ELEVENLABS WELCOME AUDIO
// ─────────────────────────────────────────────

async function generateWelcomeAudio(displayName) {
  const apiKey = process.env.ELEVENLABS_API_KEY;
  if (!apiKey) {
    info('[VOICE] ELEVENLABS_API_KEY not set — skipping welcome audio generation');
    return null;
  }

  const sanitised = displayName.toLowerCase().replace(/[^a-z0-9]/g, '_');
  const outputPath = path.join(__dirname, 'public', 'audio', `welcome_${sanitised}.mp3`);

  if (fs.existsSync(outputPath)) {
    info(`[VOICE] Welcome audio already cached for ${displayName}`);
    return `/audio/welcome_${sanitised}.mp3`;
  }

  info(`[VOICE] Generating welcome audio for ${displayName}...`);

  try {
    const response = await fetch('https://api.elevenlabs.io/v1/text-to-speech/21m00Tcm4TlvDq8ikWAM', {
      method: 'POST',
      headers: {
        'xi-api-key': apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        text: `Welcome back, ${displayName}.`,
        model_id: 'eleven_multilingual_v2',
        voice_settings: {
          stability: 0.45,
          similarity_boost: 0.85,
          style: 0.15,
          use_speaker_boost: true,
        },
      }),
    });

    if (!response.ok) {
      error(`[VOICE] ElevenLabs error: ${response.status} ${await response.text()}`);
      return null;
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    fs.mkdirSync(path.join(__dirname, 'public', 'audio'), { recursive: true });
    fs.writeFileSync(outputPath, buffer);
    info(`[VOICE] Welcome audio cached at ${outputPath}`);
    return `/audio/welcome_${sanitised}.mp3`;
  } catch (err) {
    error(`[VOICE] Failed to generate welcome audio: ${err.message}`);
    return null;
  }
}

// ─────────────────────────────────────────────
// ROUTES
// ─────────────────────────────────────────────

function registerRoutes(app) {

  // GET /api/state — full system state
  app.get('/api/state', (req, res) => {
    const state = readState();
    // Also merge live rocoState values
    if (rocoState) {
      state.emailsSent = rocoState.emailsSent || 0;
      state.startedAt = rocoState.startedAt;
      state.deal = rocoState.deal;
    }
    res.json(state);
  });

  // POST /api/toggle — toggle a system switch
  app.post('/api/toggle', async (req, res) => {
    const { key, value } = req.body;
    // Accept both camelCase variants (followUpEnabled from HTML, followupEnabled legacy)
    const allowed = ['outreachEnabled', 'followupEnabled', 'followUpEnabled',
                     'enrichmentEnabled', 'researchEnabled', 'linkedinEnabled', 'rocoStatus'];
    if (!allowed.includes(key)) {
      return res.status(400).json({ error: 'Invalid toggle key' });
    }

    // Normalise followUpEnabled → followupEnabled internally
    const normKey = key === 'followUpEnabled' ? 'followupEnabled' : key;

    const state = readState();

    if (normKey === 'rocoStatus') {
      // Accept explicit value or flip
      if (value !== undefined) {
        state.rocoStatus = value === true || value === 'ACTIVE' ? 'ACTIVE' : 'PAUSED';
      } else {
        state.rocoStatus = state.rocoStatus === 'ACTIVE' ? 'PAUSED' : 'ACTIVE';
      }
      if (rocoState) rocoState.status = state.rocoStatus;
    } else {
      // Use explicit value if provided, otherwise flip
      state[normKey] = value !== undefined ? !!value : !state[normKey];
    }

    writeState(state);

    // Persist to Supabase so the orchestrator picks it up
    saveSessionState(state).catch(() => {});

    broadcastToAll({ type: 'STATE_UPDATE', state });
    pushActivity({ type: 'SYSTEM', action: 'Setting changed', note: `${normKey} → ${state[normKey] ?? state.rocoStatus}` });

    res.json({ key: normKey, newValue: state[normKey] ?? state.rocoStatus, timestamp: new Date().toISOString() });
  });

  // Legacy pause/resume — kept for backward compat
  app.post('/api/pause', (req, res) => {
    const state = readState();
    state.rocoStatus = 'PAUSED';
    writeState(state);
    if (rocoState) rocoState.status = 'PAUSED';
    broadcastToAll({ type: 'STATE_UPDATE', state });
    res.json({ ok: true, status: 'PAUSED' });
  });

  app.post('/api/resume', (req, res) => {
    const state = readState();
    state.rocoStatus = 'ACTIVE';
    writeState(state);
    if (rocoState) rocoState.status = 'ACTIVE';
    broadcastToAll({ type: 'STATE_UPDATE', state });
    res.json({ ok: true, status: 'ACTIVE' });
  });

  // Legacy status endpoint
  app.get('/api/status', (req, res) => {
    res.json({
      status: rocoState?.status || 'UNKNOWN',
      deal: rocoState?.deal || null,
      emailsSent: rocoState?.emailsSent || 0,
      startedAt: rocoState?.startedAt || null,
    });
  });

  // GET /api/activity — last 100 events
  app.get('/api/activity', (req, res) => {
    res.json(activityFeed.slice(-100));
  });

  // GET /api/pipeline — active pipeline contacts from Supabase, filtered by deal
  app.get('/api/pipeline', async (req, res) => {
    try {
      const { dealId } = req.query;
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });

      const EXCLUDED = ['Archived', 'Skipped', 'Inactive', 'Deleted — Do Not Contact', 'Suppressed — Opt Out'];

      let query = sb.from('contacts')
        .select('id, name, company_name, job_title, linkedin_url, investor_score, pipeline_stage, enrichment_status, email, notes, updated_at, invite_sent_at, deal_id')
        .not('pipeline_stage', 'in', `(${EXCLUDED.map(s => `"${s}"`).join(',')})`)
        .order('investor_score', { ascending: false })
        .limit(500);

      if (dealId) query = query.eq('deal_id', dealId);

      const { data, error: dbErr } = await query;
      if (dbErr) throw new Error(dbErr.message);

      // Build deal name lookup
      const dealIds = [...new Set((data || []).map(c => c.deal_id).filter(Boolean))];
      const dealNames = {};
      if (dealIds.length) {
        const { data: dealRows } = await sb.from('deals').select('id, name').in('id', dealIds);
        (dealRows || []).forEach(d => { dealNames[d.id] = d.name; });
      }

      const mapped = (data || []).map(c => ({
        id: c.id,
        name: c.name,
        firm: c.company_name || '',
        jobTitle: c.job_title || '',
        score: c.investor_score,
        stage: c.pipeline_stage,
        lastContacted: c.invite_sent_at || c.updated_at,
        enrichmentStatus: c.enrichment_status,
        email: c.email,
        linkedinUrl: c.linkedin_url,
        notes: c.notes,
        deal_id: c.deal_id,
        dealName: dealNames[c.deal_id] || '',
      }));
      res.json(mapped);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // GET /api/queue — approval queue with IDs (in-memory + Supabase LinkedIn DMs)
  app.get('/api/queue', async (req, res) => {
    const inMemory = getPendingApprovals();
    try {
      const sb = getSupabase();
      if (sb) {
        const { data: sbItems } = await sb.from('approval_queue')
          .select('id, contact_id, contact_name, firm, stage, body, created_at')
          .eq('stage', 'LinkedIn DM')
          .eq('status', 'pending')
          .order('created_at', { ascending: true })
          .catch(() => ({ data: [] }));
        const sbMapped = (sbItems || []).map(r => ({
          id:       r.id,
          name:     r.contact_name || '',
          firm:     r.firm || '',
          stage:    r.stage,
          body:     r.body || '',
          channel:  'linkedin',
          queuedAt: r.created_at,
          _supabaseOnly: true,
        }));
        return res.json([...inMemory, ...sbMapped]);
      }
    } catch {}
    res.json(inMemory);
  });

  // Alias for backward compat
  app.get('/api/approvals', (req, res) => {
    res.json(getPendingApprovals());
  });

  // POST /api/approve — approve an email from dashboard
  app.post('/api/approve', async (req, res) => {
    const { id, subjectChoice, editedBody, subject } = req.body;
    if (!id) return res.status(400).json({ error: 'id required' });

    // Find the approval to get contact info before resolving
    const pending = getPendingApprovals();
    const item = pending.find(p => String(p.id) === String(id));

    const resolved = resolveApprovalFromDashboard(
      id,
      'approve',
      subject || item?.subject,
      editedBody
    );

    if (!resolved) {
      // Fall back: check Supabase for a LinkedIn DM queue item (webhook-triggered, not in-memory)
      const sb = getSupabase();
      if (sb) {
        let sbItem = null;
        try {
          const { data } = await sb.from('approval_queue')
            .select('id, contact_id, contact_name, firm, body, stage')
            .eq('id', id)
            .eq('status', 'pending')
            .single();
          sbItem = data;
        } catch { /* not found or not pending */ }

        if (sbItem?.contact_id && sbItem?.stage === 'LinkedIn DM') {
          try {
            const text = editedBody || sbItem.body || '';
            await sendApprovedLinkedInDM({ contactId: sbItem.contact_id, text });
            await sb.from('approval_queue').update({
              status:  'sent',
              sent_at: new Date().toISOString(),
            }).eq('id', sbItem.id).catch(() => {});
            pushActivity({
              type:   'linkedin',
              action: `LinkedIn DM sent: ${sbItem.contact_name || ''}`,
              note:   sbItem.firm || '',
            });
            return res.json({ success: true, message: 'LinkedIn DM sent' });
          } catch (err) {
            console.error('[/api/approve] LinkedIn DM send error:', err.message);
            return res.status(500).json({ error: 'Send failed: ' + err.message });
          }
        }
      }
      return res.status(404).json({ error: 'Approval not found — may have already been handled' });
    }

    pushActivity({
      type: 'APPROVAL',
      action: 'Approved via Dashboard',
      note: item ? `${item.name} @ ${item.firm}` : 'Unknown contact',
    });

    res.json({ success: true, message: 'Approval sent — orchestrator will fire the email' });
  });

  // POST /api/skip-approval — skip an approval from dashboard (hard delete)
  app.post('/api/skip-approval', async (req, res) => {
    const { id } = req.body;
    if (!id) return res.status(400).json({ error: 'id required' });

    // Resolve in-memory (id is the Telegram message ID)
    resolveApprovalFromDashboard(id, 'skip');

    // Hard delete from Supabase approval_queue by id (handles both telegram_msg_id and UUID items)
    await deleteApprovalFromQueue(id).catch(() => {});
    const sb = getSupabase();
    if (sb) await sb.from('approval_queue').update({ status: 'skipped' }).eq('id', id).catch(() => {});

    res.json({ success: true });
  });

  // POST /api/edit-approval — send edit instructions
  app.post('/api/edit-approval', (req, res) => {
    const { id, instructions } = req.body;
    if (!id || !instructions) return res.status(400).json({ error: 'id and instructions required' });

    const resolved = resolveApprovalFromDashboard(id, 'edit', null, instructions);
    if (!resolved) return res.status(404).json({ error: 'Approval not found' });

    res.json({ success: true, message: 'Edit instructions sent — orchestrator will redraft' });
  });

  // GET /api/health — API health status + process diagnostics
  app.get('/api/health', (req, res) => {
    try {
      const apiHealth = getApiHealth();
      // Flatten each service to a string status for the dashboard frontend
      const serviceStatuses = {};
      for (const [svc, val] of Object.entries(apiHealth)) {
        serviceStatuses[svc] = typeof val === 'object' && val !== null
          ? String(val.status ?? 'unknown')
          : String(val ?? 'unknown');
      }
      res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        uptime_seconds: Math.floor(process.uptime()),
        memory_mb: Math.floor(process.memoryUsage().heapUsed / 1024 / 1024),
        websocket_clients: wss ? wss.clients.size : 0,
        node_version: process.version,
        ...serviceStatuses,
      });
    } catch (err) {
      res.status(500).json({ status: 'error', error: err.message });
    }
  });

  // GET /api/stats — stats summary (Supabase-based, never calls Notion)
  app.get('/api/stats', async (req, res) => {
    const stats = {
      emails_sent: 0,
      response_rate: 0,
      active_prospects: 0,
      approval_queue: getPendingApprovals().length,
      capital_committed: 0,
      active_deals: 0,
      // Legacy field names for dashboard compat
      emailsSent: 0,
      activeProspects: 0,
      queueCount: getPendingApprovals().length,
      committed: 0,
      targetAmount: 0,
      dealName: '—',
    };

    try {
      const sb = getSupabase();
      if (sb) {
        // Resolve active deal IDs once — all per-deal stats use this
        let activeDealIds = [];     // non-paused (for orchestrator-relevant stats)
        let allActiveDealIds = [];  // all active incl. paused (for counting totals)
        let allDealsData  = [];
        try {
          const { data: allDeals } = await sb.from('deals').select('id, name, committed_amount, target_amount, status, paused');
          allDealsData      = allDeals || [];
          activeDealIds     = allDealsData.filter(d => d.status === 'ACTIVE' && !d.paused).map(d => d.id);
          allActiveDealIds  = allDealsData.filter(d => d.status === 'ACTIVE').map(d => d.id);
        } catch (e) { console.warn('/api/stats deal ids:', e.message); }
        const safeActiveIds    = activeDealIds.length > 0    ? activeDealIds    : ['00000000-0000-0000-0000-000000000000'];
        const safeAllActiveIds = allActiveDealIds.length > 0 ? allActiveDealIds : ['00000000-0000-0000-0000-000000000000'];

        // Emails sent — count from activity_log (EMAIL_SENT events) — include paused deals
        try {
          const { count } = await sb.from('activity_log').select('id', { count: 'exact', head: true })
            .eq('event_type', 'EMAIL_SENT')
            .in('deal_id', safeAllActiveIds);
          stats.emails_sent = count || 0;
          stats.emailsSent  = count || 0;
        } catch (e) { console.warn('/api/stats emails_sent:', e.message); }

        // Email replies — counted from replies table (webhook-driven), active deals only
        // Response rate = replies / emails sent
        try {
          const { count: replied } = await sb.from('replies').select('id', { count: 'exact', head: true })
            .in('deal_id', safeAllActiveIds);
          stats.emails_replied  = replied || 0;
          stats.response_rate   = stats.emails_sent > 0
            ? Math.round(((replied || 0) / stats.emails_sent) * 100)
            : 0;
          stats.responseRate    = stats.response_rate;
        } catch (e) { console.warn('/api/stats response_rate:', e.message); }

        // Pending approvals
        try {
          const { count } = await sb.from('emails').select('id', { count: 'exact', head: true })
            .eq('status', 'pending_approval')
            .in('deal_id', safeActiveIds);
          stats.approval_queue = (count || 0) + getPendingApprovals().length;
          stats.queueCount     = stats.approval_queue;
        } catch (e) { console.warn('/api/stats approval_queue:', e.message); }

        // Active prospects — all contacts in active deals (incl. paused) that are not archived/skipped
        try {
          const { count } = await sb.from('contacts').select('id', { count: 'exact', head: true })
            .in('deal_id', safeAllActiveIds)
            .not('pipeline_stage', 'in', '("Archived","ARCHIVED","archived","Skipped","skipped_no_name","skipped_no_linkedin","skipped_duplicate_email","Inactive","Suppressed — Opt Out","Deleted — Do Not Contact")');
          stats.active_prospects = count || 0;
          stats.activeProspects  = count || 0;
        } catch (e) { console.warn('/api/stats prospects:', e.message); }

        // Deal counts + capital
        try {
          const activeDealsData = allDealsData.filter(d => d.status === 'ACTIVE');
          stats.active_deals         = activeDealsData.length;
          stats.total_deals_launched = allDealsData.length;
          stats.total_funds_raised   = allDealsData.reduce((s, d) => s + (d.committed_amount || 0), 0);
          stats.capital_committed    = stats.total_funds_raised;
          stats.committed            = stats.total_funds_raised;
          stats.active_committed     = activeDealsData.reduce((s, d) => s + (d.committed_amount || 0), 0);
          if (activeDealsData.length) {
            stats.targetAmount = activeDealsData[0].target_amount || 0;
            stats.dealName     = activeDealsData[0].name || '—';
          }
        } catch (e) { console.warn('/api/stats deals:', e.message); }

        // LinkedIn metrics — all active deals incl. paused
        try {
          const { count: liInvites } = await sb.from('contacts').select('id', { count: 'exact', head: true })
            .in('deal_id', safeAllActiveIds)
            .not('invite_sent_at', 'is', null);
          stats.li_invites_sent = liInvites || 0;

          const { count: liAccepted } = await sb.from('contacts').select('id', { count: 'exact', head: true })
            .in('deal_id', safeAllActiveIds)
            .in('pipeline_stage', ['invite_accepted', 'dm_sent', 'Replied', 'Meeting Booked', 'Meeting Scheduled']);
          stats.li_acceptance_rate = stats.li_invites_sent > 0
            ? Math.round(((liAccepted || 0) / stats.li_invites_sent) * 100)
            : 0;

          const { count: liDms } = await sb.from('contacts').select('id', { count: 'exact', head: true })
            .in('deal_id', safeAllActiveIds)
            .in('pipeline_stage', ['dm_sent', 'Replied', 'Meeting Booked', 'Meeting Scheduled'])
            .eq('outreach_channel', 'linkedin');
          stats.li_dms_sent = liDms || 0;

          const { count: liReplied } = await sb.from('contacts').select('id', { count: 'exact', head: true })
            .in('deal_id', safeAllActiveIds)
            .in('pipeline_stage', ['Replied', 'Meeting Booked', 'Meeting Scheduled'])
            .eq('outreach_channel', 'linkedin');
          stats.li_dm_response_rate = stats.li_dms_sent > 0
            ? Math.round(((liReplied || 0) / stats.li_dms_sent) * 100)
            : 0;
        } catch (e) { console.warn('/api/stats linkedin metrics:', e.message); }
      }
    } catch (err) {
      console.error('/api/stats outer error:', err.message);
    }

    res.json(stats);
  });

  // POST /api/action — manual triggers
  app.post('/api/action', async (req, res) => {
    const { action } = req.body;

    switch (action) {
      case 'pause_all': {
        const state = readState();
        state.rocoStatus = 'PAUSED';
        state.pausedUntil = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
        writeState(state);
        if (rocoState) rocoState.status = 'PAUSED';
        broadcastToAll({ type: 'STATE_UPDATE', state });
        pushActivity({ type: 'SYSTEM', action: 'Paused All', note: 'Manual 24h pause via dashboard' });
        return res.json({ success: true, message: 'Roco paused for 24 hours' });
      }

      case 'resume_all': {
        const state = readState();
        state.rocoStatus = 'ACTIVE';
        state.pausedUntil = null;
        writeState(state);
        if (rocoState) rocoState.status = 'ACTIVE';
        broadcastToAll({ type: 'STATE_UPDATE', state });
        pushActivity({ type: 'SYSTEM', action: 'Resumed', note: 'Manual resume via dashboard' });
        return res.json({ success: true, message: 'Roco resumed' });
      }

      case 'flush_queue': {
        const pending = getPendingApprovals();
        for (const item of pending) {
          resolveApprovalFromDashboard(item.id, 'skip');
        }
        pushActivity({ type: 'SYSTEM', action: 'Queue Flushed', note: `${pending.length} items skipped` });
        return res.json({ success: true, message: `Flushed ${pending.length} pending approvals` });
      }

      case 'run_research': {
        try {
          const { getActiveDeals, getDeal } = await import('../core/supabaseSync.js');
          const { runFirmResearch } = await import('../research/firmResearcher.js');
          const { dealId } = req.body;
          let deals;
          if (dealId) {
            const d = await getDeal(dealId);
            if (!d) return res.status(404).json({ error: 'Deal not found' });
            deals = [d];
          } else {
            deals = await getActiveDeals();
            if (!deals.length) return res.json({ success: false, message: 'No active deals found' });
          }
          pushActivity({ type: 'RESEARCH', action: 'Research Started', note: `Firm-first research running for: ${deals.map(d => d.name).join(', ')}` });
          Promise.allSettled(deals.map(d => runFirmResearch(d))).then(results => {
            const total = results.reduce((sum, r) => sum + (typeof r.value === 'number' ? r.value : 0), 0);
            pushActivity({ type: 'RESEARCH', action: 'Research Complete', note: `Found ${total} new contacts for ${deals.map(d => d.name).join(', ')}` });
          });
          return res.json({ success: true, message: `Firm research started for ${deals.map(d => d.name).join(', ')} — firms and contacts will appear shortly` });
        } catch (e) {
          return res.status(500).json({ error: e.message });
        }
      }

      case 'run_enrichment': {
        try {
          const { runManualEnrich } = await import('../core/orchestrator.js');
          const { dealId } = req.body;
          let deals;
          if (dealId) {
            const sb = getSupabase();
            const { data } = await sb.from('deals').select('*').eq('id', dealId).single();
            if (!data) return res.json({ success: false, message: 'Deal not found' });
            deals = [data];
          } else {
            const { getActiveDeals } = await import('../core/supabaseSync.js');
            deals = await getActiveDeals();
            if (!deals.length) return res.json({ success: false, message: 'No active deals found' });
          }
          const state = readState();
          // Run deals sequentially so activity feed is readable contact-by-contact
          (async () => {
            for (const d of deals) {
              await runManualEnrich(d, state);
            }
          })();
          return res.json({ success: true, message: `Manual enrichment started for ${deals.map(d => d.name).join(', ')} — watch the activity feed for live updates` });
        } catch (e) {
          return res.status(500).json({ error: e.message });
        }
      }

      case 'suppress_firm': {
        const { firm } = req.body;
        if (!firm) return res.status(400).json({ error: 'firm is required' });
        try {
          const sb = getSupabase();
          // Mark all contacts from this firm as Archived across all deals
          await sb.from('contacts').update({ pipeline_stage: 'Archived' })
            .ilike('company_name', firm);
          pushActivity({ type: 'SYSTEM', action: 'Firm Suppressed', note: `All contacts from "${firm}" archived` });
          return res.json({ success: true, message: `Firm "${firm}" suppressed` });
        } catch (e) {
          return res.status(500).json({ error: e.message });
        }
      }

      case 'run_analytics': {
        try {
          const { runWeeklyAnalytics } = await import('../core/analyticsEngine.js');
          runWeeklyAnalytics().catch(err =>
            console.error('[ANALYTICS] Manual run failed:', err.message)
          );
          pushActivity({ type: 'SYSTEM', action: 'Analytics Started', note: 'Weekly analytics running — recommendations will appear shortly' });
          return res.json({ success: true, message: 'Analytics queued — check the Analytics tab in a minute' });
        } catch (e) {
          return res.status(500).json({ error: e.message });
        }
      }

      default:
        return res.status(400).json({ error: `Unknown action: ${req.body?.action}` });
    }
  });

  // POST /api/deal — update active deal
  app.post('/api/deal', (req, res) => {
    const { dealName, targetAmount, currentCommitted, sector, geography, description } = req.body;

    const state = readState();
    if (!state.activeDeals || state.activeDeals.length === 0) {
      state.activeDeals = [{ id: 'deal_001' }];
    }

    const deal = state.activeDeals[0];
    if (dealName !== undefined) deal.name = dealName;
    if (targetAmount !== undefined) deal.targetAmount = Number(targetAmount);
    if (currentCommitted !== undefined) deal.currentCommitted = Number(currentCommitted);
    if (sector !== undefined) deal.sector = sector;
    if (geography !== undefined) deal.geography = geography;
    if (description !== undefined) deal.description = description;

    writeState(state);
    broadcastToAll({ type: 'DEAL_UPDATE', deal });

    pushActivity({ type: 'SYSTEM', action: 'Deal Updated', note: deal.name || 'Deal' });
    res.json({ success: true, deal });
  });

  // POST /api/contact/:id/stage — update a contact's pipeline stage
  app.post('/api/contact/:id/stage', async (req, res) => {
    const { id } = req.params;
    const { stage } = req.body;
    if (!stage) return res.status(400).json({ error: 'stage required' });
    try {
      await updateContact(id, { pipelineStage: stage });
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/contacts/:id/reactivate — move an archived contact back into the pipeline
  app.post('/api/contacts/:id/reactivate', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: contact } = await sb.from('contacts')
        .select('id, name, deal_id, pipeline_stage')
        .eq('id', req.params.id).single();
      if (!contact) return res.status(404).json({ error: 'Contact not found' });
      if (contact.pipeline_stage !== 'Archived') return res.status(400).json({ error: 'Contact is not archived' });
      await sb.from('contacts')
        .update({ pipeline_stage: 'Ranked', enrichment_status: 'Pending' })
        .eq('id', req.params.id);
      pushActivity({ type: 'PIPELINE', action: 'Contact Reactivated', note: `${contact.name} moved back into pipeline` });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/contacts/:id — fetch full contact record for prospect drawer
  app.get('/api/contacts/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('contacts')
        .select('*').eq('id', req.params.id).single();
      if (dbErr) throw new Error(dbErr.message);
      if (!data) return res.status(404).json({ error: 'Contact not found' });
      res.json(data);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/contacts/:id/conversation — full conversation history for prospect drawer
  app.get('/api/contacts/:id/conversation', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb
        .from('conversation_messages')
        .select('*')
        .eq('contact_id', req.params.id)
        .order('sent_at', { ascending: true });
      if (dbErr) throw new Error(dbErr.message);
      res.json({ messages: data || [] });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/contacts/:id — remove contact from pipeline permanently
  app.delete('/api/contacts/:id', async (req, res) => {
    const { id } = req.params;
    try {
      const sb = getSupabase();

      // 1. Delete from Supabase contacts table
      if (sb) await sb.from('contacts').delete().eq('id', id);

      // 2. Delete from deal_contacts join table
      if (sb) await sb.from('deal_contacts').delete().eq('contact_id', id);

      // 3. Delete any pending draft emails for this contact
      if (sb) await sb.from('emails')
        .delete()
        .eq('contact_id', id)
        .in('status', ['draft', 'pending_approval']);

      // 4. Remove from Notion — set Inactive (guaranteed valid) AND archive
      //    Either one alone is enough to hide the contact from all pipeline queries
      try {
        await updateContact(id, { pipelineStage: 'Inactive' });
      } catch (notionErr) {
        error('Notion stage update failed on delete', { id, err: notionErr.message });
      }
      try {
        await archiveContact(id);
      } catch (archiveErr) {
        error('Notion archive failed on delete (contact may still be hidden via Inactive stage)', { id, err: archiveErr.message });
      }

      // 5. Log
      pushActivity({ type: 'SYSTEM', action: 'Contact Deleted', note: `Contact ${id} removed from pipeline` });
      await sbLogActivity({
        contactId: id,
        eventType: 'CONTACT_DELETED',
        summary: 'Contact deleted from pipeline — will not be contacted',
      }).catch(() => {});

      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Legacy raise-progress endpoint
  app.post('/api/raise-progress', (req, res) => {
    const { committed, target } = req.body;
    if (rocoState) {
      rocoState.committed = committed;
      rocoState.target = target || rocoState?.deal?.raiseAmount;
    }
    const state = readState();
    if (state.activeDeals?.[0]) {
      if (committed !== undefined) state.activeDeals[0].currentCommitted = Number(committed);
      writeState(state);
    }
    broadcastToAll({ type: 'raise_update', committed, target });
    res.json({ ok: true });
  });

  // ─── DEALS ────────────────────────────────────────────────────────────────

  // GET /api/deals — all deals with live contact/email/response counts
  app.get('/api/deals', async (req, res) => {
    try {
      const deals = await getAllDeals();
      const sb = getSupabase();
      if (!sb || !deals?.length) return res.json(deals || []);

      // Enrich each deal with live counts from contacts table
      const dealIds = deals.map(d => d.id);
      const { data: allContacts } = await sb.from('contacts')
        .select('deal_id, pipeline_stage, response_received, last_email_sent_at')
        .in('deal_id', dealIds);

      const byDeal = {};
      for (const c of (allContacts || [])) {
        if (!byDeal[c.deal_id]) byDeal[c.deal_id] = [];
        byDeal[c.deal_id].push(c);
      }

      const ARCHIVED_STAGES = ['Archived','ARCHIVED','archived','Skipped','skipped_no_name','skipped_no_linkedin','Inactive','Suppressed — Opt Out','Deleted — Do Not Contact'];
      const OUTREACHED_STAGES = ['email_sent','dm_sent','invite_sent','invite_accepted','Replied','In Conversation','Meeting Booked','Meeting Scheduled'];

      // Fetch current campaign batch status for all deals
      const { data: campaignBatches } = await sb.from('campaign_batches')
        .select('deal_id, id, batch_number, status, ranked_firms, target_firms')
        .in('deal_id', dealIds)
        .in('status', ['researching', 'pending_approval', 'approved'])
        .order('created_at', { ascending: false });

      // Map to most-recent batch per deal
      const batchByDeal = {};
      for (const b of (campaignBatches || [])) {
        if (!batchByDeal[b.deal_id]) batchByDeal[b.deal_id] = b;
      }

      const enriched = deals.map(deal => {
        const contacts = byDeal[deal.id] || [];
        const totalContacts = contacts.length; // all scraped
        const emailsSentCount = contacts.filter(c => c.last_email_sent_at || c.pipeline_stage === 'email_sent').length;
        const totalOutreached = contacts.filter(c => OUTREACHED_STAGES.includes(c.pipeline_stage)).length;
        const responses = contacts.filter(c => c.response_received === true).length;
        const responseRate = totalOutreached > 0 ? Math.round((responses / totalOutreached) * 100) : 0;
        const activeProspects = contacts.filter(c => !ARCHIVED_STAGES.includes(c.pipeline_stage)).length;
        const batch = batchByDeal[deal.id] || null;

        return {
          ...deal,
          contacts: totalContacts,
          active_prospects: activeProspects,
          emails_sent: emailsSentCount,
          response_rate: responseRate,
          live_contacts: totalContacts,
          live_active_prospects: activeProspects,
          live_emails_sent: emailsSentCount,
          live_response_rate: responseRate,
          live_responses: responses,
          current_batch_status: batch?.status || null,
          current_batch_number: batch?.batch_number || null,
          current_batch_id: batch?.id || null,
          current_batch_ranked_firms: batch?.ranked_firms || 0,
          current_batch_target_firms: batch?.target_firms || 20,
        };
      });

      res.json(enriched);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/history — archived/closed deals
  app.get('/api/deals/history', async (req, res) => {
    try {
      const deals = await getAllDeals();
      res.json(deals.filter(d => d.status !== 'ACTIVE'));
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id — single deal with batches + schedule status
  app.get('/api/deals/:id', async (req, res) => {
    try {
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });
      const [batches, windowStatus, windowViz] = await Promise.all([
        getBatchSummary(deal.id),
        Promise.resolve(getWindowStatus(deal, null)),
        Promise.resolve(getWindowVisualization(deal)),
      ]);

      // Fetch parsed_deal_info from linked document if available
      let parsed_deal_info = null;
      try {
        const sb = getSupabase();
        if (sb) {
          const { data: doc } = await sb.from('deal_documents')
            .select('parsed_deal_info')
            .eq('deal_id', deal.id)
            .order('created_at', { ascending: false })
            .limit(1)
            .maybeSingle();
          parsed_deal_info = doc?.parsed_deal_info || null;
        }
      } catch {}

      res.json({ ...deal, batches, windowStatus, windowVisualization: windowViz, parsed_deal_info });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/create — create new deal
  // multer handles multipart/form-data (CSV upload path); JSON path uses express.json()
  const dealUpload = multer({ storage: multer.memoryStorage() }).single('csvFile');

  app.post('/api/deals/create', (req, res, next) => {
    dealUpload(req, res, (err) => {
      if (err) console.warn('[DEAL LAUNCH] multer error (non-fatal):', err.message);
      next();
    });
  }, async (req, res) => {
    try {
      console.log('[DEAL LAUNCH] Content-Type:', req.headers['content-type']);
      console.log('[DEAL LAUNCH] Body keys:', Object.keys(req.body || {}));
      console.log('[DEAL LAUNCH] Body:', JSON.stringify(req.body));

      // Auto-detect currency from deal brief text
      const detectCurrency = (texts) => {
        const combined = texts.filter(Boolean).join(' ');
        // Check for explicit currency field first
        if (req.body.currency) return req.body.currency.toUpperCase();
        // Count occurrences of each symbol — most frequent wins
        const gbp = (combined.match(/£/g) || []).length + (combined.match(/\bpound[s]?\b|\bGBP\b/gi) || []).length;
        const usd = (combined.match(/\$/g) || []).length + (combined.match(/\bdollar[s]?\b|\bUSD\b/gi) || []).length;
        const eur = (combined.match(/€/g) || []).length + (combined.match(/\beuro[s]?\b|\bEUR\b/gi) || []).length;
        if (gbp > usd && gbp > eur) return 'GBP';
        if (eur > usd && eur > gbp) return 'EUR';
        return 'USD'; // default
      };

      // Strip currency symbols, commas, spaces before parsing monetary values
      const parseAmount = (val) => {
        if (!val) return 0;
        const cleaned = String(val).replace(/[£$€,\s]/g, '');
        // Handle shorthand: 5m = 5000000, 500k = 500000
        const shorthand = cleaned.match(/^([\d.]+)\s*([kmb]?)$/i);
        if (shorthand) {
          const num = parseFloat(shorthand[1]);
          const suffix = (shorthand[2] || '').toLowerCase();
          if (suffix === 'k') return num * 1_000;
          if (suffix === 'm') return num * 1_000_000;
          if (suffix === 'b') return num * 1_000_000_000;
          return num || 0;
        }
        return parseFloat(cleaned) || 0;
      };

      const name = (
        req.body?.name ||
        req.body?.dealName ||
        req.body?.deal_name ||
        req.body?.['deal-name'] ||
        ''
      ).toString().trim();

      if (!name) {
        console.error('[DEAL LAUNCH] Name missing. Full body:', JSON.stringify(req.body));
        return res.status(400).json({
          error: 'Deal name is required',
          received_keys: Object.keys(req.body || {}),
        });
      }

      // geography arrives as JSON string when sent via FormData
      let geography = req.body.geography || '';
      if (typeof geography === 'string' && geography.startsWith('[')) {
        try { geography = JSON.parse(geography); } catch {}
      }
      if (Array.isArray(geography)) geography = geography.join(', ');

      const descriptionText  = req.body.description || '';
      const keyMetricsText   = req.body.keyMetrics || req.body.key_metrics || '';
      const investorText     = req.body.investorProfile || req.body.investor_profile || '';
      const targetText       = req.body.targetAmount || req.body.target_amount || '';
      const detectedCurrency = detectCurrency([descriptionText, keyMetricsText, investorText, targetText, name]);

      const deal = {
        name,
        currency:              detectedCurrency,
        raise_type:            req.body.raiseType || req.body.raise_type || 'Equity',
        target_amount:         parseAmount(req.body.targetAmount || req.body.target_amount),
        min_cheque:            parseAmount(req.body.minCheque || req.body.min_cheque),
        max_cheque:            parseAmount(req.body.maxCheque || req.body.max_cheque),
        sector:                req.body.sector || '',
        geography,
        description:           descriptionText,
        key_metrics:           keyMetricsText,
        investor_profile:      investorText,
        deck_url:              req.body.deckUrl || req.body.deck_url || '',
        linkedin_seeds:        req.body.linkedinSeeds || req.body.linkedin_seeds || '',
        batch_size:            parseInt(req.body.batchSize || 15),
        max_emails_per_day:    parseInt(req.body.maxEmailsPerDay || 20),
        max_emails_per_hour:   parseInt(req.body.maxEmailsPerHour || 5),
        max_contacts_per_firm: parseInt(req.body.maxContactsPerFirm || 3),
        max_total_outreach:    parseInt(req.body.maxTotalOutreach || 200),
        send_from:        req.body.emailFrom  || req.body.sendFrom  || req.body.send_from  || '06:00',
        send_until:       req.body.emailUntil || req.body.sendUntil || req.body.send_until || '08:00',
        li_connect_from:  req.body.liConnectFrom  || null,
        li_connect_until: req.body.liConnectUntil || null,
        li_dm_from:       req.body.liDmFrom  || '20:00',
        li_dm_until:      req.body.liDmUntil || '23:00',
        timezone:              req.body.timezone || 'America/New_York',
        active_days:           req.body.activeDays || 'Mon,Tue,Wed,Thu,Fri',
        status:                'ACTIVE',
        committed_amount:      0,
        emails_sent:           0,
        response_rate:         0,
        active_prospects:      0,
        created_by:            'dashboard',
      };
      console.log(`[DEAL LAUNCH] Detected currency: ${detectedCurrency} for "${name}"`);

      const supabase = getSupabase();
      const { data: savedDeal, error: supabaseError } = await supabase
        .from('deals')
        .insert(deal)
        .select()
        .single();

      if (supabaseError) {
        error('Supabase deal insert error: ' + supabaseError.message);
        return res.status(500).json({ error: supabaseError.message });
      }

      // Save priority lists if provided
      const priorityLists = (() => {
        try { return JSON.parse(req.body.priority_lists || '[]'); } catch { return []; }
      })();
      if (priorityLists.length) {
        await supabase.from('deal_list_priorities').insert(
          priorityLists.map(l => ({
            deal_id: savedDeal.id,
            list_id: l.list_id,
            list_name: l.list_name,
            priority_order: l.priority_order,
            status: 'pending',
          }))
        ).catch(e => console.warn('[DEAL] priority lists insert:', e.message));
      }

      // Save exclusion list if provided
      const exclusions = (() => {
        try { return JSON.parse(req.body.exclusions || '[]'); } catch { return []; }
      })();
      if (exclusions?.length > 0) {
        const exclusionRows = exclusions.map(e => ({
          deal_id:     savedDeal.id,
          firm_name:   e.firm_name   ? e.firm_name.toLowerCase().trim()   : null,
          person_name: e.person_name ? e.person_name.toLowerCase().trim() : null,
          email:       e.email       ? e.email.toLowerCase().trim()       : null,
          added_by:    req.session?.displayName || 'Dom',
        }));
        await getSupabase().from('deal_exclusions').insert(exclusionRows)
          .catch(e => console.warn('[DEAL] exclusions insert:', e.message));
        pushActivity({ type: 'system', action: `Exclusion list loaded: ${exclusions.length} entries`, note: name });
      }

      pushActivity({ type: 'SYSTEM', action: 'Deal Launched', note: name });
      await sbLogActivity({ dealId: savedDeal.id, eventType: 'DEAL_CREATED', summary: `Deal "${name}" created` }).catch(() => {});
      const tgTarget = savedDeal.target_amount > 0 ? '£' + Number(savedDeal.target_amount).toLocaleString() : '—';
      await sendTelegram(`New deal launched: ${name}\nTarget: ${tgTarget}\nSector: ${savedDeal.sector || '—'}\nRaise Type: ${savedDeal.raise_type || '—'}\n\nRoco is on it.`).catch(() => {});

      // Auto-insert deck_url into deal_assets
      const deckUrl = savedDeal.deck_url || '';
      const calendlyUrl = req.body.calendlyUrl || req.body.calendly_url || '';
      let launchAssets = [];
      try { launchAssets = JSON.parse(req.body.launchAssets || '[]'); } catch {}

      const assetRows = [];
      if (deckUrl) assetRows.push({ deal_id: savedDeal.id, name: 'Pitch Deck', asset_type: 'deck', url: deckUrl });
      if (calendlyUrl) assetRows.push({ deal_id: savedDeal.id, name: 'Book a Call', asset_type: 'calendly', url: calendlyUrl });
      for (const a of launchAssets) {
        if (a.name && a.url && a.asset_type) assetRows.push({ deal_id: savedDeal.id, name: a.name, asset_type: a.asset_type, url: a.url });
      }
      if (assetRows.length) {
        const sbAssets = getSupabase();
        if (sbAssets) {
          try { await sbAssets.from('deal_assets').insert(assetRows); }
          catch(e) { console.warn('[DEAL] asset insert:', e.message); }
        }
      }

      // Link previously-parsed document to this deal
      const documentId = req.body.document_id || req.body.documentId;
      if (documentId) {
        try {
          await getSupabase()?.from('deal_documents')
            .update({ deal_id: savedDeal.id })
            .eq('id', documentId);
        } catch (e) { console.warn('[DEAL] document link failed:', e.message); }
      }

      broadcastToAll({ type: 'DEAL_CREATED', deal: savedDeal });
      info(`Deal created: ${name} (id: ${savedDeal.id})`);
      res.json({ success: true, deal: savedDeal });

      // Fire full research sequence immediately — non-blocking
      setImmediate(async () => {
        const broadcast = (msg, type = 'research') => pushActivity({
          type, action: msg, note: '', dealId: savedDeal.id, deal_name: savedDeal.name,
        });

        try {
          broadcast(`Deal launched: ${savedDeal.name}`, 'system');

          // Auto-generate deal-specific templates
          try {
            const { generateDealTemplates } = await import('../core/templateGenerator.js');
            await generateDealTemplates(savedDeal, req.session?.displayName || 'Dom');
            broadcast('Templates generated', 'system');
          } catch (e) {
            console.warn('[DEAL LAUNCH] Template generation failed:', e.message);
          }

          // Seed default outreach sequence for this deal
          try {
            const sb = getSupabase();
            if (sb) {
              await sb.from('deal_sequence').insert({
                deal_id: savedDeal.id,
                steps: [
                  { step: 1, type: 'email',           label: 'email_intro',      delay_days: 0,  description: 'Cold intro email' },
                  { step: 2, type: 'linkedin_invite', label: 'linkedin_invite',  delay_days: 0,  description: 'LinkedIn connection request' },
                  { step: 3, type: 'linkedin_dm',     label: 'linkedin_dm_1',    delay_days: 0,  description: 'LinkedIn DM after connection accepted' },
                  { step: 4, type: 'email',           label: 'email_followup_1', delay_days: 7,  description: 'Follow-up email if no reply' },
                  { step: 5, type: 'linkedin_dm',     label: 'linkedin_dm_2',    delay_days: 0,  description: 'LinkedIn follow-up DM' },
                  { step: 6, type: 'email',           label: 'email_followup_2', delay_days: 14, description: 'Second follow-up if no reply' },
                ],
                sending_window: { start_hour: 8, end_hour: 18, days: [1,2,3,4,5] },
                updated_at: new Date().toISOString(),
              });
            }
          } catch (e) {
            console.warn('[DEAL LAUNCH] Sequence seed failed:', e.message);
          }

          // Step 1: CSV ingest if file was uploaded
          const csvContent = req.body?.csvData ||
            (req.file?.buffer ? req.file.buffer.toString('utf-8') : null);

          if (csvContent) {
            broadcast('Importing CSV investors...');
            try {
              const { ingestCSV } = await import('../research/csvIngestor.js');
              const count = await ingestCSV({
                csvContent,
                dealId: savedDeal.id,
                dealName: savedDeal.name,
                broadcastFn: (msg) => broadcast(msg),
              });
              broadcast(`CSV: ${count} contacts imported`, 'research');
            } catch (e) {
              console.error('[DEAL LAUNCH] CSV ingest failed:', e.message);
              broadcast(`CSV ingest error: ${e.message}`, 'error');
            }
          }

          // Step 2: Firm-first research (Grok/Gemini finds firms, then contacts at each firm)
          broadcast(`Starting firm research for ${savedDeal.name}...`);
          try {
            const { runFirmResearch } = await import('../research/firmResearcher.js');
            await runFirmResearch(savedDeal);
          } catch (e) {
            console.error('[DEAL LAUNCH] Firm research failed:', e.message);
            broadcast(`Research error: ${e.message}`, 'error');
            // Fallback to legacy research
            try {
              const { runDealResearch } = await import('../research/dealResearcher.js');
              await runDealResearch(savedDeal);
            } catch (e2) {
              broadcast(`Legacy research also failed: ${e2.message}`, 'error');
            }
          }

          // Step 3: Full orchestrator cycle (rank, enrich, sync, outreach)
          try {
            const { triggerImmediateRun } = await import('../core/orchestrator.js');
            await triggerImmediateRun(savedDeal.id);
          } catch (e) {
            info(`Orchestrator immediate trigger failed (will pick up next cycle): ${e.message}`);
          }

        } catch (err) {
          console.error('[DEAL LAUNCH] Research sequence failed:', err.message);
          broadcast(`Research error: ${err.message}`, 'error');
        }
      });
    } catch (err) {
      error('Deal launch error: ' + err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // PATCH /api/deals/:id — update deal settings
  app.patch('/api/deals/:id', async (req, res) => {
    try {
      const updates = {};
      const allowed = [
        // Core
        'name', 'status', 'sector', 'geography', 'description', 'raise_type', 'currency',
        'target_amount', 'committed_amount', 'emails_sent', 'response_rate', 'active_prospects',
        'key_metrics', 'deck_url', 'investor_profile', 'linkedin_seeds', 'notes',
        // Cheque sizes — new column names
        'min_cheque', 'max_cheque',
        // Legacy column names (kept for backward compat)
        'minimum_cheque', 'maximum_cheque',
        // Schedule — new column names
        'send_from', 'send_until', 'timezone', 'active_days',
        // Schedule — legacy column names
        'sending_days', 'sending_start', 'sending_end', 'sending_timezone',
        // Per-channel windows (flat columns)
        'li_connect_from', 'li_connect_until', 'li_dm_from', 'li_dm_until',
        // Rate limits
        'max_emails_per_day', 'max_emails_per_hour', 'batch_size',
        'followup_cadence_days', 'max_contacts_per_firm', 'max_total_outreach',
        'min_investor_score', 'prioritise_hot_leads', 'include_unscored',
        'linkedin_daily_limit', 'followup_days_li', 'followup_days_email',
        // Pipeline cap
        'pipeline_max', 'pipeline_refill_threshold',
        // Pause / archive
        'paused', 'paused_at', 'outreach_paused_until',
        'archived_at', 'archived_reason',
      ];
      for (const key of allowed) {
        if (req.body[key] !== undefined) {
          // Guard: don't store empty strings for timezone (would cause fallback to Europe/London)
          if (key === 'timezone' && !req.body[key]) continue;
          updates[key] = req.body[key];
        }
      }
      const deal = await updateDeal(req.params.id, updates);
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      res.json({ success: true, deal });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PATCH /api/deals/:id/settings — update deal per-channel windows (flat columns, no JSONB needed)
  app.patch('/api/deals/:id/settings', async (req, res) => {
    try {
      const { timezone, currency, email_send_window, linkedin_connection_window, linkedin_dm_window } = req.body;

      const updates = {};
      if (timezone) updates.timezone = timezone;
      if (currency) updates.currency = currency.toUpperCase();
      // Map window objects to flat columns
      if (email_send_window?.start) updates.send_from = email_send_window.start;
      if (email_send_window?.end)   updates.send_until = email_send_window.end;
      if (linkedin_connection_window?.start) updates.li_connect_from  = linkedin_connection_window.start;
      if (linkedin_connection_window?.end)   updates.li_connect_until = linkedin_connection_window.end;
      if (linkedin_dm_window?.start) updates.li_dm_from  = linkedin_dm_window.start;
      if (linkedin_dm_window?.end)   updates.li_dm_until = linkedin_dm_window.end;

      if (!Object.keys(updates).length) return res.json({ success: true });

      const deal = await updateDeal(req.params.id, updates);
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/pause
  app.post('/api/deals/:id/pause', async (req, res) => {
    try {
      const deal = await updateDeal(req.params.id, {
        status: 'PAUSED',
        paused: true,
        paused_at: new Date().toISOString(),
      });
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      pushActivity({ type: 'system', action: 'Deal Paused', note: `${deal.name} — paused by user`, dealId: deal.id, deal_name: deal.name });
      res.json({ success: true, deal });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/resume
  app.post('/api/deals/:id/resume', async (req, res) => {
    try {
      const deal = await updateDeal(req.params.id, {
        status: 'ACTIVE',
        paused: false,
        paused_at: null,
      });
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      pushActivity({ type: 'system', action: 'Deal Resumed', note: `${deal.name} — resumed by user`, dealId: deal.id, deal_name: deal.name });
      res.json({ success: true, deal });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/close
  app.post('/api/deals/:id/close', async (req, res) => {
    try {
      const dealId = req.params.id;
      const deal = await updateDeal(dealId, {
        status: 'CLOSED',
        paused: false,
        closed_at: new Date().toISOString(),
        archived_at: new Date().toISOString(),
        archived_reason: 'closed',
      });

      // ── Mark all contacts Inactive so investors become available for future deals ──
      try {
        const sb = getSupabase();
        if (sb) {
          await sb.from('contacts').update({ pipeline_stage: 'Inactive' }).eq('deal_id', dealId);
          info(`[CLOSE] Set all contacts for deal ${dealId} to Inactive`);
        }
      } catch (e) { console.warn('[CLOSE] Failed to set contacts Inactive:', e.message); }

      // ── Immediate cleanup: wipe all pending activity for this deal ──────────

      // 1. Clear in-memory Telegram approval queue for this deal
      const clearedApprovals = clearApprovalsForDeal(dealId);

      // 2. Clear in-memory debounce batches for this deal (inbound reply handling)
      clearDebounceForDeal(dealId);

      // 3. Hard-delete from Supabase approval_queue table
      try {
        const sb = getSupabase();
        if (sb) {
          // approval_queue rows link via contact_id → contacts.deal_id
          const { data: dealContacts } = await sb.from('contacts')
            .select('id').eq('deal_id', dealId);
          const contactIds = (dealContacts || []).map(c => c.id);
          if (contactIds.length) {
            await sb.from('approval_queue')
              .delete()
              .in('company_contact_id', contactIds)
              .eq('status', 'pending');
          }
          info(`[CLOSE] Wiped approval_queue rows for deal ${dealId}`);
        }
      } catch (e) { console.warn('[CLOSE] approval_queue wipe failed:', e.message); }

      // 4. Signal orchestrator to skip this deal
      const { clearDealFromFlight } = await import('../core/orchestrator.js');
      clearDealFromFlight(dealId);

      info(`[CLOSE] Deal "${deal.name}" closed — cleared ${clearedApprovals} approval(s) from queue`);
      await sbLogActivity({ dealId, eventType: 'DEAL_CLOSED', summary: `Deal "${deal.name}" closed` });
      await sendTelegram(`Deal closed: ${deal.name}\nFinal committed: £${(deal.committed_amount || 0).toLocaleString()} of £${(deal.target_amount || 0).toLocaleString()} target\n\nAll outreach stopped. Deal archived.`).catch(() => {});
      pushActivity({ type: 'system', action: `Deal closed: ${deal.name}`, note: `All outreach stopped — ${clearedApprovals} pending approval(s) cleared`, dealId, deal_name: deal.name });
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      broadcastToAll({ type: 'DEAL_CLOSED', dealId });

      // Release any contacts held in other deals because of this deal closing
      try {
        const sb = getSupabase();
        const holdPattern = `%[CROSS_DEAL_HOLD:${req.params.id}|%`;
        const { data: heldContacts } = await sb.from('contacts')
          .select('id, name, deal_id, notes')
          .eq('pipeline_stage', 'Skipped')
          .ilike('notes', holdPattern);

        if (heldContacts?.length) {
          const when = new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
          const priorNote = `[PRIOR_DEAL:${deal.name}|contacted:${when}]`;
          const holdRegex = new RegExp(`\\n?\\[CROSS_DEAL_HOLD:${req.params.id}\\|[^\\]]*\\]`, 'g');

          for (const c of heldContacts) {
            const cleanedNotes = (c.notes || '').replace(holdRegex, '').trim();
            await sb.from('contacts').update({
              pipeline_stage: 'Researched',
              notes: cleanedNotes ? `${cleanedNotes}\n${priorNote}` : priorNote,
            }).eq('id', c.id);
          }
          pushActivity({ type: 'PIPELINE', action: 'Contacts Released', note: `${heldContacts.length} cross-deal held contacts released into pipeline after "${deal.name}" closed` });
          info(`[CLOSE] Released ${heldContacts.length} cross-deal held contacts`);
        }
      } catch (e) {
        console.warn('[CLOSE] Failed to release cross-deal holds:', e.message);
      }

      res.json({ success: true, deal });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/trigger-invites — manually send LinkedIn invites to Ranked/Enriched contacts
  app.post('/api/deals/:id/trigger-invites', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });

      const { data: contacts } = await sb.from('contacts')
        .select('*')
        .eq('deal_id', req.params.id)
        .in('pipeline_stage', ['Ranked', 'Enriched'])
        .not('linkedin_url', 'is', null)
        .is('invite_sent_at', null)
        .limit(10);

      if (!contacts?.length) return res.json({ sent: 0, message: 'No contacts ready for invites' });

      let sent = 0;
      const results = [];
      for (const contact of contacts) {
        try {
          let providerId = contact.linkedin_provider_id || null;
          if (!providerId && contact.linkedin_url) {
            const urnMatch = contact.linkedin_url.match(/miniProfileUrn=urn%3Ali%3A[^&]+%3A([A-Za-z0-9_-]+)/);
            if (urnMatch) providerId = urnMatch[1];
          }
          await sendLinkedInInvite({ providerId, linkedinUrl: providerId ? null : contact.linkedin_url, message: `Hi ${(contact.name || '').split(' ')[0]}, I came across your profile — we're building Roco, an autonomous fundraising AI, and I'd love to connect.` });
          await sb.from('contacts').update({ pipeline_stage: 'invite_sent', invite_sent_at: new Date().toISOString() }).eq('id', contact.id);
          sent++;
          results.push({ name: contact.name, status: 'sent' });
          await sbLogActivity({ dealId: req.params.id, contactId: contact.id, eventType: 'LINKEDIN_INVITE_SENT', summary: `Manual invite sent to ${contact.name}`, apiUsed: 'unipile' });
          await new Promise(r => setTimeout(r, 2000));
        } catch (e) {
          results.push({ name: contact.name, status: 'failed', error: e.message });
        }
      }
      pushActivity({ type: 'LINKEDIN', action: 'Invites Sent', note: `${sent} LinkedIn invites sent manually` });
      res.json({ sent, results });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/deals/:id — permanently delete a deal and all its contacts
  app.delete('/api/deals/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });
      // Delete all related data — hard wipe, nothing referencing this deal survives
      const did = req.params.id;
      await sb.from('contacts').delete().eq('deal_id', did);
      const relatedTables = [
        'emails', 'replies', 'linkedin_messages', 'firms', 'deal_assets',
        'activity_log', 'batches', 'firm_suppressions', 'firm_responses',
        'schedule_log', 'deal_contacts',
      ];
      for (const t of relatedTables) {
        try { await sb.from(t).delete().eq('deal_id', did); } catch {}
      }
      // Delete the deal itself
      await sb.from('deals').delete().eq('id', did);
      // Clear deal.json on disk if it still references this deal
      try {
        const { readFileSync, writeFileSync, existsSync } = await import('fs');
        const dealJsonPath = new URL('../deal.json', import.meta.url).pathname;
        if (existsSync(dealJsonPath)) {
          const stored = JSON.parse(readFileSync(dealJsonPath, 'utf8'));
          if (String(stored?.id) === String(req.params.id)) {
            writeFileSync(dealJsonPath, JSON.stringify({}));
          }
        }
      } catch { /* non-fatal */ }
      pushActivity({ type: 'SYSTEM', action: 'Deal Deleted', note: `Deal "${deal.name}" permanently deleted` });
      broadcastToAll({ type: 'DEAL_DELETED', dealId: req.params.id });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/clear-pipeline — delete ALL contacts for this deal
  app.post('/api/deals/:id/clear-pipeline', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });

      // Fetch all contacts so we can archive their Notion pages
      const { data: contacts } = await sb.from('contacts')
        .select('id, name, notion_page_id')
        .eq('deal_id', req.params.id);

      const count = contacts?.length || 0;

      // Archive in Notion — best-effort, non-blocking
      if (contacts?.length) {
        const { archiveContact } = await import('../crm/notionContacts.js');
        for (const c of contacts) {
          if (c.notion_page_id) archiveContact(c.notion_page_id).catch(() => {});
        }
      }

      const did = req.params.id;
      // Full memory wipe — contacts + all pipeline state for this deal
      await sb.from('contacts').delete().eq('deal_id', did);
      const pipelineTables = [
        'emails', 'replies', 'linkedin_messages', 'firms',
        'firm_suppressions', 'firm_responses', 'schedule_log', 'deal_contacts', 'batches',
      ];
      for (const t of pipelineTables) {
        try { await sb.from(t).delete().eq('deal_id', did); } catch {}
      }

      pushActivity({ type: 'SYSTEM', action: 'Pipeline Cleared', note: `${count} contacts cleared from "${deal.name}"` });
      broadcastToAll({ type: 'PIPELINE_CLEARED', dealId: did });
      await sbLogActivity({ dealId: did, eventType: 'PIPELINE_CLEARED', summary: `Pipeline cleared — ${count} contacts removed` }).catch(() => {});

      res.json({ success: true, cleared: count });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ── Deal Assets ─────────────────────────────────────────────────────────────

  // GET /api/deals/:id/assets
  app.get('/api/deals/:id/assets', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error } = await sb.from('deal_assets')
        .select('*')
        .eq('deal_id', req.params.id)
        .order('created_at', { ascending: true });
      if (error) return res.status(500).json({ error: error.message });
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/assets — add an asset
  app.post('/api/deals/:id/assets', async (req, res) => {
    const { name, asset_type, url, description } = req.body;
    if (!name || !asset_type || !url) return res.status(400).json({ error: 'name, asset_type, url required' });
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error } = await sb.from('deal_assets')
        .insert({ deal_id: req.params.id, name, asset_type, url, description: description || null })
        .select().single();
      if (error) return res.status(500).json({ error: error.message });
      pushActivity({ type: 'system', action: `Asset added: ${name}`, note: asset_type });
      res.json(data);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/deals/:id/assets/:assetId
  app.delete('/api/deals/:id/assets/:assetId', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { error } = await sb.from('deal_assets')
        .delete()
        .eq('id', req.params.assetId)
        .eq('deal_id', req.params.id); // safety: scope to this deal
      if (error) return res.status(500).json({ error: error.message });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/capital — update capital committed (Dom's manual input)
  app.post('/api/deals/:id/capital', async (req, res) => {
    const { amount } = req.body;
    if (amount == null || isNaN(Number(amount))) return res.status(400).json({ error: 'amount required' });
    try {
      const deal = await updateDeal(req.params.id, { committed_amount: Number(amount) });
      broadcastToAll({ type: 'DEAL_UPDATED', deal });
      pushActivity({ type: 'system', action: `Capital updated: ${deal.name}`, note: `£${Number(amount).toLocaleString()} committed` });
      res.json({ success: true, committed_amount: Number(amount) });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ────────────────────────────────────────────────────────────────────────────

  // GET /api/deals/:id/metrics — live metrics from contacts table
  app.get('/api/deals/:id/metrics', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });

      const [
        { data: contacts },
        { count: emailsSentCount },
        { count: emailRepliesCount },
      ] = await Promise.all([
        sb.from('contacts').select('pipeline_stage, invite_sent_at, outreach_channel').eq('deal_id', req.params.id),
        sb.from('emails').select('id', { count: 'exact', head: true }).eq('deal_id', req.params.id).eq('status', 'sent'),
        sb.from('replies').select('id', { count: 'exact', head: true }).eq('deal_id', req.params.id),
      ]);
      const all = contacts || [];

      const emailsSent      = emailsSentCount || 0;
      const emailReplies    = emailRepliesCount || 0;
      const invitesSent     = all.filter(c => c.invite_sent_at).length;
      const invitesAccepted = all.filter(c => ['invite_accepted','dm_sent','Replied','Meeting Booked','Meeting Scheduled'].includes(c.pipeline_stage)).length;
      const dmsSent         = all.filter(c => ['dm_sent','Replied','Meeting Booked','Meeting Scheduled'].includes(c.pipeline_stage) && (c.outreach_channel === 'linkedin_dm' || c.outreach_channel === 'linkedin')).length;
      const dmResponses     = all.filter(c => ['Replied','Meeting Booked','Meeting Scheduled'].includes(c.pipeline_stage) && (c.outreach_channel === 'linkedin_dm' || c.outreach_channel === 'linkedin')).length;
      const activeProspects = all.filter(c => !['Archived','Skipped','Inactive','Suppressed — Opt Out','Deleted — Do Not Contact'].includes(c.pipeline_stage)).length;

      res.json({
        totalContacts:      all.length,
        activeProspects,
        invitesSent,
        invitesAccepted,
        acceptanceRate:     invitesSent > 0 ? Math.round((invitesAccepted / invitesSent) * 100) : 0,
        dmsSent,
        dmResponses,
        dmResponseRate:     dmsSent > 0 ? Math.round((dmResponses / dmsSent) * 100) : 0,
        emailsSent,
        emailReplies,
        emailResponseRate:  emailsSent > 0 ? Math.round((emailReplies / emailsSent) * 100) : 0,
        totalResponses:     emailReplies + dmResponses,
        capitalCommitted:   deal.committed_amount || 0,
        targetAmount:       deal.target_amount || 0,
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/archived — archived contacts with reason
  app.get('/api/deals/:id/archived', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('contacts')
        .select('id, name, company_name, job_title, linkedin_url, investor_score, notes, created_at')
        .eq('deal_id', req.params.id)
        .eq('pipeline_stage', 'Archived')
        .order('investor_score', { ascending: false })
        .limit(500);
      if (dbErr) return res.status(500).json({ error: dbErr.message });

      const mapped = (data || []).map(c => {
        // Extract last score rationale from notes: "[SCORE: X — Grade] reason text"
        const matches = [...(c.notes || '').matchAll(/\[SCORE:\s*(\d+)\s*—\s*(\w+)\]\s*([^\n\[]+)/g)];
        const last = matches[matches.length - 1];
        return {
          id: c.id,
          name: c.name,
          firm: c.company_name || '',
          jobTitle: c.job_title || '',
          score: c.investor_score,
          archiveReason: last ? last[3].trim() : 'Score below threshold',
          grade: last ? last[2] : 'Archive',
        };
      });
      res.json(mapped);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/rankings — ranked contacts for a deal (sorted by investor_score desc)
  app.get('/api/deals/:id/rankings', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { page = 1, limit = 50 } = req.query;
      const offset = (Number(page) - 1) * Number(limit);
      const { data, error: dbErr, count } = await sb.from('contacts')
        .select('id, name, company_name, job_title, linkedin_url, investor_score, pipeline_stage, sector_focus, geography, typical_cheque_size, notes, source, enrichment_status, email, phone, last_email_sent_at, invite_sent_at, created_at, past_investments, person_researched, investors_db_id', { count: 'exact' })
        .eq('deal_id', req.params.id)
        .not('investor_score', 'is', null)
        .order('investor_score', { ascending: false })
        .range(offset, offset + Number(limit) - 1);
      if (dbErr) return res.status(500).json({ error: dbErr.message });
      res.json({
        contacts: data || [],
        total: count || 0,
        page: Number(page),
        limit: Number(limit),
        pages: Math.ceil((count || 0) / Number(limit)),
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/batches — batch summary for a deal
  app.get('/api/deals/:id/batches', async (req, res) => {
    try {
      const batches = await getBatchSummary(req.params.id);
      res.json(batches);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── CAMPAIGN BATCHES (firm approval flow) ────────────────────────────────

  // GET /api/deals/:id/campaign/current — get current campaign batch with firms + contacts + research
  app.get('/api/deals/:id/campaign/current', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      const dealId = req.params.id;

      // Show the most relevant batch: pending_approval first, then approved, then researching
      const { data: batches } = await sb.from('campaign_batches')
        .select('*')
        .eq('deal_id', dealId)
        .in('status', ['pending_approval', 'approved', 'researching'])
        .order('created_at', { ascending: false })
        .limit(5);

      if (!batches?.length) return res.json(null);
      // Prioritise pending_approval > approved > researching
      const batch = batches.find(b => b.status === 'pending_approval')
        || batches.find(b => b.status === 'approved')
        || batches[0];

      const { data: firms } = await sb.from('campaign_batch_firms')
        .select('*, firm_outreach_state(rank_score, status, firms(id, name, sector, hq_location, website, description))')
        .eq('batch_id', batch.id)
        .order('created_at', { ascending: true });

      // For each firm, fetch contacts in this deal that match the firm name (decision makers)
      const firmsWithContacts = await Promise.all((firms || []).map(async (f) => {
        const firmName = f.firm_outreach_state?.firms?.name || f.firm_name;
        if (!firmName) return { ...f, contacts: [] };

        const { data: contacts } = await sb.from('contacts')
          .select('id, name, job_title, email, linkedin_url, pipeline_stage, enrichment_status, investor_score, person_researched, past_investments, investment_thesis, sector_focus, geography, contact_type, is_angel')
          .eq('deal_id', dealId)
          .ilike('company_name', `%${firmName}%`)
          .not('pipeline_stage', 'eq', 'Archived')
          .order('investor_score', { ascending: false });

        // Also pull research from investors_db for any contact that links there
        return { ...f, contacts: contacts || [] };
      }));

      res.json({ ...batch, firms: firmsWithContacts });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/campaign/history — all past completed batches
  app.get('/api/deals/:id/campaign/history', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      const { data: batches } = await sb.from('campaign_batches')
        .select('*')
        .eq('deal_id', req.params.id)
        .order('created_at', { ascending: false });
      res.json(batches || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/campaign/:batchId/approve — approve a campaign batch
  app.post('/api/deals/:id/campaign/:batchId/approve', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      const { data: batch } = await sb.from('campaign_batches')
        .update({ status: 'approved', approved_at: new Date().toISOString(), updated_at: new Date().toISOString() })
        .eq('id', req.params.batchId)
        .eq('deal_id', req.params.id)
        .select()
        .single();
      if (!batch) return res.status(404).json({ error: 'Batch not found' });

      const deal = await getDeal(req.params.id);
      pushActivity({ type: 'system', action: 'Campaign batch approved', note: `${deal?.name} — Batch #${batch.batch_number} approved. Outreach begins next window.`, deal_name: deal?.name, dealId: deal?.id });
      const { sendTelegram } = await import('../approval/telegramBot.js');
      await sendTelegram(`✅ *Campaign Approved* — ${deal?.name}\n\nBatch #${batch.batch_number} approved. Outreach will begin at the next EST sending window.`).catch(() => {});

      res.json({ success: true, batch });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/campaign/:batchId/reject — reject a batch (resets to researching)
  app.post('/api/deals/:id/campaign/:batchId/reject', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      const { reason } = req.body || {};
      await sb.from('campaign_batches')
        .update({ status: 'rejected', rejection_reason: reason || null, updated_at: new Date().toISOString() })
        .eq('id', req.params.batchId)
        .eq('deal_id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/campaign-firms/:firmId — remove a firm from a batch
  app.delete('/api/campaign-firms/:firmId', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      await sb.from('campaign_batch_firms').delete().eq('id', req.params.firmId);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/campaign-firms/:firmId/contacts — get contacts for a firm in this deal
  app.get('/api/campaign-firms/:batchFirmId/contacts', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      // Get the batch firm record to find firm_outreach_state → deal_id
      const { data: bf } = await sb.from('campaign_batch_firms')
        .select('*, firm_outreach_state(deal_id, firms(id, name))')
        .eq('id', req.params.batchFirmId)
        .single();
      if (!bf) return res.status(404).json({ error: 'Not found' });
      const dealId = bf.firm_outreach_state?.deal_id;
      const firmName = bf.firm_outreach_state?.firms?.name || bf.firm_name;
      if (!dealId) return res.json([]);
      const { data: contacts } = await sb.from('contacts')
        .select('id, name, job_title, email, linkedin_url, pipeline_stage, investor_score, enrichment_status')
        .eq('deal_id', dealId)
        .ilike('company_name', `%${firmName}%`)
        .order('investor_score', { ascending: false });
      res.json(contacts || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/campaign/close — close approved batch, promote next ready batch
  app.post('/api/deals/:id/campaign/close', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      const dealId = req.params.id;
      const { data: deal } = await sb.from('deals').select('name').eq('id', dealId).single();

      // Mark approved batch as completed
      const { data: approvedBatch } = await sb.from('campaign_batches')
        .select('id, batch_number')
        .eq('deal_id', dealId).eq('status', 'approved')
        .limit(1).single();
      if (approvedBatch) {
        await sb.from('campaign_batches')
          .update({ status: 'completed', updated_at: new Date().toISOString() })
          .eq('id', approvedBatch.id);
      }

      // Promote next ready batch to pending_approval
      const { data: readyBatch } = await sb.from('campaign_batches')
        .select('id, batch_number')
        .eq('deal_id', dealId).eq('status', 'ready')
        .order('batch_number', { ascending: true })
        .limit(1).single();
      if (readyBatch) {
        await sb.from('campaign_batches')
          .update({ status: 'pending_approval', updated_at: new Date().toISOString() })
          .eq('id', readyBatch.id);
        const msg = `📋 *Campaign Review Ready* — ${deal?.name || 'Deal'}\n\nBatch #${readyBatch.batch_number} is up for review. Open the dashboard → Campaign tab.`;
        sendTelegram(msg).catch(() => {});
        pushActivity({ type: 'system', action: 'Next batch ready for review',
          note: `${deal?.name || ''} — Batch #${readyBatch.batch_number} promoted after batch #${approvedBatch?.batch_number || '?'} closed.`,
          deal_name: deal?.name, dealId });
        return res.json({ closed: approvedBatch?.batch_number || null, promoted: readyBatch.batch_number });
      }

      // No ready batch — find if one is still building
      const { data: buildingBatch } = await sb.from('campaign_batches')
        .select('batch_number, ranked_firms, target_firms')
        .eq('deal_id', dealId).eq('status', 'researching')
        .limit(1).single();
      res.json({
        closed: approvedBatch?.batch_number || null,
        promoted: null,
        building: buildingBatch?.batch_number || null,
        buildingProgress: buildingBatch ? `${buildingBatch.ranked_firms || 0}/${buildingBatch.target_firms || 20}` : null,
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/investors/search — search investors DB for manual firm add
  app.get('/api/investors/search', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'DB unavailable' });
      const { q } = req.query;
      if (!q || q.length < 2) return res.json([]);
      const { data } = await sb.from('firms')
        .select('id, name, sector, hq_location, website')
        .ilike('name', `%${q}%`)
        .limit(20);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/schedule — schedule status for a deal
  app.get('/api/deals/:id/schedule', async (req, res) => {
    try {
      const deal = await getDeal(req.params.id);
      if (!deal) return res.status(404).json({ error: 'Deal not found' });
      const state = readState();
      const status = getWindowStatus(deal, state.outreachPausedUntil);
      const visualization = getWindowVisualization(deal);
      res.json({ status, visualization });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/pause-outreach — pause outreach for a specific deal until datetime
  app.post('/api/deals/:id/pause-outreach', async (req, res) => {
    try {
      const { until } = req.body;
      const deal = await updateDeal(req.params.id, { outreach_paused_until: until || null });
      res.json({ success: true, deal });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── EXCLUSION LIST ───────────────────────────────────────────────────────

  // GET /api/deals/:id/exclusions
  app.get('/api/deals/:id/exclusions', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data, error } = await sb.from('deal_exclusions')
        .select('*')
        .eq('deal_id', req.params.id)
        .order('added_at', { ascending: false });
      if (error) throw error;
      res.json(data || []);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/deals/:id/exclusions — add single entry manually
  app.post('/api/deals/:id/exclusions', async (req, res) => {
    try {
      const sb = getSupabase();
      const { firm_name, person_name, email } = req.body;
      if (!firm_name && !person_name && !email) {
        return res.status(400).json({ error: 'At least one identifier required' });
      }
      const { data, error } = await sb.from('deal_exclusions').insert({
        deal_id:     req.params.id,
        firm_name:   firm_name   ? firm_name.toLowerCase().trim()   : null,
        person_name: person_name ? person_name.toLowerCase().trim() : null,
        email:       email       ? email.toLowerCase().trim()       : null,
        added_by:    req.session?.displayName || 'Dom',
      }).select().single();
      if (error) throw error;
      pushActivity({ type: 'system', action: `Exclusion added: ${firm_name || person_name || email}`, note: '' });

      // Invalidate cache so orchestrator picks up new entry immediately
      try {
        const { invalidateExclusionCache } = await import('../core/exclusionCheck.js');
        invalidateExclusionCache(req.params.id);
      } catch (_) {}

      res.json({ success: true, exclusion: data });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // DELETE /api/deals/:id/exclusions/:exclusionId
  app.delete('/api/deals/:id/exclusions/:exclusionId', async (req, res) => {
    try {
      const sb = getSupabase();
      await sb.from('deal_exclusions')
        .delete()
        .eq('id', req.params.exclusionId)
        .eq('deal_id', req.params.id);

      try {
        const { invalidateExclusionCache } = await import('../core/exclusionCheck.js');
        invalidateExclusionCache(req.params.id);
      } catch (_) {}

      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/deals/:id/exclusions/upload — bulk upload XLSX or CSV
  const exclUpload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } }).single('file');

  const FIRM_INDICATORS = [
    'llc', 'lp', 'llp', 'inc', 'ltd', 'corp', 'capital', 'partners', 'partner',
    'management', 'fund', 'group', 'ventures', 'venture', 'equity', 'investments',
    'investment', 'advisors', 'advisory', 'associates', 'holdings', 'trust',
    'asset', 'assets', 'financial', 'finance', 'securities', 'family office',
    'family', 'solutions', 'services', 'consulting',
  ];

  function classifyAccountName(accountName, firstName, lastName) {
    if (!accountName) return { firm_name: null, person_name: null };
    const lower = accountName.toLowerCase();
    if (FIRM_INDICATORS.some(ind => lower.includes(ind))) {
      return { firm_name: accountName, person_name: [firstName, lastName].filter(Boolean).join(' ') || null };
    }
    const fullNameFromCols = [firstName, lastName].filter(Boolean).join(' ').toLowerCase();
    if (fullNameFromCols && lower === fullNameFromCols) {
      return { firm_name: null, person_name: accountName };
    }
    const words = accountName.trim().split(/\s+/);
    if (words.length <= 2) {
      return { firm_name: null, person_name: accountName };
    }
    return { firm_name: accountName, person_name: [firstName, lastName].filter(Boolean).join(' ') || null };
  }

  function parseExclusionFile(buffer, originalname) {
    const wb = XLSX.read(buffer, { type: 'buffer' });
    const ws = wb.Sheets[wb.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(ws, { defval: null });
    return rows.map(row => {
      const norm = {};
      Object.entries(row).forEach(([k, v]) => { norm[k.toLowerCase().replace(/\s+/g, '_')] = v; });
      const accountName = norm.account_name || norm.firm || norm.company || norm.organization || norm.name || null;
      const firstName   = norm.first_name || norm.firstname || null;
      const lastName    = norm.last_name  || norm.lastname  || null;
      const email       = norm.contact_email || norm.email || norm.email_address || null;
      const { firm_name, person_name } = classifyAccountName(
        accountName ? String(accountName).trim() : null,
        firstName   ? String(firstName).trim()   : null,
        lastName    ? String(lastName).trim()     : null,
      );
      return {
        firm_name:   firm_name   ? firm_name.toLowerCase().trim()   : null,
        person_name: person_name ? person_name.toLowerCase().trim() : null,
        email:       email       ? String(email).toLowerCase().trim() : null,
      };
    }).filter(r => r.firm_name || r.person_name || r.email);
  }

  app.post('/api/deals/:id/exclusions/upload', (req, res) => {
    exclUpload(req, res, async (err) => {
      if (err) return res.status(400).json({ error: err.message });
      try {
        const sb = getSupabase();
        if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
        const parsed = parseExclusionFile(req.file.buffer, req.file.originalname);
        if (!parsed.length) return res.status(400).json({ error: 'No valid rows found in file' });

        const rows = parsed.map(p => ({
          deal_id:     req.params.id,
          firm_name:   p.firm_name,
          person_name: p.person_name,
          email:       p.email,
          added_by:    req.session?.displayName || 'Dom',
        }));

        let imported = 0;
        for (let i = 0; i < rows.length; i += 100) {
          const batch = rows.slice(i, i + 100);
          const { error: bErr } = await sb.from('deal_exclusions').insert(batch);
          if (!bErr) imported += batch.length;
        }

        try {
          const { invalidateExclusionCache } = await import('../core/exclusionCheck.js');
          invalidateExclusionCache(req.params.id);
        } catch (_) {}

        pushActivity({ type: 'system', action: `Exclusion list imported: ${imported} entries`, note: req.file.originalname });
        res.json({ success: true, imported });
      } catch (e) {
        console.error('[EXCLUSIONS UPLOAD]', e.message);
        res.status(500).json({ error: e.message });
      }
    });
  });

  // ─── DEAL TEMPLATES ────────────────────────────────────────────────────────

  // GET /api/deals/:id/templates
  app.get('/api/deals/:id/templates', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data: dealTemplates } = await sb.from('deal_templates')
        .select('*').eq('deal_id', req.params.id).order('created_at');
      const { data: globalTemplates } = await sb.from('email_templates')
        .select('*').eq('is_active', true).order('created_at');
      res.json({ deal: dealTemplates || [], global: globalTemplates || [] });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/deals/:id/templates
  app.post('/api/deals/:id/templates', async (req, res) => {
    try {
      const sb = getSupabase();
      const { name, type, sequence_step, subject_a, subject_b, body, is_primary, notes } = req.body;
      if (!name || !body) return res.status(400).json({ error: 'name and body required' });
      if (is_primary && sequence_step) {
        await sb.from('deal_templates').update({ is_primary: false })
          .eq('deal_id', req.params.id).eq('sequence_step', sequence_step);
      }
      const { data, error: insErr } = await sb.from('deal_templates').insert({
        deal_id: req.params.id, name, type: type || 'email',
        sequence_step: sequence_step || null,
        subject_a: subject_a || null, subject_b: subject_b || null,
        body, is_primary: !!is_primary, notes: notes || null,
      }).select().single();
      if (insErr) throw insErr;
      res.json({ success: true, template: data });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PUT /api/deals/:id/templates/:tmplId
  app.put('/api/deals/:id/templates/:tmplId', async (req, res) => {
    try {
      const sb = getSupabase();
      const { name, subject_a, subject_b, body, is_primary, notes, sequence_step } = req.body;
      if (is_primary && sequence_step) {
        await sb.from('deal_templates').update({ is_primary: false })
          .eq('deal_id', req.params.id).eq('sequence_step', sequence_step)
          .neq('id', req.params.tmplId);
      }
      const { data, error: updErr } = await sb.from('deal_templates').update({
        name, subject_a, subject_b, body, is_primary: !!is_primary,
        notes, sequence_step, updated_at: new Date().toISOString(),
      }).eq('id', req.params.tmplId).eq('deal_id', req.params.id).select().single();
      if (updErr) throw updErr;
      res.json({ success: true, template: data });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/deals/:id/templates/:tmplId
  app.delete('/api/deals/:id/templates/:tmplId', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data: t } = await sb.from('deal_templates').select('is_primary')
        .eq('id', req.params.tmplId).single();
      if (t?.is_primary) return res.status(400).json({ error: 'Cannot delete a primary template' });
      await sb.from('deal_templates').delete().eq('id', req.params.tmplId);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/deals/:id/sequence
  app.get('/api/deals/:id/sequence', async (req, res) => {
    try {
      const sb = getSupabase();
      let data = null;
      try {
        const r = await sb.from('deal_sequence').select('*').eq('deal_id', req.params.id).limit(1).single();
        data = r.data;
      } catch (_) {}
      res.json(data || { steps: null });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PUT /api/deals/:id/sequence
  app.put('/api/deals/:id/sequence', async (req, res) => {
    try {
      const sb = getSupabase();
      const { steps, sending_window } = req.body;
      const renumbered = (steps || []).map((s, i) => ({ ...s, step: i + 1 }));
      let existing = null;
      try {
        const r = await sb.from('deal_sequence').select('id').eq('deal_id', req.params.id).limit(1).single();
        existing = r.data;
      } catch (_) {}
      const payload = { steps: renumbered, updated_at: new Date().toISOString(),
        ...(sending_window ? { sending_window } : {}) };
      if (existing?.id) {
        await sb.from('deal_sequence').update(payload).eq('id', existing.id);
      } else {
        await sb.from('deal_sequence').insert({ deal_id: req.params.id, ...payload });
      }
      pushActivity({ type: 'system', action: 'Deal sequence updated', note: '' });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── ENV VARS ─────────────────────────────────────────────────────────────

  const ENV_PATH = path.join(__dirname, '../.env');

  // GET /api/env — list all .env vars with masked values
  app.get('/api/env', (req, res) => {
    try {
      const content = fs.existsSync(ENV_PATH) ? fs.readFileSync(ENV_PATH, 'utf8') : '';
      const vars = [];
      for (const line of content.split('\n')) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        const eqIdx = trimmed.indexOf('=');
        if (eqIdx === -1) continue;
        const key   = trimmed.slice(0, eqIdx).trim();
        const value = trimmed.slice(eqIdx + 1).trim();
        const masked = value.length === 0 ? '(empty)'
          : value.length <= 8 ? '•'.repeat(value.length)
          : value.slice(0, 6) + '•'.repeat(Math.min(value.length - 6, 24));
        vars.push({ key, masked, set: value.length > 0 });
      }
      res.json(vars);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/env — update or add a single .env variable
  app.post('/api/env', (req, res) => {
    try {
      const { key, value } = req.body;
      if (!key || !/^[A-Z0-9_]+$/.test(key)) return res.status(400).json({ error: 'Invalid key' });
      if (value === undefined || value === null) return res.status(400).json({ error: 'value required' });

      const content = fs.existsSync(ENV_PATH) ? fs.readFileSync(ENV_PATH, 'utf8') : '';
      const lines = content.split('\n');
      let found = false;
      const newLines = lines.map(line => {
        const t = line.trim();
        if (!t || t.startsWith('#') || !t.includes('=')) return line;
        const eqIdx = t.indexOf('=');
        if (t.slice(0, eqIdx).trim() === key) {
          found = true;
          // Quote values containing special chars that dotenv would misparse (#, spaces, quotes)
          const needsQuotes = /[#\s"'\\]/.test(value);
          const safe = needsQuotes ? `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"` : value;
          return `${key}=${safe}`;
        }
        return line;
      });
      if (!found) {
        const needsQuotes = /[#\s"'\\]/.test(value);
        const safe = needsQuotes ? `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"` : value;
        newLines.push(`${key}=${safe}`);
      }
      fs.writeFileSync(ENV_PATH, newLines.join('\n'));
      // Apply immediately to running process
      process.env[key] = value;
      info(`[ENV] Updated: ${key}`);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── SEQUENCE ─────────────────────────────────────────────────────────────

  // GET /api/sequence
  app.get('/api/sequence', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.json({ steps: [] });
      const { data } = await sb.from('outreach_sequence').select('*').limit(1).single();
      res.json(data || { steps: [] });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PUT /api/sequence
  app.put('/api/sequence', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { steps, sending_window } = req.body;
      if (!Array.isArray(steps)) return res.status(400).json({ error: 'steps must be array' });

      // Renumber sequentially regardless of what was sent
      const renumbered = steps.map((s, i) => ({ ...s, step: i + 1 }));

      // Check for duplicate labels (not step numbers)
      const labels = renumbered.map(s => s.label).filter(Boolean);
      if (new Set(labels).size !== labels.length) {
        return res.status(400).json({ error: 'Each step label must be unique' });
      }

      let existing = null;
      try {
        const { data } = await sb.from('outreach_sequence').select('id').limit(1).single();
        existing = data;
      } catch { /* no row yet */ }

      const payload = {
        steps:      renumbered,
        updated_at: new Date().toISOString(),
        updated_by: req.session?.displayName || 'Dom',
        ...(sending_window ? { sending_window } : {}),
      };

      let result;
      if (existing?.id) {
        const { data } = await sb.from('outreach_sequence').update(payload).eq('id', existing.id).select().single();
        result = data;
      } else {
        const { data } = await sb.from('outreach_sequence').insert(payload).select().single();
        result = data;
      }
      pushActivity({ type: 'system', action: 'Outreach sequence updated', note: `${renumbered.length} steps` });
      res.json({ success: true, sequence: result });
    } catch (err) {
      console.error('[SEQUENCE]', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // ─── TEMPLATES ────────────────────────────────────────────────────────────

  // GET /api/templates — supports ?sequence_step= filter
  app.get('/api/templates', async (req, res) => {
    try {
      const { sequence_step } = req.query;
      if (sequence_step) {
        const sb = getSupabase();
        if (!sb) return res.json([]);
        const { data } = await sb.from('email_templates').select('*').eq('sequence_step', sequence_step).eq('is_active', true);
        return res.json(data || []);
      }
      const templates = await getTemplates();
      res.json(templates);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/templates/:id
  app.get('/api/templates/:id', async (req, res) => {
    try {
      const template = await getTemplate(req.params.id);
      if (!template) return res.status(404).json({ error: 'Template not found' });
      res.json(template);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/templates — create a new template
  app.post('/api/templates', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });

      const { name, type, subject_a, subject_b, body, is_active, ab_test_enabled, sequence_step, is_primary } = req.body;
      if (!name?.trim() || !body?.trim()) return res.status(400).json({ error: 'name and body required' });

      const { data, error } = await sb.from('email_templates').insert({
        name:            name.trim(),
        type:            type || 'email',
        subject_a:       subject_a || null,
        subject_b:       subject_b || null,
        body:            body.trim(),
        is_active:       is_active !== false,
        ab_test_enabled: !!ab_test_enabled,
        sequence_step:   sequence_step || null,
        is_primary:      !!is_primary,
        updated_at:      new Date().toISOString(),
      }).select().single();

      if (error) throw error;

      pushActivity({
        type: 'system',
        action: `Template created: "${name.trim()}"`,
        note: type || 'email',
      });

      res.json({ success: true, template: data });
    } catch (err) {
      console.error('[TEMPLATES POST]', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // PATCH /api/templates/:id — update template, sync to local file
  app.patch('/api/templates/:id', async (req, res) => {
    try {
      const { subject_a, subject_b, body, notes, is_active, ab_test_enabled } = req.body;
      const updates = {};
      if (subject_a !== undefined) updates.subject_a = subject_a;
      if (subject_b !== undefined) updates.subject_b = subject_b;
      if (body !== undefined) updates.body = body;
      if (notes !== undefined) updates.notes = notes;
      if (is_active !== undefined) updates.is_active = is_active;
      if (ab_test_enabled !== undefined) updates.ab_test_enabled = ab_test_enabled;
      const template = await updateTemplate(req.params.id, updates);
      pushActivity({
        type: 'system',
        action: `Template updated: ${template?.name || req.params.id}`,
        note: template?.type || '',
      });
      res.json({ success: true, template });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PATCH /api/templates/:id/primary — set as primary for its sequence_step
  app.patch('/api/templates/:id/primary', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: tmpl } = await sb.from('email_templates').select('*').eq('id', req.params.id).single();
      if (!tmpl) return res.status(404).json({ error: 'Not found' });
      if (!req.body.keep_ab) {
        await sb.from('email_templates').update({ is_primary: false }).eq('sequence_step', tmpl.sequence_step).neq('id', req.params.id);
      }
      await sb.from('email_templates').update({ is_primary: true }).eq('id', req.params.id);
      pushActivity({ type: 'system', action: `Primary template set: "${tmpl.name}"`, note: tmpl.sequence_step || '' });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PATCH /api/templates/:id/ab — set A/B pair
  app.patch('/api/templates/:id/ab', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { ab_pair_id, ab_test_enabled } = req.body;
      await sb.from('email_templates').update({ ab_pair_id: ab_pair_id || null, ab_test_enabled: !!ab_test_enabled }).eq('id', req.params.id);
      // If pairing, update partner too
      if (ab_pair_id) {
        await sb.from('email_templates').update({ ab_pair_id: req.params.id, ab_test_enabled: true }).eq('id', ab_pair_id);
      }
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PATCH /api/templates/:id/notes — save notes
  app.patch('/api/templates/:id/notes', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { notes } = req.body;
      await sb.from('email_templates').update({ notes, updated_at: new Date().toISOString() }).eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/templates/:id
  app.delete('/api/templates/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: tmpl } = await sb.from('email_templates').select('name, is_primary').eq('id', req.params.id).single();
      if (!tmpl) return res.status(404).json({ error: 'Not found' });
      if (tmpl.is_primary) return res.status(400).json({ error: 'Cannot delete a primary template. Set another template as primary first.' });
      await sb.from('email_templates').delete().eq('id', req.params.id);
      pushActivity({ type: 'system', action: `Template deleted: "${tmpl.name}"`, note: '' });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/templates/:id/reset — reset to system default
  app.post('/api/templates/:id/reset', async (req, res) => {
    try {
      await seedDefaultTemplates(); // re-seeds if needed
      const template = await getTemplate(req.params.id);
      res.json({ success: true, template });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/templates/preview — render template with sample data
  app.post('/api/templates/preview', (req, res) => {
    const { body, subject_a, subject_b } = req.body;
    const sample = {
      firstName: 'James', lastName: 'Richardson', fullName: 'James Richardson',
      firm: 'Blackstone Real Estate', company: 'Blackstone Real Estate',
      title: 'Managing Director', jobTitle: 'Managing Director',
      dealName: 'Meridian Industrial Portfolio',
      dealBrief: 'a £12m industrial portfolio in the East Midlands, yielding 7.8%',
      sector: 'Industrial Real Estate', geography: 'UK',
      targetAmount: '£12m', keyMetrics: '7.8% yield, 95% occupancy, 8-year WAULT',
      minCheque: '£500k', maxCheque: '£5M',
      investorProfile: 'UK-focused real estate investors seeking 6%+ yield',
      comparableDeal: 'their 2023 UK logistics park acquisition',
      deckUrl: 'https://docsend.com/view/example',
      callLink: 'https://calendly.com/dom/30min',
      senderName: 'Dom', senderTitle: 'Partner',
    };
    const render = (text) => {
      if (!text) return '';
      return text.replace(/\{\{(\w+)\}\}/g, (_, key) => sample[key] || `{{${key}}}`);
    };
    res.json({
      subjectA: render(subject_a),
      subjectB: render(subject_b),
      body: render(body),
      sampleData: sample,
    });
  });

  // ─── ACTIVITY (SUPABASE) ──────────────────────────────────────────────────

  // GET /api/activity/log — persistent activity from Supabase
  app.get('/api/activity/log', async (req, res) => {
    try {
      const { dealId, limit = 200, offset = 0, eventType } = req.query;
      const logs = await getActivityLog({
        dealId: dealId || null,
        limit: Number(limit),
        offset: Number(offset),
        eventType: eventType || null,
      });
      res.json(logs);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── GLOBAL SCHEDULE CONTROLS ─────────────────────────────────────────────

  // POST /api/pause-outreach — global outreach pause until datetime
  app.post('/api/pause-outreach', async (req, res) => {
    const { until } = req.body;
    const state = readState();
    state.outreach_paused_until = until || null;  // key the orchestrator reads
    state.outreachPausedUntil   = until || null;  // key the dashboard status reads
    writeState(state);
    await saveSessionState(state).catch(() => {});
    broadcastToAll({ type: 'STATE_UPDATE', state });
    pushActivity({
      type: 'SYSTEM',
      action: until ? 'Outreach Paused' : 'Outreach Resumed',
      note: until ? `Until ${new Date(until).toLocaleString('en-GB')}` : 'Pause lifted',
    });
    res.json({ success: true, outreachPausedUntil: until });
  });

  // ─── COMPANY SOURCING ROUTES ──────────────────────────────────────────────

  // GET /api/sourcing/campaigns — list all sourcing campaigns
  app.get('/api/sourcing/campaigns', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('sourcing_campaigns')
        .select('*')
        .order('created_at', { ascending: false });
      if (dbErr) throw new Error(dbErr.message);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns — create new sourcing campaign
  app.post('/api/sourcing/campaigns', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });

      const body = req.body;
      if (!body.name)          return res.status(400).json({ error: 'Campaign name is required' });
      if (!body.target_sector) return res.status(400).json({ error: 'Target sector is required' });
      if (!body.target_geography) return res.status(400).json({ error: 'Target geography is required' });

      // Handle geography as array or string
      let geography = body.target_geography;
      if (Array.isArray(geography)) geography = geography.join(', ');

      const campaign = {
        name:               body.name,
        firm_name:          body.firm_name || null,
        firm_type:          body.firm_type || null,
        investment_thesis:  body.investment_thesis || null,
        target_sector:      body.target_sector,
        target_geography:   geography,
        company_stage:      body.company_stage || null,
        min_revenue:        body.min_revenue || null,
        max_revenue:        body.max_revenue || null,
        min_ebitda:         body.min_ebitda || null,
        max_ebitda:         body.max_ebitda || null,
        min_company_age_months: parseInt(body.min_company_age_months) || null,
        max_company_age_months: parseInt(body.max_company_age_months) || null,
        business_model:     body.business_model || null,
        ownership_type:     body.ownership_type || null,
        headcount_min:      parseInt(body.headcount_min) || null,
        headcount_max:      parseInt(body.headcount_max) || null,
        deal_type:          body.deal_type || null,
        investment_size:    body.investment_size || null,
        intent_signals:     body.intent_signals || null,
        keywords_include:   body.keywords_include || null,
        keywords_exclude:   body.keywords_exclude || null,
        timezone:           body.timezone || 'America/New_York',
        batch_size:         parseInt(body.batch_size) || 5,
        max_companies_per_campaign: parseInt(body.max_companies_per_campaign) || 200,
        linkedin_connection_window: body.linkedin_connection_window || { start: '09:00', end: '18:00' },
        linkedin_dm_window: body.linkedin_dm_window || { start: '20:00', end: '23:00' },
        email_send_window:  body.email_send_window || { start: '09:00', end: '18:00' },
        status:             'active',
      };

      const { data: saved, error: dbErr } = await sb.from('sourcing_campaigns')
        .insert(campaign).select().single();
      if (dbErr) throw new Error(dbErr.message);

      pushActivity({ type: 'system', action: 'Sourcing campaign launched', note: saved.name });
      await sbLogActivity({ eventType: 'CAMPAIGN_CREATED', summary: `Sourcing campaign "${saved.name}" created` }).catch(() => {});
      await sendTelegram(`New sourcing campaign launched: ${saved.name}\nFirm: ${saved.firm_name || '—'}\nSector: ${saved.target_sector}\nGeography: ${saved.target_geography}\n\nResearch starting now.`).catch(() => {});

      broadcastToAll({ type: 'CAMPAIGN_CREATED', campaign: saved });
      res.json({ success: true, campaign: saved });

      // Fire research immediately (non-blocking)
      setImmediate(async () => {
        try {
          const { researchCompaniesForCampaign } = await import('../sourcing/companyResearcher.js');
          await researchCompaniesForCampaign(saved);
        } catch (e) {
          console.error('[CAMPAIGN LAUNCH] Research failed:', e.message);
        }
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/sourcing/campaigns/:id — single campaign with stats
  app.get('/api/sourcing/campaigns/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });

      const { data: campaign, error: dbErr } = await sb.from('sourcing_campaigns')
        .select('*').eq('id', req.params.id).single();
      if (dbErr || !campaign) return res.status(404).json({ error: 'Campaign not found' });

      // Gather stats
      const [
        { count: companiesFound },
        { count: hotLeads },
        { count: meetingsBooked },
        { count: contactsEnriched },
        { count: outreachSent },
      ] = await Promise.all([
        sb.from('target_companies').select('id', { count: 'exact', head: true }).eq('campaign_id', req.params.id),
        sb.from('target_companies').select('id', { count: 'exact', head: true }).eq('campaign_id', req.params.id).in('match_tier', ['hot', 'warm']),
        sb.from('target_companies').select('id', { count: 'exact', head: true }).eq('campaign_id', req.params.id).eq('meeting_booked', true),
        sb.from('company_contacts').select('id', { count: 'exact', head: true }).eq('campaign_id', req.params.id).in('enrichment_status', ['enriched', 'enriched_apify']),
        sb.from('company_contacts').select('id', { count: 'exact', head: true }).eq('campaign_id', req.params.id).in('pipeline_stage', ['contacted', 'meeting_booked', 'exhausted']),
      ]);

      res.json({
        ...campaign,
        stats: {
          companies_found:   companiesFound || 0,
          hot_leads:         hotLeads || 0,
          meetings_booked:   meetingsBooked || 0,
          contacts_enriched: contactsEnriched || 0,
          outreach_sent:     outreachSent || 0,
        },
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // PATCH /api/sourcing/campaigns/:id — update campaign settings
  app.patch('/api/sourcing/campaigns/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const allowed = [
        'name', 'firm_name', 'firm_type', 'investment_thesis',
        'target_sector', 'target_geography', 'company_stage', 'business_model',
        'ownership_type', 'deal_type', 'investment_size',
        'min_revenue', 'max_revenue', 'min_ebitda', 'max_ebitda',
        'min_company_age_months', 'max_company_age_months',
        'headcount_min', 'headcount_max',
        'intent_signals', 'keywords_include', 'keywords_exclude',
        'linkedin_connection_window', 'linkedin_dm_window', 'email_send_window',
        'timezone', 'batch_size', 'max_companies_per_campaign', 'status',
      ];
      const updates = { updated_at: new Date().toISOString() };
      for (const key of allowed) {
        if (req.body[key] !== undefined) updates[key] = req.body[key];
      }
      const { data, error: dbErr } = await sb.from('sourcing_campaigns')
        .update(updates).eq('id', req.params.id).select().single();
      if (dbErr) throw new Error(dbErr.message);
      res.json({ success: true, campaign: data });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns/:id/pause
  app.post('/api/sourcing/campaigns/:id/pause', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data } = await sb.from('sourcing_campaigns')
        .update({ status: 'paused', updated_at: new Date().toISOString() })
        .eq('id', req.params.id).select().single();
      pushActivity({ type: 'system', action: 'Campaign paused', note: data?.name || req.params.id });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns/:id/resume
  app.post('/api/sourcing/campaigns/:id/resume', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data } = await sb.from('sourcing_campaigns')
        .update({ status: 'active', updated_at: new Date().toISOString() })
        .eq('id', req.params.id).select().single();
      pushActivity({ type: 'system', action: 'Campaign resumed', note: data?.name || req.params.id });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns/:id/close
  app.post('/api/sourcing/campaigns/:id/close', async (req, res) => {
    try {
      const sb = getSupabase();
      const { data } = await sb.from('sourcing_campaigns')
        .update({ status: 'closed', updated_at: new Date().toISOString() })
        .eq('id', req.params.id).select().single();
      pushActivity({ type: 'system', action: 'Campaign closed', note: data?.name || req.params.id });
      await sendTelegram(`Sourcing campaign closed: ${data?.name || req.params.id}`).catch(() => {});
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns/:id/reopen
  app.post('/api/sourcing/campaigns/:id/reopen', async (req, res) => {
    try {
      const sb = getSupabase();
      await sb.from('sourcing_campaigns')
        .update({ status: 'active', updated_at: new Date().toISOString() })
        .eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/sourcing/campaigns/:id — permanently delete campaign and all related data
  app.delete('/api/sourcing/campaigns/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const id = req.params.id;

      // Delete in dependency order: approval_queue → company_contacts → target_companies → campaign
      await sb.from('approval_queue').delete().eq('campaign_id', id).then(null, () => {});
      await sb.from('company_contacts').delete().eq('campaign_id', id).then(null, () => {});
      await sb.from('target_companies').delete().eq('campaign_id', id).then(null, () => {});
      const { error: delErr } = await sb.from('sourcing_campaigns').delete().eq('id', id);
      if (delErr) throw new Error(delErr.message);

      pushActivity({ type: 'system', action: 'Campaign deleted', note: id });
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/sourcing/campaigns/:id/companies — list target companies
  app.get('/api/sourcing/campaigns/:id/companies', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { tier } = req.query;

      let query = sb.from('target_companies')
        .select('*')
        .eq('campaign_id', req.params.id)
        .order('created_at', { ascending: true })
        .limit(200);

      if (tier) query = query.eq('match_tier', tier);

      const { data, error: dbErr } = await query;
      if (dbErr) throw new Error(dbErr.message);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/sourcing/companies/:id — single company with contacts
  app.get('/api/sourcing/companies/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: company, error: dbErr } = await sb.from('target_companies')
        .select('*').eq('id', req.params.id).single();
      if (dbErr || !company) return res.status(404).json({ error: 'Company not found' });
      const { data: contacts } = await sb.from('company_contacts')
        .select('*').eq('company_id', req.params.id).order('created_at');
      res.json({ ...company, contacts: contacts || [] });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/companies/:id/archive
  app.post('/api/sourcing/companies/:id/archive', async (req, res) => {
    try {
      const sb = getSupabase();
      await sb.from('target_companies').update({ match_tier: 'archive', outreach_status: 'declined', updated_at: new Date().toISOString() }).eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/companies/:id/reinstate
  app.post('/api/sourcing/companies/:id/reinstate', async (req, res) => {
    try {
      const sb = getSupabase();
      await sb.from('target_companies').update({ match_tier: 'warm', outreach_status: 'pending', updated_at: new Date().toISOString() }).eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/sourcing/campaigns/:id/contacts — all company contacts for a campaign
  app.get('/api/sourcing/campaigns/:id/contacts', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('company_contacts')
        .select('*, target_companies(company_name, match_tier, match_score)')
        .eq('campaign_id', req.params.id)
        .order('created_at', { ascending: true })
        .limit(500);
      if (dbErr) throw new Error(dbErr.message);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/sourcing/campaigns/:id/meetings — meetings booked
  app.get('/api/sourcing/campaigns/:id/meetings', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('target_companies')
        .select('*, company_contacts(name, title, pipeline_stage)')
        .eq('campaign_id', req.params.id)
        .eq('meeting_booked', true)
        .order('meeting_booked_at', { ascending: false });
      if (dbErr) throw new Error(dbErr.message);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/contacts/:id/skip
  app.post('/api/sourcing/contacts/:id/skip', async (req, res) => {
    try {
      const sb = getSupabase();
      await sb.from('company_contacts').update({ pipeline_stage: 'skipped', updated_at: new Date().toISOString() }).eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sourcing/campaigns/:id/trigger-research — manually trigger research
  app.post('/api/sourcing/campaigns/:id/trigger-research', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: campaign } = await sb.from('sourcing_campaigns').select('*').eq('id', req.params.id).single();
      if (!campaign) return res.status(404).json({ error: 'Campaign not found' });

      pushActivity({ type: 'research', action: 'Manual research triggered', note: `[${campaign.name}]` });
      res.json({ success: true, message: `Research triggered for ${campaign.name}` });

      setImmediate(async () => {
        try {
          const { researchCompaniesForCampaign } = await import('../sourcing/companyResearcher.js');
          await researchCompaniesForCampaign(campaign);
        } catch (e) {
          console.error('[SOURCING] Manual research failed:', e.message);
        }
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── INVESTOR DATABASE ────────────────────────────────────────────────────

  // Multer with disk storage for large file uploads (xlsx, pdf, docx)
  const fileUpload = multer({
    storage: multer.diskStorage({
      destination: (req, file, cb) => cb(null, '/tmp'),
      filename:    (req, file, cb) => cb(null, `roco-${Date.now()}-${file.originalname}`),
    }),
  }).single('file');

  // GET /api/investors-db/stats
  app.get('/api/investors-db/stats', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { count: total } = await sb.from('investors_db')
        .select('*', { count: 'exact', head: true });
      // Paginate through all rows — Supabase caps at 1,000 per request
      let allCats = [];
      const PAGE = 1000;
      let from = 0;
      while (true) {
        const { data, error: pgErr } = await sb.from('investors_db')
          .select('investor_category')
          .range(from, from + PAGE - 1);
        if (pgErr || !data || data.length === 0) break;
        allCats = allCats.concat(data);
        if (data.length < PAGE) break;
        from += PAGE;
      }
      const byCategory = {};
      let uncategorised = 0;
      allCats.forEach(r => {
        const cat = r.investor_category || 'Uncategorised';
        byCategory[cat] = (byCategory[cat] || 0) + 1;
        if (!r.investor_category) uncategorised++;
      });
      const sorted = Object.fromEntries(
        Object.entries(byCategory).sort(([, a], [, b]) => b - a)
      );
      res.json({ total: total || 0, by_category: sorted, uncategorised });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/investors-db/import — upload XLSX and import to investors_db
  app.post('/api/investors-db/import', (req, res, next) => {
    fileUpload(req, res, (err) => {
      if (err) return res.status(400).json({ error: err.message });
      next();
    });
  }, async (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
      pushActivity({ type: 'research', action: `Database import started: ${req.file.originalname}`, note: '' });

      // Create or find the named list record
      const listName = (req.body.list_name || req.file.originalname.replace(/\.[^/.]+$/, '')).trim();
      const listType = req.body.list_type || 'standard';
      const listSource = req.body.list_source || 'pitchbook';
      let listId = null;
      const sbList = getSupabase();
      if (sbList) {
        const { data: existingList } = await sbList.from('investor_lists')
          .select('id').eq('name', listName).maybeSingle();
        if (existingList) {
          listId = existingList.id;
        } else {
          const { data: newList } = await sbList.from('investor_lists').insert({
            name: listName, list_type: listType, source: listSource,
          }).select().single();
          listId = newList?.id;
        }
      }

      const { importXLSXToDatabase } = await import('../core/investorDatabaseImporter.js');
      const result = await importXLSXToDatabase({
        filePath: req.file.path,
        filename: req.file.originalname,
        listId,
        listName,
        broadcastFn: (msg) => pushActivity({ type: 'research', action: msg, note: '' }),
      });
      fs.unlinkSync(req.file.path);
      res.json({ success: true, list_id: listId, list_name: listName, ...result });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // PUT /api/investor-lists/:id — rename a list and update all investor rows
  app.put('/api/investor-lists/:id', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { name, list_type } = req.body;
      if (!name?.trim()) return res.status(400).json({ error: 'name is required' });
      const cleanName = name.trim();
      const updates = { name: cleanName, updated_at: new Date().toISOString() };
      if (list_type) updates.list_type = list_type;
      const { data, error } = await sb.from('investor_lists').update(updates).eq('id', req.params.id).select().single();
      if (error) return res.status(500).json({ error: error.message });
      // Keep investors_db rows in sync
      await sb.from('investors_db').update({ list_name: cleanName }).eq('list_id', req.params.id);
      pushActivity({
        type: 'system',
        action: `List renamed: "${cleanName}"`,
        note: `${data?.investor_count || ''} investors updated`,
      });
      res.json({ success: true, list: data });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/investor-lists — all named investor lists with counts
  app.get('/api/investor-lists', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data: lists } = await sb.from('investor_lists')
        .select('*').order('created_at', { ascending: false });
      const listsWithCounts = await Promise.all((lists || []).map(async list => {
        const { count } = await sb.from('investors_db')
          .select('*', { count: 'exact', head: true }).eq('list_id', list.id);
        return { ...list, investor_count: count || 0 };
      }));
      res.json(listsWithCounts);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/investors-db/search
  app.get('/api/investors-db/search', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { search, type, country, enrichment, contact_type, page = 1, limit = 50 } = req.query;
      const offset = (Number(page) - 1) * Number(limit);

      let query = sb.from('investors_db').select('*', { count: 'exact' });
      if (search) query = query.or(`name.ilike.%${search}%,description.ilike.%${search}%,preferred_industries.ilike.%${search}%,hq_city.ilike.%${search}%`);
      // Type filter: check both investor_category (e.g. "IndependentSponsor") and investor_type (e.g. "Family Office")
      if (type) query = query.or(`investor_category.ilike.%${type}%,investor_type.ilike.%${type}%`);
      if (country) query = query.ilike('hq_country', `%${country}%`);
      if (enrichment) query = query.ilike('enrichment_status', `%${enrichment}%`);
      if (contact_type === 'angel') query = query.eq('is_angel', true);
      else if (contact_type) query = query.eq('contact_type', contact_type);

      query = query
        .order('investments_last_12m', { ascending: false, nullsFirst: false })
        .range(offset, offset + Number(limit) - 1);

      const { data, error: dbErr, count } = await query;
      if (dbErr) throw new Error(dbErr.message);

      res.json({
        investors: data || [],
        total: count || 0,
        page: Number(page),
        limit: Number(limit),
        pages: Math.ceil((count || 0) / Number(limit)),
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/contacts-db/search — enriched individual contacts
  app.get('/api/contacts-db/search', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { search, firm, page = 1, limit = 50 } = req.query;
      const offset = (Number(page) - 1) * Number(limit);

      let query = sb.from('enriched_contacts').select('*', { count: 'exact' });
      if (search) query = query.or(`name.ilike.%${search}%,email.ilike.%${search}%,firm_name.ilike.%${search}%`);
      if (firm) query = query.eq('firm_name', firm);

      query = query.order('updated_at', { ascending: false })
        .range(offset, offset + Number(limit) - 1);

      const { data, error: dbErr, count } = await query;
      if (dbErr) throw new Error(dbErr.message);
      res.json({
        contacts: data || [],
        total: count || 0,
        page: Number(page),
        pages: Math.ceil((count || 0) / Number(limit)),
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/contacts-db/researched — contacts that have been individually researched and not archived
  app.get('/api/contacts-db/researched', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { search, page = 1, limit = 50 } = req.query;
      const offset = (Number(page) - 1) * Number(limit);

      let query = sb.from('contacts')
        .select(
          'id, name, company_name, job_title, email, phone, linkedin_url, ' +
          'sector_focus, geography, typical_cheque_size, aum_fund_size, ' +
          'past_investments, notes, source, enrichment_status, pipeline_stage, ' +
          'person_researched, investor_score, created_at, updated_at',
          { count: 'exact' }
        )
        // Show contacts that have been personally researched and queued for outreach
        // Threshold: Enriched stage and beyond (post phasePersonResearch + phaseEnrich)
        .in('pipeline_stage', [
          'Enriched', 'enriched',
          'linkedin_only', 'email_invalid_linkedin_only',
          'invite_sent', 'invite_accepted',
          'email_sent', 'dm_sent',
          'Replied', 'In Conversation',
          'Meeting Booked', 'Meeting Scheduled',
          'Archived', 'archived', 'ARCHIVED',
        ]);

      if (search) query = query.or(
        `name.ilike.%${search}%,company_name.ilike.%${search}%,email.ilike.%${search}%`
      );

      query = query.order('created_at', { ascending: false })
        .range(offset, offset + Number(limit) - 1);

      const { data, error: dbErr, count } = await query;
      if (dbErr) throw new Error(dbErr.message);
      res.json({
        contacts: data || [],
        total: count || 0,
        page: Number(page),
        pages: Math.ceil((count || 0) / Number(limit)),
      });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/investors-db/:id/deals — deal history for one investor
  app.get('/api/investors-db/:id/deals', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data, error: dbErr } = await sb.from('investor_deal_history')
        .select('*')
        .eq('investors_db_id', req.params.id)
        .order('added_at', { ascending: false });
      if (dbErr) throw new Error(dbErr.message);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── DEAL DOCUMENT PARSING ────────────────────────────────────────────────

  // POST /api/deals/parse-document — upload PDF/DOCX, parse with Kimi, return structured info
  app.post('/api/deals/parse-document', (req, res, next) => {
    fileUpload(req, res, (err) => {
      if (err) return res.status(400).json({ error: err.message });
      next();
    });
  }, async (req, res) => {
    let clientDisconnected = false;
    req.on('close', () => {
      if (!res.writableEnded) {
        clientDisconnected = true;
        pushActivity({ type: 'research', action: 'Document upload cancelled', note: '' });
        if (req.file?.path) try { fs.unlinkSync(req.file.path); } catch {}
      }
    });

    try {
      if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
      pushActivity({ type: 'research', action: `Analysing document: ${req.file.originalname}`, note: '' });
      const { parseDealDocument } = await import('../core/dealDocumentParser.js');
      const { parsed, extractedText } = await parseDealDocument(
        req.file.path,
        req.file.originalname,
        (msg) => { if (!clientDisconnected) pushActivity({ type: 'research', action: msg, note: '' }); }
      );

      if (clientDisconnected) return;

      const sb = getSupabase();
      let documentId = null;
      if (sb) {
        const { data: docRecord } = await sb.from('deal_documents').insert({
          filename:         req.file.originalname,
          file_type:        req.file.originalname.split('.').pop(),
          extracted_text:   extractedText.substring(0, 50000),
          parsed_deal_info: parsed,
        }).select().single();
        documentId = docRecord?.id || null;
      }

      try { fs.unlinkSync(req.file.path); } catch {}
      res.json({ success: true, parsed, document_id: documentId });
    } catch (err) {
      if (!clientDisconnected) res.status(500).json({ error: err.message });
    }
  });

  // ─── TRAIN YOUR AGENT — GUIDANCE ENDPOINTS ───────────────────────────────

  app.get('/api/guidance/investor', async (req, res) => {
    try {
      const guidance = await getInvestorGuidance();
      res.json({ success: true, data: guidance });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
  });

  app.get('/api/guidance/sourcing', async (req, res) => {
    try {
      const guidance = await getSourcingGuidance();
      res.json({ success: true, data: guidance });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
  });

  app.post('/api/guidance/investor', async (req, res) => {
    try {
      const saved = await saveInvestorGuidance(req.body);
      invalidateAgentContext();
      pushActivity('Agent configuration updated', 'system');
      res.json({ success: true, data: saved, message: 'Investor guidance saved. Active on next cycle.' });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
  });

  app.post('/api/guidance/sourcing', async (req, res) => {
    try {
      const saved = await saveSourcingGuidance(req.body);
      invalidateAgentContext();
      pushActivity('Agent sourcing configuration updated', 'system');
      res.json({ success: true, data: saved, message: 'Sourcing guidance saved. Active on next cycle.' });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
  });

  // DELETE /api/wipe-everything — total reset: wipe ALL data, Roco starts fresh
  app.delete('/api/wipe-everything', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });

      const tables = [
        'investors_db', 'contacts', 'deals', 'approval_queue', 'activity_log',
        'enriched_contacts', 'firm_outreach_state', 'deal_documents', 'deal_assets',
        'batches', 'firms', 'emails', 'replies', 'linkedin_messages',
      ];

      const results = {};
      for (const table of tables) {
        const { error: e } = await sb.from(table).delete().neq('id', '00000000-0000-0000-0000-000000000000');
        results[table] = e ? `error: ${e.message}` : 'wiped';
      }

      // Reset state.json
      const state = readState();
      state.activeDeals = [];
      state.rocoStatus = 'ACTIVE';
      writeState(state);

      pushActivity({ type: 'system', action: 'FULL RESET — All data wiped. Roco has zero knowledge.', note: '' });
      broadcast({ type: 'WIPE_COMPLETE' });

      info('[WIPE] Full system reset complete');
      res.json({ success: true, results });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ─── ERROR HANDLERS ───────────────────────────────────────────────────────

  // JSON error handler — must come after all routes, catches anything that calls next(err)
  app.use((err, req, res, next) => { // eslint-disable-line no-unused-vars
    console.error(`[ROCO] Unhandled route error ${req.method} ${req.path}:`, err.message);
    res.status(err.status || 500).json({ error: err.message || 'Internal server error' });
  });

  // ─── ANALYTICS ────────────────────────────────────────────────────────────

  app.get('/api/analytics/summary', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data } = await sb.from('deal_analytics')
        .select('*, deals(name)')
        .order('week_starting', { ascending: false })
        .limit(50);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/analytics/recommendations', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      const { data } = await sb.from('roco_recommendations')
        .select('*').order('generated_at', { ascending: false }).limit(20);
      res.json(data || []);
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.post('/api/analytics/recommendations/:id/apply', async (req, res) => {
    try {
      const { applyRecommendation } = await import('../core/analyticsEngine.js');
      await applyRecommendation(req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  app.post('/api/analytics/recommendations/:id/dismiss', async (req, res) => {
    try {
      const sb = getSupabase();
      if (!sb) return res.status(503).json({ error: 'Database unavailable' });
      await sb.from('roco_recommendations').update({ status: 'rejected' }).eq('id', req.params.id);
      res.json({ success: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // 404 handler — JSON for API/webhook paths, index.html for everything else
  app.use((req, res) => {
    if (req.path.startsWith('/api') || req.path.startsWith('/webhook')) {
      return res.status(404).json({ error: `Not found: ${req.method} ${req.path}` });
    }
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
  });
}

// ─────────────────────────────────────────────
// ACTIVITY PUSH
// ─────────────────────────────────────────────

export function pushActivity(entry) {
  // Normalise: ensure a human-readable `message` field is always present
  const message = entry.message || entry.note || entry.action || entry.summary || 'System event';
  const enriched = { ...entry, message, timestamp: new Date().toISOString() };
  activityFeed.push(enriched);
  if (activityFeed.length > MAX_FEED) activityFeed.shift();

  // Broadcast to all WebSocket clients
  if (wss) broadcastToAll({ type: 'activity', entry: enriched, feed: activityFeed.slice(-100) });

  // Persist to Supabase activity_log (best-effort, non-blocking)
  const sb = getSupabase();
  if (sb) {
    sb.from('activity_log').insert({
      event_type: (entry.type || 'system').toUpperCase(),
      summary:    message,
      deal_id:    entry.dealId || null,
    }).then(() => {}).catch(() => {});
  }
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────

function broadcastToAll(data) {
  if (!wss) return;
  const msg = JSON.stringify(data);
  wss.clients.forEach(ws => {
    if (ws.readyState === 1) ws.send(msg);
  });
}

// ─────────────────────────────────────────────
// DEBOUNCE BATCHER — 90-second multi-reply handler
// ─────────────────────────────────────────────

async function queueInboundWithDebounce({ fromEmail, fromUrn, fromName, bodyText, threadId, chatId, messageId, channel, emailAccountId, raw }) {
  if (!bodyText?.trim()) return;

  const contactKey = channel === 'linkedin' ? (fromUrn || fromName) : fromEmail;
  if (!contactKey) return;

  const batchKey  = `${channel}_${contactKey}`;
  const msgEntry  = { content: bodyText.trim(), received_at: new Date(), channel, threadId: threadId || chatId || null, messageId, emailAccountId: emailAccountId || null, raw };

  if (replyDebounceMap.has(batchKey)) {
    const existing = replyDebounceMap.get(batchKey);
    clearTimeout(existing.timer);
    existing.messages.push(msgEntry);
    existing.timer = setTimeout(() => flushReplyBatch(batchKey), REPLY_DEBOUNCE_MS);
    replyDebounceMap.set(batchKey, existing);
    console.log(`[REPLY] Batched message ${existing.messages.length} from ${contactKey}`);
  } else {
    // Resolve contact + context before batching
    const ctx = await resolveContactAndContext(contactKey, channel);

    if (!ctx.contact) {
      // Unknown contact — fall back to legacy handler
      console.log(`[REPLY] Contact not found for ${contactKey} — falling back to legacy handler`);
      await handleInboundReply({
        fromEmail: fromEmail || '', fromName: fromName || fromUrn || '', fromUrn: fromUrn || '',
        subject: '', bodyText, threadId: threadId || chatId || '', messageId: messageId || '',
        channel, threadField: channel === 'email' ? 'gmail_thread_id' : null,
        emailAccountId: emailAccountId || null,
      });
      return;
    }

    const timer = setTimeout(() => flushReplyBatch(batchKey), REPLY_DEBOUNCE_MS);
    replyDebounceMap.set(batchKey, {
      timer,
      contact:   ctx.contact,
      deal:      ctx.deal || null,
      campaign:  ctx.campaign || null,
      mode:      ctx.mode,
      threadId:  threadId || chatId || null,
      messages:  [msgEntry],
    });
    console.log(`[REPLY] Started batch for ${ctx.contact.name} (${ctx.mode})`);
  }
}

async function flushReplyBatch(batchKey) {
  const batch = replyDebounceMap.get(batchKey);
  replyDebounceMap.delete(batchKey);
  if (!batch?.messages?.length) return;
  console.log(`[REPLY] Flushing ${batch.messages.length} message(s) from ${batch.contact.name}`);
  await processRocoBatchedReply(batch).catch(err => {
    console.error(`[REPLY] processRocoBatchedReply failed for ${batch.contact.name}:`, err.message);
  });
}

async function resolveContactAndContext(contactKey, channel) {
  const sb = getSupabase();
  if (!sb) return { contact: null, deal: null, campaign: null, mode: null };

  // Investor outreach — check contacts table
  try {
    let q = sb.from('contacts').select('*, deals(*)').limit(1);
    q = channel === 'linkedin' ? q.eq('linkedin_provider_id', contactKey) : q.eq('email', contactKey);
    const { data: contacts } = await q;
    if (contacts?.length > 0) {
      const c = contacts[0];
      return { contact: c, deal: c.deals || null, campaign: null, mode: 'investor_outreach' };
    }
  } catch {}

  // Company sourcing — check company_contacts table
  try {
    let sq = sb.from('company_contacts').select('*, sourcing_campaigns(*), target_companies(*)').limit(1);
    sq = channel === 'linkedin' ? sq.eq('linkedin_provider_id', contactKey) : sq.eq('email', contactKey);
    const { data: sContacts } = await sq;
    if (sContacts?.length > 0) {
      const c = sContacts[0];
      return { contact: c, deal: null, campaign: c.sourcing_campaigns || null, mode: 'company_sourcing' };
    }
  } catch {}

  return { contact: null, deal: null, campaign: null, mode: null };
}

async function processRocoBatchedReply(batch) {
  const { contact, deal, campaign, mode, messages, threadId } = batch;
  const emailAccountId = messages.find(m => m.emailAccountId)?.emailAccountId || null;
  const sb = getSupabase();
  const combinedContent = messages.map(m => m.content).join('\n\n');
  const channel = messages[0].channel;
  const contextName = deal?.name || campaign?.name || 'Unknown';
  const contactTable = mode === 'investor_outreach' ? 'contacts' : 'company_contacts';

  // Log received messages
  for (const msg of messages) {
    await sb?.from('activity_log').insert({
      deal_id:    deal?.id || null,
      contact_id: contact.id,
      event_type: 'REPLY_RECEIVED',
      summary:    `[${contextName}]: ${contact.name} sent a message via ${channel}`,
      detail:     { content: msg.content, channel },
    }).catch(() => {});
  }

  // Update contact state
  await sb?.from(contactTable).update({
    response_received: true,
    last_reply_at:     new Date().toISOString(),
    response_summary:  combinedContent.slice(0, 200),
  }).eq('id', contact.id).catch(() => {});

  // Record which channel the reply came in on (for channel loyalty in responses)
  await sb?.from(contactTable).update({ reply_channel: channel })
    .eq('id', contact.id).catch(() => {});

  // Log to conversation_messages table (investor outreach only)
  if (mode === 'investor_outreach' && contact?.id) {
    const msgToLog = messages[0];
    await logConversationMessage({
      contactId:        contact.id,
      dealId:           deal?.id || null,
      direction:        'inbound',
      channel,
      subject:          msgToLog?.subject || null,
      body:             combinedContent,
      unipileMessageId: msgToLog?.messageId || null,
    }).catch(() => {});
  }

  // Load conversation history — prefer conversation_messages table, fall back to activity_log
  let convMessages = [];
  if (mode === 'investor_outreach' && contact?.id) {
    convMessages = await getConversationHistory(contact.id).catch(() => []);
  }

  // Build plain-text history for existing classifyAndDraftRocoReply prompt format
  const { data: history } = await sb?.from('activity_log')
    .select('*')
    .eq('contact_id', contact.id)
    .in('event_type', ['REPLY_RECEIVED', 'EMAIL_SENT', 'LINKEDIN_DM_SENT', 'MESSAGE_SENT', 'OUTREACH_SENT'])
    .order('created_at', { ascending: true }) || { data: [] };

  const conversationHistory = (history || []).map(h => {
    const isInbound = h.event_type === 'REPLY_RECEIVED';
    return `${isInbound ? contact.name : 'Roco'}: ${h.detail?.content || h.summary}`;
  }).join('\n');

  // Mid-conversation research if triggered
  let researchContext = '';
  if (detectResearchNeeded(combinedContent)) {
    researchContext = await conductMidConversationResearch(combinedContent, contact, deal, campaign, mode, contextName);
  }

  // Load guidance fresh
  const guidanceBlock = await buildGuidanceBlock(mode);

  // Classify and draft
  const classification = await classifyAndDraftRocoReply(
    contact, combinedContent, conversationHistory,
    deal, campaign, mode, contextName,
    researchContext, guidanceBlock, messages.length
  );

  if (!classification) {
    await sb?.from('activity_log').insert({
      deal_id:    deal?.id || null,
      contact_id: contact.id,
      event_type: 'REPLY_CLASSIFICATION_ERROR',
      summary:    `[${contextName}]: Failed to classify reply from ${contact.name}`,
    }).catch(() => {});
    return;
  }

  // ── CONVERSATION STATE MACHINE (investor_outreach only) ─────────────────────
  if (mode === 'investor_outreach' && contact?.id) {
    try {
      // Run rich intent classification via conversationManager
      const intent = await classifyIntent(combinedContent, convMessages, contact, deal).catch(() => null);

      if (intent) {
        console.log(`[INTENT] ${contact.name} @ ${contact.company_name}: ${intent.intent_key} (${intent.category}, confidence: ${intent.confidence})`);

        // Append to intent_history on contact
        await appendIntentHistory(contact.id, {
          channel,
          message_preview:  combinedContent.substring(0, 100),
          intent:           intent.intent_key,
          intent_category:  intent.category,
          action_taken:     intent.suggested_action,
        }).catch(() => {});

        // Update intent on the conversation_messages record just inserted
        if (sb) {
          await sb.from('conversation_messages')
            .update({ intent: intent.intent_key, intent_confidence: intent.confidence, action_taken: intent.suggested_action })
            .eq('contact_id', contact.id)
            .eq('direction', 'inbound')
            .order('created_at', { ascending: false })
            .limit(1)
            .catch(() => {});
        }

        // Apply conversation state
        if (intent.conversation_state === 'do_not_contact') {
          await setConversationState(contact.id, 'do_not_contact', {
            conversation_ended_reason: 'Requested removal',
            pipeline_stage:            'Inactive',
          }).catch(() => {});
          console.log(`[CONV] ${contact.name} marked Do Not Contact`);

        } else if (intent.is_temp_close) {
          await setConversationState(contact.id, 'temp_closed', {
            temp_closed_reason: combinedContent.substring(0, 200),
          }).catch(() => {});
          console.log(`[CONV] ${contact.name} temp_closed — will re-engage in 5 days`);

        } else if (intent.is_conversation_ended) {
          await setConversationState(contact.id, intent.conversation_state, {
            conversation_ended_reason: intent.conversation_ended_reason || intent.intent_key,
            pipeline_stage:            intent.category === 'positive' ? 'Meeting Booked' : 'Inactive',
          }).catch(() => {});

        } else {
          await setConversationState(contact.id, intent.conversation_state || 'awaiting_response').catch(() => {});
        }

        // Flag to Dom if required
        if (intent.requires_dom) {
          pushActivity({
            type:      'alert',
            action:    `DOM ACTION NEEDED`,
            note:      `${contact.name} @ ${contact.company_name} — ${intent.dom_flag_reason || intent.intent_key}`,
            dealId:    deal?.id,
            deal_name: deal?.name,
          });
          await sendTelegram(
            `⚠️ *Dom action needed*\n${contact.name} at ${contact.company_name}\nIntent: ${intent.intent_key}\n${intent.dom_flag_reason || ''}`
          ).catch(() => {});
        }
      }
    } catch (intentErr) {
      console.warn('[CONV] Intent classification error:', intentErr.message);
    }
  }
  // ────────────────────────────────────────────────────────────────────────────

  if (classification.intent === 'not_interested') {
    await handleNegativeReplyResponse(contact, deal, campaign, mode, contextName);
    return;
  }

  if (classification.intent === 'conversation_end') {
    // Conversation reached natural close (deal closed, they got what they needed, etc.) — archive without suppression
    await sb?.from('contacts').update({
      pipeline_stage:    'Archived',
      response_received: true,
      response_summary:  'Conversation ended',
    }).eq('id', contact.id).catch(() => {});
    await sb?.from('activity_log').insert({
      deal_id:    deal?.id || null,
      contact_id: contact.id,
      event_type: 'CONVERSATION_ENDED',
      summary:    `[${contextName}]: Conversation with ${contact.name} ended naturally`,
    }).catch(() => {});
    await sendTelegram(
      `🔚 *Conversation ended* — ${contact.name} (${contact.company_name || 'unknown'})\n[${contextName}] — archived.`
    ).catch(() => {});
    return;
  }

  // Push to activity feed
  pushActivity({
    type:   'reply',
    action: `${contact.name} replied`,
    note:   `${channel === 'linkedin' ? 'LinkedIn' : 'Email'} · ${classification.intent} · "${combinedContent.substring(0, 80)}"`,
    dealId: deal?.id,
  });

  // Queue each reply for Telegram approval
  for (let i = 0; i < (classification.messages_to_send || []).length; i++) {
    const reply = classification.messages_to_send[i];
    if (!reply?.body?.trim()) continue;

    // Simple placeholder guard
    if (/\[link\]|\[placeholder\]|\[calendar\]/i.test(reply.body)) {
      console.warn(`[REPLY] Placeholder detected in reply to ${contact.name} — skipping`);
      continue;
    }

    // Deduplication check
    if (await isDuplicateReplyApproval(contact.id, channel === 'linkedin' ? 'linkedin_dm' : 'email_reply')) {
      console.log(`[REPLY] Duplicate suppressed for ${contact.name}`);
      continue;
    }

    const { data: queued } = await sb?.from('approval_queue').insert({
      deal_id:         deal?.id || null,
      candidate_id:    contact.id,
      campaign_id:     campaign?.id || null,
      company_contact_id: mode === 'company_sourcing' ? contact.id : null,
      message_type:    channel === 'linkedin' ? 'linkedin_dm' : 'email_reply',
      outreach_mode:   mode === 'investor_outreach' ? 'investor' : 'company_sourcing',
      channel,
      message_text:    reply.body,
      subject_a:       reply.subject || null,
      reply_to_id:     threadId || null,
      status:          'pending',
    }).select().single() || { data: null };

    if (queued) {
      const label = (classification.messages_to_send.length > 1)
        ? `Reply ${i + 1}/${classification.messages_to_send.length}: ${reply.reply_to_context || ''}`
        : null;

      await sendReplyForApproval(queued.id, contact, reply.body, contextName, channel, threadId, emailAccountId).catch(() => {});

      await sb?.from('activity_log').insert({
        deal_id:    deal?.id || null,
        contact_id: contact.id,
        event_type: 'REPLY_QUEUED',
        summary:    `[${contextName}]: Reply drafted for ${contact.name} — awaiting approval`,
        detail:     label ? { label } : null,
      }).catch(() => {});
    }
  }
}

async function classifyAndDraftRocoReply(contact, combinedContent, conversationHistory, deal, campaign, mode, contextName, researchContext, guidanceBlock, messageCount) {
  const modeFrame = mode === 'investor_outreach'
    ? `You are responding on behalf of the deal principal. The contact is a potential investor. Goal: build interest and get them on a call or moving toward committing capital.`
    : `You are responding on behalf of the investment firm. The contact is a decision maker at a company the firm wants to invest in. Goal: build interest in the investment opportunity and get them on a call.`;

  const contextBlock = mode === 'investor_outreach' && deal
    ? `DEAL: ${deal.name} | ${deal.raise_type || deal.type || 'Investment'} | ${deal.sector} | Target: ${deal.target_amount || 'TBD'} | Min cheque: ${deal.min_cheque || deal.cheque_min || '—'}\nCONTACT: ${contact.name} at ${contact.company_name || 'unknown'} | Score: ${contact.investor_score || '—'}/100`
    : `CAMPAIGN: ${campaign?.name || 'Unknown'} | Firm: ${campaign?.firm_name || '—'} (${campaign?.firm_type || '—'})\nTHESIS: ${campaign?.investment_thesis || '—'}\nCONTACT: ${contact.name}, ${contact.title || '—'} at ${contact.company_name || 'unknown'}`;

  const researchBlock = researchContext
    ? `\nRESEARCH RESULTS (incorporate naturally — do not quote verbatim):\n${researchContext}`
    : '';

  const firstName = contact.name?.split(' ')[0] || 'there';

  const prompt = `${guidanceBlock}

${modeFrame}

${contextBlock}

FULL CONVERSATION:
${conversationHistory || '(no prior history)'}

LATEST (${messageCount} message${messageCount > 1 ? 's' : ''} received together):
${combinedContent}
${researchBlock}

Instructions:
1. Read all messages as one coherent turn
2. Determine intent, what they are asking/saying, and what the ideal response is
3. One reply handles everything unless they asked genuinely separate questions requiring distinct answers
4. Always move toward the next concrete step — a call, more info, or a soft close
5. Use first name: "${firstName}"
6. No em-dashes, no bullet points, no corporate language, 4-6 sentences max per reply
7. Sound like a knowledgeable human counterpart, not an AI agent

Return ONLY valid JSON (no markdown):
{
  "intent": "interested|not_interested|asking_question|providing_info|considering|neutral|conversation_end",
  "sentiment": "positive|negative|neutral",
  "key_points": ["point1"],
  "ready_for_call": true,
  "messages_to_send": [
    {
      "reply_to_context": "what this reply addresses",
      "body": "full reply body",
      "subject": "email subject if email channel, null otherwise"
    }
  ],
  "next_action": "push_for_call|answer_question|provide_info|archive|continue",
  "internal_note": "one sentence on where this conversation stands"
}`;

  try {
    const text = await aiComplete(prompt, { maxTokens: 1000, task: `reply_classify:${contact.id}` });
    const parsed = JSON.parse(text.replace(/```json|```/g, '').trim());
    return parsed;
  } catch (err) {
    console.error(`[REPLY] classifyAndDraftRocoReply failed for ${contact.name}:`, err.message);
    return null;
  }
}

function detectResearchNeeded(content) {
  const triggers = [
    'what other deals', 'track record', 'market size', 'current market',
    'comparable', 'similar deals', 'who else', 'other investors', 'proof',
    'how does this compare', 'valuation', 'recent news', 'traction',
    'revenue', 'growth rate', 'what stage', 'what round', 'case study',
  ];
  const lower = content.toLowerCase();
  return triggers.some(t => lower.includes(t));
}

async function conductMidConversationResearch(content, contact, deal, campaign, mode, contextName) {
  const key = process.env.GROK_API_KEY;
  if (!key) return '';
  try {
    const sector = deal?.sector || campaign?.target_sector || 'the relevant sector';
    const ctxDesc = mode === 'investor_outreach'
      ? `Fundraising for ${deal?.name} in ${sector}`
      : `Sourcing companies for ${campaign?.firm_name} in ${sector}`;

    const prompt = `A contact named ${contact.name} has asked this during an investment conversation:\n"${content}"\n\nContext: ${ctxDesc}\n\nUse web search to find 3-5 specific, accurate, current facts that would help answer this question in a confident, informed way. Be specific — cite real market data, comparable deals, or recent news where possible.\n\nReturn a concise factual paragraph only — no preamble, no explanation.`;

    const res = await fetch('https://api.x.ai/v1/responses', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` },
      body: JSON.stringify({
        model:  'grok-4-latest',
        input:  [{ role: 'user', content: prompt }],
        tools:  [{ type: 'web_search' }],
      }),
    });
    if (!res.ok) throw new Error(`Grok ${res.status}`);
    const data = await res.json();
    const outputMsg = (data.output || []).find(o => o.type === 'message');
    const text = outputMsg?.content?.find(c => c.type === 'output_text')?.text || '';

    const sb = getSupabase();
    await sb?.from('activity_log').insert({
      contact_id: contact.id,
      event_type: 'MID_CONVERSATION_RESEARCH',
      summary:    `[${contextName}]: Mid-conversation research for ${contact.name}`,
      detail:     { question_preview: content.slice(0, 100) },
    }).catch(() => {});

    return text;
  } catch (err) {
    console.warn(`[REPLY] Mid-conversation research failed: ${err.message}`);
    return '';
  }
}

async function handleNegativeReplyResponse(contact, deal, campaign, mode, contextName) {
  const table = mode === 'investor_outreach' ? 'contacts' : 'company_contacts';
  const sb = getSupabase();
  await sb?.from(table).update({
    pipeline_stage:   'declined',
    response_received: true,
    response_summary:  'Not interested',
  }).eq('id', contact.id).catch(() => {});

  const isAngel = contact.is_angel || contact.contact_type === 'angel';

  if (mode === 'investor_outreach' && !isAngel && contact.firm_id && deal?.id) {
    // Institutional investor — suppress the whole firm
    await sb?.from('firm_outreach_state').upsert({
      firm_id:               contact.firm_id,
      deal_id:               deal.id,
      response_received:     true,
      responding_contact_id: contact.id,
      status:                'declined',
    }).catch(() => {});

    await sb?.from('firm_suppressions').upsert({
      firm_id:    contact.firm_id,
      deal_id:    deal.id,
      reason:     'declined',
      suppressed_at: new Date().toISOString(),
    }, { onConflict: 'firm_id,deal_id' }).catch(() => {});
  } else if (mode === 'investor_outreach' && isAngel) {
    // Angel — they represent only themselves, no firm suppression
    console.log(`[SUPPRESSION] ${contact.name} is angel — individual suppression only, no firm-wide suppression`);
  } else if (mode === 'company_sourcing' && contact.company_id) {
    await sb?.from('target_companies').update({
      firm_responded:  true,
      outreach_status: 'declined',
    }).eq('id', contact.company_id).catch(() => {});
  }

  const suppressionScope = (mode === 'investor_outreach' && isAngel)
    ? 'individual suppressed'
    : mode === 'investor_outreach'
      ? 'firm suppressed'
      : 'company suppressed';
  await sendTelegram(
    `⛔ *Not interested* — ${contact.name} (${contact.company_name || 'unknown'})\n` +
    `[${contextName}] — ${suppressionScope}.`
  ).catch(() => {});

  await sb?.from('activity_log').insert({
    deal_id:    deal?.id || null,
    contact_id: contact.id,
    event_type: 'OUTREACH_SUPPRESSED',
    summary:    `[${contextName}]: ${contact.name} declined — suppressed from further outreach`,
  }).catch(() => {});
}

async function isDuplicateReplyApproval(candidateId, messageType, withinSeconds = 300) {
  try {
    const sb = getSupabase();
    if (!sb) return false;
    const since = new Date(Date.now() - withinSeconds * 1000).toISOString();
    const { data } = await sb.from('approval_queue')
      .select('id')
      .eq('candidate_id', candidateId)
      .eq('message_type', messageType)
      .eq('status', 'pending')
      .gte('created_at', since)
      .limit(1);
    return (data?.length || 0) > 0;
  } catch { return false; }
}

// ─────────────────────────────────────────────
// INBOUND REPLY HANDLERS (called by Unipile webhooks)
// ─────────────────────────────────────────────

async function classifyWithGpt({ body, fromEmail, fromName, dealName, dealDescription }) {
  const prompt =
    `Classify this investor reply for deal: "${dealName}".\n\n` +
    `From: ${fromName || fromEmail}\n` +
    `Message:\n"""\n${body.slice(0, 800)}\n"""\n\n` +
    `Return JSON: { "intent": "INTERESTED|WANTS_MORE_INFO|MEETING_REQUEST|NOT_INTERESTED|OPT_OUT|AUTO_REPLY|CONVERSATION_END|POSITIVE|NEUTRAL", "notes": "...", "sentiment": "positive|neutral|negative" }\n\nUse CONVERSATION_END when the investor is clearly wrapping up (e.g. "Thanks, I'll be in touch", "Not for us right now", "Let's revisit next year", "Good luck with the raise").`;
  try {
    const text = await aiComplete(prompt, {
      reasoning: 'low',
      maxTokens: 200,
      task: `classify:${fromName || fromEmail}`,
    });
    const parsed = JSON.parse(text.replace(/```json|```/g, '').trim());
    return { intent: parsed.intent || 'NEUTRAL', notes: parsed.notes || '', sentiment: parsed.sentiment || 'neutral' };
  } catch (err) {
    console.warn('[INBOUND] classify failed:', err.message);
    return { intent: 'NEUTRAL', notes: '', sentiment: 'neutral' };
  }
}

// Close a conversation: update stage in Supabase + Notion, generate summary, notify Dom
async function closeConversation({ contact, sb, outcome, newStage, summary }) {
  if (!contact) return;

  // Update Supabase stage
  try { await sb.from('contacts').update({ pipeline_stage: newStage }).eq('id', contact.id); } catch { /* non-fatal */ }

  // Generate a concise AI summary of the whole conversation thread
  let conversationSummary = summary;
  try {
    const { data: replies } = await sb.from('replies')
      .select('body, received_at, classification')
      .eq('contact_id', contact.id)
      .order('received_at', { ascending: true });

    if (replies?.length) {
      const thread = replies.map(r => `[${r.classification}] ${r.body}`).join('\n\n---\n\n');
      conversationSummary = await aiComplete(
        `Summarise this fundraising conversation with ${contact.name} in 3–5 sentences. ` +
        `State the outcome clearly (${outcome}), key points raised, and any action items for Dom.\n\n${thread.slice(0, 2000)}`,
        { reasoning: 'low', maxTokens: 200, task: 'conversation_summary' }
      ).catch(() => summary);
    }
  } catch { /* use fallback summary */ }

  // Write summary to Notion notes
  if (contact.notion_page_id) {
    const timestamp = new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
    await updateContact(contact.notion_page_id, {
      pipelineStage: newStage,
      notes: `[${timestamp} — Outcome: ${outcome}]\n${conversationSummary}`,
    }).catch(e => console.warn('[CLOSE] Notion notes update failed:', e.message));
  }

  // Notify Dom
  await sendTelegram(
    `🔚 *Conversation Closed — ${outcome}*\n\n` +
    `Contact: ${contact.name}\n\n` +
    `Summary:\n${conversationSummary}\n\n` +
    `Stage updated to: ${newStage}`
  ).catch(() => {});

  pushActivity({
    type: 'linkedin',
    action: `Conversation closed: ${contact.name} — ${outcome}`,
    note: conversationSummary.substring(0, 150),
  });

  console.log(`[CLOSE] ${contact.name} → ${newStage}. Summary logged to Notion.`);
}

async function fetchDealAssets(dealId) {
  if (!dealId) return [];
  try {
    const sb = getSupabase();
    if (!sb) return [];
    const { data } = await sb.from('deal_assets').select('*').eq('deal_id', dealId).order('created_at', { ascending: true });
    return data || [];
  } catch { return []; }
}

function buildAssetContext(assets, intent) {
  if (!assets.length) return '';
  // For meeting requests: prioritise Calendly. For more info: prioritise deck then others.
  const relevant = intent === 'MEETING_REQUEST'
    ? assets.filter(a => a.asset_type === 'calendly').concat(assets.filter(a => a.asset_type !== 'calendly'))
    : assets.filter(a => a.asset_type === 'deck').concat(assets.filter(a => a.asset_type !== 'deck'));

  return relevant.slice(0, 4).map(a => {
    const label = { calendly: 'Booking link', deck: 'Pitch deck', image: 'Image', video: 'Video', link: 'Link', other: 'Resource' }[a.asset_type] || 'Resource';
    return `- ${a.name} (${label}): ${a.url}${a.description ? ` — ${a.description}` : ''}`;
  }).join('\n');
}

async function draftInstantReply({ contact, originalEmail, inboundBody, classification, deal, sb }) {
  const contactName = contact?.name || contact?.email || 'Investor';
  const dealId = originalEmail?.deal_id || deal?.id || contact?.deal_id;

  // 1 — Fetch deal fresh for full context
  let freshDeal = deal;
  if (dealId && sb) {
    const { data } = await sb.from('deals').select('*').eq('id', dealId).single();
    if (data) freshDeal = data;
  }

  // 2 — Fetch firm research context
  let firmContext = '';
  if (sb && contact?.company_name && dealId) {
    const { data: firms } = await sb.from('firms')
      .select('firm_name, firm_type, aum, focus_areas, geography, notes')
      .eq('deal_id', dealId)
      .ilike('firm_name', `%${contact.company_name}%`)
      .limit(1);
    const firm = firms?.[0];
    if (firm) {
      firmContext =
        `\nFirm research on ${firm.firm_name}:\n` +
        `Type: ${firm.firm_type || 'Unknown'} | AUM: ${firm.aum || 'Unknown'} | Geography: ${firm.geography || 'Unknown'}\n` +
        `Focus: ${(firm.focus_areas || '').substring(0, 200)}\n` +
        `Notes: ${(firm.notes || '').substring(0, 300)}\n`;
    }
  }

  // 3 — Fetch full conversation thread (sent emails + received replies, chronological)
  let threadTranscript = '';
  if (sb && contact?.id) {
    const [{ data: sentEmails }, { data: receivedReplies }] = await Promise.all([
      sb.from('emails').select('body, subject_used, sent_at, stage').eq('contact_id', contact.id).order('sent_at', { ascending: true }),
      sb.from('replies').select('body, received_at, classification').eq('contact_id', contact.id).order('received_at', { ascending: true }),
    ]);
    const events = [
      ...(sentEmails || []).map(e => ({ ts: e.sent_at, role: 'Dom', text: e.body })),
      ...(receivedReplies || []).map(r => ({ ts: r.received_at, role: contactName, text: r.body })),
    ].sort((a, b) => new Date(a.ts) - new Date(b.ts));
    if (events.length) {
      threadTranscript = '\n\nFULL CONVERSATION THREAD:\n' +
        events.map(e => `[${e.role}]: ${(e.text || '').substring(0, 400)}`).join('\n\n---\n\n');
    }
  }

  // 4 — Deal assets (only if available AND intent requires them)
  const assets = await fetchDealAssets(dealId);
  const hasCalendly = assets.some(a => a.asset_type === 'calendly');
  const hasDeck     = assets.some(a => a.asset_type === 'deck');
  const needsMeeting = ['MEETING_REQUEST'].includes(classification.intent);
  const needsInfo    = ['WANTS_MORE_INFO'].includes(classification.intent);

  // Asset context: only inject real URLs. Never inject placeholders.
  let assetInstruction = '';
  if (needsMeeting && hasCalendly) {
    const link = assets.find(a => a.asset_type === 'calendly');
    assetInstruction = `\nBooking link available — share this URL directly in your reply: ${link.url}\n`;
  } else if (needsInfo && hasDeck) {
    const link = assets.find(a => a.asset_type === 'deck');
    assetInstruction = `\nPitch deck available — share this URL directly in your reply: ${link.url}\n`;
  }

  // Dom action note for Telegram (what Dom needs to do manually if no asset)
  let domNote = '';
  if (needsMeeting && !hasCalendly) domNote = '📅 *Action needed:* Book this person in your calendar — no Calendly link set up yet.';
  if (needsInfo && !hasDeck) domNote = '📄 *Action needed:* Send them your deck — no deck asset uploaded yet.';

  // Load guidance and inject at top of prompt
  const guidanceBlock = await buildGuidanceBlock('investor_outreach').catch(() => '');

  const prompt =
    `${guidanceBlock}You are Dom, a fundraising professional. Write an instant, warm reply to this investor message.\n\n` +
    `DEAL: ${freshDeal?.name || 'our current deal'}\n` +
    `Description: ${(freshDeal?.description || '').substring(0, 300)}\n` +
    `Sector: ${freshDeal?.sector || ''} | Geography: ${freshDeal?.geography || 'UK'} | Raise: £${Number(freshDeal?.target_amount || 0).toLocaleString()}\n` +
    firmContext +
    `\nINVESTOR: ${contactName} | Company: ${contact?.company_name || ''} | Title: ${contact?.job_title || ''}\n` +
    `Their latest message:\n"""\n${inboundBody.slice(0, 600)}\n"""\n` +
    `Intent: ${classification.intent} | Sentiment: ${classification.sentiment}\n` +
    threadTranscript +
    assetInstruction +
    `\nRULES — follow these strictly:\n` +
    `- NEVER use placeholders like [link], [name], [calendar], [CALENDLY_LINK], etc. If you don't have a URL, don't reference it.\n` +
    `- If they want to meet and no booking link is provided above: ask for their email so Dom can get them booked in.\n` +
    `- If they want more info and no deck link is provided above: say Dom will get the relevant info sent over to them.\n` +
    `- Write as if Dom typed this himself — natural, warm, brief (3–5 sentences max).\n` +
    `- Continue naturally from the thread. Do NOT re-introduce yourself if you've already spoken.\n` +
    `- Do NOT use corporate jargon. Sign off as Dom.\n` +
    `Return ONLY the message body. No subject line. No markdown.`;

  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key':         process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Type':      'application/json',
      },
      body: JSON.stringify({
        model:      'claude-sonnet-4-6',
        max_tokens: 500,
        messages:   [{ role: 'user', content: prompt }],
      }),
    });
    if (!res.ok) {
      const t = await res.text();
      throw new Error(`Sonnet ${res.status}: ${t.substring(0, 200)}`);
    }
    const data = await res.json();
    const text = data.content?.[0]?.text || '';
    console.log(`[INBOUND] Sonnet drafted instant reply for ${contactName}`);
    return { body: text.trim(), domNote };
  } catch (err) {
    console.warn('[INBOUND] Sonnet reply draft failed:', err.message);
    return { body: '', domNote: '' };
  }
}

async function handleInboundReply({ fromEmail, fromName, fromUrn, subject, bodyText, threadId, messageId, channel, threadField, emailAccountId }) {
  if (!bodyText && !fromEmail && !fromUrn) return;
  console.log(`[INBOUND/${channel.toUpperCase()}] Reply from ${fromEmail || fromName} on thread ${threadId}`);

  const sb = getSupabase();

  // Find contact — try email first, then LinkedIn chat_id, then provider_id
  let contact = null;
  if (sb) {
    if (fromEmail) {
      const { data } = await sb.from('contacts').select('*').eq('email', fromEmail).limit(1).single();
      contact = data || null;
    }
    if (!contact && threadId) {
      // LinkedIn: match by the chat_id stored when we sent the original DM
      const { data } = await sb.from('contacts').select('*').eq('linkedin_chat_id', threadId).limit(1).single();
      contact = data || null;
    }
    if (!contact && fromUrn) {
      // Fall back to LinkedIn provider_id
      const { data } = await sb.from('contacts').select('*').eq('linkedin_provider_id', fromUrn).limit(1).single();
      contact = data || null;
    }
  }

  // Find original email by thread_id
  let originalEmail = null;
  if (sb && threadId) {
    const query = sb.from('emails').select('*, deals(name, description, sector)')
      .order('sent_at', { ascending: true })
      .limit(1)
      .single();
    if (threadField) query.eq(threadField, threadId);
    const { data } = await query;
    originalEmail = data || null;
  }

  // Log the inbound reply
  let replyRecord = null;
  if (sb) {
    const { data } = await sb.from('replies').insert({
      body:           bodyText,
      thread_id:      threadId || null,
      message_id:     messageId || null,
      contact_id:     contact?.id || null,
      deal_id:        originalEmail?.deal_id || null,
      received_at:    new Date().toISOString(),
      classification: 'PENDING',
    }).select().single();
    replyRecord = data || null;
  }

  // Update contact to "In Conversation" as soon as they reply
  if (contact && sb) {
    await sb.from('contacts').update({
      pipeline_stage:    'In Conversation',
      last_contacted:    new Date().toISOString().split('T')[0],
      last_contact_type: channel === 'linkedin' ? 'LinkedIn' : 'Email',
    }).eq('id', contact.id);

    if (contact.notion_page_id) {
      await updateContact(contact.notion_page_id, {
        pipelineStage:     'In Conversation',
        responseReceived:  'Yes',
        lastContactType:   channel === 'linkedin' ? 'LinkedIn' : 'Email',
        lastContactNotes:  `Reply received: ${bodyText.substring(0, 200)}`,
      }).catch(e => console.warn('[INBOUND] Notion update failed:', e.message));
    }
  }

  const deal = originalEmail?.deals;
  const classification = await classifyWithGpt({
    body: bodyText, fromEmail, fromName,
    dealName: deal?.name || 'Unknown Deal',
    dealDescription: deal?.description || '',
  });

  if (sb && replyRecord) {
    await sb.from('replies').update({
      classification:       classification.intent,
      classification_notes: classification.notes,
      sentiment:            classification.sentiment,
    }).eq('id', replyRecord.id);
  }

  console.log(`[INBOUND/${channel.toUpperCase()}] Classified as: ${classification.intent}`);

  if (['INTERESTED', 'WANTS_MORE_INFO', 'MEETING_REQUEST', 'POSITIVE'].includes(classification.intent)) {
    const replyContact = contact || { email: fromEmail, name: fromName };
    const { body: draftBody, domNote } = await draftInstantReply({
      contact: replyContact, originalEmail, inboundBody: bodyText, classification, deal, sb,
    });

    if (draftBody) {
      const replySubject = `Re: ${subject || originalEmail?.subject_used || originalEmail?.subject_a || 'our conversation'}`;
      let sent = null;

      // Send immediately — no approval gate, no sending window check
      if (channel === 'email' && replyContact.email) {
        sent = await sendEmailReply({
          to:                 replyContact.email,
          toName:             replyContact.name || '',
          subject:            replySubject,
          body:               draftBody,
          replyToProviderId:  threadId || null,
          accountId:          emailAccountId || null, // use same account inbound came from
        });
      } else if (channel === 'linkedin' && threadId) {
        sent = await sendLinkedInReply({ chatId: threadId, message: draftBody });
      }

      // Log the sent reply to emails table
      if (sb) {
        await sb.from('emails').insert({
          contact_id:         replyContact?.id || null,
          deal_id:            originalEmail?.deal_id || deal?.id || contact?.deal_id || null,
          stage:              'REPLY',
          direction:          'outbound',
          subject_a:          replySubject,
          subject_used:       replySubject,
          body:               draftBody,
          status:             sent ? 'sent' : 'send_failed',
          is_reply:           true,
          reply_to_record_id: replyRecord?.id || null,
          gmail_thread_id:    channel === 'email' ? (sent?.threadId || threadId || null) : null,
          sent_at:            sent ? new Date().toISOString() : null,
        }).catch(e => console.warn('[INBOUND] email log failed:', e.message));
      }

      const sentOk = sent ? '✅ Sent instantly' : '⚠️ Send failed — check logs';
      await sendTelegram(
        `💬 *Instant Reply ${sent ? 'Sent' : 'FAILED'}*\n\n` +
        `From: ${fromName || fromEmail || 'LinkedIn contact'}\n` +
        `Intent: ${classification.intent} | Channel: ${channel}\n\n` +
        `---\n${draftBody.substring(0, 600)}\n---\n\n` +
        sentOk +
        (domNote ? `\n\n${domNote}` : '')
      );

      pushActivity({
        type: 'email',
        action: `Reply sent to ${replyContact.name || fromName}`,
        note: `${channel === 'linkedin' ? 'LinkedIn' : 'Email'} · ${classification.intent} · ${sent ? 'Delivered' : 'Send failed'}`,
        dealId: originalEmail?.deal_id || contact?.deal_id,
      });

      console.log(`[INBOUND/${channel.toUpperCase()}] Instant reply ${sent ? 'sent' : 'FAILED'} to ${replyContact.name || fromEmail}`);
    }
  } else if (classification.intent === 'NOT_INTERESTED') {
    // Conversation over — close it out
    await closeConversation({
      contact, sb, outcome: 'Not Interested',
      newStage: 'Not Interested',
      summary: `${fromName || 'Contact'} replied indicating they are not interested. Message: "${bodyText.substring(0, 300)}"`,
    });
  } else if (classification.intent === 'CONVERSATION_END') {
    await closeConversation({
      contact, sb, outcome: 'Conversation Ended',
      newStage: 'Not Interested',
      summary: `${fromName || 'Contact'} wrapped up the conversation. Message: "${bodyText.substring(0, 300)}"`,
    });
  } else if (classification.intent === 'OPT_OUT') {
    // Hard opt-out — suppress and log
    await closeConversation({
      contact, sb, outcome: 'Opt Out',
      newStage: 'Suppressed — Opt Out',
      summary: `${fromName || 'Contact'} requested to opt out. Message: "${bodyText.substring(0, 300)}"`,
    });
  }

  broadcastToAll({
    type: 'REPLY_RECEIVED',
    reply: { from: fromName || fromEmail, subject, classification: classification.intent, thread_id: threadId, channel },
  });

  pushActivity({
    type: 'reply',
    action: `${fromName || fromEmail || 'Contact'} replied`,
    note: `${channel === 'linkedin' ? 'LinkedIn' : 'Email'} · ${classification.intent} · "${bodyText.substring(0, 80)}"`,
    dealId: originalEmail?.deal_id || contact?.deal_id,
  });

  await sendTelegram(
    `📩 *${channel === 'linkedin' ? 'LinkedIn Message' : 'Email Reply'} Received*\n\n` +
    `From: ${fromName || fromEmail}\n` +
    `Intent: ${classification.intent} | Sentiment: ${classification.sentiment}\n\n` +
    `${bodyText.substring(0, 300)}${bodyText.length > 300 ? '...' : ''}`
  );
}

async function handleInboundLinkedInMessage({ fromUrn, fromName, bodyText, chatId, messageId }) {
  await handleInboundReply({
    fromEmail:   '',
    fromName:    fromName || fromUrn,
    fromUrn:     fromUrn || '',
    subject:     'LinkedIn message',
    bodyText,
    threadId:    chatId,
    messageId,
    channel:     'linkedin',
    threadField: null,
  });
}

function extractResearchSummary(notes) {
  if (!notes.includes('[RESEARCHED:')) return null;
  return {
    comparableDeals: (notes.match(/Comparable Deals: (.+)/)?.[1] || '').split(', ').filter(Boolean),
    investmentCriteria: notes.match(/Criteria: (.+)/)?.[1] || '',
    approachAngle: notes.match(/Approach: (.+)/)?.[1] || '',
    confidenceScore: parseInt(notes.match(/Confidence: (\d+)/)?.[1] || '0'),
  };
}
