/**
 * api/index.js — Vercel serverless handler for Roco Mission Control
 *
 * All requests are routed here via vercel.json rewrites.
 * Serves the static dashboard + all API routes.
 * Data sources: Supabase (state, deals, templates, approvals) + Notion (contacts/pipeline).
 * No WebSocket — dashboard.js falls back to 8s polling automatically.
 */

import { config as dotenvConfig } from 'dotenv';
dotenvConfig();

import express from 'express';

import {
  loadSessionState, saveSessionState,
  getAllDeals, getActiveDeals, getDeal, createDeal, updateDeal,
  getTemplates, getTemplate, updateTemplate, seedDefaultTemplates,
  getActivityLog, logActivity,
  getBatches,
  getApprovalQueue, resolveApprovalInSupabase, addApprovalToQueue,
  deleteApprovalFromQueue,
} from '../core/supabaseSync.js';
import { getSupabase } from '../core/supabase.js';

import {
  getAllActiveContacts, getContactProp, updateContact, archiveContact,
} from '../crm/notionContacts.js';

import { getWindowStatus, getWindowVisualization } from '../core/scheduleChecker.js';
import { getBatchSummary } from '../core/batchManager.js';

const app = express();

// ── Auth ──────────────────────────────────────────────────────────────────────

const dashboardUser = (process.env.DASHBOARD_USER || 'admin').trim();
const dashboardPass = (process.env.DASHBOARD_PASS || 'roco2026').trim();

app.use((req, res, next) => {
  const auth = req.headers['authorization'];
  if (!auth) return challenge(res);
  const [type, encoded] = auth.split(' ');
  if (type !== 'Basic') return challenge(res);
  const decoded = Buffer.from(encoded, 'base64').toString();
  const colonIdx = decoded.indexOf(':');
  const user = decoded.slice(0, colonIdx);
  const pass = decoded.slice(colonIdx + 1);
  if (user === dashboardUser && pass === dashboardPass) return next();
  return challenge(res);
});

app.use(express.json());


// ── GET /api/state ────────────────────────────────────────────────────────────

app.get('/api/state', async (req, res) => {
  try {
    const state = await loadSessionState();
    const activeDeals = await getActiveDeals();
    res.json({ ...state, activeDeals });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/toggle ──────────────────────────────────────────────────────────

app.post('/api/toggle', async (req, res) => {
  const { key } = req.body;
  const allowed = ['outreachEnabled', 'followupEnabled', 'enrichmentEnabled', 'researchEnabled', 'linkedinEnabled', 'rocoStatus'];
  if (!allowed.includes(key)) return res.status(400).json({ error: 'Invalid toggle key' });
  try {
    const state = await loadSessionState();
    if (key === 'rocoStatus') {
      state.rocoStatus = state.rocoStatus === 'ACTIVE' ? 'PAUSED' : 'ACTIVE';
    } else {
      state[key] = !state[key];
    }
    await saveSessionState(state);
    res.json({ key, newValue: state[key] ?? state.rocoStatus, timestamp: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/pause  /api/resume (legacy) ─────────────────────────────────────

app.post('/api/pause', async (req, res) => {
  const state = await loadSessionState();
  state.rocoStatus = 'PAUSED';
  await saveSessionState(state);
  res.json({ ok: true, status: 'PAUSED' });
});

app.post('/api/resume', async (req, res) => {
  const state = await loadSessionState();
  state.rocoStatus = 'ACTIVE';
  await saveSessionState(state);
  res.json({ ok: true, status: 'ACTIVE' });
});

app.get('/api/status', async (req, res) => {
  const state = await loadSessionState();
  res.json({ status: state.rocoStatus || 'UNKNOWN' });
});

// ── GET /api/pipeline ─────────────────────────────────────────────────────────

app.get('/api/pipeline', async (req, res) => {
  try {
    const contacts = await getAllActiveContacts();
    res.json(contacts.map(c => ({
      id: c.id,
      name: getContactProp(c, 'Name'),
      firm: getContactProp(c, 'Company Name'),
      score: getContactProp(c, 'Investor Score (0-100)'),
      stage: getContactProp(c, 'Pipeline Stage'),
      lastContacted: getContactProp(c, 'Last Contacted'),
      nextFollowUp: getContactProp(c, 'Next Follow-up Date'),
      enrichmentStatus: getContactProp(c, 'Enrichment Status'),
      email: getContactProp(c, 'Email'),
    })));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/queue  (approval queue from Supabase) ────────────────────────────

app.get('/api/queue', async (req, res) => {
  try { res.json(await getApprovalQueue()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/approvals', async (req, res) => {
  try { res.json(await getApprovalQueue()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// ── POST /api/approve ─────────────────────────────────────────────────────────

app.post('/api/approve', async (req, res) => {
  const { id, subject } = req.body;
  if (!id) return res.status(400).json({ error: 'id required' });
  try {
    await resolveApprovalInSupabase(id, 'approved', subject || null, null);
    res.json({ success: true, message: 'Approved — VPS will send within 15 seconds' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/skip-approval ───────────────────────────────────────────────────

app.post('/api/skip-approval', async (req, res) => {
  const { id } = req.body;
  if (!id) return res.status(400).json({ error: 'id required' });
  try {
    // Hard delete from approval_queue — draft leaves no trace
    const sb = getSupabase();
    if (sb) await sb.from('approval_queue').delete().eq('id', id);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/edit-approval ───────────────────────────────────────────────────

app.post('/api/edit-approval', async (req, res) => {
  const { id, instructions } = req.body;
  if (!id || !instructions) return res.status(400).json({ error: 'id and instructions required' });
  try {
    await resolveApprovalInSupabase(id, 'edit', null, instructions);
    res.json({ success: true, message: 'Edit instructions sent — VPS will redraft within 15 seconds' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/health ───────────────────────────────────────────────────────────

app.get('/api/health', (req, res) => {
  const services = [
    { name: 'anthropic', configured: !!process.env.ANTHROPIC_API_KEY },
    { name: 'openai',    configured: !!process.env.OPENAI_API_KEY },
    { name: 'gemini',    configured: !!process.env.GEMINI_API_KEY },
    { name: 'notion',    configured: !!process.env.NOTION_API_KEY },
    { name: 'telegram',  configured: !!process.env.TELEGRAM_BOT_TOKEN },
    { name: 'kaspr',     configured: !!process.env.KASPR_API_KEY },
    { name: 'supabase',  configured: !!(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_KEY) },
    { name: 'gmail',     configured: false },
  ].map(s => ({
    name: s.name,
    status: s.configured ? 'ok' : 'unconfigured',
    fallbackActive: false,
    lastCheck: new Date().toISOString(),
  }));
  res.json({ services, lastUpdated: new Date().toISOString() });
});

// ── GET /api/stats ────────────────────────────────────────────────────────────

app.get('/api/stats', async (req, res) => {
  try {
    const [contacts, deals, queue] = await Promise.allSettled([
      getAllActiveContacts(),
      getActiveDeals(),
      getApprovalQueue(),
    ]);
    const c = contacts.value || [];
    const d = (deals.value || [])[0] || {};
    const q = queue.value || [];
    const replied = c.filter(x => {
      const s = getContactProp(x, 'Pipeline Stage') || '';
      return s.includes('Replied') || s.includes('Meeting');
    }).length;
    const sent = c.filter(x => {
      const s = getContactProp(x, 'Pipeline Stage') || '';
      return s.includes('Sent') || s.includes('Replied') || s.includes('Follow');
    }).length;
    res.json({
      totalContacts: c.length,
      emailsSent: sent,
      responseRate: sent > 0 ? Math.round((replied / sent) * 100) : 0,
      activeProspects: c.length,
      queueCount: q.length,
      committed: d.committed_amount || 0,
      targetAmount: d.target_amount || 0,
      dealName: d.name || '—',
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/action ──────────────────────────────────────────────────────────

app.post('/api/action', async (req, res) => {
  const { action } = req.body;
  try {
    switch (action) {
      case 'pause_all': {
        const state = await loadSessionState();
        state.rocoStatus = 'PAUSED';
        state.pausedUntil = new Date(Date.now() + 86_400_000).toISOString();
        await saveSessionState(state);
        return res.json({ success: true, message: 'Roco paused for 24 hours (takes effect next VPS cycle)' });
      }
      case 'resume_all': {
        const state = await loadSessionState();
        state.rocoStatus = 'ACTIVE';
        state.pausedUntil = null;
        await saveSessionState(state);
        return res.json({ success: true, message: 'Roco resumed (takes effect next VPS cycle)' });
      }
      case 'flush_queue': {
        const pending = await getApprovalQueue();
        await Promise.all(pending.map(p => resolveApprovalInSupabase(p.id, 'skipped', null, null).catch(() => {})));
        return res.json({ success: true, message: `Flushed ${pending.length} pending approvals` });
      }
      case 'run_research':
      case 'run_enrichment':
        return res.json({ success: true, message: 'Will run on next VPS orchestrator cycle (≤15 min)' });
      default:
        return res.status(400).json({ error: 'Unknown action' });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/deal (legacy update first active deal) ─────────────────────────

app.post('/api/deal', async (req, res) => {
  const { dealName, targetAmount, currentCommitted, sector, geography, description } = req.body;
  try {
    const deals = await getActiveDeals();
    if (!deals.length) return res.json({ success: true, deal: null });
    const updates = {};
    if (dealName !== undefined) updates.name = dealName;
    if (targetAmount !== undefined) updates.target_amount = Number(targetAmount);
    if (currentCommitted !== undefined) updates.committed_amount = Number(currentCommitted);
    if (sector !== undefined) updates.sector = sector;
    if (geography !== undefined) updates.geography = geography;
    if (description !== undefined) updates.description = description;
    const deal = await updateDeal(deals[0].id, updates);
    res.json({ success: true, deal });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/contact/:id/stage ───────────────────────────────────────────────

app.post('/api/contact/:id/stage', async (req, res) => {
  const { stage } = req.body;
  if (!stage) return res.status(400).json({ error: 'stage required' });
  try {
    await updateContact(req.params.id, { pipelineStage: stage });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── DELETE /api/contacts/:id ──────────────────────────────────────────────────

app.delete('/api/contacts/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const sb = getSupabase();
    if (sb) {
      await sb.from('contacts').delete().eq('id', id);
      await sb.from('deal_contacts').delete().eq('contact_id', id);
      await sb.from('emails').delete().eq('contact_id', id).in('status', ['draft', 'pending_approval']);
    }
    // Remove from Notion — set Inactive (guaranteed valid) AND archive
    try {
      await updateContact(id, { pipelineStage: 'Inactive' });
    } catch { /* log but continue */ }
    try {
      await archiveContact(id);
    } catch { /* Inactive stage above is sufficient fallback */ }
    await logActivity({
      contactId: id,
      eventType: 'CONTACT_DELETED',
      summary: 'Contact deleted from pipeline — will not be contacted',
    }).catch(() => {});
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── DEALS CRUD ────────────────────────────────────────────────────────────────

app.get('/api/deals', async (req, res) => {
  try { res.json(await getAllDeals()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/deals/history', async (req, res) => {
  try {
    const deals = await getAllDeals();
    res.json(deals.filter(d => d.status !== 'ACTIVE'));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// NOTE: /api/deals/create must come before /api/deals/:id to avoid route collision
app.post('/api/deals/create', async (req, res) => {
  try {
    const {
      name, raiseType, targetAmount, minimumCheque, maximumCheque,
      sector, geography, description, keyMetrics, investorProfile, deckUrl,
    } = req.body;
    if (!name) return res.status(400).json({ error: 'Deal name is required' });
    const deal = await createDeal({
      name, status: 'ACTIVE',
      target_amount: Number(targetAmount) || null,
      committed_amount: 0,
      raise_type: raiseType,
      minimum_cheque: Number(minimumCheque) || null,
      maximum_cheque: Number(maximumCheque) || null,
      sector,
      geography: Array.isArray(geography) ? geography.join(', ') : geography,
      description, key_metrics: keyMetrics, investor_profile: investorProfile, deck_url: deckUrl,
    });
    await logActivity({ eventType: 'DEAL_CREATED', summary: `Deal "${name}" created`, dealId: deal.id });
    res.json({ success: true, deal });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/deals/:id', async (req, res) => {
  try {
    const deal = await getDeal(req.params.id);
    if (!deal) return res.status(404).json({ error: 'Deal not found' });
    const [batches, windowStatus, windowViz] = await Promise.allSettled([
      getBatchSummary(deal.id),
      Promise.resolve(getWindowStatus(deal, null)),
      Promise.resolve(getWindowVisualization(deal)),
    ]);
    res.json({
      ...deal,
      batches: batches.value || [],
      windowStatus: windowStatus.value || {},
      windowVisualization: windowViz.value || [],
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/deals/:id', async (req, res) => {
  try {
    const allowed = [
      'name', 'status', 'target_amount', 'committed_amount', 'sector', 'geography',
      'description', 'key_metrics', 'deck_url', 'investor_profile', 'raise_type',
      'minimum_cheque', 'maximum_cheque',
      'sending_days', 'sending_start', 'sending_end', 'sending_timezone',
      'max_emails_per_day', 'max_emails_per_hour', 'batch_size',
      'followup_cadence_days', 'max_contacts_per_firm', 'max_total_outreach',
      'min_investor_score', 'prioritise_hot_leads', 'include_unscored',
      'outreach_paused_until',
    ];
    const updates = {};
    for (const key of allowed) {
      if (req.body[key] !== undefined) updates[key] = req.body[key];
    }
    const deal = await updateDeal(req.params.id, updates);
    res.json({ success: true, deal });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/deals/:id/pause', async (req, res) => {
  try {
    const deal = await updateDeal(req.params.id, { status: 'PAUSED' });
    res.json({ success: true, deal });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/deals/:id/resume', async (req, res) => {
  try {
    const deal = await updateDeal(req.params.id, { status: 'ACTIVE' });
    res.json({ success: true, deal });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/deals/:id/close', async (req, res) => {
  try {
    const deal = await updateDeal(req.params.id, { status: 'CLOSED', closed_at: new Date().toISOString() });
    await logActivity({ dealId: req.params.id, eventType: 'DEAL_CLOSED', summary: `Deal "${deal.name}" closed` });
    res.json({ success: true, deal });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/deals/:id/batches', async (req, res) => {
  try { res.json(await getBatchSummary(req.params.id)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/deals/:id/schedule', async (req, res) => {
  try {
    const deal = await getDeal(req.params.id);
    if (!deal) return res.status(404).json({ error: 'Deal not found' });
    res.json({ status: getWindowStatus(deal, null), visualization: getWindowVisualization(deal) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/deals/:id/pause-outreach', async (req, res) => {
  try {
    const deal = await updateDeal(req.params.id, { outreach_paused_until: req.body.until || null });
    res.json({ success: true, deal });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── TEMPLATES ─────────────────────────────────────────────────────────────────

app.get('/api/templates', async (req, res) => {
  try { res.json(await getTemplates()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// NOTE: /api/templates/preview must come before /api/templates/:id
app.post('/api/templates/preview', (req, res) => {
  const { body, subject_a, subject_b } = req.body;
  const sample = {
    firstName: 'James', lastName: 'Richardson', firmName: 'Blackstone Real Estate',
    comparableDeal: 'their 2023 UK logistics park acquisition',
    dealName: 'Meridian Industrial Portfolio', dealSector: 'Industrial Real Estate',
    targetAmount: '£12m', dealBrief: 'a £12m industrial portfolio in the East Midlands, yielding 7.8%',
    investmentCriteria: 'UK industrial, 6%+ yield', keyMetrics: '£12m ask, 7.8% yield, 95% occupancy',
    deckUrl: 'https://docsend.com/view/example', sector: 'Industrial Real Estate',
  };
  const render = t => t ? t.replace(/\{\{(\w+)\}\}/g, (_, k) => sample[k] || `{{${k}}}`) : '';
  res.json({ subjectA: render(subject_a), subjectB: render(subject_b), body: render(body), sampleData: sample });
});

app.post('/api/templates/:id/reset', async (req, res) => {
  try {
    await seedDefaultTemplates();
    res.json({ success: true, template: await getTemplate(req.params.id) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/templates/:id', async (req, res) => {
  try {
    const t = await getTemplate(req.params.id);
    if (!t) return res.status(404).json({ error: 'Template not found' });
    res.json(t);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

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
    res.json({ success: true, template: await updateTemplate(req.params.id, updates) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── ACTIVITY ──────────────────────────────────────────────────────────────────

// In-memory activity feed is not available on Vercel — return empty for live feed
app.get('/api/activity', (req, res) => res.json([]));

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

// ── GLOBAL OUTREACH PAUSE ─────────────────────────────────────────────────────

app.post('/api/pause-outreach', async (req, res) => {
  const { until } = req.body;
  const state = await loadSessionState();
  state.outreachPausedUntil = until || null;
  await saveSessionState(state);
  res.json({ success: true, outreachPausedUntil: until });
});

// ── RAISE PROGRESS (legacy) ───────────────────────────────────────────────────

app.post('/api/raise-progress', async (req, res) => {
  const { committed } = req.body;
  const deals = await getActiveDeals().catch(() => []);
  if (deals.length > 0 && committed !== undefined) {
    await updateDeal(deals[0].id, { committed_amount: Number(committed) }).catch(() => {});
  }
  res.json({ ok: true });
});

// ── Auth helper ───────────────────────────────────────────────────────────────

function challenge(res) {
  res.setHeader('WWW-Authenticate', 'Basic realm="Roco Mission Control"');
  res.status(401).send('Authorisation required');
}

export default app;
