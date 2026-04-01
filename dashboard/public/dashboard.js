/* ═══════════════════════════════════════════════════════════════════════════
   ROCO MISSION CONTROL — dashboard.js
   SPA dashboard for Roco AI fundraising automation
   ═══════════════════════════════════════════════════════════════════════════ */

'use strict';

/* ─── CONSTANTS ──────────────────────────────────────────────────────────── */
const REFRESH_MS        = 30_000;
const HEALTH_REFRESH_MS = 10_000;
const WS_RECONNECT_MS   = 5_000;
const POLL_FALLBACK_MS  = 8_000;


const TEMPLATE_VARIABLES = [
  // Contact
  '{{firstName}}', '{{lastName}}', '{{fullName}}',
  '{{firm}}', '{{company}}',
  '{{title}}', '{{jobTitle}}',
  // Investor research
  '{{pastInvestments}}', '{{investmentThesis}}', '{{sectorFocus}}', '{{investorGeography}}',
  // Deal
  '{{dealName}}', '{{dealBrief}}', '{{sector}}',
  '{{targetAmount}}', '{{keyMetrics}}', '{{geography}}',
  '{{minCheque}}', '{{maxCheque}}',
  '{{investorProfile}}', '{{comparableDeal}}',
  // Assets
  '{{deckUrl}}', '{{callLink}}',
  // Sender
  '{{senderName}}', '{{senderTitle}}',
];

const TEMPLATE_DISPLAY_NAMES = {
  intro:               'Email 1',
  followup_1:          'Follow Up Email 1',
  followup_2:          'Follow Up Email 2',
  followup_3:          'Follow Up Email 3',
  linkedin:            'LinkedIn DM',
  linkedin_intro:      'LinkedIn DM',
  linkedin_followup_1: 'LinkedIn Follow Up',
};

const SAMPLE_DATA = {
  firstName: 'James', lastName: 'Whitfield', fullName: 'James Whitfield',
  firm: 'Cavendish Partners', company: 'Cavendish Partners',
  title: 'Managing Partner', jobTitle: 'Managing Partner',
  dealName: 'Apex Capital Series B',
  dealBrief: 'UK industrial property fund targeting 7.8% yield with strong institutional covenants',
  targetAmount: '£50M', sector: 'Industrial Real Estate',
  keyMetrics: '7.8% yield, 95% occupancy, 8-year WAULT',
  geography: 'UK', minCheque: '£500k', maxCheque: '£5M',
  investorProfile: 'UK-focused real estate and private equity investors',
  comparableDeal: 'Midlands Logistics Park acquisition (2023)',
  deckUrl: 'https://docsend.com/view/example',
  callLink: 'https://calendly.com/dom/30min',
  senderName: 'Dom', senderTitle: 'Partner',
};

/* ─── STATE ──────────────────────────────────────────────────────────────── */
let ws               = null;
let wsReconnectTimer = null;
let wsConnected      = false;
let pollTimer        = null;
let refreshTimer     = null;
let healthTimer      = null;
let clockTimer       = null;
let pauseCountTimer  = null;
let previewDebounce  = null;
let modalCallback    = null;

let allDeals        = [];
let activeDeal      = null;  // selected deal ID from top bar
let pipelineData    = [];
let pipelineSort    = { key: 'score', dir: 'desc' };
let activityLog     = [];
let currentTemplate = null;
let currentQueueTab = 'email';
let selectedDealId  = null;  // deal detail panel
let selectedDealReadOnly   = false;   // true when viewing from Archive (no Roco actions)
let selectedDealBackSection = 'deals'; // where Back button navigates to
let docUploadController = null;

// Deal brief launch state
let dealBriefEditMode = false;
let currentParsedDeal = null;
let currentDocumentId = null;

/* ─── INIT ───────────────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  initRouter();
  startClock();
  connectWebSocket();
  loadState();
  refreshStats();
  refreshHealth();

  refreshTimer = setInterval(fullRefresh, REFRESH_MS);
  healthTimer  = setInterval(refreshHealth, HEALTH_REFRESH_MS);

  // Pause background timers when tab is hidden to avoid "Failed to fetch" noise
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      clearInterval(refreshTimer); clearInterval(healthTimer);
      refreshTimer = null; healthTimer = null;
    } else {
      // Tab became visible again — refresh immediately then restart timers
      fullRefresh();
      refreshTimer = setInterval(fullRefresh, REFRESH_MS);
      healthTimer  = setInterval(refreshHealth, HEALTH_REFRESH_MS);
    }
  });
});

/* ═══════════════════════════════════════════════════════════════════════════
   ROUTING
   ═══════════════════════════════════════════════════════════════════════════ */

function initRouter() {
  window.addEventListener('hashchange', () => {
    navigate(window.location.hash || '#overview');
  });
  navigate(window.location.hash || '#overview');
}

function navigate(hash) {
  if (!hash || hash === '#') hash = '#overview';
  const view = hash.replace('#', '');

  // Hide all sections
  document.querySelectorAll('.view-section').forEach(s => s.classList.add('hidden'));
  const target = document.getElementById(`view-${view}`);
  if (target) target.classList.remove('hidden');

  // Update nav active state
  document.querySelectorAll('.nav-item').forEach(el => {
    el.classList.toggle('active', el.dataset.view === view);
  });

  // Close sidebar on mobile
  if (window.innerWidth <= 860) closeSidebar();

  // Section loaders
  switch (view) {
    case 'overview':   loadOverview();   break;
    case 'launch':     loadLaunchForm(); break;
    case 'deals':      loadDeals();      break;
    case 'pipeline':   loadPipeline();   break;
    case 'queue':      loadQueue();      break;
    case 'activity':   loadActivity();   break;
    case 'analytics':       loadAnalyticsPage();   break;
    case 'archive':         loadArchive();         break;
    case 'sourcing':        loadSourcingCampaigns(); break;
    case 'sourcing-launch': /* form loads statically */ break;
    case 'sourcing-detail': if (currentSourcingCampaignId) loadSourcingCampaignDetail(currentSourcingCampaignId); break;
    case 'train':           loadTrainYourAgent();  break;
    case 'database':        loadDatabase();        break;
    case 'controls':   loadControls();   break;
    case 'env':        loadEnvView();    break;
  }
}

/* ═══════════════════════════════════════════════════════════════════════════
   API HELPER
   ═══════════════════════════════════════════════════════════════════════════ */

async function api(path, method = 'GET', body = null) {
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json' },
  };
  if (body !== null) opts.body = JSON.stringify(body);

  try {
    const res = await fetch(path, opts);
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`HTTP ${res.status}: ${text}`);
    }
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('application/json')) return res.json();
    return res.text();
  } catch (err) {
    console.error(`[ROCO] API error ${method} ${path}:`, err);
    showApiAlert(err.message);
    throw err;
  }
}

function showApiAlert(msg) {
  const el = document.getElementById('api-alert');
  const msgEl = document.getElementById('api-alert-msg');
  if (msgEl) msgEl.textContent = `⚠ ${msg}`;
  el.classList.remove('hidden');
}

/* ═══════════════════════════════════════════════════════════════════════════
   WEBSOCKET
   ═══════════════════════════════════════════════════════════════════════════ */

function connectWebSocket() {
  if (wsReconnectTimer) { clearTimeout(wsReconnectTimer); wsReconnectTimer = null; }

  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url   = `${proto}//${location.host}/ws`;

  try {
    ws = new WebSocket(url);
  } catch (e) {
    startPollFallback();
    return;
  }

  ws.addEventListener('open', () => {
    wsConnected = true;
    stopPollFallback();
    setConnStatus('connected', 'WebSocket');
    document.getElementById('activity-live-dot')?.classList.remove('dim');
  });

  ws.addEventListener('message', e => {
    try {
      const msg = JSON.parse(e.data);
      handleWsMessage(msg);
    } catch { /* ignore malformed */ }
  });

  ws.addEventListener('close', () => {
    wsConnected = false;
    setConnStatus('disconnected', 'Reconnecting…');
    wsReconnectTimer = setTimeout(connectWebSocket, WS_RECONNECT_MS);
    startPollFallback();
  });

  ws.addEventListener('error', () => {
    ws?.close();
  });
}

function handleWsMessage(msg) {
  switch (msg.type) {
    case 'activity':
    case 'ACTIVITY':
      prependActivity(msg.entry || msg.data || msg);
      break;

    case 'STATE_UPDATE':
      if (msg.state || msg.data) applyState(msg.state || msg.data);
      break;

    case 'DEAL_CREATED':
    case 'DEAL_UPDATED':
      populateDealSelector();
      if (!document.getElementById('view-deals')?.classList.contains('hidden') && !selectedDealId) {
        loadDeals();
      }
      break;

    case 'DEAL_CLOSED':
      populateDealSelector();
      loadDeals();
      // If viewing the closed deal's detail panel, close it
      if (msg.dealId && String(selectedDealId) === String(msg.dealId)) {
        closeDealDetail();
      }
      break;

    case 'DEAL_DELETED':
      // Remove from local cache and refresh both views
      if (msg.dealId) allDeals = (allDeals || []).filter(d => (d.id || d._id) !== msg.dealId);
      populateDealSelector();
      if (!document.getElementById('view-deals')?.classList.contains('hidden')) loadDeals();
      if (!document.getElementById('view-archive')?.classList.contains('hidden')) loadArchive();
      break;

    case 'WIPE_COMPLETE':
      allDeals = [];
      populateDealSelector();
      closeDealDetail();
      if (!document.getElementById('view-deals')?.classList.contains('hidden')) loadDeals();
      if (!document.getElementById('view-archive')?.classList.contains('hidden')) loadArchive();
      if (!document.getElementById('view-database')?.classList.contains('hidden')) loadDatabase?.();
      break;

    case 'QUEUE_UPDATED':
      refreshQueueBadge(msg.count ?? null);
      if (!document.getElementById('view-queue')?.classList.contains('hidden')) {
        loadQueue();
      }
      break;

    case 'STATS':
      if (msg.data) applyStats(msg.data);
      break;
  }
}

function startPollFallback() {
  if (pollTimer) return;
  pollTimer = setInterval(async () => {
    if (wsConnected) { stopPollFallback(); return; }
    setConnStatus('polling', 'Polling');
    try {
      await loadActivityLog(false);
    } catch { /* silent */ }
  }, POLL_FALLBACK_MS);
}

function stopPollFallback() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

function setConnStatus(state, label) {
  const dot   = document.getElementById('conn-dot');
  const lbl   = document.getElementById('conn-label');
  if (dot) dot.className = `conn-dot ${state}`;
  if (lbl) lbl.textContent = label;
}

/* ═══════════════════════════════════════════════════════════════════════════
   CLOCK
   ═══════════════════════════════════════════════════════════════════════════ */

function startClock() {
  const tick = () => {
    const now = new Date();
    const TZ  = 'America/New_York';
    const el  = document.getElementById('clock');
    if (el) el.textContent = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false, timeZone: TZ });

    const dateEl = document.getElementById('topbar-date');
    if (dateEl) {
      dateEl.textContent = now.toLocaleDateString('en-US', {
        weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', timeZone: TZ,
      });
    }
  };
  tick();
  clockTimer = setInterval(tick, 1000);
}

/* ═══════════════════════════════════════════════════════════════════════════
   STATE & MASTER TOGGLE
   ═══════════════════════════════════════════════════════════════════════════ */

async function loadState() {
  try {
    const state = await api('/api/state');
    applyState(state);
    await populateDealSelector();
  } catch { /* already shown alert */ }
}

function applyState(state) {
  // Master toggle
  const mt = document.getElementById('master-toggle');
  const ps = document.getElementById('power-status');
  if (state.rocoStatus !== undefined) {
    const on = state.rocoStatus === 'ACTIVE' || state.rocoStatus === true;
    if (mt) mt.checked = on;
    if (ps) {
      ps.textContent = on ? 'ONLINE' : 'OFFLINE';
      ps.className   = on ? 'power-status online' : 'power-status';
    }
  }

  // System toggles in controls
  // Note: state stores followupEnabled (lowercase u) but HTML uses followUpEnabled (capital U)
  const toggleMap = {
    'outreachEnabled':   ['outreachEnabled'],
    'followUpEnabled':   ['followUpEnabled', 'followupEnabled'],
    'enrichmentEnabled': ['enrichmentEnabled'],
    'researchEnabled':   ['researchEnabled'],
    'linkedinEnabled':   ['linkedinEnabled'],
  };
  Object.entries(toggleMap).forEach(([htmlKey, stateKeys]) => {
    const input = document.querySelector(`[data-key="${htmlKey}"]`);
    if (!input) return;
    for (const sk of stateKeys) {
      if (state[sk] !== undefined) { input.checked = !!state[sk]; return; }
    }
  });

  // Pause
  if (state.pauseUntil) {
    renderPauseActive(state.pauseUntil);
  } else {
    clearPauseDisplay();
  }
}

async function onMasterToggle(checked) {
  const ps = document.getElementById('power-status');
  if (ps) {
    ps.textContent = checked ? 'ONLINE' : 'OFFLINE';
    ps.className   = checked ? 'power-status online' : 'power-status';
  }
  try {
    await api('/api/toggle', 'POST', { key: 'rocoStatus', value: checked });
  } catch {
    // revert on failure
    const mt = document.getElementById('master-toggle');
    if (mt) mt.checked = !checked;
    if (ps) {
      ps.textContent = !checked ? 'ONLINE' : 'OFFLINE';
      ps.className   = !checked ? 'power-status online' : 'power-status';
    }
  }
}

/* ═══════════════════════════════════════════════════════════════════════════
   DEAL SELECTOR (top bar)
   ═══════════════════════════════════════════════════════════════════════════ */

async function populateDealSelector() {
  try {
    const deals = await api('/api/deals');
    allDeals = Array.isArray(deals) ? deals : (deals.deals || []);
    const sel = document.getElementById('deal-selector');
    if (!sel) return;

    const prev = sel.value;
    sel.innerHTML = '<option value="">— Select Deal —</option>';
    allDeals.forEach(d => {
      const opt = document.createElement('option');
      opt.value = d.id || d._id;
      opt.textContent = d.dealName || d.name;
      sel.appendChild(opt);
    });
    if (prev) sel.value = prev;

    // Also populate filter dropdowns
    populateDealFilters(allDeals);
  } catch { /* silent */ }
}

function populateDealFilters(deals) {
  const filterIds = ['pipeline-deal-filter','activity-deal-filter','schedule-deal-select'];
  filterIds.forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    const prev = el.value;
    const first = el.options[0];
    el.innerHTML = '';
    el.appendChild(first);
    deals.forEach(d => {
      const opt = document.createElement('option');
      opt.value = d.id || d._id;
      opt.textContent = d.dealName || d.name;
      el.appendChild(opt);
    });
    if (prev) el.value = prev;
  });
}

function onDealSelectorChange(id) {
  activeDeal = id || null;
  // Refresh current section if pipeline or activity
  const hash = window.location.hash.replace('#', '');
  if (hash === 'pipeline') loadPipeline();
  if (hash === 'activity') loadActivity();
}

/* ═══════════════════════════════════════════════════════════════════════════
   OVERVIEW
   ═══════════════════════════════════════════════════════════════════════════ */

async function loadOverview() {
  await Promise.all([refreshStats(), refreshHealth(), loadActivityLog(true)]);
}

async function refreshStats() {
  try {
    const stats = await api('/api/stats');
    applyStats(stats);
  } catch { /* silent */ }
}

function applyStats(s) {
  if (!s) return;
  setText('stat-active-deals',   fmt(s.active_deals));
  setText('stat-total-deals',    fmt(s.total_deals_launched));
  setText('stat-emails-sent',    fmt(s.emailsSent || s.emails_sent));
  setText('stat-response-rate',  s.response_rate != null ? pct(s.response_rate) : (s.responseRate != null ? pct(s.responseRate) : '—'));
  if (s.emails_replied != null) setText('stat-response-sub', `${fmt(s.emails_replied)} repl${s.emails_replied === 1 ? 'y' : 'ies'}`);
  setText('stat-prospects',      fmt(s.activeProspects || s.active_prospects));
  setText('stat-queue',          fmt(s.queueCount || s.approval_queue));
  const totalRaised = s.total_funds_raised ?? s.committed ?? s.capital_committed ?? s.capitalCommitted;
  setText('stat-capital', totalRaised != null && totalRaised !== '—' ? formatMoney(totalRaised) : '—');
  const activeCom = s.active_committed;
  setText('stat-capital-sub',
    activeCom != null
      ? `Active: ${formatMoney(activeCom)}${s.targetAmount ? ` · Target: ${formatMoney(s.targetAmount)}` : ''}`
      : (s.targetAmount ? `Target: ${fmt(s.targetAmount)}` : ''));
  setText('stat-emails-sub',     s.emailsSentPeriod || '');
  setText('stat-response-sub',   s.responseRatePeriod || '');
  setText('stat-prospects-sub',  s.prospectsPeriod || '');
  setText('stat-queue-sub',      s.queuePeriod || '');

  // LinkedIn metrics
  setText('stat-li-invites',     fmt(s.li_invites_sent));
  setText('stat-li-acceptance',  s.li_acceptance_rate != null ? pct(s.li_acceptance_rate) : '—');
  setText('stat-li-dms',         fmt(s.li_dms_sent));
  setText('stat-li-dm-response', s.li_dm_response_rate != null ? pct(s.li_dm_response_rate) : '—');

  refreshQueueBadge(s.queueCount || s.approval_queue);
}

function refreshQueueBadge(count) {
  const badge = document.getElementById('queue-badge');
  if (!badge) return;
  if (count > 0) {
    badge.textContent = count;
    badge.style.display = '';
  } else {
    badge.style.display = 'none';
  }
}

let healthCheckFailures = 0;
const HEALTH_FAIL_THRESHOLD = 3; // only show error after 3 consecutive failures

async function refreshHealth() {
  try {
    const h = await api('/api/health');
    healthCheckFailures = 0;
    applyHealth(h);
  } catch (err) {
    healthCheckFailures++;
    console.warn(`[ROCO] Health check failed (${healthCheckFailures}/${HEALTH_FAIL_THRESHOLD}):`, err.message);
    // Only surface error after 3 consecutive failures (~30 seconds at 10s interval)
    // Single/double failures are ignored — could be a slow response or brief blip
  }
}

function applyHealth(h) {
  if (!h) return;
  const services = ['anthropic','openai','gemini','kaspr','notion','gmail','telegram','serpapi','apify','grok','millionverifier'];
  services.forEach(svc => {
    try {
      const el = document.getElementById(`health-${svc}`);
      if (!el) return;
      const dot = el.querySelector('.health-dot');
      if (!dot) return;
      // h[svc] may be an object {status, lastCheck,...} or a string — handle both
      const raw = h[svc] ?? h[svc.toUpperCase()] ?? h[svc.toLowerCase()] ?? 'unknown';
      const status = (raw !== null && raw !== undefined)
        ? String(typeof raw === 'object' ? (raw.status ?? 'unknown') : raw).toLowerCase()
        : 'unknown';
      // 'ok' → green, 'warn' → amber, 'error'/'degraded' → red, 'unconfigured'/'unknown' → grey
      const cssClass = status === 'ok' ? 'ok'
        : status === 'warn' ? 'warn'
        : (status === 'error' || status === 'degraded') ? 'error'
        : 'unknown';
      dot.className = `health-dot ${cssClass}`;
    } catch (svcErr) {
      console.warn(`[ROCO] Health render error for ${svc}:`, svcErr.message);
    }
  });
}

async function loadActivityLog(renderToOverview = true) {
  try {
    const data = await api('/api/activity/log');
    const items = Array.isArray(data) ? data : (data.log || data.items || []);
    activityLog = items;
    if (renderToOverview) renderActivityFeed('overview-activity', items.slice(0, 10));
    renderActivityFeed('activity-feed', items);
  } catch { /* silent */ }
}

function renderActivityFeed(elId, items) {
  const el = document.getElementById(elId);
  if (!el) return;
  if (!items || !items.length) {
    el.innerHTML = '<div class="feed-empty">No recent activity</div>';
    return;
  }
  el.innerHTML = items.map(item => {
    const badge  = typeToBadge(item.type || item.event_type || item.activityType);
    const action = item.action || '';
    const note   = item.note || item.message || item.text || item.summary || '';
    const text   = action && note ? `${action} · ${note}` : (action || note || '');
    const ts     = item.timestamp || item.createdAt || item.created_at;
    const deal   = item.deal_name || item.deal;
    return `<div class="feed-item">
      <span class="feed-time">${formatTime(ts)}</span>
      <span class="feed-badge ${badge}">${badge}</span>
      ${deal ? `<span class="feed-deal">${esc(deal)}</span>` : ''}
      <span class="feed-text">${esc(text)}</span>
    </div>`;
  }).join('');
}

function prependActivity(item) {
  activityLog.unshift(item);
  if (activityLog.length > 200) activityLog.pop();

  // Update overview feed
  const overviewFeed = document.getElementById('overview-activity');
  if (overviewFeed) {
    const badge  = typeToBadge(item.type || item.event_type || item.activityType);
    const action = item.action || '';
    const note   = item.note || item.message || item.text || item.summary || '';
    const text   = action && note ? `${action} · ${note}` : (action || note || '');
    const ts     = item.timestamp || item.createdAt || item.created_at;
    const deal   = item.deal_name || item.deal;
    const div = document.createElement('div');
    div.className = 'feed-item';
    div.innerHTML = `
      <span class="feed-time">${formatTime(ts)}</span>
      <span class="feed-badge ${badge}">${badge}</span>
      ${deal ? `<span class="feed-deal">${esc(deal)}</span>` : ''}
      <span class="feed-text">${esc(text)}</span>
    `;
    const empty = overviewFeed.querySelector('.feed-empty');
    if (empty) empty.remove();
    overviewFeed.prepend(div);
    // Trim to 10
    const all = overviewFeed.querySelectorAll('.feed-item');
    if (all.length > 10) all[all.length - 1].remove();
  }

  // Update activity feed if visible
  const feed = document.getElementById('activity-feed');
  if (feed && !document.getElementById('view-activity')?.classList.contains('hidden')) {
    filterActivity();
  }
}

function filterActivity() {
  const dealFilter = document.getElementById('activity-deal-filter')?.value || '';
  const typeFilter = document.getElementById('activity-type-filter')?.value || '';
  let filtered = activityLog;
  if (dealFilter) filtered = filtered.filter(i => i.dealId === dealFilter || i.deal_id === dealFilter || i.deal === dealFilter);
  if (typeFilter) filtered = filtered.filter(i => (i.type || i.event_type || '').toLowerCase().includes(typeFilter));
  renderActivityFeed('activity-feed', filtered);
}

/* ═══════════════════════════════════════════════════════════════════════════
   LAUNCH DEAL
   ═══════════════════════════════════════════════════════════════════════════ */

function loadLaunchForm() {
  const list = document.getElementById('launch-assets-list');
  if (list) list.innerHTML = '';
  // Always start at step 1
  resetLaunchStep();
}

function resetLaunchStep() {
  document.getElementById('launch-step-1').style.display = '';
  document.getElementById('launch-step-2').style.display = 'none';
  document.getElementById('deal-doc-text').textContent = 'Drop PDF or DOCX here, or click to browse';
  document.getElementById('deal-doc-icon').textContent = '📄';
  document.getElementById('deal-parse-spinner').style.display = 'none';
  document.getElementById('launch-document-id').value = '';
  const input = document.getElementById('deal-doc-input');
  if (input) input.value = '';
}

function onDealDocSelected(input) {
  if (!input.files?.[0]) return;
  document.getElementById('deal-doc-text').textContent = `📄 ${input.files[0].name}`;
  uploadAndParseDealDoc(input.files[0]);
}

function onDealDocDrop(e) {
  e.preventDefault();
  document.getElementById('deal-doc-drop-zone')?.classList.remove('drag-over');
  const file = e.dataTransfer?.files?.[0];
  if (file && (file.name.endsWith('.pdf') || file.name.endsWith('.docx'))) {
    const input = document.getElementById('deal-doc-input');
    const dt = new DataTransfer();
    dt.items.add(file);
    input.files = dt.files;
    document.getElementById('deal-doc-text').textContent = `📄 ${file.name}`;
    uploadAndParseDealDoc(file);
  }
}

async function uploadAndParseDealDoc(file) {
  document.getElementById('deal-parse-spinner').style.display = '';
  document.getElementById('deal-doc-drop-zone').style.opacity = '0.4';

  docUploadController = new AbortController();

  try {
    const fd = new FormData();
    fd.append('file', file);
    const res = await fetch('/api/deals/parse-document', {
      method: 'POST',
      body: fd,
      signal: docUploadController.signal,
    });
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(txt);
    }
    const data = await res.json();
    document.getElementById('deal-parse-spinner').style.display = 'none';
    document.getElementById('deal-doc-drop-zone').style.opacity = '';
    showDealBriefCard(data.parsed, data.document_id, file.name);
  } catch (err) {
    document.getElementById('deal-parse-spinner').style.display = 'none';
    document.getElementById('deal-doc-drop-zone').style.opacity = '';
    if (err.name === 'AbortError') {
      // User cancelled — reset UI silently
      document.getElementById('deal-doc-text').textContent = 'Drop PDF or DOCX here, or click to browse';
      document.getElementById('deal-doc-icon').textContent = '📄';
      const input = document.getElementById('deal-doc-input');
      if (input) input.value = '';
    } else {
      alert(`Failed to parse document: ${err.message}`);
    }
  } finally {
    docUploadController = null;
  }
}

function cancelDealDocUpload() {
  if (docUploadController) {
    docUploadController.abort();
    docUploadController = null;
    document.getElementById('deal-parse-spinner').style.display = 'none';
    document.getElementById('deal-doc-drop-zone').style.opacity = '';
    document.getElementById('deal-doc-text').textContent = 'Drop PDF or DOCX here, or click to browse';
    showToast('Upload cancelled');
  }
}

function renderDealBriefCard(parsed, editMode) {
  const fmt = (v) => v != null ? `$${v}M` : '—';
  const numField = (f, v) => editMode
    ? `<input class="dbf-num" data-f="${f}" type="number" step="0.1" value="${v ?? ''}" style="width:90px;background:#1a1a1a;border:1px solid #d4a847;border-radius:4px;color:#e5e7eb;padding:3px 6px;font-size:15px;font-weight:700">`
    : `<span class="dbf-num" data-f="${f}" style="color:#e5e7eb;font-size:18px;font-weight:700">${v != null ? '$' + v + 'M' : '—'}</span>`;
  const txtField = (f, v, style='') => editMode
    ? `<span class="dbf-text" data-f="${f}" contenteditable="true" style="outline:1px solid #d4a847;border-radius:3px;padding:1px 4px;min-width:40px;display:inline-block;${style}">${esc(v ?? '')}</span>`
    : `<span class="dbf-text" data-f="${f}" style="${style}">${esc(v ?? '')}</span>`;

  const typeBadge = `<span style="background:#1f2937;color:#d4a847;padding:3px 10px;border-radius:4px;font-size:12px">${esc(parsed.deal_type || '—')}</span>`;
  const openBadge = parsed.open_ended ? `<span style="background:#1f2937;color:#60a5fa;padding:3px 10px;border-radius:4px;font-size:12px;margin-left:6px">Open-ended raise</span>` : '';

  return `
    <div style="background:#111;border:1px solid #2a2a2a;border-radius:8px;overflow:hidden;margin-bottom:20px">
      <!-- Header -->
      <div style="background:#1a1a1a;padding:20px 24px;border-bottom:1px solid #2a2a2a">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px">
          <div>
            <h2 style="color:#e5e7eb;margin:0 0 8px">${txtField('deal_name', parsed.deal_name, 'font-size:20px;font-weight:700;color:#e5e7eb')}</h2>
            ${typeBadge}${openBadge}
          </div>
          <div style="text-align:right;color:#6b7280;font-size:12px">
            ${txtField('sector', parsed.sector)}${parsed.sub_sector ? ' / ' + esc(parsed.sub_sector) : ''}<br>
            ${txtField('hq_location', parsed.hq_location || parsed.geography)}
          </div>
        </div>
        ${parsed.company_overview !== undefined ? `<p style="color:#9ca3af;margin:12px 0 0;font-size:14px;line-height:1.6">${txtField('company_overview', parsed.company_overview, 'color:#9ca3af;font-size:14px;line-height:1.6')}</p>` : ''}
      </div>
      <!-- Financials -->
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:#2a2a2a;border-bottom:1px solid #2a2a2a">
        ${[['Revenue', 'revenue_usd_m'], ['EBITDA', 'ebitda_usd_m'], ['Enterprise Value', 'enterprise_value_usd_m'], ['Equity Required', 'equity_required_usd_m']].map(([label, f]) => `
          <div style="background:#111;padding:16px 20px">
            <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:4px">${label}</div>
            ${numField(f, parsed[f])}
          </div>`).join('')}
      </div>
      <!-- Extended financials row -->
      <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:1px;background:#2a2a2a;border-bottom:1px solid #2a2a2a">
        ${[['Seller Note', 'seller_note_usd_m'], ['Rollover Equity', 'rollover_equity_usd_m'], ['Revenue CAGR', 'revenue_cagr_pct'], ['EBITDA CAGR', 'ebitda_cagr_pct'], ['Incoming Mgmt', 'incoming_management']].map(([label, f]) => `
          <div style="background:#111;padding:12px 16px">
            <div style="color:#6b7280;font-size:9px;text-transform:uppercase;margin-bottom:4px">${label}</div>
            ${f === 'incoming_management'
              ? `<span style="color:#e5e7eb;font-size:13px;font-weight:600">${txtField(f, parsed[f])}</span>`
              : numField(f, parsed[f])}
          </div>`).join('')}
      </div>
      <!-- Strategy -->
      <div style="padding:16px 24px;border-bottom:1px solid #2a2a2a">
        <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:6px">Fundraising Strategy</div>
        <p style="color:#e5e7eb;margin:0;font-size:13px;line-height:1.6">${txtField('fundraising_strategy', parsed.fundraising_strategy, 'color:#e5e7eb;font-size:13px;line-height:1.6')}</p>
      </div>
      <!-- Target investors -->
      <div style="padding:16px 24px;border-bottom:1px solid #2a2a2a">
        <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:8px">Target Investor Types</div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">
          ${(parsed.ideal_investor_types || []).map(t => `<span style="background:#1f3a5f;color:#60a5fa;padding:3px 10px;border-radius:4px;font-size:11px">${esc(t)}</span>`).join('')}
        </div>
        ${parsed.ideal_investor_profile ? `<p style="color:#9ca3af;font-size:12px;margin:6px 0 0">${txtField('ideal_investor_profile', parsed.ideal_investor_profile, 'color:#9ca3af;font-size:12px')}</p>` : ''}
      </div>
      <!-- Metrics row -->
      <div style="padding:16px 24px;display:flex;gap:32px;border-bottom:1px solid #2a2a2a;flex-wrap:wrap">
        <div>
          <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:3px">Est. Investors to Contact</div>
          <div style="color:#e5e7eb;font-size:22px;font-weight:700">${parsed.estimated_investors_to_contact || 200}</div>
        </div>
        <div>
          <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:3px">Raise Target</div>
          <div style="color:#e5e7eb;font-size:22px;font-weight:700">${numField('target_raise_usd_m', parsed.target_raise_usd_m)}</div>
        </div>
        <div>
          <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:3px">Cheque Size</div>
          <div style="color:#e5e7eb;font-size:16px;font-weight:700">${numField('min_check_usd_m', parsed.min_check_usd_m)} – ${numField('max_check_usd_m', parsed.max_check_usd_m)}</div>
        </div>
      </div>
      <!-- Highlights -->
      ${parsed.investment_highlights?.length ? `
      <div style="padding:16px 24px;border-bottom:1px solid #2a2a2a">
        <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:6px">Investment Highlights</div>
        <ul style="margin:0;padding-left:16px;color:#9ca3af;font-size:12px">
          ${parsed.investment_highlights.map(h => `<li style="margin-bottom:3px">${esc(h)}</li>`).join('')}
        </ul>
      </div>` : ''}
      <!-- Actions -->
      <div style="padding:16px 24px;display:flex;align-items:center;gap:10px">
        <button onclick="launchDealFromParsed()"
          style="padding:10px 24px;background:#d4a847;border:none;color:#000;border-radius:6px;font-weight:700;cursor:pointer;font-size:14px">
          Launch Deal →
        </button>
        <button onclick="toggleDealBriefEdit()"
          style="padding:10px 16px;background:#1a1a1a;border:1px solid ${editMode ? '#d4a847' : '#3a3a3a'};color:${editMode ? '#d4a847' : '#9ca3af'};border-radius:6px;cursor:pointer;font-size:13px">
          ${editMode ? '✓ Done Editing' : '✎ Edit'}
        </button>
        <button onclick="briefToForm(currentParsedDeal, currentDocumentId)"
          style="padding:10px 14px;background:#1a1a1a;border:1px solid #2a2a2a;color:#6b7280;border-radius:6px;cursor:pointer;font-size:13px">
          Full Form →
        </button>
        <button onclick="document.getElementById('launch-deal-brief').style.display='none';document.getElementById('launch-step-1').style.display=''"
          style="padding:10px 14px;background:#1a1a1a;border:1px solid #2a2a2a;color:#6b7280;border-radius:6px;cursor:pointer;font-size:13px">
          ← Re-upload
        </button>
      </div>
    </div>
  `;
}

function showDealBriefCard(parsed, documentId, filename) {
  currentParsedDeal = parsed;
  currentDocumentId = documentId;
  dealBriefEditMode = false;

  // Hide step 1, show brief card before step 2
  document.getElementById('launch-step-1').style.display = 'none';

  // Create or replace brief card
  let briefEl = document.getElementById('launch-deal-brief');
  if (!briefEl) {
    briefEl = document.createElement('div');
    briefEl.id = 'launch-deal-brief';
    document.getElementById('launch-step-2').insertAdjacentElement('beforebegin', briefEl);
  }

  briefEl.innerHTML = renderDealBriefCard(parsed, false);
  briefEl.style.display = '';
  // Pre-populate manual form so it's ready if user switches to it
  populateManualFormFromParsed(parsed);
}

function populateManualFormFromParsed(parsed) {
  if (!parsed) return;
  const fmtM = (v) => (v != null && v !== '') ? `$${v}M` : null;
  const setId = (id, val) => {
    const el = document.getElementById(id);
    if (el && val != null && val !== '') el.value = val;
  };
  const setName = (name, val) => {
    const el = document.querySelector(`#launch-form [name="${name}"]`);
    if (el && val != null && val !== '') el.value = val;
  };
  const setSelect = (id, val) => {
    if (!val) return;
    const el = document.getElementById(id);
    if (!el) return;
    const opts = [...el.options];
    const exact = opts.find(o => o.value === val || o.text === val);
    if (exact) { el.value = exact.value; return; }
    const partial = opts.find(o => val.toLowerCase().includes(o.value.toLowerCase()) && o.value !== '');
    if (partial) el.value = partial.value;
  };

  setId('lf-deal-name', parsed.deal_name);
  setSelect('lf-raise-type', parsed.deal_type);
  setId('lf-sector', parsed.sector);
  const targetAmt = parsed.target_raise_usd_m ?? parsed.equity_required_usd_m;
  if (targetAmt) setId('lf-target-amount', fmtM(targetAmt));
  setId('lf-ebitda', parsed.ebitda_usd_m);
  setId('lf-ev', parsed.enterprise_value_usd_m);
  setId('lf-equity', parsed.equity_required_usd_m);
  if (parsed.min_check_usd_m != null) setName('minCheque', fmtM(parsed.min_check_usd_m));
  if (parsed.max_check_usd_m != null) setName('maxCheque', fmtM(parsed.max_check_usd_m));
  setId('lf-investor-types', (parsed.ideal_investor_types || []).join(', '));
  if (parsed.ideal_investor_profile) setName('investorProfile', parsed.ideal_investor_profile);
  if (parsed.investment_highlights?.length) setName('keyMetrics', parsed.investment_highlights.join('\n'));
  const descParts = [parsed.company_overview, parsed.additional_context].filter(Boolean);
  if (descParts.length) setName('description', descParts.join('\n\n'));
  const geoText = [parsed.geography, parsed.hq_location].filter(Boolean).join(' ').toLowerCase();
  if (geoText) {
    const GEO_MAP = {
      UK: ['uk','united kingdom','england','scotland','wales','britain','london'],
      Europe: ['europe','european','eu','germany','france','spain','italy','netherlands','sweden','nordic'],
      US: ['us','usa','united states','america','american','northeast','texas','florida','california','new york'],
      Global: ['global','worldwide','international','cross-border'],
      MENA: ['mena','middle east','north africa','uae','dubai','saudi','gulf'],
      APAC: ['apac','asia','pacific','australia','singapore','hong kong','japan','india'],
    };
    document.querySelectorAll('#launch-form [name="geography"]').forEach(cb => {
      const keywords = GEO_MAP[cb.value] || [];
      if (keywords.some(kw => geoText.includes(kw))) cb.checked = true;
    });
  }
}

function toggleDealBriefEdit() {
  if (!currentParsedDeal) return;
  // Collect any edits made so far before toggling
  currentParsedDeal = collectEdits(currentParsedDeal);
  dealBriefEditMode = !dealBriefEditMode;
  const briefEl = document.getElementById('launch-deal-brief');
  if (briefEl) briefEl.innerHTML = renderDealBriefCard(currentParsedDeal, dealBriefEditMode);
}

function collectEdits(base) {
  const result = Object.assign({}, base);
  // Text fields (contenteditable spans)
  document.querySelectorAll('#launch-deal-brief .dbf-text[data-f]').forEach(el => {
    const f = el.dataset.f;
    result[f] = el.innerText.trim();
  });
  // Number fields (inputs)
  document.querySelectorAll('#launch-deal-brief .dbf-num[data-f]').forEach(el => {
    const f = el.dataset.f;
    const raw = el.tagName === 'INPUT' ? el.value : el.innerText;
    const val = parseFloat(raw);
    result[f] = isNaN(val) ? null : val;
  });
  return result;
}

async function launchDealFromParsed() {
  if (!currentParsedDeal) return briefToForm(null, null);
  const parsed = collectEdits(currentParsedDeal);
  currentParsedDeal = parsed;
  // Jump straight to the launch form pre-filled with collected edits
  briefToForm(parsed, currentDocumentId);
}

function briefToForm(parsed, documentId) {
  document.getElementById('launch-deal-brief').style.display = 'none';
  showLaunchStep2(parsed, documentId, parsed.deal_name || '');
}

function showManualForm() {
  document.getElementById('launch-step-1').style.display = 'none';
  showLaunchStep2(null, null, null);
}

function showLaunchStep2(parsed, documentId, filename) {
  document.getElementById('launch-step-1').style.display = 'none';
  document.getElementById('launch-step-2').style.display = '';
  if (documentId) document.getElementById('launch-document-id').value = documentId;
  const label = document.getElementById('launch-doc-label');
  if (label) label.textContent = filename ? `Parsed: ${filename}` : 'Ready to launch';

  // Load priority list selector options
  loadPriorityListsForLaunch();

  if (!parsed) return;

  // Helpers
  const setId = (id, val) => {
    const el = document.getElementById(id);
    if (el && val != null && val !== '') el.value = val;
  };
  const setName = (name, val) => {
    const el = document.querySelector(`#launch-form [name="${name}"]`);
    if (el && val != null && val !== '') el.value = val;
  };
  const setSelect = (id, val) => {
    if (!val) return;
    const el = document.getElementById(id);
    if (!el) return;
    // Try exact match first, then partial
    const opts = [...el.options];
    const exact = opts.find(o => o.value === val || o.text === val);
    if (exact) { el.value = exact.value; return; }
    const partial = opts.find(o => val.toLowerCase().includes(o.value.toLowerCase()) && o.value !== '');
    if (partial) el.value = partial.value;
  };
  // Format a million-dollar number for display: 5.2 → "$5.2M"
  const fmtM = (v) => (v != null && v !== '') ? `$${v}M` : null;

  // ── Required fields ────────────────────────────────────────────────────────
  setId('lf-deal-name', parsed.deal_name);
  setSelect('lf-raise-type', parsed.deal_type);
  setId('lf-sector', parsed.sector);

  // Target amount: prefer explicit raise target, fall back to equity required
  const targetAmt = parsed.target_raise_usd_m ?? parsed.equity_required_usd_m;
  if (targetAmt) setId('lf-target-amount', fmtM(targetAmt));

  // Description: build from company_overview + additional_context
  const descParts = [parsed.company_overview, parsed.additional_context].filter(Boolean);
  if (descParts.length) setName('description', descParts.join('\n\n'));

  // ── Financial fields ───────────────────────────────────────────────────────
  setId('lf-ebitda', parsed.ebitda_usd_m);
  setId('lf-ev', parsed.enterprise_value_usd_m);
  setId('lf-equity', parsed.equity_required_usd_m);

  // Min / max cheque (no IDs — use name selector)
  if (parsed.min_check_usd_m != null) setName('minCheque', fmtM(parsed.min_check_usd_m));
  if (parsed.max_check_usd_m != null) setName('maxCheque', fmtM(parsed.max_check_usd_m));

  // ── Investor fields ────────────────────────────────────────────────────────
  setId('lf-investor-types', (parsed.ideal_investor_types || []).join(', '));

  // Investor profile
  if (parsed.ideal_investor_profile) setName('investorProfile', parsed.ideal_investor_profile);

  // ── Key metrics: investment highlights as bullet list ──────────────────────
  if (parsed.investment_highlights?.length) {
    setName('keyMetrics', parsed.investment_highlights.join('\n'));
  }

  // ── Geography checkboxes ───────────────────────────────────────────────────
  // Combine geography + hq_location for broader matching
  const geoText = [parsed.geography, parsed.hq_location].filter(Boolean).join(' ').toLowerCase();
  if (geoText) {
    const GEO_MAP = {
      UK:     ['uk', 'united kingdom', 'england', 'scotland', 'wales', 'britain', 'london'],
      Europe: ['europe', 'european', 'eu', 'germany', 'france', 'spain', 'italy', 'netherlands', 'sweden', 'nordic'],
      US:     ['us', 'usa', 'united states', 'america', 'american', 'northeast', 'texas', 'florida', 'california', 'new york'],
      Global: ['global', 'worldwide', 'international', 'cross-border'],
      MENA:   ['mena', 'middle east', 'north africa', 'uae', 'dubai', 'saudi', 'gulf'],
      APAC:   ['apac', 'asia', 'pacific', 'australia', 'singapore', 'hong kong', 'japan', 'india'],
    };
    document.querySelectorAll('#launch-form [name="geography"]').forEach(cb => {
      const keywords = GEO_MAP[cb.value] || [];
      if (keywords.some(kw => geoText.includes(kw))) cb.checked = true;
    });
  }
}

function addLaunchAssetRow() {
  const list = document.getElementById('launch-assets-list');
  if (!list) return;
  const row = document.createElement('div');
  row.style.cssText = 'display:grid;grid-template-columns:1fr 1fr auto auto;gap:8px;align-items:end;margin-bottom:4px';
  row.innerHTML = `
    <input type="text" class="form-input launch-asset-name" placeholder="Name (e.g. Book a Call)" />
    <input type="url" class="form-input launch-asset-url" placeholder="https://…" />
    <select class="form-input launch-asset-type" style="min-width:110px">
      <option value="calendly">Calendly</option>
      <option value="deck">Deck</option>
      <option value="video">Video</option>
      <option value="link">Link</option>
      <option value="other">Other</option>
    </select>
    <button type="button" class="btn" style="padding:6px 10px;color:var(--text-muted);background:transparent;border:1px solid var(--border)" onclick="this.closest('div').remove()">✕</button>
  `;
  list.appendChild(row);
}

function getLaunchAssets() {
  return [...document.querySelectorAll('#launch-assets-list > div')].map(row => ({
    name:       row.querySelector('.launch-asset-name')?.value?.trim(),
    url:        row.querySelector('.launch-asset-url')?.value?.trim(),
    asset_type: row.querySelector('.launch-asset-type')?.value,
  })).filter(a => a.name && a.url);
}

async function submitNewDeal(e) {
  e.preventDefault();
  const form   = e.target;
  const btn    = document.getElementById('launch-btn');
  const data   = new FormData(form);

  // Collect geography checkboxes
  const geo = [...form.querySelectorAll('[name="geography"]:checked')].map(el => el.value);

  const launchAssets = getLaunchAssets();
  const calendlyUrl = (data.get('calendlyUrl') || '').trim();
  if (calendlyUrl) launchAssets.unshift({ name: 'Book a Call', url: calendlyUrl, asset_type: 'calendly' });

  const payload = {
    dealName:              data.get('dealName'),
    raiseType:             data.get('raiseType'),
    targetAmount:          data.get('targetAmount'),
    minCheque:             data.get('minCheque'),
    maxCheque:             data.get('maxCheque'),
    sector:                data.get('sector'),
    geography:             geo,
    description:           data.get('description'),
    keyMetrics:            data.get('keyMetrics'),
    investorProfile:       data.get('investorProfile'),
    deckUrl:               data.get('deckUrl'),
    linkedinUrls:          (data.get('linkedinUrls') || '').split('\n').map(u => u.trim()).filter(Boolean),
    launchAssets:          JSON.stringify(launchAssets),
    ebitda_usd_m:          data.get('ebitda_usd_m') || null,
    enterprise_value_usd_m:data.get('enterprise_value_usd_m') || null,
    equity_required_usd_m: data.get('equity_required_usd_m') || null,
    idealInvestorTypes:    data.get('idealInvestorTypes') || null,
    minInvestorScore:      data.get('minInvestorScore') || 60,
    linkedinDailyLimit:    data.get('linkedinDailyLimit') || 20,
    document_id:           data.get('document_id') || null,
    priority_lists:        JSON.stringify((window.selectedPriorityLists || []).map(l => ({
      list_id: l.id, list_name: l.name, list_type: l.type, priority_order: l.order,
    }))),
    exclusions:            JSON.stringify(parsedExclusions || []),
  };

  btn.disabled    = true;
  btn.textContent = '⏳ Launching…';

  try {
    // Handle CSV separately if present
    const csvInput = document.getElementById('csv-input');
    let formData;
    if (csvInput?.files?.length) {
      formData = new FormData();
      Object.entries(payload).forEach(([k, v]) => {
        formData.append(k, Array.isArray(v) ? JSON.stringify(v) : v);
      });
      formData.append('csvFile', csvInput.files[0]);
      formData.append('timezone',        document.getElementById('nd-timezone')?.value || 'America/New_York');
      formData.append('activeDays',     getDayPickerValue('nd-active-days'));
      formData.append('liConnectFrom',  document.getElementById('nd-li-connect-from')?.value || '08:00');
      formData.append('liConnectUntil', document.getElementById('nd-li-connect-until')?.value || '20:00');
      formData.append('liDmFrom',       document.getElementById('nd-li-dm-from')?.value || '20:00');
      formData.append('liDmUntil',      document.getElementById('nd-li-dm-until')?.value || '23:00');
      formData.append('emailFrom',      document.getElementById('nd-email-from')?.value || '08:00');
      formData.append('emailUntil',     document.getElementById('nd-email-until')?.value || '18:00');
      formData.append('launchAssets',   JSON.stringify(launchAssets));
      const res = await fetch('/api/deals/create', { method: 'POST', body: formData });
      if (!res.ok) throw new Error(await res.text());
    } else {
      payload.timezone       = document.getElementById('nd-timezone')?.value || 'America/New_York';
      payload.activeDays     = getDayPickerValue('nd-active-days');
      payload.liConnectFrom  = document.getElementById('nd-li-connect-from')?.value || '08:00';
      payload.liConnectUntil = document.getElementById('nd-li-connect-until')?.value || '20:00';
      payload.liDmFrom       = document.getElementById('nd-li-dm-from')?.value || '20:00';
      payload.liDmUntil      = document.getElementById('nd-li-dm-until')?.value || '23:00';
      payload.emailFrom      = document.getElementById('nd-email-from')?.value || '08:00';
      payload.emailUntil     = document.getElementById('nd-email-until')?.value || '18:00';
      await api('/api/deals/create', 'POST', payload);
    }

    form.reset();
    document.getElementById('file-drop-text').textContent = 'Drop CSV here or click to browse';
    clearExclusionList();
    resetLaunchStep();
    await populateDealSelector();
    navigate('#deals');
  } catch (err) {
    alert(`Failed to launch deal: ${err.message}`);
  } finally {
    btn.disabled    = false;
    btn.innerHTML   = '🚀&nbsp; LAUNCH ROCO ON THIS DEAL';
  }
}

function onCsvSelected(input) {
  const el = document.getElementById('file-drop-text');
  if (el && input.files?.[0]) el.textContent = `📄 ${input.files[0].name}`;
}

function onCsvDrop(e) {
  e.preventDefault();
  document.getElementById('file-drop-zone')?.classList.remove('drag-over');
  const file = e.dataTransfer?.files?.[0];
  if (file && file.name.endsWith('.csv')) {
    const input = document.getElementById('csv-input');
    const dt = new DataTransfer();
    dt.items.add(file);
    input.files = dt.files;
    onCsvSelected(input);
  }
}

/* ═══════════════════════════════════════════════════════════════════════════
   ACTIVE DEALS
   ═══════════════════════════════════════════════════════════════════════════ */

async function loadDeals() {
  const grid = document.getElementById('deals-grid');
  if (!grid) return;

  // If user has a deal open, refresh the grid data quietly without closing the detail panel
  if (selectedDealId) {
    try {
      const data = await api('/api/deals');
      allDeals = Array.isArray(data) ? data : (data.deals || []);
      const activeDeals = allDeals.filter(d => !['CLOSED','ARCHIVED','COMPLETE','closed','archived','complete'].includes(d.status));
      grid.innerHTML = activeDeals.map(deal => renderDealCard(deal)).join('');
    } catch { /* silent — user is in detail view */ }
    return;
  }

  grid.innerHTML = '<div class="loading-placeholder">Loading deals…</div>';
  closeDealDetail();

  try {
    const data = await api('/api/deals');
    allDeals = Array.isArray(data) ? data : (data.deals || []);
    const activeDeals = allDeals.filter(d => !['CLOSED','ARCHIVED','COMPLETE','closed','archived','complete'].includes(d.status));
    if (!activeDeals.length) {
      grid.innerHTML = '<div class="loading-placeholder">No active deals. <a href="#launch" onclick="navigate(\'#launch\')">Launch one ↗</a></div>';
      return;
    }
    grid.innerHTML = activeDeals.map(deal => renderDealCard(deal)).join('');
  } catch {
    grid.innerHTML = '<div class="loading-placeholder text-red">Failed to load deals.</div>';
  }
}

function renderDealCard(deal) {
  const id       = deal.id || deal._id;
  const name     = esc(deal.dealName || deal.name);
  const type     = esc(deal.raiseType || '');
  const status   = (deal.status || 'active').toLowerCase();
  const committed = deal.committed_amount || deal.capitalCommitted || 0;
  const target   = deal.target_amount || deal.targetAmount || 0;
  const pct_     = target > 0 ? Math.min(100, Math.round((committed / target) * 100)) : 0;
  const contacts = fmt(deal.contacts || deal.live_contacts || deal.totalContacts || 0);
  const emails   = fmt(deal.emails_sent || deal.live_emails_sent || deal.emailsSent || 0);
  const rr       = (deal.response_rate ?? deal.responseRate) != null ? pct(deal.response_rate ?? deal.responseRate) : '—';
  const paused   = deal.paused === true || status === 'paused';
  const badgeLabel = paused ? 'PAUSED' : status.toUpperCase();
  const badgeClass = paused ? 'paused' : status;
  const needsReview = deal.current_batch_status === 'pending_approval';

  return `<div class="deal-card" id="deal-card-${id}">
    <div class="deal-card-top">
      <div>
        <div class="deal-card-name">${name}</div>
        <div class="deal-card-type">${type}</div>
      </div>
      <div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px">
        <span class="status-badge ${badgeClass}">${badgeLabel}</span>
        ${needsReview ? `<span class="status-badge" style="background:rgba(234,179,8,.15);color:#eab308;font-size:9px;cursor:pointer" onclick="viewDeal('${id}');setTimeout(()=>switchDealTab('campaign',document.querySelector('[data-tab=campaign]')),400)">REVIEW REQUIRED</span>` : ''}
      </div>
    </div>
    <div class="deal-progress-wrap">
      <div class="deal-progress-label">
        <span>${deal.capitalCommittedDisplay || formatMoney(committed, deal.currency || 'USD')}</span>
        <span>${deal.targetAmountDisplay || formatMoney(target, deal.currency || 'USD')}</span>
      </div>
      <div class="deal-progress-bar">
        <div class="deal-progress-fill" style="width:${pct_}%"></div>
      </div>
    </div>
    <div class="deal-stats">
      <div class="deal-stat">
        <div class="deal-stat-val">${contacts}</div>
        <div class="deal-stat-lbl">Contacts</div>
      </div>
      <div class="deal-stat">
        <div class="deal-stat-val">${emails}</div>
        <div class="deal-stat-lbl">Emails</div>
      </div>
      <div class="deal-stat">
        <div class="deal-stat-val">${rr}</div>
        <div class="deal-stat-lbl">Response</div>
      </div>
    </div>
    <div class="deal-card-actions">
      <button class="btn btn-ghost btn-sm" onclick="viewDeal('${id}')">VIEW</button>
      <button class="btn btn-danger btn-sm" onclick="closeDeal('${id}')">CLOSE</button>
      <label class="toggle-switch deal-pause-toggle" title="${paused ? 'Resume' : 'Pause'}">
        <input type="checkbox" ${paused ? '' : 'checked'} onchange="toggleDealPause('${id}', '${status}', this)" />
        <span class="toggle-track"></span>
      </label>
    </div>
  </div>`;
}

async function viewDeal(id) {
  selectedDealId = id;
  selectedDealReadOnly = false;
  selectedDealBackSection = 'deals';
  const grid   = document.getElementById('deals-grid');
  const panel  = document.getElementById('deal-detail-panel');
  if (grid) grid.classList.add('hidden');
  if (panel) panel.classList.remove('hidden');

  const backBtn = document.getElementById('deal-detail-back-btn');
  if (backBtn) backBtn.textContent = '← Back to Deals';

  const deal = allDeals.find(d => (d.id || d._id) === id) || {};
  window.__activeDealCurrency = deal.currency || 'USD';
  window.__previewDealId = id;
  setText('deal-detail-name', deal.dealName || deal.name || id);
  const statusBadge = document.getElementById('deal-detail-status-badge');
  if (statusBadge) {
    statusBadge.textContent = (deal.status || '').toUpperCase();
    statusBadge.className = `status-badge ${deal.status || ''}`;
  }

  switchDealTab('overview', document.querySelector('.deal-tab.active') || document.querySelector('.deal-tab'));
  await loadDealTabOverview(id, deal);
}

async function viewArchivedDeal(id) {
  // Ensure deal is in allDeals so viewDeal can find it
  if (!allDeals.find(d => (d.id || d._id) === id)) {
    try {
      const deal = await api(`/api/deals/${id}`);
      allDeals = [...(allDeals || []), deal];
    } catch {}
  }

  selectedDealId = id;
  selectedDealReadOnly = true;
  selectedDealBackSection = 'archive';

  // Navigate to #deals section (houses the detail panel) — loadDeals will do a quiet refresh
  // because selectedDealId is already set
  navigate('#deals');

  const grid  = document.getElementById('deals-grid');
  const panel = document.getElementById('deal-detail-panel');
  if (grid) grid.classList.add('hidden');
  if (panel) panel.classList.remove('hidden');

  const backBtn = document.getElementById('deal-detail-back-btn');
  if (backBtn) backBtn.textContent = '← Back to Archive';

  const deal = allDeals.find(d => (d.id || d._id) === id) || {};
  setText('deal-detail-name', deal.dealName || deal.name || id);
  const statusBadge = document.getElementById('deal-detail-status-badge');
  if (statusBadge) {
    statusBadge.textContent = (deal.status || 'CLOSED').toUpperCase();
    statusBadge.className = `status-badge ${(deal.status || 'closed').toLowerCase()}`;
  }

  switchDealTab('overview', document.querySelector('.deal-tab.active') || document.querySelector('.deal-tab'));
  await loadDealTabOverview(id, deal);
}

function closeDealDetail() {
  const backSection = selectedDealBackSection;
  selectedDealId = null;
  selectedDealReadOnly = false;
  selectedDealBackSection = 'deals';
  const grid  = document.getElementById('deals-grid');
  const panel = document.getElementById('deal-detail-panel');
  if (grid) grid.classList.remove('hidden');
  if (panel) panel.classList.add('hidden');
  if (backSection === 'archive') navigate('#archive');
}

async function switchDealTab(tab, btn) {
  document.querySelectorAll('.deal-tab').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');

  ['overview','brief','pipeline','rankings','batches','settings','archived','exclusions','templates','campaign'].forEach(t => {
    const el = document.getElementById(`deal-tab-${t}`);
    if (el) el.classList.toggle('hidden', t !== tab);
  });

  if (!selectedDealId) return;
  switch (tab) {
    case 'overview':   await loadDealTabOverview(selectedDealId);   break;
    case 'brief':      await loadDealTabBrief(selectedDealId);      break;
    case 'pipeline':   await loadDealTabPipeline(selectedDealId);   break;
    case 'rankings':   await loadDealTabRankings(selectedDealId, window.__rankingsNavId === selectedDealId ? (window.__rankingsCurrentPage || 1) : 1);   break;
    case 'batches':    await loadDealTabBatches(selectedDealId);    break;
    case 'settings':   await loadDealTabSettings(selectedDealId);   break;
    case 'archived':   await loadDealTabArchived(selectedDealId);   break;
    case 'exclusions': await loadDealTabExclusions(selectedDealId); break;
    case 'templates':  await loadDealTemplatesTab(selectedDealId);  break;
    case 'campaign':   await loadCampaignReviewTab(selectedDealId); break;
  }
}

async function loadDealTabOverview(id, deal) {
  const el = document.getElementById('deal-tab-overview');
  if (!el) return;
  if (!deal) {
    try { deal = await api(`/api/deals/${id}`); } catch { deal = {}; }
  }
  let m = {};
  try { m = await api(`/api/deals/${id}/metrics`); } catch {}

  const stat = (val, lbl, sub) => `<div class="deal-stat">
    <div class="deal-stat-val">${val}</div>
    <div class="deal-stat-lbl">${lbl}</div>
    ${sub ? `<div style="font-size:10px;color:var(--text-dim);margin-top:2px">${sub}</div>` : ''}
  </div>`;

  el.innerHTML = `
    <div style="margin-bottom:6px;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em">Fundraise</div>
    <div class="deal-stats" style="grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px">
      ${stat(formatMoney(m.capitalCommitted || deal.committed_amount || 0, deal.currency || 'USD'), 'Capital Committed')}
      ${stat(formatMoney(m.targetAmount || deal.target_amount || 0, deal.currency || 'USD'), 'Target')}
      ${stat(fmt(m.activeProspects || 0), 'Active Prospects')}
      ${stat(fmt(m.totalContacts || 0), 'Total Contacts')}
    </div>
    <div style="margin-bottom:6px;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em">Email</div>
    <div class="deal-stats" style="grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:24px">
      ${stat(fmt(m.emailsSent || 0), 'Emails Sent')}
      ${stat(fmt(m.emailReplies != null ? m.emailReplies : (m.emailResponses || 0)), 'Replies')}
      ${stat(m.emailResponseRate != null ? m.emailResponseRate + '%' : '—', 'Response Rate')}
    </div>
    <div style="margin-bottom:6px;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em">LinkedIn</div>
    <div class="deal-stats" style="grid-template-columns:repeat(5,1fr);gap:16px;margin-bottom:24px">
      ${stat(fmt(m.invitesSent || 0), 'Invites Sent')}
      ${stat(fmt(m.invitesAccepted || 0), 'Accepted', m.acceptanceRate != null ? m.acceptanceRate + '% rate' : '')}
      ${stat(fmt(m.dmsSent || 0), 'DMs Sent')}
      ${stat(fmt(m.dmResponses || 0), 'DM Replies')}
      ${stat(m.dmResponseRate != null ? m.dmResponseRate + '%' : '—', 'DM Response Rate')}
    </div>
    <div class="form-group mb-16">
      <div class="form-label">Description</div>
      <div style="color:var(--text-mid);font-size:13px;line-height:1.6">${esc(deal.description || '—')}</div>
    </div>
    ${deal.sector ? `<div class="form-group"><div class="form-label">Sector</div><div style="color:var(--text-mid)">${esc(deal.sector)}</div></div>` : ''}
  `;
}

async function loadDealTabBrief(id) {
  const el = document.getElementById('deal-tab-brief');
  if (!el) return;
  el.innerHTML = '<div class="loading-placeholder">Loading deal brief…</div>';
  try {
    const deal = await api(`/api/deals/${id}`);

    // Try parsed_deal_info from linked document first (Claude-parsed PDF)
    let parsed = null;
    if (deal.parsed_deal_info) {
      parsed = typeof deal.parsed_deal_info === 'string'
        ? JSON.parse(deal.parsed_deal_info)
        : deal.parsed_deal_info;
    }

    // Fall back: construct a brief from the deal's own form fields
    if (!parsed) {
      parsed = dealToParsedBrief(deal);
    }

    el.innerHTML = renderDealBriefTab(parsed);
  } catch (err) {
    el.innerHTML = `<div style="padding:24px;color:#ef4444">Failed to load deal brief: ${esc(err.message)}</div>`;
  }
}

/**
 * Build a parsed-brief-compatible object from a deal's own DB fields.
 * Used when no PDF was uploaded or Claude parsing failed.
 */
function dealToParsedBrief(deal) {
  // target_amount is stored in raw units (e.g. 5000000), convert to $M
  const toM = (v) => (v && v > 0) ? +(v / 1_000_000).toFixed(2) : null;

  // Parse geography string back into array for investor_types display
  const geoStr = deal.geography || '';

  // key_metrics lines → investment highlights
  const highlights = (deal.key_metrics || '')
    .split('\n').map(s => s.trim()).filter(Boolean);

  // ideal_investor_types from the stored text field
  const investorTypes = (deal.investor_profile || deal.ideal_investor_types || '')
    .split(',').map(s => s.trim()).filter(Boolean).slice(0, 6);

  return {
    deal_name:                    deal.name,
    deal_type:                    deal.raise_type || 'Equity',
    sector:                       deal.sector || null,
    sub_sector:                   null,
    geography:                    geoStr,
    hq_location:                  geoStr,
    company_overview:             deal.description || null,
    ebitda_usd_m:                 deal.ebitda_usd_m || null,
    ebitda_margin_pct:            null,
    enterprise_value_usd_m:       deal.enterprise_value_usd_m || null,
    ev_ebitda_multiple:           null,
    equity_required_usd_m:        deal.equity_required_usd_m || toM(deal.target_amount),
    debt_available_usd_m:         null,
    seller_note_usd_m:            null,
    rollover_equity_usd_m:        null,
    revenue_usd_m:                null,
    revenue_cagr_pct:             null,
    ebitda_cagr_pct:              null,
    target_raise_usd_m:           toM(deal.target_amount),
    open_ended:                   !deal.target_amount || deal.target_amount === 0,
    min_check_usd_m:              toM(deal.min_cheque),
    max_check_usd_m:              toM(deal.max_cheque),
    ideal_investor_types:         investorTypes.length ? investorTypes : null,
    ideal_investor_profile:       deal.investor_profile || null,
    investment_highlights:        highlights.length ? highlights : null,
    key_risks:                    null,
    growth_levers:                null,
    fundraising_strategy:         null,
    estimated_investors_to_contact: deal.max_total_outreach || 200,
    management_team_staying:      null,
    incoming_management:          null,
    timeline:                     null,
    additional_context:           null,
    _source:                      'form', // internal flag
  };
}

function renderDealBriefTab(parsed) {
  const fmt = (v, suffix = 'M') => v != null ? `$${v}${suffix}` : '—';
  const pct = (v) => v != null ? `${v}%` : '—';
  const row = (label, val) => `<tr><td style="padding:8px 12px;color:#6b7280;font-size:12px;white-space:nowrap">${label}</td><td style="padding:8px 12px;color:#e5e7eb;font-size:13px">${val}</td></tr>`;
  const sourceLabel = parsed._source === 'form'
    ? `<span style="font-size:11px;color:#6b7280;background:#1f2937;padding:2px 8px;border-radius:4px;margin-left:8px">From launch form</span>`
    : `<span style="font-size:11px;color:#6b7280;background:#1f2937;padding:2px 8px;border-radius:4px;margin-left:8px">Claude-parsed</span>`;

  return `
  <div style="padding:20px;max-width:900px">
    <!-- Header -->
    <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:8px;padding:20px 24px;margin-bottom:16px">
      <h2 style="color:#e5e7eb;margin:0 0 6px">${esc(parsed.deal_name || '—')}${sourceLabel}</h2>
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px">
        <span style="background:#1f2937;color:#d4a847;padding:3px 10px;border-radius:4px;font-size:12px">${esc(parsed.deal_type || '—')}</span>
        ${parsed.open_ended ? '<span style="background:#1f2937;color:#60a5fa;padding:3px 10px;border-radius:4px;font-size:12px">Open-ended raise</span>' : ''}
        ${parsed.sector ? `<span style="background:#1a1a1a;border:1px solid #2a2a2a;color:#9ca3af;padding:3px 10px;border-radius:4px;font-size:12px">${esc(parsed.sector)}</span>` : ''}
        ${parsed.hq_location ? `<span style="background:#1a1a1a;border:1px solid #2a2a2a;color:#9ca3af;padding:3px 10px;border-radius:4px;font-size:12px">📍 ${esc(parsed.hq_location)}</span>` : ''}
      </div>
      ${parsed.company_overview ? `<p style="color:#9ca3af;margin:0;font-size:13px;line-height:1.6">${esc(parsed.company_overview)}</p>` : ''}
    </div>

    <!-- Financials grid -->
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">
      <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:8px;overflow:hidden">
        <div style="padding:12px 16px;border-bottom:1px solid #2a2a2a;color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Deal Financials</div>
        <table style="width:100%;border-collapse:collapse">
          ${row('Revenue', fmt(parsed.revenue_usd_m))}
          ${row('EBITDA', fmt(parsed.ebitda_usd_m))}
          ${row('EBITDA Margin', pct(parsed.ebitda_margin_pct))}
          ${row('Enterprise Value', fmt(parsed.enterprise_value_usd_m))}
          ${row('EV / EBITDA', parsed.ev_ebitda_multiple != null ? parsed.ev_ebitda_multiple + 'x' : '—')}
          ${row('Equity Required', parsed.open_ended ? 'Open' : fmt(parsed.equity_required_usd_m))}
          ${row('Debt Available', fmt(parsed.debt_available_usd_m))}
          ${row('Seller Note', fmt(parsed.seller_note_usd_m))}
          ${row('Rollover Equity', fmt(parsed.rollover_equity_usd_m))}
        </table>
      </div>
      <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:8px;overflow:hidden">
        <div style="padding:12px 16px;border-bottom:1px solid #2a2a2a;color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Growth &amp; Structure</div>
        <table style="width:100%;border-collapse:collapse">
          ${row('Revenue CAGR', pct(parsed.revenue_cagr_pct))}
          ${row('EBITDA CAGR', pct(parsed.ebitda_cagr_pct))}
          ${row('Target Raise', parsed.target_raise_usd_m ? fmt(parsed.target_raise_usd_m) : 'Open-ended')}
          ${row('Min Cheque', fmt(parsed.min_check_usd_m))}
          ${row('Max Cheque', fmt(parsed.max_check_usd_m))}
          ${row('Incoming Mgmt', parsed.incoming_management != null ? esc(String(parsed.incoming_management)) : '—')}
          ${row('Mgmt Staying', parsed.management_team_staying != null ? esc(String(parsed.management_team_staying)) : '—')}
          ${row('Timeline', parsed.timeline ? esc(parsed.timeline) : '—')}
          ${row('Est. Investors', (parsed.estimated_investors_to_contact || 200).toString())}
        </table>
      </div>
    </div>

    <!-- Strategy -->
    ${parsed.fundraising_strategy ? `
    <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:8px;padding:16px 20px;margin-bottom:16px">
      <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:8px;letter-spacing:.08em">Fundraising Strategy</div>
      <p style="color:#e5e7eb;margin:0;font-size:13px;line-height:1.7">${esc(parsed.fundraising_strategy)}</p>
    </div>` : ''}

    <!-- Investor profile -->
    ${parsed.ideal_investor_profile ? `
    <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:8px;padding:16px 20px;margin-bottom:16px">
      <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:8px;letter-spacing:.08em">Ideal Investor Profile</div>
      <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px">
        ${(parsed.ideal_investor_types || []).map(t => `<span style="background:#1f3a5f;color:#60a5fa;padding:3px 10px;border-radius:4px;font-size:11px">${esc(t)}</span>`).join('')}
      </div>
      <p style="color:#9ca3af;margin:0;font-size:13px;line-height:1.6">${esc(parsed.ideal_investor_profile)}</p>
    </div>` : ''}

    <!-- Highlights & Risks -->
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">
      ${parsed.investment_highlights?.length ? `
      <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:8px;padding:16px 20px">
        <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:8px;letter-spacing:.08em">Investment Highlights</div>
        <ul style="margin:0;padding-left:16px;color:#9ca3af;font-size:12px">
          ${parsed.investment_highlights.map(h => `<li style="margin-bottom:4px">${esc(h)}</li>`).join('')}
        </ul>
      </div>` : '<div></div>'}
      ${parsed.key_risks?.length ? `
      <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:8px;padding:16px 20px">
        <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:8px;letter-spacing:.08em">Key Risks</div>
        <ul style="margin:0;padding-left:16px;color:#9ca3af;font-size:12px">
          ${parsed.key_risks.map(r => `<li style="margin-bottom:4px">${esc(r)}</li>`).join('')}
        </ul>
      </div>` : '<div></div>'}
    </div>

    <!-- Growth levers -->
    ${parsed.growth_levers?.length ? `
    <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:8px;padding:16px 20px">
      <div style="color:#6b7280;font-size:10px;text-transform:uppercase;margin-bottom:8px;letter-spacing:.08em">Growth Levers</div>
      <ul style="margin:0;padding-left:16px;color:#9ca3af;font-size:12px;columns:2">
        ${parsed.growth_levers.map(l => `<li style="margin-bottom:4px">${esc(l)}</li>`).join('')}
      </ul>
    </div>` : ''}
  </div>`;
}

async function loadDealTabPipeline(id) {
  const el = document.getElementById('deal-tab-pipeline');
  if (!el) return;
  el.innerHTML = '<div class="loading-placeholder">Loading pipeline…</div>';
  try {
    const rows = await api(`/api/pipeline?dealId=${id}`);
    if (!rows.length) { el.innerHTML = '<div class="loading-placeholder">No active contacts in pipeline.</div>'; return; }
    const stageOrder = { invite_sent: 1, Enriched: 2, Ranked: 3, invite_accepted: 4, dm_sent: 5, email_sent: 6, Replied: 7, 'Meeting Booked': 8, Researched: 9 };
    const sorted = [...rows].sort((a, b) => (stageOrder[b.stage] || 0) - (stageOrder[a.stage] || 0) || (b.score || 0) - (a.score || 0));
    // Both Ranked and Enriched contacts get LinkedIn invites (parallel dual-channel for those with email)
    const pendingInvites = rows.filter(r => (r.stage === 'Ranked' || r.stage === 'Enriched') && r.linkedinUrl).length;
    el.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <div style="font-size:13px;color:var(--text-dim)">${rows.length} contact${rows.length !== 1 ? 's' : ''}</div>
        <div style="display:flex;gap:8px;align-items:center">
          ${!selectedDealReadOnly && pendingInvites > 0 ? `<button class="btn btn-sm" onclick="sendInvitesNow('${id}')" style="font-size:12px">Send ${pendingInvites} LinkedIn Invite${pendingInvites !== 1 ? 's' : ''} Now</button>` : ''}
          <button class="btn btn-ghost btn-sm" onclick="exportDealPipelineCSV('${id}')" style="font-size:12px">&#8595; Export CSV</button>
          ${!selectedDealReadOnly ? `<button class="btn btn-sm" onclick="clearPipeline('${id}', this)" style="font-size:12px;background:rgba(220,50,50,0.15);color:#e05;border:1px solid rgba(220,50,50,0.3)">Clear Pipeline</button>` : ''}
        </div>
      </div>
      <div class="table-wrap">
      <table class="data-table">
        <thead><tr><th>Investor</th><th>Title / Firm</th><th>Score</th><th>Stage</th><th>Enrichment</th><th>Last Activity</th>${!selectedDealReadOnly ? '<th></th>' : ''}</tr></thead>
        <tbody>
          ${sorted.map(r => `<tr>
            <td>${r.linkedinUrl ? `<a href="${esc(r.linkedinUrl)}" target="_blank" style="color:var(--accent)">${esc(r.name || '—')}</a>` : esc(r.name || '—')}</td>
            <td class="text-dim" style="font-size:12px">${r.jobTitle && r.firm ? `${esc(r.jobTitle)} <span style="opacity:0.5">·</span> ${esc(r.firm)}` : esc(r.jobTitle || r.firm || '—')}</td>
            <td>${scoreHtml(r.score)}</td>
            <td><span class="status-badge">${esc(r.stage || '—')}</span></td>
            <td class="text-dim" style="font-size:11px">${esc(r.enrichmentStatus || '—')}</td>
            <td class="text-dim">${formatDate(r.lastContacted)}</td>
            ${!selectedDealReadOnly ? `<td><button class="row-action-btn" style="color:#e05c5c" onclick="deleteDealTabContact('${r.id}', '${id}', this)">✕</button></td>` : ''}
          </tr>`).join('')}
        </tbody>
      </table>
    </div>
    </div>`;
  } catch { el.innerHTML = '<div class="loading-placeholder text-red">Failed to load.</div>'; }
}

async function sendInvitesNow(dealId) {
  const btn = event.target;
  btn.disabled = true;
  btn.textContent = 'Sending…';
  try {
    const result = await api(`/api/deals/${dealId}/trigger-invites`, 'POST');
    const failed = (result.results || []).filter(r => r.status === 'failed');
    if (result.sent > 0) {
      showToast(`${result.sent} invite${result.sent !== 1 ? 's' : ''} sent`);
    } else if (failed.length) {
      showToast(`0 sent — ${failed[0].error || 'Unipile error'}`, 'error');
    } else {
      showToast(result.message || '0 invites sent — no eligible contacts', 'error');
    }
    await loadDealTabPipeline(dealId);
  } catch (err) {
    showToast('Failed: ' + (err.message || 'Unknown error'), 'error');
    btn.disabled = false;
    btn.textContent = 'Send Invites Now';
  }
}

async function clearPipeline(dealId, btn) {
  if (!confirm('Clear entire pipeline?\n\nThis permanently deletes ALL contacts for this deal from Supabase, the dashboard, and Notion.\n\nThis cannot be undone.')) return;
  if (btn) { btn.disabled = true; btn.textContent = 'Clearing…'; }
  try {
    const result = await api(`/api/deals/${dealId}/clear-pipeline`, 'POST');
    showToast(`Pipeline cleared — ${result.cleared} contacts removed`);
    await loadDealTabPipeline(dealId);
  } catch (err) {
    showToast('Failed: ' + err.message, 'error');
    if (btn) { btn.disabled = false; btn.textContent = 'Clear Pipeline'; }
  }
}

async function deleteDealTabContact(contactId, dealId, btn) {
  if (!confirm('Remove this contact from the pipeline? This cannot be undone.')) return;
  if (btn) { btn.disabled = true; btn.textContent = '…'; }
  try {
    await api(`/api/contacts/${contactId}`, 'DELETE');
    const row = btn?.closest('tr');
    if (row) row.remove();
    showToast('Contact removed');
  } catch (err) {
    if (btn) { btn.disabled = false; btn.textContent = '✕'; }
    showToast('Failed: ' + err.message, 'error');
  }
}

async function loadDealTabArchived(id) {
  const el = document.getElementById('deal-tab-archived');
  if (!el) return;
  el.innerHTML = '<div class="loading-placeholder">Loading archived contacts…</div>';
  try {
    const rows = await api(`/api/deals/${id}/archived`);
    if (!rows.length) { el.innerHTML = '<div class="loading-placeholder">No archived contacts.</div>'; return; }
    const reactivatable = rows.filter(r => (r.score || 0) >= 40);
    el.innerHTML = `
      <div style="margin-bottom:12px;display:flex;align-items:center;justify-content:space-between;gap:12px">
        <span style="color:var(--text-dim);font-size:13px">${rows.length} archived investor${rows.length !== 1 ? 's' : ''} — below minimum score threshold or outside deal criteria.</span>
        <div style="display:flex;gap:8px">
          <button class="btn btn-ghost btn-sm" style="font-size:12px;white-space:nowrap" onclick="exportDealArchivedCSV('${id}', this)">&#8595; Export CSV</button>
          ${!selectedDealReadOnly && reactivatable.length ? `<button class="btn btn-sm" style="white-space:nowrap" onclick="reactivateAllBorderline('${id}', this)">Re-activate borderline (${reactivatable.length})</button>` : ''}
        </div>
      </div>
      <div class="table-wrap">
        <table class="data-table">
          <thead><tr><th>Investor</th><th>Title / Firm</th><th>Score</th><th>Reason</th><th></th></tr></thead>
          <tbody>
            ${rows.map(r => `<tr id="archived-row-${r.id}">
              <td>${esc(r.name || '—')}</td>
              <td class="text-dim" style="font-size:12px">${r.jobTitle && r.firm ? `${esc(r.jobTitle)} <span style="opacity:0.5">·</span> ${esc(r.firm)}` : esc(r.jobTitle || r.firm || '—')}</td>
              <td>${scoreHtml(r.score)}</td>
              <td class="text-dim" style="font-size:12px;max-width:300px">${esc(r.archiveReason || '—')}</td>
              <td>${!selectedDealReadOnly && (r.score || 0) >= 40 ? `<button class="btn btn-ghost" style="font-size:11px;padding:3px 8px;white-space:nowrap" onclick="reactivateContact('${r.id}', this)">Re-activate</button>` : ''}</td>
            </tr>`).join('')}
          </tbody>
        </table>
      </div>`;
  } catch { el.innerHTML = '<div class="loading-placeholder text-red">Failed to load.</div>'; }
}

async function reactivateContact(id, btn) {
  if (btn) { btn.disabled = true; btn.textContent = '…'; }
  try {
    await api(`/api/contacts/${id}/reactivate`, 'POST');
    const row = document.getElementById(`archived-row-${id}`);
    if (row) { row.style.opacity = '0.3'; row.style.textDecoration = 'line-through'; }
    if (btn) btn.textContent = 'Done';
    showToast?.('Contact moved back to pipeline');
  } catch (err) {
    if (btn) { btn.disabled = false; btn.textContent = 'Re-activate'; }
    showToast?.('Failed: ' + err.message, 'error');
  }
}

async function reactivateAllBorderline(dealId, btn) {
  if (!confirm('Re-activate all archived contacts with score ≥ 40? They will re-enter the pipeline for enrichment and outreach.')) return;
  if (btn) { btn.disabled = true; btn.textContent = 'Working…'; }
  try {
    // Find all re-activate buttons and click them sequentially
    const allBtns = document.querySelectorAll('[id^="archived-row-"] button');
    let count = 0;
    for (const b of allBtns) {
      if (b.textContent.trim() === 'Re-activate') {
        const row = b.closest('tr');
        const contactId = row?.id?.replace('archived-row-', '');
        if (contactId) {
          await api(`/api/contacts/${contactId}/reactivate`, 'POST');
          row.style.opacity = '0.3';
          row.style.textDecoration = 'line-through';
          b.textContent = 'Done';
          count++;
        }
      }
    }
    showToast?.(`${count} contacts moved back to pipeline`);
    if (btn) { btn.textContent = `${count} reactivated`; }
  } catch (err) {
    if (btn) { btn.disabled = false; btn.textContent = 'Re-activate borderline'; }
    showToast?.('Failed: ' + err.message, 'error');
  }
}

// ─── EXCLUSION LIST TAB ──────────────────────────────────────────────────────

async function loadDealTabExclusions(id) {
  const el = document.getElementById('deal-tab-exclusions');
  if (!el) return;
  el.innerHTML = '<div class="loading-placeholder">Loading exclusions…</div>';
  try {
    const exclusions = await api(`/api/deals/${id}/exclusions`);
    el.innerHTML = `
      <div style="padding:16px">

        <!-- Bulk upload section -->
        <div style="margin-bottom:20px;padding:16px;background:#0f0f0f;
                    border:1px solid #2a2a2a;border-radius:6px">
          <div style="font-family:'DM Mono',monospace;font-size:11px;
                      text-transform:uppercase;letter-spacing:0.1em;
                      color:#6b7280;margin-bottom:10px">Bulk Upload</div>
          <div id="excl-drop-${id}"
            style="border:1px dashed #2a2a2a;border-radius:6px;padding:20px;
                   text-align:center;cursor:pointer;transition:border-color 0.2s"
            onclick="document.getElementById('excl-file-${id}').click()"
            ondragover="event.preventDefault();this.style.borderColor='#d4a847'"
            ondragleave="this.style.borderColor='#2a2a2a'"
            ondrop="handleExclDrop(event,'${id}')">
            <div style="color:#6b7280;font-size:13px;margin-bottom:4px">
              Drop XLSX or CSV here, or click to browse
            </div>
            <div style="color:#374151;font-size:11px;font-family:'DM Mono',monospace">
              Columns: Account Name, First Name, Last Name, Contact Email
            </div>
          </div>
          <input type="file" id="excl-file-${id}" accept=".xlsx,.csv"
            style="display:none"
            onchange="handleExclFile(this.files[0],'${id}')">
        </div>

        <!-- Manual single entry -->
        <div style="display:flex;gap:8px;margin-bottom:16px">
          <input id="excl-firm-${id}" placeholder="Firm name"
            style="flex:1;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;
                   color:#e5e7eb;border-radius:6px;font-size:13px">
          <input id="excl-person-${id}" placeholder="Person name (optional)"
            style="flex:1;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;
                   color:#e5e7eb;border-radius:6px;font-size:13px">
          <input id="excl-email-${id}" placeholder="Email (optional)"
            style="flex:1;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;
                   color:#e5e7eb;border-radius:6px;font-size:13px">
          <button onclick="addExclusion('${id}')"
            style="padding:8px 16px;background:#d4a847;border:none;color:#000;
                   border-radius:6px;cursor:pointer;font-weight:600;white-space:nowrap">
            + Add
          </button>
        </div>

        <!-- Count summary -->
        <div style="color:#6b7280;font-size:12px;margin-bottom:12px;
                    font-family:'DM Mono',monospace">
          ${exclusions.length} exclusion${exclusions.length !== 1 ? 's' : ''} for this deal
        </div>

        <!-- Table -->
        ${exclusions.length === 0
          ? `<p style="color:#4a4a4a;font-size:13px;text-align:center;padding:32px">
               No exclusions yet. Upload a file or add manually above.
             </p>`
          : `<div style="max-height:400px;overflow-y:auto">
               <table style="width:100%;border-collapse:collapse">
                 <thead style="position:sticky;top:0;background:#111">
                   <tr style="border-bottom:1px solid #2a2a2a">
                     <th style="text-align:left;padding:8px 12px;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;font-weight:400">Firm / Account</th>
                     <th style="text-align:left;padding:8px 12px;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;font-weight:400">Person</th>
                     <th style="text-align:left;padding:8px 12px;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;font-weight:400">Email</th>
                     <th></th>
                   </tr>
                 </thead>
                 <tbody>
                   ${exclusions.map(ex => `
                     <tr style="border-bottom:1px solid #1a1a1a" id="excl-row-${ex.id}">
                       <td style="padding:8px 12px;color:#e5e7eb;font-size:13px">${esc(ex.firm_name || '—')}</td>
                       <td style="padding:8px 12px;color:#9ca3af;font-size:13px">${esc(ex.person_name || '—')}</td>
                       <td style="padding:8px 12px;color:#6b7280;font-size:12px;font-family:'DM Mono',monospace">${esc(ex.email || '—')}</td>
                       <td style="padding:8px 12px;text-align:right">
                         <button onclick="removeExclusion('${id}','${ex.id}')"
                           style="background:none;border:none;color:#374151;cursor:pointer;font-size:18px;line-height:1">×</button>
                       </td>
                     </tr>`).join('')}
                 </tbody>
               </table>
             </div>`
        }
      </div>`;
  } catch { el.innerHTML = '<div class="loading-placeholder text-red">Failed to load.</div>'; }
}

window.handleExclDrop = function(e, dealId) {
  e.preventDefault();
  const zone = document.getElementById(`excl-drop-${dealId}`);
  if (zone) zone.style.borderColor = '#2a2a2a';
  const file = e.dataTransfer.files[0];
  if (file) handleExclFile(file, dealId);
};

window.handleExclFile = async function(file, dealId) {
  if (!file) return;
  showToast('Parsing exclusion file...');
  const formData = new FormData();
  formData.append('file', file);
  try {
    const res = await fetch(`${API_BASE}/api/deals/${dealId}/exclusions/upload`, {
      method: 'POST',
      body: formData,
      credentials: 'include',
    });
    const data = await res.json();
    if (data.success) {
      showToast(`${data.imported} exclusions imported`);
      loadDealTabExclusions(dealId);
    } else {
      showToast('Import failed: ' + (data.error || 'Unknown'), 'error');
    }
  } catch (err) {
    showToast('Import failed: ' + err.message, 'error');
  }
};

window.addExclusion = async function(dealId) {
  const firm   = document.getElementById(`excl-firm-${dealId}`)?.value?.trim();
  const person = document.getElementById(`excl-person-${dealId}`)?.value?.trim();
  const email  = document.getElementById(`excl-email-${dealId}`)?.value?.trim();
  if (!firm && !person && !email) {
    showToast('Enter at least a firm name, person, or email', 'error');
    return;
  }
  try {
    const data = await api(`/api/deals/${dealId}/exclusions`, 'POST', {
      firm_name: firm || null, person_name: person || null, email: email || null,
    });
    if (data.success) {
      showToast('Exclusion added');
      await loadDealTabExclusions(dealId);
    } else {
      showToast('Failed: ' + (data.error || 'Unknown'), 'error');
    }
  } catch (err) {
    showToast('Failed: ' + err.message, 'error');
  }
};

window.removeExclusion = async function(dealId, exclusionId) {
  try {
    await api(`/api/deals/${dealId}/exclusions/${exclusionId}`, 'DELETE');
    document.getElementById(`excl-row-${exclusionId}`)?.remove();
    showToast('Exclusion removed');
  } catch (err) {
    showToast('Failed: ' + err.message, 'error');
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// DEAL TEMPLATES & SEQUENCE TAB
// ─────────────────────────────────────────────────────────────────────────────

async function loadDealTemplatesTab(dealId) {
  const el = document.getElementById('deal-tab-templates');
  if (!el) return;
  el.innerHTML = '<div class="loading-placeholder">Loading templates…</div>';
  try {
    const [seqRes, tmplData] = await Promise.all([
      api(`/api/deals/${dealId}/sequence`).catch(() => ({ steps: null })),
      api(`/api/deals/${dealId}/templates`).catch(() => ({ deal: [], global: [] })),
    ]);

    const templates = tmplData.deal || [];
    const steps     = seqRes?.steps || [];
    const stepsJson = JSON.stringify(steps).replace(/'/g, '&#39;').replace(/"/g, '&quot;');

    const typeColors = { email: '#1f3a5f', linkedin_invite: '#1a3a2a', linkedin_dm: '#2a1f3a' };
    const typeLabels = { email: 'Email', linkedin_invite: 'LI Invite', linkedin_dm: 'LI DM' };

    el.innerHTML = `
      <div style="padding:20px">

        <!-- Sequence section -->
        <div style="margin-bottom:28px">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
            <div style="color:#e5e7eb;font-size:14px;font-weight:600">Outreach Sequence</div>
            <button onclick="editDealSequence('${dealId}','${stepsJson}')"
              style="padding:6px 14px;background:#1a1a1a;border:1px solid #2a2a2a;
                     color:#9ca3af;border-radius:6px;cursor:pointer;font-size:12px">
              Edit Sequence
            </button>
          </div>

          <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
            ${steps.map((s, i) => `
              ${i > 0 ? '<div style="color:#2a2a2a;font-size:18px;align-self:center">&#8594;</div>' : ''}
              <div style="padding:8px 14px;background:${typeColors[s.type] || '#1a1a1a'};
                          border-radius:6px;text-align:center;min-width:100px">
                <div style="color:#6b7280;font-size:10px;margin-bottom:2px">Day ${s.delay_days || 0}</div>
                <div style="color:#e5e7eb;font-size:11px;font-family:'DM Mono',monospace;font-weight:600">${esc(s.label || '')}</div>
                <div style="color:#9ca3af;font-size:10px;margin-top:2px">${typeLabels[s.type] || esc(s.type || '')}</div>
              </div>
            `).join('')}
            ${steps.length === 0 ? '<span style="color:#4b5563;font-size:13px">No sequence yet — click Edit Sequence</span>' : ''}
          </div>
        </div>

        <!-- Templates section -->
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
          <div style="color:#e5e7eb;font-size:14px;font-weight:600">Message Templates</div>
          <button onclick="window.showAddDealTemplateModal('${dealId}')"
            style="padding:6px 14px;background:#1a1a1a;border:1px solid #2a2a2a;
                   color:#9ca3af;border-radius:6px;cursor:pointer;font-size:12px">
            + Add Template
          </button>
        </div>

        <div style="display:flex;flex-direction:column;gap:12px">
          ${steps.filter(s => s.type !== 'linkedin_invite').map(step => {
            const stepLabel = step.label || step.type || '';
            const tmpl = templates.find(t => t.sequence_step === stepLabel && t.is_primary);

            if (!tmpl) {
              return `
                <div style="padding:16px;border:1px dashed #2a2a2a;border-radius:8px;
                            display:flex;justify-content:space-between;align-items:center">
                  <div>
                    <span style="font-family:'DM Mono',monospace;font-size:12px;color:#4a4a4a">${esc(stepLabel)}</span>
                    <span style="color:#374151;font-size:12px;margin-left:8px">No template assigned</span>
                  </div>
                  <button onclick="window.showAddDealTemplateModal('${dealId}','${esc(stepLabel)}','${esc(step.type || 'email')}')"
                    style="padding:5px 12px;background:#1a1a1a;border:1px solid #2a2a2a;
                           color:#9ca3af;border-radius:4px;cursor:pointer;font-size:11px">
                    + Add
                  </button>
                </div>`;
            }

            const isAI = tmpl.generated_by_ai;
            const tmplJson = JSON.stringify(tmpl).replace(/'/g, '&#39;').replace(/"/g, '&quot;');
            const vars = extractVariables(tmpl.body || '');
            return `
              <div style="padding:16px;background:#1a1a1a;border:1px solid #2a2a2a;border-radius:8px">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px">
                  <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
                    <span style="font-family:'DM Mono',monospace;font-size:12px;color:#9ca3af">${esc(stepLabel)}</span>
                    <span style="padding:2px 6px;border-radius:3px;font-size:10px;font-family:'DM Mono',monospace;
                                 background:rgba(212,168,71,0.1);color:#d4a847">PRIMARY</span>
                    ${isAI ? `<span style="padding:2px 6px;border-radius:3px;font-size:10px;font-family:'DM Mono',monospace;
                                 background:rgba(96,165,250,0.1);color:#60a5fa">AI GENERATED</span>` : ''}
                  </div>
                  <div style="display:flex;gap:6px">
                    <button onclick='window.previewDealTemplate(${tmplJson})'
                      style="padding:4px 10px;background:none;border:1px solid #2a2a2a;
                             color:#60a5fa;border-radius:4px;cursor:pointer;font-size:11px">Preview</button>
                    <button onclick='window.editDealTemplateModal("${dealId}","${tmpl.id}",${tmplJson})'
                      style="padding:4px 10px;background:none;border:1px solid #2a2a2a;
                             color:#9ca3af;border-radius:4px;cursor:pointer;font-size:11px">Edit</button>
                    <button onclick="window.deleteDealTemplate('${dealId}','${tmpl.id}')"
                      style="padding:4px 10px;background:none;border:1px solid #2a2a2a;
                             color:#6b7280;border-radius:4px;cursor:pointer;font-size:11px">Delete</button>
                  </div>
                </div>
                ${tmpl.subject_a ? `<div style="color:#6b7280;font-size:11px;font-family:'DM Mono',monospace;margin-bottom:4px">Subject A: ${esc(tmpl.subject_a)}</div>` : ''}
                ${tmpl.subject_b ? `<div style="color:#4a4a4a;font-size:11px;font-family:'DM Mono',monospace;margin-bottom:6px">Subject B: ${esc(tmpl.subject_b)}</div>` : ''}
                <div style="color:#9ca3af;font-size:12px;line-height:1.6;white-space:pre-wrap;font-family:'DM Mono',monospace">
                  ${esc((tmpl.body || '').slice(0, 160))}${(tmpl.body?.length || 0) > 160 ? '...' : ''}
                </div>
                ${vars.length > 0 ? `
                  <div style="margin-top:10px;display:flex;gap:4px;flex-wrap:wrap">
                    ${vars.map(v => `
                      <span style="padding:2px 6px;background:#1a1a2a;border:1px solid #2a2a3a;
                                   border-radius:3px;font-size:10px;font-family:'DM Mono',monospace;color:#818cf8">
                        {{${esc(v)}}}
                      </span>`).join('')}
                  </div>` : ''}
              </div>`;
          }).join('')}
        </div>
      </div>`;
  } catch (err) {
    el.innerHTML = `<div class="loading-placeholder text-red">Failed to load: ${esc(err.message)}</div>`;
  }
}

function extractVariables(body) {
  const matches = body.matchAll(/\{\{(\w+)\}\}/g);
  return [...new Set([...matches].map(m => m[1]))];
}

// ── Template / message preview ────────────────────────────────────────────────

/** Sample values shown in preview when no real contact data is available */
const PREVIEW_SAMPLE_VALUES = {
  firstName:        'James',
  lastName:         'Mitchell',
  fullName:         'James Mitchell',
  firm:             'Meridian Capital',
  company:          'Meridian Capital',
  title:            'Managing Partner',
  jobTitle:         'Managing Partner',
  pastInvestments:  'Octopus Energy, OVO Energy, Bulb',
  investmentThesis: 'Growth-stage energy transition and infrastructure plays in Europe',
  sectorFocus:      'Energy, CleanTech, Infrastructure',
  investorGeography:'UK, Europe',
  dealName:         'Project Electrify',
  dealBrief:        'Renewable energy infrastructure play targeting $1.6M raise',
  sector:           'Energy / CleanTech',
  targetAmount:     '$1.6M',
  keyMetrics:       '3.2x projected return, 18-month deployment',
  geography:        'UK & Europe',
  minCheque:        '$50K',
  maxCheque:        '$500K',
  investorProfile:  'Family offices, PE funds with energy exposure',
  comparableDeal:   'Octopus Energy Series B',
  deckUrl:          'https://deck.example.com',
  callLink:         'https://cal.com/dom',
  senderName:       'Dom',
  senderTitle:      'Principal',
};

function renderTemplateBody(body, values) {
  return (body || '').replace(/\{\{(\w+)\}\}/g, (_, key) => {
    const val = values[key];
    return val
      ? `<span style="background:rgba(96,165,250,.15);color:#93c5fd;border-radius:2px;padding:0 2px">${esc(val)}</span>`
      : `<span style="background:rgba(239,68,68,.12);color:#f87171;border-radius:2px;padding:0 2px">{{${esc(key)}}}</span>`;
  });
}

window.previewDealTemplate = function(tmpl, contactData) {
  const vals = { ...PREVIEW_SAMPLE_VALUES, ...(contactData || {}) };
  const isLinkedIn = tmpl.type === 'linkedin' || tmpl.type === 'linkedin_dm';
  const subjectA = tmpl.subject_a ? renderTemplateBody(tmpl.subject_a, vals) : null;
  const subjectB = tmpl.subject_b ? renderTemplateBody(tmpl.subject_b, vals) : null;
  const bodyHtml = renderTemplateBody(tmpl.body || '', vals).replace(/\n/g, '<br>');

  const modal = document.createElement('div');
  modal.id = 'tmpl-preview-modal';
  modal.style.cssText = 'position:fixed;inset:0;z-index:9999;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,.7)';
  modal.onclick = e => { if (e.target === modal) modal.remove(); };

  const isEditing = !!tmpl.id; // has an id = saved template, can switch to edit
  const dealId = tmpl.deal_id || window.__previewDealId;

  modal.innerHTML = isLinkedIn
    ? `<div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:12px;width:420px;max-height:85vh;overflow-y:auto;padding:0">
        <div style="padding:16px 20px;border-bottom:1px solid #2a2a2a;display:flex;justify-content:space-between;align-items:center">
          <div style="font-size:13px;font-weight:600;color:#e5e7eb">LinkedIn DM Preview</div>
          <div style="display:flex;gap:8px">
            ${isEditing ? `<button onclick="modal.remove();window.editDealTemplateModal('${dealId}','${tmpl.id}',${JSON.stringify(tmpl).replace(/"/g,'&quot;')})" style="font-size:11px;color:#9ca3af;background:none;border:1px solid #2a2a2a;border-radius:4px;padding:3px 10px;cursor:pointer">Edit</button>` : ''}
            <button onclick="document.getElementById('tmpl-preview-modal').remove()" style="font-size:16px;color:#6b7280;background:none;border:none;cursor:pointer">✕</button>
          </div>
        </div>
        <div style="padding:20px">
          <div style="background:#0a66c2;border-radius:12px 12px 12px 0;padding:14px 16px;max-width:320px;font-size:13px;line-height:1.6;color:#fff">
            ${bodyHtml}
          </div>
          <div style="margin-top:8px;font-size:10px;color:#4b5563">Sent as: Dom · via LinkedIn</div>
        </div>
        <div style="padding:12px 20px;border-top:1px solid #2a2a2a;font-size:11px;color:#4b5563">
          <span style="color:#60a5fa">■</span> Variable filled &nbsp; <span style="color:#f87171">■</span> Variable missing
        </div>
      </div>`
    : `<div style="background:#fff;border-radius:12px;width:620px;max-height:88vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.5)">
        <!-- Gmail-style chrome -->
        <div style="background:#f2f2f2;border-radius:12px 12px 0 0;padding:12px 16px;display:flex;align-items:center;gap:8px">
          <div style="width:12px;height:12px;border-radius:50%;background:#ff5f57"></div>
          <div style="width:12px;height:12px;border-radius:50%;background:#febc2e"></div>
          <div style="width:12px;height:12px;border-radius:50%;background:#28c840"></div>
          <div style="flex:1;text-align:center;font-size:12px;color:#666;font-family:system-ui">New Message</div>
          <div style="display:flex;gap:8px">
            ${isEditing ? `<button onclick="document.getElementById('tmpl-preview-modal').remove();window.editDealTemplateModal('${dealId}','${tmpl.id}',${JSON.stringify(tmpl).replace(/"/g,'&quot;')})" style="font-size:11px;color:#333;background:#e8e8e8;border:none;border-radius:4px;padding:3px 10px;cursor:pointer">Edit</button>` : ''}
            <button onclick="document.getElementById('tmpl-preview-modal').remove()" style="font-size:14px;color:#666;background:none;border:none;cursor:pointer">✕</button>
          </div>
        </div>
        <!-- Email headers -->
        <div style="padding:0 16px;border-bottom:1px solid #e0e0e0;font-family:'Google Sans',system-ui,sans-serif">
          <div style="display:flex;align-items:center;padding:10px 0;border-bottom:1px solid #f0f0f0;font-size:13px;color:#444">
            <span style="color:#888;width:40px;flex-shrink:0">From</span>
            <span>Dom &lt;dom@roco.ai&gt;</span>
          </div>
          <div style="display:flex;align-items:center;padding:10px 0;border-bottom:1px solid #f0f0f0;font-size:13px;color:#444">
            <span style="color:#888;width:40px;flex-shrink:0">To</span>
            <span>${esc(vals.fullName)} &lt;james@meridiancapital.com&gt;</span>
          </div>
          ${subjectA ? `
          <div style="padding:8px 0">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
              <span style="font-size:11px;color:#888;width:64px;flex-shrink:0">Subject A</span>
              <span style="font-size:14px;font-weight:500;color:#202124">${subjectA}</span>
            </div>
            ${subjectB ? `<div style="display:flex;align-items:center;gap:8px">
              <span style="font-size:11px;color:#888;width:64px;flex-shrink:0">Subject B</span>
              <span style="font-size:14px;color:#444">${subjectB}</span>
            </div>` : ''}
          </div>` : ''}
        </div>
        <!-- Body -->
        <div style="padding:24px 20px 32px;font-family:'Google Sans',system-ui,sans-serif;font-size:14px;line-height:1.7;color:#202124;min-height:200px">
          ${bodyHtml}
        </div>
        <!-- Legend -->
        <div style="padding:10px 20px;border-top:1px solid #e0e0e0;background:#fafafa;border-radius:0 0 12px 12px;font-size:11px;color:#888;display:flex;gap:16px">
          <span><span style="color:#60a5fa">■</span> Variable filled with sample data</span>
          <span><span style="color:#f87171">■</span> Variable missing / check spelling</span>
        </div>
      </div>`;

  document.body.appendChild(modal);
};

window.showAddDealTemplateModal = function(dealId, sequenceStep = '', stepType = 'email') {
  showDealTemplateModal({
    dealId, templateId: null, sequenceStep, type: stepType,
    name: '', subject_a: '', subject_b: '', body: '', is_primary: true,
  });
};

window.editDealTemplateModal = function(dealId, templateId, tmpl) {
  showDealTemplateModal({
    dealId, templateId,
    sequenceStep: tmpl.sequence_step || '',
    type: tmpl.type || 'email',
    name: tmpl.name || '',
    subject_a: tmpl.subject_a || '',
    subject_b: tmpl.subject_b || '',
    body: tmpl.body || '',
    is_primary: tmpl.is_primary,
  });
};

function showDealTemplateModal({ dealId, templateId, sequenceStep, type, name, subject_a, subject_b, body, is_primary }) {
  const isEmail = (type || 'email') === 'email';
  const standardVars = [
    'firstName', 'lastName', 'fullName', 'firm', 'title',
    'dealName', 'dealType', 'sector', 'ebitda', 'ev', 'equity',
    'investorFocus', 'firmLine', 'contactType', 'senderName',
  ];

  const existing = document.getElementById('deal-tmpl-modal');
  if (existing) existing.remove();

  const modal = document.createElement('div');
  modal.id = 'deal-tmpl-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.8);z-index:1000;display:flex;align-items:center;justify-content:center';

  modal.innerHTML = `
    <div style="background:#111;border:1px solid #2a2a2a;border-radius:8px;
                padding:28px;width:640px;max-height:85vh;overflow-y:auto">
      <h3 style="color:#e5e7eb;margin:0 0 20px;font-family:'Cormorant Garamond',serif;font-size:20px">
        ${templateId ? 'Edit' : 'Add'} Template
        ${sequenceStep ? `<span style="font-size:14px;color:#6b7280;font-family:'DM Mono',monospace;margin-left:8px">${esc(sequenceStep)}</span>` : ''}
      </h3>

      <div style="display:grid;grid-template-columns:2fr 1fr;gap:10px;margin-bottom:14px">
        <div>
          <label style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:5px">Name</label>
          <input id="dtm-name" value="${esc(name || '')}"
            style="width:100%;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;
                   color:#e5e7eb;border-radius:6px;box-sizing:border-box;font-size:13px">
        </div>
        <div>
          <label style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:5px">Type</label>
          <select id="dtm-type" onchange="window.toggleDtmSubjects()"
            style="width:100%;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;
                   color:#e5e7eb;border-radius:6px;font-size:13px">
            <option value="email" ${(type || 'email') === 'email' ? 'selected' : ''}>Email</option>
            <option value="linkedin" ${type === 'linkedin' ? 'selected' : ''}>LinkedIn</option>
          </select>
        </div>
      </div>

      <div id="dtm-subject-section" style="display:${isEmail ? 'block' : 'none'}">
        <div style="margin-bottom:10px">
          <label style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:5px">Subject A</label>
          <input id="dtm-subja" value="${esc(subject_a || '')}"
            style="width:100%;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;
                   color:#e5e7eb;border-radius:6px;box-sizing:border-box;font-size:13px">
        </div>
        <div style="margin-bottom:14px">
          <label style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:5px">
            Subject B
            <span style="color:#374151;font-size:10px;text-transform:none;letter-spacing:0;margin-left:4px">A/B test — optional</span>
          </label>
          <input id="dtm-subjb" value="${esc(subject_b || '')}"
            style="width:100%;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;
                   color:#e5e7eb;border-radius:6px;box-sizing:border-box;font-size:13px">
        </div>
      </div>

      <div style="margin-bottom:8px">
        <label style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:8px">
          Variables — click to insert, or type your own {{variable}}
        </label>
        <div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:10px">
          ${standardVars.map(v => `
            <button onclick="window.insertDtmVar('{{${v}}}')"
              style="padding:3px 8px;background:#1a1a2a;border:1px solid #2a2a3a;
                     color:#818cf8;border-radius:3px;cursor:pointer;font-size:11px;
                     font-family:'DM Mono',monospace">
              {{${v}}}
            </button>`).join('')}
        </div>
      </div>

      <div style="margin-bottom:20px">
        <label style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:5px">Body</label>
        <textarea id="dtm-body" rows="10"
          placeholder="Write your message. Use {{firstName}}, {{dealName}}, or any {{customVariable}} you like."
          style="width:100%;padding:10px 12px;background:#1a1a1a;border:1px solid #2a2a2a;
                 color:#e5e7eb;border-radius:6px;box-sizing:border-box;
                 font-family:'DM Mono',monospace;font-size:12px;resize:vertical;line-height:1.6">${esc(body || '')}</textarea>
      </div>

      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button onclick="document.getElementById('deal-tmpl-modal').remove()"
          style="padding:8px 16px;background:#1a1a1a;border:1px solid #2a2a2a;
                 color:#6b7280;border-radius:6px;cursor:pointer">Cancel</button>
        <button onclick="window.saveDealTemplateFromModal('${dealId}','${templateId || ''}','${esc(sequenceStep || '')}',${!!is_primary})"
          style="padding:8px 20px;background:#d4a847;border:none;color:#000;
                 border-radius:6px;cursor:pointer;font-weight:600">Save Template</button>
      </div>
    </div>`;

  document.body.appendChild(modal);
  modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
}

window.toggleDtmSubjects = function() {
  const type = document.getElementById('dtm-type')?.value;
  const section = document.getElementById('dtm-subject-section');
  if (section) section.style.display = type === 'linkedin' ? 'none' : 'block';
};

window.insertDtmVar = function(varString) {
  const ta = document.getElementById('dtm-body');
  if (!ta) return;
  const start = ta.selectionStart;
  const end   = ta.selectionEnd;
  ta.value = ta.value.slice(0, start) + varString + ta.value.slice(end);
  ta.selectionStart = ta.selectionEnd = start + varString.length;
  ta.focus();
};

window.saveDealTemplateFromModal = async function(dealId, templateId, sequenceStep, isPrimary) {
  const name     = document.getElementById('dtm-name')?.value?.trim();
  const type     = document.getElementById('dtm-type')?.value;
  const subjectA = document.getElementById('dtm-subja')?.value?.trim();
  const subjectB = document.getElementById('dtm-subjb')?.value?.trim();
  const body     = document.getElementById('dtm-body')?.value?.trim();

  if (!body) { showToast('Body is required', 'error'); return; }
  if (body.includes('\u2014') || (subjectA || '').includes('\u2014')) {
    showToast('Remove em dashes (\u2014) from the template', 'error'); return;
  }

  const method = templateId ? 'PUT' : 'POST';
  const url    = templateId
    ? `/api/deals/${dealId}/templates/${templateId}`
    : `/api/deals/${dealId}/templates`;

  try {
    const data = await api(url, method, {
      name: name || sequenceStep,
      type,
      sequence_step: sequenceStep || null,
      subject_a: type === 'email' ? (subjectA || null) : null,
      subject_b: type === 'email' ? (subjectB || null) : null,
      body,
      is_primary: isPrimary,
      ab_test_enabled: !!(subjectA && subjectB),
    });
    if (data.success) {
      document.getElementById('deal-tmpl-modal')?.remove();
      showToast('Template saved');
      loadDealTemplatesTab(dealId);
    } else {
      showToast('Save failed: ' + (data.error || 'Unknown'), 'error');
    }
  } catch (err) {
    showToast('Save failed: ' + err.message, 'error');
  }
};

window.deleteDealTemplate = async function(dealId, templateId) {
  if (!confirm('Delete this template?')) return;
  try {
    await api(`/api/deals/${dealId}/templates/${templateId}`, 'DELETE');
    showToast('Template deleted');
    loadDealTemplatesTab(dealId);
  } catch (err) {
    showToast('Failed: ' + err.message, 'error');
  }
};

window.editDealSequence = function(dealId, stepsJsonStr) {
  let steps = [];
  try { steps = JSON.parse(stepsJsonStr); } catch {}
  window._editingDealSequenceId = dealId;
  if (typeof renderSequenceEditorModal === 'function') {
    renderSequenceEditorModal(steps, { dealId });
  } else {
    showToast('Sequence editor not available', 'error');
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// CAMPAIGN REVIEW TAB
// ─────────────────────────────────────────────────────────────────────────────

async function loadCampaignReviewTab(dealId) {
  const el = document.getElementById('deal-tab-campaign');
  if (!el) return;
  el.innerHTML = '<div class="loading-placeholder">Loading campaign…</div>';
  try {
    const [batch, allBatches] = await Promise.all([
      api(`/api/deals/${dealId}/campaign/current`),
      api(`/api/deals/${dealId}/batches`).catch(() => []),
    ]);
    if (!batch) {
      el.innerHTML = `<div style="padding:32px;text-align:center;color:var(--text-dim)">
        <div style="font-size:14px;margin-bottom:8px">No active campaign batch</div>
        <div style="font-size:12px">The orchestrator will create a batch automatically when research begins.</div>
      </div>`;
      return;
    }
    // Show "next batch ready" banner if a ready batch exists alongside an approved batch
    const readyBatch = (allBatches || []).find(b => b.status === 'ready');
    const nextBanner = readyBatch && batch.status === 'approved' ? `
      <div style="background:rgba(167,139,250,.08);border:1px solid rgba(167,139,250,.25);border-radius:8px;padding:12px 16px;margin-bottom:16px;display:flex;align-items:center;gap:10px">
        <span style="font-size:16px">✅</span>
        <div style="flex:1">
          <div style="font-size:13px;font-weight:600;color:#a78bfa">Batch #${readyBatch.batch_number} ready</div>
          <div style="font-size:12px;color:var(--text-dim)">Pre-built and waiting. Close this batch to queue it for review.</div>
        </div>
      </div>` : '';
    el.innerHTML = nextBanner + renderCampaignBatch(batch, dealId);
  } catch (e) {
    el.innerHTML = `<div class="loading-placeholder text-red">Failed to load: ${esc(e.message)}</div>`;
  }
}

function renderCampaignBatch(batch, dealId) {
  const statusColors = {
    researching: 'color:#60a5fa',
    ready: 'color:#a78bfa',
    pending_approval: 'color:#eab308',
    approved: 'color:#22c55e',
    rejected: 'color:#ef4444',
    completed: 'color:var(--text-dim)',
  };
  const statusLabel = {
    researching: 'Researching',
    ready: 'Ready (Queued)',
    pending_approval: 'Review Required',
    approved: 'Approved — Outreach Active',
    rejected: 'Rejected',
    completed: 'Completed',
  };
  const firms = batch.firms || [];
  const rankedFirms = batch.ranked_firms || firms.length;
  const targetFirms = batch.target_firms || 20;
  const progressPct = Math.min(100, Math.round((rankedFirms / targetFirms) * 100));

  const firmCards = firms.map(f => {
    const fos = f.firm_outreach_state || {};
    const firmData = fos.firms || {};
    const firmName = esc(f.firm_name || firmData.name || 'Unknown');
    const sector = esc(firmData.sector || '');
    const location = esc(firmData.hq_location || '');
    const score = fos.rank_score ? Math.round(fos.rank_score) : '—';
    const justification = esc(f.justification || '');
    const contacts = f.contacts || [];
    const cardId = `cbf-${f.id}`;
    const detailId = `cbf-detail-${f.id}`;

    const contactRows = contacts.map(c => {
      const hasEmail = !!c.email;
      const hasLi = !!c.linkedin_url;
      const researched = c.person_researched;
      const typeLabel = c.contact_type === 'individual' ? '👤' : '🏢';
      const channelIcons = [
        hasEmail ? `<span title="Email" style="font-size:11px">📧</span>` : '',
        hasLi ? `<span title="LinkedIn" style="font-size:11px">💼</span>` : '',
      ].filter(Boolean).join(' ');
      return `<div style="padding:10px 0;border-bottom:1px solid var(--border-color)">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px">
          <div>
            <span style="font-size:12px;font-weight:600;color:var(--text-primary)">${typeLabel} ${esc(c.name || '—')}</span>
            ${c.job_title ? `<span style="font-size:11px;color:var(--text-dim);margin-left:6px">${esc(c.job_title)}</span>` : ''}
          </div>
          <div style="display:flex;align-items:center;gap:6px">
            ${channelIcons}
            ${c.investor_score ? `<span style="font-size:11px;font-family:var(--font-mono);color:var(--text-dim)">${Math.round(c.investor_score)}</span>` : ''}
            ${researched ? `<span style="font-size:10px;color:#22c55e">✓ researched</span>` : `<span style="font-size:10px;color:#eab308">researching…</span>`}
          </div>
        </div>
        ${c.past_investments ? `<div style="font-size:11px;color:var(--text-dim);margin-bottom:3px"><span style="color:var(--text-secondary);font-weight:500">Past investments:</span> ${esc(c.past_investments)}</div>` : ''}
        ${c.investment_thesis ? `<div style="font-size:11px;color:var(--text-dim);margin-bottom:3px"><span style="color:var(--text-secondary);font-weight:500">Thesis:</span> ${esc(c.investment_thesis)}</div>` : ''}
        ${c.sector_focus ? `<div style="font-size:11px;color:var(--text-dim);margin-bottom:3px"><span style="color:var(--text-secondary);font-weight:500">Sectors:</span> ${esc(c.sector_focus)}</div>` : ''}
        ${c.geography ? `<div style="font-size:11px;color:var(--text-dim)"><span style="color:var(--text-secondary);font-weight:500">Geography:</span> ${esc(c.geography)}</div>` : ''}
      </div>`;
    }).join('');

    return `<div class="card" style="padding:0;margin-bottom:10px;overflow:hidden" id="${cardId}">
      <div style="padding:14px 16px;display:flex;justify-content:space-between;align-items:flex-start;cursor:pointer" onclick="toggleFirmDetail('${detailId}')">
        <div style="flex:1">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:3px">
            <span style="font-size:13px;font-weight:600;color:var(--text-primary)">${firmName}</span>
            ${contacts.length ? `<span style="font-size:10px;background:rgba(96,165,250,.12);color:#60a5fa;padding:1px 6px;border-radius:10px">${contacts.length} contact${contacts.length !== 1 ? 's' : ''}</span>` : ''}
          </div>
          ${sector ? `<div style="font-size:11px;color:var(--text-dim);margin-bottom:4px">${sector}${location ? ' · ' + location : ''}</div>` : ''}
          ${justification ? `<div style="font-size:12px;color:var(--text-secondary);font-style:italic">"${justification}"</div>` : ''}
        </div>
        <div style="display:flex;align-items:center;gap:10px;margin-left:16px;flex-shrink:0">
          ${score !== '—' ? `<div style="font-size:18px;font-family:var(--font-mono);font-weight:700;color:var(--text-primary)">${score}</div>` : ''}
          ${batch.status === 'pending_approval' ? `<button class="btn btn-ghost btn-sm" style="color:#ef4444" onclick="event.stopPropagation();removeFirmFromBatch('${f.id}','${dealId}')">Remove</button>` : ''}
          <span style="font-size:10px;color:var(--text-dim)">▼</span>
        </div>
      </div>
      <div id="${detailId}" style="display:none;border-top:1px solid var(--border-color);padding:0 16px 12px">
        ${contacts.length ? contactRows : `<div style="padding:12px 0;font-size:12px;color:var(--text-dim)">No contacts found yet — research in progress.</div>`}
      </div>
    </div>`;
  }).join('');

  const approveBtn = batch.status === 'pending_approval' ? `
    <button class="btn btn-primary" onclick="approveCampaignBatch('${batch.id}','${dealId}')" style="min-width:140px">Approve Campaign</button>
    <button class="btn btn-ghost btn-sm" style="color:#ef4444" onclick="rejectCampaignBatch('${batch.id}','${dealId}')">Reject</button>
  ` : batch.status === 'approved' ? `
    <button class="btn btn-ghost" onclick="closeBatchAndNext('${dealId}')" style="border-color:#eab308;color:#eab308">Close Batch &amp; Next</button>
  ` : '';

  const researchProgress = batch.status === 'researching' ? `
    <div style="margin-bottom:20px">
      <div style="display:flex;justify-content:space-between;font-size:12px;color:var(--text-dim);margin-bottom:6px">
        <span>Research progress</span><span>${rankedFirms} / ${targetFirms} firms</span>
      </div>
      <div style="height:4px;background:var(--border-color);border-radius:2px">
        <div style="height:4px;background:#60a5fa;border-radius:2px;width:${progressPct}%;transition:width .3s"></div>
      </div>
      <div style="font-size:11px;color:var(--text-dim);margin-top:6px">Outreach is gated until ${targetFirms} firms are ranked and you approve the campaign.</div>
    </div>
  ` : '';

  const timeline = renderProjectedTimeline(batch);

  return `
    <div style="padding:0 0 24px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
        <div>
          <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px">Batch #${batch.batch_number}</div>
          <div style="font-size:20px;font-weight:600;${statusColors[batch.status] || ''}">${statusLabel[batch.status] || batch.status}</div>
        </div>
        <div style="display:flex;gap:8px;align-items:center">
          ${approveBtn}
        </div>
      </div>

      ${researchProgress}
      ${timeline}

      <div style="margin-top:20px">
        <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em;margin-bottom:12px">
          Target Firms (${firms.length})
        </div>
        ${firms.length ? firmCards : `<div style="color:var(--text-dim);font-size:13px">No firms added yet — research in progress.</div>`}
      </div>
    </div>
  `;
}

function renderProjectedTimeline(batch) {
  if (batch.status !== 'pending_approval' && batch.status !== 'approved') return '';
  const firmCount = batch.ranked_firms || (batch.firms || []).length || 20;
  // Project: 3 contacts per firm avg, outreach over ~2 weeks
  const estimatedContacts = Math.round(firmCount * 2.5);
  const estimatedEmails = Math.round(estimatedContacts * 0.6);
  const estimatedLI = estimatedContacts - estimatedEmails;
  return `
    <div class="card" style="padding:16px;margin-bottom:20px;background:rgba(96,165,250,.04);border-color:rgba(96,165,250,.15)">
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em;margin-bottom:12px">Projected Outreach</div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px">
        <div><div style="font-size:22px;font-family:var(--font-mono);font-weight:700">${firmCount}</div><div style="font-size:11px;color:var(--text-dim)">Firms</div></div>
        <div><div style="font-size:22px;font-family:var(--font-mono);font-weight:700">~${estimatedEmails}</div><div style="font-size:11px;color:var(--text-dim)">Emails</div></div>
        <div><div style="font-size:22px;font-family:var(--font-mono);font-weight:700">~${estimatedLI}</div><div style="font-size:11px;color:var(--text-dim)">LinkedIn</div></div>
      </div>
      <div style="font-size:11px;color:var(--text-dim);margin-top:10px">Outreach runs 6–8am and 8–11pm EST weekdays only.</div>
    </div>
  `;
}

window.toggleFirmDetail = function(detailId) {
  const el = document.getElementById(detailId);
  if (!el) return;
  const isOpen = el.style.display !== 'none';
  el.style.display = isOpen ? 'none' : 'block';
  // flip the arrow on the parent card header
  const arrow = el.previousElementSibling?.querySelector('span[style*="▼"], span[style*="▲"]');
  if (arrow) arrow.textContent = isOpen ? '▼' : '▲';
};

window.approveCampaignBatch = async function(batchId, dealId) {
  try {
    await api(`/api/deals/${dealId}/campaign/${batchId}/approve`, { method: 'POST' });
    showToast('Campaign approved. Outreach begins next EST window.', 'success');
    await loadCampaignReviewTab(dealId);
    await loadDeals(); // refresh deal cards for badge update
  } catch (e) {
    showToast('Failed to approve: ' + e.message, 'error');
  }
};

window.rejectCampaignBatch = async function(batchId, dealId) {
  if (!confirm('Reject this campaign batch? The orchestrator will continue researching and submit a new batch when ready.')) return;
  try {
    await api(`/api/deals/${dealId}/campaign/${batchId}/reject`, { method: 'POST', body: JSON.stringify({ reason: 'Manual rejection' }) });
    showToast('Batch rejected. Research continues.', 'success');
    await loadCampaignReviewTab(dealId);
    await loadDeals();
  } catch (e) {
    showToast('Failed: ' + e.message, 'error');
  }
};

window.closeBatchAndNext = async function(dealId) {
  if (!confirm('Close this batch? Outreach to remaining uncontacted firms in this batch will stop. The next pre-built batch (or the one in progress) will be queued for review.')) return;
  try {
    const result = await api(`/api/deals/${dealId}/campaign/close`, { method: 'POST' });
    if (result.promoted) {
      showToast(`Batch #${result.closed} closed. Batch #${result.promoted} is now up for review.`, 'success');
    } else if (result.building) {
      showToast(`Batch #${result.closed} closed. Next batch #${result.building} is still building (${result.buildingProgress || '…'}). Check back soon.`, 'info');
    } else {
      showToast('Batch closed. Research will build the next batch automatically.', 'success');
    }
    await loadCampaignReviewTab(dealId);
    await loadDeals();
  } catch (e) {
    showToast('Failed: ' + e.message, 'error');
  }
};

window.removeFirmFromBatch = async function(firmId, dealId) {
  if (!confirm('Remove this firm from the campaign batch?')) return;
  try {
    await api(`/api/campaign-firms/${firmId}`, { method: 'DELETE' });
    document.getElementById(`cbf-${firmId}`)?.remove();
    showToast('Firm removed.', 'success');
  } catch (e) {
    showToast('Failed: ' + e.message, 'error');
  }
};

// ─────────────────────────────────────────────────────────────────────────────

async function loadDealTabBatches(id) {
  const el = document.getElementById('deal-tab-batches');
  if (!el) return;
  el.innerHTML = '<div class="loading-placeholder">Loading batches…</div>';
  try {
    const data = await api(`/api/deals/${id}/batches`);
    const batches = Array.isArray(data) ? data : (data.batches || []);
    renderBatchPipeline(batches, el);
  } catch { el.innerHTML = '<div class="loading-placeholder text-red">Failed to load.</div>'; }
}

async function loadDealTabSettings(id) {
  const el = document.getElementById('deal-tab-settings');
  if (!el) return;
  el.innerHTML = '<div class="loading-placeholder">Loading settings…</div>';
  let deal = {};
  try { deal = await api(`/api/deals/${id}`); } catch {}

  const tz = ['Europe/London','America/New_York','America/Los_Angeles','America/Chicago',
               'Asia/Dubai','Asia/Singapore','Asia/Tokyo','Australia/Sydney',
               'Europe/Paris','Europe/Berlin'];

  el.innerHTML = `
    <div style="max-width:720px">

      <h3 style="font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin:0 0 12px">Deal Info</h3>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px">

        <div class="form-group">
          <label class="form-label">Deal Name</label>
          <input type="text" class="form-input" id="ds-name" value="${esc(deal.name || '')}" />
        </div>
        <div class="form-group">
          <label class="form-label">Deck URL</label>
          <input type="url" class="form-input" id="ds-deck" value="${esc(deal.deck_url || deal.deckUrl || '')}" />
        </div>

        <div class="form-group">
          <label class="form-label">Sector</label>
          <input type="text" class="form-input" id="ds-sector" value="${esc(deal.sector || '')}" placeholder="e.g. AI / SaaS" />
        </div>
        <div class="form-group">
          <label class="form-label">Geography</label>
          <input type="text" class="form-input" id="ds-geography" value="${esc(deal.geography || '')}" placeholder="e.g. UK, Europe" />
        </div>

        <div class="form-group">
          <label class="form-label">Raise Type / Stage</label>
          <input type="text" class="form-input" id="ds-raise-type" value="${esc(deal.raise_type || '')}" placeholder="e.g. Pre-Seed, Seed" />
        </div>
        <div class="form-group">
          <label class="form-label">Currency</label>
          <select class="form-input" id="ds-currency" onchange="updateDealCurrencyLabels(this.value)">
            ${['USD','GBP','EUR','CAD','AUD','CHF','SGD'].map(c => `<option value="${c}" ${(deal.currency || 'USD') === c ? 'selected' : ''}>${CURRENCY_SYMBOLS[c] || c} ${c}</option>`).join('')}
          </select>
        </div>
        <div class="form-group">
          <label class="form-label" id="ds-target-label">Target Amount (${CURRENCY_SYMBOLS[deal.currency || 'USD'] || '$'})</label>
          <input type="number" class="form-input" id="ds-target" value="${deal.target_amount || ''}" placeholder="e.g. 1600000" />
        </div>

        <div class="form-group" style="grid-column:1/-1">
          <label class="form-label">Description</label>
          <textarea class="form-input" id="ds-description" rows="3" placeholder="Brief deal description…">${esc(deal.description || '')}</textarea>
        </div>

        <div class="form-group" style="grid-column:1/-1">
          <label class="form-label">Key Metrics / USP</label>
          <textarea class="form-input" id="ds-key-metrics" rows="2" placeholder="ARR, growth rate, margins…">${esc(deal.key_metrics || '')}</textarea>
        </div>

        <div class="form-group" style="grid-column:1/-1">
          <label class="form-label">Investor Profile</label>
          <textarea class="form-input" id="ds-investor-profile" rows="2" placeholder="Target investor type, cheque size…">${esc(deal.investor_profile || '')}</textarea>
        </div>

      </div>

      <h3 style="font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin:0 0 4px">Sending Windows</h3>
      <div style="font-size:11px;color:var(--text-dim);margin-bottom:12px">Email + LinkedIn DMs send in these two daily windows (weekdays only). Connections send anytime.</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:8px">
        <div class="form-group">
          <label class="form-label">Morning window — start</label>
          <input type="time" class="form-input" id="ds-send-from" value="${esc(deal.send_from || '06:00')}" />
        </div>
        <div class="form-group">
          <label class="form-label">Morning window — end</label>
          <input type="time" class="form-input" id="ds-send-until" value="${esc(deal.send_until || '08:00')}" />
        </div>
        <div class="form-group">
          <label class="form-label">Evening window — start</label>
          <input type="time" class="form-input" id="ds-li-dm-from" value="${esc(deal.li_dm_from || '20:00')}" />
        </div>
        <div class="form-group">
          <label class="form-label">Evening window — end</label>
          <input type="time" class="form-input" id="ds-li-dm-until" value="${esc(deal.li_dm_until || '23:00')}" />
        </div>
      </div>

      <h3 style="font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin:0 0 4px;margin-top:8px">LinkedIn Connections</h3>
      <div style="font-size:11px;color:var(--text-dim);margin-bottom:12px">Leave blank to send anytime (recommended).</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:8px">
        <div class="form-group">
          <label class="form-label">Connections: from (optional)</label>
          <input type="time" class="form-input" id="ds-li-connect-from" value="${esc(deal.li_connect_from || '')}" placeholder="anytime" />
        </div>
        <div class="form-group">
          <label class="form-label">Connections: until (optional)</label>
          <input type="time" class="form-input" id="ds-li-connect-until" value="${esc(deal.li_connect_until || '')}" placeholder="anytime" />
        </div>
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px">
        <div class="form-group">
          <label class="form-label">Timezone</label>
          <select class="form-input" id="ds-timezone">
            ${tz.map(t => `<option value="${t}" ${deal.timezone === t ? 'selected' : ''}>${t}</option>`).join('')}
          </select>
        </div>
        <div class="form-group">
          <label class="form-label">Active Days</label>
          ${renderDayPickerHTML('ds-active-days', deal.active_days || 'Mon,Tue,Wed,Thu,Fri')}
        </div>
      </div>

      <h3 style="font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin:0 0 12px">Rate Limits</h3>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px">

        <div class="form-group">
          <label class="form-label">Max Emails / Day</label>
          <input type="number" class="form-input" id="ds-max-emails" value="${deal.max_emails_per_day || 20}" />
        </div>
        <div class="form-group">
          <label class="form-label">LinkedIn Invites / Day</label>
          <input type="number" class="form-input" id="ds-li-daily" value="${deal.linkedin_daily_limit || 28}" />
        </div>

        <div class="form-group">
          <label class="form-label">Max Contacts Per Firm</label>
          <input type="number" class="form-input" id="ds-max-per-firm" value="${deal.max_contacts_per_firm || 2}" />
        </div>
        <div class="form-group">
          <label class="form-label">Min Score to Contact</label>
          <input type="number" class="form-input" id="ds-min-score" value="${deal.min_investor_score || 65}" />
        </div>

        <div class="form-group">
          <label class="form-label">Days Before LinkedIn Follow-up</label>
          <input type="number" class="form-input" id="ds-followup-li" value="${deal.followup_days_li || 5}" />
        </div>
        <div class="form-group">
          <label class="form-label">Days Before Email Follow-up</label>
          <input type="number" class="form-input" id="ds-followup-email" value="${deal.followup_days_email || 7}" />
        </div>

        <div class="form-group">
          <label class="form-label">Max Active Pipeline Size</label>
          <input type="number" class="form-input" id="ds-pipeline-max" value="${deal.pipeline_max || 100}" />
        </div>
        <div class="form-group">
          <label class="form-label">Refill When Below</label>
          <input type="number" class="form-input" id="ds-pipeline-refill" value="${deal.pipeline_refill_threshold || 30}" />
        </div>

      </div>

    </div>

      <h3 style="font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin:0 0 12px">Capital Committed</h3>
      <div style="display:flex;gap:12px;align-items:flex-end;margin-bottom:24px">
        <div class="form-group" style="flex:1;margin:0">
          <label class="form-label" id="ds-capital-label">Amount Committed (${CURRENCY_SYMBOLS[deal.currency || 'USD'] || '$'})</label>
          <input type="number" class="form-input" id="ds-capital-input" value="${deal.committed_amount || ''}" placeholder="e.g. 250000" />
        </div>
        <button class="btn btn-gold" style="white-space:nowrap" onclick="updateCapital('${id}')">Update Capital</button>
      </div>

      <h3 style="font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin:0 0 12px">Deal Assets</h3>
      <div id="deal-assets-list-${id}" style="margin-bottom:12px">
        <div class="loading-placeholder" style="font-size:12px">Loading assets…</div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr auto auto;gap:8px;align-items:end;margin-bottom:24px">
        <div>
          <label class="form-label">Name</label>
          <input type="text" class="form-input" id="asset-name-${id}" placeholder="e.g. Book a Call" />
        </div>
        <div>
          <label class="form-label">URL</label>
          <input type="text" class="form-input" id="asset-url-${id}" placeholder="https://…" />
        </div>
        <div>
          <label class="form-label">Type</label>
          <select class="form-input" id="asset-type-${id}">
            <option value="calendly">Calendly</option>
            <option value="deck">Deck</option>
            <option value="video">Video</option>
            <option value="image">Image</option>
            <option value="link">Link</option>
            <option value="other">Other</option>
          </select>
        </div>
        <button class="btn btn-gold" onclick="addDealAsset('${id}')">Add</button>
      </div>

    <div style="margin-top:20px">
      <button class="btn btn-gold" onclick="saveDealSettings('${id}')">Save Settings</button>
    </div>
  `;
  loadDealAssets(id);
}

async function loadDealAssets(dealId) {
  const el = document.getElementById(`deal-assets-list-${dealId}`);
  if (!el) return;
  try {
    const assets = await api(`/api/deals/${dealId}/assets`);
    if (!assets?.length) {
      el.innerHTML = '<div style="font-size:12px;color:var(--text-muted);padding:8px 0">No assets added yet.</div>';
      return;
    }
    const typeLabel = { calendly: 'Calendly', deck: 'Deck', video: 'Video', image: 'Image', link: 'Link', other: 'Other' };
    el.innerHTML = assets.map(a => `
      <div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--border)">
        <span style="font-size:11px;background:var(--bg-secondary);border-radius:3px;padding:2px 6px;color:var(--text-muted)">${typeLabel[a.asset_type] || a.asset_type}</span>
        <span style="flex:1;font-size:13px">${esc(a.name)}</span>
        <a href="${esc(a.url)}" target="_blank" style="font-size:11px;color:var(--gold);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(a.url)}</a>
        <button class="btn" style="font-size:11px;padding:2px 8px;opacity:.6" onclick="deleteDealAsset('${dealId}','${a.id}')">Remove</button>
      </div>`).join('');
  } catch (e) {
    el.innerHTML = '<div style="font-size:12px;color:var(--text-dim)">Could not load assets.</div>';
  }
}

async function addDealAsset(dealId) {
  const name = document.getElementById(`asset-name-${dealId}`)?.value?.trim();
  const url  = document.getElementById(`asset-url-${dealId}`)?.value?.trim();
  const type = document.getElementById(`asset-type-${dealId}`)?.value;
  if (!name || !url) return alert('Name and URL are required.');
  try {
    await api(`/api/deals/${dealId}/assets`, 'POST', { name, url, asset_type: type });
    document.getElementById(`asset-name-${dealId}`).value = '';
    document.getElementById(`asset-url-${dealId}`).value = '';
    await loadDealAssets(dealId);
  } catch (e) { alert(`Failed to add asset: ${e.message}`); }
}

async function deleteDealAsset(dealId, assetId) {
  if (!confirm('Remove this asset?')) return;
  try {
    await api(`/api/deals/${dealId}/assets/${assetId}`, 'DELETE');
    await loadDealAssets(dealId);
  } catch (e) { alert(`Failed to remove asset: ${e.message}`); }
}

async function updateCapital(dealId) {
  const amount = document.getElementById(`ds-capital-input`)?.value;
  if (!amount) return showToast('Enter an amount', 'error');
  try {
    await api(`/api/deals/${dealId}/capital`, 'POST', { amount: Number(amount) });
    const currency = document.getElementById('ds-currency')?.value || window.__activeDealCurrency || 'USD';
    showToast(`Capital updated — ${formatMoney(amount, currency)}`);
    await refreshStats();
  } catch (e) { showToast(`Failed: ${e.message}`, 'error'); }
}

async function saveDealSettings(id) {
  const targetVal    = document.getElementById('ds-target')?.value;
  const committedVal = document.getElementById('ds-committed')?.value;
  const payload = {
    name:                  document.getElementById('ds-name')?.value?.trim(),
    deck_url:              document.getElementById('ds-deck')?.value?.trim(),
    sector:                document.getElementById('ds-sector')?.value?.trim(),
    geography:             document.getElementById('ds-geography')?.value?.trim(),
    raise_type:            document.getElementById('ds-raise-type')?.value?.trim(),
    description:           document.getElementById('ds-description')?.value?.trim(),
    key_metrics:           document.getElementById('ds-key-metrics')?.value?.trim(),
    investor_profile:      document.getElementById('ds-investor-profile')?.value?.trim(),
    target_amount:         targetVal    ? Number(targetVal)    : null,
    committed_amount:      committedVal ? Number(committedVal) : null,
    currency:              document.getElementById('ds-currency')?.value || 'USD',
    send_from:             document.getElementById('ds-send-from')?.value    || '06:00',
    send_until:            document.getElementById('ds-send-until')?.value   || '08:00',
    li_dm_from:            document.getElementById('ds-li-dm-from')?.value   || '20:00',
    li_dm_until:           document.getElementById('ds-li-dm-until')?.value  || '23:00',
    li_connect_from:       document.getElementById('ds-li-connect-from')?.value  || null,
    li_connect_until:      document.getElementById('ds-li-connect-until')?.value || null,
    timezone:              document.getElementById('ds-timezone')?.value,
    active_days:           getDayPickerValue('ds-active-days'),
    max_emails_per_day:    Number(document.getElementById('ds-max-emails')?.value),
    linkedin_daily_limit:  Number(document.getElementById('ds-li-daily')?.value),
    max_contacts_per_firm: Number(document.getElementById('ds-max-per-firm')?.value),
    min_investor_score:    Number(document.getElementById('ds-min-score')?.value),
    followup_days_li:      Number(document.getElementById('ds-followup-li')?.value),
    followup_days_email:   Number(document.getElementById('ds-followup-email')?.value),
    pipeline_max:          Number(document.getElementById('ds-pipeline-max')?.value) || 100,
    pipeline_refill_threshold: Number(document.getElementById('ds-pipeline-refill')?.value) || 30,
  };
  const btn = document.querySelector(`[onclick="saveDealSettings('${id}')"]`);
  const origText = btn?.textContent;
  if (btn) { btn.textContent = 'Saving…'; btn.disabled = true; }
  try {
    const res = await api(`/api/deals/${id}`, 'PATCH', payload);
    const updatedDeal = res?.deal || null;
    if (updatedDeal) {
      allDeals = (allDeals || []).map(d => ((d.id || d._id) === id ? { ...d, ...updatedDeal } : d));
    }
    // Update active currency context so formatMoney reflects change immediately
    if (payload.currency) window.__activeDealCurrency = payload.currency;
    await loadDeals();
    const activeTab = document.querySelector('.deal-tab.active')?.dataset.tab || 'overview';
    if (selectedDealId === id) {
      await switchDealTab(activeTab, document.querySelector(`.deal-tab[data-tab="${activeTab}"]`));
    }
    await populateDealSelector();
    showToast('Settings saved');
  } catch (err) {
    showToast(`Save failed: ${err.message}`, 'error');
  } finally {
    if (btn) { btn.textContent = origText; btn.disabled = false; }
  }
}

async function loadDealTabRankings(id, page) {
  page = page || 1;
  window.__rankingsCurrentPage = page;
  window.__rankingsNavId = id;
  const el = document.getElementById('deal-tab-rankings');
  if (!el) return;
  if (page === 1) el.innerHTML = '<div class="loading-placeholder">Loading rankings…</div>';
  try {
    const data = await api(`/api/deals/${id}/rankings?page=${page}&limit=50`);
    const rows = data.contacts || data; // backwards compat
    if (!rows?.length) {
      el.innerHTML = '<div class="loading-placeholder">No ranked investors yet — research is running.</div>';
      return;
    }

    const gradeStyle = (score) => {
      if (score >= 85) return 'background:var(--gold-dim);color:var(--gold)';
      if (score >= 65) return 'background:#1a3d2b;color:#4ade80';
      if (score >= 45) return 'background:#2a2a2a;color:#aaa';
      return 'background:#1a1a1a;color:#555';
    };
    const gradeLabel = (score) => {
      if (score >= 85) return 'HOT';
      if (score >= 65) return 'WARM';
      if (score >= 45) return 'POSSIBLE';
      return 'ARCHIVE';
    };

    el.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <div style="color:var(--text-dim);font-size:13px">${data.total != null ? data.total : rows.length} ranked investors — sorted by score</div>
        <button class="btn btn-ghost btn-sm" style="font-size:12px" onclick="exportDealRankingsCSV('${id}', this)">&#8595; Export CSV</button>
      </div>
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>#</th>
              <th>Firm</th>
              <th>Contact</th>
              <th>Score</th>
              <th>Type / Sector</th>
              <th>Geography</th>
              <th>Past Investments</th>
              <th>Cheque Size</th>
              <th>LI</th>
              <th>Email</th>
              <th>Sent</th>
              <th>Stage</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((r, i) => {
              const emailSentAt = r.last_email_sent_at;
              const emailSentCell = !r.email
                ? '<span style="color:#555">—</span>'
                : emailSentAt
                  ? `<span style="color:#4ade80" title="${esc(emailSentAt)}">✓</span>`
                  : '<span style="color:#666">·</span>';
              const liIcon = r.linkedin_url
                ? `<a href="${esc(r.linkedin_url)}" target="_blank" title="${esc(r.linkedin_url)}" style="color:#0a66c2;font-size:14px">in</a>`
                : '<span style="color:#555">—</span>';
              const pastInv = r.past_investments
                ? `<span style="font-size:11px;color:var(--text-dim)" title="${esc(r.past_investments)}">${esc(r.past_investments.substring(0, 60))}${r.past_investments.length > 60 ? '…' : ''}</span>`
                : r.person_researched
                  ? '<span style="color:#555;font-size:10px;font-style:italic">N/A</span>'
                  : '<span style="color:#555">—</span>';
              const rowNum = (page - 1) * 50 + i + 1;
              return `<tr style="cursor:pointer;transition:background 0.15s"
              onmouseover="this.style.background='var(--surface-2,#1a1a1a)'"
              onmouseout="this.style.background=''"
              onclick="openProspectDrawer('${r.id}', '${id}')">
              <td class="text-dim" style="font-size:11px">${rowNum}</td>
              <td style="font-weight:500">${esc(r.company_name || '—')}</td>
              <td>
                ${esc(r.name || '—')}
                ${r.is_warm_contact ? '<span style="color:#f59e0b;font-size:10px;margin-left:4px">\u2605 WARM</span>' : ''}
                ${r.job_title ? `<div style="font-size:11px;color:var(--text-dim)">${esc(r.job_title)}</div>` : ''}
              </td>
              <td>
                <span class="status-badge" style="${gradeStyle(r.investor_score)}">
                  ${r.investor_score} — ${gradeLabel(r.investor_score)}
                </span>
              </td>
              <td style="font-size:12px;color:var(--text-dim);max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
                ${esc((r.sector_focus || '—').substring(0, 60))}
              </td>
              <td style="font-size:12px">${esc(r.geography || '—')}</td>
              <td style="max-width:200px">${pastInv}</td>
              <td style="font-size:12px;white-space:nowrap">${esc(r.typical_cheque_size || '—')}</td>
              <td style="text-align:center">${liIcon}</td>
              <td style="font-size:11px;font-family:var(--font-mono);max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.email || '')}">
                ${r.email ? `<span style="color:#4ade80">${esc(r.email)}</span>` : '<span style="color:#555">—</span>'}
              </td>
              <td style="text-align:center">${emailSentCell}</td>
              <td><span class="status-badge">${esc(r.pipeline_stage || '—')}</span></td>
              ${!selectedDealReadOnly ? `<td>
                <button class="btn btn-ghost" style="font-size:11px;padding:3px 8px"
                  onclick="event.stopPropagation();skipRankedContact('${r.id}', this)">Skip</button>
              </td>` : '<td></td>'}
            </tr>`;}).join('')}
          </tbody>
        </table>
      </div>
      <div id="deal-rankings-pagination"></div>
    `;
    window.__rankingsNavId = id;
    window.__rankingsNav = (p) => loadDealTabRankings(window.__rankingsNavId, p);
    renderPagination('deal-rankings-pagination', page, data.pages || 1, 'window.__rankingsNav');
  } catch (e) {
    el.innerHTML = `<div class="loading-placeholder text-red">Failed to load: ${esc(e.message)}</div>`;
  }
}

async function skipRankedContact(contactId, btn) {
  try {
    await api(`/api/contact/${contactId}/stage`, 'POST', { stage: 'Skipped' });
    btn.closest('tr').style.opacity = '0.3';
    btn.disabled = true;
  } catch (err) { alert(`Failed: ${err.message}`); }
}

/* ═══════════════════════════════════════════════════════════════════════════
   PROSPECT DRAWER
   ═══════════════════════════════════════════════════════════════════════════ */

async function openProspectDrawer(contactId, dealId) {
  let drawer = document.getElementById('prospect-drawer');
  if (!drawer) {
    drawer = document.createElement('div');
    drawer.id = 'prospect-drawer';
    drawer.style.cssText = [
      'position:fixed;top:0;right:-480px;width:480px;height:100vh',
      'background:#0f0f0f;border-left:1px solid #2a2a2a',
      'z-index:1000;overflow-y:auto;transition:right 0.3s ease',
      'box-shadow:-20px 0 60px rgba(0,0,0,0.5)',
    ].join(';');
    document.body.appendChild(drawer);
    document.addEventListener('click', (e) => {
      if (drawer.style.right === '0px' &&
          !drawer.contains(e.target) &&
          !e.target.closest('tr[onclick]')) {
        closeProspectDrawer();
      }
    });
  }

  drawer.innerHTML = '<div style="padding:24px;color:#6b7280;font-size:13px">Loading…</div>';
  requestAnimationFrame(() => { drawer.style.right = '0px'; });

  let contact;
  try {
    contact = await api(`/api/contacts/${contactId}`);
  } catch (err) {
    drawer.innerHTML = `<div style="padding:24px;color:#ef4444;font-size:13px">Failed to load: ${esc(err.message)}</div>`;
    return;
  }

  const score = contact.investor_score || 0;
  const gradeColor = score >= 85 ? '#d4a847' : score >= 65 ? '#4ade80' : '#6b7280';
  const gradeLabel = score >= 85 ? 'HOT' : score >= 65 ? 'WARM' : score >= 45 ? 'POSSIBLE' : 'ARCHIVE';

  const row = (label, val, color = '#9ca3af') =>
    val ? `<div style="display:flex;align-items:flex-start;gap:10px;margin-bottom:8px">
      <span style="color:#6b7280;font-size:12px;min-width:110px;flex-shrink:0">${label}</span>
      <span style="color:${color};font-size:13px;line-height:1.4">${val}</span>
    </div>` : '';

  drawer.innerHTML = `
    <!-- Header -->
    <div style="padding:24px;border-bottom:1px solid #1f1f1f;position:sticky;top:0;background:#0f0f0f;z-index:1">
      <div style="display:flex;justify-content:space-between;align-items:flex-start">
        <div style="flex:1;min-width:0;padding-right:12px">
          <div style="font-size:18px;font-weight:700;color:#e5e7eb;margin-bottom:3px;word-break:break-word">${esc(contact.name || '—')}</div>
          <div style="color:#9ca3af;font-size:13px">${esc(contact.company_name || '—')}</div>
          ${contact.job_title ? `<div style="color:#6b7280;font-size:12px;margin-top:2px">${esc(contact.job_title)}</div>` : ''}
        </div>
        <div style="display:flex;align-items:center;gap:10px;flex-shrink:0">
          <div style="text-align:center">
            <div style="font-size:24px;font-weight:700;color:${gradeColor};line-height:1">${score}</div>
            <div style="font-size:10px;color:${gradeColor};letter-spacing:0.1em;margin-top:2px">${gradeLabel}</div>
          </div>
          <button onclick="closeProspectDrawer()"
            style="background:none;border:none;color:#6b7280;font-size:22px;cursor:pointer;padding:4px;line-height:1">×</button>
        </div>
      </div>
    </div>

    <!-- Contact -->
    <div style="padding:20px 24px;border-bottom:1px solid #1a1a1a">
      <div style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:12px">Contact</div>
      ${contact.email ? row('Email',
          `<a href="mailto:${esc(contact.email)}" style="color:#60a5fa;text-decoration:none">${esc(contact.email)}</a>`, '#60a5fa') : ''}
      ${contact.linkedin_url ? row('LinkedIn',
          `<a href="${esc(contact.linkedin_url.startsWith('http') ? contact.linkedin_url : 'https://'+contact.linkedin_url)}"
            target="_blank" style="color:#60a5fa;text-decoration:none">View Profile →</a>`, '#60a5fa') : ''}
      ${row('Phone', contact.phone)}
      ${row('Location', contact.geography)}
      ${!contact.email && !contact.linkedin_url && !contact.phone && !contact.geography
        ? '<div style="color:#374151;font-size:12px">No contact details yet</div>' : ''}
    </div>

    <!-- Firm intel -->
    <div style="padding:20px 24px;border-bottom:1px solid #1a1a1a">
      <div style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:12px">Firm Intelligence</div>
      ${row('AUM / Fund', contact.aum_fund_size, '#e5e7eb')}
      ${row('Cheque Size', contact.typical_cheque_size, '#e5e7eb')}
      ${contact.sector_focus ? `<div style="margin-bottom:8px">
        <div style="color:#6b7280;font-size:12px;margin-bottom:4px">Sector Focus</div>
        <div style="color:#9ca3af;font-size:12px;line-height:1.5">${esc(contact.sector_focus.substring(0, 200))}${contact.sector_focus.length > 200 ? '…' : ''}</div>
      </div>` : ''}
    </div>

    <!-- Past investments -->
    ${contact.past_investments ? `
    <div style="padding:20px 24px;border-bottom:1px solid #1a1a1a">
      <div style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:12px">Past Investments</div>
      <div style="display:flex;flex-wrap:wrap;gap:6px">
        ${contact.past_investments.split(',').filter(Boolean).map(inv =>
          `<span style="background:#1a1a2a;color:#60a5fa;padding:3px 10px;border-radius:4px;font-size:12px;border:1px solid #1f3a5f">${esc(inv.trim())}</span>`
        ).join('')}
      </div>
    </div>` : ''}

    <!-- Research notes -->
    ${contact.notes ? `
    <div style="padding:20px 24px;border-bottom:1px solid #1a1a1a">
      <div style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:12px">Research Notes</div>
      <p style="color:#9ca3af;font-size:13px;line-height:1.6;margin:0;white-space:pre-wrap">${esc(contact.notes)}</p>
    </div>` : ''}

    <!-- Outreach status -->
    <div style="padding:20px 24px;border-bottom:1px solid #1a1a1a">
      <div style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:12px">Outreach Status</div>
      ${row('Stage', `<span style="background:#1a1a1a;color:#e5e7eb;padding:2px 10px;border-radius:4px;font-size:12px">${esc(contact.pipeline_stage || '—')}</span>`)}
      ${row('Enrichment', contact.enrichment_status || 'Pending',
        contact.enrichment_status === 'Enriched' ? '#4ade80' : '#6b7280')}
      ${row('LI Invite Sent', contact.linkedin_invite_sent ? 'Yes' : 'No',
        contact.linkedin_invite_sent ? '#4ade80' : '#6b7280')}
      ${row('LI Accepted', contact.linkedin_invite_accepted ? 'Yes' : 'No',
        contact.linkedin_invite_accepted ? '#4ade80' : '#6b7280')}
      ${contact.last_contacted ? row('Last Contacted',
        new Date(contact.last_contacted).toLocaleDateString('en-GB')) : ''}
    </div>

    <!-- Conversation History (loaded async) -->
    <div id="conv-history-${contactId}" style="padding:20px 24px;border-bottom:1px solid #1a1a1a">
      <div style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:12px">Conversation History</div>
      <div style="color:#374151;font-size:12px">Loading…</div>
    </div>

    <!-- Footer -->
    <div style="padding:16px 24px">
      <div style="color:#374151;font-size:11px">
        Source: ${esc(contact.source || 'Database')} •
        Added ${contact.created_at ? new Date(contact.created_at).toLocaleDateString('en-GB') : '—'}
        ${contact.conversation_state ? ` • State: <span style="color:#d4a847">${esc(contact.conversation_state)}</span>` : ''}
      </div>
    </div>
  `;

  // Load conversation history asynchronously
  try {
    const convData = await api(`/api/contacts/${contactId}/conversation`);
    const msgs = convData.messages || [];
    const convEl = document.getElementById(`conv-history-${contactId}`);
    if (convEl) {
      convEl.innerHTML = `
        <div style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:12px">
          Conversation History (${msgs.length} message${msgs.length !== 1 ? 's' : ''})
        </div>
        ${msgs.length === 0
          ? '<div style="color:#374151;font-size:12px">No messages logged yet.</div>'
          : msgs.map(m => {
            const isOut  = m.direction === 'outbound';
            const ts     = new Date(m.sent_at || m.received_at || Date.now()).toLocaleDateString('en-GB');
            const label  = isOut ? 'ROCO' : esc(contact.name || 'INVESTOR');
            const color  = isOut ? '#1f3a5f' : '#2a1a0a';
            const border = isOut ? '#60a5fa' : '#d4a847';
            const nameC  = isOut ? '#60a5fa' : '#d4a847';
            const preview = (m.body || '').substring(0, 350);
            return `<div style="margin-bottom:10px;padding:10px 12px;background:${color};border-radius:6px;border-left:3px solid ${border}">
              <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                <span style="color:${nameC};font-size:11px;font-weight:600">${label}</span>
                <span style="color:#374151;font-size:10px">${ts}${m.channel ? ` · ${m.channel}` : ''}</span>
              </div>
              ${m.intent ? `<div style="color:#6b7280;font-size:10px;margin-bottom:4px;font-style:italic">Intent: ${esc(m.intent)}</div>` : ''}
              <p style="color:#9ca3af;font-size:12px;margin:0;line-height:1.5;white-space:pre-wrap">${esc(preview)}${m.body && m.body.length > 350 ? '…' : ''}</p>
            </div>`;
          }).join('')
        }
      `;
    }
  } catch (_) {
    const convEl = document.getElementById(`conv-history-${contactId}`);
    if (convEl) convEl.innerHTML = '<div style="color:#374151;font-size:12px">Could not load conversation history.</div>';
  }
}

function closeProspectDrawer() {
  const drawer = document.getElementById('prospect-drawer');
  if (drawer) drawer.style.right = '-480px';
}

async function closeDeal(id) {
  if (!confirm('Close this deal? This cannot be undone.')) return;
  try {
    await api(`/api/deals/${id}/close`, 'POST');
    await loadDeals();
    await populateDealSelector();
  } catch (err) { alert(`Failed: ${err.message}`); }
}

async function toggleDealPause(id, currentStatus, checkbox) {
  const willPause = !checkbox.checked;
  const endpoint  = willPause ? 'pause' : 'resume';
  try {
    await api(`/api/deals/${id}/${endpoint}`, 'POST');
    await loadDeals();
  } catch (err) {
    checkbox.checked = !willPause;
    alert(`Failed: ${err.message}`);
  }
}

/* ─── BATCH VISUALIZER ───────────────────────────────────────────────────── */
function renderBatchPipeline(batches, container) {
  if (!batches?.length) {
    container.innerHTML = '<div class="loading-placeholder">No batches yet.</div>';
    return;
  }
  const STAGES = ['queued','enriched','researched','drafted','approved','sent','replied'];
  container.innerHTML = `<div class="batch-list">
    ${batches.map(b => {
      const total     = b.total || b.contactCount || 0;
      const completed = b.sent || b.completed || 0;
      const stagePct  = total > 0 ? Math.round((completed / total) * 100) : 0;
      return `<div class="batch-item">
        <div class="batch-header">
          <span class="batch-name">${esc(b.name || `Batch ${b.batchNumber || b.id}`)}</span>
          <span class="batch-stage">${esc(b.stage || b.status || 'queued').toUpperCase()}</span>
        </div>
        <div class="batch-bar"><div class="batch-fill" style="width:${stagePct}%"></div></div>
        <div class="batch-stats">
          <span>${completed} / ${total} contacts</span>
          ${b.emailsSent != null ? `<span>${b.emailsSent} emails sent</span>` : ''}
          ${b.replies    != null ? `<span>${b.replies} replies</span>` : ''}
          <span class="text-dim">${formatDate(b.createdAt)}</span>
        </div>
      </div>`;
    }).join('')}
  </div>`;
}

/* ═══════════════════════════════════════════════════════════════════════════
   PIPELINE
   ═══════════════════════════════════════════════════════════════════════════ */

async function loadPipeline() {
  const tbody  = document.getElementById('pipeline-tbody');
  const dealId = document.getElementById('pipeline-deal-filter')?.value || activeDeal || '';
  if (!tbody) return;
  tbody.innerHTML = '<tr><td colspan="8" class="table-empty">Loading…</td></tr>';

  try {
    const qs   = dealId ? `?dealId=${dealId}` : '';
    const data = await api(`/api/pipeline${qs}`);
    pipelineData = Array.isArray(data) ? data : (data.contacts || data.pipeline || []);
    renderPipelineTable();
  } catch {
    tbody.innerHTML = '<tr><td colspan="8" class="table-empty text-red">Failed to load pipeline.</td></tr>';
  }
}

function renderPipelineTable() {
  const tbody = document.getElementById('pipeline-tbody');
  if (!tbody) return;

  // Sort
  const { key, dir } = pipelineSort;
  const sorted = [...pipelineData].sort((a, b) => {
    let av = a[key] ?? '', bv = b[key] ?? '';
    if (typeof av === 'string') av = av.toLowerCase();
    if (typeof bv === 'string') bv = bv.toLowerCase();
    if (av < bv) return dir === 'asc' ? -1 : 1;
    if (av > bv) return dir === 'asc' ? 1 : -1;
    return 0;
  });

  if (!sorted.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="table-empty">No contacts in pipeline.</td></tr>';
    return;
  }

  tbody.innerHTML = sorted.map(c => {
    const id      = c.id || c._id;
    const name    = esc(c.name || (c.firstName ? (c.firstName + ' ' + (c.lastName || '')).trim() : null) || '—');
    const firm    = esc(c.firm || c.company || '—');
    const deal    = (c.dealName || c.deal_name) ? `<span class="status-badge" style="background:var(--gold-dim);color:var(--gold)">${esc(c.dealName || c.deal_name)}</span>` : '<span class="text-dim">—</span>';
    const stage   = c.stage || 'prospecting';
    return `<tr onclick="togglePipelineRow(this, '${id}')">
      <td><strong>${name}</strong></td>
      <td>${firm}</td>
      <td>${deal}</td>
      <td>${scoreHtml(c.score)}</td>
      <td>
        <select class="stage-select" onclick="event.stopPropagation()" onchange="updateStage('${id}', this.value)">
          ${['prospecting','contacted','interested','meeting','term_sheet','closed','rejected'].map(s =>
            `<option value="${s}" ${s === stage ? 'selected' : ''}>${s.replace(/_/g,' ')}</option>`
          ).join('')}
        </select>
      </td>
      <td class="text-dim">${formatDate(c.lastContact || c.lastContacted)}</td>
      <td class="text-dim">${formatDate(c.nextFollowup || c.nextFollowUp)}</td>
      <td>
        <div class="row-actions">
          <button class="row-action-btn" onclick="event.stopPropagation(); viewContact('${id}')">View</button>
          <button class="row-action-btn" onclick="event.stopPropagation(); skipContact('${id}')">Skip</button>
          <button class="row-action-btn danger" onclick="event.stopPropagation(); suppressFirm('${esc(c.firm || c.company || '')}')">Suppress Firm</button>
          <button class="row-action-btn" style="color:#e05c5c" onclick="event.stopPropagation(); deleteContact('${id}')">Delete</button>
        </div>
      </td>
    </tr>
    <tr class="pipeline-row-detail hidden" id="pipeline-detail-${id}">
      <td colspan="8">
        <div style="color:var(--text-mid);font-size:12px;line-height:1.6">
          ${c.researchSummary ? `<div><strong>Research:</strong> ${esc(c.researchSummary)}</div>` : ''}
          ${c.comparableDeals ? `<div style="margin-top:6px"><strong>Comparable Deals:</strong> ${esc(c.comparableDeals)}</div>` : ''}
          ${!c.researchSummary && !c.comparableDeals ? 'No additional data.' : ''}
        </div>
      </td>
    </tr>`;
  }).join('');
}

function togglePipelineRow(row, id) {
  const detail = document.getElementById(`pipeline-detail-${id}`);
  if (detail) detail.classList.toggle('hidden');
}

async function sortPipeline(key) {
  if (pipelineSort.key === key) {
    pipelineSort.dir = pipelineSort.dir === 'asc' ? 'desc' : 'asc';
  } else {
    pipelineSort = { key, dir: 'desc' };
  }
  renderPipelineTable();
}

async function updateStage(contactId, stage) {
  try {
    await api(`/api/contact/${contactId}/stage`, 'POST', { stage });
    const row = pipelineData.find(c => (c.id || c._id) === contactId);
    if (row) row.stage = stage;
  } catch (err) { alert(`Failed to update stage: ${err.message}`); }
}

function viewContact(id) {
  // Implemented as expand-row; full modal could be added
  togglePipelineRow(null, id);
}

async function skipContact(id) {
  if (!confirm('Skip this contact?')) return;
  try {
    await api(`/api/contact/${id}/stage`, 'POST', { stage: 'skipped' });
    pipelineData = pipelineData.filter(c => (c.id || c._id) !== id);
    renderPipelineTable();
  } catch (err) { alert(`Failed: ${err.message}`); }
}

async function suppressFirm(firm) {
  if (!firm || !confirm(`Suppress all contacts from "${firm}"?`)) return;
  try {
    await api('/api/action', 'POST', { action: 'suppress_firm', firm });
    pipelineData = pipelineData.filter(c => (c.firm || c.company || '') !== firm);
    renderPipelineTable();
  } catch (err) { alert(`Failed: ${err.message}`); }
}

async function deleteContact(id) {
  const contact = pipelineData.find(c => (c.id || c._id) === id);
  const name = contact?.name || 'this contact';
  if (!confirm(`Remove ${name} from the pipeline?\n\nRoco will never contact them.`)) return;
  try {
    await api(`/api/contacts/${id}`, 'DELETE');
    pipelineData = pipelineData.filter(c => (c.id || c._id) !== id);
    renderPipelineTable();
    showToast(`${name} removed from pipeline`);
  } catch (err) { alert(`Delete failed: ${err.message}`); }
}

/* ═══════════════════════════════════════════════════════════════════════════
   APPROVAL QUEUE
   ═══════════════════════════════════════════════════════════════════════════ */

async function loadQueue() {
  try {
    const data  = await api('/api/queue');
    const emails = Array.isArray(data) ? data.filter(i => i.channel !== 'linkedin') : (data.emails || []);
    const linkedin = Array.isArray(data) ? data.filter(i => i.channel === 'linkedin') : (data.linkedin || []);

    document.getElementById('queue-email-count').textContent = emails.length;
    document.getElementById('queue-linkedin-count').textContent = linkedin.length;
    refreshQueueBadge(emails.length + linkedin.length);

    renderQueueList('queue-email-list', emails, 'email');
    renderQueueList('queue-linkedin-list', linkedin, 'linkedin');
  } catch { /* silent */ }
}

function renderQueueList(containerId, items, type) {
  const el = document.getElementById(containerId);
  if (!el) return;
  if (!items.length) {
    el.innerHTML = `<div class="queue-empty">&#10003;&nbsp; No ${type} messages pending approval.</div>`;
    return;
  }
  el.innerHTML = items.map(item => renderQueueCard(item)).join('');
}

function renderQueueCard(item) {
  const id      = item.id || item._id;
  const name    = esc(item.name || item.firstName || '—');
  const firm    = esc(item.firm || item.company || '');
  const scoreHt = scoreHtml(item.score);
  const stage   = esc(item.stage || '');
  const subA    = esc(item.subjectA || item.subject || '');
  const subB    = esc(item.subjectB || '');
  const hasAB   = !!subB;
  const body    = esc(item.body || item.emailBody || '');

  return `<div class="queue-card" id="qcard-${id}">
    <div class="queue-card-header">
      <div>
        <div class="queue-name">${name}</div>
        <div class="queue-firm">${firm}</div>
      </div>
      <div class="queue-meta">
        ${scoreHt}
        ${stage ? `<span class="status-badge">${stage}</span>` : ''}
        ${hasAB ? `<div class="subject-toggle">
          <button class="subject-toggle-btn active" id="stb-a-${id}" onclick="switchSubject('${id}','a')">A</button>
          <button class="subject-toggle-btn" id="stb-b-${id}" onclick="switchSubject('${id}','b')">B</button>
        </div>` : ''}
      </div>
    </div>
    <div class="queue-body">
      <div class="queue-subject" id="qsubject-${id}">${subA || '(no subject)'}</div>
      <div class="queue-preview">${body}</div>
    </div>
    <div class="queue-actions">
      <button class="btn-approve" onclick="approveEmail('${id}', currentQueueVariant('${id}'), '${id}')">&#10003; APPROVE</button>
      <button class="btn btn-ghost btn-sm" onclick="previewQueueItem('${id}')">&#128065; PREVIEW</button>
      <button class="btn btn-ghost btn-sm" onclick="editApproval('${id}')">&#9998; EDIT</button>
      <button class="btn btn-danger btn-sm" onclick="skipApproval('${id}')">SKIP</button>
      ${item.linkedinUrl ? `<a href="${esc(item.linkedinUrl)}" target="_blank" class="btn btn-ghost btn-sm" onclick="event.stopPropagation()">LinkedIn ↗</a>` : ''}
    </div>
  </div>
  <script>window._qSubjects = window._qSubjects || {}; window._qSubjects['${id}'] = {a:'${subA.replace(/'/g,"\\'")}',b:'${subB.replace(/'/g,"\\'")}',current:'a'};<\/script>`;
}

function switchSubject(id, variant) {
  const subjects = window._qSubjects?.[id];
  if (!subjects) return;
  subjects.current = variant;
  const el = document.getElementById(`qsubject-${id}`);
  if (el) el.textContent = subjects[variant] || '(no subject)';
  document.getElementById(`stb-a-${id}`)?.classList.toggle('active', variant === 'a');
  document.getElementById(`stb-b-${id}`)?.classList.toggle('active', variant === 'b');
}

function currentQueueVariant(id) {
  return window._qSubjects?.[id]?.current || 'a';
}

function previewQueueItem(id) {
  const subjects = window._qSubjects?.[id] || {};
  const card = document.getElementById(`qcard-${id}`);
  const bodyEl = card?.querySelector('.queue-preview');
  const nameEl = card?.querySelector('.queue-name');
  const firmEl = card?.querySelector('.queue-firm');
  const body = bodyEl?.textContent || '';
  const isLinkedIn = !subjects.a; // no subject = LinkedIn

  const tmpl = {
    type: isLinkedIn ? 'linkedin_dm' : 'email',
    subject_a: subjects.a || null,
    subject_b: subjects.b || null,
    body,
  };
  const contactData = {
    firstName: (nameEl?.textContent || '').split(' ')[0] || 'James',
    fullName:  nameEl?.textContent || 'James Mitchell',
    firm:      firmEl?.textContent || 'Meridian Capital',
    company:   firmEl?.textContent || 'Meridian Capital',
  };
  window.previewDealTemplate(tmpl, contactData);
}

function switchQueueTab(tab, btn) {
  currentQueueTab = tab;
  document.querySelectorAll('.queue-tab').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  document.getElementById('qpanel-email')?.classList.toggle('hidden', tab !== 'email');
  document.getElementById('qpanel-linkedin')?.classList.toggle('hidden', tab !== 'linkedin');
}

async function approveEmail(id, variant, _unused) {
  const subjects   = window._qSubjects?.[id] || {};
  const subject    = subjects[variant] || subjects.a || '';
  try {
    await api('/api/approve', 'POST', { id, variant, subject });
    document.getElementById(`qcard-${id}`)?.remove();
    await loadQueue();
  } catch (err) { alert(`Approve failed: ${err.message}`); }
}

async function editApproval(id) {
  openModal('Edit Email', async () => {
    const instructions = document.getElementById('modal-instructions').value;
    try {
      await api('/api/edit-approval', 'POST', { id, instructions });
      closeModal();
      await loadQueue();
    } catch (err) { alert(`Edit failed: ${err.message}`); }
  });
}

async function skipApproval(id) {
  try {
    await api('/api/skip-approval', 'POST', { id });
    const card = document.getElementById(`qcard-${id}`);
    if (card) {
      card.style.transition = 'opacity 0.3s';
      card.style.opacity = '0';
      setTimeout(() => { card.remove(); loadQueue(); }, 320);
    } else {
      await loadQueue();
    }
    showToast('Skipped — draft deleted');
  } catch (err) { alert(`Skip failed: ${err.message}`); }
}

/* ═══════════════════════════════════════════════════════════════════════════
   LIVE ACTIVITY
   ═══════════════════════════════════════════════════════════════════════════ */

async function loadActivity() {
  const dealId = document.getElementById('activity-deal-filter')?.value || activeDeal || '';
  try {
    const qs   = dealId ? `?dealId=${dealId}` : '';
    const data = await api(`/api/activity/log${qs}`);
    const items = Array.isArray(data) ? data : (data.log || data.items || []);
    activityLog = items;
    filterActivity();
  } catch { /* silent */ }
}

/* ═══════════════════════════════════════════════════════════════════════════
   DEAL ARCHIVE
   ═══════════════════════════════════════════════════════════════════════════ */

async function loadArchive() {
  const tbody = document.getElementById('archive-tbody');
  if (!tbody) return;
  tbody.innerHTML = '<tr><td colspan="8" class="table-empty">Loading…</td></tr>';

  try {
    const data  = await api('/api/deals');
    const deals = (Array.isArray(data) ? data : (data.deals || [])).filter(d =>
      ['closed','archived','complete','CLOSED','ARCHIVED','COMPLETE','PAUSED'].includes(d.status) ||
      d.archived_at
    );

    if (!deals.length) {
      tbody.innerHTML = '<tr><td colspan="8" class="table-empty">No archived deals.</td></tr>';
      return;
    }

    tbody.innerHTML = deals.map(d => {
      const id = d.id || d._id;
      // Support both camelCase (legacy) and snake_case (Supabase)
      const name      = d.name || d.dealName || '—';
      const openedAt  = d.created_at || d.createdAt || d.openedAt;
      const closedAt  = d.closed_at  || d.closedAt;
      const committed = d.committed_amount || d.capitalCommitted || 0;
      const target    = d.target_amount    || d.targetAmount     || 0;
      return `<tr onclick="toggleArchiveExpand(this, '${id}')">
        <td><strong>${esc(name)}</strong></td>
        <td class="text-dim">${formatDate(openedAt)}</td>
        <td class="text-dim">${formatDate(closedAt)}</td>
        <td class="mono">${formatMoney(committed, d.currency || 'USD')} / ${formatMoney(target, d.currency || 'USD')}</td>
        <td class="mono">${fmt(d.emails_sent || d.emailsSent || 0)}</td>
        <td class="mono">${fmt(d.invites_sent_week || d.invitesSentWeek || 0)}</td>
        <td class="mono">${d.response_rate != null ? pct(d.response_rate) : (d.responseRate != null ? pct(d.responseRate) : '—')}</td>
        <td>
          <div style="display:flex;gap:4px;justify-content:flex-end">
            <button class="row-action-btn" onclick="event.stopPropagation(); viewArchivedDeal('${id}')">View</button>
            <button class="row-action-btn" onclick="event.stopPropagation(); toggleArchiveExpand(this.closest('tr'), '${id}')">&#9660;</button>
          </div>
        </td>
      </tr>
      <tr class="archive-expand hidden" id="archive-expand-${id}">
        <td colspan="8">
          <div style="padding:10px 0;display:flex;gap:16px;align-items:flex-start">
            <div style="flex:1;color:var(--text-mid);font-size:12px;line-height:1.6">${d.description ? esc(d.description) : 'No additional details.'}</div>
            <button class="btn btn-sm" style="background:rgba(220,50,50,0.15);color:#e05;border:1px solid rgba(220,50,50,0.3);padding:4px 12px;font-size:11px;white-space:nowrap" onclick="event.stopPropagation();deleteDeal('${id}', '${(d.name||'').replace(/'/g,'\\x27').replace(/"/g,'\\x22')}')">Delete Permanently</button>
          </div>
        </td>
      </tr>`;
    }).join('');
  } catch {
    tbody.innerHTML = '<tr><td colspan="8" class="table-empty text-red">Failed to load archive.</td></tr>';
  }
}

function toggleArchiveExpand(row, id) {
  const expandRow = document.getElementById(`archive-expand-${id}`);
  if (expandRow) expandRow.classList.toggle('hidden');
}

async function deleteDeal(id, name) {
  if (!confirm(`Permanently delete "${name}"?\n\nAll contacts, emails, firms and activity will be removed. This cannot be undone.`)) return;
  try {
    const data = await api(`/api/deals/${id}`, 'DELETE');
    if (data?.success !== false) {
      showToast?.('Deal deleted permanently');
      loadArchive?.();
      loadDeals?.();
    } else {
      showToast?.('Delete failed: ' + (data.error || 'Unknown error'), 'error');
    }
  } catch (err) {
    console.error('[DELETE] Error:', err);
    showToast?.('Delete failed: ' + err.message, 'error');
  }
}

async function wipeEverything() {
  const confirmed = prompt(
    'This will permanently delete ALL investors, contacts, deals, approvals, and activity.\n\nRoco will have zero knowledge after this. Type WIPE to confirm.'
  );
  if (confirmed?.trim().toUpperCase() !== 'WIPE') return;
  try {
    await api('/api/wipe-everything', 'DELETE');
    showToast('All data wiped. Roco starts fresh.');
    await loadDeals();
    await loadArchive();
    await loadDatabase?.();
  } catch (err) { alert('Wipe failed: ' + err.message); }
}

/* ═══════════════════════════════════════════════════════════════════════════
   CSV EXPORT
   ═══════════════════════════════════════════════════════════════════════════ */

function downloadCSV(rows, filename) {
  if (!rows?.length) { showToast('No data to export', 'error'); return; }
  const cols = Object.keys(rows[0]);
  const escape = v => {
    if (v == null) return '';
    const s = String(v).replace(/"/g, '""');
    return /[",\n\r]/.test(s) ? `"${s}"` : s;
  };
  const lines = [cols.map(escape).join(',')];
  for (const row of rows) lines.push(cols.map(c => escape(row[c])).join(','));
  const blob = new Blob([lines.join('\r\n')], { type: 'text/csv;charset=utf-8;' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}

async function exportGlobalPipelineCSV() {
  if (!pipelineData?.length) { showToast('No pipeline data to export', 'error'); return; }
  const rows = pipelineData.map(c => ({
    name: c.name || '',
    firm: c.firm || c.company || '',
    deal: c.dealName || c.deal_name || '',
    score: c.score || '',
    stage: c.stage || '',
    enrichment_status: c.enrichmentStatus || '',
    email: c.email || '',
    linkedin_url: c.linkedinUrl || c.linkedin_url || '',
    last_contact: c.lastContact || c.lastContacted || '',
    next_followup: c.nextFollowup || c.nextFollowUp || '',
  }));
  downloadCSV(rows, `pipeline-export-${Date.now()}.csv`);
}

async function exportDealPipelineCSV(id) {
  try {
    const rows = await api(`/api/pipeline?dealId=${id}&limit=9999`);
    const data = Array.isArray(rows) ? rows : (rows.contacts || []);
    if (!data.length) { showToast('No pipeline data to export', 'error'); return; }
    const mapped = data.map(r => ({
      name: r.name || '',
      job_title: r.jobTitle || r.job_title || '',
      firm: r.firm || r.company_name || '',
      score: r.score || '',
      stage: r.stage || '',
      enrichment_status: r.enrichmentStatus || r.enrichment_status || '',
      email: r.email || '',
      linkedin_url: r.linkedinUrl || r.linkedin_url || '',
      last_contacted: r.lastContacted || '',
    }));
    downloadCSV(mapped, `deal-pipeline-${id}-${Date.now()}.csv`);
  } catch (err) { showToast('Export failed: ' + err.message, 'error'); }
}

async function exportDealRankingsCSV(id, btn) {
  if (btn) { btn.disabled = true; btn.textContent = 'Exporting…'; }
  try {
    let allRows = [];
    let page = 1;
    let totalPages = 1;
    do {
      const data = await api(`/api/deals/${id}/rankings?page=${page}&limit=100`);
      const rows = data.contacts || data || [];
      allRows = allRows.concat(rows);
      totalPages = data.pages || 1;
      page++;
    } while (page <= totalPages);
    const mapped = allRows.map(r => ({
      name: r.name || '',
      job_title: r.job_title || '',
      company: r.company_name || '',
      score: r.investor_score || r.score || '',
      stage: r.stage || '',
      email: r.email || '',
      linkedin_url: r.linkedin_url || '',
      preferred_industries: r.preferred_industries || '',
      preferred_geographies: r.preferred_geographies || '',
      preferred_deal_size_min: r.preferred_deal_size_min || '',
      preferred_deal_size_max: r.preferred_deal_size_max || '',
      past_investments: r.past_investments || '',
    }));
    downloadCSV(mapped, `rankings-${id}-${Date.now()}.csv`);
  } catch (err) { showToast('Export failed: ' + err.message, 'error'); }
  finally { if (btn) { btn.disabled = false; btn.textContent = '↓ Export CSV'; } }
}

async function exportDealArchivedCSV(id, btn) {
  if (btn) { btn.disabled = true; btn.textContent = 'Exporting…'; }
  try {
    const rows = await api(`/api/deals/${id}/archived`);
    if (!rows?.length) { showToast('No archived contacts to export', 'error'); return; }
    const mapped = rows.map(r => ({
      name: r.name || '',
      job_title: r.jobTitle || r.job_title || '',
      firm: r.firm || r.company_name || '',
      score: r.score || '',
      archive_reason: r.archiveReason || r.archive_reason || '',
      linkedin_url: r.linkedinUrl || r.linkedin_url || '',
    }));
    downloadCSV(mapped, `archived-contacts-${id}-${Date.now()}.csv`);
  } catch (err) { showToast('Export failed: ' + err.message, 'error'); }
  finally { if (btn) { btn.disabled = false; btn.textContent = '↓ Export CSV'; } }
}

async function exportInvestorsCSV(btn) {
  if (btn) { btn.disabled = true; btn.textContent = 'Exporting…'; }
  try {
    const search  = document.getElementById('db-search-input')?.value?.trim() || '';
    const type    = document.getElementById('db-filter-type')?.value || '';
    const country = document.getElementById('db-filter-country')?.value || '';
    const enrich  = document.getElementById('db-filter-enrichment')?.value || '';
    let allRows = [], page = 1, totalPages = 1;
    do {
      const params = new URLSearchParams({ page, limit: 500 });
      if (search)  params.set('search', search);
      if (type)    params.set('type', type);
      if (country) params.set('country', country);
      if (enrich)  params.set('enrichment', enrich);
      const data = await api(`/api/investors-db/search?${params}`);
      allRows = allRows.concat(data.investors || data || []);
      totalPages = data.pages || 1;
      page++;
    } while (page <= totalPages);
    if (!allRows.length) { showToast('No investors to export', 'error'); return; }
    const mapped = allRows.map(r => ({
      name: r.name || '',
      legal_name: r.legal_name || '',
      investor_type: r.investor_type || '',
      aum_millions: r.aum_millions ?? '',
      dry_powder_millions: r.dry_powder_millions ?? '',
      hq_city: r.hq_city || '',
      hq_state: r.hq_state || '',
      hq_country: r.hq_country || '',
      website: r.website || '',
      preferred_industries: r.preferred_industries || '',
      preferred_geographies: r.preferred_geographies || '',
      preferred_deal_size_min: r.preferred_deal_size_min ?? '',
      preferred_deal_size_max: r.preferred_deal_size_max ?? '',
      preferred_ebitda_min: r.preferred_ebitda_min ?? '',
      preferred_ebitda_max: r.preferred_ebitda_max ?? '',
      primary_contact_name: r.primary_contact_name || '',
      primary_contact_email: r.primary_contact_email || '',
      primary_contact_phone: r.primary_contact_phone || '',
      total_investments: r.total_investments ?? '',
      investments_last_12m: r.investments_last_12m ?? '',
      last_investment_company: r.last_investment_company || '',
      last_investment_date: r.last_investment_date || '',
      investor_category: r.investor_category || '',
    }));
    downloadCSV(mapped, `investors-db-export-${Date.now()}.csv`);
    showToast(`Exported ${mapped.length} investors`);
  } catch (err) { showToast('Export failed: ' + err.message, 'error'); }
  finally { if (btn) { btn.disabled = false; btn.textContent = '↓ Export CSV'; } }
}

async function exportContactsCSV(btn) {
  if (btn) { btn.disabled = true; btn.textContent = 'Exporting…'; }
  try {
    const search = document.getElementById('db-contacts-search')?.value?.trim() || '';
    let allRows = [], page = 1, totalPages = 1;
    do {
      const params = new URLSearchParams({ page, limit: 500 });
      if (search) params.set('search', search);
      const data = await api(`/api/contacts-db/search?${params}`);
      allRows = allRows.concat(data.contacts || []);
      totalPages = data.pages || 1;
      page++;
    } while (page <= totalPages);
    if (!allRows.length) { showToast('No contacts to export', 'error'); return; }
    const mapped = allRows.map(r => ({
      name: r.name || '',
      firm_name: r.firm_name || '',
      title: r.title || '',
      email: r.email || '',
      linkedin_url: r.linkedin_url || '',
      source: r.source || '',
      verified: r.verified ? 'Yes' : 'No',
      updated_at: r.updated_at ? r.updated_at.substring(0, 10) : '',
    }));
    downloadCSV(mapped, `contacts-export-${Date.now()}.csv`);
    showToast(`Exported ${mapped.length} contacts`);
  } catch (err) { showToast('Export failed: ' + err.message, 'error'); }
  finally { if (btn) { btn.disabled = false; btn.textContent = '↓ Export CSV'; } }
}

/* ═══════════════════════════════════════════════════════════════════════════
   TEMPLATES
   ═══════════════════════════════════════════════════════════════════════════ */

/* ═══════════════════════════════════════════════════════════════════════════
   TEMPLATES PAGE — card-based layout with sequence section
   ═══════════════════════════════════════════════════════════════════════════ */

async function loadTemplatesPage() {
  const container = document.getElementById('templates-page-content');
  if (!container) return;
  // Render skeleton with named containers
  container.innerHTML = `
    <div style="margin-bottom:32px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
        <h2 style="color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;margin:0">Outreach Sequence</h2>
        <button onclick="window.showSequenceEditor()" class="btn btn-ghost btn-sm">Edit Sequence</button>
      </div>
      <div id="sequence-bar" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <span style="color:#4b5563;font-size:13px">Loading&#8230;</span>
      </div>
    </div>
    <div style="margin-bottom:32px">
      <h2 style="color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;margin:0 0 14px">Active Templates</h2>
      <div id="active-templates-container" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px">
        <span style="color:#4b5563;font-size:13px">Loading&#8230;</span>
      </div>
    </div>
    <div id="bench-section" style="margin-bottom:32px;display:none">
      <h2 style="color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;margin:0 0 14px">Bench</h2>
      <div id="bench-templates-container" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px"></div>
    </div>`;

  try {
    const [seqData, tmplData] = await Promise.all([
      api('/api/sequence'),
      api('/api/templates'),
    ]);
    const sequenceSteps  = seqData?.steps || [];
    const allTemplates   = Array.isArray(tmplData) ? tmplData : (tmplData?.templates || []);
    const activeLabels   = new Set(sequenceSteps.map(s => s.label));

    // Active = is_primary=true AND sequence_step is in the current sequence
    const active = allTemplates.filter(t => t.is_primary && t.sequence_step && activeLabels.has(t.sequence_step));
    // Bench = everything else
    const bench  = allTemplates.filter(t => !active.find(a => a.id === t.id));

    // Steps that have no primary template (linkedin_invite skipped — handled by agent directly)
    const coveredSteps  = new Set(active.map(t => t.sequence_step));
    const missingSteps  = sequenceSteps.filter(s => s.type !== 'linkedin_invite' && !coveredSteps.has(s.label));

    renderSequenceBar(sequenceSteps);
    renderActiveTemplates(active, missingSteps);
    renderBenchTemplates(bench);
  } catch (err) {
    container.innerHTML = `<div class="loading-placeholder">Failed to load: ${esc(err.message)}</div>`;
  }
}

function renderSequenceBar(steps) {
  const bar = document.getElementById('sequence-bar');
  if (!bar) return;
  if (!steps.length) {
    bar.innerHTML = '<span style="color:#4b5563;font-size:13px">No sequence configured — click Edit Sequence to set one up.</span>';
    return;
  }
  const typeBg = { email: '#1f3a5f', linkedin_invite: '#1a3a2a', linkedin_dm: '#2a1f3a' };
  const typeLabel = { email: 'Email', linkedin_invite: 'LI Invite', linkedin_dm: 'LI DM' };
  bar.innerHTML = steps.map((s, i) => `
    ${i > 0 ? '<div style="color:#3a3a3a;font-size:20px;align-self:center">&#8594;</div>' : ''}
    <div style="display:flex;flex-direction:column;align-items:center;gap:4px;padding:10px 16px;
                background:${typeBg[s.type] || '#1a1a1a'};border-radius:6px;min-width:110px;text-align:center">
      <div style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.1em">Day ${s.delay_days || 0}</div>
      <div style="color:#e5e7eb;font-size:12px;font-weight:600;font-family:'DM Mono',monospace">${esc(s.label || '')}</div>
      <div style="font-size:10px;padding:2px 6px;border-radius:3px;background:rgba(255,255,255,0.05);color:#9ca3af">${typeLabel[s.type] || esc(s.type || '')}</div>
    </div>`).join('');
}

function renderActiveTemplates(active, missingSteps) {
  const container = document.getElementById('active-templates-container');
  if (!container) return;
  let html = '';
  // Missing step slots
  (missingSteps || []).forEach(s => {
    const sl = (s.label || '').replace(/'/g, "\\'");
    html += `
      <div style="border:2px dashed #2a2a2a;border-radius:8px;padding:24px;text-align:center;color:#6b7280">
        <div style="font-size:12px;font-family:'DM Mono',monospace;color:#4a4a4a;margin-bottom:4px">${esc(s.label)}</div>
        <div style="font-size:13px;color:#6b7280;margin-bottom:12px">No primary template assigned</div>
        <button onclick="window.showAddTemplateModal('${sl}')"
          style="padding:6px 14px;background:#1a1a1a;border:1px solid #2a2a2a;color:#9ca3af;border-radius:4px;cursor:pointer;font-size:12px">
          + Add Template for this step
        </button>
      </div>`;
  });
  active.forEach(t => { html += renderTemplateCard(t); });
  container.innerHTML = html || '<p style="color:#374151;font-size:13px">No active templates.</p>';
}

function renderBenchTemplates(bench) {
  const section = document.getElementById('bench-section');
  const container = document.getElementById('bench-templates-container');
  if (!section || !container) return;
  if (!bench.length) { section.style.display = 'none'; return; }
  section.style.display = '';
  container.innerHTML = bench.map(t => renderTemplateCard(t)).join('');
}

function renderTemplateCard(t) {
  const isPrimary  = !!t.is_primary;
  const hasAb      = !!(t.ab_test_enabled && t.ab_pair_id);
  const preview    = (t.body || t.body_a || '').slice(0, 100).replace(/\n/g, ' ');
  const tJson      = JSON.stringify(t).replace(/"/g, '&quot;');
  const stepLabel  = (t.sequence_step || '').replace(/'/g, "\\'");
  return `
    <div style="background:#111;border:1px solid ${isPrimary ? '#d4a847' : '#2a2a2a'};border-radius:8px;padding:16px 18px">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px">
        <div style="flex:1;min-width:0">
          <div style="color:#e5e7eb;font-size:14px;font-weight:500;margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(t.name || '—')}</div>
          <div style="color:#4b5563;font-size:11px;font-family:'DM Mono',monospace">
            ${t.sequence_step ? `<span style="color:#6b7280">${esc(t.sequence_step)}</span>` : '<span style="color:#3a3a3a">no step</span>'}
            ${t.type ? ` <span style="color:#3a3a3a">·</span> ${esc(t.type)}` : ''}
          </div>
        </div>
        ${isPrimary ? '<span style="background:#d4a847;color:#000;font-size:9px;font-weight:700;letter-spacing:0.08em;padding:2px 7px;border-radius:3px;white-space:nowrap;flex-shrink:0;align-self:flex-start;margin-left:8px">PRIMARY</span>' : ''}
      </div>
      ${t.subject_a ? `<div style="color:#6b7280;font-size:11px;margin-bottom:4px;font-style:italic;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(t.subject_a)}</div>` : ''}
      <div style="color:#4b5563;font-size:11px;line-height:1.5;margin-bottom:12px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden">${esc(preview)}${preview.length >= 100 ? '&#8230;' : ''}</div>
      <div style="display:flex;gap:6px;flex-wrap:wrap">
        ${!isPrimary ? `<button onclick="window.setPrimaryTemplate('${t.id}')" class="btn btn-ghost btn-sm" style="font-size:10px">Set Primary</button>` : ''}
        <button onclick="window.toggleAB('${t.id}','${stepLabel}')" class="btn btn-ghost btn-sm" style="font-size:10px${hasAb ? ';border-color:#d4a847;color:#d4a847' : ''}">A/B ${hasAb ? 'ON' : 'OFF'}</button>
        <button onclick="window.openEditTemplate(${tJson})" class="btn btn-ghost btn-sm" style="font-size:10px">Edit</button>
        <button onclick="window.deleteTemplate('${t.id}',${isPrimary})" class="btn btn-ghost btn-sm" style="font-size:10px;color:#ef4444;border-color:#7f1d1d">Delete</button>
      </div>
    </div>`;
}

function insertVariable(variable) {
  const textarea = document.getElementById('tmpl-body');
  if (!textarea) return;
  const start = textarea.selectionStart;
  const end   = textarea.selectionEnd;
  const val   = textarea.value;
  textarea.value = val.slice(0, start) + variable + val.slice(end);
  textarea.setSelectionRange(start + variable.length, start + variable.length);
  textarea.focus();
  debouncedPreview();
}

function debouncedPreview() {
  clearTimeout(previewDebounce);
  previewDebounce = setTimeout(updatePreview, 300);
}

function updatePreview() {
  if (!currentTemplate) return;
  const subjectA = document.getElementById('tmpl-subject-a')?.value || '';
  const subjectB = document.getElementById('tmpl-subject-b')?.value || '';
  const body     = document.getElementById('tmpl-body')?.value || '';
  const abOn     = document.getElementById('tmpl-ab-toggle')?.checked;
  renderPreview(currentTemplate, body, subjectA, subjectB, abOn);
}

function renderPreview(template, body, subjectA, subjectB, abOn) {
  const emptyEl = document.getElementById('template-preview-empty');
  const contentEl = document.getElementById('template-preview-content');
  if (!emptyEl || !contentEl) return;
  emptyEl.style.display   = 'none';
  contentEl.style.display = '';

  const subBWrap = document.getElementById('preview-subj-b-wrap');
  if (subBWrap) subBWrap.style.display = (abOn && subjectB) ? '' : 'none';

  document.getElementById('preview-subject-a').textContent = substituteSample(subjectA);
  document.getElementById('preview-subject-b').textContent = substituteSample(subjectB);

  const bodyEl = document.getElementById('preview-body');
  if (bodyEl) {
    bodyEl.innerHTML = substituteSample(body)
      .replace(/{{(\w+)}}/g, '<span class="preview-var">{{$1}}</span>')
      .replace(/\n/g, '<br>');
  }
}

function substituteSample(text) {
  if (!text) return '';
  return text.replace(/{{(\w+)}}/g, (_, key) => SAMPLE_DATA[key] || `{{${key}}}`);
}

/* ═══════════════════════════════════════════════════════════════════════════
   EDIT TEMPLATE MODAL
   ═══════════════════════════════════════════════════════════════════════════ */

window.openEditTemplate = function(template) {
  currentTemplate = template;
  const existing = document.getElementById('tmpl-edit-modal');
  if (existing) existing.remove();

  const isLinkedIn = template.type === 'linkedin';
  const modal = document.createElement('div');
  modal.id = 'tmpl-edit-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:1000;display:flex;align-items:center;justify-content:center';

  const subjectHtml = isLinkedIn ? '' : `
    <div class="form-group">
      <label class="form-label">Subject A</label>
      <input type="text" id="tmpl-subject-a" class="form-input" value="${esc(template.subject_a || template.subject || '')}" />
    </div>
    <div class="form-group">
      <label class="form-label">Subject B <span class="label-muted">(A/B test)</span></label>
      <input type="text" id="tmpl-subject-b" class="form-input" value="${esc(template.subject_b || '')}" />
    </div>`;

  modal.innerHTML = `
    <div style="background:#111;border:1px solid #2a2a2a;border-radius:8px;padding:32px;width:660px;max-height:85vh;overflow-y:auto">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:24px">
        <h3 style="color:#e5e7eb;margin:0;font-family:'Cormorant Garamond',serif;font-size:20px">${esc(template.name || 'Edit Template')}</h3>
        <button onclick="document.getElementById('tmpl-edit-modal').remove()" style="background:none;border:none;color:#6b7280;cursor:pointer;font-size:24px;line-height:1;padding:0">&#215;</button>
      </div>
      ${subjectHtml}
      <div class="form-group">
        <label class="form-label">Body</label>
        <textarea id="tmpl-body" class="form-textarea tmpl-body-area" rows="12">${esc(template.body_a || template.body || '')}</textarea>
      </div>
      <div class="form-group">
        <label class="form-label">Variables <span class="label-muted">(click to insert)</span></label>
        <div class="variable-chips" id="variable-chips">${TEMPLATE_VARIABLES.map(v => `<span class="var-chip" onclick="insertVariable('${v}')">${v}</span>`).join('')}</div>
      </div>
      <div class="tmpl-toggles">
        <div class="toggle-row-inline">
          <span class="toggle-label">A/B Testing</span>
          <label class="toggle-switch">
            <input type="checkbox" id="tmpl-ab-toggle" ${template.ab_test_enabled ? 'checked' : ''} />
            <span class="toggle-track"></span>
          </label>
        </div>
        <div class="toggle-row-inline">
          <span class="toggle-label">Template Active</span>
          <label class="toggle-switch">
            <input type="checkbox" id="tmpl-active-toggle" ${(template.is_active !== false) ? 'checked' : ''} />
            <span class="toggle-track"></span>
          </label>
        </div>
      </div>
      <div class="form-group">
        <label class="form-label">Notes</label>
        <textarea id="tmpl-notes" class="form-textarea" rows="2" placeholder="Internal notes&#8230;">${esc(template.notes || '')}</textarea>
      </div>
      <div class="tmpl-action-row">
        <button id="tmpl-save-btn" class="btn btn-gold" onclick="saveTemplate()">Save</button>
        <button class="btn btn-ghost" onclick="document.getElementById('tmpl-edit-modal').remove()">Cancel</button>
      </div>
    </div>`;

  document.body.appendChild(modal);
  modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
};

async function saveTemplate() {
  if (!currentTemplate) return;
  const id = currentTemplate.id || currentTemplate._id;
  const isLinkedIn = currentTemplate.type === 'linkedin';
  const payload = {
    subject_a:       isLinkedIn ? null : (document.getElementById('tmpl-subject-a')?.value || null),
    subject_b:       isLinkedIn ? null : (document.getElementById('tmpl-subject-b')?.value || null),
    body:            document.getElementById('tmpl-body')?.value || '',
    ab_test_enabled: !!(document.getElementById('tmpl-ab-toggle')?.checked),
    is_active:       !!(document.getElementById('tmpl-active-toggle')?.checked ?? true),
    notes:           document.getElementById('tmpl-notes')?.value || '',
  };
  const btn = document.getElementById('tmpl-save-btn');
  const origText = btn?.textContent;
  if (btn) { btn.textContent = 'Saving&#8230;'; btn.disabled = true; }
  try {
    await api(`/api/templates/${id}`, 'PATCH', payload);
    document.getElementById('tmpl-edit-modal')?.remove();
    showToast('Template saved');
    await loadTemplatesPage();
  } catch (err) {
    showToast(`Save failed: ${err.message}`, 'error');
    if (btn) { btn.textContent = origText; btn.disabled = false; }
  }
}

/* ═══════════════════════════════════════════════════════════════════════════
   ADD TEMPLATE MODAL
   ═══════════════════════════════════════════════════════════════════════════ */

window.showAddTemplateModal = async function(prefilledStep) {
  const existing = document.getElementById('add-template-modal');
  if (existing) existing.remove();

  // Load current sequence steps for the dropdown
  let sequenceSteps = [];
  try {
    const seqData = await api('/api/sequence');
    sequenceSteps = seqData?.steps || [];
  } catch {}

  const stepOptions = sequenceSteps.length
    ? sequenceSteps.map(s => `<option value="${esc(s.label)}" ${prefilledStep === s.label ? 'selected' : ''}>${esc(s.label)} (${esc(s.type || s.channel || '')})</option>`).join('')
    : '';

  const modal = document.createElement('div');
  modal.id = 'add-template-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.7);z-index:1000;display:flex;align-items:center;justify-content:center';

  modal.innerHTML = `
    <div style="background:#111;border:1px solid #2a2a2a;border-radius:8px;padding:32px;width:560px;max-height:80vh;overflow-y:auto">
      <h3 style="color:#e5e7eb;margin:0 0 24px;font-family:'Cormorant Garamond',serif;font-size:20px">Add Template</h3>

      <div style="margin-bottom:16px">
        <label style="color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:6px">Template Name</label>
        <input id="new-tmpl-name" type="text" placeholder="e.g. Email Follow Up 2"
          style="width:100%;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;color:#e5e7eb;border-radius:6px;box-sizing:border-box">
      </div>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">
        <div>
          <label style="color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:6px">Type</label>
          <select id="new-tmpl-type" onchange="window.toggleSubjectFields()"
            style="width:100%;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;color:#e5e7eb;border-radius:6px">
            <option value="email">Email</option>
            <option value="linkedin">LinkedIn</option>
          </select>
        </div>
        <div>
          <label style="color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:6px">Sequence Step</label>
          <select id="new-tmpl-step"
            style="width:100%;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;color:#e5e7eb;border-radius:6px">
            <option value="">-- Bench (unassigned) --</option>
            ${stepOptions}
          </select>
        </div>
      </div>

      <div id="new-tmpl-subject-section">
        <div style="margin-bottom:16px">
          <label style="color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:6px">Subject A</label>
          <input id="new-tmpl-subject-a" type="text"
            style="width:100%;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;color:#e5e7eb;border-radius:6px;box-sizing:border-box">
        </div>
        <div style="margin-bottom:16px">
          <label style="color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:6px">Subject B <span style="color:#4a4a4a;font-size:10px">(A/B test, optional)</span></label>
          <input id="new-tmpl-subject-b" type="text"
            style="width:100%;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;color:#e5e7eb;border-radius:6px;box-sizing:border-box">
        </div>
      </div>

      <div style="margin-bottom:24px">
        <label style="color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:0.1em;display:block;margin-bottom:6px">Message Body</label>
        <textarea id="new-tmpl-body" rows="8" placeholder="Use {{firstName}}, {{dealName}}, {{firm}}, etc."
          style="width:100%;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;color:#e5e7eb;border-radius:6px;box-sizing:border-box;font-family:'DM Mono',monospace;font-size:12px;resize:vertical"></textarea>
      </div>

      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button onclick="document.getElementById('add-template-modal').remove()"
          style="padding:8px 16px;background:#1a1a1a;border:1px solid #2a2a2a;color:#6b7280;border-radius:6px;cursor:pointer">Cancel</button>
        <button onclick="window.saveNewTemplate()"
          style="padding:8px 20px;background:#d4a847;border:none;color:#000;border-radius:6px;cursor:pointer;font-weight:600">Save Template</button>
      </div>
    </div>`;

  document.body.appendChild(modal);
  modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
};

window.toggleSubjectFields = function() {
  const type = document.getElementById('new-tmpl-type')?.value;
  const section = document.getElementById('new-tmpl-subject-section');
  if (section) section.style.display = type === 'linkedin' ? 'none' : 'block';
};

window.saveNewTemplate = async function() {
  const name         = document.getElementById('new-tmpl-name')?.value?.trim();
  const type         = document.getElementById('new-tmpl-type')?.value;
  const sequenceStep = document.getElementById('new-tmpl-step')?.value?.trim() || null;
  const subjectA     = document.getElementById('new-tmpl-subject-a')?.value?.trim();
  const subjectB     = document.getElementById('new-tmpl-subject-b')?.value?.trim();
  const body         = document.getElementById('new-tmpl-body')?.value?.trim();

  if (!name || !body) { showToast('Name and body are required', 'error'); return; }

  if (body.includes('\u2014') || (subjectA || '').includes('\u2014') || (subjectB || '').includes('\u2014')) {
    showToast('Remove em dashes (\u2014) from the template', 'error');
    return;
  }

  try {
    const data = await api('/api/templates', 'POST', {
      name, type,
      sequence_step:   sequenceStep || null,
      subject_a:       type === 'email' ? (subjectA || null) : null,
      subject_b:       type === 'email' ? (subjectB || null) : null,
      body,
      is_active:       true,
      ab_test_enabled: !!(subjectA && subjectB),
    });
    if (data && data.success) {
      document.getElementById('add-template-modal')?.remove();
      showToast(`Template "${name}" saved`);
      loadTemplatesPage();
    } else {
      showToast('Save failed: ' + (data?.error || 'Unknown'), 'error');
    }
  } catch (err) {
    showToast('Save failed: ' + err.message, 'error');
  }
};

/* ═══════════════════════════════════════════════════════════════════════════
   SEQUENCE EDITOR MODAL
   ═══════════════════════════════════════════════════════════════════════════ */

function _seqStepDescription(type) {
  return { email: 'Email outreach', linkedin_invite: 'LinkedIn connection request', linkedin_dm: 'LinkedIn DM (after connection accepted)' }[type] || type;
}

function collectSequenceSteps() {
  const rows = document.querySelectorAll('.sequence-step-row');
  const steps = [];
  rows.forEach((row, index) => {
    const label     = row.querySelector('.seq-label-input')?.value?.trim();
    const type      = row.querySelector('.seq-type-select')?.value;
    const delayDays = parseInt(row.querySelector('.seq-day-input')?.value || '0', 10);
    if (label && type) {
      steps.push({
        step:        index + 1,
        label,
        type,
        delay_days:  isNaN(delayDays) ? 0 : delayDays,
        description: _seqStepDescription(type),
      });
    }
  });
  return steps;
}

function _renderSeqStepRow(step) {
  const t = step.type || 'email';
  return `
    <div class="sequence-step-row" style="display:grid;grid-template-columns:1fr 160px 80px 32px;
              gap:8px;align-items:center;padding:8px;background:#1a1a1a;border:1px solid #2a2a2a;border-radius:6px">
      <input type="text" class="seq-label-input"
        value="${esc(step.label || '')}" placeholder="e.g. email_intro"
        style="padding:6px 10px;background:#0f0f0f;border:1px solid #2a2a2a;color:#e5e7eb;
               border-radius:4px;font-family:'DM Mono',monospace;font-size:12px;outline:none">
      <select class="seq-type-select"
        style="padding:6px 10px;background:#0f0f0f;border:1px solid #2a2a2a;color:#e5e7eb;border-radius:4px;font-size:12px;outline:none">
        <option value="email"           ${t === 'email'           ? 'selected' : ''}>Email</option>
        <option value="linkedin_invite" ${t === 'linkedin_invite' ? 'selected' : ''}>LinkedIn Invite</option>
        <option value="linkedin_dm"     ${t === 'linkedin_dm'     ? 'selected' : ''}>LinkedIn DM</option>
      </select>
      <input type="number" class="seq-day-input"
        value="${step.delay_days !== undefined ? step.delay_days : 0}" min="0" max="365"
        style="padding:6px 8px;background:#0f0f0f;border:1px solid #2a2a2a;color:#e5e7eb;
               border-radius:4px;font-size:12px;outline:none;text-align:center">
      <button onclick="this.closest('.sequence-step-row').remove()"
        style="background:none;border:none;color:#6b7280;cursor:pointer;font-size:20px;line-height:1;padding:0">&#215;</button>
    </div>`;
}

window.showSequenceEditor = async function() {
  const existing = document.getElementById('seq-editor-modal');
  if (existing) existing.remove();

  let seqData = {};
  try { seqData = await api('/api/sequence'); } catch {}

  let currentSteps = seqData?.steps || [];
  if (!currentSteps.length) {
    currentSteps = [
      { label: 'email_intro',      type: 'email',           delay_days: 0  },
      { label: 'linkedin_invite',  type: 'linkedin_invite', delay_days: 0  },
      { label: 'linkedin_dm_1',    type: 'linkedin_dm',     delay_days: 0  },
      { label: 'email_followup_1', type: 'email',           delay_days: 7  },
      { label: 'linkedin_dm_2',    type: 'linkedin_dm',     delay_days: 0  },
      { label: 'email_followup_2', type: 'email',           delay_days: 14 },
    ];
  }

  const sw = seqData?.sending_window || { start_hour: 8, end_hour: 18, days: [1, 2, 3, 4, 5] };

  const modal = document.createElement('div');
  modal.id = 'seq-editor-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:1000;display:flex;align-items:center;justify-content:center';

  const dayNames = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
  const dayButtonsHtml = dayNames.map((d, i) => {
    const on = (sw.days || []).includes(i + 1);
    return `<button data-day="${i+1}" data-active="${on}" onclick="window.toggleSendDay(this)"
      style="padding:4px 8px;border-radius:4px;cursor:pointer;font-size:11px;
             border:1px solid ${on ? '#d4a847' : '#2a2a2a'};
             background:${on ? 'rgba(212,168,71,0.1)' : '#0f0f0f'};
             color:${on ? '#d4a847' : '#6b7280'}">${d}</button>`;
  }).join('');

  const constantsHtml = `
    <div style="margin-bottom:20px;padding:16px;background:#0f1a0f;border:1px solid #1a2a1a;border-radius:6px">
      <div style="color:#4a7a4a;font-size:10px;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:10px">Always On — Cannot Be Changed</div>
      ${[
        { label: 'email_intro',      type: 'Email',    note: 'Day 0 — within sending window' },
        { label: 'linkedin_invite',  type: 'LI Invite', note: 'Day 0 — within sending window' },
        { label: 'unipile_webhook',  type: 'Webhook',  note: 'Always listening' },
        { label: 'firm_suppression', type: 'Rule',     note: 'Any reply suppresses the firm' },
      ].map(c => `
        <div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid #1a2a1a">
          <span style="font-family:'DM Mono',monospace;font-size:12px;color:#6b9a6b">${c.label}</span>
          <span style="font-size:11px;color:#4a4a4a">${c.type} — ${c.note}</span>
        </div>`).join('')}
    </div>`;

  const sendingWindowHtml = `
    <div style="margin-top:20px;padding:16px;background:#1a1a1a;border:1px solid #2a2a2a;border-radius:6px">
      <div style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:12px">Sending Window</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
        <div>
          <label style="color:#6b7280;font-size:11px;display:block;margin-bottom:6px">Send between (local time)</label>
          <div style="display:flex;gap:8px;align-items:center">
            <input id="sw-start" type="number" min="6" max="22" value="${sw.start_hour}"
              style="width:56px;padding:6px;background:#0f0f0f;border:1px solid #2a2a2a;color:#e5e7eb;border-radius:4px;text-align:center">
            <span style="color:#6b7280">to</span>
            <input id="sw-end" type="number" min="6" max="22" value="${sw.end_hour}"
              style="width:56px;padding:6px;background:#0f0f0f;border:1px solid #2a2a2a;color:#e5e7eb;border-radius:4px;text-align:center">
            <span style="color:#6b7280;font-size:12px">:00</span>
          </div>
        </div>
        <div>
          <label style="color:#6b7280;font-size:11px;display:block;margin-bottom:6px">Active days</label>
          <div style="display:flex;gap:4px;flex-wrap:wrap">${dayButtonsHtml}</div>
        </div>
      </div>
    </div>`;

  modal.innerHTML = `
    <div style="background:#111;border:1px solid #2a2a2a;border-radius:8px;padding:28px;width:700px;max-height:85vh;overflow-y:auto">
      <h3 style="color:#e5e7eb;margin:0 0 6px;font-family:'Cormorant Garamond',serif;font-size:20px">Edit Outreach Sequence</h3>
      <p style="color:#6b7280;font-size:13px;margin:0 0 16px">Steps run in order. "Day" = days after first contact before this step fires.</p>

      ${constantsHtml}

      <div style="display:grid;grid-template-columns:1fr 160px 80px 32px;gap:8px;margin-bottom:8px;padding:0 4px">
        <span style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.1em">Step Label</span>
        <span style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.1em">Channel</span>
        <span style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.1em">Day</span>
        <span></span>
      </div>

      <div id="sequence-steps-container" style="display:flex;flex-direction:column;gap:8px">
        ${currentSteps.map(s => _renderSeqStepRow(s)).join('')}
      </div>

      <button onclick="window.addSequenceStep()"
        style="margin-top:12px;padding:8px 16px;background:#1a1a1a;border:1px dashed #2a2a2a;
               color:#6b7280;border-radius:6px;cursor:pointer;font-size:12px;width:100%">
        + Add Step
      </button>

      ${sendingWindowHtml}

      <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:20px">
        <button onclick="document.getElementById('seq-editor-modal').remove()" class="btn btn-ghost">Cancel</button>
        <button onclick="window.saveSequence()" class="btn btn-gold">Save Sequence</button>
      </div>
    </div>`;

  document.body.appendChild(modal);
  modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
};

window.toggleSendDay = function(btn) {
  const on = btn.dataset.active === 'true';
  btn.dataset.active         = String(!on);
  btn.style.border           = !on ? '1px solid #d4a847' : '1px solid #2a2a2a';
  btn.style.background       = !on ? 'rgba(212,168,71,0.1)' : '#0f0f0f';
  btn.style.color            = !on ? '#d4a847' : '#6b7280';
};

window.addSequenceStep = function() {
  const container = document.getElementById('sequence-steps-container');
  if (!container) return;
  const div = document.createElement('div');
  div.innerHTML = _renderSeqStepRow({ label: '', type: 'email', delay_days: 0 });
  container.appendChild(div.firstElementChild);
};

window.saveSequence = async function() {
  const steps = collectSequenceSteps();
  if (!steps.length) { showToast('Add at least one step', 'error'); return; }
  const labels = steps.map(s => s.label).filter(Boolean);
  if (new Set(labels).size !== labels.length) { showToast('Each step label must be unique', 'error'); return; }

  const startHour  = parseInt(document.getElementById('sw-start')?.value || '8', 10);
  const endHour    = parseInt(document.getElementById('sw-end')?.value   || '18', 10);
  const activeDays = [...document.querySelectorAll('[data-day][data-active="true"]')]
    .map(b => parseInt(b.dataset.day, 10));
  const sending_window = { start_hour: startHour, end_hour: endHour, days: activeDays };

  try {
    await api('/api/sequence', 'PUT', { steps, sending_window });
    document.getElementById('seq-editor-modal')?.remove();
    showToast('Sequence saved');
    await loadTemplatesPage();
  } catch (err) { showToast('Failed: ' + err.message, 'error'); }
};

/* ═══════════════════════════════════════════════════════════════════════════
   TEMPLATE CARD ACTIONS
   ═══════════════════════════════════════════════════════════════════════════ */

window.setPrimaryTemplate = async function(templateId) {
  try {
    await api(`/api/templates/${templateId}/primary`, 'PATCH');
    showToast('Primary template updated');
    await loadTemplatesPage();
  } catch (err) { showToast('Failed: ' + err.message, 'error'); }
};

window.deleteTemplate = async function(templateId, isPrimary) {
  if (isPrimary) { showToast('Cannot delete the primary template — set another as primary first', 'error'); return; }
  if (!confirm('Delete this template? This cannot be undone.')) return;
  try {
    await api(`/api/templates/${templateId}`, 'DELETE');
    showToast('Template deleted');
    await loadTemplatesPage();
  } catch (err) { showToast('Failed: ' + err.message, 'error'); }
};

window.toggleAB = async function(templateId, sequenceStep) {
  if (!sequenceStep) { showToast('Template must be assigned to a sequence step first', 'error'); return; }
  try {
    // Fetch all primary templates for the same step
    const url = `/api/templates?sequence_step=${encodeURIComponent(sequenceStep)}`;
    const data = await api(url);
    const all = Array.isArray(data) ? data : (data?.templates || []);
    const others = all.filter(t => t.id !== templateId && t.is_primary);

    if (!others.length) {
      showToast(`Add another ${sequenceStep} template and set it as primary to enable A/B testing`, 'info');
      return;
    }

    // Toggle: if either template has ab_test_enabled, turn both off; otherwise turn both on
    const currentlyOn = others[0].ab_test_enabled;
    const enable  = !currentlyOn;
    const pairId  = enable ? (crypto.randomUUID ? crypto.randomUUID() : Date.now().toString(36)) : null;

    await Promise.all([
      api(`/api/templates/${templateId}/ab`, 'PATCH', { ab_test_enabled: enable, ab_pair_id: pairId }),
      api(`/api/templates/${others[0].id}/ab`, 'PATCH', { ab_test_enabled: enable, ab_pair_id: pairId }),
    ]);

    showToast(enable ? 'A/B testing enabled' : 'A/B testing disabled');
    await loadTemplatesPage();
  } catch (err) { showToast('Failed: ' + err.message, 'error'); }
};

/* ═══════════════════════════════════════════════════════════════════════════
   TRAIN YOUR AGENT
   ═══════════════════════════════════════════════════════════════════════════ */

const INVESTOR_DEFAULTS = {
  identity:          'Roco is acting as a knowledgeable representative of the deal. It approaches investors with confidence and authority — as if it already knows their portfolio and believes this deal belongs in it.',
  voice_guidance:    'Conversational and direct. No em-dashes. No corporate language. No "I hope this finds you well." Sound like a trusted colleague, not a pitch deck. Always use first name. Sign off as Dom.',
  search_guidance:   'Prioritise investors who have made 2+ investments in this sector in the last 24 months. Look for signals of active deal flow. Research their latest fund announcements. Avoid investors clearly focused on stages outside the raise target.',
  research_guidance: 'For each investor, find their 3 most recent portfolio companies in this sector. Find their stated thesis from their website or public interviews. Look for any press coverage about their investment pace or deal preferences.',
  scoring_guidance:  'Sector fit: 30pts. Cheque size match: 25pts. Geography: 20pts. Portfolio overlap: 15pts. Activity recency: 10pts. If cheque size is clearly outside range — archive immediately regardless of other scores.',
  outreach_guidance: 'Always reference a specific past investment from their portfolio. Get to the deal in the first two sentences. Never send a generic opener. The reference must be real — pulled from actual research.',
  reply_guidance:    'When they reply with interest, ask one qualifying question per message. Do not ask multiple at once. For investors: confirm cheque size appetite, current deal flow timeline, and any sector concerns. Move toward a call as soon as interest is confirmed.',
  closing_guidance:  'Once qualifying questions are answered and interest is confirmed, push directly for a 20-minute call. If no response after two follow-ups, close the thread graciously and archive. Never ghost — always close cleanly.',
};

const SOURCING_DEFAULTS = {
  identity:          'Roco is acting as a representative of the investment firm. It approaches company founders and executives as a firm that has already identified their business as a potential fit — with genuine interest, not a generic approach.',
  voice_guidance:    'Confident, peer-to-peer. Sound like an investor who already knows the market and has done their homework. No em-dashes. No pitch language. Reference their actual product and company specifically.',
  search_guidance:   'Look for companies with active signals: hiring senior roles, announcing growth milestones, raising awareness through content, or expressing openness to capital. Avoid companies that have recently closed a large round at a stage incompatible with the thesis.',
  research_guidance: 'For each company, understand exactly what their product does and who their customers are. Find revenue estimates if available. Look for founding team background and any public press. Identify the clearest reason this company fits the investment thesis.',
  scoring_guidance:  'Sector and thesis fit: 30pts. Financial criteria match: 25pts. Geography: 20pts. Ownership and stage fit: 15pts. Intent signal strength: 10pts. If clearly outside financial criteria — archive without outreach.',
  outreach_guidance: 'Reference something specific about their product or a recent signal (hiring, press, growth announcement). Position the firm as an interested party that has identified them specifically — not a cold blast. Be direct about what the firm does and why they reached out.',
  reply_guidance:    'When a founder replies, ask about current revenue run rate, ownership structure, and openness to exploring investment. One question per message. Gauge their interest level and timeline. Move toward a call as soon as they confirm interest.',
  closing_guidance:  'Once they\'ve shown genuine interest and confirmed they\'re open to exploring, push for a 30-minute introductory call. Frame it as exploratory — no commitment. If they go cold after two follow-ups, close politely and archive.',
};

const TRAIN_FIELDS = ['identity', 'voice_guidance', 'search_guidance', 'research_guidance', 'scoring_guidance', 'outreach_guidance', 'reply_guidance', 'closing_guidance'];

const TRAIN_CHAR_LIMITS = {
  identity:          2000,
  voice_guidance:    1800,
  search_guidance:   1800,
  research_guidance: 1800,
  scoring_guidance:  1800,
  outreach_guidance: 1800,
  reply_guidance:    1800,
  closing_guidance:  1800,
};

const TRAIN_FIELD_LABELS = {
  identity:          'Identity & Persona',
  voice_guidance:    'Voice & Tone',
  search_guidance:   'Search Guidance',
  research_guidance: 'Research Guidance',
  scoring_guidance:  'Scoring Guidance',
  outreach_guidance: 'Outreach Guidance',
  reply_guidance:    'Reply & Conversation Guidance',
  closing_guidance:  'Closing Guidance',
};

function updateTrainCharCount(mode, field, value) {
  const limit = TRAIN_CHAR_LIMITS[field] || 1800;
  const count = (value || '').length;
  const remaining = limit - count;
  const el = document.getElementById(`char-count-train-${mode}-${field}`);
  if (!el) return;
  el.textContent = `${count} / ${limit}`;
  el.style.color = remaining < 100 ? '#ef4444' : remaining < 300 ? '#f59e0b' : '#6b7280';
}

let trainCurrentTab = 'investor';

async function loadTrainYourAgent() {
  try {
    // Fetch both in parallel
    const [invRes, srcRes] = await Promise.all([
      api('/api/guidance/investor'),
      api('/api/guidance/sourcing'),
    ]);

    populateTrainForm('investor', invRes?.data || {}, INVESTOR_DEFAULTS);
    populateTrainForm('sourcing', srcRes?.data || {}, SOURCING_DEFAULTS);

    // Show last updated timestamps
    showTrainUpdated('investor', invRes?.data?.updated_at);
    showTrainUpdated('sourcing', srcRes?.data?.updated_at);

    // Init auto-resize for all textareas
    document.querySelectorAll('.train-auto-resize').forEach(el => {
      autoResizeTextarea(el);
      el.addEventListener('input', () => autoResizeTextarea(el));
    });
  } catch (err) {
    console.error('[TRAIN] Failed to load guidance:', err);
  }
}

function populateTrainForm(mode, data, defaults) {
  const fieldMap = {
    identity:          `train-${mode}-identity`,
    voice_guidance:    `train-${mode}-voice`,
    search_guidance:   `train-${mode}-search`,
    research_guidance: `train-${mode}-research`,
    scoring_guidance:  `train-${mode}-scoring`,
    outreach_guidance: `train-${mode}-outreach`,
    reply_guidance:    `train-${mode}-reply`,
    closing_guidance:  `train-${mode}-closing`,
  };

  for (const [field, elId] of Object.entries(fieldMap)) {
    const el = document.getElementById(elId);
    if (!el) continue;
    // Use DB value if set; otherwise use default
    el.value = data[field] || defaults[field] || '';
    autoResizeTextarea(el);
    updateTrainCharCount(mode, field, el.value);
    el.addEventListener('input', () => {
      autoResizeTextarea(el);
      updateTrainCharCount(mode, field, el.value);
    });
  }
}

function showTrainUpdated(mode, updatedAt) {
  const el = document.getElementById(`train-${mode}-updated`);
  if (!el) return;
  if (updatedAt) {
    const d = new Date(updatedAt);
    el.textContent = `Last updated: ${d.toLocaleString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' })}`;
    el.style.display = 'block';
  } else {
    el.style.display = 'none';
  }
}

function autoResizeTextarea(el) {
  el.style.height = 'auto';
  const minH = parseInt(getComputedStyle(el).minHeight) || 0;
  el.style.height = Math.max(el.scrollHeight + 2, minH) + 'px';
}

function switchTrainTab(tab) {
  trainCurrentTab = tab;

  document.getElementById('train-form-investor').style.display = tab === 'investor' ? 'block' : 'none';
  document.getElementById('train-form-sourcing').style.display = tab === 'sourcing' ? 'block' : 'none';

  const inv = document.getElementById('train-tab-investor');
  const src = document.getElementById('train-tab-sourcing');
  const activeStyle   = { color: 'var(--gold)', borderBottom: '2px solid var(--gold)' };
  const inactiveStyle = { color: 'var(--text-dim)', borderBottom: '2px solid transparent' };

  Object.assign(inv.style, tab === 'investor' ? activeStyle : inactiveStyle);
  Object.assign(src.style, tab === 'sourcing' ? activeStyle : inactiveStyle);
}

async function saveTrainGuidance(mode) {
  const statusEl = document.getElementById(`train-${mode}-save-status`);
  if (statusEl) statusEl.textContent = 'Saving…';

  const fieldMap = {
    identity:          `train-${mode}-identity`,
    voice_guidance:    `train-${mode}-voice`,
    search_guidance:   `train-${mode}-search`,
    research_guidance: `train-${mode}-research`,
    scoring_guidance:  `train-${mode}-scoring`,
    outreach_guidance: `train-${mode}-outreach`,
    reply_guidance:    `train-${mode}-reply`,
    closing_guidance:  `train-${mode}-closing`,
  };

  const payload = {};
  for (const [field, elId] of Object.entries(fieldMap)) {
    const el = document.getElementById(elId);
    if (!el) continue;
    const val = el.value.trim();
    const limit = TRAIN_CHAR_LIMITS[field] || 1800;
    if (val.length > limit) {
      if (statusEl) {
        statusEl.textContent = `${TRAIN_FIELD_LABELS[field] || field} must be under ${limit} characters (currently ${val.length})`;
        statusEl.style.color = 'var(--red, #f87171)';
      }
      return;
    }
    payload[field] = val || null;
  }

  try {
    const result = await api(`/api/guidance/${mode}`, 'POST', payload);
    if (statusEl) {
      statusEl.textContent = '✓ Guidance saved — active on next Roco cycle';
      statusEl.style.color = 'var(--green, #4ade80)';
      setTimeout(() => { if (statusEl) { statusEl.textContent = ''; statusEl.style.color = ''; } }, 4000);
    }
    showTrainUpdated(mode, result?.data?.updated_at || new Date().toISOString());
  } catch (err) {
    if (statusEl) {
      statusEl.textContent = `Save failed: ${err.message}`;
      statusEl.style.color = 'var(--red, #f87171)';
    }
  }
}

/* ═══════════════════════════════════════════════════════════════════════════
   CONTROLS
   ═══════════════════════════════════════════════════════════════════════════ */

async function loadControls() {
  try {
    const state = await api('/api/state');
    applyState(state);

    // Populate all deal selectors (schedule + research + enrichment)
    const deals = allDeals.length ? allDeals : ((await api('/api/deals').catch(() => [])) || []);
    for (const selId of ['schedule-deal-select', 'research-deal-select', 'enrichment-deal-select']) {
      const sel = document.getElementById(selId);
      if (!sel || !deals.length) continue;
      const prev = sel.value;
      sel.innerHTML = '<option value="">Select deal\u2026</option>';
      deals.forEach(d => {
        const opt = document.createElement('option');
        opt.value = d.id || d._id;
        opt.textContent = d.dealName || d.name;
        sel.appendChild(opt);
      });
      if (prev) sel.value = prev;
    }
  } catch { /* silent */ }
}

async function onSwitchChange(key, checked) {
  try {
    await api('/api/toggle', 'POST', { key, value: checked });
  } catch (err) {
    // revert
    const input = document.querySelector(`[data-key="${key}"]`);
    if (input) input.checked = !checked;
    alert(`Failed: ${err.message}`);
  }
}

async function triggerAction(action) {
  // Map frontend hyphen-names to server underscore-names
  const serverAction = action.replace(/-/g, '_').replace('24h', '').replace(/_$/, '');
  const labels = {
    'run_research':   'Run Research',
    'run_enrichment': 'Run Enrichment',
    'flush_queue':    'Flush Approval Queue — this will skip all pending approvals',
    'pause_all':      'Pause All Outreach for 24h',
  };

  // For run_research, require a deal to be selected
  if (serverAction === 'run_research') {
    const sel = document.getElementById('research-deal-select');
    const dealId = sel?.value;
    if (!dealId) { alert('Please select a deal to run research for.'); return; }
    if (!confirm(`Run new research for "${sel.options[sel.selectedIndex]?.text}"?`)) return;
    try {
      const res = await api('/api/action', 'POST', { action: 'run_research', dealId });
      showToast?.(res.message || 'Research started');
    } catch (err) { alert(`Action failed: ${err.message}`); }
    return;
  }

  // For run_enrichment, require a deal to be selected
  if (serverAction === 'run_enrichment') {
    const sel = document.getElementById('enrichment-deal-select');
    const dealId = sel?.value;
    if (!dealId) { alert('Please select a deal to run enrichment for.'); return; }
    if (!confirm(`Run enrichment for "${sel.options[sel.selectedIndex]?.text}"?\n\nThis will find email addresses for all Ranked contacts in this deal.`)) return;
    try {
      const res = await api('/api/action', 'POST', { action: 'run_enrichment', dealId });
      showToast?.(res.message || 'Enrichment started');
    } catch (err) { alert(`Action failed: ${err.message}`); }
    return;
  }

  if (!confirm(`Execute: ${labels[serverAction] || action}?`)) return;
  try {
    const res = await api('/api/action', 'POST', { action: serverAction });
    if (serverAction === 'pause_all') {
      const until = new Date(Date.now() + 86_400_000);
      renderPauseActive(until.toISOString());
    }
    if (res?.message) showToast?.(res.message);
  } catch (err) { alert(`Action failed: ${err.message}`); }
}

async function setGlobalPause() {
  const input = document.getElementById('pause-until-input');
  if (!input?.value) { alert('Select a date/time first.'); return; }
  const until = new Date(input.value).toISOString();
  try {
    await api('/api/pause-outreach', 'POST', { until });
    renderPauseActive(until);
    input.value = '';
  } catch (err) { alert(`Failed: ${err.message}`); }
}

async function clearGlobalPause() {
  try {
    await api('/api/pause-outreach', 'POST', { until: null });
    clearPauseDisplay();
  } catch (err) { alert(`Failed: ${err.message}`); }
}

function renderPauseActive(until) {
  const untilDate = new Date(until);
  const block = document.getElementById('pause-active-block');
  const form  = document.getElementById('pause-set-form');
  const dispEl = document.getElementById('pause-until-display');

  if (block) block.style.display = '';
  if (form) form.style.display = 'none';
  if (dispEl) dispEl.textContent = untilDate.toLocaleString('en-US', { dateStyle: 'medium', timeStyle: 'short', timeZone: DOM_TZ });

  // Start countdown
  if (pauseCountTimer) clearInterval(pauseCountTimer);
  const timerEl = document.getElementById('pause-countdown-timer');
  const tick = () => {
    const diff = untilDate - Date.now();
    if (!timerEl) return;
    if (diff <= 0) {
      clearInterval(pauseCountTimer);
      timerEl.textContent = 'Expired';
      clearPauseDisplay();
      return;
    }
    const h = Math.floor(diff / 3_600_000);
    const m = Math.floor((diff % 3_600_000) / 60_000);
    timerEl.textContent = `${String(h).padStart(2,'0')}h ${String(m).padStart(2,'0')}m remaining`;
  };
  tick();
  pauseCountTimer = setInterval(tick, 60_000);
}

function clearPauseDisplay() {
  if (pauseCountTimer) { clearInterval(pauseCountTimer); pauseCountTimer = null; }
  const block = document.getElementById('pause-active-block');
  const form  = document.getElementById('pause-set-form');
  if (block) block.style.display = 'none';
  if (form) form.style.display = '';
}

/* ─── SCHEDULE VISUALIZER ────────────────────────────────────────────────── */

async function loadDealSchedule(dealId) {
  const content = document.getElementById('schedule-content');
  if (!dealId) { if (content) content.style.display = 'none'; return; }
  if (content) content.style.display = '';

  try {
    const data = await api(`/api/deals/${dealId}/schedule`);
    renderScheduleViz(data);
    renderWindowStatus(data);
  } catch { /* silent */ }
}

function renderScheduleViz(data) {
  const el = document.getElementById('schedule-viz');
  if (!el) return;

  const DAYS    = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
  const windows = data?.sendingWindows || data?.windows || {};
  const startH  = data?.startHour ?? 8;
  const endH    = data?.endHour   ?? 18;

  el.innerHTML = `
    <div class="schedule-timeline-labels" style="margin-left:40px;max-width:calc(100% - 40px)">
      ${Array.from({length:7}, (_,i) => `<span>${(i*4).toString().padStart(2,'0')}:00</span>`).join('')}
    </div>
    ${DAYS.map((day, idx) => {
      const key      = day.toLowerCase();
      const active   = windows[key] !== false && windows[idx] !== false;
      const dayStart = startH / 24 * 100;
      const dayEnd   = endH   / 24 * 100;
      const width    = dayEnd - dayStart;
      return `<div class="schedule-row">
        <span class="schedule-day-label">${day}</span>
        <div class="schedule-timeline">
          ${active ? `<div class="schedule-window" style="left:${dayStart}%;width:${width}%"></div>` : ''}
        </div>
      </div>`;
    }).join('')}
  `;
}

function renderWindowStatus(data) {
  const el = document.getElementById('schedule-cadence');
  if (!el || !data) return;
  el.innerHTML = `
    <div class="cadence-row">
      <span class="cadence-label">Send Window</span>
      <span class="cadence-val">${data.startHour ?? 8}:00 – ${data.endHour ?? 18}:00</span>
    </div>
    <div class="cadence-row">
      <span class="cadence-label">Timezone</span>
      <span class="cadence-val">${esc(data.timezone || 'Europe/London')}</span>
    </div>
    <div class="cadence-row">
      <span class="cadence-label">Emails / Day</span>
      <span class="cadence-val">${data.emailsPerDay ?? '—'}</span>
    </div>
    <div class="cadence-row">
      <span class="cadence-label">Follow-up Delay</span>
      <span class="cadence-val">${data.followUpDays ? data.followUpDays + ' days' : '—'}</span>
    </div>
  `;
}

async function saveDealSchedule() {
  const dealId = document.getElementById('schedule-deal-select')?.value;
  if (!dealId) { showToast('Select a deal first.', 'error'); return; }
  try {
    await api(`/api/deals/${dealId}/schedule`, 'PATCH', {});
    showToast('Schedule saved');
  } catch (err) { showToast(`Save failed: ${err.message}`, 'error'); }
}

/* ═══════════════════════════════════════════════════════════════════════════
   MODAL
   ═══════════════════════════════════════════════════════════════════════════ */

function openModal(title, onConfirm) {
  document.getElementById('modal-title').textContent = title;
  document.getElementById('modal-instructions').value = '';
  document.getElementById('edit-modal').classList.remove('hidden');
  modalCallback = onConfirm;
}

function closeModal() {
  document.getElementById('edit-modal').classList.add('hidden');
  modalCallback = null;
}

function modalConfirm() {
  if (modalCallback) modalCallback();
}

// Close modal on overlay click
document.getElementById('edit-modal')?.addEventListener('click', e => {
  if (e.target === e.currentTarget) closeModal();
});

/* ═══════════════════════════════════════════════════════════════════════════
   SIDEBAR MOBILE
   ═══════════════════════════════════════════════════════════════════════════ */

function toggleSidebar() {
  const sidebar = document.getElementById('sidebar');
  sidebar?.classList.toggle('open');
}
function closeSidebar() {
  document.getElementById('sidebar')?.classList.remove('open');
}

/* ═══════════════════════════════════════════════════════════════════════════
   GLOBAL REFRESH
   ═══════════════════════════════════════════════════════════════════════════ */

async function fullRefresh() {
  await Promise.all([refreshStats(), populateDealSelector()]);
  // If user has a deal detail open, only silently refresh data-only tabs (not settings/batches)
  // to avoid wiping unsaved form edits
  if (selectedDealId) {
    const activeTab = document.querySelector('.deal-tab.active')?.dataset?.tab;
    const safeToRefresh = ['overview', 'pipeline', 'rankings', 'archived'];
    if (activeTab && safeToRefresh.includes(activeTab)) {
      switchDealTab(activeTab, document.querySelector('.deal-tab.active'));
    }
    return;
  }
  const view = (window.location.hash || '#overview').replace('#', '');
  switch (view) {
    case 'overview':  await loadOverview();  break;
    case 'deals':     await loadDeals();     break;
    case 'pipeline':  await loadPipeline();  break;
    case 'queue':     await loadQueue();     break;
    case 'activity':  await loadActivity();  break;
    case 'archive':         await loadArchive();         break;
    case 'sourcing':        await loadSourcingCampaigns(); break;
    case 'sourcing-detail': if (currentSourcingCampaignId) await loadSourcingCampaignDetail(currentSourcingCampaignId); break;
  }
}

/* ═══════════════════════════════════════════════════════════════════════════
   UTILITIES
   ═══════════════════════════════════════════════════════════════════════════ */

/* ─── DAY PICKER HELPERS ─────────────────────────────────────────────────── */

function getDayPickerValue(id) {
  const picker = document.getElementById(id);
  if (!picker) return 'Mon,Tue,Wed,Thu,Fri';
  const selected = Array.from(picker.querySelectorAll('.day-pill.on')).map(b => b.dataset.day);
  return selected.length ? selected.join(',') : 'Mon,Tue,Wed,Thu,Fri';
}

function setDayPickerValue(id, value) {
  const picker = document.getElementById(id);
  if (!picker) return;
  const selected = (value || 'Mon,Tue,Wed,Thu,Fri').split(',').map(d => d.trim());
  picker.querySelectorAll('.day-pill').forEach(b => {
    b.classList.toggle('on', selected.includes(b.dataset.day));
  });
}

function renderDayPickerHTML(id, value) {
  const days = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
  const selected = (value || 'Mon,Tue,Wed,Thu,Fri').split(',').map(d => d.trim());
  return `<div class="day-picker" id="${id}">
    ${days.map(d => `<button type="button" class="day-pill${selected.includes(d) ? ' on' : ''}" data-day="${d}" onclick="this.classList.toggle('on')">${d.toUpperCase()}</button>`).join('')}
  </div>`;
}

function esc(str) {
  if (str == null) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val ?? '—';
}

function fmt(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString('en-GB');
}

function pct(n) {
  if (n == null) return '—';
  const v = typeof n === 'string' && n.includes('%') ? n : (Number(n) * (Number(n) <= 1 ? 100 : 1)).toFixed(1) + '%';
  return v;
}

const CURRENCY_SYMBOLS = { USD: '$', GBP: '£', EUR: '€', CAD: 'CA$', AUD: 'A$', CHF: 'Fr', JPY: '¥', SGD: 'S$' };

function updateDealCurrencyLabels(currency) {
  const symbol = CURRENCY_SYMBOLS[currency || 'USD'] || '$';
  const targetLabel = document.getElementById('ds-target-label');
  const capitalLabel = document.getElementById('ds-capital-label');
  if (targetLabel) targetLabel.textContent = `Target Amount (${symbol})`;
  if (capitalLabel) capitalLabel.textContent = `Amount Committed (${symbol})`;
}

function formatMoney(v, currency) {
  if (!v) return '—';
  const n = Number(v);
  if (isNaN(n) || n === 0) return '—';
  const sym = CURRENCY_SYMBOLS[currency || window.__activeDealCurrency || 'USD'] || '$';
  if (n >= 1_000_000_000) return `${sym}${(n / 1_000_000_000).toFixed(1)}B`;
  if (n >= 1_000_000)     return `${sym}${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)         return `${sym}${(n / 1_000).toFixed(0)}K`;
  return `${sym}${n.toLocaleString()}`;
}

const DOM_TZ = 'America/New_York';

function formatDate(d) {
  if (!d) return '—';
  try {
    const date = new Date(d);
    if (isNaN(date)) return String(d);
    const diff = Date.now() - date;
    if (diff < 60_000)         return 'just now';
    if (diff < 3_600_000)      return `${Math.floor(diff / 60_000)}m ago`;
    if (diff < 86_400_000)     return `${Math.floor(diff / 3_600_000)}h ago`;
    if (diff < 7 * 86_400_000) return `${Math.floor(diff / 86_400_000)}d ago`;
    return date.toLocaleDateString('en-US', { day: 'numeric', month: 'short', timeZone: DOM_TZ });
  } catch { return String(d); }
}

function formatTime(d) {
  if (!d) return '—';
  try {
    const date = new Date(d);
    const datePart = date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: DOM_TZ });
    const timePart = date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: DOM_TZ });
    return `${datePart} ${timePart}`;
  } catch { return '—'; }
}

function typeToBadge(type) {
  if (!type) return 'system';
  const t = String(type).toLowerCase();
  if (t === 'reply'  || t.includes('reply') || t.includes('inbound') || t.includes('response')) return 'reply';
  if (t === 'invite' || t.includes('invite_sent') || t.includes('linkedin_invite'))              return 'invite';
  if (t === 'dm'     || t.includes('dm_sent')     || t.includes('linkedin_dm'))                  return 'dm';
  if (t.includes('linkedin'))                                                                     return 'linkedin';
  if (t.includes('email') || t.includes('email_sent'))                                           return 'email';
  if (t.includes('research') || t.includes('scored'))                                            return 'research';
  if (t.includes('enrich'))                                                                       return 'enrichment';
  if (t.includes('outreach') || t.includes('intro') || t.includes('followup') || t.includes('follow')) return 'email';
  if (t.includes('approv') || t.includes('queue'))                                               return 'approval';
  if (t.includes('error') || t.includes('fail'))                                                 return 'error';
  if (t === 'excluded' || t.includes('excluded'))                                                return 'excluded';
  return 'system';
}

function scoreHtml(score) {
  if (score == null) return '<span class="score-badge low">—</span>';
  const n   = Number(score);
  const cls = n >= 70 ? 'high' : n >= 40 ? 'medium' : 'low';
  return `<span class="score-badge ${cls}">${Math.round(n)}</span>`;
}

function showToast(msg, type = 'success', duration = 2800) {
  // Support legacy calls: showToast(msg, number)
  if (typeof type === 'number') { duration = type; type = 'success'; }
  let toast = document.getElementById('roco-toast');
  if (!toast) {
    toast = document.createElement('div');
    toast.id = 'roco-toast';
    toast.style.cssText = 'position:fixed;bottom:24px;right:24px;padding:10px 18px;border-radius:6px;font-size:13px;z-index:9999;opacity:0;transition:opacity 0.2s;pointer-events:none';
    document.body.appendChild(toast);
  }
  const isError = type === 'error';
  toast.style.background = isError ? '#3d1a1a' : '#1e2533';
  toast.style.color       = isError ? '#f87171' : '#e0e8f4';
  toast.style.border      = isError ? '1px solid #7f1d1d' : '1px solid #2d3748';
  toast.textContent = msg;
  toast.style.opacity = '1';
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => { toast.style.opacity = '0'; }, duration);
}

/* ═══════════════════════════════════════════════════════════════════════════
   ENVIRONMENT VARIABLES
   ═══════════════════════════════════════════════════════════════════════════ */

const ENV_GROUPS = [
  { label: 'AI / LLM',    keys: ['ANTHROPIC_API_KEY','OPENAI_API_KEY','GEMINI_API_KEY','GEMINI_API_KEY_FALLBACK','GROK_API_KEY'] },
  { label: 'Enrichment',  keys: ['KASPR_API_KEY','APIFY_API_TOKEN','APIFY_LINKEDIN_SCRAPER_ACTOR_ID','APIFY_USER_ID'] },
  { label: 'Unipile',     keys: ['UNIPILE_API_KEY','UNIPILE_DSN','UNIPILE_LINKEDIN_ACCOUNT_ID','UNIPILE_GMAIL_ACCOUNT_ID','UNIPILE_ACCESS_TOKEN'] },
  { label: 'Telegram',    keys: ['TELEGRAM_BOT_TOKEN','TELEGRAM_CHAT_ID'] },
  { label: 'Notion',      keys: ['NOTION_API_KEY','NOTION_CONTACTS_DB_ID','NOTION_COMPANIES_DB_ID'] },
  { label: 'Supabase',    keys: ['SUPABASE_URL','SUPABASE_SERVICE_KEY','SUPABASE_ANON_KEY'] },
  { label: 'Dashboard',   keys: ['DASHBOARD_USER','DASHBOARD_PASS','PORT','REPLY_TO_EMAIL','SENDER_NAME'] },
  { label: 'Other',       keys: ['MATONAI_API_KEY'] },
];

async function loadEnvView() {
  const container = document.getElementById('env-container');
  if (!container) return;
  container.innerHTML = '<div style="color:var(--text-dim);padding:16px">Loading&#8230;</div>';
  try {
    const vars = await api('/api/env'); // array of { key, masked, set }
    const map = {};
    for (const v of vars) map[v.key] = v;

    let html = '';
    for (const group of ENV_GROUPS) {
      const rows = group.keys.filter(k => map[k]);
      if (!rows.length) continue;
      html += `<div class="card" style="margin-bottom:16px">
        <div class="card-header"><h2 class="card-title" style="font-size:13px;letter-spacing:.08em">${group.label}</h2></div>
        <div style="padding:0 16px 12px">`;
      for (const key of rows) {
        const v = map[key];
        html += `<div id="env-row-${key}" style="display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid var(--border)">
          <div style="width:280px;font-size:12px;font-family:var(--font-mono);color:var(--text-dim);flex-shrink:0">${key}</div>
          <div style="flex:1;font-size:12px;font-family:var(--font-mono);color:${v.set ? 'var(--text)' : 'var(--text-dim)'}">${esc(v.masked)}</div>
          <button class="btn btn-sm" onclick="editEnvVar('${key}')" style="font-size:11px;padding:3px 10px;flex-shrink:0">Edit</button>
        </div>`;
      }
      html += `</div></div>`;
    }
    container.innerHTML = html;
  } catch (err) {
    container.innerHTML = `<div class="card"><div style="color:var(--text-dim);padding:16px">Failed to load: ${esc(err.message)}</div></div>`;
  }
}

function editEnvVar(key) {
  const row = document.getElementById(`env-row-${key}`);
  if (!row) return;
  row.innerHTML = `
    <div style="width:280px;font-size:12px;font-family:var(--font-mono);color:var(--text-dim);flex-shrink:0">${key}</div>
    <input id="env-input-${key}" type="password" placeholder="Paste new value…"
      style="flex:1;font-family:var(--font-mono);font-size:12px;background:var(--bg-secondary);border:1px solid var(--gold);color:var(--text);padding:6px 10px;border-radius:4px;outline:none"
      onkeydown="if(event.key==='Enter')saveEnvVar('${key}');if(event.key==='Escape')loadEnvView();" />
    <div style="display:flex;gap:6px;flex-shrink:0">
      <button class="btn btn-sm" onclick="saveEnvVar('${key}')" style="background:var(--gold);color:#000;font-size:11px;padding:3px 10px">Save</button>
      <button class="btn btn-sm" onclick="loadEnvView()" style="font-size:11px;padding:3px 10px">Cancel</button>
    </div>`;
  document.getElementById(`env-input-${key}`)?.focus();
}

async function saveEnvVar(key) {
  const input = document.getElementById(`env-input-${key}`);
  const value = input?.value?.trim();
  if (!value) { showToast('Value cannot be empty', true); return; }
  const btn = input?.nextElementSibling?.querySelector('button');
  if (btn) { btn.disabled = true; btn.textContent = 'Saving…'; }
  try {
    await api('/api/env', 'POST', { key, value });
    showToast(`${key} updated`);
    await loadEnvView();
  } catch (err) {
    showToast('Failed: ' + err.message, true);
    if (btn) { btn.disabled = false; btn.textContent = 'Save'; }
  }
}

/* ═══════════════════════════════════════════════════════════════════════════
   SOURCING CAMPAIGNS
   ═══════════════════════════════════════════════════════════════════════════ */

let currentSourcingCampaignId = null;

// ─── SHOW VIEW HELPER (for sourcing subviews) ────────────────────────────

function showView(view) {
  window.location.hash = '#' + view;
}

// ─── CAMPAIGNS LIST ──────────────────────────────────────────────────────

async function loadSourcingCampaigns() {
  const container = document.getElementById('sourcing-campaigns-list');
  if (!container) return;
  container.innerHTML = '<div class="table-empty">Loading campaigns&#8230;</div>';

  try {
    const campaigns = await api('/api/sourcing/campaigns');
    if (!campaigns?.length) {
      container.innerHTML = `
        <div class="card" style="padding:32px;text-align:center">
          <p style="opacity:0.5;margin-bottom:16px">No sourcing campaigns yet.</p>
          <button class="btn btn-primary" onclick="showView('sourcing-launch')">&#43; Launch your first campaign</button>
        </div>`;
      return;
    }

    const active   = campaigns.filter(c => c.status !== 'closed');
    const archived = campaigns.filter(c => c.status === 'closed');

    // Load stats only for active/paused campaigns
    const statsPromises = active.map(c =>
      api(`/api/sourcing/campaigns/${c.id}`).catch(() => c)
    );
    const withStats = await Promise.all(statsPromises);

    let html = '';
    if (withStats.length) {
      html += `<div class="deal-card-grid">${withStats.map(c => renderCampaignCard(c)).join('')}</div>`;
    } else {
      html += `<div class="card" style="padding:32px;text-align:center">
        <p style="opacity:0.5;margin-bottom:16px">No active campaigns.</p>
        <button class="btn btn-primary" onclick="showView('sourcing-launch')">&#43; Launch a campaign</button>
      </div>`;
    }

    if (archived.length) {
      html += `<div style="margin-top:32px">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;cursor:pointer" onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display==='none'?'':'none'">
          <span style="font-size:13px;font-weight:600;opacity:0.5;text-transform:uppercase;letter-spacing:0.08em">Archived (${archived.length})</span>
          <span style="opacity:0.3">▾</span>
        </div>
        <div>
          <div class="deal-card-grid">${archived.map(c => renderCampaignCard(c)).join('')}</div>
        </div>
      </div>`;
    }

    container.innerHTML = html;

    // Update badge
    const activeCampaigns = active.filter(c => c.status === 'active').length;
    const badge = document.getElementById('sourcing-badge');
    if (badge) {
      badge.textContent = activeCampaigns;
      badge.style.display = activeCampaigns > 0 ? '' : 'none';
    }
  } catch (err) {
    container.innerHTML = `<div class="table-empty">Error loading campaigns: ${esc(err.message)}</div>`;
  }
}

function renderCampaignCard(c) {
  const stats = c.stats || {};
  const isClosed = c.status === 'closed';
  const statusClass = c.status === 'active' ? 'chip-green' : c.status === 'paused' ? 'chip-amber' : 'chip-grey';

  const actions = isClosed
    ? `<button class="btn btn-sm btn-ghost" onclick="reopenSourcingCampaign('${esc(c.id)}')">Reopen</button>
       <button class="btn btn-sm btn-ghost" style="color:var(--red,#e55)" onclick="deleteSourcingCampaign('${esc(c.id)}','${esc(c.name)}')">Delete</button>`
    : `<button class="btn btn-sm btn-ghost" onclick="viewSourcingCampaign('${esc(c.id)}')">View</button>
       ${c.status === 'active'
         ? `<button class="btn btn-sm btn-ghost" onclick="pauseSourcingCampaign('${esc(c.id)}')">Pause</button>`
         : `<button class="btn btn-sm btn-ghost" onclick="resumeSourcingCampaign('${esc(c.id)}')">Resume</button>`}
       <button class="btn btn-sm btn-ghost" onclick="closeSourcingCampaignById('${esc(c.id)}','${esc(c.name)}')">Close</button>`;

  return `<div class="deal-card" onclick="${isClosed ? '' : `viewSourcingCampaign('${esc(c.id)}')`}" style="${isClosed ? 'opacity:0.6' : 'cursor:pointer'}">
    <div class="deal-card-header">
      <div>
        <div class="deal-card-name">${esc(c.name)}</div>
        <div class="deal-card-sub">${esc(c.firm_name || '—')} &middot; ${esc(c.firm_type || '')}</div>
      </div>
      <span class="chip ${statusClass}">${(c.status || 'active').toUpperCase()}</span>
    </div>
    <div class="deal-card-sector">${esc(c.target_sector)} &middot; ${esc(c.target_geography)}</div>
    <div class="deal-card-stats">
      <div class="deal-stat"><span class="ds-value">${stats.companies_found || 0}</span><span class="ds-label">Companies</span></div>
      <div class="deal-stat"><span class="ds-value gold">${stats.hot_leads || 0}</span><span class="ds-label">Hot Leads</span></div>
      <div class="deal-stat"><span class="ds-value green">${stats.meetings_booked || 0}</span><span class="ds-label">Meetings</span></div>
    </div>
    <div class="deal-card-actions" onclick="event.stopPropagation()">
      ${actions}
    </div>
  </div>`;
}

// ─── CAMPAIGN DETAIL ─────────────────────────────────────────────────────

async function viewSourcingCampaign(id) {
  currentSourcingCampaignId = id;
  showView('sourcing-detail');
  await loadSourcingCampaignDetail(id);
}

async function loadSourcingCampaignDetail(id) {
  try {
    const c = await api(`/api/sourcing/campaigns/${id}`);
    if (!c) return;

    const stats = c.stats || {};
    setText('sourcing-detail-title', c.name);

    const statusEl = document.getElementById('sourcing-detail-status-chip');
    if (statusEl) {
      const cls = c.status === 'active' ? 'chip-green' : c.status === 'paused' ? 'chip-amber' : 'chip-grey';
      statusEl.className = `chip ${cls}`;
      statusEl.textContent = (c.status || 'active').toUpperCase();
    }

    const isClosed = c.status === 'closed';
    const pauseBtn  = document.getElementById('sourcing-pause-btn');
    const closeBtn  = document.getElementById('sourcing-close-btn');
    const deleteBtn = document.getElementById('sourcing-delete-btn');
    if (pauseBtn)  { pauseBtn.textContent = c.status === 'active' ? 'Pause' : 'Resume'; pauseBtn.style.display = isClosed ? 'none' : ''; }
    if (closeBtn)  { closeBtn.style.display  = isClosed ? 'none' : ''; }
    if (deleteBtn) { deleteBtn.style.display = isClosed ? '' : 'none'; }

    setText('sc-stat-companies', stats.companies_found || 0);
    setText('sc-stat-hot',       stats.hot_leads || 0);
    setText('sc-stat-enriched',  stats.contacts_enriched || 0);
    setText('sc-stat-outreach',  stats.outreach_sent || 0);
    setText('sc-stat-meetings',  stats.meetings_booked || 0);

    // Load default tab
    await loadSourcingCompaniesTab(id);
    await loadSourcingContactsTab(id);
    await loadSourcingMeetingsTab(id);
    await loadSourcingArchivedTab(id);
    renderSourcingSettings(c);

  } catch (err) {
    showToast('Error loading campaign: ' + err.message, true);
  }
}

async function loadSourcingCompaniesTab(id) {
  const tbody = document.getElementById('sc-companies-tbody');
  if (!tbody) return;
  try {
    const companies = await api(`/api/sourcing/campaigns/${id}/companies`);
    if (!companies?.length) {
      tbody.innerHTML = '<tr><td colspan="9" class="table-empty">No companies found yet — research is running</td></tr>';
      return;
    }
    tbody.innerHTML = companies.map(c => `
      <tr onclick="viewSourcingCompany('${esc(c.id)}')" style="cursor:pointer">
        <td><strong>${c.website ? `<a href="${esc(c.website)}" target="_blank" onclick="event.stopPropagation()" style="color:inherit;text-decoration:underline;text-underline-offset:3px">${esc(c.company_name)}</a>` : esc(c.company_name)}</strong></td>
        <td>${esc(c.sector || '—')}</td>
        <td>${esc(c.estimated_revenue || '—')}</td>
        <td>${renderScorePill(c.match_score)}</td>
        <td>${renderTierChip(c.match_tier)}</td>
        <td style="max-width:180px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;cursor:pointer;text-decoration:underline dotted;text-underline-offset:3px" title="Click to expand intent signals" onclick="event.stopPropagation();showIntentSignals(this.dataset.signals)" data-signals="${esc(c.intent_signals_found || '—')}">${esc((c.intent_signals_found || '—').substring(0, 60))}${(c.intent_signals_found||'').length > 60 ? '…' : ''}</td>
        <td>${esc(c.research_status || 'pending')}</td>
        <td>${esc(c.outreach_status || 'pending')}</td>
        <td onclick="event.stopPropagation()">
          <button class="btn btn-sm btn-ghost" onclick="archiveSourcingCompany('${esc(c.id)}')">Archive</button>
        </td>
      </tr>`).join('');

    // Also populate hot leads tab
    const hotTbody = document.getElementById('sc-hot-tbody');
    if (hotTbody) {
      const hot = companies.filter(c => c.match_tier === 'hot' || c.match_tier === 'warm');
      hotTbody.innerHTML = hot.length
        ? hot.map(c => `<tr onclick="viewSourcingCompany('${esc(c.id)}')" style="cursor:pointer">
            <td><strong>${esc(c.company_name)}</strong></td>
            <td>${esc(c.sector || '—')}</td>
            <td>${esc(c.estimated_revenue || '—')}</td>
            <td>${esc(c.estimated_ebitda || '—')}</td>
            <td>${renderScorePill(c.match_score)}</td>
            <td>${renderTierChip(c.match_tier)}</td>
            <td>${esc(c.outreach_status || 'pending')}</td>
            <td onclick="event.stopPropagation()">
              <button class="btn btn-sm btn-ghost" onclick="archiveSourcingCompany('${esc(c.id)}')">Archive</button>
            </td>
          </tr>`).join('')
        : '<tr><td colspan="8" class="table-empty">No hot or warm leads yet</td></tr>';
    }
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="9" class="table-empty">Error: ${esc(err.message)}</td></tr>`;
  }
}

async function loadSourcingContactsTab(id) {
  const tbody = document.getElementById('sc-contacts-tbody');
  if (!tbody) return;
  try {
    const contacts = await api(`/api/sourcing/campaigns/${id}/contacts`);

    // Update tab count
    const tabEl = document.querySelector('[data-tab="sc-contacts"]');
    if (tabEl) tabEl.textContent = `Contacts (${contacts?.length || 0})`;

    if (!contacts?.length) {
      tbody.innerHTML = '<tr><td colspan="7" class="table-empty">No contacts found yet</td></tr>';
      return;
    }
    tbody.innerHTML = contacts.map(c => `
      <tr>
        <td><strong>${c.linkedin_url ? `<a href="${esc(c.linkedin_url)}" target="_blank" style="color:inherit;text-decoration:underline;text-underline-offset:3px">${esc(c.name || '—')}</a>` : esc(c.name || '—')}</strong></td>
        <td>${esc(c.title || '—')}</td>
        <td>${esc(c.target_companies?.company_name || '—')}</td>
        <td>${renderEnrichmentChip(c.enrichment_status, c.email, c.linkedin_url)}</td>
        <td>${esc(c.pipeline_stage || '—')}</td>
        <td>${c.email ? `<span class="mono" style="font-size:11px">${esc(c.email)}</span>` : '<span style="opacity:0.4">—</span>'}</td>
        <td>
          <button class="btn btn-sm btn-ghost" onclick="skipSourcingContact('${esc(c.id)}')">Skip</button>
        </td>
      </tr>`).join('');
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="7" class="table-empty">Error: ${esc(err.message)}</td></tr>`;
  }
}

async function loadSourcingMeetingsTab(id) {
  const container = document.getElementById('sc-meetings-list');
  if (!container) return;
  try {
    const meetings = await api(`/api/sourcing/campaigns/${id}/meetings`);
    if (!meetings?.length) {
      container.innerHTML = '<div class="table-empty">No meetings booked yet</div>';
      return;
    }
    container.innerHTML = meetings.map(m => `
      <div style="padding:16px;border-bottom:1px solid var(--border-subtle)">
        <strong>${esc(m.company_name)}</strong>
        <span style="margin-left:12px;opacity:0.5">${formatDate(m.meeting_booked_at)}</span>
        <div style="margin-top:4px;opacity:0.7">${esc(m.company_contacts?.map(c => `${c.name} (${c.title})`).join(', ') || '—')}</div>
      </div>`).join('');
  } catch {}
}

async function loadSourcingArchivedTab(id) {
  const tbody = document.getElementById('sc-archived-tbody');
  if (!tbody) return;
  try {
    const companies = await api(`/api/sourcing/campaigns/${id}/companies?tier=archive`);
    if (!companies?.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="table-empty">No archived companies</td></tr>';
      return;
    }
    tbody.innerHTML = companies.map(c => `
      <tr>
        <td>${esc(c.company_name)}</td>
        <td>${esc(c.sector || '—')}</td>
        <td>${renderScorePill(c.match_score)}</td>
        <td style="opacity:0.7">${esc((c.why_matches || 'Low match score').substring(0, 80))}</td>
        <td>
          <button class="btn btn-sm btn-ghost" onclick="reinstateSourcingCompany('${esc(c.id)}')">Reinstate</button>
        </td>
      </tr>`).join('');
  } catch {}
}

function renderSourcingSettings(c) {
  const container = document.getElementById('sc-settings-form');
  if (!container) return;
  container.innerHTML = `
    <div class="form-group"><label class="form-label">Campaign Name</label>
      <input type="text" class="form-input" id="sc-setting-name" value="${esc(c.name)}" /></div>
    <div class="form-group"><label class="form-label">Investment Thesis</label>
      <textarea class="form-input" id="sc-setting-thesis" rows="3">${esc(c.investment_thesis || '')}</textarea></div>
    <div class="form-group"><label class="form-label">Intent Signals</label>
      <input type="text" class="form-input" id="sc-setting-signals" value="${esc(c.intent_signals || '')}" /></div>
    <div class="form-group"><label class="form-label">Max Companies per Campaign</label>
      <input type="number" class="form-input" id="sc-setting-max" value="${c.max_companies_per_campaign || 200}" min="10" max="500" /></div>
    <button class="btn btn-primary" onclick="saveSourcingSettings('${esc(c.id)}')">Save Settings</button>`;
}

async function saveSourcingSettings(id) {
  const updates = {
    name:               document.getElementById('sc-setting-name')?.value?.trim(),
    investment_thesis:  document.getElementById('sc-setting-thesis')?.value?.trim(),
    intent_signals:     document.getElementById('sc-setting-signals')?.value?.trim(),
    max_companies_per_campaign: parseInt(document.getElementById('sc-setting-max')?.value),
  };
  try {
    await api(`/api/sourcing/campaigns/${id}`, 'PATCH', updates);
    showToast('Settings saved');
    await loadSourcingCampaignDetail(id);
  } catch (err) {
    showToast('Failed: ' + err.message, true);
  }
}

// ─── CAMPAIGN ACTIONS ────────────────────────────────────────────────────

async function toggleSourcingCampaign() {
  const id = currentSourcingCampaignId;
  if (!id) return;
  try {
    const c = await api(`/api/sourcing/campaigns/${id}`);
    const action = c.status === 'active' ? 'pause' : 'resume';
    await api(`/api/sourcing/campaigns/${id}/${action}`, 'POST');
    showToast(`Campaign ${action}d`);
    await loadSourcingCampaignDetail(id);
  } catch (err) {
    showToast('Error: ' + err.message, true);
  }
}

async function pauseSourcingCampaign(id) {
  try {
    await api(`/api/sourcing/campaigns/${id}/pause`, 'POST');
    showToast('Campaign paused');
    await loadSourcingCampaigns();
  } catch (err) { showToast('Error: ' + err.message, true); }
}

async function resumeSourcingCampaign(id) {
  try {
    await api(`/api/sourcing/campaigns/${id}/resume`, 'POST');
    showToast('Campaign resumed');
    await loadSourcingCampaigns();
  } catch (err) { showToast('Error: ' + err.message, true); }
}

async function closeSourcingCampaign() {
  if (!currentSourcingCampaignId) return;
  if (!confirm('Close this campaign? It will stop running and move to Archived.')) return;
  try {
    await api(`/api/sourcing/campaigns/${currentSourcingCampaignId}/close`, 'POST');
    showToast('Campaign closed and archived');
    showView('sourcing');
    await loadSourcingCampaigns();
  } catch (err) { showToast('Error: ' + err.message, true); }
}

async function closeSourcingCampaignById(id, name) {
  if (!confirm(`Close "${name}"? It will stop running and move to Archived.`)) return;
  try {
    await api(`/api/sourcing/campaigns/${id}/close`, 'POST');
    showToast('Campaign archived');
    await loadSourcingCampaigns();
  } catch (err) { showToast('Error: ' + err.message, true); }
}

async function reopenSourcingCampaign(id) {
  try {
    await api(`/api/sourcing/campaigns/${id}/reopen`, 'POST');
    showToast('Campaign reopened');
    await loadSourcingCampaigns();
  } catch (err) { showToast('Error: ' + err.message, true); }
}

async function deleteSourcingCampaign(id, name) {
  if (!confirm(`Permanently delete "${name}"?\n\nThis will delete all companies, contacts, and approval history. This cannot be undone.`)) return;
  try {
    await api(`/api/sourcing/campaigns/${id}`, 'DELETE');
    showToast('Campaign permanently deleted');
    await loadSourcingCampaigns();
  } catch (err) { showToast('Error: ' + err.message, true); }
}

function showIntentSignals(text) {
  const overlay = document.createElement('div');
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:9999;display:flex;align-items:center;justify-content:center;padding:24px';
  const inner = document.createElement('div');
  inner.style.cssText = 'background:var(--bg-card,#1a1a2e);border:1px solid var(--border,#333);border-radius:12px;padding:28px;max-width:600px;width:100%;max-height:80vh;overflow-y:auto';
  inner.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px"><strong style="font-size:14px">Intent Signals</strong><button style="background:none;border:none;cursor:pointer;font-size:20px;opacity:0.6;color:inherit">✕</button></div>`;
  const p = document.createElement('p');
  p.style.cssText = 'white-space:pre-wrap;font-size:13px;line-height:1.7;opacity:0.85;margin:0';
  p.textContent = text;
  inner.appendChild(p);
  inner.querySelector('button').addEventListener('click', () => overlay.remove());
  overlay.appendChild(inner);
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}

async function archiveSourcingCompany(id) {
  try {
    await api(`/api/sourcing/companies/${id}/archive`, 'POST');
    showToast('Company archived');
    if (currentSourcingCampaignId) await loadSourcingCompaniesTab(currentSourcingCampaignId);
  } catch (err) { showToast('Error: ' + err.message, true); }
}

async function reinstateSourcingCompany(id) {
  try {
    await api(`/api/sourcing/companies/${id}/reinstate`, 'POST');
    showToast('Company reinstated as warm lead');
    if (currentSourcingCampaignId) {
      await loadSourcingCompaniesTab(currentSourcingCampaignId);
      await loadSourcingArchivedTab(currentSourcingCampaignId);
    }
  } catch (err) { showToast('Error: ' + err.message, true); }
}

async function skipSourcingContact(id) {
  try {
    await api(`/api/sourcing/contacts/${id}/skip`, 'POST');
    showToast('Contact skipped');
    if (currentSourcingCampaignId) await loadSourcingContactsTab(currentSourcingCampaignId);
  } catch (err) { showToast('Error: ' + err.message, true); }
}

async function viewSourcingCompany(id) {
  // TODO: slide-in panel — for now open in a browser alert with key info
  try {
    const c = await api(`/api/sourcing/companies/${id}`);
    const contacts = (c.contacts || []).map(ct => `  ${ct.name} (${ct.title || '—'}) — ${ct.pipeline_stage}`).join('\n') || '  None found yet';
    alert(`${c.company_name}\n\n${c.product_description || 'No description'}\n\nWhy it matches:\n${c.why_matches || '—'}\n\nIntent signals:\n${c.intent_signals_found || '—'}\n\nContacts:\n${contacts}`);
  } catch (err) { showToast('Error: ' + err.message, true); }
}

// ─── LAUNCH FORM SUBMISSION ──────────────────────────────────────────────

async function submitSourcingCampaign(e) {
  e.preventDefault();
  const form = e.target;
  const data = new FormData(form);
  const obj = {};
  for (const [k, v] of data.entries()) {
    if (v !== '') obj[k] = v;
  }

  // Map time inputs to window objects
  obj.email_send_window = {
    start: obj.email_start || '09:00',
    end:   obj.email_end   || '18:00',
  };
  obj.linkedin_dm_window = {
    start: obj.li_dm_start || '20:00',
    end:   obj.li_dm_end   || '23:00',
  };
  obj.linkedin_connection_window = {
    start: obj.li_connect_start || '09:00',
    end:   obj.li_connect_end   || '18:00',
  };
  delete obj.email_start; delete obj.email_end;
  delete obj.li_dm_start; delete obj.li_dm_end;
  delete obj.li_connect_start; delete obj.li_connect_end;

  const btn = form.querySelector('[type=submit]');
  if (btn) { btn.disabled = true; btn.textContent = 'Launching…'; }

  try {
    const result = await api('/api/sourcing/campaigns', 'POST', obj);
    showToast('Campaign launched — research starting now');
    form.reset();
    showView('sourcing-detail');
    currentSourcingCampaignId = result.campaign.id;
    await loadSourcingCampaignDetail(result.campaign.id);
  } catch (err) {
    showToast('Error: ' + err.message, true);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Launch Campaign'; }
  }
}

// ─── TAB SWITCHER ────────────────────────────────────────────────────────

function switchSourcingTab(tabId, btn) {
  document.querySelectorAll('.sourcing-tab').forEach(el => el.style.display = 'none');
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  const tab = document.getElementById('tab-' + tabId);
  if (tab) tab.style.display = '';
  if (btn) btn.classList.add('active');
}

// ─── RENDER HELPERS ──────────────────────────────────────────────────────

function renderScorePill(score) {
  if (score == null) return '<span class="chip chip-grey">—</span>';
  const cls = score >= 75 ? 'chip-green' : score >= 50 ? 'chip-amber' : score >= 30 ? 'chip-grey' : 'chip-red';
  return `<span class="chip ${cls}">${score}</span>`;
}

function renderTierChip(tier) {
  if (!tier) return '<span class="chip chip-grey">—</span>';
  const cls = tier === 'hot' ? 'chip-red' : tier === 'warm' ? 'chip-amber' : tier === 'possible' ? 'chip-grey' : 'chip-grey';
  return `<span class="chip ${cls}">${tier.toUpperCase()}</span>`;
}

function renderEnrichmentChip(status, email, linkedinUrl) {
  const hasEmail = !!email;
  const hasLi    = !!linkedinUrl;
  if (hasEmail && hasLi)  return '<span class="chip chip-green">LinkedIn + Email</span>';
  if (hasEmail && !hasLi) return '<span class="chip" style="background:rgba(160,80,220,0.15);color:#c084fc;border-color:rgba(160,80,220,0.3)">Email Only</span>';
  if (!hasEmail && hasLi) return '<span class="chip chip-amber">LinkedIn Only</span>';
  // Neither — show reason
  if (status === 'skipped_no_name')     return '<span class="chip chip-red">Skipped (no name)</span>';
  if (status === 'pending')             return '<span class="chip chip-grey">Pending</span>';
  return '<span class="chip chip-red">Skipped</span>';
}

/* ═══════════════════════════════════════════════════════════════════════════
   INVESTOR DATABASE
   ═══════════════════════════════════════════════════════════════════════════ */

let dbSearchTimer = null;
let dbCurrentPage = 1;
let dbActiveSubtab = 'investors'; // 'investors' | 'contacts'
let contactsCurrentPage = 1;

function dbSearchDebounce() {
  clearTimeout(dbSearchTimer);
  dbSearchTimer = setTimeout(() => loadDatabaseTable(1), 400);
}

async function loadDatabase() {
  try {
    const stats = await api('/api/investors-db/stats');
    document.getElementById('db-stat-total').textContent = (stats.total || 0).toLocaleString();
    const catEl = document.getElementById('db-cat-breakdown');
    if (catEl && stats.by_category) {
      catEl.innerHTML = Object.entries(stats.by_category)
        .sort((a, b) => b[1] - a[1])
        .map(([cat, count]) => `<span style="padding:2px 8px;border-radius:4px;background:var(--surface-2);border:1px solid var(--border)">${cat}: <span class="mono" style="color:var(--gold)">${count.toLocaleString()}</span></span>`)
        .join('');
    }
  } catch (_) {}
  loadInvestorListsPanel();
  loadDatabaseTable(1);
}

async function loadInvestorListsPanel() {
  const panel = document.getElementById('db-lists-panel');
  if (!panel) return;
  try {
    const lists = await api('/api/investor-lists');
    if (!lists?.length) { panel.innerHTML = ''; return; }
    panel.innerHTML = `
      <div class="card">
        <div class="card-header"><h2 class="card-title">Named Lists</h2></div>
        <div style="padding:0 24px 16px;display:flex;flex-wrap:wrap;gap:8px">
          ${lists.map(l => `
            <div style="display:flex;align-items:center;gap:6px;padding:6px 12px;background:var(--surface-2);border:1px solid var(--border);border-radius:6px">
              <span id="list-label-${l.id}" style="font-size:13px;color:var(--text-bright)">${esc(l.name)}</span>
              <input id="list-input-${l.id}" type="text" value="${esc(l.name)}"
                style="display:none;padding:2px 6px;background:#111;border:1px solid var(--gold);color:var(--text-bright);border-radius:4px;font-size:13px;width:180px"
                onblur="savePillName('${l.id}')" onkeydown="if(event.key==='Enter')savePillName('${l.id}');if(event.key==='Escape')cancelEditList('${l.id}')">
              <span style="color:var(--text-dim);font-size:11px;margin-left:2px">(${(l.investor_count||0).toLocaleString()})</span>
              <button onclick="startEditList('${l.id}')" title="Rename"
                style="background:none;border:none;color:var(--text-dim);cursor:pointer;font-size:13px;padding:0 2px;line-height:1" aria-label="Edit">✎</button>
            </div>
          `).join('')}
        </div>
      </div>
    `;
  } catch (_) { panel.innerHTML = ''; }
}

function startEditList(id) {
  document.getElementById(`list-label-${id}`).style.display = 'none';
  const input = document.getElementById(`list-input-${id}`);
  input.style.display = '';
  input.focus();
  input.select();
}

function cancelEditList(id) {
  const label = document.getElementById(`list-label-${id}`);
  const input = document.getElementById(`list-input-${id}`);
  if (!label || !input) return;
  input.value = label.textContent;
  input.style.display = 'none';
  label.style.display = '';
}

async function savePillName(id) {
  const label = document.getElementById(`list-label-${id}`);
  const input = document.getElementById(`list-input-${id}`);
  if (!label || !input) return;
  const newName = input.value.trim();
  if (!newName || newName === label.textContent) { cancelEditList(id); return; }
  input.onblur = null;
  try {
    await api(`/api/investor-lists/${id}`, 'PUT', { name: newName });
    label.textContent = newName;
    showToast('List renamed');
  } catch (e) {
    showToast(`Failed: ${e.message}`, 'error');
  }
  input.style.display = 'none';
  label.style.display = '';
}

async function loadListsTab() {
  const container = document.getElementById('lists-table-container');
  if (!container) return;
  container.innerHTML = '<div class="loading-placeholder">Loading&#8230;</div>';
  try {
    const lists = await api('/api/investor-lists');
    if (!lists?.length) {
      container.innerHTML = '<p style="color:#6b7280;padding:40px;text-align:center">No lists yet. Upload an XLSX to create your first list.</p>';
      return;
    }
    const total = lists.reduce((s, l) => s + (l.investor_count || 0), 0);
    container.innerHTML = `
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead>
          <tr style="color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.08em;border-bottom:1px solid var(--border)">
            <th style="padding:10px 16px;text-align:left;font-weight:500">List Name</th>
            <th style="padding:10px 16px;text-align:left;font-weight:500">Source</th>
            <th style="padding:10px 16px;text-align:right;font-weight:500">Investors</th>
            <th style="padding:10px 16px;text-align:left;font-weight:500">Created</th>
            <th style="padding:10px 16px;text-align:left;font-weight:500">Actions</th>
          </tr>
        </thead>
        <tbody>
          ${lists.map(l => `
            <tr style="border-bottom:1px solid var(--border)">
              <td style="padding:10px 16px">
                <span id="list-name-display-${l.id}"
                  style="color:#e5e7eb;font-weight:500"
                  data-original="${(l.name || '').replace(/"/g, '&quot;')}">
                  ${esc(l.name)}
                </span>
              </td>
              <td style="padding:10px 16px;color:var(--text-dim);font-size:12px">${esc(l.source || 'pitchbook')}</td>
              <td style="padding:10px 16px;text-align:right;color:var(--text-muted);font-family:var(--font-mono)">${(l.investor_count || 0).toLocaleString()}</td>
              <td style="padding:10px 16px;color:var(--text-dim);font-size:12px">${l.created_at ? new Date(l.created_at).toLocaleDateString('en-GB') : '—'}</td>
              <td style="padding:10px 16px">
                <button onclick="window.startEditListName('${l.id}', '${(l.name || '').replace(/'/g, "\\'")}')"
                  style="background:none;border:1px solid #2a2a2a;color:#9ca3af;
                         padding:4px 10px;border-radius:4px;cursor:pointer;font-size:11px">
                  ✎ Rename
                </button>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>
      <p style="color:var(--text-dim);font-size:11px;padding:10px 16px;margin:0">${lists.length} lists · ${total.toLocaleString()} total investors</p>
    `;
  } catch (e) {
    container.innerHTML = `<div class="loading-placeholder" style="color:var(--red)">${esc(e.message)}</div>`;
  }
}

window.loadListsTab = loadListsTab;

window.startEditListName = function(listId, currentName) {
  const nameEl = document.getElementById(`list-name-display-${listId}`);
  if (!nameEl) {
    console.error('[RENAME] Could not find element list-name-display-' + listId);
    return;
  }

  const escaped = currentName.replace(/'/g, "\\'").replace(/"/g, '&quot;');

  nameEl.innerHTML = `
    <input
      type="text"
      id="list-rename-input-${listId}"
      value="${escaped}"
      style="padding:4px 8px;background:#1a1a2a;border:1px solid #d4a847;
             color:#e5e7eb;border-radius:4px;font-size:13px;width:240px;outline:none"
      onkeydown="if(event.key==='Enter'){event.preventDefault();window.saveListName('${listId}');}
                 if(event.key==='Escape'){window.loadListsTab();}"
      onblur="window.saveListName('${listId}')"
    />
  `;

  const input = document.getElementById(`list-rename-input-${listId}`);
  if (input) { input.focus(); input.select(); }
};

window.saveListName = async function(listId) {
  const input = document.getElementById(`list-rename-input-${listId}`);
  if (!input) return;

  const newName = input.value.trim();

  // Prevent double-save on blur after Enter
  input.onblur = null;

  if (!newName) { window.loadListsTab(); return; }

  try {
    const data = await api(`/api/investor-lists/${listId}`, 'PUT', { name: newName });
    if (data && data.success) {
      showToast(`List renamed to "${newName}"`);
    } else if (data && data.error) {
      showToast('Rename failed: ' + data.error, 'error');
    } else {
      showToast(`List renamed to "${newName}"`);
    }
  } catch (err) {
    showToast('Rename failed', 'error');
  }
  window.loadListsTab();
};

async function loadDatabaseTable(page) {
  dbCurrentPage = page || 1;
  if (dbActiveSubtab === 'contacts') { loadContactsTable(1); return; }

  const search      = document.getElementById('db-search-input')?.value?.trim() || '';
  const type        = document.getElementById('db-filter-type')?.value || '';
  const country     = document.getElementById('db-filter-country')?.value || '';
  const enrich      = document.getElementById('db-filter-enrichment')?.value || '';
  const contactType = document.getElementById('db-filter-contact-type')?.value || '';
  const tbody       = document.getElementById('db-table-body');
  if (!tbody) return;
  tbody.innerHTML = '<tr><td colspan="11" class="table-empty">Loading&#8230;</td></tr>';

  try {
    const params = new URLSearchParams({ page: dbCurrentPage, limit: 50 });
    if (search)      params.set('search', search);
    if (type)        params.set('type', type);
    if (country)     params.set('country', country);
    if (enrich)      params.set('enrichment', enrich);
    if (contactType) params.set('contact_type', contactType);
    const data = await api(`/api/investors-db/search?${params}`);
    renderInvestorTable(data.investors || data, data.total, data.pages);
    renderPagination('db-pagination', dbCurrentPage, data.pages || 1, 'loadDatabaseTable');
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="11" class="table-empty" style="color:var(--text-muted)">${err.message}</td></tr>`;
  }
}

function switchDbSubtab(tab) {
  dbActiveSubtab = tab;
  const tabs = { investors: 'db-subtab-investors', contacts: 'db-subtab-contacts', lists: 'db-subtab-lists' };
  const views = { investors: 'db-investors-view', contacts: 'db-contacts-view', lists: 'db-lists-view' };
  Object.entries(tabs).forEach(([t, btnId]) => {
    const btn = document.getElementById(btnId);
    if (btn) {
      btn.style.borderBottom = t === tab ? '2px solid #d4a847' : '2px solid transparent';
      btn.style.color        = t === tab ? '#e5e7eb' : '#6b7280';
    }
  });
  Object.entries(views).forEach(([t, viewId]) => {
    const el = document.getElementById(viewId);
    if (el) el.style.display = t === tab ? '' : 'none';
  });
  if (tab === 'investors') loadDatabaseTable(1);
  else if (tab === 'contacts') loadContactsTable(1);
  else if (tab === 'lists') loadListsTab();
}

async function loadContactsTable(page) {
  contactsCurrentPage = page || 1;
  const search = document.getElementById('db-contacts-search')?.value?.trim() || '';
  const tbody  = document.getElementById('db-contacts-body');
  if (!tbody) return;
  tbody.innerHTML = '<tr><td colspan="8" class="table-empty">Loading&#8230;</td></tr>';
  try {
    const params = new URLSearchParams({ page: contactsCurrentPage, limit: 50 });
    if (search) params.set('search', search);
    const data = await api(`/api/contacts-db/researched?${params}`);
    const rows = data.contacts || [];
    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="8" class="table-empty">
        <div style="padding:20px 0">
          <div style="font-size:24px;margin-bottom:8px">🔬</div>
          <div>No researched contacts yet.</div>
          <div style="font-size:12px;margin-top:4px;color:var(--text-muted)">Contacts appear here after Roco has researched and enriched them for a deal.</div>
        </div>
      </td></tr>`;
      renderPagination('db-contacts-pagination', 1, 1, 'loadContactsTable');
      return;
    }
    tbody.innerHTML = rows.map(r => `<tr
      style="cursor:pointer;transition:background 0.15s"
      onmouseover="this.style.background='var(--surface-2,#1a1a1a)'"
      onmouseout="this.style.background=''"
      onclick="openProspectDrawer('${r.id}', null)">
      <td style="font-weight:500">${esc(r.name || '—')}</td>
      <td style="font-size:12px">${esc(r.company_name || '—')}</td>
      <td style="font-size:12px;color:var(--text-dim)">${esc(r.job_title || '—')}</td>
      <td style="font-size:11px;font-family:var(--font-mono)">${r.email ? `<span style="color:#4ade80">${esc(r.email)}</span>` : '<span style="color:#555">—</span>'}</td>
      <td style="font-size:12px">${r.linkedin_url ? `<a href="${esc(r.linkedin_url.startsWith('http') ? r.linkedin_url : 'https://'+r.linkedin_url)}" target="_blank" onclick="event.stopPropagation()" style="color:#0a66c2">LinkedIn</a>` : '<span style="color:#555">—</span>'}</td>
      <td style="font-size:11px;color:var(--text-dim);max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${r.past_investments ? esc(r.past_investments.split(',').slice(0,3).join(', ')) : '—'}</td>
      <td><span style="padding:2px 8px;border-radius:4px;font-size:11px;
        background:${r.enrichment_status==='Enriched'?'#064e3b':r.enrichment_status==='Partial'?'#1e3a5f':'#1a1a1a'};
        color:${r.enrichment_status==='Enriched'?'#4ade80':r.enrichment_status==='Partial'?'#60a5fa':'#6b7280'}">
        ${esc(r.enrichment_status || 'Raw')}</span></td>
      <td style="font-size:11px;color:var(--text-dim)">${r.updated_at ? r.updated_at.substring(0, 10) : '—'}</td>
    </tr>`).join('');
    renderPagination('db-contacts-pagination', contactsCurrentPage, data.pages || 1, 'loadContactsTable');
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="8" class="table-empty" style="color:var(--text-muted)">${err.message}</td></tr>`;
  }
}

function getContactTypeBadge(contact_type, is_angel) {
  if (is_angel || contact_type === 'angel')
    return `<span style="padding:2px 6px;border-radius:3px;font-size:10px;font-family:var(--font-mono);background:rgba(245,158,11,0.15);color:#f59e0b">Angel</span>`;
  if (contact_type === 'individual_at_firm')
    return `<span style="padding:2px 6px;border-radius:3px;font-size:10px;font-family:var(--font-mono);background:rgba(96,165,250,0.15);color:#60a5fa">Institutional</span>`;
  if (contact_type === 'firm')
    return `<span style="padding:2px 6px;border-radius:3px;font-size:10px;font-family:var(--font-mono);background:rgba(167,139,250,0.15);color:#a78bfa">Firm</span>`;
  return '<span style="color:#374151;font-size:10px">—</span>';
}

function renderInvestorTable(rows, total, pages) {
  const tbody = document.getElementById('db-table-body');
  if (!tbody) return;
  if (!rows?.length) {
    tbody.innerHTML = '<tr><td colspan="11" class="table-empty">No investors found</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(r => {
    const dealSize = (r.preferred_deal_size_min != null || r.preferred_deal_size_max != null)
      ? `$${r.preferred_deal_size_min || 0}M – $${r.preferred_deal_size_max || '?'}M`
      : '—';
    const ebitdaRange = (r.preferred_ebitda_min != null || r.preferred_ebitda_max != null)
      ? `$${r.preferred_ebitda_min || 0}M – $${r.preferred_ebitda_max || '?'}M`
      : '—';
    const emailVal = r.email || r.primary_contact_email;
    const emailCell = emailVal
      ? `<span style="color:#4ade80;font-family:var(--font-mono);font-size:11px">${esc(emailVal)}</span>`
      : '<span style="color:#555">—</span>';
    const lastInv = r.last_investment_date
      ? esc(r.last_investment_date).substring(0, 10)
      : (r.last_investment_company ? esc(r.last_investment_company).substring(0, 25) : '—');
    const activity12m = r.investments_last_12m != null ? r.investments_last_12m : '—';
    const dealCell = r._active_deal
      ? `<button onclick="showInvestorDealHistory('${r.id}','${esc(r.name)}')" style="background:none;border:none;cursor:pointer;color:#60a5fa;font-size:11px;padding:0">${esc(r._active_deal)}</button>`
      : (r._deal_count > 0
        ? `<button onclick="showInvestorDealHistory('${r.id}','${esc(r.name)}')" style="background:none;border:none;cursor:pointer;color:#6b7280;font-size:11px;padding:0">${r._deal_count} deal(s)</button>`
        : '<span style="color:#374151;font-size:11px">—</span>');
    return `<tr>
      <td><span style="font-weight:500;color:var(--text-bright)">${esc(r.name || '—')}</span><br><span style="font-size:11px;color:var(--text-muted)">${esc(r.hq_country || r.hq_location || '')}</span></td>
      <td style="font-size:12px">${esc(r.investor_type || '—')}</td>
      <td>${getContactTypeBadge(r.contact_type, r.is_angel)}</td>
      <td style="font-size:12px;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.preferred_industries || '')}">${esc((r.preferred_industries || '—').substring(0, 40))}</td>
      <td class="mono" style="font-size:12px">${r.aum_millions ? '$' + Number(r.aum_millions).toLocaleString() + 'M' : '—'}</td>
      <td style="font-size:12px">${esc(dealSize)}</td>
      <td style="font-size:12px">${esc(ebitdaRange)}</td>
      <td style="font-size:12px">${lastInv}</td>
      <td style="font-size:11px;text-align:center">${activity12m}</td>
      <td>${emailCell}</td>
      <td>${dealCell}</td>
    </tr>`;
  }).join('');
}

async function showInvestorDealHistory(investorsDbId, firmName) {
  try {
    const deals = await api(`/api/investors-db/${investorsDbId}/deals`);
    const modal = document.getElementById('deal-history-modal');
    const title = document.getElementById('deal-history-title');
    const body  = document.getElementById('deal-history-body');
    if (!modal || !body) return;
    title.textContent = `Deal History — ${firmName}`;
    if (!deals?.length) {
      body.innerHTML = '<p style="color:var(--text-dim);text-align:center;padding:24px">No deal history found</p>';
    } else {
      body.innerHTML = `<table class="data-table"><thead><tr><th>Deal</th><th>Score</th><th>Grade</th><th>Stage</th><th>Outcome</th><th>Added</th></tr></thead><tbody>
        ${deals.map(d => `<tr>
          <td style="font-weight:500">${esc(d.deal_name || '—')}</td>
          <td class="mono">${d.investor_score || '—'}</td>
          <td><span class="status-badge">${esc(d.grade || '—')}</span></td>
          <td>${esc(d.pipeline_stage || '—')}</td>
          <td>${esc(d.outcome || '—')}</td>
          <td style="font-size:11px;color:var(--text-dim)">${d.added_at ? d.added_at.substring(0, 10) : '—'}</td>
        </tr>`).join('')}
      </tbody></table>`;
    }
    modal.style.display = 'flex';
  } catch (err) {
    alert(`Could not load deal history: ${err.message}`);
  }
}

function renderPagination(containerId, currentPage, totalPages, onPageChangeFn) {
  const container = document.getElementById(containerId);
  if (!container) return;
  if (totalPages <= 1) { container.innerHTML = ''; return; }

  const show = new Set([1, totalPages, currentPage, currentPage - 1, currentPage + 1]
    .filter(p => p >= 1 && p <= totalPages));
  const sorted = [...show].sort((a, b) => a - b);
  const pages = [];
  let prev = 0;
  for (const p of sorted) {
    if (p - prev > 1) pages.push('...');
    pages.push(p);
    prev = p;
  }

  container.innerHTML = `<div style="display:flex;align-items:center;gap:6px;padding:16px 0;justify-content:center">
    <button onclick="${onPageChangeFn}(${currentPage - 1})" ${currentPage === 1 ? 'disabled' : ''}
      style="padding:6px 12px;background:#1a1a1a;border:1px solid #2a2a2a;color:${currentPage === 1 ? '#374151' : '#e5e7eb'};border-radius:4px;cursor:pointer">←</button>
    ${pages.map(p => p === '...'
      ? `<span style="color:#6b7280;padding:0 4px">…</span>`
      : `<button onclick="${onPageChangeFn}(${p})"
          style="padding:6px 10px;background:${p === currentPage ? '#2a2a2a' : '#1a1a1a'};border:1px solid ${p === currentPage ? '#4a4a4a' : '#2a2a2a'};color:${p === currentPage ? '#e5e7eb' : '#6b7280'};border-radius:4px;cursor:pointer">${p}</button>`
    ).join('')}
    <button onclick="${onPageChangeFn}(${currentPage + 1})" ${currentPage === totalPages ? 'disabled' : ''}
      style="padding:6px 12px;background:#1a1a1a;border:1px solid #2a2a2a;color:${currentPage === totalPages ? '#374151' : '#e5e7eb'};border-radius:4px;cursor:pointer">→</button>
    <span style="color:#6b7280;font-size:12px;margin-left:8px">Page ${currentPage} of ${totalPages}</span>
  </div>`;
}

// ─── EXCLUSION CSV (Launch Deal) ─────────────────────────────────────────────

let parsedExclusions = [];

window.handleExclusionDrop = function(e) {
  e.preventDefault();
  document.getElementById('exclusion-drop-zone').style.borderColor = '#2a2a2a';
  const file = e.dataTransfer?.files?.[0];
  if (file?.name.endsWith('.csv')) handleExclusionFile(file);
};

window.handleExclusionFile = function(file) {
  if (!file) return;
  const reader = new FileReader();
  reader.onload = (e) => {
    parsedExclusions = parseExclusionCsv(e.target.result);
    showExclusionPreview(parsedExclusions);
  };
  reader.readAsText(file);
};

function parseExclusionCsv(text) {
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  if (lines.length < 2) return [];
  const headers = lines[0].toLowerCase().split(',').map(h => h.trim().replace(/"/g, ''));
  const firmIdx   = headers.findIndex(h => h.includes('firm') || h.includes('company') || h.includes('organization'));
  const personIdx = headers.findIndex(h => h.includes('name') || h.includes('person') || h.includes('contact'));
  const emailIdx  = headers.findIndex(h => h.includes('email'));
  return lines.slice(1).map(line => {
    const cols = line.split(',').map(c => c.trim().replace(/"/g, ''));
    return {
      firm_name:   firmIdx   >= 0 ? cols[firmIdx]   : null,
      person_name: personIdx >= 0 ? cols[personIdx] : null,
      email:       emailIdx  >= 0 ? cols[emailIdx]  : null,
    };
  }).filter(r => r.firm_name || r.person_name || r.email);
}

function showExclusionPreview(exclusions) {
  const preview = document.getElementById('exclusion-preview');
  const count   = document.getElementById('exclusion-count');
  const sample  = document.getElementById('exclusion-sample');
  if (!preview) return;
  preview.style.display = 'block';
  count.textContent = `${exclusions.length} exclusions loaded`;
  const first5 = exclusions.slice(0, 5);
  sample.innerHTML = first5.map(e =>
    [e.firm_name, e.person_name, e.email].filter(Boolean).join(' · ')
  ).join('<br>') + (exclusions.length > 5 ? `<br>…and ${exclusions.length - 5} more` : '');
}

window.clearExclusionList = function() {
  parsedExclusions = [];
  const preview = document.getElementById('exclusion-preview');
  if (preview) preview.style.display = 'none';
  const input = document.getElementById('exclusion-csv-input');
  if (input) input.value = '';
};

// ─── PRIORITY LIST SELECTOR (Launch Deal) ────────────────────────────────────

window.selectedPriorityLists = [];

async function loadPriorityListsForLaunch() {
  window.selectedPriorityLists = [];
  renderPriorityList();
  const sel = document.getElementById('add-list-select');
  if (!sel) return;
  try {
    const lists = await api('/api/investor-lists');
    // Preserve first placeholder option
    sel.innerHTML = '<option value="">+ Add a list to prioritise\u2026</option>';
    (lists || []).forEach(l => {
      const opt = document.createElement('option');
      opt.value = l.id;
      opt.dataset.name = l.name;
      opt.dataset.type = l.list_type;
      opt.textContent = `${l.name} (${l.investor_count} investors)${l.list_type === 'warm' ? ' \u2605 Warm' : ''}`;
      sel.appendChild(opt);
    });
  } catch (e) {
    console.warn('[PRIORITY] Failed to load lists:', e.message);
  }
}

function addPriorityList() {
  const sel = document.getElementById('add-list-select');
  if (!sel) return;
  const opt = sel.options[sel.selectedIndex];
  if (!opt?.value) return;
  if (window.selectedPriorityLists.find(l => l.id === opt.value)) {
    showToast('Already in priority list');
    return;
  }
  const order = window.selectedPriorityLists.length + 1;
  window.selectedPriorityLists.push({ id: opt.value, name: opt.dataset.name, type: opt.dataset.type, order });
  renderPriorityList();
  sel.selectedIndex = 0;
}

function removePriorityList(id) {
  window.selectedPriorityLists = window.selectedPriorityLists.filter(l => l.id !== id);
  window.selectedPriorityLists.forEach((l, i) => { l.order = i + 1; });
  renderPriorityList();
}

function syncDropdownDisabled() {
  const sel = document.getElementById('add-list-select');
  if (!sel) return;
  const selectedIds = new Set((window.selectedPriorityLists || []).map(l => l.id));
  for (const opt of sel.options) {
    if (!opt.value) continue;
    opt.disabled = selectedIds.has(opt.value);
  }
}

function renderPriorityList() {
  const container = document.getElementById('priority-list-container');
  if (!container) return;
  syncDropdownDisabled();
  if (!window.selectedPriorityLists.length) {
    container.innerHTML = '<p style="color:#374151;font-size:12px;margin:0">No lists selected \u2014 will search full database.</p>';
    return;
  }
  container.innerHTML = window.selectedPriorityLists.map(l => `
    <div style="display:flex;align-items:center;gap:10px;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;border-radius:6px">
      <span style="color:var(--gold);font-size:13px;font-weight:700;min-width:20px">${l.order}</span>
      <span style="color:#e5e7eb;font-size:13px;flex:1">${esc(l.name)}</span>
      ${l.type === 'warm' ? '<span style="color:#f59e0b;font-size:11px">\u2605 WARM</span>' : ''}
      <button type="button" onclick="removePriorityList('${l.id}')"
        style="background:none;border:none;color:#6b7280;cursor:pointer;font-size:16px;line-height:1">&#215;</button>
    </div>
  `).join('');
}

// ─── ANALYTICS PAGE ───────────────────────────────────────────────────────────

async function loadAnalyticsPage() {
  switchAnalyticsTab('performance');
  loadAnalyticsPerformance();
  loadAnalyticsRecommendations();
}

function switchAnalyticsTab(tab) {
  document.getElementById('analytics-panel-performance').style.display = tab === 'performance' ? '' : 'none';
  document.getElementById('analytics-panel-recommendations').style.display = tab === 'recommendations' ? '' : 'none';
  const perfBtn = document.getElementById('analytics-tab-performance');
  const recBtn  = document.getElementById('analytics-tab-recommendations');
  if (perfBtn) {
    perfBtn.style.borderBottomColor = tab === 'performance' ? 'var(--gold)' : 'transparent';
    perfBtn.style.color = tab === 'performance' ? 'var(--gold)' : 'var(--text-dim)';
  }
  if (recBtn) {
    recBtn.style.borderBottomColor = tab === 'recommendations' ? 'var(--gold)' : 'transparent';
    recBtn.style.color = tab === 'recommendations' ? 'var(--gold)' : 'var(--text-dim)';
  }
}

async function loadAnalyticsPerformance() {
  const el = document.getElementById('analytics-summary-table');
  if (!el) return;
  try {
    const data = await api('/api/analytics/summary');
    if (!data?.length) {
      el.innerHTML = '<div class="loading-placeholder">No analytics data yet. Run analysis to generate metrics.</div>';
      return;
    }

    const DAY_NAMES = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
    el.innerHTML = `
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Deal</th>
              <th>Week</th>
              <th>Outreach</th>
              <th>Email Rate</th>
              <th>LI Rate</th>
              <th>Overall Rate</th>
              <th>Meetings</th>
              <th>Best Day</th>
              <th>Best Hour</th>
            </tr>
          </thead>
          <tbody>
            ${data.map(r => `
              <tr>
                <td style="font-weight:500">${esc(r.deals?.name || r.deal_id || '—')}</td>
                <td style="font-size:12px;color:var(--text-dim)">${esc(r.week_starting || '—')}</td>
                <td>${r.total_outreach || 0}</td>
                <td>${r.email_response_rate ? (r.email_response_rate * 100).toFixed(1) + '%' : '—'}</td>
                <td>${r.linkedin_response_rate ? (r.linkedin_response_rate * 100).toFixed(1) + '%' : '—'}</td>
                <td style="font-weight:600;color:${(r.overall_response_rate || 0) > 0.1 ? '#4ade80' : 'var(--text-bright)'}">
                  ${r.overall_response_rate ? (r.overall_response_rate * 100).toFixed(1) + '%' : '—'}
                </td>
                <td>${r.meetings_booked || 0}</td>
                <td>${r.best_response_day != null ? (DAY_NAMES[r.best_response_day] || r.best_response_day) : '—'}</td>
                <td>${r.best_response_hour != null ? r.best_response_hour + ':00' : '—'}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    `;
  } catch (e) {
    el.innerHTML = `<div class="loading-placeholder text-red">Failed to load: ${esc(e.message)}</div>`;
  }
}

async function loadAnalyticsRecommendations() {
  const el = document.getElementById('analytics-recs-list');
  if (!el) return;
  try {
    const data = await api('/api/analytics/recommendations');
    if (!data?.length) {
      el.innerHTML = '<div class="loading-placeholder">No recommendations yet. Run analysis to generate insights.</div>';
      return;
    }

    const CATEGORY_COLOURS = {
      timing: '#a78bfa', copy: '#60a5fa', targeting: '#f59e0b',
      sequence: '#34d399', channel: '#fb7185',
    };

    const pending = data.filter(r => r.status === 'pending');
    const applied = data.filter(r => r.status === 'applied');
    const rejected = data.filter(r => r.status === 'rejected');

    const renderCard = (r) => `
      <div style="padding:16px;background:var(--surface-2);border:1px solid var(--border);border-radius:8px;margin-bottom:10px">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">
          <span style="padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;text-transform:uppercase;
            background:${CATEGORY_COLOURS[r.category] || '#6b7280'}22;color:${CATEGORY_COLOURS[r.category] || '#6b7280'}">
            ${esc(r.category || '—')}
          </span>
          <span style="font-weight:600;font-size:14px;color:var(--text-bright)">${esc(r.title || '—')}</span>
          ${r.status === 'applied' ? '<span style="margin-left:auto;color:#4ade80;font-size:11px">\u2713 Applied</span>' : ''}
          ${r.status === 'rejected' ? '<span style="margin-left:auto;color:#6b7280;font-size:11px">Dismissed</span>' : ''}
        </div>
        <p style="font-size:13px;color:var(--text-dim);margin:0 0 6px">${esc(r.insight || '')}</p>
        <p style="font-size:13px;color:var(--text-bright);margin:0 0 10px">${esc(r.recommendation || '')}</p>
        ${r.status === 'pending' ? `
          <div style="display:flex;gap:8px">
            <button onclick="applyRecommendation('${r.id}', this)"
              style="padding:6px 14px;background:#d4a847;border:none;color:#000;border-radius:5px;font-size:12px;font-weight:600;cursor:pointer">
              Apply \u2192
            </button>
            <button onclick="dismissRecommendation('${r.id}', this)"
              style="padding:6px 14px;background:none;border:1px solid var(--border);color:var(--text-dim);border-radius:5px;font-size:12px;cursor:pointer">
              Dismiss
            </button>
          </div>
        ` : ''}
      </div>
    `;

    let html = '';
    if (pending.length) {
      html += `<h3 style="font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);margin:0 0 12px">Pending (${pending.length})</h3>`;
      html += pending.map(renderCard).join('');
    }
    if (applied.length) {
      html += `<h3 style="font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);margin:16px 0 12px">Applied (${applied.length})</h3>`;
      html += applied.map(renderCard).join('');
    }
    if (rejected.length) {
      html += `<details style="margin-top:16px"><summary style="font-size:12px;color:var(--text-dim);cursor:pointer">Dismissed (${rejected.length})</summary><div style="margin-top:8px">${rejected.map(renderCard).join('')}</div></details>`;
    }
    el.innerHTML = html;
  } catch (e) {
    el.innerHTML = `<div class="loading-placeholder text-red">Failed: ${esc(e.message)}</div>`;
  }
}

async function applyRecommendation(id, btn) {
  if (btn) { btn.textContent = 'Applying\u2026'; btn.disabled = true; }
  try {
    await api(`/api/analytics/recommendations/${id}/apply`, 'POST', {});
    showToast('Recommendation applied');
    loadAnalyticsRecommendations();
  } catch (e) {
    showToast(`Failed: ${e.message}`, 'error');
    if (btn) { btn.textContent = 'Apply \u2192'; btn.disabled = false; }
  }
}

async function dismissRecommendation(id, btn) {
  if (btn) { btn.textContent = 'Dismissing\u2026'; btn.disabled = true; }
  try {
    await api(`/api/analytics/recommendations/${id}/dismiss`, 'POST', {});
    loadAnalyticsRecommendations();
  } catch (e) {
    showToast(`Failed: ${e.message}`, 'error');
    if (btn) { btn.textContent = 'Dismiss'; btn.disabled = false; }
  }
}

async function runAnalyticsNow(btn) {
  if (btn) { btn.textContent = 'Running\u2026'; btn.disabled = true; }
  try {
    await api('/api/action', 'POST', { action: 'run_analytics' });
    showToast('Analytics queued — check back in a minute');
  } catch {
    showToast('Trigger not available — analytics will run automatically on next weekly cycle');
  } finally {
    if (btn) { btn.textContent = '\u25BA Run Analysis Now'; btn.disabled = false; }
  }
}

function onDbFileSelected(input) {
  if (!input.files?.[0]) return;
  document.getElementById('db-drop-text').textContent = `📄 ${input.files[0].name}`;
  uploadInvestorXLSX(input.files[0]);
}

function onDbFileDrop(e) {
  e.preventDefault();
  document.getElementById('db-drop-zone')?.classList.remove('drag-over');
  const file = e.dataTransfer?.files?.[0];
  if (file && (file.name.endsWith('.xlsx') || file.name.endsWith('.xls'))) {
    const input = document.getElementById('db-file-input');
    const dt = new DataTransfer();
    dt.items.add(file);
    input.files = dt.files;
    document.getElementById('db-drop-text').textContent = `📄 ${file.name}`;
    uploadInvestorXLSX(file);
  }
}

async function uploadInvestorXLSX(file) {
  const progressEl = document.getElementById('db-import-progress');
  const resultEl   = document.getElementById('db-import-result');
  const dropZone   = document.getElementById('db-drop-zone');
  progressEl.style.display = '';
  resultEl.style.display   = 'none';
  dropZone.style.opacity   = '0.4';
  document.getElementById('db-import-msg').textContent = `Importing ${file.name}…`;

  try {
    const fd = new FormData();
    fd.append('file', file);
    const listNameEl = document.getElementById('import-list-name');
    if (listNameEl?.value?.trim()) fd.append('list_name', listNameEl.value.trim());
    const res = await fetch('/api/investors-db/import', { method: 'POST', body: fd });
    const data = await res.json();
    progressEl.style.display = 'none';
    dropZone.style.opacity   = '';
    if (data.error) throw new Error(data.error);
    resultEl.style.display = '';
    resultEl.style.color   = 'var(--text-bright)';
    const newCount     = data.imported ?? 0;
    const updatedCount = data.updated  ?? 0;
    const totalCount   = data.total;
    let msg = `✓ Import complete — ${newCount.toLocaleString()} new investors added`;
    if (updatedCount > 0) msg += `, ${updatedCount.toLocaleString()} existing updated`;
    if (data.skipped)     msg += `, ${data.skipped} skipped`;
    if (totalCount)       msg += `. Total DB: ${totalCount.toLocaleString()}`;
    resultEl.textContent = msg;
    document.getElementById('db-drop-text').textContent = 'Drop XLSX here or click to browse';
    document.getElementById('db-file-input').value = '';
    loadDatabase();
  } catch (err) {
    progressEl.style.display = 'none';
    dropZone.style.opacity   = '';
    resultEl.style.display   = '';
    resultEl.style.color     = 'var(--red, #f87171)';
    resultEl.textContent     = `✗ Import failed: ${err.message}`;
  }
}
