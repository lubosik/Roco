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
const API_BASE          = '';


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
  targetAmount: '$50M', sector: 'Industrial Real Estate',
  keyMetrics: '7.8% yield, 95% occupancy, 8-year WAULT',
  geography: 'US', minCheque: '$500k', maxCheque: '$5M',
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
let _pendingReviewsCount = 0; // campaign batches awaiting approval — kept in sync by loadQueue
let selectedDealId  = null;  // deal detail panel
let selectedDealReadOnly   = false;   // true when viewing from Archive (no Roco actions)
let selectedDealBackSection = 'deals'; // where Back button navigates to
let docUploadController = null;
let campaignReviewPage = 1;
const CAMPAIGN_FIRMS_PER_PAGE = 20;
// Deal brief launch state
let dealBriefEditMode = false;
let currentParsedDeal = null;
let currentDocumentId = null;
let investorProfileContactId = null;
let investorProfileDealId = null;
let transcriptDealContacts = [];

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
    case 'transcripts':     loadMeetingTranscriptsPage(); break;
    case 'investor-profile': loadInvestorProfilePage(); break;
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

async function api(path, method = 'GET', body = null, options = {}) {
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
    hideApiAlert();
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('application/json')) return res.json();
    return res.text();
  } catch (err) {
    console.error(`[ROCO] API error ${method} ${path}:`, err);
    if (!options.silent) {
      const message = String(err?.message || '').trim() === 'Failed to fetch'
        ? 'Dashboard connection dropped briefly. Retrying…'
        : err.message;
      showApiAlert(message);
    }
    throw err;
  }
}

function showApiAlert(msg) {
  const el = document.getElementById('api-alert');
  const msgEl = document.getElementById('api-alert-msg');
  if (msgEl) msgEl.textContent = `⚠ ${msg}`;
  el.classList.remove('hidden');
}

function hideApiAlert() {
  const el = document.getElementById('api-alert');
  if (el) el.classList.add('hidden');
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
    case 'ACTIVITY': {
      const entry = msg.entry || msg.data || msg;
      prependActivity(entry);
      // If activity page is open and on page 1, also prepend to paginated view
      const isOnActivityPage = window.location.hash === '#activity';
      if (isOnActivityPage && _activityPage === 1) {
        handleLiveActivityForPage(entry);
      }
      break;
    }

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
      if (!document.getElementById('view-queue')?.classList.contains('hidden')) {
        loadQueue(); // full refresh — sets _pendingReviewsCount and badge correctly
      } else {
        // Queue not visible — refresh reviews count in background so badge stays accurate
        api('/api/campaign-reviews').then(r => {
          _pendingReviewsCount = (r || []).length;
          refreshQueueBadge(msg.count ?? null);
        }).catch(() => refreshQueueBadge(msg.count ?? null));
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
    const state = await api('/api/state', 'GET', null, { silent: true });
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
  const filterIds = ['pipeline-deal-filter','activity-deal-filter'];
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
  await Promise.all([refreshStats(), loadActivityLog(true)]);
}

async function refreshStats() {
  try {
    const stats = await api('/api/stats', 'GET', null, { silent: true });
    applyStats(stats);
  } catch { /* silent */ }
}

function applyStats(s) {
  if (!s) return;
  setText('stat-active-deals',   fmt(s.active_deals));
  setText('stat-total-deals',    fmt(s.total_deals_launched));
  setText('stat-emails-sent',    fmt(s.emailsSent || s.emails_sent));
  setText('stat-emails-replied', fmt(s.emails_replied || 0));
  setText('stat-response-rate',  s.response_rate != null ? pct(s.response_rate) : (s.responseRate != null ? pct(s.responseRate) : '—'));
  if (s.emails_replied != null) setText('stat-response-sub', `${fmt(s.emails_replied)} email repl${s.emails_replied === 1 ? 'y' : 'ies'}`);
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
  setText('stat-emails-replied-sub', s.emailsRepliedPeriod || 'inbound replies');
  setText('stat-prospects-sub',  s.prospectsPeriod || '');
  setText('stat-queue-sub',      s.queuePeriod || '');

  // LinkedIn metrics
  setText('stat-li-invites',       fmt(s.li_invites_sent));
  setText('stat-li-invites-sub',   s.li_active_pending != null ? `${fmt(s.li_active_pending)} active pending` : '');
  setText('stat-li-acceptance',    fmt(s.li_accepts != null ? s.li_accepts : 0));
  setText('stat-li-acceptance-sub', s.li_acceptance_rate != null ? pct(s.li_acceptance_rate) + ' acceptance rate' : '');
  setText('stat-li-dms',           fmt(s.li_dms_sent));
  setText('stat-li-dm-response',   s.li_dm_response_rate != null ? pct(s.li_dm_response_rate) : '—');

  refreshQueueBadge(s.queueCount || s.approval_queue);
}

function refreshQueueBadge(count, includesReviews = false) {
  const badge = document.getElementById('queue-badge');
  if (!badge) return;
  // If caller already included reviews (loadQueue), use as-is; otherwise add cached reviews count
  const total = (count || 0) + (includesReviews ? 0 : _pendingReviewsCount);
  if (total > 0) {
    badge.textContent = total;
    badge.style.display = '';
  } else {
    badge.style.display = 'none';
  }
}

let healthCheckFailures = 0;
const HEALTH_FAIL_THRESHOLD = 3; // only show error after 3 consecutive failures

async function refreshHealth() {
  try {
    const h = await api('/api/health', 'GET', null, { silent: true });
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
    const [recentData, fullData] = await Promise.all([
      api('/api/activity/recent', 'GET', null, { silent: true }).catch(() => []),
      api('/api/activity/log?limit=200', 'GET', null, { silent: true }).catch(() => []),
    ]);
    const recentItems = Array.isArray(recentData) ? recentData : (recentData.log || recentData.items || recentData.events || []);
    const fullItems = Array.isArray(fullData) ? fullData : (fullData.log || fullData.items || fullData.events || []);
    activityLog = fullItems.length ? fullItems : recentItems;
    if (renderToOverview) renderActivityFeed('overview-activity', recentItems.slice(0, 10));
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
    const type   = String(item.type || item.event_type || item.activityType || 'system').toLowerCase();
    const badge  = getActivityBadgeMeta(item);
    const action = item.action || '';
    const note   = item.note || item.message || item.text || item.summary || '';
    const isThinking  = type === 'thinking';
    const isExpandedType = isThinking || (type === 'research' && !!item.full_content);
    const fullContent = item.full_content || null;
    const displayText = isExpandedType && fullContent
      ? fullContent
      : (action && note ? `${action} · ${note}` : (action || note || ''));
    const ts   = item.timestamp || item.createdAt || item.created_at;
    const deal = item.deal_name || item.deal;
    return `<div class="feed-item${isExpandedType ? ' feed-item--thinking' : ''}">
      <span class="feed-time">${formatTime(ts)}</span>
      <span class="feed-badge ${badge.className}">${badge.label}</span>
      ${isThinking ? '<span class="feed-badge" style="background:rgba(167,139,250,0.15);color:#A78BFA;font-size:9px;padding:1px 5px">full reasoning</span>' : ''}
      ${type === 'research' && fullContent ? '<span class="feed-badge" style="background:rgba(96,165,250,0.15);color:#60A5FA;font-size:9px;padding:1px 5px">research trace</span>' : ''}
      ${deal ? `<span class="feed-deal">${esc(deal)}</span>` : ''}
      <span class="feed-text" style="${isExpandedType ? 'white-space:pre-wrap;display:block;margin-top:4px;' : ''}">${esc(displayText)}</span>
    </div>`;
  }).join('');
}

function prependActivity(item) {
  activityLog.unshift(item);
  if (activityLog.length > 200) activityLog.pop();

  // Update overview feed
  const overviewFeed = document.getElementById('overview-activity');
  if (overviewFeed) {
    const badge  = getActivityBadgeMeta(item);
    const action = item.action || '';
    const note   = item.note || item.message || item.text || item.summary || '';
    const type   = String(item.type || item.event_type || item.activityType || 'system').toLowerCase();
    const text   = type === 'research' && item.full_content
      ? item.full_content
      : (action && note ? `${action} · ${note}` : (action || note || ''));
    const ts     = item.timestamp || item.createdAt || item.created_at;
    const deal   = item.deal_name || item.deal;
    const div = document.createElement('div');
    div.className = 'feed-item';
    div.innerHTML = `
      <span class="feed-time">${formatTime(ts)}</span>
      <span class="feed-badge ${badge.className}">${badge.label}</span>
      ${deal ? `<span class="feed-deal">${esc(deal)}</span>` : ''}
      <span class="feed-text" style="${type === 'research' && item.full_content ? 'white-space:pre-wrap;display:block;margin-top:4px;' : ''}">${esc(text)}</span>
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
  if (typeFilter) {
    filtered = filtered.filter(i => {
      const badge = getActivityBadgeMeta(i).className;
      if (typeFilter === 'linkedin') return ['linkedin', 'invite', 'dm', 'accepted'].includes(badge);
      return badge === typeFilter;
    });
  }
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
  // Load email account options
  loadEmailAccountOptions();

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
  const hasQueuedPbUploads = !!(window.pbFilesQueue?.investors || window.pbFilesQueue?.deals);

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
    target_geography:      document.getElementById('deal-geography')?.value || 'Global',
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
    knowledge_base_list_id:   (() => { const s = document.getElementById('launch-kb-select'); return s?.value || null; })(),
    knowledge_base_list_name: (() => { const s = document.getElementById('launch-kb-select'); return s?.options[s.selectedIndex]?.textContent?.split(' (')[0] || null; })(),
    exclusions:            JSON.stringify(parsedExclusions || []),
    sending_account_id:    document.getElementById('launch-email-account')?.value || null,
    sending_email:         (() => {
      const sel = document.getElementById('launch-email-account');
      return sel?.options[sel.selectedIndex]?.dataset?.email || null;
    })(),
    pending_intelligence_uploads: hasQueuedPbUploads,
  };

  btn.disabled    = true;
  btn.textContent = '⏳ Launching…';

  try {
    let createdDealId = null;
    const pbQueueSnapshot = {
      investors: window.pbFilesQueue?.investors || null,
      deals: window.pbFilesQueue?.deals || null,
    };
    const exclusionFileSnapshot = queuedExclusionFile;

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
      const created = await res.json().catch(() => ({}));
      createdDealId = created?.deal?.id || null;
    } else {
      payload.timezone       = document.getElementById('nd-timezone')?.value || 'America/New_York';
      payload.activeDays     = getDayPickerValue('nd-active-days');
      payload.liConnectFrom  = document.getElementById('nd-li-connect-from')?.value || '08:00';
      payload.liConnectUntil = document.getElementById('nd-li-connect-until')?.value || '20:00';
      payload.liDmFrom       = document.getElementById('nd-li-dm-from')?.value || '20:00';
      payload.liDmUntil      = document.getElementById('nd-li-dm-until')?.value || '23:00';
      payload.emailFrom      = document.getElementById('nd-email-from')?.value || '08:00';
      payload.emailUntil     = document.getElementById('nd-email-until')?.value || '18:00';
      const created = await api('/api/deals/create', 'POST', payload);
      createdDealId = created?.deal?.id || null;
    }

    form.reset();
    document.getElementById('file-drop-text').textContent = 'Drop CSV here or click to browse';
    clearExclusionList();
    resetLaunchStep();
    window.pbFilesQueue = { investors: null, deals: null };
    const kbSelReset = document.getElementById('launch-kb-select');
    if (kbSelReset) kbSelReset.value = '';
    await populateDealSelector();
    navigate('#deals');
    showToast('Deal launched. Follow-up imports and research are continuing in the background.');

    if (createdDealId) {
      Promise.resolve().then(async () => {
        const pbResults = await uploadQueuedPbFiles(createdDealId, pbQueueSnapshot);
        await uploadQueuedExclusionFile(createdDealId, exclusionFileSnapshot);
        const hadInvestorUniverse = !!pbQueueSnapshot.investors;
        const investorUniverseOk = !hadInvestorUniverse || pbResults?.investors?.ok;
        if (hasQueuedPbUploads && investorUniverseOk) {
          await api(`/api/deals/${createdDealId}/trigger-research`, 'POST');
        } else if (hasQueuedPbUploads && !investorUniverseOk) {
          showToast('PitchBook Investor Universe import failed. Research was not started.', 'error', 6000);
        }
      }).catch(err => {
        console.error('[LAUNCH] Background post-create steps failed:', err);
        showToast(`Post-launch import failed: ${err.message}`, 'error', 5000);
      });
    }
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
      patchDealsGrid(activeDeals);
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
    patchDealsGrid(activeDeals, true);
  } catch {
    grid.innerHTML = '<div class="loading-placeholder text-red">Failed to load deals.</div>';
  }
}

function patchDealsGrid(deals, forceReplace = false) {
  const grid = document.getElementById('deals-grid');
  if (!grid) return;
  if (forceReplace || !grid.querySelector('.deal-card')) {
    grid.innerHTML = deals.map(deal => renderDealCard(deal)).join('');
    return;
  }

  const nextIds = new Set(deals.map(deal => String(deal.id || deal._id)));
  deals.forEach(deal => {
    const id = String(deal.id || deal._id);
    const existing = document.getElementById(`deal-card-${id}`);
    const html = renderDealCard(deal);
    if (existing) {
      existing.outerHTML = html;
    } else {
      grid.insertAdjacentHTML('beforeend', html);
    }
  });

  grid.querySelectorAll('.deal-card[id^="deal-card-"]').forEach(card => {
    const id = String(card.id.replace('deal-card-', ''));
    if (!nextIds.has(id)) card.remove();
  });
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
  const firms    = fmt(deal.firms || deal.live_firms || deal.current_batch_ranked_firms || 0);
  const rr       = (deal.response_rate ?? deal.responseRate) != null ? pct(deal.response_rate ?? deal.responseRate) : '—';
  const paused   = deal.paused === true || status === 'paused';
  const badgeLabel = paused ? 'PAUSED' : status.toUpperCase();
  const badgeClass = paused ? 'paused' : status;
  const needsReview = deal.current_batch_status === 'pending_approval';

  return `<div class="deal-card" id="deal-card-${id}" data-deal-id="${id}">
    <div class="deal-card-top">
      <div>
        <div class="deal-card-name">${name}</div>
        <div class="deal-card-type">${type}</div>
      </div>
      <div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px">
        <span class="status-badge ${badgeClass}">${badgeLabel}</span>
        ${needsReview ? `<span class="status-badge" style="background:rgba(234,179,8,.15);color:#eab308;font-size:9px;cursor:pointer" onclick="viewDeal('${id}');setTimeout(()=>switchDealTab('rankings',document.querySelector('[data-tab=rankings]')),400)">REVIEW REQUIRED</span>` : ''}
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
        <div class="deal-stat-val">${firms}</div>
        <div class="deal-stat-lbl">Firms</div>
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

function mergeDealIntoCache(updatedDeal) {
  if (!updatedDeal) return;
  const updatedId = updatedDeal.id || updatedDeal._id;
  if (!updatedId) return;
  const existingIndex = (allDeals || []).findIndex(d => (d.id || d._id) === updatedId);
  if (existingIndex >= 0) {
    allDeals[existingIndex] = { ...allDeals[existingIndex], ...updatedDeal };
  } else {
    allDeals = [...(allDeals || []), updatedDeal];
  }
}

function refreshSelectedDealChrome(deal) {
  if (!deal || !selectedDealId) return;
  window.__activeDealCurrency = deal.currency || 'USD';
  setText('deal-detail-name', deal.dealName || deal.name || selectedDealId);
  const statusBadge = document.getElementById('deal-detail-status-badge');
  if (statusBadge) {
    const normalizedStatus = String(deal.status || (selectedDealReadOnly ? 'closed' : 'active')).toLowerCase();
    statusBadge.textContent = normalizedStatus.toUpperCase();
    statusBadge.className = `status-badge ${normalizedStatus}`;
  }
}

async function quietRefreshSelectedDeal() {
  if (!selectedDealId) return;
  try {
    const deal = await api(`/api/deals/${selectedDealId}`);
    mergeDealIntoCache(deal);
    refreshSelectedDealChrome(deal);
    await patchOpenDealTabInPlace(selectedDealId, deal);
  } catch {
    // Preserve the current open state if the quiet refresh fails.
  }
}

async function patchOpenDealTabInPlace(dealId, deal = null) {
  const activeTab = document.querySelector('.deal-tab.active')?.dataset?.tab;
  if (!activeTab) return;
  if (activeTab === 'overview') {
    await patchDealOverviewInPlace(dealId, deal);
  } else if (activeTab === 'rankings') {
    await patchCampaignTrackerInPlace(dealId);
  } else if (activeTab === 'pipeline') {
    await patchDealPipelineInPlace(dealId);
  } else if (activeTab === 'archived') {
    await patchDealArchivedInPlace(dealId);
  }
}

async function switchDealTab(tab, btn) {
  document.querySelectorAll('.deal-tab').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');

  ['overview','brief','pipeline','rankings','batches','settings','archived','exclusions','templates'].forEach(t => {
    const el = document.getElementById(`deal-tab-${t}`);
    if (el) el.classList.toggle('hidden', t !== tab);
  });

  if (!selectedDealId) return;
  switch (tab) {
    case 'overview':   await loadDealTabOverview(selectedDealId);   break;
    case 'brief':      await loadDealTabBrief(selectedDealId);      break;
    case 'pipeline':   await loadDealTabPipeline(selectedDealId);   break;
    case 'rankings':   await loadCampaignReviewTab(selectedDealId); break;
    case 'batches':    await loadDealTabBatches(selectedDealId);    break;
    case 'settings':   await loadDealTabSettings(selectedDealId);   break;
    case 'archived':   await loadDealTabArchived(selectedDealId);   break;
    case 'exclusions': await loadDealTabExclusions(selectedDealId); break;
    case 'templates':  await loadDealTemplatesTab(selectedDealId);  break;
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

  const stat = (val, lbl, key, sub = '') => `<div class="deal-stat">
    <div class="deal-stat-val" data-overview-stat="${key}">${val}</div>
    <div class="deal-stat-lbl">${lbl}</div>
    <div data-overview-substat="${key}" style="font-size:10px;color:var(--text-dim);margin-top:2px;${sub ? '' : 'display:none'}">${sub || ''}</div>
  </div>`;

  el.innerHTML = `
    <div data-overview-deal-id="${id}">
    <div style="margin-bottom:6px;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em">Fundraise</div>
    <div class="deal-stats" style="grid-template-columns:repeat(5,1fr);gap:16px;margin-bottom:24px">
      ${stat(formatMoney(m.capitalCommitted || deal.committed_amount || 0, deal.currency || 'USD'), 'Capital Committed', 'capitalCommitted')}
      ${stat(formatMoney(m.targetAmount || deal.target_amount || 0, deal.currency || 'USD'), 'Target', 'targetAmount')}
      ${stat(fmt(m.firms || deal.firms || deal.live_firms || deal.current_batch_ranked_firms || 0), 'Firms', 'firms')}
      ${stat(fmt(m.activeProspects || 0), 'Active Prospects', 'activeProspects')}
      ${stat(fmt(m.totalContacts || 0), 'Total Contacts', 'totalContacts')}
    </div>
    <div style="margin-bottom:6px;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em">Email</div>
    <div class="deal-stats" style="grid-template-columns:repeat(5,1fr);gap:16px;margin-bottom:24px">
      ${stat(fmt(m.emailsSent || 0), 'Emails Sent', 'emailsSent')}
      ${stat(fmt(m.emailReplies != null ? m.emailReplies : (m.emailResponses || 0)), 'Replies', 'emailReplies')}
      ${stat(fmt(m.emailsOpened || 0), 'Opened', 'emailsOpened', m.emailOpenRate != null ? m.emailOpenRate + '% open rate' : '')}
      ${stat(fmt(m.emailsClicked || 0), 'Clicked', 'emailsClicked', m.emailClickRate != null ? m.emailClickRate + '% click rate' : '')}
      ${stat(m.emailResponseRate != null ? m.emailResponseRate + '%' : '—', 'Response Rate', 'emailResponseRate')}
    </div>
    <div style="margin-bottom:6px;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em">LinkedIn</div>
    <div class="deal-stats" style="grid-template-columns:repeat(5,1fr);gap:16px;margin-bottom:24px">
      ${stat(fmt(m.invitesSent || 0), 'Invites Sent', 'invitesSent', m.activePendingInvites != null ? `${fmt(m.activePendingInvites)} active pending` : '')}
      ${stat(fmt(m.invitesAccepted || 0), 'Accepted', 'invitesAccepted', m.acceptanceRate != null ? m.acceptanceRate + '% rate' : '')}
      ${stat(fmt(m.dmsSent || 0), 'DMs Sent', 'dmsSent')}
      ${stat(fmt(m.dmResponses || 0), 'DM Replies', 'dmResponses')}
      ${stat(m.dmResponseRate != null ? m.dmResponseRate + '%' : '—', 'DM Response Rate', 'dmResponseRate')}
    </div>
    <div class="form-group mb-16">
      <div class="form-label">Description</div>
      <div style="color:var(--text-mid);font-size:13px;line-height:1.6">${esc(deal.description || '—')}</div>
    </div>
    ${deal.sector ? `<div class="form-group"><div class="form-label">Sector</div><div style="color:var(--text-mid)">${esc(deal.sector)}</div></div>` : ''}
    </div>
  `;
}

async function patchDealOverviewInPlace(id, deal = null) {
  const container = document.querySelector(`#deal-tab-overview [data-overview-deal-id="${id}"]`);
  if (!container) return;
  if (!deal) {
    try { deal = await api(`/api/deals/${id}`); } catch { deal = {}; }
  }
  let m = {};
  try { m = await api(`/api/deals/${id}/metrics`); } catch {}
  const currency = deal.currency || 'USD';
  const values = {
    capitalCommitted: formatMoney(m.capitalCommitted || deal.committed_amount || 0, currency),
    targetAmount: formatMoney(m.targetAmount || deal.target_amount || 0, currency),
    firms: fmt(m.firms || deal.firms || deal.live_firms || deal.current_batch_ranked_firms || 0),
    activeProspects: fmt(m.activeProspects || 0),
    totalContacts: fmt(m.totalContacts || 0),
    emailsSent: fmt(m.emailsSent || 0),
    emailReplies: fmt(m.emailReplies != null ? m.emailReplies : (m.emailResponses || 0)),
    emailsOpened: fmt(m.emailsOpened || 0),
    emailsClicked: fmt(m.emailsClicked || 0),
    emailResponseRate: m.emailResponseRate != null ? `${m.emailResponseRate}%` : '—',
    emailOpenRate: m.emailOpenRate != null ? `${m.emailOpenRate}%` : '',
    emailClickRate: m.emailClickRate != null ? `${m.emailClickRate}%` : '',
    invitesSent: fmt(m.invitesSent || 0),
    invitesAccepted: fmt(m.invitesAccepted || 0),
    dmsSent: fmt(m.dmsSent || 0),
    dmResponses: fmt(m.dmResponses || 0),
    dmResponseRate: m.dmResponseRate != null ? `${m.dmResponseRate}%` : '—',
  };
  const subvalues = {
    emailsOpened: m.emailOpenRate != null ? `${m.emailOpenRate}% open rate` : '',
    emailsClicked: m.emailClickRate != null ? `${m.emailClickRate}% click rate` : '',
    invitesSent: m.activePendingInvites != null ? `${fmt(m.activePendingInvites)} active pending` : '',
    invitesAccepted: m.acceptanceRate != null ? `${m.acceptanceRate}% rate` : '',
  };
  Object.entries(values).forEach(([key, value]) => {
    const node = container.querySelector(`[data-overview-stat="${key}"]`);
    if (node) node.textContent = value;
  });
  Object.entries(subvalues).forEach(([key, value]) => {
    const node = container.querySelector(`[data-overview-substat="${key}"]`);
    if (!node) return;
    node.textContent = value || '';
    node.style.display = value ? '' : 'none';
  });
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

function sentimentBadge(intentLabel, intentKey, convState) {
  if (convState === 'meeting_booked')            return `<span style="background:rgba(74,222,128,0.15);color:#4ade80;padding:2px 7px;border-radius:3px;font-size:10px;font-family:var(--font-mono)">Meeting Booked</span>`;
  if (convState === 'conversation_ended_positive') return `<span style="background:rgba(74,222,128,0.12);color:#4ade80;padding:2px 7px;border-radius:3px;font-size:10px;font-family:var(--font-mono)">Closed Positive</span>`;
  if (convState === 'conversation_ended_negative') return `<span style="background:rgba(220,50,50,0.12);color:#e05c5c;padding:2px 7px;border-radius:3px;font-size:10px;font-family:var(--font-mono)">Closed Negative</span>`;
  if (convState === 'do_not_contact')             return `<span style="background:rgba(220,50,50,0.12);color:#e05c5c;padding:2px 7px;border-radius:3px;font-size:10px;font-family:var(--font-mono)">Do Not Contact</span>`;
  if (convState === 'temp_closed')                return `<span style="background:rgba(212,168,71,0.15);color:var(--gold);padding:2px 7px;border-radius:3px;font-size:10px;font-family:var(--font-mono)">Temp Closed</span>`;
  if (!intentKey && !intentLabel) return '';
  const colours = { positive: '#4ade80', soft: 'var(--gold)', negative: '#e05c5c', question: '#7dd3fc', unknown: 'var(--text-dim)' };
  const bg = { positive: 'rgba(74,222,128,0.12)', soft: 'rgba(212,168,71,0.12)', negative: 'rgba(220,50,50,0.12)', question: 'rgba(125,211,252,0.12)', unknown: 'rgba(255,255,255,0.05)' };
  const col = colours[intentLabel] || colours.unknown;
  const bgCol = bg[intentLabel] || bg.unknown;
  const label = intentKey ? intentKey.replace(/_/g,' ') : intentLabel || '';
  return `<span style="background:${bgCol};color:${col};padding:2px 7px;border-radius:3px;font-size:10px;font-family:var(--font-mono)">${esc(label)}</span>`;
}

function renderDealPipelineRows(rows, dealId) {
  const renderScheduledFollowUp = (value) => {
    if (!value) return '<span class="text-dim">—</span>';
    return `<span class="text-dim">${formatScheduleDate(value)}</span>`;
  };

  return rows.map(r => `
    <tr data-pipeline-contact-id="${r.id}" style="cursor:pointer" onclick="toggleDealPipelineDetail('${r.id}', '${dealId}')">
      <td>
        <div style="font-weight:500">${esc(r.name || '—')}</div>
        ${r.email ? `<div style="font-size:11px;color:var(--text-dim)">${esc(r.email)}</div>` : ''}
      </td>
      <td class="text-dim" style="font-size:12px">${r.jobTitle && r.firm ? `${esc(r.jobTitle)} <span style="opacity:0.5">·</span> ${esc(r.firm)}` : esc(r.jobTitle || r.firm || '—')}</td>
      <td>${scoreHtml(r.score)}</td>
      <td><span class="status-badge">${esc(r.stage || '—')}</span></td>
      <td>${sentimentBadge(r.lastIntentLabel, r.lastIntent, r.conversationState)}</td>
      <td class="text-dim">${renderScheduledFollowUp(r.scheduledFollowUpAt)}</td>
      <td class="text-dim">${formatDate(r.lastReplyAt || r.lastContacted)}</td>
      ${!selectedDealReadOnly ? `<td><button class="row-action-btn" style="color:#e05c5c" onclick="event.stopPropagation();deleteDealTabContact('${r.id}', '${dealId}', this)">✕</button></td>` : ''}
    </tr>
    <tr class="hidden" id="deal-pipeline-detail-${r.id}">
      <td colspan="${!selectedDealReadOnly ? 8 : 7}" style="padding:0;background:var(--bg-raised)">
        <div id="deal-pipeline-conv-${r.id}" style="padding:16px 20px;font-size:12px;color:var(--text-mid)">
          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:14px">
            ${r.email ? `<div><span style="color:var(--text-dim)">Email</span><br><a href="mailto:${esc(r.email)}" style="color:var(--accent)">${esc(r.email)}</a></div>` : ''}
            ${r.phone ? `<div><span style="color:var(--text-dim)">Phone</span><br>${esc(r.phone)}</div>` : ''}
            ${r.linkedinUrl ? `<div><span style="color:var(--text-dim)">LinkedIn</span><br><a href="${esc(r.linkedinUrl)}" target="_blank" style="color:var(--accent)">View profile</a></div>` : ''}
          </div>
          <div style="color:var(--text-dim);font-size:11px">Loading conversation…</div>
        </div>
      </td>
    </tr>`).join('');
}

// Cached pipeline rows per deal for live search filtering
const _pipelineRowsCache = {};

async function loadDealTabPipeline(id) {
  const el = document.getElementById('deal-tab-pipeline');
  if (!el) return;
  el.innerHTML = '<div class="loading-placeholder">Loading pipeline…</div>';
  try {
    const rows = await api(`/api/pipeline?dealId=${id}`);
    if (!rows.length) { el.innerHTML = '<div class="loading-placeholder">No active contacts in pipeline.</div>'; return; }
    const stageOrder = { 'Approved for Outreach': 1, Ranked: 2, Enriched: 3, invite_sent: 4, invite_accepted: 5, dm_sent: 6, email_sent: 7, Replied: 8, 'In Conversation': 9, 'Meeting Booked': 10 };
    const sorted = [...rows].sort((a, b) => (stageOrder[b.stage] || 0) - (stageOrder[a.stage] || 0) || (b.score || 0) - (a.score || 0));
    _pipelineRowsCache[id] = sorted;
    el.innerHTML = `
      <div data-pipeline-deal-id="${id}">
      <div class="pipeline-search-bar">
        <span style="color:var(--text-dim);font-size:13px">⌕</span>
        <input type="text" placeholder="Search firms or contacts…"
               oninput="window.filterPipeline('${id}',this.value,this.nextElementSibling)"
               autocomplete="off" spellcheck="false">
        <button class="search-clear" onclick="window.clearPipelineSearch('${id}',this)">×</button>
      </div>
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <div data-pipeline-count style="font-size:13px;color:var(--text-dim)">${rows.length} contact${rows.length !== 1 ? 's' : ''}</div>
        <div style="display:flex;gap:8px;align-items:center">
          <button class="btn btn-ghost btn-sm" onclick="exportDealPipelineCSV('${id}')" style="font-size:12px">&#8595; Export CSV</button>
          ${!selectedDealReadOnly ? `<button class="btn btn-sm" onclick="clearPipeline('${id}', this)" style="font-size:12px;background:rgba(220,50,50,0.15);color:#e05;border:1px solid rgba(220,50,50,0.3)">Clear Pipeline</button>` : ''}
        </div>
      </div>
      <div class="table-wrap">
      <table class="data-table">
        <thead><tr><th>Investor</th><th>Title / Firm</th><th>Score</th><th>Stage</th><th>Sentiment</th><th>Scheduled Follow-up</th><th>Last Activity</th>${!selectedDealReadOnly ? '<th></th>' : ''}</tr></thead>
        <tbody id="deal-pipeline-tbody-${id}">
          ${renderDealPipelineRows(sorted, id)}
        </tbody>
      </table>
    </div>
    <div id="deal-pipeline-no-results-${id}" style="display:none;padding:24px;text-align:center;color:var(--text-dim);font-size:13px;font-style:italic">No firms or contacts match your search</div>
    </div>`;
  } catch { el.innerHTML = '<div class="loading-placeholder text-red">Failed to load.</div>'; }
}

window.filterPipeline = function(dealId, val, clearBtn) {
  const tbody = document.getElementById(`deal-pipeline-tbody-${dealId}`);
  const noResults = document.getElementById(`deal-pipeline-no-results-${dealId}`);
  const countEl = document.querySelector(`[data-pipeline-deal-id="${dealId}"] [data-pipeline-count]`);
  if (!tbody) return;
  if (clearBtn) clearBtn.style.display = val ? 'block' : 'none';
  const term = (val || '').toLowerCase().trim();
  const rows = _pipelineRowsCache[dealId] || [];
  if (!term) {
    tbody.innerHTML = renderDealPipelineRows(rows, dealId);
    if (noResults) noResults.style.display = 'none';
    if (countEl) countEl.textContent = `${rows.length} contact${rows.length !== 1 ? 's' : ''}`;
    return;
  }
  const filtered = rows.filter(r =>
    (r.name || '').toLowerCase().includes(term) ||
    (r.firm || '').toLowerCase().includes(term)
  );
  tbody.innerHTML = renderDealPipelineRowsHighlighted(filtered, dealId, term);
  if (noResults) noResults.style.display = filtered.length ? 'none' : 'block';
  if (countEl) countEl.textContent = `${filtered.length} of ${rows.length} contact${rows.length !== 1 ? 's' : ''}`;
};

window.clearPipelineSearch = function(dealId, clearBtn) {
  const el = document.getElementById('deal-tab-pipeline');
  const input = el?.querySelector('.pipeline-search-bar input');
  if (input) input.value = '';
  if (clearBtn) clearBtn.style.display = 'none';
  window.filterPipeline(dealId, '', clearBtn);
};

function highlightMatch(text, term) {
  if (!term || !text) return esc(text || '');
  const lower = text.toLowerCase();
  const idx = lower.indexOf(term.toLowerCase());
  if (idx === -1) return esc(text);
  return esc(text.slice(0, idx)) + `<mark class="gold-match">${esc(text.slice(idx, idx + term.length))}</mark>` + esc(text.slice(idx + term.length));
}

function renderDealPipelineRowsHighlighted(rows, dealId, term) {
  const renderScheduledFollowUp = (value) => {
    if (!value) return '<span class="text-dim">—</span>';
    return `<span class="text-dim">${formatScheduleDate(value)}</span>`;
  };
  return rows.map(r => `
    <tr data-pipeline-contact-id="${r.id}" style="cursor:pointer" onclick="toggleDealPipelineDetail('${r.id}', '${dealId}')">
      <td>
        <div style="font-weight:500">${highlightMatch(r.name || '—', term)}</div>
        ${r.email ? `<div style="font-size:11px;color:var(--text-dim)">${esc(r.email)}</div>` : ''}
      </td>
      <td class="text-dim" style="font-size:12px">${r.jobTitle && r.firm
        ? `${esc(r.jobTitle)} <span style="opacity:0.5">·</span> ${highlightMatch(r.firm, term)}`
        : highlightMatch(r.jobTitle || r.firm || '—', term)}</td>
      <td>${scoreHtml(r.score)}</td>
      <td><span class="status-badge">${esc(r.stage || '—')}</span></td>
      <td>${sentimentBadge(r.lastIntentLabel, r.lastIntent, r.conversationState)}</td>
      <td class="text-dim">${renderScheduledFollowUp(r.scheduledFollowUpAt)}</td>
      <td class="text-dim">${formatDate(r.lastReplyAt || r.lastContacted)}</td>
      ${!selectedDealReadOnly ? `<td><button class="row-action-btn" style="color:#e05c5c" onclick="event.stopPropagation();deleteDealTabContact('${r.id}', '${dealId}', this)">✕</button></td>` : ''}
    </tr>
    <tr class="hidden" id="deal-pipeline-detail-${r.id}">
      <td colspan="${!selectedDealReadOnly ? 8 : 7}" style="padding:0;background:var(--bg-raised)">
        <div id="deal-pipeline-conv-${r.id}" style="padding:16px 20px;font-size:12px;color:var(--text-mid)">
          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:14px">
            ${r.email ? `<div><span style="color:var(--text-dim)">Email</span><br><a href="mailto:${esc(r.email)}" style="color:var(--accent)">${esc(r.email)}</a></div>` : ''}
            ${r.phone ? `<div><span style="color:var(--text-dim)">Phone</span><br>${esc(r.phone)}</div>` : ''}
            ${r.linkedinUrl ? `<div><span style="color:var(--text-dim)">LinkedIn</span><br><a href="${esc(r.linkedinUrl)}" target="_blank" style="color:var(--accent)">View profile</a></div>` : ''}
          </div>
          <div style="color:var(--text-dim);font-size:11px">Loading conversation…</div>
        </div>
      </td>
    </tr>`).join('');
}

async function patchDealPipelineInPlace(id) {
  const container = document.querySelector(`#deal-tab-pipeline [data-pipeline-deal-id="${id}"]`);
  if (!container) return;
  const tbody = document.getElementById(`deal-pipeline-tbody-${id}`);
  if (!tbody) return;
  try {
    const rows = await api(`/api/pipeline?dealId=${id}`);
    const stageOrder = { 'Approved for Outreach': 1, Ranked: 2, Enriched: 3, invite_sent: 4, invite_accepted: 5, dm_sent: 6, email_sent: 7, Replied: 8, 'In Conversation': 9, 'Meeting Booked': 10 };
    const sorted = [...rows].sort((a, b) => (stageOrder[b.stage] || 0) - (stageOrder[a.stage] || 0) || (b.score || 0) - (a.score || 0));
    const countEl = container.querySelector('[data-pipeline-count]');
    if (countEl) countEl.textContent = `${rows.length} contact${rows.length !== 1 ? 's' : ''}`;
    tbody.innerHTML = renderDealPipelineRows(sorted, id);
  } catch {
    // Leave the current table intact on refresh failure.
  }
}

async function toggleDealPipelineDetail(contactId, dealId) {
  openContactSidePanel(contactId, dealId);
}

// ── CONTACT SIDE PANEL ────────────────────────────────────────────────────────

let _sidePanelContactId = null;

function buildFallbackConversationMessage(contact) {
  if (!contact?.last_intent && !contact?.conversation_state) return null;
  const fallbackBody = contact.last_intent === 'not_interested'
    ? 'Not interested'
    : contact.last_intent
      ? String(contact.last_intent).replace(/_/g, ' ')
      : String(contact.conversation_state || '').replace(/_/g, ' ');
  if (!fallbackBody) return null;
  return {
    direction: 'inbound',
    channel: 'email',
    body: fallbackBody.charAt(0).toUpperCase() + fallbackBody.slice(1),
    timestamp: contact.last_reply_at || contact.updated_at || null,
    isFallback: true,
  };
}

async function openContactSidePanel(contactId, dealId = null) {
  const panel   = document.getElementById('contact-side-panel');
  const overlay = document.getElementById('contact-panel-overlay');
  const nameEl  = document.getElementById('contact-panel-name');
  const bodyEl  = document.getElementById('contact-panel-body');
  if (!panel) return;

  _sidePanelContactId = contactId;
  investorProfileContactId = contactId;
  investorProfileDealId = dealId || null;
  panel.style.right = '0';
  if (overlay) overlay.style.display = 'block';
  if (nameEl) nameEl.textContent = 'Loading…';
  if (bodyEl) bodyEl.innerHTML = '<div style="padding:24px;color:var(--text-dim)">Loading contact…</div>';

  try {
    const qs = dealId ? `?dealId=${encodeURIComponent(dealId)}` : '';
    const data = await api(`/api/contacts/${contactId}/investor-card${qs}`);
    if (_sidePanelContactId !== contactId) return; // navigated away
    if (nameEl) nameEl.textContent = data.contact?.name || 'Investor';
    if (bodyEl) renderSidePanelBody(bodyEl, data);
  } catch {
    if (bodyEl) bodyEl.innerHTML = '<div style="padding:24px;color:#e05c5c">Failed to load contact.</div>';
  }
}

async function openInvestorDatabaseSidePanel(investorsDbId) {
  const panel   = document.getElementById('contact-side-panel');
  const overlay = document.getElementById('contact-panel-overlay');
  const nameEl  = document.getElementById('contact-panel-name');
  const bodyEl  = document.getElementById('contact-panel-body');
  if (!panel) return;

  const panelKey = `investor-db:${investorsDbId}`;
  _sidePanelContactId = panelKey;
  investorProfileContactId = null;
  investorProfileDealId = null;
  panel.style.right = '0';
  if (overlay) overlay.style.display = 'block';
  if (nameEl) nameEl.textContent = 'Loading…';
  if (bodyEl) bodyEl.innerHTML = '<div style="padding:24px;color:var(--text-dim)">Loading investor…</div>';

  try {
    const data = await api(`/api/investors-db/${investorsDbId}/profile`);
    if (_sidePanelContactId !== panelKey) return;
    if (nameEl) nameEl.textContent = data.contact?.name || 'Investor';
    if (bodyEl) renderSidePanelBody(bodyEl, data);
  } catch {
    if (bodyEl) bodyEl.innerHTML = '<div style="padding:24px;color:#e05c5c">Failed to load investor.</div>';
  }
}

async function openInvestorProfilePage(contactId, dealId = null) {
  investorProfileContactId = contactId;
  investorProfileDealId = dealId || null;
  window.location.hash = '#investor-profile';
}

async function loadInvestorProfilePage() {
  const container = document.getElementById('investor-profile-container');
  if (!container) return;
  if (!investorProfileContactId) {
    container.innerHTML = '<div class="card" style="padding:32px;color:var(--text-dim)">No investor selected yet.</div>';
    return;
  }
  container.innerHTML = '<div class="card" style="padding:32px;color:var(--text-dim)">Loading investor profile...</div>';
  try {
    const qs = investorProfileDealId ? `?dealId=${encodeURIComponent(investorProfileDealId)}` : '';
    const data = await api(`/api/contacts/${investorProfileContactId}/investor-card${qs}`);
    const contact = data.contact || {};
    const history = Array.isArray(data.history) ? data.history : [];
    container.innerHTML = `
      <div class="section-header">
        <h1 class="section-title">${esc(contact.name || 'Investor Profile')}</h1>
        <button class="btn btn-ghost btn-sm" onclick="openContactSidePanel('${esc(contact.id)}','${esc(investorProfileDealId || contact.deal_id || '')}')">Open Side Panel</button>
      </div>
      <div class="card" style="padding:24px">
        <div style="display:grid;grid-template-columns:minmax(0,1.3fr) minmax(0,1fr);gap:24px">
          <div>
            <div style="font-size:14px;color:#9ca3af;margin-bottom:8px">${esc(contact.job_title || '—')} · ${esc(contact.company_name || '—')}</div>
            ${contact.linkedin_url ? `<div style="margin-bottom:8px"><a href="${esc(contact.linkedin_url)}" target="_blank" rel="noopener" style="color:#60a5fa">LinkedIn profile</a></div>` : ''}
            ${contact.email ? `<div style="margin-bottom:8px;color:#4ade80">${esc(contact.email)}</div>` : ''}
            ${contact.phone ? `<div style="margin-bottom:8px;color:#cbd5e1">${esc(contact.phone)}</div>` : ''}
            ${contact.investment_thesis ? `<div style="margin-top:20px"><div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.12em;margin-bottom:6px">Investment Thesis</div><div style="font-size:13px;color:#cbd5e1;line-height:1.7">${esc(contact.investment_thesis)}</div></div>` : ''}
          </div>
          <div>
            ${contact.aum_display ? `<div style="margin-bottom:12px"><div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.12em;margin-bottom:6px">AUM</div><div style="font-size:13px;color:#e5e7eb">${esc(contact.aum_display)}</div></div>` : ''}
            ${contact.cheque_size_range ? `<div style="margin-bottom:12px"><div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.12em;margin-bottom:6px">Cheque Size</div><div style="font-size:13px;color:#e5e7eb">${esc(contact.cheque_size_range)}</div></div>` : ''}
            ${contact.sectors_of_interest?.length ? `<div style="margin-bottom:12px"><div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.12em;margin-bottom:6px">Sectors</div><div style="display:flex;flex-wrap:wrap;gap:6px">${contact.sectors_of_interest.map(item => `<span style="padding:3px 8px;border:1px solid var(--border);border-radius:999px;font-size:11px">${esc(item)}</span>`).join('')}</div></div>` : ''}
            ${contact.past_investments_list?.length ? `<div><div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.12em;margin-bottom:6px">Past Investments</div><div style="display:flex;flex-wrap:wrap;gap:6px">${contact.past_investments_list.map(item => `<span style="padding:3px 8px;border-radius:999px;background:rgba(96,165,250,0.12);color:#93c5fd;font-size:11px">${esc(item)}</span>`).join('')}</div></div>` : ''}
          </div>
        </div>
      </div>
      <div class="card mt-24" style="padding:24px">
        <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.12em;margin-bottom:12px">Conversation History</div>
        ${history.length ? history.map(item => `<div style="padding:12px 0;border-bottom:1px solid var(--border)"><div style="display:flex;justify-content:space-between;gap:12px;margin-bottom:4px"><span style="color:#e5e7eb">${esc(item.type || 'Interaction')}</span><span style="color:#6b7280;font-size:12px">${item.date ? new Date(item.date).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) : '—'}</span></div><div style="color:#9ca3af;font-size:13px;line-height:1.6">${esc(item.summary || 'No summary available.')}</div></div>`).join('') : '<div style="color:#6b7280">No interactions recorded yet.</div>'}
      </div>
    `;
  } catch (err) {
    container.innerHTML = `<div class="card" style="padding:32px;color:#ef4444">Failed to load investor profile: ${esc(err.message)}</div>`;
  }
}

function closeContactPanel() {
  const panel   = document.getElementById('contact-side-panel');
  const overlay = document.getElementById('contact-panel-overlay');
  if (panel)   panel.style.right = '-440px';
  if (overlay) overlay.style.display = 'none';
  _sidePanelContactId = null;
}

function renderSidePanelBody(el, data) {
  const contact = data.contact;
  const history = Array.isArray(data.history) ? data.history : [];
  if (!contact) {
    el.innerHTML = '<div style="padding:24px;color:var(--text-dim)">No contact data.</div>';
    return;
  }

  const sentiment = Number(contact.transcript_sentiment || 0);
  const sentimentBadgeHtml = sentiment
    ? `<span style="padding:4px 10px;border-radius:999px;font-size:10px;font-family:'DM Mono',monospace;background:${sentiment >= 8 ? 'rgba(74,222,128,0.14)' : sentiment >= 5 ? 'rgba(251,191,36,0.14)' : 'rgba(248,113,113,0.14)'};color:${sentiment >= 8 ? '#4ade80' : sentiment >= 5 ? '#fbbf24' : '#f87171'}">Sentiment ${sentiment}/10</span>`
    : '';

  el.innerHTML = `
    <div style="padding:20px;border-bottom:1px solid #1a1a1a">
      <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:12px">
        <div>
          <div style="font-size:18px;color:#e5e7eb;font-weight:600">${esc(contact.name || '—')}</div>
          <div style="font-size:12px;color:#9ca3af">${esc(contact.job_title || '—')}</div>
          <div style="font-size:12px;color:#6b7280">${esc(contact.company_name || '—')}</div>
        </div>
        <div style="display:flex;flex-direction:column;align-items:flex-end;gap:6px">
          ${contact.pipeline_stage ? `<span style="padding:4px 10px;border-radius:999px;font-size:10px;font-family:'DM Mono',monospace;background:rgba(212,168,71,0.12);color:#d4a847">${esc(contact.pipeline_stage)}</span>` : ''}
          ${sentimentBadgeHtml}
        </div>
      </div>
      <div style="display:flex;flex-direction:column;gap:8px;font-size:12px;color:#cbd5e1">
        ${contact.linkedin_url ? `<a href="${esc(contact.linkedin_url)}" target="_blank" rel="noopener" style="color:#60a5fa">LinkedIn profile</a>` : ''}
        ${contact.email ? `<a href="mailto:${esc(contact.email)}" style="color:#4ade80">${esc(contact.email)}</a>` : ''}
        ${contact.phone ? `<div>${esc(contact.phone)}</div>` : ''}
      </div>
      <div style="margin-top:16px">
        ${contact.linked_contact_id || (!contact.is_database_record && contact.id)
          ? `<a href="#investor-profile" onclick="event.preventDefault(); openInvestorProfilePage('${esc(contact.linked_contact_id || contact.id)}','${esc(investorProfileDealId || contact.deal_id || '')}')" style="color:#60a5fa;font-size:12px">View full profile</a>`
          : `<span style="color:#6b7280;font-size:12px">Database-only profile</span>`}
      </div>
    </div>
    <div style="padding:20px;border-bottom:1px solid #1a1a1a">
      <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.12em;font-family:'DM Mono',monospace;margin-bottom:12px">Profile</div>
      <div style="display:grid;grid-template-columns:1fr;gap:12px">
        ${contact.aum_display ? `<div><div style="font-size:10px;color:#6b7280;margin-bottom:3px">AUM</div><div style="font-size:13px;color:#e5e7eb">${esc(contact.aum_display)}</div></div>` : ''}
        ${contact.investment_thesis ? `<div><div style="font-size:10px;color:#6b7280;margin-bottom:3px">Investment Thesis</div><div style="font-size:13px;color:#cbd5e1;line-height:1.6">${esc(contact.investment_thesis)}</div></div>` : ''}
        ${contact.sectors_of_interest?.length ? `<div><div style="font-size:10px;color:#6b7280;margin-bottom:6px">Sectors of Interest</div><div style="display:flex;flex-wrap:wrap;gap:6px">${contact.sectors_of_interest.map(item => `<span style="padding:3px 8px;border-radius:999px;background:#17171b;color:#cbd5e1;font-size:10px;border:1px solid #24242a">${esc(item)}</span>`).join('')}</div></div>` : ''}
        ${contact.cheque_size_range ? `<div><div style="font-size:10px;color:#6b7280;margin-bottom:3px">Cheque Size</div><div style="font-size:13px;color:#e5e7eb">${esc(contact.cheque_size_range)}</div></div>` : ''}
        ${contact.past_investments_list?.length ? `<div><div style="font-size:10px;color:#6b7280;margin-bottom:6px">Past Investments</div><div style="display:flex;flex-wrap:wrap;gap:6px">${contact.past_investments_list.map(item => `<span style="padding:3px 8px;border-radius:999px;background:rgba(96,165,250,0.12);color:#93c5fd;font-size:10px;border:1px solid rgba(96,165,250,0.2)">${esc(item)}</span>`).join('')}</div></div>` : ''}
      </div>
    </div>
    <div style="padding:20px">
      <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.12em;font-family:'DM Mono',monospace;margin-bottom:12px">Conversation History</div>
      ${history.length ? `<div style="display:flex;flex-direction:column;gap:10px">${history.map(item => `<div style="padding:12px 14px;border:1px solid #1f1f24;border-radius:10px;background:#111217"><div style="display:flex;justify-content:space-between;gap:10px;margin-bottom:6px"><span style="font-size:11px;color:#e5e7eb">${esc(item.type || 'Interaction')}</span><span style="font-size:10px;color:#6b7280">${item.date ? new Date(item.date).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) : '—'}</span></div><div style="font-size:12px;color:#9ca3af;line-height:1.55">${esc(item.summary || 'No summary available.')}</div></div>`).join('')}</div>` : `<div style="color:#6b7280;font-size:12px">No interactions recorded yet.</div>`}
    </div>
  `;
}

// Also fetch firm research (AUM, EBITDA, thesis) and inject into the panel
async function loadFirmResearchIntoPanel(contact) {
  if (!contact?.company_name || !contact?.deal_id) return null;
  try {
    const { data: firm } = await fetch(`/api/firms/research?firm=${encodeURIComponent(contact.company_name)}&dealId=${contact.deal_id}`, { credentials: 'include' }).then(r => r.json()).catch(() => ({}));
    return firm;
  } catch { return null; }
}

function renderContactConvPanel(el, data) {
  const { contact, messages, intentHistory } = data;
  const contactInfo = `
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px;margin-bottom:16px">
      ${contact?.email ? `<div><div style="color:var(--text-dim);font-size:10px;margin-bottom:2px">EMAIL</div><a href="mailto:${esc(contact.email)}" style="color:var(--accent)">${esc(contact.email)}</a></div>` : ''}
      ${contact?.phone ? `<div><div style="color:var(--text-dim);font-size:10px;margin-bottom:2px">PHONE</div>${esc(contact.phone)}</div>` : ''}
      ${contact?.linkedin_url ? `<div><div style="color:var(--text-dim);font-size:10px;margin-bottom:2px">LINKEDIN</div><a href="${esc(contact.linkedin_url)}" target="_blank" style="color:var(--accent)">View profile</a></div>` : ''}
      ${contact?.job_title ? `<div><div style="color:var(--text-dim);font-size:10px;margin-bottom:2px">TITLE</div>${esc(contact.job_title)}</div>` : ''}
      ${contact?.company_name ? `<div><div style="color:var(--text-dim);font-size:10px;margin-bottom:2px">FIRM</div>${esc(contact.company_name)}</div>` : ''}
    </div>`;

  const msgHtml = messages.length ? `
    <div style="font-size:10px;color:var(--text-dim);margin-bottom:8px;letter-spacing:0.08em">CONVERSATION HISTORY</div>
    <div style="display:flex;flex-direction:column;gap:8px;max-height:320px;overflow-y:auto">
      ${messages.map(m => {
        const isOut = m.direction === 'outbound';
        const ts = m.timestamp ? new Date(m.timestamp).toLocaleDateString('en-GB', { day:'numeric', month:'short', hour:'2-digit', minute:'2-digit' }) : '';
        const intentTag = m.intent ? ` <span style="background:rgba(255,255,255,0.06);padding:1px 5px;border-radius:2px;font-size:9px">${esc(m.intent.replace(/_/g,' '))}</span>` : '';
        return `<div style="display:flex;flex-direction:column;align-items:${isOut ? 'flex-end' : 'flex-start'}">
          <div style="max-width:85%;background:${isOut ? 'rgba(212,168,71,0.1)' : 'rgba(255,255,255,0.05)'};padding:8px 12px;border-radius:6px;line-height:1.5">
            ${esc(m.body || '')}
          </div>
          <div style="font-size:10px;color:var(--text-dim);margin-top:2px">${isOut ? 'Roco' : 'Investor'} · ${esc(m.channel || '')} · ${ts}${intentTag}</div>
        </div>`;
      }).join('')}
    </div>` : `<div style="color:var(--text-dim);font-size:12px">No messages yet.</div>`;

  const intentHtml = intentHistory.length ? `
    <div style="font-size:10px;color:var(--text-dim);margin:14px 0 8px;letter-spacing:0.08em">INTENT TIMELINE</div>
    <div style="display:flex;flex-wrap:wrap;gap:6px">
      ${intentHistory.slice(-10).map(h => `
        <div style="background:rgba(255,255,255,0.04);padding:4px 8px;border-radius:4px;font-size:10px">
          <span style="color:var(--text-dim)">${new Date(h.timestamp).toLocaleDateString('en-GB', {day:'numeric',month:'short'})}</span>
          <span style="margin-left:6px">${esc((h.intent || '').replace(/_/g,' '))}</span>
        </div>`).join('')}
    </div>` : '';

  el.innerHTML = contactInfo + msgHtml + intentHtml;
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
      <div data-archived-deal-id="${id}">
      <div style="margin-bottom:12px;display:flex;align-items:center;justify-content:space-between;gap:12px">
        <span data-archived-count style="color:var(--text-dim);font-size:13px">${rows.length} archived investor${rows.length !== 1 ? 's' : ''} — below minimum score threshold or outside deal criteria.</span>
        <div style="display:flex;gap:8px">
          <button class="btn btn-ghost btn-sm" style="font-size:12px;white-space:nowrap" onclick="exportDealArchivedCSV('${id}', this)">&#8595; Export CSV</button>
          ${!selectedDealReadOnly && reactivatable.length ? `<button class="btn btn-sm" style="white-space:nowrap" onclick="reactivateAllBorderline('${id}', this)">Re-activate borderline (${reactivatable.length})</button>` : ''}
        </div>
      </div>
      <div class="table-wrap">
        <table class="data-table">
          <thead><tr><th>Investor</th><th>Title / Firm</th><th>Score</th><th>Reason</th><th></th></tr></thead>
          <tbody id="deal-archived-tbody-${id}">
            ${renderArchivedRows(rows)}
          </tbody>
        </table>
      </div>
      </div>`;
  } catch { el.innerHTML = '<div class="loading-placeholder text-red">Failed to load.</div>'; }
}

function renderArchivedRows(rows) {
  return rows.map(r => `<tr id="archived-row-${r.id}" data-archived-contact-id="${r.id}">
    <td>${esc(r.name || '—')}</td>
    <td class="text-dim" style="font-size:12px">${r.jobTitle && r.firm ? `${esc(r.jobTitle)} <span style="opacity:0.5">·</span> ${esc(r.firm)}` : esc(r.jobTitle || r.firm || '—')}</td>
    <td>${scoreHtml(r.score)}</td>
    <td class="text-dim" style="font-size:12px;max-width:300px">${esc(r.archiveReason || '—')}</td>
    <td>${!selectedDealReadOnly && (r.score || 0) >= 40 ? `<button class="btn btn-ghost" style="font-size:11px;padding:3px 8px;white-space:nowrap" onclick="reactivateContact('${r.id}', this)">Re-activate</button>` : ''}</td>
  </tr>`).join('');
}

async function patchDealArchivedInPlace(id) {
  const container = document.querySelector(`#deal-tab-archived [data-archived-deal-id="${id}"]`);
  if (!container) return;
  const tbody = document.getElementById(`deal-archived-tbody-${id}`);
  if (!tbody) return;
  try {
    const rows = await api(`/api/deals/${id}/archived`);
    const countEl = container.querySelector('[data-archived-count]');
    if (countEl) {
      countEl.textContent = `${rows.length} archived investor${rows.length !== 1 ? 's' : ''} — below minimum score threshold or outside deal criteria.`;
    }
    tbody.innerHTML = renderArchivedRows(rows);
  } catch {
    // Keep current archived view intact if refresh fails.
  }
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
    const res = await fetch(`/api/deals/${dealId}/exclusions/upload`, {
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

    el.innerHTML = `
      <div style="padding:20px">

        <!-- Sequence section -->
        <div style="margin-bottom:28px">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
            <div style="color:#e5e7eb;font-size:14px;font-weight:600">Outreach Sequence</div>
            <button onclick="window.editDealSequence('${dealId}')"
              style="padding:6px 14px;background:#1a1a1a;border:1px solid #2a2a2a;
                     color:#9ca3af;border-radius:6px;cursor:pointer;font-size:12px">
              Edit Sequence
            </button>
          </div>

          <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
            ${steps.map((s, i) => {
              const def = (typeof SEQ_STEP_DEFS !== 'undefined' && (SEQ_STEP_DEFS[s.label] || SEQ_STEP_DEFS[s.action_type])) || null;
              const bg = def?.color || { email: '#1f3a5f', linkedin_invite: '#1a3a2a', linkedin_dm: '#2a1f3a' }[s.type] || '#1a1a1a';
              const badge = def?.badge || '#6b7280';
              const display = def?.display || (s.label || '').replace(/_/g,' ');
              const delayLabel = Number(s.delay_days) > 0 ? `+${s.delay_days}d` : 'Day 0';
              return `
                ${i > 0 ? '<div style="color:#2a2a2a;font-size:18px;align-self:center">&#8594;</div>' : ''}
                <div style="padding:8px 14px;background:${bg};border-radius:6px;text-align:center;min-width:100px">
                  <div style="color:${badge};font-size:9px;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:2px">${delayLabel}</div>
                  <div style="color:#e5e7eb;font-size:12px;font-weight:600">${esc(display)}</div>
                  <div style="color:#3a3a3a;font-size:9px;font-family:'DM Mono',monospace;margin-top:2px">${s.type !== 'linkedin_invite' ? 'template' : 'auto'}</div>
                </div>`;
            }).join('')}
            ${steps.length === 0 ? '<span style="color:#4b5563;font-size:13px">No sequence yet — click Edit Sequence</span>' : ''}
          </div>
        </div>

        <!-- Templates section -->
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
          <div style="color:#e5e7eb;font-size:14px;font-weight:600">Message Templates</div>
          <div style="display:flex;gap:8px">
            <button id="regen-templates-btn-${dealId}" onclick="window.regenerateDealTemplates('${dealId}')"
              style="padding:6px 14px;background:#1a1a1a;border:1px solid #2a2a2a;
                     color:#60a5fa;border-radius:6px;cursor:pointer;font-size:12px">
              Regenerate AI
            </button>
            <button onclick="window.showAddDealTemplateModal('${dealId}')"
              style="padding:6px 14px;background:#1a1a1a;border:1px solid #2a2a2a;
                     color:#9ca3af;border-radius:6px;cursor:pointer;font-size:12px">
              + Add Template
            </button>
          </div>
        </div>

        <div style="display:flex;flex-direction:column;gap:12px">
          ${steps.filter(s => s.type !== 'linkedin_invite').map(step => {
            const stepNorm = normaliseSeqStep(step);
            const stepLabel = stepNorm.label || step.label || step.action_type || step.type || '';
            const def = SEQ_STEP_DEFS[stepLabel] || null;
            const tmpl = templates.find(t => t.sequence_step === stepLabel && t.is_primary);

            if (!tmpl) {
              return `
                <div style="padding:16px;border:1px dashed #2a2a2a;border-radius:8px;
                            display:flex;justify-content:space-between;align-items:center">
                  <div>
                    <span style="font-family:'DM Mono',monospace;font-size:12px;color:#C9A84C">${esc(def?.display || stepLabel)}</span>
                    <span style="color:#374151;font-size:12px;margin-left:8px">No template assigned</span>
                  </div>
                  <button onclick="window.showAddDealTemplateModal('${dealId}','${esc(stepLabel)}','${esc(stepNorm.type || 'email')}')"
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
                    <span style="font-family:'DM Mono',monospace;font-size:12px;color:#C9A84C">${esc(def?.display || stepLabel)}</span>
                    <span style="padding:2px 6px;border-radius:3px;font-size:10px;font-family:'DM Mono',monospace;color:#6b7280">${esc(stepLabel)}</span>
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
  senderEmail:      'Dominick.Pandolfo@novastone-ca.com',
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
  const previewSubject = tmpl.preview_subject ? renderTemplateBody(tmpl.preview_subject, vals) : null;
  const subjectA = tmpl.subject_a ? renderTemplateBody(tmpl.subject_a, vals) : null;
  const subjectB = tmpl.subject_b ? renderTemplateBody(tmpl.subject_b, vals) : null;
  const bodyHtml = renderTemplateBody(tmpl.body || '', vals).replace(/\n/g, '<br>');
  const toEmail = vals.email || vals.contactEmail || 'james@meridiancapital.com';
  const fromEmail = vals.senderEmail || 'Dominick.Pandolfo@novastone-ca.com';

  const modal = document.createElement('div');
  modal.id = 'tmpl-preview-modal';
  modal.style.cssText = 'position:fixed;inset:0;z-index:9999;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,.7)';
  modal.onclick = e => { if (e.target === modal) modal.remove(); };

  const isEditing = !!tmpl.id; // has an id = saved template, can switch to edit
  const dealId = tmpl.deal_id || window.__previewDealId;

  modal.innerHTML = isLinkedIn
    ? `<div style="background:#f3f2ef;border:1px solid #d9d8d6;border-radius:16px;width:440px;max-height:88vh;overflow:hidden;box-shadow:0 24px 70px rgba(0,0,0,.35)">
        <div style="background:#fff;padding:14px 18px;border-bottom:1px solid #e5e7eb;display:flex;justify-content:space-between;align-items:center">
          <div style="display:flex;align-items:center;gap:12px;min-width:0">
            <div style="width:42px;height:42px;border-radius:50%;background:linear-gradient(135deg,#0a66c2,#378fe9);display:flex;align-items:center;justify-content:center;color:#fff;font-size:15px;font-weight:700;flex-shrink:0">${esc((vals.fullName || 'J').split(' ').map(part => part[0]).slice(0,2).join('').toUpperCase())}</div>
            <div style="min-width:0">
              <div style="font-size:14px;font-weight:600;color:#191919;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(vals.fullName || 'James Mitchell')}</div>
              <div style="font-size:11px;color:#666;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(vals.jobTitle || vals.title || 'Managing Partner')}${vals.firm ? ` · ${esc(vals.firm)}` : ''}</div>
            </div>
          </div>
          <div style="display:flex;gap:8px">
            ${isEditing ? `<button onclick="document.getElementById('tmpl-preview-modal').remove();window.editDealTemplateModal('${dealId}','${tmpl.id}',${JSON.stringify(tmpl).replace(/"/g,'&quot;')})" style="font-size:11px;color:#344054;background:#f8fafc;border:1px solid #d0d5dd;border-radius:999px;padding:4px 10px;cursor:pointer">Edit</button>` : ''}
            <button onclick="document.getElementById('tmpl-preview-modal').remove()" style="font-size:16px;color:#667085;background:none;border:none;cursor:pointer">✕</button>
          </div>
        </div>
        <div style="padding:18px;background:linear-gradient(180deg,#f8fafc 0%,#f3f2ef 100%);min-height:420px;display:flex;flex-direction:column;gap:12px">
          <div style="align-self:center;font-size:11px;color:#667085;background:rgba(255,255,255,.72);border:1px solid #e4e7ec;border-radius:999px;padding:4px 10px">Today · LinkedIn messaging</div>
          <div style="display:flex;gap:10px;align-items:flex-end">
            <div style="width:30px;height:30px;border-radius:50%;background:#d0d5dd;display:flex;align-items:center;justify-content:center;color:#475467;font-size:11px;font-weight:700;flex-shrink:0">${esc((vals.fullName || 'J')[0].toUpperCase())}</div>
            <div style="background:#fff;border:1px solid #e4e7ec;border-radius:18px 18px 18px 6px;padding:10px 13px;max-width:260px;font-size:12px;line-height:1.5;color:#344054;box-shadow:0 6px 20px rgba(16,24,40,.04)">
              Thanks for connecting.
            </div>
          </div>
          <div style="display:flex;justify-content:flex-end">
            <div style="background:#0a66c2;border:1px solid #095aa8;border-radius:18px 18px 6px 18px;padding:12px 14px;max-width:300px;font-size:13px;line-height:1.65;color:#fff;box-shadow:0 10px 26px rgba(10,102,194,.22)">
              ${bodyHtml}
            </div>
          </div>
          <div style="display:flex;justify-content:flex-end;padding-right:6px;font-size:10px;color:#667085">You · Draft preview</div>
          <div style="margin-top:auto;background:#fff;border:1px solid #d0d5dd;border-radius:14px;padding:12px 14px;display:flex;align-items:center;gap:10px">
            <div style="color:#98a2b3;font-size:13px;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">Write a message...</div>
            <div style="width:28px;height:28px;border-radius:50%;background:#eef2f6;display:flex;align-items:center;justify-content:center;color:#667085;font-size:13px">+</div>
          </div>
        </div>
        <div style="padding:12px 18px;border-top:1px solid #e4e7ec;background:#fff;font-size:11px;color:#667085">
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
            <span>${esc(vals.senderName || 'Dom')} &lt;${esc(fromEmail)}&gt;</span>
          </div>
          <div style="display:flex;align-items:center;padding:10px 0;border-bottom:1px solid #f0f0f0;font-size:13px;color:#444">
            <span style="color:#888;width:40px;flex-shrink:0">To</span>
            <span>${esc(vals.fullName)} &lt;${esc(toEmail)}&gt;</span>
          </div>
          ${previewSubject ? `
          <div style="padding:8px 0">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
              <span style="font-size:11px;color:#888;width:64px;flex-shrink:0">Subject</span>
              <span style="font-size:14px;font-weight:500;color:#202124">${previewSubject}</span>
            </div>
            ${(subjectA && subjectB) ? `<div style="display:flex;align-items:center;gap:8px">
              <span style="font-size:11px;color:#888;width:64px;flex-shrink:0">A/B</span>
              <span style="font-size:12px;color:#444">${subjectA} &nbsp;|&nbsp; ${subjectB}</span>
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
  const varGroups = [
    { label: 'Contact',           vars: ['firstName','lastName','fullName','firm','jobTitle'] },
    { label: 'Investor Research', vars: ['pastInvestments','investmentThesis','sectorFocus','investorGeography'] },
    { label: 'Deal',              vars: ['dealName','dealBrief','sector','targetAmount','keyMetrics','geography','minCheque','maxCheque','investorProfile','comparableDeal'] },
    { label: 'Links & Sender',    vars: ['deckUrl','callLink','senderName','senderTitle'] },
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
        <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:10px">
          ${varGroups.map(g => `
            <div>
              <div style="font-size:10px;color:#4b5563;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">${g.label}</div>
              <div style="display:flex;flex-wrap:wrap;gap:4px">
                ${g.vars.map(v => `
                  <button onclick="window.insertDtmVar('{{${v}}}')"
                    style="padding:3px 8px;background:#1a1a2a;border:1px solid #2a2a3a;
                           color:#818cf8;border-radius:3px;cursor:pointer;font-size:11px;
                           font-family:'DM Mono',monospace">
                    {{${v}}}
                  </button>`).join('')}
              </div>
            </div>`).join('')}
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

window.regenerateDealTemplates = async function(dealId) {
  const btn = document.getElementById(`regen-templates-btn-${dealId}`);
  if (btn) { btn.textContent = 'Generating…'; btn.disabled = true; }
  try {
    const data = await api(`/api/deals/${dealId}/templates/regenerate`, 'POST');
    showToast(`${data.count} templates regenerated with latest deal data`);
    loadDealTemplatesTab(dealId);
  } catch (err) {
    showToast('Regeneration failed: ' + err.message, 'error');
  } finally {
    if (btn) { btn.textContent = 'Regenerate AI'; btn.disabled = false; }
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// SEQUENCE EDITOR
// Steps use {type, label, delay_days} — orchestrator filters by type per channel
// ─────────────────────────────────────────────────────────────────────────────

const SEQ_STEP_DEFS = {
  linkedin_invite:  { type: 'linkedin_invite', label: 'linkedin_invite',  display: 'LI Invite',          note: 'Sends connection request — no template needed',  color: '#1a3a2a', badge: '#4ade80' },
  linkedin_dm_1:    { type: 'linkedin_dm',     label: 'linkedin_dm_1',    display: 'LI DM Intro',        note: 'First DM sent on acceptance',                    color: '#2a1f3a', badge: '#a78bfa' },
  linkedin_dm_2:    { type: 'linkedin_dm',     label: 'linkedin_dm_2',    display: 'LI DM Follow-up',    note: 'Follow-up DM if no reply',                       color: '#2a1f3a', badge: '#a78bfa' },
  email_intro:      { type: 'email',           label: 'email_intro',      display: 'Email Intro',        note: 'First email — requires template',                color: '#1f3a5f', badge: '#60a5fa' },
  email_followup_1: { type: 'email',           label: 'email_followup_1', display: 'Email Follow-up 1',  note: 'Second email if no reply',                       color: '#1f3a5f', badge: '#60a5fa' },
  email_followup_2: { type: 'email',           label: 'email_followup_2', display: 'Email Follow-up 2',  note: 'Third email if no reply',                        color: '#1f3a5f', badge: '#60a5fa' },
};

const SEQ_DEFAULT_STEPS = [
  { type: 'linkedin_invite',  label: 'linkedin_invite',  delay_days: 0 },
  { type: 'linkedin_dm',      label: 'linkedin_dm_1',    delay_days: 0 },
  { type: 'linkedin_dm',      label: 'linkedin_dm_2',    delay_days: 7 },
  { type: 'email',            label: 'email_intro',      delay_days: 0 },
  { type: 'email',            label: 'email_followup_1', delay_days: 7 },
  { type: 'email',            label: 'email_followup_2', delay_days: 14 },
];

// Normalise steps from DB — handle legacy action_type format
function normaliseSeqStep(s) {
  if (s.type && s.label) return s;
  // Legacy: action_type field
  const key = s.action_type || s.label || 'email_intro';
  const def = SEQ_STEP_DEFS[key];
  return { ...s, type: def?.type || 'email', label: def?.label || key };
}

function buildSeqStepRow(s, isNew = false) {
  const key = s.label || s.action_type || 'email_intro';
  const def = SEQ_STEP_DEFS[key] || SEQ_STEP_DEFS.email_intro;
  const delay = Number(s.delay_days) || 0;
  const typeColor = { linkedin_invite: '#4ade80', linkedin_dm: '#a78bfa', email: '#60a5fa' }[def.type] || '#6b7280';

  return `
    <div class="seq-step-row" style="display:grid;grid-template-columns:120px 1fr auto 32px;gap:10px;
         align-items:center;padding:10px 12px;background:#111;border:1px solid #1C1C1F;
         border-radius:6px;margin-bottom:6px${isNew ? ';border-color:#C9A84C44' : ''}">
      <select class="seq-type-select"
        style="background:#0d0d0f;border:1px solid #2a2a2a;color:#e5e7eb;
               padding:5px 8px;border-radius:4px;font-size:11px;font-family:'DM Mono',monospace">
        ${Object.entries(SEQ_STEP_DEFS).map(([k, d]) =>
          `<option value="${k}"${key === k ? ' selected' : ''}>${d.display}</option>`
        ).join('')}
      </select>
      <div style="font-size:11px;color:#4a4845;font-family:'DM Mono',monospace" class="seq-step-note">
        ${esc(def.note)}
        ${def.type !== 'linkedin_invite' ? `<span style="color:#3a3a3a;margin-left:4px">· template required</span>` : ''}
      </div>
      <div style="display:flex;align-items:center;gap:6px">
        <span style="color:#4a4845;font-size:11px;font-family:'DM Mono',monospace;white-space:nowrap">+</span>
        <input type="number" class="seq-delay-input" value="${delay}" min="0"
          style="width:50px;background:#0d0d0f;border:1px solid #2a2a2a;color:#e5e7eb;
                 padding:5px;border-radius:4px;font-size:11px;text-align:center;font-family:'DM Mono',monospace">
        <span style="color:#4a4845;font-size:11px;font-family:'DM Mono',monospace;white-space:nowrap">days</span>
      </div>
      <button onclick="this.closest('.seq-step-row').remove()"
        style="background:none;border:1px solid #2a2a2a;color:#6b7280;width:28px;height:28px;
               border-radius:4px;cursor:pointer;font-size:14px;line-height:1">&#215;</button>
    </div>`;
}

// Update note text when step type dropdown changes
window._seqTypeChanged = function(sel) {
  const row = sel.closest('.seq-step-row');
  const key = sel.value;
  const def = SEQ_STEP_DEFS[key] || SEQ_STEP_DEFS.email_intro;
  const note = row.querySelector('.seq-step-note');
  if (note) note.innerHTML = esc(def.note) + (def.type !== 'linkedin_invite' ? `<span style="color:#3a3a3a;margin-left:4px">· template required</span>` : '');
};

window.editDealSequence = async function(dealId) {
  window._editingDealSequenceId = dealId;
  await window.openEditSequence(dealId);
};

window.openEditSequence = async function(dealId) {
  const id = dealId || window._editingDealSequenceId || activeDeal;
  if (!id) { showToast('No deal selected', 'error'); return; }
  window._editingDealSequenceId = id;

  let rawSteps = [];
  try {
    const data = await api(`/api/deals/${id}/sequence`);
    rawSteps = data?.steps || [];
  } catch {}

  const steps = rawSteps.length ? rawSteps.map(normaliseSeqStep) : SEQ_DEFAULT_STEPS;

  let modal = document.getElementById('deal-sequence-modal');
  if (modal) modal.remove();
  modal = document.createElement('div');
  modal.id = 'deal-sequence-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.88);z-index:700;display:flex;align-items:center;justify-content:center;padding:24px';
  modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });

  modal.innerHTML = `
    <div style="background:#0A0A0C;border:1px solid #1C1C1F;border-radius:10px;
                width:100%;max-width:640px;max-height:88vh;overflow-y:auto;padding:28px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <h3 style="color:#EDE9E3;font-family:'Cormorant Garamond',serif;font-size:20px;margin:0">
          Edit Outreach Sequence
        </h3>
        <button onclick="document.getElementById('deal-sequence-modal').remove()"
          style="background:none;border:1px solid #2a2a2a;color:#6b7280;width:30px;height:30px;border-radius:4px;cursor:pointer;font-size:16px">&#215;</button>
      </div>
      <p style="color:#4a4845;font-size:12px;font-family:'DM Mono',monospace;margin:0 0 20px">
        One contact per firm at a time. LI invite &rarr; LI DM on acceptance &rarr; email if no reply.
        All outreach to other contacts at the firm is suppressed once anyone responds.
      </p>

      <div id="deal-seq-steps">
        ${steps.map(s => buildSeqStepRow(s)).join('')}
      </div>

      <div style="margin-top:14px;padding-top:14px;border-top:1px solid #1C1C1F;
                  display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <span style="color:#4a4845;font-size:11px;font-family:'DM Mono',monospace">Add step:</span>
        ${Object.entries(SEQ_STEP_DEFS).map(([k, d]) => `
          <button onclick="window._addSeqStep('${k}')"
            style="padding:4px 10px;background:#111;border:1px solid #2a2a2a;color:#9ca3af;
                   border-radius:4px;cursor:pointer;font-size:11px;font-family:'DM Mono',monospace">
            + ${esc(d.display)}
          </button>`).join('')}
      </div>

      <div style="margin-top:20px;display:flex;gap:10px">
        <button onclick="window.saveDealSequence()"
          style="padding:10px 22px;background:#C9A84C;border:none;color:#000;border-radius:6px;cursor:pointer;font-weight:700;font-size:13px">
          Save Sequence
        </button>
        <button onclick="document.getElementById('deal-sequence-modal').remove()"
          style="padding:10px 18px;background:none;border:1px solid #2a2a2a;color:#6b7280;border-radius:6px;cursor:pointer;font-size:13px">
          Cancel
        </button>
      </div>
    </div>`;

  // Wire up the onchange handlers after innerHTML is set
  modal.addEventListener('change', e => {
    if (e.target.classList.contains('seq-type-select')) window._seqTypeChanged(e.target);
  });

  document.body.appendChild(modal);
};

window._addSeqStep = function(key) {
  const container = document.getElementById('deal-seq-steps');
  if (!container) return;
  const def = SEQ_STEP_DEFS[key] || SEQ_STEP_DEFS.email_intro;
  const defaultDelay = key.includes('followup_2') ? 14 : key.includes('followup') ? 7 : 0;
  const tmp = document.createElement('div');
  tmp.innerHTML = buildSeqStepRow({ type: def.type, label: def.label, delay_days: defaultDelay }, true);
  container.appendChild(tmp.firstElementChild);
};

window.saveDealSequence = async function() {
  const id = window._editingDealSequenceId;
  if (!id) { showToast('No deal context', 'error'); return; }

  const rows = document.querySelectorAll('#deal-seq-steps .seq-step-row');
  const steps = [...rows].map((row, i) => {
    const key = row.querySelector('.seq-type-select')?.value || 'email_intro';
    const def = SEQ_STEP_DEFS[key] || { type: 'email', label: key };
    const delay = parseInt(row.querySelector('.seq-delay-input')?.value) || 0;
    return { step: i + 1, type: def.type, label: def.label, delay_days: delay };
  });

  const btn = document.querySelector('#deal-sequence-modal button[onclick="window.saveDealSequence()"]');
  if (btn) { btn.textContent = 'Saving…'; btn.disabled = true; }

  try {
    await api(`/api/deals/${id}/sequence`, 'PUT', { steps });
    showToast('Sequence saved');
    document.getElementById('deal-sequence-modal')?.remove();
    await loadDealTemplatesTab(id).catch(() => {});
  } catch (err) {
    showToast('Save failed: ' + err.message, 'error');
    if (btn) { btn.textContent = 'Save Sequence'; btn.disabled = false; }
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// CAMPAIGN REVIEW TAB
// ─────────────────────────────────────────────────────────────────────────────

let campaignRefreshInterval = null;
async function loadCampaignReviewTab(dealId) {
  const container = document.getElementById('deal-tab-rankings');
  if (!container) return;

  container.innerHTML = '<div style="padding:24px;color:#6b7280">Loading...</div>';

  try {
    const batchRes = await fetch(`${API_BASE}/api/deals/${dealId}/batch/current`, { credentials: 'include' });
    const batch = batchRes.ok ? await batchRes.json() : null;

    if (!batch) {
      stopCampaignAutoRefresh();
      container.innerHTML = `
        <div style="padding:32px;text-align:center;color:#374151">
          No batch active. Research will begin automatically.
        </div>
      `;
      return;
    }

    const firmsUrl = batch.status === 'pending_approval'
      ? `${API_BASE}/api/deals/${dealId}/batch/${batch.id}/firms?page=${campaignReviewPage}&limit=${CAMPAIGN_FIRMS_PER_PAGE}`
      : `${API_BASE}/api/deals/${dealId}/batch/${batch.id}/firms`;
    const firmsRes = await fetch(firmsUrl, { credentials: 'include' });
    if (!firmsRes.ok) {
      const errText = await firmsRes.text().catch(() => '');
      throw new Error(errText || `Failed to load firms (${firmsRes.status})`);
    }
    const firmsPayload = await firmsRes.json();
    const firms = Array.isArray(firmsPayload) ? firmsPayload : (firmsPayload.firms || []);

    if (batch.status === 'researching') {
      container.innerHTML = renderResearchingState(dealId, batch);
      await loadResearchFirmsList(dealId, batch.id);
      startCampaignAutoRefresh(dealId);
    } else if (batch.status === 'pending_approval') {
      stopCampaignAutoRefresh();
      const allFirmsRes = await fetch(`${API_BASE}/api/deals/${dealId}/batch/${batch.id}/firms`, { credentials: 'include' });
      const allFirmsPayload = allFirmsRes.ok ? await allFirmsRes.json() : firms;
      const allFirms = Array.isArray(allFirmsPayload) ? allFirmsPayload : firms;
      container.innerHTML = renderCampaignReview(dealId, batch, firms, {
        total: firmsPayload.total || firms.length,
        pages: firmsPayload.pages || 1,
        page: firmsPayload.page || campaignReviewPage,
        allFirms,
      });
    } else if (batch.status === 'approved' || batch.status === 'active') {
      stopCampaignAutoRefresh();
      const allEnriched = firms.length > 0 && firms.every(f => f.enrichment_status === 'complete');
      container.innerHTML = allEnriched
        ? renderCampaignTrackerState(dealId, batch, firms)
        : renderEnrichmentState(dealId, batch, firms);
      if (!allEnriched) {
        startEnrichmentStatusPoll(dealId, batch.id);
      }
    } else {
      stopCampaignAutoRefresh();
      container.innerHTML = renderCampaignReview(dealId, batch, firms);
    }
  } catch (e) {
    container.innerHTML = `<div class="loading-placeholder text-red">Failed to load: ${esc(e.message)}</div>`;
  }
}

function renderResearchingState(dealId, batch) {
  const firmsFound = batch.firms_researched || batch.ranked_firms || 0;
  const target = batch.firms_target || batch.target_firms || 100;
  const pct = Math.min(Math.round((firmsFound / target) * 100), 100);
  const startedAt = batch.created_at ? new Date(batch.created_at) : new Date();
  const elapsedMs = Date.now() - startedAt.getTime();
  const elapsedHours = Math.floor(elapsedMs / (1000 * 60 * 60));
  const elapsedMins = Math.floor((elapsedMs % (1000 * 60 * 60)) / (1000 * 60));
  const elapsedStr = elapsedHours > 0 ? `${elapsedHours}h ${elapsedMins}m` : `${elapsedMins}m`;
  const msPerFirm = firmsFound > 0 ? elapsedMs / firmsFound : 0;
  const remaining = target - firmsFound;
  const estimatedMsLeft = msPerFirm * remaining;
  const estHours = Math.floor(estimatedMsLeft / (1000 * 60 * 60));
  const estMins = Math.floor((estimatedMsLeft % (1000 * 60 * 60)) / (1000 * 60));
  const estStr = firmsFound > 2 ? (estHours > 0 ? `~${estHours}h ${estMins}m remaining` : `~${estMins}m remaining`) : 'Estimating...';

  return `
    <div style="padding:24px">
      <div style="margin-bottom:28px">
        <div style="font-size:10px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:4px">
          Batch ${batch.batch_number}
        </div>
        <h2 style="font-family:'Playfair Display',serif;font-size:22px;color:#e5e7eb;margin:0 0 4px">Researching Firms</h2>
        <div style="color:#6b7280;font-size:13px">
          Roco is identifying and ranking the best-fit investors for this deal. No outreach will begin until you approve the campaign.
        </div>
      </div>

      <div style="background:#0d0d0f;border:1px solid #1a1a1a;border-radius:10px;padding:24px;margin-bottom:20px">
        <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:16px">
          <div>
            <span data-firms-count style="font-size:36px;font-weight:600;color:#d4a847;font-family:'Playfair Display',serif">${firmsFound}</span>
            <span style="font-size:18px;color:#4a4a4a;font-family:'Playfair Display',serif">/${target}</span>
            <span style="font-size:13px;color:#6b7280;margin-left:8px">firms researched</span>
          </div>
          <div style="text-align:right">
            <div style="font-size:12px;color:#6b7280;font-family:'DM Mono',monospace">${elapsedStr} elapsed</div>
            <div style="font-size:11px;color:#374151;font-family:'DM Mono',monospace;margin-top:2px">${estStr}</div>
          </div>
        </div>
        <div style="background:#1a1a1a;border-radius:4px;height:8px;overflow:hidden;margin-bottom:10px;position:relative">
          <div data-progress-bar style="height:100%;border-radius:4px;background:linear-gradient(90deg,#b8903e,#d4a847,#e8c97a);width:${pct}%;transition:width 0.6s ease;box-shadow:0 0 8px rgba(212,168,71,0.4)"></div>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:10px;color:#374151;font-family:'DM Mono',monospace">
          <span>${pct}% complete</span>
          <span>${remaining} firms to go</span>
        </div>
      </div>

      ${firmsFound > 0 ? `
        <div>
          <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:10px">
            Firms Found So Far
          </div>
          <div id="research-firms-list">Loading...</div>
        </div>
      ` : `
        <div style="text-align:center;padding:32px;color:#374151;border:1px dashed #1a1a1a;border-radius:8px">
          Research starting — firms will appear here as they are ranked.
        </div>
      `}
    </div>
  `;
}

async function loadResearchFirmsList(dealId, batchId) {
  const container = document.getElementById('research-firms-list');
  if (!container) return;
  const res = await fetch(`${API_BASE}/api/deals/${dealId}/batch/${batchId}/firms`, { credentials: 'include' });
  if (!res.ok) { container.innerHTML = ''; return; }
  const firms = await res.json();
  container.innerHTML = firms.length === 0
    ? '<div style="color:#374151;font-size:12px">No firms yet.</div>'
    : firms.map((f, i) => `
        <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:#111;border-radius:6px;margin-bottom:6px">
          <div style="display:flex;align-items:center;gap:10px">
            <span style="color:#d4a847;font-size:13px;font-family:'DM Mono',monospace;width:20px">${f.rank || i + 1}</span>
            <span style="color:#e5e7eb;font-size:13px">${esc(f.firm_name || 'Unknown')}</span>
          </div>
          <span style="padding:2px 8px;background:rgba(212,168,71,0.1);color:#d4a847;border-radius:3px;font-size:11px;font-family:'DM Mono',monospace">
            ${f.score || '--'}
          </span>
        </div>
      `).join('');
}

function startCampaignAutoRefresh(dealId) {
  stopCampaignAutoRefresh();
  campaignRefreshInterval = setInterval(async () => {
    const res = await fetch(`${API_BASE}/api/deals/${dealId}/batch/current`, { credentials: 'include' });
    if (!res.ok) return;
    const batch = await res.json();
    if (!batch) return;

    const countEl = document.querySelector('[data-firms-count]');
    if (countEl) countEl.textContent = batch.firms_researched || batch.ranked_firms || 0;

    const barEl = document.querySelector('[data-progress-bar]');
    if (barEl) {
      const target = batch.firms_target || batch.target_firms || 100;
      const pct = Math.min(Math.round((((batch.firms_researched || batch.ranked_firms || 0)) / target) * 100), 100);
      barEl.style.width = pct + '%';
    }

    if (batch.status === 'pending_approval') {
      stopCampaignAutoRefresh();
      await loadCampaignReviewTab(dealId);
    } else if (batch.status === 'researching') {
      await loadResearchFirmsList(dealId, batch.id);
    }
  }, 8000);
}

function stopCampaignAutoRefresh() {
  if (campaignRefreshInterval) {
    clearInterval(campaignRefreshInterval);
    campaignRefreshInterval = null;
  }
  stopEnrichmentStatusPoll();
}

let enrichmentPollInterval = null;
function startEnrichmentStatusPoll(dealId, batchId) {
  stopEnrichmentStatusPoll();
  enrichmentPollInterval = setInterval(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/deals/${dealId}/batch/${batchId}/firms`, { credentials: 'include' });
      if (!res.ok) return;
      const firms = await res.json();

      // Patch each firm's status badge in-place — never rebuild the DOM
      let allDone = true;
      for (const firm of firms) {
        const badge = document.getElementById(`enrich-status-${firm.id}`);
        if (!badge) continue;
        if (firm.enrichment_status !== 'complete') allDone = false;
        const { label, color } = enrichStatusDisplay(firm);
        badge.textContent = label;
        badge.style.color = color;
        const countEl = document.getElementById(`enrich-count-${firm.id}`);
        if (countEl && firm.contacts_found != null) {
          countEl.textContent = `${firm.contacts_found} contact${firm.contacts_found !== 1 ? 's' : ''}`;
        }
      }

      // Update overall progress bar if present
      const enriched = firms.filter(f => f.enrichment_status === 'complete').length;
      const pct = Math.round((enriched / (firms.length || 1)) * 100);
      const bar = document.querySelector('[data-enrich-bar]');
      if (bar) bar.style.width = pct + '%';
      const countSummary = document.querySelector('[data-enrich-summary]');
      if (countSummary) countSummary.textContent = `${enriched} / ${firms.length} firms enriched`;

      if (allDone) {
        stopEnrichmentStatusPoll();
        await loadCampaignReviewTab(dealId);
      }
    } catch (_) {}
  }, 10000);
}

function parseFirmEnrichmentMeta(firm) {
  const raw = String(firm?.status_reason || '').trim();
  if (!raw.startsWith('{')) return {};
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

function enrichStatusDisplay(firmOrStatus) {
  const firm = typeof firmOrStatus === 'object' && firmOrStatus !== null ? firmOrStatus : { enrichment_status: firmOrStatus };
  const status = firm.enrichment_status || firmOrStatus;
  const meta = parseFirmEnrichmentMeta(firm);
  switch (status) {
    case 'complete':     return { label: '✓ Enriched',    color: '#4ade80' };
    case 'in_progress':  return { label: '⟳ In progress', color: '#d4a847' };
    case 'failed':
      return meta.manual_review_required
        ? { label: '✕ Manual review', color: '#ef4444' }
        : { label: '✕ Failed', color: '#ef4444' };
    default:             return { label: '· Pending',     color: '#4a4a4a' };
  }
}

function stopEnrichmentStatusPoll() {
  if (enrichmentPollInterval) {
    clearInterval(enrichmentPollInterval);
    enrichmentPollInterval = null;
  }
}

function getFirmTrackerStatusDisplay(firm) {
  switch (firm.firm_stage) {
    case 'meeting_booked':   return { label: 'Meeting booked', color: '#22c55e', bg: 'rgba(34,197,94,0.12)' };
    case 'replied':          return { label: 'Replied', color: '#38bdf8', bg: 'rgba(56,189,248,0.12)' };
    case 'invite_accepted':  return { label: 'Connection accepted', color: '#a78bfa', bg: 'rgba(167,139,250,0.12)' };
    case 'outreach_started': return { label: 'Outreach in progress', color: '#f59e0b', bg: 'rgba(245,158,11,0.12)' };
    case 'ready_for_outreach': return { label: 'Ready for outreach', color: '#4ade80', bg: 'rgba(74,222,128,0.12)' };
    case 'closed':           return { label: 'Closed', color: '#f87171', bg: 'rgba(248,113,113,0.12)' };
    default:                 return { label: 'Finding decision makers', color: '#d4a847', bg: 'rgba(212,168,71,0.12)' };
  }
}

function renderFirmTrackerProgress(firm) {
  const total = Number(firm.total_contacts || firm.contacts_found || 0);
  const contacted = Math.min(Number(firm.contacted_count || 0), total || Number(firm.contacted_count || 0));
  const accepted = Math.min(Number(firm.invite_accepted_count || 0), total || Number(firm.invite_accepted_count || 0));
  const replied = Math.min(Number(firm.replied_count || 0), total || Number(firm.replied_count || 0));
  const meeting = Math.min(Number(firm.meeting_booked_count || 0), total || Number(firm.meeting_booked_count || 0));
  if (!total) return '0 contacts found';
  if (meeting > 0) return `${meeting}/${total} meeting booked`;
  if (replied > 0) return `${replied}/${total} replied`;
  if (accepted > 0) return `${accepted}/${total} accepted`;
  if (contacted > 0) return `${contacted}/${total} outreached`;
  return `0/${total} outreached`;
}

function renderFirmTrackerMilestones(firm) {
  const total = Number(firm.total_contacts || firm.contacts_found || 0);
  const items = [
    [`Invite sent`, Number(firm.invite_sent_count || 0)],
    [`Email sent`, Number(firm.email_sent_count || 0)],
    [`Accepted`, Number(firm.invite_accepted_count || 0)],
    [`Replied`, Number(firm.replied_count || 0)],
  ].filter(([, count]) => count > 0);
  if (!total) return 'No contacts enriched';
  if (!items.length) return `0/${total} outreached`;
  return items.map(([label, count]) => `${label}: ${count}/${total}`).join(' · ');
}

const CONTROL_STYLE = {
  majority: { label: 'Majority',  color: '#C9A84C', bg: 'rgba(201,168,76,0.12)'  },
  minority: { label: 'Minority',  color: '#60A5FA', bg: 'rgba(96,165,250,0.12)'  },
  both:     { label: 'Maj+Min',   color: '#4ADE80', bg: 'rgba(74,222,128,0.12)'  },
  unknown:  { label: 'Control ?', color: '#4A4845', bg: 'rgba(74,72,69,0.10)'    },
};

function controlSortKey(firm) {
  // Perfect match first, flexible second, unknown third, mismatch last
  const pref = (firm.control_preference || 'unknown').toLowerCase();
  if (pref === 'majority') return 0;
  if (pref === 'both')     return 1;
  if (pref === 'unknown')  return 2;
  if (pref === 'minority') return 3;
  return 2;
}

function renderControlMixSummary(firms, dealControlPref) {
  const counts = { majority: 0, minority: 0, both: 0, unknown: 0 };
  firms.forEach(f => {
    const p = (f.control_preference || 'unknown').toLowerCase();
    if (counts[p] !== undefined) counts[p]++;
    else counts.unknown++;
  });
  const total = firms.length;
  return `
    <div style="display:flex;gap:16px;align-items:center;flex-wrap:wrap;
                padding:10px 16px;background:#0d0d0f;border-radius:6px;
                margin-bottom:14px;font-family:'DM Mono',monospace;font-size:11px">
      <span style="color:#6b7280">Control mix (${total} firms):</span>
      ${counts.majority > 0 ? `<span style="color:#C9A84C">&#9679;&nbsp;${counts.majority} majority</span>` : ''}
      ${counts.minority > 0 ? `<span style="color:#60A5FA">&#9679;&nbsp;${counts.minority} minority</span>` : ''}
      ${counts.both > 0    ? `<span style="color:#4ADE80">&#9679;&nbsp;${counts.both} flexible</span>` : ''}
      ${counts.unknown > 0 ? `<span style="color:#4A4845">&#9679;&nbsp;${counts.unknown} unclassified</span>` : ''}
      ${dealControlPref && dealControlPref !== 'either'
        ? `<span style="color:#6b7280;margin-left:4px">&#183; Deal requires: <span style="color:#EDE9E3">${esc(dealControlPref)}</span></span>`
        : ''}
    </div>`;
}

function renderFirmCard(firm, rank, dealId, batchId) {
  const pastInv = Array.isArray(firm.past_investments) ? firm.past_investments.slice(0, 5) : [];
  const isAngel = firm.contact_type === 'angel';
  const displayRank = firm.rank || rank;
  const scoreValue = Number.isFinite(Number(firm.score)) ? Math.round(Number(firm.score)) : 0;
  const firmLinkUrl = firm.firm_link_url || null;
  const firmLinkType = firm.firm_link_type || null;
  const firmLabel = firmLinkUrl
    ? `<a href="${esc(firmLinkUrl)}" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation()" style="color:#e5e7eb;text-decoration:underline;text-underline-offset:3px">${esc(firm.firm_name || 'Unknown')}</a>`
    : `<span style="color:#e5e7eb;font-size:14px;font-weight:600">${esc(firm.firm_name || 'Unknown')}</span>`;
  const firmLinkBadge = firmLinkUrl
    ? `<span style="padding:2px 8px;border-radius:3px;font-size:10px;font-family:'DM Mono',monospace;background:${firmLinkType === 'linkedin' ? 'rgba(96,165,250,0.12)' : 'rgba(34,197,94,0.12)'};color:${firmLinkType === 'linkedin' ? '#60a5fa' : '#4ade80'}">${firmLinkType === 'linkedin' ? 'LinkedIn' : 'Website'}</span>`
    : '';
  return `
    <div id="firm-card-${firm.id}" style="border:1px solid #1e1e1e;border-radius:8px;margin-bottom:8px;overflow:hidden">
      <div style="display:flex;justify-content:space-between;align-items:center;padding:14px 16px;background:#111;cursor:pointer;user-select:none"
           onclick="window.toggleFirmCard('${firm.id}','${dealId}')"
           onmouseover="this.style.background='#161616'"
           onmouseout="this.style.background='#111'">
        <div style="display:flex;align-items:center;gap:12px">
          <span style="font-size:22px;font-weight:500;color:#C9A84C;font-family:'Playfair Display',serif;min-width:42px;flex-shrink:0;line-height:1">
            #${displayRank}
          </span>
          <div>
            <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:3px">
              ${firmLabel}
              ${firmLinkBadge}
              <span style="padding:2px 8px;border-radius:999px;font-size:10px;font-family:'DM Mono',monospace;background:rgba(201,168,76,0.12);color:#C9A84C;border:1px solid rgba(201,168,76,0.2)">
                ${scoreValue}/100
              </span>
            </div>
            <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
              ${firm.aum ? `<span style="padding:2px 8px;border-radius:3px;font-size:10px;font-family:'DM Mono',monospace;background:#1a1a2a;color:#818cf8">AUM: ${esc(firm.aum)}</span>` : ''}
              ${isAngel ? `<span style="padding:2px 8px;border-radius:3px;font-size:10px;font-family:'DM Mono',monospace;background:rgba(245,158,11,0.1);color:#f59e0b">Angel</span>` : ''}
              ${(() => { const cp = CONTROL_STYLE[(firm.control_preference || 'unknown').toLowerCase()] || CONTROL_STYLE.unknown; return `<span style="padding:2px 8px;border-radius:3px;font-size:10px;font-family:'DM Mono',monospace;background:${cp.bg};color:${cp.color}">${cp.label}</span>`; })()}
            </div>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:8px">
          <button onclick="event.stopPropagation();window.removeFirmFromBatch('${batchId}','${firm.id}','${dealId}')"
            style="padding:4px 10px;background:none;border:1px solid #2a2a2a;color:#ef4444;border-radius:4px;cursor:pointer;font-size:11px">
            Remove
          </button>
          <span id="chevron-${firm.id}" style="color:#3a3a3a;font-size:16px;display:inline-block;transition:transform 0.2s">▾</span>
        </div>
      </div>
      <div id="firm-detail-${firm.id}" data-batch-id="${batchId}" style="display:none;padding:16px;background:#0d0d0f;border-top:1px solid #1a1a1a">
        <div style="margin-bottom:14px">
          <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:6px">Why This Firm</div>
          <p style="color:#9ca3af;font-size:13px;line-height:1.65;margin:0;font-style:italic">${esc(firm.justification || 'Justification not available.')}</p>
        </div>
        ${firm.thesis ? `<div style="margin-bottom:14px"><div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:6px">Investment Thesis</div><p style="color:#9ca3af;font-size:13px;line-height:1.65;margin:0">${esc(firm.thesis.slice(0, 400))}${firm.thesis.length > 400 ? '...' : ''}</p></div>` : ''}
        ${pastInv.length > 0 ? `<div style="margin-bottom:14px"><div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:6px">Past Investments in Similar Deals</div><div style="display:flex;flex-wrap:wrap;gap:6px">${pastInv.map(inv => `<span style="padding:4px 10px;background:#1a1a2a;border:1px solid #2a2a3a;border-radius:4px;font-size:11px;color:#9ca3af;font-family:'DM Mono',monospace">${esc(typeof inv === 'string' ? inv : (inv.company || inv.name || JSON.stringify(inv)))}</span>`).join('')}</div></div>` : ''}
        <div>
          <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:8px">
            Contacts${firm.contacts_found ? ` — ${firm.contacts_found} found` : ''}
          </div>
          <div data-contacts-slot></div>
        </div>
      </div>
    </div>
  `;
}

function getDealTimezone(dealId) {
  const deal = (allDeals || []).find(d => String(d.id || d._id) === String(dealId));
  return deal?.timezone || 'America/New_York';
}

function renderVisualTimeline(batch, firms, dealId) {
  const steps = [
    { label: 'Email Intro', type: 'email', day: 0, color: '#3b82f6', mode: 'fixed' },
    { label: 'LI Connect', type: 'linkedin', day: 0, color: '#22c55e', mode: 'fixed' },
    { label: 'Email Follow-up', type: 'email', day: 7, color: '#3b82f6', mode: 'fixed' },
    { label: 'Final Email', type: 'email', day: 14, color: '#3b82f6', mode: 'fixed' },
    { label: 'LI DM', type: 'linkedin', day: 3, color: '#a78bfa', mode: 'event', meta: 'On acceptance' },
    { label: 'LI Follow-up', type: 'linkedin', day: 10, color: '#a78bfa', mode: 'event', meta: 'After LI DM' },
  ];
  const totalDays = 21;
  const firmCount = firms.length;
  const enrichedContacts = firms.reduce((sum, firm) => sum + Number(firm.contacts_found || 0), 0);
  const readyEmailCount = enrichedContacts > 0 ? enrichedContacts * 3 : 0;
  const readyLinkedInCount = enrichedContacts > 0 ? enrichedContacts * 3 : 0;
  const timezone = getDealTimezone(dealId);
  const today = new Date();
  const addDays = (date, days) => {
    const result = new Date(date);
    result.setDate(result.getDate() + days);
    return result;
  };
  const fmtDate = (date) => date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    timeZone: timezone,
  });
  const startDate = today;
  const endDate = addDays(today, totalDays);
  const timelineRows = steps.map(step => ({ ...step, stepDate: addDays(startDate, step.day), leftPct: (step.day / totalDays) * 100, fmtDate: fmtDate(addDays(startDate, step.day)) }));

  return `
    <div style="background:#080809;border:1px solid #1e1e20;border-radius:10px;padding:28px;margin-bottom:24px;overflow:hidden">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:24px">
        <div>
          <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.2em;font-family:'DM Mono',monospace;margin-bottom:6px">Projected Outreach Timeline</div>
          <div style="font-family:'Playfair Display',serif;font-size:18px;color:#e5e7eb">${fmtDate(startDate)} — ${fmtDate(endDate)}</div>
        </div>
        <div style="display:flex;gap:16px;text-align:right">
          ${[['Firms', firmCount], ['Emails Ready', readyEmailCount], ['LinkedIn Ready', readyLinkedInCount]].map(([l, v]) => `<div><div style="font-size:18px;color:#d4a847;font-family:'Playfair Display',serif">${v}</div><div style="font-size:10px;color:#4a4a4a;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.1em">${l}</div></div>`).join('')}
        </div>
      </div>
      <div style="position:relative;margin-bottom:6px;padding:0 2px">
        <div style="display:flex;justify-content:space-between;margin-bottom:4px">
          ${[0, 7, 14, 21].map(d => `<div style="font-size:10px;color:#2a2a2a;font-family:'DM Mono',monospace;text-align:center">Day ${d}<br><span style="font-size:9px;color:#1e1e1e">${fmtDate(addDays(startDate, d))}</span></div>`).join('')}
        </div>
        <div style="position:relative;height:3px;background:#1a1a1a;border-radius:2px;margin-bottom:4px">
          <div style="position:absolute;left:0;top:0;height:3px;border-radius:2px;width:66%;background:linear-gradient(90deg,#1a2a3a,#2a3a4a);opacity:0.4"></div>
          ${timelineRows.map(step => `<div style="position:absolute;left:${step.leftPct}%;top:-4px;transform:translateX(-50%)"><div style="width:2px;height:11px;background:${step.color};border-radius:1px;opacity:0.6"></div></div>`).join('')}
        </div>
      </div>
      <div style="display:flex;flex-direction:column;gap:6px;margin-top:16px">
        ${timelineRows.map(step => `<div style="display:grid;grid-template-columns:110px 1fr auto;align-items:center;gap:12px;padding:10px 14px;background:#0f0f11;border-radius:6px;border:1px solid #141416;border-left:3px solid ${step.color}"><div><div style="font-size:12px;color:#e5e7eb;font-weight:500">${step.label}</div><div style="font-size:10px;color:#4a4a4a;font-family:'DM Mono',monospace;margin-top:1px">${step.type === 'email' ? 'Email' : 'LinkedIn'}</div></div><div style="position:relative;height:6px;background:#111;border-radius:3px"><div style="position:absolute;left:${step.leftPct}%;top:50%;transform:translate(-50%,-50%);width:10px;height:10px;border-radius:50%;background:${step.color};box-shadow:0 0 6px ${step.color}88"></div><div style="position:absolute;left:0;top:50%;transform:translateY(-50%);height:2px;width:${step.leftPct}%;background:${step.color}33;border-radius:1px"></div></div><div style="text-align:right;white-space:nowrap"><div style="font-size:11px;color:${step.color};font-family:'DM Mono',monospace">${step.mode === 'fixed' ? step.fmtDate : (step.meta || 'Event-based')}</div><div style="font-size:10px;color:#2a2a2a;font-family:'DM Mono',monospace">${step.mode === 'fixed' ? `Day ${step.day}` : 'Trigger-based'}</div></div></div>`).join('')}
      </div>
      <div style="margin-top:16px;padding-top:14px;border-top:1px solid #141416;display:flex;justify-content:space-between;align-items:center">
        <div style="display:flex;gap:14px">${[['#3b82f6', 'Email'], ['#22c55e', 'Connection'], ['#a78bfa', 'LinkedIn DM']].map(([c, l]) => `<div style="display:flex;align-items:center;gap:5px"><div style="width:8px;height:8px;border-radius:50%;background:${c}"></div><span style="font-size:11px;color:#4a4a4a">${l}</span></div>`).join('')}</div>
        <div style="font-size:10px;color:#2a2a2a;font-family:'DM Mono',monospace">Timezone: ${esc(timezone)} · Sends in the deal's configured windows</div>
      </div>
    </div>
  `;
}

function renderScoringMethodology(deal) {
  const d = deal || {};
  const rows = [
    ['Geographic alignment',   '25 pts', `Target: ${d.geography || d.target_geography || 'Global'}`],
    ['Check size / EBITDA fit', '25 pts', `Deal equity: $${d.equity || d.min_cheque || '?'}M`],
    ['Sector alignment',        '25 pts', d.sector || 'Not specified'],
    ['Investor type match',     '15 pts', d.deal_type || d.raise_type || 'Not specified'],
    ['Recent deal activity',    '10 pts', 'Investments in last 12 months'],
    ['Deal intelligence boost', '+20 max', 'Backed comparable deals in PitchBook'],
  ];
  return `
    <details style="margin-bottom:16px">
      <summary style="cursor:pointer;font-size:11px;color:#6b7280;font-family:'DM Mono',monospace;
                      text-transform:uppercase;letter-spacing:0.15em;list-style:none;user-select:none">
        How firms are scored ▾
      </summary>
      <div style="margin-top:10px;padding:14px;background:#0d0d0f;border:1px solid #1a1a1a;border-radius:6px">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
          ${rows.map(([label, pts, context]) => `
            <div style="padding:8px 10px;background:#111;border-radius:4px">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:3px">
                <span style="font-size:12px;color:#e5e7eb">${label}</span>
                <span style="font-size:11px;color:#d4a847;font-family:'DM Mono',monospace">${pts}</span>
              </div>
              <div style="font-size:10px;color:#4a4a4a">${context}</div>
            </div>
          `).join('')}
        </div>
        <div style="margin-top:8px;font-size:11px;color:#374151;font-family:'DM Mono',monospace">
          Maximum score: 100 + 20 intelligence boost = 100 (capped)
        </div>
      </div>
    </details>
  `;
}

function renderCampaignReview(dealId, batch, firms) {
  const deal = (allDeals || []).find(d => String(d.id) === String(dealId)) || {};
  const meta = arguments[3] || { total: (firms || []).length, pages: 1, page: 1 };
  const dealControlPref = deal.investor_control_preference || 'majority';
  const orderedFirms = [...(firms || [])].sort((a, b) => {
    const rankDiff = Number(a.rank || 9999) - Number(b.rank || 9999);
    if (rankDiff !== 0) return rankDiff;
    // Within same rank tier, sort by control match quality
    const ctrlDiff = controlSortKey(a) - controlSortKey(b);
    if (ctrlDiff !== 0) return ctrlDiff;
    return Number(b.score || 0) - Number(a.score || 0);
  });
  const totalPages = meta.pages || 1;
  const currentPage = meta.page || 1;
  const totalFirms = meta.total || orderedFirms.length;
  const timelineFirms = Array.isArray(meta.allFirms) && meta.allFirms.length ? meta.allFirms : orderedFirms;
  const startRank = ((currentPage - 1) * CAMPAIGN_FIRMS_PER_PAGE) + 1;
  return `
    <div style="padding:24px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
        <div>
          <div style="font-size:10px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:4px">Batch ${batch.batch_number}</div>
          <h2 style="font-family:'Playfair Display',serif;font-size:22px;color:#e5e7eb;margin:0 0 4px">Campaign Review</h2>
          <div style="color:#6b7280;font-size:13px">${totalFirms} ranked firms, reviewed before any contact enrichment begins.</div>
        </div>
        <div style="display:flex;gap:10px">
          <button class="btn btn-ghost" onclick="showAddFirmModal('${batch.id}','${dealId}')">Add Firm</button>
          <button class="btn btn-ghost" style="border-color:#7f1d1d;color:#fca5a5" onclick="rejectCampaignBatch('${batch.id}','${dealId}')">Reject Campaign</button>
          <button class="btn btn-primary" onclick="approveCampaignBatch('${batch.id}','${dealId}')">Approve Campaign</button>
        </div>
      </div>
      ${renderScoringMethodology(deal)}
      ${renderVisualTimeline(batch, timelineFirms, dealId)}
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid #1C1C1F">
        <div style="font-size:11px;color:#6b7280;font-family:'DM Mono',monospace">
          ${totalFirms} firms total · Page ${currentPage} of ${totalPages}
        </div>
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          ${Array.from({ length: totalPages }, (_, index) => index + 1).map(page => `
            <button onclick="window.loadCampaignReviewPage('${dealId}', ${page})"
              style="width:28px;height:28px;border-radius:4px;cursor:pointer;font-size:11px;font-family:'DM Mono',monospace;background:${page === currentPage ? '#C9A84C' : '#1a1a1a'};border:1px solid ${page === currentPage ? '#C9A84C' : '#2a2a2a'};color:${page === currentPage ? '#000' : '#6b7280'}">
              ${page}
            </button>`).join('')}
        </div>
      </div>
      ${renderControlMixSummary(orderedFirms, dealControlPref)}
      <div>${orderedFirms.map((firm, index) => renderFirmCard(firm, startRank + index, dealId, batch.id)).join('')}</div>
      ${totalPages > 1 ? `
        <div style="display:flex;justify-content:space-between;align-items:center;padding-top:16px;border-top:1px solid #1C1C1F;margin-top:12px">
          ${currentPage > 1 ? `<button onclick="window.loadCampaignReviewPage('${dealId}', ${currentPage - 1})" style="padding:6px 14px;background:#1a1a1a;border:1px solid #2a2a2a;color:#8A8680;border-radius:4px;cursor:pointer;font-size:11px">← Page ${currentPage - 1}</button>` : '<div></div>'}
          <div style="font-size:11px;color:#3A3835;font-family:'DM Mono',monospace">
            Showing ranks ${startRank}–${startRank + orderedFirms.length - 1} of ${totalFirms}
          </div>
          ${currentPage < totalPages ? `<button onclick="window.loadCampaignReviewPage('${dealId}', ${currentPage + 1})" style="padding:6px 14px;background:#1a1a1a;border:1px solid #2a2a2a;color:#8A8680;border-radius:4px;cursor:pointer;font-size:11px">Page ${currentPage + 1} →</button>` : '<div></div>'}
        </div>` : ''}
    </div>
  `;
}

window.loadCampaignReviewPage = async function(dealId, page = 1) {
  campaignReviewPage = Math.max(1, Number(page) || 1);
  await loadCampaignReviewTab(dealId);
};

function renderEnrichmentState(dealId, batch, firms) {
  const enriched = firms.filter(f => f.enrichment_status === 'complete').length;
  const total = firms.length || 1;
  const pct = Math.round((enriched / total) * 100);
  return `
    <div style="padding:24px">
      <div style="margin-bottom:20px">
        <div style="font-size:10px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:4px">Batch ${batch.batch_number}</div>
        <h2 style="font-family:'Playfair Display',serif;font-size:22px;color:#e5e7eb;margin:0 0 4px">Finding Decision Makers</h2>
        <div style="color:#6b7280;font-size:13px">Outreach will begin as each firm's contacts are confirmed.</div>
      </div>
      <div style="background:#0d0d0f;border:1px solid #1a1a1a;border-radius:10px;padding:24px;margin-bottom:20px">
        <div style="display:flex;justify-content:space-between;margin-bottom:12px">
          <span data-enrich-summary style="color:#e5e7eb;font-size:14px">${enriched}/${firms.length} firms enriched</span>
          <span style="color:#6b7280;font-size:12px;font-family:'DM Mono',monospace">${pct}%</span>
        </div>
        <div style="background:#1a1a1a;border-radius:4px;height:6px;overflow:hidden">
          <div data-enrich-bar style="height:100%;background:linear-gradient(90deg,#22c55e,#4ade80);width:${pct}%;border-radius:4px;transition:width 0.6s ease"></div>
        </div>
      </div>
      <div style="display:flex;flex-direction:column;gap:6px">
        ${firms.map((f, i) => {
          const { label, color } = enrichStatusDisplay(f);
          const pastInv = Array.isArray(f.past_investments) ? f.past_investments.slice(0, 5) : [];
          return `<div style="border:1px solid #1e1e1e;border-radius:6px;overflow:hidden">
            <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:#111;cursor:pointer"
                 onclick="toggleEnrichFirmCard('${f.id}','${dealId}','${batch.id}')"
                 onmouseover="this.style.background='#161616'" onmouseout="this.style.background='#111'">
              <div style="display:flex;align-items:center;gap:10px">
                <span style="color:#4a4a4a;font-size:12px;font-family:'DM Mono',monospace;width:20px">${i + 1}</span>
                <div>
                  <div style="color:#e5e7eb;font-size:13px">${esc(f.firm_name || 'Unknown')}</div>
                  ${f.score ? `<div style="font-size:10px;color:#6b7280;font-family:'DM Mono',monospace">Score: ${f.score}</div>` : ''}
                </div>
              </div>
              <div style="display:flex;align-items:center;gap:12px">
                <span id="enrich-count-${f.id}" style="color:#6b7280;font-size:11px">${f.contacts_found ? `${f.contacts_found} contact${f.contacts_found !== 1 ? 's' : ''}` : ''}</span>
                <span id="enrich-status-${f.id}" style="color:${color};font-size:12px;font-family:'DM Mono',monospace;min-width:80px;text-align:right">${label}</span>
                <button onclick="event.stopPropagation();window.removeFirmFromBatch('${batch.id}','${f.id}','${dealId}')"
                  style="padding:3px 8px;background:none;border:1px solid #2a2a2a;color:#ef4444;border-radius:4px;cursor:pointer;font-size:11px">Remove</button>
                <span id="enrich-chevron-${f.id}" style="color:#3a3a3a;font-size:14px">▾</span>
              </div>
            </div>
            <div id="enrich-firm-detail-${f.id}" style="display:none;padding:16px;background:#0d0d0f;border-top:1px solid #1a1a1a">
              ${f.justification ? `<div style="margin-bottom:12px">
                <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:6px">Why This Firm</div>
                <p style="color:#9ca3af;font-size:13px;line-height:1.65;margin:0;font-style:italic">${esc(f.justification)}</p>
              </div>` : ''}
              ${f.thesis ? `<div style="margin-bottom:12px">
                <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:6px">Investment Thesis</div>
                <p style="color:#9ca3af;font-size:13px;line-height:1.65;margin:0">${esc(f.thesis.slice(0,400))}${f.thesis.length > 400 ? '…' : ''}</p>
              </div>` : ''}
              ${pastInv.length ? `<div style="margin-bottom:12px">
                <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:6px">Past Investments</div>
                <div style="display:flex;flex-wrap:wrap;gap:6px">${pastInv.map(inv => `<span style="padding:3px 8px;background:#1a1a2a;border:1px solid #2a2a3a;border-radius:4px;font-size:11px;color:#9ca3af;font-family:'DM Mono',monospace">${esc(typeof inv === 'string' ? inv : (inv.company || inv.name || ''))}</span>`).join('')}</div>
              </div>` : ''}
              <div>
                <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:6px">Contacts${f.contacts_found ? ` — ${f.contacts_found} found` : ''}</div>
                <div id="enrich-contacts-slot-${f.id}">
                  <div style="font-size:11px;color:#4a4a4a;font-family:'DM Mono',monospace">Loading…</div>
                </div>
              </div>
            </div>
          </div>`;
        }).join('')}
      </div>
      <div style="margin-top:16px"><button onclick="window.closeBatch('${dealId}','${batch.id}')" style="padding:8px 16px;background:#1a1a1a;border:1px solid #d4a847;color:#d4a847;border-radius:6px;cursor:pointer;font-size:13px">Close Batch</button></div>
    </div>
  `;
}

function renderCampaignTrackerState(dealId, batch, firms) {
  const totalFirms = firms.length || 1;
  const totalContacts = firms.reduce((sum, firm) => sum + Number(firm.total_contacts || firm.contacts_found || 0), 0);
  const outreachedFirms = firms.filter(firm => Number(firm.contacted_count || 0) > 0).length;
  const repliedFirms = firms.filter(firm => Number(firm.replied_count || 0) > 0).length;
  const closedFirms = firms.filter(firm => firm.firm_stage === 'closed').length;
  const pct = Math.round((outreachedFirms / totalFirms) * 100);

  return `
    <div style="padding:24px" data-campaign-tracker-deal-id="${dealId}" data-campaign-tracker-batch-id="${batch.id}">
      <div style="margin-bottom:20px">
        <div style="font-size:10px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:4px">Batch ${batch.batch_number}</div>
        <h2 style="font-family:'Playfair Display',serif;font-size:22px;color:#e5e7eb;margin:0 0 4px">Campaign Tracker</h2>
        <div style="color:#6b7280;font-size:13px">Firm-level outreach progress for the current batch. The pipeline continues to track each individual contact.</div>
      </div>
      <div style="background:#0d0d0f;border:1px solid #1a1a1a;border-radius:10px;padding:24px;margin-bottom:20px">
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:16px;margin-bottom:16px">
          <div><div data-campaign-summary="outreachedFirms" style="font-size:28px;color:#d4a847;font-family:'Playfair Display',serif">${outreachedFirms}/${firms.length}</div><div style="font-size:11px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.12em">Firms Reached</div></div>
          <div><div data-campaign-summary="repliedFirms" style="font-size:28px;color:#38bdf8;font-family:'Playfair Display',serif">${repliedFirms}</div><div style="font-size:11px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.12em">Firms Replied</div></div>
          <div><div data-campaign-summary="totalContacts" style="font-size:28px;color:#4ade80;font-family:'Playfair Display',serif">${totalContacts}</div><div style="font-size:11px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.12em">Decision Makers</div></div>
          <div><div data-campaign-summary="closedFirms" style="font-size:28px;color:#f87171;font-family:'Playfair Display',serif">${closedFirms}</div><div style="font-size:11px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.12em">Closed Firms</div></div>
        </div>
        <div style="display:flex;justify-content:space-between;margin-bottom:12px">
          <span data-campaign-summary="outreachLabel" style="color:#e5e7eb;font-size:14px">${outreachedFirms}/${firms.length} firms in outreach</span>
          <span data-campaign-summary="outreachPct" style="color:#6b7280;font-size:12px;font-family:'DM Mono',monospace">${pct}%</span>
        </div>
        <div style="background:#1a1a1a;border-radius:4px;height:6px;overflow:hidden">
          <div data-campaign-summary="outreachBar" style="height:100%;background:linear-gradient(90deg,#38bdf8,#4ade80);width:${pct}%;border-radius:4px;transition:width 0.6s ease"></div>
        </div>
      </div>
      <div style="display:flex;align-items:center;justify-content:flex-end;margin-bottom:14px">
        <div class="view-toggle-bar">
          <button class="view-toggle-btn active" id="campaign-list-toggle-${dealId}" onclick="window.setKanbanView('${dealId}','${batch.id}','list')">List View</button>
          <button class="view-toggle-btn" id="campaign-kanban-toggle-${dealId}" onclick="window.setKanbanView('${dealId}','${batch.id}','kanban')">Kanban View</button>
        </div>
      </div>
      <div id="campaign-list-view-${dealId}">
      <div style="display:flex;flex-direction:column;gap:6px">
        ${firms.map((f, i) => {
          const status = getFirmTrackerStatusDisplay(f);
          const pastInv = Array.isArray(f.past_investments) ? f.past_investments.slice(0, 5) : [];
          return `<div style="border:1px solid #1e1e1e;border-radius:6px;overflow:hidden" data-campaign-firm-id="${f.id}">
            <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:#111;cursor:pointer"
                 onclick="toggleEnrichFirmCard('${f.id}','${dealId}','${batch.id}')"
                 onmouseover="this.style.background='#161616'" onmouseout="this.style.background='#111'">
              <div style="display:flex;align-items:center;gap:10px">
                <span style="color:#4a4a4a;font-size:12px;font-family:'DM Mono',monospace;width:20px">${f.rank || i + 1}</span>
                <div>
                  <div style="color:#e5e7eb;font-size:13px">${esc(f.firm_name || 'Unknown')}</div>
                  <div data-campaign-firm-progress="${f.id}" style="font-size:10px;color:#6b7280;font-family:'DM Mono',monospace">${renderFirmTrackerProgress(f)}</div>
                </div>
              </div>
              <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;justify-content:flex-end">
                <span data-campaign-firm-milestones="${f.id}" style="color:#6b7280;font-size:11px;max-width:320px;text-align:right">${renderFirmTrackerMilestones(f)}</span>
                <span id="enrich-status-${f.id}" style="padding:3px 8px;border-radius:999px;background:${status.bg};color:${status.color};font-size:11px;font-family:'DM Mono',monospace;white-space:nowrap">${status.label}</span>
                <button onclick="event.stopPropagation();window.removeFirmFromBatch('${batch.id}','${f.id}','${dealId}')"
                  style="padding:3px 8px;background:none;border:1px solid #2a2a2a;color:#ef4444;border-radius:4px;cursor:pointer;font-size:11px">Remove</button>
                <span id="enrich-chevron-${f.id}" style="color:#3a3a3a;font-size:14px">▾</span>
              </div>
            </div>
            <div id="enrich-firm-detail-${f.id}" style="display:none;padding:16px;background:#0d0d0f;border-top:1px solid #1a1a1a">
              ${f.justification ? `<div style="margin-bottom:12px">
                <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:6px">Why This Firm</div>
                <p style="color:#9ca3af;font-size:13px;line-height:1.65;margin:0;font-style:italic">${esc(f.justification)}</p>
              </div>` : ''}
              ${f.thesis ? `<div style="margin-bottom:12px">
                <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:6px">Investment Thesis</div>
                <p style="color:#9ca3af;font-size:13px;line-height:1.65;margin:0">${esc(f.thesis.slice(0,400))}${f.thesis.length > 400 ? '…' : ''}</p>
              </div>` : ''}
              ${pastInv.length ? `<div style="margin-bottom:12px">
                <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:6px">Past Investments</div>
                <div style="display:flex;flex-wrap:wrap;gap:6px">${pastInv.map(inv => `<span style="padding:3px 8px;background:#1a1a2a;border:1px solid #2a2a3a;border-radius:4px;font-size:11px;color:#9ca3af;font-family:'DM Mono',monospace">${esc(typeof inv === 'string' ? inv : (inv.company || inv.name || ''))}</span>`).join('')}</div>
              </div>` : ''}
              <div>
                <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.15em;font-family:'DM Mono',monospace;margin-bottom:6px">Contacts${f.total_contacts ? ` — ${f.total_contacts} tracked` : ''}</div>
                <div id="enrich-contacts-slot-${f.id}">
                  <div style="font-size:11px;color:#4a4a4a;font-family:'DM Mono',monospace">Loading…</div>
                </div>
              </div>
            </div>
          </div>`;
        }).join('')}
      </div>
      <div style="margin-top:16px"><button onclick="window.closeBatch('${dealId}','${batch.id}')" style="padding:8px 16px;background:#1a1a1a;border:1px solid #d4a847;color:#d4a847;border-radius:6px;cursor:pointer;font-size:13px">Close Batch</button></div>
      </div>
      <div id="campaign-kanban-view-${dealId}" style="display:none">
        <div style="padding:20px 0;color:#4a4a4a;font-size:12px;font-family:'DM Mono',monospace">Loading Kanban…</div>
      </div>
    </div>
  `;
}

// ─── KANBAN VIEW ──────────────────────────────────────────────────────────────

let kanbanPollTimer = null;

window.setKanbanView = function(dealId, batchId, mode) {
  const listView   = document.getElementById(`campaign-list-view-${dealId}`);
  const kanbanView = document.getElementById(`campaign-kanban-view-${dealId}`);
  const listBtn    = document.getElementById(`campaign-list-toggle-${dealId}`);
  const kanbanBtn  = document.getElementById(`campaign-kanban-toggle-${dealId}`);
  if (!listView || !kanbanView) return;

  if (mode === 'kanban') {
    listView.style.display  = 'none';
    kanbanView.style.display = '';
    listBtn?.classList.remove('active');
    kanbanBtn?.classList.add('active');
    loadKanbanView(dealId, batchId, kanbanView);
    if (kanbanPollTimer) clearInterval(kanbanPollTimer);
    kanbanPollTimer = setInterval(() => {
      const kv = document.getElementById(`campaign-kanban-view-${dealId}`);
      if (kv && kv.style.display !== 'none') loadKanbanView(dealId, batchId, kv, true);
    }, 30000);
  } else {
    listView.style.display   = '';
    kanbanView.style.display = 'none';
    listBtn?.classList.add('active');
    kanbanBtn?.classList.remove('active');
    if (kanbanPollTimer) { clearInterval(kanbanPollTimer); kanbanPollTimer = null; }
  }
};

async function loadKanbanView(dealId, batchId, container, silent = false) {
  if (!silent) {
    container.innerHTML = '<div style="padding:20px 0;color:#4a4a4a;font-size:12px;font-family:\'DM Mono\',monospace">Loading Kanban…</div>';
  }
  try {
    const data = await api(`/api/deals/${dealId}/kanban`);
    container.innerHTML = renderKanbanBoard(data, dealId, batchId);
  } catch (e) {
    if (!silent) container.innerHTML = `<div style="color:var(--red);padding:20px 0;font-size:12px">${esc(e.message)}</div>`;
  }
}

function renderKanbanBoard(data, dealId, batchId) {
  const cols = data.columns || {};
  const COLS = [
    { key: 'queued',         label: 'Queued' },
    { key: 'contacted',      label: 'Contacted' },
    { key: 'connected',      label: 'Connected' },
    { key: 'engaged',        label: 'Engaged' },
    { key: 'meeting_booked', label: 'Meeting Booked' },
    { key: 'passed',         label: 'Passed / Declined', dim: true,
      tooltip: 'Firm declined the opportunity, said they\'re not the right fit, or was manually marked as not pursuing' },
    { key: 'exhausted',      label: 'Exhausted', dim: true },
  ];
  const boardId = `kanban-board-${dealId}`;
  return `<div class="kanban-wrap">
    <div class="kanban-search-bar">
      <span style="color:var(--text-dim);font-size:13px">⌕</span>
      <input type="text" placeholder="Search firms…" oninput="window.filterKanban('${boardId}',this.value,this.nextElementSibling)"
             autocomplete="off" spellcheck="false">
      <button class="kanban-search-clear" onclick="window.clearKanbanSearch('${boardId}',this)">×</button>
    </div>
    <div class="kanban-board" id="${boardId}">
      ${COLS.map(col => {
        const firms = cols[col.key] || [];
        return `<div class="kanban-col">
          <div class="kanban-col-header">
            <span class="kanban-col-title${col.dim ? ' kanban-col-title-dim' : ''}">${col.label}</span>
            ${col.tooltip ? `<span class="kanban-col-info-icon">ⓘ<span class="kanban-tooltip">${esc(col.tooltip)}</span></span>` : ''}
            <span class="kanban-col-count">${firms.length}</span>
          </div>
          <div class="kanban-col-cards">
            <div class="kanban-empty" style="${firms.length > 0 ? 'display:none' : ''}">—</div>
            ${firms.map(firm => `
              <div class="kanban-firm-card${col.dim ? ' kanban-firm-card-dim' : ''}"
                   data-firm-name="${esc((firm.firm_name || '').toLowerCase())}"
                   data-firm-id="${esc(firm.id)}"
                   data-score="${esc(String(firm.score || 0))}"
                   data-deal-id="${esc(dealId)}"
                   data-batch-id="${esc(batchId)}"
                   data-stage-label="${esc(firm.firm_stage_label || firm.firm_stage || '')}"
                   onclick="window.onKanbanFirmClick(this)">
                <div class="kanban-firm-name" title="${esc(firm.firm_name)}">${esc(firm.firm_name)}</div>
                ${firm.top_contact
                  ? `<div class="kanban-firm-contact">${esc(firm.top_contact.name || '—')}${firm.top_contact.title ? ` <span style="opacity:0.5">·</span> ${esc(firm.top_contact.title)}` : ''}</div>`
                  : `<div class="kanban-firm-contact">${firm.total_contacts || 0} contact${firm.total_contacts !== 1 ? 's' : ''}</div>`}
                ${firm.score ? `<div class="kanban-firm-footer"><span class="kanban-score-badge">${firm.score}</span></div>` : ''}
              </div>`).join('')}
          </div>
        </div>`;
      }).join('')}
    </div>
  </div>`;
}

// Reads firm data from data-* attributes to avoid quoting issues in onclick HTML
window.onKanbanFirmClick = function(card) {
  const firmId     = card.dataset.firmId;
  const firmName   = card.querySelector('.kanban-firm-name')?.getAttribute('title') || card.dataset.firmName || '';
  const score      = Number(card.dataset.score || 0);
  const dealId     = card.dataset.dealId;
  const batchId    = card.dataset.batchId;
  const stageLabel = card.dataset.stageLabel || '';
  window.openFirmKanbanDrillDown(firmId, firmName, score, dealId, batchId, stageLabel);
};

window.filterKanban = function(boardId, val, clearBtn) {
  const board = document.getElementById(boardId);
  if (!board) return;
  const term = (val || '').toLowerCase().trim();
  if (clearBtn) clearBtn.style.display = term ? 'block' : 'none';
  board.querySelectorAll('.kanban-col-cards').forEach(colCards => {
    const cards = colCards.querySelectorAll('.kanban-firm-card');
    let anyVisible = false;
    cards.forEach(card => {
      const name = card.dataset.firmName || '';
      const show = !term || name.includes(term);
      card.style.display = show ? '' : 'none';
      if (show) anyVisible = true;
    });
    const empty = colCards.querySelector('.kanban-empty');
    if (empty) empty.style.display = anyVisible ? 'none' : '';
  });
};

window.clearKanbanSearch = function(boardId, clearBtn) {
  const board = document.getElementById(boardId);
  if (!board) return;
  // Find the input — it's in the parent .kanban-wrap before the board
  const wrap = board.parentElement;
  const input = wrap?.querySelector('input');
  if (input) { input.value = ''; }
  if (clearBtn) clearBtn.style.display = 'none';
  board.querySelectorAll('.kanban-firm-card').forEach(c => { c.style.display = ''; });
  board.querySelectorAll('.kanban-col-cards').forEach(colCards => {
    const empty = colCards.querySelector('.kanban-empty');
    if (empty) {
      const hasCards = colCards.querySelectorAll('.kanban-firm-card').length > 0;
      empty.style.display = hasCards ? 'none' : '';
    }
  });
};

window.openFirmKanbanDrillDown = async function(firmId, firmName, score, dealId, batchId, firmStageLabel) {
  window.closeKanbanDrillDown();

  const overlay = document.createElement('div');
  overlay.className = 'kanban-drill-overlay';
  overlay.id = 'kanban-drill-overlay';
  overlay.addEventListener('click', window.closeKanbanDrillDown);

  const stageLabel = (firmStageLabel || '').replace(/_/g, ' ').toUpperCase();

  const panel = document.createElement('div');
  panel.className = 'kanban-drill-panel';
  panel.id = 'kanban-drill-panel';
  panel.innerHTML = `
    <div class="kanban-drill-header">
      <div style="min-width:0;flex:1">
        <div class="kanban-drill-firm-name">${esc(firmName)}</div>
        <div style="display:flex;align-items:center;gap:8px;margin-top:6px;flex-wrap:wrap">
          ${stageLabel ? `<span style="font-family:'DM Mono',monospace;font-size:9px;letter-spacing:0.12em;text-transform:uppercase;padding:2px 7px;border-radius:3px;background:rgba(201,168,76,0.1);color:var(--gold);border:1px solid rgba(201,168,76,0.2)">${esc(stageLabel)}</span>` : ''}
          ${score ? `<span style="font-family:'DM Mono',monospace;font-size:11px;color:var(--gold)">Score ${score}</span>` : ''}
        </div>
      </div>
      <button class="kanban-drill-close" onclick="window.closeKanbanDrillDown()">&#215;</button>
    </div>
    <div class="kanban-drill-body" id="kanban-drill-body">
      <div style="padding:20px;color:var(--text-dim);font-size:12px;font-family:'DM Mono',monospace">Loading contacts…</div>
    </div>`;

  document.body.appendChild(overlay);
  document.body.appendChild(panel);

  try {
    const contacts = await api(`/api/deals/${dealId}/batch/${batchId}/firms/${firmId}/contacts`);
    const body = document.getElementById('kanban-drill-body');
    if (body) body.innerHTML = renderFirmContactPanel(contacts);
  } catch (e) {
    const body = document.getElementById('kanban-drill-body');
    if (body) body.innerHTML = `<div style="padding:20px;color:var(--red);font-size:12px">${esc(e.message)}</div>`;
  }
};

window.closeKanbanDrillDown = function() {
  document.getElementById('kanban-drill-overlay')?.remove();
  document.getElementById('kanban-drill-panel')?.remove();
};

function getContactKanbanColumn(contact) {
  const ps = contact.pipeline_stage;
  if (['Meeting Booked', 'Replied', 'In Conversation'].includes(ps)) return 'replied';
  if (['Archived', 'Deleted — Do Not Contact', 'Suppressed — Opt Out', 'Inactive'].includes(ps)) return 'skipped';
  if (contact.invite_accepted_at || ps === 'invite_accepted') return 'connected';
  if (['dm_sent', 'DM Sent', 'DM Approved'].includes(ps)) return 'dm_sent';
  if (contact.last_email_sent_at || ['email_sent', 'Email Sent', 'Email Approved'].includes(ps)) return 'email_sent';
  if (contact.invite_sent_at || ps === 'invite_sent') return 'request_sent';
  return 'queued';
}

// Returns the URL only if it's a real LinkedIn profile URL (not a raw provider ID)
function safeLinkedInUrl(url) {
  if (!url) return null;
  if (!url.startsWith('http')) return null;
  if (!url.includes('linkedin.com')) return null;
  return url;
}

function timeAgo(dateStr) {
  if (!dateStr) return null;
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 2) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  return `${Math.floor(days / 30)}mo ago`;
}

function getContactLastAction(contact) {
  return contact.last_reply_at || contact.invite_accepted_at || contact.last_outreach_at || contact.last_email_sent_at || contact.invite_sent_at || null;
}

function contactStagePillClass(ps) {
  if (['Meeting Booked', 'Replied', 'In Conversation'].includes(ps)) return 'kanban-stage-pill-replied';
  if (['Inactive', 'Archived', 'Deleted — Do Not Contact', 'Suppressed — Opt Out'].includes(ps)) return 'kanban-stage-pill-neutral';
  if (ps) return 'kanban-stage-pill-active';
  return 'kanban-stage-pill-neutral';
}

function contactStagePillLabel(contact) {
  const ps = contact.pipeline_stage;
  if (!ps || ps === 'Approved for Outreach') return 'Queued';
  return ps.replace(/_/g, ' ');
}

function renderFirmContactPanel(contacts) {
  if (!contacts || !contacts.length) {
    return '<div style="padding:24px 20px;color:var(--text-dim);font-size:12px;text-align:center;font-style:italic">No contacts found for this firm yet</div>';
  }

  // Sort: active first, then by last action desc
  const sorted = [...contacts].sort((a, b) => {
    const aInactive = ['Inactive','Archived'].includes(a.pipeline_stage) ? 1 : 0;
    const bInactive = ['Inactive','Archived'].includes(b.pipeline_stage) ? 1 : 0;
    if (aInactive !== bInactive) return aInactive - bInactive;
    const aDate = new Date(getContactLastAction(a) || 0).getTime();
    const bDate = new Date(getContactLastAction(b) || 0).getTime();
    return bDate - aDate;
  });

  const rows = sorted.map(c => {
    const lastAction = getContactLastAction(c);
    const ago = timeAgo(lastAction);
    const liUrl = safeLinkedInUrl(c.linkedin_url);
    const hasEmail    = !!(c.email || c.last_email_sent_at);
    const isSkipped   = !liUrl && !hasEmail && !c.last_outreach_at;
    const liSvg = `<svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor" style="color:#0a66c2"><path d="M20.45 20.45h-3.56v-5.57c0-1.33-.03-3.04-1.85-3.04-1.86 0-2.14 1.45-2.14 2.95v5.66H9.34V9h3.41v1.56h.05c.48-.9 1.64-1.85 3.37-1.85 3.6 0 4.27 2.37 4.27 5.45v6.29zM5.34 7.43a2.07 2.07 0 1 1 0-4.14 2.07 2.07 0 0 1 0 4.14zm1.78 13.02H3.56V9h3.56v11.45zM22.23 0H1.77C.79 0 0 .77 0 1.72v20.56C0 23.23.79 24 1.77 24h20.46C23.21 24 24 23.23 24 22.28V1.72C24 .77 23.21 0 22.23 0z"/></svg>`;
    // Only show the LinkedIn icon when there's an actual URL to click — never show it unlinked
    const chIcon = liUrl
      ? `<a href="${esc(liUrl)}" target="_blank" rel="noopener" style="display:flex;align-items:center;color:inherit" title="Open LinkedIn profile">${liSvg}</a>`
      : hasEmail ? '✉' : '—';
    const pillClass = isSkipped ? 'kanban-stage-pill-neutral' : contactStagePillClass(c.pipeline_stage);
    const pillLabel = isSkipped ? 'Skipped' : contactStagePillLabel(c);

    return `<div class="kanban-contact-row">
      <div class="kanban-contact-ch-icon">${chIcon}</div>
      <div class="kanban-contact-main">
        <div class="kanban-contact-name">${liUrl
          ? `<a href="${esc(liUrl)}" target="_blank" style="color:var(--text-bright);text-decoration:none">${esc(c.name || '—')}</a>`
          : esc(c.name || '—')}</div>
        ${c.job_title ? `<div class="kanban-contact-role">${esc(c.job_title)}</div>` : ''}
        ${c.email ? `<div style="font-size:10px;color:var(--text-dim);margin-top:2px">${esc(c.email)}</div>` : ''}
        ${isSkipped ? `<div style="font-size:10px;color:var(--text-dim);margin-top:2px;font-style:italic">No email or LinkedIn found</div>` : ''}
      </div>
      <div class="kanban-contact-meta">
        <span class="kanban-stage-pill ${pillClass}">${esc(pillLabel)}</span>
        ${ago ? `<span class="kanban-contact-ts">${ago}</span>` : ''}
      </div>
    </div>`;
  }).join('');

  return `<div class="kanban-contact-list">
    <div class="kanban-contact-list-label">${contacts.length} contact${contacts.length !== 1 ? 's' : ''}</div>
    ${rows}
  </div>`;
}

async function patchCampaignTrackerInPlace(dealId) {
  const container = document.querySelector(`#deal-tab-rankings [data-campaign-tracker-deal-id="${dealId}"]`);
  if (!container) return;
  const batchId = container.getAttribute('data-campaign-tracker-batch-id');
  if (!batchId) return;

  let firms = [];
  try {
    firms = await api(`/api/deals/${dealId}/batch/${batchId}/firms`);
  } catch {
    return;
  }

  const totalFirms = firms.length || 1;
  const totalContacts = firms.reduce((sum, firm) => sum + Number(firm.total_contacts || firm.contacts_found || 0), 0);
  const outreachedFirms = firms.filter(firm => Number(firm.contacted_count || 0) > 0).length;
  const repliedFirms = firms.filter(firm => Number(firm.replied_count || 0) > 0).length;
  const closedFirms = firms.filter(firm => firm.firm_stage === 'closed').length;
  const pct = Math.round((outreachedFirms / totalFirms) * 100);

  const summaryValues = {
    outreachedFirms: `${outreachedFirms}/${firms.length}`,
    repliedFirms: String(repliedFirms),
    totalContacts: String(totalContacts),
    closedFirms: String(closedFirms),
    outreachLabel: `${outreachedFirms}/${firms.length} firms in outreach`,
    outreachPct: `${pct}%`,
  };
  Object.entries(summaryValues).forEach(([key, value]) => {
    const node = container.querySelector(`[data-campaign-summary="${key}"]`);
    if (node) node.textContent = value;
  });
  const bar = container.querySelector('[data-campaign-summary="outreachBar"]');
  if (bar) bar.style.width = `${pct}%`;

  firms.forEach(firm => {
    const progress = container.querySelector(`[data-campaign-firm-progress="${firm.id}"]`);
    if (progress) progress.textContent = renderFirmTrackerProgress(firm);
    const milestones = container.querySelector(`[data-campaign-firm-milestones="${firm.id}"]`);
    if (milestones) milestones.textContent = renderFirmTrackerMilestones(firm);
    const badge = document.getElementById(`enrich-status-${firm.id}`);
    if (badge) {
      const status = getFirmTrackerStatusDisplay(firm);
      badge.textContent = status.label;
      badge.style.color = status.color;
      badge.style.background = status.bg;
    }
  });
}

function getContactCampaignStatus(contact) {
  if (contact.pipeline_stage === 'Meeting Booked') return { label: 'Meeting booked', color: '#22c55e' };
  if (['Archived', 'Deleted — Do Not Contact', 'Suppressed — Opt Out', 'Inactive'].includes(contact.pipeline_stage)) {
    return { label: 'Closed', color: '#f87171' };
  }
  if (contact.response_received || contact.last_reply_at || ['Replied', 'In Conversation'].includes(contact.pipeline_stage)) {
    return { label: 'Replied', color: '#38bdf8' };
  }
  if (contact.invite_accepted_at || ['invite_accepted', 'dm_sent'].includes(contact.pipeline_stage)) {
    return { label: 'Connection accepted', color: '#a78bfa' };
  }
  if (contact.invite_sent_at || contact.last_email_sent_at || contact.last_outreach_at || ['invite_sent', 'email_sent'].includes(contact.pipeline_stage)) {
    return { label: 'Outreach sent', color: '#f59e0b' };
  }
  return { label: 'Ready', color: '#4ade80' };
}

function renderBatchFirmContacts(contacts) {
  if (!contacts.length) {
    return '<div style="font-size:11px;color:#374151;font-family:\'DM Mono\',monospace">No contacts found yet</div>';
  }
  return contacts.map(contact => {
    const status = getContactCampaignStatus(contact);
    return `
      <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 10px;background:#111;border-radius:4px;margin-bottom:4px;gap:12px">
        <div>
          <div style="font-size:13px;color:#e5e7eb">${contact.linkedin_url ? `<a href="${esc(contact.linkedin_url)}" target="_blank" style="color:#e5e7eb;text-decoration:none">${esc(contact.name || '—')}</a>` : esc(contact.name || '—')}</div>
          <div style="font-size:11px;color:#6b7280;margin-top:2px">${esc(contact.job_title || '—')}</div>
        </div>
        <div style="text-align:right">
          ${contact.email ? `<div style="font-size:11px;color:#4ade80;font-family:'DM Mono',monospace">${esc(contact.email)}</div>` : '<div style="font-size:11px;color:#374151">No email yet</div>'}
          <div style="font-size:10px;color:${status.color};margin-top:2px">${status.label}</div>
        </div>
      </div>
    `;
  }).join('');
}

window.toggleEnrichFirmCard = async function(firmId, dealId, batchId) {
  const detail = document.getElementById(`enrich-firm-detail-${firmId}`);
  const chevron = document.getElementById(`enrich-chevron-${firmId}`);
  if (!detail) return;
  const isOpen = detail.style.display !== 'none';
  detail.style.display = isOpen ? 'none' : 'block';
  if (chevron) chevron.style.transform = isOpen ? 'rotate(0deg)' : 'rotate(180deg)';
  if (!isOpen) {
    const slot = document.getElementById(`enrich-contacts-slot-${firmId}`);
    if (slot && !slot.dataset.loaded) {
      slot.dataset.loaded = '1';
      try {
        const contacts = await api(`/api/deals/${dealId}/batch/${batchId}/firms/${firmId}/contacts`);
        slot.innerHTML = renderBatchFirmContacts(contacts);
      } catch { slot.innerHTML = '<div style="font-size:11px;color:#ef4444">Failed to load</div>'; }
    }
  }
};

window.toggleFirmCard = async function(firmId, dealId) {
  const detail = document.getElementById(`firm-detail-${firmId}`);
  const chevron = document.getElementById(`chevron-${firmId}`);
  const card = document.getElementById(`firm-card-${firmId}`);
  if (!detail) return;
  const isOpen = detail.style.display !== 'none';
  detail.style.display = isOpen ? 'none' : 'block';
  chevron.style.transform = isOpen ? 'rotate(0deg)' : 'rotate(180deg)';
  if (card) card.style.borderColor = isOpen ? '#1e1e1e' : '#d4a847';

  // Load contacts when opening, only once
  if (!isOpen && dealId && !detail.dataset.contactsLoaded) {
    const contactSlot = detail.querySelector('[data-contacts-slot]');
    if (!contactSlot) return;
    contactSlot.innerHTML = '<div style="font-size:11px;color:#4a4a4a;font-family:\'DM Mono\',monospace">Loading contacts…</div>';
    try {
      // Resolve batchId from the nearest data attribute or the current active batch
      const batchId = detail.dataset.batchId;
      if (!batchId) { contactSlot.innerHTML = ''; return; }
      const contacts = await api(`/api/deals/${dealId}/batch/${batchId}/firms/${firmId}/contacts`);
      contactSlot.innerHTML = renderBatchFirmContacts(contacts);
      detail.dataset.contactsLoaded = '1';
    } catch (e) {
      contactSlot.innerHTML = `<div style="font-size:11px;color:#ef4444">Failed to load contacts</div>`;
    }
  }
};

window.approveCampaignBatch = async function(batchId, dealId) {
  try {
    await api(`/api/deals/${dealId}/batch/${batchId}/approve`, 'POST');
    showToast('Campaign approved. Contact enrichment starting.', 'success');
    await loadCampaignReviewTab(dealId);
    await loadDeals();
  } catch (e) {
    showToast('Failed to approve: ' + e.message, 'error');
  }
};

window.closeBatch = async function(dealId, batchId) {
  if (!confirm('Close this batch? The next batch will begin on the next orchestrator cycle.')) return;
  try {
    await api(`/api/deals/${dealId}/batch/${batchId}/close`, 'POST');
    showToast('Batch closed.', 'success');
    await loadCampaignReviewTab(dealId);
    await loadDeals();
  } catch (e) {
    showToast('Failed: ' + e.message, 'error');
  }
};

window.rejectCampaignBatch = async function(batchId, dealId) {
  if (!confirm('Reject this campaign batch? The current firms will be skipped and the next batch will be rebuilt on the next cycle.')) return;
  try {
    await api(`/api/deals/${dealId}/batch/${batchId}/skip`, 'POST', { reason: 'Rejected during campaign review' });
    showToast('Campaign rejected.', 'success');
    await loadCampaignReviewTab(dealId);
    await loadDeals();
  } catch (e) {
    showToast('Reject failed: ' + e.message, 'error');
  }
};

window.removeFirmFromBatch = async function(batchId, firmId, dealId) {
  if (!confirm('Remove this firm from the campaign batch?')) return;
  try {
    await api(`/api/deals/${dealId}/batch/${batchId}/firms/${firmId}`, 'DELETE');
    showToast('Firm removed.', 'success');
    await loadCampaignReviewTab(dealId);
  } catch (e) {
    showToast('Failed: ' + e.message, 'error');
  }
};

window.showAddFirmModal = function(batchId, dealId) {
  const existing = document.getElementById('add-firm-modal');
  if (existing) existing.remove();

  const modal = document.createElement('div');
  modal.id = 'add-firm-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.8);z-index:2000;display:flex;align-items:center;justify-content:center';
  modal.innerHTML = `
    <div style="background:#111;border:1px solid #2a2a2a;border-radius:8px;padding:28px;width:520px;max-height:80vh;display:flex;flex-direction:column">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
        <h3 style="color:#e5e7eb;margin:0;font-family:'Cormorant Garamond',serif;font-size:20px">Add Firm to Campaign</h3>
        <button onclick="document.getElementById('add-firm-modal').remove()" style="background:none;border:none;color:#6b7280;cursor:pointer;font-size:24px;line-height:1">&#215;</button>
      </div>
      <input id="afm-search" type="text" placeholder="Search investor database by firm name…"
        style="width:100%;padding:10px 12px;background:#1a1a1a;border:1px solid #2a2a2a;color:#e5e7eb;border-radius:6px;box-sizing:border-box;font-size:13px;margin-bottom:12px"
        oninput="searchFirmsForBatch(this.value)" autocomplete="off" />
      <div id="afm-results" style="flex:1;overflow-y:auto;min-height:80px;max-height:320px">
        <div style="color:#6b7280;font-size:13px">Type to search the investor database…</div>
      </div>
      <div style="margin-top:16px;padding-top:14px;border-top:1px solid #2a2a2a">
        <div style="font-size:11px;color:#6b7280;margin-bottom:8px">Not in the database? Create a new entry:</div>
        <div style="display:flex;gap:8px">
          <input id="afm-new-name" type="text" placeholder="Firm name"
            style="flex:1;padding:8px 12px;background:#1a1a1a;border:1px solid #2a2a2a;color:#e5e7eb;border-radius:6px;font-size:13px" />
          <button onclick="addNewFirmToBatch('${batchId}','${dealId}')"
            style="padding:8px 16px;background:#1e293b;border:1px solid #2a2a3a;color:#818cf8;border-radius:6px;cursor:pointer;font-size:13px;white-space:nowrap">
            Create &amp; Add
          </button>
        </div>
      </div>
    </div>`;
  document.body.appendChild(modal);
  modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
  setTimeout(() => document.getElementById('afm-search')?.focus(), 50);
};

let _afmSearchTimer = null;
window.searchFirmsForBatch = function(q) {
  clearTimeout(_afmSearchTimer);
  const el = document.getElementById('afm-results');
  if (!el) return;
  if (!q || q.length < 2) {
    el.innerHTML = '<div style="color:#6b7280;font-size:13px">Type to search the investor database…</div>';
    return;
  }
  el.innerHTML = '<div style="color:#6b7280;font-size:13px">Searching…</div>';
  _afmSearchTimer = setTimeout(async () => {
    try {
      const data = await api(`/api/investors-db/search?search=${encodeURIComponent(q)}&limit=12`);
      const results = data?.investors || [];
      if (!results.length) {
        el.innerHTML = '<div style="color:#6b7280;font-size:13px">No matches found — use "Create & Add" below.</div>';
        return;
      }
      el.innerHTML = results.map(r => `
        <div onclick="selectFirmForBatch('${r.id}','${esc(r.name || '')}','${document.getElementById('add-firm-modal')?.dataset?.batchId || ''}')"
          style="padding:10px 12px;border-radius:6px;cursor:pointer;border:1px solid transparent;margin-bottom:4px;transition:all .15s"
          onmouseover="this.style.background='#1a1a2a';this.style.borderColor='#2a2a3a'"
          onmouseout="this.style.background='';this.style.borderColor='transparent'">
          <div style="font-size:13px;font-weight:600;color:#e5e7eb">${esc(r.name || '—')}</div>
          <div style="font-size:11px;color:#6b7280;margin-top:2px">
            ${[r.investor_type, r.hq_country, r.preferred_industries].filter(Boolean).slice(0,3).map(v => esc(String(v))).join(' · ') || 'No details'}
          </div>
        </div>`).join('');
      // Store batchId on modal for the onclick
      const modal = document.getElementById('add-firm-modal');
      if (modal) modal.dataset.batchId = modal.dataset.batchId || '';
    } catch {
      el.innerHTML = '<div style="color:#ef4444;font-size:13px">Search failed — try again.</div>';
    }
  }, 300);
};

window.selectFirmForBatch = async function(investorsDbId, firmName, batchId) {
  // batchId comes from the modal's data attribute — re-read it from DOM
  const modal = document.getElementById('add-firm-modal');
  if (!modal) return;
  // Extract batchId and dealId from the modal's "Create & Add" button onclick
  const createBtn = modal.querySelector('button[onclick*="addNewFirmToBatch"]');
  const match = createBtn?.getAttribute('onclick')?.match(/addNewFirmToBatch\('([^']+)','([^']+)'\)/);
  if (!match) return;
  const [, bId, dealId] = match;
  await _addFirmToBatch(bId, dealId, firmName, investorsDbId);
};

window.addNewFirmToBatch = async function(batchId, dealId) {
  const name = document.getElementById('afm-new-name')?.value?.trim();
  if (!name) { showToast('Enter a firm name first', 'error'); return; }
  await _addFirmToBatch(batchId, dealId, name, null);
};

async function _addFirmToBatch(batchId, dealId, firmName, investorsDbId) {
  try {
    const result = await api(`/api/deals/${dealId}/campaign/${batchId}/firms`, 'POST', {
      firm_name: firmName, investors_db_id: investorsDbId || undefined,
    });
    document.getElementById('add-firm-modal')?.remove();
    if (result.researched) {
      showToast(`${firmName} added to campaign.`, 'success');
    } else {
      showToast(`${firmName} added — Roco will research them before outreach fires.`, 'success');
    }
    await loadCampaignReviewTab(dealId);
  } catch (e) {
    showToast('Failed: ' + e.message, 'error');
  }
}

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

  // Load email accounts for the switcher
  let emailAccountsHtml = '<option value="">Loading&#8230;</option>';
  try {
    const accounts = await api('/api/email-accounts');
    emailAccountsHtml = '<option value="">No sending account</option>' +
      (accounts || []).map(a =>
        `<option value="${esc(a.connection_id)}" data-email="${esc(a.email)}" ${deal.sending_account_id === a.connection_id ? 'selected' : ''}>${esc(a.label || a.email)}</option>`
      ).join('');
  } catch {}

  const priorityLists = deal.pitchbook?.priority_lists || [];
  const kbList        = deal.pitchbook?.kb_list || null;

  // Load available lists for dropdowns (async, populated after render)
  let availableInvestorLists = [];
  let availableKBLists = [];
  try {
    [availableInvestorLists, availableKBLists] = await Promise.all([
      api('/api/investor-lists?type=investors'),
      api('/api/investor-lists?type=knowledge_base'),
    ]);
  } catch {}

  const attachedListIds = new Set(priorityLists.map(l => String(l.list_id)));
  const priorityListOpts = (availableInvestorLists || [])
    .filter(l => !attachedListIds.has(String(l.id)))
    .map(l => `<option value="${esc(l.id)}" data-name="${esc(l.name)}">${esc(l.name)} (${(l.investor_count||0).toLocaleString()})</option>`)
    .join('');
  const kbOpts = (availableKBLists || [])
    .map(l => `<option value="${esc(l.id)}" data-name="${esc(l.name)}" ${kbList?.id === l.id ? 'selected' : ''}>${esc(l.name)} (${(l.investor_count||0).toLocaleString()})</option>`)
    .join('');

  el.innerHTML = `
    <div style="max-width:720px">

      <h3 style="font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin:0 0 8px">Sending Account</h3>
      <div style="display:flex;gap:8px;align-items:center;margin-bottom:24px">
        <select class="form-input" id="ds-email-account-${id}" style="flex:1">
          ${emailAccountsHtml}
        </select>
        <button class="btn btn-gold" id="ds-email-account-btn-${id}" onclick="window.saveDealEmailAccount('${id}')" style="white-space:nowrap">Save Account</button>
      </div>

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
          <label class="form-label">Target Investor Geography</label>
          <select class="form-input" id="ds-target-geography">
            ${['Global','US','UK','US,UK','UAE','Europe','North America'].map(opt => `
              <option value="${opt}" ${String(deal.target_geography || 'Global') === opt ? 'selected' : ''}>${opt}</option>
            `).join('')}
          </select>
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

      <h3 style="font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin:0 0 4px">Email Sending Window</h3>
      <div style="font-size:11px;color:var(--text-dim);margin-bottom:12px">Email sends during this daytime window on the deal's selected active days.</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:8px">
        <div class="form-group">
          <label class="form-label">Email: from</label>
          <input type="time" class="form-input" id="ds-send-from" value="${esc(deal.send_from || '06:00')}" />
        </div>
        <div class="form-group">
          <label class="form-label">Email: until</label>
          <input type="time" class="form-input" id="ds-send-until" value="${esc(deal.send_until || '18:00')}" />
        </div>
      </div>

      <h3 style="font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin:0 0 4px;margin-top:8px">LinkedIn DM Window</h3>
      <div style="font-size:11px;color:var(--text-dim);margin-bottom:12px">LinkedIn DMs send in this separate window on the deal's selected active days.</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:8px">
        <div class="form-group">
          <label class="form-label">LinkedIn DM: from</label>
          <input type="time" class="form-input" id="ds-li-dm-from" value="${esc(deal.li_dm_from || '20:00')}" />
        </div>
        <div class="form-group">
          <label class="form-label">LinkedIn DM: until</label>
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
        <div class="form-group" style="grid-column:1/-1">
          <label style="display:flex;align-items:center;gap:10px;cursor:pointer">
            <input type="checkbox" id="ds-no-followups" ${deal.no_follow_ups ? 'checked' : ''}
              style="width:16px;height:16px;accent-color:#C9A84C;cursor:pointer" />
            <span class="form-label" style="margin:0">No follow-ups — intro only on each channel</span>
          </label>
          <div style="font-size:11px;color:#6b7280;margin-top:4px;padding-left:26px;font-family:'DM Mono',monospace">
            LinkedIn DM sent &rarr; ${deal.followup_days_li || 3}d no response &rarr; email intro &rarr; ${deal.followup_days_email || 7}d no response &rarr; next person
          </div>
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

    <div style="margin-top:32px;padding-top:24px;border-top:1px solid var(--border)">

      <!-- Priority Lists -->
      <div style="margin-bottom:24px">
        <h3 style="font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin:0 0 4px">Priority Lists</h3>
        <p style="font-size:12px;color:var(--text-dim);margin:0 0 12px">Investor lists Roco will search first when identifying candidates. Scored in priority order.</p>
        <div id="deal-priority-list-rows-${id}" style="margin-bottom:10px">
          ${priorityLists.length ? priorityLists.map((l, i) => `
            <div style="display:flex;align-items:center;justify-content:space-between;padding:8px 12px;background:var(--bg-secondary);border:1px solid var(--border);border-radius:7px;margin-bottom:6px">
              <div>
                <span style="font-size:13px;color:var(--text-bright)">${esc(l.list_name || l.list_id)}</span>
                <span style="font-size:11px;color:var(--text-muted);margin-left:8px">Priority ${i + 1}</span>
              </div>
              <button onclick="removeDealPriorityList('${id}','${esc(l.list_id)}')" style="background:transparent;border:none;color:#6b7280;cursor:pointer;font-size:18px;line-height:1;padding:0 4px" title="Remove">&times;</button>
            </div>
          `).join('') : '<div style="font-size:12px;color:var(--text-dim);padding:8px 0">No priority lists attached — Roco will query the full investor database.</div>'}
        </div>
        ${priorityListOpts ? `
        <div style="display:flex;gap:8px;align-items:center">
          <select id="add-priority-list-select-${id}" style="flex:1;padding:7px 10px;background:var(--surface-2);border:1px solid var(--border);color:var(--text-bright);border-radius:6px;font-size:12px">
            <option value="">Select a list to add…</option>
            ${priorityListOpts}
          </select>
          <button onclick="addDealPriorityList('${id}')" class="btn btn-ghost btn-sm" style="white-space:nowrap">+ Add List</button>
        </div>` : '<div style="font-size:12px;color:var(--text-dim)">All available investor lists are already attached.</div>'}
      </div>

      <!-- Knowledge Base -->
      <div>
        <h3 style="font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin:0 0 4px">Knowledge Base</h3>
        <p style="font-size:12px;color:var(--text-dim);margin:0 0 12px">Enrichment source — fills data gaps in investor profiles before scoring. Upload KB lists in the Database section.</p>
        <div style="padding:10px 12px;background:var(--bg-secondary);border:1px solid var(--border);border-radius:7px;margin-bottom:10px">
          ${kbList
            ? `<div style="display:flex;align-items:center;justify-content:space-between">
                 <div>
                   <span style="font-size:13px;color:var(--text-bright)">${esc(kbList.name)}</span>
                   <span style="font-size:11px;color:var(--text-muted);margin-left:8px">${(kbList.investor_count||0).toLocaleString()} records</span>
                 </div>
                 <button onclick="removeDealKB('${id}')" style="background:transparent;border:none;color:#6b7280;cursor:pointer;font-size:18px;line-height:1;padding:0 4px" title="Remove">&times;</button>
               </div>`
            : '<div style="font-size:12px;color:var(--text-dim)">No knowledge base attached — gap-fill research will use Gemini/Grok for sparse profiles.</div>'}
        </div>
        ${kbOpts ? `
        <div style="display:flex;gap:8px;align-items:center">
          <select id="change-kb-select-${id}" style="flex:1;padding:7px 10px;background:var(--surface-2);border:1px solid var(--border);color:var(--text-bright);border-radius:6px;font-size:12px">
            <option value="">Select a knowledge base…</option>
            ${kbOpts}
          </select>
          <button onclick="setDealKB('${id}')" class="btn btn-ghost btn-sm" style="white-space:nowrap">${kbList ? 'Change KB' : '+ Attach KB'}</button>
        </div>` : '<div style="font-size:12px;color:var(--text-dim)">No knowledge base lists in database yet. Upload one in the Database section.</div>'}
      </div>

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

async function addDealPriorityList(dealId) {
  const sel = document.getElementById(`add-priority-list-select-${dealId}`);
  if (!sel?.value) return showToast('Select a list first', 'error');
  const listName = sel.options[sel.selectedIndex]?.dataset?.name || sel.options[sel.selectedIndex]?.textContent?.split(' (')[0] || sel.value;
  try {
    await api(`/api/deals/${dealId}/priority-lists`, 'POST', { list_id: sel.value, list_name: listName });
    showToast(`"${listName}" added as priority list`);
    await loadDealTabSettings(dealId);
  } catch (e) { showToast(e.message, 'error'); }
}

async function removeDealPriorityList(dealId, listId) {
  try {
    await api(`/api/deals/${dealId}/priority-lists/${listId}`, 'DELETE');
    await loadDealTabSettings(dealId);
  } catch (e) { showToast(e.message, 'error'); }
}

async function setDealKB(dealId) {
  const sel = document.getElementById(`change-kb-select-${dealId}`);
  if (!sel?.value) return showToast('Select a knowledge base first', 'error');
  const kbName = sel.options[sel.selectedIndex]?.dataset?.name || sel.options[sel.selectedIndex]?.textContent?.split(' (')[0] || sel.value;
  try {
    await api(`/api/deals/${dealId}/kb`, 'PATCH', { kb_list_id: sel.value, kb_list_name: kbName });
    showToast(`Knowledge base set to "${kbName}"`);
    await loadDealTabSettings(dealId);
  } catch (e) { showToast(e.message, 'error'); }
}

async function removeDealKB(dealId) {
  try {
    await api(`/api/deals/${dealId}/kb`, 'PATCH', { kb_list_id: null, kb_list_name: null });
    showToast('Knowledge base removed');
    await loadDealTabSettings(dealId);
  } catch (e) { showToast(e.message, 'error'); }
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
    target_geography:      document.getElementById('ds-target-geography')?.value || 'Global',
    raise_type:            document.getElementById('ds-raise-type')?.value?.trim(),
    description:           document.getElementById('ds-description')?.value?.trim(),
    key_metrics:           document.getElementById('ds-key-metrics')?.value?.trim(),
    investor_profile:      document.getElementById('ds-investor-profile')?.value?.trim(),
    target_amount:         targetVal    ? Number(targetVal)    : null,
    committed_amount:      committedVal ? Number(committedVal) : null,
    currency:              document.getElementById('ds-currency')?.value || 'USD',
    send_from:             document.getElementById('ds-send-from')?.value    || '06:00',
    send_until:            document.getElementById('ds-send-until')?.value   || '18:00',
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
    no_follow_ups:         document.getElementById('ds-no-followups')?.checked || false,
    sending_account_id:    (() => { const s = document.getElementById(`ds-email-account-${id}`); return s?.value || null; })(),
    sending_email:         (() => { const s = document.getElementById(`ds-email-account-${id}`); return s?.options[s.selectedIndex]?.dataset?.email || null; })(),
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
  return openContactSidePanel(contactId, dealId || null);
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
      ${contact.last_intent || contact.conversation_state ? row('Sentiment',
        `${sentimentBadge(contact.last_intent_label, contact.last_intent, contact.conversation_state)}${contact.last_intent ? ` <span style="color:#9ca3af;font-size:12px">${esc(String(contact.last_intent).replace(/_/g, ' '))}</span>` : ''}`) : ''}
      ${row('Conversation', contact.conversation_state ? `<span style="color:#d4a847">${esc(contact.conversation_state)}</span>` : '')}
      ${row('Reply Channel', contact.reply_channel ? esc(String(contact.reply_channel).toUpperCase()) : '')}
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
    const qs = dealId ? `?dealId=${encodeURIComponent(dealId)}` : '';
    const convData = await api(`/api/contacts/${contactId}/conversation${qs}`);
    const msgs = convData.messages || [];
    const projectName = convData.selectedDealName || (dealId ? 'Unknown Project' : null);
    const convEl = document.getElementById(`conv-history-${contactId}`);
    if (convEl) {
      convEl.innerHTML = `
        <div style="color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:0.15em;margin-bottom:12px">
          Conversation History${projectName ? ` — ${esc(projectName)}` : ''} (${msgs.length} message${msgs.length !== 1 ? 's' : ''})
        </div>
        ${projectName ? `<div style="margin-bottom:12px;padding:8px 10px;background:#141414;border:1px solid #262626;border-radius:6px;color:#d4a847;font-size:11px;letter-spacing:0.08em;text-transform:uppercase">Project ${esc(projectName)}</div>` : ''}
        ${msgs.length === 0
          ? `<div style="color:#374151;font-size:12px">No messages logged yet${projectName ? ` for ${esc(projectName)}` : ''}.</div>`
          : msgs.map(m => {
            const isOut  = m.direction === 'outbound';
            const ts     = new Date(m.timestamp || Date.now()).toLocaleDateString('en-GB');
            const label  = isOut ? 'ROCO' : esc(contact.name || 'INVESTOR');
            const color  = isOut ? '#1f3a5f' : '#2a1a0a';
            const border = isOut ? '#60a5fa' : '#d4a847';
            const nameC  = isOut ? '#60a5fa' : '#d4a847';
            const preview = (m.body || '').substring(0, 350);
            const messageProject = m.dealName ? `<div style="color:#6b7280;font-size:10px;margin-bottom:4px;text-transform:uppercase;letter-spacing:0.08em">${esc(m.dealName)}</div>` : '';
            const subjectHtml = (m.channel === 'email' && m.subject)
              ? `<div style="color:#e5e7eb;font-size:11px;margin-bottom:6px;font-weight:600">Subject: ${esc(m.subject)}</div>`
              : '';
            return `<div style="margin-bottom:10px;padding:10px 12px;background:${color};border-radius:6px;border-left:3px solid ${border}">
              ${messageProject}
              <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                <span style="color:${nameC};font-size:11px;font-weight:600">${label}</span>
                <span style="color:#374151;font-size:10px">${ts}${m.channel ? ` · ${m.channel}` : ''}</span>
              </div>
              ${m.intent ? `<div style="color:#6b7280;font-size:10px;margin-bottom:4px;font-style:italic">Intent: ${esc(m.intent)}</div>` : ''}
              ${subjectHtml}
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
  tbody.innerHTML = '<tr><td colspan="10" class="table-empty">Loading…</td></tr>';

  try {
    const qs   = dealId ? `?dealId=${dealId}` : '';
    const data = await api(`/api/pipeline${qs}`);
    pipelineData = Array.isArray(data) ? data : (data.contacts || data.pipeline || []);
    renderPipelineTable();
  } catch {
    tbody.innerHTML = '<tr><td colspan="10" class="table-empty text-red">Failed to load pipeline.</td></tr>';
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
    tbody.innerHTML = '<tr><td colspan="10" class="table-empty">No contacts in pipeline.</td></tr>';
    return;
  }

  const renderDealsCell = (deals) => {
    if (!Array.isArray(deals) || deals.length === 0) return '<span class="text-dim">—</span>';
    return `<div style="display:flex;flex-wrap:wrap;gap:4px">${deals.map(d =>
      `<span class="status-badge" style="background:#1a1a1a;color:#9ca3af;border:1px solid #2a2a2a">${esc(d.dealName || '—')}</span>`
    ).join('')}</div>`;
  };

  tbody.innerHTML = sorted.map(c => {
    const id      = c.id || c._id;
    const name    = esc(c.name || (c.firstName ? (c.firstName + ' ' + (c.lastName || '')).trim() : null) || '—');
    const firm    = esc(c.firm || c.company || '—');
    const activeDeal = c.activeDealName ? `<span class="status-badge" style="background:var(--gold-dim);color:var(--gold)">${esc(c.activeDealName)}</span>` : '<span class="text-dim">—</span>';
    const project = c.projectName ? `<span class="status-badge" style="background:#1f2937;color:#9ca3af">${esc(c.projectName)}</span>` : '<span class="text-dim">—</span>';
    const deals = renderDealsCell(c.deals);
    const stage   = c.stage || '';
    const badge   = sentimentBadge(c.lastIntentLabel, c.lastIntent, c.conversationState);
    return `<tr onclick="togglePipelineRow(this, '${id}', '${c.deal_id || ''}')" style="cursor:pointer">
      <td>
        <div style="font-weight:500">${name}</div>
        ${c.email ? `<div style="font-size:11px;color:var(--text-dim)">${esc(c.email)}</div>` : ''}
      </td>
      <td>${firm}</td>
      <td>${activeDeal}</td>
      <td>${project}</td>
      <td>${deals}</td>
      <td>${scoreHtml(c.score)}</td>
      <td><span class="status-badge">${esc(stage || '—')}</span></td>
      <td>${badge || '<span class="text-dim">—</span>'}</td>
      <td class="text-dim">${formatDate(c.lastReplyAt || c.lastContacted)}</td>
      <td>
        <div class="row-actions">
          <button class="row-action-btn" onclick="event.stopPropagation(); openPipelineContactPanel('${id}', '${c.deal_id || ''}')">View</button>
          <button class="row-action-btn" onclick="event.stopPropagation(); skipContact('${id}')">Skip</button>
          <button class="row-action-btn danger" onclick="event.stopPropagation(); suppressFirm('${esc(c.firm || c.company || '')}')">Suppress Firm</button>
          <button class="row-action-btn" style="color:#e05c5c" onclick="event.stopPropagation(); deleteContact('${id}')">Delete</button>
        </div>
      </td>
    </tr>
    <tr class="pipeline-row-detail hidden" id="pipeline-detail-${id}">
      <td colspan="10" style="padding:0;background:var(--bg-raised)">
        <div id="pipeline-conv-${id}" style="padding:16px 20px;font-size:12px;color:var(--text-mid)">
          <div style="color:var(--text-dim);font-size:11px">Click to expand…</div>
        </div>
      </td>
    </tr>`;
  }).join('');
}

async function togglePipelineRow(row, id, dealId = '') {
  openContactSidePanel(id, dealId || null);
}

async function openPipelineContactPanel(id, dealId = '') {
  openContactSidePanel(id, dealId || null);
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
    const [data, reviews] = await Promise.all([
      api('/api/queue'),
      api('/api/campaign-reviews').catch(() => []),
    ]);
    const inferChannel = item => {
      const stage = String(item?.stage || '').toLowerCase();
      const explicit = String(item?.channel || '').toLowerCase();
      if (explicit) return explicit;
      if (stage.includes('linkedin') || item?.message_type === 'prior_chat_review') return 'linkedin';
      return 'email';
    };
    const emails   = Array.isArray(data) ? data.filter(i => inferChannel(i) !== 'linkedin') : (data.emails || []);
    const linkedin = Array.isArray(data) ? data.filter(i => inferChannel(i) === 'linkedin') : (data.linkedin || []);

    document.getElementById('queue-email-count').textContent     = emails.length;
    document.getElementById('queue-linkedin-count').textContent  = linkedin.length;
    document.getElementById('queue-campaigns-count').textContent = reviews.length;
    _pendingReviewsCount = reviews.length;
    refreshQueueBadge(emails.length + linkedin.length + reviews.length, true);

    renderQueueList('queue-email-list', emails, 'email');
    renderQueueList('queue-linkedin-list', linkedin, 'linkedin');
    renderCampaignReviewCards(reviews);

    // Auto-surface the campaigns tab if reviews are waiting and nothing else is active
    if (reviews.length > 0) {
      const activeTab = document.querySelector('.queue-tab.active');
      if (!activeTab || activeTab.id === 'qtab-email') {
        switchQueueTab('campaigns', document.getElementById('qtab-campaigns'));
      }
    }
  } catch { /* silent */ }
}

function renderCampaignReviewCards(reviews) {
  const el = document.getElementById('queue-campaigns-list');
  if (!el) return;
  if (!reviews.length) {
    el.innerHTML = '<div class="queue-empty">&#10003;&nbsp; No campaigns awaiting review.</div>';
    return;
  }
  el.innerHTML = reviews.map(r => {
    const topFirms  = (r.firms || []).slice(0, 5);
    const totalRanked = Number(r.ranked_firms || (r.firms || []).length || 0);
    const remaining = Math.max(0, totalRanked - topFirms.length);
    return `
    <div style="background:var(--surface-1,var(--bg-card));border:1px solid var(--border);border-radius:12px;padding:24px;margin-bottom:20px">

      <!-- Header -->
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;margin-bottom:20px">
        <div>
          <div style="font-size:18px;font-weight:600;color:var(--text-bright);margin-bottom:6px">${esc(r.deal_name)}</div>
          <div style="display:flex;flex-wrap:wrap;gap:10px;align-items:center">
            <span style="font-size:12px;color:var(--text-muted);background:var(--bg-secondary);border:1px solid var(--border);padding:3px 10px;border-radius:20px">Batch ${r.batch_number}</span>
            <span style="font-size:12px;color:var(--text-muted);background:var(--bg-secondary);border:1px solid var(--border);padding:3px 10px;border-radius:20px">${totalRanked} firms ranked</span>
            ${r.deal_sector    ? `<span style="font-size:12px;color:var(--text-muted);background:var(--bg-secondary);border:1px solid var(--border);padding:3px 10px;border-radius:20px">${esc(r.deal_sector)}</span>`    : ''}
            ${r.deal_raise_type ? `<span style="font-size:12px;color:var(--text-muted);background:var(--bg-secondary);border:1px solid var(--border);padding:3px 10px;border-radius:20px">${esc(r.deal_raise_type)}</span>` : ''}
          </div>
        </div>
        <div style="display:flex;gap:10px;flex-shrink:0;align-items:center">
          <button class="btn btn-ghost" style="font-size:13px;padding:8px 18px" onclick="openDealAndBatch('${r.deal_id}')">View Detail</button>
          <button class="btn btn-gold"  style="font-size:13px;padding:8px 18px" onclick="approveCampaignFromQueue('${r.deal_id}','${r.id}',this)">&#10003;&nbsp; Approve Campaign</button>
        </div>
      </div>

      <!-- Top firms -->
      <div style="border-top:1px solid var(--border);padding-top:16px">
        <div style="font-size:11px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);margin-bottom:10px">Top Firms</div>
        <div style="display:flex;flex-direction:column;gap:8px">
          ${topFirms.map((f, i) => `
            <div style="display:flex;align-items:center;gap:14px;padding:10px 14px;background:var(--bg-secondary);border:1px solid var(--border);border-radius:8px">
              <span style="font-size:12px;color:var(--text-muted);font-family:'DM Mono',monospace;width:18px;flex-shrink:0">${i + 1}</span>
              <span style="font-size:13px;color:var(--text-bright);flex:1">${f.firm_link_url ? `<a href="${esc(f.firm_link_url)}" target="_blank" rel="noopener noreferrer" style="color:inherit;text-decoration:underline;text-underline-offset:3px">${esc(f.firm_name)}</a>` : esc(f.firm_name)}</span>
              <span style="font-size:13px;font-family:'DM Mono',monospace;color:var(--gold);font-weight:600">${f.score}<span style="font-size:10px;color:var(--text-muted);font-weight:400">/100</span></span>
            </div>`).join('')}
          ${remaining > 0 ? `
            <div style="text-align:center;padding:10px 0;font-size:12px;color:var(--text-muted)">
              + ${remaining} more — click <strong style="color:var(--text)">View Detail</strong> for the full ranked list
            </div>` : ''}
        </div>
      </div>

    </div>`;
  }).join('');
}

async function approveCampaignFromQueue(dealId, batchId, btn) {
  if (btn) { btn.disabled = true; btn.textContent = 'Approving…'; }
  try {
    await api(`/api/deals/${dealId}/batch/${batchId}/approve`, 'POST');
    showToast('Campaign approved — enrichment starting');
    await loadQueue();
  } catch (e) {
    showToast(e.message || 'Approval failed', 'error');
    if (btn) { btn.disabled = false; btn.textContent = '✓ Approve'; }
  }
}

async function openDealAndBatch(dealId) {
  // Navigate to Deals section, open the deal, land on Campaign tab
  window.location.hash = '#deals';
  await loadDeals();
  await viewDeal(dealId);
  const campaignBtn = document.querySelector('.deal-tab[data-tab="rankings"]');
  await switchDealTab('rankings', campaignBtn);
}

function renderQueueList(containerId, items, type) {
  const el = document.getElementById(containerId);
  if (!el) return;
  if (!items.length) {
    el.innerHTML = `<div class="queue-empty">&#10003;&nbsp; No ${type} messages pending approval.</div>`;
    return;
  }
  window._qItems = window._qItems || {};
  window._qSubjects = window._qSubjects || {};
  items.forEach(item => {
    const id = item.id || item._id;
    window._qItems[id] = item;
    const subA = item.subjectA || item.subject || '';
    const subB = item.subjectB || '';
    const existing = window._qSubjects[id];
    window._qSubjects[id] = { a: subA, b: subB, current: existing?.current || 'a' };
  });
  el.innerHTML = items.map(item => renderQueueCard(item)).join('');
}

function renderQueueCard(item) {
  const id      = item.id || item._id;
  const name    = esc(item.name || item.firstName || '—');
  const firm    = esc(item.firm || item.company || '');
  const stageValue = String(item.stage || '').toLowerCase();
  const channelValue = String(item.channel || '').toLowerCase();
  const messageType = String(item.message_type || '').toLowerCase();
  const isReply = !!item.isReply || messageType === 'email_reply' || messageType === 'linkedin_reply' || stageValue.includes('reply');

  // Prior chat review — amber card with Proceed/Skip
  if (item.message_type === 'prior_chat_review') {
    const summary      = esc(item.message_text || 'Prior conversation found — review before sending DM.');
    const msgCount     = item.metadata?.messageCount || '?';
    const contactId    = item.contact_id || '';
    return `<div class="queue-card" id="qcard-${id}" style="border-left:3px solid #f5a623">
      <div class="queue-card-header">
        <div>
          <div class="queue-name">${name}</div>
          <div class="queue-firm">${firm}</div>
        </div>
        <div class="queue-meta">
          <span class="status-badge" style="background:#f5a62322;color:#f5a623;border-color:#f5a623">Prior Chat</span>
          <span style="font-size:11px;color:var(--text-muted)">${msgCount} msg(s)</span>
        </div>
      </div>
      <div class="queue-body">
        <div class="queue-preview" style="font-style:italic;color:var(--text-secondary)">${summary}</div>
      </div>
      <div class="queue-actions">
        <button class="btn-approve" onclick="decidePriorChat('${id}','${contactId}','proceed')">&#10003; PROCEED — SEND DM</button>
        <button class="btn btn-danger btn-sm" onclick="decidePriorChat('${id}','${contactId}','skip')">SKIP CONTACT</button>
      </div>
    </div>`;
  }

  const scoreHt = scoreHtml(item.score);
  const stage   = esc(item.stage || '');
  const subA    = esc(item.subjectA || item.subject || '');
  const subB    = esc(item.subjectB || '');
  const hasAB   = !!subB;
  const body    = esc(item.body || item.emailBody || '');
  const isWaiting = !!item.waitingForWindow;

  const approveButtons = isWaiting
    ? '' // already approved — no approve button, just edit/skip
    : isReply
      ? `
        <button class="btn-approve" onclick="approveEmail('${id}', currentQueueVariant('${id}'), '${id}', false)">&#10003; APPROVE</button>
        <button class="btn btn-gold btn-sm" onclick="approveEmail('${id}', currentQueueVariant('${id}'), '${id}', true)">&#9889; SEND NOW</button>
      `
      : `<button class="btn-approve" onclick="approveEmail('${id}', currentQueueVariant('${id}'), '${id}', false)">&#10003; APPROVE</button>`;

  const waitingBadge = isWaiting
    ? `<span class="status-badge" style="background:rgba(34,197,94,0.12);color:#4ade80;border-color:#4ade80;margin-left:6px">&#10003; Approved — awaiting window</span>`
    : '';

  return `<div class="queue-card" id="qcard-${id}" style="${isWaiting ? 'border-left:2px solid #4ade80;opacity:0.85' : ''}">
    <div class="queue-card-header">
      <div>
        <div class="queue-name">${name}</div>
        <div class="queue-firm">${firm}</div>
      </div>
      <div class="queue-meta">
        ${scoreHt}
        ${stage ? `<span class="status-badge">${stage}</span>` : ''}
        ${waitingBadge}
        ${hasAB ? `<div class="subject-toggle">
          <button class="subject-toggle-btn active" id="stb-a-${id}" onclick="switchSubject('${id}','a')">A</button>
          <button class="subject-toggle-btn" id="stb-b-${id}" onclick="switchSubject('${id}','b')">B</button>
        </div>` : ''}
      </div>
    </div>
    <div class="queue-body">
      <div class="queue-subject-row">
        <div class="queue-subject" id="qsubject-${id}">${subA || '(no subject)'}</div>
        <button class="subject-edit-btn" onclick="openSubjectModal('${id}')" title="Edit subject">&#9998;</button>
      </div>
      <div class="queue-preview">${body}</div>
    </div>
    <div class="queue-actions">
      ${approveButtons}
      <button class="btn btn-ghost btn-sm" onclick="previewQueueItem('${id}')">&#128065; PREVIEW</button>
      <button class="btn btn-ghost btn-sm" onclick="editApproval('${id}')">&#9998; EDIT</button>
      <button class="btn btn-danger btn-sm" onclick="skipApproval('${id}')">SKIP</button>
      ${item.linkedinUrl ? `<a href="${esc(item.linkedinUrl)}" target="_blank" class="btn btn-ghost btn-sm" onclick="event.stopPropagation()">LinkedIn ↗</a>` : ''}
    </div>
  </div>
`;
}

window.decidePriorChat = async function(approvalId, contactId, decision) {
  const card = document.getElementById(`qcard-${approvalId}`);
  if (card) card.style.opacity = '0.5';
  try {
    const r = await fetch(`/api/approvals/${approvalId}/prior-chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ decision }),
    });
    if (!r.ok) throw new Error(await r.text());
    if (card) card.remove();
    showToast(decision === 'proceed' ? 'DM queued for next cycle' : 'Contact skipped');
  } catch (err) {
    if (card) card.style.opacity = '1';
    showToast('Error: ' + err.message, 'error');
  }
};

function switchSubject(id, variant) {
  const subjects = window._qSubjects?.[id];
  if (!subjects) return;
  subjects.current = variant;
  const el = document.getElementById(`qsubject-${id}`);
  if (el) el.textContent = subjects[variant] || '(no subject)';
  document.getElementById(`stb-a-${id}`)?.classList.toggle('active', variant === 'a');
  document.getElementById(`stb-b-${id}`)?.classList.toggle('active', variant === 'b');
  api('/api/edit-approval', 'POST', { id, subject: subjects[variant] || '' }).catch(() => {});
}

function currentQueueVariant(id) {
  return window._qSubjects?.[id]?.current || 'a';
}

let _subjectEditId = null;

function openSubjectModal(id) {
  const subjects = window._qSubjects?.[id];
  if (!subjects) return;
  _subjectEditId = id;
  const current = subjects.current || 'a';
  const hasB = !!(subjects.b);

  document.getElementById('subject-modal')?.remove();
  const modal = document.createElement('div');
  modal.id = 'subject-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.82);z-index:2000;display:flex;align-items:center;justify-content:center;padding:24px';
  modal.addEventListener('click', e => { if (e.target === modal) closeSubjectModal(); });
  modal.innerHTML = `
    <div class="modal" style="max-width:540px;width:100%">
      <div class="modal-header">
        <h3 class="modal-title">Edit Subject Line</h3>
        <button class="modal-close" onclick="closeSubjectModal()">&#215;</button>
      </div>
      <div class="modal-body" style="display:flex;flex-direction:column;gap:18px">
        <div>
          <label class="form-label" style="display:flex;align-items:center;gap:10px;margin-bottom:8px;cursor:pointer">
            <input type="radio" name="subj-pick" value="a" ${current === 'a' || !hasB ? 'checked' : ''} style="accent-color:var(--gold)">
            <span>Subject A ${!hasB ? '' : '— select to send this one'}</span>
          </label>
          <input type="text" id="subj-val-a" class="form-input" value="${(subjects.a || '').replace(/"/g, '&quot;').replace(/</g, '&lt;')}" style="width:100%">
        </div>
        ${hasB ? `<div>
          <label class="form-label" style="display:flex;align-items:center;gap:10px;margin-bottom:8px;cursor:pointer">
            <input type="radio" name="subj-pick" value="b" ${current === 'b' ? 'checked' : ''} style="accent-color:var(--gold)">
            <span>Subject B — select to send this one</span>
          </label>
          <input type="text" id="subj-val-b" class="form-input" value="${(subjects.b || '').replace(/"/g, '&quot;').replace(/</g, '&lt;')}" style="width:100%">
        </div>` : ''}
      </div>
      <div class="modal-footer">
        <button class="btn btn-ghost" onclick="closeSubjectModal()">Cancel</button>
        <button class="btn btn-gold" onclick="confirmSubjectEdit()">Confirm</button>
      </div>
    </div>`;
  document.body.appendChild(modal);
}

function closeSubjectModal() {
  document.getElementById('subject-modal')?.remove();
  _subjectEditId = null;
}

function confirmSubjectEdit() {
  const id = _subjectEditId;
  if (!id) return;
  const subjects = window._qSubjects?.[id];
  if (!subjects) { closeSubjectModal(); return; }

  const picked = document.querySelector('input[name="subj-pick"]:checked')?.value || 'a';
  const newA = document.getElementById('subj-val-a')?.value?.trim();
  const newB = document.getElementById('subj-val-b')?.value?.trim();

  if (newA) subjects.a = newA;
  if (newB) subjects.b = newB;
  subjects.current = picked;

  const displayEl = document.getElementById(`qsubject-${id}`);
  if (displayEl) displayEl.textContent = subjects[picked] || '(no subject)';
  document.getElementById(`stb-a-${id}`)?.classList.toggle('active', picked === 'a');
  document.getElementById(`stb-b-${id}`)?.classList.toggle('active', picked === 'b');

  api('/api/edit-approval', 'POST', { id, subject: subjects[picked] }).catch(() => {});
  closeSubjectModal();
}

function previewQueueItem(id) {
  const item = window._qItems?.[id] || {};
  const subjects = window._qSubjects?.[id] || {};
  const variant = currentQueueVariant(id);
  const body = item.body || item.emailBody || '';
  const stage = String(item.stage || '').toLowerCase();
  const channel = String(item.channel || '').toLowerCase();
  const messageType = String(item.message_type || '').toLowerCase();
  const isLinkedIn = channel === 'linkedin'
    || stage.includes('linkedin')
    || messageType === 'prior_chat_review';

  const tmpl = {
    type: isLinkedIn ? 'linkedin_dm' : 'email',
    subject_a: subjects.a || null,
    subject_b: subjects.b || null,
    preview_subject: subjects[variant] || subjects.a || null,
    body,
  };
  const contactData = {
    firstName: (item.name || item.firstName || '').split(' ')[0] || 'James',
    fullName:  item.name || item.fullName || 'James Mitchell',
    email:     item.contactEmail || item.email || 'james@meridiancapital.com',
    firm:      item.firm || item.company || 'Meridian Capital',
    company:   item.firm || item.company || 'Meridian Capital',
  };
  window.previewDealTemplate(tmpl, contactData);
}

function switchQueueTab(tab, btn) {
  currentQueueTab = tab;
  document.querySelectorAll('.queue-tab').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  document.getElementById('qpanel-campaigns')?.classList.toggle('hidden', tab !== 'campaigns');
  document.getElementById('qpanel-email')?.classList.toggle('hidden', tab !== 'email');
  document.getElementById('qpanel-linkedin')?.classList.toggle('hidden', tab !== 'linkedin');
}

async function approveEmail(id, variant, _unused, sendNow = false) {
  const subjects   = window._qSubjects?.[id] || {};
  const subject    = subjects[variant] || subjects.a || '';
  try {
    await api('/api/approve', 'POST', { id, variant, subject, sendNow });
    document.getElementById(`qcard-${id}`)?.remove();
    await loadQueue();
  } catch (err) { alert(`Approve failed: ${err.message}`); }
}

async function editApproval(id) {
  const item = window._qItems?.[id] || {};
  const stage = String(item.stage || '').toLowerCase();
  const isLinkedIn = String(item.channel || '').toLowerCase() === 'linkedin' || stage.includes('linkedin');
  openModal(isLinkedIn ? 'Edit LinkedIn DM' : 'Edit Email', async () => {
    const body = document.getElementById('modal-instructions').value;
    try {
      await api('/api/edit-approval', 'POST', { id, body });
      closeModal();
      await loadQueue();
    } catch (err) { alert(`Edit failed: ${err.message}`); }
  }, item.body || item.emailBody || '');
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

// Paginated activity log state
let _activityPage    = 1;
let _activityDealFilter = null;
let _activityTotal   = 0;
let _activityPages   = 1;
let _activityLastServerEvents = [];
let _activityLivePending = [];

function activityEventKey(event) {
  if (!event) return '';
  return String(
    event.id
    || [
      event.created_at || event.timestamp || '',
      event.type || event.event_type || '',
      event.action || event.summary || '',
      event.note || event.detail || '',
      event.deal_id || event.dealId || '',
    ].join('|')
  );
}

function activityMatchesDealFilter(event, dealId) {
  if (!dealId) return true;
  return String(event?.deal_id || event?.dealId || event?.deal || '') === String(dealId);
}

function mergeActivityEvents(events = [], dealId = null) {
  const merged = [];
  const seen = new Set();

  const add = (event) => {
    if (!event || !activityMatchesDealFilter(event, dealId)) return;
    const key = activityEventKey(event);
    if (!key || seen.has(key)) return;
    seen.add(key);
    merged.push(event);
  };

  _activityLivePending.forEach(add);
  events.forEach(add);
  return merged;
}

function reconcilePendingActivity(events = []) {
  const persistedKeys = new Set((events || []).map(activityEventKey));
  _activityLivePending = _activityLivePending.filter(event => !persistedKeys.has(activityEventKey(event)));
}

async function loadActivity(page = 1) {
  _activityPage = page;
  const dealId = document.getElementById('activity-deal-filter')?.value || activeDeal || '';
  _activityDealFilter = dealId || null;

  try {
    const params = new URLSearchParams({ page });
    if (dealId) params.set('deal_id', dealId);

    const data = await api(`/api/activity?${params}`, 'GET', null, { silent: true });

    // New paginated format
    if (data && typeof data === 'object' && 'events' in data) {
      _activityLastServerEvents = data.events || [];
      reconcilePendingActivity(_activityLastServerEvents);
      _activityTotal = data.total || 0;
      _activityPages = data.pages || 1;
      const mergedEvents = page === 1
        ? mergeActivityEvents(_activityLastServerEvents, _activityDealFilter).slice(0, 50)
        : _activityLastServerEvents;
      const mergedTotal = page === 1 ? Math.max(_activityTotal, mergedEvents.length) : _activityTotal;
      renderPaginatedActivityLog(mergedEvents, page, data.pages, mergedTotal);
      return;
    }

    // Fallback: flat array from legacy endpoint
    const items = Array.isArray(data) ? data : (data.log || data.items || []);
    activityLog = items;
    filterActivity();
  } catch { /* silent */ }
}

function renderPaginatedActivityLog(events, currentPage, totalPages, total) {
  const container = document.getElementById('activity-feed');
  if (!container) return;

  const typeColors = {
    thinking: '#A78BFA', research: '#60A5FA', email: '#C9A84C',
    linkedin: '#4ADE80', accepted: '#A78BFA', relation: '#f59e0b', reply: '#C084FC', linkedin_reply: '#38bdf8', email_reply: '#a78bfa', email_opened: '#ec4899', email_clicked: '#f472b6', system: '#8A8680',
    error: '#F87171', analysis: '#4ADE80', excluded: '#6b7280', dm: '#fb923c', invite: '#4ADE80', enrichment: '#60A5FA', approval: '#C9A84C',
  };
  const typeIcons = {
    thinking: '🧠', research: '🔍', email: '📧', linkedin: '💼',
    accepted: '✓', relation: '🟧', reply: '↩️', linkedin_reply: '↩️', email_reply: '↩️', email_opened: '👁️', email_clicked: '🔗', system: '⚙️', error: '⚠️', analysis: '📊', excluded: '✕',
    dm: '💬', invite: '🔗', enrichment: '🔎', approval: '⏳',
  };

  function stringifyActivityField(value) {
    if (value == null) return '';
    if (typeof value === 'string') return value;
    if (typeof value === 'number' || typeof value === 'boolean') return String(value);
    if (typeof value === 'object') {
      const preferred = [
        value.reason,
        value.error,
        value.message,
        value.linkedin_url,
        value.provider_id,
        value.public_id,
        value.source,
      ].filter(Boolean);
      if (preferred.length) return preferred.join(' · ');
      try {
        return JSON.stringify(value);
      } catch {
        return '';
      }
    }
    return String(value);
  }

  const eventsHtml = (events || []).map(event => {
    const badge = getActivityBadgeMeta(event);
    const type  = badge.className;
    const color = typeColors[type] || '#8A8680';
    const icon  = typeIcons[type] || '⚙️';
    const isThinking = type === 'thinking';
    const isResearchTrace = type === 'research' && !!event.full_content;
    const isExpandedType = isThinking || isResearchTrace;
    const ts    = new Date(event.created_at || event.timestamp).toLocaleString('en-US', {
      month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true, timeZone: DOM_TZ,
    });
    const mainText = isExpandedType && event.full_content
      ? event.full_content
      : stringifyActivityField(event.action || event.summary || '');
    const note  = stringifyActivityField(event.note || event.detail || '');

    return `<div style="padding:10px 14px;background:rgba(${color === '#A78BFA' ? '167,139,250' : '138,134,128'},0.06);
                        border-left:3px solid ${color};border-radius:0 4px 4px 0;margin-bottom:5px">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px">
        <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
          <span style="font-size:13px">${icon}</span>
          <span style="font-size:10px;color:${color};font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.08em">
            ${badge.label}${isThinking ? ' · full reasoning' : ''}
          </span>
          ${isResearchTrace ? `<span style="font-size:10px;color:#60A5FA;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.08em">research trace</span>` : ''}
        </div>
        <span style="font-size:10px;color:#3A3835;font-family:'DM Mono',monospace;white-space:nowrap;flex-shrink:0">${ts}</span>
      </div>
      <div style="font-size:12px;color:#EDE9E3;line-height:1.6;margin-top:5px;word-break:break-word;${isExpandedType ? 'white-space:pre-wrap;' : ''}">
        ${esc(mainText)}
      </div>
      ${note ? `<div style="margin-top:3px;font-size:10px;color:#6b7280;font-family:'DM Mono',monospace">${esc(note)}</div>` : ''}
    </div>`;
  }).join('') || '<div style="color:#3A3835;font-size:12px;padding:16px 0">No activity yet.</div>';

  const paginationHtml = totalPages > 1 ? `
    <div style="display:flex;justify-content:space-between;align-items:center;padding:12px 0;border-top:1px solid #1C1C1F;margin-top:10px">
      <span style="font-size:11px;color:#6b7280;font-family:'DM Mono',monospace">
        ${Number(total).toLocaleString()} events · Page ${currentPage} of ${totalPages}
      </span>
      <div style="display:flex;gap:6px">
        ${currentPage > 1 ? `<button onclick="loadActivity(${currentPage - 1})" style="padding:4px 10px;background:#1a1a1a;border:1px solid #2a2a2a;color:#8A8680;border-radius:4px;cursor:pointer;font-size:11px">← Newer</button>` : ''}
        ${currentPage < totalPages ? `<button onclick="loadActivity(${currentPage + 1})" style="padding:4px 10px;background:#1a1a1a;border:1px solid #2a2a2a;color:#8A8680;border-radius:4px;cursor:pointer;font-size:11px">Older →</button>` : ''}
      </div>
    </div>` : '';

  container.innerHTML = `
    <div style="margin-bottom:10px;display:flex;justify-content:space-between;align-items:center">
      <span style="font-size:11px;color:#3A3835;font-family:'DM Mono',monospace">
        ${Number(total).toLocaleString()} events · Page ${currentPage}/${totalPages}
      </span>
      <button onclick="loadActivity(1)" style="padding:4px 10px;background:#1a1a1a;border:1px solid #2a2a2a;color:#6b7280;border-radius:4px;cursor:pointer;font-size:11px">↻ Refresh</button>
    </div>
    ${eventsHtml}
    ${paginationHtml}
  `;
}

// Live WS: prepend new event to activity page when on page 1
function handleLiveActivityForPage(event) {
  if (_activityPage !== 1) return;
  const container = document.getElementById('activity-feed');
  if (!container) return;
  const key = activityEventKey(event);
  if (key && !_activityLivePending.some(item => activityEventKey(item) === key)) {
    _activityLivePending.unshift(event);
    if (_activityLivePending.length > 50) _activityLivePending.pop();
    _activityTotal = (_activityTotal || 0) + 1;
  }
  if (!activityMatchesDealFilter(event, _activityDealFilter)) return;

  const mergedEvents = mergeActivityEvents(_activityLastServerEvents, _activityDealFilter).slice(0, 50);
  renderPaginatedActivityLog(mergedEvents, 1, _activityPages, Math.max(_activityTotal, mergedEvents.length));
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
  // Remove both archive table rows immediately to prevent double-click
  const expandRow = document.getElementById(`archive-expand-${id}`);
  const mainRow = expandRow?.previousElementSibling;
  expandRow?.remove();
  mainRow?.remove();
  document.getElementById(`deal-card-${id}`)?.remove();
  try {
    await api(`/api/deals/${id}`, 'DELETE');
    showToast?.('Deal deleted permanently');
    loadArchive?.();
    loadDeals?.();
  } catch (err) {
    console.error('[DELETE] Error:', err);
    showToast?.('Delete failed: ' + err.message, 'error');
    loadArchive?.();
    loadDeals?.();
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
    active_deal: c.activeDealName || '',
    project: c.projectName || c.dealName || c.deal_name || '',
    deals: c.dealNamesText || '',
    score: c.score || '',
    stage: c.stage || '',
    enrichment_status: c.enrichmentStatus || '',
    email: c.email || '',
    linkedin_url: c.linkedinUrl || c.linkedin_url || '',
    last_contact: c.lastContact || c.lastContacted || '',
    scheduled_follow_up: c.scheduledFollowUpAt || '',
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
      scheduled_follow_up: r.scheduledFollowUpAt || '',
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
      const data = await api(`/api/contacts-db/researched?${params}`);
      allRows = allRows.concat(data.contacts || []);
      totalPages = data.pages || 1;
      page++;
    } while (page <= totalPages);
    if (!allRows.length) { showToast('No contacts to export', 'error'); return; }
    const mapped = allRows.map(r => ({
      name: r.name || '',
      firm_name: r.company_name || '',
      title: r.job_title || '',
      active_deal: r.activeDealName || '',
      project: r.projectName || r.dealName || '',
      deals: r.dealNamesText || '',
      email: r.email || '',
      linkedin_url: r.linkedin_url || '',
      stage: r.pipeline_stage || '',
      verified: r.enrichment_status || '',
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
  bar.innerHTML = steps.map((s, i) => {
    const def = SEQ_STEP_DEFS[s.label] || SEQ_STEP_DEFS[s.action_type] || null;
    const bg = def?.color || { email: '#1f3a5f', linkedin_invite: '#1a3a2a', linkedin_dm: '#2a1f3a' }[s.type] || '#1a1a1a';
    const badge = def?.badge || '#6b7280';
    const display = def?.display || (s.label || '').replace(/_/g, ' ');
    const delayLabel = Number(s.delay_days) > 0 ? `+${s.delay_days}d` : 'Day 0';
    return `
      ${i > 0 ? '<div style="color:#2a2a2a;font-size:18px;align-self:center;padding-top:8px">&#8594;</div>' : ''}
      <div style="display:flex;flex-direction:column;align-items:center;gap:3px;padding:8px 14px;
                  background:${bg};border-radius:6px;min-width:100px;text-align:center">
        <span style="font-size:9px;color:${badge};font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:0.12em">${delayLabel}</span>
        <span style="color:#e5e7eb;font-size:12px;font-weight:600">${esc(display)}</span>
        ${s.type !== 'linkedin_invite' ? '<span style="font-size:9px;color:#3a3a3a;font-family:\'DM Mono\',monospace">template</span>' : '<span style="font-size:9px;color:#2a2a2a;font-family:\'DM Mono\',monospace">auto</span>'}
      </div>`;
  }).join('');
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
        <div id="variable-chips" style="display:flex;flex-direction:column;gap:8px">
          <div>
            <div style="font-size:10px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">Contact</div>
            <div class="variable-chips">${['{{firstName}}','{{lastName}}','{{fullName}}','{{firm}}','{{jobTitle}}'].map(v=>`<span class="var-chip" onclick="insertVariable('${v}')">${v}</span>`).join('')}</div>
          </div>
          <div>
            <div style="font-size:10px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">Investor Research</div>
            <div class="variable-chips">${['{{pastInvestments}}','{{investmentThesis}}','{{sectorFocus}}','{{investorGeography}}'].map(v=>`<span class="var-chip" onclick="insertVariable('${v}')">${v}</span>`).join('')}</div>
          </div>
          <div>
            <div style="font-size:10px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">Deal</div>
            <div class="variable-chips">${['{{dealName}}','{{dealBrief}}','{{sector}}','{{targetAmount}}','{{keyMetrics}}','{{geography}}','{{minCheque}}','{{maxCheque}}','{{investorProfile}}','{{comparableDeal}}'].map(v=>`<span class="var-chip" onclick="insertVariable('${v}')">${v}</span>`).join('')}</div>
          </div>
          <div>
            <div style="font-size:10px;color:#6b7280;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">Links &amp; Sender</div>
            <div class="variable-chips">${['{{deckUrl}}','{{callLink}}','{{senderName}}','{{senderTitle}}'].map(v=>`<span class="var-chip" onclick="insertVariable('${v}')">${v}</span>`).join('')}</div>
          </div>
        </div>
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
    refreshHealth();
    loadWebhookStatus();

    // Populate controls deal selectors
    const deals = allDeals.length ? allDeals : ((await api('/api/deals').catch(() => [])) || []);
    for (const selId of ['research-deal-select', 'enrichment-deal-select']) {
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

async function loadWebhookStatus() {
  const el = document.getElementById('webhook-monitor-body');
  if (!el) return;
  el.innerHTML = '<div style="color:var(--text-dim);font-size:13px">Loading webhook status…</div>';

  try {
    const data = await api('/api/admin/webhook-status');
    renderWebhookStatus(el, data || {});
  } catch (err) {
    el.innerHTML = `<div style="color:#e05c5c;font-size:13px">Failed to load webhook status: ${esc(err.message)}</div>`;
  }
}

function renderWebhookStatus(el, data) {
  const latest = data?.latest || {};
  const hooks = Array.isArray(data?.hooks) ? data.hooks : [];
  const row = (label, item, tone = '#4ade80') => `
    <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;padding:12px 0;border-bottom:1px solid var(--border)">
      <div>
        <div style="font-size:13px;color:var(--text-bright)">${esc(label)}</div>
        <div style="font-size:11px;color:var(--text-dim);margin-top:3px">${esc(item?.event_type || 'No receipt yet')}</div>
      </div>
      <div style="text-align:right">
        <div style="font-size:12px;color:${item ? tone : 'var(--text-dim)'};font-family:var(--font-mono)">${item ? formatDate(item.received_at) : 'Never'}</div>
        <div style="font-size:10px;color:var(--text-dim);margin-top:3px">${item?.received_at ? esc(formatTime(item.received_at)) : ''}</div>
      </div>
    </div>`;

  const hookCards = hooks.length
    ? hooks.map(hook => {
        const name = hook.name || hook.label || hook.id || 'Unnamed webhook';
        const url = hook.request_url || hook.url || hook.endpoint || hook.target_url || '';
        const events = Array.isArray(hook.events) ? hook.events.join(', ') : (hook.event || hook.type || '');
        const source = hook.source || hook.object_type || '';
        return `<div style="padding:10px 12px;background:rgba(255,255,255,0.03);border:1px solid var(--border);border-radius:6px">
          <div style="font-size:12px;color:var(--text-bright);margin-bottom:4px">${esc(name)}</div>
          <div style="font-size:10px;color:var(--text-dim);font-family:var(--font-mono);margin-bottom:4px;word-break:break-all">${esc(url || 'No URL')}</div>
          <div style="font-size:10px;color:var(--gold);font-family:var(--font-mono)">${esc(events || 'No events listed')}</div>
          ${source ? `<div style="font-size:10px;color:var(--text-dim);font-family:var(--font-mono);margin-top:4px">${esc(source)}</div>` : ''}
        </div>`;
      }).join('')
    : '<div style="color:var(--text-dim);font-size:12px">No registered webhooks returned by Unipile.</div>';

  el.innerHTML = `
    <div style="display:grid;grid-template-columns:minmax(280px,1fr) minmax(320px,1.15fr);gap:24px">
      <div>
        <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px">Latest Receipts</div>
        ${row('Gmail', latest.gmail)}
        ${row('Outlook', latest.outlook, '#60a5fa')}
        ${row('LinkedIn Acceptance', latest.linkedin_acceptance, 'var(--gold)')}
        ${row('LinkedIn DM', latest.linkedin_dm, '#c084fc')}
      </div>
      <div>
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
          <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.08em">Registered Webhooks</div>
          <div style="font-size:11px;color:var(--text-dim);font-family:var(--font-mono)">${hooks.length} total</div>
        </div>
        <div style="display:grid;gap:10px">${hookCards}</div>
      </div>
    </div>
  `;
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

/* ═══════════════════════════════════════════════════════════════════════════
   MODAL
   ═══════════════════════════════════════════════════════════════════════════ */

function openModal(title, onConfirm, initialValue = '') {
  document.getElementById('modal-title').textContent = title;
  document.getElementById('modal-instructions').value = initialValue;
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
  // If a deal detail is open, keep the tab DOM intact and only refresh shared/cache data.
  if (selectedDealId) {
    await Promise.all([
      quietRefreshSelectedDeal(),
      (!document.getElementById('view-deals')?.classList.contains('hidden') ? loadDeals() : Promise.resolve()),
    ]);
    return;
  }
  const view = (window.location.hash || '#overview').replace('#', '');
  switch (view) {
    case 'overview':  await loadOverview();  break;
    case 'deals':     await loadDeals();     break;
    case 'pipeline':  await loadPipeline();  break;
    case 'queue':     await loadQueue();     break;
    case 'activity':  await loadActivity();  break;
    case 'controls':  await loadControls();  break;
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
  if (typeof n === 'string' && n.includes('%')) return n;
  const num = Number(n);
  // Values strictly between 0 and 1 (exclusive) are decimal rates → multiply by 100.
  // All other values (integers 0–100, or exactly 0/1) are already percentages.
  const v = (num > 0 && num < 1) ? (num * 100).toFixed(1) + '%' : num.toFixed(1) + '%';
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

function formatScheduleDate(d) {
  if (!d) return '—';
  try {
    const date = new Date(d);
    if (isNaN(date)) return String(d);
    const datePart = date.toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      timeZone: DOM_TZ,
    });
    const timePart = date.toLocaleTimeString('en-GB', {
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZone: DOM_TZ,
    });
    return `${datePart} ${timePart}`;
  } catch {
    return String(d);
  }
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
  if (t === 'accepted' || t.includes('accepted') || t.includes('invite_accepted'))               return 'accepted';
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
  if (t === 'analysis' || t.includes('analysis'))                                                return 'analysis';
  if (t === 'jarvis'  || t.includes('jarvis'))                                                  return 'jarvis';
  return 'system';
}

function getActivityBadgeMeta(item) {
  const explicitBadge = String(item?.activity_badge || item?.badge || '').toLowerCase();
  const normalizedExplicitBadge = explicitBadge === 'replied' ? 'reply' : explicitBadge;
  const className = normalizedExplicitBadge || typeToBadge(item?.type || item?.event_type || item?.activityType);
  const labels = {
    accepted: 'Accepted',
    relation: 'New Relation',
    reply: 'Replied',
    linkedin_reply: 'LinkedIn Reply',
    email_reply: 'Email Reply',
    email_opened: 'Email Opened',
    email_clicked: 'Email Clicked',
    invite: 'Invite',
    dm: 'DM Sent',
    linkedin: 'LinkedIn',
    email: 'Email',
    research: 'Research',
    enrichment: 'Enrichment',
    approval: 'Approval',
    error: 'Error',
    excluded: 'No Match',
    analysis: 'Analysis',
    thinking: 'Thinking',
    jarvis: 'JARVIS',
    system: 'System',
  };
  return { className, label: labels[className] || className };
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
    const lists = await api('/api/lists');
    if (!lists?.length) { panel.innerHTML = ''; return; }
    panel.innerHTML = `
      <div class="card">
        <div class="card-header"><h2 class="card-title">Investor Lists</h2></div>
        <div style="padding:0 24px 24px;display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:16px">
          ${lists.map(l => `
            <div style="background:#111113;border:1px solid var(--border);border-radius:10px;padding:16px;display:flex;flex-direction:column;gap:12px">
              <div style="display:flex;justify-content:space-between;gap:10px;align-items:flex-start">
                <div>
                  <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
                    <input id="list-input-${l.id}" type="text" value="${esc(l.name)}"
                      style="padding:6px 8px;background:#0b0b0d;border:1px solid var(--border);color:var(--text-bright);border-radius:6px;font-size:14px;font-weight:600;width:min(100%,240px)">
                    <span style="${listTypeBadgeStyle(l.list_type)}">${esc(formatListTypeLabel(l.list_type))}</span>
                  </div>
                  <div style="margin-top:8px;color:var(--text-dim);font-size:12px">${(l.investor_count || 0).toLocaleString()} investors · Used ${Number(l.use_count || 0).toLocaleString()} deals · ${Number(l.success_rate || 0)}% response rate</div>
                </div>
                <div style="font-size:11px;color:var(--text-dim);font-family:var(--font-mono)">Priority ${l.priority_order ?? 99}</div>
              </div>
              <div>
                <label style="display:block;font-size:10px;text-transform:uppercase;letter-spacing:.12em;color:var(--gold);font-family:var(--font-mono);margin-bottom:6px">Description</label>
                <textarea id="list-description-${l.id}" rows="3" class="form-textarea" style="min-height:82px">${esc(l.description || '')}</textarea>
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
                <div>
                  <label style="display:block;font-size:10px;text-transform:uppercase;letter-spacing:.12em;color:var(--gold);font-family:var(--font-mono);margin-bottom:6px">List Type</label>
                  <select id="list-type-${l.id}" class="form-input">
                    ${buildListTypeOptions(l.list_type)}
                  </select>
                </div>
                <div>
                  <label style="display:block;font-size:10px;text-transform:uppercase;letter-spacing:.12em;color:var(--gold);font-family:var(--font-mono);margin-bottom:6px">Priority Order</label>
                  <input id="list-priority-${l.id}" type="number" min="1" max="99" class="form-input" value="${Number(l.priority_order ?? 99)}">
                </div>
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
                <div>
                  <label style="display:block;font-size:10px;text-transform:uppercase;letter-spacing:.12em;color:var(--gold);font-family:var(--font-mono);margin-bottom:6px">Deal Types</label>
                  <select id="list-deal-types-${l.id}" class="form-input" multiple size="4">${buildMultiSelectOptions(DEAL_TYPE_OPTIONS, l.deal_types)}</select>
                </div>
                <div>
                  <label style="display:block;font-size:10px;text-transform:uppercase;letter-spacing:.12em;color:var(--gold);font-family:var(--font-mono);margin-bottom:6px">Sectors</label>
                  <select id="list-sectors-${l.id}" class="form-input" multiple size="6">${buildMultiSelectOptions(SECTOR_OPTIONS, l.sectors)}</select>
                </div>
              </div>
              <div style="display:flex;flex-wrap:wrap;gap:6px">
                ${renderTagGroup(l.sectors, '#1f2937', '#9ca3af')}
                ${renderTagGroup(l.deal_types, 'rgba(201,168,76,0.12)', 'var(--gold)')}
              </div>
              <div style="display:flex;justify-content:flex-end">
                <button class="btn btn-gold btn-sm" onclick="saveListMetadata('${l.id}')">Save Metadata</button>
              </div>
            </div>
          `).join('')}
        </div>
      </div>
    `;
  } catch (_) { panel.innerHTML = ''; }
}

const DEAL_TYPE_OPTIONS = ['buyout', 'independent_sponsor', 'growth_equity', 'secondaries'];
const SECTOR_OPTIONS = ['healthcare', 'manufacturing', 'distribution', 'business_services', 'software', 'industrial', 'consumer', 'financial_services'];

function formatListTypeLabel(value) {
  const labels = {
    deal_specific: 'Deal-Specific PitchBook',
    comparable_deals: 'Comparable Deals',
    standing: 'Standing List',
    news_research: 'News Research',
    manual: 'Other',
    knowledge_base: 'Knowledge Base',
    warm: 'Warm',
    standard: 'Standing List',
  };
  return labels[value] || 'Other';
}

function listTypeBadgeStyle(value) {
  const styles = {
    deal_specific: 'display:inline-flex;align-items:center;padding:4px 8px;border-radius:999px;background:rgba(201,168,76,0.16);color:var(--gold);font-size:10px;font-family:var(--font-mono);text-transform:uppercase;letter-spacing:.12em',
    comparable_deals: 'display:inline-flex;align-items:center;padding:4px 8px;border-radius:999px;background:rgba(56,189,248,0.14);color:#7dd3fc;font-size:10px;font-family:var(--font-mono);text-transform:uppercase;letter-spacing:.12em',
    standing: 'display:inline-flex;align-items:center;padding:4px 8px;border-radius:999px;background:rgba(148,163,184,0.14);color:#cbd5e1;font-size:10px;font-family:var(--font-mono);text-transform:uppercase;letter-spacing:.12em',
    news_research: 'display:inline-flex;align-items:center;padding:4px 8px;border-radius:999px;background:rgba(74,222,128,0.14);color:#86efac;font-size:10px;font-family:var(--font-mono);text-transform:uppercase;letter-spacing:.12em',
    manual: 'display:inline-flex;align-items:center;padding:4px 8px;border-radius:999px;background:rgba(244,114,182,0.14);color:#f9a8d4;font-size:10px;font-family:var(--font-mono);text-transform:uppercase;letter-spacing:.12em',
  };
  return styles[value] || styles.manual;
}

function buildListTypeOptions(selected) {
  const options = [
    ['deal_specific', 'Deal-Specific PitchBook'],
    ['comparable_deals', 'Comparable Deals'],
    ['standing', 'Standing List'],
    ['manual', 'Other'],
  ];
  return options.map(([value, label]) => `<option value="${value}" ${selected === value ? 'selected' : ''}>${label}</option>`).join('');
}

function buildMultiSelectOptions(options, selectedValues) {
  const selected = new Set((selectedValues || []).map(v => String(v)));
  return options.map(value => `<option value="${value}" ${selected.has(String(value)) ? 'selected' : ''}>${value.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}</option>`).join('');
}

function renderTagGroup(values, bg, color) {
  if (!values?.length) return '';
  return values.map(value => `
    <span style="padding:4px 8px;border-radius:999px;background:${bg};color:${color};font-size:11px">
      ${esc(String(value).replace(/_/g, ' '))}
    </span>
  `).join('');
}

function getMultiSelectValues(id) {
  const el = document.getElementById(id);
  if (!el) return [];
  return Array.from(el.selectedOptions).map(option => option.value);
}

async function saveListMetadata(id) {
  const payload = {
    name: document.getElementById(`list-input-${id}`)?.value?.trim(),
    list_type: document.getElementById(`list-type-${id}`)?.value || null,
    description: document.getElementById(`list-description-${id}`)?.value?.trim() || '',
    priority_order: Number(document.getElementById(`list-priority-${id}`)?.value || 99),
    deal_types: getMultiSelectValues(`list-deal-types-${id}`),
    sectors: getMultiSelectValues(`list-sectors-${id}`),
  };

  if (!payload.name) {
    showToast('List name is required', 'error');
    return;
  }

  try {
    await api(`/api/lists/${id}`, 'PUT', payload);
    showToast('List metadata updated');
    await loadDatabase();
  } catch (err) {
    showToast(`Failed: ${err.message}`, 'error');
  }
}

async function loadListsTab() {
  const container = document.getElementById('lists-table-container');
  if (!container) return;
  container.innerHTML = '<div class="loading-placeholder">Loading&#8230;</div>';
  try {
    const lists = await api('/api/lists');
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
                <div style="margin-top:6px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
                  <span style="${listTypeBadgeStyle(l.list_type)}">${esc(formatListTypeLabel(l.list_type))}</span>
                  <span style="font-size:12px;color:var(--text-dim)">${esc(l.description || 'No description')}</span>
                </div>
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
    const data = await api(`/api/lists/${listId}`, 'PUT', { name: newName });
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
  tbody.innerHTML = '<tr><td colspan="11" class="table-empty">Loading&#8230;</td></tr>';
  try {
    const params = new URLSearchParams({ page: contactsCurrentPage, limit: 50 });
    if (search) params.set('search', search);
    const data = await api(`/api/contacts-db/researched?${params}`);
    const rows = data.contacts || [];
    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="11" class="table-empty">
        <div style="padding:20px 0">
          <div style="font-size:24px;margin-bottom:8px">👥</div>
          <div>No contacts found.</div>
          <div style="font-size:12px;margin-top:4px;color:var(--text-muted)">Contacts tied to deals appear here, including closed conversations unless they were deleted or suppressed.</div>
        </div>
      </td></tr>`;
      renderPagination('db-contacts-pagination', 1, 1, 'loadContactsTable');
      return;
    }
    const renderDealsCell = (deals) => {
      if (!Array.isArray(deals) || deals.length === 0) return '<span style="color:#555">—</span>';
      return `<div style="display:flex;flex-wrap:wrap;gap:4px">${deals.map(d =>
        `<span style="padding:2px 8px;border-radius:4px;font-size:10px;border:1px solid #2a2a2a;background:#1a1a1a;color:#9ca3af">${esc(d.dealName || '—')}</span>`
      ).join('')}</div>`;
    };
    tbody.innerHTML = rows.map(r => `<tr
      style="cursor:pointer;transition:background 0.15s"
      onmouseover="this.style.background='var(--surface-2,#1a1a1a)'"
      onmouseout="this.style.background=''"
      onclick="openProspectDrawer('${r.id}', '${r.deal_id || ''}')">
      <td style="font-weight:500">${esc(r.name || '—')}</td>
      <td style="font-size:12px">${esc(r.company_name || '—')}</td>
      <td style="font-size:12px;color:var(--text-dim)">${esc(r.job_title || '—')}</td>
      <td>${r.activeDealName ? `<span class="status-badge" style="background:var(--gold-dim);color:var(--gold)">${esc(r.activeDealName)}</span>` : '<span style="color:#555">—</span>'}</td>
      <td>${r.projectName ? `<span class="status-badge" style="background:#1f2937;color:#9ca3af">${esc(r.projectName)}</span>` : '<span style="color:#555">—</span>'}</td>
      <td style="max-width:220px">${renderDealsCell(r.deals)}</td>
      <td style="font-size:11px;font-family:var(--font-mono)">${r.email ? `<span style="color:#4ade80">${esc(r.email)}</span>` : '<span style="color:#555">—</span>'}</td>
      <td style="font-size:12px">${r.linkedin_url ? `<a href="${esc(r.linkedin_url.startsWith('http') ? r.linkedin_url : 'https://'+r.linkedin_url)}" target="_blank" onclick="event.stopPropagation()" style="color:#0a66c2">LinkedIn</a>` : '<span style="color:#555">—</span>'}</td>
      <td style="font-size:11px;color:var(--text-dim);max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(r.pipeline_stage || '—')}</td>
      <td><span style="padding:2px 8px;border-radius:4px;font-size:11px;
        background:${r.enrichment_status==='Enriched'?'#064e3b':r.enrichment_status==='Partial'?'#1e3a5f':'#1a1a1a'};
        color:${r.enrichment_status==='Enriched'?'#4ade80':r.enrichment_status==='Partial'?'#60a5fa':'#6b7280'}">
        ${esc(r.enrichment_status || 'Raw')}</span></td>
      <td style="font-size:11px;color:var(--text-dim)">${r.updated_at ? r.updated_at.substring(0, 10) : '—'}</td>
    </tr>`).join('');
    renderPagination('db-contacts-pagination', contactsCurrentPage, data.pages || 1, 'loadContactsTable');
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="11" class="table-empty" style="color:var(--text-muted)">${err.message}</td></tr>`;
  }
}

function getContactTypeBadge(contact_type, is_angel) {
  if (is_angel || contact_type === 'angel')
    return `<span style="padding:2px 6px;border-radius:3px;font-size:10px;font-family:var(--font-mono);background:rgba(245,158,11,0.15);color:#f59e0b">Angel</span>`;
  if (contact_type === 'individual_at_firm')
    return `<span style="padding:2px 6px;border-radius:3px;font-size:10px;font-family:var(--font-mono);background:rgba(96,165,250,0.15);color:#60a5fa">Institutional</span>`;
  if (contact_type === 'firm')
    return `<span style="padding:2px 6px;border-radius:3px;font-size:10px;font-family:var(--font-mono);background:rgba(167,139,250,0.15);color:#a78bfa">Firm</span>`;
  return '';
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
      ? `<button onclick="event.stopPropagation();showInvestorDealHistory('${r.id}','${esc(r.name)}')" style="background:none;border:none;cursor:pointer;color:#60a5fa;font-size:11px;padding:0">${esc(r._active_deal)}</button>`
      : (r._deal_count > 0
        ? `<button onclick="event.stopPropagation();showInvestorDealHistory('${r.id}','${esc(r.name)}')" style="background:none;border:none;cursor:pointer;color:#6b7280;font-size:11px;padding:0">${r._deal_count} deal(s)</button>`
        : '<span style="color:#374151;font-size:11px">—</span>');
    const rowClick = `onclick="openInvestorDatabaseSidePanel('${r.id}')" style="cursor:pointer"`;
    return `<tr ${rowClick}>
      <td><span style="font-weight:500;color:var(--text-bright)">${esc(r.name || '—')}</span><br><span style="font-size:11px;color:var(--text-muted)">${esc(r.hq_country || r.hq_location || '')}</span>${r.description ? `<div style="font-size:11px;color:var(--text-dim);margin-top:4px;max-width:280px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(r.description)}</div>` : ''}</td>
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
let queuedExclusionFile = null; // XLSX files — uploaded server-side after deal creation

window.handleExclusionDrop = function(e) {
  e.preventDefault();
  document.getElementById('exclusion-drop-zone').style.borderColor = '#2a2a2a';
  const file = e.dataTransfer?.files?.[0];
  if (file) handleExclusionFile(file);
};

window.handleExclusionFile = function(file) {
  if (!file) return;
  const isXlsx = /\.(xlsx|xls)$/i.test(file.name);
  if (isXlsx) {
    // XLSX files are uploaded server-side after deal creation — just queue and preview the filename
    queuedExclusionFile = file;
    parsedExclusions = []; // will be parsed server-side
    const preview = document.getElementById('exclusion-preview');
    const count   = document.getElementById('exclusion-count');
    const sample  = document.getElementById('exclusion-sample');
    if (preview) preview.style.display = 'block';
    if (count)   count.textContent = `${file.name} queued`;
    if (sample)  sample.innerHTML  = 'Will be parsed and uploaded after deal is created.';
    return;
  }
  // CSV — parse client-side as before
  queuedExclusionFile = null;
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
  queuedExclusionFile = null;
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
    // Only show investor-type lists (not knowledge bases) in priority dropdown
    const lists = await api('/api/investor-lists?type=investors');
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

  // Load Knowledge Base dropdown separately
  const kbSel = document.getElementById('launch-kb-select');
  if (!kbSel) return;
  try {
    const kbLists = await api('/api/investor-lists?type=knowledge_base');
    kbSel.innerHTML = '<option value="">None \u2014 use standard database scoring only</option>';
    (kbLists || []).forEach(l => {
      const opt = document.createElement('option');
      opt.value = l.id;
      opt.dataset.name = l.name;
      const label = `${l.name} (${(l.investor_count || 0).toLocaleString()} records)`;
      opt.textContent = label;
      opt.title = label;
      kbSel.appendChild(opt);
    });
    // Also update the select's own title to show the selected option on hover
    kbSel.addEventListener('change', () => {
      const sel = kbSel.options[kbSel.selectedIndex];
      kbSel.title = sel ? sel.title || sel.textContent : '';
    });
  } catch (e) {
    console.warn('[KB] Failed to load knowledge bases:', e.message);
  }
}

async function loadEmailAccountOptions(selectedId) {
  const sel = document.getElementById('launch-email-account');
  if (!sel) return;
  try {
    const accounts = await api('/api/email-accounts');
    sel.innerHTML = '<option value="">No sending account selected…</option>';
    (accounts || []).forEach(a => {
      const opt = document.createElement('option');
      opt.value = a.connection_id;
      opt.dataset.email = a.email;
      opt.textContent = a.label || a.email;
      if (selectedId && a.connection_id === selectedId) opt.selected = true;
      sel.appendChild(opt);
    });
    if (!selectedId && accounts?.length === 1) sel.value = accounts[0].connection_id;
  } catch (e) {
    console.warn('[EMAIL ACCOUNTS] Failed to load:', e.message);
  }
}

// Queue of PitchBook files to upload after deal creation
window.pbFilesQueue = { investors: null, deals: null };

window.queuePbFile = function(file, type) {
  if (!file) return;
  window.pbFilesQueue[type] = file;
  const labelId = type === 'investors' ? 'pb-investors-label' : 'pb-deals-label';
  const label = document.getElementById(labelId);
  if (label) label.textContent = `\u2713 ${file.name}`;
};

async function uploadQueuedPbFiles(dealId, queueOverride = null) {
  const queue = queueOverride || window.pbFilesQueue || {};
  const results = {};
  for (const type of ['investors', 'deals']) {
    const file = queue[type];
    if (!file) continue;
    try {
      const fd = new FormData();
      fd.append('file', file);
      fd.append('fileType', type === 'deals' ? 'intelligence' : type);
      const res = await fetch(`/api/deals/${dealId}/import-intelligence`, {
        method: 'POST',
        body: fd,
        credentials: 'include',
      });
      if (!res.ok) {
        const msg = await res.text().catch(() => '');
        console.warn(`[PB IMPORT] ${type} upload failed:`, msg);
        results[type] = { ok: false, error: msg || `HTTP ${res.status}` };
      } else {
        const data = await res.json().catch(() => ({}));
        console.log(`[PB IMPORT] ${type} uploaded for deal ${dealId}`);
        results[type] = { ok: true, data };
      }
    } catch (e) {
      console.warn(`[PB IMPORT] ${type} error:`, e.message);
      results[type] = { ok: false, error: e.message };
    }
  }
  window.pbFilesQueue = { investors: null, deals: null };
  return results;
}

async function uploadQueuedExclusionFile(dealId, fileOverride = null) {
  const file = fileOverride || queuedExclusionFile;
  if (!file) return;
  try {
    const fd = new FormData();
    fd.append('file', file);
    const res = await fetch(`/api/deals/${dealId}/exclusions/upload`, { method: 'POST', body: fd });
    if (!res.ok) {
      const msg = await res.text().catch(() => '');
      console.warn('[EXCLUSION UPLOAD] failed:', msg);
    } else {
      const data = await res.json().catch(() => ({}));
      console.log(`[EXCLUSION UPLOAD] ${data.imported || '?'} exclusions imported for deal ${dealId}`);
    }
  } catch (e) {
    console.warn('[EXCLUSION UPLOAD] error:', e.message);
  }
  queuedExclusionFile = null;
}

window.uploadActiveDealFile = async function(file, dealId, fileType = null) {
  if (!file || !dealId) return;
  const btn = document.getElementById(`pb-upload-btn-${dealId}`);
  const origText = btn?.textContent;
  if (btn) { btn.textContent = 'Uploading…'; btn.disabled = true; }
  try {
    const fd = new FormData();
    fd.append('file', file);
    if (fileType) fd.append('fileType', fileType);
    const res = await fetch(`/api/deals/${dealId}/import-intelligence`, {
      method: 'POST',
      body: fd,
      credentials: 'include',
    });
    if (!res.ok) throw new Error(await res.text());
    const result = await res.json();
    showToast(result.message || 'File imported successfully');
    await loadDealTabSettings(dealId);
  } catch (e) {
    showToast(`Import failed: ${e.message}`, 'error');
  } finally {
    if (btn) { btn.textContent = origText; btn.disabled = false; }
  }
};

window.saveDealEmailAccount = async function(dealId) {
  const sel = document.getElementById(`ds-email-account-${dealId}`);
  if (!sel) return;
  const accountId = sel.value;
  const email = sel.options[sel.selectedIndex]?.dataset?.email || '';
  const btn = document.getElementById(`ds-email-account-btn-${dealId}`);
  const origText = btn?.textContent;
  if (btn) { btn.textContent = 'Saving…'; btn.disabled = true; }
  try {
    await api(`/api/deals/${dealId}`, 'PATCH', { sending_account_id: accountId, sending_email: email });
    showToast('Sending account updated');
  } catch (e) {
    showToast(`Failed: ${e.message}`, 'error');
  } finally {
    if (btn) { btn.textContent = origText; btn.disabled = false; }
  }
};

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

// ─── ANALYTICS PAGE — WEEKLY INTELLIGENCE BOOKS ──────────────────────────────

async function loadAnalyticsPage() {
  const container = document.getElementById('analytics-main-container');
  if (!container) return;

  container.innerHTML = `
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,500;0,600;1,400&family=DM+Mono:wght@300;400;500&display=swap');
      :root {
        --gold:#C9A84C; --gold-dim:rgba(201,168,76,0.12);
        --gold-b:rgba(201,168,76,0.25);
        --bg:#080809; --card:#0F0F11; --deep:#0A0A0C;
        --border:#1C1C1F; --text:#EDE9E3;
        --mid:#8A8680; --dim:#3A3835;
        --green:#4ADE80; --red:#F87171;
        --blue:#60A5FA; --amber:#FBBF24;
      }
      .ap { padding:32px; background:var(--bg); min-height:100vh;
            font-family:'DM Mono',monospace; }
      .ap-head { margin-bottom:48px; }
      .ap-title { font-family:'Playfair Display',serif; font-size:32px;
                  font-weight:400; color:var(--text); margin:0 0 6px; }
      .ap-sub { font-size:11px; color:var(--dim); text-transform:uppercase;
                letter-spacing:0.15em; }
      .ap-section { margin-bottom:34px; }
      .ap-section-head { display:flex; justify-content:space-between; align-items:flex-end; gap:20px; margin-bottom:14px; }
      .ap-section-title { font-family:'Playfair Display',serif; font-size:20px; color:var(--text); margin:0; font-weight:400; }
      .ap-section-sub { font-size:10px; color:var(--dim); text-transform:uppercase; letter-spacing:.14em; }
      .ap-tabs { display:flex; gap:10px; margin-bottom:24px; }
      .ap-tab { background:transparent; border:1px solid var(--border); color:var(--mid); padding:10px 14px; border-radius:999px; cursor:pointer; font-family:'DM Mono',monospace; font-size:11px; letter-spacing:.12em; text-transform:uppercase; transition:border-color .2s,color .2s,background .2s; }
      .ap-tab.active { border-color:var(--gold); color:var(--gold); background:var(--gold-dim); }
      .analytics-panel.hidden { display:none; }
      .books-grid { display:grid; grid-template-columns:repeat(4,120px);
                    gap:20px 20px; padding:4px 4px 24px; }
      .books-pgr { display:flex; align-items:center; gap:16px; margin-top:8px;
                   padding-top:16px; border-top:1px solid var(--border); }
      .books-pgr-btn { background:transparent; border:1px solid var(--border);
                       color:var(--mid); padding:7px 14px; border-radius:6px;
                       cursor:pointer; font-family:'DM Mono',monospace; font-size:11px;
                       transition:border-color .2s,color .2s; }
      .books-pgr-btn:hover:not(:disabled) { border-color:var(--gold); color:var(--gold); }
      .books-pgr-btn:disabled { opacity:0.3; cursor:default; }
      .books-pgr-info { font-size:11px; color:var(--mid); flex:1; text-align:center; }
      .book { width:120px; height:180px; cursor:pointer;
              transition:transform .3s,filter .3s; }
      .book:hover { transform:translateY(-14px) rotateY(-6deg);
                    filter:brightness(1.2); }
      .cover { width:100%; height:100%;
               background:linear-gradient(160deg,#1C1C1F 0%,#141416 60%,#0F0F11 100%);
               border-radius:4px 8px 8px 4px;
               border-left:6px solid var(--gold);
               border-top:1px solid #2a2a2a; border-right:1px solid #1a1a1a;
               border-bottom:1px solid #2a2a2a;
               display:flex; flex-direction:column;
               justify-content:space-between; padding:14px 10px 12px;
               position:relative;
               box-shadow:4px 4px 20px rgba(0,0,0,.7),
                          inset -1px 0 0 rgba(255,255,255,.02); }
      .cover::before { content:''; position:absolute; left:-6px; top:0;
                       width:6px; height:100%;
                       background:linear-gradient(90deg,#a88828,#C9A84C,#b8942e);
                       border-radius:4px 0 0 4px; }
      .ribbon { position:absolute; top:-4px; right:12px; width:14px; height:28px;
                background:var(--gold);
                clip-path:polygon(0 0,100% 0,100% 78%,50% 100%,0 78%); }
      .latest-b { position:absolute; top:9px; right:9px; font-size:7px;
                  padding:2px 5px; background:var(--gold-dim); color:var(--gold);
                  border:1px solid var(--gold-b); border-radius:2px;
                  text-transform:uppercase; letter-spacing:.15em; }
      .book-wn { font-family:'Playfair Display',serif; font-size:22px;
                 color:var(--gold); font-weight:500; line-height:1; }
      .book-lbl { font-size:8px; color:var(--dim); text-transform:uppercase;
                  letter-spacing:.15em; margin-top:2px; }
      .book-dates { font-size:9px; color:#5a5855; letter-spacing:.05em;
                    line-height:1.5; }
      .bst { font-size:8px; padding:3px 6px; border-radius:2px;
             text-transform:uppercase; letter-spacing:.1em; margin-top:8px;
             display:inline-block; }
      .st-gen { background:rgba(74,222,128,.12); color:#4ADE80;
                border:1px solid rgba(74,222,128,.2); }
      .st-pend { background:rgba(201,168,76,.1); color:#C9A84C;
                 border:1px solid rgba(201,168,76,.2); }
      .st-gen2 { background:rgba(96,165,250,.1); color:#60A5FA;
                 border:1px solid rgba(96,165,250,.2);
                 animation:pulse 1.5s ease-in-out infinite; }
      .st-fail { background:rgba(248,113,113,.1); color:#F87171;
                 border:1px solid rgba(248,113,113,.2); }
      @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.5} }
      .ov { position:fixed; inset:0; background:rgba(0,0,0,.88);
            z-index:600; display:flex; align-items:center;
            justify-content:center; padding:24px;
            animation:fadeIn .2s ease forwards; }
      @keyframes fadeIn { from{opacity:0} to{opacity:1} }
      .rp { background:var(--deep); border:1px solid var(--border);
            border-radius:10px; width:100%; max-width:860px;
            max-height:88vh; overflow-y:auto; scrollbar-width:thin;
            scrollbar-color:#2a2a2a transparent;
            animation:up .25s ease forwards; }
      @keyframes up { from{transform:translateY(24px)} to{transform:translateY(0)} }
      .rph { padding:26px 32px 22px; border-bottom:1px solid var(--border);
             display:flex; justify-content:space-between; align-items:flex-start;
             position:sticky; top:0; background:var(--deep); z-index:5; }
      .rp-tag { font-size:10px; color:var(--gold); text-transform:uppercase;
                letter-spacing:.2em; margin-bottom:6px; }
      .rp-hl { font-family:'Playfair Display',serif; font-size:20px;
               color:var(--text); font-weight:400; line-height:1.3;
               max-width:580px; margin:0; }
      .xbtn { background:none; border:1px solid #2a2a2a; color:#5a5855;
              width:32px; height:32px; border-radius:4px; cursor:pointer;
              font-size:18px; display:flex; align-items:center;
              justify-content:center; flex-shrink:0;
              transition:border-color .2s,color .2s; }
      .xbtn:hover { border-color:var(--gold); color:var(--gold); }
      .rpb { padding:26px 32px; }
      .sec-lbl { font-size:9px; color:var(--dim); text-transform:uppercase;
                 letter-spacing:.2em; margin-bottom:14px;
                 padding-bottom:8px; border-bottom:1px solid var(--border); }
      .mgrid { display:grid; grid-template-columns:repeat(4,1fr);
               gap:1px; background:var(--border); border-radius:8px;
               overflow:hidden; margin-bottom:28px; }
      .mc { background:var(--card); padding:18px; text-align:center; }
      .mv { font-family:'Playfair Display',serif; font-size:28px;
            color:var(--gold); margin-bottom:3px; font-weight:400; }
      .ml { font-size:9px; color:var(--dim); text-transform:uppercase;
            letter-spacing:.15em; }
      .ms { font-size:10px; color:#5a5855; margin-top:2px; }
      .crow { display:flex; align-items:center; gap:14px; margin-bottom:10px; }
      .cn { font-size:12px; color:var(--mid); width:130px; flex-shrink:0; }
      .cbg { flex:1; height:6px; background:var(--border); border-radius:3px;
             overflow:hidden; }
      .cf { height:100%; border-radius:3px; }
      .cr { font-size:12px; color:var(--text); width:48px; text-align:right; }
      .sg { display:grid; grid-template-columns:180px 1fr; gap:20px;
            margin-bottom:28px; align-items:center; }
      .sdial { text-align:center; }
      .sscore { font-family:'Playfair Display',serif; font-size:48px;
                font-weight:400; margin-bottom:4px; }
      .slbl { font-size:10px; color:var(--dim); text-transform:uppercase;
              letter-spacing:.15em; }
      .ig { display:grid; grid-template-columns:1fr 1fr; gap:10px;
            margin-bottom:28px; }
      .ic { background:var(--card); border:1px solid var(--border);
            border-radius:6px; padding:18px; }
      .ic.fw { grid-column:1/-1; }
      .icl { font-size:9px; color:var(--gold); text-transform:uppercase;
             letter-spacing:.2em; margin-bottom:10px; }
      .ict { font-size:13px; color:var(--mid); line-height:1.7; }
      .acts { display:flex; flex-direction:column; gap:8px; }
      .ai { display:flex; gap:14px; align-items:flex-start; padding:12px 16px;
            background:rgba(201,168,76,.05);
            border:1px solid rgba(201,168,76,.12); border-radius:6px; }
      .an { font-family:'Playfair Display',serif; font-size:18px;
            color:var(--gold); flex-shrink:0; line-height:1; margin-top:1px; }
      .at { font-size:13px; color:var(--mid); line-height:1.6; }
      .pend-c { padding:48px 32px; text-align:center; }
      .pend-ico { font-size:40px; margin-bottom:16px; }
      .pend-t { font-family:'Playfair Display',serif; font-size:20px;
                color:var(--text); margin-bottom:8px; }
      .pend-s { font-size:13px; color:var(--dim); }
    </style>

    <div class="ap">
      <div class="ap-head">
        <h1 class="ap-title">Intelligence</h1>
        <div class="ap-sub">Weekly reports plus end-of-day operating logs in America/New_York time</div>
      </div>
      <div class="ap-tabs">
        <button class="ap-tab active" id="analytics-tab-daily" onclick="switchAnalyticsTab('daily')">Daily Logs</button>
        <button class="ap-tab" id="analytics-tab-weekly" onclick="switchAnalyticsTab('weekly')">Weekly Reports</button>
      </div>
      <div id="analytics-panel-daily" class="analytics-panel">
        <div class="ap-section">
          <div class="ap-section-head">
            <div>
              <h2 class="ap-section-title">Daily Logs</h2>
              <div class="ap-section-sub">Generated automatically at the end of each America/New_York day</div>
            </div>
          </div>
          <div id="daily-log-shelf">
            <div style="color:#3A3835;font-size:12px;padding:40px 0">Loading daily logs...</div>
          </div>
        </div>
      </div>
      <div id="analytics-panel-weekly" class="analytics-panel hidden">
        <div class="ap-section">
          <div class="ap-section-head">
            <div>
              <h2 class="ap-section-title">Weekly Reports</h2>
              <div class="ap-section-sub">Generated automatically every Monday at 9am EST</div>
            </div>
          </div>
          <div id="roco-shelf">
            <div style="color:#3A3835;font-size:12px;padding:40px 0">Loading reports...</div>
          </div>
        </div>
      </div>
    </div>
  `;

  // Reset caches so fresh data loads on each analytics page open
  _dailyLogData = null;
  _weeklyData = null;
  analyticsPageState.daily = 0;
  analyticsPageState.weekly = 0;
  renderDailyLogShelf();
  renderShelf();
  window.switchAnalyticsTab('daily');
}

window.switchAnalyticsTab = function(tab) {
  const dailyBtn = document.getElementById('analytics-tab-daily');
  const weeklyBtn = document.getElementById('analytics-tab-weekly');
  const dailyPanel = document.getElementById('analytics-panel-daily');
  const weeklyPanel = document.getElementById('analytics-panel-weekly');
  if (!dailyBtn || !weeklyBtn || !dailyPanel || !weeklyPanel) return;

  const showDaily = tab !== 'weekly';
  dailyBtn.classList.toggle('active', showDaily);
  weeklyBtn.classList.toggle('active', !showDaily);
  dailyPanel.classList.toggle('hidden', !showDaily);
  weeklyPanel.classList.toggle('hidden', showDaily);
};

// ─── Analytics shelf pagination state ──────────────────────────────────────
const analyticsPageState = { daily: 0, weekly: 0 };
const BOOKS_PER_PAGE = 12; // 4 columns × 3 rows

function renderBooksPage(containerId, items, pageKey, renderCardFn) {
  const container = document.getElementById(containerId);
  if (!container) return;
  const page = analyticsPageState[pageKey] || 0;
  const totalPages = Math.max(1, Math.ceil(items.length / BOOKS_PER_PAGE));
  // Clamp page within bounds
  analyticsPageState[pageKey] = Math.min(Math.max(0, page), totalPages - 1);
  const safePage = analyticsPageState[pageKey];
  const pageItems = items.slice(safePage * BOOKS_PER_PAGE, (safePage + 1) * BOOKS_PER_PAGE);

  const gridHtml = `<div class="books-grid">${pageItems.map(renderCardFn).join('')}</div>`;
  const pgrHtml = totalPages > 1 ? `
    <div class="books-pgr">
      <button class="books-pgr-btn" ${safePage === 0 ? 'disabled' : ''}
        onclick="analyticsPageState['${pageKey}']--;renderBooksPageRefresh('${containerId}','${pageKey}')">← Prev</button>
      <span class="books-pgr-info">Page ${safePage + 1} of ${totalPages}</span>
      <button class="books-pgr-btn" ${safePage >= totalPages - 1 ? 'disabled' : ''}
        onclick="analyticsPageState['${pageKey}']++;renderBooksPageRefresh('${containerId}','${pageKey}')">Next →</button>
    </div>` : '';

  container.innerHTML = gridHtml + pgrHtml;
}

// Called by inline onclick handlers to re-render the page after navigating
window.renderBooksPageRefresh = function(containerId, pageKey) {
  if (pageKey === 'daily') _renderDailyLogGrid();
  else _renderWeeklyGrid();
};

// Cached data for re-renders without refetching
let _weeklyData = null;
let _dailyLogData = null;

async function renderShelf() {
  const container = document.getElementById('roco-shelf');
  if (!container) return;

  // Use cache if available (pagination re-render)
  if (_weeklyData) {
    _renderWeeklyGrid();
    return;
  }

  try {
    const weeks = await api('/api/analytics/weeks');
    _weeklyData = Array.isArray(weeks) ? weeks : [];
  } catch {
    container.innerHTML = '<div style="color:#3A3835;font-size:12px;padding:40px 0">Could not load reports.</div>';
    return;
  }

  if (!_weeklyData.length) {
    container.innerHTML = '<div style="color:#3A3835;font-size:12px;padding:40px 0">No reports yet. First report generates automatically next Monday.</div>';
    return;
  }

  _renderWeeklyGrid();
}

function _renderWeeklyGrid() {
  if (!_weeklyData) return;
  const sorted = [..._weeklyData].sort((a, b) => new Date(a.week_start) - new Date(b.week_start));

  renderBooksPage('roco-shelf', sorted, 'weekly', (w) => {
    const isLatest = sorted.indexOf(w) === sorted.length - 1;
    const isGen    = w.status === 'generated';
    const isGenning = w.status === 'generating';
    const isFailed = w.status === 'failed';
    const stClass  = isGen ? 'st-gen' : isGenning ? 'st-gen2' : isFailed ? 'st-fail' : 'st-pend';
    const stText   = isGen ? 'Ready' : isGenning ? 'Generating...' : isFailed ? 'Failed' : 'Pending';
    const d1 = new Date(w.week_start);
    const d2 = new Date(w.week_end);
    const fmtD = d => d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
    return `
      <div class="book" onclick="window.openBook('${esc(w.week_start)}')">
        <div class="cover">
          ${isLatest ? '<div class="latest-b">Latest</div>' : ''}
          ${isGen    ? '<div class="ribbon"></div>'          : ''}
          <div>
            <div class="book-wn">W${w.week_number}</div>
            <div class="book-lbl">Week ${w.week_number}</div>
          </div>
          <div>
            <div class="book-dates">${fmtD(d1)}<br>${fmtD(d2)}</div>
            <div class="bst ${stClass}">${stText}</div>
          </div>
        </div>
      </div>`;
  });
}

async function renderDailyLogShelf() {
  const container = document.getElementById('daily-log-shelf');
  if (!container) return;

  // Re-render from cache if pagination triggered (no new fetch needed)
  if (_dailyLogData) {
    _renderDailyLogGrid();
    return;
  }

  try {
    const logs = await api('/api/analytics/daily-logs');
    _dailyLogData = Array.isArray(logs) ? logs : [];
  } catch {
    container.innerHTML = '<div style="color:#3A3835;font-size:12px;padding:40px 0">Could not load daily logs.</div>';
    return;
  }

  if (!_dailyLogData.length) {
    container.innerHTML = '<div style="color:#3A3835;font-size:12px;padding:40px 0">No daily logs yet. The first end-of-day log will appear automatically.</div>';
    return;
  }

  _renderDailyLogGrid();
}

function _renderDailyLogGrid() {
  if (!_dailyLogData) return;
  const sorted = [..._dailyLogData].sort((a, b) => new Date(a.report_date) - new Date(b.report_date));
  renderBooksPage('daily-log-shelf', sorted, 'daily', (log) => {
    const isLatest = sorted.indexOf(log) === sorted.length - 1;
    const isGen = log.status === 'generated';
    const isFailed = log.status === 'failed';
    const stClass = isGen ? 'st-gen' : isFailed ? 'st-fail' : 'st-pend';
    const stText = isGen ? 'Ready' : isFailed ? 'Failed' : 'Pending';
    const date = new Date(`${log.report_date}T12:00:00`);
    const day = date.toLocaleDateString('en-GB', { day: 'numeric' });
    const month = date.toLocaleDateString('en-GB', { month: 'short' });
    return `
      <div class="book" onclick="window.openDailyLog('${esc(log.report_date)}')">
        <div class="cover">
          ${isLatest ? '<div class="latest-b">Latest</div>' : ''}
          ${isGen ? '<div class="ribbon"></div>' : ''}
          <div>
            <div class="book-wn">${day}</div>
            <div class="book-lbl">${month}</div>
          </div>
          <div>
            <div class="book-dates">${esc(log.headline || 'Daily Log')}<br>${log.deals_covered || 0} deals · ${log.activity_count || 0} actions</div>
            <div class="bst ${stClass}">${stText}</div>
          </div>
        </div>
      </div>`;
  });
}

window.openBook = async function(weekStart) {
  let weeks;
  try {
    weeks = await api('/api/analytics/weeks');
    if (!Array.isArray(weeks)) weeks = [];
  } catch { return; }

  const w = weeks.find(x => x.week_start === weekStart);
  if (!w) return;

  const d1 = new Date(w.week_start);
  const d2 = new Date(w.week_end);
  const fmtFull = d => d.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' });
  const sentColor = w.avg_sentiment >= 0.3 ? '#4ADE80'
    : w.avg_sentiment <= -0.3 ? '#F87171' : '#FBBF24';
  const sentLabel = w.avg_sentiment >= 0.3 ? 'Positive'
    : w.avg_sentiment <= -0.3 ? 'Negative' : 'Neutral';
  const raw = w.raw_recommendations || {};

  const ov = document.createElement('div');
  ov.className = 'ov';
  ov.addEventListener('click', e => { if (e.target === ov) ov.remove(); });

  const inner = w.status !== 'generated' ? `
    <div class="pend-c">
      <div class="pend-ico">${w.status === 'generating' ? '⟳' : w.status === 'failed' ? '⚠' : '📋'}</div>
      <div class="pend-t">${w.status === 'generating' ? 'Generating...' : w.status === 'failed' ? 'Generation Failed' : 'Report Pending'}</div>
      <div class="pend-s">
        ${w.status === 'generating'
          ? "SAGE is analysing this week's data. Check back in a moment."
          : w.status === 'failed'
            ? esc(raw.error || 'This report failed to save. The watchdog will retry automatically.')
          : 'This report will be generated automatically next Monday at 9am EST.'}
      </div>
    </div>` : `
    <div class="rpb">
      <div class="sec-lbl">Key Metrics</div>
      <div class="mgrid">
        <div class="mc"><div class="mv">${w.emails_sent || 0}</div><div class="ml">Emails Sent</div><div class="ms">${w.email_reply_rate || 0}% reply rate</div></div>
        <div class="mc"><div class="mv">${w.linkedin_invites_sent || 0}</div><div class="ml">LI Invites</div><div class="ms">${w.linkedin_acceptance_rate || 0}% acceptance</div></div>
        <div class="mc"><div class="mv">${w.linkedin_dms_sent || 0}</div><div class="ml">LI DMs</div><div class="ms">${w.linkedin_dm_reply_rate || 0}% reply rate</div></div>
        <div class="mc"><div class="mv">${w.meetings_booked || 0}</div><div class="ml">Meetings</div><div class="ms">booked this week</div></div>
      </div>
      <div class="sec-lbl">Channel Performance</div>
      ${[['Email Reply Rate', w.email_reply_rate || 0, '#60A5FA'], ['LI Acceptance Rate', w.linkedin_acceptance_rate || 0, '#4ADE80'], ['LI DM Reply Rate', w.linkedin_dm_reply_rate || 0, '#A78BFA']].map(([label, rate, color]) => `
      <div class="crow">
        <div class="cn">${label}</div>
        <div class="cbg"><div class="cf" style="width:${Math.min(rate * 3, 100)}%;background:${color};box-shadow:0 0 6px ${color}44"></div></div>
        <div class="cr">${rate}%</div>
      </div>`).join('')}
      <div class="sec-lbl" style="margin-top:22px">Conversation Sentiment</div>
      <div class="sg">
        <div class="sdial">
          <div class="sscore" style="color:${sentColor}">${w.avg_sentiment >= 0 ? '+' : ''}${(w.avg_sentiment || 0).toFixed(2)}</div>
          <div class="slbl">${sentLabel}</div>
        </div>
        <div>
          ${[['Positive', w.sentiment_breakdown?.positive || 0, '#4ADE80'], ['Neutral', w.sentiment_breakdown?.neutral || 0, '#FBBF24'], ['Negative', w.sentiment_breakdown?.negative || 0, '#F87171']].map(([l, p, c]) => `
          <div class="crow">
            <div class="cn" style="width:80px">${l}</div>
            <div class="cbg"><div class="cf" style="width:${p}%;background:${c}"></div></div>
            <div class="cr">${p}%</div>
          </div>`).join('')}
        </div>
      </div>
      <div class="sec-lbl">Intelligence</div>
      <div class="ig">
        <div class="ic"><div class="icl">What Worked</div><div class="ict">${esc(w.what_worked || 'Collecting data...')}</div></div>
        <div class="ic"><div class="icl">What Underperformed</div><div class="ict">${esc(w.what_didnt_work || 'Collecting data...')}</div></div>
        <div class="ic"><div class="icl">Best Investor Profile</div><div class="ict">${esc(w.best_investor_profile || 'Collecting data...')}</div></div>
        <div class="ic"><div class="icl">Best Sending Time</div><div class="ict">${esc(w.best_sending_time || 'Collecting data...')}</div></div>
        <div class="ic fw"><div class="icl">Template Direction — Next Week</div><div class="ict">${esc(w.template_recommendations || 'Collecting data...')}</div></div>
      </div>
      <div class="sec-lbl">Three Actions for Next Week</div>
      <div class="acts">
        ${(raw.three_actions || ['Gather more data.', 'Review investor match quality.', 'Check template performance.']).map((a, i) => `
        <div class="ai"><div class="an">${i + 1}</div><div class="at">${esc(a)}</div></div>`).join('')}
      </div>
      ${raw.trend_vs_last_week ? `<div style="margin-top:22px;padding:14px 18px;background:var(--card);border:1px solid var(--border);border-radius:6px"><div class="icl">Trend vs Last Week</div><div class="ict">${esc(raw.trend_vs_last_week)}</div></div>` : ''}
    </div>`;

  ov.innerHTML = `
    <div class="rp">
      <div class="rph">
        <div>
          <div class="rp-tag">Week ${w.week_number} · ${fmtFull(d1)} — ${fmtFull(d2)}</div>
          <p class="rp-hl">${esc(w.headline || 'Weekly Intelligence Report')}</p>
        </div>
        <button class="xbtn" onclick="this.closest('.ov').remove()">×</button>
      </div>
      ${inner}
    </div>`;

  document.body.appendChild(ov);
};

window.openDailyLog = async function(reportDate) {
  let logs;
  try {
    logs = await api('/api/analytics/daily-logs');
    if (!Array.isArray(logs)) logs = [];
  } catch { return; }

  const log = logs.find(entry => entry.report_date === reportDate);
  if (!log) return;

  const date = new Date(`${log.report_date}T12:00:00`);
  const fmtFull = date.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' });
  const ov = document.createElement('div');
  ov.className = 'ov';
  ov.addEventListener('click', e => { if (e.target === ov) ov.remove(); });

  const dealSections = Array.isArray(log.deal_sections) ? log.deal_sections : [];
  const inner = log.status !== 'generated' ? `
    <div class="pend-c">
      <div class="pend-ico">${log.status === 'failed' ? '⚠' : '📋'}</div>
      <div class="pend-t">${log.status === 'failed' ? 'Daily Log Failed' : 'Daily Log Pending'}</div>
      <div class="pend-s">${esc(log.raw_payload?.error || log.raw_payload?.message || 'This daily log is not ready yet.')}</div>
    </div>` : `
    <div class="rpb">
      <div class="sec-lbl">Daily Summary</div>
      <div class="ic fw" style="margin-bottom:22px"><div class="icl">Executive Summary</div><div class="ict">${esc(log.executive_summary || 'No executive summary saved.')}</div></div>
      <div class="mgrid">
        <div class="mc"><div class="mv">${log.activity_count || 0}</div><div class="ml">Activity Events</div><div class="ms">captured today</div></div>
        <div class="mc"><div class="mv">${log.deals_covered || 0}</div><div class="ml">Deals Covered</div><div class="ms">with progress checks</div></div>
        <div class="mc"><div class="mv">${dealSections.length}</div><div class="ml">Deal Notes</div><div class="ms">written summary cards</div></div>
        <div class="mc"><div class="mv">${log.voice_name ? esc(log.voice_name) : 'Text'}</div><div class="ml">Voice Note</div><div class="ms">${log.voice_note_sent_at ? 'sent to Telegram' : 'not sent'}</div></div>
      </div>
      <div class="sec-lbl">Per-Deal Operating Notes</div>
      <div class="ig">
        ${dealSections.length ? dealSections.map(section => `
          <div class="ic fw">
            <div class="icl">${esc(section.deal_name || 'Deal')}</div>
            <div class="ict"><strong>${esc(section.target_status || section.progress_status || 'Status')}</strong><br>${esc(section.summary || 'No summary saved.')}</div>
            ${(section.key_actions || []).length ? `<div class="acts" style="margin-top:14px">${section.key_actions.map((action, index) => `<div class="ai"><div class="an">${index + 1}</div><div class="at">${esc(action)}</div></div>`).join('')}</div>` : ''}
            ${section.next_move ? `<div style="margin-top:14px;padding:14px 18px;background:var(--card);border:1px solid var(--border);border-radius:6px"><div class="icl">Next Move</div><div class="ict">${esc(section.next_move)}</div></div>` : ''}
          </div>`).join('') : '<div class="ic fw"><div class="ict">No deal-specific notes were saved for this day.</div></div>'}
      </div>
      ${log.voice_script ? `<div class="sec-lbl">Voice Script</div><div class="ic fw"><div class="ict">${esc(log.voice_script)}</div></div>` : ''}
    </div>`;

  ov.innerHTML = `
    <div class="rp">
      <div class="rph">
        <div>
          <div class="rp-tag">Daily Log · ${fmtFull}</div>
          <p class="rp-hl">${esc(log.headline || 'Daily Operating Log')}</p>
        </div>
        <button class="xbtn" onclick="this.closest('.ov').remove()">×</button>
      </div>
      ${inner}
    </div>`;

  document.body.appendChild(ov);
};

async function loadMeetingTranscriptsPage() {
  const container = document.getElementById('transcripts-main-container');
  if (!container) return;
  container.innerHTML = `
    <div class="section-header">
      <h1 class="section-title">Meeting Transcripts</h1>
      <button class="btn btn-gold btn-sm" onclick="openTranscriptUploadModal()">Upload Transcript</button>
    </div>
    <div class="card" style="padding:28px;border-radius:22px;background:linear-gradient(180deg,rgba(18,20,26,.96),rgba(11,12,16,.98));border:1px solid rgba(212,168,71,.14);box-shadow:0 28px 80px rgba(0,0,0,.28)">
      <div style="display:flex;justify-content:space-between;align-items:flex-end;gap:18px;flex-wrap:wrap;margin-bottom:22px">
        <div>
          <div style="font-size:12px;letter-spacing:.14em;text-transform:uppercase;color:var(--gold);margin-bottom:8px">Transcript Archive</div>
          <div style="font-size:13px;color:var(--text-dim);max-width:760px;line-height:1.7">Every uploaded meeting transcript is saved to the historical log and remains available here for follow-up context, investor memory, and deal review.</div>
        </div>
        <div id="transcripts-summary" style="padding:12px 16px;border-radius:16px;background:rgba(212,168,71,.08);border:1px solid rgba(212,168,71,.16);font-size:12px;color:var(--text-dim)">Loading history...</div>
      </div>
      <div id="transcripts-list" style="color:var(--text-dim)"></div>
    </div>
  `;

  try {
    const rows = await api('/api/meeting-transcripts');
    const list = document.getElementById('transcripts-list');
    const summary = document.getElementById('transcripts-summary');
    if (!list) return;
    if (summary) {
      summary.innerHTML = `<div style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--gold);margin-bottom:4px">Saved Entries</div><div style="font-size:22px;color:var(--text-bright);font-family:'DM Mono',monospace">${fmt((rows || []).length)}</div>`;
    }
    if (!Array.isArray(rows) || !rows.length) {
      list.innerHTML = '<div style="color:var(--text-dim);padding:32px 0;font-size:13px">No transcripts uploaded yet.</div>';
      return;
    }
    list.innerHTML = rows.map(row => `
      <div style="padding:22px 0;border-bottom:1px solid rgba(255,255,255,.06)">
        <div style="display:flex;justify-content:space-between;gap:20px;align-items:flex-start;flex-wrap:wrap;margin-bottom:14px">
          <div style="min-width:260px;flex:1">
            <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:8px">
              <div style="font-size:16px;color:var(--text-bright);font-weight:600">${esc(row.investor_name || 'Unknown investor')}</div>
              <span style="padding:4px 9px;border-radius:999px;background:${row.is_new_investor ? 'rgba(34,197,94,.12)' : 'rgba(96,165,250,.12)'};border:1px solid ${row.is_new_investor ? 'rgba(34,197,94,.2)' : 'rgba(96,165,250,.2)'};font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:${row.is_new_investor ? '#86efac' : '#93c5fd'}">${row.is_new_investor ? 'New Investor' : 'Existing Investor'}</span>
              ${row.deal_name ? `<span style="padding:4px 9px;border-radius:999px;background:rgba(212,168,71,.1);border:1px solid rgba(212,168,71,.18);font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:var(--gold)">${esc(row.deal_name)}</span>` : ''}
            </div>
            <div style="font-size:12px;color:var(--text-dim);line-height:1.7">${esc(row.investor_email || row.investor_linkedin || row.investor_phone || 'Investor record')}</div>
          </div>
          <div style="text-align:right;min-width:160px">
            <div style="font-size:12px;color:${Number(row.sentiment_score || 0) >= 8 ? '#4ade80' : Number(row.sentiment_score || 0) >= 5 ? '#fbbf24' : '#f87171'}">${row.sentiment_score ? `Sentiment ${row.sentiment_score}/10` : 'Pending analysis'}</div>
            <div style="font-size:11px;color:var(--text-dim)">${row.created_at ? new Date(row.created_at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) : '—'}</div>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:minmax(0,1.7fr) minmax(240px,1fr);gap:18px;align-items:start">
          <div style="padding:18px;border-radius:16px;background:rgba(255,255,255,.025);border:1px solid rgba(255,255,255,.06)">
            <div style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--gold);margin-bottom:8px">Summary</div>
            <div style="font-size:13px;color:#d8dee9;line-height:1.8">${esc(row.summary || 'No summary saved yet.')}</div>
            ${row.transcript_text ? `<div style="margin-top:14px;font-size:12px;color:var(--text-dim);line-height:1.7">${esc(String(row.transcript_text).replace(/\s+/g, ' ').trim().slice(0, 260))}${String(row.transcript_text).length > 260 ? '…' : ''}</div>` : ''}
          </div>
          <div style="padding:18px;border-radius:16px;background:rgba(212,168,71,.05);border:1px solid rgba(212,168,71,.12)">
            <div style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--gold);margin-bottom:10px">Follow Up Actions</div>
            ${(Array.isArray(row.follow_up_actions) && row.follow_up_actions.length)
              ? row.follow_up_actions.slice(0, 4).map(action => `<div style="font-size:12px;color:#d8dee9;line-height:1.7;margin-bottom:8px">• ${esc(action)}</div>`).join('')
              : '<div style="font-size:12px;color:var(--text-dim)">No follow-up actions saved.</div>'}
          </div>
        </div>
      </div>
    `).join('');
  } catch (err) {
    const list = document.getElementById('transcripts-list');
    if (list) list.innerHTML = `<div style="color:#ef4444">Failed to load transcripts: ${esc(err.message)}</div>`;
  }
}

async function openTranscriptUploadModal() {
  const modal = document.createElement('div');
  modal.id = 'transcript-upload-modal';
  modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:2000;display:flex;align-items:center;justify-content:center;padding:24px';
  modal.onclick = e => { if (e.target === modal) modal.remove(); };
  modal.innerHTML = `
    <div style="width:100%;max-width:920px;max-height:88vh;overflow-y:auto;background:linear-gradient(180deg,#0b0d11,#090a0d);border:1px solid rgba(212,168,71,.14);border-radius:24px;padding:30px;box-shadow:0 36px 90px rgba(0,0,0,.4)">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:18px;margin-bottom:24px">
        <div>
          <div style="font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--gold);margin-bottom:10px">Meeting Memory</div>
          <h3 style="margin:0;color:#f8fafc;font-size:24px;font-weight:500">Upload Transcript</h3>
          <div style="margin-top:8px;color:var(--text-dim);font-size:13px;line-height:1.7;max-width:620px">Save every investor meeting into the transcript archive, enrich the investor record, and keep the full historical context available for future outreach and follow-up.</div>
        </div>
        <button onclick="document.getElementById('transcript-upload-modal').remove()" style="background:none;border:none;color:#6b7280;font-size:24px;cursor:pointer;line-height:1">×</button>
      </div>
      <div style="display:grid;grid-template-columns:1.15fr .85fr;gap:18px;margin-bottom:18px">
        <div style="padding:18px;border-radius:18px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06)">
          <div style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--gold);margin-bottom:12px">Investor Source</div>
          <input type="hidden" id="transcript-mode" value="existing" />
          <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px">
            <button type="button" id="transcript-mode-existing-btn" onclick="setTranscriptInvestorMode('existing')" style="padding:15px 16px;border-radius:16px;border:1px solid rgba(96,165,250,.24);background:rgba(96,165,250,.12);color:#dbeafe;text-align:left;cursor:pointer">
              <div style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#93c5fd;margin-bottom:6px">Existing Investor</div>
              <div style="font-size:13px;line-height:1.6;color:#cbd5e1">Link this transcript to someone already in the pipeline or database.</div>
            </button>
            <button type="button" id="transcript-mode-new-btn" onclick="setTranscriptInvestorMode('new')" style="padding:15px 16px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.02);color:#cbd5e1;text-align:left;cursor:pointer">
              <div style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#86efac;margin-bottom:6px">New Investor</div>
              <div style="font-size:13px;line-height:1.6;color:var(--text-dim)">Create a fresh investor record from this meeting and save it into history.</div>
            </button>
          </div>
        </div>
        <div style="padding:18px;border-radius:18px;background:rgba(212,168,71,.05);border:1px solid rgba(212,168,71,.14)">
          <div style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--gold);margin-bottom:12px">Source Material</div>
          <label style="display:block;padding:16px;border-radius:16px;border:1px dashed rgba(212,168,71,.26);background:rgba(255,255,255,.02);cursor:pointer">
            <div style="font-size:13px;color:#e5e7eb;margin-bottom:6px">Upload transcript file</div>
            <div style="font-size:12px;color:var(--text-dim);line-height:1.6">PDF, DOC, DOCX, TXT or Markdown. You can also paste the transcript manually below.</div>
            <input class="form-input" id="transcript-file" type="file" accept=".pdf,.doc,.docx,.txt,.md" style="margin-top:14px" />
          </label>
        </div>
      </div>
      <div id="transcript-existing-wrap" style="margin-top:10px;padding:22px;border-radius:20px;background:rgba(255,255,255,.025);border:1px solid rgba(255,255,255,.06)">
        <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-end;flex-wrap:wrap;margin-bottom:14px">
          <div>
            <div style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--gold);margin-bottom:8px">Existing Investor</div>
            <div style="font-size:13px;color:var(--text-dim)">Search by name, firm, or email and attach the meeting to the existing person record.</div>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr;gap:14px">
          <div>
            <label class="form-label" style="margin-bottom:8px;display:block">Search</label>
            <input class="form-input" id="transcript-contact-search" placeholder="Search database name, email, or firm" oninput="searchTranscriptExisting(this.value)" />
          </div>
          <div>
            <label class="form-label" style="margin-bottom:8px;display:block">Select Investor</label>
            <select class="form-input" id="transcript-contact"><option value="">Select contact</option></select>
          </div>
        </div>
      </div>
      <div id="transcript-new-wrap" style="display:none;margin-top:10px;padding:22px;border-radius:20px;background:rgba(34,197,94,.035);border:1px solid rgba(34,197,94,.12)">
        <div style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#86efac;margin-bottom:8px">New Investor Record</div>
        <div style="font-size:13px;color:var(--text-dim);margin-bottom:16px;line-height:1.7">Create a fresh investor profile from this meeting. The transcript analysis will enrich the contact and investor database record after upload.</div>
        <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px">
          <div><label class="form-label" style="margin-bottom:8px;display:block">Name</label><input class="form-input" id="transcript-new-name" /></div>
          <div><label class="form-label" style="margin-bottom:8px;display:block">Email</label><input class="form-input" id="transcript-new-email" /></div>
          <div><label class="form-label" style="margin-bottom:8px;display:block">Firm Name</label><input class="form-input" id="transcript-new-firm" /></div>
          <div><label class="form-label" style="margin-bottom:8px;display:block">Category</label><select class="form-input" id="transcript-new-category"><option value="institutional">Institutional</option><option value="angel">Angel / UHNW</option><option value="athlete">Athlete / Creator</option><option value="family_office">Family Office</option></select></div>
          <div><label class="form-label" style="margin-bottom:8px;display:block">Phone</label><input class="form-input" id="transcript-new-phone" /></div>
          <div><label class="form-label" style="margin-bottom:8px;display:block">LinkedIn URL</label><input class="form-input" id="transcript-new-linkedin" /></div>
        </div>
      </div>
      <div style="margin-top:18px;padding:22px;border-radius:20px;background:rgba(255,255,255,.025);border:1px solid rgba(255,255,255,.06)">
        <label class="form-label" style="margin-bottom:10px;display:block">Transcript Text</label>
        <textarea class="form-input" id="transcript-text" rows="14" style="min-height:280px;line-height:1.75" placeholder="Optional if you upload PDF/DOC/DOCX/TXT."></textarea>
      </div>
      <div style="display:flex;justify-content:flex-end;gap:12px;margin-top:22px;padding-top:18px;border-top:1px solid rgba(255,255,255,.06)">
        <button class="btn btn-ghost" onclick="document.getElementById('transcript-upload-modal').remove()">Cancel</button>
        <button class="btn btn-gold" onclick="submitTranscriptUpload()">Submit</button>
      </div>
    </div>
  `;
  document.body.appendChild(modal);
}

window.setTranscriptInvestorMode = function(mode) {
  const input = document.getElementById('transcript-mode');
  if (input) input.value = mode;
  window.toggleTranscriptInvestorMode();
};

window.toggleTranscriptInvestorMode = function() {
  const mode = document.getElementById('transcript-mode')?.value || 'existing';
  const existingWrap = document.getElementById('transcript-existing-wrap');
  const newWrap = document.getElementById('transcript-new-wrap');
  const existingBtn = document.getElementById('transcript-mode-existing-btn');
  const newBtn = document.getElementById('transcript-mode-new-btn');
  if (existingWrap) existingWrap.style.display = mode === 'existing' ? '' : 'none';
  if (newWrap) newWrap.style.display = mode === 'new' ? '' : 'none';
  if (existingBtn) existingBtn.style.cssText = mode === 'existing'
    ? 'padding:15px 16px;border-radius:16px;border:1px solid rgba(96,165,250,.24);background:rgba(96,165,250,.12);color:#dbeafe;text-align:left;cursor:pointer'
    : 'padding:15px 16px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.02);color:#cbd5e1;text-align:left;cursor:pointer';
  if (newBtn) newBtn.style.cssText = mode === 'new'
    ? 'padding:15px 16px;border-radius:16px;border:1px solid rgba(34,197,94,.22);background:rgba(34,197,94,.1);color:#dcfce7;text-align:left;cursor:pointer'
    : 'padding:15px 16px;border-radius:16px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.02);color:#cbd5e1;text-align:left;cursor:pointer';
};

window.searchTranscriptExisting = async function(query) {
  const select = document.getElementById('transcript-contact');
  if (!select) return;
  const q = String(query || '').toLowerCase().trim();
  if (!q) {
    select.innerHTML = '<option value="">Select contact</option>';
    return;
  }
  select.innerHTML = '<option value="">Searching...</option>';
  try {
    const rows = await api(`/api/meeting-transcripts/search-existing?search=${encodeURIComponent(q)}`);
    transcriptDealContacts = Array.isArray(rows) ? rows : [];
    select.innerHTML = '<option value="">Select contact</option>' + transcriptDealContacts.map(row => `<option value="${esc(row.id)}">${esc(row.name || '—')} · ${esc(row.company_name || 'No firm')} · ${esc(row.email || 'no email')}</option>`).join('');
  } catch (err) {
    select.innerHTML = `<option value="">${esc(err.message)}</option>`;
  }
};

window.submitTranscriptUpload = async function() {
  const mode = document.getElementById('transcript-mode')?.value || 'existing';
  const transcriptText = document.getElementById('transcript-text')?.value?.trim();
  const transcriptFile = document.getElementById('transcript-file')?.files?.[0] || null;
  const contactId = document.getElementById('transcript-contact')?.value || null;
  const investorName = document.getElementById('transcript-new-name')?.value?.trim();
  if ((!transcriptText && !transcriptFile) || (mode === 'existing' && !contactId) || (mode === 'new' && !investorName)) {
    showToast('Investor and transcript file/text are required', 'error');
    return;
  }
  try {
    const fd = new FormData();
    fd.append('investor_mode', mode);
    if (mode === 'existing') fd.append('contact_id', contactId);
    if (mode === 'new') {
      fd.append('investor_name', investorName);
      fd.append('investor_email', document.getElementById('transcript-new-email')?.value?.trim() || '');
      fd.append('investor_phone', document.getElementById('transcript-new-phone')?.value?.trim() || '');
      fd.append('investor_linkedin', document.getElementById('transcript-new-linkedin')?.value?.trim() || '');
      fd.append('investor_firm', document.getElementById('transcript-new-firm')?.value?.trim() || '');
      fd.append('investor_category', document.getElementById('transcript-new-category')?.value || '');
    }
    if (transcriptText) fd.append('transcript_text', transcriptText);
    if (transcriptFile) fd.append('file', transcriptFile);
    const res = await fetch('/api/meeting-transcripts', { method: 'POST', body: fd });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Upload failed');
    document.getElementById('transcript-upload-modal')?.remove();
    showToast('Transcript uploaded');
    loadMeetingTranscriptsPage();
  } catch (err) {
    showToast(`Transcript upload failed: ${err.message}`, 'error');
  }
};

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
    const listTypeEl = document.getElementById('import-list-type');
    if (listTypeEl?.value) fd.append('list_type', listTypeEl.value);
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
    const recognized   = newCount + updatedCount;
    let msg;
    if (newCount === 0 && updatedCount > 0) {
      msg = `✓ Import successful — ${recognized.toLocaleString()} investors recognised`;
    } else {
      msg = `✓ Import complete — ${newCount.toLocaleString()} new investors added`;
      if (updatedCount > 0) msg += `, ${updatedCount.toLocaleString()} existing updated`;
    }
    if (data.skipped) msg += `, ${data.skipped} skipped`;
    if (totalCount)   msg += `. Total DB: ${totalCount.toLocaleString()}`;
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

// ── JARVIS ORB ───────────────────────────────────────────────────────────────

const jarvisOrb = (() => {
  let isActive          = false;
  let recognition       = null;
  let isListening       = false;
  let currentAudio      = null;
  let audioCtx          = null;
  let pendingTranscript = '';
  let bargeInRecognition = null;
  let bargeInTriggered  = false;
  let playbackVersion   = 0;
  let turnVersion       = 0;
  let activeDispatchController = null;
  let activeSpeakController = null;
  let activeSpeechText  = '';
  let playbackDuckState = null;
  const INTERRUPT_WORDS = new Set(['ok', 'okay', 'yeah', 'yep', 'yup', 'wait', 'stop', 'sorry', 'actually', 'fine', 'no', 'nah', 'hold']);

  // ── AudioContext unlock (proper gesture registration) ─────────────────────
  // Must run synchronously inside a user gesture handler.
  // Primes a silent buffer so all subsequent Audio() play() calls are allowed.
  function unlockAudio() {
    if (audioCtx) {
      if (audioCtx.state === 'suspended') audioCtx.resume().catch(() => {});
      return;
    }
    try {
      audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      const buf = audioCtx.createBuffer(1, 1, 22050);
      const src = audioCtx.createBufferSource();
      src.buffer = buf;
      src.connect(audioCtx.destination);
      src.start(0);
    } catch {}
  }

  // ── filler audio — pre-cached at page load for zero-latency acks ──────────
  const FILLERS = ['On it.', 'Got it.', 'One moment.', 'Let me check.'];
  const fillerCache = new Map();  // text -> Blob

  function prewarmFillers() {
    FILLERS.forEach(async (text) => {
      try {
        const res = await fetch('/api/jarvis/speak', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          credentials: 'same-origin', body: JSON.stringify({ text }),
        });
        if (res.ok) fillerCache.set(text, await res.blob());
      } catch {}
    });
  }
  // Prewarm 3 seconds after page load (non-blocking)
  setTimeout(prewarmFillers, 3000);

  // ── orb element helpers ───────────────────────────────────────────────────
  const orb   = () => document.getElementById('jarvis-orb');
  const label = () => document.getElementById('jarvis-status-label');

  function syncActiveVisuals() {
    document.body.classList.toggle('jarvis-engaged', !!isActive);
  }

  function normalizeSpeechText(text) {
    return String(text || '')
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function isLikelySelfSpeech(text) {
    const heard = normalizeSpeechText(text);
    if (!heard) return false;
    if (FILLERS.some(f => normalizeSpeechText(f) === heard)) return true;
    const spoken = normalizeSpeechText(activeSpeechText);
    if (!spoken) return false;
    if (spoken.includes(heard)) return true;
    const heardWords = heard.split(' ').filter(Boolean);
    if (heardWords.length < 2) return false;
    const overlap = heardWords.filter(word => spoken.includes(word));
    return overlap.length >= Math.min(heardWords.length, 3);
  }

  function hasExplicitInterruptIntent(text) {
    const words = normalizeSpeechText(text).split(/\s+/).filter(Boolean);
    if (!words.length) return false;
    if (words.some(word => INTERRUPT_WORDS.has(word))) return true;
    const joined = words.join(' ');
    return joined.includes('hold on') || joined.includes('one sec') || joined.includes('one second');
  }

  function shouldTriggerBargeIn(text, confidence, isFinal) {
    const clean = String(text || '').trim().toLowerCase();
    if (!clean) return false;
    if (isLikelySelfSpeech(clean)) return false;
    if (hasExplicitInterruptIntent(clean)) return true;
    if (!isFinal) return false;
    const words = clean.split(/\s+/).filter(Boolean);
    if (words.length >= 3 && Number(confidence || 0) >= 0.45) return true;
    if (words.length >= 2 && Number(confidence || 0) >= 0.75) return true;
    const first = words[0] || '';
    if (first.length >= 4 && Number(confidence || 0) >= 0.9) return true;
    return false;
  }

  function setOrbState(state, text) {
    const el = orb();
    if (!el) return;
    el.classList.remove('listening', 'thinking', 'speaking');
    if (state) el.classList.add(state);
    syncActiveVisuals();
    const lbl = label();
    if (lbl) {
      lbl.textContent = text || '';
      lbl.className = text ? 'jarvis-label visible' : 'jarvis-label';
    }
  }

  function stopPlayback() {
    activeSpeechText = '';
    if (currentAudio) {
      try { currentAudio.pause(); } catch {}
      try { currentAudio.src = ''; } catch {}
      currentAudio = null;
    }
  }

  function invalidatePlayback() {
    playbackVersion += 1;
  }

  function invalidateTurn() {
    turnVersion += 1;
    if (activeDispatchController) {
      try { activeDispatchController.abort(); } catch {}
      activeDispatchController = null;
    }
    if (activeSpeakController) {
      try { activeSpeakController.abort(); } catch {}
      activeSpeakController = null;
    }
  }

  function applyPlaybackDuck(enabled) {
    if (enabled) {
      playbackDuckState = {
        currentAudioVolume: currentAudio ? currentAudio.volume : null,
        ackAudioVolume: ackAudio ? ackAudio.volume : null,
      };
      if (currentAudio) currentAudio.volume = 0.35;
      if (ackAudio) ackAudio.volume = 0.25;
      return;
    }
    if (!playbackDuckState) return;
    if (currentAudio && playbackDuckState.currentAudioVolume != null) currentAudio.volume = playbackDuckState.currentAudioVolume;
    if (ackAudio && playbackDuckState.ackAudioVolume != null) ackAudio.volume = playbackDuckState.ackAudioVolume;
    playbackDuckState = null;
  }

  // ── MediaSource streaming TTS — plays audio as chunks arrive ─────────────
  // Falls back to blob if MediaSource not supported (Safari quirks)
  async function playStream(fetchResponse) {
    return new Promise((resolve) => {
      const done = (played) => { currentAudio = null; resolve(!!played); };

      if (window.MediaSource && MediaSource.isTypeSupported('audio/mpeg')) {
        const ms    = new MediaSource();
        const audio = new Audio();
        audio.src   = URL.createObjectURL(ms);
        currentAudio = audio;
        audio.onended = () => done(true);
        audio.onerror = () => done(false);

        ms.addEventListener('sourceopen', async () => {
          let sb;
          try { sb = ms.addSourceBuffer('audio/mpeg'); } catch { done(false); return; }

          const waitUpdate = () => new Promise(r => sb.addEventListener('updateend', r, { once: true }));
          const reader = fetchResponse.body?.getReader?.();
          if (!reader) { done(false); return; }
          audio.play().catch(() => {});

          const pump = async () => {
            try {
              const { done: streamDone, value } = await reader.read();
              if (streamDone) {
                if (sb.updating) await waitUpdate();
                try { ms.endOfStream(); } catch {}
                return;
              }
              if (sb.updating) await waitUpdate();
              sb.appendBuffer(value);
              await waitUpdate();
              pump();
            } catch { done(false); }
          };
          pump();
        }, { once: true });
      } else {
        // Safari / fallback: buffer whole response then play
        fetchResponse.blob().then(blob => {
          const url   = URL.createObjectURL(blob);
          const audio = new Audio(url);
          currentAudio = audio;
          audio.onended = () => { URL.revokeObjectURL(url); done(true); };
          audio.onerror = () => { URL.revokeObjectURL(url); done(false); };
          audio.play().catch(() => { URL.revokeObjectURL(url); done(false); });
        }).catch(() => done(false));
      }
    });
  }

  // ── barge-in: listen for user speech while JARVIS is speaking ────────────
  function startBargeIn() {
    var SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SR) return;
    if (bargeInRecognition) { try { bargeInRecognition.stop(); } catch (e) {} }
    var r = new SR();
    r.continuous     = true;
    r.interimResults = true;
    r.lang           = 'en-US';
    var bargeInText  = '';
    var bargeInConfidence = 0;
    var sawFinalResult = false;
    bargeInTriggered = false;
    applyPlaybackDuck(true);
    r.onresult = function(e) {
      var result = e.results[e.results.length - 1];
      var alt = result && result[0];
      bargeInText = (alt && alt.transcript) || bargeInText || '';
      bargeInConfidence = Math.max(bargeInConfidence, Number(alt && alt.confidence) || 0);
      sawFinalResult = sawFinalResult || !!(result && result.isFinal);
      if (!bargeInTriggered && isActive && shouldTriggerBargeIn(bargeInText, bargeInConfidence, !!(result && result.isFinal))) {
        bargeInTriggered = true;
        invalidatePlayback();
        stopPlayback();
        if (ackAudio)     { ackAudio.pause();     ackAudio     = null; }
        try { r.stop(); } catch (e) {}
        dispatch(bargeInText);
      }
    };
    r.maxAlternatives = 1;
    r.onend = function() {
      bargeInRecognition = null;
      applyPlaybackDuck(false);
      if (!bargeInTriggered && sawFinalResult && bargeInText && isActive && shouldTriggerBargeIn(bargeInText, bargeInConfidence, true)) {
        invalidatePlayback();
        stopPlayback();
        if (ackAudio)     { ackAudio.pause();     ackAudio     = null; }
        dispatch(bargeInText);
      }
    };
    r.onerror = function() {
      bargeInRecognition = null;
      applyPlaybackDuck(false);
    };
    try { r.start(); bargeInRecognition = r; } catch (e) {}
  }

  function stopBargeIn() {
    applyPlaybackDuck(false);
    if (bargeInRecognition) {
      try { bargeInRecognition.stop(); } catch (e) {}
      bargeInRecognition = null;
    }
  }

  // ── sentence-boundary truncation — avoids cutting mid-sentence ───────────
  function truncateAtSentence(text, maxLen) {
    maxLen = maxLen || 580;
    if (text.length <= maxLen) return text;
    var chunk    = text.slice(0, maxLen);
    var lastEnd  = Math.max(chunk.lastIndexOf('. '), chunk.lastIndexOf('! '), chunk.lastIndexOf('? '));
    if (lastEnd > maxLen * 0.5) return chunk.slice(0, lastEnd + 1);
    return chunk; // fallback to hard cut
  }

  // ── ElevenLabs TTS ────────────────────────────────────────────────────────
  async function speakText(text) {
    const clean = text.replace(/[*_`#]/g, '').replace(/\s+/g, ' ').trim().slice(0, 2500);
    if (!clean) { afterSpeak(); return; }
    if (!isActive) return;
    setOrbState('speaking', 'Speaking...');
    startBargeIn();
    invalidatePlayback();
    stopPlayback();
    activeSpeechText = clean;
    const currentTurn = turnVersion;
    const speakVersion = playbackVersion;
    const finishCurrentSpeak = () => {
      if (!isActive || currentTurn !== turnVersion || speakVersion !== playbackVersion) return;
      afterSpeak();
    };
    try {
      const controller = typeof AbortController === 'function' ? new AbortController() : null;
      activeSpeakController = controller;
      const timeoutId = controller ? setTimeout(() => controller.abort(), 5000) : null;
      const res = await fetch('/api/jarvis/speak', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        signal: controller ? controller.signal : undefined,
        body: JSON.stringify({ text: clean }),
      });
      if (activeSpeakController === controller) activeSpeakController = null;
      if (timeoutId) clearTimeout(timeoutId);
      if (!isActive || currentTurn !== turnVersion) return;
      if (!res.ok) {
        let detail = 'ElevenLabs voice unavailable';
        try {
          const data = await res.json();
          detail = data?.error || detail;
        } catch {}
        showToast(detail, 'error', 5000);
        finishCurrentSpeak();
        return;
      }
      const played = await playStream(res);
      if (!isActive || currentTurn !== turnVersion) return;
      if (!played) {
        showToast('Jarvis voice playback failed', 'error', 5000);
        finishCurrentSpeak();
        return;
      }
      finishCurrentSpeak();
    } catch {
      activeSpeakController = null;
      if (!isActive || currentTurn !== turnVersion) return;
      showToast('Jarvis voice request failed', 'error', 5000);
      finishCurrentSpeak();
    }
  }

  function afterSpeak() {
    stopBargeIn();
    if (isActive) {
      // Always-on: stay in conversation loop — restart mic after speaking
      setTimeout(() => { if (isActive) startListening(); }, 400);
    } else {
      stopPlayback();
      setOrbState(null, '');
    }
  }

  // ── instant ack from pre-cached filler ────────────────────────────────────
  let ackAudio = null;
  function speakAck() {
    const text = FILLERS[Math.floor(Math.random() * FILLERS.length)];
    const blob = fillerCache.get(text);
    if (!blob) return;
    if (ackAudio) { ackAudio.pause(); ackAudio = null; }
    const url   = URL.createObjectURL(blob);
    const audio = new Audio(url);
    ackAudio = audio;
    audio.onended = () => { URL.revokeObjectURL(url); ackAudio = null; };
    audio.onerror = () => { URL.revokeObjectURL(url); ackAudio = null; };
    audio.play().catch(() => { ackAudio = null; });
  }

  function waitForAck() {
    return new Promise(r => {
      if (!ackAudio || ackAudio.ended || ackAudio.paused) { r(); return; }
      ackAudio.addEventListener('ended',  r, { once: true });
      ackAudio.addEventListener('error',  r, { once: true });
    });
  }

  // ── deal context ──────────────────────────────────────────────────────────
  function getActiveDealId() {
    const sel = document.querySelector('[data-active-deal-id]');
    return sel ? sel.dataset.activeDealId : null;
  }

  // ── classify whether text is an action request (no API call needed) ─────────
  function isActionRequest(text) {
    const actionKeywords = ['find', 'search', 'pull', 'run', 'trigger',
      'send', 'research', 'enrich', 'approve', 'skip', 'pause', 'resume',
      'update', 'draft', 'dm', 'email'];
    const lower = text.toLowerCase();
    return actionKeywords.some(k => lower.includes(k));
  }

  // ── dispatch transcript to JARVIS ─────────────────────────────────────────
  async function dispatch(text) {
    const cleanText = String(text || '').trim();
    if (!cleanText) { afterSpeak(); return; }
    if (!isActive) return;
    stopListening();
    invalidateTurn();
    const currentTurn = turnVersion;
    setOrbState('thinking', 'Thinking...');
    const needsAck = isActionRequest(cleanText);
    if (needsAck) speakAck();  // plays cached filler immediately — zero added latency
    try {
      const controller = typeof AbortController === 'function' ? new AbortController() : null;
      activeDispatchController = controller;
      const res = await fetch('/api/jarvis', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        signal: controller ? controller.signal : undefined,
        body: JSON.stringify({ message: cleanText, dealId: getActiveDealId() }),
      });
      if (activeDispatchController === controller) activeDispatchController = null;
      if (!isActive || currentTurn !== turnVersion) return;
      const data  = await res.json();
      const reply = data.reply || data.error || '';
      if (needsAck) await waitForAck();  // let ack phrase finish before main response
      if (!isActive || currentTurn !== turnVersion) return;
      await speakText(reply);
    } catch {
      activeDispatchController = null;
      if (!isActive || currentTurn !== turnVersion) return;
      afterSpeak();
    }
  }

  // ── speech recognition ────────────────────────────────────────────────────
  function buildRecognition() {
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SR) return null;
    const r = new SR();
    r.continuous     = false;
    r.interimResults = false;
    r.lang           = 'en-US';
    r.onresult  = (e) => { pendingTranscript = e.results[0]?.[0]?.transcript || ''; };
    r.onspeechend = () => { try { r.stop(); } catch {} };
    r.onend = () => {
      isListening = false;
      const t = pendingTranscript;
      pendingTranscript = '';
      if (isActive) dispatch(t);
    };
    r.onerror = () => {
      isListening = false;
      pendingTranscript = '';
      // Don't auto-retry — user must tap orb again
      afterSpeak();
    };
    return r;
  }

  function startListening() {
    if (!recognition) recognition = buildRecognition();
    if (!recognition || isListening) return;
    try {
      recognition.start();
      isListening = true;
      setOrbState('listening', 'Listening...');
    } catch {}
  }

  function stopListening() {
    isListening = false;
    try { recognition?.stop(); } catch {}
  }

  // ── toggle: press orb ─────────────────────────────────────────────────────
  function toggle() {
    unlockAudio();  // must be synchronous inside this gesture handler
    if (isActive) {
      isActive = false;
      syncActiveVisuals();
      stopListening();
      stopBargeIn();
      invalidateTurn();
      invalidatePlayback();
      if (ackAudio)     { ackAudio.pause();     ackAudio = null; }
      stopPlayback();
      setOrbState(null, '');
    } else {
      isActive = true;
      syncActiveVisuals();
      startListening();
    }
  }

  // legacy shims
  function open()        { if (!isActive) toggle(); }
  function close()       { if (isActive)  toggle(); }
  function toggleMic()   { toggle(); }
  function toggleVoice() { toggle(); }
  function send()        {}

  return { open, close, toggle, send, toggleMic, toggleVoice };
})();
