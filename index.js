import { config as dotenvConfig } from 'dotenv';
dotenvConfig({ path: new URL('.env', import.meta.url).pathname });
import { info, warn, error } from './core/logger.js';
import {
  initTelegramBot,
  sendTelegram,
  reloadPendingInvestorApprovals,
  reloadPendingSourcingApprovals,
  startSupabaseApprovalPoller,
} from './approval/telegramBot.js';
import { initDashboard } from './dashboard/server.js';
import { startFileWatcher } from './research/pitchbookIngestor.js';
import { startOrchestrator, rocoState } from './core/orchestrator.js';
import { getDeal } from './core/dealContext.js';
import { verifySupabase, seedDefaultTemplates, getActiveDeals, loadSessionState } from './core/supabaseSync.js';
import { registerWebhooks } from './core/unipileSetup.js';

const BASE_REQUIRED_VARS = [];

function envFlag(name, defaultValue) {
  const raw = process.env[name];
  if (raw == null || raw === '') return defaultValue;
  return ['1', 'true', 'yes', 'on'].includes(String(raw).trim().toLowerCase());
}

function getAppRole() {
  const role = String(process.env.ROCO_APP_ROLE || 'all').trim().toLowerCase();
  if (['web', 'worker', 'all'].includes(role)) return role;
  return 'all';
}

function getRuntimePlan() {
  const role = getAppRole();
  const telegramTransport = String(process.env.TELEGRAM_TRANSPORT || 'polling').trim().toLowerCase();
  const defaultTelegramEnabled = role === 'all' || role === 'worker' || (role === 'web' && telegramTransport === 'webhook');
  return {
    role,
    dashboard: envFlag('ROCO_ENABLE_DASHBOARD', role !== 'worker'),
    telegram: envFlag('ROCO_ENABLE_TELEGRAM', defaultTelegramEnabled),
    orchestrator: envFlag('ROCO_ENABLE_ORCHESTRATOR', role !== 'web'),
    fileWatcher: envFlag('ROCO_ENABLE_FILE_WATCHER', role !== 'web'),
    registerUnipileWebhooks: envFlag('ROCO_REGISTER_UNIPILE_WEBHOOKS', role !== 'worker'),
    startupTelegram: envFlag('ROCO_ENABLE_STARTUP_TELEGRAM', role !== 'web'),
  };
}

function getServerBaseUrl(port) {
  const explicit = process.env.PUBLIC_URL || process.env.SERVER_BASE_URL;
  if (explicit) return explicit.replace(/\/+$/, '');

  const railwayStaticUrl = String(process.env.RAILWAY_STATIC_URL || '').trim();
  if (railwayStaticUrl) {
    return railwayStaticUrl.startsWith('http')
      ? railwayStaticUrl.replace(/\/+$/, '')
      : `https://${railwayStaticUrl.replace(/^https?:\/\//, '').replace(/\/+$/, '')}`;
  }

  const railwayDomain = String(process.env.RAILWAY_PUBLIC_DOMAIN || '').trim();
  if (railwayDomain) return `https://${railwayDomain.replace(/^https?:\/\//, '').replace(/\/+$/, '')}`;

  return 'https://roco-production.up.railway.app';
}

async function validateEnv(runtime) {
  const requiredVars = [...BASE_REQUIRED_VARS];
  if (runtime.telegram) requiredVars.push('TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID');
  if (runtime.dashboard) requiredVars.push('DASHBOARD_USER', 'DASHBOARD_PASS');

  const missing = requiredVars.filter(v => !process.env[v]);
  if (missing.length) {
    console.error('\nMissing required environment variables:\n');
    missing.forEach(v => console.error(`  - ${v}`));
    console.error('\nCopy .env.example to .env and fill in your values.\n');
    process.exit(1);
  }
  info('Environment variables validated');
}

async function main() {
  const runtime = getRuntimePlan();
  console.log('\n' + '='.repeat(50));
  console.log('  ROCO — Autonomous Fundraising Agent');
  console.log('='.repeat(50) + '\n');
  console.log(`  Start time: ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`);
  console.log(`  Runtime role: ${runtime.role}`);
  console.log(`  Components: dashboard=${runtime.dashboard} telegram=${runtime.telegram} orchestrator=${runtime.orchestrator} fileWatcher=${runtime.fileWatcher} registerUnipileWebhooks=${runtime.registerUnipileWebhooks}`);

  // 1. Validate env
  await validateEnv(runtime);

  // 2. Connect to Supabase
  const supabaseOk = await verifySupabase();
  if (supabaseOk) {
    info('Supabase connected');
  } else {
    warn('Supabase unavailable — running on local state cache');
  }

  // 3. Load persisted session state from Supabase
  const sessionState = await loadSessionState();
  Object.assign(rocoState, {
    status: sessionState.rocoStatus || 'ACTIVE',
  });
  info(`Roco status: ${rocoState.status}`);

  // 4. Seed default templates if needed
  await seedDefaultTemplates();

  // 5. Start Telegram bot
  if (runtime.telegram) {
    initTelegramBot(rocoState);
    startSupabaseApprovalPoller();
    await reloadPendingInvestorApprovals().catch(() => {});
    await reloadPendingSourcingApprovals().catch(() => {});
    info('Telegram bot initialised');
  } else {
    info('Telegram bot skipped for this runtime role');
  }

  // 6. Start dashboard + webhook server
  if (runtime.dashboard) {
    initDashboard(rocoState);
  } else {
    info('Dashboard server skipped for this runtime role');
  }

  // Self-check: verify dashboard is reachable
  if (runtime.dashboard) {
    await new Promise(r => setTimeout(r, 1500));
    try {
      const port = process.env.PORT || 3000;
      const checkRes = await fetch(`http://127.0.0.1:${port}/health`);
      if (checkRes.ok) {
        info('Dashboard self-check passed');
      } else {
        warn(`Dashboard self-check returned HTTP ${checkRes.status}`);
      }
    } catch (selfCheckErr) {
      warn(`Dashboard self-check failed: ${selfCheckErr.message}`);
    }
  }

  // 8. Register Unipile webhooks
  if (runtime.registerUnipileWebhooks && runtime.dashboard) {
    try {
      const port = process.env.PORT || 3000;
      const baseUrl = getServerBaseUrl(port);
      await registerWebhooks(baseUrl);
    } catch (webhookErr) {
      warn(`Unipile webhook registration failed: ${webhookErr.message}`);
    }
  } else {
    info('Unipile webhook registration skipped for this runtime role');
  }

  // 9. Start CSV file watcher
  if (runtime.fileWatcher) {
    startFileWatcher();
  } else {
    info('CSV file watcher skipped for this runtime role');
  }

  // 9. Load active deals and log them
  let activeDeals = [];
  let activeDealsLoaded = false;
  let activeDealsError = '';
  try {
    if (!supabaseOk) {
      activeDealsError = 'Supabase unavailable';
      console.log('  Active deals: not checked (Supabase unavailable)');
    } else {
      activeDeals = await getActiveDeals();
      activeDealsLoaded = true;
      console.log(`✓ Active deals: ${activeDeals.length > 0 ? activeDeals.map(d => d.name).join(', ') : 'none — launch a deal from Mission Control'}`);
    }
  } catch (err) {
    activeDealsError = err.message || 'unknown error';
    console.log(`  Active deals: could not load (${err.message})`);
  }

  // 10. Start orchestrator
  if (runtime.orchestrator) {
    console.log('[STARTUP] Calling startOrchestrator...');
    try {
      await startOrchestrator();
      console.log('[STARTUP] Orchestrator started successfully');
    } catch (err) {
      console.error('[STARTUP] startOrchestrator FAILED:', err.message);
      console.error(err.stack);
    }
  } else {
    info('Orchestrator skipped for this runtime role');
  }

  console.log('\n' + '='.repeat(50));
  console.log('  ROCO IS LIVE — watching for work');
  console.log(`  Time: ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`);
  console.log('='.repeat(50) + '\n');

  // 11. Send startup notification
  let dealSummary = 'No active deal configured';
  try {
    if (activeDeals.length > 0) {
      dealSummary = activeDeals.map(d => `${d.name} (${d.status})`).join(', ');
    } else {
      const deal = getDeal();
      if (deal) dealSummary = deal.name;
    }
  } catch {}

  const activeDealCount = activeDeals.length;
  const activeDealLine = activeDealCount > 0
    ? dealSummary
    : activeDealsLoaded
      ? 'None — use Mission Control to launch a deal'
      : `Unknown — ${activeDealsError || 'Supabase check did not complete'}`;
  if (runtime.telegram && runtime.startupTelegram) {
    await sendTelegram(
      `ROCO is online.\n\nActive deals: ${activeDealLine}\nSupabase: ${supabaseOk ? '✓ Connected' : '⚠ Offline or not configured on this service'}\nRole: ${runtime.role}\n\n/status — check status\n/pause — pause all\n/pipeline — top prospects`
    );
  }

  info('Roco fully operational');
}

// Catch any unhandled promise rejection — log it but DO NOT crash
process.on('unhandledRejection', (reason, promise) => {
  console.error('UNHANDLED REJECTION — Roco kept running');
  console.error('Reason:', reason?.message || reason);
  console.error('Stack:', reason?.stack || 'no stack');
  error('Unhandled rejection', { reason: String(reason?.message || reason) });
});

// Catch any uncaught synchronous exception — log it but DO NOT crash
process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT EXCEPTION — Roco kept running');
  console.error('Error:', err.message);
  console.error('Stack:', err.stack);
  error('Uncaught exception', { err: err.message, stack: err.stack });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received — shutting down gracefully');
  info('Received SIGTERM — shutting down gracefully');
  rocoState.status = 'CLOSED';
  await import('./core/orchestrator.js').then(m => m.releaseOrchestratorLease?.()).catch(() => {});
  await sendTelegram('ROCO is shutting down (SIGTERM received).').catch(() => {});
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('SIGINT received — shutting down gracefully');
  import('./core/orchestrator.js').then(m => m.releaseOrchestratorLease?.()).catch(() => {});
  process.exit(0);
});

main().catch(err => {
  console.error('Fatal startup error:', err.message);
  process.exit(1);
});
