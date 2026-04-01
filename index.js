import { config as dotenvConfig } from 'dotenv';
dotenvConfig({ path: new URL('.env', import.meta.url).pathname });
import { info, warn, error } from './core/logger.js';
import { initTelegramBot, sendTelegram, startSupabaseApprovalPoller, reloadPendingSourcingApprovals } from './approval/telegramBot.js';
import { initDashboard } from './dashboard/server.js';
import { startFileWatcher } from './research/pitchbookIngestor.js';
import { startOrchestrator, rocoState } from './core/orchestrator.js';
import { getDeal } from './core/dealContext.js';
import { verifySupabase, seedDefaultTemplates, getActiveDeals, loadSessionState } from './core/supabaseSync.js';
import { registerWebhooks } from './core/unipileSetup.js';

const REQUIRED_VARS = [
  'TELEGRAM_BOT_TOKEN',
  'TELEGRAM_CHAT_ID',
  'NOTION_API_KEY',
  'NOTION_CONTACTS_DB_ID',
  'NOTION_COMPANIES_DB_ID',
  'DASHBOARD_USER',
  'DASHBOARD_PASS',
];

async function validateEnv() {
  const missing = REQUIRED_VARS.filter(v => !process.env[v]);
  if (missing.length) {
    console.error('\nMissing required environment variables:\n');
    missing.forEach(v => console.error(`  - ${v}`));
    console.error('\nCopy .env.example to .env and fill in your values.\n');
    process.exit(1);
  }
  info('Environment variables validated');
}

async function verifyNotionAccess() {
  const { NOTION_BASE, NOTION_VERSION } = await import('./config/constants.js');
  const headers = {
    Authorization: `Bearer ${process.env.NOTION_API_KEY}`,
    'Notion-Version': NOTION_VERSION,
  };

  for (const [name, id] of [
    ['Contacts DB', process.env.NOTION_CONTACTS_DB_ID],
    ['Companies DB', process.env.NOTION_COMPANIES_DB_ID],
  ]) {
    try {
      const res = await fetch(`${NOTION_BASE}/databases/${id}`, { headers });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      info(`Notion ${name} — connected`);
    } catch (err) {
      error(`Notion ${name} — connection failed: ${err.message}`);
      console.error(`\nFailed to connect to Notion ${name}.\n`);
      process.exit(1);
    }
  }
}

async function main() {
  console.log('\n' + '='.repeat(50));
  console.log('  ROCO — Autonomous Fundraising Agent');
  console.log('='.repeat(50) + '\n');
  console.log(`  Start time: ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`);

  // 1. Validate env
  await validateEnv();

  // 2. Verify Notion connectivity
  await verifyNotionAccess();

  // 3. Connect to Supabase
  const supabaseOk = await verifySupabase();
  if (supabaseOk) {
    info('Supabase connected');
  } else {
    warn('Supabase unavailable — running on local state cache');
  }

  // 4. Load persisted session state from Supabase
  const sessionState = await loadSessionState();
  Object.assign(rocoState, {
    status: sessionState.rocoStatus || 'ACTIVE',
  });
  info(`Roco status: ${rocoState.status}`);

  // 5. Seed default templates if needed
  await seedDefaultTemplates();

  // 6. Start Telegram bot
  const bot = initTelegramBot(rocoState);
  startSupabaseApprovalPoller();
  reloadPendingSourcingApprovals().catch(() => {});  // Reload pending sourcing approvals after restart
  info('Telegram bot initialised');

  // 7. Start dashboard + webhook server
  initDashboard(rocoState);

  // Self-check: verify dashboard is reachable
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

  // 8. Register Unipile webhooks
  try {
    const port = process.env.PORT || 3000;
    const baseUrl = process.env.PUBLIC_URL || `http://76.13.44.185:${port}`;
    await registerWebhooks(baseUrl);
  } catch (webhookErr) {
    warn(`Unipile webhook registration failed: ${webhookErr.message}`);
  }

  // 9. Start CSV file watcher
  startFileWatcher();

  // 9. Load active deals and log them
  let activeDeals = [];
  try {
    activeDeals = await getActiveDeals();
    console.log(`✓ Active deals: ${activeDeals.length > 0 ? activeDeals.map(d => d.name).join(', ') : 'none — launch a deal from Mission Control'}`);
  } catch (err) {
    console.log(`  Active deals: could not load (${err.message})`);
  }

  // 10. Start orchestrator
  console.log('[STARTUP] Calling startOrchestrator...');
  try {
    await startOrchestrator();
    console.log('[STARTUP] Orchestrator started successfully');
  } catch (err) {
    console.error('[STARTUP] startOrchestrator FAILED:', err.message);
    console.error(err.stack);
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
  await sendTelegram(
    `ROCO is online.\n\nActive deals: ${activeDealCount > 0 ? dealSummary : 'None — use Mission Control to launch a deal'}\nSupabase: ${supabaseOk ? '✓ Connected' : '⚠ Offline (local cache)'}\n\n/status — check status\n/pause — pause all\n/pipeline — top prospects`
  );

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
  await sendTelegram('ROCO is shutting down (SIGTERM received).').catch(() => {});
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('SIGINT received — shutting down gracefully');
  process.exit(0);
});

main().catch(err => {
  console.error('Fatal startup error:', err.message);
  process.exit(1);
});
