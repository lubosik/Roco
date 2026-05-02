# PROJECT: Roco — Autonomous PE/VC Fundraising Agent

## DEPLOYMENT — READ THIS FIRST

**This is a Railway deployment. There is no local server to run.**

- Production runs on **Railway** — two services: **Roco Web** (dashboard + API, `dashboard/server.js`) and **Roco Worker** (orchestrator + background processes, `index.js`)
- Pushing to `master` or `main` on GitHub triggers Railway auto-deploy
- **Never run `pm2 start` or start any local server** — it conflicts with Railway's Telegram bot polling (409 errors)
- Environment variables live in the **Railway dashboard**, not in `.env`. The `.env` file is local reference only and is gitignored
- SSH to VPS (`ssh root@76.13.44.185`) is for editing code only, not running it

To deploy any change:
```
git add <files>
git commit -m "..."
git push origin master
git push origin main
```

Railway picks up both branches. Monitor the Railway dashboard for deploy status.

---

## Goal

Autonomous fundraising AI agent for independent sponsor PE deals. Roco researches investor firms, enriches contacts, drafts personalised outreach (email + LinkedIn), seeks human approval via Telegram/dashboard, and sends. All pipeline state lives in Supabase. JARVIS is the AI brain (Claude via OpenRouter) that runs morning briefs, autonomous health checks, and conversational control.

## Features in scope
- FIRMS-FIRST research pipeline (firmResearcher.js)
- Apify LinkedIn enrichment
- Email outreach via Unipile (Outlook + Gmail)
- LinkedIn invite + DM outreach via Unipile
- Approval flow: Telegram inline buttons + dashboard UI
- JARVIS AI agent: morning brief, autonomous alerts, conversational control
- Deal intelligence, scoring, patience rules, waterfall cascade
- Dashboard on port 3000 (proxied by Railway)

## Tech stack
- Runtime: Node.js ES Modules (`"type": "module"`)
- Database: Supabase PostgreSQL
- Email/LinkedIn API: Unipile (`core/unipile.js`, `integrations/unipileClient.js`)
- LLM: OpenRouter → Claude (brain/opus), Gemini (classify), Haiku (draft/conversation), Perplexity (web research)
- Approval/notifications: Telegram Bot API
- Enrichment: Apify LinkedIn scraper
- Hosting: Railway (Web + Worker services)

## Key files
- `index.js` — Worker entry point (orchestrator + Telegram bot)
- `dashboard/server.js` — Web entry point (Express API + WebSocket dashboard)
- `core/orchestrator.js` — Main 10-min loop, all outreach phases
- `core/jarvis.js` — JARVIS AI brain (ET timezone, morning brief, autonomous check)
- `core/unipile.js` — Unipile credential manager + LinkedIn helpers
- `integrations/unipileClient.js` — Raw Unipile HTTP client
- `core/scheduleChecker.js` — Sending window gate (timezone: America/New_York)
- `state.json` — Local runtime control (not used on Railway — state comes from Supabase)
- `approval/telegramBot.js` — Telegram bot, approval flow

## Environment variables (set in Railway dashboard)
- `UNIPILE_DSN` — e.g. `https://api17.unipile.com:14756`
- `UNIPILE_API_KEY` — Unipile access token (also stored as `UNIPILE_ACCESS_TOKEN`)
- `UNIPILE_LINKEDIN_ACCOUNT_ID` — LinkedIn account ID in Unipile
- `UNIPILE_GMAIL_ACCOUNT_ID` — Gmail account ID in Unipile
- `UNIPILE_OUTLOOK_ACCOUNT_ID` — Outlook account ID in Unipile
- `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` — Supabase project
- `TELEGRAM_BOT_TOKEN` — Telegram bot
- `OPENROUTER_API_KEY` — OpenRouter for all LLM calls
- `APIFY_API_TOKEN`, `APIFY_LINKEDIN_SCRAPER_ACTOR_ID` — Apify enrichment

## Unipile credential types — important distinction
There are TWO separate credential layers:
1. **Unipile API token** (`UNIPILE_API_KEY`) — authenticates calls to Unipile's API. Expires in ~10 years. Visible and regeneratable in the Unipile dashboard.
2. **Connected account credentials** (LinkedIn, Gmail, Outlook sessions stored inside Unipile) — these are the actual social/email account sessions. LinkedIn sessions expire when LinkedIn invalidates them (typically every few weeks/months). When expired, the Unipile API returns `401 errors/expired_credentials`. Fix: client reconnects their LinkedIn profile in Unipile → provides new `UNIPILE_LINKEDIN_ACCOUNT_ID`.

The orchestrator runs a `checkUnipileAccountHealth()` call each cycle. If any connected account is expired, it fires a Telegram alert immediately.

## Deploy target
Railway — auto-deploys on push to `master` or `main`

## Reviewer notes
- Timezone: all date/time logic must use `America/New_York` (Luxon). Server runs UTC. Never use raw `new Date()` for day-of-week or "today" boundaries.
- LinkedIn DM approval: fires immediately via `sendApprovedLinkedInDM` on approval — does not wait for orchestrator loop.
- Never start local PM2 — it conflicts with Railway's Telegram polling.
