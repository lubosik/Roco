# Roco — Autonomous Fundraising Agent

Roco runs 24/7 to raise capital for Dom's deals. It researches investors, scores them, enriches their contact data, drafts personalised emails, and manages the full outreach lifecycle via Telegram approval.

## Quick Start

### 1. Install dependencies
```bash
npm install
```

### 2. Configure environment
```bash
cp .env.example .env
# Fill in all values in .env
```

### 3. Set up Gmail OAuth
- Download `credentials.json` from Google Cloud Console (Gmail API enabled)
- Place in project root
- First run will prompt for OAuth token generation

### 4. Configure Notion
- Create two databases: "AI Fundraising Agent CRM (Contacts)" and "AI Fundraising Agent CRM (Companies)"
- Copy the database IDs from the Notion URLs into .env
- Share both databases with your Notion integration

### 5. Seed your deal
```bash
node scripts/seedDeal.js
```

### 6. Start Roco
```bash
node index.js
```

## Telegram Commands

| Command | Action |
|---|---|
| `/status` | Current status, deal, emails sent |
| `/pause` | Pause Roco |
| `/resume` | Resume Roco |
| `/pipeline` | Top 10 active prospects |
| `/newdeal` | Trigger new deal setup |

## Approval Flow

When Roco drafts an email it sends it to Telegram. Reply with:
- `APPROVE A` — send with Subject A
- `APPROVE B` — send with Subject B
- `EDIT [instructions]` — redraft with your changes (max 3 edits)
- `SKIP` — skip this contact
- `STOP` — pause Roco completely

## CSV Import

Drop any PitchBook/Preqin CSV into the `/imports` folder. Roco detects it automatically, maps columns, creates Notion records, and notifies you via Telegram.

## Dashboard

Opens at `http://localhost:3000` (or your Railway URL). Password protected via `DASHBOARD_USER` / `DASHBOARD_PASS` in `.env`.

## LinkedIn (Kondo + Make.com)

Configure a Make.com HTTP POST to:
```
POST https://[your-railway-url]/webhook/kondo
Headers:
  X-Roco-Secret: [your KONDO_WEBHOOK_SECRET]
  Content-Type: application/json
```

## Deploy to Railway

1. Push to GitHub
2. Connect repo in Railway
3. Set all env vars from `.env.example` in Railway Variables
4. Add a Railway Volume mounted at `/app/imports` for CSV ingestion
5. Gmail token: upload `tokens/gmail_token.json` or generate via OAuth on first boot

## Architecture

```
index.js                 — Entry point, startup, validation
core/orchestrator.js     — Main loop (15min cycles)
core/dealContext.js      — Active deal management
research/geminiResearcher.js   — Deep investor research (Gemini 2.5 Pro)
scoring/investorScorer.js      — Claude Sonnet 0-100 scoring
enrichment/kaspEnricher.js     — KASPR API contact enrichment
outreach/emailDrafter.js       — Claude Sonnet email writing (GPT-5.2 fallback)
outreach/sequenceManager.js    — Day 3/7/14 follow-up scheduling
outreach/firmSuppressor.js     — Firm-level suppression on decline
approval/telegramBot.js        — Telegram approval interface
sending/gmailSender.js         — Gmail send + inbox monitoring
crm/notionContacts.js          — Contacts CRM (Notion)
crm/notionCompanies.js         — Companies CRM (Notion)
linkedin/kondoWebhook.js       — Make.com/Kondo LinkedIn webhook
dashboard/                     — Mission Control web UI
```
