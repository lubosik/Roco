# Raxion — Autonomous Recruiting Agent
## Technical Architecture & Implementation Specification

> Raxion is the recruiting equivalent of Roco. Where Roco autonomously raises capital for a deal, Raxion autonomously sources, shortlists, and engages candidates for an active job. The architecture is deliberately mirrored so any engineer who has built Roco can build Raxion.

---

## Core Philosophy

- **Fully autonomous between 08:00–18:00 Mon–Fri** (configurable per job). Outside those hours: research, scoring, enrichment only. No outreach.
- **Human-in-the-loop approval** — every message (LinkedIn DM, email, LinkedIn post) is drafted by AI and approved by the recruiter before sending.
- **One active job = one pipeline**. Multiple jobs run in parallel but a candidate can only be in one active job's pipeline at a time (cross-job deduplication).
- **LinkedIn-first**. Primary channel is LinkedIn DM (via Unipile). Email is fallback. LinkedIn posts for job broadcasting.
- **Relentless but respectful**. Automated follow-up sequences with configurable cadence. Firm-level suppression if a candidate declines.

---

## System Architecture

```
index.js                        — Entry point, env validation, startup
core/orchestrator.js            — Main loop (every 10–15 min)
core/scheduleChecker.js         — Sending window gates (per-job config)
core/apiFallback.js             — withFallback() + API health tracking
core/supabase.js                — Supabase client singleton
core/supabaseSync.js            — getActiveJobs(), logActivity()
core/logger.js                  — Winston file + console logger

research/candidateResearcher.js — Deep candidate research (Gemini 2.5 Pro)
research/jobResearcher.js       — Job/company context builder
research/csvIngestor.js         — LinkedIn Sales Navigator / CSV imports

scoring/candidateScorer.js      — Claude Sonnet 0–100 fit scoring

enrichment/linkedinFinder.js    — LinkedIn URL discovery (search APIs)
enrichment/emailFinder.js       — Email enrichment (Hunter.io / KASPR)
enrichment/phoneEnricher.js     — Phone enrichment (KASPR)

outreach/linkedinDrafter.js     — LinkedIn DM drafting (Claude Sonnet)
outreach/emailDrafter.js        — Email drafting (Claude Sonnet)
outreach/postDrafter.js         — LinkedIn post drafting (Claude Sonnet)
outreach/sequenceManager.js     — Follow-up scheduling (Day 3/7/14)
outreach/firmSuppressor.js      — Company-level suppression on decline

approval/telegramBot.js         — Telegram approval interface (APPROVE/SKIP/EDIT)

integrations/unipileClient.js   — Unipile API: send DMs, invites, posts, fetch chats
integrations/unipileSender.js   — Higher-level send wrappers (email via Unipile)
integrations/unipileSetup.js    — Webhook registration on startup

crm/notionContacts.js           — Candidate CRM (Notion database)
crm/notionCompanies.js          — Client/firm CRM (Notion database)
crm/notionLogger.js             — Notion activity logging

dashboard/server.js             — Express + WebSocket server (Mission Control)
dashboard/public/index.html     — Single-page dashboard UI
dashboard/public/dashboard.js   — Frontend logic (ES modules)
dashboard/public/styles.css     — Dashboard styles

linkedin/kondoWebhook.js        — Legacy LinkedIn webhook fallback (Make.com)
scripts/                        — One-off migration and seed scripts
supabase/migration.sql          — Database schema
```

---

## Database Schema (Supabase)

### `jobs` table
```sql
CREATE TABLE jobs (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  title                 TEXT NOT NULL,              -- e.g. "Senior React Engineer"
  status                TEXT DEFAULT 'ACTIVE',       -- ACTIVE | PAUSED | CLOSED
  paused                BOOLEAN DEFAULT false,
  client_name           TEXT,                        -- hiring company
  client_description    TEXT,
  job_description       TEXT,                        -- full JD for AI context
  location              TEXT,
  salary_range          TEXT,                        -- e.g. "£80,000–£100,000"
  contract_type         TEXT,                        -- Permanent | Contract | Interim
  seniority             TEXT,                        -- Junior | Mid | Senior | Lead | Head of
  key_skills            TEXT,                        -- comma-separated for AI prompt context
  ideal_candidate_notes TEXT,                        -- recruiter's own notes on ideal fit
  linkedin_daily_limit  INTEGER DEFAULT 28,          -- LinkedIn invites per day
  send_from             TIME DEFAULT '08:00',
  send_until            TIME DEFAULT '18:00',
  timezone              TEXT DEFAULT 'Europe/London',
  active_days           TEXT DEFAULT 'Mon,Tue,Wed,Thu,Fri',
  followup_cadence_days JSONB DEFAULT '[3,7,14]',   -- follow-up schedule
  followup_days_li      INTEGER DEFAULT 5,           -- LinkedIn DM follow-up gap
  followup_days_email   INTEGER DEFAULT 3,
  max_candidates_per_company INTEGER DEFAULT 3,      -- avoid carpet-bombing one employer
  max_total_outreach    INTEGER DEFAULT 500,
  min_candidate_score   INTEGER DEFAULT 60,
  outreach_paused_until TIMESTAMPTZ,
  closed_at             TIMESTAMPTZ,
  created_at            TIMESTAMPTZ DEFAULT now(),
  updated_at            TIMESTAMPTZ DEFAULT now()
);
```

### `candidates` table
```sql
CREATE TABLE candidates (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id                UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  name                  TEXT NOT NULL,
  email                 TEXT,
  phone                 TEXT,
  linkedin_url          TEXT,
  linkedin_provider_id  TEXT,                        -- Unipile URN (ACoAAA...)
  linkedin_chat_id      TEXT,                        -- stored after first DM sent
  current_title         TEXT,
  current_company       TEXT,
  location              TEXT,
  years_experience      INTEGER,
  skills                TEXT,                        -- enriched skills summary
  candidate_score       INTEGER,                     -- 0–100 fit score
  pipeline_stage        TEXT DEFAULT 'Sourced',
  -- Stages: Sourced → Enriched → Ranked → invite_sent → invite_accepted →
  --         In Conversation → Shortlisted → Submitted → Interview → Offer →
  --         Placed | Not Interested | Suppressed — Opt Out | Archived
  notes                 TEXT,                        -- AI research notes + [PERSON_RESEARCHED] marker
  invite_sent_at        TIMESTAMPTZ,
  last_contacted        DATE,
  last_contact_type     TEXT,                        -- LinkedIn | Email
  enrichment_status     TEXT DEFAULT 'Pending',      -- Pending | Enriched | Not Found
  notion_page_id        TEXT,                        -- Notion CRM page ID
  source                TEXT,                        -- CSV | LinkedIn Search | Manual
  archived              BOOLEAN DEFAULT false,
  created_at            TIMESTAMPTZ DEFAULT now(),
  updated_at            TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_candidates_job_id ON candidates(job_id);
CREATE INDEX idx_candidates_pipeline_stage ON candidates(pipeline_stage);
CREATE INDEX idx_candidates_linkedin_provider_id ON candidates(linkedin_provider_id);
CREATE INDEX idx_candidates_linkedin_chat_id ON candidates(linkedin_chat_id);
CREATE INDEX idx_candidates_invite_sent_at ON candidates(invite_sent_at);
```

### `job_assets` table
```sql
CREATE TABLE job_assets (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id      UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  name        TEXT NOT NULL,                         -- e.g. "Apply Link", "Company Video"
  asset_type  TEXT NOT NULL CHECK (asset_type IN ('calendly','jd','video','image','link','other')),
  url         TEXT NOT NULL,
  description TEXT,
  created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_job_assets_job_id ON job_assets(job_id);
```

### Supporting tables
```sql
-- Dual-write activity log (mirrors Notion)
CREATE TABLE activity (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id      UUID REFERENCES jobs(id),
  candidate_id UUID REFERENCES candidates(id),
  event_type  TEXT,                                  -- INVITE_SENT | INVITE_ACCEPTED | DM_SENT | EMAIL_SENT | REPLIED | PLACED etc.
  summary     TEXT,
  detail      JSONB,
  api_used    TEXT,
  created_at  TIMESTAMPTZ DEFAULT now()
);

-- Outbound messages (DMs and emails pending/sent)
CREATE TABLE messages (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  candidate_id        UUID REFERENCES candidates(id),
  job_id              UUID REFERENCES jobs(id),
  stage               TEXT,                          -- INTRO | FOLLOW_UP_1 | FOLLOW_UP_2 | REPLY
  channel             TEXT,                          -- linkedin_dm | email
  direction           TEXT DEFAULT 'outbound',       -- outbound | inbound
  subject             TEXT,                          -- email only
  body                TEXT,
  status              TEXT DEFAULT 'pending_approval', -- pending_approval | approved | sent | skipped
  is_reply            BOOLEAN DEFAULT false,
  reply_to_record_id  UUID,
  linkedin_chat_id    TEXT,
  gmail_thread_id     TEXT,
  sent_at             TIMESTAMPTZ,
  created_at          TIMESTAMPTZ DEFAULT now()
);

-- Inbound LinkedIn messages (webhook-persisted)
CREATE TABLE linkedin_messages (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  chat_id           TEXT,
  message_text      TEXT,
  from_name         TEXT,
  from_linkedin_url TEXT,
  from_provider_id  TEXT,
  created_at        TIMESTAMPTZ DEFAULT now()
);

-- Inbound email/DM replies
CREATE TABLE replies (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  body                TEXT,
  thread_id           TEXT,
  message_id          TEXT,
  candidate_id        UUID REFERENCES candidates(id),
  job_id              UUID,
  received_at         TIMESTAMPTZ,
  classification      TEXT,
  classification_notes TEXT,
  sentiment           TEXT
);

-- Approval queue (Telegram ↔ dashboard bridge)
CREATE TABLE approval_queue (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  telegram_msg_id   BIGINT,
  status            TEXT DEFAULT 'pending',          -- pending | approved | skipped | edited
  candidate_id      TEXT,
  candidate_name    TEXT,
  candidate_email   TEXT,
  company           TEXT,
  job_title         TEXT,
  stage             TEXT,
  subject           TEXT,
  body              TEXT,
  score             INTEGER,
  research_summary  TEXT,
  edit_instructions TEXT,
  approved_subject  TEXT,
  created_at        TIMESTAMPTZ DEFAULT now(),
  resolved_at       TIMESTAMPTZ
);
```

---

## Orchestrator Loop (`core/orchestrator.js`)

Runs every **10 minutes**. Each cycle:

```
runCycle()
  ├── Read state.json  (raxionStatus, module toggles)
  ├── getActiveJobs()  (status=ACTIVE, paused=false)
  └── For each job:
       ├── Phase 0: Cross-job deduplication
       ├── Phase 1: Pipeline refill (CSV ingestion / LinkedIn search seeds)
       ├── Phase 2: Candidate research (Gemini — adds [PERSON_RESEARCHED] marker)
       ├── Phase 3: Enrichment (LinkedIn URL, email, phone)
       ├── Phase 4: Scoring (Claude — 0–100 fit score vs job description)
       ├── Phase 5: Notion sync (create/update Notion CRM pages)
       ├── Phase 6: LinkedIn invites (only within sending window, 28/day limit)
       ├── Phase 7: Outreach (DM for invite_accepted, email for enriched-no-LinkedIn)
       ├── Phase 8: Follow-ups (per cadence, only non-replied stages)
       └── Phase 9: LinkedIn post scheduling (weekly, Dom-approved)
```

### Sending window gate
All phases that **send** (6, 7, 8, 9) are gated by `isWithinSendingWindow(job)`:
- Checks `active_days` (comma-separated: `Mon,Tue,Wed,Thu,Fri,Sat,Sun`)
- Checks `send_from` / `send_until` in `timezone`
- Research, enrichment, scoring run 24/7

### Phase 0 — Cross-job deduplication
Before processing any candidate:
```javascript
// If a candidate's linkedin_url or email already exists in ANOTHER active job's pipeline
// (any stage except Archived/Suppressed/Not Interested), skip them with a hold note.
// This prevents the same person receiving DMs from two parallel job searches.
```

### Phase 6 — LinkedIn invites
```javascript
// 1. Check sentToday: SELECT count(*) WHERE invite_sent_at >= today's midnight
// 2. remainingToday = job.linkedin_daily_limit (default 28) - sentToday
// 3. Pick up to 5 candidates per cycle at Ranked/Enriched stage with a linkedin_url and no invite_sent_at
// 4. Call sendLinkedInInvite({ providerId, message }) via Unipile POST /api/v1/users/:id/invite
// 5. Update: pipeline_stage = 'invite_sent', invite_sent_at = now()
// 6. Dual-write: Supabase update + Notion update
```

### Phase 7 — Outreach
```javascript
// DM: candidates at invite_accepted stage → draftLinkedInDM() → approval queue
// Email: candidates at Enriched stage with email but no linkedin_url → draftEmail() → approval queue
// Max 2 DMs + 2 emails per cycle (throttle to avoid spam detection)
// On approval: sendLinkedInDM() via Unipile POST /api/v1/chats
//   → store returned chatId in candidates.linkedin_chat_id
//   → update pipeline_stage = 'dm_sent' | 'email_sent'
//   → dual-write Notion
```

---

## LinkedIn DM Conversation Flow

### Full lifecycle
```
invite_sent
    ↓ (webhook: POST /webhook/unipile/linkedin/relations)
invite_accepted
    ↓ (phaseOutreach — next cycle within sending window)
    → draftLinkedInDM(INTRO) → approval queue → Dom approves
    → Unipile POST /api/v1/chats  (new chat, store chatId)
dm_sent
    ↓ (webhook: POST /webhook/unipile/linkedin/messages)
In Conversation
    → classifyWithAI(inbound message)
    → if INTERESTED/WANTS_MORE_INFO/MEETING_REQUEST:
         draftReply() + inject job_assets (Calendly, JD link) → approval queue
         → Dom approves → Unipile POST /api/v1/chats/{chatId}/messages
    → if NOT_INTERESTED:
         closeConversation() → stage = 'Not Interested'
         AI summarises thread → Notion notes updated
         Telegram notification to recruiter
    → if OPT_OUT:
         stage = 'Suppressed — Opt Out'
         Notion notes updated
Shortlisted           ← recruiter manually advances in Mission Control
    ↓
Submitted to Client
    ↓
Interview
    ↓
Offer
    ↓
Placed                ← conversation close, summary logged to Notion
```

### Inbound webhook contact lookup (CRITICAL — must try all three in order)
```javascript
// 1. linkedin_chat_id match (most reliable — set after first DM sent)
// 2. linkedin_provider_id match (Unipile member URN)
// 3. email match (fallback for email replies)
// If no match: silently skip (person not in any active job pipeline)
```

### Accepted connection webhook
```javascript
// POST /webhook/unipile/linkedin/relations
// 1. Extract provider_id from payload (try nested paths: attendee, object, user, data)
// 2. Log full raw body for debugging on every fire
// 3. Supabase join: candidates !inner jobs where jobs.status = 'ACTIVE' AND jobs.paused = false
// 4. If not found: silent skip (not our candidate)
// 5. If found: stage → invite_accepted (Supabase + Notion), pushActivity, sbLogActivity
```

---

## Message Drafting

### LinkedIn DM — INTRO (`outreach/linkedinDrafter.js`)
```javascript
// Prompt includes:
// - Recruiter persona (not "Dom" — use configurable recruiter name from env)
// - Job title, client name (optionally anonymised), key selling points
// - Candidate's current role, company, skills, score rationale
// - Tone: warm, direct, specific to their background — NOT generic spam
// - Length: 3–4 sentences max (LinkedIn DM best practice)
// - CTA: ask if they're open to a quick chat, NOT "apply here"
// Returns: message body only (no subject)
```

### Email — INTRO (`outreach/emailDrafter.js`)
```javascript
// Same context as DM but slightly longer (5–7 sentences)
// A/B subject line testing: generate two subjects, record both
// Sign off as recruiter name from env
```

### Reply drafting (inline in dashboard/server.js)
```javascript
// Triggered when inbound classified as: INTERESTED | WANTS_MORE_INFO | MEETING_REQUEST | POSITIVE
// Fetches job_assets for the job:
//   - MEETING_REQUEST → prioritise Calendly link
//   - WANTS_MORE_INFO → prioritise JD link, then other assets
// Assets injected as plain URLs in message body (no binary attachments needed)
// LinkedIn DM binary attachments (images/video) require Unipile multipart upload — avoid for reliability
```

### LinkedIn post drafting (`outreach/postDrafter.js`)
```javascript
// Triggered weekly (or on demand via Mission Control)
// Generates a compelling job post for LinkedIn feed
// Includes: role headline, why it's a great opportunity, 3 bullet requirements, CTA
// Dom/recruiter approves before posting via Unipile POST /api/v1/posts
// Track engagement via Unipile GET /api/v1/posts — repurpose high-performing formats
```

---

## Approval Flow

Every outbound message goes through approval before sending.

### Telegram interface (`approval/telegramBot.js`)
```
APPROVE     — send as drafted
EDIT [text] — redraft with instructions (max 3 rounds)
SKIP        — skip this candidate (no send)
STOP        — pause Raxion globally
```

### Dashboard approval queue (Mission Control)
```
GET  /api/queue                    — pending approvals
POST /api/approve   { id }         — approve and send
POST /api/skip-approval { id }     — skip
POST /api/edit-approval { id, instructions } — redraft
```

When approved:
- Channel = `linkedin_dm`: Unipile `POST /api/v1/chats` (new) or `POST /api/v1/chats/{chatId}/messages` (reply)
- Channel = `email`: Unipile Gmail send
- Update `messages.status = 'sent'`, `messages.sent_at = now()`
- Update `candidates.pipeline_stage`, `last_contacted`, `last_contact_type`
- Dual-write Notion
- `sbLogActivity()`

---

## Candidate Research (`research/candidateResearcher.js`)

Uses **Gemini 2.5 Pro** (long context, web search):
```
Prompt includes:
- Candidate's LinkedIn URL + current title/company
- Full job description
- Client company context

Returns structured JSON:
{
  background_summary: string,       // 2–3 sentence professional summary
  relevant_experience: string[],    // bullet points matching the JD
  potential_concerns: string[],     // gaps, overqualified, location mismatch etc.
  web_findings: string,             // notable projects, GitHub, publications
  recommended_angle: string         // how to personalise the outreach
}

Appends [PERSON_RESEARCHED] marker to candidates.notes when complete.
Scoring phase only runs on [PERSON_RESEARCHED] candidates.
```

---

## Candidate Scoring (`scoring/candidateScorer.js`)

Uses **Claude Sonnet**:
```
Score 0–100 based on:
- Skills match vs job key_skills (40%)
- Experience level vs seniority required (25%)
- Location/availability (15%)
- Career trajectory / growth signal (10%)
- Red flags / concerns (–10 to –30)

Grades:
  85–100: HOT (immediate priority outreach)
  65–84:  WARM
  45–64:  COOL (outreach but lower priority)
  0–44:   Archive (skip outreach)

Stage after scoring:
  HOT/WARM/COOL → 'Ranked'
  Archive       → 'Archived' (excluded from invite phases)
```

---

## Pipeline Refill (`Phase 1`)

The orchestrator monitors pipeline health each cycle:
```javascript
// Count candidates at Ranked/Enriched/invite_pending stages (not yet contacted)
// If count < LOW_PIPELINE_THRESHOLD (default 15):
//   1. Pull from archived candidates (score >= min_candidate_score, re-evaluate)
//   2. Trigger LinkedIn search using job keywords → enqueue new sourced candidates
//   3. Check /imports folder for new CSV files → ingest via csvIngestor.js
//   4. Notify recruiter via Telegram if pipeline critically low
```

### CSV ingestor (`research/csvIngestor.js`)
Watches `/imports` folder. Accepts:
- **LinkedIn Sales Navigator exports** — maps: `Full Name`, `Current Company`, `Current Title`, `LinkedIn Profile URL`, `Location`
- **Manual CSV** — flexible column mapping with AI-assisted header detection
- De-duplicates by `linkedin_url` across ALL jobs (active and closed)
- Assigns to the active job specified in the filename or defaults to the first ACTIVE job

---

## Follow-up Sequences (`outreach/sequenceManager.js`)

```javascript
// Per-job cadence configured in jobs.followup_cadence_days (default [3, 7, 14])
// Only follow up candidates at: email_sent | dm_sent | invite_accepted
// Stages that block follow-ups: In Conversation | Shortlisted | Placed | Not Interested | Suppressed
//
// Follow-up message: context-aware redraft (not a copy-paste)
//   Mentions passage of time, reiterates the opportunity differently
//   Max follow-up number stored in messages table
//
// After max follow-up (usually #3): stage → 'Unresponsive', stop all outreach
```

---

## Job Assets (`job_assets` table)

Each job can have attached assets Raxion pulls into replies:
- **Calendly** — booking link (injected when MEETING_REQUEST detected)
- **JD** — full job description URL (injected when WANTS_MORE_INFO detected)
- **Video** — company culture video, Loom walkthrough (URL in message)
- **Image** — company/team photo URL
- **Link** — any other relevant URL (apply link, company website)

Assets are URL-based only. LinkedIn DM binary attachments (file upload via Unipile multipart) are technically supported but fragile — use public URLs in message text for reliability.

---

## LinkedIn Post Management

```javascript
// POST /api/jobs/:id/trigger-post — manually trigger a post draft
// Weekly scheduled post via orchestrator (configurable toggle)
//
// Draft includes: role, USPs, 3 key requirements, CTA (comment/DM)
// Approval: same Telegram/dashboard flow as messages
// Send: Unipile POST /api/v1/posts { account_id, text }
// Track: Unipile GET /api/v1/posts/:id/analytics — store impressions/reactions
```

---

## Firm/Company Suppression (`outreach/firmSuppressor.js`)

```javascript
// If ANY candidate at a company declines (Not Interested / OPT_OUT):
//   Check job config: suppress_whole_company (default false)
//   If true: flag all other candidates from same company as Suppressed — Firm Decline
// Prevents blanketing a single employer and damaging the recruiter's brand
```

---

## Inbound Reply Classification

```javascript
// AI classifies every inbound message:
intents = [
  'INTERESTED',           // → draft reply, continue conversation
  'WANTS_MORE_INFO',      // → draft reply, inject JD/assets
  'MEETING_REQUEST',      // → draft reply, inject Calendly
  'POSITIVE',             // → draft reply
  'NOT_INTERESTED',       // → closeConversation(), stage = Not Interested
  'OPT_OUT',              // → stage = Suppressed — Opt Out
  'ALREADY_PLACED',       // → stage = Not Available, suppress
  'NEUTRAL',              // → no action (noise/one-word reply)
  'OTHER',                // → notify recruiter, no auto-reply
]
// Classification includes: intent, sentiment, notes
// Stored in replies table
```

### Conversation close (`closeConversation()`)
```javascript
// Triggered on: NOT_INTERESTED | OPT_OUT | ALREADY_PLACED | manual close
// 1. Update pipeline_stage in Supabase
// 2. Fetch all replies for this candidate from replies table
// 3. AI (Claude) generates 3–5 sentence outcome summary
// 4. Write summary to Notion candidate notes (with timestamp + outcome label)
// 5. Telegram notification to recruiter
// 6. pushActivity() to Mission Control live feed
```

---

## Webhook Endpoints (Unipile)

Registered on startup via `integrations/unipileSetup.js`:

```javascript
webhooks = [
  {
    name:        'raxion_linkedin_relations',
    source:      'users',
    request_url: `${BASE_URL}/webhook/unipile/linkedin/relations`,
    account_ids: [LINKEDIN_ACCOUNT_ID],
    // Fires when someone accepts a connection request
  },
  {
    name:        'raxion_linkedin_messages',
    source:      'messaging',
    events:      ['message_received'],
    request_url: `${BASE_URL}/webhook/unipile/linkedin/messages`,
    account_ids: [LINKEDIN_ACCOUNT_ID],
    // Fires on every inbound LinkedIn DM
  },
  {
    name:        'raxion_gmail_inbound',
    source:      'email',
    events:      ['mail_received'],
    request_url: `${BASE_URL}/webhook/unipile/gmail`,
    account_ids: [GMAIL_ACCOUNT_ID],
    // Fires on every inbound email
  },
]
// Webhook handler: always log full raw body for debugging
// Try multiple field paths for provider_id (Unipile nests differently per event type):
//   payload?.attendee?.provider_id || payload?.user?.provider_id || payload?.provider_id
```

---

## Mission Control Dashboard

Single-page Express app on port 3000. Basic auth (`DASHBOARD_USER` / `DASHBOARD_PASS`).

### Views
- **Overview** — live stats: candidates sourced, messages sent, response rate, interviews booked, placements, total fees
- **Launch Job** — create new job with all config fields
- **Active Jobs** — per-job tabs: Overview | Pipeline | Rankings | Sequences | Assets | Settings
- **Pipeline** — cross-job candidate table with stage filter, sort, manual stage updates
- **Approval Queue** — pending messages: preview, approve, skip, edit
- **Live Activity** — real-time feed (WebSocket push), last 200 events
- **Job Archive** — closed/filled positions
- **Templates** — message template management per stage
- **Controls** — system toggles: outreachEnabled, followupEnabled, enrichmentEnabled, researchEnabled, linkedinEnabled, postsEnabled

### Key API routes
```
GET  /api/stats                       — live dashboard metrics
GET  /api/state                       — full system state
POST /api/toggle { key }              — toggle any module
GET  /api/queue                       — pending approvals
POST /api/approve { id }              — approve message
POST /api/skip-approval { id }        — skip
POST /api/edit-approval { id, instructions }
GET  /api/health                      — API health status
GET  /api/jobs                        — all jobs
GET  /api/jobs/:id                    — single job with metrics
POST /api/jobs/create                 — create job
PATCH /api/jobs/:id                   — update settings
POST /api/jobs/:id/pause              — pause job
POST /api/jobs/:id/resume             — resume job
POST /api/jobs/:id/close              — close job (filled/cancelled)
POST /api/jobs/:id/capital            — update fees/placements count
GET  /api/jobs/:id/assets             — list job assets
POST /api/jobs/:id/assets             — add asset { name, asset_type, url }
DELETE /api/jobs/:id/assets/:assetId  — remove asset
POST /api/jobs/:id/trigger-invites    — manually trigger LinkedIn invite batch
POST /api/jobs/:id/trigger-post       — trigger LinkedIn post draft
GET  /api/jobs/:id/metrics            — live pipeline metrics
POST /api/candidate/:id/stage { stage } — manual stage update (Supabase + Notion)
POST /api/candidates/:id/reactivate   — move archived candidate back to pipeline
DELETE /api/candidates/:id            — remove from pipeline
POST /api/contact/:id/shortlist       — advance to Shortlisted (Supabase + Notion)
POST /webhook/unipile/linkedin/relations — connection accepted
POST /webhook/unipile/linkedin/messages  — inbound DM
POST /webhook/unipile/gmail              — inbound email
```

### WebSocket events (server → client)
```
{ type: 'init', feed: [...] }          — on connect: last 100 activity items
{ type: 'activity', ...item }          — new live activity event
{ type: 'JOB_UPDATED', job }
{ type: 'JOB_CREATED', job }
{ type: 'JOB_DELETED', jobId }
{ type: 'QUEUE_UPDATED', count }
{ type: 'REPLY_RECEIVED', reply }
{ type: 'STATE_UPDATE', state }
{ type: 'STATS', data }
```

### Dashboard UX rules (avoid glitches)
- `fullRefresh()` runs every 30s but **skips re-rendering settings/sequences tabs** — only refreshes data-only tabs (overview, pipeline, rankings, archived) to avoid wiping unsaved form edits
- `saveJobSettings()` does NOT call `loadJobs()` after saving — uses `populateJobSelector()` only
- All save/action confirmations use toast notifications, never `alert()`
- `DEAL_UPDATED` / `JOB_UPDATED` WebSocket events do NOT trigger full re-render when a job detail panel is open
- Approval buttons show loading state while API call is in flight

---

## State File (`state.json`)

```json
{
  "raxionStatus": "ACTIVE",
  "outreachEnabled": true,
  "followupEnabled": true,
  "enrichmentEnabled": true,
  "researchEnabled": true,
  "linkedinEnabled": true,
  "postsEnabled": false,
  "activeJobIds": [],
  "outreachPausedUntil": null,
  "lastUpdated": "2026-03-14T..."
}
```

Read fresh at the top of every orchestrator cycle. Orchestrator respects changes within 10–15 minutes without restart.

---

## Environment Variables

```bash
# Claude / AI
ANTHROPIC_API_KEY=

# Gemini (research)
GEMINI_API_KEY=

# Supabase
SUPABASE_URL=
SUPABASE_ANON_KEY=
SUPABASE_SERVICE_ROLE_KEY=

# Notion CRM
NOTION_API_KEY=
NOTION_CONTACTS_DB_ID=          # Candidates database
NOTION_COMPANIES_DB_ID=         # Clients/firms database

# Unipile (LinkedIn + Gmail via API)
UNIPILE_API_KEY=
UNIPILE_DSN=                    # e.g. https://api34.unipile.com:16411
UNIPILE_LINKEDIN_ACCOUNT_ID=
UNIPILE_GMAIL_ACCOUNT_ID=

# Telegram approvals
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# Dashboard auth
DASHBOARD_USER=admin
DASHBOARD_PASS=

# Enrichment
KASPR_API_KEY=                  # email + phone enrichment
HUNTER_API_KEY=                 # email enrichment fallback

# Server
PORT=3000
BASE_URL=                       # public URL for Unipile webhook registration (e.g. https://raxion.yourdomain.com)
NODE_ENV=production

# Recruiter identity (used in message drafts)
RECRUITER_NAME=                 # e.g. "Sarah"
RECRUITER_FIRM=                 # e.g. "Apex Talent"
```

---

## Deployment

### PM2 (VPS)
```bash
pm2 start index.js --name raxion
pm2 save
pm2 startup
```

Log files: `logs/raxion-out.log`, `logs/raxion-error.log`

### SSH tunnel to dashboard
```bash
ssh -N -L 3000:localhost:3000 root@YOUR_VPS_IP
# Then open http://localhost:3000
```

### Railway / Render
- Set all env vars in dashboard
- Mount a volume at `/app/imports` for CSV ingestion
- `BASE_URL` must be the public Railway URL for webhook registration

---

## Critical Implementation Nuances

These are lessons from Roco that must be carried over to Raxion:

1. **Cross-job deduplication is Phase 0, not optional.** Check by `linkedin_url` AND `email`. A candidate appearing in two job pipelines simultaneously will receive conflicting outreach and damage recruiter credibility.

2. **Notion is the CRM view, Supabase is the source of truth.** Every stage change writes to Supabase first, then Notion. Notion failures are caught and logged but never block the Supabase write.

3. **LinkedIn contact lookup on inbound webhook must try three paths in order:** `linkedin_chat_id` → `linkedin_provider_id` → `email`. Only `linkedin_chat_id` is reliably set after a DM has been sent.

4. **Accepted connection webhook payload field paths vary by Unipile event type.** Always log the full raw body. Try: `payload?.attendee?.provider_id`, `payload?.user?.provider_id`, `payload?.provider_id`. The correct path can only be confirmed by inspecting a real webhook payload.

5. **The LinkedIn 28/day invite limit is enforced per-cycle by querying `invite_sent_at >= todayStart`.** It is NOT stored in state.json. This ensures accuracy across restarts.

6. **Non-pipeline webhook events must be silently ignored.** Use `!inner` join in Supabase to filter: only advance stage if the candidate exists in an active job (`status = 'ACTIVE'`, `paused = false`). Never push to Live Activity for unmatched webhooks.

7. **`closeConversation()` must always run when a terminal classification is received** (NOT_INTERESTED, OPT_OUT, ALREADY_PLACED). It generates an AI summary of the full reply thread and writes it to the candidate's Notion notes field with a timestamp and outcome label. This is the recruiter's record of what was said.

8. **Stage names must be exact strings.** Inconsistent casing or spacing breaks all downstream filters. The canonical stage list is:
   `Sourced | Enriched | Ranked | invite_sent | invite_accepted | dm_sent | email_sent | In Conversation | Shortlisted | Submitted | Interview | Offer | Placed | Not Interested | Unresponsive | Suppressed — Opt Out | Suppressed — Firm Decline | Archived | Inactive`

9. **The orchestrator reads `state.json` at the top of each cycle**, not once at startup. This means you can toggle modules (e.g. pause outreach) and it takes effect within one cycle without restarting the process.

10. **API fallback (`core/apiFallback.js`) must wrap every external API call** — Gemini, Claude, Unipile, KASPR, Notion, Supabase. Track health with 60s pings. The dashboard health grid reads from this module. Fallback order: primary AI → secondary AI → last-resort template.
