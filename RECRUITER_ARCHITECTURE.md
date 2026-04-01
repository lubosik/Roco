# Roco Recruiter — Technical Architecture
> Autonomous AI recruiting agent. Sources candidates, manages conversations, ranks by fit, books interviews.
> Built on the same infrastructure as Roco (Supabase, Unipile, Gemini, KASPR, Notion, Telegram, Express dashboard).

---

## 1. System Overview

```
Job Briefing Form (Dashboard)
        ↓
LinkedIn Search + GitHub API → candidates added to Supabase (stage: 'Sourced')
        ↓
Gemini Person Research → fills skills, experience, GitHub activity, past employers
        ↓
AI Ranking (0–100 fit score) → Shortlisted / Archived
        ↓
KASPR Enrichment → email, phone
        ↓
Notion CRM sync → candidate page created/updated
        ↓
LinkedIn Connection Request (Unipile) → stage: 'invite_sent'
        ↓
Invite Accepted → DM outreach → stage: 'dm_sent'
        ↓
Email outreach (Unipile Gmail) → stage: 'email_sent'
        ↓
Reply received → AI qualification check → stage: 'Replied'
        ↓
Qualified → Calendly/booking link sent → stage: 'Interview Booked'
        ↓
Interview Confirmed → stage: 'Interview Scheduled'
        ↓
Post-interview → Offered / Rejected / Placed
```

**Orchestrator loop**: every 15 minutes, processes all active jobs through the phase pipeline.
**Dashboard**: same Express + WebSocket dashboard, re-skinned for recruiting context.
**Approval flow**: outreach messages go to Telegram for recruiter approval before sending (same as Roco).

---

## 2. Database Schema (Supabase)

### `jobs` table (replaces `deals`)
```sql
id                    uuid PRIMARY KEY DEFAULT gen_random_uuid()
name                  text NOT NULL                    -- e.g. "Senior Backend Engineer – Fintech"
status                text DEFAULT 'ACTIVE'            -- ACTIVE | PAUSED | CLOSED | FILLED
recruiter_name        text                             -- who owns this job
client_name           text                             -- hiring company
job_title             text                             -- exact title to source for
seniority_level       text                             -- Junior | Mid | Senior | Lead | Principal
employment_type       text                             -- Full-time | Contract | Part-time
location              text                             -- Remote | London | Hybrid – London
salary_min            numeric
salary_max            numeric
currency              text DEFAULT 'GBP'
equity                text                             -- e.g. "0.1–0.5%"
sector                text                             -- Fintech | HealthTech | AI | SaaS...
tech_stack            text                             -- comma-separated: "Python, FastAPI, Postgres"
must_have_skills      text                             -- non-negotiable requirements
nice_to_have_skills   text
years_experience_min  int
years_experience_max  int
visa_sponsorship      boolean DEFAULT false
candidate_profile     text                             -- free-text ideal candidate description
target_candidates     int DEFAULT 28                   -- daily outreach target (same as linkedin_daily_limit)
linkedin_daily_limit  int DEFAULT 28
description           text                             -- full job description
interview_stages      text                             -- e.g. "Screening → Technical → Final"
calendly_link         text                             -- booking link sent to qualified candidates
notify_email          text                             -- recruiter email for alerts
committed_placements  int DEFAULT 0
target_placements     int DEFAULT 1
created_at            timestamptz DEFAULT now()
closed_at             timestamptz
paused                boolean DEFAULT false
```

### `candidates` table (replaces `contacts`)
```sql
id                    uuid PRIMARY KEY DEFAULT gen_random_uuid()
job_id                uuid REFERENCES jobs(id)
name                  text
email                 text
phone                 text
linkedin_url          text
linkedin_provider_id  text
github_url            text
github_username       text
current_title         text
current_company       text
location              text
years_experience      int
tech_skills           text                             -- comma-separated from GitHub/LinkedIn/Gemini
top_languages         text                             -- from GitHub API: "Python, TypeScript, Go"
github_contributions  int                              -- last 12 months
github_stars          int                              -- total stars across repos
open_source_notable   text                             -- notable OSS projects/contributions
past_employers        text                             -- comma-separated
education             text
salary_expectation    text
notice_period         text
visa_status           text                             -- Citizen | Settled | Visa Required
fit_score             int                              -- 0–100 AI ranking
fit_grade             text                             -- HOT | WARM | POSSIBLE | ARCHIVE
fit_rationale         text
pipeline_stage        text DEFAULT 'Sourced'
  -- Sourced → Shortlisted → Enriched → invite_sent → invite_accepted
  -- → dm_sent → email_sent → Replied → Qualified → Interview Booked
  -- → Interview Scheduled → Offered → Placed | Rejected | Withdrawn
enrichment_status     text DEFAULT 'Pending'           -- Pending | Enriched | No Data
notion_page_id        text
notes                 text                             -- Gemini research + [PERSON_RESEARCHED] marker
source                text                             -- 'LinkedIn Search' | 'GitHub' | 'CSV Import'
invite_sent_at        timestamptz
follow_up_count       int DEFAULT 0
follow_up_due_at      timestamptz
qualified_at          timestamptz
interview_booked_at   timestamptz
created_at            timestamptz DEFAULT now()
```

### `activity_log` table (identical to Roco)
```sql
id          uuid PRIMARY KEY DEFAULT gen_random_uuid()
job_id      uuid REFERENCES jobs(id)
candidate_id uuid REFERENCES candidates(id)
event_type  text
summary     text
detail      jsonb
api_used    text
created_at  timestamptz DEFAULT now()
```

### `emails` table (identical to Roco)
```sql
id          uuid PRIMARY KEY DEFAULT gen_random_uuid()
job_id      uuid REFERENCES jobs(id)
candidate_id uuid REFERENCES candidates(id)
subject     text
body        text
status      text    -- draft | pending_approval | sent | replied
channel     text    -- email | linkedin_dm
created_at  timestamptz DEFAULT now()
```

---

## 3. Environment Variables
All existing Roco env vars carry over directly. No new keys needed.

```bash
# Already configured — all reused as-is:
SUPABASE_URL=
SUPABASE_SERVICE_KEY=
GEMINI_API_KEY=                  # person + firm research
GEMINI_API_KEY_FALLBACK=
KASPR_API_KEY=                   # email/phone enrichment
UNIPILE_API_KEY=                 # LinkedIn messages + Gmail
UNIPILE_DSN=
UNIPILE_LINKEDIN_ACCOUNT_ID=
UNIPILE_GMAIL_ACCOUNT_ID=
ANTHROPIC_API_KEY=               # AI ranking + message drafting
OPENAI_API_KEY=                  # fallback AI
NOTION_API_KEY=                  # CRM sync
NOTION_CONTACTS_DB_ID=           # reuse same DB or create new one for candidates
TELEGRAM_BOT_TOKEN=              # recruiter approval flow
TELEGRAM_CHAT_ID=
REPLY_TO_EMAIL=
SENDER_NAME=

# New — add to .env:
GITHUB_TOKEN=                    # GitHub API personal access token (public data, optional but avoids rate limits)
CALENDLY_API_KEY=                # optional — for auto-booking confirmation webhooks
```

---

## 4. Core Modules

### `research/candidateResearcher.js`
Uses Gemini with Google Search grounding (same pattern as `personResearcher.js`).

**Input**: `{ candidate, job }` — candidate name, LinkedIn URL, GitHub username, job context
**Output**: structured JSON written to `candidates` table
```json
{
  "current_title": "Senior Backend Engineer",
  "current_company": "Monzo",
  "years_experience": 7,
  "tech_skills": "Python, FastAPI, PostgreSQL, Redis, Kubernetes",
  "past_employers": "Revolut, Starling Bank",
  "education": "CS BSc, UCL",
  "location": "London, UK",
  "visa_status": "Settled",
  "github_url": "https://github.com/username",
  "open_source_notable": "Contributor to FastAPI core",
  "salary_expectation": "£120k–£140k",
  "summary": "2-3 sentence professional overview",
  "confidence": "high|medium|low"
}
```

### `research/githubEnricher.js`
Calls GitHub REST API to fill in technical signal before ranking.

```javascript
// GET /users/:username — basic profile
// GET /users/:username/repos — public repos
// GET /search/commits?author=:username — contribution signal

// Extracts:
{
  top_languages: "Python, TypeScript, Go",       // from repo language breakdown
  github_stars: 847,                             // sum of stars across all repos
  github_contributions: 1240,                    // commits in last 12 months
  open_source_notable: "fastapi (23 PRs merged)" // notable contributions
}
```
Rate limit: 5,000 requests/hour with GITHUB_TOKEN.
Falls back gracefully if username not found — returns null, doesn't block pipeline.

### `research/candidateRanker.js`
AI-powered fit scoring (same pattern as `investorRanker.js`).

**Scoring criteria (0–100 total)**:
| Criterion | Points | Logic |
|---|---|---|
| Tech stack match | 35 | Must-have skills matched vs job requirement |
| Seniority fit | 20 | Years experience vs min/max requirement |
| Location/visa | 15 | Remote OK / local / visa sponsored |
| Sector/domain experience | 15 | Past employers in relevant sector |
| GitHub signal | 15 | Active OSS, stars, contribution volume |

**Grade thresholds**:
- HOT: 80+
- WARM: 60–79
- POSSIBLE: 40–59
- ARCHIVE: 0–39

**Model**: uses `aiComplete()` (GPT/Claude fallback chain, same as Roco)

### `research/jobResearcher.js`
Runs once at job creation (same as `dealResearcher.js`).

**Sources**:
1. **LinkedIn People Search** (Unipile) — keyword search for job title + skills + location
2. **Gemini Deep Research** — prompt asks for real, active professionals matching the brief

Both results deduplicated by LinkedIn URL, saved to `candidates` table with `pipeline_stage: 'Sourced'`.

### `enrichment/candidateEnricher.js`
Wraps KASPR (same as `kaspEnricher.js`) — looks up email and phone by LinkedIn URL.

Advance to `Enriched` only if email found. Otherwise stays `Shortlisted`.

### `outreach/candidateMessageDrafter.js`
Uses AI to draft LinkedIn DMs and emails.
Context passed: job title, client name, salary range, tech stack, candidate's background, pipeline stage.

```javascript
// Connection request (300 char limit):
// "Hi [Name], I'm recruiting for a Senior Python Engineer role at a UK fintech —
//  your FastAPI background looks like a great fit. Happy to share details if you're open."

// Follow-up DM (after connection accepted):
// Personalised pitch referencing their specific background + the role

// Email (if KASPR found email):
// Longer-form with full job brief, salary, equity, what makes it interesting
```

Prior outreach awareness: checks `[PRIOR_JOB:...]` marker in notes — same cross-deal logic as Roco.

### `qualification/qualificationChecker.js`
Triggered when a candidate replies. AI reads the reply and determines:
```json
{
  "interested": true,
  "key_concerns": "Salary might be low, asks about remote policy",
  "qualified": true,
  "next_action": "send_booking_link | answer_question | escalate_to_recruiter | archive"
}
```
If `qualified: true` → sends Calendly link automatically (or drafts a response for Telegram approval).
If concerns → drafts a tailored reply addressing them.
If clearly not interested → archives with reason.

---

## 5. Orchestrator (`core/orchestrator.js`)

Pipeline phases run every 15 minutes per active job:

```
Phase 0:  phaseTopUpPipeline     — ensure ≥10 candidates ready; auto-promote borderline archived;
                                   trigger new research if depleted (once/day cooldown)
Phase 0b: phaseCrossJobCheck     — hold candidates already in another active job pipeline
Phase 1:  phaseCandidateResearch — Gemini person research + GitHub enrichment (BEFORE ranking)
Phase 2:  phaseRank              — AI fit scoring (only after [PERSON_RESEARCHED] marker)
Phase 3:  phaseArchive           — move fit_score < 40 to Archived
Phase 4:  phaseEnrich            — KASPR email/phone lookup
Phase 5:  phaseNotionSync        — create/update Notion candidate pages
Phase 6:  phaseLinkedInInvites   — send connection requests (daily limit per job)
Phase 7:  phaseOutreach          — DMs to invite_accepted; emails to Enriched
Phase 8:  phaseQualification     — process replies, check qualification, send booking links
Phase 9:  phaseFollowUps         — 3-7-14 day follow-up cadence
```

Key constants:
```javascript
const DAILY_INVITE_TARGET = 28;       // LinkedIn connection requests per day per job
const REACTIVATION_MIN_SCORE = 40;    // re-promote archived candidates above this score
const PIPELINE_LOW_THRESHOLD = 10;    // trigger top-up when ready pipeline drops below this
const RESEARCH_COOLDOWN_MS = 24 * 60 * 60 * 1000; // max 1 auto-research run per day per job
```

---

## 6. Dashboard (`dashboard/`)

### Re-skinned sections (same infrastructure, different labels):

| Roco label | Recruiter label |
|---|---|
| Active Deals | Active Jobs |
| Deal Archive | Closed Jobs |
| Launch Deal | Post a Job |
| Prospects | Candidates |
| Active Prospects | Candidates in Outreach |
| Contacts | Candidates |
| Sector | Tech Stack / Role Type |
| Target Amount | Salary Budget |
| Committed | Placements Made |

### Dashboard tabs per job (same structure as deal tabs):
- **Overview** — metrics: invites sent, acceptance rate, replies, qualified, interviews booked, placed
- **Pipeline** — live candidate pipeline table with stage, fit score, enrichment status
- **Rankings** — all ranked candidates sorted by fit_score, HOT/WARM/POSSIBLE grades
- **Settings** — job brief fields, schedule, daily limits, Calendly link
- **Archived** — rejected/withdrawn candidates with Re-activate button

### New dashboard section: **Job Briefing Form** (replaces Launch Deal)

Fields:
```
Job Title *
Client / Hiring Company *
Seniority Level (dropdown: Junior/Mid/Senior/Lead/Principal)
Employment Type (Full-time / Contract / Part-time)
Location / Remote Policy
Salary Range (min / max / currency)
Equity (optional)
Sector / Industry
Tech Stack — Must Have *
Tech Stack — Nice to Have
Years Experience (min / max)
Visa Sponsorship? (toggle)
Ideal Candidate Description (free text)
Full Job Description (textarea)
Interview Stages
Calendly Booking Link
Recruiter Email for Alerts
Daily Outreach Target (default: 28)
```

On submit:
1. Save job to `jobs` table
2. Run `jobResearcher.js` (LinkedIn search + Gemini) — finds initial candidate list
3. Trigger immediate orchestrator cycle for this job

### API routes (server.js additions):

```
POST   /api/jobs/create                    — create job, trigger research
GET    /api/jobs                           — all active jobs
GET    /api/jobs/:id                       — single job with metrics
PATCH  /api/jobs/:id                       — update job settings
POST   /api/jobs/:id/pause                 — pause a job
POST   /api/jobs/:id/resume                — resume a job
POST   /api/jobs/:id/close                 — close/fill a job, release cross-job holds
DELETE /api/jobs/:id                       — permanently delete job + all candidates
GET    /api/jobs/:id/rankings              — ranked candidates sorted by fit_score
GET    /api/jobs/:id/archived              — archived candidates with Re-activate button
POST   /api/jobs/:id/trigger-invites       — manual LinkedIn invite send
GET    /api/jobs/:id/metrics               — live stats

POST   /api/candidates/:id/reactivate      — move archived candidate back to Shortlisted
DELETE /api/candidates/:id                 — remove candidate permanently
POST   /api/contact/:id/stage             — update candidate pipeline stage

GET    /api/stats                          — active_placements, candidates_in_outreach,
                                             interviews_booked, response_rate, queue_count

POST   /api/action { action: 'run_research', jobId }
POST   /api/action { action: 'run_enrichment' }
POST   /api/action { action: 'flush_queue' }
POST   /api/action { action: 'pause_all' }
```

### Stats tracked (Overview + Live Activity):

```javascript
{
  candidates_sourced:       // total in pipeline
  candidates_in_outreach:   // invite_sent | dm_sent | email_sent | Replied | Qualified | Interview*
  invites_sent:             // LinkedIn connections sent
  acceptance_rate:          // invite_accepted / invites_sent %
  reply_rate:               // Replied / outreach_sent %
  qualified_rate:           // Qualified / Replied %
  interviews_booked:        // Interview Booked + Interview Scheduled count
  placements_made:          // Placed count
  active_jobs:              // ACTIVE job count
  approval_queue:           // pending Telegram approvals
}
```

### Live Activity feed (identical to Roco):
Events streamed via WebSocket:
- `CANDIDATE_SOURCED` — N new candidates found for [Job]
- `CANDIDATE_RANKED` — [Name] scored 84 — HOT for [Job]
- `CANDIDATE_ENRICHED` — Email found for [Name]
- `LINKEDIN_INVITE_SENT` — Connection request sent to [Name]
- `INVITE_ACCEPTED` — [Name] accepted your connection request
- `DM_SENT` — Outreach message sent to [Name]
- `EMAIL_SENT` — Email sent to [Name]
- `REPLY_RECEIVED` — [Name] replied
- `CANDIDATE_QUALIFIED` — [Name] marked as qualified, booking link sent
- `INTERVIEW_BOOKED` — [Name] booked an interview for [Job]
- `CANDIDATE_PLACED` — [Name] placed at [Client] 🎉
- `PIPELINE_TOP_UP` — N borderline candidates reactivated for [Job]
- `AUTO_RESEARCH` — Top-up research found N new candidates

---

## 7. Notion CRM Integration

Candidate page properties (maps to `updateContact()` in `notionContacts.js`):

```
Name                text title
Email               email
Phone               phone
LinkedIn URL        url
GitHub URL          url
Current Title       rich_text
Current Company     rich_text
Tech Skills         rich_text
Top Languages       rich_text        (from GitHub)
GitHub Stars        number
Years Experience    number
Location            rich_text
Visa Status         select
Salary Expectation  rich_text
Fit Score (0-100)   number
Fit Grade           select (HOT / WARM / POSSIBLE)
Pipeline Stage      select
Enrichment Status   select
Job                 relation → jobs DB
Notes               rich_text        (Gemini research + all markers)
Source              select
Created             date
```

---

## 8. GitHub API Integration (`research/githubEnricher.js`)

```javascript
const BASE = 'https://api.github.com';
const headers = {
  Authorization: `token ${process.env.GITHUB_TOKEN}`,
  Accept: 'application/vnd.github.v3+json',
};

// 1. Resolve username from LinkedIn URL or candidate name (Gemini search)
// 2. Fetch profile: GET /users/:username
// 3. Fetch repos: GET /users/:username/repos?sort=stars&per_page=30
// 4. Count contributions: GET /search/commits?q=author-email:${email}+committer-date:>one_year_ago
//    (requires authenticated token for best results)
// 5. Aggregate top languages from repos weighted by repo stars

// Returns null cleanly if:
// - Username not found
// - API rate limited (falls back, doesn't block pipeline)
// - Profile is private
```

---

## 9. Qualification Flow

When a LinkedIn DM reply or email reply comes in (via Unipile webhook):

```javascript
// webhook: POST /webhook/unipile-reply
// 1. Identify candidate by thread/message metadata
// 2. Update pipeline_stage → 'Replied'
// 3. Run qualificationChecker.js:
//    - AI reads reply + job requirements + candidate profile
//    - Returns: { interested, qualified, concerns, next_action }
// 4. If qualified → draft booking message with Calendly link
//    → send to Telegram for recruiter approval
//    → on approve: send DM/email, update stage to 'Qualified'
// 5. If concerns → draft response addressing them → Telegram approval
// 6. If not interested → archive with reason, log activity
```

---

## 10. Cross-Job Deduplication

Same logic as Roco cross-deal check:

- Before ranking, check if candidate's LinkedIn URL exists in another **active** job's pipeline
- If yes → `Skipped` with `[CROSS_JOB_HOLD:jobId|jobName]` in notes
- When that job closes → release candidate to `Sourced` stage in waiting job, add `[PRIOR_JOB:jobName|contacted:date]`
- AI drafters detect `[PRIOR_JOB:]` marker → reference prior interaction, don't cold-approach

---

## 11. Approval Flow (Telegram)

Identical to Roco. Every outreach message (connection request, DM, email) sent to recruiter via Telegram bot before firing.

Telegram message format:
```
👤 CANDIDATE: [Name] — [Current Title] at [Company]
💼 JOB: [Job Title] at [Client]
📊 Fit Score: 84 — HOT
📍 Stage: INTRO DM

--- MESSAGE ---
[Draft message]
---------------

✅ Approve | ✏️ Edit | ⏭ Skip
```

Buttons: Approve / Edit (re-drafts with instructions) / Skip (moves to next candidate).

---

## 12. File Structure

```
/
├── core/
│   ├── orchestrator.js          # main loop — job phases
│   ├── supabase.js
│   ├── supabaseSync.js          # getActiveJobs, getDeal→getJob, logActivity
│   ├── aiClient.js              # aiComplete() — unchanged
│   ├── apiFallback.js           # API health tracking — unchanged
│   ├── scheduleChecker.js       # sending window logic — unchanged
│   └── state.js                 # state.json read/write — unchanged
│
├── research/
│   ├── jobResearcher.js         # replaces dealResearcher.js — LinkedIn search + Gemini
│   ├── candidateResearcher.js   # replaces personResearcher.js — Gemini person research
│   ├── candidateRanker.js       # replaces investorRanker.js — fit scoring
│   └── githubEnricher.js        # NEW — GitHub API signal
│
├── enrichment/
│   ├── candidateEnricher.js     # wraps kaspEnricher.js for candidates
│   └── linkedinFinder.js        # unchanged
│
├── qualification/
│   └── qualificationChecker.js  # NEW — reply analysis + booking trigger
│
├── outreach/
│   ├── candidateMessageDrafter.js  # replaces emailDrafter.js
│   └── linkedinDrafter.js          # unchanged (reused)
│
├── crm/
│   └── notionContacts.js        # unchanged — same property mapping
│
├── integrations/
│   └── unipileClient.js         # unchanged — LinkedIn + Gmail via Unipile
│
├── approval/
│   └── telegramBot.js           # unchanged — same approval flow
│
├── dashboard/
│   ├── server.js                # routes renamed jobs/* candidates/*
│   └── public/
│       ├── index.html           # re-skinned — Active Jobs, Post a Job, etc.
│       └── dashboard.js         # same SPA logic, job/candidate naming
│
├── config/
│   └── constants.js             # DAILY_INVITE_TARGET, thresholds
│
├── templates/
│   └── messageTemplates.js      # LinkedIn + email templates for recruiting
│
├── .env                         # all existing vars + GITHUB_TOKEN
├── index.js                     # entry point — unchanged
└── state.json                   # rocoStatus → recruiterStatus, same structure
```

---

## 13. Key Differences from Roco

| Roco (Fundraising) | Recruiter |
|---|---|
| Investor fit scoring | Candidate fit scoring |
| Firm AUM, sector focus, cheque size | Tech skills, years exp, GitHub signal |
| `deals` table | `jobs` table |
| `contacts` table | `candidates` table |
| Active Deals | Active Jobs |
| Active Prospects | Candidates in Outreach |
| Committed Amount | Placements Made |
| Deal research (find investors) | Job research (find candidates) |
| investorRanker.js | candidateRanker.js |
| personResearcher.js | candidateResearcher.js + githubEnricher.js |
| No qualification step | qualificationChecker.js → booking link |
| Calendly not needed | Calendly link per job |

---

## 14. Build Order for Codex

Implement in this sequence to keep the system testable at each step:

1. `jobs` + `candidates` tables in Supabase (SQL above)
2. `core/supabaseSync.js` — add `getActiveJobs`, `getJob`, `createJob`, `updateJob` (mirrors existing deal functions)
3. `research/jobResearcher.js` — copy dealResearcher.js, rename deal→job, contact→candidate
4. `research/candidateResearcher.js` — copy personResearcher.js, rename, adjust prompt for recruiting context
5. `research/githubEnricher.js` — new module, GitHub REST API calls
6. `research/candidateRanker.js` — copy investorRanker.js, replace scoring criteria with fit criteria above
7. `enrichment/candidateEnricher.js` — thin wrapper around existing kaspEnricher.js
8. `qualification/qualificationChecker.js` — new module, AI reply analysis
9. `outreach/candidateMessageDrafter.js` — copy emailDrafter.js, recruiting-specific prompts
10. `core/orchestrator.js` — add job phases (can run alongside deal phases or replace)
11. `dashboard/server.js` — add /api/jobs/* and /api/candidates/* routes
12. `dashboard/public/` — re-skin index.html + dashboard.js (rename labels, add Job Briefing form)
13. Test end-to-end with one job: create → research → rank → enrich → invite → qualify → book

---

*Generated by Claude Code — copy this file into your Codex assistant with your existing .env and dashboard codebase.*
