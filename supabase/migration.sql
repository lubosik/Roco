-- ─────────────────────────────────────────────────────────────────────────────
-- ROCO Supabase Schema Migration
-- Run this once in the Supabase SQL editor before deploying the Vercel dashboard
-- ─────────────────────────────────────────────────────────────────────────────

-- Sessions (system state — rocoStatus, toggles, etc.)
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY DEFAULT 'singleton',
  roco_status TEXT DEFAULT 'ACTIVE',
  outreach_enabled BOOLEAN DEFAULT TRUE,
  followup_enabled BOOLEAN DEFAULT TRUE,
  enrichment_enabled BOOLEAN DEFAULT TRUE,
  research_enabled BOOLEAN DEFAULT TRUE,
  linkedin_enabled BOOLEAN DEFAULT TRUE,
  active_deal_ids JSONB DEFAULT '[]',
  outreach_paused_until TIMESTAMPTZ,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO sessions (id) VALUES ('singleton') ON CONFLICT DO NOTHING;

-- Deals
CREATE TABLE IF NOT EXISTS deals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  status TEXT DEFAULT 'ACTIVE',
  raise_type TEXT,
  target_amount NUMERIC,
  committed_amount NUMERIC DEFAULT 0,
  minimum_cheque NUMERIC,
  maximum_cheque NUMERIC,
  sector TEXT,
  geography TEXT,
  description TEXT,
  key_metrics TEXT,
  investor_profile TEXT,
  deck_url TEXT,
  sending_days JSONB DEFAULT '["Mon","Tue","Wed","Thu","Fri"]',
  sending_start TEXT DEFAULT '08:00',
  sending_end TEXT DEFAULT '18:00',
  sending_timezone TEXT DEFAULT 'Europe/London',
  max_emails_per_day INTEGER DEFAULT 20,
  max_emails_per_hour INTEGER DEFAULT 5,
  batch_size INTEGER DEFAULT 20,
  followup_cadence_days JSONB DEFAULT '[3,7,14]',
  max_contacts_per_firm INTEGER DEFAULT 2,
  max_total_outreach INTEGER DEFAULT 500,
  min_investor_score INTEGER DEFAULT 40,
  prioritise_hot_leads BOOLEAN DEFAULT TRUE,
  include_unscored BOOLEAN DEFAULT FALSE,
  outreach_paused_until TIMESTAMPTZ,
  closed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Emails
CREATE TABLE IF NOT EXISTS emails (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deal_id UUID REFERENCES deals(id),
  contact_id TEXT,
  contact_name TEXT,
  contact_email TEXT,
  firm TEXT,
  stage TEXT,
  subject TEXT,
  body TEXT,
  status TEXT DEFAULT 'pending',
  batch_id UUID,
  queued_to_send_at TIMESTAMPTZ,
  sent_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Replies
CREATE TABLE IF NOT EXISTS replies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deal_id UUID REFERENCES deals(id),
  contact_id TEXT,
  email_id UUID REFERENCES emails(id),
  body TEXT,
  classification TEXT,
  confidence INTEGER,
  received_at TIMESTAMPTZ DEFAULT NOW()
);

-- Email templates
CREATE TABLE IF NOT EXISTS email_templates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL UNIQUE,
  type TEXT DEFAULT 'email',
  subject_a TEXT,
  subject_b TEXT,
  body TEXT,
  notes TEXT,
  variables JSONB DEFAULT '[]',
  ab_test_enabled BOOLEAN DEFAULT TRUE,
  is_active BOOLEAN DEFAULT TRUE,
  updated_by TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Batches
CREATE TABLE IF NOT EXISTS batches (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deal_id UUID REFERENCES deals(id),
  batch_number INTEGER,
  status TEXT DEFAULT 'ACTIVE',
  contact_ids JSONB DEFAULT '[]',
  launched_at TIMESTAMPTZ DEFAULT NOW(),
  followup1_at TIMESTAMPTZ,
  followup2_at TIMESTAMPTZ,
  followup3_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Activity log
CREATE TABLE IF NOT EXISTS activity_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deal_id UUID REFERENCES deals(id),
  contact_id TEXT,
  event_type TEXT,
  summary TEXT,
  detail TEXT,
  api_used TEXT,
  fallback_used BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Firm responses
CREATE TABLE IF NOT EXISTS firm_responses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deal_id UUID REFERENCES deals(id),
  company_name TEXT,
  response_type TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Schedule log
CREATE TABLE IF NOT EXISTS schedule_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  deal_id UUID REFERENCES deals(id),
  event_type TEXT,
  detail TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Approval queue (bridges VPS in-memory approvals with Vercel dashboard)
CREATE TABLE IF NOT EXISTS approval_queue (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  telegram_msg_id BIGINT,
  status TEXT DEFAULT 'pending',
  contact_id TEXT,
  contact_name TEXT,
  contact_email TEXT,
  firm TEXT,
  deal_name TEXT,
  stage TEXT,
  subject_a TEXT,
  subject_b TEXT,
  body TEXT,
  score INTEGER,
  research_summary TEXT,
  edit_instructions TEXT,
  approved_subject TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  resolved_at TIMESTAMPTZ
);

-- Index for fast pending queue lookups
CREATE INDEX IF NOT EXISTS idx_approval_queue_status ON approval_queue(status);
CREATE INDEX IF NOT EXISTS idx_approval_queue_telegram_msg ON approval_queue(telegram_msg_id);

-- Deal assets — Calendly links, deck URLs, images, videos etc. per deal
CREATE TABLE IF NOT EXISTS deal_assets (
  id          UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  deal_id     UUID NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
  name        TEXT NOT NULL,
  asset_type  TEXT NOT NULL CHECK (asset_type IN ('calendly','deck','image','video','link','other')),
  url         TEXT NOT NULL,
  description TEXT,
  created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS deal_assets_deal_id_idx ON deal_assets(deal_id);
-- Add sector_focus column to investors_db (run once in Supabase SQL Editor)
ALTER TABLE investors_db ADD COLUMN IF NOT EXISTS sector_focus TEXT;
