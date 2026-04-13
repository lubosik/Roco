-- ROCO — Supabase Schema Setup
-- Run this in the Supabase SQL Editor: https://supabase.com/dashboard/project/xunqaxmqdknlrqdztepw/sql

-- ─── DEALS ────────────────────────────────────────────────────────────────────
create table if not exists deals (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  status text default 'ACTIVE',
  target_amount bigint,
  committed_amount bigint default 0,
  sector text,
  geography text,
  description text,
  key_metrics text,
  raise_type text,
  minimum_cheque bigint,
  maximum_cheque bigint,
  deck_url text,
  investor_profile text,
  -- Schedule fields
  sending_days text[] default array['monday','tuesday','wednesday','thursday','friday'],
  sending_start time default '08:00',
  sending_end time default '18:00',
  sending_timezone text default 'America/New_York',
  max_emails_per_day integer default 20,
  max_emails_per_hour integer default 5,
  batch_size integer default 15,
  batch_followup_wait_days integer default 3,
  followup_cadence_days integer[] default array[3,7,14],
  outreach_paused_until timestamptz,
  max_contacts_per_firm integer default 3,
  max_total_outreach integer default 200,
  min_investor_score integer default 60,
  prioritise_hot_leads boolean default true,
  include_unscored boolean default false,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  closed_at timestamptz,
  created_by text default 'dom'
);

-- ─── CONTACTS ─────────────────────────────────────────────────────────────────
create table if not exists contacts (
  id uuid primary key default gen_random_uuid(),
  notion_page_id text unique,
  name text not null,
  company_name text,
  title text,
  linkedin_url text,
  email text,
  phone text,
  investor_score integer,
  sector_focus text,
  typical_cheque_size text,
  geography text,
  source text,
  pipeline_stage text,
  enrichment_status text default 'Pending',
  kondo_label text,
  similar_past_deals text,
  pitchbook_reference text,
  notes text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- ─── COMPANIES ────────────────────────────────────────────────────────────────
create table if not exists companies (
  id uuid primary key default gen_random_uuid(),
  notion_page_id text unique,
  company_name text not null,
  type text,
  sector_focus text,
  typical_cheque_size text,
  geography text,
  aum text,
  website text,
  linkedin_page text,
  investor_score integer,
  pipeline_stage text,
  status text default 'Active',
  notes text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- ─── DEAL_CONTACTS ────────────────────────────────────────────────────────────
create table if not exists deal_contacts (
  id uuid primary key default gen_random_uuid(),
  deal_id uuid references deals(id) on delete cascade,
  contact_id uuid references contacts(id) on delete cascade,
  stage text default 'Researched',
  investor_score integer,
  company_name text,
  assigned_at timestamptz default now(),
  unique(deal_id, contact_id)
);

-- ─── EMAILS ───────────────────────────────────────────────────────────────────
create table if not exists emails (
  id uuid primary key default gen_random_uuid(),
  deal_id uuid references deals(id),
  contact_id uuid references contacts(id),
  contact_email text,
  direction text default 'outbound',
  stage text,
  subject_a text,
  subject_b text,
  subject_used text,
  body text,
  body_edited text,
  status text default 'draft',
  approved_by text,
  approved_at timestamptz,
  sent_at timestamptz,
  queued_to_send_at timestamptz,
  gmail_message_id text,
  gmail_thread_id text,
  ab_variant text,
  open_tracked boolean default false,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- ─── REPLIES ──────────────────────────────────────────────────────────────────
create table if not exists replies (
  id uuid primary key default gen_random_uuid(),
  deal_id uuid references deals(id),
  contact_id uuid references contacts(id),
  email_id uuid references emails(id),
  body text,
  classification text,
  classification_confidence integer,
  classification_reason text,
  gmail_message_id text,
  gmail_thread_id text,
  received_at timestamptz default now(),
  actioned boolean default false,
  action_taken text
);

-- ─── LINKEDIN_MESSAGES ────────────────────────────────────────────────────────
create table if not exists linkedin_messages (
  id uuid primary key default gen_random_uuid(),
  deal_id uuid references deals(id),
  contact_id uuid references contacts(id),
  direction text,
  body text,
  kondo_label text,
  status text,
  received_at timestamptz default now(),
  sent_at timestamptz
);

-- ─── ACTIVITY_LOG ─────────────────────────────────────────────────────────────
create table if not exists activity_log (
  id uuid primary key default gen_random_uuid(),
  deal_id uuid references deals(id),
  contact_id uuid references contacts(id),
  event_type text not null,
  summary text,
  detail jsonb,
  api_used text,
  fallback_used boolean default false,
  created_at timestamptz default now()
);

create table if not exists outreach_events (
  id uuid primary key default gen_random_uuid(),
  deal_id uuid references deals(id),
  contact_id uuid references contacts(id),
  event_type text not null,
  channel text,
  status text not null default 'confirmed',
  provider text,
  provider_message_id text,
  provider_account_id text,
  metadata jsonb default '{}'::jsonb,
  created_at timestamptz default now()
);

-- ─── FIRM_SUPPRESSIONS ────────────────────────────────────────────────────────
create table if not exists firm_suppressions (
  id uuid primary key default gen_random_uuid(),
  deal_id uuid references deals(id),
  company_name text not null,
  triggered_by_contact text,
  reason text,
  contacts_suppressed integer default 0,
  suppression_type text default 'DECLINE',
  created_at timestamptz default now()
);

-- ─── EMAIL_TEMPLATES ──────────────────────────────────────────────────────────
create table if not exists email_templates (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  type text not null,
  subject_a text,
  subject_b text,
  body text not null,
  variables jsonb,
  is_active boolean default true,
  ab_test_enabled boolean default true,
  notes text,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  updated_by text
);

-- ─── API_HEALTH_LOG ───────────────────────────────────────────────────────────
create table if not exists api_health_log (
  id uuid primary key default gen_random_uuid(),
  api_name text not null,
  status text not null,
  fallback_active boolean default false,
  error_message text,
  recorded_at timestamptz default now()
);

-- ─── SESSIONS ─────────────────────────────────────────────────────────────────
create table if not exists sessions (
  id text primary key default 'singleton',
  roco_status text default 'ACTIVE',
  outreach_enabled boolean default true,
  followup_enabled boolean default true,
  enrichment_enabled boolean default true,
  research_enabled boolean default true,
  linkedin_enabled boolean default true,
  active_deal_ids text[] default '{}',
  outreach_paused_until timestamptz,
  updated_at timestamptz default now()
);

insert into sessions (id) values ('singleton') on conflict (id) do nothing;

-- ─── BATCHES ──────────────────────────────────────────────────────────────────
create table if not exists batches (
  id uuid primary key default gen_random_uuid(),
  deal_id uuid references deals(id) on delete cascade,
  batch_number integer not null,
  status text default 'ACTIVE',
  contact_ids uuid[],
  launched_at timestamptz default now(),
  completed_at timestamptz,
  contacts_total integer,
  contacts_replied integer default 0,
  contacts_inactive integer default 0,
  intro_sent_at timestamptz,
  followup1_sent_at timestamptz,
  followup2_sent_at timestamptz,
  followup3_sent_at timestamptz
);

-- ─── FIRM_RESPONSES ───────────────────────────────────────────────────────────
create table if not exists firm_responses (
  id uuid primary key default gen_random_uuid(),
  company_name text not null,
  deal_id uuid references deals(id),
  contact_id uuid references contacts(id),
  response_type text,
  responded_at timestamptz default now(),
  contacts_held integer default 0,
  notes text
);

-- ─── SCHEDULE_LOG ─────────────────────────────────────────────────────────────
create table if not exists schedule_log (
  id uuid primary key default gen_random_uuid(),
  deal_id uuid references deals(id),
  event_type text,
  detail text,
  created_at timestamptz default now()
);

-- ─── ANALYTICS ───────────────────────────────────────────────────────────────
create table if not exists deal_analytics (
  id uuid primary key default gen_random_uuid(),
  deal_id uuid references deals(id) on delete cascade,
  week_starting date not null,
  emails_sent integer default 0,
  linkedin_invites_sent integer default 0,
  linkedin_dms_sent integer default 0,
  total_outreach integer default 0,
  email_replies integer default 0,
  linkedin_replies integer default 0,
  positive_responses integer default 0,
  negative_responses integer default 0,
  temp_closes integer default 0,
  meetings_booked integer default 0,
  email_response_rate numeric default 0,
  linkedin_response_rate numeric default 0,
  overall_response_rate numeric default 0,
  meeting_conversion_rate numeric default 0,
  best_response_hour integer,
  best_response_day integer,
  sector text,
  deal_type text,
  timezone text default 'America/New_York',
  template_performance jsonb default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique (deal_id, week_starting)
);

create table if not exists roco_recommendations (
  id uuid primary key default gen_random_uuid(),
  category text not null,
  title text not null,
  insight text,
  recommendation text,
  supporting_data jsonb default '{}'::jsonb,
  suggested_setting_change jsonb,
  week_starting date,
  deals_analysed integer default 0,
  status text default 'pending',
  applied_at timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists roco_learned_settings (
  id uuid primary key default gen_random_uuid(),
  key text not null unique,
  value text,
  source_recommendation_id uuid references roco_recommendations(id) on delete set null,
  applied_at timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists daily_activity_reports (
  id uuid primary key default gen_random_uuid(),
  report_date date not null unique,
  timezone text default 'America/New_York',
  headline text,
  executive_summary text,
  voice_script text,
  telegram_caption text,
  deal_sections jsonb default '[]'::jsonb,
  raw_payload jsonb default '{}'::jsonb,
  activity_count integer default 0,
  deals_covered integer default 0,
  status text default 'generated',
  voice_name text,
  sent_to_telegram_at timestamptz,
  voice_note_sent_at timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- ─── ALTER TABLES (add new columns safely) ────────────────────────────────────
alter table deals add column if not exists sending_days text[] default array['monday','tuesday','wednesday','thursday','friday'];
alter table deals add column if not exists sending_start time default '08:00';
alter table deals add column if not exists sending_end time default '18:00';
alter table deals add column if not exists sending_timezone text default 'America/New_York';
alter table deals add column if not exists max_emails_per_day integer default 20;
alter table deals add column if not exists max_emails_per_hour integer default 5;
alter table deals add column if not exists batch_size integer default 15;
alter table deals add column if not exists followup_cadence_days integer[] default array[3,7,14];
alter table deals add column if not exists outreach_paused_until timestamptz;
alter table deals add column if not exists max_contacts_per_firm integer default 3;
alter table deals add column if not exists max_total_outreach integer default 200;
alter table deals add column if not exists min_investor_score integer default 60;
alter table contacts add column if not exists last_email_sent_at timestamptz;
alter table contacts add column if not exists dm_sent_at timestamptz;
alter table contacts add column if not exists last_outreach_at timestamptz;
alter table contacts add column if not exists follow_up_due_at timestamptz;
alter table contacts add column if not exists follow_up_count integer default 0;
alter table emails add column if not exists queued_to_send_at timestamptz;
alter table emails add column if not exists contact_email text;
alter table firm_suppressions add column if not exists suppression_type text default 'DECLINE';
alter table sessions add column if not exists outreach_paused_until timestamptz;
alter table deal_analytics add column if not exists created_at timestamptz default now();
alter table deal_analytics add column if not exists updated_at timestamptz default now();
alter table deal_analytics add column if not exists timezone text default 'America/New_York';
alter table roco_recommendations add column if not exists created_at timestamptz default now();
alter table roco_recommendations add column if not exists updated_at timestamptz default now();
alter table roco_recommendations add column if not exists status text default 'pending';
alter table roco_learned_settings add column if not exists created_at timestamptz default now();
alter table roco_learned_settings add column if not exists updated_at timestamptz default now();
alter table daily_activity_reports add column if not exists created_at timestamptz default now();
alter table daily_activity_reports add column if not exists updated_at timestamptz default now();
alter table daily_activity_reports add column if not exists timezone text default 'America/New_York';
alter table daily_activity_reports add column if not exists telegram_caption text;
alter table daily_activity_reports add column if not exists deal_sections jsonb default '[]'::jsonb;
alter table daily_activity_reports add column if not exists raw_payload jsonb default '{}'::jsonb;
alter table daily_activity_reports add column if not exists activity_count integer default 0;
alter table daily_activity_reports add column if not exists deals_covered integer default 0;
alter table daily_activity_reports add column if not exists status text default 'generated';
alter table daily_activity_reports add column if not exists voice_name text;
alter table daily_activity_reports add column if not exists sent_to_telegram_at timestamptz;
alter table daily_activity_reports add column if not exists voice_note_sent_at timestamptz;

-- ─── RLS POLICIES ─────────────────────────────────────────────────────────────
alter table deals enable row level security;
alter table contacts enable row level security;
alter table emails enable row level security;
alter table activity_log enable row level security;
alter table outreach_events enable row level security;
alter table email_templates enable row level security;
alter table sessions enable row level security;
alter table batches enable row level security;
alter table daily_activity_reports enable row level security;

-- Drop existing policies if they exist (safe re-run)
do $$ begin
  drop policy if exists "service_role_all" on deals;
  drop policy if exists "service_role_all" on contacts;
  drop policy if exists "service_role_all" on emails;
  drop policy if exists "service_role_all" on activity_log;
  drop policy if exists "service_role_all" on email_templates;
  drop policy if exists "service_role_all" on sessions;
  drop policy if exists "service_role_all" on batches;
  drop policy if exists "service_role_all" on daily_activity_reports;
exception when others then null;
end $$;

create policy "service_role_all" on deals for all using (true);
create policy "service_role_all" on contacts for all using (true);
create policy "service_role_all" on emails for all using (true);
create policy "service_role_all" on activity_log for all using (true);
drop policy if exists "service_role_all" on outreach_events;
create policy "service_role_all" on outreach_events for all using (true);
create policy "service_role_all" on email_templates for all using (true);
create policy "service_role_all" on sessions for all using (true);
create policy "service_role_all" on batches for all using (true);
create policy "service_role_all" on daily_activity_reports for all using (true);
