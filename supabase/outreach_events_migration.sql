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

create index if not exists outreach_events_deal_created_idx
  on outreach_events (deal_id, created_at desc);

create index if not exists outreach_events_type_status_idx
  on outreach_events (event_type, status, created_at desc);

alter table outreach_events enable row level security;

drop policy if exists "service_role_all" on outreach_events;
create policy "service_role_all" on outreach_events for all using (true);
