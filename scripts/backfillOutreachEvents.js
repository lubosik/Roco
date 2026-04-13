import { createClient } from '@supabase/supabase-js';

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_KEY;

if (!url || !key) {
  console.error('SUPABASE_URL and SUPABASE_SERVICE_KEY are required');
  process.exit(1);
}

const sb = createClient(url, key, { auth: { persistSession: false } });

const STATUSES = {
  LINKEDIN_INVITE_SENT: 'confirmed',
  LINKEDIN_DM_SENT: 'confirmed',
  EMAIL_SENT: 'confirmed',
  LINKEDIN_INVITE_ALREADY_PENDING: 'inferred',
  LINKEDIN_ALREADY_CONNECTED: 'inferred',
  LINKEDIN_INVITE_PROVIDER_LIMIT: 'deferred',
  LINKEDIN_INVITE_PROVIDER_LIMIT_ESCALATED: 'deferred',
  LINKEDIN_INVITE_SKIPPED_NO_PROFILE: 'skipped',
  LINKEDIN_INVITE_FAILED: 'failed',
};

const CHANNELS = {
  LINKEDIN_INVITE_SENT: 'linkedin_invite',
  LINKEDIN_INVITE_ALREADY_PENDING: 'linkedin_invite',
  LINKEDIN_ALREADY_CONNECTED: 'linkedin_invite',
  LINKEDIN_INVITE_PROVIDER_LIMIT: 'linkedin_invite',
  LINKEDIN_INVITE_PROVIDER_LIMIT_ESCALATED: 'linkedin_invite',
  LINKEDIN_INVITE_SKIPPED_NO_PROFILE: 'linkedin_invite',
  LINKEDIN_INVITE_FAILED: 'linkedin_invite',
  LINKEDIN_DM_SENT: 'linkedin_dm',
  EMAIL_SENT: 'email',
};

async function fetchRows(from, to) {
  const { data, error } = await sb.from('activity_log')
    .select('id, deal_id, contact_id, event_type, summary, detail, api_used, fallback_used, created_at')
    .in('event_type', Object.keys(STATUSES))
    .order('created_at', { ascending: true })
    .range(from, to);
  if (error) throw error;
  return data || [];
}

async function run() {
  let from = 0;
  const pageSize = 500;
  let inserted = 0;

  while (true) {
    const rows = await fetchRows(from, from + pageSize - 1);
    if (!rows.length) break;

    const payload = rows.map(row => ({
      deal_id: row.deal_id || null,
      contact_id: row.contact_id || null,
      event_type: row.event_type,
      channel: CHANNELS[row.event_type] || null,
      status: STATUSES[row.event_type] || 'confirmed',
      provider: row.api_used || 'unipile',
      provider_message_id: row.detail?.invitation_id || row.detail?.message_id || row.detail?.email_id || null,
      provider_account_id: row.detail?.account_id || null,
      metadata: {
        activity_log_id: row.id,
        summary: row.summary || null,
        fallback_used: !!row.fallback_used,
        ...(row.detail && typeof row.detail === 'object' ? row.detail : {}),
      },
      created_at: row.created_at,
    }));

    const { error } = await sb.from('outreach_events').upsert(payload, {
      onConflict: 'event_type,contact_id,provider_message_id,created_at',
      ignoreDuplicates: true,
    });
    if (error) throw error;

    inserted += payload.length;
    from += pageSize;
    console.log(`Backfilled ${inserted} outreach events so far`);
  }

  console.log(`Backfill complete. Processed ${inserted} rows.`);
}

run().catch(err => {
  console.error('Backfill failed:', err.message);
  process.exit(1);
});
