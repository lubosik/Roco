const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_KEY;

if (!url || !key) {
  console.error('SUPABASE_URL and SUPABASE_SERVICE_KEY are required');
  process.exit(1);
}

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

const EVENT_TYPES = Object.keys(STATUSES);

function restHeaders(extra = {}) {
  return {
    apikey: key,
    Authorization: `Bearer ${key}`,
    'Content-Type': 'application/json',
    ...extra,
  };
}

async function restGet(path) {
  const res = await fetch(`${url}/rest/v1/${path}`, { headers: restHeaders() });
  if (!res.ok) throw new Error(`GET ${path} failed: ${res.status} ${await res.text().catch(() => '')}`);
  return res.json();
}

async function restUpsert(path, rows) {
  const res = await fetch(`${url}/rest/v1/${path}`, {
    method: 'POST',
    headers: restHeaders({ Prefer: 'return=minimal' }),
    body: JSON.stringify(rows),
  });
  if (!res.ok) throw new Error(`POST ${path} failed: ${res.status} ${await res.text().catch(() => '')}`);
}

async function fetchRows(offset, limit) {
  const eventFilter = EVENT_TYPES.map(item => `"${item}"`).join(',');
  const query = [
    'activity_log',
    `?select=id,deal_id,contact_id,event_type,summary,detail,api_used,fallback_used,created_at`,
    `&event_type=in.(${eventFilter})`,
    `&order=created_at.asc`,
    `&limit=${limit}`,
    `&offset=${offset}`,
  ].join('');
  return restGet(query);
}

function mapRow(row) {
  const detail = row?.detail && typeof row.detail === 'object' ? row.detail : {};
  return {
    deal_id: row.deal_id || null,
    contact_id: row.contact_id || null,
    event_type: row.event_type,
    channel: CHANNELS[row.event_type] || null,
    status: STATUSES[row.event_type] || 'confirmed',
    provider: row.api_used || 'unipile',
    provider_message_id: detail.invitation_id || detail.message_id || detail.email_id || null,
    provider_account_id: detail.account_id || null,
    metadata: {
      activity_log_id: row.id,
      summary: row.summary || null,
      fallback_used: !!row.fallback_used,
      ...detail,
    },
    created_at: row.created_at,
  };
}

function dedupeKey(row) {
  return [
    row.event_type || '',
    row.contact_id || '',
    row.provider_message_id || '',
    row.created_at || '',
  ].join('|');
}

async function fetchExistingRows(minCreatedAt, maxCreatedAt) {
  const query = [
    'outreach_events',
    '?select=event_type,contact_id,provider_message_id,created_at',
    `&created_at=gte.${encodeURIComponent(minCreatedAt)}`,
    `&created_at=lte.${encodeURIComponent(maxCreatedAt)}`,
    '&limit=5000',
  ].join('');
  return restGet(query);
}

async function run() {
  let offset = 0;
  const limit = 500;
  let processed = 0;

  while (true) {
    const rows = await fetchRows(offset, limit);
    if (!rows.length) break;

    const payload = rows.map(mapRow);
    const existing = await fetchExistingRows(payload[0].created_at, payload[payload.length - 1].created_at);
    const existingKeys = new Set((existing || []).map(dedupeKey));
    const missing = payload.filter(row => !existingKeys.has(dedupeKey(row)));
    if (missing.length) {
      await restUpsert('outreach_events', missing);
    }

    processed += payload.length;
    offset += limit;
    console.log(`Processed ${processed} activity rows${missing.length ? ` · inserted ${missing.length}` : ' · no inserts needed'}`);
  }

  console.log(`Backfill complete. Processed ${processed} rows.`);
}

run().catch(err => {
  console.error('Backfill failed:', err.message);
  process.exit(1);
});
