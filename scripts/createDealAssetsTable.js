/**
 * Creates the deal_assets table in Supabase.
 * Run once: node scripts/createDealAssetsTable.js
 */
import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';

const sb = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY,
);

const sql = `
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
`;

const { error } = await sb.rpc('exec_sql', { sql }).catch(() => ({ error: { message: 'rpc not available' } }));
if (error) {
  // Fallback: try inserting a dummy row to test if table exists
  const { error: testErr } = await sb.from('deal_assets').select('id').limit(1);
  if (testErr && testErr.code === '42P01') {
    console.error('Table does not exist and could not be created via RPC.');
    console.log('Please run the following SQL in your Supabase dashboard SQL editor:\n');
    console.log(sql);
  } else {
    console.log('deal_assets table already exists or was created.');
  }
} else {
  console.log('deal_assets table created successfully.');
}
