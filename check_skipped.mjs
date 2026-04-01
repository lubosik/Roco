import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

const { data: skipped } = await sb.from('contacts')
  .select('name, pipeline_stage, enrichment_status, email, notes')
  .eq('pipeline_stage', 'Skipped')
  .not('email', 'is', null)
  .limit(5);
console.log('Skipped with email:', JSON.stringify(skipped?.map(c => ({ name: c.name, enrichment_status: c.enrichment_status, notes: c.notes?.slice(0,100) })), null, 2));

const { data: ranked } = await sb.from('contacts')
  .select('name, pipeline_stage, enrichment_status, investor_score')
  .eq('pipeline_stage', 'Ranked')
  .limit(5);
console.log('\nRanked contacts enrichment_status:', JSON.stringify(ranked?.map(c => ({ name: c.name, enrichment_status: c.enrichment_status, score: c.investor_score })), null, 2));

const { data: dist } = await sb.from('contacts')
  .select('enrichment_status, pipeline_stage')
  .eq('pipeline_stage', 'Ranked');
const grouped = {};
for (const c of dist || []) {
  const key = c.enrichment_status || 'NULL';
  grouped[key] = (grouped[key] || 0) + 1;
}
console.log('\nRanked contacts by enrichment_status:', grouped);
process.exit(0);
