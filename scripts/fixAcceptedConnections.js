/**
 * One-off script: advance Barry T. Nguyen, Ameet Nandlaskar, and Matt Miller
 * to pipeline_stage = 'invite_accepted' in both Supabase and Notion.
 *
 * Usage: node scripts/fixAcceptedConnections.js
 */

import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
import { updateContact } from '../crm/notionContacts.js';

const sb = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY,
);

const NAMES = ['Barry T. Nguyen', 'Ameet Nandlaskar', 'Matt Miller'];
const NEW_STAGE = 'invite_accepted';

async function run() {
  // Fetch active deal IDs first so we only touch contacts in active deals
  const { data: activeDeals } = await sb.from('deals')
    .select('id, name')
    .eq('status', 'ACTIVE')
    .eq('paused', false);

  if (!activeDeals?.length) {
    console.log('No active deals found — aborting');
    process.exit(1);
  }

  const activeDealIds = activeDeals.map(d => d.id);
  console.log(`Active deals: ${activeDeals.map(d => d.name).join(', ')}`);

  for (const fullName of NAMES) {
    // Search by exact name and also partial (e.g. "Matt Miller" vs "Matthew Miller")
    const { data: candidates } = await sb.from('contacts')
      .select('id, name, deal_id, pipeline_stage, notion_page_id')
      .ilike('name', `%${fullName.split(' ').slice(-1)[0]}%`) // last name match
      .in('deal_id', activeDealIds);

    if (!candidates?.length) {
      console.log(`[SKIP] ${fullName} — not found in any active deal`);
      continue;
    }

    // Pick best match — prefer exact name
    const contact = candidates.find(c =>
      c.name.toLowerCase() === fullName.toLowerCase()
    ) || candidates.find(c =>
      c.name.toLowerCase().includes(fullName.split(' ').pop().toLowerCase())
    );

    if (!contact) {
      console.log(`[SKIP] ${fullName} — no confident match among: ${candidates.map(c => c.name).join(', ')}`);
      continue;
    }

    console.log(`\n[FOUND] ${contact.name} (id: ${contact.id}, current stage: ${contact.pipeline_stage})`);

    if (contact.pipeline_stage === NEW_STAGE) {
      console.log(`[SKIP] Already at ${NEW_STAGE}`);
      continue;
    }

    // Update Supabase
    const { error: sbErr } = await sb.from('contacts')
      .update({ pipeline_stage: NEW_STAGE })
      .eq('id', contact.id);

    if (sbErr) {
      console.error(`[ERROR] Supabase update failed for ${contact.name}: ${sbErr.message}`);
      continue;
    }
    console.log(`[OK] Supabase updated → ${NEW_STAGE}`);

    // Update Notion
    if (contact.notion_page_id) {
      await updateContact(contact.notion_page_id, { pipelineStage: NEW_STAGE })
        .then(() => console.log(`[OK] Notion updated → ${NEW_STAGE}`))
        .catch(e => console.warn(`[WARN] Notion update failed: ${e.message}`));
    } else {
      console.log(`[WARN] No notion_page_id — Notion not updated`);
    }
  }

  console.log('\nDone.');
}

run().catch(e => { console.error(e); process.exit(1); });
