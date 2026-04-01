/**
 * research/csvIngestor.js
 * Ingests a PitchBook LP export CSV into Supabase contacts.
 * Skips the 8 metadata rows at the top — data headers are on row 9.
 */

import Papa from 'papaparse';
import { getSupabase } from '../core/supabase.js';

export async function ingestCSV({ csvContent, dealId, dealName, broadcastFn }) {
  if (!csvContent) {
    console.log('[CSV] No CSV content provided');
    return 0;
  }

  console.log('[CSV] Parsing PitchBook LP export...');

  const lines = csvContent.split('\n');

  // Find the actual header row — search first 15 lines
  let headerRowIndex = 8; // default: row 9 (0-indexed = 8)
  for (let i = 0; i < Math.min(15, lines.length); i++) {
    if (lines[i].includes('Limited Partner ID') || lines[i].includes('Limited Partners')) {
      headerRowIndex = i;
      break;
    }
  }

  console.log(`[CSV] Header row found at line ${headerRowIndex + 1}`);

  const csvDataOnly = lines.slice(headerRowIndex).join('\n');

  const { data: rows, errors } = Papa.parse(csvDataOnly, {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: false,
  });

  if (errors.length > 0) {
    console.warn('[CSV] Parse warnings:', errors.slice(0, 3).map(e => e.message));
  }

  console.log(`[CSV] Parsed ${rows.length} rows`);

  const sb = getSupabase();
  if (!sb) { console.error('[CSV] No Supabase connection'); return 0; }

  // Fetch existing contacts for dedup
  const { data: existing } = await sb.from('contacts')
    .select('email, company_name').eq('deal_id', dealId);

  const existingEmails = new Set((existing || []).map(c => c.email).filter(Boolean).map(e => e.toLowerCase()));
  const existingFirms  = new Set((existing || []).map(c => c.company_name?.toLowerCase()).filter(Boolean));

  let imported = 0;
  let skipped  = 0;
  const batch  = [];

  for (const row of rows) {
    try {
      const firmName    = (row['Limited Partners'] || '').trim();
      const contactName = (row['Primary Contact'] || '').trim();
      const title       = (row['Primary Contact Title'] || '').trim();
      const email       = (row['Primary Contact Email'] || '').trim().toLowerCase() || null;
      const phone       = (row['Primary Contact Phone'] || '').trim() || null;
      const geography   = (row['HQ Country/Territory/Region'] || '').trim();
      const aum         = (row['AUM'] || '').trim();
      const investorType = (row['Limited Partner Type'] || '').trim();
      const fundStrategy = (row['Fund Strategy Preferences'] || '').trim();
      const vcCommitments = parseInt(row['Commitments in VC Funds'] || '0') || 0;
      const website     = (row['Website'] || '').trim() || null;
      const description = (row['Description'] || '').trim() || null;
      const preferredSize = (row['Preferred Commitment Size'] || '').trim() || null;

      if (!firmName) { skipped++; continue; }

      // Dedup
      if (email && existingEmails.has(email)) { skipped++; continue; }
      if (existingFirms.has(firmName.toLowerCase())) { skipped++; continue; }

      // Only import VC-relevant investors
      const stratLower = fundStrategy.toLowerCase();
      const typeLower  = investorType.toLowerCase();
      const isVCRelevant = vcCommitments > 0 ||
        stratLower.includes('venture') ||
        stratLower.includes('early stage') ||
        stratLower.includes('growth') ||
        typeLower.includes('venture') ||
        typeLower.includes('family office') ||
        typeLower.includes('endowment') ||
        typeLower.includes('fund of funds') ||
        typeLower.includes('asset manager');

      if (!isVCRelevant) { skipped++; continue; }

      const name = contactName || firmName;

      batch.push({
        deal_id:          dealId,
        name,
        company_name:     firmName,
        job_title:        title || null,
        email,
        phone,
        geography:        geography || null,
        aum_fund_size:    aum || null,
        investor_type:    investorType || null,
        sector_focus:     fundStrategy ? fundStrategy.substring(0, 500) : null,
        typical_cheque_size: preferredSize || null,
        notes:            description ? description.substring(0, 1000) : null,
        website:          website || null,
        source:           'CSV Import',
        enrichment_status: email ? 'Complete' : 'Pending',
        pipeline_stage:   'Researched',
        investor_score:   null,
        notion_page_id:   null,
        linkedin_url:     null,
        created_at:       new Date().toISOString(),
      });

      if (email) existingEmails.add(email);
      existingFirms.add(firmName.toLowerCase());

      // Insert in batches of 50
      if (batch.length >= 50) {
        try { await sb.from('contacts').insert([...batch]); } catch(e) { console.warn('[CSV] Batch insert error:', e.message); }
        imported += batch.length;
        batch.length = 0;
        broadcastFn?.(`CSV: imported ${imported} contacts so far...`);
      }

    } catch (err) {
      console.warn('[CSV] Row error:', err.message);
    }
  }

  // Insert remaining
  if (batch.length > 0) {
    try { await sb.from('contacts').insert([...batch]); } catch(e) { console.warn('[CSV] Final batch insert error:', e.message); }
    imported += batch.length;
  }

  console.log(`[CSV] Import complete: ${imported} imported, ${skipped} skipped`);
  broadcastFn?.(`CSV import complete: ${imported} contacts added (${skipped} skipped)`);
  return imported;
}
