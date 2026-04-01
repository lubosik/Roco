// core/investorDatabaseImporter.js
import { getSupabase } from './supabase.js';
import xlsx from 'xlsx';
import { classifyContact } from './classifyContact.js';

const COLUMN_MAP = {
  'Investor ID': 'pitchbook_id',
  'Investors': 'name',
  'Investor Legal Name': 'legal_name',
  'Description': 'description',
  'Primary Investor Type': 'investor_type',
  'Other Investor Types': 'other_types',
  'AUM': 'aum_millions',
  'Dry Powder': 'dry_powder_millions',
  '# of Investment Professionals': 'num_professionals',
  'Year Founded': 'year_founded',
  'HQ Location': 'hq_location',
  'HQ City': 'hq_city',
  'HQ State/Province': 'hq_state',
  'HQ Country/Territory/Region': 'hq_country',
  'HQ Global Region': 'hq_region',
  'HQ Email': 'hq_email',
  'HQ Phone': 'hq_phone',
  'HQ Fax': 'hq_fax',
  'Website': 'website',
  'Parent Company': 'parent_company',
  'Ownership Status': 'ownership_status',
  'Primary Contact': 'primary_contact_name',
  'Primary Contact Title': 'primary_contact_title',
  'Primary Contact Email': 'primary_contact_email',
  'Primary Contact Phone': 'primary_contact_phone',
  'Preferred Industry': 'preferred_industries',
  'Preferred Verticals': 'preferred_verticals',
  'Preferred Geography': 'preferred_geographies',
  'Preferred Investment Types': 'preferred_investment_types',
  'Preferred Investment Horizon': 'preferred_investment_horizon',
  'Preferred Deal Size Min': 'preferred_deal_size_min',
  'Preferred Deal Size Max': 'preferred_deal_size_max',
  'Preferred EBITDA Min': 'preferred_ebitda_min',
  'Preferred EBITDA Max': 'preferred_ebitda_max',
  'Preferred Revenue Min': 'preferred_revenue_min',
  'Preferred Revenue Max': 'preferred_revenue_max',
  'Preferred Investment Amount Min': 'preferred_investment_amount_min',
  'Preferred Investment Amount Max': 'preferred_investment_amount_max',
  'Preferred Direct Investment Size Min': 'preferred_direct_investment_size_min',
  'Preferred Direct Investment Size Max': 'preferred_direct_investment_size_max',
  'Preferred Company Valuation Min': 'preferred_company_valuation_min',
  'Preferred Company Valuation Max': 'preferred_company_valuation_max',
  'Real Asset Preferences': 'real_asset_preferences',
  'Impact Category Preferences': 'impact_preferences',
  'Other Stated Preferences': 'other_preferences',
  'Investments in the last 12 months': 'investments_last_12m',
  'Investments in the last 6 months': 'investments_last_6m',
  'Investments in the last 7 days': 'investments_last_7d',
  'Investments in the last 2 years': 'investments_last_2y',
  'Investments in the last 5 years': 'investments_last_5y',
  'Total Investments': 'total_investments',
  'Active Portfolio': 'active_portfolio',
  'Exits': 'exits',
  'Last Investment Company': 'last_investment_company',
  'Last Investment Date': 'last_investment_date',
  'Last Investment Type': 'last_investment_type',
  'Last Investment Size': 'last_investment_size',
  'Last Investment Class': 'last_investment_class',
  'Last Investment Status': 'last_investment_status',
  '# Funds Open': 'num_funds_open',
  '# Funds Closed': 'num_funds_closed',
  'Last Closed Fund Name': 'last_closed_fund_name',
  'Last Closed Fund Size': 'last_closed_fund_size',
  'Last Closed Fund Vintage': 'last_closed_fund_vintage',
  'Last Closed Fund Type': 'last_closed_fund_type',
  'Investor Status': 'investor_status',
  // LP file columns
  'Limited Partner ID': 'pitchbook_id',
  'Limited Partners': 'name',
  'Limited Partner Type': 'investor_type',
};

const NUMERIC_FIELDS = [
  'aum_millions', 'dry_powder_millions', 'preferred_deal_size_min', 'preferred_deal_size_max',
  'preferred_ebitda_min', 'preferred_ebitda_max', 'preferred_revenue_min', 'preferred_revenue_max',
  'preferred_investment_amount_min', 'preferred_investment_amount_max',
  'preferred_direct_investment_size_min', 'preferred_direct_investment_size_max',
  'preferred_company_valuation_min', 'preferred_company_valuation_max', 'last_closed_fund_size',
];
const INT_FIELDS = [
  'num_professionals', 'year_founded', 'investments_last_12m', 'investments_last_2y',
  'investments_last_5y', 'total_investments', 'active_portfolio',
  'investments_last_6m', 'investments_last_7d', 'exits', 'num_funds_open', 'num_funds_closed',
];

function inferCategory(filename) {
  const f = filename.toLowerCase();
  if (f.includes('buyout')) return 'Buyout';
  if (f.includes('fundless')) return 'FundlessSponsor';
  if (f.includes('independent')) return 'IndependentSponsor';
  if (f.includes('lmm') || f.includes('minority')) return 'LMM_Minority';
  if (f.includes('vc') || f.includes('venture') || f.includes('lp')) return 'VC_LP';
  if (f.includes('paper') || f.includes('manufactur')) return 'Manufacturing';
  if (f.includes('secondary')) return 'Secondary';
  return 'General';
}

export async function importXLSXToDatabase({ filePath, filename, listId, listName, broadcastFn }) {
  console.log(`[DB IMPORT] Starting: ${filename}`);
  broadcastFn?.(`Importing ${filename}...`);

  const supabase = getSupabase();
  if (!supabase) throw new Error('Supabase not available');

  const category = inferCategory(filename);
  const workbook = xlsx.readFile(filePath);
  const ws = workbook.Sheets[workbook.SheetNames[0]];
  const rawData = xlsx.utils.sheet_to_json(ws, { header: 1, defval: '' });

  // Find actual header row (PitchBook exports have 7-8 metadata rows)
  let headerRowIdx = -1;
  for (let i = 0; i < Math.min(15, rawData.length); i++) {
    const row = rawData[i].map(c => String(c));
    if (row.some(c => c.includes('Investor ID') || c.includes('Limited Partner ID') || c === 'Investor Name')) {
      headerRowIdx = i;
      break;
    }
  }

  if (headerRowIdx === -1) {
    console.warn(`[DB IMPORT] No header found in ${filename}`);
    return { imported: 0, skipped: 0 };
  }

  const headers = rawData[headerRowIdx].map(h => String(h).trim());
  const dataRows = rawData.slice(headerRowIdx + 1);

  // Snapshot count before import so we can report true new inserts
  const { count: countBefore } = await supabase
    .from('investors_db').select('*', { count: 'exact', head: true });

  let processed = 0;
  let skipped = 0;
  const batch = [];

  for (const row of dataRows) {
    if (!row || row.every(c => !c)) continue;

    const record = { source_file: filename, investor_category: category };
    if (listId) record.list_id = listId;
    if (listName) record.list_name = listName;

    headers.forEach((header, idx) => {
      const dbField = COLUMN_MAP[header];
      if (!dbField) return;
      let val = row[idx];
      if (val === '' || val === null || val === undefined) return;

      if (NUMERIC_FIELDS.includes(dbField)) {
        val = parseFloat(String(val).replace(/[,$]/g, '')) || null;
      } else if (INT_FIELDS.includes(dbField)) {
        val = parseInt(String(val)) || null;
      }

      if (!record[dbField]) record[dbField] = val;
    });

    // Manual format fallback (LMM file: Investor Name, Type, Focus, Deal Size, ...)
    if (headers[0] === 'Investor Name' && !record.pitchbook_id) {
      record.name = String(row[0] || '').trim();
      record.investor_type = String(row[1] || '').trim();
      record.preferred_industries = String(row[2] || '').trim();
      record.pitchbook_id = `LMM-${record.name.replace(/\s+/g, '-').substring(0, 40)}`;
      record.hq_country = String(row[5] || '').trim();
      record.description = String(row[6] || '').trim();
    }

    if (!record.name || !record.pitchbook_id) { skipped++; continue; }

    // Classify as angel / individual_at_firm / firm
    const classification = classifyContact({
      firm_name: record.name,        // investors_db rows ARE the firm
      job_title: record.primary_contact_title || '',
      name:      record.primary_contact_name || record.name,
      notes:     record.description || '',
    });
    record.contact_type = classification.contact_type;
    record.is_angel     = classification.is_angel;

    batch.push(record);

    if (batch.length >= 100) {
      // Dedup batch by pitchbook_id (last write wins within a file)
      const dedupedBatch = [...batch.reduce((m, r) => m.set(r.pitchbook_id, r), new Map()).values()];
      const { error } = await supabase.from('investors_db')
        .upsert(dedupedBatch, { onConflict: 'pitchbook_id' });
      if (error) console.warn('[DB IMPORT] Batch error:', error.message);
      else processed += dedupedBatch.length;
      batch.length = 0;
      broadcastFn?.(`Processing ${filename}: ${processed} rows done…`);
    }
  }

  if (batch.length > 0) {
    // Dedup batch by pitchbook_id (last write wins within a file)
    const dedupedBatch = [...batch.reduce((m, r) => m.set(r.pitchbook_id, r), new Map()).values()];
    const { error } = await supabase.from('investors_db')
      .upsert(dedupedBatch, { onConflict: 'pitchbook_id' });
    if (!error) processed += dedupedBatch.length;
    else skipped += dedupedBatch.length;
  }

  // True new inserts = count delta
  const { count: countAfter } = await supabase
    .from('investors_db').select('*', { count: 'exact', head: true });
  const newInserts = (countAfter || 0) - (countBefore || 0);
  const updated    = processed - newInserts;

  console.log(`[DB IMPORT] Done: ${newInserts} new, ${updated} updated, ${skipped} skipped. Total: ${countAfter}`);
  broadcastFn?.(`Import complete: ${newInserts} new investors added, ${updated} existing updated. Total DB: ${countAfter?.toLocaleString()}`);
  return { imported: newInserts, updated, skipped, total: countAfter };
}

/**
 * One-time backfill: classify all investors_db rows that have no contact_type set.
 * Safe to call on startup — exits immediately if nothing to backfill.
 */
export async function backfillContactTypes() {
  const supabase = getSupabase();
  if (!supabase) return;

  let total = 0;
  let offset = 0;
  const batchSize = 500;

  while (true) {
    const { data: investors } = await supabase.from('investors_db')
      .select('id, name, description, primary_contact_name, primary_contact_title')
      .is('contact_type', null)
      .range(offset, offset + batchSize - 1);

    if (!investors?.length) break;

    for (const inv of investors) {
      const c = classifyContact({
        firm_name: inv.name,
        job_title: inv.primary_contact_title || '',
        name:      inv.primary_contact_name || inv.name,
        notes:     inv.description || '',
      });
      await supabase.from('investors_db').update({
        contact_type: c.contact_type,
        is_angel:     c.is_angel,
      }).eq('id', inv.id);
    }

    total += investors.length;
    offset += batchSize;
    if (investors.length < batchSize) break;
  }

  if (total > 0) console.log(`[BACKFILL] Classified ${total} investors_db rows`);
}
