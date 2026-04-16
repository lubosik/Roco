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

// Enrichment fields that a KB upload is allowed to update on existing rows
const KB_ENRICH_FIELDS = [
  'description', 'investor_type', 'other_types', 'preferred_industries', 'preferred_verticals',
  'other_preferences', 'preferred_geographies', 'preferred_investment_types',
  'preferred_investment_amount_min', 'preferred_investment_amount_max',
  'preferred_deal_size_min', 'preferred_deal_size_max',
  'preferred_ebitda_min', 'preferred_ebitda_max',
  'aum_millions', 'dry_powder_millions', 'last_closed_fund_vintage', 'num_funds_open',
  'investments_last_12m', 'investments_last_6m', 'investments_last_7d', 'investments_last_2y',
  'last_investment_date', 'last_investment_company',
  'year_founded', 'hq_city', 'hq_state', 'hq_country',
];

async function flushBatch(supabase, dedupedBatch, isKB, listId, listName) {
  if (!isKB) {
    // Standard investor list upload: upsert everything including list_id
    const { error } = await supabase.from('investors_db')
      .upsert(dedupedBatch, { onConflict: 'pitchbook_id' });
    if (error) { console.warn('[DB IMPORT] Batch error:', error.message); return 0; }
    return dedupedBatch.length;
  }

  // KB upload — 2-pass to preserve priority list assignments on existing investors:
  // Pass 1: tag only NEW rows with this KB list_id (ignoreDuplicates skips existing)
  const taggedBatch = dedupedBatch.map(r => ({ ...r, list_id: listId, list_name: listName }));
  const { error: insertErr } = await supabase.from('investors_db')
    .upsert(taggedBatch, { onConflict: 'pitchbook_id', ignoreDuplicates: true });
  if (insertErr) console.warn('[DB IMPORT] KB new-rows insert error:', insertErr.message);

  // Pass 2: update enrichment fields on ALL rows (existing rows get richer data, list_id untouched)
  for (const rec of dedupedBatch) {
    const patch = {};
    for (const f of KB_ENRICH_FIELDS) {
      if (rec[f] != null && rec[f] !== '') patch[f] = rec[f];
    }
    if (Object.keys(patch).length > 0) {
      await supabase.from('investors_db').update(patch).eq('pitchbook_id', rec.pitchbook_id);
    }
  }
  return dedupedBatch.length;
}

function dealTypeToInvestorType(dealType) {
  const d = String(dealType || '').toLowerCase();
  if (/buyout|lbo|mbo|mbi/.test(d))         return 'Private Equity';
  if (/growth equity|growth capital/.test(d)) return 'Growth Equity';
  if (/venture|series|seed|angel/.test(d))   return 'Venture Capital';
  if (/independent sponsor/.test(d))         return 'Independent Sponsor';
  if (/family office/.test(d))               return 'Family Office';
  if (/mezz|mezzanine|debt/.test(d))         return 'Mezzanine/Debt';
  if (/real estate/.test(d))                 return 'Real Estate';
  if (/recapitali/.test(d))                  return 'Private Equity';
  return null;
}

function col(headers, row, ...candidates) {
  for (const name of candidates) {
    const idx = headers.findIndex(h => h.toLowerCase().includes(name.toLowerCase()));
    if (idx !== -1 && row[idx] != null && String(row[idx]).trim()) return String(row[idx]).trim();
  }
  return '';
}

async function importDealsExport({ supabase, headers, dataRows, filename, listId, listName, broadcastFn }) {
  console.log(`[DB IMPORT] Deals format detected in "${filename}" — extracting investors from comparable deals`);
  broadcastFn?.(`Parsing comparable deals from ${filename}...`);

  // Build a map: normalised investor name → aggregated KB record
  const investorMap = new Map();

  for (const row of dataRows) {
    if (!row || row.every(c => !c)) continue;

    const company    = col(headers, row, 'Companies', 'Company Name', 'Company');
    const dealType   = col(headers, row, 'Deal Type', 'Deal Class', 'Transaction Type');
    const industry   = col(headers, row, 'Primary Industry Group', 'Primary Industry Sector', 'Primary Industry', 'Industry Vertical', 'Industry');
    const geography  = col(headers, row, 'Company Country/Territory/Region', 'Company Country', 'Company State/Province', 'HQ Location', 'Geography');
    const dealDate   = col(headers, row, 'Deal Date', 'Announced Date', 'Close Date');
    const inferredType = dealTypeToInvestorType(dealType);

    // Collect all investor name strings — covers PitchBook deals export column variants
    const rawInvestors = [
      col(headers, row, 'Lead/Sole Investors', 'Lead Investors', 'Lead Investor'),
      col(headers, row, 'New Investors'),
      col(headers, row, 'Follow-on Investors'),
      col(headers, row, 'Investors'),
      col(headers, row, 'All Investors'),
      col(headers, row, 'Add-on Sponsors', 'Sponsor'),
    ].filter(Boolean).join(';');

    // Split by common delimiters, clean up
    const names = rawInvestors
      .split(/[;,\n]/)
      .map(n => n.trim().replace(/^\[|\]$/g, ''))
      .filter(n => n.length > 2 && !/^(unknown|undisclosed|n\/a)$/i.test(n));

    for (const name of names) {
      const key = name.toLowerCase().replace(/\s+/g, ' ');
      if (!investorMap.has(key)) {
        investorMap.set(key, {
          name,
          pitchbook_id: `DEALS-KB-${key.replace(/[^a-z0-9]/g, '-').slice(0, 60)}`,
          investor_type: inferredType,
          preferred_industries: industry || null,
          preferred_geographies: geography || null,
          source_file: filename,
          investor_category: 'KnowledgeBase',
          _deals: [],
        });
      }
      const rec = investorMap.get(key);
      // Aggregate: industries (unique), deal history
      if (industry && rec.preferred_industries && !rec.preferred_industries.includes(industry)) {
        rec.preferred_industries += `, ${industry}`;
      }
      if (company) rec._deals.push(company);
      // Take the most recent date seen
      if (dealDate && (!rec.last_investment_date || dealDate > rec.last_investment_date)) {
        rec.last_investment_date = dealDate;
      }
    }
  }

  if (investorMap.size === 0) {
    console.warn(`[DB IMPORT] Deals export "${filename}" — no investor names found. Headers: ${headers.join(' | ')}`);
    return { imported: 0, updated: 0, skipped: 0 };
  }

  broadcastFn?.(`Found ${investorMap.size} unique investors in comparable deals — importing...`);

  // Snapshot before
  const { count: countBefore } = await supabase
    .from('investors_db').select('*', { count: 'exact', head: true });

  let processed = 0;
  const records = [...investorMap.values()].map(rec => {
    const { _deals, ...rest } = rec;
    // Store the list of comparable companies in past_investments
    const pastInv = [...new Set(_deals)].slice(0, 20);
    const description = pastInv.length
      ? `Backed comparable deals including: ${pastInv.slice(0, 5).join(', ')}${pastInv.length > 5 ? ` and ${pastInv.length - 5} more` : ''}.`
      : null;
    return {
      ...rest,
      past_investments: pastInv,
      description:      rest.description || description,
    };
  });

  // Upsert in batches of 100 using the same KB-safe two-pass logic
  for (let i = 0; i < records.length; i += 100) {
    const batch = records.slice(i, i + 100);
    processed += await flushBatch(supabase, batch, true, listId, listName);
    broadcastFn?.(`Processing comparable deals KB: ${Math.min(i + 100, records.length)}/${records.length} investors`);
  }

  const { count: countAfter } = await supabase
    .from('investors_db').select('*', { count: 'exact', head: true });
  const newInserts = (countAfter || 0) - (countBefore || 0);
  const updated    = processed - newInserts;

  console.log(`[DB IMPORT] Deals KB done: ${newInserts} new, ${updated} enriched existing, from ${dataRows.length} comparable deals`);
  broadcastFn?.(`Deals KB import complete: ${investorMap.size} investors from comparable deals processed`);
  return { imported: newInserts, updated, skipped: 0, total: countAfter };
}

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

export async function importXLSXToDatabase({ filePath, filename, listId, listName, listType, broadcastFn }) {
  // For knowledge_base uploads: tag rows but don't overwrite existing list_id assignments
  const isKB = listType === 'knowledge_base';
  console.log(`[DB IMPORT] Starting: ${filename}`);
  broadcastFn?.(`Importing ${filename}...`);

  const supabase = getSupabase();
  if (!supabase) throw new Error('Supabase not available');

  const category = inferCategory(filename);
  const workbook = xlsx.readFile(filePath);
  const ws = workbook.Sheets[workbook.SheetNames[0]];
  const rawData = xlsx.utils.sheet_to_json(ws, { header: 1, defval: '' });

  // Find actual header row (PitchBook exports have 7-8 metadata rows before the real header)
  let headerRowIdx = -1;
  let isDealsFormat = false;
  for (let i = 0; i < Math.min(15, rawData.length); i++) {
    const row = rawData[i].map(c => String(c));
    if (row.some(c => c.includes('Investor ID') || c.includes('Limited Partner ID') || c === 'Investor Name')) {
      headerRowIdx = i;
      break;
    }
    // PitchBook deals export: look for deal-level columns
    if (row.some(c => c === 'Company Name' || c === 'Companies' || c === 'Deal Date' || c === 'Announced Date' || c === 'Lead Investors' || c === 'Lead/Sole Investors' || c === 'All Investors' || c === 'Investors')) {
      headerRowIdx = i;
      isDealsFormat = true;
      break;
    }
  }

  if (headerRowIdx === -1) {
    console.warn(`[DB IMPORT] No header found in ${filename}`);
    return { imported: 0, skipped: 0 };
  }

  const headers = rawData[headerRowIdx].map(h => String(h).trim());
  const dataRows = rawData.slice(headerRowIdx + 1);

  // Route deals-format exports through a separate parser
  if (isDealsFormat) {
    return importDealsExport({ supabase, headers, dataRows, filename, listId, listName, broadcastFn });
  }

  // Snapshot count before import so we can report true new inserts
  const { count: countBefore } = await supabase
    .from('investors_db').select('*', { count: 'exact', head: true });

  let processed = 0;
  let skipped = 0;
  const batch = [];

  for (const row of dataRows) {
    if (!row || row.every(c => !c)) continue;

    const record = { source_file: filename, investor_category: category };
    // KB uploads: list_id is set in a separate pass (new-only insert) — don't set here
    if (listId && !isKB) { record.list_id = listId; record.list_name = listName; }

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

    // If no pitchbook_id (non-PitchBook CSV), generate a stable key from the firm name
    // so the UPSERT can still deduplicate repeat uploads of the same list
    if (record.name && !record.pitchbook_id) {
      record.pitchbook_id = `IMPORT-${record.name.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '').slice(0, 60)}`;
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
      const dedupedBatch = [...batch.reduce((m, r) => m.set(r.pitchbook_id, r), new Map()).values()];
      processed += await flushBatch(supabase, dedupedBatch, isKB, listId, listName);
      batch.length = 0;
      broadcastFn?.(`Processing ${filename}: ${processed} rows done…`);
    }
  }

  if (batch.length > 0) {
    const dedupedBatch = [...batch.reduce((m, r) => m.set(r.pitchbook_id, r), new Map()).values()];
    const n = await flushBatch(supabase, dedupedBatch, isKB, listId, listName);
    processed += n;
    if (n === 0) skipped += dedupedBatch.length;
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
