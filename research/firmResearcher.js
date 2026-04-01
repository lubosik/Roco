/**
 * research/firmResearcher.js
 * Firms-first research pipeline.
 * Stream 1: CSV import → firms table
 * Stream 2: Grok/Gemini deep research → firms table
 * Stream 3: LinkedIn search → firms table
 * Then: firm enrichment loop → contacts table
 */

import { getSupabase } from '../core/supabase.js';
import { pushActivity } from '../dashboard/server.js';
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';

// Keys read lazily inside functions so dotenv is loaded first

// Labels that describe a role, not a real firm — don't persist as company_name
const GENERIC_FIRM_NAMES = new Set([
  'angel investor', 'angel investors', 'independent investor', 'independent',
  'self-employed', 'self employed', 'freelance', 'freelancer', 'consultant',
  'private investor', 'individual investor', 'personal investment',
  'n/a', 'na', 'none', 'unknown',
]);
function isGenericFirm(name) {
  return !name || GENERIC_FIRM_NAMES.has(name.toLowerCase().trim());
}

/**
 * Main entry point — called immediately after deal is saved.
 * Runs all three streams in parallel, then runs firm enrichment to find contacts.
 */
export async function runFirmResearch(deal) {
  console.log(`[FIRM RESEARCH] Starting firms-first research for: ${deal.name}`);

  const sb = getSupabase();
  if (!sb) { console.warn('[FIRM RESEARCH] No Supabase — skipping'); return 0; }

  // Log start
  pushActivity({ type: 'research', action: 'Research starting', note: `Finding firms for ${deal.name}`, deal_name: deal.name, dealId: deal.id });
  sb.from('activity_log').insert({ deal_id: deal.id, event_type: 'RESEARCH_STARTED', summary: `Firm research started for ${deal.name}`, created_at: new Date().toISOString() }).then(null, () => {});

  // Run all three streams in parallel
  const [csvRes, grokRes, linkedinRes] = await Promise.allSettled([
    importFirmsFromCSV(deal),
    runGrokFirmResearch(deal),
    runLinkedInFirmSearch(deal),
  ]);

  const csvFirms = csvRes.status === 'fulfilled' ? csvRes.value : [];
  const grokFirms = grokRes.status === 'fulfilled' ? grokRes.value : [];
  const linkedinFirms = linkedinRes.status === 'fulfilled' ? linkedinRes.value : [];

  console.log(`[FIRM RESEARCH] CSV: ${csvFirms.length} | Grok/Gemini: ${grokFirms.length} | LinkedIn: ${linkedinFirms.length}`);

  // Upsert all firms
  const savedFirms = await upsertFirms(deal, csvFirms, grokFirms, linkedinFirms);
  console.log(`[FIRM RESEARCH] ${savedFirms} firms saved to Supabase`);

  // Now run firm enrichment — find contacts at each firm
  const contactsFound = await runFirmEnrichmentLoop(deal);

  pushActivity({ type: 'research', action: 'Research complete', note: `${savedFirms} firms, ${contactsFound} contacts found for ${deal.name}`, deal_name: deal.name, dealId: deal.id });
  sb.from('activity_log').insert({ deal_id: deal.id, event_type: 'RESEARCH_COMPLETE', summary: `${savedFirms} firms, ${contactsFound} contacts`, created_at: new Date().toISOString() }).then(null, () => {});

  return contactsFound;
}

// ── STREAM 1: CSV IMPORT ───────────────────────────────────────────────

async function importFirmsFromCSV(deal) {
  const importsDir = '/root/roco/imports';
  if (!fs.existsSync(importsDir)) return [];

  const files = fs.readdirSync(importsDir).filter(f => f.endsWith('.csv'));
  if (!files.length) return [];

  const firms = [];
  for (const file of files) {
    try {
      const content = fs.readFileSync(path.join(importsDir, file), 'utf8');
      const records = parse(content, { columns: true, skip_empty_lines: true, trim: true });

      for (const row of records) {
        // Try common column names for firm/company name
        const firmName = row['Firm Name'] || row['Company Name'] || row['Fund Name'] || row['Name'] ||
          row['firm_name'] || row['company_name'] || row['name'] || null;

        if (!firmName || firmName.trim() === '') continue;

        firms.push({
          name: firmName.trim(),
          website: row['Website'] || row['website'] || null,
          firm_type: row['Type'] || row['Firm Type'] || row['firm_type'] || null,
          geography_focus: row['Geography'] || row['geography'] || deal.geography || null,
          source: 'csv',
        });
      }

      console.log(`[FIRM RESEARCH] CSV "${file}": extracted ${firms.length} firms`);

      // Move to processed
      const processed = path.join(importsDir, 'processed');
      if (!fs.existsSync(processed)) fs.mkdirSync(processed, { recursive: true });
      fs.renameSync(path.join(importsDir, file), path.join(processed, file));
    } catch (err) {
      console.warn(`[FIRM RESEARCH] CSV parse error for ${file}:`, err.message);
    }
  }

  return firms;
}

// ── STREAM 2: GROK/GEMINI DEEP RESEARCH ───────────────────────────────

async function runGrokFirmResearch(deal) {
  const key = process.env.GROK_API_KEY;
  if (!key) {
    console.warn('[FIRM RESEARCH] GROK_API_KEY not set — trying Gemini');
    return runGeminiFirmResearch(deal);
  }

  console.log('[FIRM RESEARCH] Using Grok Responses API with web_search tool...');

  const prompt = buildFirmResearchPrompt(deal);

  try {
    const res = await fetch('https://api.x.ai/v1/responses', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` },
      body: JSON.stringify({
        model: 'grok-4',
        input: [{ role: 'user', content: prompt }],
        tools: [{ type: 'web_search' }],
      }),
    });

    if (!res.ok) {
      const err = await res.text().catch(() => '');
      console.warn(`[FIRM RESEARCH] Grok failed (${res.status}): ${err.substring(0, 150)} — falling back to Gemini`);
      return runGeminiFirmResearch(deal);
    }

    const data = await res.json();
    // Responses API: output[] contains message objects with content[]
    const outputMsg = (data.output || []).find(o => o.type === 'message');
    const text = outputMsg?.content?.find(c => c.type === 'output_text')?.text || '[]';
    const firms = parseFirmResearchResults(text, 'grok-4');
    if (firms.length > 0) {
      console.log(`[FIRM RESEARCH] Grok returned ${firms.length} firms`);
      return firms;
    }

    console.warn('[FIRM RESEARCH] Grok returned 0 firms — falling back to Gemini');
    return runGeminiFirmResearch(deal);
  } catch (err) {
    console.warn('[FIRM RESEARCH] Grok error:', err.message, '— falling back to Gemini');
    return runGeminiFirmResearch(deal);
  }
}

async function runGeminiFirmResearch(deal) {
  const GEMINI_KEY = process.env.GEMINI_API_KEY;
  const GEMINI_FALLBACK = process.env.GEMINI_API_KEY_FALLBACK;
  if (!GEMINI_KEY && !GEMINI_FALLBACK) {
    console.warn('[FIRM RESEARCH] No Gemini keys — skipping');
    return [];
  }

  console.log('[FIRM RESEARCH] Using Gemini with google_search...');

  const prompt = buildFirmResearchPrompt(deal);
  const models = ['gemini-2.5-pro', 'gemini-1.5-pro'];
  const keys = [GEMINI_KEY, GEMINI_FALLBACK].filter(Boolean);

  for (const key of keys) {
    for (const model of models) {
      try {
        const res = await fetch(
          `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              contents: [{ parts: [{ text: prompt }] }],
              tools: [{ googleSearch: {} }],
              generationConfig: { temperature: 0.3, maxOutputTokens: 8192 },
            }),
          }
        );

        if (res.ok) {
          const data = await res.json();
          const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '[]';
          const firms = parseFirmResearchResults(text, `gemini/${model}`);
          if (firms.length > 0) {
            console.log(`[FIRM RESEARCH] Gemini (${model}) returned ${firms.length} firms`);
            return firms;
          }
        } else {
          const errText = await res.text().catch(() => '');
          console.warn(`[FIRM RESEARCH] Gemini ${model} failed (${res.status}): ${errText.substring(0, 100)}`);
        }
      } catch (err) {
        console.warn(`[FIRM RESEARCH] Gemini ${model} error:`, err.message);
      }
    }
  }

  console.error('[FIRM RESEARCH] All Gemini models failed');
  return [];
}

function buildFirmResearchPrompt(deal) {
  const dealType = (deal.type || deal.raise_type || '').toLowerCase();
  const isISSponsor = /independent.sponsor|fundless|co.invest/i.test(dealType);
  const isBuyout    = isISSponsor || /buyout|private.equity|lbo|pe\b/i.test(dealType);
  const isVC        = /venture|seed|series|startup|pre.seed|growth.equity/i.test(dealType) && !isBuyout;

  // Currency — default to $ if geography looks US, else £
  const isUS = /united states|usa|us\b|\$|usd/i.test((deal.geography || '') + (deal.description || ''));
  const sym = isUS ? '$' : '£';

  // Financial metrics — pull from top-level or settings JSONB
  const s = deal.settings || {};
  const ebitda  = deal.ebitda_usd_m  || s.ebitda  || deal.ebitda;
  const ev      = deal.enterprise_value_usd_m || s.ev || deal.ev;
  const equity  = deal.equity_required_usd_m  || s.equity || (deal.target_amount ? Number(deal.target_amount) / 1_000_000 : null);
  const revenue = deal.revenue_usd_m || s.revenue;

  const financials = [
    ebitda  ? `EBITDA: ${sym}${ebitda}M`              : null,
    ev      ? `Enterprise Value: ${sym}${ev}M`         : null,
    equity  ? `Equity Required: ${sym}${equity}M`      : null,
    revenue ? `Revenue: ${sym}${revenue}M`             : null,
    (!ebitda && !ev && deal.target_amount) ? `Raise Target: ${sym}${Number(deal.target_amount).toLocaleString()}` : null,
    (deal.cheque_min || deal.min_cheque) ? `Min Cheque: ${sym}${Number(deal.cheque_min || deal.min_cheque).toLocaleString()}` : null,
    (deal.cheque_max || deal.max_cheque) ? `Max Cheque: ${sym}${Number(deal.cheque_max || deal.max_cheque).toLocaleString()}` : null,
  ].filter(Boolean).join('\n');

  // Deal-type specific targeting instructions
  let targetingInstructions;
  if (isISSponsor) {
    targetingInstructions = `\nINVESTOR TARGETING — INDEPENDENT SPONSOR DEAL:
This deal requires equity co-investment partners, NOT fund investments. Target ONLY:
• Lower/middle market PE firms active in the ${deal.sector || 'relevant'} sector with EV range matching ${ev ? `~${sym}${ev}M` : 'this deal size'}
• Family offices with direct deal or co-investment mandates (NOT fund-of-funds)
• Fundless sponsors / independent sponsors open to equity partnerships
• Search funds or HNW operators with sector experience who co-invest

EXCLUDE: early-stage VCs, biotech/drug discovery VCs, large-cap PE ($10B+ AUM), pension funds, endowments, sovereign wealth funds, growth equity firms focused on software/tech startups.`;
  } else if (isBuyout) {
    targetingInstructions = `\nINVESTOR TARGETING — BUYOUT / PE DEAL:
Target: PE funds and family offices active in ${deal.sector || 'this sector'} buyouts at this deal size. Exclude pure VC/venture investors.`;
  } else if (isVC) {
    targetingInstructions = `\nINVESTOR TARGETING — VENTURE RAISE:
Target: VC funds, angels, and family offices with VC mandates active at the ${deal.raise_type || 'early'} stage in ${deal.sector || 'this sector'}. Exclude pure buyout PE, secondary funds, and pension/endowment vehicles.`;
  } else {
    targetingInstructions = `\nTarget investment firms with demonstrated activity in ${deal.sector || 'this sector'} at similar deal sizes. Prioritise the types listed in the deal description.`;
  }

  return `You are a specialist fundraising researcher. Research firms for this specific deal and return only highly relevant matches.

DEAL BRIEF:
Name: ${deal.name}
Structure: ${deal.type || deal.raise_type || 'Investment'}
Sector: ${deal.sector || 'Unknown'}${deal.sub_sector ? ` / ${deal.sub_sector}` : ''}
Geography: ${deal.geography || 'United States'}
${financials}
${deal.description ? `\nDescription: ${deal.description.substring(0, 500)}` : ''}
${deal.investor_profile ? `\nIdeal Investor Profile: ${deal.investor_profile}` : ''}
${targetingInstructions}

YOUR TASK:
Identify 20-30 firms with the HIGHEST likelihood of investing in this specific deal. Precision matters — irrelevant firms waste outreach budget. Each firm must genuinely match the deal structure, size, sector, and geography.

For each firm return these exact JSON keys:
firm_name (exact legal name), firm_type (specific: e.g. "LMM Healthcare PE", "Family Office Direct", "Independent Sponsor", "Healthcare VC"), website, geography_focus, sector_focus, cheque_size (typical equity ticket in ${sym}), aum, past_investments (array of 3-5 specific deals: company name + year + amount), investment_thesis (1-2 sentences from their own materials), match_rationale (2-3 sentences explaining why THIS deal fits THEIR criteria — be specific about deal size, sector, and structure fit), match_score (integer 0-100)

Return ONLY a valid JSON array. No preamble, no markdown, no explanation.`;
}

function parseFirmResearchResults(text, source) {
  try {
    const clean = text.replace(/```json|```/g, '').trim();
    const match = clean.match(/\[[\s\S]*\]/);
    if (!match) return [];
    const firms = JSON.parse(match[0]);
    console.log(`[FIRM RESEARCH] ${source} parsed ${firms.length} firms`);
    return firms
      .filter(f => f.firm_name && f.firm_name.trim() !== '')
      .map(f => ({
        name: f.firm_name.trim(),
        firm_type: f.firm_type || null,
        website: f.website || null,
        geography_focus: f.geography_focus || null,
        sector_focus: f.sector_focus || null,
        cheque_size: f.cheque_size || null,
        aum: f.aum || null,
        past_investments: Array.isArray(f.past_investments) ? f.past_investments : (f.past_investments ? [f.past_investments] : []),
        investment_thesis: f.investment_thesis || null,
        match_rationale: f.match_rationale || null,
        match_score: parseInt(f.match_score) || 50,
        research_model: source,
        source: 'grok_gemini',
      }));
  } catch (err) {
    console.warn(`[FIRM RESEARCH] Parse error from ${source}:`, err.message);
    return [];
  }
}

// ── STREAM 3: LINKEDIN SEARCH ──────────────────────────────────────────

async function runLinkedInFirmSearch(deal) {
  if (!process.env.UNIPILE_API_KEY) return [];

  console.log('[FIRM RESEARCH] Running LinkedIn people search to find firm candidates...');

  // Generate search queries via GPT/OpenAI
  const queries = await generateLinkedInSearchQueries(deal);

  const firmMap = new Map(); // firm name → firm data
  const candidatesForFirms = []; // people found who belong to firms

  for (const query of queries.slice(0, 3)) {
    try {
      const url = `${process.env.UNIPILE_DSN}/api/v1/linkedin/search?account_id=${process.env.UNIPILE_LINKEDIN_ACCOUNT_ID}&limit=20`;
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          'X-API-KEY': process.env.UNIPILE_API_KEY,
          'Content-Type': 'application/json',
          'accept': 'application/json',
        },
        body: JSON.stringify({ api: 'classic', category: 'people', keywords: query }),
      });

      if (!res.ok) continue;

      const data = await res.json();
      const results = data.items || [];

      for (const person of results) {
        const firmName = person.current_company?.name || person.company_name || null;
        if (!firmName) continue;

        if (!firmMap.has(firmName.toLowerCase())) {
          firmMap.set(firmName.toLowerCase(), {
            name: firmName,
            source: 'linkedin_search',
            candidates: [],
          });
        }

        const name = [person.first_name, person.last_name].filter(Boolean).join(' ') || person.name || null;
        if (name && name !== 'Unknown') {
          firmMap.get(firmName.toLowerCase()).candidates.push({
            name,
            title: person.title || person.headline || null,
            linkedin_url: person.profile_url || (person.public_identifier ? `https://linkedin.com/in/${person.public_identifier}` : null),
            linkedin_provider_id: person.provider_id || null,
          });
        }
      }

      await sleep(500);
    } catch (err) {
      console.warn(`[FIRM RESEARCH] LinkedIn search error for "${query}":`, err.message);
    }
  }

  const firms = [...firmMap.values()].map(f => ({
    name: f.name,
    source: 'linkedin_search',
    candidates: f.candidates,
  }));

  console.log(`[FIRM RESEARCH] LinkedIn found ${firms.length} firms from people search`);
  return firms;
}

async function generateLinkedInSearchQueries(deal) {
  try {
    const res = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: 'gpt-4o',
        messages: [{
          role: 'user',
          content: `Generate 5 LinkedIn people search queries to find investors likely to invest in the following deal:
Deal: ${deal.name} | Sector: ${deal.sector || 'Technology'} | Stage: ${deal.type || deal.raise_type || 'Investment'} | Geography: ${deal.geography || 'UK'}

Return ONLY a JSON array of strings, each being a LinkedIn search query targeting: VCs, partners at PE firms, angel investors, or family office principals. Queries should target people by title and sector. No explanation.`,
        }],
        max_tokens: 300,
      }),
    });

    if (res.ok) {
      const data = await res.json();
      const text = data.choices?.[0]?.message?.content || '[]';
      const clean = text.replace(/```json|```/g, '').trim();
      const queries = JSON.parse(clean.match(/\[[\s\S]*\]/)?.[0] || '[]');
      return Array.isArray(queries) ? queries : [];
    }
  } catch (err) {
    console.warn('[FIRM RESEARCH] Query generation error:', err.message);
  }

  // Fallback queries
  return [
    `${deal.sector || 'technology'} investor UK`,
    `VC partner ${deal.sector || 'technology'}`,
    `family office ${deal.sector || 'technology'} investments`,
  ];
}

// ── UPSERT FIRMS ───────────────────────────────────────────────────────

async function upsertFirms(deal, csvFirms, grokFirms, linkedinFirms) {
  const sb = getSupabase();
  if (!sb) return 0;

  // Build a map: normalized firm name → merged data
  const firmMap = new Map();

  // Start with CSV firms (lowest priority - just names)
  for (const f of csvFirms) {
    const key = f.name.toLowerCase().trim();
    firmMap.set(key, {
      name: f.name,
      website: f.website || null,
      firm_type: f.firm_type || null,
      geography_focus: f.geography_focus || null,
      source: 'csv',
      status: 'pending_research',
      deal_id: deal.id,
      candidates: [],
    });
  }

  // Merge Grok/Gemini results (highest quality)
  for (const f of grokFirms) {
    const key = f.name.toLowerCase().trim();
    const existing = firmMap.get(key) || {};
    firmMap.set(key, {
      ...existing,
      name: f.name,
      firm_type: f.firm_type || existing.firm_type || null,
      website: f.website || existing.website || null,
      geography_focus: f.geography_focus || existing.geography_focus || null,
      sector_focus: f.sector_focus || null,
      cheque_size: f.cheque_size || null,
      aum: f.aum || null,
      past_investments: f.past_investments || [],
      investment_thesis: f.investment_thesis || null,
      match_rationale: f.match_rationale || null,
      match_score: f.match_score || 50,
      research_model: f.research_model || null,
      source: existing.source === 'csv' ? 'csv+research' : 'grok_gemini',
      status: 'researched',
      deal_id: deal.id,
      candidates: existing.candidates || [],
    });
  }

  // Merge LinkedIn results
  for (const f of linkedinFirms) {
    const key = f.name.toLowerCase().trim();
    const existing = firmMap.get(key) || {};
    firmMap.set(key, {
      ...existing,
      name: f.name,
      source: existing.source ? existing.source : 'linkedin_search',
      status: existing.status || 'pending_research',
      deal_id: deal.id,
      candidates: [...(existing.candidates || []), ...(f.candidates || [])],
    });
  }

  let saved = 0;
  for (const [, firm] of firmMap) {
    // Skip generic role labels — not real firms
    if (isGenericFirm(firm.name)) {
      console.log(`[FIRM RESEARCH] Skipping generic firm label: "${firm.name}"`);
      continue;
    }
    try {
      // Check if firm already exists for this deal
      const { data: existing } = await sb.from('firms')
        .select('id')
        .eq('deal_id', deal.id)
        .ilike('name', firm.name)
        .maybeSingle();

      const firmData = {
        deal_id: firm.deal_id,
        name: firm.name,
        firm_type: firm.firm_type || null,
        website: firm.website || null,
        geography_focus: firm.geography_focus || null,
        sector_focus: firm.sector_focus || null,
        cheque_size: firm.cheque_size || null,
        aum: firm.aum || null,
        past_investments: firm.past_investments || [],
        investment_thesis: firm.investment_thesis || null,
        match_rationale: firm.match_rationale || null,
        match_score: firm.match_score || null,
        research_model: firm.research_model || null,
        source: firm.source,
        status: firm.status || 'pending_research',
        updated_at: new Date().toISOString(),
      };

      let firmId;
      if (existing) {
        await sb.from('firms').update(firmData).eq('id', existing.id);
        firmId = existing.id;
      } else {
        firmData.created_at = new Date().toISOString();
        const { data: inserted } = await sb.from('firms').insert(firmData).select('id').single();
        firmId = inserted?.id;
        saved++;
      }

      // Save LinkedIn candidates as pending contacts linked to this firm
      if (firmId && firm.candidates?.length) {
        for (const candidate of firm.candidates) {
          if (!candidate.name || candidate.name === 'Unknown') continue;

          // Check if contact already exists
          const { data: existingContact } = await sb.from('contacts')
            .select('id')
            .eq('deal_id', deal.id)
            .eq('firm_id', firmId)
            .ilike('name', candidate.name)
            .maybeSingle();

          if (!existingContact) {
            try {
              await sb.from('contacts').insert({
                deal_id: deal.id,
                firm_id: firmId,
                name: candidate.name,
                company_name: isGenericFirm(firm.name) ? null : firm.name,
                job_title: candidate.title || null,
                linkedin_url: candidate.linkedin_url || null,
                linkedin_provider_id: candidate.linkedin_provider_id || null,
                source: 'LinkedIn Search',
                enrichment_status: 'Pending',
                pipeline_stage: 'Researched',
                created_at: new Date().toISOString(),
              });
            } catch { /* non-fatal — contact may already exist */ }
          }
        }
      }
    } catch (err) {
      console.warn(`[FIRM RESEARCH] Upsert error for "${firm.name}":`, err.message);
    }
  }

  return saved;
}

// ── FIRM ENRICHMENT LOOP: Find contacts at each firm ──────────────────

export async function runFirmEnrichmentLoop(deal) {
  const sb = getSupabase();
  if (!sb) return 0;

  // Get firms that have been researched but don't have contacts yet
  const { data: firms } = await sb.from('firms')
    .select('*')
    .eq('deal_id', deal.id)
    .in('status', ['researched', 'csv+research', 'pending_research'])
    .neq('status', 'contacts_found')
    .limit(10);

  if (!firms?.length) {
    console.log(`[FIRM RESEARCH] No firms need contact enrichment for ${deal.name}`);
    return 0;
  }

  console.log(`[FIRM RESEARCH] Finding contacts for ${firms.length} firms...`);
  let totalContacts = 0;

  for (const firm of firms) {
    const contacts = await findContactsAtFirm(firm, deal);
    totalContacts += contacts;

    // Update firm status
    await sb.from('firms').update({
      status: 'contacts_found',
      updated_at: new Date().toISOString(),
    }).eq('id', firm.id);

    await sleep(500);
  }

  return totalContacts;
}

async function findContactsAtFirm(firm, deal) {
  const sb = getSupabase();
  const key = process.env.GROK_API_KEY;

  const prompt = `Search for the following investment firm and find the names, titles, and LinkedIn URLs of the key decision-makers who evaluate and invest in deals:

Firm: ${firm.name}
Firm Type: ${firm.firm_type || 'Investment firm'}
Website: ${firm.website || 'Unknown'}

Return the 2-4 most relevant people (Partners, Managing Directors, Investment Directors, Principals, or Founders who are active investors). For each person return:
- full_name
- title
- linkedin_url (if findable — must be a real linkedin.com/in/ URL, do NOT guess)
- email (if publicly listed)
- notes (any relevant info about their investment focus)

Return ONLY a valid JSON array. If you cannot find real LinkedIn URLs, set linkedin_url to null. Never fabricate URLs.`;

  let people = [];
  let modelUsed = null;

  // Try Grok first (Responses API with web_search tool)
  if (key) {
    try {
      const res = await fetch('https://api.x.ai/v1/responses', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` },
        body: JSON.stringify({
          model: 'grok-4',
          input: [{ role: 'user', content: prompt }],
          tools: [{ type: 'web_search' }],
        }),
      });

      if (res.ok) {
        const data = await res.json();
        const outputMsg = (data.output || []).find(o => o.type === 'message');
        const text = outputMsg?.content?.find(c => c.type === 'output_text')?.text || '[]';
        people = parseContactsFromFirm(text);
        if (people.length > 0) modelUsed = 'grok-4';
      } else {
        const errText = await res.text().catch(() => '');
        console.warn(`[FIRM RESEARCH] Grok contact search failed (${res.status}) for ${firm.name}: ${errText.substring(0, 100)}`);
      }
    } catch (err) {
      console.warn(`[FIRM RESEARCH] Grok contact search error for ${firm.name}:`, err.message);
    }
  }

  // Fallback to Gemini
  if (people.length === 0) {
    const GEMINI_KEY = process.env.GEMINI_API_KEY || process.env.GEMINI_API_KEY_FALLBACK;
    if (GEMINI_KEY) {
      try {
        const res = await fetch(
          `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?key=${GEMINI_KEY}`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              contents: [{ parts: [{ text: prompt }] }],
              tools: [{ googleSearch: {} }],
              generationConfig: { temperature: 0.2, maxOutputTokens: 2048 },
            }),
          }
        );

        if (res.ok) {
          const data = await res.json();
          const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '[]';
          people = parseContactsFromFirm(text);
          if (people.length > 0) modelUsed = 'gemini';
        }
      } catch (err) {
        console.warn(`[FIRM RESEARCH] Gemini contact search error for ${firm.name}:`, err.message);
      }
    }
  }

  if (people.length === 0) {
    console.log(`[FIRM RESEARCH] No contacts found for ${firm.name}`);
    return 0;
  }

  console.log(`[FIRM RESEARCH] Found ${people.length} contacts at ${firm.name} via ${modelUsed}`);
  pushActivity({ type: 'research', action: `Found ${people.length} contacts`, note: `at ${firm.name}`, deal_name: deal.name, dealId: deal.id });

  let saved = 0;
  for (const person of people) {
    // CRITICAL: skip contacts with no valid name
    if (!person.full_name || person.full_name.trim() === '' || person.full_name.toLowerCase() === 'null') {
      console.warn(`[FIRM RESEARCH] Skipping contact with no valid name at ${firm.name}`);
      continue;
    }

    const firstName = person.full_name.split(' ')[0];
    if (!firstName || firstName.toLowerCase() === 'null') {
      console.warn(`[FIRM RESEARCH] Skipping — cannot extract first name: ${person.full_name}`);
      continue;
    }

    try {
      // Check if already exists
      const { data: existing } = await sb.from('contacts')
        .select('id')
        .eq('deal_id', deal.id)
        .eq('firm_id', firm.id)
        .ilike('name', person.full_name)
        .maybeSingle();

      if (!existing) {
        await sb.from('contacts').insert({
          deal_id: deal.id,
          firm_id: firm.id,
          name: person.full_name.trim(),
          company_name: firm.name,
          job_title: person.title || null,
          linkedin_url: validateLinkedInUrl(person.linkedin_url),
          email: person.email || null,
          notes: person.notes || null,
          source: `Firm Research (${modelUsed || 'AI'})`,
          enrichment_status: 'Pending',
          pipeline_stage: 'Researched',
          created_at: new Date().toISOString(),
        });
        saved++;
      }
    } catch (err) {
      console.warn(`[FIRM RESEARCH] Contact insert error for ${person.full_name}:`, err.message);
    }
  }

  return saved;
}

function parseContactsFromFirm(text) {
  try {
    const clean = text.replace(/```json|```/g, '').trim();
    const match = clean.match(/\[[\s\S]*\]/);
    if (!match) return [];
    return JSON.parse(match[0]).filter(p => p.full_name);
  } catch {
    return [];
  }
}

function validateLinkedInUrl(url) {
  if (!url) return null;
  if (!url.includes('linkedin.com/in/')) return null;
  // Basic sanity check — reject obviously wrong URLs
  const slug = url.match(/linkedin\.com\/in\/([^/?#\s]+)/i)?.[1];
  if (!slug || slug.length < 3) return null;
  return url.startsWith('http') ? url : `https://${url}`;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
