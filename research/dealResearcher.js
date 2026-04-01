/**
 * research/dealResearcher.js
 * Two-source research on deal launch: Gemini Deep Research + LinkedIn People Search.
 * All results saved directly to Supabase contacts table (not Notion — that happens in phaseNotionSync).
 */

import { getSupabase } from '../core/supabase.js';
import { aiComplete } from '../core/aiClient.js';

// Keys read lazily inside functions so dotenv is loaded first

/**
 * Main entry point — called immediately after deal is saved.
 * Runs Gemini + LinkedIn in parallel, deduplicates, saves to Supabase.
 * @returns {number} Number of new contacts saved
 */
export async function runDealResearch(deal) {
  console.log(`[RESEARCH] Starting research for: ${deal.name}`);

  const [deepResearchRes, linkedinRes] = await Promise.allSettled([
    runDeepResearch(deal),
    runLinkedInSearch(deal),
  ]);
  const geminiRes = deepResearchRes;

  const allResults = [
    ...(geminiRes.status === 'fulfilled' ? geminiRes.value : []),
    ...(linkedinRes.status === 'fulfilled' ? linkedinRes.value : []),
  ];

  console.log(`[RESEARCH] Raw results: ${allResults.length}`);

  // Deduplicate by LinkedIn URL or name+company
  const seen = new Set();
  const unique = allResults.filter(r => {
    const key = r.linkedin_url
      ? r.linkedin_url.toLowerCase().replace(/\/$/, '')
      : `${r.name}|${r.company_name}`.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // Deduplicate against existing Supabase contacts for this deal
  const sb = getSupabase();
  const { data: existing } = await sb.from('contacts')
    .select('linkedin_url, name, company_name')
    .eq('deal_id', deal.id);

  const existingKeys = new Set([
    ...(existing || []).map(c => (c.linkedin_url || '').toLowerCase().replace(/\/$/, '')).filter(Boolean),
    ...(existing || []).map(c => `${c.name}|${c.company_name}`.toLowerCase()),
  ]);

  const newContacts = unique.filter(r => {
    const key = r.linkedin_url
      ? r.linkedin_url.toLowerCase().replace(/\/$/, '')
      : `${r.name}|${r.company_name}`.toLowerCase();
    return key && !existingKeys.has(key);
  });

  console.log(`[RESEARCH] ${newContacts.length} new unique contacts to add`);

  let saved = 0;
  for (const contact of newContacts) {
    try {
      await sb.from('contacts').insert({
        deal_id: deal.id,
        name: contact.name,
        company_name: contact.company_name || '',
        job_title: contact.job_title || null,
        linkedin_url: contact.linkedin_url || null,
        linkedin_provider_id: contact.linkedin_provider_id || null,
        email: null,
        sector_focus: contact.sector_focus || null,
        geography: contact.geography || null,
        typical_cheque_size: contact.typical_cheque_size || null,
        aum_fund_size: contact.aum_fund_size || null,
        past_investments: contact.past_investments || null,
        notes: contact.notes || null,
        source: contact.source || 'Research',
        enrichment_status: 'Pending',
        pipeline_stage: 'Researched',
        investor_score: null,
        notion_page_id: null,
        created_at: new Date().toISOString(),
      });
      saved++;
    } catch (err) {
      console.warn(`[RESEARCH] Save failed for ${contact.name}:`, err.message);
    }
  }

  console.log(`[RESEARCH] Saved ${saved} new contacts to Supabase`);

  // Broadcast to dashboard
  global.broadcast?.({ type: 'ACTIVITY', activity: {
    message: `Research complete — ${saved} new investors found for ${deal.name}`,
    type: 'research',
    deal_id: deal.id,
    deal_name: deal.name,
    timestamp: new Date().toISOString(),
  }});

  if (sb) {
    sb.from('activity_log').insert({
      message: `Research complete — ${saved} new investors found`,
      activity_type: 'research',
      deal_id: deal.id,
      created_at: new Date().toISOString(),
    }).catch(() => {});
  }

  return saved;
}

// ── DEEP RESEARCH ORCHESTRATOR — Grok primary, Gemini fallback ────────

async function runDeepResearch(deal) {
  // Try Grok first
  const grokResults = await runGrokDeepResearch(deal);
  if (grokResults.length > 0) return grokResults;

  console.warn('[RESEARCH] Grok deep research returned 0 results — falling back to Gemini');
  return runGeminiDeepResearch(deal);
}

// ── GROK DEEP RESEARCH ────────────────────────────────────────────────

async function runGrokDeepResearch(deal) {
  const key = process.env.GROK_API_KEY;
  if (!key) return [];

  console.log('[RESEARCH] Starting Grok Deep Research...');

  const prompt = `Research the top 25 most relevant investors for this fundraising deal.

DEAL DETAILS:
Name: ${deal.name}
Sector: ${deal.sector || 'Technology'}
Geography: ${deal.geography || 'UK'}
Stage: ${deal.raise_type || 'Pre-Seed/Seed'}
Target: £${Number(deal.target_amount || 0).toLocaleString()}
Cheque range: £${Number(deal.min_cheque || 0).toLocaleString()}–£${Number(deal.max_cheque || 0).toLocaleString()}
Description: ${deal.description || ''}
Ideal investor: ${deal.investor_profile || ''}

Find REAL, currently active investors (VCs, family offices, angels, PE) who:
- Have invested in ${deal.sector || 'technology'} companies in the last 3 years
- Are active in ${deal.geography || 'UK'}
- Typically write cheques in the target range
- Include their specific partner/decision-maker names with LinkedIn URLs
- Include 2-3 specific past portfolio companies similar to this deal

Return ONLY a JSON array, no markdown:
[{
  "firm_name": "",
  "contact_name": "",
  "contact_title": "",
  "contact_linkedin": "",
  "sector_focus": "",
  "geography": "",
  "typical_cheque": "",
  "aum": "",
  "past_investments": "",
  "why_relevant": "",
  "source": "Grok Research"
}]`;

  try {
    const res = await fetch('https://api.x.ai/v1/responses', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${key}`,
      },
      body: JSON.stringify({
        model: 'grok-4-latest',
        input: [{ role: 'user', content: prompt }],
        tools: [{ type: 'web_search' }],
      }),
    });

    if (!res.ok) {
      const errText = await res.text().catch(() => '');
      console.warn(`[RESEARCH] Grok failed (${res.status}): ${errText.substring(0, 150)}`);
      return [];
    }

    const data = await res.json();
    const outputMsg = (data.output || []).find(o => o.type === 'message');
    const text = outputMsg?.content?.find(c => c.type === 'output_text')?.text || '[]';
    return parseDeepResearchResults(text, 'Grok Research');
  } catch (err) {
    console.warn('[RESEARCH] Grok deep research error:', err.message);
    return [];
  }
}

// ── GEMINI DEEP RESEARCH ──────────────────────────────────────────────

async function runGeminiDeepResearch(deal) {
  const GEMINI_KEY = process.env.GEMINI_API_KEY;
  const GEMINI_FALLBACK = process.env.GEMINI_API_KEY_FALLBACK;
  if (!GEMINI_KEY && !GEMINI_FALLBACK) {
    console.warn('[RESEARCH] GEMINI_API_KEY not set — skipping Gemini research');
    return [];
  }

  console.log('[RESEARCH] Starting Gemini Deep Research...');

  const prompt = `Research the top 25 most relevant investors for this fundraising deal.

DEAL DETAILS:
Name: ${deal.name}
Sector: ${deal.sector || 'Technology'}
Geography: ${deal.geography || 'UK'}
Stage: ${deal.raise_type || 'Pre-Seed/Seed'}
Target: £${Number(deal.target_amount || 0).toLocaleString()}
Cheque range: £${Number(deal.min_cheque || 0).toLocaleString()}–£${Number(deal.max_cheque || 0).toLocaleString()}
Description: ${deal.description || ''}
Ideal investor: ${deal.investor_profile || ''}

Find REAL, currently active investors (VCs, family offices, angels, PE) who:
- Have invested in ${deal.sector || 'technology'} companies in the last 3 years
- Are active in ${deal.geography || 'UK'}
- Typically write cheques in the target range
- Include their specific partner/decision-maker names with LinkedIn URLs
- Include 2-3 specific past portfolio companies similar to this deal

For each investor return structured data. If any field is uncertain, omit it rather than guess.

Return ONLY a JSON array, no markdown:
[{
  "firm_name": "",
  "contact_name": "",
  "contact_title": "",
  "contact_linkedin": "",
  "sector_focus": "",
  "geography": "",
  "typical_cheque": "",
  "aum": "",
  "past_investments": "",
  "why_relevant": "",
  "source": "Gemini Research"
}]`;

  const models = [
    'gemini-2.5-pro-preview-06-05',
    'gemini-1.5-pro',
  ];
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
              generationConfig: { temperature: 0.3, maxOutputTokens: 8192 },
            }),
          }
        );

        if (res.ok) {
          const data = await res.json();
          const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '[]';
          const results = parseDeepResearchResults(text, 'Gemini Research');
          if (results.length > 0) return results;
          console.warn(`[RESEARCH] ${model} returned 0 results — trying next`);
        } else {
          const errText = await res.text().catch(() => '');
          console.warn(`[RESEARCH] ${model} failed (${res.status}): ${errText.substring(0, 100)}`);
        }
      } catch (err) {
        console.warn(`[RESEARCH] ${model} error: ${err.message}`);
      }
    }
    if (key !== keys[keys.length - 1]) {
      console.warn('[RESEARCH] Primary Gemini key exhausted — trying fallback key');
    }
  }

  console.error('[RESEARCH] All Gemini models/keys failed');
  return [];
}

function parseDeepResearchResults(text, source) {
  try {
    const clean = text.replace(/```json|```/g, '').trim();
    const match = clean.match(/\[[\s\S]*\]/);
    if (!match) return [];
    const investors = JSON.parse(match[0]);
    console.log(`[RESEARCH] ${source} found ${investors.length} investors`);
    return investors
      .filter(inv => inv.contact_name && inv.contact_name !== 'Unknown')
      .map(inv => ({
        name: inv.contact_name,
        company_name: inv.firm_name || '',
        job_title: inv.contact_title || '',
        linkedin_url: inv.contact_linkedin || null,
        sector_focus: inv.sector_focus || '',
        geography: inv.geography || '',
        typical_cheque_size: inv.typical_cheque || '',
        aum_fund_size: inv.aum || '',
        past_investments: inv.past_investments || '',
        notes: inv.why_relevant || '',
        source,
      }));
  } catch (err) {
    console.warn(`[RESEARCH] ${source} JSON parse failed:`, err.message);
    return [];
  }
}

// ── LINKEDIN SEARCH ───────────────────────────────────────────────────

/**
 * Use GPT-5.4 (low reasoning) to generate the best LinkedIn search keyword string for a deal.
 * Falls back to a simple constructed string if AI fails.
 */
async function generateSearchQueries(deal) {
  const prompt = `Generate the single best LinkedIn people search keyword string to find investors for this fundraising deal.

Deal: ${deal.name}
Sector: ${deal.sector || 'technology'}
Geography: ${deal.geography || 'UK'}
Stage: ${deal.raise_type || 'Pre-Seed/Seed'}
Target: £${Number(deal.target_amount || 0).toLocaleString()}
Ideal investor: ${deal.investor_profile || ''}

Return ONLY a JSON array of 3–5 short keyword strings (no markdown), e.g.:
["fintech investor UK", "seed stage VC partner", "angel investor fintech"]`;

  try {
    const text = await aiComplete(prompt, {
      reasoning: 'low',
      maxTokens: 150,
      task: 'search_queries',
    });
    const queries = JSON.parse(text.replace(/```json|```/g, '').trim());
    return Array.isArray(queries) ? queries : [];
  } catch (err) {
    console.warn('[LI SEARCH] Query generation failed:', err.message);
    return [
      `${deal.sector || 'technology'} investor`,
      `${deal.sector || 'technology'} VC partner`,
      `family office ${deal.sector || 'technology'}`,
      `angel investor ${deal.sector || 'technology'}`,
      `${deal.sector || 'technology'} fund manager`,
    ];
  }
}

/**
 * Fetch LinkedIn search parameter IDs for a given type and query.
 * Types: INDUSTRY | LOCATION | COMPANY | SCHOOL | SERVICE
 * LinkedIn's search API requires internal IDs, not raw text strings.
 */
async function getLinkedInSearchParamId({ type, query }) {
  try {
    const url = `${process.env.UNIPILE_DSN}/api/v1/linkedin/search/parameters` +
      `?account_id=${process.env.UNIPILE_LINKEDIN_ACCOUNT_ID}&type=${type}&query=${encodeURIComponent(query)}`;

    const res = await fetch(url, {
      headers: {
        'X-API-KEY': process.env.UNIPILE_API_KEY,
        'accept': 'application/json',
      },
    });

    if (!res.ok) return null;
    const data = await res.json();
    return data.items?.[0]?.id || null;
  } catch (err) {
    console.warn(`[LI SEARCH PARAMS] Failed for ${type}="${query}":`, err.message);
    return null;
  }
}

async function runLinkedInSearch(deal) {
  if (!process.env.UNIPILE_API_KEY) {
    console.warn('[RESEARCH] UNIPILE_API_KEY not set — skipping LinkedIn search');
    return [];
  }

  console.log('[RESEARCH] Running LinkedIn People Search...');

  // Resolve location ID — LinkedIn requires internal IDs, not raw text
  const locationId = await getLinkedInSearchParamId({
    type: 'LOCATION',
    query: deal.geography || 'United Kingdom',
  });

  // Generate the best keyword string via GPT-5.4; use first query for the search
  const queries = await generateSearchQueries(deal);
  const keywords = queries[0] || `${deal.sector || 'technology'} investor`;

  const searchPayload = {
    api: 'classic',
    category: 'people',
    keywords,
    ...(locationId && { location: [locationId] }),
  };

  try {
    const res = await fetch(
      `${process.env.UNIPILE_DSN}/api/v1/linkedin/search` +
      `?account_id=${process.env.UNIPILE_LINKEDIN_ACCOUNT_ID}&limit=50`,
      {
        method: 'POST',
        headers: {
          'X-API-KEY': process.env.UNIPILE_API_KEY,
          'Content-Type': 'application/json',
          'accept': 'application/json',
        },
        body: JSON.stringify(searchPayload),
      }
    );

    if (!res.ok) {
      const errText = await res.text();
      console.warn('[LI SEARCH] Failed:', res.status, errText.substring(0, 200));
      return [];
    }

    const data = await res.json();
    const results = data.items || [];
    console.log(`[RESEARCH] LinkedIn search returned ${results.length} people`);

    return results
      .map(person => ({
        name: [person.first_name, person.last_name].filter(Boolean).join(' ') || person.name || 'Unknown',
        company_name: person.current_company?.name || '',
        job_title: person.title || person.headline || '',
        linkedin_url: person.profile_url ||
          (person.public_identifier ? `https://linkedin.com/in/${person.public_identifier}` : null),
        linkedin_provider_id: person.provider_id || null,
        source: 'LinkedIn Search',
      }))
      .filter(p => p.name && p.name !== 'Unknown');

  } catch (err) {
    console.error('[LI SEARCH] Error:', err.message);
    return [];
  }
}
