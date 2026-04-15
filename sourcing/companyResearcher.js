/**
 * sourcing/companyResearcher.js
 * Finds target companies for a sourcing campaign using 3 parallel streams:
 * Stream 1: Grok deep research (primary) / Gemini (fallback)
 * Stream 2: LinkedIn company search via Unipile + GPT-generated queries
 * Stream 3: Intent signal web search via Grok
 */

import { getSupabase } from '../core/supabase.js';
import { pushActivity } from '../dashboard/server.js';
import { aiComplete } from '../core/aiClient.js';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function extractJSON(text) {
  try {
    const clean = (text || '').replace(/```json|```/g, '').trim();
    const match = clean.match(/\[[\s\S]*\]/);
    if (match) return JSON.parse(match[0]);
  } catch {}
  return [];
}

// ─────────────────────────────────────────────────────────
// MAIN ENTRY POINT
// ─────────────────────────────────────────────────────────

export async function researchCompaniesForCampaign(campaign) {
  console.log(`[SOURCING RESEARCH] Starting company research for campaign: ${campaign.name}`);
  const sb = getSupabase();
  if (!sb) { console.warn('[SOURCING RESEARCH] No Supabase — skipping'); return 0; }

  pushActivity({ type: 'research', action: 'Company research starting', note: `[${campaign.name}]: Searching for target companies` });

  sb.from('activity_log').insert({
    event_type: 'SOURCING_RESEARCH_STARTED',
    summary: `[${campaign.name}]: Company research started`,
    created_at: new Date().toISOString(),
  }).then(null, () => {});

  const [grokRes, linkedinRes, intentRes] = await Promise.allSettled([
    runGrokCompanyResearch(campaign),
    runLinkedInCompanySearch(campaign),
    runIntentSignalSearch(campaign),
  ]);

  const grokCompanies    = grokRes.status    === 'fulfilled' ? (grokRes.value    || []) : [];
  const linkedinCompanies = linkedinRes.status === 'fulfilled' ? (linkedinRes.value || []) : [];
  const intentCompanies   = intentRes.status   === 'fulfilled' ? (intentRes.value   || []) : [];

  console.log(`[SOURCING RESEARCH] Grok: ${grokCompanies.length} | LinkedIn: ${linkedinCompanies.length} | Intent: ${intentCompanies.length}`);

  const allCompanies = [...grokCompanies, ...linkedinCompanies, ...intentCompanies];
  const saved = await upsertTargetCompanies(campaign.id, allCompanies);

  // Mark last_research_at
  const { error: updateErr } = await sb.from('sourcing_campaigns')
    .update({ last_research_at: new Date().toISOString(), updated_at: new Date().toISOString() })
    .eq('id', campaign.id);
  if (updateErr) console.warn('[SOURCING RESEARCH] Failed to update last_research_at:', updateErr.message);

  pushActivity({ type: 'research', action: 'Company research complete', note: `[${campaign.name}]: ${saved} target companies saved` });
  sb.from('activity_log').insert({
    event_type: 'SOURCING_RESEARCH_COMPLETE',
    summary: `[${campaign.name}]: ${saved} target companies found`,
    created_at: new Date().toISOString(),
  }).then(null, () => {});

  return saved;
}

// ─────────────────────────────────────────────────────────
// STREAM 1: GROK / GEMINI COMPANY RESEARCH
// ─────────────────────────────────────────────────────────

async function runGrokCompanyResearch(campaign) {
  const key = process.env.GROK_API_KEY;
  if (!key) {
    console.warn('[SOURCING RESEARCH] GROK_API_KEY not set — trying Gemini');
    return runGeminiCompanyResearch(campaign);
  }

  console.log('[SOURCING RESEARCH] Using Grok with web_search...');
  const prompt = buildCompanyResearchPrompt(campaign);

  try {
    const res = await fetch('https://api.x.ai/v1/responses', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` },
      body: JSON.stringify({
        model: process.env.RESEARCH_GROK_MODEL || 'grok-3-fast',
        input: [{ role: 'user', content: prompt }],
        tools: [{ type: 'web_search' }],
      }),
    });

    if (!res.ok) {
      const err = await res.text().catch(() => '');
      console.warn(`[SOURCING RESEARCH] Grok failed (${res.status}): ${err.substring(0, 150)} — falling back to Gemini`);
      return runGeminiCompanyResearch(campaign);
    }

    const data = await res.json();
    const outputMsg = (data.output || []).find(o => o.type === 'message');
    const text = outputMsg?.content?.find(c => c.type === 'output_text')?.text || '[]';
    const companies = parseCompanyResults(text, process.env.RESEARCH_GROK_MODEL || 'grok-3-fast');

    if (companies.length > 0) {
      console.log(`[SOURCING RESEARCH] Grok returned ${companies.length} companies`);
      return companies;
    }

    console.warn('[SOURCING RESEARCH] Grok returned 0 companies — falling back to Gemini');
    return runGeminiCompanyResearch(campaign);
  } catch (err) {
    console.warn('[SOURCING RESEARCH] Grok error:', err.message, '— falling back to Gemini');
    return runGeminiCompanyResearch(campaign);
  }
}

async function runGeminiCompanyResearch(campaign) {
  const GEMINI_KEY = process.env.GEMINI_API_KEY;
  const GEMINI_FALLBACK = process.env.GEMINI_API_KEY_FALLBACK;
  if (!GEMINI_KEY && !GEMINI_FALLBACK) {
    console.warn('[SOURCING RESEARCH] No Gemini keys — skipping');
    return [];
  }

  console.log('[SOURCING RESEARCH] Using Gemini with google_search...');
  const prompt = buildCompanyResearchPrompt(campaign);
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
          const companies = parseCompanyResults(text, `gemini/${model}`);
          if (companies.length > 0) {
            console.log(`[SOURCING RESEARCH] Gemini (${model}) returned ${companies.length} companies`);
            return companies;
          }
        } else {
          const errText = await res.text().catch(() => '');
          console.warn(`[SOURCING RESEARCH] Gemini ${model} failed (${res.status}): ${errText.substring(0, 100)}`);
        }
      } catch (err) {
        console.warn(`[SOURCING RESEARCH] Gemini ${model} error:`, err.message);
      }
    }
  }

  console.error('[SOURCING RESEARCH] All Gemini models failed');
  return [];
}

function buildCompanyResearchPrompt(campaign) {
  return `You are a specialist deal sourcing researcher for a ${campaign.firm_type || 'investment'} firm.

INVESTMENT FIRM: ${campaign.firm_name || 'Unnamed Firm'}
INVESTMENT THESIS: ${campaign.investment_thesis || 'Not specified'}

TARGET COMPANY CRITERIA:
- Sector: ${campaign.target_sector}
- Geography: ${campaign.target_geography}
- Business Model: ${campaign.business_model || 'Any'}
- Company Stage: ${campaign.company_stage || 'Any'}
- Ownership: ${campaign.ownership_type || 'Any'}
- Deal Type: ${campaign.deal_type || 'Any'}
- Investment Size: ${campaign.investment_size || 'Not specified'}
- Revenue Range: ${campaign.min_revenue || 'No minimum'} to ${campaign.max_revenue || 'No maximum'}
- EBITDA Range: ${campaign.min_ebitda || 'No minimum'} to ${campaign.max_ebitda || 'No maximum'}
- Company Age: ${campaign.min_company_age_months ? `Min ${campaign.min_company_age_months} months` : 'No minimum'} ${campaign.max_company_age_months ? `to max ${campaign.max_company_age_months} months` : ''}
- Headcount: ${campaign.headcount_min || 'Any'} to ${campaign.headcount_max || 'Any'} employees
- Intent Signals to Find: ${campaign.intent_signals || 'Not specified'}
- Keywords Required: ${campaign.keywords_include || 'None'}
- Keywords to Avoid: ${campaign.keywords_exclude || 'None'}

YOUR TASK:
Identify 20-30 real, specific companies that match these criteria exactly. Use your web search capability to find companies with recent news, LinkedIn activity, or public signals suggesting they are growing, profitable, and potentially open to investment.

For each company return:
- company_name (exact legal or trading name)
- website
- linkedin_company_url (real linkedin.com/company/ URL — do NOT guess)
- sector
- business_model
- geography (city + country)
- estimated_headcount (integer or null)
- estimated_revenue (e.g. "$2M-$5M annually" or null if unknown)
- estimated_ebitda (or null)
- founded_year (integer or null)
- ownership_type
- funding_stage (if VC/PE backed — latest round)
- recent_funding (amount and date if known)
- product_description (2-3 sentences on exactly what they do and who their customers are)
- intent_signals_found (specific signals you found: e.g. "posted hiring for CFO on LinkedIn March 2026, announced 40% revenue growth in press release")
- why_matches (2-3 sentences specific to why this company fits this firm's thesis — reference their actual product, revenue signals, and deal type alignment)
- match_score (integer 0-100 based on: sector fit 30pts, financial criteria fit 25pts, geography 20pts, ownership/stage fit 15pts, intent signal strength 10pts)

Return ONLY a valid JSON array. No preamble. No markdown. If you cannot find real data, set that field to null — never fabricate financial figures.`;
}

function parseCompanyResults(text, source) {
  try {
    const companies = extractJSON(text);
    if (!Array.isArray(companies)) return [];

    return companies
      .filter(c => c.company_name && typeof c.company_name === 'string' && c.company_name.trim().length > 1)
      .map(c => ({
        company_name:        (c.company_name || '').trim(),
        website:             c.website || null,
        linkedin_company_url: c.linkedin_company_url || null,
        sector:              c.sector || null,
        business_model:      c.business_model || null,
        geography:           c.geography || null,
        headcount:           typeof c.estimated_headcount === 'number' ? c.estimated_headcount : null,
        estimated_revenue:   c.estimated_revenue || null,
        estimated_ebitda:    c.estimated_ebitda || null,
        founded_year:        typeof c.founded_year === 'number' ? c.founded_year : null,
        ownership_type:      c.ownership_type || null,
        funding_stage:       c.funding_stage || null,
        recent_funding:      c.recent_funding || null,
        product_description: c.product_description || null,
        intent_signals_found: c.intent_signals_found || null,
        why_matches:         c.why_matches || null,
        match_score:         typeof c.match_score === 'number' ? Math.min(100, Math.max(0, c.match_score)) : null,
        source:              'research',
        research_status:     'researched',
      }));
  } catch (err) {
    console.warn(`[SOURCING RESEARCH] Parse error from ${source}:`, err.message);
    return [];
  }
}

// ─────────────────────────────────────────────────────────
// STREAM 2: LINKEDIN COMPANY SEARCH
// ─────────────────────────────────────────────────────────

async function runLinkedInCompanySearch(campaign) {
  try {
    const queries = await generateCompanySearchQueries(campaign);
    if (!queries?.length) return [];

    const { searchLinkedInPeople } = await import('../integrations/unipileClient.js');
    const results = [];

    for (const query of queries.slice(0, 3)) {
      try {
        // Unipile rejects queries > ~150 chars
        const truncatedQuery = query.length > 120 ? query.slice(0, 120) : query;
        const people = await searchLinkedInPeople({ keywords: truncatedQuery, limit: 10 });
        // Extract companies from people results
        for (const person of (people || [])) {
          if (person.company_name && person.company_name.trim()) {
            results.push({
              company_name:    person.company_name.trim(),
              sector:          campaign.target_sector,
              geography:       campaign.target_geography,
              source:          'linkedin_search',
              research_status: 'pending',
            });
          }
        }
        await sleep(1500);
      } catch (err) {
        console.warn(`[SOURCING RESEARCH] LinkedIn search error for "${query}":`, err.message);
      }
    }

    // Dedupe by company name
    const seen = new Set();
    return results.filter(r => {
      const key = r.company_name.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  } catch (err) {
    console.warn('[SOURCING RESEARCH] LinkedIn company search failed:', err.message);
    return [];
  }
}

async function generateCompanySearchQueries(campaign) {
  const prompt = `Generate 5 LinkedIn people search queries to find founders and CEOs at companies matching this investment thesis:

Sector: ${campaign.target_sector}
Geography: ${campaign.target_geography}
Business Model: ${campaign.business_model || 'Any'}
Stage: ${campaign.company_stage || 'Any'}
Revenue: ${campaign.min_revenue || '?'} - ${campaign.max_revenue || '?'}
Thesis: ${campaign.investment_thesis || ''}

Return ONLY a JSON array of 5 search query strings optimised for LinkedIn people search (targeting founders/CEOs at relevant companies).
No explanation.`;

  try {
    const text = await aiComplete(prompt, { maxTokens: 300, task: 'company-search-queries' });
    const queries = extractJSON(text);
    if (Array.isArray(queries) && queries.length > 0) return queries;
    return [];
  } catch (err) {
    console.warn('[SOURCING RESEARCH] GPT query generation failed:', err.message);
    return [];
  }
}

// ─────────────────────────────────────────────────────────
// STREAM 3: INTENT SIGNAL SEARCH
// ─────────────────────────────────────────────────────────

async function runIntentSignalSearch(campaign) {
  if (!campaign.intent_signals) return [];

  const key = process.env.GROK_API_KEY;
  if (!key) return [];

  const signals = campaign.intent_signals.split(',').map(s => s.trim()).filter(Boolean).slice(0, 3);
  const allResults = [];

  for (const signal of signals) {
    const query = `${campaign.target_sector} companies ${campaign.target_geography} "${signal}" 2025 2026`;

    const prompt = `Search the web for: "${query}"

Return real company names that appear in results showing this specific intent signal: "${signal}"
Only include companies in the ${campaign.target_sector} sector located in ${campaign.target_geography}.

For each company found, return:
- company_name
- website (if found)
- geography
- intent_signal_evidence (the specific text or evidence showing the signal)

Return ONLY a valid JSON array of up to 10 companies. If nothing relevant found, return [].`;

    try {
      const res = await fetch('https://api.x.ai/v1/responses', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` },
        body: JSON.stringify({
          model: process.env.RESEARCH_GROK_MODEL || 'grok-3-fast',
          input: [{ role: 'user', content: prompt }],
          tools: [{ type: 'web_search' }],
        }),
      });

      if (res.ok) {
        const data = await res.json();
        const outputMsg = (data.output || []).find(o => o.type === 'message');
        const text = outputMsg?.content?.find(c => c.type === 'output_text')?.text || '[]';
        const companies = extractJSON(text);

        if (Array.isArray(companies)) {
          for (const c of companies) {
            if (c.company_name) {
              allResults.push({
                company_name:        c.company_name.trim(),
                website:             c.website || null,
                geography:           c.geography || campaign.target_geography,
                sector:              campaign.target_sector,
                intent_signals_found: c.intent_signal_evidence || signal,
                source:              'research',
                research_status:     'researched',
              });
            }
          }
        }
      }
      await sleep(2000);
    } catch (err) {
      console.warn(`[SOURCING RESEARCH] Intent signal search error for "${signal}":`, err.message);
    }
  }

  return allResults;
}

// ─────────────────────────────────────────────────────────
// UPSERT TO SUPABASE
// ─────────────────────────────────────────────────────────

async function upsertTargetCompanies(campaignId, companies) {
  const sb = getSupabase();
  if (!sb || !companies.length) return 0;

  // Fetch already-saved company names for this campaign to avoid dupes
  const { data: existing } = await sb.from('target_companies')
    .select('company_name')
    .eq('campaign_id', campaignId);

  const existingNames = new Set((existing || []).map(c => c.company_name.toLowerCase().trim()));

  // Dedupe within the batch
  const seen = new Set();
  const toInsert = companies.filter(c => {
    const key = c.company_name.toLowerCase().trim();
    if (seen.has(key) || existingNames.has(key)) return false;
    seen.add(key);
    return true;
  }).map(c => ({ ...c, campaign_id: campaignId }));

  if (!toInsert.length) return 0;

  let saved = 0;
  // Batch insert in chunks of 20
  for (let i = 0; i < toInsert.length; i += 20) {
    const chunk = toInsert.slice(i, i + 20);
    const { error } = await sb.from('target_companies').insert(chunk);
    if (error) {
      console.warn('[SOURCING RESEARCH] Insert error:', error.message);
    } else {
      saved += chunk.length;
    }
  }

  return saved;
}

// ─────────────────────────────────────────────────────────
// DECISION MAKER RESEARCH (per company)
// ─────────────────────────────────────────────────────────

export async function findDecisionMakersAtCompany(company, campaign) {
  const sb = getSupabase();
  if (!sb) return 0;

  const key = process.env.GROK_API_KEY;
  const prompt = `Find the key decision makers at this company who would handle investment conversations:

Company: ${company.company_name}
Website: ${company.website || 'unknown'}
Sector: ${company.sector || campaign.target_sector}
LinkedIn: ${company.linkedin_company_url || 'unknown'}
What they do: ${company.product_description || 'Not available'}

Find the 1-3 most relevant people: CEO, Founder, Co-founder, Managing Director, or CFO.
These are people who would respond to an approach from an investment firm.

For each person return:
- full_name (must be a real person — do NOT invent names)
- title
- linkedin_url (real linkedin.com/in/ URL — set to null if not findable, never guess)
- email (only if publicly listed)
- notes (any relevant context about their role or investment openness)

Return ONLY a valid JSON array. If you cannot find real people, return an empty array.
Never fabricate names, URLs, or emails.`;

  let contacts = [];

  if (key) {
    try {
      const res = await fetch('https://api.x.ai/v1/responses', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` },
        body: JSON.stringify({
          model: process.env.RESEARCH_GROK_MODEL || 'grok-3-fast',
          input: [{ role: 'user', content: prompt }],
          tools: [{ type: 'web_search' }],
        }),
      });

      if (res.ok) {
        const data = await res.json();
        const outputMsg = (data.output || []).find(o => o.type === 'message');
        const text = outputMsg?.content?.find(c => c.type === 'output_text')?.text || '[]';
        contacts = extractJSON(text) || [];
      }
    } catch (err) {
      console.warn(`[SOURCING RESEARCH] Grok DM research error for ${company.company_name}:`, err.message);
    }
  }

  if (!contacts.length) {
    // Gemini fallback
    try {
      const text = await aiComplete(prompt, { maxTokens: 1000, task: `dm-research-${company.company_name}` });
      contacts = extractJSON(text) || [];
    } catch (err) {
      console.warn(`[SOURCING RESEARCH] AI fallback failed for ${company.company_name}:`, err.message);
    }
  }

  let added = 0;
  for (const contact of contacts) {
    const name = (contact.full_name || '').trim();
    if (!name || name.toLowerCase() === 'null' || name.length < 2) continue;

    // Validate linkedin_url — must be real linkedin.com/in/ URL
    let linkedinUrl = contact.linkedin_url || null;
    if (linkedinUrl && !linkedinUrl.includes('linkedin.com/in/')) linkedinUrl = null;

    const { error } = await sb.from('company_contacts').insert({
      campaign_id:    campaign.id,
      company_id:     company.id,
      name:           name,
      first_name:     name.split(' ')[0],
      title:          contact.title || null,
      linkedin_url:   linkedinUrl,
      email:          contact.email || null,
      notes:          contact.notes || null,
      pipeline_stage: 'researched',
      enrichment_status: 'pending',
    });

    if (!error) added++;
  }

  // Update company research_status
  await sb.from('target_companies')
    .update({ research_status: 'contacts_found', updated_at: new Date().toISOString() })
    .eq('id', company.id);

  pushActivity({
    type: 'research',
    action: 'Decision makers found',
    note: `[${campaign.name}]: Found ${added} contacts at ${company.company_name}`,
  });

  sb.from('activity_log').insert({
    event_type: 'CONTACTS_FOUND',
    summary: `[${campaign.name}] Found ${added} contacts at ${company.company_name}`,
    created_at: new Date().toISOString(),
  }).then(null, () => {});

  return added;
}
