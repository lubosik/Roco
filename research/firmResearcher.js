/**
 * research/firmResearcher.js
 * Firms-first research pipeline.
 * Stream 1: CSV import → firms table
 * Stream 2: Grok/Gemini grounded research → firms table
 * Stream 3: LinkedIn search → firms table
 * Then: firm enrichment loop → contacts table
 */

import { getSupabase } from '../core/supabase.js';
import { pushActivity } from '../dashboard/server.js';
import { sendTelegram } from '../approval/telegramBot.js';
import {
  canonicalizeLinkedInProfileUrl,
  getLinkedInSearchParameters,
  searchLinkedInPeople,
  searchLinkedInPeopleSalesNavigator,
} from '../integrations/unipileClient.js';
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';

// Keys read lazily inside functions so dotenv is loaded first

function boolFromEnv(value, fallback = false) {
  if (value == null || value === '') return fallback;
  return !['0', 'false', 'no', 'off'].includes(String(value).toLowerCase());
}

function getResearchConfig() {
  const configuredGeminiModels = (process.env.RESEARCH_FIRM_GEMINI_MODELS || '')
    .split(',')
    .map(v => v.trim())
    .filter(Boolean)
    .filter(model => !['gemini-2.0-flash', 'gemini-1.5-pro'].includes(model));

  return {
    primaryProvider: (process.env.RESEARCH_PRIMARY_PROVIDER || 'grok').toLowerCase(),
    enableGrokFallback: boolFromEnv(process.env.RESEARCH_ENABLE_GROK_FALLBACK, true),
    geminiModels: configuredGeminiModels
      .slice(0, 5)
      .concat(['gemini-2.5-flash', 'gemini-2.5-pro', 'gemini-2.5-flash-lite'])
      .filter((model, index, all) => all.indexOf(model) === index),
    grokModel: process.env.RESEARCH_FIRM_GROK_MODEL || 'grok-4',
  };
}

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

function parseDecisionMakerName(name) {
  const cleaned = String(name || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!cleaned) return null;
  const suffixes = new Set(['phd', 'md']);
  const parts = cleaned.split(' ').filter(Boolean).filter(part => !suffixes.has(part));
  if (!parts.length) return null;
  const first = parts[0] || '';
  const last = parts.length >= 2 ? parts[parts.length - 1] : '';
  const middle = parts.slice(1, -1);
  return {
    first,
    last,
    middleInitials: middle.map(part => part[0]).join(''),
    hasNamedMiddle: middle.some(part => part.length > 1),
    normalized: [first, last].filter(Boolean).join(' '),
    tokens: parts,
  };
}

function normalizeFirmIdentity(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\b(inc|llc|ltd|lp|llp|plc|corp|corporation|partners|partner|capital|holdings|group|ventures|management|advisors)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function arePrefixNameVariants(leftName, rightName) {
  const left = String(leftName || '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim().split(' ').filter(Boolean);
  const right = String(rightName || '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim().split(' ').filter(Boolean);
  if (left.length < 2 || right.length < 2) return false;
  if (left[0] !== right[0]) return false;
  const shorter = left.length <= right.length ? left : right;
  const longer = shorter === left ? right : left;
  return shorter.every((token, index) => longer[index] === token);
}

function areLikelySameDecisionMaker(leftName, rightName) {
  const left = parseDecisionMakerName(leftName);
  const right = parseDecisionMakerName(rightName);
  if (!left || !right) return false;
  if (left.first === right.first && left.last === right.last) {
    if (!left.middleInitials || !right.middleInitials) return true;
    if (left.middleInitials === right.middleInitials) return true;
    if (left.hasNamedMiddle && right.hasNamedMiddle) return false;
    return false;
  }
  return arePrefixNameVariants(leftName, rightName);
}

function scoreDecisionMaker(person) {
  let score = 0;
  if (person?.email) score += 5;
  if (person?.linkedin_provider_id) score += 4;
  if (person?.linkedin_url) score += 3;
  if (person?.title) score += 2;
  if (person?.notes) score += 1;
  score += Math.min(String(person?.full_name || person?.name || '').trim().length / 100, 0.5);
  return score;
}

function mergeDecisionMaker(existing, incoming) {
  const existingScore = scoreDecisionMaker(existing);
  const incomingScore = scoreDecisionMaker(incoming);
  const winner = incomingScore > existingScore ? incoming : existing;
  const loser = winner === incoming ? existing : incoming;
  return {
    ...loser,
    ...winner,
    full_name: winner.full_name || winner.name || loser.full_name || loser.name || null,
    title: winner.title || loser.title || null,
    linkedin_url: winner.linkedin_url || loser.linkedin_url || null,
    linkedin_provider_id: winner.linkedin_provider_id || loser.linkedin_provider_id || null,
    email: winner.email || loser.email || null,
    notes: winner.notes || loser.notes || null,
  };
}

function buildDecisionMakerIdentityKey(person, firmName = '') {
  const providerId = String(person?.linkedin_provider_id || person?.provider_id || '').trim().toLowerCase();
  if (providerId) return `provider:${providerId}`;
  const linkedinUrl = validateLinkedInUrl(person?.linkedin_url || person?.public_profile_url || person?.profile_url);
  if (linkedinUrl) return `linkedin:${canonicalizeLinkedInProfileUrl(linkedinUrl).toLowerCase()}`;
  const email = String(person?.email || '').trim().toLowerCase();
  if (email) return `email:${email}`;
  const parsedName = parseDecisionMakerName(person?.full_name || person?.name);
  if (!parsedName?.normalized) return '';
  const firmKey = normalizeFirmIdentity(firmName || person?.company_name || '');
  return `name:${parsedName.normalized}:${firmKey}`;
}

function buildDecisionMakerPatch(existing, incoming, firmName = '') {
  const merged = mergeDecisionMaker(
    { ...existing, company_name: existing.company_name || firmName || null },
    { ...incoming, company_name: incoming.company_name || firmName || null }
  );
  const patch = {};
  for (const field of ['name', 'linkedin_url', 'linkedin_provider_id', 'email', 'job_title']) {
    const nextValue = merged[field] ?? null;
    const currentValue = existing[field] ?? null;
    if (nextValue && nextValue !== currentValue) patch[field] = nextValue;
  }
  return patch;
}

function findMatchingDecisionMaker(existingContacts = [], incoming = {}, firmName = '') {
  const incomingKey = buildDecisionMakerIdentityKey(incoming, firmName);
  const normalizedFirm = normalizeFirmIdentity(firmName || incoming.company_name || '');
  let best = null;
  let bestScore = -1;

  for (const existing of existingContacts) {
    const existingFirm = normalizeFirmIdentity(existing.company_name || '');
    if (normalizedFirm && existingFirm && existingFirm !== normalizedFirm) continue;

    const existingKey = buildDecisionMakerIdentityKey(existing, firmName || existing.company_name || '');
    const exactIdentityMatch = !!incomingKey && incomingKey === existingKey;
    const sameNameVariant = areLikelySameDecisionMaker(existing.name || existing.full_name, incoming.name || incoming.full_name);
    if (exactIdentityMatch && (existing.name || existing.full_name) && (incoming.name || incoming.full_name) && !sameNameVariant) continue;
    if (!exactIdentityMatch && !sameNameVariant) continue;

    const score = scoreDecisionMaker(existing);
    if (score > bestScore) {
      best = existing;
      bestScore = score;
    }
  }

  return best;
}

function truncateList(items = [], limit = 4) {
  return items
    .filter(Boolean)
    .slice(0, limit)
    .join(', ');
}

async function notifyResearchOutcome(deal, summary = {}) {
  const totalQueuedContacts = (summary.insertedContacts || 0) + (summary.enrichedContacts || 0);
  const sampleFirms = truncateList(summary.sampleFirmNames || []);
  const headline = summary.totalDistinctFirms > 0
    ? `Firm research: ${summary.totalDistinctFirms} relevant firm${summary.totalDistinctFirms === 1 ? '' : 's'} found for ${deal.name}`
    : `Firm research: no relevant firms found for ${deal.name}`;
  const note = summary.totalDistinctFirms > 0
    ? [
        `${summary.newFirms || 0} added`,
        `${summary.updatedFirms || 0} updated`,
        `${summary.insertedContacts || 0} contact${summary.insertedContacts === 1 ? '' : 's'} added`,
        `${summary.enrichedContacts || 0} discovered via follow-on contact research`,
        sampleFirms ? `Examples: ${sampleFirms}` : null,
      ].filter(Boolean).join(' · ')
    : 'Searched CSV imports, grounded web research, and LinkedIn. Nothing strong enough to add automatically.';

  pushActivity({
    type: 'research',
    action: headline,
    note,
    deal_name: deal.name,
    dealId: deal.id,
  });

  const telegramMessage = summary.totalDistinctFirms > 0
    ? [
        `*Research update — ${deal.name}*`,
        `Found ${summary.totalDistinctFirms} relevant firm${summary.totalDistinctFirms === 1 ? '' : 's'}.`,
        `Added to campaign: ${summary.newFirms || 0}`,
        `Updated existing firms: ${summary.updatedFirms || 0}`,
        `New contacts queued: ${totalQueuedContacts}`,
        sampleFirms ? `Examples: ${sampleFirms}` : null,
      ].filter(Boolean).join('\n')
    : [
        `*Research update — ${deal.name}*`,
        `I researched more firms because the pipeline needed support, but I did not find any firm I was confident enough to add.`,
        `Checked: grounded web research, LinkedIn search, and CSV/imported data.`,
      ].join('\n');

  await sendTelegram(telegramMessage).catch(() => {});
}

/**
 * Main entry point — called immediately after deal is saved.
 * Runs all three firm-discovery streams and persists only firm-level data.
 */
export async function runFirmResearch(deal) {
  console.log(`[FIRM RESEARCH] Starting firms-first research for: ${deal.name}`);

  const sb = getSupabase();
  if (!sb) { console.warn('[FIRM RESEARCH] No Supabase — skipping'); return 0; }

  // Log start
  pushActivity({ type: 'research', action: `Firm research starting for ${deal.name}`, note: `Scanning investor database, PitchBook imports, and LinkedIn`, deal_name: deal.name, dealId: deal.id });
  sb.from('activity_log').insert({ deal_id: deal.id, event_type: 'RESEARCH_STARTED', summary: `Firm research started for ${deal.name}`, created_at: new Date().toISOString() }).then(null, () => {});

  // Run all three streams in parallel
  const [csvRes, researchRes, linkedinRes] = await Promise.allSettled([
    importFirmsFromCSV(deal),
    runPrimaryFirmResearch(deal),
    runLinkedInFirmSearch(deal),
  ]);

  const csvFirms = csvRes.status === 'fulfilled' ? csvRes.value : [];
  const researchedFirms = researchRes.status === 'fulfilled' ? researchRes.value : [];
  const linkedinFirms = linkedinRes.status === 'fulfilled' ? linkedinRes.value : [];

  console.log(`[FIRM RESEARCH] CSV: ${csvFirms.length} | AI research: ${researchedFirms.length} | LinkedIn: ${linkedinFirms.length}`);

  // Upsert all firms
  const summary = await upsertFirms(deal, csvFirms, researchedFirms, linkedinFirms);
  const savedFirms = summary.newFirms || 0;
  console.log(`[FIRM RESEARCH] ${savedFirms} firms saved to Supabase`);

  let enrichedContacts = 0;
  try {
    enrichedContacts = await runFirmEnrichmentLoop(deal);
  } catch (err) {
    console.warn(`[FIRM RESEARCH] Follow-on contact discovery failed for ${deal.name}:`, err.message);
  }
  summary.enrichedContacts = enrichedContacts;

  pushActivity({ type: 'research', action: `Firm research complete — ${savedFirms} firms identified for ${deal.name}`, note: `Scoring and ranking in progress`, deal_name: deal.name, dealId: deal.id });
  sb.from('activity_log').insert({ deal_id: deal.id, event_type: 'RESEARCH_COMPLETE', summary: `${savedFirms} firms`, created_at: new Date().toISOString() }).then(null, () => {});
  await notifyResearchOutcome(deal, summary);

  return savedFirms;
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

// ── STREAM 2: GEMINI/GROK DEEP RESEARCH ───────────────────────────────

async function runPrimaryFirmResearch(deal) {
  const config = getResearchConfig();
  if (config.primaryProvider === 'grok') {
    const grokFirst = await runGrokFirmResearch(deal);
    if (grokFirst.length > 0 || !config.enableGrokFallback) return grokFirst;
    return runGeminiFirmResearch(deal);
  }

  const geminiFirst = await runGeminiFirmResearch(deal);
  if (geminiFirst.length > 0 || !config.enableGrokFallback) return geminiFirst;
  return runGrokFirmResearch(deal);
}

async function runGrokFirmResearch(deal) {
  const { grokModel } = getResearchConfig();
  const key = process.env.XAI_API_KEY || process.env.GROK_API_KEY;
  if (!key) {
    console.warn('[FIRM RESEARCH] No Grok key set — skipping Grok firm research');
    return [];
  }

  console.log('[FIRM RESEARCH] Using Grok Responses API with web_search tool...');

  const prompt = buildFirmResearchPrompt(deal);

  try {
    const res = await fetch('https://api.x.ai/v1/responses', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` },
      body: JSON.stringify({
        model: grokModel,
        input: [{ role: 'user', content: prompt }],
        tools: [{ type: 'web_search' }],
      }),
    });

    if (!res.ok) {
      const err = await res.text().catch(() => '');
      console.warn(`[FIRM RESEARCH] Grok failed (${res.status}): ${err.substring(0, 150)}`);
      return [];
    }

    const data = await res.json();
    // Responses API: output[] contains message objects with content[]
    const outputMsg = (data.output || []).find(o => o.type === 'message');
    const text = outputMsg?.content?.find(c => c.type === 'output_text')?.text || '[]';
    const firms = parseFirmResearchResults(text, grokModel);
    if (firms.length > 0) {
      console.log(`[FIRM RESEARCH] Grok returned ${firms.length} firms`);
      return firms;
    }

    console.warn('[FIRM RESEARCH] Grok returned 0 firms');
    return [];
  } catch (err) {
    console.warn('[FIRM RESEARCH] Grok error:', err.message);
    return [];
  }
}

async function researchFirmWithGrok(firmName, deal, investor = {}) {
  const { grokModel } = getResearchConfig();
  const key = process.env.XAI_API_KEY || process.env.GROK_API_KEY;
  if (!key) throw new Error('No Grok key set');

  const prompt = buildSingleFirmPrompt(firmName, deal, investor);
  const res = await fetch('https://api.x.ai/v1/responses', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` },
    body: JSON.stringify({
      model: grokModel,
      input: [{ role: 'user', content: prompt }],
      tools: [{ type: 'web_search' }],
    }),
  });

  if (!res.ok) {
    const err = await res.text().catch(() => '');
    throw new Error(`Grok ${res.status}: ${err.substring(0, 150)}`);
  }

  const data = await res.json();
  const outputMsg = (data.output || []).find(o => o.type === 'message');
  const text = outputMsg?.content?.find(c => c.type === 'output_text')?.text || '{}';
  return parseSingleFirmResult(text, `grok/${grokModel}`);
}

async function runGeminiFirmResearch(deal) {
  const GEMINI_KEY = process.env.GEMINI_API_KEY;
  const GEMINI_FALLBACK = process.env.GEMINI_API_KEY_FALLBACK;
  if (!GEMINI_KEY && !GEMINI_FALLBACK) {
    console.warn('[FIRM RESEARCH] No Gemini keys — skipping');
    return [];
  }

  const { geminiModels } = getResearchConfig();
  console.log('[FIRM RESEARCH] Using Gemini with google_search...');

  const prompt = buildFirmResearchPrompt(deal);
  const keys = [GEMINI_KEY, GEMINI_FALLBACK].filter(Boolean);

  for (const key of keys) {
    for (const model of geminiModels) {
      try {
        const res = await fetch(
          `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              contents: [{ parts: [{ text: prompt }] }],
              tools: [{ google_search: {} }],
              generationConfig: { temperature: 0.2, maxOutputTokens: 4096 },
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

async function researchFirmWithGemini(firmName, deal, investor = {}) {
  const { geminiModels } = getResearchConfig();
  const keys = [process.env.GEMINI_API_KEY, process.env.GEMINI_API_KEY_FALLBACK].filter(Boolean);
  if (!keys.length) throw new Error('No Gemini keys set');

  const prompt = buildSingleFirmPrompt(firmName, deal, investor);
  let lastError = null;

  for (const key of keys) {
    for (const model of geminiModels) {
      try {
        const res = await fetch(
          `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              contents: [{ parts: [{ text: prompt }] }],
              tools: [{ google_search: {} }],
              generationConfig: { temperature: 0.2, maxOutputTokens: 2048 },
            }),
          }
        );

        if (!res.ok) {
          const errText = await res.text().catch(() => '');
          throw new Error(`Gemini ${model} ${res.status}: ${errText.substring(0, 150)}`);
        }

        const data = await res.json();
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
        return parseSingleFirmResult(text, `gemini/${model}`);
      } catch (err) {
        lastError = err;
      }
    }
  }

  throw lastError || new Error('All Gemini models failed');
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

function buildSingleFirmPrompt(firmName, deal, investor = {}) {
  return `Research this investor firm for a fundraising campaign and return only firm-level data.

DEAL:
Name: ${deal.name}
Sector: ${deal.sector || 'Unknown'}
Stage: ${deal.raise_type || deal.type || 'Investment'}
Geography: ${deal.geography || 'United States'}
Description: ${(deal.description || '').slice(0, 300)}

FIRM:
Name: ${firmName}
Type: ${investor.investor_type || investor.firm_type || 'Unknown'}
Description: ${(investor.description || investor.research_notes || '').slice(0, 300)}

Return ONLY valid JSON:
{
  "base_score": <integer 0-100>,
  "thesis": "<1-3 sentence investment thesis or null>",
  "past_investments": ["Company 1", "Company 2"],
  "aum": "<AUM or fund size or null>",
  "justification": "<2 sentence explanation of why this firm fits the deal>"
}`;
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

function parseSingleFirmResult(text, source) {
  const clean = text.replace(/```json|```/g, '').trim();
  const match = clean.match(/\{[\s\S]*\}/);
  if (!match) throw new Error(`No firm JSON returned from ${source}`);
  const parsed = JSON.parse(match[0]);
  return {
    base_score: Math.max(0, Math.min(Number(parsed.base_score) || 50, 100)),
    thesis: typeof parsed.thesis === 'string' ? parsed.thesis.trim() || null : null,
    past_investments: Array.isArray(parsed.past_investments)
      ? parsed.past_investments.slice(0, 5)
      : [],
    aum: typeof parsed.aum === 'string' ? parsed.aum.trim() || null : null,
    justification: typeof parsed.justification === 'string' ? parsed.justification.trim() || null : null,
  };
}

export async function researchFirmOnly(investor, deal) {
  const firmName = investor.firm_name || investor.name;
  if (!firmName) return buildMinimalFirmData(investor, deal);

  // Skip external research if the record is already rich enough
  // (from investor list + KB enrichment) — don't waste API calls on data we already have
  const richText = investor.description || investor.thesis || investor.research_notes || '';
  const hasRichData = richText.length > 100 && investor.investor_type;
  const hasInvestmentHistory = Array.isArray(investor.past_investments) && investor.past_investments.length > 0;
  if (hasRichData || hasInvestmentHistory) {
    return buildFirmDataFromDB(investor, deal);
  }

  const config = getResearchConfig();

  if (config.primaryProvider === 'grok') {
    try {
      return await researchFirmWithGrok(firmName, deal, investor);
    } catch (grokErr) {
      console.warn(`[FIRM RESEARCH] Grok failed for ${firmName} — trying Gemini:`, grokErr.message);
      pushActivity({
        type: 'error',
        action: `Grok failed for ${firmName} — falling back to Gemini`,
        note: grokErr.message?.slice(0, 60),
        deal_name: deal.name,
        dealId: deal.id,
      });
    }

    try {
      return await researchFirmWithGemini(firmName, deal, investor);
    } catch (geminiErr) {
      pushActivity({
        type: 'error',
        action: `Research failed: ${firmName} — both Grok and Gemini unavailable`,
        note: geminiErr.message?.slice(0, 60),
        deal_name: deal.name,
        dealId: deal.id,
      });
      return buildMinimalFirmData(investor, deal);
    }
  }

  try {
    return await researchFirmWithGemini(firmName, deal, investor);
  } catch (geminiErr) {
    console.warn(`[FIRM RESEARCH] Gemini failed for ${firmName} — trying Grok:`, geminiErr.message);
    pushActivity({
      type: 'error',
      action: `Gemini failed for ${firmName} — falling back to Grok`,
      note: geminiErr.message?.slice(0, 60),
      deal_name: deal.name,
      dealId: deal.id,
    });
  }

  try {
    return await researchFirmWithGrok(firmName, deal, investor);
  } catch (grokErr) {
    pushActivity({
      type: 'error',
      action: `Research failed: ${firmName} — both Gemini and Grok unavailable`,
      note: grokErr.message?.slice(0, 60),
      deal_name: deal.name,
      dealId: deal.id,
    });
    return buildMinimalFirmData(investor, deal);
  }
}

function buildFirmDataFromDB(investor, deal) {
  const sectorMatch = (investor.description || '').toLowerCase()
    .includes((deal.sector || '').toLowerCase());
  const firmName = investor.firm_name || investor.name || 'This firm';
  const investorType = investor.investor_type || investor.firm_type || 'investor';
  const focusText = cleanFirmText(investor.preferred_industries || investor.description || '', 220);
  const description = cleanFirmText(investor.description || investor.research_notes || '', 520);
  const geography = cleanFirmText(
    investor.preferred_geographies || investor.hq_city || investor.hq_country || investor.geography_focus || '',
    120
  );
  const pastInvestments = normalizePastInvestments(investor.past_investments);
  const sizeRange = formatDealSizeRange(investor);

  return {
    base_score: sectorMatch ? 60 : 40,
    thesis: description || null,
    past_investments: pastInvestments,
    aum: investor.aum || investor.aum_fund_size || null,
    justification: buildFirmJustification({
      firmName,
      investorType,
      focusText,
      geography,
      sizeRange,
      pastInvestments,
      deal,
      fallback: `${firmName} is relevant for ${deal.name || 'this deal'} based on its recorded mandate and sector profile.`,
    }),
  };
}

function buildMinimalFirmData(investor, deal) {
  const firmName = investor.firm_name || investor.name || 'This investor';
  const investorType = investor.investor_type || investor.firm_type || 'investor';
  const focusText = cleanFirmText(investor.preferred_industries || investor.description || '', 180);
  return {
    base_score: 35,
    thesis: null,
    past_investments: normalizePastInvestments(investor.past_investments),
    aum: investor.aum || investor.aum_fund_size || null,
    justification: buildFirmJustification({
      firmName,
      investorType,
      focusText,
      geography: cleanFirmText(investor.preferred_geographies || investor.hq_country || '', 80),
      sizeRange: formatDealSizeRange(investor),
      pastInvestments: normalizePastInvestments(investor.past_investments),
      deal,
      fallback: `${firmName} is included because its recorded profile appears relevant to ${deal.sector || 'the deal'} and matches the current investor search constraints. Full external research was unavailable, so this firm should be reviewed manually before approval.`,
    }),
  };
}

function cleanFirmText(value, maxLen = 220) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  if (text.length <= maxLen) return text.replace(/[;,:\-–—\s]+$/, '');
  const shortened = text.slice(0, maxLen);
  const boundary = Math.max(
    shortened.lastIndexOf('. '),
    shortened.lastIndexOf('; '),
    shortened.lastIndexOf(', '),
    shortened.lastIndexOf(' ')
  );
  return shortened.slice(0, boundary > 60 ? boundary : maxLen).replace(/[;,:\-–—\s]+$/, '');
}

function normalizePastInvestments(value) {
  if (Array.isArray(value)) {
    return value
      .map(item => typeof item === 'string' ? item.trim() : (item?.company || item?.name || '').trim())
      .filter(Boolean)
      .slice(0, 5);
  }
  if (typeof value === 'string' && value.trim()) {
    return value
      .split(/[;,\n]+/)
      .map(item => item.trim())
      .filter(Boolean)
      .slice(0, 5);
  }
  return [];
}

function formatDealSizeRange(investor) {
  const min = investor.preferred_deal_size_min;
  const max = investor.preferred_deal_size_max;
  if (min && max) return `$${Number(min)}M-$${Number(max)}M`;
  if (max) return `up to $${Number(max)}M`;
  if (min) return `from $${Number(min)}M`;
  return '';
}

function buildFirmJustification({ firmName, investorType, focusText, geography, sizeRange, pastInvestments, deal, fallback }) {
  const pieces = [];
  const lead = `${firmName} is a ${investorType}${geography ? ` active in ${geography}` : ''}.`;
  pieces.push(lead);

  if (focusText) {
    pieces.push(`Its stated investment focus includes ${focusText}.`);
  }

  if (deal?.sector) {
    pieces.push(`That mandate lines up with ${deal.sector} and the investor profile defined for ${deal.name || 'this deal'}.`);
  }

  if (sizeRange) {
    pieces.push(`Recorded deal size appetite is ${sizeRange}.`);
  }

  if (pastInvestments.length > 0) {
    pieces.push(`Relevant portfolio evidence includes ${pastInvestments.slice(0, 3).join(', ')}.`);
  }

  const paragraph = pieces.join(' ').replace(/\s+/g, ' ').trim();
  return paragraph.length >= 120 ? paragraph : fallback;
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
  if (!sb) {
    return {
      totalDistinctFirms: 0,
      newFirms: 0,
      updatedFirms: 0,
      insertedContacts: 0,
      updatedContacts: 0,
      sampleFirmNames: [],
    };
  }

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
  let updatedFirms = 0;
  let insertedContacts = 0;
  let updatedContacts = 0;
  const processedFirmNames = [];
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
        updatedFirms++;
      } else {
        firmData.created_at = new Date().toISOString();
        const { data: inserted } = await sb.from('firms').insert(firmData).select('id').single();
        firmId = inserted?.id;
        saved++;
      }
      if (firmId && !processedFirmNames.includes(firm.name)) processedFirmNames.push(firm.name);

      // Save LinkedIn candidates as pending contacts linked to this firm
      if (firmId && firm.candidates?.length) {
        const { data: existingDealContacts } = await sb.from('contacts')
          .select('id, name, email, linkedin_url, linkedin_provider_id, job_title, company_name')
          .eq('deal_id', deal.id);

        for (const candidate of firm.candidates) {
          if (!candidate.name || candidate.name === 'Unknown') continue;

          const existingContact = findMatchingDecisionMaker(existingDealContacts || [], {
            ...candidate,
            company_name: isGenericFirm(firm.name) ? null : firm.name,
          }, firm.name);

          if (existingContact) {
            const patch = buildDecisionMakerPatch(existingContact, {
              ...candidate,
              company_name: isGenericFirm(firm.name) ? null : firm.name,
            }, firm.name);
            if (Object.keys(patch).length) {
              await sb.from('contacts').update(patch).eq('id', existingContact.id).then(null, () => {});
              Object.assign(existingContact, patch);
              updatedContacts++;
            }
          } else {
            try {
              const { data: inserted } = await sb.from('contacts').insert({
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
              }).select('id, name, email, linkedin_url, linkedin_provider_id, job_title, company_name').single();
              if (inserted) {
                (existingDealContacts || []).push(inserted);
                insertedContacts++;
              }
            } catch { /* non-fatal — contact may already exist */ }
          }
        }
      }
    } catch (err) {
      console.warn(`[FIRM RESEARCH] Upsert error for "${firm.name}":`, err.message);
    }
  }

  return {
    totalDistinctFirms: processedFirmNames.length,
    newFirms: saved,
    updatedFirms,
    insertedContacts,
    updatedContacts,
    sampleFirmNames: processedFirmNames.slice(0, 5),
  };
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
    const enrichedFirm = await hydrateFirmResearchForEnrichment(firm, deal);
    const contacts = await findContactsAtFirm(enrichedFirm, deal);
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

function hasFirmResearchValue(value) {
  if (Array.isArray(value)) return value.filter(Boolean).length > 0;
  return String(value || '').trim().length > 0;
}

function listMissingFirmResearchFields(firm) {
  const missing = [];
  if (!hasFirmResearchValue(firm?.aum)) missing.push('AUM');
  if (!hasFirmResearchValue(firm?.past_investments)) missing.push('past investments');
  if (!hasFirmResearchValue(firm?.investment_thesis || firm?.thesis)) missing.push('investment thesis');
  if (!hasFirmResearchValue(firm?.match_rationale || firm?.justification)) missing.push('why this firm');
  return missing;
}

async function hydrateFirmResearchForEnrichment(firm, deal) {
  const sb = getSupabase();
  if (!sb || !firm?.id) return firm;

  const missingFields = listMissingFirmResearchFields(firm);
  if (!missingFields.length) return firm;

  try {
    const refreshed = await researchFirmOnly({
      ...firm,
      name: firm.name || firm.firm_name,
      firm_name: firm.firm_name || firm.name,
      investor_type: firm.firm_type || firm.investor_type,
      thesis: firm.thesis || firm.investment_thesis || null,
      justification: firm.justification || firm.match_rationale || null,
    }, deal);

    const updates = {};
    if (!hasFirmResearchValue(firm.aum) && hasFirmResearchValue(refreshed?.aum)) {
      updates.aum = refreshed.aum;
    }
    if (!hasFirmResearchValue(firm.past_investments) && hasFirmResearchValue(refreshed?.past_investments)) {
      updates.past_investments = refreshed.past_investments;
    }
    if (!hasFirmResearchValue(firm.investment_thesis || firm.thesis) && hasFirmResearchValue(refreshed?.thesis)) {
      updates.investment_thesis = refreshed.thesis;
    }
    if (!hasFirmResearchValue(firm.match_rationale || firm.justification) && hasFirmResearchValue(refreshed?.justification)) {
      updates.match_rationale = refreshed.justification;
    }

    if (!Object.keys(updates).length) return firm;

    updates.updated_at = new Date().toISOString();
    await sb.from('firms').update(updates).eq('id', firm.id);

    pushActivity({
      type: 'research',
      action: `Firm enrichment filled ${missingFields.join(', ')} for ${firm.name}`,
      note: deal.name,
      deal_name: deal.name,
      dealId: deal.id,
    });

    return {
      ...firm,
      ...updates,
      thesis: updates.investment_thesis || firm.thesis || firm.investment_thesis || null,
      justification: updates.match_rationale || firm.justification || firm.match_rationale || null,
    };
  } catch (err) {
    console.warn(`[FIRM RESEARCH] Could not enrich missing firm data for ${firm.name}:`, err.message);
    return firm;
  }
}

export async function findDecisionMakers(firm, deal) {
  const storedContacts = await getStoredDecisionMakersForFirm(firm);
  const unipileContacts = await findDecisionMakersViaUnipile(firm, deal);

  const firmRecord = {
    name: firm.firm_name || firm.name,
    firm_type: firm.contact_type || firm.firm_type || 'Investment firm',
    website: firm.website || null,
  };
  const aiContacts = (storedContacts.length + unipileContacts.length) >= 2
    ? []
    : await findContactsAtFirm(firmRecord, deal, { persist: false });

  const merged = dedupeDecisionMakers([
    ...storedContacts,
    ...unipileContacts,
    ...aiContacts.map(person => ({
      full_name: person.full_name,
      title: person.title || null,
      linkedin_url: null,  // AI-generated URLs are unverified — enrichment finds the real one via Unipile
      linkedin_provider_id: null,
      email: person.email || null,
      source: person.source || 'gemini',
    })),
  ]);

  return merged.map(person => ({
    name: person.full_name,
    job_title: person.title || null,
    linkedin_url: validateLinkedInUrl(person.linkedin_url),
    linkedin_provider_id: person.linkedin_provider_id || null,
    email: person.email || null,
    source: person.source || 'gemini',
  }));
}

async function getStoredDecisionMakersForFirm(firm) {
  const sb = getSupabase();
  if (!sb || !firm?.investor_id) return [];

  try {
    const { data: investor } = await sb.from('investors_db')
      .select('decision_maker_name, primary_contact_name, primary_contact_title, decision_maker_linkedin, linkedin_url, email, primary_contact_email')
      .eq('id', firm.investor_id)
      .maybeSingle();

    const name = investor?.decision_maker_name || investor?.primary_contact_name || null;
    if (!name) return [];

    return [{
      full_name: name,
      title: investor.primary_contact_title || null,
      // Legacy stored LinkedIn URLs are not trusted here. The dedicated finder verifies them later.
      linkedin_url: null,
      linkedin_provider_id: null,
      email: investor.email || investor.primary_contact_email || null,
      source: 'investors_db',
    }];
  } catch (err) {
    console.warn(`[FIRM RESEARCH] Stored decision-maker lookup failed for ${firm?.firm_name || firm?.name}:`, err.message);
    return [];
  }
}

function normalizeSearchText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function tokenizeSearchText(value) {
  return normalizeSearchText(value).split(/\s+/).filter(Boolean);
}

function buildDecisionMakerSearchQueries(firm, deal) {
  const firmName = firm?.firm_name || firm?.name || '';
  const roles = ['partner', 'managing partner', 'principal', 'investment director', 'managing director', 'founder'];
  const queries = roles.map(role => `${firmName} ${role}`);
  if (deal?.sector) queries.push(`${firmName} ${deal.sector} investor`);
  return [...new Set(queries.map(query => query.trim()).filter(Boolean))].slice(0, 5);
}

async function resolveCompanyParameterIds(firm, deal) {
  const firmName = firm?.firm_name || firm?.name || '';
  if (!firmName) return [];

  const attempts = [
    { keywords: firmName, service: 'CLASSIC' },
    { keywords: firmName, service: 'SALES_NAVIGATOR' },
    { keywords: buildDecisionMakerSearchQueries(firm, deal)[0] || firmName, service: 'CLASSIC' },
  ];

  const ids = new Set();
  const normalizedFirmName = normalizeSearchText(firmName);

  for (const attempt of attempts) {
    try {
      const items = await getLinkedInSearchParameters({
        type: 'COMPANY',
        keywords: attempt.keywords,
        service: attempt.service,
        limit: 10,
      });
      for (const item of items || []) {
        const title = normalizeSearchText(item?.title || item?.name || '');
        if (!title) continue;
        if (title === normalizedFirmName || title.includes(normalizedFirmName) || normalizedFirmName.includes(title)) {
          ids.add(String(item.id));
        }
      }
      if (ids.size) break;
    } catch (err) {
      console.warn(`[FIRM RESEARCH] Company parameter lookup failed for "${firmName}" (${attempt.service}): ${err.message}`);
    }
  }

  return [...ids];
}

function scoreUnipileDecisionMaker(person, firm) {
  const firmTokens = tokenizeSearchText(firm?.firm_name || firm?.name);
  const titleTokens = tokenizeSearchText(person?.title || person?.headline);
  const companyTokens = tokenizeSearchText([
    person?.company_name,
    person?.company,
    person?.current_company?.name,
    person?.headline,
  ].filter(Boolean).join(' '));

  const firmOverlap = firmTokens.filter(token => companyTokens.includes(token)).length;
  let score = firmOverlap * 4;
  if (titleTokens.includes('partner')) score += 5;
  if (titleTokens.includes('principal')) score += 4;
  if (titleTokens.includes('director')) score += 3;
  if (titleTokens.includes('founder')) score += 3;
  if (titleTokens.includes('investor')) score += 2;
  if (person?.provider_id) score += 1;
  return score;
}

async function findDecisionMakersViaUnipile(firm, deal) {
  const queries = buildDecisionMakerSearchQueries(firm, deal);
  if (!queries.length) return [];
  const companyIds = await resolveCompanyParameterIds(firm, deal);

  const seen = new Set();
  const candidates = [];
  const pushCandidate = (person, source) => {
    const fullName = [person?.first_name, person?.last_name].filter(Boolean).join(' ').trim() || person?.name || null;
    if (!fullName) return;
    const linkedinUrl = validateLinkedInUrl(
      person?.public_profile_url
      || person?.profile_url
      || (person?.public_identifier ? `https://www.linkedin.com/in/${person.public_identifier}` : null)
    );
    const key = buildDecisionMakerIdentityKey({
      ...person,
      full_name: fullName,
      linkedin_url: linkedinUrl,
    }, firm?.name);
    if (!key || seen.has(key)) return;
    seen.add(key);
    candidates.push({
      full_name: fullName,
      title: person?.title || person?.headline || null,
      linkedin_url: linkedinUrl,
      linkedin_provider_id: person?.provider_id || null,
      email: null,
      source,
      _score: scoreUnipileDecisionMaker(person, firm),
    });
  };

  for (const query of queries) {
    try {
      const salesResults = await searchLinkedInPeopleSalesNavigator({
        keywords: query,
        companyIds,
        networkDistance: [1, 2, 3],
        limit: 10,
      });
      (salesResults || []).forEach(person => pushCandidate(person, 'unipile_sales_navigator'));
    } catch (err) {
      const message = String(err?.message || '');
      if (!/403|501|feature_not_subscribed|subscription_required|not implemented/i.test(message)) {
        console.warn(`[FIRM RESEARCH] Sales Navigator search failed for "${query}": ${message}`);
      }
    }

    try {
      const classicResults = await searchLinkedInPeople({
        keywords: companyIds.length ? undefined : query,
        companyIds,
        networkDistance: [1, 2, 3],
        limit: 25,
      });
      (classicResults || []).forEach(person => pushCandidate(person, 'unipile_linkedin_search'));
    } catch (err) {
      console.warn(`[FIRM RESEARCH] LinkedIn search failed for "${query}": ${err.message}`);
    }

    if (candidates.length >= 8) break;
  }

  return candidates
    .filter(person => person._score >= 4)
    .sort((left, right) => right._score - left._score)
    .slice(0, 4);
}

function dedupeDecisionMakers(people) {
  const seen = new Map();
  const deduped = [];

  for (const person of people || []) {
    const fullName = String(person?.full_name || person?.name || '').trim();
    const linkedinUrl = validateLinkedInUrl(person?.linkedin_url);
    const normalized = {
      ...person,
      full_name: fullName,
      linkedin_url: linkedinUrl ? canonicalizeLinkedInProfileUrl(linkedinUrl) : null,
    };
    const key = buildDecisionMakerIdentityKey(normalized, person?.company_name || '');
    if (!key) continue;
    const existingIndex = seen.get(key);
    if (existingIndex === undefined) {
      seen.set(key, deduped.length);
      deduped.push(normalized);
      continue;
    }
    if (key.startsWith('name:') && !areLikelySameDecisionMaker(deduped[existingIndex].full_name, normalized.full_name)) {
      seen.set(`${key}:${deduped.length}`, deduped.length);
      deduped.push(normalized);
      continue;
    }
    deduped[existingIndex] = mergeDecisionMaker(deduped[existingIndex], normalized);
  }

  return deduped;
}

async function findContactsAtFirm(firm, deal, options = {}) {
  const sb = getSupabase();
  const grokKey = process.env.XAI_API_KEY || process.env.GROK_API_KEY;
  const shouldPersist = options.persist !== false;
  const { geminiModels, grokModel } = getResearchConfig();
  const geminiKeys = [process.env.GEMINI_API_KEY, process.env.GEMINI_API_KEY_FALLBACK].filter(Boolean);

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
  if (grokKey) {
    try {
      const res = await fetch('https://api.x.ai/v1/responses', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${grokKey}` },
        body: JSON.stringify({
          model: grokModel,
          input: [{ role: 'user', content: prompt }],
          tools: [{ type: 'web_search' }],
        }),
      });

      if (res.ok) {
        const data = await res.json();
        const outputMsg = (data.output || []).find(o => o.type === 'message');
        const text = outputMsg?.content?.find(c => c.type === 'output_text')?.text || '[]';
          people = parseContactsFromFirm(text);
          if (people.length > 0) modelUsed = grokModel;
      } else {
        const errText = await res.text().catch(() => '');
        console.warn(`[FIRM RESEARCH] Grok contact search failed (${res.status}) for ${firm.name}: ${errText.substring(0, 100)}`);
      }
    } catch (err) {
      console.warn(`[FIRM RESEARCH] Grok contact search error for ${firm.name}:`, err.message);
    }
  }

  // Fallback to Gemini
  if (people.length === 0 && geminiKeys.length > 0) {
    for (const key of geminiKeys) {
      for (const model of geminiModels) {
        try {
          const res = await fetch(
            `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`,
            {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                contents: [{ parts: [{ text: prompt }] }],
                tools: [{ google_search: {} }],
                generationConfig: { temperature: 0.2, maxOutputTokens: 2048 },
              }),
            }
          );

          if (!res.ok) {
            const errText = await res.text().catch(() => '');
            console.warn(`[FIRM RESEARCH] Gemini ${model} contact search failed (${res.status}) for ${firm.name}: ${errText.substring(0, 100)}`);
            continue;
          }

          const data = await res.json();
          const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '[]';
          people = parseContactsFromFirm(text);
          if (people.length > 0) {
            modelUsed = model;
            break;
          }
        } catch (err) {
          console.warn(`[FIRM RESEARCH] Gemini ${model} contact search error for ${firm.name}:`, err.message);
        }
      }
      if (people.length > 0) break;
    }
  }

  if (people.length === 0) {
    console.log(`[FIRM RESEARCH] No contacts found for ${firm.name}`);
    return shouldPersist ? 0 : [];
  }

  console.log(`[FIRM RESEARCH] Found ${people.length} contacts at ${firm.name} via ${modelUsed}`);
  pushActivity({ type: 'research', action: `Decision makers identified: ${people.length} contact${people.length !== 1 ? 's' : ''} at ${firm.name}`, note: `${deal.name} · Source: ${modelUsed || 'gemini'}`, deal_name: deal.name, dealId: deal.id });

  if (!shouldPersist) {
    return people.map(person => ({
      ...person,
      source: modelUsed || 'gemini',
    }));
  }

  let saved = 0;
  const { data: existingDealContacts } = await sb.from('contacts')
    .select('id, name, email, linkedin_url, linkedin_provider_id, job_title, company_name')
    .eq('deal_id', deal.id);

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
      const existing = findMatchingDecisionMaker(existingDealContacts || [], {
        ...person,
        name: person.full_name.trim(),
        company_name: firm.name,
      }, firm.name);

      if (existing) {
        const patch = buildDecisionMakerPatch(existing, {
          ...person,
          name: person.full_name.trim(),
          company_name: firm.name,
        }, firm.name);
        if (Object.keys(patch).length) {
          await sb.from('contacts').update(patch).eq('id', existing.id).then(null, () => {});
          Object.assign(existing, patch);
        }
      } else {
        await sb.from('contacts').insert({
          deal_id: deal.id,
          firm_id: firm.id,
          name: person.full_name.trim(),
          company_name: firm.name,
          job_title: person.title || null,
          linkedin_url: null,
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
