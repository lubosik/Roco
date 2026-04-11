// core/dealDocumentParser.js
import fs from 'fs';
import path from 'path';
import pdfParse from '@cedrugs/pdf-parse';
import Anthropic from '@anthropic-ai/sdk';
import { getSupabase } from './supabase.js';

const anthropic = new Anthropic();

async function parseWithClaude(documentText, agentContext) {
  const truncated = documentText.substring(0, 55000);

  const prompt = `You are an expert M&A analyst and fundraising advisor.

AGENT CONTEXT:
You are working on behalf of: ${agentContext.agentName}
Firm context: ${agentContext.firmDescription}
${agentContext.classificationGuidance ? `Additional guidance: ${agentContext.classificationGuidance}` : ''}

${DEAL_CLASSIFICATION_RULES}

DEAL DOCUMENT:
${truncated}

INSTRUCTIONS:
1. Extract ALL financial and business information from the document
2. Classify the deal type using the rules above
3. Identify the exact investor types needed based on EBITDA size, sector, and geography
4. Calculate or estimate equity required
5. Build a fundraising strategy
6. If no target raise amount is specified, set open_ended to true

Return ONLY valid JSON — no markdown, no explanation:
{
  "deal_name": "project name or company name",
  "company_name": "actual company name if revealed",
  "company_overview": "2-3 sentence description of the business",
  "deal_type": "Buyout|MBO|Growth Equity|Minority|Independent Sponsor|Fundless Sponsor|Venture|Secondary|Debt|Real Estate|Other",
  "sector": "primary sector",
  "sub_sector": "specific niche",
  "geography": "where company operates",
  "hq_location": "company HQ",
  "revenue_usd_m": null,
  "ebitda_usd_m": null,
  "ebitda_margin_pct": null,
  "enterprise_value_usd_m": null,
  "ev_ebitda_multiple": null,
  "equity_required_usd_m": null,
  "equity_required_notes": "how you calculated this",
  "debt_available_usd_m": null,
  "seller_note_usd_m": null,
  "rollover_equity_usd_m": null,
  "incoming_management": null,
  "revenue_cagr_pct": null,
  "ebitda_cagr_pct": null,
  "target_raise_usd_m": null,
  "open_ended": false,
  "min_check_usd_m": null,
  "max_check_usd_m": null,
  "ideal_investor_types": ["PE/Buyout", "Family Office"],
  "disqualified_investor_types": [],
  "ideal_investor_profile": "Detailed: what sectors they must have invested in, check size range, geography, specific experience needed",
  "estimated_investors_to_contact": 200,
  "fundraising_strategy": "3-4 sentence strategy: who will be targeted, why, in what order, and what the outreach approach will be",
  "investment_highlights": ["highlight 1", "highlight 2", "highlight 3"],
  "key_risks": ["risk 1", "risk 2"],
  "growth_levers": ["lever 1", "lever 2"],
  "succession_situation": false,
  "management_team_staying": null,
  "deal_source": null,
  "advisor": null,
  "timeline": null,
  "additional_context": null
}`;

  const msg = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 6000,
    messages: [{ role: 'user', content: prompt }],
  });

  const responseText = msg.content[0].text;
  const match = responseText.replace(/```json|```/g, '').trim().match(/\{[\s\S]*\}/);
  if (!match) throw new Error('No JSON in Claude response');
  return JSON.parse(match[0]);
}

// ─────────────────────────────────────────────────────────────────────────────
// CLASSIFICATION RULES — 2025 market data, embedded in every prompt
// ─────────────────────────────────────────────────────────────────────────────

const DEAL_CLASSIFICATION_RULES = `
DEAL CLASSIFICATION AND INVESTOR MATCHING RULES (2025 Market Data)

== BY DEAL TYPE ==

BUYOUT / MBO / SUCCESSION:
- Primary: PE/Buyout firms, Independent Sponsors, Family Offices with control appetite
- Secondary: Fundless Sponsors, Search Funds (for smaller deals)
- Avoid: Venture Capital, pure LP funds, passive minority investors

GROWTH EQUITY (minority, no control):
- Primary: Growth equity funds, Family Offices (flexible mandate)
- Secondary: PE firms with minority strategies, Strategic investors
- Avoid: Buyout-only PE firms, Independent Sponsors seeking control

INDEPENDENT SPONSOR / FUNDLESS SPONSOR:
- Primary: Family Offices (LP capital providers), High Net Worth individuals
- Secondary: Institutional LPs with co-investment mandates, Small PE funds
- Note: These deals need equity capital providers, not fund managers

VENTURE / EARLY STAGE:
- Primary: Venture Capital firms, Angel groups, CVCs
- Secondary: Family Offices with VC allocation, Accelerators
- Avoid: PE/Buyout, Independent Sponsors, LMM PE

SECONDARY / GP-LED:
- Primary: Secondary buyers, Fund of Funds with secondary mandates
- Secondary: Institutional LPs actively buying secondaries
- Avoid: Direct investors, VCs, buyout-only PE

REAL ESTATE:
- Primary: Real estate PE, REITs, Family Offices with RE allocation
- Avoid: Operating company investors, VCs

== BY EBITDA SIZE (2025 MARKET NORMS) ==

EBITDA < $1M (micro / search fund territory):
- Target: Search Funds, Individual investors, Small family offices, Fundless Sponsors
- Typical multiple: 3x-5x EBITDA
- Typical equity check: $500K-$3M

EBITDA $1M-$3M (lower main street / LMM):
- Target: Independent Sponsors, Small family offices, Fundless Sponsors, LMM PE
- Typical multiple: 4x-6x EBITDA
- Typical equity check: $2M-$10M

EBITDA $3M-$10M (lower middle market):
- Target: LMM PE firms, Independent Sponsors, Family Offices
- Typical multiple: 5x-7x EBITDA (avg 6.4x per GF Data 2025)
- Typical equity check: $5M-$40M

EBITDA $10M-$25M (middle market):
- Target: Mid-market PE firms, Family Offices, Growth equity
- Typical multiple: 7x-9x EBITDA (avg 7.2x per GF Data 2025)
- Typical equity check: $30M-$150M

EBITDA $25M+ (upper middle market):
- Target: Larger PE funds, Institutional family offices
- Typical multiple: 8x-12x EBITDA
- Typical equity check: $100M+

== BY SECTOR (2025 EV/EBITDA BENCHMARKS) ==

Healthcare / Medical: 12.8x median — target healthcare-focused PE, medtech funds
IT / Software / SaaS: 12.5x median — target tech PE, growth equity, VCs
Financial Services: 10.3x median — target financial services PE, fintech investors
B2B Services: 8.1x-8.4x median — broad PE mandate
Consumer / Retail: 8.1x-8.4x median — consumer-focused PE, family offices
Manufacturing / Industrials: 6x-8x median — industrial PE, family offices with ops experience
Energy / Resources: 7.4x median — energy-focused funds, infrastructure investors

== BY GEOGRAPHY ==

US-based deal:
- Must include: US PE, US family offices, US independent sponsors
- Also include: Global funds with US allocation

UK-based deal:
- Must include: UK PE, UK family offices, European PE with UK focus
- Also include: US funds active in UK/Europe

Europe-based deal:
- Must include: European PE, pan-European family offices
- Also include: Global funds with European mandate

== EQUITY REQUIRED CALCULATION ==

If EV and debt terms are known:
  Equity Required = EV - Senior Debt - Seller Note

If only EBITDA and deal type known:
  Estimated EV = EBITDA x typical_multiple_for_sector_and_size
  Typical Senior Debt = 2x-3x EBITDA (LMM) or 3x-4x EBITDA (MM)
  Estimated Equity = EV - Senior Debt

If no target raise amount specified:
  set target_raise_usd_m = null, set open_ended = true

== INVESTOR COUNT ESTIMATE ==

To raise equity_required:
  min_investors_needed = ceil(equity_required / max_check_size)
  target_outreach = min_investors_needed x 15  (typical conversion: 5-7%)

If open_ended: target_outreach = 200 (default)
`;

// ─────────────────────────────────────────────────────────────────────────────
// AGENT CONTEXT — read persona/settings from Supabase or fallback to defaults
// ─────────────────────────────────────────────────────────────────────────────

async function getAgentContext() {
  try {
    const sb = getSupabase();
    if (sb) {
      const { data } = await sb.from('deal_settings').select('key, value');
      const settings = {};
      (data || []).forEach(r => { settings[r.key] = r.value; });
      return {
        agentName:              settings.agent_name || settings.advisor_name || process.env.AGENT_NAME || 'Dom',
        firmDescription:        settings.firm_description || settings.agent_persona || 'Independent fundraising advisor specialising in private capital mandates.',
        classificationGuidance: settings.classification_guidance || '',
        targetGeographies:      settings.target_geographies || 'UK, US, Europe, Global',
      };
    }
  } catch {}
  return {
    agentName:              process.env.AGENT_NAME || 'Dom',
    firmDescription:        'Independent fundraising advisor specialising in private capital mandates.',
    classificationGuidance: '',
    targetGeographies:      'UK, US, Europe, Global',
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN EXPORT
// ─────────────────────────────────────────────────────────────────────────────

export async function parseDealDocument(filePath, filename, broadcastFn = null) {
  console.log(`[DEAL PARSER] Parsing: ${filename}`);
  broadcastFn?.('Reading document...');

  const extractedText = await extractDocumentText(filePath, filename);

  broadcastFn?.('Document read — classifying deal...');

  const agentContext = await getAgentContext();

  broadcastFn?.('Running AI classification...');

  try {
    const parsed = await parseWithClaude(extractedText, agentContext);

    console.log(`[DEAL PARSER] Extracted: ${parsed.deal_name}, type=${parsed.deal_type}, EBITDA=$${parsed.ebitda_usd_m}M`);
    console.log(`[DEAL PARSER] Ideal investors: ${parsed.ideal_investor_types?.join(', ')}`);

    broadcastFn?.(`Deal: ${parsed.deal_name || filename} — ${parsed.deal_type}`);
    broadcastFn?.(`Sector: ${parsed.sector} | EBITDA: $${parsed.ebitda_usd_m || '?'}M | EV: $${parsed.enterprise_value_usd_m || '?'}M`);
    broadcastFn?.(`Equity needed: $${parsed.equity_required_usd_m || '?'}M${parsed.open_ended ? ' (open-ended raise)' : ''}`);
    broadcastFn?.(`Target investors: ${(parsed.ideal_investor_types || []).join(', ')}`);
    if (parsed.fundraising_strategy) broadcastFn?.(`Strategy: ${parsed.fundraising_strategy.substring(0, 200)}`);
    broadcastFn?.(`Estimated outreach: ${parsed.estimated_investors_to_contact || 200} investors`);

    return { extractedText, parsed };

  } catch (err) {
    console.error('[DEAL PARSER] Claude parsing failed:', err.message);
    broadcastFn?.(`Classification failed: ${err.message}`);
    return {
      extractedText,
      parsed: {
        deal_name:                      filename.replace(/\.[^/.]+$/, ''),
        deal_type:                      'Unknown',
        sector:                         'Unknown',
        company_overview:               extractedText.substring(0, 300),
        ideal_investor_types:           ['PE/Buyout', 'Family Office'],
        ideal_investor_profile:         'Investor with relevant sector experience',
        disqualified_investor_types:    [],
        open_ended:                     true,
        fundraising_strategy:           'Broad outreach to relevant investors until deal is closed.',
        estimated_investors_to_contact: 200,
      },
    };
  }
}

export async function extractDocumentText(filePath, filename) {
  const ext = path.extname(filename).toLowerCase();
  if (ext === '.pdf') {
    const buffer = fs.readFileSync(filePath);
    const data = await pdfParse(buffer);
    return data.text;
  }
  if (ext === '.docx' || ext === '.doc') {
    const mammoth = (await import('mammoth')).default;
    const result = await mammoth.extractRawText({ path: filePath });
    return result.value;
  }
  return fs.readFileSync(filePath, 'utf-8');
}
