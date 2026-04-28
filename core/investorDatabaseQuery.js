// core/investorDatabaseQuery.js
// Professional-grade investor identification engine.
//
// Architecture (mirrors how placement agents work):
//   Phase 1 — Full universe scan: load ALL investors, apply hard filters (status, type kill, size kill, geo kill)
//   Phase 2 — Intelligence pre-load: fetch comp-deal boosts BEFORE scoring
//   Phase 3 — Deterministic scoring: pure JS, no AI, produces ranked longlist (0–100 base + up to 40 boost)
//   Phase 4 — AI batch scoring: top candidates get final AI refinement with richer context
//   Output  — Top 150 ranked by combined score
//
// Point budget for deterministic scoring (100 pts base):
//   Investor type fit          22 pts  — mandate match to deal structure
//   Deal / check size          22 pts  — check size compatibility (incl. AUM inference)
//   Sector / thesis depth      24 pts  — preferred industries + verticals + sub-sector + description
//   Geography                  14 pts  — HQ, preferred geo, region overlap
//   Activity velocity           8 pts  — 12m pace, velocity vs prior year, 7-day recency
//   Fund timing & dry powder    7 pts  — vintage age, dry powder, open funds (pacing cycle)
//   Deal structure preference   3 pts  — built into type scoring as bonus
//                             ------
//   Max base                  100 pts
//   Intelligence boost        +0–40   (comparable deal history — capped at 100 total)

import { getSupabase } from './supabase.js';
import { haikuComplete } from './aiClient.js';
import { researchFirmOnly } from '../research/firmResearcher.js';

// ─── deal type detection ───────────────────────────────────────────────────
function getDealTypeFlags(dealInfo) {
  const t = (dealInfo.deal_type || '').toLowerCase();
  const isISSponsor = /independent.sponsor|fundless|co.invest/i.test(t);
  const isBuyout    = isISSponsor || /buyout|private.equity|lbo/i.test(t);
  const isVC        = /venture|seed|series|startup|growth.equity/i.test(t) && !isBuyout;
  return { isISSponsor, isBuyout, isVC };
}

// ─── hard filter — returns true if investor should be excluded entirely ─────
function isHardExcluded(investor, dealInfo, flags) {
  const { isBuyout } = flags;
  const invType   = (investor.investor_type || '').toLowerCase();
  const invStatus = (investor.investor_status || '').toLowerCase();
  const prefInvMin = parseFloat(investor.preferred_investment_amount_min) || null;
  const prefDealMin = parseFloat(investor.preferred_deal_size_min) || null;
  const equity = parseFloat(dealInfo.equity_required_usd_m) || 0;
  const ev     = parseFloat(dealInfo.enterprise_value_usd_m) || 0;

  // Skip dissolved / inactive investors — they are not deploying capital
  if (/inactive|dissolved|wound.down|liquidated|closed.fund/i.test(invStatus)) return true;

  // VC-only on a PE/buyout deal
  const isVCOnly = /venture capital|seed stage|early.stage|accelerator|incubator/i.test(invType) &&
    !/private equity|buyout|growth equity|family office/i.test(invType);
  if (isBuyout && isVCOnly) return true;

  // Mega-PE whose minimum far exceeds this deal (investor minimum > 10× deal equity)
  const statedMin = prefInvMin || prefDealMin;
  if (statedMin && equity > 0 && equity < statedMin * 0.1) return true;
  if (statedMin && ev > 0 && ev < statedMin * 0.05) return true;

  return false;
}

// ─── deterministic scoring (pure JS, no AI) ─────────────────────────────────
// Returns 0–100 base score. Intelligence boost applied separately in the caller.
function scoreDeterministic(investor, dealInfo) {
  let score = 0;
  const reasons = [];

  const { isBuyout, isVC, isISSponsor } = getDealTypeFlags(dealInfo);

  // Extract investor fields
  const invType        = (investor.investor_type     || '').toLowerCase();
  const invPrefTypes   = (investor.preferred_investment_types || '').toLowerCase();
  const invIndustries  = (investor.preferred_industries || '').toLowerCase();
  const invVerticals   = (investor.preferred_verticals  || '').toLowerCase();
  const invOtherPrefs  = (investor.other_preferences    || '').toLowerCase();
  const invDesc        = (investor.description          || '').toLowerCase();
  const invPrefGeo     = (investor.preferred_geographies || '').toLowerCase();
  const invCountry     = (investor.hq_country           || '').toLowerCase();
  const invRegion      = (investor.hq_region            || '').toLowerCase();
  const invAUM         = parseFloat(investor.aum_millions) || 0;
  const prefInvMin     = parseFloat(investor.preferred_investment_amount_min) || null;
  const prefInvMax     = parseFloat(investor.preferred_investment_amount_max) || null;
  const prefDealMin    = parseFloat(investor.preferred_deal_size_min) || null;
  const prefDealMax    = parseFloat(investor.preferred_deal_size_max) || null;
  const prefEbitdaMin  = parseFloat(investor.preferred_ebitda_min) || null;
  const prefEbitdaMax  = parseFloat(investor.preferred_ebitda_max) || null;
  const inv12mo        = Number(investor.investments_last_12m) || 0;
  const inv6mo         = Number(investor.investments_last_6m)  || 0;
  const inv7d          = Number(investor.investments_last_7d)  || 0;
  const inv2y          = Number(investor.investments_last_2y)  || 0;
  const dryPowder      = parseFloat(investor.dry_powder_millions) || 0;
  const fundVintage    = parseInt(investor.last_closed_fund_vintage) || 0;
  const fundsOpen      = parseInt(investor.num_funds_open) || 0;

  // Deal parameters
  const equity    = parseFloat(dealInfo.equity_required_usd_m) || 0;
  const ev        = parseFloat(dealInfo.enterprise_value_usd_m) || 0;
  const ebitda    = parseFloat(dealInfo.ebitda_usd_m) || 0;
  const sector    = (dealInfo.sector || '').toLowerCase();
  const subSector = (dealInfo.sub_sector || '').toLowerCase().trim();
  const geo       = (dealInfo.geography || dealInfo.hq_location || 'United States').toLowerCase();

  // ── 1. INVESTOR TYPE + STRUCTURE FIT (22 pts) ──────────────────────────
  const peIdealTypes = ['private equity', 'pe/buyout', 'buyout', 'family office', 'independent sponsor',
    'fundless sponsor', 'holding company', 'family investment office', 'endowment', 'pension fund',
    'sovereign wealth', 'asset manager'];
  const peBroadTypes = ['growth equity', 'mezzanine', 'credit', 'debt', 'real assets', 'infrastructure'];
  const vcOnlyTypes  = ['venture capital', 'angel', 'seed', 'accelerator', 'incubator', 'corporate venture'];

  const invTypeText = `${invType} ${invPrefTypes}`;
  let typeScore = 0;

  if (isBuyout) {
    const idealHit = peIdealTypes.some(t => invTypeText.includes(t));
    const broadHit = peBroadTypes.some(t => invTypeText.includes(t));
    const vcHit    = vcOnlyTypes.some(t => invTypeText.includes(t));
    if (idealHit && !vcHit)       { typeScore = 22; reasons.push(`Type ideal for PE/IS: ${investor.investor_type} (+22)`); }
    else if (broadHit && !vcHit)  { typeScore = 13; reasons.push(`Type broadly compatible: ${investor.investor_type} (+13)`); }
    else if (!invType)            { typeScore = 9;  reasons.push('Type unknown — neutral (+9)'); }
    else                          { typeScore = 3;  reasons.push(`Type weak fit: ${investor.investor_type} (+3)`); }

    // Deal structure precision bonus
    if (isISSponsor && /co.invest|co-invest|direct/i.test(invPrefTypes)) {
      typeScore = Math.min(typeScore + 3, 22);
      reasons.push('Explicitly prefers co-invest / direct — structure bonus (+3)');
    } else if (/buyout|lbo/i.test(dealInfo.deal_type || '') && /buyout|control|acquisition/i.test(invPrefTypes)) {
      typeScore = Math.min(typeScore + 2, 22);
      reasons.push('Explicitly prefers buyout / control deals — structure bonus (+2)');
    }
  } else if (isVC) {
    const vcHit = vcOnlyTypes.some(t => invTypeText.includes(t));
    const foHit = invTypeText.includes('family office') || invTypeText.includes('growth');
    if (vcHit)       { typeScore = 22; reasons.push(`Type ideal for VC deal: ${investor.investor_type} (+22)`); }
    else if (foHit)  { typeScore = 14; reasons.push(`Family office / growth — compatible with VC deal (+14)`); }
    else             { typeScore = 8;  reasons.push(`Type uncertain fit for VC deal: ${investor.investor_type} (+8)`); }
  } else {
    // Generic / unknown deal type
    const matchTypes = (dealInfo.ideal_investor_types || []).map(t => t.toLowerCase());
    const hit = matchTypes.some(t => invTypeText.includes(t));
    typeScore = hit ? 18 : (invType ? 10 : 8);
    reasons.push(`Type: ${investor.investor_type || 'Unknown'} (+${typeScore})`);
  }
  score += typeScore;

  // ── 2. DEAL / CHECK SIZE PRECISION (22 pts) ──────────────────────────────
  let sizeScore = 0;
  if (prefInvMin != null && prefInvMax != null && equity > 0) {
    if (equity >= prefInvMin && equity <= prefInvMax)             { sizeScore = 22; reasons.push(`Equity $${equity}M in sweet spot $${prefInvMin}M–$${prefInvMax}M (+22)`); }
    else if (equity >= prefInvMin * 0.4 && equity <= prefInvMax * 2.5) { sizeScore = 14; reasons.push(`Equity $${equity}M near preferred range (+14)`); }
    else if (equity < prefInvMin * 0.15 || equity > prefInvMax * 6)    { sizeScore = 0;  reasons.push(`Size mismatch: $${equity}M vs $${prefInvMin}M–$${prefInvMax}M (+0)`); }
    else                                                          { sizeScore = 6;  reasons.push(`Equity $${equity}M outside but not far from range (+6)`); }
  } else if (prefDealMin != null && prefDealMax != null && ev > 0) {
    if (ev >= prefDealMin && ev <= prefDealMax)                   { sizeScore = 22; reasons.push(`EV $${ev}M in deal size range $${prefDealMin}M–$${prefDealMax}M (+22)`); }
    else if (ev >= prefDealMin * 0.4 && ev <= prefDealMax * 2.5) { sizeScore = 14; reasons.push(`EV $${ev}M near preferred range (+14)`); }
    else if (ev < prefDealMin * 0.15 || ev > prefDealMax * 6)    { sizeScore = 0;  reasons.push(`Size mismatch: EV $${ev}M vs $${prefDealMin}M–$${prefDealMax}M (+0)`); }
    else                                                          { sizeScore = 6;  reasons.push(`EV $${ev}M outside but not far from deal size range (+6)`); }
  } else if (prefEbitdaMin != null && prefEbitdaMax != null && ebitda > 0) {
    if (ebitda >= prefEbitdaMin && ebitda <= prefEbitdaMax)       { sizeScore = 22; reasons.push(`EBITDA $${ebitda}M in preferred EBITDA range (+22)`); }
    else if (ebitda >= prefEbitdaMin * 0.4 && ebitda <= prefEbitdaMax * 2.5) { sizeScore = 13; reasons.push(`EBITDA $${ebitda}M near preferred range (+13)`); }
    else                                                          { sizeScore = 3;  reasons.push(`EBITDA $${ebitda}M outside preferred EBITDA range (+3)`); }
  } else if (invAUM > 0 && equity > 0) {
    // AUM inference: typical PE/FO deploys 2–15% of AUM per deal
    const estMin = invAUM * 0.01, estMax = invAUM * 0.15;
    if (equity >= estMin && equity <= estMax)             { sizeScore = 13; reasons.push(`Deal size plausible vs AUM $${invAUM}M (est. $${Math.round(estMin)}M–$${Math.round(estMax)}M) (+13)`); }
    else if (equity < estMin * 0.05 || equity > estMax * 6) { sizeScore = 1; reasons.push(`Deal size likely misaligned vs AUM $${invAUM}M (+1)`); }
    else                                                  { sizeScore = 7; reasons.push(`Deal size loosely consistent with AUM $${invAUM}M (+7)`); }
  } else {
    sizeScore = 9; // no size data — neutral
    reasons.push('No size preference data — neutral (+9)');
  }
  score += sizeScore;

  // ── 3. SECTOR / THESIS DEPTH (24 pts) ────────────────────────────────────
  // Sector synonym expansion — mirrors how a placement agent expands the comp set
  const sectorSynonyms = {
    healthcare:           ['health', 'medical', 'pharma', 'pharmaceutical', 'biotech', 'life sciences',
                           'clinical', 'hospital', 'therapeutics', 'diagnostics', 'dental', 'veterinary',
                           'wellness', 'medtech', 'healthtech'],
    technology:           ['tech', 'software', 'saas', 'cloud', 'digital', 'data', 'information technology',
                           'cybersecurity', 'artificial intelligence', 'fintech', 'edtech', 'proptech',
                           'b2b software', 'enterprise software'],
    manufacturing:        ['industrial', 'factory', 'production', 'fabrication', 'machinery', 'aerospace',
                           'defense', 'contract manufacturing', 'precision'],
    distribution:         ['logistics', 'supply chain', 'warehouse', 'fulfillment', 'transport', 'freight',
                           'shipping', 'last-mile', '3pl'],
    'business services':  ['b2b', 'professional services', 'consulting', 'staffing', 'outsourcing', 'facility',
                           'managed services', 'hr services', 'marketing services', 'bpo', 'facilities management'],
    'financial services': ['fintech', 'insurance', 'banking', 'asset management', 'wealth management',
                           'lending', 'payments', 'insurtech'],
    'real estate':        ['property', 'reit', 'commercial real estate', 'residential', 'multifamily',
                           'industrial real estate', 'office', 'retail property'],
    consumer:             ['retail', 'ecommerce', 'brand', 'cpg', 'food', 'beverage', 'restaurant',
                           'hospitality', 'consumer products', 'd2c'],
    energy:               ['oil', 'gas', 'renewables', 'cleantech', 'utilities', 'power', 'solar', 'wind',
                           'energy services', 'midstream'],
    education:            ['edtech', 'training', 'learning', 'higher education', 'k-12', 'workforce'],
    'media & entertainment': ['media', 'entertainment', 'content', 'streaming', 'gaming', 'sports'],
  };

  const primarySector = sector.split(/[\/,]+/)[0].trim();
  const expandedTerms = new Set();
  expandedTerms.add(primarySector);
  sector.split(/[\/,;]+/).map(s => s.trim()).filter(Boolean).forEach(s => expandedTerms.add(s));
  for (const [key, synonyms] of Object.entries(sectorSynonyms)) {
    if (primarySector.includes(key) || key.includes(primarySector) ||
        synonyms.some(s => primarySector.includes(s))) {
      synonyms.forEach(s => expandedTerms.add(s));
    }
  }
  const relevantTerms = [...expandedTerms].filter(t => t.length > 3);

  const industryHits  = relevantTerms.filter(t => invIndustries.includes(t));
  const verticalHits  = relevantTerms.filter(t => invVerticals.includes(t));
  const descHits      = relevantTerms.filter(t => invDesc.includes(t));
  const otherPrefHits = relevantTerms.filter(t => invOtherPrefs.includes(t));
  const hasIndustryData = !!(invIndustries || invVerticals);

  // Sub-sector specificity: investor explicitly focuses on the sub-sector
  const subSectorHit = subSector && subSector.length > 3 && (
    invIndustries.includes(subSector) || invVerticals.includes(subSector) || invDesc.includes(subSector)
  );

  let sectorScore = 0;
  if (industryHits.length >= 2 || (industryHits.length >= 1 && verticalHits.length >= 1)) {
    sectorScore = 24;
    reasons.push(`Strong sector match: ${[...new Set([...industryHits, ...verticalHits])].slice(0,3).join(', ')} (+24)`);
  } else if (industryHits.length === 1) {
    sectorScore = 17;
    reasons.push(`Sector match in preferred industries: ${industryHits[0]} (+17)`);
  } else if (verticalHits.length >= 1) {
    sectorScore = 14;
    reasons.push(`Sector match in preferred verticals: ${verticalHits[0]} (+14)`);
  } else if (descHits.length >= 2) {
    sectorScore = 11;
    reasons.push(`Sector in description: ${descHits.slice(0,2).join(', ')} (+11)`);
  } else if (descHits.length === 1 || otherPrefHits.length >= 1) {
    sectorScore = 6;
    reasons.push(`Weak sector signal: ${descHits[0] || otherPrefHits[0]} (+6)`);
  } else if (!hasIndustryData && !invDesc) {
    sectorScore = 7;
    reasons.push('No sector preference data — neutral (+7)');
  } else {
    sectorScore = 0;
    reasons.push(`No ${primarySector} sector alignment found (+0)`);
  }
  if (subSectorHit && sectorScore > 0 && sectorScore < 24) {
    sectorScore = Math.min(sectorScore + 3, 24);
    reasons.push(`Sub-sector precision: "${subSector}" in investor focus (+3)`);
  }
  score += sectorScore;

  // ── 4. GEOGRAPHY (14 pts) ─────────────────────────────────────────────────
  const geoAliases = {
    'united states': ['united states', 'north america', 'americas', 'us', 'usa', 'u.s.'],
    us:              ['united states', 'north america', 'americas', 'us', 'usa', 'u.s.'],
    usa:             ['united states', 'north america', 'americas', 'us', 'usa'],
    'north america': ['north america', 'united states', 'canada', 'americas', 'us', 'usa'],
    'united kingdom':['united kingdom', 'uk', 'great britain', 'europe', 'western europe'],
    uk:              ['united kingdom', 'uk', 'great britain', 'europe', 'western europe'],
    europe:          ['europe', 'western europe', 'european union', 'uk', 'dach', 'benelux'],
    uae:             ['uae', 'united arab emirates', 'middle east', 'mena', 'gcc'],
    'middle east':   ['middle east', 'mena', 'gcc', 'uae', 'saudi', 'gulf'],
    global:          [],
  };

  const targetGeos = geo.split(',').map(v => v.trim()).filter(Boolean);
  let geoScore = 0;
  let geoMatch = false;

  if (targetGeos.includes('global') || !targetGeos.length) {
    geoScore = 10; geoMatch = true;
    reasons.push('Global mandate — any geography accepted (+10)');
  } else {
    const matchTerms = new Set(targetGeos.flatMap(g => geoAliases[g] || [g]));
    const hqMatch   = invCountry && [...matchTerms].some(t => invCountry.includes(t));
    const regMatch  = invRegion  && [...matchTerms].some(t => invRegion.includes(t));
    const prefMatch = invPrefGeo && ([...matchTerms].some(t => invPrefGeo.includes(t)) ||
      invPrefGeo.includes('global') || invPrefGeo.includes('worldwide'));

    geoMatch = hqMatch || regMatch || prefMatch;

    if (hqMatch)            { geoScore = 14; reasons.push(`HQ in target region: ${investor.hq_country} (+14)`); }
    else if (prefMatch)     { geoScore = 11; reasons.push(`Preferred geo covers target: ${(investor.preferred_geographies || '').slice(0,60)} (+11)`); }
    else if (regMatch)      { geoScore = 7;  reasons.push(`Region overlaps target: ${investor.hq_region} (+7)`); }
    else if (!invCountry && !invPrefGeo) { geoScore = 5; reasons.push('Geography unknown — neutral (+5)'); }
    else                    { geoScore = 0;  reasons.push(`Geographic mismatch: ${investor.hq_country || 'Unknown'} vs ${geo} (+0)`); }
  }
  score += geoScore;

  // ── 5. ACTIVITY VELOCITY (8 pts) ─────────────────────────────────────────
  const annualised2y = inv2y > 0 ? inv2y / 2 : 0;
  const velocity     = inv12mo > 0 && annualised2y > 0 ? inv12mo / annualised2y : null;

  let activityScore = 0;
  if (inv7d > 0) {
    activityScore = 8;
    reasons.push(`Just invested (${inv7d} deal(s) in last 7 days) — peak deployment (+8)`);
  } else if (inv12mo >= 5 && velocity && velocity >= 1.2) {
    activityScore = 8;
    reasons.push(`Very active + accelerating: ${inv12mo} deals/12m (${velocity.toFixed(1)}x pace vs prior year) (+8)`);
  } else if (inv12mo >= 5) {
    activityScore = 6;
    reasons.push(`Very active: ${inv12mo} investments in last 12 months (+6)`);
  } else if (inv12mo >= 2 && velocity && velocity >= 1.0) {
    activityScore = 5;
    reasons.push(`Active at steady pace: ${inv12mo} deals/12m (+5)`);
  } else if (inv12mo >= 2) {
    activityScore = 3;
    reasons.push(`Active: ${inv12mo} investments in last 12 months (+3)`);
  } else if (inv12mo >= 1 || inv6mo >= 1) {
    activityScore = 2;
    reasons.push(`Some recent activity — ${inv12mo || inv6mo} deal(s) in recent period (+2)`);
  } else {
    reasons.push('No recent investment activity (+0)');
  }
  score += activityScore;

  // ── 6. FUND TIMING & DRY POWDER (7 pts) ──────────────────────────────────
  // Placement agent insight: recently-closed fund = actively deploying capital.
  // Dry powder confirmed = money available NOW. This is the pacing cycle signal.
  const currentYear = new Date().getFullYear();
  const vintageAge  = fundVintage > 1990 ? currentYear - fundVintage : null;

  let timingScore = 0;
  if (dryPowder > 0 && vintageAge !== null && vintageAge <= 3) {
    timingScore = 7;
    reasons.push(`Recent fund (${fundVintage}) + $${Math.round(dryPowder)}M dry powder — prime deployment window (+7)`);
  } else if (dryPowder > 0 && vintageAge !== null && vintageAge <= 5) {
    timingScore = 6;
    reasons.push(`Fund (${fundVintage}) with $${Math.round(dryPowder)}M dry powder — still deploying (+6)`);
  } else if (dryPowder > 0) {
    timingScore = 5;
    reasons.push(`$${Math.round(dryPowder)}M dry powder — capital available to deploy (+5)`);
  } else if (vintageAge !== null && vintageAge <= 2) {
    timingScore = 6;
    reasons.push(`Very recent fund close (${fundVintage}) — early deployment window (+6)`);
  } else if (vintageAge !== null && vintageAge <= 4) {
    timingScore = 4;
    reasons.push(`Recent fund vintage (${fundVintage}) — within deployment period (+4)`);
  } else if (fundsOpen > 0) {
    timingScore = 4;
    reasons.push(`${fundsOpen} open fund(s) — active deployment (+4)`);
  } else if (vintageAge !== null && vintageAge <= 7) {
    timingScore = 1;
    reasons.push(`Fund vintage ${fundVintage} — may be in late deployment/harvesting (+1)`);
  }
  score += timingScore;

  return {
    deterministicScore: Math.round(Math.min(score, 100)),
    scoring_breakdown: reasons,
    geo_match: geoMatch,
  };
}

// ─── helper: normalise large dollar amounts stored as full dollars → millions ─
function normToM(val) {
  const n = parseFloat(val) || 0;
  return n > 10_000 ? n / 1_000_000 : n;
}

function isSparseInvestorForScoring(investor) {
  const richText = [
    investor?.description,
    investor?.thesis,
    investor?.research_notes,
    investor?.other_preferences,
  ].filter(Boolean).join(' ').trim();

  const structuredSignals = [
    investor?.investor_type,
    investor?.preferred_industries,
    investor?.preferred_verticals,
    investor?.preferred_geographies,
    investor?.preferred_investment_types,
    investor?.preferred_deal_size_min,
    investor?.preferred_deal_size_max,
    investor?.preferred_investment_amount_min,
    investor?.preferred_investment_amount_max,
    investor?.aum_millions,
    investor?.dry_powder_millions,
    investor?.investments_last_12m,
    investor?.last_investment_date,
  ].filter(v => v != null && String(v).trim() !== '').length;

  const hasHistory = Array.isArray(investor?.past_investments) && investor.past_investments.length > 0;
  const alreadyResearched = !!(investor?.person_researched || investor?.last_researched_at);

  if (alreadyResearched || hasHistory) return false;
  return richText.length < 120 || structuredSignals < 4;
}

function mergeGapFillIntoInvestor(investor, gapFill) {
  if (!gapFill) return investor;
  return {
    ...investor,
    investor_type: investor.investor_type || gapFill.investor_type || gapFill.firm_type || null,
    description: investor.description || gapFill.description || gapFill.thesis || null,
    thesis: investor.thesis || gapFill.thesis || null,
    research_notes: investor.research_notes || gapFill.justification || null,
    preferred_industries: investor.preferred_industries || gapFill.sector_focus || null,
    preferred_geographies: investor.preferred_geographies || gapFill.geography_focus || null,
    aum_millions: investor.aum_millions || normToM(gapFill.aum) || null,
    website: investor.website || gapFill.website || null,
    hq_country: investor.hq_country || gapFill.country || null,
    past_investments: (Array.isArray(investor.past_investments) && investor.past_investments.length)
      ? investor.past_investments
      : (gapFill.past_investments || []),
    last_researched_at: investor.last_researched_at || new Date().toISOString(),
  };
}

// ─── main export ──────────────────────────────────────────────────────────────
export async function queryInvestorDatabase(dealInfo, deal) {
  console.log(`[DB QUERY] Full-universe scan for: ${dealInfo.deal_name || deal?.name}`);

  const supabase = getSupabase();
  if (!supabase) throw new Error('Supabase not available');

  const threshold = deal?.min_investor_score || 50;
  const flags = getDealTypeFlags(dealInfo);

  // ── Phase 1: Load intelligence boosts FIRST ──────────────────────────────
  // Comparable deal history is the strongest investor signal (mirrors comp-fund analysis)
  const boostMap = new Map(); // investor name (lowercased) → boost score
  if (deal?.id) {
    const { data: boosts } = await supabase.from('deal_investor_scores')
      .select('investor_name, intelligence_boost, times_backed_similar, backed_companies')
      .eq('deal_id', deal.id)
      .gt('intelligence_boost', 0);
    for (const row of (boosts || [])) {
      const key = String(row.investor_name || '').toLowerCase().trim();
      if (key) boostMap.set(key, {
        boost: Number(row.intelligence_boost || 0),
        times: Number(row.times_backed_similar || 0),
        companies: row.backed_companies || [],
      });
    }
    console.log(`[DB QUERY] Intelligence boosts loaded: ${boostMap.size} investors with comp-deal history`);
  }

  const getBoost = (name) => {
    const key = String(name || '').toLowerCase().trim();
    // Exact match
    if (boostMap.has(key)) return boostMap.get(key);
    // First-token fuzzy match (e.g. "Blackstone Capital" matches "Blackstone")
    const firstToken = key.split(/\s+/)[0];
    if (firstToken && firstToken.length > 3) {
      for (const [k, v] of boostMap.entries()) {
        if (k.includes(firstToken) || firstToken.includes(k.split(/\s+/)[0])) return v;
      }
    }
    return null;
  };

  // ── Phase 2: Full universe scan (paginated, no arbitrary cap) ────────────
  // Load ALL investors and apply hard filters — same as a placement agent sweeping Preqin
  const universe = new Map(); // pitchbook_id → record
  const PAGE = 1000;
  let from = 0;
  let totalLoaded = 0;

  while (true) {
    const { data: page, error: pageErr } = await supabase.from('investors_db')
      .select('*')
      .range(from, from + PAGE - 1)
      .order('investments_last_12m', { ascending: false, nullsFirst: false });

    if (pageErr || !page?.length) break;

    for (const investor of page) {
      const key = investor.pitchbook_id || investor.id || `${investor.name}-${from}`;
      if (universe.has(key)) continue; // de-duplicate
      if (!isHardExcluded(investor, dealInfo, flags)) {
        universe.set(key, investor);
      }
    }
    totalLoaded += page.length;
    if (page.length < PAGE) break;
    from += PAGE;
  }

  console.log(`[DB QUERY] Universe loaded: ${totalLoaded} total → ${universe.size} after hard filters`);

  // ── Phase 3: Deterministic scoring across full universe ──────────────────
  const scored = [];
  for (const investor of universe.values()) {
    const { deterministicScore, scoring_breakdown, geo_match } = scoreDeterministic(investor, dealInfo);
    const boostData = getBoost(investor.name || investor.firm_name || '');
    const intelligenceBoost = boostData ? boostData.boost : 0;
    const finalScore = Math.round(Math.min(deterministicScore + intelligenceBoost, 100));

    scored.push({
      ...investor,
      _det_score: deterministicScore,
      _intelligence_boost: intelligenceBoost,
      _backed_similar: boostData?.times || 0,
      _backed_companies: boostData?.companies || [],
      score: finalScore,
      scoring_breakdown,
      geo_match,
    });
  }

  // Sort: primary = finalScore, secondary = recency (7d > 12m > 2y), tertiary = dry powder
  scored.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    const recencyA = (Number(a.investments_last_7d) || 0) * 52 + (Number(a.investments_last_12m) || 0);
    const recencyB = (Number(b.investments_last_7d) || 0) * 52 + (Number(b.investments_last_12m) || 0);
    if (recencyB !== recencyA) return recencyB - recencyA;
    return (parseFloat(b.dry_powder_millions) || 0) - (parseFloat(a.dry_powder_millions) || 0);
  });

  const aboveThreshold = scored.filter(s => s.score >= threshold);
  console.log(`[DB QUERY] Scored: ${scored.length} investors — ${aboveThreshold.length} at/above threshold ${threshold}`);

  // Take top 300 for AI batch scoring (AI refines; we pre-filter intelligently)
  const forAI = aboveThreshold.slice(0, 300);

  if (!forAI.length) {
    // Fallback: take top 100 regardless of threshold
    return scored.slice(0, 100);
  }

  // ── Phase 4: AI batch refinement ─────────────────────────────────────────
  const aiRefined = await batchScoreInvestors(forAI, dealInfo, deal);
  const disqualified = (dealInfo.disqualified_investor_types || []).map(t => t.toLowerCase());

  const result = aiRefined
    .filter(s => {
      if (Number(s.score) < threshold) return false;
      if (disqualified.some(d => (s.investor_type || '').toLowerCase().includes(d))) return false;
      return true;
    })
    .sort((a, b) => Number(b.score) - Number(a.score))
    .slice(0, 150);

  console.log(`[DB QUERY] Final shortlist: ${result.length} investors`);
  return result;
}

// Keep legacy export name for callers outside this module
export { batchScoreInvestors as batchScoreWithKimi };

export async function batchScoreInvestors(investors, dealInfo, deal = null) {
  const results = [];
  const BATCH = 20;
  const { isISSponsor, isBuyout, isVC } = getDealTypeFlags(dealInfo);

  // Build deal-type specific scoring criteria (professional-grade rubric)
  let scoringCriteria;
  if (isISSponsor) {
    scoringCriteria = `SCORING (0-100) — INDEPENDENT SPONSOR / CO-INVESTMENT DEAL:
- Mandate fit (35pts): Direct buyout/co-invest/LMM PE mandate? Family office with direct deal programme? Score 0 if pure VC/venture/early-stage.
- Deal size alignment (30pts): Does their typical equity ticket match $${dealInfo.equity_required_usd_m || '?'}M? Does EV of $${dealInfo.enterprise_value_usd_m || '?'}M fit their sweet spot?
- Sector/thesis match (25pts): Active in ${dealInfo.sector}${dealInfo.sub_sector ? ` / ${dealInfo.sub_sector}` : ''}? Portfolio companies in adjacent sectors?
- Geography (10pts): Active in ${dealInfo.geography || 'US'}?
Archive if: pure VC/venture/early-stage; mega-PE ($10B+ AUM) whose minimum far exceeds this deal; investor status inactive.`;
  } else if (isBuyout) {
    scoringCriteria = `SCORING (0-100) — BUYOUT / PE DEAL:
- Mandate fit (35pts): PE fund, family office direct, growth equity with buyout appetite?
- Deal size alignment (30pts): Does their range match EV $${dealInfo.enterprise_value_usd_m || '?'}M / equity $${dealInfo.equity_required_usd_m || '?'}M?
- Sector/thesis match (25pts): Active in ${dealInfo.sector}${dealInfo.sub_sector ? ` / ${dealInfo.sub_sector}` : ''}?
- Geography (10pts): Active in ${dealInfo.geography || 'US'}?
Archive if: pure VC/angel/accelerator; deal size clearly out of range; status inactive.`;
  } else {
    scoringCriteria = `SCORING (0-100):
- Mandate/strategy match (30pts): Fund strategies align with ${dealInfo.sector}? Stage appropriate?
- Cheque size match (25pts): Is deal target in their typical range? Unknown = 12pts.
- Sector/thesis depth (25pts): Active in ${dealInfo.sector}? Related portfolio companies?
- Geography (10pts): Active in ${dealInfo.geography || 'US/UK'}? Unknown = 5pts.
- Activity/pacing (10pts): Recently active? Recent fund close or dry powder?`;
  }

  const deterministicFallback = (inv) => {
    const det = Number.isFinite(Number(inv._det_score))
      ? Number(inv._det_score)
      : scoreDeterministic(inv, dealInfo).deterministicScore;
    const score = Math.max(0, Math.min(100, Math.round(det)));
    return {
      ...inv,
      _det_score: det,
      score,
      grade: score >= 65 ? 'Warm' : score >= 45 ? 'Possible' : 'Archive',
      score_reason: 'AI scoring unavailable - using deterministic score',
    };
  };

  for (let i = 0; i < investors.length; i += BATCH) {
    const rawBatch = investors.slice(i, i + BATCH);
    const batch = [];

    for (const investor of rawBatch) {
      if (!isSparseInvestorForScoring(investor)) {
        batch.push(investor);
        continue;
      }

      try {
        const gapFill = await researchFirmOnly(investor, {
          ...dealInfo,
          ...(deal || {}),
          id: deal?.id || null,
          name: deal?.name || dealInfo.deal_name || 'Deal',
          sector: deal?.sector || dealInfo.sector || null,
          geography: deal?.geography || dealInfo.geography || dealInfo.hq_location || null,
        });
        batch.push(mergeGapFillIntoInvestor(investor, gapFill));
      } catch (err) {
        console.warn(`[BATCH SCORE] Sparse-profile research failed for ${investor?.name || investor?.firm_name || 'investor'}:`, err.message);
        batch.push(investor);
      }
    }

    const summaries = batch.map((inv, idx) => {
      const currentYear = new Date().getFullYear();
      const vintageAge  = parseInt(inv.last_closed_fund_vintage) > 1990
        ? currentYear - parseInt(inv.last_closed_fund_vintage) : null;
      const inv12mo = Number(inv.investments_last_12m) || 0;
      const inv2y   = Number(inv.investments_last_2y) || 0;
      const velocity = inv12mo > 0 && inv2y > 0 ? (inv12mo / (inv2y / 2)).toFixed(1) : null;

      return {
        idx,
        name: inv.name,
        type: inv.investor_type || 'Unknown',
        aum_m: inv.aum_millions,
        country: inv.hq_country,
        industries: (inv.preferred_industries || '').substring(0, 120),
        verticals: (inv.preferred_verticals || '').substring(0, 80),
        deal_size: inv.preferred_deal_size_min && inv.preferred_deal_size_max
          ? `$${inv.preferred_deal_size_min}M-$${inv.preferred_deal_size_max}M` : null,
        ebitda_range: inv.preferred_ebitda_min && inv.preferred_ebitda_max
          ? `$${inv.preferred_ebitda_min}M-$${inv.preferred_ebitda_max}M` : null,
        equity_range: inv.preferred_investment_amount_min && inv.preferred_investment_amount_max
          ? `$${inv.preferred_investment_amount_min}M-$${inv.preferred_investment_amount_max}M` : null,
        investments_12m: inv12mo,
        velocity: velocity ? `${velocity}x pace vs prior year` : null,
        dry_powder_m: parseFloat(inv.dry_powder_millions) || null,
        fund_vintage: inv.last_closed_fund_vintage || null,
        fund_age_yrs: vintageAge,
        funds_open: parseInt(inv.num_funds_open) || null,
        status: inv.investor_status,
        backed_similar: inv._backed_similar > 0
          ? `Backed ${inv._backed_similar} comparable deal(s): ${(inv._backed_companies || []).slice(0,3).join(', ')}`
          : null,
        det_score: inv._det_score,
      };
    });

    const prompt = `You are a senior placement agent scoring investor fit for a fundraise. Return ONLY a JSON array.

DEAL:
Type: ${dealInfo.deal_type}
Sector: ${dealInfo.sector}${dealInfo.sub_sector ? ` / ${dealInfo.sub_sector}` : ''}
Geography: ${dealInfo.geography || dealInfo.hq_location || 'United States'}
EBITDA: $${dealInfo.ebitda_usd_m || 'Unknown'}M
Enterprise Value: $${dealInfo.enterprise_value_usd_m || 'Unknown'}M
Equity Required: $${dealInfo.equity_required_usd_m || 'Unknown'}M
Ideal investor profile: ${(dealInfo.ideal_investor_profile || '').substring(0, 200)}

INVESTORS:
${JSON.stringify(summaries, null, 1)}

${scoringCriteria}

Key signals to weight heavily:
- "backed_similar" = investor backed comparable deals → very strong thesis alignment signal
- "dry_powder_m" > 0 = actively deploying capital NOW → strong timing signal
- "fund_age_yrs" <= 3 = recently closed fund, in deployment window → strong timing signal
- "velocity" > 1.0 = accelerating deal pace → strong activity signal
- "det_score" = pre-computed deterministic score for reference (do not just echo it)

Return ONLY: [{"idx":0,"score":75,"grade":"Warm","reason":"one sentence max"}, ...]
Grades: Hot=85+, Warm=65-84, Possible=45-64, Archive=0-44`;

    try {
      const text = await haikuComplete(prompt, { maxTokens: 500 });
      const match = text.replace(/```json|```/g, '').trim().match(/\[[\s\S]*\]/);
      if (!match) throw new Error('AI scoring returned no JSON array');
      const scores = JSON.parse(match[0]);
      const scoredIndexes = new Set();
      scores.forEach(s => {
        const inv = batch[s.idx];
        if (inv) {
          scoredIndexes.add(Number(s.idx));
          results.push({
            ...deterministicFallback(inv),
            score: Math.min(100, Math.max(0, Number(s.score) || 0)),
            grade: s.grade || 'Possible',
            score_reason: s.reason || '',
          });
        }
      });
      batch.forEach((inv, idx) => {
        if (!scoredIndexes.has(idx)) results.push(deterministicFallback(inv));
      });
    } catch (err) {
      console.warn('[BATCH SCORE] Error:', err.message);
      // Fall back to deterministic score on AI failure
      batch.forEach(inv => results.push(deterministicFallback(inv)));
    }
  }

  return results;
}
