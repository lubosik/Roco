// core/investorDatabaseQuery.js
import { getSupabase } from './supabase.js';
import { haikuComplete } from './aiClient.js';

// Derive deal structure flags from dealInfo
function getDealTypeFlags(dealInfo) {
  const t = (dealInfo.deal_type || '').toLowerCase();
  const isISSponsor = /independent.sponsor|fundless|co.invest/i.test(t);
  const isBuyout    = isISSponsor || /buyout|private.equity|lbo/i.test(t);
  const isVC        = /venture|seed|series|startup|growth.equity/i.test(t) && !isBuyout;
  return { isISSponsor, isBuyout, isVC };
}

// Investor types to actively exclude from pool for PE/IS deals
const VC_ONLY_TYPES = ['Venture Capital', 'Angel', 'Accelerator', 'Incubator', 'Corporate Venture'];

export async function queryInvestorDatabase(dealInfo, deal) {
  console.log(`[DB QUERY] Querying for: ${dealInfo.deal_name}`);

  const supabase = getSupabase();
  if (!supabase) throw new Error('Supabase not available');

  const threshold = deal.min_investor_score || 60;
  const { isISSponsor, isBuyout, isVC } = getDealTypeFlags(dealInfo);
  const pool = new Map(); // pitchbook_id → record

  // ── Pass 1: Geography match ──
  const geo = (dealInfo.geography || '') + ' ' + (dealInfo.hq_location || '');
  const isUS = /united states|usa|rhode island|new york|california|texas/i.test(geo);

  if (isUS) {
    const { data } = await supabase.from('investors_db')
      .select('*')
      .or('preferred_geographies.ilike.%United States%,preferred_geographies.ilike.%North America%,preferred_geographies.is.null')
      .order('investments_last_12m', { ascending: false, nullsFirst: false })
      .limit(1000);
    (data || []).forEach(r => pool.set(r.pitchbook_id, r));
  }

  // ── Pass 2: EBITDA range match (PE/buyout deals) ──
  if (dealInfo.ebitda_usd_m) {
    const ebitda = dealInfo.ebitda_usd_m;
    const { data } = await supabase.from('investors_db')
      .select('*')
      .or(`preferred_ebitda_max.gte.${ebitda * 0.5},preferred_ebitda_max.is.null`)
      .or(`preferred_ebitda_min.lte.${ebitda * 2},preferred_ebitda_min.is.null`)
      .order('investments_last_12m', { ascending: false, nullsFirst: false })
      .limit(500);
    (data || []).forEach(r => pool.set(r.pitchbook_id, r));
  }

  // ── Pass 3: Deal size / EV match ──
  if (dealInfo.enterprise_value_usd_m) {
    const ev = dealInfo.enterprise_value_usd_m;
    const { data } = await supabase.from('investors_db')
      .select('*')
      .or(`preferred_deal_size_max.gte.${ev * 0.3},preferred_deal_size_max.is.null`)
      .or(`preferred_deal_size_min.lte.${ev * 3},preferred_deal_size_min.is.null`)
      .order('investments_last_12m', { ascending: false, nullsFirst: false })
      .limit(400);
    (data || []).forEach(r => pool.set(r.pitchbook_id, r));
  }

  // ── Pass 4: Investor type match — targeted to deal structure ──
  const typeKeywords = [];
  if (isBuyout) {
    // IS/buyout: PE firms, family offices with direct invest, co-investors
    typeKeywords.push('Buyout', 'Private Equity', 'Family Office', 'Independent Sponsor', 'Fundless Sponsor', 'Growth Equity');
  } else if (isVC) {
    typeKeywords.push('Venture Capital', 'Angel', 'Family Office', 'Growth');
  } else {
    // Derive from dealInfo
    (dealInfo.ideal_investor_types || []).forEach(t => {
      if (/buyout|pe\b/i.test(t))     typeKeywords.push('Buyout', 'Private Equity');
      if (/family/i.test(t))           typeKeywords.push('Family Office');
      if (/independent|fundless/i.test(t)) typeKeywords.push('Independent Sponsor', 'Fundless Sponsor');
      if (/venture|vc/i.test(t))       typeKeywords.push('Venture Capital');
      if (/growth/i.test(t))           typeKeywords.push('Growth');
    });
  }

  for (const keyword of [...new Set(typeKeywords)].slice(0, 6)) {
    const { data } = await supabase.from('investors_db')
      .select('*')
      .ilike('investor_type', `%${keyword}%`)
      .order('investments_last_12m', { ascending: false, nullsFirst: false })
      .limit(300);
    (data || []).forEach(r => pool.set(r.pitchbook_id, r));
  }

  // ── Pass 5: Sector keyword match ──
  if (dealInfo.sector && dealInfo.sector !== 'Unknown') {
    const sectorKeyword = dealInfo.sector.split('/')[0].trim();
    const { data } = await supabase.from('investors_db')
      .select('*')
      .ilike('preferred_industries', `%${sectorKeyword}%`)
      .order('investments_last_12m', { ascending: false, nullsFirst: false })
      .limit(300);
    (data || []).forEach(r => pool.set(r.pitchbook_id, r));
  }

  // ── Fallback: active investors in same sector (NOT generic most-active) ──
  if (pool.size < 200) {
    const sectorFallback = dealInfo.sector && dealInfo.sector !== 'Unknown'
      ? dealInfo.sector.split('/')[0].trim() : null;
    const q = supabase.from('investors_db')
      .select('*')
      .not('investments_last_12m', 'is', null)
      .gte('investments_last_12m', 1)
      .order('investments_last_12m', { ascending: false })
      .limit(400);
    const { data } = sectorFallback
      ? await q.ilike('preferred_industries', `%${sectorFallback}%`)
      : await q;
    (data || []).forEach(r => pool.set(r.pitchbook_id, r));
  }

  // ── Remove clearly wrong investor types for PE/IS deals ──
  let allCandidates = Array.from(pool.values());
  if (isBuyout) {
    allCandidates = allCandidates.filter(r => {
      const type = (r.investor_type || '').toLowerCase();
      return !VC_ONLY_TYPES.some(bad => type.includes(bad.toLowerCase()));
    });
  }

  console.log(`[DB QUERY] Pool: ${allCandidates.length} candidates`);

  // ── Batch score ──
  const scored = await batchScoreInvestors(allCandidates, dealInfo);

  // ── Filter disqualified types ──
  const disqualified = (dealInfo.disqualified_investor_types || []).map(t => t.toLowerCase());
  const shortlisted = scored
    .filter(s => {
      if (s.score < threshold) return false;
      if (disqualified.some(d => (s.investor_type || '').toLowerCase().includes(d))) return false;
      return true;
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, 150);

  console.log(`[DB QUERY] Shortlisted: ${shortlisted.length} above score ${threshold}`);
  return shortlisted;
}

// Keep legacy export name for callers outside this module
export { batchScoreInvestors as batchScoreWithKimi };

export async function batchScoreInvestors(investors, dealInfo) {
  const results = [];
  const BATCH = 20;
  const { isISSponsor, isBuyout, isVC } = getDealTypeFlags(dealInfo);

  // Build deal-type specific scoring criteria
  let scoringCriteria;
  if (isISSponsor) {
    scoringCriteria = `Scoring (0-100) for an INDEPENDENT SPONSOR / CO-INVESTMENT deal:
- Investor type fit (35pts): Do they do direct buyouts, co-investments, or LMM PE? Family offices with direct deal mandates? Independent sponsors? Award 0 if pure VC/early-stage/venture-only.
- Deal size fit (30pts): Does their typical equity ticket match $${dealInfo.equity_required_usd_m || '?'}M? Does EV of $${dealInfo.enterprise_value_usd_m || '?'}M fit their deal size range?
- Sector match (25pts): Do they invest in ${dealInfo.sector}? Direct experience scores highest.
- Geography (10pts): Active in ${dealInfo.geography || 'US'}?
Archive immediately if: pure VC/venture/early-stage with no buyout/co-invest activity; pure biotech drug discovery; mega-PE ($10B+ AUM only doing $500M+ deals.`;
  } else if (isBuyout) {
    scoringCriteria = `Scoring (0-100) for a BUYOUT / PE deal:
- Investor type fit (35pts): PE fund, family office direct, growth equity with buyout appetite?
- Deal size fit (30pts): Does their deal size range match EV of $${dealInfo.enterprise_value_usd_m || '?'}M?
- Sector match (25pts): Active in ${dealInfo.sector}?
- Geography (10pts): Active in ${dealInfo.geography || 'US'}?
Archive if: pure VC, angel, or accelerator.`;
  } else {
    scoringCriteria = `Scoring (0-100):
- Investor type match (30pts): Match to ideal types: ${(dealInfo.ideal_investor_types || []).join(', ')}
- Deal/EBITDA size fit (25pts): Does their typical deal size match?
- Sector/industry match (25pts): Active in ${dealInfo.sector}?
- Geography (10pts): Active in ${dealInfo.geography || 'US'}?
- Activity level (10pts): Recent investments in last 12 months?`;
  }

  for (let i = 0; i < investors.length; i += BATCH) {
    const batch = investors.slice(i, i + BATCH);

    const summaries = batch.map((inv, idx) => ({
      idx,
      name: inv.name,
      type: inv.investor_type || 'Unknown',
      aum_m: inv.aum_millions,
      country: inv.hq_country,
      industries: (inv.preferred_industries || '').substring(0, 100),
      deal_size: inv.preferred_deal_size_min && inv.preferred_deal_size_max
        ? `$${inv.preferred_deal_size_min}M-$${inv.preferred_deal_size_max}M` : null,
      ebitda_range: inv.preferred_ebitda_min && inv.preferred_ebitda_max
        ? `$${inv.preferred_ebitda_min}M-$${inv.preferred_ebitda_max}M` : null,
      investments_12m: inv.investments_last_12m,
      status: inv.investor_status,
    }));

    const prompt = `Score each investor's fit for this deal. Return ONLY a JSON array.

DEAL:
Type: ${dealInfo.deal_type}
Sector: ${dealInfo.sector}${dealInfo.sub_sector ? ` / ${dealInfo.sub_sector}` : ''}
Geography: ${dealInfo.geography || dealInfo.hq_location || 'United States'}
EBITDA: $${dealInfo.ebitda_usd_m || 'Unknown'}M
EV: $${dealInfo.enterprise_value_usd_m || 'Unknown'}M
Equity needed: $${dealInfo.equity_required_usd_m || 'Unknown'}M
Ideal investor types: ${(dealInfo.ideal_investor_types || []).join(', ')}
Ideal profile: ${(dealInfo.ideal_investor_profile || '').substring(0, 200)}

INVESTORS:
${JSON.stringify(summaries, null, 1)}

${scoringCriteria}

Return ONLY: [{"idx":0,"score":75,"grade":"Warm","reason":"one sentence"}, ...]
Grades: Hot=85+, Warm=65-84, Possible=45-64, Archive=0-44`;

    try {
      const text = await haikuComplete(prompt, { maxTokens: 2048 });
      const match = text.replace(/```json|```/g, '').trim().match(/\[[\s\S]*\]/);
      if (match) {
        const scores = JSON.parse(match[0]);
        scores.forEach(s => {
          const inv = batch[s.idx];
          if (inv) results.push({
            ...inv,
            score: Math.min(100, Math.max(0, Number(s.score) || 0)),
            grade: s.grade || 'Possible',
            score_reason: s.reason || '',
          });
        });
      }
    } catch (err) {
      console.warn('[BATCH SCORE] Error:', err.message);
      batch.forEach(inv => results.push({ ...inv, score: 0, grade: 'Archive' }));
    }
  }

  return results;
}
