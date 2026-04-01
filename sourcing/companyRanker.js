/**
 * sourcing/companyRanker.js
 * Ranks target companies for a sourcing campaign using GPT.
 * Scores 0-100 and assigns hot/warm/possible/archive tiers.
 */

import { getSupabase } from '../core/supabase.js';
import { pushActivity } from '../dashboard/server.js';
import { aiComplete } from '../core/aiClient.js';

function extractJSON(text) {
  try {
    const clean = (text || '').replace(/```json|```/g, '').trim();
    const match = clean.match(/\[[\s\S]*\]/);
    if (match) return JSON.parse(match[0]);
  } catch {}
  return null;
}

function scoreToTier(score) {
  if (score >= 75) return 'hot';
  if (score >= 50) return 'warm';
  if (score >= 30) return 'possible';
  return 'archive';
}

/**
 * Rank all unranked target_companies for a campaign.
 * Processes in batches of 10.
 */
export async function rankUnrankedCompanies(campaign) {
  const sb = getSupabase();
  if (!sb) return;

  // Also tier companies that have a score from Grok/Gemini but no tier yet
  const { data: scoreButNoTier } = await sb.from('target_companies')
    .select('id, match_score')
    .eq('campaign_id', campaign.id)
    .not('match_score', 'is', null)
    .is('match_tier', null);
  for (const c of (scoreButNoTier || [])) {
    await sb.from('target_companies').update({
      match_tier: scoreToTier(c.match_score),
      updated_at: new Date().toISOString(),
    }).eq('id', c.id);
  }
  if (scoreButNoTier?.length) {
    console.log(`[SOURCING RANK] Applied tiers to ${scoreButNoTier.length} pre-scored companies`);
  }

  const { data: unranked } = await sb.from('target_companies')
    .select('*')
    .eq('campaign_id', campaign.id)
    .is('match_score', null)
    .in('research_status', ['researched', 'contacts_found'])
    .limit(30);

  if (!unranked?.length) return;

  console.log(`[SOURCING RANK] Ranking ${unranked.length} companies for campaign: ${campaign.name}`);
  pushActivity({ type: 'research', action: 'Ranking companies', note: `[${campaign.name}]: Scoring ${unranked.length} target companies` });

  // Process in batches of 10
  for (let i = 0; i < unranked.length; i += 10) {
    const batch = unranked.slice(i, i + 10);
    await rankBatch(batch, campaign);
  }
}

async function rankBatch(companies, campaign) {
  const sb = getSupabase();

  const rankingPrompt = `You are ranking companies for the following investment thesis:

FIRM: ${campaign.firm_name || 'Investment Firm'} (${campaign.firm_type || 'Investment Firm'})
THESIS: ${campaign.investment_thesis || 'Not specified'}
CRITERIA:
- Sector: ${campaign.target_sector}
- Revenue: ${campaign.min_revenue || '?'} to ${campaign.max_revenue || '?'}
- EBITDA: ${campaign.min_ebitda || '?'} to ${campaign.max_ebitda || '?'}
- Geography: ${campaign.target_geography}
- Deal Type: ${campaign.deal_type || 'Any'}
- Business Model: ${campaign.business_model || 'Any'}
- Ownership: ${campaign.ownership_type || 'Any'}

COMPANIES TO RANK:
${JSON.stringify(companies.map(c => ({
  id: c.id,
  company_name: c.company_name,
  sector: c.sector,
  geography: c.geography,
  business_model: c.business_model,
  estimated_revenue: c.estimated_revenue,
  estimated_ebitda: c.estimated_ebitda,
  ownership_type: c.ownership_type,
  funding_stage: c.funding_stage,
  product_description: c.product_description,
  intent_signals_found: c.intent_signals_found,
  why_matches: c.why_matches,
})), null, 2)}

Score each company 0-100:
- Sector and thesis fit (30pts): Does what they do match exactly what the firm invests in?
- Financial fit (25pts): Do estimated revenue/EBITDA/stage match the criteria?
- Geography fit (20pts): Are they in the right market?
- Ownership/stage fit (15pts): Right ownership type and deal stage?
- Intent signal strength (10pts): Are there active signals they may be open to investment?

For each company return:
- company_id (from input — the "id" field, UUID string)
- score (0-100 integer)
- tier: "hot" (75+) | "warm" (50-74) | "possible" (30-49) | "archive" (<30)
- ranking_rationale (2 sentences max — specific, references actual product and financial signals)

Return ONLY a valid JSON array. company_id must be the exact UUID from the input.`;

  try {
    const text = await aiComplete(rankingPrompt, { maxTokens: 2000, task: `rank-companies-${campaign.id}`, reasoning: 'medium' });
    const results = extractJSON(text);

    if (!Array.isArray(results)) {
      console.warn('[SOURCING RANK] Could not parse ranking results');
      // Fall back to using Grok's own match_score
      await applyGrokScores(companies, sb);
      return;
    }

    for (const result of results) {
      const company = companies.find(c => c.id === result.company_id);
      if (!company) continue;

      const score = typeof result.score === 'number' ? Math.min(100, Math.max(0, result.score)) : 50;
      const tier = result.tier || scoreToTier(score);

      await sb.from('target_companies').update({
        match_score: score,
        match_tier: tier,
        why_matches: result.ranking_rationale || company.why_matches,
        updated_at: new Date().toISOString(),
      }).eq('id', company.id).then(null, () => {});

      await sb.from('activity_log').insert({
        event_type: 'COMPANY_RANKED',
        summary: `[${campaign.name}] ${company.company_name} scored ${score} — ${tier}`,
        detail: { score, tier, rationale: result.ranking_rationale },
        created_at: new Date().toISOString(),
      }).then(null, () => {});

      pushActivity({
        type: 'research',
        action: `Ranked: ${company.company_name}`,
        note: `[${campaign.name}]: Score ${score}/100 — ${tier.toUpperCase()}`,
      });
    }
  } catch (err) {
    console.warn('[SOURCING RANK] Ranking failed:', err.message);
    await applyGrokScores(companies, sb);
  }
}

// If AI ranking fails, use the match_score Grok already provided
async function applyGrokScores(companies, sb) {
  for (const company of companies) {
    const score = company.match_score || 50;
    const tier = scoreToTier(score);
    await sb.from('target_companies').update({
      match_score: score,
      match_tier: tier,
      updated_at: new Date().toISOString(),
    }).eq('id', company.id).then(null, () => {});
  }
}
