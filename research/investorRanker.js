/**
 * research/investorRanker.js
 * Scores an investor/LP's fit for a deal using GPT-5.4 (medium reasoning) + Claude fallback.
 * Handles both PitchBook LP CSV imports and Gemini/LinkedIn-sourced contacts.
 * Returns: { score: 0-100, grade: 'Hot'|'Warm'|'Possible'|'Archive', rationale }
 */

import { getScoringContext } from '../core/agentContext.js';
import { pushActivity } from '../dashboard/server.js';

// Haiku 4.5 primary → gpt-5.4-mini-2026-03-17 fallback
async function haikuscore(prompt) {
  const anthropicKey = process.env.ANTHROPIC_API_KEY;
  if (anthropicKey) {
    try {
      const res = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'x-api-key': anthropicKey,
          'anthropic-version': '2023-06-01',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: 'claude-haiku-4-5-20251001',
          max_tokens: 250,
          messages: [{ role: 'user', content: prompt }],
        }),
      });
      if (!res.ok) {
        const t = await res.text();
        throw new Error(`Haiku ${res.status}: ${t.substring(0, 200)}`);
      }
      const data = await res.json();
      const text = data.content?.[0]?.text || '';
      if (text) return text;
      throw new Error('Haiku returned empty response');
    } catch (err) {
      console.warn(`[RANKER] Haiku failed: ${err.message} — falling back to gpt-5.4-mini`);
      pushActivity({ type: 'error', action: 'Ranker Fallback', note: `Haiku 4.5 failed — ${err.message}` });
    }
  }

  // Fallback: gpt-5.4-mini-2026-03-17
  const openaiKey = process.env.OPENAI_API_KEY;
  if (!openaiKey) throw new Error('No AI keys available for ranking');
  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${openaiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'gpt-5.4-mini-2026-03-17',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 250,
      temperature: 0.1,
    }),
  });
  if (!res.ok) {
    const t = await res.text();
    const errMsg = `gpt-5.4-mini ${res.status}: ${t.substring(0, 200)}`;
    pushActivity({ type: 'error', action: 'Ranker Fallback Failed', note: errMsg });
    throw new Error(errMsg);
  }
  const data = await res.json();
  return data.choices?.[0]?.message?.content || '';
}

function buildScoringCriteria(deal) {
  const dealType = (deal.type || deal.raise_type || '').toLowerCase();
  const isISSponsor = /independent.sponsor|fundless|co.invest/i.test(dealType);
  const isBuyout    = isISSponsor || /buyout|private.equity|lbo/i.test(dealType);
  const isVC        = /venture|seed|series|startup|pre.seed/i.test(dealType) && !isBuyout;

  // Currency — $ for US deals, £ for UK
  const isUS = /united states|usa|\$|usd/i.test((deal.geography || '') + (deal.description || ''));
  const sym = isUS ? '$' : '£';

  // Financial summary
  const s = deal.settings || {};
  const ebitda = deal.ebitda_usd_m || s.ebitda || deal.ebitda;
  const ev     = deal.enterprise_value_usd_m || s.ev || deal.ev;
  const equity = deal.equity_required_usd_m  || s.equity;
  const target = deal.target_amount ? `${sym}${Number(deal.target_amount).toLocaleString()}` : null;

  const financialLines = [
    ebitda ? `EBITDA: ${sym}${ebitda}M` : null,
    ev     ? `Enterprise Value (EV): ${sym}${ev}M` : null,
    equity ? `Equity Required: ${sym}${equity}M` : null,
    !ebitda && !ev && target ? `Raise Target: ${target}` : null,
    (deal.min_cheque || deal.cheque_min) ? `Min Cheque: ${sym}${Number(deal.min_cheque || deal.cheque_min).toLocaleString()}` : null,
    (deal.max_cheque || deal.cheque_max) ? `Max Cheque: ${sym}${Number(deal.max_cheque || deal.cheque_max).toLocaleString()}` : null,
  ].filter(Boolean).join('\n');

  let scoringBlock;
  if (isISSponsor) {
    scoringBlock = `SCORING (0-100 total) — INDEPENDENT SPONSOR / CO-INVESTMENT DEAL:
- Investor type fit (35pts): Do they do direct buyouts, co-investments, or LMM PE? Family office with direct deal mandate? Independent/fundless sponsor? Score 0 if pure VC/venture/early-stage with no buyout activity.
- Deal size fit (30pts): Does their typical equity check (${equity ? `${sym}${equity}M needed` : 'see financials'}) and deal size (${ev ? `EV ${sym}${ev}M` : 'LMM'}) fit their range? Unknown = 15pts.
- Sector match (25pts): Direct experience in ${deal.sector || 'this sector'}? Adjacent sector = partial credit.
- Geography (10pts): Active in ${deal.geography || 'United States'}? Unknown = 5pts.

Grade thresholds: Hot=85+, Warm=65-84, Possible=45-64, Archive=0-44
Archive if: pure VC/venture/early-stage with ZERO buyout or co-invest activity; pure biotech/drug-discovery with no services/distribution exposure; mega-PE ($10B+ AUM) whose minimum deal size far exceeds this EV.`;
  } else if (isBuyout) {
    scoringBlock = `SCORING (0-100 total) — BUYOUT / PE DEAL:
- Investor type fit (35pts): PE fund, family office with direct mandate, growth equity with buyout appetite?
- Deal size fit (30pts): Does their deal size range match ${ev ? `EV ${sym}${ev}M` : 'this deal size'}?
- Sector match (25pts): Active in ${deal.sector || 'this sector'}?
- Geography (10pts): Active in ${deal.geography || 'United States'}? Unknown = 5pts.

Grade thresholds: Hot=85+, Warm=65-84, Possible=45-64, Archive=0-44
Archive if: pure VC/angel/accelerator with no buyout history; deal size clearly out of range.`;
  } else {
    scoringBlock = `SCORING (0-100 total) — VENTURE / EQUITY RAISE:
- Sector/Strategy match (30pts): Do their fund strategies align with ${deal.sector || 'this sector'}? Early-stage/growth focus?
- Stage fit (20pts): Do they back companies at the ${deal.raise_type || 'current'} stage? Reduce if only late-stage or buyout-only.
- Cheque size match (20pts): Is ${target || 'the deal target'} in their typical range? Unknown = 10pts.
- Geography (15pts): Active in ${deal.geography || 'United States/UK'}? Unknown = 8pts.
- Engagement potential (15pts): Named contact with email = 10pts. Active investment programme = +5pts.

Grade thresholds: Hot=85+, Warm=65-84, Possible=45-64, Archive=0-44
Archive if: pure pension/sovereign wealth with zero VC activity, or clear geography mismatch.`;
  }

  return { financialLines, scoringBlock, dealType };
}

export async function rankInvestor({ investor, deal }) {
  const agentCtx = await getScoringContext();
  const { financialLines, scoringBlock, dealType } = buildScoringCriteria(deal);

  const prompt = `${agentCtx}Score this investor's fit for this fundraising deal. Return ONLY JSON.

DEAL:
Name: ${deal.name}
Structure: ${deal.type || deal.raise_type || 'Investment'}
Sector: ${deal.sector || 'Unknown'}
Geography: ${deal.geography || 'United States'}
${financialLines}
${deal.description ? `Description: ${(deal.description).substring(0, 300)}` : ''}
${deal.investor_profile ? `Ideal investor: ${deal.investor_profile}` : ''}

INVESTOR:
Firm: ${investor.company_name || investor.name}
Contact: ${investor.name}
Title: ${investor.job_title || 'Unknown'}
Investor Type: ${investor.investor_type || 'Unknown'}
Geography (HQ): ${investor.geography || investor.hq_country || 'Unknown'}
AUM: ${investor.aum_fund_size || investor.aum_millions ? `$${investor.aum_millions}M` : 'Unknown'}
Strategy/Focus: ${(investor.sector_focus || investor.preferred_industries || 'Unknown').substring(0, 300)}
Typical deal size: ${investor.typical_cheque_size || (investor.preferred_deal_size_min ? `$${investor.preferred_deal_size_min}M-$${investor.preferred_deal_size_max}M` : 'Unknown')}
Past investments: ${(investor.past_investments || '').substring(0, 250)}
Has email on file: ${investor.email ? 'Yes' : 'No'}
Notes: ${(investor.notes || '').substring(0, 200)}

${scoringBlock}

Return ONLY this JSON (no markdown):
{
  "score": <0-100>,
  "grade": "Hot"|"Warm"|"Possible"|"Archive",
  "rationale": "<1-2 sentences explaining score>",
  "key_reason": "<single most important factor>"
}`;

  try {
    const text = await haikuscore(prompt);
    console.log(`[RANKER] Haiku → ${investor.company_name || investor.name}`);
    const result = JSON.parse(text.replace(/```json|```/g, '').trim());
    return {
      score: Math.min(100, Math.max(0, Number(result.score) || 0)),
      grade: result.grade || 'Archive',
      rationale: result.rationale || '',
      key_reason: result.key_reason || '',
    };
  } catch (err) {
    console.error('[RANKER] Failed for', investor.company_name || investor.name, ':', err.message);
    return { score: 0, grade: 'Archive', rationale: 'Scoring failed', key_reason: '' };
  }
}
