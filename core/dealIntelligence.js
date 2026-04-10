// core/dealIntelligence.js
// Analyses comparable deal records from PitchBook to extract investor signals.
// Builds intelligence_boost scores so Roco ranks investors with proven
// track records in similar deals higher for every campaign batch.

import Anthropic from '@anthropic-ai/sdk';
import { getSupabase } from './supabase.js';

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

export async function analyzeDealIntelligence(dealId, pushActivity = null) {
  const sb = getSupabase();
  if (!sb) return;

  const { data: deal } = await sb.from('deals')
    .select('name, raise_type, sector, description').eq('id', dealId).single();
  if (!deal) return;

  console.log('[INTELLIGENCE] Analysing comparables for:', deal.name);

  let offset = 0;
  const batchSize = 25;

  while (true) {
    const { data: companies } = await sb.from('deal_intelligence')
      .select('id, source_company, description, financing_note, investors_raw, lead_investors, sponsor')
      .eq('deal_id', dealId)
      .is('similarity_score', null)
      .range(offset, offset + batchSize - 1);

    if (!companies?.length) break;

    const prompt = `You are analysing comparable companies to rank them by similarity to a deal.

THE DEAL:
Name: ${deal.name}
Type: ${deal.raise_type || 'PE/Buyout'}
Sector: ${deal.sector || 'Unknown'}
Description: ${(deal.description || '').slice(0, 300)}

COMPANIES TO ANALYSE:
${companies.map((c, i) => `
[${i + 1}] ${c.source_company}
Description: ${(c.description || '').slice(0, 250)}
Financing: ${(c.financing_note || '').slice(0, 200)}
Named Investors: ${[c.investors_raw, c.lead_investors, c.sponsor].filter(Boolean).join(' | ').slice(0, 250)}
`).join('\n')}

For each company:
1. Score similarity 0-100 (how similar is this company to the deal above?)
2. Extract investor names from the financing note (firm names only, no individuals)
3. Write one sentence rationale

Return ONLY this JSON, no other text:
{"results":[{"index":1,"score":75,"investors":["Firm A","Firm B"],"rationale":"..."}]}`;

    try {
      const response = await anthropic.messages.create({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 2000,
        messages: [{ role: 'user', content: prompt }],
      });

      const raw = (response.content[0]?.text || '').replace(/```json|```/g, '').trim();
      const parsed = JSON.parse(raw);

      for (const item of parsed.results || []) {
        const company = companies[item.index - 1];
        if (!company) continue;
        await sb.from('deal_intelligence').update({
          similarity_score:      item.score,
          investors_extracted:   item.investors || [],
          similarity_rationale:  item.rationale || '',
        }).eq('id', company.id);
      }
    } catch (err) {
      console.error('[INTELLIGENCE] Batch analysis error:', err.message);
    }

    offset += batchSize;
    await new Promise(r => setTimeout(r, 1000));
  }

  await buildInvestorBoosts(dealId, deal, sb, pushActivity);
  console.log('[INTELLIGENCE] Analysis complete for:', deal.name);
}

async function buildInvestorBoosts(dealId, deal, sb, pushActivity) {
  const { data: similar } = await sb.from('deal_intelligence')
    .select('investors_extracted, investors_raw, lead_investors, sponsor, similarity_score, source_company')
    .eq('deal_id', dealId)
    .gte('similarity_score', 60)
    .order('similarity_score', { ascending: false });

  if (!similar?.length) return;

  const investorData = {};
  for (const company of similar) {
    const combinedInvestors = [
      ...(company.investors_extracted || []),
      ...String(company.investors_raw || '').split(/[;,]\s*/),
      ...String(company.lead_investors || '').split(/[;,]\s*/),
      ...String(company.sponsor || '').split(/[;,]\s*/),
    ].map(v => String(v || '').trim()).filter(Boolean);

    for (const inv of combinedInvestors) {
      const key = inv.toLowerCase().trim();
      if (!key || key.length < 3) continue;
      if (!investorData[key]) {
        investorData[key] = { name: inv, count: 0, totalScore: 0, companies: [] };
      }
      investorData[key].count++;
      investorData[key].totalScore += company.similarity_score;
      investorData[key].companies.push(company.source_company);
    }
  }

  for (const data of Object.values(investorData)) {
    const boost = Math.min(data.count * 12, 40);
    let dbMatch = null;
    try {
      const result = await sb.from('investors_db')
        .select('id')
        .ilike('name', `%${data.name.split(' ')[0]}%`)
        .limit(1)
        .maybeSingle();
      dbMatch = result.data || null;
    } catch (_) {}

    await sb.from('deal_investor_scores').upsert({
      deal_id:             dealId,
      investor_name:       data.name,
      investor_db_id:      dbMatch?.id || null,
      intelligence_boost:  boost,
      times_backed_similar: data.count,
      backed_companies:    data.companies.slice(0, 10),
    }, { onConflict: 'deal_id,investor_name' });
  }

  const msg = `Deal intelligence scoring complete — ${Object.keys(investorData).length} investors scored by comparable deal history`;
  console.log('[INTELLIGENCE]', msg);
  if (pushActivity) {
    pushActivity({ type: 'research', action: 'Deal intelligence scoring complete',
      note: `${Object.keys(investorData).length} investors scored by comparable deal history` });
  }
}
