import { getSupabase } from './supabase.js';
import { sendTelegram } from '../approval/telegramBot.js';
import { buildGuidanceBlock } from '../services/guidanceService.js';
import Anthropic from '@anthropic-ai/sdk';
import { DateTime } from 'luxon';

const GROK_API_KEY = process.env.GROK_API_KEY || process.env.XAI_API_KEY;
const GROK_BASE = 'https://api.x.ai/v1';
const anthropic = process.env.ANTHROPIC_API_KEY
  ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
  : null;
const NEWS_SUMMARY_MODEL = process.env.ROCO_NEWS_SUMMARY_MODEL || 'claude-sonnet-4-6';

function extractJSONArray(text) {
  const match = String(text || '').match(/\[[\s\S]*\]/);
  return match ? JSON.parse(match[0]) : [];
}

function extractJSONObject(text) {
  const match = String(text || '').match(/\{[\s\S]*\}/);
  return match ? JSON.parse(match[0]) : null;
}

function truncate(value, max = 180) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  return text.length > max ? `${text.slice(0, max - 1)}...` : text;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function normalizeDealLead(lead) {
  return {
    firm_name: String(lead?.firm_name || '').trim(),
    news_event: String(lead?.news_event || '').trim(),
    why_relevant: String(lead?.why_relevant || '').trim(),
    urgency: String(lead?.urgency || 'medium').trim().toLowerCase() === 'high' ? 'high' : 'medium',
    source_hint: String(lead?.source_hint || '').trim() || null,
    source_url: String(lead?.source_url || '').trim() || null,
    published_at: String(lead?.published_at || '').trim() || null,
    investor_type: String(lead?.investor_type || '').trim() || null,
    hq_country: String(lead?.hq_country || '').trim() || null,
    relevance_score: Math.max(1, Math.min(10, Number(lead?.relevance_score || 7) || 7)),
    recommended_action: String(lead?.recommended_action || '').trim() || null,
    match_reasons: safeArray(lead?.match_reasons).map(item => String(item || '').trim()).filter(Boolean).slice(0, 3),
  };
}

function normalizeGeneralLead(lead) {
  return {
    firm_name: String(lead?.firm_name || '').trim(),
    investor_type: String(lead?.investor_type || '').trim() || null,
    hq_country: String(lead?.hq_country || '').trim() || null,
    signal: String(lead?.signal || '').trim(),
    why_relevant: String(lead?.why_relevant || '').trim(),
    source_hint: String(lead?.source_hint || '').trim() || null,
    confidence: Math.max(1, Math.min(10, Number(lead?.confidence || 7) || 7)),
  };
}

function normalizeNewsText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/https?:\/\/\S+/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function buildNewsDedupKey(item) {
  const firm = normalizeNewsText(item?.firm_name || 'market');
  const event = normalizeNewsText(item?.news_event || item?.reason || '').slice(0, 120);
  return `${firm}|${event}`;
}

function buildSpecificGrokQueries(deal) {
  const sector = String(deal?.sector || 'private capital').trim();
  const geography = String(deal?.target_geography || deal?.geography || 'United States').trim();
  const investorProfile = String(deal?.investor_profile || deal?.target_investor_profile || 'family offices private equity independent sponsors').trim();
  const description = String(deal?.description || deal?.deal_description || '').trim();
  const monthLabel = DateTime.now().setZone('America/New_York').toFormat('LLLL yyyy');
  const year = DateTime.now().setZone('America/New_York').year;

  return [
    `${investorProfile} investments ${sector} ${year}`,
    `${sector} investor activity ${geography} ${monthLabel}`,
    `${deal?.raise_type || deal?.deal_type || 'growth capital'} ${sector} investors ${geography} ${year}`,
    `${sector} acquisitions fundraise family office private equity ${geography} ${year}`,
    description ? `${description.slice(0, 90)} investors ${geography} ${monthLabel}` : null,
  ].filter(Boolean).slice(0, 5);
}

function simplifyGrokQuery(query) {
  return String(query || '')
    .replace(/\b(family offices?|private equity|independent sponsors?)\b/gi, 'investors')
    .replace(/\b(lower middle market|growth capital|growth equity)\b/gi, 'investments')
    .replace(/\s+/g, ' ')
    .trim();
}

async function executeGrokNewsQueries(deal, queries = [], pushActivity) {
  const rawResults = [];
  for (const query of queries) {
    let result = await grokWebSearch(
      query,
      'You are an investor intelligence analyst. Search the live web and return concrete findings with source detail.',
      1100
    );
    let retryQuery = null;
    if (!result) {
      retryQuery = simplifyGrokQuery(query);
      if (retryQuery && retryQuery !== query) {
        result = await grokWebSearch(
          retryQuery,
          'You are an investor intelligence analyst. Search the live web and return concrete findings with source detail.',
          900
        );
      }
    }
    rawResults.push({
      query,
      retry_query: retryQuery,
      success: !!result,
      raw_result: result || `Query failed for: ${query}`,
    });
    if (!result) {
      pushActivity?.({
        type: 'warning',
        action: 'News scan query failed',
        note: `${deal.name} · ${query.slice(0, 140)}`,
        deal_id: deal.id,
      });
    }
  }
  return rawResults;
}

export async function grokWebSearch(query, systemContext = '', maxTokens = 1000) {
  if (!GROK_API_KEY) return null;

  try {
    const response = await fetch(`${GROK_BASE}/chat/completions`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${GROK_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'grok-4-fast',
        max_tokens: maxTokens,
        messages: [
          ...(systemContext ? [{ role: 'system', content: systemContext }] : []),
          { role: 'user', content: query },
        ],
        search: true,
      }),
    });

    if (!response.ok) {
      const err = await response.text().catch(() => '');
      console.warn('[GROK SEARCH]', response.status, err.slice(0, 120));
      return null;
    }

    const data = await response.json();
    return data.choices?.[0]?.message?.content || null;
  } catch (err) {
    console.warn('[GROK SEARCH]', err.message);
    return null;
  }
}

async function gatherDealOperatingContext(deal) {
  const sb = getSupabase();
  if (!sb || !deal?.id) {
    return {
      active_firms: 0,
      latest_firms: [],
      recent_replies: [],
      pending_approvals: 0,
      current_batch: null,
    };
  }

  const [
    { data: firms },
    { data: replies },
    { data: recentNews },
    { count: pendingApprovals },
    { data: currentBatch },
  ] = await Promise.all([
    sb.from('batch_firms')
      .select('firm_name, score, thesis, created_at')
      .eq('deal_id', deal.id)
      .order('created_at', { ascending: false })
      .limit(8)
      .then(result => result)
      .catch(() => ({ data: [] })),
    sb.from('conversation_messages')
      .select('body, intent, channel, sent_at, contact_name')
      .eq('deal_id', deal.id)
      .eq('direction', 'inbound')
      .order('sent_at', { ascending: false })
      .limit(5)
      .then(result => result)
      .catch(() => ({ data: [] })),
    sb.from('news_investor_leads')
      .select('firm_name, reason, created_at')
      .eq('deal_id', deal.id)
      .gte('created_at', new Date(Date.now() - 5 * 86400000).toISOString())
      .order('created_at', { ascending: false })
      .limit(8)
      .then(result => result)
      .catch(() => ({ data: [] })),
    sb.from('approval_queue')
      .select('id', { count: 'exact', head: true })
      .eq('deal_id', deal.id)
      .eq('status', 'pending')
      .then(result => result)
      .catch(() => ({ count: 0 })),
    sb.from('campaign_batches')
      .select('batch_number, status, ranked_firms, target_firms')
      .eq('deal_id', deal.id)
      .order('batch_number', { ascending: false })
      .limit(1)
      .maybeSingle()
      .then(result => result)
      .catch(() => ({ data: null })),
  ]);

  return {
    active_firms: safeArray(firms).length,
    latest_firms: safeArray(firms).map(firm => ({
      firm_name: firm.firm_name,
      score: firm.score,
      thesis: truncate(firm.thesis, 120),
    })),
    recent_replies: safeArray(replies).map(reply => ({
      from: reply.contact_name || 'Unknown contact',
      channel: reply.channel || 'unknown',
      intent: reply.intent || 'unknown',
      summary: truncate(reply.body, 140),
      sent_at: reply.sent_at || null,
    })),
    recent_news_items: safeArray(recentNews).map(item => ({
      firm_name: item.firm_name || null,
      reason: truncate(item.reason, 160),
      created_at: item.created_at || null,
    })),
    pending_approvals: Number(pendingApprovals || 0),
    current_batch: currentBatch || null,
  };
}

function buildPortfolioCoverageBrief(portfolioDeals = [], currentDeal = null) {
  const deals = safeArray(portfolioDeals).filter(Boolean);
  if (!deals.length) return 'No other active deals are running right now.';

  return deals.map((deal, index) => {
    const parts = [
      `${index + 1}. ${deal.name}`,
      `sector ${deal.sector || 'General'}`,
      `geography ${deal.target_geography || deal.geography || 'United States'}`,
      deal.raise_type ? `type ${deal.raise_type}` : null,
      deal.target_amount ? `target ${(Number(deal.target_amount) / 1_000_000).toFixed(1)}M` : null,
      currentDeal?.id === deal.id ? 'current scan target' : 'parallel active deal',
    ].filter(Boolean);
    return parts.join(' | ');
  }).join('\n');
}

function buildDealNewsSearchPrompt(deal, portfolioDeals, operatingContext) {
  return `You are running ROCO's daily investor intelligence scan.

ACTIVE DEAL PORTFOLIO
${buildPortfolioCoverageBrief(portfolioDeals, deal)}

PRIMARY DEAL TO MATCH
- Name: ${deal.name}
- Sector: ${deal.sector || 'General'}
- Geography: ${deal.target_geography || deal.geography || 'United States'}
- Deal type: ${deal.deal_type || deal.raise_type || 'Growth / Private capital'}
- Target amount: ${deal.target_amount ? `$${Number(deal.target_amount).toLocaleString()}` : 'Not specified'}
- Equity need: ${deal.equity || deal.equity_required_usd_m ? `$${deal.equity || deal.equity_required_usd_m}M` : 'Not specified'}
- EBITDA / scale: ${deal.ebitda || deal.ebitda_usd_m ? `$${deal.ebitda || deal.ebitda_usd_m}M EBITDA` : 'Not specified'}

CURRENT OPERATING CONTEXT
- Active firms already in pipeline: ${operatingContext.active_firms || 0}
- Current batch: ${operatingContext.current_batch?.batch_number ? `#${operatingContext.current_batch.batch_number}` : 'none'} (${operatingContext.current_batch?.status || 'n/a'})
- Pending approvals: ${operatingContext.pending_approvals || 0}
- Recent firms already being worked:
${operatingContext.latest_firms?.length
    ? operatingContext.latest_firms.map(firm => `  - ${firm.firm_name}${firm.score ? ` (${firm.score}/100)` : ''}${firm.thesis ? `: ${firm.thesis}` : ''}`).join('\n')
    : '  - none yet'}
- Recent inbound replies:
${operatingContext.recent_replies?.length
    ? operatingContext.recent_replies.map(reply => `  - ${reply.from} via ${reply.channel}: ${reply.intent} | ${reply.summary}`).join('\n')
    : '  - none yet'}
- Recent news items already surfaced in the last 5 days:
${operatingContext.recent_news_items?.length
    ? operatingContext.recent_news_items.map(item => `  - ${item.firm_name || 'Market context'}: ${item.reason}`).join('\n')
    : '  - none logged recently'}

SEARCH TASK
Search live investor news, deal announcements, fund closes, mandate changes, partner hires, and investment activity from the last 21 days.
Cross-reference multiple credible sources where possible.
Use multiple search angles for this deal: sector M&A, fund closes, partner moves, strategy shifts, and geography-specific buyer activity.

IMPORTANT: Return ALL findings — even general sector news with no specific investor named.
Do not filter for relevance. Include fund closes, acquisitions, strategic hires, market commentary,
and any sector-specific M&A activity. The operator needs to know what is happening in this sector
today even if nothing is immediately actionable as an investor lead.
Avoid repeating the same firm/event combinations that already appeared in the recent-news context unless there is a materially new development.

Return ONLY valid JSON in this exact shape:
{
  "search_summary": "2-3 sentences on what changed in the market for this deal sector",
  "findings": [
    {
      "firm_name": "name of investor or acquirer if identified, otherwise null",
      "news_event": "specific recent development or sector headline",
      "why_relevant": "tie to this deal sector or geography",
      "urgency": "high OR medium OR low",
      "source_hint": "publication name",
      "source_url": "https://...",
      "published_at": "YYYY-MM-DD or approximate date",
      "investor_type": "PE/Buyout OR Family Office OR Independent Sponsor OR VC/Growth OR other",
      "hq_country": "United States",
      "relevance_score": 1-10
    }
  ]
}
Maximum 8 findings. Always include at least 3 items — use general sector context if specific investor news is sparse.`;
}

async function reviewDealNewsWithClaude(deal, searchPayload, operatingContext) {
  const findings = safeArray(searchPayload?.findings).map(normalizeDealLead);
  const rawResults = safeArray(searchPayload?.raw_results);

  const prompt = `You are ROCO's investment-news reviewer.

PRIMARY DEAL
${JSON.stringify({
    name: deal.name,
    sector: deal.sector || 'General',
    geography: deal.target_geography || deal.geography || 'United States',
    deal_type: deal.deal_type || deal.raise_type || null,
    target_amount: deal.target_amount || null,
    equity: deal.equity || deal.equity_required_usd_m || null,
    target_close_date: deal.target_close_date || null,
  }, null, 2)}

CURRENT OPERATING CONTEXT
${JSON.stringify(operatingContext, null, 2)}

RAW SEARCH OUTPUT
${JSON.stringify(searchPayload, null, 2)}

TASK
1. Summarise the key market developments, investor activity, and relevant news for this deal.
2. Filter the findings to only the firms that should realistically be added to this deal's pipeline now.
3. Use the operating context to avoid weak duplicates.
4. Never say there is no data. Synthesize what is available from the raw search results.

Return ONLY valid JSON:
{
  "summary": "3-5 sentence operator summary",
  "is_relevant": true,
  "notes": "short note on why this matters",
  "recommended_investors": [
    {
      "name": "person name or null",
      "firm": "firm name",
      "reason": "why this investor should be added"
    }
  ],
  "telegram_summary": "clean digest paragraph"
}
Maximum 5 recommended investors.`;

  if (!anthropic) {
    return {
      summary: `${deal.name}: ${findings.length || rawResults.length} market signal${(findings.length || rawResults.length) === 1 ? '' : 's'} reviewed for this sector today.`,
      is_relevant: findings.length > 0,
      notes: findings.length ? 'Fresh investor activity was identified.' : 'Market context was reviewed even though source data was sparse.',
      recommended_investors: findings.filter(lead => lead.relevance_score >= 7).slice(0, 5).map(lead => ({
        name: null,
        firm: lead.firm_name,
        reason: lead.why_relevant || lead.news_event,
      })),
      telegram_summary: `${deal.name}: ${findings.length || rawResults.length} investor or market signal${(findings.length || rawResults.length) === 1 ? '' : 's'} reviewed. ${findings.slice(0, 2).map(lead => `${lead.firm_name} looks relevant because ${truncate(lead.why_relevant, 80)}`).join(' ')}`.trim(),
    };
  }

  try {
    const response = await anthropic.messages.create({
      model: NEWS_SUMMARY_MODEL,
      max_tokens: 1600,
      messages: [{ role: 'user', content: prompt }],
    });
    const json = extractJSONObject(response.content?.[0]?.text || '');
    return {
      summary: String(json?.summary || '').trim() || `${deal.name}: investor-news scan completed.`,
      is_relevant: json?.is_relevant !== false,
      notes: String(json?.notes || '').trim(),
      recommended_investors: safeArray(json?.recommended_investors).map(item => ({
        name: String(item?.name || '').trim() || null,
        firm: String(item?.firm || '').trim(),
        reason: String(item?.reason || '').trim(),
      })).filter(item => item.firm && item.reason),
      telegram_summary: String(json?.telegram_summary || '').trim() || `${deal.name}: investor-news scan completed.`,
    };
  } catch (err) {
    console.warn('[NEWS REVIEW]', err.message);
    return {
      summary: `${deal.name}: investor-news scan completed with ${findings.length || rawResults.length} candidate signal${(findings.length || rawResults.length) === 1 ? '' : 's'}.`,
      is_relevant: findings.length > 0,
      notes: 'Claude review failed, so Roco kept the strongest raw findings only.',
      recommended_investors: findings.filter(lead => lead.relevance_score >= 7).slice(0, 5).map(lead => ({
        name: null,
        firm: lead.firm_name,
        reason: lead.why_relevant || lead.news_event,
      })),
      telegram_summary: `${deal.name}: investor-news scan completed with ${findings.length || rawResults.length} candidate signal${(findings.length || rawResults.length) === 1 ? '' : 's'}. Claude review failed, so the strongest raw findings were kept.`,
    };
  }
}

export async function buildDealNewsScan(deal, portfolioDeals = [], pushActivity) {
  if (!deal?.id || !GROK_API_KEY) {
    return { leads: [], summary: '', rawSummary: '', rejectedFindings: [], grokQueries: [], grokRawResults: [], isRelevant: false, notes: '', recommendedInvestors: [] };
  }

  const operatingContext = await gatherDealOperatingContext(deal);
  const queryPlan = buildSpecificGrokQueries(deal);
  const grokRawResults = await executeGrokNewsQueries(deal, queryPlan, pushActivity);
  const successfulResults = grokRawResults.filter(item => item.success && item.raw_result).map(item => item.raw_result);

  let searchPayload = null;
  if (successfulResults.length) {
    const query = buildDealNewsSearchPrompt(deal, portfolioDeals, operatingContext) + `\n\nRAW RESULT SNAPSHOTS\n${successfulResults.map((result, index) => `QUERY ${index + 1}\n${result}`).join('\n\n')}`;
    const result = await grokWebSearch(
      query,
      'You are a buy-side research analyst. Search the live web, cross-check sources, and return only valid JSON.',
      1400
    );
    try {
      searchPayload = result ? extractJSONObject(result) : null;
    } catch (err) {
      pushActivity?.({
        type: 'error',
        action: 'News scan parse failed',
        note: `${deal.name} · ${err.message?.slice(0, 100)}`,
        deal_id: deal.id,
      });
    }
  }

  if (!searchPayload) {
    searchPayload = {
      search_summary: `${deal.name}: searched ${queryPlan.length} specific investor and market queries for ${deal.sector || 'this sector'}.`,
      findings: [],
      raw_results: grokRawResults,
    };
  } else {
    searchPayload.raw_results = grokRawResults;
  }

  const review = await reviewDealNewsWithClaude(deal, searchPayload, operatingContext);
  const recentKeys = new Set(
    safeArray(operatingContext.recent_news_items).map(item => buildNewsDedupKey(item))
  );
  const leadKeys = new Set(recentKeys);
  const findingKeys = new Set(recentKeys);
  const dedupedLeads = safeArray(searchPayload?.findings)
    .map(normalizeDealLead)
    .filter(lead => {
      const key = buildNewsDedupKey(lead);
      if (!key || leadKeys.has(key)) return false;
      leadKeys.add(key);
      return lead.firm_name && lead.why_relevant;
    })
    .filter(lead => safeArray(review.recommended_investors).some(item => normalizeNewsText(item.firm) === normalizeNewsText(lead.firm_name)))
    .map(lead => ({
      ...lead,
      recommended_action: 'add_to_pipeline_now',
    }));
  const dedupedFindings = safeArray(searchPayload?.findings)
    .map(normalizeDealLead)
    .filter(item => {
      const key = buildNewsDedupKey(item);
      if (!key || findingKeys.has(key)) return false;
      findingKeys.add(key);
      return Boolean(item.news_event || item.why_relevant);
    });

  return {
    leads: dedupedLeads,
    summary: review.telegram_summary || review.summary,
    rawSummary: String(searchPayload?.search_summary || '').trim(),
    rejectedFindings: [],
    allFindings: dedupedFindings,
    grokQueries: queryPlan,
    grokRawResults,
    isRelevant: review.is_relevant !== false,
    notes: review.notes || '',
    recommendedInvestors: review.recommended_investors || [],
  };
}

export async function summarizePortfolioNewsDigest(scanRows = []) {
  const rows = safeArray(scanRows)
    .filter(row => row?.deal?.name)
    .map(row => ({
      deal_name: row.deal.name,
      sector: row.deal.sector || 'General',
      summary: row.scanResult?.rawSummary || row.scanResult?.summary || '',
      findings: safeArray(row.scanResult?.allFindings).slice(0, 3).map(item => ({
        firm_name: item.firm_name || null,
        news_event: item.news_event || '',
        why_relevant: item.why_relevant || '',
      })),
      leads_added: safeArray(row.scanResult?.leads).length,
    }));

  if (!rows.length) return '';

  if (!anthropic) {
    return rows
      .map(row => `${row.deal_name}: ${row.summary || `${row.leads_added} fresh signal${row.leads_added === 1 ? '' : 's'} reviewed.`}`)
      .join(' ');
  }

  try {
    const prompt = `You are writing ROCO's top-level daily market brief across all active deals.

SCAN DATA
${JSON.stringify(rows, null, 2)}

TASK
Write 3-5 concise sentences for Telegram.
Summarise the most important fresh market movements across these sectors.
Do not repeat the same point twice.
Mention which sectors look active, cautious, or quiet.
If some deals had no meaningful fresh items, say that cleanly.
No bullets. No markdown.`;

    const response = await anthropic.messages.create({
      model: NEWS_SUMMARY_MODEL,
      max_tokens: 300,
      messages: [{ role: 'user', content: prompt }],
    });
    return String(response.content?.[0]?.text || '').trim();
  } catch (err) {
    console.warn('[NEWS DIGEST]', err.message);
    return rows
      .map(row => `${row.deal_name}: ${row.summary || `${row.leads_added} fresh signal${row.leads_added === 1 ? '' : 's'} reviewed.`}`)
      .join(' ');
  }
}

export async function searchInvestorsWithGrok(deal, existingFirmNames, pushActivity) {
  if (!deal?.id || !GROK_API_KEY) return [];

  pushActivity?.({
    type: 'research',
    action: `Grok web search: finding active investors for ${deal.name}`,
    note: `${deal.sector || 'General'} · ${deal.target_geography || deal.geography || 'US'}`,
    deal_id: deal.id,
  });

  const query = `Find PE firms, family offices, and independent sponsors that have recently invested in or announced interest in ${deal.sector || 'the target sector'} companies in ${deal.target_geography || deal.geography || 'the United States'}.

Focus on deals from the last 18 months. Include firms that:
- Are based in or actively invest in ${deal.target_geography || deal.geography || 'the US'}
- Have done ${deal.deal_type || deal.raise_type || 'similar'} transactions in ${deal.sector || 'the target sector'} or adjacent sectors
- Would consider a deal with ${deal.ebitda || deal.ebitda_usd_m ? `$${deal.ebitda || deal.ebitda_usd_m}M EBITDA` : 'the current EBITDA profile'} and ${deal.equity || deal.equity_required_usd_m ? `$${deal.equity || deal.equity_required_usd_m}M equity needed` : 'the current equity need'}

Return ONLY a JSON array:
[
  {
    "firm_name": "...",
    "investor_type": "PE/Buyout OR Family Office OR Independent Sponsor",
    "hq_country": "United States",
    "reason_fit": "1 sentence specific to this deal",
    "recent_activity": "specific deal or announcement that makes them relevant",
    "confidence": 7
  }
]
Maximum 10 results.`;

  const result = await grokWebSearch(
    query,
    'You are a PE research analyst. Return only valid JSON and only genuinely relevant firms.',
    1200
  );
  if (!result) return [];

  try {
    const firms = extractJSONArray(result)
      .filter(firm => Number(firm.confidence || 0) >= 7)
      .filter(firm => !existingFirmNames.has(String(firm.firm_name || '').toLowerCase().trim()));

    if (firms.length) {
      pushActivity?.({
        type: 'research',
        action: `Grok found ${firms.length} additional investor leads`,
        note: firms.map(firm => `${firm.firm_name} (${firm.confidence}/10)`).join(' · ').slice(0, 180),
        deal_id: deal.id,
      });
    }

    return firms.map(firm => ({
      name: firm.firm_name,
      firm_name: firm.firm_name,
      investor_type: firm.investor_type,
      hq_country: firm.hq_country,
      description: `${firm.reason_fit} Recent: ${firm.recent_activity || 'N/A'}`,
      source_file: 'grok_web_search',
      list_name: 'Grok Web Research',
      _from_internet: true,
    }));
  } catch (err) {
    pushActivity?.({
      type: 'error',
      action: 'Grok search parse failed',
      note: err.message?.slice(0, 80),
      deal_id: deal.id,
    });
    return [];
  }
}

export async function scanInvestorNewsForDeal(deal, optionsOrPushActivity, maybePushActivity) {
  const pushActivity = typeof optionsOrPushActivity === 'function'
    ? optionsOrPushActivity
    : maybePushActivity;
  const portfolioDeals = typeof optionsOrPushActivity === 'function'
    ? [deal]
    : safeArray(optionsOrPushActivity?.portfolioDeals).length
      ? safeArray(optionsOrPushActivity?.portfolioDeals)
      : [deal];

  if (!deal?.id || !GROK_API_KEY) return [];

  try {
    pushActivity?.({
      type: 'research',
      action: 'Daily news scan: searching for recent investor activity',
      note: `${deal.name} · ${deal.sector || 'General'}`,
      deal_id: deal.id,
    });
    const packageResult = await buildDealNewsScan(deal, portfolioDeals, pushActivity);
    const leads = safeArray(packageResult.leads)
      .map(normalizeDealLead)
      .filter(lead => lead.firm_name && lead.news_event && lead.why_relevant);

    if (!leads.length) {
      pushActivity?.({
        type: 'research',
        action: 'Daily news scan: no new leads found today',
        note: `${deal.name} · Pipeline is current`,
        deal_id: deal.id,
      });
      return [];
    }

    pushActivity?.({
      type: 'research',
      action: `News scan: ${leads.length} relevant lead${leads.length !== 1 ? 's' : ''} found`,
      note: packageResult.summary || leads.map(lead => `${lead.firm_name}: ${String(lead.news_event || '').slice(0, 60)}`).join(' | ').slice(0, 200),
      deal_id: deal.id,
    });

    return leads;
  } catch (err) {
    console.error('[NEWS SCANNER]', err.message);
    pushActivity?.({
      type: 'error',
      action: 'News scan failed',
      note: err.message?.slice(0, 100),
      deal_id: deal.id,
    });
    return [];
  }
}

export async function saveDealNewsLeads(dealId, leads) {
  const sb = getSupabase();
  if (!sb || !dealId || !Array.isArray(leads) || !leads.length) return 0;
  let saved = 0;
  for (const lead of leads) {
    const { error } = await sb.from('news_investor_leads').insert({
      deal_id: dealId,
      firm_name: lead.firm_name,
      reason: `${lead.why_relevant} Event: ${lead.news_event}`,
      confidence: lead.urgency === 'high' ? 9 : 7,
      source_url: lead.source_url || lead.source_hint || null,
    });
    if (!error) saved += 1;
  }
  return saved;
}

export async function scanGeneralInvestorSignals(pushActivity) {
  if (!GROK_API_KEY) return [];

  try {
    const guidanceBlock = await buildGuidanceBlock('investor_outreach').catch(() => '');
    pushActivity?.({
      type: 'research',
      action: 'Daily news scan: no active deals, scanning market using Dom persona',
      note: 'Using Train Your Agent investor guidance to source general investor signals',
    });

    const query = `${guidanceBlock}
Find PE firms, family offices, and independent sponsors with fresh, specific signals in the last 14 days that suggest they should be added to Dom's investor database.

Focus on:
- new investments or acquisitions
- fund closes or new mandates
- partner hires or strategy shifts
- public statements showing active appetite

Return ONLY a JSON array:
[
  {
    "firm_name": "...",
    "investor_type": "PE/Buyout OR Family Office OR Independent Sponsor",
    "hq_country": "United States",
    "signal": "specific recent event",
    "why_relevant": "why Dom should track this firm",
    "source_hint": "publication or source",
    "confidence": 8
  }
]
Maximum 8 results.`;

    const result = await grokWebSearch(
      query,
      'You are Dom\'s investor research analyst. Search the live web and return only valid JSON.',
      1000
    );
    if (!result) return [];

    const leads = extractJSONArray(result)
      .map(normalizeGeneralLead)
      .filter(lead => lead.firm_name && lead.signal && lead.why_relevant);

    pushActivity?.({
      type: 'research',
      action: leads.length ? `General investor scan: ${leads.length} market signals found` : 'General investor scan: no useful market signals found',
      note: leads.length
        ? leads.map(lead => `${lead.firm_name}: ${lead.signal.slice(0, 60)}`).join(' | ').slice(0, 200)
        : 'No active deals today',
    });

    return leads;
  } catch (err) {
    pushActivity?.({
      type: 'error',
      action: 'General investor news scan failed',
      note: err.message?.slice(0, 100),
    });
    return [];
  }
}

export async function storeGeneralInvestorSignals(leads, pushActivity) {
  const sb = getSupabase();
  if (!sb || !Array.isArray(leads) || !leads.length) return { stored: 0, skipped: 0 };

  let stored = 0;
  let skipped = 0;

  for (const lead of leads) {
    const firmName = String(lead.firm_name || '').trim();
    if (!firmName) {
      skipped += 1;
      continue;
    }

    const { data: existing } = await sb.from('investors_db')
      .select('id, name')
      .ilike('name', firmName)
      .limit(1);
    if (existing?.length) {
      skipped += 1;
      continue;
    }

    const baseRow = {
      name: firmName,
      firm_name: firmName,
      investor_type: lead.investor_type || null,
      hq_country: lead.hq_country || null,
      description: `${lead.why_relevant} Recent signal: ${lead.signal}`,
      source_file: 'grok_news_scan',
      list_name: 'Grok News Scan',
    };

    let { error } = await sb.from('investors_db').insert(baseRow);
    if (error) {
      ({ error } = await sb.from('investors_db').insert({
        name: firmName,
        description: `${lead.why_relevant} Recent signal: ${lead.signal}`,
      }));
    }

    if (error) skipped += 1;
    else stored += 1;
  }

  pushActivity?.({
    type: 'system',
    action: `General investor scan stored ${stored} firm${stored !== 1 ? 's' : ''} in investors_db`,
    note: skipped ? `${skipped} skipped because they already existed or failed to insert` : 'All new signals were stored',
  });

  return { stored, skipped };
}

// ── PUBLIC INVESTOR DIRECTORY SCRAPER ────────────────────────────────────────
// Fetches publicly accessible investor directory pages and extracts firm names.
// Called alongside searchInvestorsWithGrok() during researchNextFirms() in orchestrator.

const PUBLIC_INVESTOR_DIRECTORIES = [
  'https://www.axial.net/forum/companies/private-equity/',
  'https://www.axial.net/forum/companies/family-offices/',
  'https://www.axial.net/forum/companies/independent-sponsors/',
  'https://www.axial.net/forum/companies/health-care-family-offices/2/',
  'https://smash.vc/independent-sponsor-investors/',
];

export async function scrapePublicInvestorDirectories(deal, existingNames, pushActivity) {
  if (!GROK_API_KEY || !deal?.id) return [];

  const relevantUrls = [];
  const ebitda = parseFloat(deal.ebitda_usd_m || deal.ebitda || 0);

  if (ebitda < 20 || !ebitda) {
    relevantUrls.push('https://www.axial.net/forum/companies/independent-sponsors/');
    relevantUrls.push('https://www.axial.net/forum/companies/family-offices/');
  }

  const sector = (deal.sector || '').toLowerCase();
  if (sector.includes('health') || sector.includes('medical')) {
    relevantUrls.push('https://www.axial.net/forum/companies/health-care-family-offices/2/');
  }

  relevantUrls.push('https://smash.vc/independent-sponsor-investors/');

  const newFirms = [];
  const namesSnap = new Set([...existingNames].map(n => String(n).toLowerCase().trim()));

  for (const url of relevantUrls) {
    try {
      pushActivity?.({
        type: 'research',
        action: `Scanning investor directory: ${url.split('/').slice(-3).join('/')}`,
        note: `${deal.name} · Looking for ${deal.investor_control_preference || 'majority'} investors`,
        deal_id: deal.id,
        dealId: deal.id,
      });

      const extractPrompt = `Fetch this URL and extract all investor/firm names listed on the page: ${url}

For each firm found, return:
{
  "firm_name": "exact name as listed",
  "investor_type": "PE Fund | Family Office | Independent Sponsor | other",
  "description": "brief description if available",
  "website": "website URL if listed",
  "source_url": "${url}"
}

Return as JSON array only. Maximum 30 results. Only include firms, not individuals.
Exclude any firm already in this list: ${[...namesSnap].slice(0, 50).join(', ')}`;

      const result = await grokWebSearch(
        extractPrompt,
        'Extract investor names from the page. Return valid JSON array only.',
        1500
      );
      if (!result) continue;

      const match = result.match(/\[[\s\S]*?\]/);
      if (!match) continue;

      const firms = JSON.parse(match[0]);
      const filtered = firms.filter(f =>
        f.firm_name &&
        !namesSnap.has(String(f.firm_name || '').toLowerCase().trim())
      );

      filtered.forEach(f => namesSnap.add(String(f.firm_name || '').toLowerCase().trim()));

      newFirms.push(...filtered.map(f => ({
        name: f.firm_name,
        firm_name: f.firm_name,
        investor_type: f.investor_type || null,
        description: f.description || null,
        list_name: 'Public Directory',
        source_file: 'directory_scrape',
        _from_internet: true,
      })));

      if (filtered.length > 0) {
        pushActivity?.({
          type: 'research',
          action: `Directory scan: ${filtered.length} new firms from ${url.split('/').slice(-3).join('/')}`,
          note: `${deal.name} · ${filtered.slice(0, 3).map(f => f.firm_name).join(', ')}${filtered.length > 3 ? '...' : ''}`,
          deal_id: deal.id,
          dealId: deal.id,
        });
      }

      await new Promise(r => setTimeout(r, 2000));
    } catch (err) {
      console.warn('[DIRECTORY SCRAPE]', url, err.message);
    }
  }

  return newFirms;
}

export async function runInvestorNewsScan(deal, pushActivity) {
  const leads = await scanInvestorNewsForDeal(deal, pushActivity);
  await saveDealNewsLeads(deal?.id, leads);

  const urgent = leads.filter(lead => lead.urgency === 'high');
  if (urgent.length && deal?.name) {
    const urgentText =
      `🔥 *Roco found urgent investor leads — ${deal.name}*\n\n` +
      urgent.map(lead => `*${lead.firm_name}*\n_${lead.news_event}_\n→ ${lead.why_relevant}`).join('\n\n') +
      '\n\n_These firms are active right now — Roco is adding them to the pipeline._';

    await sendTelegram(urgentText).catch(() => {});
    pushActivity?.({
      type: 'system',
      action: 'Urgent news leads flagged to Dom via Telegram',
      note: `${urgent.length} high-priority firms: ${urgent.map(lead => lead.firm_name).join(', ')}`,
      deal_id: deal.id,
    });
  }

  return leads;
}
