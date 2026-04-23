import { orComplete } from '../core/openRouterClient.js';
import { getDeal } from '../core/dealContext.js';
import { info, error } from '../core/logger.js';
import { updateContact, getContactProp } from '../crm/notionContacts.js';
import { RESEARCH_CACHE_DAYS } from '../config/constants.js';

export async function researchInvestor(contactPage) {
  const name = getContactProp(contactPage, 'Name');
  const firm = getContactProp(contactPage, 'Company Name') || 'Unknown Firm';
  const lastResearch = getContactProp(contactPage, 'Notes');

  if (lastResearch?.includes('[RESEARCHED:')) {
    const match = lastResearch.match(/\[RESEARCHED:(\d{4}-\d{2}-\d{2})\]/);
    if (match) {
      const researchedAt = new Date(match[1]);
      const daysSince = (Date.now() - researchedAt) / (1000 * 60 * 60 * 24);
      if (daysSince < RESEARCH_CACHE_DAYS) {
        info(`Research cache hit for ${name} — skipping (${Math.floor(daysSince)}d old)`);
        return null;
      }
    }
  }

  info(`Researching investor: ${name} at ${firm}`);
  const deal = getDeal();
  const prompt = buildResearchPrompt(name, firm, deal);

  try {
    const text = await orComplete(prompt, { tier: 'web', maxTokens: 1024 });
    const json = extractJSON(text);
    if (!json) throw new Error('No valid JSON in response');
    await saveResearch(contactPage.id, name, json);
    info(`Research complete for ${name}`, { score: json.confidenceScore });
    return json;
  } catch (err) {
    error(`Research failed for ${name}`, { err: err.message });
    return fallbackWebResearch(name, firm, deal, contactPage.id);
  }
}

async function fallbackWebResearch(name, firm, deal, pageId) {
  info(`Falling back to web search for ${name}`);
  try {
    const summary = await orComplete(
      `${name} ${firm} investor investments portfolio background`,
      { tier: 'web', maxTokens: 500 }
    );
    const json = {
      comparableDeals: [],
      investmentCriteria: (summary || '').slice(0, 500),
      approachAngle: 'Research via web search — manual review recommended.',
      recentActivity: (summary || '').slice(0, 300),
      confidenceScore: 20,
    };
    await saveResearch(pageId, name, json);
    return json;
  } catch (err) {
    error(`Web search fallback also failed for ${name}`, { err: err.message });
    return null;
  }
}

function buildResearchPrompt(name, firm, deal) {
  return `You are a senior research analyst at an elite investment bank. You are researching ${name} at ${firm} for a fundraising approach on behalf of a client.

The deal we are fundraising for: ${deal.description}
Deal sector: ${deal.sector}
Deal size: ${deal.raiseAmount}
Geography: ${deal.geography}

Research this investor exhaustively:
1. What specific deals have they invested in that are most comparable to our deal? List deal names, dates, amounts if known.
2. What is their stated investment criteria? Quote it verbatim if you can find it.
3. What do they typically look for in a deal like ours?
4. Have they made any public statements about this sector recently?
5. What is the best angle to approach them with our specific deal?

Return your findings as structured JSON with fields: comparableDeals (array of strings), investmentCriteria (string), approachAngle (string), recentActivity (string), confidenceScore (0-100).

Return ONLY valid JSON. No explanation before or after.`;
}

async function saveResearch(pageId, name, json) {
  const today = new Date().toISOString().split('T')[0];
  const summary = [
    `[RESEARCHED:${today}]`,
    `Comparable Deals: ${json.comparableDeals?.join(', ') || 'None found'}`,
    `Criteria: ${json.investmentCriteria?.slice(0, 300) || 'Unknown'}`,
    `Approach: ${json.approachAngle?.slice(0, 200) || 'N/A'}`,
    `Confidence: ${json.confidenceScore}/100`,
  ].join('\n');

  await updateContact(pageId, {
    notes: summary.slice(0, 2000),
    similarPastDeals: (json.comparableDeals || []).join(', ').slice(0, 2000),
    pipelineStage: 'Researched',
  });
}

function extractJSON(text) {
  try {
    const match = text.match(/\{[\s\S]*\}/);
    if (match) return JSON.parse(match[0]);
  } catch {}
  return null;
}
