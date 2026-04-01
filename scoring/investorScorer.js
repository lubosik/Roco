import Anthropic from '@anthropic-ai/sdk';
import { getDeal } from '../core/dealContext.js';
import { updateContact, getContactProp } from '../crm/notionContacts.js';
import { info, error } from '../core/logger.js';
import { SCORE_THRESHOLDS, PIPELINE_STAGES } from '../config/constants.js';

let client;

function getClient() {
  if (!client) client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
  return client;
}

export async function scoreInvestor(contactPage, researchData) {
  const name = getContactProp(contactPage, 'Name');
  const deal = getDeal();

  info(`Scoring investor: ${name}`);

  const prompt = buildScoringPrompt(name, contactPage, researchData, deal);

  try {
    const response = await getClient().messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 512,
      messages: [{ role: 'user', content: prompt }],
    });

    const text = response.content[0].text;
    const json = extractJSON(text);
    if (!json?.score) throw new Error('Invalid scoring response');

    const score = Math.min(100, Math.max(0, Math.round(json.score)));
    const classification = classifyScore(score);

    await updateContact(contactPage.id, {
      score,
      pipelineStage: score >= SCORE_THRESHOLDS.POSSIBLE ? PIPELINE_STAGES.RESEARCHED : PIPELINE_STAGES.INACTIVE,
    });

    info(`Scored ${name}: ${score}/100 (${classification})`);
    return { score, classification, breakdown: json.breakdown };
  } catch (err) {
    error(`Scoring failed for ${name}`, { err: err.message });
    return null;
  }
}

function buildScoringPrompt(name, page, research, deal) {
  const sectorFocus = getContactProp(page, 'Sector Focus') || 'Unknown';
  const chequeSize = getContactProp(page, 'Typical Cheque Size ($)') || 'Unknown';
  const geography = getContactProp(page, 'Geography') || 'Unknown';

  return `You are scoring an investor for deal fit. Score 0-100 based on these weighted criteria:

SCORING CRITERIA (weights):
- Sector fit: 30% — does their sector focus match ours?
- Cheque size match: 20% — does their typical investment match our raise?
- Comparable past deals: 25% — have they done specifically similar deals?
- Geographic fit: 10% — do they invest in our geography?
- Recency: 10% — have they been active in the last 12 months?
- Accessibility: 5% — direct contact vs gatekeeper?

DEAL CONTEXT:
- Deal: ${deal.name}
- Sector: ${deal.sector}
- Raise: ${deal.raiseAmount}
- Geography: ${deal.geography}

INVESTOR PROFILE for ${name}:
- Sector focus: ${sectorFocus}
- Typical cheque size: ${chequeSize}
- Geography: ${geography}
- Comparable past deals: ${research?.comparableDeals?.join(', ') || 'None found'}
- Investment criteria: ${research?.investmentCriteria?.slice(0, 400) || 'Unknown'}
- Recent activity: ${research?.recentActivity?.slice(0, 300) || 'Unknown'}

Return ONLY valid JSON:
{
  "score": <number 0-100>,
  "breakdown": {
    "sectorFit": <0-30>,
    "chequeSizeMatch": <0-20>,
    "comparableDeals": <0-25>,
    "geographicFit": <0-10>,
    "recency": <0-10>,
    "accessibility": <0-5>
  },
  "rationale": "<one sentence>"
}`;
}

function classifyScore(score) {
  if (score >= SCORE_THRESHOLDS.HOT) return 'Hot Lead';
  if (score >= SCORE_THRESHOLDS.WARM) return 'Warm Lead';
  if (score >= SCORE_THRESHOLDS.POSSIBLE) return 'Possible Lead';
  return 'Archive';
}

function extractJSON(text) {
  try {
    const match = text.match(/\{[\s\S]*\}/);
    if (match) return JSON.parse(match[0]);
  } catch {}
  return null;
}
