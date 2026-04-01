/**
 * core/agentContext.js
 * Phase-aware guidance loader. Reads from roco_guidance_investor and returns
 * only the sections relevant to the current LLM call phase.
 * 5-minute in-memory cache to avoid DB hits on every call.
 *
 * Phase → sections injected:
 *   scoring / db_query      → identity, search, scoring
 *   research                → identity, research
 *   outreach_draft          → identity, voice, outreach
 *   reply / temp_close      → identity, voice, reply, closing
 *   intent_classification   → identity, reply, closing
 */

import { getInvestorGuidance } from '../services/guidanceService.js';

let _cache = null;
let _cacheTime = 0;
const CACHE_TTL = 5 * 60 * 1000;

async function loadGuidance() {
  if (_cache && Date.now() - _cacheTime < CACHE_TTL) return _cache;
  try {
    const guidance = await getInvestorGuidance();
    _cache = guidance || {};
    _cacheTime = Date.now();
  } catch (err) {
    console.warn('[AGENT_CTX] Failed to load guidance:', err.message);
    _cache = _cache || {}; // keep stale cache on error
  }
  return _cache;
}

export function invalidateCache() {
  _cache = null;
}

export async function getAgentContext(phase) {
  const g = await loadGuidance();

  const identity = g.identity          || '';
  const voice    = g.voice_guidance     || '';
  const search   = g.search_guidance    || '';
  const research = g.research_guidance  || '';
  const scoring  = g.scoring_guidance   || '';
  const outreach = g.outreach_guidance  || '';
  const reply    = g.reply_guidance     || '';
  const closing  = g.closing_guidance   || '';

  const sections = [];

  // Identity anchors every phase
  if (identity) sections.push(`## IDENTITY & PERSONA\n${identity}`);

  switch (phase) {
    case 'db_query':
    case 'scoring':
      if (search)   sections.push(`## SEARCH GUIDANCE\n${search}`);
      if (scoring)  sections.push(`## SCORING GUIDANCE\n${scoring}`);
      break;

    case 'research':
      if (research) sections.push(`## RESEARCH GUIDANCE\n${research}`);
      break;

    case 'outreach_draft':
      if (voice)    sections.push(`## VOICE & TONE\n${voice}`);
      if (outreach) sections.push(`## OUTREACH GUIDANCE\n${outreach}`);
      break;

    case 'reply':
    case 'temp_close_followup':
      if (voice)    sections.push(`## VOICE & TONE\n${voice}`);
      if (reply)    sections.push(`## REPLY & CONVERSATION GUIDANCE\n${reply}`);
      if (closing)  sections.push(`## CLOSING GUIDANCE\n${closing}`);
      break;

    case 'intent_classification':
      if (reply)    sections.push(`## REPLY & CONVERSATION GUIDANCE\n${reply}`);
      if (closing)  sections.push(`## CLOSING GUIDANCE\n${closing}`);
      break;

    default:
      break;
  }

  if (sections.length === 0) return '';
  return `\n===== ROCO AGENT GUIDANCE (FOLLOW THESE RULES EXACTLY) =====\n${sections.join('\n\n')}\n===== END OF GUIDANCE =====\n\n`;
}

export const getDbQueryContext   = () => getAgentContext('db_query');
export const getResearchContext  = () => getAgentContext('research');
export const getScoringContext   = () => getAgentContext('scoring');
export const getOutreachContext  = () => getAgentContext('outreach_draft');
export const getReplyContext     = () => getAgentContext('reply');
export const getIntentContext    = () => getAgentContext('intent_classification');
export const getTempCloseContext = () => getAgentContext('temp_close_followup');
