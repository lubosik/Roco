import { getDeal } from '../core/dealContext.js';
import { getContactProp } from '../crm/notionContacts.js';
import { info, error, warn } from '../core/logger.js';
import { buildGuidanceBlock } from '../services/guidanceService.js';
import { orComplete } from '../core/openRouterClient.js';

const SYSTEM_PROMPT = `You are Dom's personal writing assistant. Dom is a senior fundraising professional who has been operating in private markets for 15 years. You produce the FINAL, send-ready email — it must read exactly like Dom typed it himself.

WRITING RULES — non-negotiable:
- Write as if Dom already knows this person, even on a first introduction. Warm, familiar, confident.
- Never use em-dashes. Never use hashtags. Never use bullet points or symbols in email copy.
- Ultra-conversational. Short sentences. Natural pauses. Proper grammar throughout.
- Never be salesy. Never oversell. Nonchalant — like Dom is sharing something interesting, not pitching.
- Reference specific past investments the investor has made where available. Generic emails get ignored.
- One clear ask at the end. Nothing more.
- Subject lines drive curiosity or familiarity — what a colleague would send, not clickbait.
- Maximum 150 words for the body. Brevity signals confidence.
- No sign-off fluff. End cleanly after the ask.
- Every sentence must make complete sense and flow naturally. If any part of a brief or template doesn't fit this investor, rewrite it — never leave awkward or generic phrasing in the final output.

FORBIDDEN — never output these:
- "Hope this finds you well" / "Hope you're well"
- "I wanted to reach out" / "Reaching out"
- "Please find attached" / "As per my previous email"
- "Touching base" / "Circle back" / any chase language
- "Synergy" or any corporate jargon whatsoever
- "Exciting opportunity" / "Impressive portfolio" / flattery of any kind
- Placeholder text like [name], [company], [link] — if you don't have the info, write around it naturally`;

const EMAIL_LENGTH = {
  INTRO: { min: 80, max: 130 },
  'FOLLOW-UP 1': { min: 60, max: 90 },
  'FOLLOW-UP 2': { min: 50, max: 80 },
  'FOLLOW-UP 3': { min: 30, max: 50 },
};


export async function draftEmail(contactPage, researchData, stage = 'INTRO', editInstructions = null) {
  const name = getContactProp(contactPage, 'Name');
  const firstName = name?.split(' ')[0] || name;
  const firmRelation = contactPage.properties?.['Company Name']?.relation;
  const email = getContactProp(contactPage, 'Email');
  const deal = getDeal();
  const len = EMAIL_LENGTH[stage] || EMAIL_LENGTH.INTRO;

  info(`Drafting ${stage} email for ${firstName}`);

  const guidanceBlock = await buildGuidanceBlock('investor_outreach').catch(() => '');
  const userPrompt = buildUserPrompt(contactPage, firstName, name, researchData, deal, stage, len, editInstructions, guidanceBlock);

  try {
    const text = await orComplete(userPrompt, { tier: 'draft', maxTokens: 1024, systemPrompt: SYSTEM_PROMPT });
    const parsed = extractJSON(text);
    if (!parsed?.subject || !parsed?.body) throw new Error('Invalid email JSON from model');
    info(`Email drafted for ${firstName}`);
    return parsed;
  } catch (err) {
    error(`Email draft failed for ${firstName}`, { err: err.message });
    return null;
  }
}

function buildUserPrompt(contactPage, firstName, fullName, research, deal, stage, len, editInstructions, guidanceBlock = '') {
  const comparableDeals = research?.comparableDeals?.join(', ') || 'None on record';
  const criteria = research?.investmentCriteria?.slice(0, 400) || 'Unknown';
  const approach = research?.approachAngle?.slice(0, 300) || 'General interest in sector';
  const whyThisFirm = getContactProp(contactPage, 'Why This Firm')
    || research?.whyThisFirm
    || 'Not on record';
  const pastInvestments = getContactProp(contactPage, 'Past Investments')
    || research?.pastInvestments
    || comparableDeals;
  const investmentThesis = getContactProp(contactPage, 'Investment Thesis')
    || research?.investmentThesis
    || approach;
  const aum = getContactProp(contactPage, 'AUM')
    || research?.aum
    || 'Unknown';

  let editNote = '';
  if (editInstructions) {
    editNote = `\n\nDOM'S EDIT INSTRUCTIONS: ${editInstructions}\nApply these changes to the email. Keep everything else the same.`;
  }

  return `${guidanceBlock}Write an outreach email from Dom to ${fullName}.

Investor research summary: ${approach}
Why this firm matches: ${whyThisFirm}
Comparable deals they have done: ${pastInvestments}
Investment thesis: ${investmentThesis}
Known AUM / fund size: ${aum}
Their stated investment criteria: ${criteria}

The deal Dom is fundraising for:
- Deal name: ${deal.name}
- Sector: ${deal.sector}
- Raise amount: ${deal.raiseAmount}
- Geography: ${deal.geography}
- Key metrics: ${deal.keyMetrics || 'Available on request'}
- Brief description: ${deal.description}

Email stage: ${stage}
Target word count: ${len.min}-${len.max} words for the body.

For an INTRO email, the rough structure is:
${firstName},
Thought this might be interesting for you given [their specific past investment in X] or [their stated criteria around Y].
[2-3 sentences on the deal — what it is, the opportunity, why now.]
Let me know if you would like to explore further.
Dom

For FOLLOW-UP emails, do not repeat the same angle. Each follow-up must take a slightly different approach — new angle, new piece of information, genuine curiosity rather than chasing.

Apply these psychological principles:
- Specificity over flattery — reference actual past deals by name
- Use the "Why this firm matches" rationale when it gives a stronger, more precise reason to write than the generic criteria line
- Scarcity without desperation — imply the deal is moving forward
- Pattern interrupt subject lines — not "Investment Opportunity" — ever
- For follow-ups, use the Ben Franklin effect — ask a small question rather than restating the pitch
${editNote}

Return ONLY valid JSON:
{
  "subject": "subject line here",
  "body": "full email body here",
  "alternativeSubject": "alternative A/B subject line here"
}`;
}

function extractJSON(text) {
  try {
    const match = text.match(/\{[\s\S]*\}/);
    if (match) return JSON.parse(match[0]);
  } catch {}
  return null;
}
