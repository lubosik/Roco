import { getSupabase } from './supabase.js';
import { haikuComplete } from './aiClient.js';

const TEMPLATE_SPECS = [
  { name: 'Email Intro', type: 'email', sequence_step: 'email_intro' },
  { name: 'Email Follow Up', type: 'email', sequence_step: 'email_followup_1' },
  { name: 'Final Email', type: 'email', sequence_step: 'email_followup_2' },
  { name: 'LinkedIn Intro DM', type: 'linkedin_dm', sequence_step: 'linkedin_dm_1' },
  { name: 'LinkedIn Follow Up DM', type: 'linkedin_dm', sequence_step: 'linkedin_dm_2' },
];

function buildDealContext(deal, senderName) {
  return {
    senderName,
    senderTitle: deal.sender_title || 'Principal',
    dealName: deal.name || 'Unnamed Deal',
    dealType: deal.deal_type || deal.raise_type || 'Private deal',
    sector: deal.sector || 'Unknown',
    targetAmount: deal.target_amount || deal.targetAmount || deal.equity || 'Unknown',
    currency: deal.currency || 'USD',
    ebitda: deal.ebitda || 'Unknown',
    ev: deal.ev || 'Unknown',
    geography: deal.target_geography || deal.geography || 'Global',
    description: (deal.description || '').trim(),
    keyMetrics: (deal.key_metrics || '').trim(),
    investorProfile: (deal.investor_profile || '').trim(),
    deckUrl: deal.deck_url || '',
  };
}

function buildPrompt(context) {
  return `
You are writing the default outreach sequence for an enterprise AI fundraising platform.
The sender is a sophisticated deal principal approaching investors for a private deal.
The copy should feel commercially sharp, psychologically aware, and credible to institutional investors, family offices, and private capital allocators.

Write exactly 5 templates as valid JSON only, with no markdown.

Objectives:
- Make the default sequence strong enough that most users keep it as-is.
- Sound selective, informed, and commercially disciplined.
- Use the deal facts provided. Do not invent facts.
- Use the personalization variables naturally, especially where they strengthen relevance.
- No em dashes.
- No exclamation marks.
- No hype, clichés, or generic filler.
- Avoid phrases like "hope you're well", "wanted to reach out", and "just following up".

Variable rules:
- Use only these variables:
  {{firstName}}, {{firm}}, {{company}}, {{title}}, {{pastInvestments}}, {{investmentThesis}},
  {{sectorFocus}}, {{investorGeography}}, {{dealName}}, {{dealBrief}}, {{sector}},
  {{targetAmount}}, {{keyMetrics}}, {{geography}}, {{minCheque}}, {{maxCheque}},
  {{investorProfile}}, {{comparableDeal}}, {{deckUrl}}, {{callLink}}, {{senderName}}, {{senderTitle}}
- Variables must use double curly braces exactly.
- Subject lines should be direct and professional.

Deal context:
Deal Name: ${context.dealName}
Deal Type: ${context.dealType}
Sector: ${context.sector}
Target Amount: ${context.currency} ${context.targetAmount}
EBITDA: ${context.ebitda}
Enterprise Value: ${context.ev}
Target Investor Geography: ${context.geography}
Description: ${context.description || 'Not provided'}
Key Metrics / USP: ${context.keyMetrics || 'Not provided'}
Investor Profile: ${context.investorProfile || 'Not provided'}
Deck URL: ${context.deckUrl || 'Not provided'}
Sender: ${context.senderName}, ${context.senderTitle}

Output format:
{
  "templates": [
    {
      "name": "Email Intro",
      "type": "email",
      "sequence_step": "email_intro",
      "subject_a": "...",
      "subject_b": "...",
      "body": "..."
    },
    {
      "name": "Email Follow Up",
      "type": "email",
      "sequence_step": "email_followup_1",
      "subject_a": "...",
      "subject_b": "...",
      "body": "..."
    },
    {
      "name": "Final Email",
      "type": "email",
      "sequence_step": "email_followup_2",
      "subject_a": "...",
      "subject_b": "...",
      "body": "..."
    },
    {
      "name": "LinkedIn Intro DM",
      "type": "linkedin_dm",
      "sequence_step": "linkedin_dm_1",
      "subject_a": null,
      "subject_b": null,
      "body": "..."
    },
    {
      "name": "LinkedIn Follow Up DM",
      "type": "linkedin_dm",
      "sequence_step": "linkedin_dm_2",
      "subject_a": null,
      "subject_b": null,
      "body": "..."
    }
  ]
}

Writing constraints:
- Email bodies should usually stay under 130 words.
- LinkedIn DMs should usually stay under 65 words.
- Email intro should establish fit and selective outreach.
- Email follow-up should advance the process with urgency but without pressure.
- Final email should be crisp and easy to answer.
- LinkedIn intro should reference fit and offer the deck or a short summary.
- LinkedIn follow-up should be a brief nudge with a clear next step.
`.trim();
}

function sanitizeTemplate(template, senderName) {
  return {
    name: template.name,
    type: template.type,
    sequence_step: template.sequence_step,
    subject_a: template.subject_a || null,
    subject_b: template.subject_b || null,
    body: String(template.body || '')
      .replace(/\u2014/g, ',')
      .replace(/\u2013/g, ',')
      .trim()
      || `{{firstName}},\n\nHappy to share more on {{dealName}} if helpful.\n\n${senderName}`,
  };
}

function fallbackTemplates(senderName) {
  return [
    {
      name: 'Email Intro',
      type: 'email',
      sequence_step: 'email_intro',
      subject_a: '{{dealName}} | {{sector}} opportunity',
      subject_b: '{{firstName}}, relevance to {{firm}}',
      body: `{{firstName}},\n\nReaching out selectively on {{dealName}}, a {{sector}} opportunity seeking {{targetAmount}}. The fit for {{firm}} stood out because of {{pastInvestments}} and {{investmentThesis}}.\n\nIf useful, I can send the deck and a short investment summary.\n\n{{senderName}}`,
    },
    {
      name: 'Email Follow Up',
      type: 'email',
      sequence_step: 'email_followup_1',
      subject_a: 'Re: {{dealName}}',
      subject_b: '{{firstName}}, should I send the deck?',
      body: `{{firstName}},\n\nFollowing up on {{dealName}} in case the first note was directionally relevant. The setup is particularly compelling on {{keyMetrics}}, and the investor profile is aligned with groups active in {{geography}}.\n\nIf there is interest, I can send the deck or a tighter summary.\n\n{{senderName}}`,
    },
    {
      name: 'Final Email',
      type: 'email',
      sequence_step: 'email_followup_2',
      subject_a: 'Close the loop on {{dealName}}',
      subject_b: '{{firstName}}, worth keeping open?',
      body: `{{firstName}},\n\nClosing the loop on {{dealName}}. If this is outside mandate or timing, no problem. If it is relevant, I can send materials and key diligence points immediately.\n\n{{senderName}}`,
    },
    {
      name: 'LinkedIn Intro DM',
      type: 'linkedin_dm',
      sequence_step: 'linkedin_dm_1',
      subject_a: null,
      subject_b: null,
      body: `{{firstName}}, thanks for connecting. Reaching out on {{dealName}}, which looked relevant given {{pastInvestments}} and {{investmentThesis}}. Happy to send a short summary or the deck if useful. {{senderName}}`,
    },
    {
      name: 'LinkedIn Follow Up DM',
      type: 'linkedin_dm',
      sequence_step: 'linkedin_dm_2',
      subject_a: null,
      subject_b: null,
      body: `{{firstName}}, circling back on {{dealName}} in case the timing is right. If helpful, I can send the deck and the key points in one note. {{senderName}}`,
    },
  ];
}

async function upsertGeneratedTemplates(dealId, templates, generatedByAI = true) {
  const sb = getSupabase();

  await sb.from('deal_templates')
    .delete()
    .eq('deal_id', dealId)
    .eq('generated_by_ai', true);

  const rows = templates.map(t => ({
    deal_id: dealId,
    name: t.name,
    type: t.type,
    sequence_step: t.sequence_step,
    subject_a: t.subject_a || null,
    subject_b: t.subject_b || null,
    body: t.body,
    is_primary: true,
    ab_test_enabled: !!(t.subject_a && t.subject_b),
    generated_by_ai: generatedByAI,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  }));

  await sb.from('deal_templates')
    .update({ is_primary: false })
    .eq('deal_id', dealId)
    .in('sequence_step', rows.map(r => r.sequence_step));

  const { error } = await sb.from('deal_templates').insert(rows);
  if (error) throw new Error(error.message);
  return rows;
}

export async function generateDealTemplates(deal, updatedBy = 'Dom') {
  console.log(`[TEMPLATE GEN] Generating templates for: ${deal.name}`);
  const context = buildDealContext(deal, updatedBy);

  try {
    const raw = await haikuComplete(buildPrompt(context), { maxTokens: 2200 });
    const clean = raw.replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(clean);
    const generated = new Map((parsed.templates || []).map(t => [t.sequence_step, sanitizeTemplate(t, updatedBy)]));
    const fallback = fallbackTemplates(updatedBy);
    const templates = TEMPLATE_SPECS.map(spec =>
      generated.get(spec.sequence_step) || fallback.find(t => t.sequence_step === spec.sequence_step)
    );

    const rows = await upsertGeneratedTemplates(deal.id, templates, true);
    console.log(`[TEMPLATE GEN] Generated ${rows.length} templates for ${deal.name}`);
    return rows;
  } catch (err) {
    console.error('[TEMPLATE GEN] Failed:', err.message);
    const rows = await upsertGeneratedTemplates(deal.id, fallbackTemplates(updatedBy), false);
    console.log(`[TEMPLATE GEN] Inserted ${rows.length} fallback templates`);
    return rows;
  }
}
