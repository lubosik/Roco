// core/templateGenerator.js
import { getSupabase } from './supabase.js';

export async function generateDealTemplates(deal, updatedBy = 'Dom') {
  console.log(`[TEMPLATE GEN] Generating templates for: ${deal.name}`);

  const dealContext = `
Deal Name: ${deal.name}
Deal Type: ${deal.deal_type || deal.raise_type || 'Unknown'}
Sector: ${deal.sector || 'Unknown'}
EBITDA: $${deal.ebitda || deal.settings?.ebitda || 'Unknown'}M
Enterprise Value: $${deal.ev || deal.settings?.ev || 'Unknown'}M
Equity Needed: $${deal.equity || deal.settings?.equity || deal.target_amount || 'Unknown'}M
Strategy: ${deal.outreach_strategy || deal.strategy || deal.investor_profile || ''}
Sender Name: ${updatedBy}
  `.trim();

  const prompt = `
You are writing outbound email and LinkedIn DM templates for a deal placement agent.
The sender is a deal principal reaching out to investors.

DEAL CONTEXT:
${dealContext}

Write exactly 4 templates. Return ONLY valid JSON, no explanation, no markdown.

Rules:
- No em dashes anywhere (use commas or periods instead)
- No exclamation marks
- No "just following up" or "I wanted to reach out" or "hope this finds you well"
- Double line breaks between paragraphs (use \\n\\n)
- Sign off as: ${updatedBy}
- Keep emails under 100 words
- Keep LinkedIn DMs under 50 words
- Use these variables where appropriate: {{firstName}}, {{firm}}, {{dealName}},
  {{dealType}}, {{sector}}, {{ebitda}}, {{ev}}, {{equity}}, {{senderName}}
- Variables must use double curly braces exactly as shown
- Subject lines must not contain em dashes

Return this exact JSON structure:
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
      "name": "LinkedIn Intro DM",
      "type": "linkedin",
      "sequence_step": "linkedin_dm_1",
      "subject_a": null,
      "subject_b": null,
      "body": "..."
    },
    {
      "name": "LinkedIn Follow Up DM",
      "type": "linkedin",
      "sequence_step": "linkedin_dm_2",
      "subject_a": null,
      "subject_b": null,
      "body": "..."
    }
  ]
}
  `.trim();

  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key':         process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Type':      'application/json',
      },
      body: JSON.stringify({
        model:      'claude-sonnet-4-6',
        max_tokens: 2000,
        messages:   [{ role: 'user', content: prompt }],
      }),
    });

    if (!res.ok) throw new Error(`Anthropic API error: ${res.status}`);
    const data = await res.json();
    const raw = data.content?.[0]?.text || '';
    const clean = raw.replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(clean);

    const rows = (parsed.templates || []).map(t => ({
      deal_id:         deal.id,
      name:            t.name,
      type:            t.type,
      sequence_step:   t.sequence_step,
      subject_a:       t.subject_a || null,
      subject_b:       t.subject_b || null,
      body:            t.body,
      is_primary:      true,
      ab_test_enabled: !!(t.subject_a && t.subject_b),
      generated_by_ai: true,
      created_at:      new Date().toISOString(),
      updated_at:      new Date().toISOString(),
    }));

    await getSupabase().from('deal_templates').insert(rows);
    console.log(`[TEMPLATE GEN] Generated ${rows.length} templates for ${deal.name}`);
    return rows;
  } catch (err) {
    console.error('[TEMPLATE GEN] Failed:', err.message);
    await insertFallbackTemplates(deal, updatedBy);
  }
}

async function insertFallbackTemplates(deal, senderName) {
  const rows = [
    {
      deal_id: deal.id,
      name: 'Email Intro',
      type: 'email',
      sequence_step: 'email_intro',
      subject_a: `{{dealName}} -- {{dealType}}, {{sector}}`,
      subject_b: `{{firstName}}, quick question on {{dealName}}`,
      body: `{{firstName}},\n\nI am working on {{dealName}}, a {{dealType}} in the {{sector}} space generating ${{ebitda}}M EBITDA at a ${{ev}}M enterprise value.\n\nGiven your focus on {{investorFocus}}, I believe this fits your mandate well.\n\nHappy to send across the executive summary if this is of interest.\n\n${senderName}`,
      is_primary: true,
      ab_test_enabled: true,
      generated_by_ai: false,
    },
    {
      deal_id: deal.id,
      name: 'Email Follow Up',
      type: 'email',
      sequence_step: 'email_followup_1',
      subject_a: `Re: {{dealName}}`,
      subject_b: `{{firstName}}, last note on {{dealName}}`,
      body: `{{firstName}},\n\nWanted to follow up on {{dealName}}. The process is moving and I wanted to make sure you had visibility before we progress further.\n\nThis is an exclusive mandate and I am reaching out to a small number of investors whose profile aligns with the deal.\n\nWorth a quick conversation?\n\n${senderName}`,
      is_primary: true,
      ab_test_enabled: true,
      generated_by_ai: false,
    },
    {
      deal_id: deal.id,
      name: 'LinkedIn Intro DM',
      type: 'linkedin',
      sequence_step: 'linkedin_dm_1',
      subject_a: null,
      subject_b: null,
      body: `{{firstName}}, thanks for connecting. I am working on {{dealName}}, a {{dealType}} in {{sector}} with ${{ebitda}}M EBITDA. Given your background at {{firm}}, I thought it might be relevant. Happy to share more if useful. ${senderName}`,
      is_primary: true,
      ab_test_enabled: false,
      generated_by_ai: false,
    },
    {
      deal_id: deal.id,
      name: 'LinkedIn Follow Up DM',
      type: 'linkedin',
      sequence_step: 'linkedin_dm_2',
      subject_a: null,
      subject_b: null,
      body: `{{firstName}}, wanted to make sure my last message came through on {{dealName}}. Still happy to share the summary if the timing works. ${senderName}`,
      is_primary: true,
      ab_test_enabled: false,
      generated_by_ai: false,
    },
  ].map(r => ({ ...r, created_at: new Date().toISOString(), updated_at: new Date().toISOString() }));

  await getSupabase().from('deal_templates').insert(rows);
  console.log('[TEMPLATE GEN] Inserted fallback templates');
}
