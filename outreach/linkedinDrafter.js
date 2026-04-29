/**
 * outreach/linkedinDrafter.js — LinkedIn DM drafting
 *
 * Uses the active deal row, LinkedIn sequence templates, Train Your Agent
 * guidance, deal assets, investor research, and prior conversation context.
 */

import { getDeal } from '../core/dealContext.js';
import { getSupabase } from '../core/supabase.js';
import { getContactProp } from '../crm/notionContacts.js';
import { info, warn } from '../core/logger.js';
import { buildGuidanceBlock } from '../services/guidanceService.js';
import { retrieveLinkedInProfile } from '../integrations/unipileClient.js';
import { orComplete } from '../core/openRouterClient.js';

const LINKEDIN_SYSTEM_PROMPT = `You are Dom's writing assistant for LinkedIn messages. Dom raises capital for private deals. You produce the FINAL, send-ready LinkedIn message exactly as Dom would send it.

LINKEDIN RULES — non-negotiable:
- Maximum 1000 characters for DMs. Stay comfortably under.
- Ultra-conversational. Peer-to-peer. No email tone.
- No greeting fluff. Start cleanly.
- No formal sign-off. End after the ask.
- Personalise specifically when the context supports it.
- If a template, brief, or guidance line does not fit this investor, rewrite it so the final message flows naturally.
- If prior conversation history exists, respect it. Do not restart the relationship like a cold intro.
- FORBIDDEN: "reaching out", "hope you're well", "exciting opportunity", "just wanted to", "touching base", placeholder text like [name] or [company], corporate jargon.

Return only the final message text. No JSON, no labels, no markdown.`;

const STEP_LABEL_MAP = {
  intro: 'linkedin_dm_1',
  followup: 'linkedin_dm_2',
  connection_request: 'linkedin_dm_1',
};

const TEMPLATE_NAME_PATTERNS = {
  intro: ['linkedin intro', 'intro dm', 'intro'],
  followup: ['linkedin follow', 'follow up dm', 'follow-up dm', 'follow up', 'follow-up'],
};

const DEFAULT_LINKEDIN_TEMPLATES = {
  intro: `{{firstName}}, thought {{dealName}} could be relevant given {{firm}}'s work around {{sectorFocus}}.

{{dealBrief}}

Happy to send the deck or a tighter summary if useful.`,
  followup: `{{firstName}}, quick one on {{dealName}}.

Given {{firm}}'s focus around {{sectorFocus}}, I thought the angle might be worth a look.

Worth me sending the short version?`,
  connection_request: `{{firstName}}, thought it would be useful to connect given your work around {{sectorFocus}} and what Dom is doing with {{dealName}}.`,
};

function clip(value, max = 400) {
  const text = String(value || '').trim();
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

function formatAmount(value, currency = 'GBP') {
  const num = Number(value || 0);
  if (!num) return '';
  const symbol = currency === 'USD' ? '$' : currency === 'EUR' ? 'EUR ' : '£';
  return `${symbol}${num.toLocaleString()}`;
}

function normaliseTemplate(record) {
  return { ...record, body_a: record?.body_a || record?.body || '' };
}

function fillTemplate(template, { contact, deal }) {
  const name = String(contact?.name || '').trim();
  const firstName = name.split(' ')[0] || name;
  return String(template || '')
    .replace(/\{\{firstName\}\}/gi, firstName)
    .replace(/\{\{firmName\}\}/gi, contact?.company_name || '')
    .replace(/\{\{firm\}\}/gi, contact?.company_name || '')
    .replace(/\{\{company\}\}/gi, contact?.company_name || '')
    .replace(/\{\{title\}\}/gi, contact?.job_title || '')
    .replace(/\{\{pastInvestments\}\}/gi, contact?.past_investments || '')
    .replace(/\{\{investmentThesis\}\}/gi, contact?.investment_thesis || '')
    .replace(/\{\{whyThisFirm\}\}/gi, contact?.why_this_firm || '')
    .replace(/\{\{sectorFocus\}\}/gi, contact?.sector_focus || '')
    .replace(/\{\{investorGeography\}\}/gi, contact?.geography || '')
    .replace(/\{\{dealName\}\}/gi, deal?.name || '')
    .replace(/\{\{dealBrief\}\}/gi, clip(deal?.description || '', 220))
    .replace(/\{\{sector\}\}/gi, deal?.sector || '')
    .replace(/\{\{targetAmount\}\}/gi, formatAmount(deal?.target_amount || deal?.targetAmount, deal?.currency || 'GBP'))
    .replace(/\{\{keyMetrics\}\}/gi, clip(deal?.key_metrics || deal?.keyMetrics || '', 180))
    .replace(/\{\{geography\}\}/gi, deal?.geography || deal?.target_geography || '')
    .replace(/\{\{investorProfile\}\}/gi, clip(deal?.investor_profile || '', 140))
    .replace(/\{\{deckUrl\}\}/gi, deal?.deck_url || '')
    .replace(/\{\{callLink\}\}/gi, deal?.calendly_url || deal?.call_link || '')
    .replace(/\{\{senderName\}\}/gi, process.env.SENDER_NAME || 'Dom')
    .replace(/\{\{senderTitle\}\}/gi, process.env.SENDER_TITLE || '');
}

async function getLinkedInTemplate(type, dealId, contactId, explicitStepLabel = null) {
  try {
    const sb = getSupabase();
    if (!sb) return null;

    const stepLabel = explicitStepLabel || STEP_LABEL_MAP[type] || STEP_LABEL_MAP.intro;
    if (dealId && stepLabel) {
      const { data: dealTemplates } = await sb.from('deal_templates')
        .select('*')
        .eq('deal_id', dealId)
        .eq('sequence_step', stepLabel)
        .eq('is_primary', true)
        .limit(5);
      if (dealTemplates?.length) {
        const idx = dealTemplates.length > 1 ? (String(contactId || '').charCodeAt(0) % dealTemplates.length) : 0;
        return normaliseTemplate(dealTemplates[idx] || dealTemplates[0]);
      }
    }

    const { data: globalTemplates } = await sb.from('email_templates')
      .select('*')
      .eq('is_active', true)
      .in('type', ['linkedin', 'linkedin_dm'])
      .limit(50);
    const pool = globalTemplates || [];
    const patterns = TEMPLATE_NAME_PATTERNS[type] || TEMPLATE_NAME_PATTERNS.intro;
    const match = pool.find(t => patterns.some(p => String(t.name || '').toLowerCase().includes(p)))
      || pool.find(t => t.sequence_step === stepLabel)
      || pool[0];
    return match ? normaliseTemplate(match) : null;
  } catch {
    return null;
  }
}

function buildContactContext(contactPage, researchData) {
  return {
    name: getContactProp(contactPage, 'Name') || contactPage?.name || '',
    company_name: getContactProp(contactPage, 'Company Name') || contactPage?.company_name || '',
    job_title: getContactProp(contactPage, 'Job Title') || contactPage?.job_title || '',
    notes: getContactProp(contactPage, 'Notes') || contactPage?.notes || '',
    past_investments: getContactProp(contactPage, 'Past Investments') || researchData?.comparableDeals?.slice(0, 5).join(', ') || contactPage?.past_investments || '',
    investment_thesis: getContactProp(contactPage, 'Investment Thesis') || researchData?.approachAngle || contactPage?.investment_thesis || '',
    why_this_firm: getContactProp(contactPage, 'Why This Firm') || researchData?.whyThisFirm || contactPage?.why_this_firm || '',
    sector_focus: getContactProp(contactPage, 'Sector Focus') || contactPage?.sector_focus || '',
    geography: getContactProp(contactPage, 'Geography') || contactPage?.geography || '',
    typical_cheque_size: getContactProp(contactPage, 'Typical Cheque Size') || contactPage?.typical_cheque_size || '',
    aum: getContactProp(contactPage, 'AUM') || contactPage?.aum_fund_size || '',
  };
}

async function buildProfileContext(contactPage) {
  const identifier = contactPage?.linkedin_provider_id || contactPage?.linkedin_url || null;
  if (!identifier) return '';

  const profile = await retrieveLinkedInProfile(identifier).catch(() => null);
  if (!profile) return '';

  const experience = Array.isArray(profile.experience)
    ? profile.experience
      .map(role => [role?.title, role?.company_name || role?.company].filter(Boolean).join(' at '))
      .filter(Boolean)
      .slice(0, 2)
      .join('; ')
    : '';
  const skills = Array.isArray(profile.skills) ? profile.skills.slice(0, 5).join(', ') : '';

  return [
    `LinkedIn headline: ${profile.headline || 'Not available'}`,
    `LinkedIn summary: ${clip(profile.summary, 260) || 'Not available'}`,
    `Current role: ${[profile.current_title, profile.current_company].filter(Boolean).join(' at ') || 'Not available'}`,
    `Location: ${profile.location || 'Not available'}`,
    experience ? `Recent experience: ${experience}` : null,
    skills ? `Skills: ${skills}` : null,
  ].filter(Boolean).join('\n');
}

function formatConversationHistory(history = [], firstName = 'Them') {
  const rows = (history || [])
    .filter(item => item && (item.text || item.body || item.message))
    .slice(-12)
    .map(item => {
      const outbound = item.direction === 'outbound' || item.is_self || item.isFromUs;
      const role = outbound ? 'Dom' : firstName;
      const text = item.text || item.body || item.message || '';
      return `${role}: ${clip(text, 280)}`;
    });
  return rows.join('\n');
}

function buildDealBrief(deal) {
  if (!deal) return 'No deal context available.';
  const parts = [
    `Deal name: ${deal.name || 'Unknown'}`,
    `Sector: ${deal.sector || 'Unknown'}`,
    deal.raise_type || deal.deal_type ? `Structure: ${deal.raise_type || deal.deal_type}` : null,
    deal.target_amount || deal.targetAmount ? `Target amount: ${formatAmount(deal.target_amount || deal.targetAmount, deal.currency || 'GBP')}` : null,
    deal.geography || deal.target_geography ? `Geography: ${deal.geography || deal.target_geography}` : null,
    deal.key_metrics || deal.keyMetrics ? `Key metrics: ${clip(deal.key_metrics || deal.keyMetrics, 240)}` : null,
    deal.investor_profile ? `Target investor profile: ${clip(deal.investor_profile, 220)}` : null,
    deal.description ? `Brief: ${clip(deal.description, 450)}` : null,
  ];
  return parts.filter(Boolean).join('\n');
}

function buildAssetContext(deal) {
  const assets = [
    deal?.deck_url ? `Deck URL: ${deal.deck_url}` : null,
    deal?.calendly_url ? `Calendar link: ${deal.calendly_url}` : null,
    deal?.call_link ? `Call link: ${deal.call_link}` : null,
  ].filter(Boolean);
  return assets.length ? assets.join('\n') : 'No deal assets attached.';
}

export async function draftLinkedInDM(contactPage, researchData = null, type = 'intro', options = {}) {
  const contact = buildContactContext(contactPage, researchData);
  const firstName = contact.name?.split(' ')[0] || contact.name || 'there';
  const firm = contact.company_name || 'their firm';
  const deal = options.deal || getDeal();
  const maxChars = type === 'connection_request' ? 300 : 1000;
  const linkedinProfileContext = await buildProfileContext(contactPage);

  const template = await getLinkedInTemplate(
    type,
    deal?.id || null,
    contactPage?.id || null,
    options.sequenceStepLabel || null,
  );
  const fallbackTemplate = DEFAULT_LINKEDIN_TEMPLATES[type] || DEFAULT_LINKEDIN_TEMPLATES.intro;
  const templateBody = fillTemplate(template?.body_a || fallbackTemplate, { contact, deal });

  const conversationHistory = Array.isArray(options.conversationHistory) ? options.conversationHistory : [];
  const priorChatSummary = clip(options.priorChatSummary || '', 500);
  const guidanceBlock = await buildGuidanceBlock('investor_outreach').catch(() => '');

  const userPrompt = `${guidanceBlock}Draft a LinkedIn ${type === 'followup' ? 'follow-up DM' : 'DM'} from Dom to ${contact.name || firstName} at ${firm}.

USE THIS TEMPLATE AS THE DEFAULT STRUCTURE FOR THIS STEP:
${templateBody}

CONTACT CONTEXT:
Name: ${contact.name || 'Unknown'}
Firm: ${firm}
Title: ${contact.job_title || 'Unknown'}
Past investments: ${contact.past_investments || 'Not on record'}
Investment thesis / approach angle: ${contact.investment_thesis || 'Not on record'}
Why this firm matches: ${contact.why_this_firm || 'Not on record'}
Sector focus: ${contact.sector_focus || 'Not on record'}
Geography: ${contact.geography || 'Not on record'}
Cheque size / AUM: ${contact.typical_cheque_size || contact.aum || 'Not on record'}
Notes / CRM context: ${clip(contact.notes, 260) || 'None'}
${linkedinProfileContext ? `LinkedIn profile context:\n${linkedinProfileContext}` : ''}

FULL DEAL CONTEXT:
${buildDealBrief(deal)}

DEAL ASSETS:
${buildAssetContext(deal)}

${priorChatSummary ? `PRIOR CHAT SUMMARY:\n${priorChatSummary}\n` : ''}${conversationHistory.length ? `RECENT CONVERSATION HISTORY:\n${formatConversationHistory(conversationHistory, firstName)}\n` : 'RECENT CONVERSATION HISTORY:\nNo prior LinkedIn conversation found.\n'}

INSTRUCTIONS:
- Use the LinkedIn template as the starting point for this sequence step, but personalise it heavily.
- Use the deal brief, deal assets, and Train Your Agent guidance.
- When "Why this firm matches" is specific, use it as the main reason for the message.
- If prior conversation exists, continue naturally from that context instead of sounding like a cold opener.
- If there is no prior conversation, this is the first post-acceptance DM.
- Keep it comfortably under ${maxChars} characters.
- One clear ask.
- No placeholders. No stale deal references. Use only the deal context above.

Return only the message text.`;

  try {
    const text = await orComplete(userPrompt, { tier: 'draft', maxTokens: 512, systemPrompt: LINKEDIN_SYSTEM_PROMPT });
    const trimmed = text.trim();
    if (trimmed) {
      info(`LinkedIn DM drafted for ${firstName}`);
      return { body: trimmed.slice(0, maxChars), type, templateName: template?.name || 'Default LinkedIn DM' };
    }
    throw new Error('Empty model response');
  } catch (err) {
    warn(`LinkedIn DM draft failed for ${firstName}: ${err.message} — using filled template`);
  }

  if (templateBody) {
    return { body: templateBody.slice(0, maxChars), type, templateName: template?.name || 'Default LinkedIn DM' };
  }

  return {
    body: `${firstName}, thought ${deal?.name || 'this deal'} could be relevant given your work at ${firm}. Happy to send the deck or a tighter summary if useful.`,
    type,
    templateName: null,
  };
}

/**
 * Log a LinkedIn message to Supabase.
 */
export async function logLinkedInMessage({ dealId, contactId, direction, body, status }) {
  const { getSupabase } = await import('../core/supabase.js');
  const sb = getSupabase();
  if (!sb) return null;
  try {
    const { data } = await sb.from('linkedin_messages').insert([{
      deal_id: dealId,
      contact_id: contactId,
      direction,
      body,
      status,
      ...(direction === 'inbound' ? { received_at: new Date().toISOString() } : { sent_at: new Date().toISOString() }),
    }]).select().single();
    return data;
  } catch {
    return null;
  }
}
