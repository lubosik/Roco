/**
 * services/guidanceService.js
 * Train Your Agent — loads and saves Roco's LLM guidance from Supabase.
 * All functions load fresh from DB on every call — never cached at startup.
 */

import { getSupabase } from '../core/supabase.js';

const INVESTOR_GUIDANCE_ID = '00000000-0000-0000-0000-000000000001';
const SOURCING_GUIDANCE_ID = '00000000-0000-0000-0000-000000000002';

export async function getInvestorGuidance() {
  try {
    const sb = getSupabase();
    if (!sb) return {};
    const { data } = await sb
      .from('roco_guidance_investor')
      .select('*')
      .eq('id', INVESTOR_GUIDANCE_ID)
      .single();
    return data || {};
  } catch { return {}; }
}

export async function getSourcingGuidance() {
  try {
    const sb = getSupabase();
    if (!sb) return {};
    const { data } = await sb
      .from('roco_guidance_sourcing')
      .select('*')
      .eq('id', SOURCING_GUIDANCE_ID)
      .single();
    return data || {};
  } catch { return {}; }
}

export async function saveInvestorGuidance(fields) {
  const sb = getSupabase();
  if (!sb) throw new Error('Database unavailable');
  const { data, error } = await sb
    .from('roco_guidance_investor')
    .update({ ...fields, updated_at: new Date().toISOString() })
    .eq('id', INVESTOR_GUIDANCE_ID)
    .select()
    .single();
  if (error) throw new Error(`Failed to save investor guidance: ${error.message}`);
  return data;
}

export async function saveSourcingGuidance(fields) {
  const sb = getSupabase();
  if (!sb) throw new Error('Database unavailable');
  const { data, error } = await sb
    .from('roco_guidance_sourcing')
    .update({ ...fields, updated_at: new Date().toISOString() })
    .eq('id', SOURCING_GUIDANCE_ID)
    .select()
    .single();
  if (error) throw new Error(`Failed to save sourcing guidance: ${error.message}`);
  return data;
}

/**
 * Build a guidance injection block for any LLM prompt.
 * Returns '' if no guidance has been configured.
 * @param {'investor_outreach'|'company_sourcing'} mode
 */
export async function buildGuidanceBlock(mode) {
  try {
    const guidance = mode === 'investor_outreach'
      ? await getInvestorGuidance()
      : await getSourcingGuidance();

    if (!guidance) return '';

    const parts = [];
    if (guidance.identity)          parts.push(`IDENTITY & PERSONA:\n${guidance.identity}`);
    if (guidance.voice_guidance)    parts.push(`VOICE & TONE RULES:\n${guidance.voice_guidance}`);
    if (guidance.search_guidance)   parts.push(`SEARCH GUIDANCE:\n${guidance.search_guidance}`);
    if (guidance.research_guidance) parts.push(`RESEARCH GUIDANCE:\n${guidance.research_guidance}`);
    if (guidance.scoring_guidance)  parts.push(`SCORING GUIDANCE:\n${guidance.scoring_guidance}`);
    if (guidance.outreach_guidance) parts.push(`OUTREACH GUIDANCE:\n${guidance.outreach_guidance}`);
    if (guidance.reply_guidance)    parts.push(`REPLY & CONVERSATION GUIDANCE:\n${guidance.reply_guidance}`);
    if (guidance.closing_guidance)  parts.push(`CLOSING GUIDANCE:\n${guidance.closing_guidance}`);

    if (parts.length === 0) return '';

    return `\n===== ROCO AGENT GUIDANCE (FOLLOW THESE RULES EXACTLY) =====\n${parts.join('\n\n')}\n===== END OF GUIDANCE =====\n`;
  } catch (err) {
    console.warn('[GUIDANCE] Failed to load guidance block:', err.message);
    return '';
  }
}
