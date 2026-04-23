/**
 * sourcing/companyEnricher.js
 * Enriches company_contacts with email/phone data via Apify.
 */

import { getSupabase } from '../core/supabase.js';
import { pushActivity } from '../dashboard/server.js';
import { enrichWithApify } from '../enrichment/apifyEnricher.js';

const MV_API_KEY = () => process.env.MILLIONVERIFIER_API_KEY;

async function verifyEmail(email) {
  const key = MV_API_KEY();
  if (!key || !email) return { valid: false, suggestion: null };
  try {
    const res = await fetch(`https://api.millionverifier.com/api/v3/?api=${key}&email=${encodeURIComponent(email)}`);
    if (!res.ok) return { valid: true, suggestion: null };
    const data = await res.json();
    const result = data.result || data.resultcode;
    const isValid = ['ok', 'valid', 'catch_all'].includes(String(result).toLowerCase());
    const suggestion = data.suggested_email || data.suggested || null;
    return { valid: isValid, suggestion };
  } catch {
    return { valid: true, suggestion: null };
  }
}

/**
 * Enrich a single company_contact.
 * @param {object} contact - company_contacts row
 * @param {object} campaign - sourcing_campaigns row
 */
export async function enrichCompanyContact(contact, campaign) {
  const sb = getSupabase();
  if (!sb) return;

  // GATE: skip contacts without a name
  if (!contact.name || contact.name.trim().toLowerCase() === 'null') {
    await sb.from('company_contacts').update({
      enrichment_status: 'skipped_no_name',
      updated_at: new Date().toISOString(),
    }).eq('id', contact.id).then(null, () => {});
    return;
  }

  // If we already have email but no LinkedIn — mark as email_only and queue for email outreach
  if (contact.email && !contact.linkedin_url) {
    await sb.from('company_contacts').update({
      enrichment_status: 'email_only',
      pipeline_stage: 'enriched',
      updated_at: new Date().toISOString(),
    }).eq('id', contact.id).then(null, () => {});
    pushActivity({ type: 'enrichment', action: 'Email only (no LinkedIn)', note: `[${campaign.name}]: ${contact.name}` });
    return;
  }

  // GATE: no LinkedIn URL and no email — nothing to enrich
  if (!contact.linkedin_url) {
    await sb.from('company_contacts').update({
      enrichment_status: 'skipped_no_linkedin',
      updated_at: new Date().toISOString(),
    }).eq('id', contact.id).then(null, () => {});
    return;
  }

  pushActivity({ type: 'enrichment', action: 'Enriching', note: `[${campaign.name}]: ${contact.name}` });
  console.log(`[SOURCING ENRICH] Enriching: ${contact.name}`);

  // ── APIFY ──────────────────────────────────────────────────────
  if (contact.linkedin_url) {
    try {
      const apifyResult = await enrichWithApify({ linkedinUrl: contact.linkedin_url, fullName: contact.name });

      if (apifyResult) {
        let email = apifyResult.email || null;
        let validEmail = email;

        if (email) {
          const { valid, suggestion } = await verifyEmail(email);
          if (!valid && suggestion) {
            validEmail = suggestion;
          } else if (!valid) {
            validEmail = null;
          }
        }

        const updates = {
          phone: apifyResult.phone || null,
          enrichment_status: validEmail ? 'enriched_apify' : 'linkedin_only',
          enrichment_source: 'apify',
          pipeline_stage: 'enriched',
          updated_at: new Date().toISOString(),
        };
        if (validEmail) updates.email = validEmail;

        await sb.from('company_contacts').update(updates).eq('id', contact.id).then(null, () => {});

        pushActivity({
          type: 'enrichment',
          action: validEmail ? 'Enriched via Apify' : 'LinkedIn-only (Apify no email)',
          note: `[${campaign.name}]: ${contact.name}`,
        });
        return;
      }
    } catch (err) {
      console.warn(`[SOURCING ENRICH] Apify error for ${contact.name}:`, err.message);
    }
  }

  // ── LINKEDIN ONLY FALLBACK ─────────────────────────────────────
  const finalStatus = contact.linkedin_url ? 'linkedin_only' : 'skipped_no_linkedin';
  await sb.from('company_contacts').update({
    enrichment_status: finalStatus,
    pipeline_stage: finalStatus === 'linkedin_only' ? 'enriched' : 'skipped_no_linkedin',
    updated_at: new Date().toISOString(),
  }).eq('id', contact.id).then(null, () => {});

  pushActivity({
    type: 'enrichment',
    action: finalStatus === 'linkedin_only' ? 'LinkedIn-only' : 'No email found',
    note: `[${campaign.name}]: ${contact.name}`,
  });
}

/**
 * Sync a company_contact to Notion contacts DB.
 * Reuses existing Notion contacts DB with Type = "Company Sourcing Target".
 */
export async function syncCompanyContactToNotion(contact, company, campaign) {
  try {
    const { createContact } = await import('../crm/notionContacts.js');
    await createContact({
      name:           contact.name,
      companyName:    company.company_name,
      jobTitle:       contact.title || '',
      email:          contact.email || '',
      phone:          contact.phone || '',
      linkedinUrl:    contact.linkedin_url || '',
      pipelineStage:  contact.pipeline_stage || 'researched',
      notes:          `[Campaign: ${campaign.name}] ${contact.notes || ''}`.trim(),
      type:           'Company Sourcing Target',
    });
  } catch (err) {
    console.warn(`[SOURCING ENRICH] Notion sync failed for ${contact.name}:`, err.message);
  }
}
