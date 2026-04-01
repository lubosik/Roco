/**
 * enrichment/apifyEnricher.js
 * Apify LinkedIn scraper enrichment — fallback when KASPR returns no data.
 * Uses actor 2SyF0bVxmgGr8IVCZ which takes LinkedIn URLs and returns enriched data.
 */

const APIFY_TIMEOUT_MS = 3 * 60 * 1000; // 3 minutes max per contact

/**
 * Enrich a contact via Apify LinkedIn scraper.
 * @param {{ linkedin_url: string, name: string }} contact
 * @returns {{ email: string|null, phone: string|null, source: 'apify' } | null}
 */
export async function enrichWithApify(contact) {
  if (!contact.linkedin_url) return null;
  if (!process.env.APIFY_API_TOKEN) {
    console.warn('[APIFY] APIFY_API_TOKEN not set — skipping');
    return null;
  }

  const actorId = process.env.APIFY_LINKEDIN_SCRAPER_ACTOR_ID || '2SyF0bVxmgGr8IVCZ';
  const token = process.env.APIFY_API_TOKEN;

  console.log(`[APIFY] Enriching ${contact.name} via LinkedIn scraper...`);

  try {
    // Start actor run
    const runRes = await Promise.race([
      fetch(`https://api.apify.com/v2/acts/${actorId}/runs?token=${token}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ profileUrls: [contact.linkedin_url] }),
      }),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Apify start timeout')), 30000)),
    ]);

    if (!runRes.ok) {
      const err = await runRes.text().catch(() => '');
      console.warn(`[APIFY] Failed to start run for ${contact.name}: ${runRes.status} — ${err.substring(0, 150)}`);
      return null;
    }

    const runData = await runRes.json();
    const runId = runData.data?.id;
    const datasetId = runData.data?.defaultDatasetId;

    if (!runId) {
      console.warn(`[APIFY] No run ID returned for ${contact.name}`);
      return null;
    }

    // Poll for completion
    const startTime = Date.now();
    let status = 'RUNNING';

    while (status === 'RUNNING' || status === 'READY') {
      if (Date.now() - startTime > APIFY_TIMEOUT_MS) {
        console.warn(`[APIFY] Timeout waiting for run ${runId} for ${contact.name}`);
        // Try to abort
        fetch(`https://api.apify.com/v2/actor-runs/${runId}/abort?token=${token}`, { method: 'POST' }).catch(() => {});
        return null;
      }

      await sleep(5000); // Poll every 5 seconds

      try {
        const statusRes = await fetch(`https://api.apify.com/v2/actor-runs/${runId}?token=${token}`);
        if (statusRes.ok) {
          const statusData = await statusRes.json();
          status = statusData.data?.status || 'UNKNOWN';
        }
      } catch (err) {
        console.warn(`[APIFY] Status poll error for ${contact.name}:`, err.message);
      }
    }

    if (status !== 'SUCCEEDED') {
      console.warn(`[APIFY] Run ${runId} ended with status ${status} for ${contact.name}`);
      return null;
    }

    // Retrieve dataset items
    const itemsRes = await fetch(
      `https://api.apify.com/v2/datasets/${datasetId}/items?token=${token}&clean=true`
    );

    if (!itemsRes.ok) {
      console.warn(`[APIFY] Could not retrieve dataset for ${contact.name}`);
      return null;
    }

    const items = await itemsRes.json();

    if (!items || items.length === 0) {
      console.log(`[APIFY] No data returned for ${contact.name}`);
      return null;
    }

    const result = items[0];

    // Detect free-plan API block
    if (result.error) {
      console.warn(`[APIFY] Actor error for ${contact.name}: ${result.error}`);
      return null;
    }

    // Log raw keys for debugging
    console.log(`[APIFY] Raw keys for ${contact.name}:`, Object.keys(result).join(', '));

    const email = result.email || result.emailAddress || result.emails?.[0]
               || result.contactInfo?.email || result.personalEmail || result.workEmail || null;
    const phone = result.mobileNumber || result.phone || result.phoneNumber || result.phones?.[0]
               || result.contactInfo?.phone || null;

    // Extra profile fields — save even when there's no email
    const headline     = result.headline || null;
    const job_title    = result.jobTitle || result.headline?.split(' at ')?.[0] || null;
    const company_name = result.companyName || result.experiences?.[0]?.companyName || null;
    const company_website = result.companyWebsite || result.experiences?.[0]?.companyWebsite || null;
    // URN can be used as linkedin_provider_id for sending DMs/invites
    const linkedin_provider_id = result.urn || result.publicIdentifier || null;

    if (!email && !phone) {
      console.log(`[APIFY] No email or phone found for ${contact.name}${headline ? ` (${headline})` : ''}`);
      // Return partial data so orchestrator can still save profile enrichment
      if (headline || company_name || linkedin_provider_id) {
        return { email: null, phone: null, headline, job_title, company_name, company_website, linkedin_provider_id, source: 'apify' };
      }
      return null;
    }

    console.log(`[APIFY] Found data for ${contact.name}: email=${email ? 'yes' : 'no'}, phone=${phone ? 'yes' : 'no'}`);
    return { email, phone, headline, job_title, company_name, company_website, linkedin_provider_id, source: 'apify' };

  } catch (err) {
    console.warn(`[APIFY] Error enriching ${contact.name}:`, err.message);
    return null;
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
