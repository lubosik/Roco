// Apify LinkedIn scraper integration
// Requires APIFY_API_KEY in .env
import { info, error } from '../core/logger.js';

const APIFY_ACTOR = 'apify/linkedin-profile-scraper';

export async function scrapeLinkedInProfile(linkedinUrl) {
  const key = process.env.APIFY_API_KEY;
  if (!key) {
    info('APIFY_API_KEY not set — LinkedIn scraping skipped');
    return null;
  }

  try {
    // Start actor run
    const startRes = await fetch(
      `https://api.apify.com/v2/acts/${APIFY_ACTOR}/runs?token=${key}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ startUrls: [{ url: linkedinUrl }] }),
      }
    );
    if (!startRes.ok) throw new Error(`Apify start failed: ${startRes.status}`);
    const { data: run } = await startRes.json();

    // Poll for completion (max 60s)
    let dataset = null;
    for (let i = 0; i < 12; i++) {
      await new Promise(r => setTimeout(r, 5000));
      const statusRes = await fetch(
        `https://api.apify.com/v2/acts/${APIFY_ACTOR}/runs/${run.id}?token=${key}`
      );
      const { data: status } = await statusRes.json();
      if (status.status === 'SUCCEEDED') {
        const dataRes = await fetch(
          `https://api.apify.com/v2/datasets/${status.defaultDatasetId}/items?token=${key}`
        );
        dataset = await dataRes.json();
        break;
      }
      if (status.status === 'FAILED' || status.status === 'ABORTED') break;
    }

    return dataset?.[0] || null;
  } catch (err) {
    error('LinkedIn scraper failed', { url: linkedinUrl, err: err.message });
    return null;
  }
}
