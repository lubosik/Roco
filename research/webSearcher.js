import { orComplete } from '../core/openRouterClient.js';

/**
 * Web search via Perplexity sonar-pro (OpenRouter web tier).
 * Returns an array with a single result whose `snippet` holds the full response.
 * Format matches the old SerpAPI shape so callers don't need changes.
 */
export async function webSearch(query, _num = 5, _options = {}) {
  try {
    const result = await orComplete(query, { tier: 'web', maxTokens: 1500 });
    if (!result) return [];
    return [{ title: 'Web Research', source: 'perplexity', snippet: result, link: '', date: '' }];
  } catch (err) {
    console.warn('[WEB SEARCH] Perplexity failed:', err.message);
    return [];
  }
}

export async function getSerpAccountStatus() {
  return null;
}

export function formatWebResultsForPrompt(results = []) {
  return (results || [])
    .slice(0, 8)
    .map((item, index) => {
      const parts = [
        `${index + 1}. ${item.title || 'Untitled result'}`,
        item.source ? `Source: ${item.source}` : null,
        item.date ? `Date: ${item.date}` : null,
        item.snippet ? `Snippet: ${item.snippet}` : null,
        item.link ? `URL: ${item.link}` : null,
      ].filter(Boolean);
      return parts.join('\n');
    })
    .join('\n\n');
}
