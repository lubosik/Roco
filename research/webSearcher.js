function getSerpApiKey() {
  return process.env.SERP_API_KEY || process.env.SERPAPI_KEY || '';
}

function normalizeOrganicResult(result = {}) {
  return {
    position: result.position || null,
    title: result.title || '',
    link: result.link || result.redirect_link || '',
    source: result.source || result.displayed_link || '',
    snippet: result.snippet || '',
    date: result.date || '',
  };
}

export async function webSearch(query, num = 5, options = {}) {
  const key = getSerpApiKey();
  if (!key) return [];

  const url = new URL('https://serpapi.com/search.json');
  url.searchParams.set('engine', options.engine || 'google');
  url.searchParams.set('q', query);
  url.searchParams.set('num', String(Math.min(Math.max(Number(num) || 5, 1), 10)));
  url.searchParams.set('api_key', key);
  if (options.hl) url.searchParams.set('hl', options.hl);
  if (options.gl) url.searchParams.set('gl', options.gl);
  if (options.location) url.searchParams.set('location', options.location);

  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`SerpAPI error: ${res.status}`);
  const data = await res.json();
  if (data?.error) throw new Error(`SerpAPI error: ${data.error}`);

  return (data.organic_results || []).map(normalizeOrganicResult);
}

export async function getSerpAccountStatus() {
  const key = getSerpApiKey();
  if (!key) return null;
  const url = new URL('https://serpapi.com/account.json');
  url.searchParams.set('api_key', key);
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`SerpAPI account error: ${res.status}`);
  return res.json();
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
