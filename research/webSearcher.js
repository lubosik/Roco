// SerpAPI web search — fallback when Gemini is unavailable
export async function webSearch(query, num = 5) {
  const key = process.env.SERPAPI_KEY;
  if (!key) {
    // No SerpAPI key — return empty results gracefully
    return [];
  }

  const url = new URL('https://serpapi.com/search');
  url.searchParams.set('q', query);
  url.searchParams.set('num', String(num));
  url.searchParams.set('api_key', key);

  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`SerpAPI error: ${res.status}`);
  const data = await res.json();

  return (data.organic_results || []).map(r => ({
    title: r.title,
    link: r.link,
    snippet: r.snippet || '',
  }));
}
