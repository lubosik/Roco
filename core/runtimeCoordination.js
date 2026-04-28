import { getSupabase } from './supabase.js';

const CACHE_TTL_MS = 15_000;
const cache = new Map();

function normalizeValue(raw) {
  if (raw == null) return null;
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw);
    } catch {
      return raw;
    }
  }
  return raw;
}

function stringifyValue(value) {
  return typeof value === 'string' ? value : JSON.stringify(value);
}

function readCache(key) {
  const entry = cache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.at > CACHE_TTL_MS) return null;
  return entry.value;
}

function writeCache(key, value) {
  cache.set(key, { value, at: Date.now() });
}

export async function readGlobalRuntimeSetting(key) {
  const cached = readCache(key);
  if (cached !== null) return cached;

  const sb = getSupabase();
  if (!sb) return null;

  try {
    const { data } = await sb.from('deal_settings')
      .select('id, value, updated_at')
      .eq('key', key)
      .order('updated_at', { ascending: false })
      .limit(5);
    const row = (data || [])[0];
    const value = row ? normalizeValue(row.value) : null;
    writeCache(key, value);
    return value;
  } catch (err) {
    console.warn('[RUNTIME] readGlobalRuntimeSetting failed:', err.message);
    return null;
  }
}

export async function writeGlobalRuntimeSetting(key, value) {
  const sb = getSupabase();
  if (!sb) return false;

  try {
    const encoded = stringifyValue(value);
    const { data: rows } = await sb.from('deal_settings')
      .select('id')
      .eq('key', key)
      .order('updated_at', { ascending: false })
      .limit(5);
    const [primary, ...duplicates] = rows || [];

    if (primary?.id) {
      await sb.from('deal_settings')
        .update({ value: encoded, updated_at: new Date().toISOString() })
        .eq('id', primary.id);
      if (duplicates.length) {
        const duplicateIds = duplicates.map(row => row.id).filter(Boolean);
        if (duplicateIds.length) {
          await sb.from('deal_settings').delete().in('id', duplicateIds).then(null, () => {});
        }
      }
    } else {
      await sb.from('deal_settings').insert({
        key,
        value: encoded,
        updated_at: new Date().toISOString(),
      });
    }

    writeCache(key, value);
    return true;
  } catch (err) {
    console.warn('[RUNTIME] writeGlobalRuntimeSetting failed:', err.message);
    return false;
  }
}
