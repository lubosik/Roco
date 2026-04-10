import { getSupabase } from './supabase.js';

export async function writeMemory(dealId, memoryPatch = {}) {
  if (!dealId || !memoryPatch || typeof memoryPatch !== 'object') return false;

  const sb = getSupabase();
  if (!sb) return false;

  try {
    const { data: existing } = await sb.from('deal_settings')
      .select('id, value')
      .eq('deal_id', dealId)
      .eq('key', 'ROCO_MEMORY')
      .limit(1)
      .maybeSingle();

    let current = {};
    if (existing?.value) {
      try {
        current = typeof existing.value === 'string' ? JSON.parse(existing.value) : existing.value;
      } catch {
        current = {};
      }
    }

    const nextValue = JSON.stringify({
      ...current,
      ...memoryPatch,
      updated_at: new Date().toISOString(),
    });

    await sb.from('deal_settings').upsert({
      id: existing?.id || undefined,
      deal_id: dealId,
      key: 'ROCO_MEMORY',
      value: nextValue,
    }, { onConflict: 'deal_id,key' });

    return true;
  } catch (err) {
    console.warn('[ROCO MEMORY] writeMemory failed:', err.message);
    return false;
  }
}
