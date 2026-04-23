/**
 * core/jarvisMemory.js
 * Persistent memory store for JARVIS — reads and writes to jarvis_memory table.
 *
 * Memory types:
 *   ACTION   — something JARVIS did ("Sent invite to X at Greenvolt")
 *   DECISION — why JARVIS chose something ("Suppressed Greenvolt — 2 strikes")
 *   LEARNING — pattern discovered ("Short subjects get 2x open rate")
 *   STRATEGY — deliberate shift ("Moved focus to infrastructure VCs")
 *   INTEL    — discovery about a firm/person ("Fund III closing Q3")
 *
 * Table required (run once in Supabase SQL editor):
 *
 *   CREATE TABLE jarvis_memory (
 *     id         uuid DEFAULT gen_random_uuid() PRIMARY KEY,
 *     deal_id    uuid REFERENCES deals(id) ON DELETE CASCADE,
 *     type       text NOT NULL,
 *     subject    text NOT NULL,
 *     content    text NOT NULL,
 *     tags       text[] DEFAULT '{}',
 *     metadata   jsonb DEFAULT '{}',
 *     created_at timestamptz DEFAULT now(),
 *     expires_at timestamptz
 *   );
 *   CREATE INDEX jarvis_memory_deal_id_idx  ON jarvis_memory(deal_id);
 *   CREATE INDEX jarvis_memory_tags_idx     ON jarvis_memory USING gin(tags);
 *   CREATE INDEX jarvis_memory_created_idx  ON jarvis_memory(created_at DESC);
 */

import { getSupabase } from './supabase.js';

export async function writeMemory(dealId, { type, subject, content, tags = [], metadata = {} }) {
  const sb = getSupabase();
  if (!sb) return null;
  try {
    const { data } = await sb.from('jarvis_memory').insert({
      deal_id:    dealId || null,
      type,
      subject,
      content,
      tags,
      metadata,
      created_at: new Date().toISOString(),
    }).select('id').single();
    return data?.id || null;
  } catch (err) {
    console.warn('[JARVIS MEM] write failed:', err.message);
    return null;
  }
}

export async function readMemories(dealId, { tags = [], type = null, limit = 20 } = {}) {
  const sb = getSupabase();
  if (!sb) return [];
  try {
    let q = sb.from('jarvis_memory')
      .select('id, type, subject, content, tags, metadata, created_at')
      .order('created_at', { ascending: false })
      .limit(limit);

    if (dealId)       q = q.eq('deal_id', dealId);
    if (type)         q = q.eq('type', type);
    if (tags.length)  q = q.overlaps('tags', tags);

    const { data } = await q;
    return data || [];
  } catch (err) {
    console.warn('[JARVIS MEM] read failed:', err.message);
    return [];
  }
}

/**
 * Builds a compact memory context string to inject into JARVIS's system prompt.
 * Pulls recent actions, learnings, and strategy shifts.
 */
export async function buildMemoryContext(dealId) {
  if (!dealId) return 'No prior memories.';
  try {
    const [actions, learnings, strategy, intel] = await Promise.all([
      readMemories(dealId, { type: 'ACTION',   limit: 8  }),
      readMemories(dealId, { type: 'LEARNING', limit: 5  }),
      readMemories(dealId, { type: 'STRATEGY', limit: 4  }),
      readMemories(dealId, { type: 'INTEL',    limit: 5  }),
    ]);

    const all = [...strategy, ...intel, ...learnings, ...actions]
      .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
      .slice(0, 18);

    if (!all.length) return 'No prior memories for this deal.';

    return all.map(m => {
      const d = new Date(m.created_at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
      return `[${d}] [${m.type}] ${m.subject}: ${m.content}`;
    }).join('\n');
  } catch {
    return '';
  }
}
