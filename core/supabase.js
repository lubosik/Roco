/**
 * core/supabase.js — Single Supabase client for all modules
 * Uses service role key — never expose to browser
 *
 * Reads env vars lazily (inside getSupabase()) so dotenv has time to load
 * before the first actual call, regardless of module import order.
 */
import { createClient } from '@supabase/supabase-js';

let _client = null;

export function getSupabase() {
  if (!_client) {
    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_KEY;
    if (!url || !key) {
      console.warn('[supabase] SUPABASE_URL or SUPABASE_SERVICE_KEY not set — Supabase disabled');
      return null;
    }
    _client = createClient(url, key, {
      auth: { persistSession: false },
    });
  }
  return _client;
}

export default getSupabase;
