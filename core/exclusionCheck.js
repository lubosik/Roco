// core/exclusionCheck.js
import { getSupabase } from './supabase.js';

// Cache exclusions per deal to avoid repeated DB calls
const exclusionCache = new Map();
const CACHE_TTL = 5 * 60_000; // 5 minutes

async function getExclusions(dealId) {
  const cached = exclusionCache.get(dealId);
  if (cached && Date.now() - cached.at < CACHE_TTL) return cached.data;

  const supabase = getSupabase();
  const { data } = await supabase.from('deal_exclusions')
    .select('firm_name, person_name, email')
    .eq('deal_id', dealId);

  const result = data || [];
  exclusionCache.set(dealId, { data: result, at: Date.now() });
  return result;
}

export function invalidateExclusionCache(dealId) {
  exclusionCache.delete(dealId);
}

export async function isExcluded(dealId, contact) {
  const exclusions = await getExclusions(dealId);
  if (!exclusions.length) return false;

  const firmNorm   = (contact.company_name || contact.firm || '').toLowerCase().trim();
  const personNorm = (contact.name || contact.full_name || '').toLowerCase().trim();
  const emailNorm  = (contact.email || '').toLowerCase().trim();
  // Angels and individuals (HNWIs) are excluded by person name or email only —
  // not by firm name, since their "firm" is often just their own name or generic
  const isPersonalInvestor = contact.is_angel
    || contact.contact_type === 'angel'
    || contact.contact_type === 'individual';

  for (const ex of exclusions) {
    // Exact email match always applies regardless of type
    if (ex.email && emailNorm && emailNorm === ex.email) return true;

    // Exact person name match always applies regardless of type
    if (ex.person_name && personNorm && personNorm === ex.person_name) return true;

    // Firm name match ONLY for institutional contacts — not angels or individuals
    if (!isPersonalInvestor && ex.firm_name && firmNorm) {
      if (firmNorm.includes(ex.firm_name) || ex.firm_name.includes(firmNorm)) return true;
    }
  }

  return false;
}
