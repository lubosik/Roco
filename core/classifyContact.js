// core/classifyContact.js
// Classifies a contact as angel, individual_at_firm, or firm entity.

const FIRM_INDICATORS = [
  'llc', 'lp', 'llp', 'inc', 'ltd', 'corp',
  'capital', 'partners', 'partner', 'management', 'fund', 'funds',
  'group', 'ventures', 'venture', 'equity', 'investments', 'investment',
  'advisors', 'advisory', 'associates', 'holdings', 'trust',
  'asset', 'assets', 'financial', 'finance', 'securities',
  'family office', 'solutions', 'services', 'consulting',
  'portfolio', 'global', 'international', 'strategies', 'strategy',
  'realty', 'real estate', 'properties', 'property',
];

const ANGEL_SIGNALS = [
  'angel', 'angel investor', 'independent investor', 'private investor',
  'individual investor', 'personal investment', 'seed investor',
  'founder investor', 'operator investor',
];

const INSTITUTIONAL_TITLES = [
  'managing director', 'managing partner', 'general partner', 'gp',
  'principal', 'director', 'vice president', 'vp', 'associate',
  'analyst', 'partner', 'head of', 'chief', 'cio', 'cfo', 'ceo',
  'portfolio manager', 'investment manager',
];

/**
 * Classify a contact as 'angel', 'individual_at_firm', or 'firm'.
 *
 * @param {{ firm_name?: string, job_title?: string, name?: string, notes?: string }} contact
 * @returns {{ contact_type: string, is_angel: boolean }}
 */
export function classifyContact({ firm_name, job_title, name, notes } = {}) {
  const firmLower  = (firm_name  || '').toLowerCase();
  const titleLower = (job_title  || '').toLowerCase();
  const notesLower = (notes      || '').toLowerCase();

  // 1. Angel signals in title or notes take priority
  if (ANGEL_SIGNALS.some(s => titleLower.includes(s) || notesLower.includes(s))) {
    return { contact_type: 'angel', is_angel: true };
  }

  // 2. No firm name — likely angel or self-employed
  if (!firm_name || firm_name.trim() === '') {
    return { contact_type: 'angel', is_angel: true };
  }

  // 3. Firm name matches person's own name (e.g. "John Smith" as firm) — angel
  const nameParts = (name || '').toLowerCase().split(/\s+/).filter(Boolean);
  const firmParts = firmLower.split(/\s+/).filter(Boolean);
  const firmIsPersonName = nameParts.length >= 2 &&
    nameParts.every(part => firmParts.includes(part));
  if (firmIsPersonName) {
    return { contact_type: 'angel', is_angel: true };
  }

  // 4. Firm name contains institutional indicator
  if (FIRM_INDICATORS.some(ind => firmLower.includes(ind))) {
    return { contact_type: 'individual_at_firm', is_angel: false };
  }

  // 5. Job title confirms institutional role
  if (INSTITUTIONAL_TITLES.some(t => titleLower.includes(t))) {
    return { contact_type: 'individual_at_firm', is_angel: false };
  }

  // 6. Default — firm name exists, assume individual at firm
  return { contact_type: 'individual_at_firm', is_angel: false };
}

export function getContactTypeLabel(contact_type, is_angel) {
  if (is_angel || contact_type === 'angel') return 'Angel Investor';
  if (contact_type === 'individual_at_firm') return 'Individual at Firm';
  if (contact_type === 'firm') return 'Firm';
  return 'Unknown';
}

export function getContactTypeColor(contact_type, is_angel) {
  if (is_angel || contact_type === 'angel') return '#f59e0b';   // amber
  if (contact_type === 'individual_at_firm') return '#60a5fa';  // blue
  if (contact_type === 'firm') return '#a78bfa';                // purple
  return '#6b7280';
}
