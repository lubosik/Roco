export function deriveOutreachEventStatus(eventType) {
  const normalized = String(eventType || '').toUpperCase();
  if (['LINKEDIN_INVITE_SENT', 'LINKEDIN_DM_SENT', 'EMAIL_SENT'].includes(normalized)) return 'confirmed';
  if (['LINKEDIN_INVITE_ALREADY_PENDING', 'LINKEDIN_ALREADY_CONNECTED'].includes(normalized)) return 'inferred';
  if (['LINKEDIN_INVITE_PROVIDER_LIMIT', 'LINKEDIN_INVITE_PROVIDER_LIMIT_ESCALATED'].includes(normalized)) return 'deferred';
  if (['LINKEDIN_INVITE_SKIPPED_NO_PROFILE'].includes(normalized)) return 'skipped';
  return 'failed';
}

export function normalizeComparableName(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\b(caia|cfa|mba|phd|md)\b/gi, ' ')
    .replace(/[^a-z0-9]+/gi, ' ')
    .trim()
    .toLowerCase();
}

export function getInvestorSearchProviderOrder({
  hasGrok = false,
  hasAnthropic = false,
  hasSerpApi = false,
} = {}) {
  const providers = [];
  if (hasGrok) providers.push('grok');
  if (hasAnthropic) providers.push('claude');
  if (hasSerpApi) providers.push('serpapi');
  return providers;
}
