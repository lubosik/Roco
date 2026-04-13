import test from 'node:test';
import assert from 'node:assert/strict';

import {
  deriveOutreachEventStatus,
  normalizeComparableName,
  getInvestorSearchProviderOrder,
} from '../core/hardeningHelpers.js';

test('invite send events are marked confirmed in outreach ledger mapping', () => {
  assert.equal(deriveOutreachEventStatus('LINKEDIN_INVITE_SENT'), 'confirmed');
  assert.equal(deriveOutreachEventStatus('LINKEDIN_DM_SENT'), 'confirmed');
  assert.equal(deriveOutreachEventStatus('EMAIL_SENT'), 'confirmed');
});

test('linkedin acceptance name normalization strips credentials and punctuation', () => {
  assert.equal(
    normalizeComparableName('Ugur Sarman, CAIA'),
    'ugur sarman'
  );
  assert.equal(
    normalizeComparableName('Mike Beauregard, CFA, MBA'),
    'mike beauregard'
  );
});

test('research fallback routing honors Grok then Claude then SerpApi order', () => {
  assert.deepEqual(
    getInvestorSearchProviderOrder({ hasGrok: true, hasAnthropic: true, hasSerpApi: true }),
    ['grok', 'claude', 'serpapi']
  );
  assert.deepEqual(
    getInvestorSearchProviderOrder({ hasGrok: false, hasAnthropic: true, hasSerpApi: true }),
    ['claude', 'serpapi']
  );
  assert.deepEqual(
    getInvestorSearchProviderOrder({ hasGrok: false, hasAnthropic: false, hasSerpApi: true }),
    ['serpapi']
  );
});
