// A/B subject line tracking and optimisation
// Tracks which subject variants get better open rates via Notion

import { info } from '../core/logger.js';

const STRONG_SUBJECT_EXAMPLES = [
  'Thought of you on this one',
  'Quick one for you',
  'Relevant to what you are looking at?',
  'Worth 5 minutes?',
  'Something in your space',
];

export function getSubjectExamples() {
  return STRONG_SUBJECT_EXAMPLES;
}

// Track which subject was used and update Notion for A/B tracking
export async function recordSubjectUsed(contactPage, subject, variant) {
  info(`Subject used for ${contactPage.id}: variant=${variant} — "${subject}"`);
  // Variant is tracked via the Email Thread Link field or a separate notes entry
  // Future: aggregate open rate data when Gmail API confirms opens
}

// Recommend which variant to use based on historical performance
// Currently defaults to A — expand with ML-based selection as data grows
export function recommendVariant() {
  return 'A';
}
