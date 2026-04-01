export const ORCHESTRATOR_INTERVAL_MS = 10 * 60 * 1000; // 15 minutes
export const INBOX_POLL_INTERVAL_MS = 5 * 60 * 1000;    // 5 minutes
export const MIN_PIPELINE_SIZE = 50;
export const RESEARCH_CACHE_DAYS = 7;
export const KASPR_RETRY_DAYS = 30;
export const MAX_EDIT_LOOPS = 3;

export const FOLLOW_UP_DAYS = [3, 7, 14];

export const PIPELINE_STAGES = {
  RESEARCHED: 'Researched',
  ENRICHED: 'Enriched',
  EMAIL_SENT: 'Intro Sent',
  FOLLOW_UP_1: 'Follow-up 1 Sent',
  FOLLOW_UP_2: 'Follow-up 2 Sent',
  FOLLOW_UP_3: 'Follow-up 3 Sent',
  REPLIED: 'Replied',
  MEETING_SET: 'Meeting Set',
  CLOSED: 'Closed',
  SUPPRESSED: 'Suppressed — Firm Decline',
  INACTIVE: 'Inactive',
  SKIPPED: 'Skipped by Dom',
};

export const ENRICHMENT_STATUS = {
  PENDING: 'Pending',
  COMPLETE: 'Complete',
  NO_DATA: 'No Data',
};

export const SCORE_THRESHOLDS = {
  HOT: 80,
  WARM: 60,
  POSSIBLE: 40,
};

export const REPLY_CLASSIFICATIONS = {
  INTERESTED: 'INTERESTED',
  NEEDS_MORE_INFO: 'NEEDS_MORE_INFO',
  SOFT_DECLINE: 'SOFT_DECLINE',
  HARD_DECLINE: 'HARD_DECLINE',
  OPT_OUT: 'OPT_OUT',
  NEUTRAL: 'NEUTRAL',
};

export const NOTION_VERSION = '2022-06-28';
export const NOTION_BASE = 'https://api.notion.com/v1';

export const COLUMN_MAP = {
  'Contact Name': 'name',
  'Full Name': 'name',
  'Name': 'name',
  'LinkedIn': 'linkedinUrl',
  'LinkedIn URL': 'linkedinUrl',
  'LinkedIn Profile': 'linkedinUrl',
  'Email': 'email',
  'Email Address': 'email',
  'Firm': 'firm',
  'Company': 'firm',
  'Company Name': 'firm',
  'Title': 'title',
  'Job Title': 'title',
  'Position': 'title',
  'Sector': 'sector',
  'Focus': 'sector',
  'Cheque Size': 'chequeSize',
  'Investment Size': 'chequeSize',
  'Geography': 'geography',
  'Region': 'geography',
};
