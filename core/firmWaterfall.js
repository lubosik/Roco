const GENERIC_FIRM_NAMES = new Set([
  'angel investor', 'angel investors', 'independent investor', 'independent',
  'self-employed', 'self employed', 'freelance', 'freelancer', 'consultant',
  'private investor', 'individual investor', 'personal investment',
  'n/a', 'na', 'none', 'unknown', 'unknown firm',
]);

const RESPONDED_STAGES = new Set(['In Conversation', 'Replied', 'Meeting Booked', 'Meeting Scheduled']);
const DEAD_STAGES = new Set(['Inactive', 'inactive', 'Archived', 'archived', 'Skipped', 'skipped', 'Declined', 'declined']);
const PENDING_STAGES = new Set(['pending_email_approval', 'pending_dm_approval', 'Email Approved', 'DM Approved']);
const SENT_EMAIL_STAGES = new Set(['Email Sent', 'email_sent']);
const SENT_LINKEDIN_STAGES = new Set(['invite_sent', 'invite_accepted', 'DM Sent', 'dm_sent']);
const ACTIVE_APPROVAL_STATUSES = ['pending', 'approved', 'approved_waiting_for_window', 'sending'];

export function normalizeFirmWaterfallName(value) {
  return String(value || '').toLowerCase().replace(/\s+/g, ' ').trim();
}

export function isGenericWaterfallFirm(value) {
  const normalized = normalizeFirmWaterfallName(value);
  return !normalized || GENERIC_FIRM_NAMES.has(normalized);
}

function newerThan(value, days, now = Date.now()) {
  const timestamp = Date.parse(value || '');
  return Number.isFinite(timestamp) && (now - timestamp) <= days * 24 * 60 * 60 * 1000;
}

function describeContactBlock(row) {
  return {
    type: 'contact',
    contactId: row.id,
    name: row.name || null,
    firm: row.company_name || null,
    stage: row.pipeline_stage || null,
  };
}

/**
 * Returns a blocker object if another contact at the same firm is still inside
 * the waterfall patience window, or null if the next person may be contacted.
 */
export async function getFirmWaterfallBlock({
  sb,
  dealId,
  contactId = null,
  firm,
  emailDays = 3,
  linkedinDays = 2,
  includeApprovals = true,
} = {}) {
  if (!sb || !dealId || isGenericWaterfallFirm(firm)) return null;
  const normalizedFirm = normalizeFirmWaterfallName(firm);
  const now = Date.now();

  const { data: contacts } = await sb.from('contacts')
    .select('id, name, company_name, pipeline_stage, response_received, last_reply_at, last_email_sent_at, invite_sent_at, invite_accepted_at, dm_sent_at, last_outreach_at')
    .eq('deal_id', dealId)
    .ilike('company_name', firm)
    .limit(100);

  for (const row of contacts || []) {
    if (contactId && String(row.id) === String(contactId)) continue;
    if (normalizeFirmWaterfallName(row.company_name) !== normalizedFirm) continue;
    if (DEAD_STAGES.has(row.pipeline_stage)) continue;
    if (row.response_received || row.last_reply_at || RESPONDED_STAGES.has(row.pipeline_stage)) {
      return { ...describeContactBlock(row), reason: 'firm_responded' };
    }
    if (PENDING_STAGES.has(row.pipeline_stage)) {
      return { ...describeContactBlock(row), reason: 'firm_approval_or_send_pending' };
    }
    if (SENT_EMAIL_STAGES.has(row.pipeline_stage) && newerThan(row.last_email_sent_at || row.last_outreach_at, emailDays, now)) {
      return { ...describeContactBlock(row), reason: 'email_patience_window' };
    }
    if (SENT_LINKEDIN_STAGES.has(row.pipeline_stage)) {
      const sentAt = row.dm_sent_at || row.invite_accepted_at || row.invite_sent_at || row.last_outreach_at;
      // null sentAt = no date recorded → treat as stale, do not block
      if (sentAt && newerThan(sentAt, linkedinDays, now)) {
        return { ...describeContactBlock(row), reason: 'linkedin_patience_window' };
      }
    }
  }

  if (!includeApprovals) return null;
  const { data: approvals } = await sb.from('approval_queue')
    .select('id, contact_id, contact_name, firm, stage, status')
    .eq('deal_id', dealId)
    .ilike('firm', firm)
    .in('status', ACTIVE_APPROVAL_STATUSES)
    .limit(20);

  for (const row of approvals || []) {
    if (contactId && String(row.contact_id) === String(contactId)) continue;
    if (normalizeFirmWaterfallName(row.firm) !== normalizedFirm) continue;
    return {
      type: 'approval',
      approvalId: row.id,
      contactId: row.contact_id || null,
      name: row.contact_name || null,
      firm: row.firm || null,
      stage: row.stage || null,
      status: row.status || null,
      reason: 'firm_approval_active',
    };
  }

  return null;
}

export function formatFirmWaterfallBlock(block) {
  if (!block) return '';
  const who = [block.name, block.firm].filter(Boolean).join(' @ ') || block.firm || 'same firm';
  return `${who} is already in the firm waterfall (${block.reason || block.stage || 'active'})`;
}
