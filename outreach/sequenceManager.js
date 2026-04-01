import { getContactProp, updateContact } from '../crm/notionContacts.js';
import { FOLLOW_UP_DAYS, PIPELINE_STAGES } from '../config/constants.js';
import { info } from '../core/logger.js';

export function calculateNextFollowUpDate(sentDate, followUpNumber) {
  const days = FOLLOW_UP_DAYS[followUpNumber - 1];
  if (!days) return null; // No more follow-ups after sequence complete

  const date = new Date(sentDate);
  date.setDate(date.getDate() + days);
  return date.toISOString().split('T')[0];
}

export async function scheduleFollowUp(contactPage, followUpNumber, sentDate = new Date().toISOString()) {
  const name = getContactProp(contactPage, 'Name');
  const nextDate = calculateNextFollowUpDate(sentDate, followUpNumber);

  if (!nextDate) {
    info(`${name} — sequence complete (${FOLLOW_UP_DAYS.length} follow-ups exhausted)`);
    await updateContact(contactPage.id, { pipelineStage: PIPELINE_STAGES.INACTIVE });
    return null;
  }

  await updateContact(contactPage.id, {
    nextFollowUpDate: nextDate,
    followUpNumber,
  });

  info(`${name} — follow-up ${followUpNumber} scheduled for ${nextDate}`);
  return nextDate;
}

export async function cancelFollowUps(contactPage) {
  const name = getContactProp(contactPage, 'Name');
  await updateContact(contactPage.id, {
    nextFollowUpDate: null,
    followUpNumber: 0,
  });
  info(`${name} — follow-up sequence cancelled (reply received)`);
}

export function getFollowUpStage(followUpNumber) {
  const stages = {
    0: PIPELINE_STAGES.EMAIL_SENT,
    1: PIPELINE_STAGES.FOLLOW_UP_1,
    2: PIPELINE_STAGES.FOLLOW_UP_2,
    3: PIPELINE_STAGES.FOLLOW_UP_3,
  };
  return stages[followUpNumber] || PIPELINE_STAGES.EMAIL_SENT;
}

export function getEmailStageLabel(followUpNumber) {
  const labels = {
    0: 'INTRO',
    1: 'FOLLOW-UP 1',
    2: 'FOLLOW-UP 2',
    3: 'FOLLOW-UP 3',
  };
  return labels[followUpNumber] || 'INTRO';
}
