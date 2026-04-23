import { getContactsByFirm, updateContact, getContactProp } from '../crm/notionContacts.js';
import { suppressFirm, getCompanyByName, getCompanyProp } from '../crm/notionCompanies.js';
import { logActivity } from '../crm/notionLogger.js';
import { sendTelegram } from '../approval/telegramBot.js';
import { PIPELINE_STAGES, REPLY_CLASSIFICATIONS } from '../config/constants.js';
import { info, error } from '../core/logger.js';
import { orComplete } from '../core/openRouterClient.js';

export async function classifyReply(emailBody) {
  const prompt = `You are analysing an email reply from an investor. Classify it as one of:
INTERESTED | NEEDS_MORE_INFO | SOFT_DECLINE | HARD_DECLINE | OPT_OUT | NEUTRAL

Reply text:
"""
${emailBody.slice(0, 1000)}
"""

Return ONLY valid JSON: { "classification": "...", "confidence": 0-100, "reason": "one sentence" }`;

  try {
    const text = await orComplete(prompt, { tier: 'classify', maxTokens: 256 });
    const match = text.match(/\{[\s\S]*\}/);
    if (match) return JSON.parse(match[0]);
  } catch (err) {
    error('Reply classification failed', { err: err.message });
  }
  return { classification: REPLY_CLASSIFICATIONS.NEUTRAL, confidence: 0, reason: 'Classification failed' };
}

export async function handleFirmSuppression(triggerContact, firmName, reason) {
  // Angels represent only themselves — skip firm-wide suppression
  const isAngel = getContactProp(triggerContact, 'is_angel') ||
    getContactProp(triggerContact, 'contact_type') === 'angel';
  if (isAngel) {
    info(`[SUPPRESSION] ${getContactProp(triggerContact, 'Name')} is angel — individual only, no firm suppression`);
    return;
  }

  info(`Suppressing firm: ${firmName} — reason: ${reason}`);

  try {
    // Suppress company record
    const company = await getCompanyByName(firmName);
    let companyId = null;
    if (company) {
      companyId = company.id;
      await suppressFirm(company.id);
    }

    // Find and suppress all contacts at this firm
    let affectedContacts = [];
    if (companyId) {
      affectedContacts = await getContactsByFirm(companyId);
    }

    const triggerName = getContactProp(triggerContact, 'Name');
    const today = new Date().toISOString().split('T')[0];

    for (const contact of affectedContacts) {
      const cName = getContactProp(contact, 'Name');
      await updateContact(contact.id, {
        pipelineStage: PIPELINE_STAGES.SUPPRESSED,
        nextFollowUpDate: null,
        followUpNumber: 0,
        lastContactNotes: `Firm suppressed on ${today}: ${reason}`,
      });
      info(`Suppressed contact: ${cName} at ${firmName}`);
    }

    const suppressed = affectedContacts.length;

    await logActivity(
      'Firm Suppression',
      triggerName,
      firmName,
      `Triggered by: ${triggerName}. Reason: ${reason}. ${suppressed} contacts paused.`,
      'Suppression'
    );

    await sendTelegram(
      `ROCO — Firm Suppressed\n\n` +
      `Firm: ${firmName}\n` +
      `Triggered by: ${triggerName}\n` +
      `Reason: ${reason}\n` +
      `${suppressed} contact(s) at this firm have been paused.`
    );

    info(`Firm suppression complete: ${firmName} — ${suppressed} contacts paused`);
  } catch (err) {
    error(`Firm suppression failed for ${firmName}`, { err: err.message });
  }
}

export function isDecline(classification) {
  return [
    REPLY_CLASSIFICATIONS.SOFT_DECLINE,
    REPLY_CLASSIFICATIONS.HARD_DECLINE,
    REPLY_CLASSIFICATIONS.OPT_OUT,
  ].includes(classification);
}
