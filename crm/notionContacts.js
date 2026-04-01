import { NOTION_VERSION, NOTION_BASE, ENRICHMENT_STATUS } from '../config/constants.js';

function headers() {
  return {
    Authorization: `Bearer ${process.env.NOTION_API_KEY}`,
    'Notion-Version': NOTION_VERSION,
    'Content-Type': 'application/json',
  };
}

const DB = () => process.env.NOTION_CONTACTS_DB_ID;

async function query(filter = {}, sorts = []) {
  const body = {};
  if (Object.keys(filter).length) body.filter = filter;
  if (sorts.length) body.sorts = sorts;

  const res = await fetch(`${NOTION_BASE}/databases/${DB()}/query`, {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Notion query failed: ${JSON.stringify(data)}`);
  return data.results;
}

export async function getContactsByEnrichmentStatus(status) {
  return query({
    property: 'Enrichment Status',
    select: { equals: status },
  });
}

export async function getContactsByPipelineStage(stage) {
  return query({
    property: 'Pipeline Stage',
    select: { equals: stage },
  });
}

export async function getContactsDueFollowUp() {
  const today = new Date().toISOString().split('T')[0];
  return query({
    and: [
      { property: 'Next Follow-up Date', date: { on_or_before: today } },
      { property: 'Pipeline Stage', select: { does_not_equal: 'Inactive' } },
      { property: 'Pipeline Stage', select: { does_not_equal: 'Suppressed — Firm Decline' } },
      { property: 'Pipeline Stage', select: { does_not_equal: 'Deleted — Do Not Contact' } },
    ],
  });
}

export async function getContactByEmail(email) {
  const results = await query({
    property: 'Email',
    email: { equals: email },
  });
  return results[0] || null;
}

export async function getContactByLinkedIn(url) {
  const results = await query({
    property: 'LinkedIn URL',
    url: { equals: url },
  });
  return results[0] || null;
}

export async function getContactsByFirm(firmPageId) {
  return query({
    property: 'Firm',
    relation: { contains: firmPageId },
  });
}

export async function getAllActiveContacts() {
  return query({
    and: [
      { property: 'Pipeline Stage', select: { does_not_equal: 'Inactive' } },
      { property: 'Pipeline Stage', select: { does_not_equal: 'Suppressed — Firm Decline' } },
      { property: 'Pipeline Stage', select: { does_not_equal: 'Deleted — Do Not Contact' } },
      { property: 'Pipeline Stage', select: { does_not_equal: 'Skipped by Dom' } },
    ],
  });
}

export async function getContactsByDeal(dealName) {
  return query({
    and: [
      { property: 'Deal Name', rich_text: { equals: dealName } },
      { property: 'Pipeline Stage', select: { does_not_equal: 'Inactive' } },
      { property: 'Pipeline Stage', select: { does_not_equal: 'Deleted — Do Not Contact' } },
    ],
  });
}

export async function archiveContact(pageId) {
  const res = await fetch(`${NOTION_BASE}/pages/${pageId}`, {
    method: 'PATCH',
    headers: headers(),
    body: JSON.stringify({ archived: true }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Archive contact failed: ${JSON.stringify(data)}`);
  return data;
}

export async function createContact(fields) {
  const res = await fetch(`${NOTION_BASE}/pages`, {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify({
      parent: { database_id: DB() },
      properties: buildProperties(fields),
    }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Create contact failed: ${JSON.stringify(data)}`);
  return data;
}

export async function updateContact(pageId, fields) {
  const res = await fetch(`${NOTION_BASE}/pages/${pageId}`, {
    method: 'PATCH',
    headers: headers(),
    body: JSON.stringify({ properties: buildProperties(fields) }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Update contact failed: ${JSON.stringify(data)}`);
  return data;
}

export function getContactProp(page, prop) {
  const p = page.properties?.[prop];
  if (!p) return null;
  switch (p.type) {
    case 'title': return p.title?.[0]?.plain_text || null;
    case 'rich_text': return p.rich_text?.[0]?.plain_text || null;
    case 'email': return p.email || null;
    case 'url': return p.url || null;
    case 'number': return p.number ?? null;
    case 'select': return p.select?.name || null;
    case 'date': return p.date?.start || null;
    case 'phone_number': return p.phone_number || null;
    case 'checkbox': return p.checkbox ?? false;
    case 'relation': return p.relation?.map(r => r.id) || [];
    default: return null;
  }
}

function buildProperties(fields) {
  const props = {};
  if (fields.name !== undefined)
    props['Name'] = { title: [{ text: { content: fields.name } }] };
  if (fields.email !== undefined)
    props['Email'] = { email: fields.email };
  if (fields.phone !== undefined)
    props['Phone'] = { phone_number: fields.phone };
  if (fields.linkedinUrl !== undefined)
    props['LinkedIn URL'] = { url: fields.linkedinUrl };
  if (fields.title !== undefined)
    props['Title'] = { rich_text: [{ text: { content: fields.title || '' } }] };
  if (fields.dealName !== undefined)
    props['Deal Name'] = { rich_text: [{ text: { content: fields.dealName || '' } }] };
  if (fields.score !== undefined)
    props['Investor Score (0-100)'] = { number: fields.score };
  if (fields.sectorFocus !== undefined)
    props['Sector Focus'] = { rich_text: [{ text: { content: fields.sectorFocus || '' } }] };
  if (fields.chequeSize !== undefined)
    props['Typical Cheque Size ($)'] = { rich_text: [{ text: { content: fields.chequeSize || '' } }] };
  if (fields.geography !== undefined)
    props['Geography'] = { rich_text: [{ text: { content: fields.geography || '' } }] };
  if (fields.source !== undefined)
    props['Source'] = { rich_text: [{ text: { content: fields.source || '' } }] };
  if (fields.pipelineStage !== undefined)
    props['Pipeline Stage'] = { select: { name: fields.pipelineStage } };
  if (fields.dateAdded !== undefined)
    props['Date Added'] = { date: { start: fields.dateAdded } };
  if (fields.lastContacted !== undefined)
    props['Last Contacted'] = { date: { start: fields.lastContacted } };
  if (fields.lastContactType !== undefined)
    props['Last Contact Type'] = { select: { name: fields.lastContactType } };
  if (fields.lastContactNotes !== undefined)
    props['Last Contact Notes'] = { rich_text: [{ text: { content: fields.lastContactNotes || '' } }] };
  if (fields.nextFollowUpDate !== undefined)
    props['Next Follow-up Date'] = { date: { start: fields.nextFollowUpDate } };
  if (fields.followUpNumber !== undefined)
    props['Follow-up Number'] = { number: fields.followUpNumber };
  if (fields.responseReceived !== undefined)
    props['Response Received (Y/N)'] = { checkbox: fields.responseReceived === true || fields.responseReceived === 'Yes' };
  if (fields.responseSummary !== undefined)
    props['Response Summary'] = { rich_text: [{ text: { content: fields.responseSummary || '' } }] };
  if (fields.domApproved !== undefined)
    props['Dom Approved (Y/N)'] = { checkbox: fields.domApproved === true || fields.domApproved === 'Yes' };
  if (fields.emailThreadLink !== undefined)
    props['Email Thread Link'] = { url: fields.emailThreadLink };
  if (fields.linkedinDmStatus !== undefined)
    props['LinkedIn DM Status'] = { select: { name: fields.linkedinDmStatus } };
  if (fields.pitchbookRef !== undefined)
    props['PitchBook / DB Reference'] = { rich_text: [{ text: { content: fields.pitchbookRef || '' } }] };
  if (fields.similarPastDeals !== undefined)
    props['Similar Past Deals'] = { rich_text: [{ text: { content: fields.similarPastDeals || '' } }] };
  if (fields.enrichmentStatus !== undefined)
    props['Enrichment Status'] = { select: { name: fields.enrichmentStatus } };
  if (fields.notes !== undefined)
    props['Notes'] = { rich_text: [{ text: { content: fields.notes || '' } }] };
  if (fields.companyRelation !== undefined)
    props['Firm'] = { relation: [{ id: fields.companyRelation }] };
  return props;
}

export async function countActiveContacts() {
  const all = await getAllActiveContacts();
  return all.length;
}
