import { NOTION_VERSION, NOTION_BASE } from '../config/constants.js';

function headers() {
  return {
    Authorization: `Bearer ${process.env.NOTION_API_KEY}`,
    'Notion-Version': NOTION_VERSION,
    'Content-Type': 'application/json',
  };
}

const DB = () => process.env.NOTION_COMPANIES_DB_ID;

async function query(filter = {}) {
  const body = Object.keys(filter).length ? { filter } : {};
  const res = await fetch(`${NOTION_BASE}/databases/${DB()}/query`, {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Notion companies query failed: ${JSON.stringify(data)}`);
  return data.results;
}

export async function getAllCompanies() {
  return query();
}

export async function getCompanyByName(name) {
  const results = await query({
    property: 'Company Name',
    title: { equals: name },
  });
  return results[0] || null;
}

export async function createCompany(fields) {
  const res = await fetch(`${NOTION_BASE}/pages`, {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify({
      parent: { database_id: DB() },
      properties: buildProperties(fields),
    }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Create company failed: ${JSON.stringify(data)}`);
  return data;
}

export async function updateCompany(pageId, fields) {
  const res = await fetch(`${NOTION_BASE}/pages/${pageId}`, {
    method: 'PATCH',
    headers: headers(),
    body: JSON.stringify({ properties: buildProperties(fields) }),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`Update company failed: ${JSON.stringify(data)}`);
  return data;
}

export async function suppressFirm(pageId) {
  return updateCompany(pageId, { status: 'Suppressed', pipelineStage: 'Suppressed — Firm Decline' });
}

export function getCompanyProp(page, prop) {
  const p = page.properties?.[prop];
  if (!p) return null;
  switch (p.type) {
    case 'title': return p.title?.[0]?.plain_text || null;
    case 'rich_text': return p.rich_text?.[0]?.plain_text || null;
    case 'url': return p.url || null;
    case 'number': return p.number ?? null;
    case 'select': return p.select?.name || null;
    case 'date': return p.date?.start || null;
    case 'checkbox': return p.checkbox ?? false;
    case 'relation': return p.relation?.map(r => r.id) || [];
    default: return null;
  }
}

function buildProperties(fields) {
  const props = {};
  if (fields.name !== undefined)
    props['Company Name'] = { title: [{ text: { content: fields.name } }] };
  if (fields.type !== undefined)
    props['Type'] = { select: { name: fields.type } };
  if (fields.sectorFocus !== undefined)
    props['Sector Focus'] = { rich_text: [{ text: { content: fields.sectorFocus } }] };
  if (fields.chequeSize !== undefined)
    props['Typical Cheque Size ($)'] = { rich_text: [{ text: { content: fields.chequeSize } }] };
  if (fields.geography !== undefined)
    props['Geography'] = { rich_text: [{ text: { content: fields.geography } }] };
  if (fields.aum !== undefined)
    props['AUM / Fund Size'] = { rich_text: [{ text: { content: fields.aum } }] };
  if (fields.website !== undefined)
    props['Website'] = { url: fields.website };
  if (fields.linkedinPage !== undefined)
    props['LinkedIn Page'] = { url: fields.linkedinPage };
  if (fields.source !== undefined)
    props['Source'] = { select: { name: fields.source } };
  if (fields.score !== undefined)
    props['Investor Score (0-100)'] = { number: fields.score };
  if (fields.totalContacts !== undefined)
    props['Total Contacts'] = { number: fields.totalContacts };
  if (fields.contactNames !== undefined)
    props['Contact Names'] = { relation: Array.isArray(fields.contactNames) ? fields.contactNames.map(id => ({ id })) : [] };
  if (fields.activeContacts !== undefined)
    props['Active Contacts'] = { number: fields.activeContacts };
  if (fields.pipelineStage !== undefined)
    props['Pipeline Stage'] = { select: { name: fields.pipelineStage } };
  if (fields.lastOutreachDate !== undefined)
    props['Last Outreach Date'] = { date: { start: fields.lastOutreachDate } };
  if (fields.bestContact !== undefined)
    props['Best Contact'] = { rich_text: [{ text: { content: fields.bestContact } }] };
  if (fields.notes !== undefined)
    props['Notes'] = { rich_text: [{ text: { content: fields.notes } }] };
  if (fields.dateAdded !== undefined)
    props['Date Added'] = { date: { start: fields.dateAdded } };
  if (fields.status !== undefined)
    props['Status'] = { select: { name: fields.status } };
  return props;
}
