import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { parse } from 'csv-parse/sync';
import chokidar from 'chokidar';
import { COLUMN_MAP, PIPELINE_STAGES, ENRICHMENT_STATUS } from '../config/constants.js';
import { createContact, getContactByLinkedIn, getContactByEmail, updateContact } from '../crm/notionContacts.js';
import { info, error } from '../core/logger.js';
import { sendTelegram } from '../approval/telegramBot.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const IMPORTS_DIR = path.join(__dirname, '../imports');
const PROCESSED_DIR = path.join(IMPORTS_DIR, 'processed');

export function startFileWatcher() {
  if (!fs.existsSync(IMPORTS_DIR)) fs.mkdirSync(IMPORTS_DIR, { recursive: true });
  if (!fs.existsSync(PROCESSED_DIR)) fs.mkdirSync(PROCESSED_DIR, { recursive: true });

  const watcher = chokidar.watch(path.join(IMPORTS_DIR, '*.csv'), {
    persistent: true,
    ignoreInitial: false,
    awaitWriteFinish: { stabilityThreshold: 2000 },
  });

  watcher.on('add', async (filePath) => {
    if (filePath.includes('/processed/')) return;
    info(`New CSV detected: ${path.basename(filePath)}`);
    await ingestCSV(filePath);
  });

  info('CSV file watcher started');
  return watcher;
}

async function ingestCSV(filePath) {
  const filename = path.basename(filePath);
  let created = 0;
  let updated = 0;
  let skipped = 0;

  try {
    const content = fs.readFileSync(filePath, 'utf8');
    const rows = parse(content, { columns: true, skip_empty_lines: true, trim: true });

    for (const row of rows) {
      try {
        const mapped = mapRow(row);
        if (!mapped.name) { skipped++; continue; }

        // Check for existing contact
        let existing = null;
        if (mapped.linkedinUrl) existing = await getContactByLinkedIn(mapped.linkedinUrl);
        if (!existing && mapped.email) existing = await getContactByEmail(mapped.email);

        if (existing) {
          // Update only missing fields
          const updates = {};
          if (!existing.properties['Email']?.email && mapped.email) updates.email = mapped.email;
          if (!existing.properties['LinkedIn URL']?.url && mapped.linkedinUrl) updates.linkedinUrl = mapped.linkedinUrl;
          if (!existing.properties['Title']?.rich_text?.[0] && mapped.title) updates.title = mapped.title;
          if (!existing.properties['Sector Focus']?.rich_text?.[0] && mapped.sector) updates.sectorFocus = mapped.sector;
          if (Object.keys(updates).length) {
            await updateContact(existing.id, updates);
            updated++;
          } else {
            skipped++;
          }
        } else {
          await createContact({
            name: mapped.name,
            email: mapped.email || null,
            linkedinUrl: mapped.linkedinUrl || null,
            title: mapped.title || null,
            sectorFocus: mapped.sector || null,
            chequeSize: mapped.chequeSize || null,
            geography: mapped.geography || null,
            source: `PitchBook Import — ${filename}`,
            pipelineStage: PIPELINE_STAGES.RESEARCHED,
            enrichmentStatus: ENRICHMENT_STATUS.PENDING,
            dateAdded: new Date().toISOString().split('T')[0],
          });
          created++;
        }
      } catch (err) {
        error(`Row ingest failed`, { row: row, err: err.message });
        skipped++;
      }
    }

    // Move to processed
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const destPath = path.join(PROCESSED_DIR, `${timestamp}_${filename}`);
    fs.renameSync(filePath, destPath);

    const summary = `CSV import complete: ${filename} — ${created} created, ${updated} updated, ${skipped} skipped`;
    info(summary);
    await sendTelegram(`ROCO — Import Complete\n\n${summary}`);
  } catch (err) {
    error(`CSV ingest failed for ${filename}`, { err: err.message });
  }
}

function mapRow(row) {
  const result = {};
  for (const [col, val] of Object.entries(row)) {
    const key = COLUMN_MAP[col] || COLUMN_MAP[col.trim()];
    if (key && val) result[key] = val.trim();
  }
  return result;
}
