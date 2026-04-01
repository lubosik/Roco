import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEAL_FILE = path.join(__dirname, '../deal.json');

let _deal = null;

export function loadDeal() {
  if (!fs.existsSync(DEAL_FILE)) {
    throw new Error('No active deal found. Run `node scripts/seedDeal.js` first.');
  }
  _deal = JSON.parse(fs.readFileSync(DEAL_FILE, 'utf8'));
  return _deal;
}

export function getDeal() {
  if (!_deal) loadDeal();
  return _deal;
}

export function saveDeal(deal) {
  fs.writeFileSync(DEAL_FILE, JSON.stringify(deal, null, 2));
  _deal = deal;
}

export function updateDeal(fields) {
  const current = getDeal();
  saveDeal({ ...current, ...fields });
}
