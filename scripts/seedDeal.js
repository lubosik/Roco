import { createInterface } from 'readline';
import { saveDeal } from '../core/dealContext.js';

const rl = createInterface({ input: process.stdin, output: process.stdout });

function ask(question) {
  return new Promise(resolve => rl.question(question, resolve));
}

async function main() {
  console.log('\nROCO — New Deal Setup\n' + '='.repeat(40));

  const deal = {
    name: await ask('Deal name: '),
    sector: await ask('Sector (e.g. PropTech, Logistics, Biotech): '),
    raiseAmount: await ask('Raise amount (e.g. £5m): '),
    geography: await ask('Geography (e.g. UK & Ireland): '),
    keyMetrics: await ask('Key metrics / USP (one line): '),
    description: await ask('Brief description (2-3 sentences): '),
    status: 'ACTIVE',
    createdAt: new Date().toISOString(),
  };

  rl.close();

  saveDeal(deal);
  console.log('\nDeal saved. Roco will pick this up on next cycle.');
  console.log(`Deal: ${deal.name} | ${deal.raiseAmount} | ${deal.sector}`);
}

main().catch(console.error);
