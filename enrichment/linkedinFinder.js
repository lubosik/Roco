/**
 * enrichment/linkedinFinder.js
 * Attempt to find a LinkedIn URL for a contact using Unipile People Search.
 * Returns a URL string or null.
 */

import { searchLinkedInPeople } from '../integrations/unipileClient.js';

export async function findLinkedInUrl({ name, company, title }) {
  if (!name) return null;

  const keywords = [name, company, title].filter(Boolean).join(' ');

  try {
    const results = await searchLinkedInPeople({ keywords, limit: 5 });

    for (const person of results) {
      const fullName = [person.first_name, person.last_name].filter(Boolean).join(' ');
      const personCompany = person.current_company?.name || person.company || '';

      // Require name match (first + last) and company match if we have both
      const nameMatch = fullName.toLowerCase().includes(name.split(' ')[0].toLowerCase()) &&
                        fullName.toLowerCase().includes((name.split(' ').slice(-1)[0] || '').toLowerCase());

      const companyMatch = !company ||
        personCompany.toLowerCase().includes(company.toLowerCase().split(' ')[0]);

      if (nameMatch && companyMatch) {
        if (person.profile_url) return person.profile_url;
        if (person.public_identifier) return `https://linkedin.com/in/${person.public_identifier}`;
      }
    }

    return null;
  } catch (err) {
    console.warn(`[LINKEDIN FINDER] Search failed for "${name}":`, err.message);
    return null;
  }
}
