# Reverse Contact Integration

Roco uses Reverse Contact as a contact-data backfill and enrichment provider.

## Authentication

- Store the API key in `.env` as `REVERSECONTACT_API_KEY`.
- Never expose the key in client-side code or logs.
- Current public docs describe authentication as an `apikey` query parameter for legacy enrichment endpoints.
- Some newer endpoints use `Authorization: Bearer rc_*`; Roco attempts those first where implemented, then falls back to documented legacy endpoints.

## Supported Paths

- Reverse email lookup: `GET https://api.reversecontact.com/enrichment?apikey=...&email=...`
- LinkedIn profile extraction: `GET https://api.reversecontact.com/enrichment/profile?apikey=...&linkedInUrl=...`
- Email finder: `GET https://api.reversecontact.com/enrichment/email-finder?apikey=...&full_name=...&company_name=...`
- V2 person search / resolution is used opportunistically by `enrichment/reverseContactEnricher.js` when the account supports it.

## Roco Usage

- `enrichment/reverseContactEnricher.js` normalizes Reverse Contact responses into `{ linkedInUrl, email, fullName, company }`.
- `scripts/backfillReverseContact.js --live --find-emails --limit N` fills missing LinkedIn URLs and emails for contacts.
- Matches are accepted only when the returned name and company are compatible with the Roco contact.
- Email-only results are enough for the pipeline: email-ready contacts advance to approval even if LinkedIn is unavailable or rate-limited.

## Operational Notes

- Reverse Contact calls are rate-limited locally by `REVERSECONTACT_MIN_INTERVAL_MS` with a default of 6500ms.
- Run small batches first, check approval queue movement, then increase the limit.
- LinkedIn provider limits should not block email backfill or email approval queueing.
