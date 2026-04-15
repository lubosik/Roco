-- ─────────────────────────────────────────────────────────────────────────────
-- LinkedIn activity scoring + channel routing columns
-- Run once in Supabase SQL editor before deploying this build.
-- ─────────────────────────────────────────────────────────────────────────────

-- Activity score (0-100) computed from LinkedIn profile data.
-- NULL = not yet scored.  < 40 = low activity → route to email first.
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS linkedin_activity_score integer DEFAULT NULL;

-- Preferred first-touch channel: 'linkedin' or 'email'.
-- Set by Roco after scoring the LinkedIn profile.  NULL = not yet decided.
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS recommended_channel text DEFAULT NULL;
