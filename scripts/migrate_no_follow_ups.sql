-- Migration: Add no_follow_ups column to deals table
-- Run this in the Supabase SQL Editor

ALTER TABLE deals ADD COLUMN IF NOT EXISTS no_follow_ups BOOLEAN DEFAULT false;

-- Set Project Electrify to no_follow_ups = true (no channel switching, advance to next person)
UPDATE deals SET no_follow_ups = true WHERE id = 'd72e87e0-b0f8-456c-9097-a366cacd957c';

-- Verify
SELECT id, name, no_follow_ups FROM deals;
