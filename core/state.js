/**
 * core/state.js
 * Thin wrapper around Supabase session state using the orchestrator's key names.
 * Keys used here (snake_case): roco_status, outreach_enabled, followup_enabled,
 * enrichment_enabled, research_enabled, linkedin_enabled.
 * Maps to/from the camelCase keys stored in supabaseSync (rocoStatus etc).
 */

import { loadSessionState, saveSessionState } from './supabaseSync.js';

export async function loadState() {
  try {
    const s = await loadSessionState();
    return {
      roco_status:         s.rocoStatus || 'ACTIVE',
      outreach_enabled:    s.outreachEnabled !== false,
      followup_enabled:    s.followupEnabled !== false,
      enrichment_enabled:  s.enrichmentEnabled !== false,
      research_enabled:    s.researchEnabled !== false,
      linkedin_enabled:    s.linkedinEnabled !== false,
      outreach_paused_until: s.outreachPausedUntil || null,
    };
  } catch {
    return {
      roco_status: 'ACTIVE',
      outreach_enabled: true,
      followup_enabled: true,
      enrichment_enabled: true,
      research_enabled: true,
      linkedin_enabled: true,
    };
  }
}

export async function saveState(state) {
  try {
    await saveSessionState({
      rocoStatus:         state.roco_status || 'ACTIVE',
      outreachEnabled:    state.outreach_enabled !== false,
      followupEnabled:    state.followup_enabled !== false,
      enrichmentEnabled:  state.enrichment_enabled !== false,
      researchEnabled:    state.research_enabled !== false,
      linkedinEnabled:    state.linkedin_enabled !== false,
      outreachPausedUntil: state.outreach_paused_until || null,
    });
  } catch (err) {
    console.warn('[STATE] saveState failed:', err.message);
  }
}
