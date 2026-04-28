/**
 * core/openRouterClient.js
 * Central OpenRouter client — single source of truth for all LLM calls except Grok.
 *
 * Model tiers (override any via env vars):
 *   draft       → anthropic/claude-haiku-4-5   client-facing copy, outreach writing
 *   classify    → google/gemini-2.5-flash        scoring, classification, analytics
 *   brain       → anthropic/claude-opus-4-7      JARVIS reasoning, deal strategy
 *   research    → moonshotai/kimi-k2             bulk web research, long-context tasks
 *   conversation→ anthropic/claude-haiku-4-5    reply drafting, intent classification
 *
 * Falls back to ANTHROPIC_API_KEY direct when no OPENROUTER_API_KEY set.
 */

import { pushActivity } from '../dashboard/server.js';

const MODELS = {
  draft:        process.env.OR_DRAFT_MODEL    || 'anthropic/claude-haiku-4-5',
  classify:     process.env.OR_CLASSIFY_MODEL || 'google/gemini-2.5-flash',
  brain:        process.env.OR_BRAIN_MODEL    || 'anthropic/claude-opus-4-7',
  research:     process.env.OR_RESEARCH_MODEL || 'moonshotai/kimi-k2',
  conversation: process.env.OR_CONV_MODEL     || 'anthropic/claude-haiku-4-5',
  web:          process.env.OR_WEB_MODEL      || 'perplexity/sonar-pro',
};

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Circuit breakers per tier ─────────────────────────────────────────────────
const _breakers = {};
function getBreaker(tier) {
  if (!_breakers[tier]) _breakers[tier] = { failures: 0, openUntil: null };
  return _breakers[tier];
}
function isBreakerOpen(tier) {
  const b = getBreaker(tier);
  if (b.openUntil && Date.now() < b.openUntil) return true;
  if (b.openUntil && Date.now() >= b.openUntil) { b.failures = 0; b.openUntil = null; }
  return false;
}
function recordSuccess(tier) { const b = getBreaker(tier); b.failures = 0; b.openUntil = null; }
function recordFailure(tier) {
  const b = getBreaker(tier);
  b.failures++;
  if (b.failures >= 3) {
    b.openUntil = Date.now() + 60_000;
    console.warn(`[OR] Circuit breaker open: ${tier} — 60s cooldown`);
  }
}

// ── Retry helper ──────────────────────────────────────────────────────────────
async function withRetry(fn, label, maxAttempts = 3) {
  const delays = [5_000, 15_000, 45_000];
  let lastErr;
  for (let i = 0; i < maxAttempts; i++) {
    try { return await fn(); }
    catch (err) {
      lastErr = err;
      const is429 = err.status === 429 || /429|rate.limit/i.test(err.message);
      if (is429 && i < maxAttempts - 1) {
        const delay = delays[i] ?? 45_000;
        console.warn(`[OR] ${label} rate limited — retry ${i + 1} in ${delay / 1000}s`);
        await sleep(delay);
      } else {
        throw err;
      }
    }
  }
  throw lastErr;
}

// ── Fallback: Anthropic direct ────────────────────────────────────────────────
async function anthropicDirect(messages, { maxTokens = 500, model = 'claude-haiku-4-5-20251001', systemPrompt = null } = {}) {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) throw new Error('No OPENROUTER_API_KEY and no ANTHROPIC_API_KEY set');
  const body = {
    model,
    max_tokens: maxTokens,
    messages: systemPrompt
      ? [{ role: 'user', content: messages[messages.length - 1]?.content || '' }]
      : messages,
  };
  if (systemPrompt) body.system = systemPrompt;
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'x-api-key': key, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    const e = new Error(`Anthropic direct ${res.status}: ${txt.substring(0, 200)}`);
    e.status = res.status;
    throw e;
  }
  const data = await res.json();
  return data.content?.[0]?.text || '';
}

// ── Main completion — OpenRouter chat/completions format ──────────────────────
/**
 * @param {string|Array} prompt — string or pre-built messages array
 * @param {{ tier, maxTokens, systemPrompt, model }} opts
 * @returns {Promise<string>}
 */
export async function orComplete(prompt, {
  tier = 'classify',
  maxTokens = 500,
  systemPrompt = null,
  model = null,
} = {}) {
  const orKey = process.env.OPENROUTER_API_KEY;
  const resolvedModel = model || MODELS[tier] || MODELS.classify;

  // Build messages array
  const messages = [];
  if (systemPrompt) messages.push({ role: 'system', content: systemPrompt });
  if (typeof prompt === 'string') {
    messages.push({ role: 'user', content: prompt });
  } else if (Array.isArray(prompt)) {
    messages.push(...prompt);
  }

  // OpenRouter path
  if (orKey && !isBreakerOpen(tier)) {
    try {
      const text = await withRetry(async () => {
        const res = await fetch('https://openrouter.ai/api/v1/chat/completions', {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${orKey}`,
            'Content-Type': 'application/json',
            'HTTP-Referer': 'https://roco.app',
            'X-Title': 'Roco',
          },
          body: JSON.stringify({
            model: resolvedModel,
            messages,
            max_tokens: maxTokens,
            temperature: tier === 'draft' || tier === 'conversation' ? 0.7 : 0.1,
            // web_search_options is only for OpenRouter's own search layer (e.g. openai/gpt-4o-search-preview).
            // Perplexity sonar models have built-in search and reject this parameter.
            ...(tier === 'web' && !resolvedModel.startsWith('perplexity/') ? { web_search_options: { search_context_size: 'medium' } } : {}),
          }),
        });
        if (!res.ok) {
          const txt = await res.text().catch(() => '');
          const e = new Error(`OR ${res.status}: ${txt.substring(0, 200)}`);
          e.status = res.status;
          throw e;
        }
        const data = await res.json();
        if (data.error) {
          const e = new Error(`OR error: ${data.error.message || JSON.stringify(data.error)}`);
          e.status = data.error.code || 500;
          throw e;
        }
        const t = data.choices?.[0]?.message?.content || '';
        if (!t) throw new Error(`OR (${resolvedModel}) returned empty response`);
        return t;
      }, resolvedModel);

      recordSuccess(tier);
      return text;
    } catch (err) {
      recordFailure(tier);
      console.warn(`[OR] ${resolvedModel} failed: ${err.message} — falling back to Anthropic direct`);
      pushActivity({ type: 'warning', action: 'OpenRouter fallback', note: `${resolvedModel}: ${err.message.substring(0, 100)}` });
    }
  }

  // Anthropic direct fallback
  const fallbackModel = tier === 'draft' || tier === 'conversation' || tier === 'brain'
    ? 'claude-haiku-4-5-20251001'
    : 'claude-haiku-4-5-20251001';
  return anthropicDirect(messages, { maxTokens, model: fallbackModel, systemPrompt });
}

/**
 * Get the resolved model ID for a given tier (useful for logging).
 */
export function getModelForTier(tier) {
  return MODELS[tier] || MODELS.classify;
}
