/**
 * core/aiClient.js
 * AI model router with rate limit protection:
 * - Semaphore: max 2 Haiku concurrent, max 3 Sonnet/GPT concurrent
 * - Retry with exponential backoff on 429 (5s → 10s → 20s → 60s, max 4 attempts)
 * - Circuit breaker per model: 3 failures → 60s cooldown
 * - Billing errors: pushActivity + Telegram notification + throw immediately (no fallback)
 * - haikuComplete: Claude Haiku 4.5 → gpt-5.4-mini-2026-03-17 fallback
 * - grokResearch: Grok-3 via xAI (per-investor web research)
 * - aiComplete: GPT-5.4 → gpt-4o → Claude Sonnet fallback (outreach drafting)
 */

import OpenAI from 'openai';
import Anthropic from '@anthropic-ai/sdk';
import { pushActivity } from '../dashboard/server.js';

// ── Semaphore ─────────────────────────────────────────────────────────────────
class Semaphore {
  constructor(limit) {
    this._limit = limit;
    this._active = 0;
    this._queue = [];
  }
  acquire() {
    if (this._active < this._limit) {
      this._active++;
      return Promise.resolve();
    }
    return new Promise(resolve => this._queue.push(resolve));
  }
  release() {
    this._active--;
    if (this._queue.length > 0) {
      this._active++;
      this._queue.shift()();
    }
  }
  async run(fn) {
    await this.acquire();
    try { return await fn(); } finally { this.release(); }
  }
}

const haikuSem  = new Semaphore(2); // max 2 concurrent Haiku calls
const sonnetSem = new Semaphore(3); // max 3 concurrent Sonnet calls
const gptSem    = new Semaphore(3); // max 3 concurrent OpenAI calls

// ── Circuit Breaker ───────────────────────────────────────────────────────────
class CircuitBreaker {
  constructor(name, failureThreshold = 3, cooldownMs = 60_000) {
    this.name = name;
    this.failureThreshold = failureThreshold;
    this.cooldownMs = cooldownMs;
    this.failures = 0;
    this.openUntil = null;
  }
  isOpen() {
    if (this.openUntil && Date.now() < this.openUntil) return true;
    if (this.openUntil && Date.now() >= this.openUntil) {
      this.failures = 0;
      this.openUntil = null;
    }
    return false;
  }
  recordSuccess() {
    this.failures = 0;
    this.openUntil = null;
  }
  recordFailure() {
    this.failures++;
    if (this.failures >= this.failureThreshold) {
      this.openUntil = Date.now() + this.cooldownMs;
      console.warn(`[AI] Circuit breaker OPEN: ${this.name} — 60s cooldown`);
      pushActivity({ type: 'error', action: 'Circuit Breaker', note: `${this.name} tripped — 60s cooldown` });
    }
  }
}

const haikuCB   = new CircuitBreaker('Haiku 4.5');
const gptMiniCB = new CircuitBreaker('gpt-5.4-mini');
const gptCB     = new CircuitBreaker('gpt-5.4');
const sonnetCB  = new CircuitBreaker('Claude Sonnet');

// ── Helpers ───────────────────────────────────────────────────────────────────
function isBillingError(status, body = '') {
  if (status === 402) return true;
  if (status === 429 && /quota|exceeded.*quota|billing/i.test(body)) return true;
  if (/exceeded.*quota|billing.*detail|check your plan/i.test(body)) return true;
  return false;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function notifyBillingError(model, message) {
  const note = `${model}: ${message}`.substring(0, 200);
  pushActivity({ type: 'error', action: `Billing Error — ${model}`, note });
  try {
    const { sendTelegram } = await import('../approval/telegramBot.js');
    await sendTelegram(`🚨 *Billing Error — ${model}*\n${message.substring(0, 300)}\n\nCheck API billing dashboard.`);
  } catch {}
}

// Retry with exponential backoff for transient 429s (not billing)
async function withRetry(fn, modelName, maxAttempts = 4) {
  const delays = [5_000, 10_000, 20_000, 60_000];
  let lastErr;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const status = err.status || 0;
      const is429 = /429/.test(err.message) || status === 429;
      if (isBillingError(status, err.message)) {
        await notifyBillingError(modelName, err.message);
        throw err; // Don't retry billing errors
      }
      if (is429 && attempt < maxAttempts - 1) {
        const delay = delays[attempt] ?? 60_000;
        console.warn(`[AI] ${modelName} rate limit — retry ${attempt + 1}/${maxAttempts - 1} in ${delay / 1000}s`);
        pushActivity({ type: 'warning', action: 'Rate Limit', note: `${modelName} — retry in ${delay / 1000}s` });
        await sleep(delay);
      } else {
        throw err;
      }
    }
  }
  throw lastErr;
}

// ── Claude Haiku 4.5 (batch scoring, parsing) with gpt-5.4-mini fallback ──────
export async function haikuComplete(prompt, { maxTokens = 300, systemPrompt = null } = {}) {
  const anthropicKey = process.env.ANTHROPIC_API_KEY;

  // Primary: Claude Haiku 4.5
  if (anthropicKey && !haikuCB.isOpen()) {
    try {
      const text = await haikuSem.run(() => withRetry(async () => {
        const messages = [];
        if (systemPrompt) messages.push({ role: 'system', content: systemPrompt });
        messages.push({ role: 'user', content: prompt });

        const res = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: {
            'x-api-key': anthropicKey,
            'anthropic-version': '2023-06-01',
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            model: 'claude-haiku-4-5-20251001',
            max_tokens: Math.min(maxTokens, 4096),
            messages,
          }),
        });

        if (!res.ok) {
          const errBody = await res.text();
          const e = new Error(`Haiku ${res.status}: ${errBody.substring(0, 200)}`);
          e.status = res.status;
          throw e;
        }

        const data = await res.json();
        const t = data.content?.[0]?.text || '';
        if (!t) throw new Error('Haiku returned empty response');
        return t;
      }, 'Haiku 4.5'));

      haikuCB.recordSuccess();
      console.log('[AI] Haiku 4.5 OK');
      return text;
    } catch (err) {
      haikuCB.recordFailure();
      if (isBillingError(err.status || 0, err.message)) throw err;
      console.warn(`[AI] Haiku failed: ${err.message} — falling back to gpt-5.4-mini`);
      pushActivity({ type: 'error', action: 'AI Fallback', note: `Haiku 4.5 → gpt-5.4-mini: ${err.message.substring(0, 100)}` });
    }
  }

  // Fallback: gpt-5.4-mini-2026-03-17
  if (gptMiniCB.isOpen()) throw new Error('gpt-5.4-mini circuit breaker open — both AI paths unavailable');
  const openaiKey = process.env.OPENAI_API_KEY;
  if (!openaiKey) throw new Error('No AI keys available (ANTHROPIC_API_KEY and OPENAI_API_KEY both missing)');

  try {
    const text = await gptSem.run(() => withRetry(async () => {
      const messages = [];
      if (systemPrompt) messages.push({ role: 'system', content: systemPrompt });
      messages.push({ role: 'user', content: prompt });

      const res = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${openaiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: 'gpt-5.4-mini-2026-03-17',
          messages,
          max_tokens: Math.min(maxTokens, 4096),
          temperature: 0.1,
        }),
      });

      if (!res.ok) {
        const errBody = await res.text();
        const e = new Error(`gpt-5.4-mini ${res.status}: ${errBody.substring(0, 200)}`);
        e.status = res.status;
        throw e;
      }

      const data = await res.json();
      const t = data.choices?.[0]?.message?.content || '';
      if (!t) throw new Error('gpt-5.4-mini returned empty response');
      return t;
    }, 'gpt-5.4-mini'));

    gptMiniCB.recordSuccess();
    console.log('[AI] gpt-5.4-mini-2026-03-17 OK');
    return text;
  } catch (err) {
    gptMiniCB.recordFailure();
    pushActivity({ type: 'error', action: 'AI Fallback Failed', note: err.message.substring(0, 150) });
    throw err;
  }
}

// ── Grok via xAI API (live web access for per-investor research) ──────────────
export async function grokResearch(prompt, { maxTokens = 1000 } = {}) {
  const apiKey = process.env.XAI_API_KEY || process.env.GROK_API_KEY;
  if (!apiKey) throw new Error('No xAI API key set (XAI_API_KEY or GROK_API_KEY)');

  return gptSem.run(() => withRetry(async () => {
    const client = new OpenAI({ apiKey, baseURL: 'https://api.x.ai/v1' });
    const response = await client.chat.completions.create({
      model: 'grok-3',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: maxTokens,
      temperature: 0.2,
    });
    return response.choices?.[0]?.message?.content || '';
  }, 'Grok-3'));
}

// ── GPT-5.4 via OpenAI Responses API (outreach drafting) ─────────────────────
async function gptComplete(prompt, { reasoning = 'medium', maxTokens = 2000 } = {}) {
  const OPENAI_KEY = process.env.OPENAI_API_KEY;
  if (!OPENAI_KEY) throw new Error('OPENAI_API_KEY not set');

  for (const model of ['gpt-5.4', 'gpt-4o']) {
    if (gptCB.isOpen()) { console.warn('[AI] gpt CB open — skipping to Claude'); break; }
    try {
      const text = await gptSem.run(() => withRetry(async () => {
        const body = { model, input: prompt, max_output_tokens: maxTokens };
        if (model === 'gpt-5.4') body.reasoning = { effort: reasoning };

        const res = await fetch('https://api.openai.com/v1/responses', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${OPENAI_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });

        if (!res.ok) {
          const errBody = await res.text();
          const e = new Error(`${model} ${res.status}: ${errBody.substring(0, 200)}`);
          e.status = res.status;
          throw e;
        }

        const data = await res.json();
        let t = '';
        if (Array.isArray(data.output)) {
          outer: for (const item of data.output) {
            const content = Array.isArray(item.content) ? item.content : [];
            for (const c of content) { if (c.text) { t = c.text; break outer; } }
          }
        }
        if (!t) t = data.output_text || '';
        if (!t) throw new Error(`${model} returned empty response`);
        return t;
      }, model));

      gptCB.recordSuccess();
      console.log(`[AI] ${model} (${reasoning}) OK`);
      return text;
    } catch (err) {
      gptCB.recordFailure();
      if (isBillingError(err.status || 0, err.message)) throw err;
      console.warn(`[AI] ${model} failed: ${err.message}${model === 'gpt-5.4' ? ' — trying gpt-4o' : ' — falling back to Claude'}`);
    }
  }

  throw new Error('All GPT models failed');
}

// ── Claude Sonnet fallback / structured reasoning ────────────────────────────
export async function sonnetComplete(prompt, { maxTokens = 2000, model = 'claude-sonnet-4-5-20251001' } = {}) {
  const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
  if (!ANTHROPIC_KEY) throw new Error('ANTHROPIC_API_KEY not set');
  if (sonnetCB.isOpen()) throw new Error('Sonnet circuit breaker open');

  try {
    const text = await sonnetSem.run(() => withRetry(async () => {
      const anthropic = new Anthropic({ apiKey: ANTHROPIC_KEY });
      const msg = await anthropic.messages.create({
        model,
        max_tokens: maxTokens,
        messages: [{ role: 'user', content: prompt }],
      });
      const t = msg.content?.[0]?.text || '';
      if (!t) throw new Error('Sonnet returned empty response');
      return t;
    }, 'Claude Sonnet'));

    sonnetCB.recordSuccess();
    return text;
  } catch (err) {
    sonnetCB.recordFailure();
    throw err;
  }
}

// ── Main router — GPT first, Claude as fallback ───────────────────────────────
export async function aiComplete(prompt, { reasoning = 'low', maxTokens = 500, task = 'task' } = {}) {
  try {
    return await gptComplete(prompt, { reasoning, maxTokens });
  } catch (err) {
    if (isBillingError(err.status || 0, err.message)) throw err;
    console.warn(`[AI] GPT failed (${task}): ${err.message} — falling back to Claude`);
    try {
      return await sonnetComplete(prompt, { maxTokens, model: 'claude-sonnet-4-6' });
    } catch (err2) {
      console.error(`[AI] Claude also failed (${task}): ${err2.message}`);
      throw err2;
    }
  }
}
