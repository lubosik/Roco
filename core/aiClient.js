/**
 * core/aiClient.js
 * AI model router.
 * - haikuComplete: Claude Haiku 4.5 (batch scoring, parsing) → gpt-5.4-mini-2026-03-17 fallback
 * - grokResearch: Grok via xAI API (per-investor web research)
 * - aiComplete: GPT-5.4 → gpt-4o → Claude fallback (outreach drafting)
 */

import OpenAI from 'openai';
import Anthropic from '@anthropic-ai/sdk';
import { pushActivity } from '../dashboard/server.js';

// ── Claude Haiku 4.5 (batch scoring, parsing) with gpt-4o-mini fallback ──
export async function haikuComplete(prompt, { maxTokens = 4096, systemPrompt = null } = {}) {
  // Primary: Claude Haiku 4.5
  const anthropicKey = process.env.ANTHROPIC_API_KEY;
  if (anthropicKey) {
    try {
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
        const err = await res.text();
        throw new Error(`Haiku ${res.status}: ${err.substring(0, 200)}`);
      }

      const data = await res.json();
      const text = data.content?.[0]?.text || '';
      if (text) { console.log('[AI] Haiku 4.5 OK'); return text; }
      throw new Error('Haiku returned empty response');
    } catch (err) {
      console.warn(`[AI] Haiku failed: ${err.message} — falling back to gpt-5.4-mini`);
      pushActivity({ type: 'error', action: 'AI Fallback', note: `Haiku 4.5 failed — ${err.message}` });
    }
  }

  // Fallback: gpt-5.4-mini-2026-03-17
  const openaiKey = process.env.OPENAI_API_KEY;
  if (!openaiKey) throw new Error('No AI keys available (ANTHROPIC_API_KEY and OPENAI_API_KEY both missing)');

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
    const err = await res.text();
    const errMsg = `gpt-5.4-mini ${res.status}: ${err.substring(0, 200)}`;
    pushActivity({ type: 'error', action: 'AI Fallback Failed', note: errMsg });
    throw new Error(errMsg);
  }

  const data = await res.json();
  const text = data.choices?.[0]?.message?.content || '';
  if (!text) throw new Error('gpt-5.4-mini returned empty response');
  console.log('[AI] gpt-5.4-mini-2026-03-17 fallback OK');
  return text;
}

// ── Grok via xAI API (live web access for per-investor research) ──
export async function grokResearch(prompt, { maxTokens = 4096 } = {}) {
  const apiKey = process.env.XAI_API_KEY || process.env.GROK_API_KEY;
  if (!apiKey) throw new Error('No xAI API key set (XAI_API_KEY or GROK_API_KEY)');

  const client = new OpenAI({
    apiKey,
    baseURL: 'https://api.x.ai/v1',
  });

  const response = await client.chat.completions.create({
    model: 'grok-3',
    messages: [{ role: 'user', content: prompt }],
    max_tokens: maxTokens,
    temperature: 0.2,
  });

  return response.choices?.[0]?.message?.content || '';
}

// ── GPT-5.4 via OpenAI Responses API (outreach drafting) ──
async function gptComplete(prompt, { reasoning = 'medium', maxTokens = 2000 } = {}) {
  const OPENAI_KEY = process.env.OPENAI_API_KEY;
  if (!OPENAI_KEY) throw new Error('OPENAI_API_KEY not set');

  for (const model of ['gpt-5.4', 'gpt-4o']) {
    try {
      const body = {
        model,
        input: prompt,
        max_output_tokens: maxTokens,
      };
      if (model === 'gpt-5.4') body.reasoning = { effort: reasoning };

      const res = await fetch('https://api.openai.com/v1/responses', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${OPENAI_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        const err = await res.text();
        throw new Error(`${model} ${res.status}: ${err.substring(0, 200)}`);
      }

      const data = await res.json();

      let text = '';
      if (Array.isArray(data.output)) {
        outer: for (const item of data.output) {
          const content = Array.isArray(item.content) ? item.content : [];
          for (const c of content) {
            if (c.text) { text = c.text; break outer; }
          }
        }
      }
      if (!text) text = data.output_text || '';
      if (!text) throw new Error(`${model} returned empty response`);

      console.log(`[AI] ${model} (${reasoning}) OK`);
      return text;

    } catch (err) {
      console.warn(`[AI] ${model} failed: ${err.message}${model === 'gpt-5.4' ? ' — trying gpt-4o' : ' — falling back to Claude'}`);
    }
  }

  throw new Error('All GPT models failed');
}

// ── Claude fallback ──
async function claudeComplete(prompt, { maxTokens = 2000 } = {}) {
  const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
  if (!ANTHROPIC_KEY) throw new Error('ANTHROPIC_API_KEY not set');

  const anthropic = new Anthropic({ apiKey: ANTHROPIC_KEY });
  const msg = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: maxTokens,
    messages: [{ role: 'user', content: prompt }],
  });
  return msg.content?.[0]?.text || '';
}

// ── Main router — GPT first, Claude as fallback ──
export async function aiComplete(prompt, { reasoning = 'low', maxTokens = 500, task = 'task' } = {}) {
  try {
    return await gptComplete(prompt, { reasoning, maxTokens });
  } catch (err) {
    console.warn(`[AI] GPT failed (${task}): ${err.message} — falling back to Claude`);
    try {
      return await claudeComplete(prompt, { maxTokens });
    } catch (err2) {
      console.error(`[AI] Claude also failed (${task}): ${err2.message}`);
      throw err2;
    }
  }
}
