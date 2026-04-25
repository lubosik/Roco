// core/telegram.js
// Telegram helpers: send messages with inline buttons, edit messages, register webhook.

export async function sendTelegramWithButtons(text, buttons) {
  const token  = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) {
    console.warn('[TELEGRAM] No token/chatId — cannot send with buttons');
    return null;
  }

  try {
    const res = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text,
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: buttons },
      }),
    });
    const data = await res.json();
    if (!data.ok) console.warn('[TELEGRAM] sendMessage failed:', data.description);
    return data.result?.message_id || null;
  } catch (err) {
    console.error('[TELEGRAM] sendTelegramWithButtons error:', err.message);
    return null;
  }
}

export async function editTelegramMessage(chatId, messageId, text) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) return;
  try {
    await fetch(`https://api.telegram.org/bot${token}/editMessageText`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, message_id: messageId, text, parse_mode: 'Markdown' }),
    });
  } catch (err) {
    console.warn('[TELEGRAM] editMessage error:', err.message);
  }
}

export async function answerCallbackQuery(callbackQueryId, text = 'Done') {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) return;
  try {
    await fetch(`https://api.telegram.org/bot${token}/answerCallbackQuery`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ callback_query_id: callbackQueryId, text }),
    });
  } catch {}
}

export async function registerTelegramWebhook() {
  const token   = process.env.TELEGRAM_BOT_TOKEN;
  const baseUrl = process.env.SERVER_BASE_URL || process.env.PUBLIC_URL;
  const transport = String(process.env.TELEGRAM_TRANSPORT || 'polling').trim().toLowerCase();
  if (transport !== 'webhook') {
    console.log(`[TELEGRAM] Webhook registration skipped — TELEGRAM_TRANSPORT=${transport || 'polling'}`);
    return;
  }
  if (!token || !baseUrl) {
    console.log('[TELEGRAM] Webhook registration skipped — no token or SERVER_BASE_URL');
    return;
  }
  try {
    const r = await fetch(`https://api.telegram.org/bot${token}/setWebhook`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: `${baseUrl}/api/telegram/webhook` }),
    });
    const d = await r.json();
    console.log('[TELEGRAM] Webhook:', d.ok ? `OK — ${baseUrl}/api/telegram/webhook` : d.description);
  } catch (err) {
    console.warn('[TELEGRAM] Webhook registration error:', err.message);
  }
}
