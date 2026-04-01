import { NOTION_VERSION, NOTION_BASE } from '../config/constants.js';

export async function notionLog(level, message, meta = {}) {
  const key = process.env.NOTION_API_KEY;
  if (!key) return;

  const content = `[${level.toUpperCase()}] ${message}${Object.keys(meta).length ? ' | ' + JSON.stringify(meta) : ''}`;

  // Log to console in case Notion is down — actual DB logging uses the activity log page
  // You can optionally create a dedicated "Roco Activity Log" Notion page ID in .env
  const logPageId = process.env.NOTION_LOG_PAGE_ID;
  if (!logPageId) return;

  try {
    await fetch(`${NOTION_BASE}/blocks/${logPageId}/children`, {
      method: 'PATCH',
      headers: {
        Authorization: `Bearer ${key}`,
        'Notion-Version': NOTION_VERSION,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        children: [
          {
            object: 'block',
            type: 'paragraph',
            paragraph: {
              rich_text: [{ type: 'text', text: { content: content.slice(0, 2000) } }],
            },
          },
        ],
      }),
    });
  } catch {
    // Silent fail — logging must never crash the app
  }
}

export async function logActivity(action, contactName, firm, note, type = 'General') {
  await notionLog('info', `[${type}] ${action} — ${contactName} @ ${firm}: ${note}`);
}
