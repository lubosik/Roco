import winston from 'winston';
import { notionLog } from '../crm/notionLogger.js';

const { combine, timestamp, printf, colorize } = winston.format;

const fmt = printf(({ level, message, timestamp, ...meta }) => {
  const extra = Object.keys(meta).length ? ' ' + JSON.stringify(meta) : '';
  return `${timestamp} [${level}] ${message}${extra}`;
});

const logger = winston.createLogger({
  level: 'info',
  format: combine(timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }), fmt),
  transports: [
    new winston.transports.Console({
      format: combine(colorize(), timestamp({ format: 'HH:mm:ss' }), fmt),
    }),
    new winston.transports.File({ filename: 'roco.log', maxsize: 10 * 1024 * 1024 }),
  ],
});

export async function log(level, message, meta = {}) {
  logger[level](message, meta);
  try {
    await notionLog(level, message, meta);
  } catch {
    // Notion logging is best-effort
  }
}

export const info = (msg, meta) => log('info', msg, meta);
export const warn = (msg, meta) => log('warn', msg, meta);
export const error = (msg, meta) => log('error', msg, meta);

export default logger;
