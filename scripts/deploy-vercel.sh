#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Roco Mission Control — Vercel Deploy Script
# Usage: VERCEL_TOKEN=your_token bash scripts/deploy-vercel.sh
# ─────────────────────────────────────────────────────────────────────────────

set -e
cd /root/roco

if [ -z "$VERCEL_TOKEN" ]; then
  echo "Error: VERCEL_TOKEN is not set."
  echo "Get your token from: https://vercel.com/account/tokens"
  echo "Then run: VERCEL_TOKEN=your_token bash scripts/deploy-vercel.sh"
  exit 1
fi

echo "→ Deploying to Vercel..."

# Source .env for the values
set -a
source /root/roco/.env
set +a

# Deploy to Vercel
vercel --prod --token "$VERCEL_TOKEN" --yes \
  --build-env NODE_ENV=production \
  2>&1

echo ""
echo "→ Deploy complete. Now add environment variables:"
echo ""

# Add all env vars from .env to Vercel (non-interactive)
ENV_VARS=(
  "ANTHROPIC_API_KEY"
  "OPENAI_API_KEY"
  "GEMINI_API_KEY"
  "NOTION_API_KEY"
  "NOTION_CONTACTS_DB_ID"
  "NOTION_COMPANIES_DB_ID"
  "TELEGRAM_BOT_TOKEN"
  "TELEGRAM_CHAT_ID"
  "SUPABASE_URL"
  "SUPABASE_SERVICE_KEY"
  "SUPABASE_ANON_KEY"
  "DASHBOARD_USER"
  "DASHBOARD_PASS"
  "KONDO_WEBHOOK_SECRET"
  "KASPR_API_KEY"
  "MATONAI_API_KEY"
)

for VAR in "${ENV_VARS[@]}"; do
  VALUE="${!VAR}"
  if [ -n "$VALUE" ]; then
    echo "  Adding $VAR..."
    echo "$VALUE" | vercel env add "$VAR" production --token "$VERCEL_TOKEN" --yes 2>/dev/null || \
    vercel env rm "$VAR" production --token "$VERCEL_TOKEN" --yes 2>/dev/null && \
    echo "$VALUE" | vercel env add "$VAR" production --token "$VERCEL_TOKEN" --yes 2>/dev/null || true
  else
    echo "  Skipping $VAR (empty)"
  fi
done

echo ""
echo "→ Redeploying with environment variables..."
vercel --prod --token "$VERCEL_TOKEN" --yes 2>&1

echo ""
echo "✓ Done! Your Vercel URL is shown above."
echo ""
echo "Next step — run this SQL in your Supabase dashboard SQL editor:"
echo "https://supabase.com/dashboard/project/_/sql"
echo ""
echo "  -- Paste the contents of: /root/roco/supabase/migration.sql"
echo "  -- (Only the approval_queue table is missing — the rest exist already)"
