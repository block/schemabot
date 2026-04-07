#!/bin/bash
set -euo pipefail

# Store GitHub App credentials in AWS Secrets Manager
# Usage: ./setup-github-app.sh [--deploy]
#
# Prompts for App ID, private key file, and webhook secret.
# Safe to re-run — updates the secret in place.
# Use --deploy to build and deploy the service afterwards.

DEPLOY=false
if [ "${1:-}" = "--deploy" ]; then
    DEPLOY=true
fi

REGION="us-west-2"
export AWS_PROFILE="${AWS_PROFILE:-schemabot-deployer}"

echo "🔐 GitHub App Setup"
echo "======================"
echo ""

cd "$(dirname "$0")/.."

# Derive secret name from Terraform output (falls back to default)
SECRET_ID="${GITHUB_APP_SECRET_ID:-}"
if [ -z "$SECRET_ID" ] && command -v terraform &> /dev/null && [ -d .terraform ]; then
    SECRET_ID=$(terraform output -raw github_app_secret_id 2>/dev/null || true)
fi
if [ -z "$SECRET_ID" ]; then
    SECRET_ID="schemabot-example/github-app"
fi

# Prompt and validate each input immediately
read -r -p "GitHub App ID: " APP_ID
if [ -z "$APP_ID" ]; then
    echo "❌ App ID is required"
    exit 1
fi

read -r -p "Path to private key (.pem file): " PEM_PATH
if [ ! -f "$PEM_PATH" ]; then
    echo "❌ Private key file not found: $PEM_PATH"
    exit 1
fi

read -r -s -p "Webhook secret: " WEBHOOK_SECRET
echo ""
if [ -z "$WEBHOOK_SECRET" ]; then
    echo "❌ Webhook secret is required"
    exit 1
fi

# Store credentials in Secrets Manager
echo ""
echo "📦 Storing credentials in Secrets Manager..."
PRIVATE_KEY=$(cat "$PEM_PATH")
aws secretsmanager put-secret-value \
    --secret-id "$SECRET_ID" \
    --secret-string "$(jq -n \
        --arg id "$APP_ID" \
        --arg pk "$PRIVATE_KEY" \
        --arg ws "$WEBHOOK_SECRET" \
        '{app_id: $id, private_key: $pk, webhook_secret: $ws}')" \
    --region "$REGION" \
    --output text > /dev/null

echo "   ✅ Credentials stored"

if [ "$DEPLOY" = true ]; then
    echo ""
    exec ./scripts/deploy.sh
else
    echo ""
    echo "Run ./scripts/deploy.sh to deploy with the new credentials."
fi
