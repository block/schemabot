#!/bin/bash
set -euo pipefail

# Store GitHub App credentials in AWS Secrets Manager
# Usage: ./setup-github-app.sh [--profile PROFILE] [--secret-id ID] [--deploy]
#
# Prompts for App ID, private key file, and webhook secret.
# Safe to re-run — creates the secret if missing, updates if it exists.
# Use --deploy to build and deploy the service afterwards.

DEPLOY=false
AWS_PROFILE="${AWS_PROFILE:-schemabot-deployer}"
GITHUB_APP_SECRET_ID="${GITHUB_APP_SECRET_ID:-}"

while [ $# -gt 0 ]; do
    case "$1" in
        --profile)    AWS_PROFILE="${2:?--profile requires a value}"; shift 2 ;;
        --secret-id)  GITHUB_APP_SECRET_ID="${2:?--secret-id requires a value}"; shift 2 ;;
        --deploy)     DEPLOY=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done
export AWS_PROFILE

REGION="us-west-2"

echo "🔐 GitHub App Setup"
echo "======================"
echo ""

cd "$(dirname "$0")/.."

# Derive secret name from Terraform output (falls back to default)
SECRET_ID="${GITHUB_APP_SECRET_ID}"
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

# Store credentials in Secrets Manager (create if missing, update if exists)
echo ""
echo "📦 Storing credentials in Secrets Manager..."
PRIVATE_KEY=$(cat "$PEM_PATH")
SECRET_VALUE="$(jq -n \
    --arg id "$APP_ID" \
    --arg pk "$PRIVATE_KEY" \
    --arg ws "$WEBHOOK_SECRET" \
    '{"app-id": $id, "private-key": $pk, "webhook-secret": $ws}')"

if aws secretsmanager describe-secret --secret-id "$SECRET_ID" --region "$REGION" > /dev/null 2>&1; then
    aws secretsmanager put-secret-value \
        --secret-id "$SECRET_ID" \
        --secret-string "$SECRET_VALUE" \
        --region "$REGION" \
        --output text > /dev/null
else
    aws secretsmanager create-secret \
        --name "$SECRET_ID" \
        --secret-string "$SECRET_VALUE" \
        --region "$REGION" \
        --output text > /dev/null
fi

echo "   ✅ Credentials stored"

if [ "$DEPLOY" = true ]; then
    echo ""
    exec ./scripts/deploy.sh
else
    echo ""
    echo "Run ./scripts/deploy.sh to deploy with the new credentials."
fi
