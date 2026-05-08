#!/bin/bash
set -euo pipefail

# Store PlanetScale credentials in AWS Secrets Manager for SchemaBot.
#
# Usage: cd deploy/aws-multi-env/staging && ../scripts/setup-planetscale-token.sh \
#          --token '<id>:<secret>' --vtgate-dsn '<user>:<pass>@tcp(<host>:3306)/'
#
# Prerequisites:
#   - bootstrap.sh has already run (terraform state exists)
#   - bootstrap-planetscale.sh has been run to create credentials
#   - AWS CLI configured with correct region

REGION="${AWS_DEFAULT_REGION:-us-west-2}"

# ============================================================================
# Parse flags
# ============================================================================

TOKEN=""
VTGATE_DSN=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --token)
            TOKEN="$2"
            shift 2
            ;;
        --vtgate-dsn)
            VTGATE_DSN="$2"
            shift 2
            ;;
        --help|-h)
            echo "Store PlanetScale credentials in AWS Secrets Manager"
            echo ""
            echo "Usage: $0 --token <id:secret> --vtgate-dsn <dsn>"
            echo ""
            echo "Required:"
            echo "  --token <id:secret>       PlanetScale service token (from bootstrap-planetscale.sh)"
            echo "  --vtgate-dsn <dsn>        Vtgate MySQL DSN (from bootstrap-planetscale.sh)"
            echo ""
            echo "Run from an environment directory (e.g., staging/) with terraform state."
            exit 0
            ;;
        *)
            echo "Unknown flag: $1"
            exit 1
            ;;
    esac
done

if [ -z "$TOKEN" ]; then
    echo "Error: --token is required"
    echo "Usage: $0 --token <id:secret> --vtgate-dsn <dsn>"
    exit 1
fi

if [ -z "$VTGATE_DSN" ]; then
    echo "Error: --vtgate-dsn is required"
    echo "Usage: $0 --token <id:secret> --vtgate-dsn <dsn>"
    exit 1
fi

# ============================================================================
# Derive prefix from terraform output
# ============================================================================

TF_OUTPUT=$(terraform output -json 2>/dev/null)
PREFIX=$(echo "$TF_OUTPUT" | jq -r '.storage_dsn_secret_id.value // empty' | sed 's|/storage-dsn||')
if [ -z "$PREFIX" ]; then
    echo "Error: could not determine prefix from terraform output."
    echo "Run from the environment directory (e.g., staging/)."
    exit 1
fi

echo "🔑 PlanetScale Credentials Setup"
echo "================================="
echo "  Region: $REGION"
echo "  Prefix: $PREFIX"
echo ""

# ============================================================================
# Create or update secrets
# ============================================================================

create_or_update_secret() {
    local secret_id="$1"
    local secret_value="$2"
    local description="$3"

    if aws secretsmanager describe-secret --region "$REGION" --secret-id "$secret_id" > /dev/null 2>&1; then
        echo "  Updating existing secret: $secret_id"
        aws secretsmanager update-secret \
            --region "$REGION" \
            --secret-id "$secret_id" \
            --secret-string "$secret_value" > /dev/null
    else
        echo "  Creating secret: $secret_id"
        aws secretsmanager create-secret \
            --region "$REGION" \
            --name "$secret_id" \
            --description "$description" \
            --secret-string "$secret_value" > /dev/null
    fi
}

# Service token
TOKEN_SECRET_ID="$PREFIX/planetscale-token"
TOKEN_VALUE=$(jq -n --arg token "$TOKEN" '{"token": $token}')
create_or_update_secret "$TOKEN_SECRET_ID" "$TOKEN_VALUE" "PlanetScale service token for SchemaBot"
echo "  ✓ Service token stored"
echo ""

# Vtgate DSN
VTGATE_SECRET_ID="$PREFIX/planetscale-vtgate"
VTGATE_VALUE=$(jq -n --arg dsn "$VTGATE_DSN" '{"dsn": $dsn}')
create_or_update_secret "$VTGATE_SECRET_ID" "$VTGATE_VALUE" "PlanetScale vtgate credentials for SchemaBot"
echo "  ✓ Vtgate DSN stored"
echo ""

echo "Done. Secrets stored:"
echo "  - $TOKEN_SECRET_ID"
echo "  - $VTGATE_SECRET_ID"
echo ""
echo "Config references:"
echo "  token_secret_ref: \"secretsmanager:$TOKEN_SECRET_ID#token\""
echo "  dsn: \"secretsmanager:$VTGATE_SECRET_ID#dsn\""
