#!/bin/bash
set -euo pipefail

# Setup multiple databases on existing RDS instances for multi-app testing.
# Creates databases and Secrets Manager DSN entries by cloning the testapp config.
#
# Usage: cd deploy/aws-multi-env/staging && ../scripts/setup-multi-db.sh [database names...]
# Default: payments orders inventory
#
# Prerequisites:
#   - bootstrap.sh has already run (RDS instances and testapp secrets exist)
#   - AWS CLI configured with correct region
#   - SSM access to bastion instance

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0" 2>/dev/null || echo "$0")")" && pwd)"
REGION="${AWS_DEFAULT_REGION:-us-west-2}"

# Derive prefix from terraform output (name_prefix used during bootstrap)
TF_OUTPUT=$(terraform output -json 2>/dev/null)
PREFIX=$(echo "$TF_OUTPUT" | jq -r '.storage_dsn_secret_id.value // empty' | sed 's|/storage-dsn||')
if [ -z "$PREFIX" ]; then
    echo "❌ Could not determine prefix from terraform output. Run from the environment directory."
    exit 1
fi

# Detect which environment this instance handles (from the testapp secret name)
ENVIRONMENT=""
for env in staging production; do
    if echo "$TF_OUTPUT" | jq -e ".testapp_${env}_dsn_secret_id.value" > /dev/null 2>&1; then
        ENVIRONMENT="$env"
        break
    fi
done
# Fallback: check which testapp secret exists
if [ -z "$ENVIRONMENT" ]; then
    for env in staging production; do
        if aws secretsmanager describe-secret --region "$REGION" --secret-id "$PREFIX/testapp-$env" > /dev/null 2>&1; then
            ENVIRONMENT="$env"
            break
        fi
    done
fi
if [ -z "$ENVIRONMENT" ]; then
    echo "❌ Could not detect environment. Ensure testapp secret exists."
    exit 1
fi

if [ $# -gt 0 ]; then
    DATABASES=("$@")
else
    DATABASES=(payments orders inventory)
fi

echo "🗄️  Multi-Database Setup"
echo "========================"
echo "  Region:      $REGION"
echo "  Prefix:      $PREFIX"
echo "  Environment: $ENVIRONMENT"
echo "  Databases:   ${DATABASES[*]}"
echo ""

# Get bastion from terraform output
BASTION_ID=$(echo "$TF_OUTPUT" | jq -r '.bastion_instance_id.value // empty')

if [ -z "$BASTION_ID" ]; then
    echo "❌ No bastion instance found. Run bootstrap.sh first."
    exit 1
fi

# Read existing testapp DSN as template
SECRET_ID="$PREFIX/testapp-$ENVIRONMENT"
TEMPLATE_DSN=$(aws secretsmanager get-secret-value --region "$REGION" \
    --secret-id "$SECRET_ID" --query SecretString --output text | \
    python3 -c "import sys,json; print(json.loads(sys.stdin.read())['dsn'])")

if [ -z "$TEMPLATE_DSN" ]; then
    echo "❌ Could not read DSN from $SECRET_ID"
    exit 1
fi

# Extract parts: user:pass@tcp(host:port)/dbname?params
BASE_DSN=$(echo "$TEMPLATE_DSN" | sed 's|/testapp?|/DBNAME?|')
echo "✅ Read template DSN from $SECRET_ID"

# Extract host for MySQL connections
DB_HOST=$(echo "$TEMPLATE_DSN" | sed 's|.*tcp(\([^)]*\)).*|\1|' | cut -d: -f1)
DB_PORT=$(echo "$TEMPLATE_DSN" | sed 's|.*tcp(\([^)]*\)).*|\1|' | cut -d: -f2)
MYSQL_USER=$(echo "$TEMPLATE_DSN" | cut -d: -f1)
MYSQL_PASS=$(echo "$TEMPLATE_DSN" | sed 's/[^:]*://' | sed 's/@.*//')

echo ""
echo "📡 Setting up SSM tunnel..."

# Find a free port
LOCAL_PORT=$((49152 + RANDOM % 16384))
while nc -z localhost "$LOCAL_PORT" 2>/dev/null; do
    LOCAL_PORT=$((49152 + RANDOM % 16384))
done

aws ssm start-session \
    --target "$BASTION_ID" \
    --region "$REGION" \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters "{\"host\":[\"$DB_HOST\"],\"portNumber\":[\"$DB_PORT\"],\"localPortNumber\":[\"$LOCAL_PORT\"]}" \
    > /dev/null 2>&1 &
TUNNEL_PID=$!

cleanup() {
    kill "$TUNNEL_PID" 2>/dev/null || true
    wait "$TUNNEL_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "   Waiting for tunnel..."
for i in $(seq 1 30); do
    if nc -z localhost "$LOCAL_PORT" 2>/dev/null; then
        break
    fi
    sleep 1
done

if ! nc -z localhost "$LOCAL_PORT" 2>/dev/null; then
    echo "❌ Tunnel failed to connect"
    exit 1
fi
echo "   ✅ Tunnel connected ($ENVIRONMENT: localhost:$LOCAL_PORT)"

MYSQL_CMD="mysql -h 127.0.0.1 -P $LOCAL_PORT -u $MYSQL_USER -p$MYSQL_PASS --ssl-mode=REQUIRED"

echo ""
echo "🗃️  Creating databases..."

for db in "${DATABASES[@]}"; do
    echo "   Creating '$db' on $ENVIRONMENT..."
    $MYSQL_CMD -e "CREATE DATABASE IF NOT EXISTS \`$db\`" 2>/dev/null
done

echo "   ✅ Databases created"

echo ""
echo "🔑 Creating Secrets Manager entries..."

for db in "${DATABASES[@]}"; do
    SECRET_ID="$PREFIX/$db-$ENVIRONMENT"
    DSN=$(echo "$BASE_DSN" | sed "s|/DBNAME?|/$db?|")
    SECRET_JSON=$(python3 -c "import json; print(json.dumps({'dsn': '$DSN'}))")

    if aws secretsmanager describe-secret --region "$REGION" --secret-id "$SECRET_ID" > /dev/null 2>&1; then
        aws secretsmanager put-secret-value --region "$REGION" \
            --secret-id "$SECRET_ID" \
            --secret-string "$SECRET_JSON" > /dev/null
        echo "   Updated $SECRET_ID"
    else
        aws secretsmanager create-secret --region "$REGION" \
            --name "$SECRET_ID" \
            --secret-string "$SECRET_JSON" > /dev/null
        echo "   Created $SECRET_ID"
    fi
done

echo "   ✅ Secrets created"

echo ""
echo "📝 Add to your config.yaml:"
echo ""

for db in "${DATABASES[@]}"; do
    echo "  $db:"
    echo "    type: mysql"
    echo "    environments:"
    echo "      $ENVIRONMENT:"
    echo "        dsn: \"secretsmanager:$PREFIX/$db-$ENVIRONMENT#dsn\""
done

echo ""
echo "✅ Multi-database setup complete!"
echo ""
echo "Next steps:"
echo "  1. Add the config above to your config.yaml"
echo "  2. Redeploy: ../scripts/deploy.sh"
