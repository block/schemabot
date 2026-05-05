#!/bin/bash
set -euo pipefail

# Setup multiple databases on existing RDS instances for multi-app testing.
# Creates databases and Secrets Manager DSN entries by cloning the testapp config.
#
# Usage: ./setup-multi-db.sh [database names...]
# Default: payments orders inventory
#
# Prerequisites:
#   - bootstrap.sh has already run (RDS instances and testapp secrets exist)
#   - AWS CLI configured with correct region
#   - SSM access to bastion instance

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REGION="${AWS_DEFAULT_REGION:-us-west-2}"
PREFIX="schemabot-example"
if [ $# -gt 0 ]; then
    DATABASES=("$@")
else
    DATABASES=(payments orders inventory)
fi

echo "🗄️  Multi-Database Setup"
echo "========================"
echo "  Region:    $REGION"
echo "  Prefix:    $PREFIX"
echo "  Databases: ${DATABASES[*]}"
echo ""

# Get terraform output for bastion and endpoints
cd "$SCRIPT_DIR/../"
TF_OUTPUT=$(terraform output -json 2>/dev/null)
BASTION_ID=$(echo "$TF_OUTPUT" | jq -r '.bastion_instance_id.value // empty')

if [ -z "$BASTION_ID" ]; then
    echo "❌ No bastion instance found. Run bootstrap.sh first."
    exit 1
fi

# Read existing testapp DSNs as templates
for env in staging production; do
    SECRET_ID="$PREFIX/testapp-$env"
    DSN=$(aws secretsmanager get-secret-value --region "$REGION" \
        --secret-id "$SECRET_ID" --query SecretString --output text | \
        python3 -c "import sys,json; print(json.loads(sys.stdin.read())['dsn'])")

    if [ -z "$DSN" ]; then
        echo "❌ Could not read DSN from $SECRET_ID"
        exit 1
    fi

    # Extract parts: user:pass@tcp(host:port)/dbname?params
    BASE_DSN=$(echo "$DSN" | sed 's|/testapp?|/DBNAME?|')
    if [ "$env" = "staging" ]; then
        TEMPLATE_DSN_STAGING="$BASE_DSN"
    else
        TEMPLATE_DSN_PRODUCTION="$BASE_DSN"
    fi
done

echo "✅ Read template DSNs from testapp secrets"

# Extract host for MySQL connections
STAGING_HOST=$(echo "$TEMPLATE_DSN_STAGING" | sed 's|.*tcp(\([^)]*\)).*|\1|' | cut -d: -f1)
STAGING_PORT=$(echo "$TEMPLATE_DSN_STAGING" | sed 's|.*tcp(\([^)]*\)).*|\1|' | cut -d: -f2)
PROD_HOST=$(echo "$TEMPLATE_DSN_PRODUCTION" | sed 's|.*tcp(\([^)]*\)).*|\1|' | cut -d: -f1)
PROD_PORT=$(echo "$TEMPLATE_DSN_PRODUCTION" | sed 's|.*tcp(\([^)]*\)).*|\1|' | cut -d: -f2)
MYSQL_USER=$(echo "$TEMPLATE_DSN_STAGING" | cut -d: -f1)
MYSQL_PASS=$(echo "$TEMPLATE_DSN_STAGING" | sed 's/[^:]*://' | sed 's/@.*//')

echo ""
echo "📡 Setting up SSM tunnels..."

# Find free ports for tunnels
STAGING_LOCAL_PORT=$((49152 + RANDOM % 16384))
while nc -z localhost "$STAGING_LOCAL_PORT" 2>/dev/null; do
    STAGING_LOCAL_PORT=$((49152 + RANDOM % 16384))
done

PROD_LOCAL_PORT=$((49152 + RANDOM % 16384))
while nc -z localhost "$PROD_LOCAL_PORT" 2>/dev/null; do
    PROD_LOCAL_PORT=$((49152 + RANDOM % 16384))
done

# Start SSM tunnels
aws ssm start-session \
    --target "$BASTION_ID" \
    --region "$REGION" \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters "{\"host\":[\"$STAGING_HOST\"],\"portNumber\":[\"$STAGING_PORT\"],\"localPortNumber\":[\"$STAGING_LOCAL_PORT\"]}" \
    > /dev/null 2>&1 &
STAGING_TUNNEL_PID=$!

aws ssm start-session \
    --target "$BASTION_ID" \
    --region "$REGION" \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters "{\"host\":[\"$PROD_HOST\"],\"portNumber\":[\"$PROD_PORT\"],\"localPortNumber\":[\"$PROD_LOCAL_PORT\"]}" \
    > /dev/null 2>&1 &
PROD_TUNNEL_PID=$!

cleanup() {
    kill "$STAGING_TUNNEL_PID" "$PROD_TUNNEL_PID" 2>/dev/null || true
    wait "$STAGING_TUNNEL_PID" "$PROD_TUNNEL_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for tunnels
echo "   Waiting for tunnels..."
for i in $(seq 1 30); do
    if nc -z localhost "$STAGING_LOCAL_PORT" 2>/dev/null && nc -z localhost "$PROD_LOCAL_PORT" 2>/dev/null; then
        break
    fi
    sleep 1
done

if ! nc -z localhost "$STAGING_LOCAL_PORT" 2>/dev/null; then
    echo "❌ Staging tunnel failed to connect"
    exit 1
fi
echo "   ✅ Tunnels connected (staging: $STAGING_LOCAL_PORT, production: $PROD_LOCAL_PORT)"

MYSQL_STAGING="mysql -h 127.0.0.1 -P $STAGING_LOCAL_PORT -u $MYSQL_USER -p$MYSQL_PASS --ssl-mode=REQUIRED"
MYSQL_PROD="mysql -h 127.0.0.1 -P $PROD_LOCAL_PORT -u $MYSQL_USER -p$MYSQL_PASS --ssl-mode=REQUIRED"

echo ""
echo "🗃️  Creating databases..."

for db in "${DATABASES[@]}"; do
    echo "   Creating '$db' on staging..."
    $MYSQL_STAGING -e "CREATE DATABASE IF NOT EXISTS \`$db\`" 2>/dev/null

    echo "   Creating '$db' on production..."
    $MYSQL_PROD -e "CREATE DATABASE IF NOT EXISTS \`$db\`" 2>/dev/null
done

echo "   ✅ Databases created"

echo ""
echo "🔑 Creating Secrets Manager entries..."

for db in "${DATABASES[@]}"; do
    for env in staging production; do
        SECRET_ID="$PREFIX/$db-$env"

        if [ "$env" = "staging" ]; then
            DSN=$(echo "$TEMPLATE_DSN_STAGING" | sed "s|/DBNAME?|/$db?|")
        else
            DSN=$(echo "$TEMPLATE_DSN_PRODUCTION" | sed "s|/DBNAME?|/$db?|")
        fi

        SECRET_JSON=$(python3 -c "import json; print(json.dumps({'dsn': '$DSN'}))")

        # Create or update secret
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
done

echo "   ✅ Secrets created"

echo ""
echo "📝 Config snippet for deploy/aws/config.yaml:"
echo ""
echo "databases:"
echo "  testapp:"
echo "    type: mysql"
echo "    environments:"
echo "      staging:"
echo "        dsn: \"secretsmanager:$PREFIX/testapp-staging#dsn\""
echo "      production:"
echo "        dsn: \"secretsmanager:$PREFIX/testapp-production#dsn\""

for db in "${DATABASES[@]}"; do
    echo "  $db:"
    echo "    type: mysql"
    echo "    environments:"
    echo "      staging:"
    echo "        dsn: \"secretsmanager:$PREFIX/$db-staging#dsn\""
    echo "      production:"
    echo "        dsn: \"secretsmanager:$PREFIX/$db-production#dsn\""
done

echo ""
echo "✅ Multi-database setup complete!"
echo ""
echo "Next steps:"
echo "  1. Update deploy/aws/config.yaml with the config above"
echo "  2. Deploy: deploy/aws/scripts/deploy.sh"
echo "  3. Copy examples/multi-app/ into your test repo for schema directories"
