#!/bin/bash
set -euo pipefail

# Connect to RDS MySQL via SSM port forwarding
# Usage: cd deploy/aws-multi-env/staging && ../scripts/shell.sh [-e staging|production|schemabot] [-c SQL]
#
# Options:
#   -e, --env       Database (staging|production|schemabot), default: staging
#   -c, --command   Execute SQL and exit (non-interactive)
#   --reset-schema  Drop and recreate the database (prompts for confirmation)
#
# Prerequisites:
#   - AWS CLI v2.12+
#   - Session Manager plugin
#   - MySQL client: brew install mysql-client

REGION="us-west-2"
DATABASE="staging"
SQL_COMMAND=""
RESET_SCHEMA=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--env)
            DATABASE="$2"
            if [[ "$DATABASE" != "staging" && "$DATABASE" != "production" && "$DATABASE" != "schemabot" ]]; then
                echo "❌ Invalid database: $DATABASE"
                echo "   Valid values: staging, production, schemabot"
                exit 1
            fi
            shift 2
            ;;
        -c|--command)
            if [ -z "${2:-}" ]; then
                echo "❌ --command requires a SQL string"
                echo "   Example: ../scripts/shell.sh -e schemabot -c 'SHOW TABLES'"
                exit 1
            fi
            SQL_COMMAND="$2"
            shift 2
            ;;
        --reset-schema)
            RESET_SCHEMA=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: ../scripts/shell.sh [-e staging|production|schemabot] [-c SQL] [--reset-schema]"
            exit 1
            ;;
    esac
done

# Validate flags
if [ "$RESET_SCHEMA" = true ] && [ -n "$SQL_COMMAND" ]; then
    echo "❌ --reset-schema and -c/--command are mutually exclusive."
    exit 1
fi

if [ -z "${AWS_PROFILE:-}" ]; then
    echo "❌ AWS_PROFILE is not set."
    echo "   Example: export AWS_PROFILE=my-profile"
    exit 1
fi

DATABASE_UPPER=$(echo "$DATABASE" | tr '[:lower:]' '[:upper:]')
echo "🔌 RDS MySQL Shell (via SSM) - ${DATABASE_UPPER}"
echo "============================================"

# Check for required tools
if ! command -v mysql &> /dev/null; then
    echo "❌ mysql client not found. Install with:"
    echo "   brew install mysql-client"
    exit 1
fi

if ! command -v session-manager-plugin &> /dev/null; then
    echo "❌ AWS Session Manager plugin not found. Install with:"
    echo "   brew install --cask session-manager-plugin"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "❌ jq not found. Install with:"
    echo "   brew install jq"
    exit 1
fi

# Find an available local port
find_free_port() {
    local port attempts=0
    while [ $attempts -lt 100 ]; do
        port=$((49152 + RANDOM % 16384))
        if ! nc -z localhost "$port" 2>/dev/null; then
            echo "$port"
            return 0
        fi
        attempts=$((attempts + 1))
    done
    return 1
}

LOCAL_PORT=$(find_free_port) || {
    echo "❌ Could not find an available port"
    exit 1
}

# Get terraform outputs (CWD is the environment directory)
TF_OUTPUT=$(terraform output -json 2>/dev/null) || {
    echo "❌ Could not get terraform output"
    echo "   Run: terraform init && terraform apply"
    exit 1
}

INSTANCE_ID=$(echo "$TF_OUTPUT" | jq -r '.bastion_instance_id.value // empty')

# Get database-specific endpoint and credentials
case "$DATABASE" in
    staging)
        ENDPOINT=$(echo "$TF_OUTPUT" | jq -r '.testapp_staging_endpoint.value // empty')
        DSN=$(echo "$TF_OUTPUT" | jq -r '.testapp_dsns.value.staging // empty')
        DB_NAME="testapp"
        ;;
    production)
        ENDPOINT=$(echo "$TF_OUTPUT" | jq -r '.testapp_production_endpoint.value // empty')
        DSN=$(echo "$TF_OUTPUT" | jq -r '.testapp_dsns.value.production // empty')
        DB_NAME="testapp"
        ;;
    schemabot)
        # SchemaBot internal storage database
        ENDPOINT=$(echo "$TF_OUTPUT" | jq -r '.schemabot_endpoint.value // empty')
        DSN=$(echo "$TF_OUTPUT" | jq -r '.schemabot_dsn.value // empty')
        DB_NAME="schemabot"
        ;;
esac

if [ -z "$INSTANCE_ID" ] || [ "$INSTANCE_ID" = "null" ]; then
    echo "❌ Bastion not deployed"
    exit 1
fi

# Parse username and password from DSN (format: user:pass@tcp(host:port)/db)
USERNAME=$(echo "$DSN" | sed 's/:.*//')
PASSWORD=$(echo "$DSN" | sed 's/[^:]*://' | sed 's/@.*//')
HOST=$(echo "$ENDPOINT" | cut -d: -f1)

echo "   Bastion: $INSTANCE_ID"
echo "   Endpoint: $HOST"
echo "   Database: $DB_NAME"
echo "   Username: $USERNAME"
echo "   Local Port: $LOCAL_PORT"
echo ""

# Check instance state
echo "🔐 Checking bastion state..."
INSTANCE_STATE=$(aws ec2 describe-instances \
    --instance-ids "$INSTANCE_ID" \
    --region "$REGION" \
    --query 'Reservations[0].Instances[0].State.Name' \
    --output text 2>/dev/null)

if [ "$INSTANCE_STATE" != "running" ]; then
    echo "⚠️  Bastion is $INSTANCE_STATE, starting..."
    aws ec2 start-instances --instance-ids "$INSTANCE_ID" --region "$REGION" > /dev/null
    aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$REGION"
    echo "   Waiting for SSM agent (30s)..."
    sleep 30
fi

# Start SSM port forwarding
echo "🔗 Starting SSM port forwarding..."
SSM_LOG=$(mktemp)
aws ssm start-session \
    --target "$INSTANCE_ID" \
    --region "$REGION" \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters "{\"host\":[\"$HOST\"],\"portNumber\":[\"3306\"],\"localPortNumber\":[\"$LOCAL_PORT\"]}" \
    > "$SSM_LOG" 2>&1 &

SSM_PID=$!

# MySQL config file (avoids password on command line)
MYSQL_CNF=$(mktemp)
chmod 600 "$MYSQL_CNF"
cat > "$MYSQL_CNF" <<EOF
[client]
user=$USERNAME
password=$PASSWORD
EOF

# Cleanup
cleanup() {
    echo ""
    echo "🧹 Cleaning up..."
    rm -f "$MYSQL_CNF" "$SSM_LOG" 2>/dev/null || true
    kill $SSM_PID 2>/dev/null || true
}
trap cleanup EXIT

# Wait for tunnel
echo -n "   Waiting for tunnel"
for i in $(seq 1 60); do
    if ! kill -0 $SSM_PID 2>/dev/null; then
        echo ""
        echo "❌ SSM session terminated"
        cat "$SSM_LOG"
        exit 1
    fi
    if nc -z localhost "$LOCAL_PORT" 2>/dev/null; then
        echo ""
        break
    fi
    if [ $i -eq 60 ]; then
        echo ""
        echo "❌ Tunnel failed to establish"
        exit 1
    fi
    echo -n "."
    sleep 1
done

echo "   ✅ Tunnel established"
echo ""
# Handle --reset-schema
if [ "$RESET_SCHEMA" = true ]; then
    echo "⚠️  This will DROP and recreate the '$DB_NAME' database."
    echo "   All data will be lost. EnsureSchema will rebuild tables on next server restart."
    echo ""
    read -r -p "   Type the database name to confirm: " CONFIRM
    if [ "$CONFIRM" != "$DB_NAME" ]; then
        echo "❌ Confirmation failed. Aborting."
        exit 1
    fi
    echo ""
    echo "🗑️  Dropping and recreating $DB_NAME..."
    mysql --defaults-extra-file="$MYSQL_CNF" -h 127.0.0.1 -P "$LOCAL_PORT" -e "DROP DATABASE \`$DB_NAME\`; CREATE DATABASE \`$DB_NAME\`;"
    echo "   ✅ Database reset. Restart the service to trigger EnsureSchema."
    exit 0
fi

# Handle -c (non-interactive SQL command)
if [ -n "$SQL_COMMAND" ]; then
    mysql --defaults-extra-file="$MYSQL_CNF" -h 127.0.0.1 -P "$LOCAL_PORT" "$DB_NAME" -e "$SQL_COMMAND"
    exit 0
fi

echo "🚀 Connecting to MySQL..."
echo "   (Use 'exit' or Ctrl+D to disconnect)"
echo ""

mysql --defaults-extra-file="$MYSQL_CNF" -h 127.0.0.1 -P "$LOCAL_PORT" "$DB_NAME"
