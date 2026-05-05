#!/bin/bash
# Clear data from testapp tables via SSM tunnel
#
# Usage:
#   ./scripts/clear-data.sh                  # Clear all tables in staging
#   ./scripts/clear-data.sh production       # Clear all tables in production
#   ./scripts/clear-data.sh both             # Clear all tables in both envs
#   ./scripts/clear-data.sh staging orders   # Clear only orders table in staging

set -e

REGION="us-west-2"
export AWS_PROFILE="${AWS_PROFILE:-schemabot-deployer}"

ENV=${1:-staging}
TABLE=${2:-all}

# Determine which environments to clear
if [ "$ENV" = "staging" ]; then
    envs="staging"
elif [ "$ENV" = "production" ]; then
    envs="production"
elif [ "$ENV" = "both" ]; then
    envs="staging production"
else
    echo "Usage: $0 [staging|production|both] [table]"
    echo ""
    echo "Examples:"
    echo "  $0                     # Clear all tables in staging"
    echo "  $0 production          # Clear all tables in production"
    echo "  $0 both                # Clear all tables in both envs"
    echo "  $0 staging orders      # Clear only orders in staging"
    exit 1
fi

# Determine which tables to clear
if [ "$TABLE" = "all" ]; then
    tables="users orders products"
else
    tables="$TABLE"
fi

# Check prerequisites
if ! command -v mysql &> /dev/null; then
    echo "mysql client not found. Install with: brew install mysql-client"
    exit 1
fi

if ! command -v session-manager-plugin &> /dev/null; then
    echo "AWS Session Manager plugin not found. Install with: brew install --cask session-manager-plugin"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "jq not found. Install with: brew install jq"
    exit 1
fi

if ! command -v nc &> /dev/null; then
    echo "nc (netcat) not found. Install with: brew install netcat"
    exit 1
fi

# Get terraform outputs
cd "$(dirname "$0")/.."

TF_OUTPUT=$(terraform output -json 2>/dev/null) || {
    echo "Could not get terraform output. Run: terraform init && terraform apply"
    exit 1
}

INSTANCE_ID=$(echo "$TF_OUTPUT" | jq -r '.bastion_instance_id.value // empty')

if [ -z "$INSTANCE_ID" ] || [ "$INSTANCE_ID" = "null" ]; then
    echo "Bastion not deployed"
    exit 1
fi

# Check bastion state
INSTANCE_STATE=$(aws ec2 describe-instances \
    --instance-ids "$INSTANCE_ID" \
    --region "$REGION" \
    --query 'Reservations[0].Instances[0].State.Name' \
    --output text 2>/dev/null)

if [ "$INSTANCE_STATE" != "running" ]; then
    echo "Starting bastion ($INSTANCE_STATE)..."
    aws ec2 start-instances --instance-ids "$INSTANCE_ID" --region "$REGION" > /dev/null
    aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$REGION"
    echo "Waiting for SSM agent (30s)..."
    sleep 30
fi

# Find available port
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

clear_env() {
    local env=$1
    local LOCAL_PORT
    LOCAL_PORT=$(find_free_port) || { echo "Could not find available port"; exit 1; }

    # Get endpoint and DSN for this environment
    local ENDPOINT DSN HOST USERNAME PASSWORD
    ENDPOINT=$(echo "$TF_OUTPUT" | jq -r ".testapp_${env}_endpoint.value // empty")
    DSN=$(echo "$TF_OUTPUT" | jq -r ".testapp_dsns.value.${env} // empty")
    HOST=$(echo "$ENDPOINT" | cut -d: -f1)
    USERNAME=$(echo "$DSN" | sed 's/:.*//')
    PASSWORD=$(echo "$DSN" | sed 's/[^:]*://' | sed 's/@.*//')

    echo "=== testapp-${env} ==="
    echo "    Endpoint: $HOST"

    # Start SSM tunnel
    SSM_LOG=$(mktemp)
    aws ssm start-session \
        --target "$INSTANCE_ID" \
        --region "$REGION" \
        --document-name AWS-StartPortForwardingSessionToRemoteHost \
        --parameters "{\"host\":[\"$HOST\"],\"portNumber\":[\"3306\"],\"localPortNumber\":[\"$LOCAL_PORT\"]}" \
        > "$SSM_LOG" 2>&1 &
    SSM_PID=$!

    # MySQL config file
    MYSQL_CNF=$(mktemp)
    chmod 600 "$MYSQL_CNF"
    cat > "$MYSQL_CNF" <<EOF
[client]
user=$USERNAME
password=$PASSWORD
host=127.0.0.1
port=$LOCAL_PORT
database=testapp
EOF

    # Cleanup function
    cleanup_tunnel() {
        rm -f "$MYSQL_CNF" "$SSM_LOG" 2>/dev/null || true
        kill $SSM_PID 2>/dev/null || true
    }
    trap cleanup_tunnel RETURN

    # Wait for tunnel
    echo -n "    Establishing tunnel"
    for i in $(seq 1 60); do
        if ! kill -0 $SSM_PID 2>/dev/null; then
            echo ""
            echo "    SSM session terminated"
            cat "$SSM_LOG"
            return 1
        fi
        if nc -z localhost "$LOCAL_PORT" 2>/dev/null; then
            echo " OK"
            break
        fi
        if [ $i -eq 60 ]; then
            echo ""
            echo "    Tunnel failed to establish"
            return 1
        fi
        echo -n "."
        sleep 1
    done

    # Clear each table
    for table in $tables; do
        echo -n "    Truncating $table... "
        mysql --defaults-extra-file="$MYSQL_CNF" -e "TRUNCATE TABLE $table;" 2>/dev/null
        echo "OK"
    done

    echo ""
}

echo "Clearing tables: $tables"
echo ""

for env in $envs; do
    clear_env "$env"
done

echo "Done!"
