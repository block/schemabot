#!/bin/bash
# Seed testapp with large dataset via SSM tunnel
#
# Usage:
#   ./scripts/seed-large.sh                       # 100 MB per table, staging only
#   ./scripts/seed-large.sh 500                   # 500 MB per table, staging only
#   ./scripts/seed-large.sh 100 both              # 100 MB per table, both envs
#   ./scripts/seed-large.sh 600 both orders       # 600 MB orders only, both envs

set -e

REGION="us-west-2"
export AWS_PROFILE="${AWS_PROFILE:-schemabot-deployer}"

TARGET_MB=${1:-100}
ENV=${2:-staging}
TABLE=${3:-all}
BATCH_SIZE=100000

# Determine which environments to seed
if [ "$ENV" = "staging" ]; then
    envs="staging"
elif [ "$ENV" = "production" ]; then
    envs="production"
elif [ "$ENV" = "both" ]; then
    envs="staging production"
else
    echo "Usage: $0 [target_mb] [staging|production|both] [table]"
    echo ""
    echo "Examples:"
    echo "  $0 100                 # 100 MB per table, staging"
    echo "  $0 100 both            # 100 MB per table, both envs"
    echo "  $0 500 production      # 500 MB per table, production"
    echo "  $0 600 both orders     # 600 MB orders only, both envs"
    exit 1
fi

# Determine which tables to seed
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

if ! command -v bc &> /dev/null; then
    echo "bc not found. Install with: brew install bc"
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

seed_env() {
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
    echo "    Local port: $LOCAL_PORT"
    echo ""

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

    # Seed each table
    for table in $tables; do
        seed_table "$MYSQL_CNF" "$table"
    done

    echo ""
    echo "    Final sizes:"
    # Build IN clause for selected tables
    local in_clause
    in_clause=$(echo $tables | sed "s/ /','/g" | sed "s/^/('/" | sed "s/$/')/")
    mysql --defaults-extra-file="$MYSQL_CNF" -e "
        SELECT table_name, table_rows,
            ROUND(data_length / 1024 / 1024, 2) AS data_mb,
            ROUND(index_length / 1024 / 1024, 2) AS index_mb,
            ROUND((data_length + index_length) / 1024 / 1024, 2) AS total_mb
        FROM information_schema.tables
        WHERE table_schema = 'testapp' AND table_name IN $in_clause
        ORDER BY table_name;"
    echo ""
}

get_table_size_mb() {
    local cnf=$1
    local table=$2
    mysql --defaults-extra-file="$cnf" -sN -e "
        ANALYZE TABLE $table;
        SELECT ROUND((data_length + index_length) / 1024 / 1024, 2)
        FROM information_schema.tables
        WHERE table_schema = 'testapp' AND table_name = '$table';" 2>/dev/null | tail -1 || echo "0"
}

seed_users() {
    local cnf=$1
    mysql --defaults-extra-file="$cnf" -e "
        INSERT INTO users (email)
        SELECT CONCAT(SUBSTRING(MD5(RAND()), 1, 12), '_', seq, '@', SUBSTRING(MD5(RAND()), 1, 8), '.com')
        FROM (SELECT @row := @row + 1 as seq FROM
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) e,
            (SELECT @row := 0) r
        ) nums LIMIT $BATCH_SIZE;" 2>/dev/null
}

seed_orders() {
    local cnf=$1
    mysql --defaults-extra-file="$cnf" -e "
        INSERT INTO orders (user_id, total_cents, status)
        SELECT
            FLOOR(1 + RAND() * 1000000),
            FLOOR(100 + RAND() * 100000),
            ELT(FLOOR(1 + RAND() * 4), 'pending', 'processing', 'shipped', 'delivered')
        FROM (SELECT @row := @row + 1 as seq FROM
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) e,
            (SELECT @row := 0) r
        ) nums LIMIT $BATCH_SIZE;" 2>/dev/null
}

seed_products() {
    local cnf=$1
    mysql --defaults-extra-file="$cnf" -e "
        INSERT INTO products (name, description, price_cents, sku)
        SELECT
            CONCAT('Product ', FLOOR(RAND() * 1000000)),
            CONCAT('Description for product ', FLOOR(RAND() * 1000000), '. This is a longer description to add some data.'),
            FLOOR(100 + RAND() * 100000),
            CONCAT('SKU-', SUBSTRING(MD5(RAND()), 1, 12))
        FROM (SELECT @row := @row + 1 as seq FROM
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
            (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) e,
            (SELECT @row := 0) r
        ) nums LIMIT $BATCH_SIZE;" 2>/dev/null
}

seed_table() {
    local cnf=$1
    local table=$2

    local current_mb
    current_mb=$(get_table_size_mb "$cnf" "$table")
    echo "    $table: ${current_mb} MB (target: ${TARGET_MB} MB)"

    while (( $(echo "$current_mb < $TARGET_MB" | bc -l) )); do
        case "$table" in
            users) seed_users "$cnf" ;;
            orders) seed_orders "$cnf" ;;
            products) seed_products "$cnf" ;;
        esac
        current_mb=$(get_table_size_mb "$cnf" "$table")
        local row_count
        row_count=$(mysql --defaults-extra-file="$cnf" -sN -e "SELECT COUNT(*) FROM $table" 2>/dev/null)
        printf "\r    $table: %.2f MB / %d MB (%s rows)   " "$current_mb" "$TARGET_MB" "$row_count"
    done
    echo ""
}

echo "Target: ${TARGET_MB} MB per table ($tables)"
echo ""

for env in $envs; do
    seed_env "$env"
done

echo "Done!"
