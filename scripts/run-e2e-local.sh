#!/bin/bash
# Run e2e tests against local mode (SchemaBot with embedded Tern).
#
# Usage:
#   ./scripts/run-e2e-local.sh         # Run local mode e2e tests
#   DEBUG=1 ./scripts/run-e2e-local.sh # Keep containers running for debugging

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Generate unique project name based on git branch for parallel runs
BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "main")
SANITIZED_BRANCH=$(echo "$BRANCH" | tr -cs 'a-zA-Z0-9-_' '-')
export COMPOSE_PROJECT_NAME="schemabot-local-e2e-${SANITIZED_BRANCH}"

COMPOSE_FILE="deploy/local/docker-compose.yml"

# Use dynamic ports (0 = let Docker assign available ports) to avoid conflicts
export SCHEMABOT_PORT=0
export SCHEMABOT_MYSQL_PORT=0
export STAGING_MYSQL_PORT=0
export PRODUCTION_MYSQL_PORT=0

cleanup() {
    if [ "$DEBUG" = "1" ]; then
        SCHEMABOT_ADDR=$(docker compose -f "$COMPOSE_FILE" port schemabot 8080 2>/dev/null || echo "unknown")
        echo ""
        echo "DEBUG mode: containers are still running"
        echo "SchemaBot URL: http://${SCHEMABOT_ADDR}"
        echo ""
        echo "To stop containers: docker compose -f $COMPOSE_FILE down"
        exit 0
    fi

    echo "Cleaning up containers..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}

trap cleanup EXIT

echo "Building and starting services (local mode)..."
docker compose -f "$COMPOSE_FILE" build --quiet
docker compose -f "$COMPOSE_FILE" up -d

echo "Waiting for SchemaBot to be healthy..."
timeout 120 bash -c '
    while true; do
        ADDR=$(docker compose -f "'"$COMPOSE_FILE"'" port schemabot 8080 2>/dev/null || echo "")
        if [ -n "$ADDR" ] && curl -sf "http://${ADDR}/health" > /dev/null 2>&1; then
            break
        fi
        echo "  Waiting for SchemaBot to be healthy..."
        sleep 2
    done
'

# Get the dynamically assigned ports
SCHEMABOT_ADDR=$(docker compose -f "$COMPOSE_FILE" port schemabot 8080)
export E2E_SCHEMABOT_URL="http://${SCHEMABOT_ADDR}"

# Get MySQL ports for DSNs
MYSQL_SCHEMABOT_PORT=$(docker compose -f "$COMPOSE_FILE" port mysql-schemabot 3306 | cut -d: -f2)
MYSQL_STAGING_PORT=$(docker compose -f "$COMPOSE_FILE" port mysql-staging 3306 | cut -d: -f2)

export E2E_MYSQL_DSN="root:testpassword@tcp(127.0.0.1:${MYSQL_SCHEMABOT_PORT})/schemabot?parseTime=true"
export E2E_TESTAPP_STAGING_DSN="root:testpassword@tcp(127.0.0.1:${MYSQL_STAGING_PORT})/testapp?parseTime=true"

echo ""
echo "Services ready!"
echo "  SchemaBot: $E2E_SCHEMABOT_URL"
echo "  SchemaBot MySQL: 127.0.0.1:${MYSQL_SCHEMABOT_PORT}"
echo "  Staging MySQL: 127.0.0.1:${MYSQL_STAGING_PORT}"
echo ""

echo "Running local mode e2e tests..."
go test -tags=e2e -v -count=1 ./e2e/local/...

echo ""
echo "Local mode e2e tests passed!"
