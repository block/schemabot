#!/bin/bash
# Run e2e tests against gRPC mode (SchemaBot + separate Tern services).
#
# Usage:
#   ./scripts/run-e2e-grpc.sh         # Run gRPC mode e2e tests
#   DEBUG=1 ./scripts/run-e2e-grpc.sh # Keep containers running for debugging

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Generate unique project name based on git branch for parallel runs
BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "main")
SANITIZED_BRANCH=$(echo "$BRANCH" | tr -cs 'a-zA-Z0-9-_' '-')
export COMPOSE_PROJECT_NAME="schemabot-grpc-e2e-${SANITIZED_BRANCH}"

# Use dynamic ports to avoid conflicts in parallel runs
export SCHEMABOT_PORT=0
export SCHEMABOT_MYSQL_PORT=0
export TERN_STAGING_MYSQL_PORT=0
export TERN_STAGING_PORT=0
export TERN_STAGING_GRPC_PORT=0
export TERN_PRODUCTION_MYSQL_PORT=0
export TERN_PRODUCTION_PORT=0
export TERN_PRODUCTION_GRPC_PORT=0

COMPOSE_FILE="deploy/local/docker-compose.grpc.yml"

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

echo "Building and starting services (gRPC mode)..."
docker compose -f "$COMPOSE_FILE" build --quiet
docker compose -f "$COMPOSE_FILE" up -d

echo "Waiting for services to be healthy..."
timeout 120 bash -c '
    while true; do
        ADDR=$(docker compose -f "'"$COMPOSE_FILE"'" port schemabot 8080 2>/dev/null || echo "")
        if [ -n "$ADDR" ] && curl -sf "http://${ADDR}/health" > /dev/null 2>&1; then
            break
        fi
        echo "  Waiting for SchemaBot..."
        sleep 2
    done
'

SCHEMABOT_ADDR=$(docker compose -f "$COMPOSE_FILE" port schemabot 8080)
export E2E_SCHEMABOT_URL="http://${SCHEMABOT_ADDR}"

echo ""
echo "Services ready!"
echo "  SchemaBot: $E2E_SCHEMABOT_URL"
echo ""

echo "Running gRPC mode e2e tests..."
go test -tags=e2e -v -count=1 ./e2e/grpc/...

echo ""
echo "gRPC mode e2e tests passed!"
