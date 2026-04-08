#!/bin/bash
# Start all services for local development.
#
# Usage:
#   ./scripts/dev-up.sh          # Start local mode (embedded Tern)
#   ./scripts/dev-up.sh --grpc   # Start gRPC mode (separate Tern services)
#   ./scripts/dev-up.sh --build  # Rebuild images before starting
#
# Local mode endpoints:
#   SchemaBot API:     http://localhost:13370
#   SchemaBot MySQL:   localhost:13371
#   Staging MySQL:     localhost:13372
#   Production MySQL:  localhost:13373
#
# gRPC mode endpoints:
#   SchemaBot API:     http://localhost:13380
#   SchemaBot MySQL:   localhost:13381
#   Staging MySQL:     localhost:13382
#   Production MySQL:  localhost:13383

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

COMPOSE_FILE="deploy/local/docker-compose.yml"
MODE="local"
BUILD_FLAG=""

for arg in "$@"; do
    case "$arg" in
        --grpc) COMPOSE_FILE="deploy/local/docker-compose.grpc.yml"; MODE="grpc" ;;
        --build) BUILD_FLAG="--build" ;;
    esac
done

echo "Starting SchemaBot development environment ($MODE mode)..."
docker compose -f "$COMPOSE_FILE" up -d $BUILD_FLAG

echo ""
echo "Waiting for services to be ready..."
sleep 5

check_health() {
    local name="$1"
    local url="$2"
    if curl -sf "$url" > /dev/null 2>&1; then
        echo "  $name: ready"
    else
        echo "  $name: not ready yet (check logs)"
    fi
}

if [ "$MODE" = "grpc" ]; then
    check_health "SchemaBot" "http://localhost:13380/health"
    echo ""
    echo "Development environment ready!"
    echo ""
    echo "Endpoints:"
    echo "  SchemaBot:       http://localhost:13380"
    echo "  Tern Staging:    http://localhost:13384 (HTTP), localhost:13385 (gRPC)"
    echo "  Tern Production: http://localhost:13386 (HTTP), localhost:13387 (gRPC)"
else
    check_health "SchemaBot" "http://localhost:13370/health"
    echo ""
    echo "Development environment ready!"
    echo ""
    echo "Endpoints:"
    echo "  SchemaBot API:     http://localhost:13370"
    echo "  SchemaBot MySQL:   localhost:13371"
    echo "  Staging MySQL:     localhost:13372"
    echo "  Production MySQL:  localhost:13373"
fi

echo ""
echo "Useful commands:"
echo "  ./scripts/dev-logs.sh          # View all logs"
echo "  ./scripts/dev-logs.sh schemabot  # View SchemaBot logs"
echo "  ./scripts/dev-down.sh          # Stop all services"
