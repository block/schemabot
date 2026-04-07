#!/bin/bash
# Quick smoke test for SchemaBot API endpoints.
#
# Usage:
#   ./scripts/test-endpoints.sh          # Test local mode (default)
#   ./scripts/test-endpoints.sh --grpc   # Test gRPC mode
#
# Requires services to be running (./scripts/dev-up.sh).
set -e

COMPOSE_FILE="deploy/local/docker-compose.yml"
for arg in "$@"; do
    case "$arg" in
        --grpc) COMPOSE_FILE="deploy/local/docker-compose.grpc.yml" ;;
    esac
done

SCHEMABOT_ADDR=$(docker compose -f "$COMPOSE_FILE" port schemabot 8080 2>/dev/null)
if [ -z "$SCHEMABOT_ADDR" ]; then
    echo "Error: SchemaBot not running. Start with: ./scripts/dev-up.sh"
    exit 1
fi

echo "=== SchemaBot (http://$SCHEMABOT_ADDR) ==="
echo
echo "GET /health"
curl -s "http://$SCHEMABOT_ADDR/health" | jq .
echo
