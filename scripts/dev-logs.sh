#!/bin/bash
# View logs from development services.
#
# Usage:
#   ./scripts/dev-logs.sh              # All logs (local mode)
#   ./scripts/dev-logs.sh schemabot    # SchemaBot logs
#   ./scripts/dev-logs.sh --grpc       # All logs (gRPC mode)
#   ./scripts/dev-logs.sh --grpc -f    # Follow all logs (gRPC mode)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

COMPOSE_FILE="deploy/local/docker-compose.yml"
ARGS=()

for arg in "$@"; do
    case "$arg" in
        --grpc) COMPOSE_FILE="deploy/local/docker-compose.grpc.yml" ;;
        *) ARGS+=("$arg") ;;
    esac
done

docker compose -f "$COMPOSE_FILE" logs "${ARGS[@]}"
