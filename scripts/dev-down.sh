#!/bin/bash
# Stop all development services.
#
# Usage:
#   ./scripts/dev-down.sh          # Stop local mode services, keep data
#   ./scripts/dev-down.sh -v       # Stop local mode services and remove volumes
#   ./scripts/dev-down.sh --grpc   # Stop gRPC mode services

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

COMPOSE_FILE="deploy/local/docker-compose.yml"
VOLUME_FLAG=""

for arg in "$@"; do
    case "$arg" in
        --grpc) COMPOSE_FILE="deploy/local/docker-compose.grpc.yml" ;;
        -v) VOLUME_FLAG="-v" ;;
    esac
done

if [ -n "$VOLUME_FLAG" ]; then
    echo "Stopping services and removing volumes..."
else
    echo "Stopping services..."
fi

docker compose -f "$COMPOSE_FILE" down $VOLUME_FLAG

echo "Done."
