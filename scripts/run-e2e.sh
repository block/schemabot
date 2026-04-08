#!/bin/bash
# Run all e2e tests (local mode and gRPC mode).
#
# Usage:
#   ./scripts/run-e2e.sh         # Run all e2e tests
#   ./scripts/run-e2e.sh local   # Run only local mode tests
#   ./scripts/run-e2e.sh grpc    # Run only gRPC mode tests
#   DEBUG=1 ./scripts/run-e2e.sh # Keep containers running for debugging

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

MODE="${1:-all}"

case "$MODE" in
    local)
        "$SCRIPT_DIR/run-e2e-local.sh"
        ;;
    grpc)
        "$SCRIPT_DIR/run-e2e-grpc.sh"
        ;;
    all)
        echo "Running local mode e2e tests..."
        "$SCRIPT_DIR/run-e2e-local.sh"
        echo ""
        echo "Running gRPC mode e2e tests..."
        "$SCRIPT_DIR/run-e2e-grpc.sh"
        echo ""
        echo "All e2e tests passed!"
        ;;
    *)
        echo "Usage: $0 [local|grpc|all]"
        exit 1
        ;;
esac
