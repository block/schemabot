#!/usr/bin/env bash
#
# Run a Go test multiple times to detect flakiness.
#
# Usage:
#   scripts/test-flaky.sh <test-name> [iterations] [package]
#
# Examples:
#   scripts/test-flaky.sh TestCLI_AllowUnsafe 10 ./integration/...
#   scripts/test-flaky.sh TestMyFunction 5 ./pkg/api/...
#   scripts/test-flaky.sh TestMyFunction 5  # defaults to ./...
#   scripts/test-flaky.sh TestLocal_Progress_DuringApply 10 ./e2e/local/...
#
# For e2e tests (./e2e/...), the script automatically manages Docker:
#   - Starts docker-compose before the first run
#   - Tears down after all runs complete
#
# Environment variables:
#   TAGS_OVERRIDE - full go test tags flag, e.g. "-tags=e2e" (default: auto-detected from package path)
#   DEBUG         - set to 1 to keep e2e containers running after tests

set -euo pipefail

TEST_NAME="${1:?Usage: $0 <test-name> [iterations] [package]}"
ITERATIONS="${2:-5}"
PACKAGE="${3:-./...}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Auto-detect build tags from package path
if [ -n "${TAGS_OVERRIDE:-}" ]; then
  TAGS="$TAGS_OVERRIDE"
elif [[ "$PACKAGE" == *e2e* ]]; then
  TAGS="-tags=e2e"
elif [[ "$PACKAGE" == *integration* ]]; then
  TAGS="-tags=integration"
else
  TAGS=""
fi

COMPOSE_FILE="deploy/local/docker-compose.yml"

# Generate unique project name based on git branch for parallel runs
BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "main")
SANITIZED_BRANCH=$(echo "$BRANCH" | tr -cs 'a-zA-Z0-9-_' '-')
export COMPOSE_PROJECT_NAME="schemabot-flaky-${SANITIZED_BRANCH}"

# Use dynamic ports to avoid conflicts with other test runs
export SCHEMABOT_PORT=0
export SCHEMABOT_MYSQL_PORT=0
export STAGING_MYSQL_PORT=0
export PRODUCTION_MYSQL_PORT=0

# E2E environment management
E2E_MANAGED=false

start_e2e_env() {
  echo "Starting e2e environment..."

  # Build linux binary for docker
  CGO_ENABLED=0 GOOS=linux go build -o bin/schemabot-linux ./pkg/cmd
  cp bin/schemabot-linux deploy/local/schemabot

  docker compose -f "$COMPOSE_FILE" build --quiet
  rm -f deploy/local/schemabot
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

  # Apply base schema
  make build > /dev/null 2>&1
  bin/schemabot apply -s examples/mysql/schema/testapp -e staging --endpoint "$E2E_SCHEMABOT_URL" -y --watch=false > /dev/null 2>&1 || true
  bin/schemabot apply -s examples/mysql/schema/testapp -e production --endpoint "$E2E_SCHEMABOT_URL" -y --watch=false > /dev/null 2>&1 || true

  E2E_MANAGED=true
}

stop_e2e_env() {
  if [ "$E2E_MANAGED" = true ] && [ "${DEBUG:-}" != "1" ]; then
    echo "Tearing down e2e environment..."
    docker_output=$(docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>&1 || true)
    echo "$docker_output" | tail -3
  elif [ "$E2E_MANAGED" = true ]; then
    SCHEMABOT_ADDR=$(docker compose -f "$COMPOSE_FILE" port schemabot 8080 2>/dev/null || echo "unknown")
    echo ""
    echo "DEBUG mode: containers are still running"
    echo "SchemaBot URL: http://${SCHEMABOT_ADDR}"
    echo ""
    echo "To stop containers: docker compose -f $COMPOSE_FILE down"
  fi
}

# Start e2e env if testing e2e package
if [[ "$PACKAGE" == *e2e/local* ]] || [[ "$PACKAGE" == *e2e/grpc* ]]; then
  trap stop_e2e_env EXIT
  start_e2e_env
fi

PASS=0
FAIL=0

echo ""
echo "Running $TEST_NAME $ITERATIONS times in $PACKAGE ${TAGS:+($TAGS)}"
echo "---"

for i in $(seq 1 "$ITERATIONS"); do
  if go test -count=1 -timeout=5m $TAGS -run "^${TEST_NAME}$" "$PACKAGE" > /dev/null 2>&1; then
    PASS=$((PASS + 1))
    echo "  Run $i/$ITERATIONS: PASS"
  else
    FAIL=$((FAIL + 1))
    echo "  Run $i/$ITERATIONS: FAIL"
  fi
done

echo "---"
echo "Results: $PASS passed, $FAIL failed out of $ITERATIONS runs"

if [ "$FAIL" -gt 0 ]; then
  echo "FLAKY: $TEST_NAME failed $FAIL/$ITERATIONS times"
  exit 1
else
  echo "STABLE: $TEST_NAME passed $ITERATIONS/$ITERATIONS times"
fi
