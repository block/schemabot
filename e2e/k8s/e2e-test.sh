#!/usr/bin/env bash
# Runs k8s e2e tests on minikube.
#
# Starts minikube if needed, builds and loads the SchemaBot image, deploys
# MySQL + control plane + data plane via Helm, then runs the e2e/k8s test
# suite against the control plane's HTTP API.
#
# Prerequisites: minikube, helm, and docker installed.
#
# Usage:
#   e2e/k8s/e2e-test.sh

set -euo pipefail

NAMESPACE="schemabot-e2e"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# --- Prerequisites ---

for cmd in minikube helm docker kubectl go; do
    if ! command -v "$cmd" > /dev/null 2>&1; then
        echo "Error: $cmd is not installed"
        exit 1
    fi
done

# --- Minikube ---

if ! minikube status > /dev/null 2>&1; then
    echo "Starting minikube..."
    minikube start --driver=docker --cpus=2 --memory=2048
fi

# --- Build image ---

echo "Building image for minikube..."
CGO_ENABLED=0 GOOS=linux go build -o "$REPO_ROOT/bin/schemabot-linux" ./pkg/cmd
cp "$REPO_ROOT/bin/schemabot-linux" "$REPO_ROOT/deploy/local/schemabot-dev"
eval $(minikube docker-env)
docker build -t schemabot:test -f "$REPO_ROOT/deploy/local/Dockerfile.dev" "$REPO_ROOT/deploy/local/"
rm -f "$REPO_ROOT/deploy/local/schemabot-dev"

# Track background PIDs for cleanup
PIDS=()
cleanup() {
    echo "Cleaning up port-forwards..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

# --- Deploy ---

echo "Creating namespace..."
kubectl create namespace "$NAMESPACE" 2>/dev/null || true

echo "Deploying MySQL..."
kubectl apply -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/mysql.yaml"
kubectl wait --for=condition=ready pod -l app=mysql-control-plane -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=ready pod -l app=mysql-data-plane -n "$NAMESPACE" --timeout=120s

echo "Installing data plane..."
helm upgrade --install data-plane "$REPO_ROOT/charts/schemabot" \
    -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/data-plane-values.yaml"
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=data-plane -n "$NAMESPACE" --timeout=120s

echo "Installing control plane..."
helm upgrade --install control-plane "$REPO_ROOT/charts/schemabot" \
    -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/control-plane-values.yaml"
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=control-plane -n "$NAMESPACE" --timeout=120s

# --- Port-forwards ---

echo "Starting port-forwards..."
kubectl port-forward -n "$NAMESPACE" svc/control-plane-schemabot 8080:8080 &
PIDS+=($!)
kubectl port-forward -n "$NAMESPACE" svc/mysql-control-plane 3307:3306 &
PIDS+=($!)
kubectl port-forward -n "$NAMESPACE" svc/mysql-data-plane 3308:3306 &
PIDS+=($!)

echo "Waiting for port-forwards..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
        echo "Control plane is healthy"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "Timeout waiting for port-forward"
        exit 1
    fi
    sleep 1
done

# --- Test ---

echo "Running k8s e2e tests..."
TEST_EXIT_CODE=0
E2E_SCHEMABOT_URL=http://localhost:8080 \
E2E_SCHEMABOT_MYSQL_DSN="root:testpassword@tcp(localhost:3307)/schemabot?parseTime=true&multiStatements=true" \
E2E_TERN_STAGING_MYSQL_DSN="root:testpassword@tcp(localhost:3308)/testapp?parseTime=true&multiStatements=true" \
go test -count=1 -v -tags=e2e -timeout=10m ./e2e/k8s/... || TEST_EXIT_CODE=$?

# --- Teardown ---

echo "Tearing down..."
kubectl delete namespace "$NAMESPACE" --ignore-not-found

exit "$TEST_EXIT_CODE"
