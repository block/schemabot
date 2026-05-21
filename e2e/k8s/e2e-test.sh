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
kubectl config use-context minikube > /dev/null

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
    for pid in "${PIDS[@]:-}"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT

collect_pod_logs() {
    echo "=== Control Plane ==="
    kubectl logs -n "$NAMESPACE" -l app.kubernetes.io/instance=control-plane --tail=200 2>/dev/null || true
    echo "=== Data Plane ==="
    kubectl logs -n "$NAMESPACE" -l app.kubernetes.io/instance=data-plane --tail=200 2>/dev/null || true
    echo "=== MySQL Control Plane ==="
    kubectl logs -n "$NAMESPACE" -l app=mysql-control-plane --tail=50 2>/dev/null || true
    echo "=== MySQL Data Plane ==="
    kubectl logs -n "$NAMESPACE" -l app=mysql-data-plane --tail=50 2>/dev/null || true
    echo "=== All pods ==="
    kubectl get pods -n "$NAMESPACE" -o wide 2>/dev/null || true
}

wait_for_schemabot_rollout() {
    local release_name="$1"

    # Wait on the Deployment created by Helm. Selecting pods directly can race
    # the controller before it creates the first matching pod.
    kubectl rollout status "deployment/${release_name}-schemabot" \
        -n "$NAMESPACE" --timeout=120s
}

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
wait_for_schemabot_rollout data-plane

echo "Installing control plane..."
helm upgrade --install control-plane "$REPO_ROOT/charts/schemabot" \
    -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/control-plane-values.yaml"
wait_for_schemabot_rollout control-plane

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
go test -count=1 -v -tags=e2e -timeout=8m ./e2e/k8s/... || TEST_EXIT_CODE=$?

# --- Teardown ---

if [ "$TEST_EXIT_CODE" -ne 0 ]; then
    echo "k8s e2e tests failed; collecting pod logs before teardown..."
    collect_pod_logs
fi

echo "Tearing down..."
kubectl delete namespace "$NAMESPACE" --ignore-not-found

exit "$TEST_EXIT_CODE"
