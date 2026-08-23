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

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# The namespace and local ports are derived per run so concurrent suites on
# one cluster (e.g. two worktrees) deploy isolated stacks instead of fighting
# over one namespace and one set of localhost forwards. E2E_K8S_NAMESPACE is
# exported to the Go tests, which target the same namespace for pod crashes
# and replacement port-forwards.
sanitized_branch() {
    git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null \
        | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9-' '-' \
        | sed 's/^-*//' | cut -c1-40 | sed 's/-*$//'
}
BRANCH_SLUG="$(sanitized_branch)"
NAMESPACE="${E2E_K8S_NAMESPACE:-schemabot-e2e-${BRANCH_SLUG:-local}}"
export E2E_K8S_NAMESPACE="$NAMESPACE"

# free_local_port picks a random port nothing is listening on, from the given
# base/span. The probe uses bash's /dev/tcp so it needs no external tool, and
# each forward draws from its own disjoint range so the three ports can never
# collide with each other.
free_local_port() {
    local base="$1" span="$2" port
    while :; do
        port=$(( (RANDOM % span) + base ))
        if ! (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
            echo "$port"
            return
        fi
        exec 3>&-
    done
}
CONTROL_PLANE_PORT="$(free_local_port 20000 6000)"
MYSQL_CONTROL_PLANE_PORT="$(free_local_port 26000 6000)"
MYSQL_DATA_PLANE_PORT="$(free_local_port 32000 6000)"

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

# The image tag is per-run for the same reason as the namespace: the chart
# pulls with pullPolicy Never, so a concurrent run rebuilding a shared tag
# would swap the binary under this run's pod restarts.
IMAGE_TAG="e2e-${BRANCH_SLUG:-local}"

echo "Building image schemabot:${IMAGE_TAG} for minikube..."
CGO_ENABLED=0 GOOS=linux go build -o "$REPO_ROOT/bin/schemabot-linux" ./pkg/cmd
cp "$REPO_ROOT/bin/schemabot-linux" "$REPO_ROOT/deploy/local/schemabot-dev"
eval $(minikube docker-env)
docker build -t "schemabot:${IMAGE_TAG}" -f "$REPO_ROOT/deploy/local/Dockerfile.dev" "$REPO_ROOT/deploy/local/"
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

wait_for_ready_pods() {
    local selector="$1"
    local timeout_seconds="${2:-120}"
    local deadline=$((SECONDS + timeout_seconds))

    until kubectl get pod -n "$NAMESPACE" -l "$selector" -o name 2>/dev/null | grep -q .; do
        if ((SECONDS >= deadline)); then
            echo "Timeout waiting for pods matching selector: $selector"
            kubectl get pods -n "$NAMESPACE" -o wide || true
            exit 1
        fi
        sleep 1
    done

    local remaining=$((deadline - SECONDS))
    if ((remaining < 1)); then
        remaining=1
    fi
    kubectl wait --for=condition=ready pod -l "$selector" -n "$NAMESPACE" --timeout="${remaining}s"
}

# --- Deploy ---

echo "Creating namespace..."
kubectl create namespace "$NAMESPACE" 2>/dev/null || true

echo "Deploying MySQL..."
kubectl apply -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/mysql.yaml"
wait_for_ready_pods "app=mysql-control-plane"
wait_for_ready_pods "app=mysql-data-plane"

echo "Installing data plane..."
helm upgrade --install data-plane "$REPO_ROOT/charts/schemabot" \
    -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/data-plane-values.yaml" \
    --set image.tag="$IMAGE_TAG"
wait_for_ready_pods "app.kubernetes.io/instance=data-plane"

echo "Installing control plane..."
helm upgrade --install control-plane "$REPO_ROOT/charts/schemabot" \
    -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/control-plane-values.yaml" \
    --set image.tag="$IMAGE_TAG"
wait_for_ready_pods "app.kubernetes.io/instance=control-plane"

# --- Port-forwards ---

echo "Starting port-forwards (namespace $NAMESPACE, control plane :$CONTROL_PLANE_PORT, mysql :$MYSQL_CONTROL_PLANE_PORT/:$MYSQL_DATA_PLANE_PORT)..."
kubectl port-forward -n "$NAMESPACE" svc/control-plane-schemabot "${CONTROL_PLANE_PORT}:8080" &
PIDS+=($!)
kubectl port-forward -n "$NAMESPACE" svc/mysql-control-plane "${MYSQL_CONTROL_PLANE_PORT}:3306" &
PIDS+=($!)
kubectl port-forward -n "$NAMESPACE" svc/mysql-data-plane "${MYSQL_DATA_PLANE_PORT}:3306" &
PIDS+=($!)

echo "Waiting for port-forwards..."
for i in $(seq 1 30); do
    if curl -sf "http://localhost:${CONTROL_PLANE_PORT}/health" > /dev/null 2>&1; then
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
# The etre- and vitess-resolver tests need their own in-cluster stacks and run in
# dedicated jobs, so skip them here rather than against the base stack.
E2E_SCHEMABOT_URL="http://localhost:${CONTROL_PLANE_PORT}" \
E2E_SCHEMABOT_MYSQL_DSN="root:testpassword@tcp(localhost:${MYSQL_CONTROL_PLANE_PORT})/schemabot?parseTime=true&multiStatements=true" \
E2E_TERN_STAGING_MYSQL_DSN="root:testpassword@tcp(localhost:${MYSQL_DATA_PLANE_PORT})/testapp?parseTime=true&multiStatements=true" \
    go test -count=1 -v -tags=e2e -timeout=10m -skip 'TestK8sEtre|TestK8sVitess' ./e2e/k8s/... || TEST_EXIT_CODE=$?

# --- Teardown ---

echo "Tearing down..."
kubectl delete namespace "$NAMESPACE" --ignore-not-found
# Best-effort: without this, per-run image tags accumulate in minikube's
# docker daemon across branches.
docker rmi "schemabot:${IMAGE_TAG}" > /dev/null 2>&1 || true

exit "$TEST_EXIT_CODE"
