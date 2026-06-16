#!/usr/bin/env bash
# Runs the etre-resolver k8s e2e on minikube.
#
# Like e2e-test.sh, but the data plane resolves an opaque target (a DSID)
# through Etre and assembles credentials by assuming an IAM role + reading
# Secrets Manager — both emulated by ministack, deployed in-cluster. Proves the
# dynamic resolution path end to end against a real Etre server and the AWS
# credential flow rather than mocks.
#
# Prerequisites: minikube, helm, docker, kubectl, go.
#
# Usage:
#   e2e/k8s/e2e-etre-test.sh

set -euo pipefail

NAMESPACE="schemabot-e2e-etre"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

for cmd in minikube helm docker kubectl go; do
    if ! command -v "$cmd" > /dev/null 2>&1; then
        echo "Error: $cmd is not installed"
        exit 1
    fi
done

if ! minikube status > /dev/null 2>&1; then
    echo "Starting minikube..."
    minikube start --driver=docker --cpus=4 --memory=4096
fi

# --- Build images into minikube's docker daemon ---

echo "Building schemabot + etre images for minikube..."
CGO_ENABLED=0 GOOS=linux go build -o "$REPO_ROOT/bin/schemabot-linux" ./pkg/cmd
cp "$REPO_ROOT/bin/schemabot-linux" "$REPO_ROOT/deploy/local/schemabot-dev"
eval "$(minikube docker-env)"
docker build -t schemabot:test -f "$REPO_ROOT/deploy/local/Dockerfile.dev" "$REPO_ROOT/deploy/local/"
rm -f "$REPO_ROOT/deploy/local/schemabot-dev"
docker build -t etre:test "$REPO_ROOT/e2e/k8s/etre/"

PIDS=()
cleanup() {
    echo "Cleaning up port-forwards..."
    for pid in ${PIDS[@]+"${PIDS[@]}"}; do
        kill "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

# Namespace teardown is left to the caller (KEEP_NAMESPACE=1 to inspect after a
# failure); a re-run reconciles the existing namespace via apply + upgrade.
teardown_namespace() {
    if [ "${KEEP_NAMESPACE:-0}" != "1" ]; then
        kubectl delete namespace "$NAMESPACE" --ignore-not-found
    fi
}

wait_for_ready_pods() {
    local selector="$1"
    local timeout_seconds="${2:-180}"
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
    ((remaining < 1)) && remaining=1
    kubectl wait --for=condition=ready pod -l "$selector" -n "$NAMESPACE" --timeout="${remaining}s"
}

# --- Deploy ---

echo "Creating namespace..."
kubectl create namespace "$NAMESPACE" 2>/dev/null || true

echo "Deploying MySQL + Etre + MongoDB + ministack..."
kubectl apply -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/mysql.yaml"
# Build the entity fixtures ConfigMap from the JSON documents so the seed Job
# loads exactly what is checked in under e2e/k8s/etre/fixtures.
kubectl create configmap etre-fixtures \
    --from-file="$REPO_ROOT/e2e/k8s/etre/fixtures" \
    -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
# Jobs are immutable, so a KEEP_NAMESPACE rerun would not re-run the completed
# seed Jobs. Delete them first so kubectl apply recreates them and reruns seed
# deterministically.
kubectl delete job etre-mongo-init etre-seed ministack-seed -n "$NAMESPACE" --ignore-not-found
kubectl apply -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/etre-stack.yaml"
wait_for_ready_pods "app=mysql-control-plane"
wait_for_ready_pods "app=mysql-data-plane"
wait_for_ready_pods "app=etre"
wait_for_ready_pods "app=ministack"

echo "Waiting for seed jobs..."
kubectl wait --for=condition=complete job/etre-mongo-init -n "$NAMESPACE" --timeout=180s
kubectl wait --for=condition=complete job/etre-seed -n "$NAMESPACE" --timeout=180s
kubectl wait --for=condition=complete job/ministack-seed -n "$NAMESPACE" --timeout=180s

echo "Installing etre data plane..."
helm upgrade --install data-plane-etre "$REPO_ROOT/charts/schemabot" \
    -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/data-plane-etre-values.yaml"
wait_for_ready_pods "app.kubernetes.io/instance=data-plane-etre"

echo "Installing control plane..."
helm upgrade --install control-plane-etre "$REPO_ROOT/charts/schemabot" \
    -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/control-plane-etre-values.yaml"
wait_for_ready_pods "app.kubernetes.io/instance=control-plane-etre"

# --- Port-forwards ---

echo "Starting port-forwards..."
kubectl port-forward -n "$NAMESPACE" svc/control-plane-etre-schemabot 8080:8080 &
PIDS+=($!)
kubectl port-forward -n "$NAMESPACE" svc/mysql-control-plane 3307:3306 &
PIDS+=($!)
kubectl port-forward -n "$NAMESPACE" svc/mysql-data-plane 3308:3306 &
PIDS+=($!)

echo "Waiting for control plane..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
        echo "Control plane is healthy"
        break
    fi
    [ "$i" -eq 30 ] && { echo "Timeout waiting for port-forward"; exit 1; }
    sleep 1
done

# --- Test ---

echo "Running etre-resolver k8s e2e..."
TEST_EXIT_CODE=0
E2E_SCHEMABOT_URL=http://localhost:8080 \
E2E_SCHEMABOT_MYSQL_DSN="root:testpassword@tcp(localhost:3307)/schemabot?parseTime=true&multiStatements=true" \
E2E_TERN_STAGING_MYSQL_DSN="root:testpassword@tcp(localhost:3308)/testapp?parseTime=true&multiStatements=true" \
go test -count=1 -v -tags=e2e -timeout=10m -run TestK8sEtre ./e2e/k8s/... || TEST_EXIT_CODE=$?

teardown_namespace
exit "$TEST_EXIT_CODE"
