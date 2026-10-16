#!/usr/bin/env bash
# Runs the multi-target k8s e2e on minikube.
#
# One data plane deployment serves several targets, each a DSID resolved
# through Etre to its own MySQL server, with credentials read from Secrets
# Manager (ministack). The control plane addresses them with a targets list, so
# every apply rolls out table by table across the shards over gRPC: the shape a
# database split across several clusters runs in production.
#
# Prerequisites: minikube, helm, docker, kubectl, go.
#
# Usage:
#   e2e/k8s/e2e-multi-target-test.sh
#
# Environment:
#   E2E_TEST_TIMEOUT   go test's own timeout (default 15m); CI sets it on the
#                      job whose budget must exceed it plus setup, so the two
#                      numbers are read and changed together
#   E2E_TEST_RUN       go test -run pattern (default TestK8sMultiTarget), so CI
#                      can split the suite across jobs that each stand up the
#                      stack
#   KEEP_NAMESPACE     1 leaves the namespace up after the run for inspection;
#                      a re-run reuses it and rolls the SchemaBot pods onto the
#                      freshly built binary
#   E2E_LOG_DIR        where pod logs are written when the tests fail (default
#                      e2e-multi-target-logs under $TMPDIR)

set -euo pipefail

NAMESPACE="schemabot-e2e-multi-target"
SHARDS=(001 002 003)
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

for cmd in minikube helm docker kubectl go lsof; do
    if ! command -v "$cmd" > /dev/null 2>&1; then
        echo "Error: $cmd is not installed"
        exit 1
    fi
done

if ! minikube status > /dev/null 2>&1; then
    echo "Starting minikube..."
    minikube start --driver=docker --container-runtime=docker --cpus=4 --memory=6144
fi

# shellcheck source=e2e/k8s/pin-minikube-kubeconfig.sh
source "$REPO_ROOT/e2e/k8s/pin-minikube-kubeconfig.sh"

# --- Build images into minikube's docker daemon ---

# The suite forwards these local ports; one already taken would send the tests
# to whatever else is listening there.
LOCAL_PORTS=(8080 3307 3308)
for i in "${!SHARDS[@]}"; do
    LOCAL_PORTS+=($((3311 + i)))
done
for port in "${LOCAL_PORTS[@]}"; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN > /dev/null 2>&1; then
        echo "Error: local port $port is already in use; stop whatever is listening on it (lsof -nP -iTCP:$port -sTCP:LISTEN) and re-run"
        exit 1
    fi
done

echo "Building schemabot + etre images for minikube..."
CGO_ENABLED=0 GOOS=linux go build -o "$REPO_ROOT/bin/schemabot-linux" ./pkg/cmd
cp "$REPO_ROOT/bin/schemabot-linux" "$REPO_ROOT/deploy/local/schemabot-dev"
# The tag names the binary, so a re-run into a kept namespace rolls the pods
# whenever the code changed instead of leaving them on the previous build.
IMAGE_TAG="test-$(shasum -a 256 "$REPO_ROOT/bin/schemabot-linux" | cut -c1-12)"
eval "$(minikube docker-env)"
docker build -t "schemabot:$IMAGE_TAG" -f "$REPO_ROOT/deploy/local/Dockerfile.dev" "$REPO_ROOT/deploy/local/"
rm -f "$REPO_ROOT/deploy/local/schemabot-dev"
docker build -t etre:test "$REPO_ROOT/e2e/k8s/etre/"

PIDS=()
cleanup() {
    rm -f "$PINNED_KUBECONFIG"
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

echo "Deploying MySQL + shards + Etre + MongoDB + ministack..."
kubectl apply -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/mysql.yaml"
kubectl apply -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/multi-target/mysql-shards.yaml"
# Build the entity fixtures ConfigMap from this suite's shard entities so the
# seed Jobs load exactly what is checked in under e2e/k8s/multi-target/fixtures.
kubectl create configmap etre-fixtures \
    --from-file="$REPO_ROOT/e2e/k8s/multi-target/fixtures" \
    -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
# Jobs are immutable, so a KEEP_NAMESPACE rerun would not re-run the completed
# seed Jobs. Delete them first so kubectl apply recreates them and reruns seed
# deterministically.
kubectl delete job etre-mongo-init etre-seed ministack-seed -n "$NAMESPACE" --ignore-not-found
kubectl apply -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/etre-stack.yaml"
wait_for_ready_pods "app=mysql-control-plane"
wait_for_ready_pods "app=mysql-data-plane"
for shard in "${SHARDS[@]}"; do
    wait_for_ready_pods "app=mysql-shard-$shard"
done
wait_for_ready_pods "app=etre"
wait_for_ready_pods "app=ministack"

echo "Waiting for seed jobs..."
kubectl wait --for=condition=complete job/etre-mongo-init -n "$NAMESPACE" --timeout=180s
kubectl wait --for=condition=complete job/etre-seed -n "$NAMESPACE" --timeout=180s
kubectl wait --for=condition=complete job/ministack-seed -n "$NAMESPACE" --timeout=180s

echo "Installing multi-target data plane..."
helm upgrade --install data-plane-mt "$REPO_ROOT/charts/schemabot" \
    -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/multi-target/data-plane-values.yaml" \
    --set image.tag="$IMAGE_TAG" --wait --timeout 3m
# A rerun rolls the pods onto the new image tag, so a label-selector wait would
# also select the old pods while they terminate, and those never turn Ready.
# rollout status waits on the deployment's current ReplicaSet only.
kubectl rollout status deployment/data-plane-mt-schemabot -n "$NAMESPACE" --timeout=180s

echo "Installing control plane..."
helm upgrade --install control-plane-mt "$REPO_ROOT/charts/schemabot" \
    -n "$NAMESPACE" -f "$REPO_ROOT/e2e/k8s/multi-target/control-plane-values.yaml" \
    --set image.tag="$IMAGE_TAG" --wait --timeout 3m
kubectl rollout status deployment/control-plane-mt-schemabot -n "$NAMESPACE" --timeout=180s

# --- Port-forwards ---

echo "Starting port-forwards..."
kubectl port-forward -n "$NAMESPACE" svc/control-plane-mt-schemabot 8080:8080 &
PIDS+=($!)
kubectl port-forward -n "$NAMESPACE" svc/mysql-control-plane 3307:3306 &
PIDS+=($!)
kubectl port-forward -n "$NAMESPACE" svc/mysql-data-plane 3308:3306 &
PIDS+=($!)
# Shard N listens on 331N locally; the suite reads them, in target order, from
# E2E_MULTI_TARGET_SHARD_DSNS.
SHARD_DSNS=""
for i in "${!SHARDS[@]}"; do
    port=$((3311 + i))
    kubectl port-forward -n "$NAMESPACE" "svc/mysql-shard-${SHARDS[$i]}" "$port:3306" &
    PIDS+=($!)
    SHARD_DSNS+="${SHARD_DSNS:+,}root:testpassword@tcp(localhost:$port)/?parseTime=true&multiStatements=true"
done

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

echo "Running multi-target k8s e2e..."
TEST_EXIT_CODE=0
E2E_SCHEMABOT_URL=http://localhost:8080 \
E2E_SCHEMABOT_MYSQL_DSN="root:testpassword@tcp(localhost:3307)/schemabot?parseTime=true&multiStatements=true" \
E2E_TERN_STAGING_MYSQL_DSN="root:testpassword@tcp(localhost:3308)/testapp?parseTime=true&multiStatements=true" \
E2E_MULTI_TARGET_SHARD_DSNS="$SHARD_DSNS" \
E2E_K8S_NAMESPACE="$NAMESPACE" \
go test -count=1 -v -tags=e2e -timeout="${E2E_TEST_TIMEOUT:-15m}" -run "${E2E_TEST_RUN:-TestK8sMultiTarget}" ./e2e/k8s/... || TEST_EXIT_CODE=$?

if [ "$TEST_EXIT_CODE" -ne 0 ]; then
    LOG_DIR="${E2E_LOG_DIR:-${TMPDIR:-/tmp}/e2e-multi-target-logs}"
    mkdir -p "$LOG_DIR"
    for instance in control-plane-mt data-plane-mt; do
        for pod in $(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$instance" -o name); do
            kubectl logs -n "$NAMESPACE" "$pod" > "$LOG_DIR/${pod#pod/}.log" 2>&1 || true
        done
    done
    echo "Tests failed; pod logs are in $LOG_DIR"
fi

teardown_namespace
exit "$TEST_EXIT_CODE"
