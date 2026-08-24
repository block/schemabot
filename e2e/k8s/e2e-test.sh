#!/usr/bin/env bash
# Runs k8s e2e tests on minikube.
#
# Starts minikube if needed, builds and loads the SchemaBot image, deploys
# MySQL + control plane + data plane via Helm, then runs the e2e/k8s test
# suite against the control plane's HTTP API.
#
# Prerequisites: minikube, helm, docker, kubectl, go, and git installed.
#
# Usage:
#   e2e/k8s/e2e-test.sh
#
# Environment:
#   E2E_K8S_NAMESPACE  pin the namespace instead of deriving one per run
#   KEEP_NAMESPACE=1   leave the stack up on exit (to read pod logs, or inspect)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# --- Prerequisites ---

for cmd in minikube helm docker kubectl go git; do
    if ! command -v "$cmd" > /dev/null 2>&1; then
        echo "Error: $cmd is not installed"
        exit 1
    fi
done

# --- Per-run isolation ---

# The namespace and local ports are derived per run so concurrent suites on
# one cluster (e.g. two worktrees) deploy isolated stacks instead of fighting
# over one namespace and one set of localhost forwards. E2E_K8S_NAMESPACE is
# exported to the Go tests, which target the same namespace for pod crashes
# and replacement port-forwards; setting it beforehand pins the namespace for
# a caller that needs to know it in advance.
#
# run_slug names this run. The branch is the recognizable handle when several
# worktrees share a cluster, and a detached HEAD (a CI checkout, a bisect) has
# no branch to read, so the commit stands in for it. The PID makes the handle
# per-run rather than per-ref: two runs at the same ref would otherwise share
# a namespace, Helm releases, and image tag, and the first to tear down would
# destroy the other's stack.
run_slug() {
    local ref
    ref="$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
    if [ -z "$ref" ] || [ "$ref" = "HEAD" ]; then
        ref="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || true)"
    fi
    printf '%s' "$ref" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9-' '-' \
        | sed 's/^-*//' | cut -c1-40 | sed 's/-*$//'
}
RUN_SLUG="$(run_slug)"
RUN_ID="${RUN_SLUG:-local}-$$"
NAMESPACE="${E2E_K8S_NAMESPACE:-schemabot-e2e-${RUN_ID}}"
export E2E_K8S_NAMESPACE="$NAMESPACE"

# free_local_port picks a random port nothing is listening on, from the given
# base/span. The probe uses bash's /dev/tcp in a subshell so it needs no
# external tool and leaves no descriptor behind, and each forward draws from
# its own disjoint range so the three ports can never collide with each other.
# All three ranges sit below the OS ephemeral port ranges (32768+ on Linux,
# 49152+ on macOS): the probe only sees listening sockets, so a port inside
# the ephemeral range could probe free while held as an outbound source port
# and then fail the forward's bind minutes later.
free_local_port() {
    local base="$1" span="$2" port
    while :; do
        port=$(( (RANDOM % span) + base ))
        if ! (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
            echo "$port"
            return
        fi
    done
}
CONTROL_PLANE_PORT="$(free_local_port 20000 4000)"
MYSQL_CONTROL_PLANE_PORT="$(free_local_port 24000 4000)"
MYSQL_DATA_PLANE_PORT="$(free_local_port 28000 4000)"

# --- Minikube ---

if ! minikube status > /dev/null 2>&1; then
    echo "Starting minikube..."
    minikube start --driver=docker --cpus=2 --memory=2048
fi

# --- Build image ---

# The image tag is per-run for the same reason as the namespace: the chart
# pulls with pullPolicy Never, so a concurrent run rebuilding a shared tag
# would swap the binary under this run's pod restarts.
IMAGE_TAG="e2e-${RUN_ID}"

echo "Building image schemabot:${IMAGE_TAG} for minikube..."
CGO_ENABLED=0 GOOS=linux go build -o "$REPO_ROOT/bin/schemabot-linux" ./pkg/cmd
cp "$REPO_ROOT/bin/schemabot-linux" "$REPO_ROOT/deploy/local/schemabot-dev"
eval $(minikube docker-env)
docker build -t "schemabot:${IMAGE_TAG}" -f "$REPO_ROOT/deploy/local/Dockerfile.dev" "$REPO_ROOT/deploy/local/"
rm -f "$REPO_ROOT/deploy/local/schemabot-dev"

# Track background PIDs for cleanup. The trap is armed before the forwards
# exist, so the array can still be empty when it fires — under `set -u` an
# empty "${PIDS[@]}" would abort the cleanup itself. Teardown lives in the
# trap so an exit anywhere past this point — a failed wait, a failed forward,
# an interrupt — removes this run's namespace and image instead of leaving a
# per-run stack behind. KEEP_NAMESPACE=1 leaves the stack and its image in
# place so the caller can read pod logs after a failure, or inspect the
# deployment by hand.
PIDS=()
cleanup() {
    echo "Cleaning up port-forwards..."
    for pid in ${PIDS[@]+"${PIDS[@]}"}; do
        kill "$pid" 2>/dev/null || true
    done
    if [ "${KEEP_NAMESPACE:-0}" = "1" ]; then
        echo "Keeping namespace $NAMESPACE and image schemabot:${IMAGE_TAG}"
    else
        echo "Tearing down..."
        kubectl delete namespace "$NAMESPACE" --ignore-not-found
        # Best-effort: without this, per-run image tags accumulate in
        # minikube's docker daemon across runs.
        docker rmi "schemabot:${IMAGE_TAG}" > /dev/null 2>&1 || true
    fi
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

# wait_for_forward waits until a port-forward accepts connections on its
# local port while its own kubectl process is still running. The ports were
# probed free at derivation time, minutes before the binds — a forward that
# lost its port in between exits immediately, and the port answering without
# it would silently route this run's traffic to whichever process owns the
# port instead.
wait_for_forward() {
    local name="$1" port="$2" pid="$3"
    for i in $(seq 1 30); do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "Port-forward for $name exited before serving :$port; the port was taken between the free probe and the bind"
            exit 1
        fi
        if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
            echo "$name port-forward is serving :$port"
            return
        fi
        sleep 1
    done
    echo "Timeout waiting for the $name port-forward on :$port"
    exit 1
}

echo "Starting port-forwards (namespace $NAMESPACE, control plane :$CONTROL_PLANE_PORT, mysql :$MYSQL_CONTROL_PLANE_PORT/:$MYSQL_DATA_PLANE_PORT)..."
kubectl port-forward -n "$NAMESPACE" svc/control-plane-schemabot "${CONTROL_PLANE_PORT}:8080" &
CONTROL_PLANE_FWD_PID=$!
PIDS+=("$CONTROL_PLANE_FWD_PID")
kubectl port-forward -n "$NAMESPACE" svc/mysql-control-plane "${MYSQL_CONTROL_PLANE_PORT}:3306" &
MYSQL_CONTROL_PLANE_FWD_PID=$!
PIDS+=("$MYSQL_CONTROL_PLANE_FWD_PID")
kubectl port-forward -n "$NAMESPACE" svc/mysql-data-plane "${MYSQL_DATA_PLANE_PORT}:3306" &
MYSQL_DATA_PLANE_FWD_PID=$!
PIDS+=("$MYSQL_DATA_PLANE_FWD_PID")

echo "Waiting for port-forwards..."
wait_for_forward "control-plane" "$CONTROL_PLANE_PORT" "$CONTROL_PLANE_FWD_PID"
wait_for_forward "mysql-control-plane" "$MYSQL_CONTROL_PLANE_PORT" "$MYSQL_CONTROL_PLANE_FWD_PID"
wait_for_forward "mysql-data-plane" "$MYSQL_DATA_PLANE_PORT" "$MYSQL_DATA_PLANE_FWD_PID"
for i in $(seq 1 30); do
    if curl -sf "http://localhost:${CONTROL_PLANE_PORT}/health" > /dev/null 2>&1; then
        echo "Control plane is healthy"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "Timeout waiting for the control plane to answer /health"
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

# Teardown runs in the EXIT trap.
exit "$TEST_EXIT_CODE"
