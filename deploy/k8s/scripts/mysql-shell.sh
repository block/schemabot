#!/bin/bash
set -euo pipefail

# Connect to SchemaBot's database via a temporary kubectl pod.
# Mounts the k8s secret directly into the pod — credentials never
# appear in kubectl args or local process listings.
#
# Usage: mysql-shell.sh [--context CONTEXT] [--namespace NAMESPACE] [--secret SECRET]
#
# Prerequisites:
#   - kubectl configured with access to the cluster

# Check for required tools
for cmd in kubectl jq; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "❌ ${cmd} not found. Please install it first."
        exit 1
    fi
done

usage() {
    echo "Usage: $0 [--context CONTEXT] [--namespace NAMESPACE] [--secret SECRET]"
    echo ""
    echo "Options:"
    echo "  --context    kubectl context (default: current context)"
    echo "  --namespace  Kubernetes namespace (default: schemabot)"
    echo "  --secret     Name of k8s secret containing a 'dsn' key (default: schemabot-db)"
}

CONTEXT=""
NAMESPACE="schemabot"
SECRET_NAME="schemabot-db"

while [ $# -gt 0 ]; do
    case "$1" in
        --context)   CONTEXT="${2:?--context requires a value}"; shift 2 ;;
        --namespace) NAMESPACE="${2:?--namespace requires a value}"; shift 2 ;;
        --secret)    SECRET_NAME="${2:?--secret requires a value}"; shift 2 ;;
        -h|--help)   usage; exit 0 ;;
        *)           echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

KUBECTL=(kubectl)
if [ -n "$CONTEXT" ]; then
    KUBECTL=(kubectl --context "$CONTEXT")
fi

echo "🔌 MySQL Shell (SchemaBot)"
echo "=========================="
echo ""

# Verify the secret exists and has a dsn key
echo "📦 Verifying k8s secret ${SECRET_NAME}..."
if ! "${KUBECTL[@]}" get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath='{.data.dsn}' > /dev/null 2>&1; then
    echo "❌ Secret ${SECRET_NAME} not found or missing 'dsn' key."
    exit 1
fi
echo "   ✅ Secret found"
echo ""

# Launch temporary mysql pod with the secret volume-mounted.
# A shell wrapper inside the pod parses the DSN and connects.
POD_NAME="mysql-shell-$(date +%s)-${RANDOM}"

echo "🚀 Launching mysql client pod..."
echo "   (Pulling image and connecting — this may take a moment)"
echo "   (Use 'exit' or Ctrl+D to disconnect)"
echo ""

# Build the pod spec JSON. The secret is mounted into the pod and a shell
# wrapper parses the DSN and connects — no credentials leave the cluster.
# Build overrides JSON using jq for safe escaping of the shell entrypoint.
# The entrypoint parses the Go MySQL DSN format: user:pass@tcp(host:port)/db?params
ENTRYPOINT='DSN=$(cat /secrets/db/dsn); USER=${DSN%%:*}; REST=${DSN#*:}; PASS=${REST%%@*}; HOST=$(echo "$DSN" | sed "s/.*tcp(//;s/:.*//"); PORT=$(echo "$DSN" | sed "s/.*tcp([^:]*://;s/).*//"); DB=$(echo "$DSN" | sed "s|.*/||;s|?.*||"); export MYSQL_PWD="$PASS"; exec mysql -h "$HOST" -P "$PORT" -u "$USER" "$DB"'

OVERRIDES=$(jq -n \
    --arg name "$POD_NAME" \
    --arg secret "$SECRET_NAME" \
    --arg cmd "$ENTRYPOINT" \
    '{
        metadata: {annotations: {"sidecar.istio.io/inject": "false"}},
        spec: {
            containers: [{
                name: $name,
                image: "mysql:8.0",
                stdin: true,
                tty: true,
                command: ["sh", "-c", $cmd],
                volumeMounts: [{name: "db-secret", mountPath: "/secrets/db", readOnly: true}]
            }],
            volumes: [{name: "db-secret", secret: {secretName: $secret}}]
        }
    }')

"${KUBECTL[@]}" run "$POD_NAME" --rm -it \
    --image=mysql:8.0 \
    -n "$NAMESPACE" \
    --restart=Never \
    --overrides="$OVERRIDES"
