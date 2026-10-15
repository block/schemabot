# Sourced by the k8s e2e runner scripts once minikube is up.
#
# Points kubectl and helm at a private kubeconfig holding only the minikube
# context. The default kubeconfig is shared with every other kubectl user on
# the machine, so a context switch there mid-run would aim the deploy, the
# port-forwards, and the tests' pod deletions at whatever cluster it switched
# to. The caller's cleanup removes $PINNED_KUBECONFIG.

PINNED_KUBECONFIG="$(mktemp)"
kubectl config view --minify --flatten --context minikube > "$PINNED_KUBECONFIG"
export KUBECONFIG="$PINNED_KUBECONFIG"
if [ "$(kubectl config current-context)" != "minikube" ]; then
    echo "Error: could not pin kubectl to the minikube context"
    exit 1
fi
