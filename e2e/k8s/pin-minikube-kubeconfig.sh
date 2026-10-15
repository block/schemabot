# Sourced by the k8s e2e runner scripts once minikube is up.
#
# Points kubectl and helm at a private kubeconfig holding only the minikube
# context. The default kubeconfig is shared with every other kubectl user on
# the machine, so a context switch there mid-run would aim the deploy, the
# port-forwards, and the tests' pod deletions at whatever cluster it switched
# to. The file carries the cluster credentials, so it is removed on exit from
# the moment it exists; the caller's cleanup, which replaces this trap once it
# is armed, removes it too.

PINNED_KUBECONFIG="$(mktemp)"
trap 'rm -f "$PINNED_KUBECONFIG"' EXIT
kubectl config view --minify --flatten --context minikube > "$PINNED_KUBECONFIG"
export KUBECONFIG="$PINNED_KUBECONFIG"
if [ "$(kubectl config current-context)" != "minikube" ]; then
    echo "Error: could not pin kubectl to the minikube context"
    exit 1
fi
