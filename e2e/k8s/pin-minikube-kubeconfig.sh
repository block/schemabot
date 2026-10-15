# Sourced by the k8s e2e runner scripts once minikube is up.
#
# Points kubectl and helm at a private kubeconfig holding only the context of
# the active minikube profile, the cluster the runner's minikube commands act
# on. The default kubeconfig is shared with every other kubectl user on
# the machine, so a context switch there mid-run would aim the deploy, the
# port-forwards, and the tests' pod deletions at whatever cluster it switched
# to. The file carries the cluster credentials, so it is removed on exit from
# the moment it exists; the caller's cleanup, which replaces this trap once it
# is armed, removes it too.

# minikube names a profile's kube context after the profile. The last field
# drops the bullet minikube prints before it on a terminal.
MINIKUBE_CONTEXT="$(minikube profile | awk '{print $NF}')"
if [ -z "$MINIKUBE_CONTEXT" ]; then
    echo "Error: could not read the active minikube profile"
    exit 1
fi
export MINIKUBE_CONTEXT

PINNED_KUBECONFIG="$(mktemp)"
trap 'rm -f "$PINNED_KUBECONFIG"' EXIT
if ! kubectl config view --minify --flatten --context "$MINIKUBE_CONTEXT" > "$PINNED_KUBECONFIG"; then
    echo "Error: no kube context for minikube profile $MINIKUBE_CONTEXT; run: minikube update-context -p $MINIKUBE_CONTEXT"
    exit 1
fi
export KUBECONFIG="$PINNED_KUBECONFIG"
if [ "$(kubectl config current-context)" != "$MINIKUBE_CONTEXT" ]; then
    echo "Error: could not pin kubectl to the context of minikube profile $MINIKUBE_CONTEXT"
    exit 1
fi
