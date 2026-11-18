package presentation

import "github.com/block/schemabot/pkg/state"

// KeyedOperation is the part of an operation row the sharded layout decisions
// read: its deployment and its operation key.
type KeyedOperation struct {
	Deployment   string
	OperationKey string
}

// IsShardedApply reports whether the apply's operations are the per-shard
// fan-out of one or more keyspaces within one deployment: at least one work
// operation carries a "namespace/shard/table" key, every operation is a shard
// or finalizer operation, and they all share one deployment. A non-sharded
// multi-deployment apply (empty operation keys) and an apply spanning more than
// one deployment return false, so they keep the deployment-unit layout — their
// operations differ by deployment, not shard.
func IsShardedApply(ops []KeyedOperation) bool {
	deployment := ""
	hasShard := false
	for _, op := range ops {
		_, _, _, isShard := state.ShardWorkKey(op.OperationKey)
		if _, isFinalizer := state.NamespaceFinalizerKey(op.OperationKey); !isShard && !isFinalizer {
			return false
		}
		if deployment == "" {
			deployment = op.Deployment
		} else if op.Deployment != deployment {
			return false
		}
		if isShard {
			hasShard = true
		}
	}
	return hasShard
}
