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

// ReadsAsSingleShard reports whether a sharded apply reads as one change on
// one database, so every surface renders it the way it renders an apply with
// one operation, with its progress bars and DDL, instead of a section per
// shard and finalizer. That holds when every keyspace with shard work runs on
// the shard covering its whole keyrange and every finalizer only finalizes a
// keyspace beside its DDL, with no VSchema change to show. The shard is judged
// by its keyrange, not by how many shards the apply touches: operations exist
// only for the shards that change, so one changing shard of a keyspace with
// several is still a sharded change and keeps the shard layout, which names
// it. So do a VSchema change and a keyspace whose only work is its finalize.
//
// keys are every operation key the apply declared, not only those attached so
// far, so an apply whose operations attach over time takes one layout from its
// first render rather than switching as its siblings appear.
// finalizesWithoutVSchemaChange reports what the stored plan says about a
// finalizer's namespace; a caller that could not read the plan answers false
// for every namespace, which keeps the shard layout. It is consulted only once
// the keys alone admit the single-shard layout, so a caller can read the plan
// lazily.
func ReadsAsSingleShard(keys []string, finalizesWithoutVSchemaChange func(namespace string) bool) bool {
	keyspacesWithWork := make(map[string]bool)
	var finalizerKeyspaces []string
	for _, key := range keys {
		if ns, shard, _, ok := state.ShardWorkKey(key); ok {
			if shard != state.FullKeyRangeShard {
				return false
			}
			keyspacesWithWork[ns] = true
			continue
		}
		if ns, ok := state.NamespaceFinalizerKey(key); ok {
			finalizerKeyspaces = append(finalizerKeyspaces, ns)
		}
	}
	if len(keyspacesWithWork) == 0 {
		return false
	}
	for _, ns := range finalizerKeyspaces {
		if !keyspacesWithWork[ns] {
			return false
		}
	}
	for _, ns := range finalizerKeyspaces {
		if !finalizesWithoutVSchemaChange(ns) {
			return false
		}
	}
	return true
}
