package webhook

import (
	"strings"
	"time"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

const finalizerKeySegment = "group_finalizer"

// parseShardOperationKey splits a sharded work operation key
// "namespace/shard/table" into its parts. ok is false for any other shape — an
// empty key (a non-sharded apply) or a "namespace/group_finalizer" finalizer
// key — so callers can tell shard work apart from the rest.
func parseShardOperationKey(key string) (namespace, shard, table string, ok bool) {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// isFinalizerOperationKey reports whether the key is a "namespace/group_finalizer"
// finalizer operation key.
func isFinalizerOperationKey(key string) bool {
	ns, ok := strings.CutSuffix(key, "/"+finalizerKeySegment)
	return ok && ns != "" && !strings.Contains(ns, "/")
}

// isShardedApply reports whether the apply's operations are the per-shard
// fan-out of a single keyspace within one deployment: at least one work
// operation carries a "namespace/shard/table" key, every operation is a shard
// or finalizer operation, and they all share one deployment. A non-sharded
// multi-deployment apply (empty operation keys) and any apply spanning more than
// one deployment return false, so they keep the existing layout.
func isShardedApply(ops []*storage.ApplyOperation) bool {
	deployment := ""
	hasShard := false
	for _, op := range ops {
		_, _, _, isShard := parseShardOperationKey(op.OperationKey)
		if !isShard && !isFinalizerOperationKey(op.OperationKey) {
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

// buildShardedApplyData projects the per-shard operation rows into the
// sharded-apply comment input. Each shard work operation is one (shard, table)
// cell carrying its DDL; per-shard status is derived through pkg/presentation
// with the shard name as the operation identity, so the ordering labels
// ("waiting for `-40`", "halted — `-40` failed") reference shards. Finalizer
// (VSchema) operations are not shard work and are omitted from the shard view;
// their outcome is still reflected in the aggregate headline state.
func buildShardedApplyData(apply *storage.Apply, ops []*storage.ApplyOperation, tasks []*storage.Task) templates.ShardedApplyData {
	tasksByOp := groupTasksByOperation(tasks)

	keyspace := ""
	cells := make([]templates.ShardCell, 0, len(ops))
	// Group work operations by shard in resolved order so a shard with more than
	// one table change (a divergent shard) collapses to one status row.
	var shardOrder []string
	opsByShard := make(map[string][]*storage.ApplyOperation)
	for _, op := range ops {
		ns, shard, table, ok := parseShardOperationKey(op.OperationKey)
		if !ok {
			continue
		}
		if keyspace == "" {
			keyspace = ns
		}
		// An operation can carry more than one task for its (namespace, shard,
		// table) — a shard plan may yield multiple statements for the same table —
		// so join every non-empty task DDL in task order. Taking only the first
		// would drop statements and corrupt the change signature used to group
		// shards.
		var ddls []string
		for _, t := range tasksByOp[op.ID] {
			if strings.TrimSpace(t.DDL) != "" {
				ddls = append(ddls, t.DDL)
			}
		}
		cells = append(cells, templates.ShardCell{Shard: shard, Table: table, DDL: strings.Join(ddls, "\n")})
		if _, seen := opsByShard[shard]; !seen {
			shardOrder = append(shardOrder, shard)
		}
		opsByShard[shard] = append(opsByShard[shard], op)
	}

	data := templates.ShardedApplyData{
		State:       apply.State,
		Environment: apply.Environment,
		Database:    apply.Database,
		Keyspace:    keyspace,
		ApplyID:     apply.ApplyIdentifier,
		RequestedBy: apply.Caller,
		Shards:      shardStatuses(shardOrder, opsByShard),
		Cells:       cells,
	}
	if apply.StartedAt != nil {
		data.StartedAt = apply.StartedAt.Format(time.RFC3339)
	}
	if apply.CompletedAt != nil {
		data.CompletedAt = apply.CompletedAt.Format(time.RFC3339)
	}
	return data
}

// shardStatuses derives one status per shard. Each shard's operations are
// aggregated to a single representative state, then the shards are projected
// together through pkg/presentation (shard name as identity) so ordering labels
// reference sibling shards.
func shardStatuses(shardOrder []string, opsByShard map[string][]*storage.ApplyOperation) []templates.ShardStatus {
	inputs := make([]presentation.Operation, 0, len(shardOrder))
	for _, shard := range shardOrder {
		shardOps := opsByShard[shard]
		st, errMsg := aggregateShardState(shardOps)
		first := shardOps[0]
		inputs = append(inputs, presentation.Operation{
			Deployment:        shard,
			State:             st,
			Barrier:           first.CutoverPolicy == storage.CutoverPolicyBarrier,
			Parallel:          first.CutoverPolicy == storage.CutoverPolicyParallel,
			HaltOnFailure:     first.OnFailure != storage.OnFailureContinue,
			ContinueOnFailure: first.OnFailure == storage.OnFailureContinue,
			Error:             errMsg,
		})
	}
	derived := presentation.Derive(inputs).Deployments
	out := make([]templates.ShardStatus, 0, len(derived))
	for _, d := range derived {
		out = append(out, templates.ShardStatus{
			Shard: d.Deployment,
			Emoji: d.Emoji,
			Label: d.Label,
			State: d.State,
			Error: d.Error,
		})
	}
	return out
}

// aggregateShardState reduces a shard's operations to its most significant
// state (and that operation's error), so a shard whose tables are in different
// states shows the state an operator should act on first. A shard with a single
// operation — the common case — returns that operation's state unchanged.
func aggregateShardState(ops []*storage.ApplyOperation) (string, string) {
	best := ops[0]
	for _, op := range ops[1:] {
		if shardStateRank(op.State) > shardStateRank(best.State) {
			best = op
		}
	}
	return best.State, best.ErrorMessage
}

// shardStateRank orders operation states by how much they demand attention, so
// aggregateShardState surfaces the most actionable one. Failure ranks highest;
// completed lowest.
func shardStateRank(s string) int {
	switch s {
	case state.ApplyOperation.Failed:
		return 12
	case state.ApplyOperation.FailedRetryable:
		return 11
	case state.ApplyOperation.Running:
		return 10
	case state.ApplyOperation.CuttingOver:
		return 9
	case state.ApplyOperation.WaitingForCutover:
		return 8
	case state.ApplyOperation.Recovering:
		return 7
	case state.ApplyOperation.Resuming:
		return 6
	case state.ApplyOperation.Stopped:
		return 5
	case state.ApplyOperation.RevertWindow:
		return 4
	case state.ApplyOperation.Pending:
		return 3
	case state.ApplyOperation.Cancelled:
		return 2
	case state.ApplyOperation.Reverted:
		return 1
	case state.ApplyOperation.Completed:
		return 0
	default:
		return 3
	}
}
