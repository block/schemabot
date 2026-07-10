package webhook

import (
	"sort"
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
	// Split without a limit so a key with extra segments (e.g.
	// "ns/-40/table/extra") fails the exact-three-parts check rather than folding
	// the remainder into the table and being misclassified as shard work.
	parts := strings.Split(key, "/")
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
// or finalizer operation, they all share one deployment, and every shard work
// operation is in the same namespace. A non-sharded multi-deployment apply
// (empty operation keys), an apply spanning more than one deployment, and a
// multi-keyspace apply all return false, so they keep the existing layout rather
// than mislabelling — the sharded layout shows a single keyspace.
func isShardedApply(ops []*storage.ApplyOperation) bool {
	deployment := ""
	namespace := ""
	hasShard := false
	for _, op := range ops {
		ns, _, _, isShard := parseShardOperationKey(op.OperationKey)
		if !isShard && !isFinalizerOperationKey(op.OperationKey) {
			return false
		}
		if deployment == "" {
			deployment = op.Deployment
		} else if op.Deployment != deployment {
			return false
		}
		if isShard {
			if namespace == "" {
				namespace = ns
			} else if ns != namespace {
				return false
			}
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
func buildShardedApplyData(apply *storage.Apply, ops []*storage.ApplyOperation, released bool, tasks []*storage.Task, tenant string) templates.ShardedApplyData {
	tasksByOp := groupTasksByOperation(tasks)
	// Tasks arrive in created_at DESC order with no id tiebreaker. Sort each
	// operation's tasks by id so the joined DDL (and the change signature derived
	// from it) is deterministic. In practice a (shard, table) operation has a
	// single task — multiple statements for one table are combined into one ALTER
	// upstream — but this keeps the rendering stable regardless.
	for _, ts := range tasksByOp {
		sort.Slice(ts, func(i, j int) bool { return ts[i].ID < ts[j].ID })
	}

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
		RequestedBy: actorFromCaller(apply.Caller),
		Shards:      shardStatuses(shardOrder, opsByShard, released, tasksByOp),
		Cells:       cells,
		Tenant:      tenant,
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
func shardStatuses(shardOrder []string, opsByShard map[string][]*storage.ApplyOperation, released bool, tasksByOp map[int64][]*storage.Task) []templates.ShardStatus {
	inputs := make([]presentation.Operation, 0, len(shardOrder))
	for _, shard := range shardOrder {
		shardOps := opsByShard[shard]
		st, errMsg := aggregateShardState(shardOps, tasksByOp)
		first := shardOps[0]
		inputs = append(inputs, presentation.Operation{
			Deployment:        shard,
			State:             st,
			Barrier:           first.CutoverPolicy == storage.CutoverPolicyBarrier,
			Parallel:          first.CutoverPolicy == storage.CutoverPolicyParallel,
			ContinueOnFailure: first.OnFailure == storage.OnFailureContinue,
			PauseOnFailure:    first.OnFailure == storage.OnFailurePause,
			Released:          released,
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
// operation — the common case — returns that operation's state unchanged. When
// the chosen operation row carries no error message (a remote failure records
// the error on the operation's tasks, and the operator may not have stamped the
// row), it falls back to the first task error so a failed shard always shows why
// — otherwise the comment is silent and the operator has to dig through logs.
func aggregateShardState(ops []*storage.ApplyOperation, tasksByOp map[int64][]*storage.Task) (string, string) {
	best := ops[0]
	for _, op := range ops[1:] {
		if shardStateRank(op.State) > shardStateRank(best.State) {
			best = op
		}
	}
	errMsg := best.ErrorMessage
	if errMsg == "" {
		errMsg = firstTaskError(tasksByOp[best.ID])
	}
	return best.State, errMsg
}

// firstTaskError returns the first non-empty task error for an operation.
func firstTaskError(tasks []*storage.Task) string {
	for _, t := range tasks {
		if t.ErrorMessage != "" {
			return t.ErrorMessage
		}
	}
	return ""
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
