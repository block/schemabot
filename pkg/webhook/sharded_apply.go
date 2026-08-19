package webhook

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
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

// parseFinalizerOperationKey splits a "namespace/group_finalizer" finalizer
// operation key into its namespace. ok is false for any other shape, including
// the bare "group_finalizer" key a vschema-only plan produces — that shape has
// no shard work alongside it, so it never reaches the sharded layout.
func parseFinalizerOperationKey(key string) (namespace string, ok bool) {
	ns, ok := strings.CutSuffix(key, "/"+finalizerKeySegment)
	if !ok || ns == "" || strings.Contains(ns, "/") {
		return "", false
	}
	return ns, true
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
		if _, isFinalizer := parseFinalizerOperationKey(op.OperationKey); !isShard && !isFinalizer {
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
// (VSchema) operations are not shard work: each one becomes a VSchema change —
// its keyspace from the operation key, its display status from the operation
// state, and its diff from the stored plan's per-namespace diffs
// (vschemaDiffs, see resolveShardedVSchemaDiffs) — rendered in the comment's
// VSchema section. A failed
// finalizer's error also stands in for the apply-level failure cause when the
// apply row carries none, since a finalizer failure is operation-scoped and
// leaves no failed shard row to name it.
func buildShardedApplyData(apply *storage.Apply, ops []*storage.ApplyOperation, released bool, tasks []*storage.Task, vschemaDiffs map[string]string, tenant string) templates.ShardedApplyData {
	tasksByOp := groupTasksByOperation(tasks)
	// Sort each operation's tasks by id so the joined DDL (and the change
	// signature derived from it) is deterministic without depending on the
	// loader's ordering. In practice a (shard, table) operation has a single
	// task — multiple statements for one table are combined into one ALTER
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
	var vschemaChanges []apitypes.VSchemaChange
	finalizerError := ""
	for _, op := range ops {
		ns, shard, table, ok := parseShardOperationKey(op.OperationKey)
		if !ok {
			// isShardedApply admits only shard work and finalizer keys, so a
			// non-shard key here is a finalizer: one keyspace's VSchema change.
			finalizerNS, isFinalizer := parseFinalizerOperationKey(op.OperationKey)
			if !isFinalizer {
				continue
			}
			vschemaChanges = append(vschemaChanges, apitypes.VSchemaChange{
				Namespace: finalizerNS,
				Status:    vschemaStatusForOperationState(op.State),
				Diff:      vschemaDiffs[finalizerNS],
			})
			if finalizerError == "" && isFinalizerFailureState(op.State) && op.ErrorMessage != "" {
				finalizerError = op.ErrorMessage
			}
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

	// A finalizer failure is operation-scoped and does not write the parent
	// apply's error, so when the apply row carries no cause of its own the
	// failed finalizer's error is the one to surface.
	errorMessage := apply.ErrorMessage
	if errorMessage == "" {
		errorMessage = finalizerError
	}

	data := templates.ShardedApplyData{
		State:          apply.State,
		Environment:    apply.Environment,
		Database:       apply.Database,
		Keyspace:       keyspace,
		ApplyID:        apply.ApplyIdentifier,
		RequestedBy:    actorFromCaller(apply.Caller),
		ErrorMessage:   errorMessage,
		Shards:         shardStatuses(shardOrder, opsByShard, released, tasksByOp),
		Cells:          cells,
		VSchemaChanges: vschemaChanges,
		Tenant:         tenant,
		Rollback:       apply.IsRollback(),
	}
	if apply.StartedAt != nil {
		data.StartedAt = apply.StartedAt.Format(time.RFC3339)
	}
	if apply.CompletedAt != nil {
		data.CompletedAt = apply.CompletedAt.Format(time.RFC3339)
	}
	return data
}

// resolveShardedVSchemaDiffs loads the stored plan's per-namespace rendered
// VSchema diffs for a sharded apply's comment: the diff the engine annotated
// at plan time and plan persistence kept (PlanMetadataVSchemaDiff), so the
// comment shows the change the operator approved rather than a re-diff
// against live state. Returns nil when the apply carries no finalizer
// operation — only finalizer rows render VSchema entries, so there is nothing
// to attach a diff to. Best-effort: a plan load failure, a missing plan row,
// or a stored plan without diffs (recorded before diffs were persisted)
// contributes nothing rather than blocking the comment.
func resolveShardedVSchemaDiffs(ctx context.Context, stor storage.Storage, apply *storage.Apply, ops []*storage.ApplyOperation) map[string]string {
	hasFinalizer := false
	for _, op := range ops {
		if _, ok := parseFinalizerOperationKey(op.OperationKey); ok {
			hasFinalizer = true
			break
		}
	}
	if !hasFinalizer {
		return nil
	}

	plan, err := stor.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		slog.Warn("comment will omit VSchema diffs: failed to load stored plan",
			append(apply.LogAttrs(), "error", err)...)
		return nil
	}
	if plan == nil {
		slog.Warn("comment will omit VSchema diffs: stored plan row not found",
			apply.LogAttrs()...)
		return nil
	}

	var diffs map[string]string
	for namespace, nsData := range plan.Namespaces {
		if nsData == nil {
			continue
		}
		if d := nsData.Metadata[storage.PlanMetadataVSchemaDiff]; d != "" {
			if diffs == nil {
				diffs = make(map[string]string)
			}
			diffs[namespace] = d
		}
	}
	return diffs
}

// vschemaStatusForOperationState projects a finalizer operation's state onto
// the VSchema display status vocabulary the single-deployment comment uses, so
// both comment shapes describe VSchema application identically: applied when
// the finalizer completed, applying while it runs, failed on a failure
// (terminal or auto-retrying), and pending (empty) before it starts.
func vschemaStatusForOperationState(opState string) string {
	switch {
	case state.IsState(opState, state.ApplyOperation.Completed):
		return "applied"
	case state.IsState(opState, state.ApplyOperation.Running):
		return "applying"
	case isFinalizerFailureState(opState):
		return "failed"
	default:
		return ""
	}
}

// isFinalizerFailureState reports whether a finalizer operation's state carries
// an operator-facing error — a terminal failure or an automatic retry after
// one, mirroring the shard-failure vocabulary.
func isFinalizerFailureState(opState string) bool {
	return state.IsState(opState, state.ApplyOperation.Failed, state.ApplyOperation.FailedRetryable)
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
