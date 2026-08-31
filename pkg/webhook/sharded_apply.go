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
// fan-out of one or more keyspaces within one deployment: at least one work
// operation carries a "namespace/shard/table" key, every operation is a shard
// or finalizer operation, and they all share one deployment. A non-sharded
// multi-deployment apply (empty operation keys) and an apply spanning more than
// one deployment return false, so they keep the deployment-unit layout — their
// operations differ by deployment, not shard.
func isShardedApply(ops []*storage.ApplyOperation) bool {
	deployment := ""
	hasShard := false
	for _, op := range ops {
		_, _, _, isShard := parseShardOperationKey(op.OperationKey)
		if _, isFinalizer := parseFinalizerOperationKey(op.OperationKey); !isShard && !isFinalizer {
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

// shardWorkGroup is one shard's work within a keyspace: the (namespace, shard)
// pair and its operations in resolved order, the unit a status row is derived
// for.
type shardWorkGroup struct {
	namespace string
	shard     string
	ops       []*storage.ApplyOperation
}

// buildShardedApplyData projects the per-shard operation rows into the
// sharded-apply comment input, grouped per keyspace in resolved order. Each
// shard work operation is one (shard, table) cell carrying its DDL; per-shard
// status is derived through pkg/presentation with the shard name as the
// operation identity, so the ordering labels ("waiting for `-40`", "halted —
// `-40` failed") reference shards. Finalizer (VSchema) operations are not
// shard work: each one becomes a VSchema change — its keyspace from the
// operation key, its display status from the operation state, and its diff
// from the stored plan's per-namespace diffs (vschemaDiffs, see
// resolveShardedVSchemaDiffs) — rendered in the comment's VSchema section. A
// failed finalizer's error also stands in for the apply-level failure cause
// when the apply row carries none, since a finalizer failure is
// operation-scoped and leaves no failed shard row to name it.
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

	// Group work operations by (keyspace, shard) in resolved order so a shard
	// with more than one table change (a divergent shard) collapses to one
	// status row, and keyspaces render in the order their shards first appear.
	var keyspaceOrder []string
	cellsByKeyspace := make(map[string][]templates.ShardCell)
	var groupOrder []shardWorkGroup
	type keyspaceShard struct{ namespace, shard string }
	groupIndex := make(map[keyspaceShard]int)
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
				Status:    vschemaStatusForOperationState(apply.State, op.State),
				Diff:      vschemaDiffs[finalizerNS],
			})
			if finalizerError == "" && isFinalizerFailureState(op.State) && op.ErrorMessage != "" {
				finalizerError = op.ErrorMessage
			}
			continue
		}
		if _, seen := cellsByKeyspace[ns]; !seen {
			keyspaceOrder = append(keyspaceOrder, ns)
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
		cellsByKeyspace[ns] = append(cellsByKeyspace[ns], templates.ShardCell{Shard: shard, Table: table, DDL: strings.Join(ddls, "\n")})
		groupKey := keyspaceShard{namespace: ns, shard: shard}
		i, seen := groupIndex[groupKey]
		if !seen {
			i = len(groupOrder)
			groupIndex[groupKey] = i
			groupOrder = append(groupOrder, shardWorkGroup{namespace: ns, shard: shard})
		}
		groupOrder[i].ops = append(groupOrder[i].ops, op)
	}

	// A finalizer failure is operation-scoped and does not write the parent
	// apply's error, so when the apply row carries no cause of its own the
	// failed finalizer's error is the one to surface.
	errorMessage := apply.ErrorMessage
	if errorMessage == "" {
		errorMessage = finalizerError
	}

	shardsByKeyspace := shardStatusesByKeyspace(groupOrder, len(keyspaceOrder) > 1, released, tasksByOp)
	tablesByKeyspace := shardedTableStatusesByKeyspace(ops, tasksByOp)
	keyspaces := make([]templates.ShardedKeyspace, 0, len(keyspaceOrder))
	for _, ns := range keyspaceOrder {
		keyspaces = append(keyspaces, templates.ShardedKeyspace{
			Keyspace: ns,
			Tables:   tablesByKeyspace[ns],
			Shards:   shardsByKeyspace[ns],
			Cells:    cellsByKeyspace[ns],
		})
	}

	data := templates.ShardedApplyData{
		State:          apply.State,
		Environment:    apply.Environment,
		Database:       apply.Database,
		ApplyID:        apply.ApplyIdentifier,
		RequestedBy:    actorFromCaller(apply.Caller),
		ErrorMessage:   errorMessage,
		Keyspaces:      keyspaces,
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
// against live state. Returns nil without touching storage unless the apply
// renders the sharded layout and carries a finalizer operation — only the
// sharded layout consumes these diffs, and only finalizer rows render VSchema
// entries, so any other shape would pay a stored-plan read on every comment
// edit just to discard the result. Best-effort: a plan load failure, a
// missing plan row, or a stored plan without diffs (recorded before diffs
// were persisted) contributes nothing rather than blocking the comment.
func resolveShardedVSchemaDiffs(ctx context.Context, stor storage.Storage, apply *storage.Apply, ops []*storage.ApplyOperation) map[string]string {
	if !isShardedApply(ops) {
		return nil
	}
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
// (terminal or auto-retrying), and pending (empty) before it starts. A
// finalizer whose rollout ended without running it reads as cancelled rather
// than pending, so the terminal summary never promises VSchema work that no
// claim arm will run. That covers both routes to a dead row: the operation
// itself holds cancelled or reverted (written by the cancel path or mirrored
// from the settled parent by the stranded-operation reaper), and the row
// still pending under a parent whose verdict is already final — a halted
// rollout terminalizes the apply immediately, while the reaper only settles
// the stranded row minutes later, well after the summary posted. Stopped —
// on the operation or the parent — stays its own status: a stopped apply is
// resumable, so its finalizer may yet run, but "pending" would overpromise.
func vschemaStatusForOperationState(applyState, opState string) string {
	switch {
	case state.IsState(opState, state.ApplyOperation.Completed):
		return "applied"
	case state.IsState(opState, state.ApplyOperation.Running):
		return "applying"
	case isFinalizerFailureState(opState):
		return "failed"
	case state.IsState(opState, state.ApplyOperation.Cancelled, state.ApplyOperation.Reverted):
		return "cancelled"
	case state.IsState(opState, state.ApplyOperation.Stopped) || state.IsState(applyState, state.Apply.Stopped):
		return "stopped"
	case state.IsTerminalApplyState(applyState):
		return "cancelled"
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

// shardStatusesByKeyspace derives one status per (keyspace, shard) group and
// buckets the results per keyspace, preserving resolved order. Each shard's
// operations are aggregated to a single representative state, then every
// shard — across all keyspaces — is projected through pkg/presentation in one
// pass so ordering labels reference sibling shards regardless of keyspace.
// The presentation identity is the shard name; when the apply spans more than
// one keyspace it is keyspace-qualified ("keyspace/shard"), because shard
// names repeat across keyspaces (every unsharded keyspace's shard is "-") and
// an ordering label naming a bare duplicate shard would be ambiguous. The
// status row's own Shard stays the plain name either way — it renders under
// its keyspace heading. Either way the identity is unique across groups —
// bare names are unique within a single keyspace, qualified names are unique
// by construction — so results key back to their groups by identity rather
// than by output position.
func shardStatusesByKeyspace(groups []shardWorkGroup, qualifyIdentity bool, released bool, tasksByOp map[int64][]*storage.Task) map[string][]templates.ShardStatus {
	inputs := make([]presentation.Operation, 0, len(groups))
	for _, g := range groups {
		st, errMsg := aggregateShardState(g.ops, tasksByOp)
		first := g.ops[0]
		identity := g.shard
		if qualifyIdentity {
			identity = g.namespace + "/" + g.shard
		}
		inputs = append(inputs, presentation.Operation{
			Deployment:        identity,
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
	byIdentity := make(map[string]presentation.Deployment, len(derived))
	for _, d := range derived {
		byIdentity[d.Deployment] = d
	}
	out := make(map[string][]templates.ShardStatus, len(groups))
	for i, g := range groups {
		d, ok := byIdentity[inputs[i].Deployment]
		if !ok {
			// Derive returns one deployment per input operation with its
			// identity preserved; a missing identity means that contract broke.
			// Omit the row rather than render some other shard's status under
			// this shard's name.
			slog.Warn("sharded apply comment will omit a shard status row: presentation returned no deployment for identity",
				"identity", inputs[i].Deployment, "keyspace", g.namespace, "shard", g.shard)
			continue
		}
		out[g.namespace] = append(out[g.namespace], templates.ShardStatus{
			Shard: g.shard,
			Emoji: d.Emoji,
			Label: d.Label,
			State: d.State,
			Error: d.Error,
		})
	}
	return out
}

// shardedTableStatusesByKeyspace derives one rollup per (keyspace, table) in
// resolved order and buckets the results per keyspace — the table-unit view of
// the same shard work shardStatusesByKeyspace rolls up per shard. Each shard's
// entry carries the task-vocabulary state (and copy percent) of its (shard,
// table) operation, and the table's aggregate is its most attention-worthy
// shard state, so a table with one failed shard reads failed even while its
// siblings copy.
func shardedTableStatusesByKeyspace(ops []*storage.ApplyOperation, tasksByOp map[int64][]*storage.Task) map[string][]templates.ShardedTableStatus {
	type keyspaceTable struct{ namespace, table string }
	type tableRollup struct {
		shards          []templates.ShardProgressData
		rowsCopied      int64
		rowsTotal       int64
		etaSeconds      int64
		shardsReporting int
	}
	var order []keyspaceTable
	rollups := make(map[keyspaceTable]*tableRollup)
	for _, op := range ops {
		ns, shard, table, ok := parseShardOperationKey(op.OperationKey)
		if !ok {
			// Finalizers render in the VSchema section, not as a table.
			continue
		}
		key := keyspaceTable{namespace: ns, table: table}
		r := rollups[key]
		if r == nil {
			r = &tableRollup{}
			rollups[key] = r
			order = append(order, key)
		}
		sp := shardTaskProgress(op, tasksByOp[op.ID])
		r.shards = append(r.shards, templates.ShardProgressData{
			Shard:           shard,
			Status:          sp.status,
			PercentComplete: sp.percent,
		})
		// Rows sum across the shards that have reported; the ETA is the slowest
		// reporting shard's. Shards whose dispatch wave has not started
		// contribute nothing yet, so the reporting count travels with the sums
		// and the renderer discloses the coverage instead of presenting a
		// wave's figures as the whole table's.
		if sp.rowsTotal > 0 {
			r.shardsReporting++
		}
		r.rowsCopied += sp.rowsCopied
		r.rowsTotal += sp.rowsTotal
		if sp.etaSeconds > r.etaSeconds {
			r.etaSeconds = sp.etaSeconds
		}
	}
	out := make(map[string][]templates.ShardedTableStatus, len(order))
	for _, key := range order {
		r := rollups[key]
		out[key.namespace] = append(out[key.namespace], templates.ShardedTableStatus{
			Table:           key.table,
			Status:          aggregateTableStatus(r.shards),
			RowsCopied:      r.rowsCopied,
			RowsTotal:       r.rowsTotal,
			ETASeconds:      r.etaSeconds,
			ShardsReporting: r.shardsReporting,
			Shards:          r.shards,
		})
	}
	return out
}

// shardProgress is one (shard, table) operation's display projection: the
// state and copy figures of its most attention-worthy task.
type shardProgress struct {
	status     string
	percent    int
	rowsCopied int64
	rowsTotal  int64
	etaSeconds int64
}

// shardTaskProgress resolves one (shard, table) operation's display status and
// copy figures from its most attention-worthy task — the task is where the
// engine reports live shard state. The operation state stands in when the
// operation has no tasks yet (dispatch creates them when its wave starts) or a
// task has not reported state; it normalizes into the same vocabulary.
func shardTaskProgress(op *storage.ApplyOperation, tasks []*storage.Task) shardProgress {
	best := shardProgress{}
	for _, t := range tasks {
		status := t.State
		if status == "" {
			status = op.State
		}
		if best.status == "" || taskStateRank(status) > taskStateRank(best.status) {
			best = shardProgress{
				status:     status,
				percent:    t.ProgressPercent,
				rowsCopied: t.RowsCopied,
				rowsTotal:  t.RowsTotal,
				etaSeconds: int64(t.ETASeconds),
			}
		}
	}
	if best.status == "" {
		return shardProgress{status: op.State}
	}
	return best
}

// aggregateTableStatus reduces a table's per-shard states to the one an
// operator should act on first: failure over active work, active work over
// waiting, waiting over done.
func aggregateTableStatus(shards []templates.ShardProgressData) string {
	best := shards[0].Status
	for _, sh := range shards[1:] {
		if taskStateRank(sh.Status) > taskStateRank(best) {
			best = sh.Status
		}
	}
	return best
}

// taskStateRank orders task states by how much they demand attention — the
// task-vocabulary analogue of shardStateRank, normalizing first so operation
// states fed through the no-task fallback rank the same way. Failure ranks
// highest, then active work, then paused and queued work, then the settled
// states. Pending outranks the revert window, matching deriveOverallState's
// precedence: a table with undispatched shards still has work ahead of it,
// however its landed shards hold, so the aggregate must not read as complete.
func taskStateRank(s string) int {
	switch state.NormalizeTaskStatus(s) {
	case state.Task.Failed:
		return 17
	case state.Task.FailedRetryable:
		return 16
	case state.Task.CuttingOver:
		return 15
	case state.Task.Running:
		return 14
	case state.Task.PostChecksum:
		return 13
	case state.Task.Checksumming:
		return 12
	case state.Task.CatchingUp:
		return 11
	case state.Task.Reverting:
		return 10
	case state.Task.WaitingForCutover:
		return 9
	case state.Task.Recovering:
		return 8
	case state.Task.WaitingForDeploy:
		return 7
	case state.Task.Stopped:
		return 6
	case state.Task.Pending:
		return 5
	case state.Task.RevertWindow:
		return 4
	case state.Task.Cancelled:
		return 2
	case state.Task.Reverted:
		return 1
	case state.Task.Completed:
		return 0
	default:
		// NormalizeTaskStatus maps unrecognized statuses to Task.Running, so
		// this arm is reachable only if that mapping changes; rank it the same
		// way so an unknown state still reads as active work.
		return 14
	}
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
