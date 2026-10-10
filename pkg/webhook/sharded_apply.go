package webhook

import (
	"container/list"
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// isShardedApply reports whether the apply's operations are the per-shard
// fan-out of one or more keyspaces within one deployment
// (presentation.IsShardedApply).
func isShardedApply(ops []*storage.ApplyOperation) bool {
	return presentation.IsShardedApply(keyedOperations(ops))
}

// keyedOperations maps operation rows to the deployment and key the sharded
// layout decisions read.
func keyedOperations(ops []*storage.ApplyOperation) []presentation.KeyedOperation {
	keyed := make([]presentation.KeyedOperation, 0, len(ops))
	for _, op := range ops {
		keyed = append(keyed, presentation.KeyedOperation{Deployment: op.Deployment, OperationKey: op.OperationKey})
	}
	return keyed
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
// `-40` failed") reference shards. Finalizer operations are not shard work:
// each one becomes a VSchema change — its keyspace from the operation key, its
// display status from the operation state, and its diff from the stored plan
// (the view, see resolveShardedPlanView) — rendered in the comment's
// VSchema section. A keyspace the stored plan finalizes without a VSchema
// change renders in the Finalize section instead, so the comment does not
// claim a VSchema change the plan never carried. A
// failed finalizer's error also stands in for the apply-level failure cause
// when the apply row carries none, since a finalizer failure is
// operation-scoped and leaves no failed shard row to name it.
func buildShardedApplyData(apply *storage.Apply, ops []*storage.ApplyOperation, released bool, tasks []*storage.Task, view *shardedPlanView, tenant string) templates.ShardedApplyData {
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
	var finalizes []templates.ShardedFinalize
	finalizerError := ""
	for _, op := range ops {
		ns, shard, table, ok := state.ShardWorkKey(op.OperationKey)
		if !ok {
			// isShardedApply admits only shard work and finalizer keys, so a
			// non-shard key here is a finalizer: one keyspace's VSchema change.
			finalizerNS, isFinalizer := state.NamespaceFinalizerKey(op.OperationKey)
			if !isFinalizer {
				continue
			}
			status := presentation.FinalizerVSchemaStatus(apply.State, op.State)
			if view.finalizesOnly(finalizerNS) {
				finalizes = append(finalizes, templates.ShardedFinalize{Keyspace: finalizerNS, Status: status})
			} else {
				vschemaChanges = append(vschemaChanges, apitypes.VSchemaChange{
					Namespace: finalizerNS,
					Status:    status,
					Diff:      view.vschemaDiff(finalizerNS),
				})
			}
			if finalizerError == "" && isOperationFailureState(op.State) && op.ErrorMessage != "" {
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
		cellsByKeyspace[ns] = append(cellsByKeyspace[ns], templates.ShardCell{Shard: shard, Table: table, Statements: ddls})
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

	shardsByKeyspace := shardStatusesByKeyspace(groupOrder, released, tasksByOp)
	tablesByKeyspace := shardedTableStatusesByKeyspace(ops, tasksByOp, view)
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
		Finalizes:      finalizes,
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

// shardedPlanView is what a sharded apply's comment reads from the stored
// plan: each namespace's rendered VSchema diff, the namespaces finalized
// without a VSchema change to show, and each table's planned size. A nil
// view, when the stored plan could not be read, renders every finalizer as a
// VSchema change without a diff and every table without a size.
type shardedPlanView struct {
	vschemaDiffs map[string]string
	finalizeOnly map[string]bool
	// tableSizes is each table's planned on-disk size, summed across its
	// shards, with the number of shards the sum covers. The shard tasks carry
	// no size of their own, since the plan's figure is the whole table's, so
	// the comment reads it here and shows it on the table's line.
	tableSizes map[shardedTableKey]plannedTableSize
}

// shardedTableKey names one table of a sharded apply.
type shardedTableKey struct{ namespace, table string }

// plannedTableSize is one table's planned size and the shards it spans. The
// shard count comes from the plan rather than from the apply's operations,
// which may not all be attached yet, so it always names the shards the size
// was summed over.
type plannedTableSize struct {
	bytes  int64
	shards int
}

// plannedSize returns the table's planned size across its shards and the
// number of shards that size covers, or nil and zero when the stored plan
// carries no estimate for it.
func (p *shardedPlanView) plannedSize(namespace, table string) (*int64, int) {
	if p == nil {
		return nil, 0
	}
	size, ok := p.tableSizes[shardedTableKey{namespace, table}]
	if !ok {
		return nil, 0
	}
	return &size.bytes, size.shards
}

// vschemaDiff returns the namespace's rendered VSchema diff, or "" when the
// stored plan carries none.
func (p *shardedPlanView) vschemaDiff(namespace string) string {
	if p == nil {
		return ""
	}
	return p.vschemaDiffs[namespace]
}

// finalizesOnly reports whether the stored plan finalizes the namespace
// without a VSchema change to show.
func (p *shardedPlanView) finalizesOnly(namespace string) bool {
	return p != nil && p.finalizeOnly[namespace]
}

// resolveShardedPlanView loads what a sharded apply's comment needs from the
// stored plan: which namespaces finalize without a VSchema change, each
// namespace's rendered VSchema diff — the diff the engine annotated at plan
// time and plan persistence kept (PlanMetadataVSchemaDiff), so the comment
// shows the change the operator approved rather than a re-diff against live
// state — and each table's planned size. Returns nil without touching storage
// unless the apply renders a sharded layout, since any other shape would pay
// a stored-plan read on every comment edit just to discard the result.
// Best-effort: a plan load failure or a missing plan row contributes nothing
// rather than blocking the comment, and a stored plan without diffs or sizes
// (recorded before they were persisted) contributes none.
func resolveShardedPlanView(ctx context.Context, stor storage.Storage, apply *storage.Apply, ops []*storage.ApplyOperation) *shardedPlanView {
	if !needsShardedPlanView(ops) {
		return nil
	}

	plan, err := stor.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		slog.Warn("comment will omit VSchema diffs and table sizes and render every finalizer as a VSchema change: failed to load stored plan",
			append(apply.LogAttrs(), "error", err)...)
		return nil
	}
	if plan == nil {
		slog.Warn("comment will omit VSchema diffs and table sizes and render every finalizer as a VSchema change: stored plan row not found",
			apply.LogAttrs()...)
		return nil
	}

	view := &shardedPlanView{vschemaDiffs: map[string]string{}, finalizeOnly: map[string]bool{}, tableSizes: map[shardedTableKey]plannedTableSize{}}
	for namespace, nsData := range plan.Namespaces {
		if nsData == nil {
			continue
		}
		for _, tc := range nsData.Tables {
			if tc.EstimatedBytes != nil {
				view.tableSizes[shardedTableKey{namespace, tc.Table}] = plannedTableSize{bytes: *tc.EstimatedBytes, shards: tc.ShardCount}
			}
		}
		if d := nsData.Metadata[storage.PlanMetadataVSchemaDiff]; d != "" {
			view.vschemaDiffs[namespace] = d
		}
		if nsData.FinalizesWithoutVSchemaChange() {
			view.finalizeOnly[namespace] = true
		}
	}
	return view
}

// needsShardedPlanView reports whether the apply's comment consumes the
// stored plan view: a sharded apply does, for its finalizers and its tables'
// sizes.
func needsShardedPlanView(ops []*storage.ApplyOperation) bool {
	return isShardedApply(ops)
}

// shardedPlanCacheLimit bounds how many plans a shardedPlanCache holds, so
// a long-lived process does not keep every plan it has ever rendered.
const shardedPlanCacheLimit = 1024

// shardedPlanCache remembers what each stored plan says about its apply's
// finalizers and table sizes once a read of it has succeeded. A stored plan
// never changes, so the first successful read stays true for every later
// render. The plan decides whether a Strata apply's comments take the
// single-deployment layout, and one cache is shared by every comment render in
// the process (each driver's observer, the aggregate terminal observer, and
// the summary repair), so within a process a failed read in a later render
// cannot switch the comments back to the shard layout that earlier renders did
// not use. A full cache evicts the plan rendered longest ago, so an apply still
// in flight, which renders on every progress tick, keeps its entry. A nil
// cache reads storage on every call.
type shardedPlanCache struct {
	mu sync.Mutex
	// recent orders the cached plans from most to least recently rendered.
	recent *list.List
	byPlan map[int64]*list.Element
}

// shardedPlanCacheEntry is one cached plan in shardedPlanCache.recent.
type shardedPlanCacheEntry struct {
	planID int64
	plan   *shardedPlanView
}

func newShardedPlanCache() *shardedPlanCache {
	return &shardedPlanCache{recent: list.New(), byPlan: make(map[int64]*list.Element)}
}

// resolve returns the cached view of the apply's stored plan, or
// reads it with resolveShardedPlanView and caches a successful read. The
// read runs outside the lock, so a slow read for one apply does not hold up
// another's comment.
func (c *shardedPlanCache) resolve(ctx context.Context, stor storage.Storage, apply *storage.Apply, ops []*storage.ApplyOperation) *shardedPlanView {
	if c == nil || !needsShardedPlanView(ops) {
		return resolveShardedPlanView(ctx, stor, apply, ops)
	}
	if cached := c.lookup(apply.PlanID); cached != nil {
		return cached
	}
	plan := resolveShardedPlanView(ctx, stor, apply, ops)
	if plan == nil {
		return nil
	}
	return c.store(apply, plan)
}

// lookup returns the cached plan for planID, marking it the most recently
// rendered, or nil when it is not cached.
func (c *shardedPlanCache) lookup(planID int64) *shardedPlanView {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.byPlan[planID]
	if !ok {
		return nil
	}
	c.recent.MoveToFront(el)
	return el.Value.(*shardedPlanCacheEntry).plan
}

// store caches a plan read for the apply and returns the cached plan: the one
// passed in, or the one a concurrent render stored first. When the cache is
// over its limit it evicts the plan rendered longest ago.
func (c *shardedPlanCache) store(apply *storage.Apply, plan *shardedPlanView) *shardedPlanView {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byPlan[apply.PlanID]; ok {
		c.recent.MoveToFront(el)
		return el.Value.(*shardedPlanCacheEntry).plan
	}
	c.byPlan[apply.PlanID] = c.recent.PushFront(&shardedPlanCacheEntry{planID: apply.PlanID, plan: plan})
	if c.recent.Len() > shardedPlanCacheLimit {
		oldest := c.recent.Back()
		evicted := oldest.Value.(*shardedPlanCacheEntry).planID
		c.recent.Remove(oldest)
		delete(c.byPlan, evicted)
		slog.Debug("sharded plan cache is full; evicted the plan rendered longest ago",
			append(apply.LogAttrs(), "evicted_plan_id", evicted)...)
	}
	return plan
}

// isOperationFailureState reports whether an operation's state carries
// an operator-facing error — a terminal failure or an automatic retry after
// one, mirroring the shard-failure vocabulary.
func isOperationFailureState(opState string) bool {
	return state.IsState(opState, state.ApplyOperation.Failed, state.ApplyOperation.FailedRetryable)
}

// shardStatusesByKeyspace derives one status per (keyspace, shard) group
// (presentation.DeriveShards) and buckets the results per keyspace, preserving
// resolved order. The status row's Shard is the plain name; it renders under
// its keyspace heading.
func shardStatusesByKeyspace(groups []shardWorkGroup, released bool, tasksByOp map[int64][]*storage.Task) map[string][]templates.ShardStatus {
	work := make([]presentation.ShardWork, 0, len(groups))
	for _, g := range groups {
		sw := presentation.ShardWork{Keyspace: g.namespace, Shard: g.shard}
		for _, op := range g.ops {
			sw.Operations = append(sw.Operations, presentation.Operation{
				State:             op.State,
				Barrier:           op.CutoverPolicy == storage.CutoverPolicyBarrier,
				Parallel:          op.CutoverPolicy == storage.CutoverPolicyParallel,
				ContinueOnFailure: op.OnFailure == storage.OnFailureContinue,
				PauseOnFailure:    op.OnFailure == storage.OnFailurePause,
				Released:          released,
				Error:             shardOperationError(op, tasksByOp[op.ID]),
			})
		}
		work = append(work, sw)
	}
	out := make(map[string][]templates.ShardStatus, len(groups))
	for _, s := range presentation.DeriveShards(work) {
		out[s.Keyspace] = append(out[s.Keyspace], templates.ShardStatus{
			Shard: s.Shard,
			Emoji: s.Emoji,
			Label: s.Label,
			State: s.State,
			Error: s.Error,
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
// siblings copy. Each table carries its planned size from the stored plan.
func shardedTableStatusesByKeyspace(ops []*storage.ApplyOperation, tasksByOp map[int64][]*storage.Task, view *shardedPlanView) map[string][]templates.ShardedTableStatus {
	type keyspaceTable struct{ namespace, table string }
	var order []keyspaceTable
	shardsByTable := make(map[keyspaceTable][]templates.ShardProgressData)
	copiesByTable := make(map[keyspaceTable][]presentation.ShardCopy)
	for _, op := range ops {
		ns, shard, table, ok := state.ShardWorkKey(op.OperationKey)
		if !ok {
			// Finalizers render in the VSchema section, not as a table.
			continue
		}
		key := keyspaceTable{namespace: ns, table: table}
		if _, seen := copiesByTable[key]; !seen {
			order = append(order, key)
		}
		sp := shardOperationCopy(op, tasksByOp[op.ID])
		copiesByTable[key] = append(copiesByTable[key], sp)
		shardsByTable[key] = append(shardsByTable[key], templates.ShardProgressData{
			Shard:           shard,
			Status:          sp.Status,
			PercentComplete: sp.PercentComplete,
		})
	}
	out := make(map[string][]templates.ShardedTableStatus, len(order))
	for _, key := range order {
		rollup := presentation.RollUpShardedTable(copiesByTable[key])
		estimatedBytes, plannedShards := view.plannedSize(key.namespace, key.table)
		out[key.namespace] = append(out[key.namespace], templates.ShardedTableStatus{
			Table:           key.table,
			Status:          rollup.Status,
			RowsCopied:      rollup.RowsCopied,
			RowsTotal:       rollup.RowsTotal,
			ETASeconds:      rollup.ETASeconds,
			ShardsReporting: rollup.ShardsReporting,
			EstimatedBytes:  estimatedBytes,
			PlannedShards:   plannedShards,
			Shards:          shardsByTable[key],
		})
	}
	return out
}

// shardOperationCopy resolves one (shard, table) operation's display progress
// from its stored tasks (presentation.ShardOperationCopy).
func shardOperationCopy(op *storage.ApplyOperation, tasks []*storage.Task) presentation.ShardCopy {
	copies := make([]presentation.ShardCopy, 0, len(tasks))
	for _, t := range tasks {
		copies = append(copies, presentation.ShardCopy{
			Status:          t.State,
			PercentComplete: t.ProgressPercent,
			RowsCopied:      t.RowsCopied,
			RowsTotal:       t.RowsTotal,
			ETASeconds:      int64(t.ETASeconds),
		})
	}
	return presentation.ShardOperationCopy(op.State, copies)
}

// shardOperationError is a shard operation's error. When the row carries no
// error message (a remote failure records the error on the operation's tasks,
// and the operator may not have stamped the row), it falls back to the first
// task error so a failed shard always shows why; otherwise the comment is
// silent and the operator has to dig through logs.
func shardOperationError(op *storage.ApplyOperation, tasks []*storage.Task) string {
	if op.ErrorMessage != "" {
		return op.ErrorMessage
	}
	return firstTaskError(tasks)
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

// rendersAsSingleShard reports whether a sharded apply reads as one change on
// one database, so its comments take the single-deployment layout, with its
// progress bars and DDL, instead of the shard rollup. That holds when every
// keyspace with shard work runs on the shard covering its whole keyrange and
// every finalizer only finalizes a keyspace beside its DDL, with no VSchema
// change to show. The shard is judged by its keyrange, not by how many shards
// the apply touches: operations exist only for the shards that change, so one
// changing shard of a keyspace with several is still a sharded change and
// keeps the shard layout, which names it. So do a VSchema change, a keyspace
// whose only work is its finalize, and a stored plan that could not be read
// (a nil view). The decision reads every operation the apply declared, not
// only those attached so far, so an apply whose operations attach over time
// takes one layout from its first comment rather than switching as its
// siblings appear.
func rendersAsSingleShard(apply *storage.Apply, ops []*storage.ApplyOperation, view *shardedPlanView) bool {
	keyspacesWithWork := make(map[string]bool)
	var finalizerKeyspaces []string
	for _, key := range applyOperationKeys(apply, ops) {
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
	for _, ns := range finalizerKeyspaces {
		if !keyspacesWithWork[ns] || !view.finalizesOnly(ns) {
			return false
		}
	}
	return len(keyspacesWithWork) > 0
}

// buildSingleShardApplyCommentData maps a sharded apply that rendersAsSingleShard
// onto the single-deployment comment data: the shard operations' tables and the
// lone shard operation's display projection when there is one. A finalize
// beside a keyspace's DDL is not shown, the same as in the plan. When the apply
// row carries no failure cause, the most significant operation's error stands
// in for it, falling back to that operation's task error as the shard layout
// does, so a failed shard or finalizer still says why. The per-shard summary is
// left out, since each keyspace has only one shard. That shard covers the
// whole table, so each table's planned size from the stored plan is its own.
func buildSingleShardApplyCommentData(apply *storage.Apply, ops []*storage.ApplyOperation, tasks []*storage.Task, displayByOp map[int64]operationDisplay, view *shardedPlanView, tenant string) templates.ApplyStatusCommentData {
	tasksByOp := groupTasksByOperation(tasks)
	var workTasks []*storage.Task
	var workOps []*storage.ApplyOperation
	for _, op := range ops {
		if _, _, _, ok := state.ShardWorkKey(op.OperationKey); ok {
			workOps = append(workOps, op)
			workTasks = append(workTasks, tasksByOp[op.ID]...)
		}
	}
	sort.Slice(workTasks, func(i, j int) bool { return workTasks[i].ID < workTasks[j].ID })

	data := buildApplyCommentData(apply, workTasks, singleOpDisplay(workOps, displayByOp), nil, tenant)
	for i := range data.Tables {
		if data.Tables[i].EstimatedBytes == nil {
			data.Tables[i].EstimatedBytes, _ = view.plannedSize(data.Tables[i].Namespace, data.Tables[i].TableName)
		}
	}
	if data.ErrorMessage == "" {
		data.ErrorMessage = mostSignificantFailure(ops, tasksByOp)
	}
	return data
}

// mostSignificantFailure is the error of the operation an operator should act
// on first (presentation.ShardStateRank) when that operation failed, and ""
// otherwise.
func mostSignificantFailure(ops []*storage.ApplyOperation, tasksByOp map[int64][]*storage.Task) string {
	best := ops[0]
	for _, op := range ops[1:] {
		if presentation.ShardStateRank(op.State) > presentation.ShardStateRank(best.State) {
			best = op
		}
	}
	if !isOperationFailureState(best.State) {
		return ""
	}
	return shardOperationError(best, tasksByOp[best.ID])
}

// applyOperationKeys returns the keys of every operation the apply is made of:
// the manifest the dispatcher declared when the apply carries one, since its
// operations may still be attaching, and the attached operations' keys
// otherwise.
func applyOperationKeys(apply *storage.Apply, ops []*storage.ApplyOperation) []string {
	if apply != nil && len(apply.ExpectedOperationKeys) > 0 {
		return apply.ExpectedOperationKeys
	}
	keys := make([]string, 0, len(ops))
	for _, op := range ops {
		keys = append(keys, op.OperationKey)
	}
	return keys
}
