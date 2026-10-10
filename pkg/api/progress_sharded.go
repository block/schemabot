package api

import (
	"context"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// rollUpShardedProgress reads a sharded apply the way its PR comments do: as
// one change across its keyspaces' shards, not as an operation row per shard
// and table. Each keyspace table becomes one row with its shards listed under
// it, a table on its keyspace's only shard keeps its own rows, and each
// finalizer with a VSchema diff joins the VSchema display metadata, all read
// against the stored plan. The apply reads as every operation its generation
// manifest declares, not only those attached so far: a declared operation
// still to attach joins Operations and its table's shards as pending, so the
// rollout's scope holds steady while its operations attach one dispatch at a
// time. Operations still lists every attached row. Any other apply is left as
// it is.
//
// rows are the apply's table rows, one per task, in the order of tasks.
func (s *Service) rollUpShardedProgress(ctx context.Context, resp *apitypes.ProgressResponse, apply *storage.Apply, attached []*storage.ApplyOperation, tasks []*storage.Task, rows []*apitypes.TableProgressResponse) {
	ops := withPendingDeclaredOperations(apply, attached)
	keyed := make([]presentation.KeyedOperation, 0, len(ops))
	for _, op := range ops {
		keyed = append(keyed, presentation.KeyedOperation{Deployment: op.Deployment, OperationKey: op.OperationKey})
	}
	if !presentation.IsShardedApply(keyed) {
		resp.Tables = rows
		return
	}
	resp.Sharded = true
	for _, op := range ops[len(attached):] {
		resp.Operations = append(resp.Operations, progressOperationResponseFromStorage(op))
	}
	plan := s.storedPlanForShardedProgress(ctx, apply)
	resp.Tables = shardedTableRows(ops, tasks, rows, plan)
	s.addFinalizerVSchemaChanges(resp, apply, ops, plan)
}

// withPendingDeclaredOperations returns the apply's attached operations
// followed by a pending stand-in for each key its generation manifest declares
// with no operation attached yet (storage.Apply.MissingExpectedOperationKeys),
// on the deployment its attached operations run on. A stand-in has ID zero,
// which no stored task references, so it carries no rows.
func withPendingDeclaredOperations(apply *storage.Apply, attached []*storage.ApplyOperation) []*storage.ApplyOperation {
	missing := apply.MissingExpectedOperationKeys(attached)
	if len(missing) == 0 {
		return attached
	}
	deployment := apply.Deployment
	if len(attached) > 0 {
		deployment = attached[0].Deployment
	}
	ops := slices.Clone(attached)
	for _, key := range missing {
		kind := storage.ApplyOperationKindWork
		if _, ok := state.NamespaceFinalizerKey(key); ok {
			kind = storage.ApplyOperationKindGroupFinalizer
		}
		ops = append(ops, &storage.ApplyOperation{
			ApplyID:       apply.ID,
			Deployment:    deployment,
			OperationKey:  key,
			OperationKind: kind,
			State:         state.ApplyOperation.Pending,
		})
	}
	return ops
}

// shardedTableRows rolls a sharded apply's per-task rows up into one row per
// keyspace table, in the order the table's first shard operation appears. A
// table running only on its keyspace's only shard keeps its task rows as they
// are, since there is no shard to name. Rows of finalizer operations are
// dropped: a finalizer is a keyspace's VSchema change, not a table. A row whose
// task belongs to no listed operation is kept as it is. A rolled-up row carries
// the table's planned size from plan, which may be nil.
func shardedTableRows(ops []*storage.ApplyOperation, tasks []*storage.Task, rows []*apitypes.TableProgressResponse, plan *storage.Plan) []*apitypes.TableProgressResponse {
	opByID := make(map[int64]*storage.ApplyOperation, len(ops))
	for _, op := range ops {
		opByID[op.ID] = op
	}
	tasksByOp := make(map[int64][]*storage.Task)
	rowsByOp := make(map[int64][]*apitypes.TableProgressResponse)
	var unattributed []*apitypes.TableProgressResponse
	for i, task := range tasks {
		if task.ApplyOperationID == nil || opByID[*task.ApplyOperationID] == nil {
			unattributed = append(unattributed, rows[i])
			continue
		}
		id := *task.ApplyOperationID
		tasksByOp[id] = append(tasksByOp[id], task)
		rowsByOp[id] = append(rowsByOp[id], rows[i])
	}

	type keyspaceTable struct{ namespace, table string }
	var order []keyspaceTable
	opsByTable := make(map[keyspaceTable][]*storage.ApplyOperation)
	for _, op := range ops {
		ns, _, table, ok := state.ShardWorkKey(op.OperationKey)
		if !ok {
			continue
		}
		key := keyspaceTable{namespace: ns, table: table}
		if _, seen := opsByTable[key]; !seen {
			order = append(order, key)
		}
		opsByTable[key] = append(opsByTable[key], op)
	}

	out := make([]*apitypes.TableProgressResponse, 0, len(order)+len(unattributed))
	for _, key := range order {
		tableOps := opsByTable[key]
		if runsOnOnlyShard(tableOps) && len(rowsByOp[tableOps[0].ID]) > 0 {
			out = append(out, rowsByOp[tableOps[0].ID]...)
			continue
		}
		row := shardedTableRow(key.namespace, key.table, tableOps, tasksByOp, rowsByOp, plan)
		if planned := plannedTable(plan, key.namespace, key.table); planned != nil {
			row.EstimatedBytes, row.PlannedShards = planned.EstimatedBytes, int32(planned.ShardCount)
		}
		out = append(out, row)
	}
	return append(out, unattributed...)
}

// runsOnOnlyShard reports whether a table's change runs on one shard, the one
// covering its keyspace's whole keyrange.
func runsOnOnlyShard(tableOps []*storage.ApplyOperation) bool {
	if len(tableOps) != 1 {
		return false
	}
	_, shard, _, _ := state.ShardWorkKey(tableOps[0].OperationKey)
	return shard == state.FullKeyRangeShard
}

// shardedTableRow is one keyspace table's row rolled up across the shard
// operations that run it (presentation.RollUpShardedTable), with each shard's
// progress listed under it. The DDL is each distinct statement the shards run,
// in the order they first appear, so a shard running a different change is
// not hidden behind its siblings' statement. A shard with no rows yet, whose
// wave has not started or whose operation has not attached, reads as its
// operation's state, with the change the stored plan reviewed for it, so the
// table still names its statement and whether it creates, alters, or drops.
func shardedTableRow(namespace, table string, tableOps []*storage.ApplyOperation, tasksByOp map[int64][]*storage.Task, rowsByOp map[int64][]*apitypes.TableProgressResponse, plan *storage.Plan) *apitypes.TableProgressResponse {
	row := &apitypes.TableProgressResponse{
		TableName:  table,
		Keyspace:   namespace,
		Deployment: tableOps[0].Deployment,
	}
	var ddls []string
	copies := make([]presentation.ShardCopy, 0, len(tableOps))
	for _, op := range tableOps {
		_, shard, _, _ := state.ShardWorkKey(op.OperationKey)
		sc := presentation.ShardOperationCopy(op.State, taskCopies(tasksByOp[op.ID]))
		copies = append(copies, sc)
		row.Shards = append(row.Shards, &apitypes.ShardProgressResponse{
			Shard:           shard,
			Status:          sc.Status,
			RowsCopied:      sc.RowsCopied,
			RowsTotal:       sc.RowsTotal,
			ETASeconds:      sc.ETASeconds,
			PercentComplete: int32(sc.PercentComplete),
		})
		shardRows := rowsByOp[op.ID]
		if len(shardRows) == 0 {
			if planned := plannedShardChange(plan, namespace, shard, table); planned != nil {
				shardRows = []*apitypes.TableProgressResponse{{ChangeType: planned.Operation, DDL: planned.DDL}}
			}
		}
		for _, r := range shardRows {
			if row.ChangeType == "" {
				row.ChangeType = r.ChangeType
			}
			if r.DDL != "" && !slices.Contains(ddls, r.DDL) {
				ddls = append(ddls, r.DDL)
			}
			if r.Throttled && !row.Throttled {
				row.Throttled = true
				row.ThrottleReason = r.ThrottleReason
			}
		}
	}
	row.DDL = strings.Join(ddls, "\n")

	rollup := presentation.RollUpShardedTable(copies)
	row.Status = rollup.Status
	row.RowsCopied = rollup.RowsCopied
	row.RowsTotal = rollup.RowsTotal
	row.ETASeconds = rollup.ETASeconds
	if rollup.RowsTotal > 0 {
		row.PercentComplete = int32(min(100, rollup.RowsCopied*100/rollup.RowsTotal))
	}
	return row
}

// plannedTable is the change the stored plan recorded for a keyspace table
// across all its shards, or nil when plan is nil or records none.
func plannedTable(plan *storage.Plan, namespace, table string) *storage.TableChange {
	if plan == nil || plan.Namespaces[namespace] == nil {
		return nil
	}
	tables := plan.Namespaces[namespace].Tables
	for i := range tables {
		if tables[i].Table == table {
			return &tables[i]
		}
	}
	return nil
}

// plannedShardChange is the change the stored plan reviewed for a table on
// one shard: the shard's own entry, which carries the exact DDL the shard
// applies, else the keyspace's entry for the table (plannedTable).
func plannedShardChange(plan *storage.Plan, namespace, shard, table string) *storage.TableChange {
	if plan == nil || plan.Namespaces[namespace] == nil {
		return nil
	}
	for _, sp := range plan.Namespaces[namespace].Shards {
		if sp.Shard != shard {
			continue
		}
		for i := range sp.Changes {
			if sp.Changes[i].Table == table {
				return &sp.Changes[i]
			}
		}
	}
	return plannedTable(plan, namespace, table)
}

// taskCopies is a shard operation's stored tasks as the shard rollup reads
// them.
func taskCopies(tasks []*storage.Task) []presentation.ShardCopy {
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
	return copies
}

// addFinalizerVSchemaChanges adds each finalizer whose keyspace's VSchema
// diff the stored plan carries, the change the operator approved, to the
// response's VSchema display metadata, with its status
// (presentation.FinalizerVSchemaStatus). A finalizer with no diff to show adds
// nothing, nor does any finalizer when the plan cannot be read (a nil plan). A
// VSchema change the engine already reported is left as it is.
func (s *Service) addFinalizerVSchemaChanges(resp *apitypes.ProgressResponse, apply *storage.Apply, ops []*storage.ApplyOperation, plan *storage.Plan) {
	if plan == nil {
		return
	}
	existing, err := apitypes.ParseVSchemaChanges(resp.Metadata)
	if err != nil {
		s.logger.Warn("progress response will keep the engine's VSchema display metadata without the finalizers': failed to decode it",
			append(apply.LogAttrs(), "error", err)...)
		return
	}
	reported := make(map[string]bool, len(existing))
	for _, c := range existing {
		reported[c.Namespace] = true
	}
	changes := existing
	for _, op := range ops {
		ns, ok := state.NamespaceFinalizerKey(op.OperationKey)
		if !ok || reported[ns] || plan.Namespaces[ns] == nil {
			continue
		}
		diff := plan.Namespaces[ns].Metadata[storage.PlanMetadataVSchemaDiff]
		if diff == "" {
			continue
		}
		changes = append(changes, apitypes.VSchemaChange{Namespace: ns, Status: presentation.FinalizerVSchemaStatus(apply.State, op.State), Diff: diff})
	}
	if len(changes) == len(existing) {
		return
	}
	encoded, err := apitypes.EncodeVSchemaChanges(changes)
	if err != nil {
		s.logger.Warn("progress response will omit the finalizers' VSchema changes: failed to encode them",
			append(apply.LogAttrs(), "error", err)...)
		return
	}
	if resp.Metadata == nil {
		resp.Metadata = make(map[string]string, 1)
	}
	resp.Metadata[apitypes.VSchemaChangesMetadataKey] = encoded
}

// storedPlanForShardedProgress loads the apply's stored plan for its tables'
// planned sizes and its finalizers' VSchema changes, or nil when it cannot be
// read. The progress
// response is display only, so a failed read is logged rather than failing it.
func (s *Service) storedPlanForShardedProgress(ctx context.Context, apply *storage.Apply) *storage.Plan {
	plan, err := s.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		s.logger.Warn("progress response will show every table without its planned size and no finalizer VSchema change: failed to load stored plan",
			append(apply.LogAttrs(), "plan_id", apply.PlanID, "error", err)...)
		return nil
	}
	if plan == nil {
		s.logger.Warn("progress response will show every table without its planned size and no finalizer VSchema change: stored plan row not found",
			append(apply.LogAttrs(), "plan_id", apply.PlanID)...)
		return nil
	}
	return plan
}
