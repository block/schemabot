package templates

import (
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
)

// ShardCounts is a sharded apply's per-status shard count, as the PR comment's
// Shards line reads it ("1 running table copy, 3 queued"), or "" when
// every keyspace runs on its only shard, which needs no count. The shards of
// an apply in applyState read as presentation.DeriveShards reads them. While
// the apply runs, the count ends with how many of its operations run at once
// when more of them compete for a slot than atOnce allows
// (presentation.WithLimit).
func ShardCounts(applyState string, ops []ProgressOperation, tables []TableProgress, released bool, atOnce int) string {
	statuses, operations, competing, ok := shardStatuses(applyState, ops, tables, released)
	if !ok {
		return ""
	}
	labels := make([]string, 0, len(statuses))
	for _, s := range statuses {
		labels = append(labels, s.Label)
	}
	counts := presentation.ShardCounts(labels)
	if state.IsTerminalApplyState(applyState) {
		return counts
	}
	return presentation.WithLimit(counts, len(statuses), operations, competing, atOnce)
}

// ShardedStoppedAtCutoverNote explains the work of a sharded apply that
// SchemaBot stopped at the cutover because other work failed, and that applying
// again resumes the saved copies, or "" when nothing was stopped at the
// cutover. It counts shards, or tables when every keyspace runs on its only
// shard, the way the PR comment counts them.
func ShardedStoppedAtCutoverNote(applyState string, ops []ProgressOperation, tables []TableProgress, released bool) string {
	statuses, _, _, sharded := shardStatuses(applyState, ops, tables, released)
	if !sharded {
		return tablesStoppedAtCutover(ops).Note(presentation.TableNoun, func(name string) string { return name })
	}
	var stopped presentation.StoppedAtCutover
	for _, s := range statuses {
		if s.Presentation == presentation.StateStoppedAtCutover {
			stopped.Add(s.EndedBy)
		}
	}
	return stopped.Note(presentation.ShardNoun, func(name string) string { return name })
}

// tablesStoppedAtCutover counts the table operations, where every keyspace
// runs on its only shard, that SchemaBot stopped at the cutover because
// another failed, naming that other operation's table when it is listed.
func tablesStoppedAtCutover(ops []ProgressOperation) presentation.StoppedAtCutover {
	tableByRef := make(map[presentation.OperationRef]string, len(ops))
	for _, op := range ops {
		if _, _, table, ok := state.ShardWorkKey(op.OperationKey); ok {
			tableByRef[presentation.OperationRef{Deployment: op.Deployment, OperationKey: op.OperationKey}] = table
		}
	}
	var stopped presentation.StoppedAtCutover
	for _, op := range ops {
		if op.EndedBy != nil && state.IsState(op.State, state.ApplyOperation.Failed, state.ApplyOperation.FailedRetryable) {
			stopped.Add(tableByRef[*op.EndedBy])
		}
	}
	return stopped
}

// shardStatuses derives one status per keyspace shard of the apply's shard
// work, and counts the shard work operations behind them and how many of those
// compete for a slot (presentation.CompetingShardOperations). It reports false
// when every keyspace runs on its only shard.
func shardStatuses(applyState string, ops []ProgressOperation, tables []TableProgress, released bool) (statuses []presentation.ShardStatus, operations, competing int, sharded bool) {
	type keyspaceShard struct{ keyspace, shard string }
	index := make(map[keyspaceShard]int)
	var work []presentation.ShardWork
	for _, op := range ProgressOperationsForPresentation(ops, tables, released) {
		keyspace, shard, table, ok := state.ShardWorkKey(op.OperationKey)
		if !ok {
			continue
		}
		if shard != state.FullKeyRangeShard {
			sharded = true
		}
		op.CopiesNoRows = shardTableCopiesNoRows(keyspace, shard, table, tables)
		key := keyspaceShard{keyspace, shard}
		j, seen := index[key]
		if !seen {
			j = len(work)
			index[key] = j
			work = append(work, presentation.ShardWork{Keyspace: keyspace, Shard: shard})
		}
		work[j].Operations = append(work[j].Operations, op)
		operations++
	}
	if !sharded {
		return nil, 0, 0, false
	}
	statuses = presentation.DeriveShards(applyState, work)
	return statuses, operations, presentation.CompetingShardOperations(work, statuses), true
}

// shardTableCopiesNoRows reports whether one shard's changes to a keyspace
// table copy no rows, so the shard reads as applying on the CLI as it does on
// the PR comment. A table rolled up across shards carries the answer on each
// shard's entry, which the server derives from that shard's own tasks. A table
// that runs on its keyspace's only shard is listed as its task rows, so the
// answer is read from them. A shard the progress does not list reads as
// copying, since nothing says otherwise.
func shardTableCopiesNoRows(keyspace, shard, table string, tables []TableProgress) bool {
	var taskRows []presentation.TableChange
	for _, t := range tables {
		if t.Namespace != keyspace || t.TableName != table {
			continue
		}
		if len(t.Shards) == 0 {
			taskRows = append(taskRows, presentation.TableChange{DDLAction: t.ChangeType, Instant: t.IsInstant})
			continue
		}
		for _, sh := range t.Shards {
			if sh.Shard == shard {
				return sh.CopiesNoRows
			}
		}
	}
	return presentation.ChangesCopyNoRows(taskRows)
}
