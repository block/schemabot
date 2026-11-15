package templates

import (
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
)

// ShardCounts is a sharded apply's per-status shard count, as the PR comment's
// Shards line reads it ("1 running table copy, 3 waiting for -40"), or "" when
// every keyspace runs on its only shard, which needs no count. The shards of
// an apply in applyState read as presentation.DeriveShards reads them.
func ShardCounts(applyState string, ops []ProgressOperation, tables []TableProgress, released bool) string {
	statuses, ok := shardStatuses(applyState, ops, tables, released)
	if !ok {
		return ""
	}
	labels := make([]string, 0, len(statuses))
	for _, s := range statuses {
		labels = append(labels, s.Label)
	}
	return presentation.ShardCounts(labels)
}

// ShardedStoppedAtCutoverNote explains the work of a sharded apply that
// SchemaBot stopped at the cutover because other work failed, and that applying
// again resumes the saved copies, or "" when nothing was stopped at the
// cutover. It counts shards, or tables when every keyspace runs on its only
// shard, the way the PR comment counts them.
func ShardedStoppedAtCutoverNote(applyState string, ops []ProgressOperation, tables []TableProgress, released bool) string {
	statuses, sharded := shardStatuses(applyState, ops, tables, released)
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
// work. It reports false when every keyspace runs on its only shard.
func shardStatuses(applyState string, ops []ProgressOperation, tables []TableProgress, released bool) ([]presentation.ShardStatus, bool) {
	type keyspaceShard struct{ keyspace, shard string }
	index := make(map[keyspaceShard]int)
	var work []presentation.ShardWork
	sharded := false
	for _, op := range ProgressOperationsForPresentation(ops, tables, released) {
		keyspace, shard, _, ok := state.ShardWorkKey(op.OperationKey)
		if !ok {
			continue
		}
		if shard != state.FullKeyRangeShard {
			sharded = true
		}
		key := keyspaceShard{keyspace, shard}
		j, seen := index[key]
		if !seen {
			j = len(work)
			index[key] = j
			work = append(work, presentation.ShardWork{Keyspace: keyspace, Shard: shard})
		}
		work[j].Operations = append(work[j].Operations, op)
	}
	if !sharded {
		return nil, false
	}
	return presentation.DeriveShards(applyState, work), true
}
