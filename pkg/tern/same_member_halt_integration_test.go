//go:build integration

package tern

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// insertFailedShardSibling inserts a work row for another shard of the
// fixture's own target and fails it, as a sibling drive records a shard whose
// copy hit an error.
func insertFailedShardSibling(t *testing.T, f *multiOpResumeFixture) *storage.ApplyOperation {
	t.Helper()
	ctx := t.Context()
	id, err := f.stor.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:       f.apply.ID,
		Deployment:    "testdb",
		Target:        "testdb",
		OperationKey:  storage.ShardOperationKey("testdb", "80-", "users"),
		OperationKind: storage.ApplyOperationKindWork,
		State:         state.ApplyOperation.Running,
	})
	require.NoError(t, err)
	require.NoError(t, f.stor.ApplyOperations().MarkFailed(ctx, id, "duplicate entry for key 'uk_users_email'"))
	failed, err := f.stor.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, failed)
	return failed
}

// storedOperationTasks loads the fixture operation's tasks as a drive reads
// them, with their row IDs, so a lease-guarded write lands on the row.
func storedOperationTasks(t *testing.T, f *multiOpResumeFixture) []*storage.Task {
	t.Helper()
	tasks, err := f.stor.Tasks().GetByApplyOperationID(t.Context(), f.opID)
	require.NoError(t, err)
	require.Len(t, tasks, len(f.tasks))
	return tasks
}

// Shard 80- of target testdb fails under on_failure halt while shard -80 of
// the same target is still copying under its own operation. Shard -80's drive
// reads the failure at its next checkpoint, cancels its own engine work, and
// records its task cancelled under its own lease, with an apply log line that
// names the shard that failed. The apply row is left to the rollout
// projection, which settles it failed because failed outranks cancelled.
func TestLocalClient_FailedShardCancelsItsRunningSibling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.Running})
	eng := &multiOpTargetDrainerEngine{multiOpResumeEngine: f.eng}
	f.client.spiritEngine = eng
	failed := insertFailedShardSibling(t, f)
	tasks := storedOperationTasks(t, f)

	standDown, err := f.client.cancelForFailedSibling(f.opCtx, f.client.logger, eng, f.apply, tasks, firstTaskAtCutover, new(bool))
	require.NoError(t, err)
	assert.True(t, standDown, "the drive stands down once it has cancelled its own work")
	assert.Equal(t, 1, f.eng.cancelCalls, "the drive cancels its own shard's engine work")
	assert.Equal(t, state.Task.Cancelled, f.taskState(t, f.tasks[0]), "the halted shard's task ends cancelled")
	f.requireParentUntouched(t)

	logs, err := f.stor.ApplyLogs().GetByApply(t.Context(), f.apply.ID)
	require.NoError(t, err)
	var halt *storage.ApplyLog
	for _, log := range logs {
		if strings.HasPrefix(log.Message, "Cancelled ") {
			halt = log
		}
	}
	require.NotNil(t, halt, "the apply log says why work no operator stopped ended cancelled")
	assert.Equal(t, "Cancelled testdb: "+failed.OperationKey+" failed, and on_failure halt stops the rest of the work on testdb", halt.Message)
	assert.Equal(t, storage.LogLevelWarn, halt.Level)
}

// Shard 80- of target testdb fails under on_failure halt after shard -80 has
// started to cut over. Interrupting a swap part-way would leave the target
// described by neither schema, so shard -80's drive leaves its work running:
// it finishes, and the rollout still settles failed on shard 80-.
func TestLocalClient_FailedShardLeavesASiblingThatIsCuttingOver(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.CuttingOver})
	eng := &multiOpTargetDrainerEngine{multiOpResumeEngine: f.eng}
	f.client.spiritEngine = eng
	insertFailedShardSibling(t, f)
	tasks := storedOperationTasks(t, f)

	var logged bool
	for range 2 {
		standDown, err := f.client.cancelForFailedSibling(f.opCtx, f.client.logger, eng, f.apply, tasks, firstTaskAtCutover, &logged)
		require.NoError(t, err)
		assert.False(t, standDown, "the drive keeps driving a shard that is cutting over")
	}
	assert.True(t, logged, "the drive logs once that the shard finishes behind the failure")
	assert.Zero(t, f.eng.cancelCalls, "nothing cancels a shard mid-swap")
	assert.Equal(t, state.Task.CuttingOver, f.taskState(t, f.tasks[0]))
	f.requireParentUntouched(t)
}

// Shard 80- of target testdb fails under on_failure halt just as shard -80's
// own run fails on the engine. The engine refuses the cancel because the run
// already settled, so shard -80's drive leaves its task to the next poll,
// which records the engine's own outcome rather than a cancel that never
// happened.
func TestLocalClient_FailedShardLeavesASiblingWhoseRunAlreadySettled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.Running})
	f.eng.cancelErr = engine.NewSettledOutcomeError("cancel rejected: the schema change on database testdb already failed")
	eng := &multiOpTargetDrainerEngine{multiOpResumeEngine: f.eng}
	f.client.spiritEngine = eng
	insertFailedShardSibling(t, f)
	tasks := storedOperationTasks(t, f)

	standDown, err := f.client.cancelForFailedSibling(f.opCtx, f.client.logger, eng, f.apply, tasks, firstTaskAtCutover, new(bool))
	require.NoError(t, err)
	assert.False(t, standDown, "the drive keeps polling so it records the engine's own outcome")
	assert.Equal(t, 1, f.eng.cancelCalls)
	assert.Equal(t, state.Task.Running, f.taskState(t, f.tasks[0]), "a run the engine already settled is not recorded cancelled")
	f.requireParentUntouched(t)
}

// Shard 80- of target testdb fails under on_failure halt, and shard -80's
// drive cancels its engine work, but storage refuses the write that records
// its task cancelled. The drive stands down with an error that carries
// ErrTaskVerdictRefused, so the operator leaves the operation row claimable
// rather than deriving it from tasks that are only partly settled.
func TestLocalClient_FailedShardHaltWhoseCancelIsNotRecordedStaysClaimable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.Running})
	eng := &multiOpTargetDrainerEngine{multiOpResumeEngine: f.eng}
	f.client.spiritEngine = eng
	insertFailedShardSibling(t, f)
	tasks := storedOperationTasks(t, f)
	_, err := f.leaseDB.ExecContext(t.Context(), "UPDATE `apply_operations` SET `lease_token` = ? WHERE `id` = ?", "another-driver", f.opID)
	require.NoError(t, err)

	standDown, err := f.client.cancelForFailedSibling(f.opCtx, f.client.logger, eng, f.apply, tasks, firstTaskAtCutover, new(bool))
	require.ErrorIs(t, err, ErrTaskVerdictRefused)
	assert.True(t, standDown, "the engine work is cancelled, so the drive stands down")
	assert.Equal(t, 1, f.eng.cancelCalls)
	assert.Equal(t, state.Task.Running, f.taskState(t, f.tasks[0]), "storage refused the cancelled verdict")
}

// A shard of another target fails under on_failure halt. Each target is its
// own rollout member, so the failure halts nothing on target testdb: the drive
// keeps copying, and later targets are held by the claim's member gate instead.
func TestLocalClient_FailedShardOfAnotherTargetLeavesThisOneRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.Running})
	eng := &multiOpTargetDrainerEngine{multiOpResumeEngine: f.eng}
	f.client.spiritEngine = eng
	require.NoError(t, f.stor.ApplyOperations().MarkFailed(t.Context(), f.siblingOpID, "duplicate entry for key 'uk_users_email'"))
	tasks := storedOperationTasks(t, f)

	standDown, err := f.client.cancelForFailedSibling(f.opCtx, f.client.logger, eng, f.apply, tasks, firstTaskAtCutover, new(bool))
	require.NoError(t, err)
	assert.False(t, standDown)
	assert.Zero(t, f.eng.cancelCalls)
	assert.Equal(t, state.Task.Running, f.taskState(t, f.tasks[0]))
}

// Shard 80- of target testdb fails under on_failure halt before a sequential
// drive of shard -80 starts its first table. The drive starts no engine work:
// it cancels every queued task of its own operation under its own lease and
// leaves the apply row to the rollout projection.
func TestLocalClient_FailedShardCancelsASequentialSiblingsQueuedTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.Pending, state.Task.Pending})
	f.eng.planChanges = []engine.SchemaChange{{
		Namespace:    "testdb",
		TableChanges: []engine.TableChange{{Table: "users", DDL: multiOpResumeDDL}},
	}}
	insertFailedShardSibling(t, f)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.opID))
	assert.Zero(t, f.eng.applyCalls, "no table of a halted shard starts")
	for _, task := range f.tasks {
		assert.Equal(t, state.Task.Cancelled, f.taskState(t, task), "every queued table of the halted shard ends cancelled")
	}
	f.requireParentUntouched(t)
}

// siblingFailsMidCopyEngine reports its first table copying and, on its first
// progress poll, has a sibling shard fail, the way a sibling drive records a
// shard whose copy hit an error while this one is still copying.
type siblingFailsMidCopyEngine struct {
	*multiOpTargetDrainerEngine
	failSibling func()
	once        sync.Once
}

func (e *siblingFailsMidCopyEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.once.Do(e.failSibling)
	return &engine.ProgressResult{State: engine.StateRunning, Progress: 40}, nil
}

// Shard 80- of target testdb fails under on_failure halt while a sequential
// drive of shard -80 is copying its first table. At its next poll the drive
// cancels that table's engine work, cancels the table it has not started, and
// leaves the apply row to the rollout projection.
func TestLocalClient_FailedShardCancelsASequentialSiblingMidCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.Pending, state.Task.Pending})
	f.eng.planChanges = []engine.SchemaChange{{
		Namespace:    "testdb",
		TableChanges: []engine.TableChange{{Table: "users", DDL: multiOpResumeDDL}},
	}}
	f.client.spiritEngine = &siblingFailsMidCopyEngine{
		multiOpTargetDrainerEngine: &multiOpTargetDrainerEngine{multiOpResumeEngine: f.eng},
		failSibling:                func() { insertFailedShardSibling(t, f) },
	}
	f.client.taskPollIntervalOverride = 10 * time.Millisecond

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.opID))
	assert.Equal(t, 1, f.eng.applyCalls, "only the first table started before the sibling failed")
	assert.Equal(t, 1, f.eng.cancelCalls, "the drive cancels the table it was copying")
	for _, task := range f.tasks {
		assert.Equal(t, state.Task.Cancelled, f.taskState(t, task), "every table of the halted shard ends cancelled")
	}
	f.requireParentUntouched(t)
}

// Shard 80- of target testdb fails under on_failure halt while shard -80 of
// the same target waits at its cutover in an apply that did not defer its
// cutover. A drive that resumes shard -80 neither resumes its copy nor parks it
// again: it cancels the parked engine work, records the task cancelled, and
// leaves the apply row to the rollout projection.
func TestLocalClient_FailedShardCancelsASiblingWaitingAtItsCutover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.WaitingForCutover})
	f.eng.planChanges = []engine.SchemaChange{{
		Namespace:    "testdb",
		TableChanges: []engine.TableChange{{Table: "users", DDL: multiOpResumeDDL}},
	}}
	f.client.spiritEngine = &multiOpTargetDrainerEngine{multiOpResumeEngine: f.eng}
	insertFailedShardSibling(t, f)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.opID))
	assert.Zero(t, f.eng.applyCalls, "a halted shard's copy is not resumed")
	assert.Equal(t, 1, f.eng.cancelCalls, "the drive cancels the copy parked at the cutover")
	assert.Equal(t, state.Task.Cancelled, f.taskState(t, f.tasks[0]), "the parked shard ends cancelled, not waiting for its cutover")
	f.requireParentUntouched(t)
}
