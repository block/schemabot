//go:build integration

package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// An apply defers its cutover across shards -40 and 40-80. Shard -40 fails
// under the halt policy while 40-80 has copied its table and waits at the
// cutover. The cutover can never run for both, so the reaper records a stop
// as SchemaBot and says why in the apply's log, rather than leaving 40-80 to
// hold its target until the cutover wait times out. Once the stop has reached
// 40-80 and saved its copy, the next pass fails the apply: 40-80 fails naming
// -40 as the cause, its task stays stopped so a new apply resumes the copy,
// the stop request is answered, and the terminal summary is published once.
func TestReaper_EndedOperationStopsThenSettlesTheHeldShard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "ended-operation-stops-held-shard",
		parentState:     state.Apply.RunningDegraded,
		cutoverPolicy:   storage.CutoverPolicyBarrier,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a"},
		targets:         []string{"-40", "40-80"},
		perOpState: map[string]string{
			"-40":   state.ApplyOperation.Failed,
			"40-80": state.ApplyOperation.WaitingForCutover,
		},
		perTaskState: map[string]string{
			"-40":   state.Task.Failed,
			"40-80": state.Task.WaitingForCutover,
		},
	})
	apply, err := stor.Applies().Get(ctx, seed.applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	apply.Options = storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true})
	require.NoError(t, stor.Applies().Update(ctx, apply))

	svc := newMatrixService(t, stor, map[string]tern.Client{"region-a/staging": &mockTernClient{}})

	svc.runEndedOperationPass(ctx)

	stop, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, stop, "the reaper records a stop on the held apply")
	assert.Equal(t, storage.ControlRequestPending, stop.Status)
	assert.Equal(t, storage.EndedOperationStopCaller, stop.RequestedBy)
	require.True(t, applyLogContains(t, stor, seed.applyID, "ended before the cutover"),
		"the apply log says why SchemaBot stopped the apply")

	svc.runEndedOperationPass(ctx)
	apply, err = stor.Applies().Get(ctx, seed.applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, state.Apply.RunningDegraded, apply.State, "the apply stays open while 40-80 still waits at the cutover")
	assert.Equal(t, 0, svc.matrixSummary.count())

	// The drive holding 40-80 delivers the stop: the copy is checkpointed and
	// the operation and its task read stopped.
	require.NoError(t, stor.ApplyOperations().UpdateState(ctx, seed.opID("40-80"), state.ApplyOperation.Stopped))
	heldTasks, err := stor.Tasks().GetByApplyOperationID(ctx, seed.opID("40-80"))
	require.NoError(t, err)
	require.Len(t, heldTasks, 1)
	heldTasks[0].State = state.Task.Stopped
	require.NoError(t, stor.Tasks().Update(ctx, heldTasks[0]))

	svc.runEndedOperationPass(ctx)

	apply, err = stor.Applies().Get(ctx, seed.applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, state.Apply.Failed, apply.State)
	held, err := stor.ApplyOperations().Get(ctx, seed.opID("40-80"))
	require.NoError(t, err)
	require.NotNil(t, held)
	assert.Equal(t, state.ApplyOperation.Failed, held.State)
	assert.Equal(t, seed.opID("-40"), held.EndedByOperationID, "40-80 names the shard it failed for")
	heldTasks, err = stor.Tasks().GetByApplyOperationID(ctx, seed.opID("40-80"))
	require.NoError(t, err)
	require.Len(t, heldTasks, 1)
	assert.Equal(t, state.Task.Stopped, heldTasks[0].State, "the saved copy stays stopped for a new apply to resume")

	stop, err = stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, stop)
	assert.Equal(t, storage.ControlRequestCompleted, stop.Status, "SchemaBot's stop is answered once the apply settles")
	assert.True(t, applyLogContains(t, stor, seed.applyID, "The apply failed"),
		"the apply log records why the apply failed")
	assert.Equal(t, 1, svc.matrixSummary.count(), "the terminal summary is published once")

	svc.runEndedOperationPass(ctx)
	assert.Equal(t, 1, svc.matrixSummary.count(), "a settled apply is not settled again")
}

func applyLogContains(t *testing.T, stor storage.Storage, applyID int64, text string) bool {
	t.Helper()
	logs, err := stor.ApplyLogs().GetByApply(t.Context(), applyID)
	require.NoError(t, err)
	for _, entry := range logs {
		if strings.Contains(entry.Message, text) {
			return true
		}
	}
	return false
}
