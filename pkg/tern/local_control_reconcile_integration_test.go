//go:build integration

package tern

import (
	"context"
	"errors"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// authoritativeProgressEngine overlays the invocation-tracking engine with an
// externally authoritative progress view, modelling an engine (like
// PlanetScale) whose backend is the source of truth for a change's state
// regardless of which instance asks. Tests configure the state the backend
// reports (or a read failure) to exercise the drive's terminal-truth reconcile
// before pending commands are consumed.
type authoritativeProgressEngine struct {
	*invocationTrackingEngine
	progressState engine.State
	progressErr   error
}

func (e *authoritativeProgressEngine) ProgressIsExternallyAuthoritative() bool { return true }

func (e *authoritativeProgressEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.record("Progress")
	if e.progressErr != nil {
		return nil, e.progressErr
	}
	return &engine.ProgressResult{State: e.progressState}, nil
}

// newAuthoritativeControlClient builds a MySQL-type LocalClient whose engine
// declares its progress externally authoritative, reporting the given backend
// state (or failing the read with progressErr).
func newAuthoritativeControlClient(t *testing.T, dsn string, stor storage.Storage, progressState engine.State, progressErr error) (*LocalClient, *authoritativeProgressEngine) {
	t.Helper()
	client, tracking := newTasklessControlClient(t, dsn, stor)
	eng := &authoritativeProgressEngine{
		invocationTrackingEngine: tracking,
		progressState:            progressState,
		progressErr:              progressErr,
	}
	client.spiritEngine = eng
	return client, eng
}

// dispatchApplyAwaitingDeployWithPendingCancel dispatches a queued apply whose
// task holds live engine work (waiting for the backend deploy) and records a
// durable cancel request against it, so the next drive claim must decide
// between consuming the cancel and adopting the engine's terminal truth.
func dispatchApplyAwaitingDeployWithPendingCancel(t *testing.T, stor storage.Storage, client *LocalClient) *storage.Apply {
	t.Helper()
	ctx := t.Context()

	apply := dispatchQueuedApply(t, stor, client, []storage.TableChange{{
		Namespace: "testdb",
		Table:     "users",
		DDL:       "ALTER TABLE `users` ADD COLUMN reconcile_note VARCHAR(255)",
		Operation: "alter",
	}})

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "the dispatched apply must carry its task")
	tasks[0].State = state.Task.WaitingForDeploy
	require.NoError(t, stor.Tasks().Update(ctx, tasks[0]),
		"the task must hold live engine work so the drive has a backend change to reconcile")

	cancelResp, err := client.Cancel(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, cancelResp.Accepted)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestPending)
	return apply
}

// A cancel is requested while the engine's backend is still finishing the
// change, and the backend completes it before an operator drive consumes the
// durable cancel request. The drive that claims the apply must learn the
// backend's terminal truth before running the cancel: it settles the apply and
// its task to completed — the authoritative outcome — and completes the durable
// cancel request without ever invoking the engine's cancel. Running the cancel
// instead would have the backend reject it forever, and the operator would
// re-claim the apply and re-run the doomed cancel on every poll.
func TestLocalClient_DriveAdoptsEngineTerminalTruthBeforeConsumingCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, eng := newAuthoritativeControlClient(t, dsn, stor, engine.StateCompleted, nil)

	apply := dispatchApplyAwaitingDeployWithPendingCancel(t, stor, client)

	driveQueuedApply(t, stor, client, apply.ApplyIdentifier)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Completed, settled.State,
		"the apply must adopt the engine's completed outcome, not cancelled")
	assert.NotNil(t, settled.CompletedAt, "a completed apply must carry its completion time")

	settledTasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, settledTasks, 1)
	assert.Equal(t, state.Task.Completed, settledTasks[0].State,
		"the task must adopt the engine's completed outcome, not cancelled")
	assert.Equal(t, 100, settledTasks[0].ProgressPercent, "a completed task reports full progress")
	assert.NotNil(t, settledTasks[0].CompletedAt, "a completed task must carry its completion time")

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestCompleted)
	recorded := eng.recorded()
	assert.Contains(t, recorded, "Progress", "the drive must read the engine's authoritative state")
	assert.NotContains(t, recorded, "Cancel", "the doomed cancel must never reach the engine")

	reclaimed, err := stor.Applies().ClaimApplyByID(ctx, apply.ID, "test-reclaim-"+t.Name())
	require.NoError(t, err)
	assert.Nil(t, reclaimed, "a completed apply must not be claimable again")
}

// The counterpart: the engine's backend still reports the change live when the
// drive claims the apply. The pending cancel must be consumed exactly as
// always — the engine's cancel is invoked and the apply settles cancelled.
func TestLocalClient_DriveConsumesCancelWhenEngineReportsLiveWork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, eng := newAuthoritativeControlClient(t, dsn, stor, engine.StateRunning, nil)

	apply := dispatchApplyAwaitingDeployWithPendingCancel(t, stor, client)

	driveQueuedApply(t, stor, client, apply.ApplyIdentifier)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Cancelled, settled.State,
		"a live backend change must be cancelled by the pending command")

	settledTasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, settledTasks, 1)
	assert.Equal(t, state.Task.Cancelled, settledTasks[0].State)

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestCompleted)
	recorded := eng.recorded()
	assert.Contains(t, recorded, "Progress", "the drive must read the engine's authoritative state first")
	assert.Contains(t, recorded, "Cancel", "a live change must receive the cancel command")
}

// When the engine's authoritative state cannot be read, the drive must fail
// toward consuming the pending cancel — never toward skipping it. An
// unreadable backend must not delay a kill command that may still be able to
// act.
func TestLocalClient_DriveConsumesCancelWhenEngineProgressUnreadable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, eng := newAuthoritativeControlClient(t, dsn, stor, "", errors.New("backend api unavailable"))

	apply := dispatchApplyAwaitingDeployWithPendingCancel(t, stor, client)

	driveQueuedApply(t, stor, client, apply.ApplyIdentifier)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Cancelled, settled.State,
		"an unreadable backend must not delay the pending cancel")

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestCompleted)
	recorded := eng.recorded()
	assert.Contains(t, recorded, "Progress", "the drive must attempt the authoritative read")
	assert.Contains(t, recorded, "Cancel", "the cancel must be consumed despite the failed read")
}
