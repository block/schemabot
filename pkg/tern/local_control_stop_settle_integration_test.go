//go:build integration

package tern

import (
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A durable stop can land on an apply whose engine no longer tracks any work
// for the task — after a driver crash and restart, the engine stop fails while
// the persisted checkpoint already holds the resume point. The driver consuming
// the pending stop must complete it durably: the task and apply settle to
// stopped (resumable, no completion time), the request resolves, and a later
// start can resume from the checkpoint. Surfacing the engine error instead
// would abort the drive and re-run the same failing stop on every claim.
func TestLocalClient_PendingStopCompletesWhenEngineHasNoLiveWork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)
	eng := &controlCaptureEngine{
		stopErr:        engine.NewPermanentError("no active schema change to stop"),
		progressResult: &engine.ProgressResult{State: engine.StatePending, Message: "No active schema change"},
	}
	client.spiritEngine = eng

	apply, _ := seedRunningApplyWithTask(t, stor, storage.DatabaseTypeMySQL, storage.EngineSpirit)

	stopResp, err := client.Stop(ctx, &ternv1.StopRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, stopResp.Accepted)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationStop, storage.ControlRequestPending)

	expireApplyLease(t, dsn, apply.ID)
	claimed := claimApplyForDrive(t, stor, apply.ID)
	driveCtx := storage.WithApplyLease(ctx, claimed.Lease())

	handled, err := client.processPendingStopControlRequest(driveCtx, claimed)
	require.NoError(t, err, "a stop with no live engine work must complete, not abort the drive")
	assert.True(t, handled, "the pending stop must be consumed by the drive")
	require.NotNil(t, eng.stopReq, "the drive must attempt the engine stop before deciding it has no live work")
	require.NotNil(t, eng.progressReq, "the live-work probe must ask the engine before completing the stop")

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Stopped, settled.State)
	assert.Nil(t, settled.CompletedAt, "a stopped apply is resumable and must not carry a completion time")

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, state.Task.Stopped, tasks[0].State, "the task must settle to stopped with the durable stop")

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationStop, storage.ControlRequestCompleted)

	handled, err = client.processPendingStopControlRequest(driveCtx, settled)
	require.NoError(t, err)
	assert.False(t, handled, "a completed stop request must not be re-consumed on a later claim")
}
