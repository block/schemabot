//go:build integration

package tern

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// seedRunningApplyWithTask stores a running apply with one running task linked
// to a single apply operation, and returns the stored apply and operation. The
// rows go through real storage so the control paths under test exercise the
// same reads and lease-guarded writes a production drive does.
func seedRunningApplyWithTask(t *testing.T, stor storage.Storage, databaseType, engineName string) (*storage.Apply, *storage.ApplyOperation) {
	t.Helper()
	ctx := t.Context()
	now := time.Now()
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-cancel-settle-%d", now.UnixNano()),
		Database:       "testdb",
		DatabaseType:   databaseType,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {Tables: []storage.TableChange{{
				Namespace: "testdb",
				Table:     "users",
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
				Operation: "alter",
			}}},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-cancel-settle-%d", now.UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    databaseType,
		Deployment:      "testdb",
		Engine:          engineName,
		State:           state.Apply.Running,
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	task := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-cancel-settle-%d", now.UnixNano()),
		PlanID:         planID,
		Database:       "testdb",
		DatabaseType:   databaseType,
		Engine:         engineName,
		State:          state.Task.Running,
		Namespace:      "testdb",
		TableName:      "users",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		DDLAction:      "alter",
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	operation := &storage.ApplyOperation{
		Deployment: "testdb",
		State:      state.ApplyOperation.Running,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	applyID, err := stor.Applies().CreateWithTasksAndOperations(ctx, apply, []*storage.Task{task}, []*storage.ApplyOperation{operation})
	require.NoError(t, err)
	apply.ID = applyID
	return apply, operation
}

// expireApplyLease backdates the apply's heartbeat so a running apply reads as
// lease-expired and becomes reclaimable, standing in for the driver crash that
// hands the apply to a fresh claim in production.
func expireApplyLease(t *testing.T, dsn string, applyID int64) {
	t.Helper()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "open database to expire the apply lease")
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(t.Context(),
		"UPDATE applies SET updated_at = updated_at - INTERVAL ? MICROSECOND WHERE id = ?",
		2*storage.ApplyLeaseStaleAfter.Microseconds(), applyID)
	require.NoError(t, err, "backdate the apply heartbeat")
}

// claimApplyForDrive claims the apply the way an operator driver does and
// returns the claimed row, whose lease authorizes the drive's storage writes.
func claimApplyForDrive(t *testing.T, stor storage.Storage, applyID int64) *storage.Apply {
	t.Helper()
	ctx := t.Context()
	owner := "test-driver-" + t.Name()
	var claimed *storage.Apply
	require.Eventually(t, func() bool {
		var err error
		claimed, err = stor.Applies().ClaimApplyByID(ctx, applyID, owner)
		return err == nil && claimed != nil
	}, 10*time.Second, 50*time.Millisecond, "the apply never became claimable")
	return claimed
}

// A durable cancel can land on an apply whose engine no longer tracks any work
// for the task — the engine cancel fails, but there is nothing left to stop.
// The driver consuming the pending cancel must complete it durably: the task
// and apply settle to cancelled, the request resolves, and the apply is never
// re-claimed. Surfacing the engine error instead would abort the drive and
// re-run the same failing cancel on every claim.
func TestLocalClient_PendingCancelCompletesWhenEngineHasNoLiveWork(t *testing.T) {
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
		cancelErr:      engine.NewPermanentError("no active schema change to cancel"),
		progressResult: &engine.ProgressResult{State: engine.StatePending, Message: "No active schema change"},
	}
	client.spiritEngine = eng

	apply, _ := seedRunningApplyWithTask(t, stor, storage.DatabaseTypeMySQL, storage.EngineSpirit)

	cancelResp, err := client.Cancel(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, cancelResp.Accepted)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestPending)

	expireApplyLease(t, dsn, apply.ID)
	claimed := claimApplyForDrive(t, stor, apply.ID)
	driveCtx := storage.WithApplyLease(ctx, claimed.Lease())

	handled, err := client.processPendingCancelControlRequest(driveCtx, claimed)
	require.NoError(t, err, "a cancel with no live engine work must complete, not abort the drive")
	assert.True(t, handled, "the pending cancel must be consumed by the drive")
	require.NotNil(t, eng.cancelReq, "the drive must attempt the engine cancel before deciding it has no live work")
	require.NotNil(t, eng.progressReq, "the live-work probe must ask the engine before completing the cancel")

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Cancelled, settled.State)
	assert.NotNil(t, settled.CompletedAt, "a cancelled apply must carry its completion time")

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, state.Task.Cancelled, tasks[0].State, "the task must terminalize with the durable cancel")

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestCompleted)

	handled, err = client.processPendingCancelControlRequest(driveCtx, settled)
	require.NoError(t, err)
	assert.False(t, handled, "a completed cancel request must not be re-consumed on a later claim")

	reclaimed, err := stor.Applies().ClaimApplyByID(ctx, apply.ID, "test-reclaim-"+t.Name())
	require.NoError(t, err)
	assert.Nil(t, reclaimed, "a cancelled apply must not be claimable again")
}

// A database type without a built-in engine addresses remote work through
// persisted engine resume state, the same way Vitess does. When a durable
// cancel is consumed for such a type, the control request handed to the engine
// must carry the resume state stored for the task's apply operation — loaded
// through real storage — so the engine can reach the work it is asked to
// terminate.
func TestLocalClient_PendingCancelCarriesStoredResumeStateForCustomEngineType(t *testing.T) {
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

	eng := &controlValidatingCaptureEngine{}
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeStrata,
		TargetDSN: dsn,
		EngineFactories: map[string]EngineFactory{
			storage.DatabaseTypeStrata: func(LocalConfig, *slog.Logger) (engine.Engine, error) {
				return eng, nil
			},
		},
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	apply, operation := seedRunningApplyWithTask(t, stor, storage.DatabaseTypeStrata, storage.EngineStrata)

	persistedMetadata := `{"change_id":"321"}`
	require.NoError(t, stor.ApplyOperations().SaveEngineResumeState(ctx, operation.ID, &storage.EngineResumeState{
		ApplyOperationID: operation.ID,
		MigrationContext: apply.ApplyIdentifier,
		Metadata:         persistedMetadata,
	}))

	cancelResp, err := client.Cancel(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, cancelResp.Accepted)

	expireApplyLease(t, dsn, apply.ID)
	claimed := claimApplyForDrive(t, stor, apply.ID)
	driveCtx := storage.WithApplyLease(ctx, claimed.Lease())

	handled, err := client.processPendingCancelControlRequest(driveCtx, claimed)
	require.NoError(t, err)
	assert.True(t, handled)

	require.NotNil(t, eng.cancelReq, "the drive must deliver the cancel to the engine")
	require.NotNil(t, eng.cancelReq.ResumeState, "the cancel request must carry the stored engine resume state")
	assert.Equal(t, apply.ApplyIdentifier, eng.cancelReq.ResumeState.MigrationContext)
	assert.JSONEq(t, persistedMetadata, eng.cancelReq.ResumeState.Metadata)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Cancelled, settled.State)

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, state.Task.Cancelled, tasks[0].State)

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestCompleted)
}
