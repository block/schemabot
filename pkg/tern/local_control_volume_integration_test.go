//go:build integration

package tern

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
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

// volumeTrackingEngine models a schema change that stays running until the
// driver delivers a volume adjustment, then completes. It records every
// Volume call so a test can prove which engine instance the driver retuned
// and with which level.
type volumeTrackingEngine struct {
	engine.Engine

	mu             sync.Mutex
	volumeLevels   []int32
	volumeAttempts int
	volumeErr      error
}

func (e *volumeTrackingEngine) Name() string { return "volume-tracking" }

func (e *volumeTrackingEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	return &engine.PlanResult{}, nil
}

func (e *volumeTrackingEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	return &engine.ApplyResult{Accepted: true}, nil
}

// Progress keeps the schema change observable mid-flight until a volume
// adjustment has been attempted, so the drive's progress ticks are guaranteed
// to see the pending request before the apply settles.
func (e *volumeTrackingEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.volumeAttempts == 0 {
		return &engine.ProgressResult{State: engine.StateRunning}, nil
	}
	return &engine.ProgressResult{State: engine.StateCompleted}, nil
}

func (e *volumeTrackingEngine) Volume(_ context.Context, req *engine.VolumeRequest) (*engine.VolumeResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.volumeAttempts++
	if e.volumeErr != nil {
		return nil, e.volumeErr
	}
	e.volumeLevels = append(e.volumeLevels, req.Volume)
	return &engine.VolumeResult{Accepted: true, PreviousVolume: 4, NewVolume: req.Volume}, nil
}

func (e *volumeTrackingEngine) recordedVolumes() []int32 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]int32(nil), e.volumeLevels...)
}

// newVolumeControlClient builds a MySQL-type LocalClient backed by the shared
// container with a volume-tracking engine installed.
func newVolumeControlClient(t *testing.T, dsn string, stor storage.Storage) (*LocalClient, *volumeTrackingEngine) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })
	eng := &volumeTrackingEngine{}
	client.spiritEngine = eng
	return client, eng
}

// dispatchQueuedApplyWithOptions dispatches an apply with one table change and
// the given apply options through LocalClient.Apply, returning the stored
// apply row, still queued for the operator.
func dispatchQueuedApplyWithOptions(t *testing.T, stor storage.Storage, client *LocalClient, options map[string]string) *storage.Apply {
	t.Helper()
	ctx := t.Context()
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-volume-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {Tables: []storage.TableChange{{
				Namespace: "testdb",
				Table:     "users",
				DDL:       "ALTER TABLE `users` ADD COLUMN volume_note VARCHAR(255)",
				Operation: "alter",
			}}},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	resp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Database:    "testdb",
		Type:        storage.DatabaseTypeMySQL,
		Environment: localClientTestEnvironment,
		Options:     options,
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, state.Apply.Pending, apply.State, "a dispatched apply must be queued, not driven inline")
	return apply
}

// requireStoredVolume asserts the apply's stored options carry the given
// volume level — the value progress and the CLI display.
func requireStoredVolume(t *testing.T, stor storage.Storage, applyID int64, want int) {
	t.Helper()
	stored, err := stor.Applies().Get(t.Context(), applyID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, want, stored.GetOptions().Volume, "stored apply options must carry the applied volume")
}

// A volume adjustment is a durable control request: it can be issued against
// any client instance sharing the route's storage — not just the instance that
// ends up driving the apply — and the driver delivers it to its own engine at
// a progress tick. This exercises the sequential (per-task) drive path: the
// request is queued through one client, a different client drives the apply,
// and only the driving client's engine is retuned. The request completes and
// the applied level lands on the stored apply options so progress reports it.
func TestLocalClient_VolumeQueuedCrossInstanceAppliedBySequentialDrive(t *testing.T) {
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
	drivingClient, drivingEngine := newVolumeControlClient(t, dsn, stor)
	receivingClient, receivingEngine := newVolumeControlClient(t, dsn, stor)

	apply := dispatchQueuedApplyWithOptions(t, stor, drivingClient, nil)

	// The RPC lands on an instance that will never drive this apply.
	volumeResp, err := receivingClient.Volume(ctx, &ternv1.VolumeRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
		Volume:      9,
	})
	require.NoError(t, err)
	assert.True(t, volumeResp.Accepted)
	assert.Equal(t, int32(9), volumeResp.NewVolume, "the accepted response must echo the queued target level")
	assert.Zero(t, volumeResp.PreviousVolume, "the previous level is only known to the driver")
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationVolume, storage.ControlRequestPending)

	// Re-requesting the same level while pending is an idempotent accept.
	again, err := receivingClient.Volume(ctx, &ternv1.VolumeRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
		Volume:      9,
	})
	require.NoError(t, err)
	assert.True(t, again.Accepted)

	// A different level while one is pending is rejected with retry guidance,
	// so the level the driver reads, applies, and completes stays unambiguous.
	// The rejection travels as an unaccepted response, not an error, so callers
	// across the gRPC boundary can tell a conflict from a transport failure.
	conflicting, err := receivingClient.Volume(ctx, &ternv1.VolumeRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
		Volume:      3,
	})
	require.NoError(t, err)
	assert.False(t, conflicting.Accepted, "a conflicting level must not be accepted")
	assert.Contains(t, conflicting.ErrorMessage, "already queued")

	driveNextQueuedApply(t, stor, drivingClient)

	assert.Equal(t, []int32{9}, drivingEngine.recordedVolumes(),
		"the driving instance's engine must receive the queued level exactly once")
	assert.Empty(t, receivingEngine.recordedVolumes(),
		"the instance that accepted the RPC must never touch its own engine")
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationVolume, storage.ControlRequestCompleted)
	requireStoredVolume(t, stor, apply.ID, 9)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Completed, settled.State)
}

// The grouped (atomic) drive path services a pending volume request from its
// progress tick just like the sequential path. A MySQL apply with
// defer_cutover drives through the grouped poll, so a volume queued before the
// claim must reach the engine, complete the request, and land on the stored
// apply options.
func TestLocalClient_VolumeQueuedAppliedByGroupedDrive(t *testing.T) {
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
	client, eng := newVolumeControlClient(t, dsn, stor)

	apply := dispatchQueuedApplyWithOptions(t, stor, client, map[string]string{"defer_cutover": "true"})

	volumeResp, err := client.Volume(ctx, &ternv1.VolumeRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
		Volume:      2,
	})
	require.NoError(t, err)
	assert.True(t, volumeResp.Accepted)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationVolume, storage.ControlRequestPending)

	driveNextQueuedApply(t, stor, client)

	assert.Equal(t, []int32{2}, eng.recordedVolumes(),
		"the grouped drive must deliver the queued level to the engine exactly once")
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationVolume, storage.ControlRequestCompleted)
	requireStoredVolume(t, stor, apply.ID, 2)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Completed, settled.State)
}

// Volume is a tuning knob, not a safety operation: when the engine rejects the
// adjustment the request fails terminally with the engine's error, the copy
// continues at its current volume, and the drive itself still runs to
// completion.
func TestLocalClient_VolumeEngineFailureFailsRequestWithoutAbortingDrive(t *testing.T) {
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
	client, eng := newVolumeControlClient(t, dsn, stor)
	eng.volumeErr = fmt.Errorf("throttle service unavailable")

	apply := dispatchQueuedApplyWithOptions(t, stor, client, nil)

	volumeResp, err := client.Volume(ctx, &ternv1.VolumeRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
		Volume:      7,
	})
	require.NoError(t, err)
	assert.True(t, volumeResp.Accepted)

	driveNextQueuedApply(t, stor, client)

	req, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationVolume)
	require.NoError(t, err)
	require.NotNil(t, req)
	assert.Equal(t, storage.ControlRequestFailed, req.Status,
		"an engine rejection must fail the request terminally so the driver stops retrying it")
	assert.Contains(t, req.ErrorMessage, "throttle service unavailable")
	requireStoredVolume(t, stor, apply.ID, 0)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Completed, settled.State,
		"a failed volume adjustment must never abort the drive")
}

// A terminal apply has no running copy to retune, so a volume request against
// it is rejected synchronously instead of queueing work no driver will ever
// service. An out-of-range level is rejected before anything is recorded.
func TestLocalClient_VolumeRejectsTerminalApplyAndInvalidLevel(t *testing.T) {
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
	// The invocation-tracking engine completes on its first progress poll, so
	// the apply settles without needing a volume adjustment to unblock it.
	client, _ := newTasklessControlClient(t, dsn, stor)

	apply := dispatchQueuedApply(t, stor, client, []storage.TableChange{{
		Namespace: "testdb",
		Table:     "users",
		DDL:       "ALTER TABLE `users` ADD COLUMN volume_note VARCHAR(255)",
		Operation: "alter",
	}})

	for _, level := range []int32{0, 12} {
		_, err := client.Volume(ctx, &ternv1.VolumeRequest{
			ApplyId:     apply.ApplyIdentifier,
			Environment: localClientTestEnvironment,
			Volume:      level,
		})
		require.Error(t, err, "volume level %d must be rejected", level)
		assert.Contains(t, err.Error(), "volume must be between")
	}

	driveNextQueuedApply(t, stor, client)
	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	require.Equal(t, state.Apply.Completed, settled.State)

	rejected, err := client.Volume(ctx, &ternv1.VolumeRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
		Volume:      5,
	})
	require.NoError(t, err)
	assert.False(t, rejected.Accepted, "a terminal apply must not accept a volume adjustment")
	assert.Contains(t, rejected.ErrorMessage, "volume can only be adjusted while it is running")

	req, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationVolume)
	require.NoError(t, err)
	assert.Nil(t, req, "a rejected volume request must record nothing")
}

// insertSiblingOperation adds a second apply_operation row for a different
// deployment, giving the apply the multi-deployment fan-out shape where each
// operation's driver holds its own engine.
func insertSiblingOperation(t *testing.T, stor storage.Storage, applyID int64) {
	t.Helper()
	_, err := stor.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{
		ApplyID:    applyID,
		Deployment: "testdb-sibling",
		Target:     "testdb-sibling",
		State:      state.ApplyOperation.Pending,
	})
	require.NoError(t, err)
}

// A multi-deployment (fan-out) apply runs one engine per operation, so an
// apply-scoped volume request could only ever retune the first operation
// driver's deployment while reporting the adjustment as applied everywhere.
// The request is rejected at queue time with nothing recorded, and a request
// that reached storage anyway is failed by the driver without retuning any
// engine.
func TestLocalClient_VolumeRejectsMultiDeploymentApply(t *testing.T) {
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
	client, eng := newVolumeControlClient(t, dsn, stor)

	apply := dispatchQueuedApplyWithOptions(t, stor, client, nil)
	insertSiblingOperation(t, stor, apply.ID)

	rejected, err := client.Volume(ctx, &ternv1.VolumeRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
		Volume:      9,
	})
	require.NoError(t, err)
	assert.False(t, rejected.Accepted, "a fan-out apply must not accept an apply-scoped volume adjustment")
	assert.Contains(t, rejected.ErrorMessage, "multi-deployment")

	req, err := stor.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationVolume)
	require.NoError(t, err)
	assert.Nil(t, req, "a queue-time rejection must record nothing")

	// A request that reached storage anyway (recorded before fan-out gating
	// could see the sibling operation) is failed by the driver's tick without
	// retuning any engine.
	metadata, err := storage.EncodeVolumeControlRequestMetadata(9)
	require.NoError(t, err)
	_, _, err = stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationVolume,
		Status:      storage.ControlRequestPending,
		RequestedBy: "test",
		Metadata:    metadata,
	})
	require.NoError(t, err)

	require.NoError(t, client.processPendingVolumeControlRequest(ctx, apply, eng, nil, nil))
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationVolume, storage.ControlRequestFailed)
	assert.Empty(t, eng.recordedVolumes(), "no engine may be retuned for a fan-out apply")
}

// A volume request races the drive settling: the state check passes while the
// apply is still running, the driver settles the apply and runs its terminal
// pending-request sweep, and only then does the request row land. The queue
// path re-checks after the insert, sweeps its own request, and rejects — so no
// pending row lingers forever with no driver left to service it.
func TestLocalClient_VolumeQueuedAfterSettleIsSweptAndRejected(t *testing.T) {
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
	client, eng := newVolumeControlClient(t, dsn, stor)

	apply := dispatchQueuedApplyWithOptions(t, stor, client, nil)

	// Nudge the tracking engine past its running phase so the drive settles
	// without needing a queued volume adjustment.
	eng.mu.Lock()
	eng.volumeAttempts = 1
	eng.mu.Unlock()
	driveNextQueuedApply(t, stor, client)

	// The dispatch-time apply snapshot still reads as non-terminal, modelling a
	// state check that passed just before the driver settled the apply.
	rejected, err := client.queueVolumeRequest(ctx, apply, 5)
	require.NoError(t, err)
	assert.False(t, rejected.Accepted, "a request that raced the settle must not be accepted")
	assert.Contains(t, rejected.ErrorMessage, "volume can only be adjusted while it is running")

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationVolume, storage.ControlRequestCompleted)
	assert.Empty(t, eng.recordedVolumes(), "a swept request must never reach an engine")
}

// Engine volume lives with each running schema change, so on a sequential
// multi-table apply a task started after the adjustment would run at the
// engine default while stored options, progress, and the CLI report the
// adjusted level. Each newly started task converges to the apply's stored
// level, so the display stays true for the whole drive.
func TestLocalClient_VolumeConvergesLaterTasksToStoredLevel(t *testing.T) {
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
	client, eng := newVolumeControlClient(t, dsn, stor)

	apply := dispatchQueuedApply(t, stor, client, []storage.TableChange{
		{
			Namespace: "testdb",
			Table:     "users",
			DDL:       "ALTER TABLE `users` ADD COLUMN volume_note VARCHAR(255)",
			Operation: "alter",
		},
		{
			Namespace: "testdb",
			Table:     "orders",
			DDL:       "ALTER TABLE `orders` ADD COLUMN volume_note VARCHAR(255)",
			Operation: "alter",
		},
	})

	volumeResp, err := client.Volume(ctx, &ternv1.VolumeRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
		Volume:      9,
	})
	require.NoError(t, err)
	require.True(t, volumeResp.Accepted)

	driveNextQueuedApply(t, stor, client)

	assert.Equal(t, []int32{9, 9}, eng.recordedVolumes(),
		"the first task is retuned by the pending request and the second converges to the stored level at start")
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationVolume, storage.ControlRequestCompleted)
	requireStoredVolume(t, stor, apply.ID, 9)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Completed, settled.State)
}
