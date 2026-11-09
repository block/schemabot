//go:build integration

package tern

import (
	"context"
	"database/sql"
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
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// finalizerControlEngine records every Apply a group_finalizer drive hands it,
// so a test can prove whether the drive published the namespace's VSchema.
type finalizerControlEngine struct {
	engine.Engine

	mu       sync.Mutex
	requests []*engine.ApplyRequest
	// applyErrs holds the error each Apply call returns, in call order, until
	// it runs out; later calls succeed.
	applyErrs []error
	// applyResult, when set, is what a call that returns no error reports.
	applyResult *engine.ApplyResult
	// applyHook, when set, runs on every Apply call with the drive's context
	// before applyErrs is consulted; a non-nil error it returns ends the call.
	applyHook func(context.Context) error
	// progressState, when set, is the terminal state Progress reports.
	progressState engine.State
	// progressErr, when set, is the error every Progress call returns.
	progressErr error
}

func (e *finalizerControlEngine) Name() string { return "finalizer-control" }

func (e *finalizerControlEngine) Apply(ctx context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.requests = append(e.requests, req)
	if e.applyHook != nil {
		if err := e.applyHook(ctx); err != nil {
			return nil, err
		}
	}
	if len(e.applyErrs) > 0 {
		err := e.applyErrs[0]
		e.applyErrs = e.applyErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if e.applyResult != nil {
		return e.applyResult, nil
	}
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *finalizerControlEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.progressErr != nil {
		return nil, e.progressErr
	}
	if e.progressState != "" {
		return &engine.ProgressResult{State: e.progressState}, nil
	}
	return &engine.ProgressResult{State: engine.StateCompleted, Progress: 100}, nil
}

func (e *finalizerControlEngine) applies() []*engine.ApplyRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]*engine.ApplyRequest(nil), e.requests...)
}

// cleanupApplyOperations removes the operations earlier tests left behind. The
// shared cleanup removes their applies but not their operations, and a test
// that claims an operation through the store would otherwise be handed a
// leftover row instead of its own.
func cleanupApplyOperations(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "failed to open database for apply_operations cleanup")
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(t.Context(), "DELETE FROM apply_operations")
	require.NoError(t, err, "failed to delete leftover apply_operations")
}

// finalizerControlFixture is a two-namespace sharded rollout on one deployment:
// ns_0 and ns_1 each carry shard work and a VSchema change, so each has a work
// operation and a task-less group_finalizer. The ns_0 finalizer is leased to
// the drive under test, the way the operator's operation claim leaves it.
type finalizerControlFixture struct {
	stor        storage.Storage
	client      *LocalClient
	eng         *finalizerControlEngine
	apply       *storage.Apply
	finalizerID int64
	opCtx       context.Context
	leaseDB     *sql.DB
}

const finalizerControlDDL = "ALTER TABLE `orders` ADD COLUMN `region` VARCHAR(32)"

// finalizerControlOptions varies the rollout a finalizer control test seeds.
type finalizerControlOptions struct {
	// databaseType is the rollout's database type; empty means Strata.
	databaseType string
	// wrapStorage, when set, wraps the storage the client under test drives
	// through, so a test can interleave a concurrent writer with the drive.
	wrapStorage func(storage.Storage) storage.Storage
}

// newFinalizerControlFixture seeds a Strata rollout with the ns_0 finalizer in
// finalizerState, and returns a context carrying that finalizer's operation
// lease alone.
func newFinalizerControlFixture(t *testing.T, finalizerState string) *finalizerControlFixture {
	t.Helper()
	return newFinalizerControlFixtureWith(t, finalizerState, finalizerControlOptions{})
}

func newFinalizerControlFixtureWith(t *testing.T, finalizerState string, opts finalizerControlOptions) *finalizerControlFixture {
	t.Helper()
	databaseType := opts.databaseType
	if databaseType == "" {
		databaseType = storage.DatabaseTypeStrata
	}
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupApplyOperations(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	t.Cleanup(func() { utils.CloseAndLog(stor) })
	clientStor := stor
	if opts.wrapStorage != nil {
		clientStor = opts.wrapStorage(stor)
	}

	eng := &finalizerControlEngine{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := NewLocalClient(LocalConfig{
		Database:  "orders",
		Type:      databaseType,
		TargetDSN: dsn,
		EngineFactories: map[string]EngineFactory{
			databaseType: func(LocalConfig, *slog.Logger) (engine.Engine, error) {
				return eng, nil
			},
		},
	}, clientStor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })
	// Vitess drives through the built-in PlanetScale engine rather than a
	// registered factory, so the recording engine replaces it directly.
	if databaseType == storage.DatabaseTypeVitess {
		client.planetscaleEngine = eng
	}

	now := time.Now()
	namespace := func(ns string) *storage.NamespacePlanData {
		return &storage.NamespacePlanData{
			Tables: []storage.TableChange{{
				Namespace: ns, Table: "orders", DDL: finalizerControlDDL, Operation: "alter",
			}},
			Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`},
			Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
		}
	}
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-finalizer-control-%d", now.UnixNano()),
		Database:       "orders",
		DatabaseType:   databaseType,
		Deployment:     "orders",
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		Namespaces: map[string]*storage.NamespacePlanData{
			"ns_0": namespace("ns_0"),
			"ns_1": namespace("ns_1"),
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-finalizer-control-%d", now.UnixNano()),
		PlanID:          planID,
		Database:        "orders",
		DatabaseType:    databaseType,
		Deployment:      "orders",
		Environment:     localClientTestEnvironment,
		State:           state.Apply.Running,
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	insert := func(key, kind, opState string) int64 {
		id, err := stor.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID:       applyID,
			Deployment:    "orders",
			Target:        "orders",
			OperationKey:  key,
			OperationKind: kind,
			State:         opState,
		})
		require.NoError(t, err, "seed apply_operation %s", key)
		return id
	}
	insert("ns_0", storage.ApplyOperationKindWork, state.ApplyOperation.Stopped)
	insert("ns_1", storage.ApplyOperationKindWork, state.ApplyOperation.Running)
	finalizerID := insert("ns_0/group_finalizer", storage.ApplyOperationKindGroupFinalizer, finalizerState)
	insert("ns_1/group_finalizer", storage.ApplyOperationKindGroupFinalizer, state.ApplyOperation.Stopped)

	leaseDB, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(leaseDB) })
	require.NoError(t, leaseDB.PingContext(ctx))
	_, err = leaseDB.ExecContext(ctx, `
		UPDATE apply_operations SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW() WHERE id = ?
	`, "finalizer-driver", "finalizer-token", finalizerID)
	require.NoError(t, err)

	opCtx := storage.WithOperationLease(ctx, storage.OperationLease{
		ApplyID: applyID, OperationID: finalizerID, Owner: "finalizer-driver", Token: "finalizer-token",
	})
	return &finalizerControlFixture{
		stor: stor, client: client, eng: eng, apply: apply,
		finalizerID: finalizerID, opCtx: opCtx, leaseDB: leaseDB,
	}
}

func (f *finalizerControlFixture) requestPending(t *testing.T, operation storage.ControlOperation) {
	t.Helper()
	_, alreadyPending, err := f.stor.ControlRequests().RequestPending(t.Context(), &storage.ApplyControlRequest{
		ApplyID:     f.apply.ID,
		Operation:   operation,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
}

func (f *finalizerControlFixture) finalizerState(t *testing.T) string {
	t.Helper()
	op, err := f.stor.ApplyOperations().Get(t.Context(), f.finalizerID)
	require.NoError(t, err)
	require.NotNil(t, op)
	return op.State
}

// requireRequestPending asserts the operator's command is still pending: the
// finalizer owns only its own row, so completing the apply-level request is the
// rollout projection's job once every sibling has settled.
func (f *finalizerControlFixture) requireRequestPending(t *testing.T, operation storage.ControlOperation) {
	t.Helper()
	req, err := f.stor.ControlRequests().GetPending(t.Context(), f.apply.ID, operation)
	require.NoError(t, err)
	assert.NotNil(t, req, "the pending %s request is the projection's to complete", operation)
}

func (f *finalizerControlFixture) requireParentUntouched(t *testing.T) {
	t.Helper()
	parent, err := f.stor.Applies().Get(t.Context(), f.apply.ID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, state.Apply.Running, parent.State,
		"the parent is owned by the rollout projection, not by a finalizer drive")
}

// A sharded rollout is stopped with ns_0's shard work unfinished, which leaves
// ns_0's never-started finalizer stopped. The operator then cancels, and the
// cancel claim hands the stopped finalizer to a drive. The drive must discard
// the finalizer: publishing ns_0's VSchema would expose a VSchema for shard
// work that never landed, on a rollout the operator asked to throw away.
func TestLocalClient_StoppedFinalizerWithPendingCancelSettlesCancelledWithoutApplying(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Resuming)
	f.requestPending(t, storage.ControlOperationCancel)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	assert.Empty(t, f.eng.applies(), "a cancelled finalizer must never hand its VSchema to the engine")
	assert.Equal(t, state.ApplyOperation.Cancelled, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationCancel)
	f.requireParentUntouched(t)
}

// The cutover drive reaches the same finalizer drive, so it honors a pending
// cancel the same way.
func TestLocalClient_FinalizerCutoverDriveWithPendingCancelSettlesCancelledWithoutApplying(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	f.requestPending(t, storage.ControlOperationCancel)

	require.NoError(t, f.client.ResumeApplyOperationCutover(f.opCtx, f.apply, f.finalizerID))

	assert.Empty(t, f.eng.applies(), "a cancelled finalizer must never hand its VSchema to the engine")
	assert.Equal(t, state.ApplyOperation.Cancelled, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationCancel)
	f.requireParentUntouched(t)
}

// A stop lands after a driver claimed ns_0's finalizer but before its drive
// began. The finalizer must not start: it settles stopped, like every other
// never-started operation of a stopped rollout, so a later start resumes it.
func TestLocalClient_FinalizerWithPendingStopSettlesStoppedWithoutApplying(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	f.requestPending(t, storage.ControlOperationStop)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	assert.Empty(t, f.eng.applies(), "a finalizer must not start while a stop is pending")
	assert.Equal(t, state.ApplyOperation.Stopped, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationStop)
	f.requireParentUntouched(t)
}

// With no command pending, the finalizer publishes its namespace's VSchema and
// completes.
func TestLocalClient_FinalizerWithoutPendingCommandAppliesVSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	applies := f.eng.applies()
	require.Len(t, applies, 1)
	assert.Equal(t, f.apply.ApplyIdentifier, applies[0].ApplyIdentifier, "the engine learns which apply the finalizer belongs to")
	require.Len(t, applies[0].Changes, 1)
	assert.Equal(t, "ns_0", applies[0].Changes[0].Namespace)
	assert.Equal(t, "true", applies[0].Changes[0].Metadata[storage.PlanMetadataVSchemaChanged])
	assert.Equal(t, state.ApplyOperation.Completed, f.finalizerState(t))
}

// A finalizer whose VSchema deploy is already in flight on the engine has
// recorded engine resume state. A pending cancel cannot unpublish work the
// engine is carrying out, so the drive reattaches to it and records the
// outcome the engine reports rather than settling storage as cancelled over a
// deploy that is still landing.
func TestLocalClient_InFlightFinalizerWithPendingCancelReattachesToEngineWork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	require.NoError(t, f.stor.ApplyOperations().SaveEngineResumeState(f.opCtx, f.finalizerID, &storage.EngineResumeState{
		ApplyOperationID: f.finalizerID,
		MigrationContext: "deploy-ns-0",
	}))
	f.requestPending(t, storage.ControlOperationCancel)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	applies := f.eng.applies()
	require.Len(t, applies, 1, "the drive reattaches to the in-flight deploy")
	require.NotNil(t, applies[0].ResumeState)
	assert.Equal(t, "deploy-ns-0", applies[0].ResumeState.MigrationContext)
	assert.Equal(t, state.ApplyOperation.Completed, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationCancel)
}

// On a database type whose stop cannot pause, a stop that lands before ns_0's
// finalizer reaches the engine settles the finalizer cancelled, the single
// stop-terminality outcome for that type, rather than a resumable stopped row
// a later start could never honor.
func TestLocalClient_FinalizerWithPendingStopSettlesCancelledWhereStopCannotPause(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixtureWith(t, state.ApplyOperation.Running, finalizerControlOptions{
		databaseType: storage.DatabaseTypeVitess,
	})
	f.requestPending(t, storage.ControlOperationStop)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	assert.Empty(t, f.eng.applies(), "a finalizer must not start while a stop is pending")
	assert.Equal(t, state.ApplyOperation.Cancelled, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationStop)
	f.requireParentUntouched(t)
}

// cancelOnStartStorage commits an operator's cancel at the moment the drive
// marks its operation started, the latest a command can land before the
// finalizer reaches the engine.
type cancelOnStartStorage struct {
	storage.Storage
	apply *storage.Apply
}

func (s *cancelOnStartStorage) ApplyOperations() storage.ApplyOperationStore {
	return &cancelOnStartOperations{ApplyOperationStore: s.Storage.ApplyOperations(), parent: s}
}

type cancelOnStartOperations struct {
	storage.ApplyOperationStore
	parent *cancelOnStartStorage
}

func (o *cancelOnStartOperations) MarkStarted(ctx context.Context, id int64) error {
	_, alreadyPending, err := o.parent.Storage.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     o.parent.apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	if err != nil {
		return fmt.Errorf("record concurrent cancel for apply %s: %w", o.parent.apply.ApplyIdentifier, err)
	}
	if alreadyPending {
		return fmt.Errorf("concurrent cancel for apply %s was already pending", o.parent.apply.ApplyIdentifier)
	}
	return o.ApplyOperationStore.MarkStarted(ctx, id)
}

// An operator cancels the rollout while ns_0's finalizer is being claimed into
// its drive: the cancel commits after the drive has loaded the finalizer and
// just as it marks the operation started. The finalizer still stands down
// cancelled without publishing ns_0's VSchema, because the drive reads pending
// commands only once the start is durable, immediately before the engine call.
func TestLocalClient_FinalizerHonorsCancelCommittedAsItStarts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	wrapper := &cancelOnStartStorage{}
	f := newFinalizerControlFixtureWith(t, state.ApplyOperation.Running, finalizerControlOptions{
		wrapStorage: func(stor storage.Storage) storage.Storage {
			wrapper.Storage = stor
			return wrapper
		},
	})
	wrapper.apply = f.apply

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	assert.Empty(t, f.eng.applies(), "a cancel committed before the engine call must keep the VSchema off the engine")
	assert.Equal(t, state.ApplyOperation.Cancelled, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationCancel)
	f.requireParentUntouched(t)
}

// An operator stops a sharded rollout and then escalates to cancel before the
// stop is processed. The finalizer settles cancelled, the same outcome the
// rollout's other operations reach by consuming the cancel first, so it is
// never left stopped under a cancelled rollout where a start could resume it
// into publishing ns_0's VSchema.
func TestLocalClient_FinalizerWithPendingStopAndCancelSettlesCancelled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	f.requestPending(t, storage.ControlOperationStop)
	f.requestPending(t, storage.ControlOperationCancel)

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	assert.Empty(t, f.eng.applies(), "a cancelled finalizer must never hand its VSchema to the engine")
	assert.Equal(t, state.ApplyOperation.Cancelled, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationCancel)
	f.requireRequestPending(t, storage.ControlOperationStop)
	f.requireParentUntouched(t)
}

// failingFinalizerWritesStorage fails the finalizer drive's chosen storage
// writes, standing in for a pod that dies or loses its database connection at
// that point of the drive.
type failingFinalizerWritesStorage struct {
	storage.Storage
	failMarkCompletedOnce bool
	failSaveResumeState   bool
	// failGetResumeStateOnce fails the next read of the finalizer's engine
	// resume state, the read the drive makes before it starts.
	failGetResumeStateOnce bool
	// failMarkFailedRetryable fails every pause write, standing in for storage
	// going away between the engine's answer and the drive recording it.
	failMarkFailedRetryable bool
}

func (s *failingFinalizerWritesStorage) ApplyOperations() storage.ApplyOperationStore {
	return &failingFinalizerWritesOperations{ApplyOperationStore: s.Storage.ApplyOperations(), parent: s}
}

type failingFinalizerWritesOperations struct {
	storage.ApplyOperationStore
	parent *failingFinalizerWritesStorage
}

func (o *failingFinalizerWritesOperations) MarkCompleted(ctx context.Context, id int64) error {
	if o.parent.failMarkCompletedOnce {
		o.parent.failMarkCompletedOnce = false
		return fmt.Errorf("mark apply_operation %d completed: connection lost", id)
	}
	return o.ApplyOperationStore.MarkCompleted(ctx, id)
}

func (o *failingFinalizerWritesOperations) GetEngineResumeState(ctx context.Context, id int64) (*storage.EngineResumeState, error) {
	if o.parent.failGetResumeStateOnce {
		o.parent.failGetResumeStateOnce = false
		return nil, fmt.Errorf("read engine resume state for apply_operation %d: connection lost", id)
	}
	return o.ApplyOperationStore.GetEngineResumeState(ctx, id)
}

func (o *failingFinalizerWritesOperations) SaveEngineResumeState(ctx context.Context, id int64, rs *storage.EngineResumeState) error {
	if o.parent.failSaveResumeState {
		return fmt.Errorf("save engine resume state for apply_operation %d: connection lost", id)
	}
	return o.ApplyOperationStore.SaveEngineResumeState(ctx, id, rs)
}

func (o *failingFinalizerWritesOperations) MarkFailedRetryable(ctx context.Context, id int64, errMsg string) error {
	if o.parent.failMarkFailedRetryable {
		return fmt.Errorf("mark apply_operation %d failed_retryable: connection lost", id)
	}
	return o.ApplyOperationStore.MarkFailedRetryable(ctx, id, errMsg)
}

// ns_0's finalizer publishes its VSchema on a synchronous engine, which reports
// no resume state, and the drive dies before it records the finalizer
// completed. The operator then cancels, and a later drive picks the row up.
// The VSchema is already live, so that drive must not record the finalizer
// cancelled over it: it re-applies the idempotent VSchema from the plan and
// records the finalizer completed, leaving the cancel for the rollout to
// resolve as outrun.
func TestLocalClient_RedrivenFinalizerWithPendingCancelReappliesPublishedVSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	wrapper := &failingFinalizerWritesStorage{failMarkCompletedOnce: true}
	f := newFinalizerControlFixtureWith(t, state.ApplyOperation.Running, finalizerControlOptions{
		wrapStorage: func(stor storage.Storage) storage.Storage {
			wrapper.Storage = stor
			return wrapper
		},
	})

	require.Error(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID),
		"the first drive fails recording the finalizer completed")
	require.Len(t, f.eng.applies(), 1, "the first drive published ns_0's VSchema")
	require.Equal(t, state.ApplyOperation.Running, f.finalizerState(t))

	f.requestPending(t, storage.ControlOperationCancel)
	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	applies := f.eng.applies()
	require.Len(t, applies, 2, "the re-drive re-applies the published VSchema rather than settling over it")
	assert.Nil(t, applies[1].ResumeState, "the engine reported no resume state, so the re-drive applies from the plan")
	require.Len(t, applies[1].Changes, 1)
	assert.Equal(t, "ns_0", applies[1].Changes[0].Namespace)
	assert.Equal(t, state.ApplyOperation.Completed, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationCancel)
}

// A drive that cannot record handing ns_0's finalizer to the engine stops
// before the engine call, so no VSchema is ever live without a record a later
// drive can see.
func TestLocalClient_FinalizerDoesNotApplyWhenHandoffCannotBeRecorded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	wrapper := &failingFinalizerWritesStorage{failSaveResumeState: true}
	f := newFinalizerControlFixtureWith(t, state.ApplyOperation.Running, finalizerControlOptions{
		wrapStorage: func(stor storage.Storage) storage.Storage {
			wrapper.Storage = stor
			return wrapper
		},
	})

	err := f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "record engine handoff")
	assert.Empty(t, f.eng.applies(), "the VSchema must not reach the engine without a handoff record")
	assert.Equal(t, state.ApplyOperation.Running, f.finalizerState(t), "the row stays running for a later drive to retry")
}

func (f *finalizerControlFixture) finalizerOperation(t *testing.T) *storage.ApplyOperation {
	t.Helper()
	op, err := f.stor.ApplyOperations().Get(t.Context(), f.finalizerID)
	require.NoError(t, err)
	require.NotNil(t, op)
	return op
}

// redispatch plays the rest of a retry the way the operator does it: the
// rollout projection has moved the parent to failed_retryable, so the claim
// re-leases the paused finalizer under the parent's retry budget and moves it
// back to running. It returns a context carrying the new lease, the one the
// re-drive writes under.
func (f *finalizerControlFixture) redispatch(t *testing.T) context.Context {
	t.Helper()
	_, err := f.leaseDB.ExecContext(t.Context(), `UPDATE applies SET state = ? WHERE id = ?`,
		state.Apply.FailedRetryable, f.apply.ID)
	require.NoError(t, err)

	claimed, err := f.stor.ApplyOperations().FindNextApplyOperation(t.Context(), "retry-driver")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a failed_retryable finalizer under a failed_retryable parent must be claimable")
	require.Equal(t, f.finalizerID, claimed.ID)
	assert.Equal(t, state.ApplyOperation.Running, claimed.State, "the claim moves the paused finalizer back to running")
	assert.Empty(t, claimed.ErrorMessage, "the claim clears the pause's reason")

	parent, err := f.stor.Applies().Get(t.Context(), f.apply.ID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, 1, parent.Attempt, "the redispatch spends one unit of the parent's retry budget")

	return storage.WithOperationLease(t.Context(), storage.OperationLease{
		ApplyID: f.apply.ID, OperationID: f.finalizerID, Owner: claimed.LeaseOwner, Token: claimed.LeaseToken,
	})
}

// ns_0's finalizer reaches the engine, which reports a transient error. The
// VSchema is not lost: the finalizer pauses as failed_retryable with the error
// recorded, the claim re-drives it under the parent's retry budget, and the
// re-drive re-applies the idempotent VSchema and completes. The pause is the
// drive's outcome, not a resume failure: the drive returns no error, and the
// operation row alone carries the reason.
func TestLocalClient_FinalizerRetryableEngineErrorPausesThenRedriveCompletes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	f.eng.applyErrs = []error{fmt.Errorf("vschema deploy endpoint unreachable")}

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID),
		"a stored pause is the drive's outcome, not a resume failure")

	paused := f.finalizerOperation(t)
	assert.Equal(t, state.ApplyOperation.FailedRetryable, paused.State)
	assert.Contains(t, paused.ErrorMessage, "vschema deploy endpoint unreachable")
	assert.Nil(t, paused.CompletedAt, "a paused finalizer is resumable, so it carries no completion time")
	require.Len(t, f.eng.applies(), 1)

	redriveCtx := f.redispatch(t)
	require.NoError(t, f.client.ResumeApplyOperation(redriveCtx, f.apply, f.finalizerID))

	require.Len(t, f.eng.applies(), 2, "the re-drive re-applies the VSchema from the plan")
	assert.Equal(t, "ns_0", f.eng.applies()[1].Changes[0].Namespace)
	completed := f.finalizerOperation(t)
	assert.Equal(t, state.ApplyOperation.Completed, completed.State)
	assert.Empty(t, completed.ErrorMessage)
	assert.NotNil(t, completed.CompletedAt)
}

// The drive cannot read the finalizer's engine resume state, which happens
// before it starts. Proceeding could restart or duplicate in-flight VSchema
// work, so the drive stops, but the storage error is transient: the finalizer
// pauses as failed_retryable instead of failing, and the re-drive completes
// without the engine having been reached by the first attempt.
func TestLocalClient_FinalizerResumeStateReadErrorPausesThenRedriveCompletes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	wrapper := &failingFinalizerWritesStorage{failGetResumeStateOnce: true}
	f := newFinalizerControlFixtureWith(t, state.ApplyOperation.Running, finalizerControlOptions{
		wrapStorage: func(stor storage.Storage) storage.Storage {
			wrapper.Storage = stor
			return wrapper
		},
	})

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))

	paused := f.finalizerOperation(t)
	assert.Equal(t, state.ApplyOperation.FailedRetryable, paused.State)
	assert.Contains(t, paused.ErrorMessage, "load engine resume state")
	assert.Contains(t, paused.ErrorMessage, "connection lost")
	assert.Empty(t, f.eng.applies(), "a drive that cannot read its resume state must not reach the engine")

	redriveCtx := f.redispatch(t)
	require.NoError(t, f.client.ResumeApplyOperation(redriveCtx, f.apply, f.finalizerID))

	require.Len(t, f.eng.applies(), 1)
	assert.Equal(t, state.ApplyOperation.Completed, f.finalizerOperation(t).State)
}

// requireHandoffRecorded asserts the finalizer row carries the drive's own
// handoff record, with nothing the engine reported since.
func (f *finalizerControlFixture) requireHandoffRecorded(t *testing.T) {
	t.Helper()
	stored, err := f.stor.ApplyOperations().GetEngineResumeState(t.Context(), f.finalizerID)
	require.NoError(t, err, "the handoff record must still be on the row")
	assert.True(t, isFinalizerHandoffRecordOnly(stored), "the row carries the drive's handoff record, not engine state")
}

// The engine refuses ns_0's VSchema with a transient error, so nothing is live
// on it. While the finalizer sits paused the operator stops the rollout. The
// re-drive must honor that stop before it reaches the engine: the pause
// withdrew the handoff record the refused drive wrote, so the re-drive reads
// the row as never handed to the engine and settles it stopped without
// re-applying, the way a never-started finalizer does (CO-4).
func TestLocalClient_RefusedFinalizerPauseWithdrawsHandoffSoRedriveHonorsPendingStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	f.eng.applyErrs = []error{fmt.Errorf("vschema deploy endpoint unreachable")}

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))
	require.Equal(t, state.ApplyOperation.FailedRetryable, f.finalizerState(t))
	_, err := f.stor.ApplyOperations().GetEngineResumeState(t.Context(), f.finalizerID)
	require.ErrorIs(t, err, storage.ErrEngineResumeStateNotFound, "a refused drive leaves no handoff behind")

	f.requestPending(t, storage.ControlOperationStop)
	redriveCtx := f.redispatch(t)
	require.NoError(t, f.client.ResumeApplyOperation(redriveCtx, f.apply, f.finalizerID))

	require.Len(t, f.eng.applies(), 1, "the re-drive must not re-apply under a pending stop when nothing is live")
	assert.Equal(t, state.ApplyOperation.Stopped, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationStop)
}

// The engine cannot tell whether it started ns_0's VSchema deploy, so the
// drive pauses but keeps its handoff record: the work may be live. A stop the
// operator issues meanwhile cannot pause work the engine may be carrying out,
// so the re-drive re-applies the idempotent VSchema and records the outcome
// the engine reports, leaving the stop for the rollout to resolve as outrun.
func TestLocalClient_UnknownOutcomeFinalizerPauseKeepsHandoffSoRedriveReobservesWork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	f.eng.applyErrs = []error{engine.NewOutcomeUnknownError("deploy request may have been created")}

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))
	paused := f.finalizerOperation(t)
	require.Equal(t, state.ApplyOperation.FailedRetryable, paused.State)
	assert.Contains(t, paused.ErrorMessage, "deploy request may have been created")
	f.requireHandoffRecorded(t)

	f.requestPending(t, storage.ControlOperationStop)
	redriveCtx := f.redispatch(t)
	require.NoError(t, f.client.ResumeApplyOperation(redriveCtx, f.apply, f.finalizerID))

	require.Len(t, f.eng.applies(), 2, "the re-drive re-observes work that may be live rather than settling over it")
	assert.Equal(t, state.ApplyOperation.Completed, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationStop)
}

// An earlier drive published ns_0's VSchema and died before recording the
// finalizer completed, leaving its handoff on the row. A later drive is
// refused by the engine and pauses. That pause must not withdraw the earlier
// drive's handoff: the VSchema is live, and a stop the operator issues
// meanwhile must not settle the row over it, so the re-drive re-applies and
// completes.
func TestLocalClient_EarlierDriveHandoffSurvivesLaterRefusedFinalizerPause(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	wrapper := &failingFinalizerWritesStorage{failMarkCompletedOnce: true}
	f := newFinalizerControlFixtureWith(t, state.ApplyOperation.Running, finalizerControlOptions{
		wrapStorage: func(stor storage.Storage) storage.Storage {
			wrapper.Storage = stor
			return wrapper
		},
	})

	require.Error(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID),
		"the first drive fails recording the finalizer completed")
	require.Len(t, f.eng.applies(), 1, "the first drive published ns_0's VSchema")
	require.Equal(t, state.ApplyOperation.Running, f.finalizerState(t))
	f.requireHandoffRecorded(t)

	f.eng.applyErrs = []error{fmt.Errorf("vschema deploy endpoint unreachable")}
	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID))
	require.Len(t, f.eng.applies(), 2)
	paused := f.finalizerOperation(t)
	require.Equal(t, state.ApplyOperation.FailedRetryable, paused.State)
	assert.Contains(t, paused.ErrorMessage, "vschema deploy endpoint unreachable")
	f.requireHandoffRecorded(t)

	f.requestPending(t, storage.ControlOperationStop)
	redriveCtx := f.redispatch(t)
	require.NoError(t, f.client.ResumeApplyOperation(redriveCtx, f.apply, f.finalizerID))

	require.Len(t, f.eng.applies(), 3, "the re-drive re-applies the published VSchema rather than settling over it")
	assert.Equal(t, state.ApplyOperation.Completed, f.finalizerState(t))
	f.requireRequestPending(t, storage.ControlOperationStop)
}

// The engine refuses ns_0's VSchema with a transient error and storage then
// refuses the pause write. The row stays running for a later drive, and the
// drive's error names both the engine's refusal and the refused pause, so the
// log shows the row was left as it is on purpose.
func TestLocalClient_FinalizerPauseRefusedByStorageLeavesRowRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	wrapper := &failingFinalizerWritesStorage{failMarkFailedRetryable: true}
	f := newFinalizerControlFixtureWith(t, state.ApplyOperation.Running, finalizerControlOptions{
		wrapStorage: func(stor storage.Storage) storage.Storage {
			wrapper.Storage = stor
			return wrapper
		},
	})
	f.eng.applyErrs = []error{fmt.Errorf("vschema deploy endpoint unreachable")}

	err := f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID)
	require.ErrorContains(t, err, "vschema deploy endpoint unreachable")
	require.ErrorContains(t, err, "pause group_finalizer")

	op := f.finalizerOperation(t)
	assert.Equal(t, state.ApplyOperation.Running, op.State, "a refused pause leaves the row as the drive found it")
	assert.Empty(t, op.ErrorMessage)
	require.Len(t, f.eng.applies(), 1)
}

// The drive is cancelled while the engine is applying ns_0's VSchema, and the
// engine returns the cancellation. The error describes the driver, not the
// VSchema work, so the drive records nothing: the row stays running with no
// error and its handoff in place, and the driver that reclaims it decides the
// outcome.
func TestLocalClient_CancelledFinalizerDriveRecordsNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	driveCtx, cancelDrive := context.WithCancel(f.opCtx)
	defer cancelDrive()
	f.eng.applyHook = func(ctx context.Context) error {
		cancelDrive()
		return ctx.Err()
	}

	err := f.client.ResumeApplyOperation(driveCtx, f.apply, f.finalizerID)
	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, err.Error(), "pause group_finalizer", "a cancelled drive attempts no pause")

	op := f.finalizerOperation(t)
	assert.Equal(t, state.ApplyOperation.Running, op.State)
	assert.Empty(t, op.ErrorMessage, "a cancelled drive records no outcome on the row")
	f.requireHandoffRecorded(t)
	require.Len(t, f.eng.applies(), 1)
}

// A failure retrying cannot cure fails the finalizer for good, and nothing
// revives a failed row: the claim never offers it again, even under a parent
// that is failed_retryable with budget left.
func TestLocalClient_FinalizerPermanentEngineErrorFailsWithoutRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	f.eng.applyErrs = []error{engine.NewPermanentError("vschema rejected: unknown keyspace")}

	err := f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID)
	require.ErrorContains(t, err, "vschema rejected: unknown keyspace")

	failed := f.finalizerOperation(t)
	assert.Equal(t, state.ApplyOperation.Failed, failed.State)
	assert.Contains(t, failed.ErrorMessage, "vschema rejected: unknown keyspace")
	assert.NotNil(t, failed.CompletedAt)

	_, err = f.leaseDB.ExecContext(t.Context(), `UPDATE applies SET state = ? WHERE id = ?`,
		state.Apply.FailedRetryable, f.apply.ID)
	require.NoError(t, err)
	claimed, err := f.stor.ApplyOperations().FindNextApplyOperation(t.Context(), "retry-driver")
	require.NoError(t, err)
	if claimed != nil {
		assert.NotEqual(t, f.finalizerID, claimed.ID, "a failed finalizer is never re-claimed")
	}
}

// The engine's own refusals, a non-success terminal outcome, and a progress
// poll the engine fails permanently are confirmed failures: the VSchema did
// not apply and retrying the same request cannot change that, so the finalizer
// fails instead of pausing.
func TestLocalClient_FinalizerNonSuccessOutcomeFailsWithoutRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	tests := []struct {
		name    string
		prepare func(*finalizerControlEngine)
		message string
	}{
		{
			name: "engine does not accept the apply",
			prepare: func(e *finalizerControlEngine) {
				e.applyResult = &engine.ApplyResult{Accepted: false}
			},
			message: "was not accepted",
		},
		{
			name: "deploy ends in a failed terminal state",
			prepare: func(e *finalizerControlEngine) {
				e.applyResult = &engine.ApplyResult{Accepted: true, ResumeState: &engine.ResumeState{MigrationContext: "deploy-ns-0"}}
				e.progressState = engine.StateFailed
			},
			message: "ended in non-success state",
		},
		{
			name: "progress poll fails permanently",
			prepare: func(e *finalizerControlEngine) {
				e.applyResult = &engine.ApplyResult{Accepted: true, ResumeState: &engine.ResumeState{MigrationContext: "deploy-ns-0"}}
				e.progressErr = engine.NewPermanentError("deploy request deploy-ns-0 not found")
			},
			message: "progress poll failed permanently",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
			tt.prepare(f.eng)

			err := f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID)
			require.ErrorContains(t, err, tt.message)

			failed := f.finalizerOperation(t)
			assert.Equal(t, state.ApplyOperation.Failed, failed.State)
			assert.Contains(t, failed.ErrorMessage, tt.message)
		})
	}
}

// Whether an engine error pauses the finalizer follows the apply's other
// engine drives: a retryable error pauses it on a database type whose recovery
// resumes from a checkpoint, a retryable error on a type that could dispatch
// duplicate work fails it unless the engine says the outcome is unknown, and a
// permanent error fails it everywhere. What the drive returns follows what it
// recorded: a pause ends the drive without an error, a failure returns its
// cause.
func TestLocalClient_FinalizerEngineErrorDispositionFollowsDatabaseType(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	// An embedder-registered engine type has no checkpoint-resume contract, so
	// it takes the conservative disposition a retryable start error fails on.
	const embeddedEngineType = "embedded-engine"
	tests := []struct {
		name         string
		databaseType string
		err          error
		wantState    string
	}{
		{"retryable error pauses a checkpoint-resuming type", storage.DatabaseTypeStrata, fmt.Errorf("deploy endpoint unreachable"), state.ApplyOperation.FailedRetryable},
		{"retryable error fails a type that could duplicate work", embeddedEngineType, fmt.Errorf("deploy endpoint unreachable"), state.ApplyOperation.Failed},
		{"unknown outcome pauses every type", embeddedEngineType, engine.NewOutcomeUnknownError("deploy request may have been created"), state.ApplyOperation.FailedRetryable},
		{"permanent error fails every type", storage.DatabaseTypeStrata, engine.NewPermanentError("vschema rejected"), state.ApplyOperation.Failed},
		{"retryable error starting work fails a reattaching type", storage.DatabaseTypeVitess, fmt.Errorf("deploy endpoint unreachable"), state.ApplyOperation.Failed},
		{"retryable error reattaching to accepted work pauses a reattaching type", storage.DatabaseTypeVitess, fmt.Errorf("%w: %w", engine.ErrReattachFailed, fmt.Errorf("read deploy request: connection reset")), state.ApplyOperation.FailedRetryable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFinalizerControlFixtureWith(t, state.ApplyOperation.Running, finalizerControlOptions{databaseType: tt.databaseType})
			f.eng.applyErrs = []error{tt.err}

			err := f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID)
			if tt.wantState == state.ApplyOperation.FailedRetryable {
				require.NoError(t, err, "a stored pause is the drive's outcome, not a resume failure")
			} else {
				require.ErrorContains(t, err, tt.err.Error())
			}

			op := f.finalizerOperation(t)
			assert.Equal(t, tt.wantState, op.State)
			assert.Contains(t, op.ErrorMessage, tt.err.Error())
		})
	}
}

// The engine accepts ns_0's VSchema deploy, then every progress poll fails with
// a transient error. The finalizer must not poll forever: once the poll's error
// budget is spent it pauses as failed_retryable with the poll error recorded,
// the drive ends without an error because the pause is its outcome, and the
// claim's re-drive reattaches to the stored deploy and completes it.
func TestLocalClient_FinalizerProgressOutagePausesThenRedriveCompletes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newFinalizerControlFixture(t, state.ApplyOperation.Running)
	f.client.taskPollIntervalOverride = time.Millisecond
	f.eng.applyResult = &engine.ApplyResult{Accepted: true, ResumeState: &engine.ResumeState{MigrationContext: "deploy-ns-0"}}
	f.eng.progressErr = fmt.Errorf("get deploy request: rate limited")

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID),
		"a stored pause is the drive's outcome, not a resume failure")

	paused := f.finalizerOperation(t)
	assert.Equal(t, state.ApplyOperation.FailedRetryable, paused.State)
	assert.Contains(t, paused.ErrorMessage, fmt.Sprintf("failed after %d consecutive errors", maxConsecutiveProgressPollErrors))
	assert.Contains(t, paused.ErrorMessage, "rate limited")
	assert.Nil(t, paused.CompletedAt, "a paused finalizer is resumable, so it carries no completion time")

	f.eng.progressErr = nil
	redriveCtx := f.redispatch(t)
	require.NoError(t, f.client.ResumeApplyOperation(redriveCtx, f.apply, f.finalizerID))

	applies := f.eng.applies()
	require.Len(t, applies, 2)
	require.NotNil(t, applies[1].ResumeState, "the re-drive hands the engine the deploy the first drive stored")
	assert.Equal(t, "deploy-ns-0", applies[1].ResumeState.MigrationContext)
	completed := f.finalizerOperation(t)
	assert.Equal(t, state.ApplyOperation.Completed, completed.State)
	assert.Empty(t, completed.ErrorMessage)
}

// A progress outage on work the engine already accepted follows the
// accepted-work policy: a type whose recovery resumes from a checkpoint or
// reattaches to accepted work pauses the finalizer, while a type with neither
// fails it, because a retry there could dispatch the work again. What the
// drive returns follows what it recorded: a pause ends the drive without an
// error, a failure returns the poll error.
func TestLocalClient_FinalizerProgressOutageDispositionFollowsDatabaseType(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	const embeddedEngineType = "embedded-engine"
	tests := []struct {
		name         string
		databaseType string
		wantState    string
	}{
		{"checkpoint-resuming type pauses", storage.DatabaseTypeStrata, state.ApplyOperation.FailedRetryable},
		{"reattaching type pauses", storage.DatabaseTypeVitess, state.ApplyOperation.FailedRetryable},
		{"type with neither fails", embeddedEngineType, state.ApplyOperation.Failed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFinalizerControlFixtureWith(t, state.ApplyOperation.Running, finalizerControlOptions{databaseType: tt.databaseType})
			f.client.taskPollIntervalOverride = time.Millisecond
			f.eng.applyResult = &engine.ApplyResult{Accepted: true, ResumeState: &engine.ResumeState{MigrationContext: "deploy-ns-0"}}
			f.eng.progressErr = fmt.Errorf("get deploy request: connection reset")

			err := f.client.ResumeApplyOperation(f.opCtx, f.apply, f.finalizerID)
			if tt.wantState == state.ApplyOperation.FailedRetryable {
				require.NoError(t, err, "a stored pause is the drive's outcome, not a resume failure")
			} else {
				require.ErrorContains(t, err, "connection reset")
			}

			op := f.finalizerOperation(t)
			assert.Equal(t, tt.wantState, op.State)
			assert.Contains(t, op.ErrorMessage, "connection reset")
		})
	}
}
