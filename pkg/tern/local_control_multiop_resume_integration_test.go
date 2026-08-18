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

// multiOpResumeEngine is a fake engine for operation-scoped resume tests. Its
// re-plan result is configurable so a test can choose whether the driven task
// still has remaining work, and Apply can be forced to reject so the failure
// path is exercised without a real backend.
type multiOpResumeEngine struct {
	engine.Engine

	mu          sync.Mutex
	planChanges []engine.SchemaChange
	rejectApply bool
}

func (e *multiOpResumeEngine) Name() string { return "multi-op-resume" }

func (e *multiOpResumeEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return &engine.PlanResult{Changes: e.planChanges}, nil
}

func (e *multiOpResumeEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.rejectApply {
		return &engine.ApplyResult{Accepted: false, Message: "engine rejected the schema change"}, nil
	}
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *multiOpResumeEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{State: engine.StateCompleted, Progress: 100}, nil
}

const multiOpResumeDDL = "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)"

// multiOpResumeFixture is a running multi-operation apply whose parent state is
// owned by the operator's rollout projection: two operation rows, with the
// drive's own operation leased and carrying the fixture's tasks.
type multiOpResumeFixture struct {
	stor    storage.Storage
	client  *LocalClient
	eng     *multiOpResumeEngine
	apply   *storage.Apply
	opID    int64
	opCtx   context.Context
	tasks   []*storage.Task
	leaseDB *sql.DB
}

// newMultiOpResumeFixture seeds a running two-operation apply with the given
// task states scoped to the first operation, stamps that operation's lease, and
// returns a context carrying the operation lease alone — the shape of an
// operator's operation-scoped drive claim.
func newMultiOpResumeFixture(t *testing.T, taskStates []string) *multiOpResumeFixture {
	t.Helper()
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	t.Cleanup(func() { utils.CloseAndLog(stor) })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })
	eng := &multiOpResumeEngine{}
	client.spiritEngine = eng

	now := time.Now()
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-multiop-%d", now.UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {Tables: []storage.TableChange{{Table: "users", DDL: multiOpResumeDDL, Operation: "alter"}}},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-multiop-%d", now.UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Environment:     localClientTestEnvironment,
		State:           state.Apply.Running,
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	opID, err := stor.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyID,
		Deployment: "testdb",
		Target:     "testdb",
		State:      state.ApplyOperation.Running,
	})
	require.NoError(t, err)
	_, err = stor.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyID,
		Deployment: "testdb-sibling",
		Target:     "testdb-sibling",
		State:      state.ApplyOperation.Running,
	})
	require.NoError(t, err)

	tasks := make([]*storage.Task, 0, len(taskStates))
	for i, taskState := range taskStates {
		task := &storage.Task{
			TaskIdentifier:   fmt.Sprintf("task-multiop-%d-%d", now.UnixNano(), i),
			ApplyID:          applyID,
			ApplyOperationID: &opID,
			PlanID:           planID,
			Database:         "testdb",
			DatabaseType:     storage.DatabaseTypeMySQL,
			Engine:           storage.EngineSpirit,
			Environment:      localClientTestEnvironment,
			State:            taskState,
			Namespace:        "testdb",
			TableName:        "users",
			DDL:              multiOpResumeDDL,
			DDLAction:        "alter",
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		_, err := stor.Tasks().Create(ctx, task)
		require.NoError(t, err)
		tasks = append(tasks, task)
	}

	leaseDB, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(leaseDB) })
	require.NoError(t, leaseDB.PingContext(ctx))
	_, err = leaseDB.ExecContext(ctx, `
		UPDATE apply_operations SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW() WHERE id = ?
	`, "op-driver", "op-token", opID)
	require.NoError(t, err)

	opCtx := storage.WithOperationLease(ctx, storage.OperationLease{
		ApplyID: applyID, OperationID: opID, Owner: "op-driver", Token: "op-token",
	})

	return &multiOpResumeFixture{
		stor: stor, client: client, eng: eng,
		apply: apply, opID: opID, opCtx: opCtx, tasks: tasks, leaseDB: leaseDB,
	}
}

// requireParentUntouched asserts the parent applies row still carries the state
// the operator's projection gave it — running, not completed, no error — so the
// drive provably left the parent to the projection.
func (f *multiOpResumeFixture) requireParentUntouched(t *testing.T) {
	t.Helper()
	parent, err := f.stor.Applies().Get(t.Context(), f.apply.ID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, state.Apply.Running, parent.State,
		"the parent applies row is owned by the rollout projection and must not be written by an operation-scoped drive")
	assert.Nil(t, parent.CompletedAt, "the parent must not be terminalized by an operation-scoped drive")
	assert.Empty(t, parent.ErrorMessage, "the parent must not carry a drive-written error message")
}

func (f *multiOpResumeFixture) taskState(t *testing.T, task *storage.Task) string {
	t.Helper()
	fresh, err := f.stor.Tasks().Get(t.Context(), task.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, fresh)
	return fresh.State
}

// An operation-scoped drive of a multi-operation apply holds only its operation
// lease; the parent applies row belongs to the operator's rollout projection.
// A sequential-mode resume must drive its own tasks to completion without ever
// writing the parent — a parent write would be refused by storage and abort the
// drive before any task work starts, leaving the operation re-claimed and
// re-refused on every operator tick while the schema change never progresses.
func TestLocalClient_SequentialOperationResumeDrivesTasksWithoutParentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.Pending})
	f.eng.planChanges = []engine.SchemaChange{{
		Namespace:    "testdb",
		TableChanges: []engine.TableChange{{Table: "users", DDL: multiOpResumeDDL}},
	}}

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.opID),
		"an operation-scoped sequential resume must complete without a parent-write refusal")

	assert.Equal(t, state.Task.Completed, f.taskState(t, f.tasks[0]),
		"the operation's task must be driven to completion")
	f.requireParentUntouched(t)
}

// When the resume re-plan finds no remaining work for the operation's tasks,
// the drive marks its own tasks completed and exits; deriving the operation row
// and projecting the parent terminal is the operator's job. The drive must not
// attempt the parent completed write itself — it holds no apply lease.
func TestLocalClient_OperationResumeWithNoRemainingWorkLeavesParentToProjection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.Pending})

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.opID),
		"a re-plan that finds no remaining work must exit cleanly under the operation lease")

	assert.Equal(t, state.Task.Completed, f.taskState(t, f.tasks[0]),
		"a task whose live schema already matches the reviewed target settles completed")
	f.requireParentUntouched(t)
}

// When the engine rejects a task during an operation-scoped sequential resume,
// the drive settles its own tasks — the rejected one failed, queued siblings
// cancelled — and leaves the parent failed state to the operator's projection.
func TestLocalClient_OperationResumeEngineFailureSettlesTasksNotParent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.Pending, state.Task.Pending})
	f.eng.planChanges = []engine.SchemaChange{{
		Namespace:    "testdb",
		TableChanges: []engine.TableChange{{Table: "users", DDL: multiOpResumeDDL}},
	}}
	f.eng.rejectApply = true

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.opID),
		"an engine rejection settles the operation's tasks; the drive itself exits cleanly")

	assert.Equal(t, state.Task.Failed, f.taskState(t, f.tasks[0]),
		"the rejected task must settle failed with the engine's verdict")
	assert.Equal(t, state.Task.Cancelled, f.taskState(t, f.tasks[1]),
		"a queued sibling task behind a failed one must settle cancelled")
	f.requireParentUntouched(t)
}

// A grouped-mode engine failure during an operation-scoped resume settles the
// drive's own tasks as failed and leaves the parent to the operator's
// projection — and the drive itself exits cleanly. The failure is already
// durably recorded in the tasks; a returned error would read as a transient
// drive failure that leaves the settled operation claimable on every operator
// poll, instead of letting the claim loop persist the operation row from its
// now-failed tasks immediately.
func TestLocalClient_OperationGroupedResumeFailureSettlesTasksAndExitsCleanly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newMultiOpResumeFixture(t, []string{state.Task.Pending})
	f.apply.Options = []byte(`{"defer_cutover":"true"}`)
	f.eng.planChanges = []engine.SchemaChange{{
		Namespace:    "testdb",
		TableChanges: []engine.TableChange{{Table: "users", DDL: multiOpResumeDDL}},
	}}
	f.eng.rejectApply = true

	require.NoError(t, f.client.ResumeApplyOperation(f.opCtx, f.apply, f.opID),
		"a non-retryable grouped failure settles the operation's tasks; the drive itself exits cleanly")

	assert.Equal(t, state.Task.Failed, f.taskState(t, f.tasks[0]),
		"a grouped failure must settle the driven task as failed")
	f.requireParentUntouched(t)
}
