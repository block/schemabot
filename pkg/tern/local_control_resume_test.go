package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/statement"
)

// A sharded re-plan repeats the same table across shards (and across keyspaces),
// each with its own DDL. replanShardTableDDL must key by (namespace, shard,
// table) so one shard's — or one keyspace's — remaining diff never reconciles
// another's task. Keying by less than the full tuple would collapse these
// entries and conflate tasks.
func TestReplanShardTableDDLKeysPerNamespaceAndShard(t *testing.T) {
	ddlA := "ALTER TABLE `mutes` ADD INDEX (`created_at`)"
	ddlB := "ALTER TABLE `mutes` ADD INDEX (`updated_at`)" // 80- has drifted differently
	ddlC := "ALTER TABLE `mutes` ADD INDEX (`deleted_at`)" // a second keyspace, same shard+table
	result := &engine.PlanResult{
		Changes: []engine.SchemaChange{
			{Namespace: "ks1", Shard: engine.Shard{Name: "-80"}, TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddlA}}},
			{Namespace: "ks1", Shard: engine.Shard{Name: "80-"}, TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddlB}}},
			{Namespace: "ks2", Shard: engine.Shard{Name: "-80"}, TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddlC}}},
		},
	}

	got := replanShardTableDDL(result)

	require.Len(t, got, 3, "same table across shards and keyspaces must produce three distinct keys")
	assert.Equal(t, ddlA, got[shardTableKey{namespace: "ks1", shard: "-80", table: "mutes"}])
	assert.Equal(t, ddlB, got[shardTableKey{namespace: "ks1", shard: "80-", table: "mutes"}])
	assert.Equal(t, ddlC, got[shardTableKey{namespace: "ks2", shard: "-80", table: "mutes"}], "the same shard+table in another keyspace is not conflated")
}

// For a non-sharded engine the shard name is empty, so keying degrades to
// (namespace, table) and matches the pre-sharding lookup.
func TestReplanShardTableDDLNonShardedDegradesToTable(t *testing.T) {
	ddl := "ALTER TABLE `mutes` ADD INDEX (`created_at`)"
	result := &engine.PlanResult{
		Changes: []engine.SchemaChange{
			{Namespace: "commerce", TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddl}}},
		},
	}

	got := replanShardTableDDL(result)

	require.Len(t, got, 1)
	assert.Equal(t, ddl, got[shardTableKey{namespace: "commerce", table: "mutes"}])
}

// On resume, replanAndFilterTasks recomputes each deployment's delta against its
// live schema and overwrites task.DDL with it. verifyReplannedTaskDDL is the
// gate that keeps a drifted deployment from silently applying that recomputed
// DDL: it must pass only when the re-plan matches what the task was reviewed
// with, tolerating incidental formatting, and fail closed otherwise.
func TestVerifyReplannedTaskDDL(t *testing.T) {
	task := func(reviewed string) *storage.Task {
		return &storage.Task{
			TaskIdentifier: "task_abc123",
			Namespace:      "commerce",
			Shard:          "-80",
			TableName:      "users",
			DDLAction:      "alter",
			DDL:            reviewed,
		}
	}

	t.Run("matching re-plan passes", func(t *testing.T) {
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		err := verifyReplannedTaskDDL(tk, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		require.NoError(t, err)
	})

	t.Run("incidental formatting differences pass", func(t *testing.T) {
		// Unquoted identifiers and extra whitespace canonicalize to the same form
		// as the reviewed DDL, so they are not drift.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		err := verifyReplannedTaskDDL(tk, "ALTER TABLE   users   ADD COLUMN email varchar(255)")
		require.NoError(t, err)
	})

	t.Run("divergent re-plan fails closed", func(t *testing.T) {
		// The deployment drifted: the re-plan would apply a different column type
		// than the one reviewed. This unreviewed DDL must be refused.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		err := verifyReplannedTaskDDL(tk, "ALTER TABLE `users` ADD COLUMN `email` varchar(100)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "drifted from the reviewed plan")
		assert.Contains(t, err.Error(), "commerce[-80].users/alter")
	})

	t.Run("empty reviewed DDL is left to the caller", func(t *testing.T) {
		// Only legacy synthetic VSchema tasks carry no reviewed DDL; they have no
		// reference to compare against and are handled downstream, not here.
		tk := task("")
		err := verifyReplannedTaskDDL(tk, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		require.NoError(t, err)
	})

	t.Run("unparseable re-planned DDL fails closed", func(t *testing.T) {
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		err := verifyReplannedTaskDDL(tk, "this is not valid sql")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "re-planned DDL for task task_abc123")
	})
}

// replanAndFilterTasks recomputes the delta against live schema and overwrites
// each still-needed task's DDL with it. When the deployment has drifted, that
// recomputed DDL diverges from what was reviewed; the resume must fail closed
// rather than silently apply the unreviewed DDL.
func TestReplanAndFilterTasks_FailsClosedOnDrift(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	drifted := &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: statement.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(100)",
			}},
		}},
	}
	c := newPlanMaterializeClientWithPlan(store, drifted)

	apply := &storage.Apply{Database: "testapp"}
	plan := &storage.Plan{}
	tasks := []*storage.Task{{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}}

	_, err := c.replanAndFilterTasks(t.Context(), apply, tasks, plan)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted from the reviewed plan")
	assert.Contains(t, err.Error(), "testapp.users/alter")
}

// The sequential resume loop re-plans each table right before applying it to
// catch a cutover that raced the resume. tableStillNeedsChange must return the
// DDL that re-plan would now apply so the loop can confirm it still matches the
// reviewed DDL before applying — closing the window between the resume-entry
// re-plan and this later per-task apply.
func TestTableStillNeedsChange_ReturnsReplannedDDL(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	apply := &storage.Apply{Database: "testapp"}
	plan := &storage.Plan{}
	task := &storage.Task{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}

	ddl, needsChange, err := c.tableStillNeedsChange(t.Context(), apply, plan, task)
	require.NoError(t, err)
	assert.True(t, needsChange)
	assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ddl)
	require.NoError(t, verifyReplannedTaskDDL(task, ddl), "matching re-plan is not drift")
}

// When the table has dropped out of the re-plan diff (its cutover completed) the
// sequential loop treats it as already applied, so tableStillNeedsChange must
// report that no change remains.
func TestTableStillNeedsChange_TableAbsentReportsDone(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	// Re-plan for a different table only: the task's table is no longer in the diff.
	otherTablePlan := &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			TableChanges: []engine.TableChange{{
				Table:     "orders",
				Operation: statement.StatementAlterTable,
				DDL:       "ALTER TABLE `orders` ADD COLUMN `total` int",
			}},
		}},
	}
	c := newPlanMaterializeClientWithPlan(store, otherTablePlan)

	apply := &storage.Apply{Database: "testapp"}
	plan := &storage.Plan{}
	task := &storage.Task{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}

	ddl, needsChange, err := c.tableStillNeedsChange(t.Context(), apply, plan, task)
	require.NoError(t, err)
	assert.False(t, needsChange)
	assert.Empty(t, ddl)
}

// If live drifts between resume entry and a later per-task apply, the re-plan the
// sequential loop performs returns DDL that no longer matches the reviewed DDL.
// tableStillNeedsChange surfaces that DDL and verifyReplannedTaskDDL fails closed
// so the loop refuses to apply unreviewed DDL.
func TestTableStillNeedsChange_DriftFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	drifted := &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: statement.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(100)",
			}},
		}},
	}
	c := newPlanMaterializeClientWithPlan(store, drifted)

	apply := &storage.Apply{Database: "testapp"}
	plan := &storage.Plan{}
	task := &storage.Task{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}

	ddl, needsChange, err := c.tableStillNeedsChange(t.Context(), apply, plan, task)
	require.NoError(t, err)
	require.True(t, needsChange)
	err = verifyReplannedTaskDDL(task, ddl)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted from the reviewed plan")
	assert.Contains(t, err.Error(), "testapp.users/alter")
}

// When the re-plan matches the reviewed DDL the deployment has not drifted, so
// the task stays active and its DDL is refreshed from the re-plan.
func TestReplanAndFilterTasks_MatchKeepsTaskActive(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	apply := &storage.Apply{Database: "testapp"}
	plan := &storage.Plan{}
	tasks := []*storage.Task{{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}}

	rp, err := c.replanAndFilterTasks(t.Context(), apply, tasks, plan)
	require.NoError(t, err)
	require.Len(t, rp.ActiveTasks, 1)
	assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", rp.ActiveTasks[0].DDL)
	assert.Zero(t, rp.CompletedCount)
}

// scriptedPlanStore scripts plan reads for recovery tests: a nil plan with a
// nil error models a confirmed-missing plan row, while a non-nil error models
// a storage read failure.
type scriptedPlanStore struct {
	storage.PlanStore
	plan *storage.Plan
	err  error
}

func (s *scriptedPlanStore) GetByID(context.Context, int64) (*storage.Plan, error) {
	return s.plan, s.err
}

// terminalRecordingObserver records terminal notifications so recovery tests
// can assert whether an apply's registered waiter (e.g. the PR check/comment)
// was told the apply reached a terminal state.
type terminalRecordingObserver struct {
	terminal []*storage.Apply
}

func (o *terminalRecordingObserver) OnProgress(*storage.Apply, []*storage.Task) {}
func (o *terminalRecordingObserver) OnTerminal(apply *storage.Apply, _ []*storage.Task) {
	o.terminal = append(o.terminal, apply)
}

// recoveryPlanLoadFixture builds an in-flight Vitess apply whose recovery is
// about to load its plan, with the plan store scripted by the caller.
func recoveryPlanLoadFixture(plans storage.PlanStore) (*LocalClient, *storage.Apply, []*storage.Task, *exactProgressApplyStore) {
	operationID := int64(3)
	apply := &storage.Apply{
		ID:              21,
		ApplyIdentifier: "apply-recover-plan",
		PlanID:          5,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Engine:          storage.EnginePlanetScale,
		State:           state.Apply.Running,
	}
	tasks := []*storage.Task{{
		ID:               2,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-recover-plan",
		Database:         "testdb",
		DatabaseType:     storage.DatabaseTypeVitess,
		Namespace:        "commerce",
		TableName:        "users",
		DDL:              "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		DDLAction:        "alter",
		State:            state.Task.Running,
	}}
	applyStore := &exactProgressApplyStore{apply: apply}
	client := &LocalClient{
		config: LocalConfig{Database: "testdb", Type: storage.DatabaseTypeVitess},
		storage: &exactProgressStorage{
			applies:         applyStore,
			tasks:           &exactProgressTaskStore{tasks: tasks},
			controlRequests: &testControlRequestStore{},
			plans:           plans,
		},
		logger: slog.Default(),
	}
	return client, apply, tasks, applyStore
}

// Recovery must not convert a transient storage failure on the plan load into
// terminal apply state: the engine-side work (a checkpointed copy or a live
// deploy request) is untouched. The recovery attempt exits with an error so
// the claim is released and a later attempt retries against intact storage,
// and no terminal notification reaches the apply's observer.
func TestResumeApplyPlanLoadStorageErrorStaysRecoverable(t *testing.T) {
	storageErr := errors.New("storage unavailable")
	client, apply, tasks, applyStore := recoveryPlanLoadFixture(&scriptedPlanStore{err: storageErr})
	observer := &terminalRecordingObserver{}
	client.SetObserver(apply.ID, observer)

	err := client.resumeApplyWithTasks(t.Context(), apply, tasks, nil, false, false)

	require.ErrorIs(t, err, storageErr)
	assert.ErrorContains(t, err, "apply-recover-plan")
	assert.True(t, state.IsState(applyStore.apply.State, state.Apply.Running),
		"in-flight apply must stay recoverable, not terminal: got %s", applyStore.apply.State)
	assert.False(t, state.IsTerminalApplyState(applyStore.apply.State))
	assert.Empty(t, applyStore.apply.ErrorMessage)
	assert.Empty(t, observer.terminal, "a transient plan-load failure must not notify the terminal observer")
}

// A confirmed-missing plan row (a nil plan with no read error) is
// unrecoverable — the reviewed DDL cannot be rebuilt — so recovery fails the
// apply with an operator-facing reason and notifies its terminal observer.
func TestResumeApplyMissingPlanFailsApply(t *testing.T) {
	client, apply, tasks, applyStore := recoveryPlanLoadFixture(&scriptedPlanStore{})
	observer := &terminalRecordingObserver{}
	client.SetObserver(apply.ID, observer)

	err := client.resumeApplyWithTasks(t.Context(), apply, tasks, nil, false, false)

	require.NoError(t, err)
	assert.True(t, state.IsState(applyStore.apply.State, state.Apply.Failed),
		"apply with no plan row must fail, got %s", applyStore.apply.State)
	assert.Equal(t, "plan not found during recovery", applyStore.apply.ErrorMessage)
	assert.NotNil(t, applyStore.apply.CompletedAt, "a failed apply must record its completion time")
	assert.True(t, state.IsState(tasks[0].State, state.Task.Failed),
		"in-flight task must fail with its apply, got %s", tasks[0].State)
	assert.Equal(t, "plan not found during recovery", tasks[0].ErrorMessage)
	require.Len(t, observer.terminal, 1)
	assert.True(t, state.IsState(observer.terminal[0].State, state.Apply.Failed))
}

// When another actor settles the apply between the recovery claim and the
// plan-missing terminalization (e.g. a raced Stop()), the stored terminal
// state wins: it is not overwritten, and the observer is notified with the
// settled verdict rather than the stale in-flight state this recovery
// attempt was holding.
func TestResumeApplyMissingPlanAdoptsConcurrentTerminalState(t *testing.T) {
	client, apply, tasks, applyStore := recoveryPlanLoadFixture(&scriptedPlanStore{})
	settled := *apply
	settled.State = state.Apply.Stopped
	settled.ErrorMessage = "stopped by operator"
	applyStore.apply = &settled
	observer := &terminalRecordingObserver{}
	client.SetObserver(apply.ID, observer)

	err := client.resumeApplyWithTasks(t.Context(), apply, tasks, nil, false, false)

	require.NoError(t, err)
	assert.True(t, state.IsState(applyStore.apply.State, state.Apply.Stopped),
		"concurrently-settled state must not be overwritten, got %s", applyStore.apply.State)
	assert.Equal(t, "stopped by operator", applyStore.apply.ErrorMessage)
	require.Len(t, observer.terminal, 1)
	assert.True(t, state.IsState(observer.terminal[0].State, state.Apply.Stopped),
		"observer must see the settled verdict, got %s", observer.terminal[0].State)
}
