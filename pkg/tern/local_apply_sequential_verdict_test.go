package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// failingApplyEngine answers every Apply with a scripted outcome: err when set,
// otherwise a result that rejects the work with message.
type failingApplyEngine struct {
	engine.Engine
	err        error
	message    string
	applyCalls int
}

func (e *failingApplyEngine) Name() string { return "failing-apply" }

func (e *failingApplyEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.applyCalls++
	if e.err != nil {
		return nil, e.err
	}
	return &engine.ApplyResult{Accepted: false, Message: e.message}, nil
}

// failingProgressEngine answers every Progress with err.
type failingProgressEngine struct {
	engine.Engine
	err   error
	calls int
}

func (e *failingProgressEngine) Name() string { return "failing-progress" }

func (e *failingProgressEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.calls++
	return nil, e.err
}

// verdictRefusingTaskStore keeps task rows by value, the way a real store does,
// and refuses the first `refusals` writes that record a failure verdict — failed
// or failed_retryable — with err, accepting every other write.
type verdictRefusingTaskStore struct {
	storage.TaskStore
	stored   map[string]storage.Task
	err      error
	refusals int
	refused  int
}

func newVerdictRefusingTaskStore(err error, refusals int, tasks ...*storage.Task) *verdictRefusingTaskStore {
	s := &verdictRefusingTaskStore{stored: map[string]storage.Task{}, err: err, refusals: refusals}
	for _, task := range tasks {
		s.stored[task.TaskIdentifier] = *task
	}
	return s
}

func (s *verdictRefusingTaskStore) Get(_ context.Context, taskIdentifier string) (*storage.Task, error) {
	stored, ok := s.stored[taskIdentifier]
	if !ok {
		return nil, nil
	}
	return &stored, nil
}

func (s *verdictRefusingTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	tasks := make([]*storage.Task, 0, len(s.stored))
	for _, stored := range s.stored {
		tasks = append(tasks, &stored)
	}
	return tasks, nil
}

func (s *verdictRefusingTaskStore) Update(_ context.Context, task *storage.Task) error {
	isVerdict := state.IsState(task.State, state.Task.Failed) || state.IsState(task.State, state.Task.FailedRetryable)
	if isVerdict && s.refusals != 0 {
		s.refusals--
		s.refused++
		return s.err
	}
	s.stored[task.TaskIdentifier] = *task
	return nil
}

// A sequential drive derives the apply's outcome from the task that failed, so
// it must finalize only over a failure verdict storage recorded. When storage
// refuses the write that rests the first table failed_retryable (or failed),
// the task row still reads pending: finalizing over it would record the apply
// permanently failed and cancel the next table, turning a transient engine
// error into a verdict no retry can undo. The drive exits instead and leaves
// the apply active, both tables still pending, for a later claim to re-drive.
func TestExecuteApplySequential_RefusedFailureVerdictLeavesApplyActive(t *testing.T) {
	for name, eng := range map[string]*failingApplyEngine{
		"retryable engine error": {err: errors.New("connection reset by peer")},
		"engine rejection":       {message: "table line_items is locked by another schema change"},
	} {
		t.Run(name, func(t *testing.T) {
			first := &storage.Task{
				ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
				Database: "orders", TableName: "line_items", State: state.Task.Pending,
				DDL: "ALTER TABLE line_items ADD COLUMN sku VARCHAR(64)",
			}
			second := &storage.Task{
				ID: 2, ApplyID: 1, TaskIdentifier: "task-2",
				Database: "orders", TableName: "shipments", State: state.Task.Pending,
				DDL: "ALTER TABLE shipments ADD COLUMN carrier VARCHAR(64)",
			}
			apply := &storage.Apply{
				ID: 1, ApplyIdentifier: "apply-1", Database: "orders",
				DatabaseType: storage.DatabaseTypeStrata, Environment: "staging", State: state.Apply.Pending,
			}
			applies := &snapshotApplyStore{stored: *apply}
			tasks := newVerdictRefusingTaskStore(errors.New("storage down"), 1, first, second)
			client := &LocalClient{
				config:            LocalConfig{Database: "orders", Type: storage.DatabaseTypeStrata},
				customEngine:      eng,
				heartbeatInterval: time.Hour,
				storage: &exactProgressStorage{
					applies:         applies,
					tasks:           tasks,
					controlRequests: &testControlRequestStore{},
					logs:            &mockApplyLogStore{},
				},
				logger: slog.Default(),
			}

			require.NoError(t, client.executeApplySequential(t.Context(), apply, []*storage.Task{first, second}, &storage.Plan{}, nil),
				"a refused failure verdict ends the drive as a hand-back, not a drive error")

			storedApply, err := applies.Get(t.Context(), apply.ID)
			require.NoError(t, err)
			storedFirst, err := tasks.Get(t.Context(), first.TaskIdentifier)
			require.NoError(t, err)
			storedSecond, err := tasks.Get(t.Context(), second.TaskIdentifier)
			require.NoError(t, err)

			assert.Equal(t, 1, tasks.refused, "the drive attempted the failure verdict once")
			assert.Equal(t, 1, eng.applyCalls, "no further table's DDL starts after the refused verdict")
			assert.Equal(t, state.Apply.Running, storedApply.State, "the apply is not finalized over an unrecorded verdict")
			assert.Nil(t, storedApply.CompletedAt)
			assert.Empty(t, storedApply.ErrorMessage)
			assert.Equal(t, state.Task.Pending, storedFirst.State, "storage still records the first table pending")
			assert.Equal(t, state.Task.Pending, storedSecond.State, "the next table is not cancelled")
			assert.Equal(t, state.Task.Pending, first.State, "the drive's own task does not claim a verdict storage refused")
			assert.Empty(t, first.ErrorMessage)
			assert.Nil(t, first.CompletedAt)
		})
	}
}

// The poll records a permanent progress error as a failed task. When storage
// refuses that write, the task row still reads running, so the poll hands the
// drive back rather than reporting a failure the caller would finalize from.
func TestPollTaskToCompletion_RefusedFailureVerdictExits(t *testing.T) {
	for name, storeErr := range map[string]error{
		"storage blip": errors.New("storage down"),
		"lease lost":   storage.ErrApplyLeaseLost,
	} {
		t.Run(name, func(t *testing.T) {
			eng := &failingProgressEngine{err: engine.NewPermanentError("schema change for orders was dropped by the engine")}
			client, apply, task, recording := lostWorkPollFixture(eng, lostWorkTrustBudgetAmple)
			refusing := &settlementRefusingTaskStore{
				stateRecordingTaskStore: recording,
				err:                     storeErr,
				refusals:                1,
			}
			client.storage.(*exactProgressStorage).tasks = refusing

			action := client.pollTaskToCompletion(t.Context(), apply, task, nil, nil)

			assert.Equal(t, taskAbort, action, "an unrecorded failure verdict is not reported as a task failure")
			assert.Equal(t, 1, refusing.refused)
			assert.Equal(t, 1, eng.calls, "the drive exits at the refused verdict without polling again")
			assert.Equal(t, state.Task.Running, task.State, "the in-memory task is left as stored")
			assert.Empty(t, task.ErrorMessage)
			assert.Nil(t, task.CompletedAt)
			assert.Empty(t, recording.states, "no write landed")
		})
	}
}

// A row security task whose desired schema plan cannot be resolved is failed
// before the engine runs. When storage refuses that failure verdict, the drive
// exits with the apply active instead of finalizing over a task row that still
// reads pending, so the next table is not cancelled.
func TestExecuteApplySequential_RefusedRowSecurityPlanVerdictLeavesApplyActive(t *testing.T) {
	first := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "orders", TableName: "documents", State: state.Task.Pending,
		DDL: "ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY",
	}
	second := &storage.Task{
		ID: 2, ApplyID: 1, TaskIdentifier: "task-2",
		Database: "orders", TableName: "shipments", State: state.Task.Pending,
		DDL: "ALTER TABLE public.shipments ADD COLUMN carrier text",
	}
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-1", Database: "orders",
		DatabaseType: storage.DatabaseTypePostgres, Environment: "staging", State: state.Apply.Pending,
	}
	applies := &snapshotApplyStore{stored: *apply}
	tasks := newVerdictRefusingTaskStore(errors.New("storage down"), 1, first, second)
	eng := &failingApplyEngine{message: "unreachable"}
	client := &LocalClient{
		config:            LocalConfig{Database: "orders", Type: storage.DatabaseTypePostgres},
		postgresEngine:    eng,
		heartbeatInterval: time.Hour,
		storage: &exactProgressStorage{
			applies:         applies,
			tasks:           tasks,
			controlRequests: &testControlRequestStore{},
			logs:            &mockApplyLogStore{},
		},
		logger: slog.Default(),
	}

	require.NoError(t, client.executeApplySequential(t.Context(), apply, []*storage.Task{first, second}, &storage.Plan{}, nil),
		"a refused failure verdict ends the drive as a hand-back, not a drive error")

	storedApply, err := applies.Get(t.Context(), apply.ID)
	require.NoError(t, err)
	storedFirst, err := tasks.Get(t.Context(), first.TaskIdentifier)
	require.NoError(t, err)
	storedSecond, err := tasks.Get(t.Context(), second.TaskIdentifier)
	require.NoError(t, err)
	assert.Equal(t, 1, tasks.refused, "the drive attempted the failure verdict once")
	assert.Equal(t, 0, eng.applyCalls, "the engine never runs for a task whose plan cannot be resolved")
	assert.Equal(t, state.Apply.Running, storedApply.State, "the apply is not finalized over an unrecorded verdict")
	assert.Nil(t, storedApply.CompletedAt)
	assert.Equal(t, state.Task.Pending, storedFirst.State, "storage still records the row security task pending")
	assert.Equal(t, state.Task.Pending, storedSecond.State, "the next table is not cancelled")
	assert.Equal(t, state.Task.Pending, first.State, "the drive's own task does not claim a verdict storage refused")
	assert.Empty(t, first.ErrorMessage)
}

// The sequential poll gives up on a progress outage after a bounded run of
// transient errors and rests the task failed_retryable (or failed, where the
// engine cannot resume from a checkpoint). When storage refuses that verdict
// the poll hands the drive back rather than reporting a failure the caller
// would finalize from.
func TestPollTaskToCompletion_RefusedVerdictAfterPollOutageExits(t *testing.T) {
	for name, dbType := range map[string]string{
		"retryable verdict on a checkpointing engine":  storage.DatabaseTypeStrata,
		"failed verdict on a non-checkpointing engine": "registered-engine",
	} {
		t.Run(name, func(t *testing.T) {
			eng := &failingProgressEngine{err: errors.New("connection reset by peer")}
			client, apply, task, recording := lostWorkPollFixture(eng, lostWorkTrustBudgetAmple)
			client.config.Type = dbType
			refusing := &settlementRefusingTaskStore{
				stateRecordingTaskStore: recording,
				err:                     errors.New("storage down"),
				refusals:                1,
			}
			client.storage.(*exactProgressStorage).tasks = refusing

			action := client.pollTaskToCompletion(t.Context(), apply, task, nil, nil)

			assert.Equal(t, taskAbort, action, "an unrecorded failure verdict is not reported as a task failure")
			assert.Equal(t, 1, refusing.refused)
			assert.Equal(t, maxConsecutiveProgressPollErrors, eng.calls, "the poll spends its whole error budget before the verdict")
			assert.Equal(t, state.Task.Running, task.State, "the in-memory task is left as stored")
			assert.Empty(t, task.ErrorMessage)
			assert.Empty(t, recording.states, "no write landed")
		})
	}
}

// The sequential resume fails a task closed when the re-plan no longer carries
// its reviewed DDL. When storage refuses that failure verdict, the resume exits
// with the apply still running instead of finalizing it failed over a task row
// that never recorded the failure.
func TestResumeApplySequential_RefusedReviewedDDLVerdictLeavesApplyActive(t *testing.T) {
	const (
		emailDDL = "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
		phoneDDL = "ALTER TABLE `users` ADD COLUMN `phone` varchar(255)"
	)
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace:    "testapp",
			TableChanges: []engine.TableChange{{Table: "users", Operation: ddl.StatementAlterTable, DDL: phoneDDL}},
		}},
	})
	c.heartbeatInterval = time.Hour
	c.taskPollIntervalOverride = time.Millisecond
	apply := &storage.Apply{
		ID: 21, ApplyIdentifier: "apply-sequential-unreviewed", Database: "testapp",
		Environment: "staging", State: state.Apply.Running,
	}
	task := &storage.Task{
		ID: 1, ApplyID: apply.ID, TaskIdentifier: "task_email", Database: "testapp",
		Namespace: "testapp", TableName: "users", DDLAction: "alter", DDL: emailDDL,
		State: state.Task.Running,
	}
	taskStore := &updateFailingTaskStore{exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{task}}, updateErr: errors.New("storage down")}
	applies := &snapshotApplyStore{stored: *apply}
	c.storage = &exactProgressStorage{
		plans:           store,
		applies:         applies,
		tasks:           taskStore,
		controlRequests: &testControlRequestStore{},
		logs:            &mockApplyLogStore{},
	}

	require.NoError(t, c.resumeApplySequential(t.Context(), apply, []*storage.Task{task}, &storage.Plan{}, nil),
		"a refused failure verdict ends the resume as a hand-back, not a drive error")

	stored, err := applies.Get(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Running, stored.State, "the resume is not finalized over an unrecorded verdict")
	assert.Nil(t, stored.CompletedAt)
	assert.Equal(t, state.Task.Running, task.State, "the in-memory task is left as stored")
	assert.Empty(t, task.ErrorMessage)
}
