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

const groupedStartResumeDDL = "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"

// replanSequenceEngine answers each re-plan with the next scripted result,
// holding the last one, and answers an engine apply with applyErr when it is
// set. It counts the applies it is handed so a test can assert whether the
// drive reached the engine.
type replanSequenceEngine struct {
	engine.Engine
	plans    []*engine.PlanResult
	planned  int
	applyErr error
	applied  int
}

func (e *replanSequenceEngine) Name() string { return "spirit" }

func (e *replanSequenceEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	result := e.plans[min(e.planned, len(e.plans)-1)]
	e.planned++
	return result, nil
}

func (e *replanSequenceEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.applied++
	if e.applyErr != nil {
		return nil, e.applyErr
	}
	return &engine.ApplyResult{Accepted: true}, nil
}

// usersEmailChange is a re-plan that still finds the reviewed ALTER pending.
func usersEmailChange() *engine.PlanResult {
	return &engine.PlanResult{Changes: []engine.SchemaChange{{
		Namespace:    "testapp",
		TableChanges: []engine.TableChange{{Table: "users", Operation: ddl.StatementAlterTable, DDL: groupedStartResumeDDL}},
	}}}
}

// startAnswerRefusingStore serves the wrapped store but fails the write that
// answers a start request, modelling a storage outage that begins after the
// apply's outcome landed and before the operator's start could be answered.
type startAnswerRefusingStore struct {
	*testControlRequestStore
	err error
}

func (s *startAnswerRefusingStore) CompletePending(ctx context.Context, applyID int64, operation storage.ControlOperation) error {
	if operation == storage.ControlOperationStart {
		return s.err
	}
	return s.testControlRequestStore.CompletePending(ctx, applyID, operation)
}

// groupedStartResume is a stopped grouped apply an operator has started
// again, claimed for the resume that answers the start.
type groupedStartResume struct {
	client   *LocalClient
	apply    *storage.Apply
	task     *storage.Task
	applies  *snapshotApplyStore
	requests *testControlRequestStore
	observer *terminalRecordingObserver
	logs     *mockApplyLogStore
	records  *[]capturedLog
}

// newGroupedStartResume builds the resume over one stopped task. The claim has
// already moved the stored row to resuming while the drive holds the stopped
// snapshot it was claimed from, as a start claim does. wrapRequests lets a
// test fault-inject the control request store.
func newGroupedStartResume(t *testing.T, eng *replanSequenceEngine, wrapRequests func(*testControlRequestStore) storage.ControlRequestStore) *groupedStartResume {
	t.Helper()
	startedAt := time.Now().Add(-time.Hour)
	apply := &storage.Apply{
		ID:              21,
		ApplyIdentifier: "apply-3f9a",
		PlanID:          5,
		Database:        "testapp",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		Repository:      "octo/app",
		PullRequest:     42,
		State:           state.Apply.Stopped,
		StartedAt:       &startedAt,
		Options:         []byte(`{"defer_cutover":true}`),
	}
	task := &storage.Task{
		ID:             1,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-users-email",
		Database:       "testapp",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            groupedStartResumeDDL,
		State:          state.Task.Stopped,
	}
	plan := &storage.Plan{
		ID:             apply.PlanID,
		PlanIdentifier: "plan-5",
		Namespaces:     map[string]*storage.NamespacePlanData{"testapp": {}},
	}
	stored := *apply
	stored.State = state.Apply.Resuming
	applies := &snapshotApplyStore{stored: stored}
	requests := pendingControlRequestStore(apply.ID, storage.ControlOperationStart)
	logs := &mockApplyLogStore{}
	var records []capturedLog
	client := &LocalClient{
		config: LocalConfig{
			Database:  "testapp",
			Type:      storage.DatabaseTypeMySQL,
			TargetDSN: "user:pass@tcp(127.0.0.1:3306)/testapp",
		},
		spiritEngine: eng,
		storage: &exactProgressStorage{
			applies:         applies,
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			logs:            logs,
			controlRequests: wrapRequests(requests),
			plans:           &fakePlanStore{getByIDFn: func(int64) (*storage.Plan, error) { return plan, nil }},
		},
		logger:            slog.New(captureHandler{records: &records}),
		heartbeatInterval: time.Hour,
	}
	observer := &terminalRecordingObserver{}
	client.SetObserver(apply.ID, observer)
	return &groupedStartResume{
		client:   client,
		apply:    apply,
		task:     task,
		applies:  applies,
		requests: requests,
		observer: observer,
		logs:     logs,
		records:  &records,
	}
}

func (r *groupedStartResume) resume(t *testing.T) error {
	t.Helper()
	return r.client.resumeApplyWithTasks(t.Context(), r.apply, nil, []*storage.Task{r.task}, r.apply.GetOptions().Map(), false, false)
}

// An operator stopped a grouped apply and then started it again. The resume's
// first re-plan still finds the change, but the re-plan the grouped resume
// runs before reattaching to the engine finds the live schema already matching
// the reviewed target, so the resume records the apply completed. Storage then
// refuses the write answering the operator's start. The completed outcome is
// stored, so it stands: the apply and its task stay completed, the completed
// summary posts once, nothing records a failure over it, and the drive returns
// the reconciliation it could not finish, with a warning that names the apply.
func TestResumeApplyWithTasks_GroupedStartAnswerFailureKeepsTheStoredOutcome(t *testing.T) {
	startErr := errors.New("control request store unavailable")
	eng := &replanSequenceEngine{plans: []*engine.PlanResult{usersEmailChange(), {}}}
	r := newGroupedStartResume(t, eng, func(requests *testControlRequestStore) storage.ControlRequestStore {
		return &startAnswerRefusingStore{testControlRequestStore: requests, err: startErr}
	})

	err := r.resume(t)

	var reconcileErr *postTerminalReconcileError
	require.ErrorAs(t, err, &reconcileErr, "a failure after the stored outcome surfaces as post-terminal reconciliation")
	require.ErrorIs(t, err, startErr)
	assert.Equal(t, state.Apply.Completed, reconcileErr.storedState)

	assert.Equal(t, state.Apply.Completed, r.applies.stored.State, "the stored outcome stands")
	assert.NotNil(t, r.applies.stored.CompletedAt)
	assert.Empty(t, r.applies.stored.ErrorMessage, "no failure reason is recorded over the completed outcome")
	assert.Equal(t, state.Task.Completed, r.task.State, "the task keeps its completed verdict")
	assert.Empty(t, r.task.ErrorMessage)
	assert.Zero(t, eng.applied, "an apply with no remaining work hands nothing to the engine")

	require.Len(t, r.requests.requests, 1)
	assert.Equal(t, storage.ControlRequestPending, r.requests.requests[0].Status,
		"the start whose answer storage refused stays pending")

	require.Len(t, r.observer.terminal, 1, "the completed summary posts exactly once")
	assert.Equal(t, state.Apply.Completed, r.observer.terminal[0].State)
	assert.False(t, hasLogMessageContaining(r.logs.logs, "Apply failed"),
		"the apply log records no failure for a completed apply")

	warning := requireCapturedLog(t, *r.records,
		"grouped resume stored the apply's terminal outcome but could not reconcile after it; the stored outcome stands, no failure is recorded over it, and the current apply owner will exit with the error")
	assert.Equal(t, slog.LevelWarn, warning.level)
	assert.Equal(t, "apply-3f9a", warning.attrs["apply_id"])
	assert.Equal(t, "testapp", warning.attrs["database"])
	assert.Equal(t, "staging", warning.attrs["environment"])
	assert.Equal(t, "octo/app", warning.attrs["repo"])
	assert.EqualValues(t, 42, warning.attrs["pr"])
	assert.Equal(t, state.Apply.Completed, warning.attrs["state"])
	assert.Equal(t, err, warning.attrs["error"])
	for _, rec := range *r.records {
		assert.NotEqual(t, "engine apply failed during recovery", rec.msg,
			"a stored outcome is never reported as an engine failure")
	}
}

// The same restarted grouped apply, but the change is still pending when the
// grouped resume hands it to the engine, and the engine refuses it
// permanently. Nothing was stored for the apply before the refusal, so the
// refusal is the schema change's outcome: the apply and its task are recorded
// failed with the engine's reason, the operator's start is answered with that
// failure, and the failed summary posts once.
func TestResumeApplyWithTasks_GroupedEngineRefusalBeforeAnOutcomeFailsTheApply(t *testing.T) {
	engineErr := engine.NewPermanentError("duplicate column name 'email'")
	eng := &replanSequenceEngine{plans: []*engine.PlanResult{usersEmailChange()}, applyErr: engineErr}
	r := newGroupedStartResume(t, eng, func(requests *testControlRequestStore) storage.ControlRequestStore {
		return requests
	})

	err := r.resume(t)

	require.ErrorIs(t, err, engineErr)
	var reconcileErr *postTerminalReconcileError
	assert.NotErrorAs(t, err, &reconcileErr, "an engine refusal before any outcome is stored is the schema change's failure")
	assert.Equal(t, 1, eng.applied)

	assert.Equal(t, state.Apply.Failed, r.applies.stored.State)
	assert.Contains(t, r.applies.stored.ErrorMessage, "duplicate column name 'email'")
	assert.Equal(t, state.Task.Failed, r.task.State)
	assert.Contains(t, r.task.ErrorMessage, "duplicate column name 'email'")

	require.Len(t, r.requests.requests, 1)
	assert.Equal(t, storage.ControlRequestFailed, r.requests.requests[0].Status,
		"the start is answered with the failure it ran into")
	assert.Contains(t, r.requests.requests[0].ErrorMessage, "duplicate column name 'email'")

	require.Len(t, r.observer.terminal, 1, "the failed summary posts exactly once")
	assert.Equal(t, state.Apply.Failed, r.observer.terminal[0].State)
	assert.True(t, hasLogMessageContaining(r.logs.logs, "Apply failed"),
		"the apply log records the failure")
}
