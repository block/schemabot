package tern

import (
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// driveLoggerTestApply returns an apply carrying the full identity set so
// tests can prove drive log lines inherit it from the bound logger.
func driveLoggerTestApply(applyState string) *storage.Apply {
	return &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-identity",
		Database: "cdb_resolute", DatabaseType: storage.DatabaseTypeStrata,
		Environment: "staging", State: applyState,
		Repository: "org/repo", PullRequest: 123,
	}
}

// assertLogCarriesApplyIdentity asserts a captured drive log line inherits the
// apply's identity attrs from the bound logger.
func assertLogCarriesApplyIdentity(t *testing.T, line capturedLog) {
	t.Helper()
	assert.Equal(t, "apply-identity", line.attrs["apply_id"])
	assert.Equal(t, "cdb_resolute", line.attrs["database"])
	assert.Equal(t, storage.DatabaseTypeStrata, line.attrs["database_type"])
	assert.Equal(t, "staging", line.attrs["environment"])
	assert.Equal(t, "org/repo", line.attrs["repo"])
	assert.Equal(t, int64(123), line.attrs["pr"])
}

// Sequential finalization binds the apply's identity to a drive-scoped logger,
// so the line reporting that a concurrently-settled apply is being adopted
// carries apply_id, repo, and pr without the call site hand-listing them.
func TestFinalizeSequentialApply_LogsCarryApplyIdentity(t *testing.T) {
	stored := driveLoggerTestApply(state.Apply.Completed)
	var records []capturedLog
	client := &LocalClient{
		storage: &mockStorage{applies: &mockApplyStore{apply: stored}},
		logger:  slog.New(captureHandler{records: &records}),
	}

	running := driveLoggerTestApply(state.Apply.Running)
	client.finalizeSequentialApply(t.Context(), running, nil, nil, false)

	line := requireCapturedLog(t, records, "apply already terminal in storage, not overwriting during sequential finalization")
	assertLogCarriesApplyIdentity(t, line)
	assert.Equal(t, state.Apply.Completed, line.attrs["stored_state"],
		"the adopt line must report the stored terminal state")
	assert.Equal(t, state.Apply.Completed, running.State,
		"the in-memory apply must adopt the stored terminal state")
}

// Task readiness checks receive the caller's identity-bound drive logger, so
// the line explaining why a task was skipped carries apply_id, repo, and pr
// alongside the task's own attributes.
func TestCheckTaskReady_LogsCarryApplyIdentity(t *testing.T) {
	task := &storage.Task{TaskIdentifier: "task-1", TableName: "users", State: state.Task.Completed}
	var records []capturedLog
	client := &LocalClient{
		storage: &mockStorage{tasks: &mockTaskStore{getTask: task}},
		logger:  slog.New(captureHandler{records: &records}),
	}

	apply := driveLoggerTestApply(state.Apply.Running)
	logger := client.logger.With(apply.IdentityLogAttrs()...)
	action := client.checkTaskReady(t.Context(), logger, task)

	assert.Equal(t, taskSkip, action)
	line := requireCapturedLog(t, records, "task already in terminal state, skipping")
	assertLogCarriesApplyIdentity(t, line)
	assert.Equal(t, "task-1", line.attrs["task_id"])
	assert.Equal(t, "users", line.attrs["table"])
	assert.Equal(t, state.Task.Completed, line.attrs["state"])
}

// Failing an apply binds the apply's identity to the failure logger, so the
// line reporting that storage already holds a terminal verdict carries
// apply_id, repo, and pr while the mutable state stays a per-call snapshot.
func TestFailApplyWithTasks_TerminalLogCarriesApplyIdentity(t *testing.T) {
	stored := driveLoggerTestApply(state.Apply.Cancelled)
	var records []capturedLog
	client := &LocalClient{
		storage: &mockStorage{applies: &mockApplyStore{apply: stored}},
		logger:  slog.New(captureHandler{records: &records}),
	}

	running := driveLoggerTestApply(state.Apply.Running)
	client.failApplyWithTasks(t.Context(), running, nil, "engine failure")

	line := requireCapturedLog(t, records, "apply already in terminal state, not overwriting")
	assertLogCarriesApplyIdentity(t, line)
	assert.Equal(t, state.Apply.Cancelled, line.attrs["state"],
		"the log must snapshot the stored terminal state at emission time")
	assert.Equal(t, state.Apply.Cancelled, running.State,
		"the in-memory apply must adopt the stored terminal state")
}

// Grouped aggregate projection keeps an ambiguous task-set warning tied to the
// apply whose state cannot safely be derived.
func TestDeriveAggregateApplyState_AmbiguousTasksLogCarriesApplyIdentity(t *testing.T) {
	var records []capturedLog
	client := &LocalClient{
		logger: slog.New(captureHandler{records: &records}),
	}
	firstOperationID := int64(11)
	secondOperationID := int64(12)
	tasks := []*storage.Task{
		{ApplyOperationID: &firstOperationID, State: state.Task.Completed},
		{ApplyOperationID: &secondOperationID, State: state.Task.Completed},
	}

	derived, ok := client.deriveAggregateApplyState(t.Context(), driveLoggerTestApply(state.Apply.Running), tasks)

	require.False(t, ok)
	require.Empty(t, derived)
	line := requireCapturedLog(t, records, "cannot determine aggregate apply state: task set is not scoped to one apply operation")
	assertLogCarriesApplyIdentity(t, line)
	assert.Error(t, line.attrs["error"].(error))
}

// Grouped aggregate projection identifies the apply when no sibling operation
// rows are available and the non-terminal task state remains the safe fallback.
func TestDeriveAggregateApplyState_MissingOperationRowsLogCarriesApplyIdentity(t *testing.T) {
	var records []capturedLog
	client := &LocalClient{
		storage: &mockStorage{},
		logger:  slog.New(captureHandler{records: &records}),
	}
	operationID := int64(11)
	tasks := []*storage.Task{{ApplyOperationID: &operationID, State: state.Task.Running}}

	derived, ok := client.deriveAggregateApplyState(t.Context(), driveLoggerTestApply(state.Apply.Running), tasks)

	require.True(t, ok)
	require.Equal(t, state.Apply.Running, derived)
	line := requireCapturedLog(t, records, "cannot determine aggregate apply state: tasks reference an apply operation but no operation rows were found")
	assertLogCarriesApplyIdentity(t, line)
}
