package tern

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// capturingApplyLogStore records every entry written to an apply's log stream.
type capturingApplyLogStore struct {
	storage.ApplyLogStore
	entries []*storage.ApplyLog
}

func (s *capturingApplyLogStore) Append(_ context.Context, entry *storage.ApplyLog) error {
	s.entries = append(s.entries, entry)
	return nil
}

func newFailureLogTestClient(apply *storage.Apply, tasks []*storage.Task) (*LocalClient, *capturingApplyLogStore) {
	logs := &capturingApplyLogStore{}
	return &LocalClient{
		config: LocalConfig{Database: "testdb", Type: storage.DatabaseTypeMySQL},
		storage: &controlTestStorage{
			applies:         &controlTestApplyStore{apply: apply},
			tasks:           &controlTestTaskStore{tasks: tasks},
			applyLogs:       logs,
			controlRequests: &testControlRequestStore{},
		},
		logger: slog.Default(),
	}, logs
}

func failureLogTestApply(applyState string, attempt int) *storage.Apply {
	return &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-7",
		Database:        "orders",
		Environment:     "staging",
		State:           applyState,
		Attempt:         attempt,
	}
}

// The apply log is the only failure record an operator reads from the CLI or
// the PR summary comment, so an apply that fails must say so there — not only
// in the server logs, where a reader of the apply's own history would see it
// reach a terminal state with nothing stating why.
func TestFailApplyWithTasksRecordsTheFailureInTheApplyLog(t *testing.T) {
	apply := failureLogTestApply(state.Apply.Running, 0)
	task := &storage.Task{ID: 1, TaskIdentifier: "task-1", ApplyID: apply.ID, TableName: "orders", State: state.Task.Running}
	client, logs := newFailureLogTestClient(apply, []*storage.Task{task})

	client.failApplyWithTasks(t.Context(), apply, []*storage.Task{task}, "engine lost its connection to the target")

	require.Len(t, logs.entries, 1)
	entry := logs.entries[0]
	assert.Equal(t, storage.LogLevelError, entry.Level)
	assert.Equal(t, storage.LogEventError, entry.EventType)
	assert.Equal(t, "Apply failed: engine lost its connection to the target", entry.Message)
	assert.Equal(t, state.Apply.Running, entry.OldState)
	assert.Equal(t, state.Apply.Failed, entry.NewState)
}

// A retryable failure is the state operator recovery keeps re-driving, so each
// paused attempt records the budget it spent. Without it the apply log shows
// only the gaps between attempts, and the retry budget drains invisibly. The
// count reported is the apply's own attempt counter, so it can never exceed the
// budget it is measured against.
func TestMarkApplyRetryableWithTasksRecordsTheSpentAttempt(t *testing.T) {
	apply := failureLogTestApply(state.Apply.Running, 3)
	task := &storage.Task{ID: 1, TaskIdentifier: "task-1", ApplyID: apply.ID, TableName: "orders", State: state.Task.Running}
	client, logs := newFailureLogTestClient(apply, []*storage.Task{task})

	client.markApplyRetryableWithTasks(t.Context(), apply, []*storage.Task{task}, "target refused the connection")

	require.Len(t, logs.entries, 1)
	entry := logs.entries[0]
	assert.Equal(t, storage.LogLevelWarn, entry.Level, "a paused attempt is not yet a permanent failure")
	assert.Contains(t, entry.Message, "3 of 10 recovery attempts used")
	assert.Contains(t, entry.Message, "target refused the connection")
	assert.Equal(t, state.Apply.Running, entry.OldState)
	assert.Equal(t, state.Apply.FailedRetryable, entry.NewState)
}

// The last drive operator recovery is allowed to make still reports a count
// inside the budget, so an operator reading the apply log sees the retry budget
// reach its limit rather than a figure that exceeds it.
func TestMarkApplyRetryableWithTasksReportsTheFinalAttemptWithinBudget(t *testing.T) {
	apply := failureLogTestApply(state.Apply.Running, storage.MaxRecoveryAttempts)
	client, logs := newFailureLogTestClient(apply, nil)

	client.markApplyRetryableWithTasks(t.Context(), apply, nil, "target refused the connection")

	require.Len(t, logs.entries, 1)
	assert.Contains(t, logs.entries[0].Message,
		fmt.Sprintf("%d of %d recovery attempts used", storage.MaxRecoveryAttempts, storage.MaxRecoveryAttempts))
}

// An apply that another driver already settled is not this drive's to fail: the
// stored verdict stands, and writing a second failure record would report a
// state transition that never happened.
func TestFailApplyWithTasksLeavesASettledApplyUnrecorded(t *testing.T) {
	apply := failureLogTestApply(state.Apply.Cancelled, 0)
	client, logs := newFailureLogTestClient(apply, nil)

	client.failApplyWithTasks(t.Context(), apply, nil, "engine lost its connection to the target")

	assert.Empty(t, logs.entries)
	assert.Equal(t, state.Apply.Cancelled, apply.State)
}
