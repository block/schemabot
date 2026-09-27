package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// rereadFailingTaskStore answers every task read from the seeded rows except
// the re-read of one task, which either errors or finds no row. It models a
// storage blip, or a missing row, landing on exactly the read a sequential
// drive makes before starting that task.
type rereadFailingTaskStore struct {
	*exactProgressTaskStore
	failTaskIdentifier string
	getErr             error
}

func (s *rereadFailingTaskStore) Get(ctx context.Context, taskIdentifier string) (*storage.Task, error) {
	if taskIdentifier == s.failTaskIdentifier {
		if s.getErr != nil {
			return nil, s.getErr
		}
		return nil, nil
	}
	return s.exactProgressTaskStore.Get(ctx, taskIdentifier)
}

// rereadFailureCases are the two ways a task re-read can fail to answer: the
// read errors, or it succeeds and finds no row. Both leave the drive unable to
// say whether the task's DDL still needs to run.
var rereadFailureCases = map[string]error{
	"storage error": errors.New("read tasks row: i/o timeout"),
	"missing row":   nil,
}

// A task re-read that cannot answer is reported as an abort, never a skip, and
// the two causes log distinct lines so an operator can tell a storage failure
// from a missing row.
func TestCheckTaskReady_UnansweredRereadAborts(t *testing.T) {
	messages := map[string]string{
		"storage error": "re-reading task state before start failed; current apply owner will exit for operator retry",
		"missing row":   "task row not found when re-reading it before start; current apply owner will exit for operator retry",
	}
	for name, getErr := range rereadFailureCases {
		t.Run(name, func(t *testing.T) {
			task := &storage.Task{TaskIdentifier: "task-1", TableName: "users", State: state.Task.Pending}
			var records []capturedLog
			client := &LocalClient{
				storage: &exactProgressStorage{tasks: &rereadFailingTaskStore{
					exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{task}},
					failTaskIdentifier:     "task-1",
					getErr:                 getErr,
				}},
				logger: slog.New(captureHandler{records: &records}),
			}

			action := client.checkTaskReady(t.Context(), client.logger, task)

			assert.Equal(t, taskAbort, action, "an unanswered re-read ends the drive attempt instead of skipping the task")
			line := requireCapturedLog(t, records, messages[name])
			assert.Equal(t, slog.LevelError, line.level)
			assert.Equal(t, "task-1", line.attrs["task_id"])
			assert.Equal(t, "users", line.attrs["table"])
			assert.Equal(t, state.Task.Pending, line.attrs["state"])
			if getErr != nil {
				assert.Equal(t, getErr, line.attrs["error"])
			} else {
				assert.NotContains(t, line.attrs, "error", "a missing row carries no error to report")
			}
		})
	}
}

// A two-table sequential apply whose first table completes and whose second
// task cannot be re-read before it starts must not be recorded completed: the
// second table's DDL never ran. The drive exits without finalizing, so the
// apply stays running and claimable and the second task stays pending for a
// later drive to re-read and run.
func TestExecuteApplySequential_UnansweredRereadLeavesApplyActive(t *testing.T) {
	for name, getErr := range rereadFailureCases {
		t.Run(name, func(t *testing.T) {
			first := &storage.Task{
				ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
				Database: "orders", TableName: "line_items", State: state.Task.Pending,
				DDL: "ALTER TABLE line_items ADD COLUMN sku VARCHAR(64)",
			}
			second := &storage.Task{
				ID: 2, ApplyID: 1, TaskIdentifier: "task-2",
				Database: "orders", TableName: "invoices", State: state.Task.Pending,
				DDL: "ALTER TABLE invoices ADD COLUMN due_at DATETIME",
			}
			tasks := []*storage.Task{first, second}
			apply := &storage.Apply{
				ID: 1, ApplyIdentifier: "apply-1", Database: "orders",
				Environment: "staging", State: state.Apply.Pending,
			}
			applies := &snapshotApplyStore{stored: *apply}
			eng := &countingEngine{}
			client := &LocalClient{
				config:                   LocalConfig{Database: "orders"},
				customEngine:             eng,
				heartbeatInterval:        time.Hour,
				taskPollIntervalOverride: time.Millisecond,
				storage: &exactProgressStorage{
					applies: applies,
					tasks: &rereadFailingTaskStore{
						exactProgressTaskStore: &exactProgressTaskStore{tasks: tasks},
						failTaskIdentifier:     "task-2",
						getErr:                 getErr,
					},
					controlRequests: &testControlRequestStore{},
					logs:            &mockApplyLogStore{},
				},
				logger: slog.Default(),
			}

			client.executeApplySequential(t.Context(), apply, tasks, &storage.Plan{}, nil)

			assert.Equal(t, 1, eng.applyCalls, "only the first table reaches the engine")
			assert.True(t, state.IsState(first.State, state.Task.Completed), "the first table completes, got %s", first.State)
			assert.Equal(t, state.Task.Pending, second.State, "the unread task stays pending for a later drive")
			stored, err := applies.Get(t.Context(), apply.ID)
			require.NoError(t, err)
			assert.True(t, state.IsState(stored.State, state.Apply.Running),
				"the apply stays running for a later drive to claim; stored state was %q", stored.State)
			assert.Nil(t, stored.CompletedAt, "the apply is not finished, so it is not stamped completed")
		})
	}
}

// A sequential resume holds the same rule: when the second task cannot be
// re-read before it starts, the resume exits without finalizing rather than
// recording the apply completed over a task whose DDL never ran.
func TestResumeApplySequential_UnansweredRereadLeavesApplyActive(t *testing.T) {
	for name, getErr := range rereadFailureCases {
		t.Run(name, func(t *testing.T) {
			logs := &mockApplyLogStore{}
			taskStore := &rereadFailingTaskStore{
				exactProgressTaskStore: &exactProgressTaskStore{},
				failTaskIdentifier:     "task_name",
				getErr:                 getErr,
			}
			c, eng, apply, applies, tasks := newLandedSiblingResume(t, taskStore, logs)
			taskStore.tasks = tasks

			c.resumeApplySequential(t.Context(), apply, tasks, &storage.Plan{}, nil)

			assert.True(t, state.IsState(tasks[0].State, state.Task.Completed),
				"the first task settles before the failed re-read, got %s", tasks[0].State)
			assert.Empty(t, eng.applied, "the task that could not be re-read never reaches the engine")
			assert.True(t, state.IsState(tasks[1].State, state.Task.Running),
				"the unread task keeps its state for a later drive, got %s", tasks[1].State)
			stored, err := applies.Get(t.Context(), apply.ID)
			require.NoError(t, err)
			assert.True(t, state.IsState(stored.State, state.Apply.Running),
				"the apply stays running for a later drive to claim; stored state was %q", stored.State)
			assert.Nil(t, stored.CompletedAt, "the apply is not finished, so it is not stamped completed")
		})
	}
}
