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

func countLogKey(keys []string, want string) int {
	var count int
	for _, key := range keys {
		if key == want {
			count++
		}
	}
	return count
}

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

// rereadFailureCase is one way a task re-read can fail to answer, with the
// action the drive must take and the error a sequential resume must report.
type rereadFailureCase struct {
	getErr    error
	action    taskAction
	message   string
	resumeErr error
}

// rereadFailureCases are the two ways a task re-read can fail to answer: the
// read errors, or it succeeds and finds no row. Both leave the drive unable to
// say whether the task's DDL still needs to run. A storage error is transient,
// so the drive aborts and leaves the apply for a later drive; a missing row
// cannot heal on its own, so the resume reports the apply undriveable.
var rereadFailureCases = map[string]rereadFailureCase{
	"storage error": {
		getErr:    errors.New("read tasks row: i/o timeout"),
		action:    taskAbort,
		message:   "re-reading task state before start failed; current apply owner will exit for operator retry",
		resumeErr: nil,
	},
	"missing row": {
		getErr:    nil,
		action:    taskMissing,
		message:   "task row not found when re-reading it before start; the apply is undriveable and stays claimable until the row is restored or an operator intervenes",
		resumeErr: ErrApplyTaskRowMissing,
	},
}

// A task re-read that cannot answer never skips the task: a storage error
// aborts the drive attempt and a missing row reports the apply undriveable. The
// two causes log distinct lines so an operator can tell them apart.
func TestCheckTaskReady_UnansweredRereadNeverSkips(t *testing.T) {
	for name, tc := range rereadFailureCases {
		t.Run(name, func(t *testing.T) {
			task := &storage.Task{TaskIdentifier: "task-1", TableName: "users", State: state.Task.Pending}
			apply := driveLoggerTestApply(state.Apply.Running)
			var records []capturedLog
			client := &LocalClient{
				storage: &exactProgressStorage{tasks: &rereadFailingTaskStore{
					exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{task}},
					failTaskIdentifier:     "task-1",
					getErr:                 tc.getErr,
				}},
				logger: slog.New(captureHandler{records: &records}),
			}

			logger := client.logger.With(apply.IdentityLogAttrs()...)
			action := client.checkTaskReady(t.Context(), logger, task)

			assert.Equal(t, tc.action, action, "an unanswered re-read ends the drive attempt instead of skipping the task")
			assert.NotEqual(t, taskSkip, action)
			line := requireCapturedLog(t, records, tc.message)
			assert.Equal(t, slog.LevelError, line.level)
			assert.Equal(t, "task-1", line.attrs["task_id"])
			assert.Equal(t, "users", line.attrs["table"])
			assert.Equal(t, state.Task.Pending, line.attrs["state"])
			assertLogCarriesApplyIdentity(t, line)
			for _, key := range []string{"apply_id", "database", "database_type", "environment", "repo", "pr"} {
				assert.Equal(t, 1, countLogKey(line.keys, key), "identity attribute %q must be emitted once", key)
			}
			if tc.getErr != nil {
				assert.Equal(t, tc.getErr, line.attrs["error"])
			} else {
				assert.NotContains(t, line.attrs, "error", "a missing row carries no error to report")
			}
		})
	}
}

type cancellingTaskStore struct {
	*exactProgressTaskStore
	cancel context.CancelFunc
}

func (s *cancellingTaskStore) Get(ctx context.Context, _ string) (*storage.Task, error) {
	s.cancel()
	return nil, ctx.Err()
}

// Cancellation while the task re-read is in flight hands the active apply to
// another driver without reporting the expected ownership handover as a
// storage failure.
func TestCheckTaskReady_CancelledDuringRereadHandsOver(t *testing.T) {
	task := &storage.Task{TaskIdentifier: "task-1", TableName: "users", State: state.Task.Pending}
	ctx, cancel := context.WithCancel(t.Context())
	var records []capturedLog
	client := &LocalClient{
		storage: &exactProgressStorage{tasks: &cancellingTaskStore{
			exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{task}},
			cancel:                 cancel,
		}},
		logger: slog.New(captureHandler{records: &records}),
	}
	apply := driveLoggerTestApply(state.Apply.Running)
	logger := client.logger.With(apply.IdentityLogAttrs()...)

	action := client.checkTaskReady(ctx, logger, task)

	assert.Equal(t, taskHandover, action)
	line := requireCapturedLog(t, records, "drive context cancelled before task start; handing the apply back for another driver to claim")
	assert.Equal(t, slog.LevelInfo, line.level)
	assertLogCarriesApplyIdentity(t, line)
	assert.Equal(t, "task-1", line.attrs["task_id"])
	assert.Equal(t, "users", line.attrs["table"])
}

// A two-table sequential apply whose first table completes and whose second
// task cannot be re-read before it starts must not be recorded completed: the
// second table's DDL never ran. The drive exits without finalizing, so the
// apply stays running and claimable and the second task stays pending for a
// later drive to re-read and run.
func TestExecuteApplySequential_UnansweredRereadLeavesApplyActive(t *testing.T) {
	for name, tc := range rereadFailureCases {
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
						getErr:                 tc.getErr,
					},
					controlRequests: &testControlRequestStore{},
					logs:            &mockApplyLogStore{},
				},
				logger: slog.Default(),
			}

			require.NoError(t, client.executeApplySequential(t.Context(), apply, tasks, &storage.Plan{}, nil))

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
// recording the apply completed over a task whose DDL never ran. A storage
// error returns nil so the apply is simply re-driven later; a missing row
// returns ErrApplyTaskRowMissing so the operator refuses to derive an
// operation verdict from the rows that remain.
func TestResumeApplySequential_UnansweredRereadLeavesApplyActive(t *testing.T) {
	for name, tc := range rereadFailureCases {
		t.Run(name, func(t *testing.T) {
			logs := &mockApplyLogStore{}
			taskStore := &rereadFailingTaskStore{
				exactProgressTaskStore: &exactProgressTaskStore{},
				failTaskIdentifier:     "task_name",
				getErr:                 tc.getErr,
			}
			c, eng, apply, applies, tasks := newLandedSiblingResume(t, taskStore, logs)
			taskStore.tasks = tasks

			err := c.resumeApplySequential(t.Context(), apply, tasks, &storage.Plan{}, nil)

			if tc.resumeErr == nil {
				require.NoError(t, err, "a transient storage error is not reported as an undriveable apply")
			} else {
				require.ErrorIs(t, err, tc.resumeErr, "a vanished task row is reported so the operator refuses to project a verdict")
				assert.Contains(t, err.Error(), "task_name", "the error names the task whose row is gone")
			}
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
