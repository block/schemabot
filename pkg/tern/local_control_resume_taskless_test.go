package tern

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A task-less apply recovered by the operator is a no-op — e.g. a sharded
// dispatch for a shard whose schema already matches the desired state, or an
// apply whose tasks already completed. The initial drive completes such an apply
// (finalizeSequentialApply with no failed task), so recovery must complete it too
// rather than failing it with "no tasks found during recovery". (A VSchema-only
// plan is handled separately so its VSchema is still applied.)
func TestResumeApply_TasklessNoOpCompletesNotFails(t *testing.T) {
	apply := &storage.Apply{
		ID: 1, PlanID: 7, ApplyIdentifier: "apply-noop",
		Database: "cdb_resolute", DatabaseType: storage.DatabaseTypeStrata,
		Environment: "production", State: state.Apply.Pending,
	}
	client := &LocalClient{
		config: LocalConfig{Database: "cdb_resolute", Type: storage.DatabaseTypeStrata},
		storage: &mockStorage{
			applies: &mockApplyStore{apply: apply},
			tasks:   &mockTaskStore{}, // no tasks for the apply
			// A plan with no VSchema artifact, so it is not a VSchema-only plan —
			// the exact case the recovery guard used to fail.
			plans: &mockPlanStore{plan: &storage.Plan{ID: 7, PlanIdentifier: "plan-noop"}},
		},
		logger: slog.Default(),
	}
	reader := newTernMetricsReader(t)

	err := client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Completed, apply.State,
		"a task-less no-op apply must complete on recovery, not be marked failed")
	assert.Equal(t, int64(-1), activeAppliesDelta(t, reader), "completing the apply releases it from the active-apply gauge")
	assert.NotEqual(t, state.Apply.Failed, apply.State)
}

// Recovery of a task-less apply re-reads it before completing it, so a stop or
// a concurrent drive's outcome that landed first is kept. When that re-read
// fails, recovery returns the read error and writes nothing: completing over a
// row it could not read would let the completion land on a stopped apply.
func TestResumeApply_TasklessNoOpFailedReReadWritesNothing(t *testing.T) {
	apply := &storage.Apply{
		ID: 1, PlanID: 7, ApplyIdentifier: "apply-noop",
		Database: "cdb_resolute", DatabaseType: storage.DatabaseTypeStrata,
		Environment: "production", State: state.Apply.Pending,
	}
	getErr := errors.New("connection reset by peer")
	inner := &mockApplyStore{apply: &storage.Apply{ID: 1, ApplyIdentifier: "apply-noop", State: state.Apply.Stopped}}
	client := &LocalClient{
		config: LocalConfig{Database: "cdb_resolute", Type: storage.DatabaseTypeStrata},
		storage: &exactProgressStorage{
			applies: &reReadFailingApplyStore{mockApplyStore: inner, getErr: getErr},
			tasks:   &mockTaskStore{},
			plans:   &mockPlanStore{plan: &storage.Plan{ID: 7, PlanIdentifier: "plan-noop"}},
		},
		logger: slog.Default(),
	}

	err := client.ResumeApply(t.Context(), apply)

	require.ErrorIs(t, err, getErr)
	assert.Contains(t, err.Error(), "complete task-less apply apply-noop", "the error comes from the completion's re-read and names the apply")
	assert.Empty(t, inner.updates, "no completion is written without knowing the stored state")
	assert.Equal(t, state.Apply.Stopped, inner.apply.State)
	assert.NotEqual(t, state.Apply.Completed, apply.State, "the in-memory apply is not marked completed")
}

// A local resume binds the apply's identity to a drive-scoped logger, so every
// log line of the resume — including call sites that pass no identity attrs at
// all, such as the task-less no-op completion — carries apply_id, repo, and pr
// and is filterable by them without each call hand-listing the identifiers.
func TestResumeApply_DriveLogsCarryApplyIdentity(t *testing.T) {
	apply := &storage.Apply{
		ID: 1, PlanID: 7, ApplyIdentifier: "apply-identity",
		Database: "cdb_resolute", DatabaseType: storage.DatabaseTypeStrata,
		Environment: "staging", State: state.Apply.Pending,
		Repository: "org/repo", PullRequest: 123,
	}
	var records []capturedLog
	client := &LocalClient{
		config: LocalConfig{Database: "cdb_resolute", Type: storage.DatabaseTypeStrata},
		storage: &mockStorage{
			applies: &mockApplyStore{apply: apply},
			tasks:   &mockTaskStore{}, // no tasks: the no-op completion line fires
			plans:   &mockPlanStore{plan: &storage.Plan{ID: 7, PlanIdentifier: "plan-identity"}},
		},
		logger: slog.New(captureHandler{records: &records}),
	}

	require.NoError(t, client.ResumeApply(t.Context(), apply))

	// The call site passes only the message — every identity attr must come
	// from the bound logger.
	line := requireCapturedLog(t, records, "no tasks found for apply during recovery; completing as a no-op")
	assert.Equal(t, "apply-identity", line.attrs["apply_id"])
	assert.Equal(t, "cdb_resolute", line.attrs["database"])
	assert.Equal(t, storage.DatabaseTypeStrata, line.attrs["database_type"])
	assert.Equal(t, "staging", line.attrs["environment"])
	assert.Equal(t, "org/repo", line.attrs["repo"])
	assert.Equal(t, int64(123), line.attrs["pr"])
	assert.NotContains(t, line.attrs, "state",
		"mutable state must not be frozen into the bound drive logger")
}

// If persisting the no-op completion fails, recovery surfaces the error so it
// retries — rather than reporting a completion that was never written.
func TestResumeApply_TasklessNoOpUpdateErrorReturnsError(t *testing.T) {
	apply := &storage.Apply{
		ID: 1, PlanID: 7, ApplyIdentifier: "apply-noop",
		Database: "cdb_resolute", DatabaseType: storage.DatabaseTypeStrata,
		Environment: "production", State: state.Apply.Pending,
	}
	client := &LocalClient{
		config: LocalConfig{Database: "cdb_resolute", Type: storage.DatabaseTypeStrata},
		storage: &mockStorage{
			applies: &mockApplyStore{apply: apply, updateErr: errors.New("db down")},
			tasks:   &mockTaskStore{},
			plans:   &mockPlanStore{plan: &storage.Plan{ID: 7, PlanIdentifier: "plan-noop"}},
		},
		logger: slog.Default(),
	}

	err := client.ResumeApply(t.Context(), apply)
	require.Error(t, err, "a failed completion write must be surfaced, not swallowed as success")
	assert.Contains(t, err.Error(), "complete task-less apply")
}
