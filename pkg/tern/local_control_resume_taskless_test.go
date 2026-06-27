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

	err := client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Completed, apply.State,
		"a task-less no-op apply must complete on recovery, not be marked failed")
	assert.NotEqual(t, state.Apply.Failed, apply.State)
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
