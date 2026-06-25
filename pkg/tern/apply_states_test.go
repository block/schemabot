package tern

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
)

// TestDeriveApplyPhase verifies that engine events with structured state
// produce the correct transitions, and events without a state produce none.
func TestDeriveApplyPhase(t *testing.T) {
	tests := []struct {
		name         string
		event        engine.ApplyEvent
		wantState    string
		wantNoChange bool
	}{
		{
			name: "preparing branch transitions to preparing_branch",
			event: engine.ApplyEvent{
				Message:  "Creating branch schemabot-boardgames-123",
				NewState: state.Apply.PreparingBranch,
			},
			wantState: state.Apply.PreparingBranch,
		},
		{
			name: "reusing branch transitions to preparing_branch",
			event: engine.ApplyEvent{
				Message:  "Reusing branch dr-branch-reuse",
				NewState: state.Apply.PreparingBranch,
			},
			wantState: state.Apply.PreparingBranch,
		},
		{
			name: "branch ready transitions to applying_branch_changes",
			event: engine.ApplyEvent{
				Message:  "Branch schemabot-boardgames-123 ready (44s)",
				NewState: state.Apply.ApplyingBranchChanges,
			},
			wantState: state.Apply.ApplyingBranchChanges,
		},
		{
			name: "branch schema refreshed transitions to applying_branch_changes",
			event: engine.ApplyEvent{
				Message:  "Branch dr-branch-reuse schema refreshed (5s)",
				NewState: state.Apply.ApplyingBranchChanges,
			},
			wantState: state.Apply.ApplyingBranchChanges,
		},
		{
			name: "applying changes transitions to applying_branch_changes",
			event: engine.ApplyEvent{
				Message:  "Applying changes to 33 keyspaces on branch dr-branch-reuse",
				NewState: state.Apply.ApplyingBranchChanges,
			},
			wantState: state.Apply.ApplyingBranchChanges,
		},
		{
			name: "DDL applied transitions to creating_deploy_request",
			event: engine.ApplyEvent{
				Message:  "Applied 3 DDL changes to branch schemabot-commerce-456",
				NewState: state.Apply.CreatingDeployRequest,
			},
			wantState: state.Apply.CreatingDeployRequest,
		},
		{
			name: "applied keyspace — no transition",
			event: engine.ApplyEvent{
				Message:  "Applied keyspace commerce_sharded_015 (12/33)",
				Metadata: map[string]string{"keyspace": "commerce_sharded_015"},
			},
			wantNoChange: true,
		},
		{
			name:         "empty event — no transition",
			event:        engine.ApplyEvent{Message: "some log line"},
			wantNoChange: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newState := deriveApplyPhase(tt.event)

			if tt.wantNoChange {
				assert.Empty(t, newState, "expected no state change for %q", tt.event.Message)
			} else {
				assert.Equal(t, tt.wantState, newState, "wrong state for %q", tt.event.Message)
			}
		})
	}
}

func TestTaskStateFromProgressResult(t *testing.T) {
	t.Run("retryable failed result becomes operator-retryable task state", func(t *testing.T) {
		result := &engine.ProgressResult{State: engine.StateFailed, Retryable: true}
		assert.Equal(t, state.Task.FailedRetryable, taskStateFromProgressResult(result))
	})

	t.Run("failed result without retry hint stays permanent", func(t *testing.T) {
		result := &engine.ProgressResult{State: engine.StateFailed}
		assert.Equal(t, state.Task.Failed, taskStateFromProgressResult(result))
	})

	t.Run("unknown engine state stays visible as running", func(t *testing.T) {
		result := &engine.ProgressResult{State: engine.State("something_new")}
		assert.Equal(t, state.Task.Running, taskStateFromProgressResult(result))
	})
}

func TestApplyEventStateTransition(t *testing.T) {
	logger := slog.Default()
	succeedUpdate := func(_ *storage.Apply) error { return nil }
	failUpdate := func(_ *storage.Apply) error { return fmt.Errorf("db unavailable") }

	t.Run("transitions state on new event", func(t *testing.T) {
		apply := &storage.Apply{State: state.Apply.Pending}
		event := engine.ApplyEvent{NewState: state.Apply.PreparingBranch}

		got := applyEventStateTransition(apply, event, succeedUpdate, logger)

		assert.Equal(t, state.Apply.PreparingBranch, got)
		assert.Equal(t, state.Apply.PreparingBranch, apply.State)
	})

	t.Run("skips write when state unchanged", func(t *testing.T) {
		apply := &storage.Apply{State: state.Apply.ApplyingBranchChanges}
		event := engine.ApplyEvent{NewState: state.Apply.ApplyingBranchChanges}

		got := applyEventStateTransition(apply, event, succeedUpdate, logger)

		assert.Empty(t, got)
		assert.Equal(t, state.Apply.ApplyingBranchChanges, apply.State)
	})

	t.Run("skips informational event with no NewState", func(t *testing.T) {
		apply := &storage.Apply{State: state.Apply.ApplyingBranchChanges}
		event := engine.ApplyEvent{Message: "Applied keyspace ks1 (3/10)"}

		got := applyEventStateTransition(apply, event, succeedUpdate, logger)

		assert.Empty(t, got)
		assert.Equal(t, state.Apply.ApplyingBranchChanges, apply.State)
	})

	t.Run("rolls back in-memory state on failed write", func(t *testing.T) {
		apply := &storage.Apply{State: state.Apply.Pending}
		event := engine.ApplyEvent{NewState: state.Apply.PreparingBranch}

		got := applyEventStateTransition(apply, event, failUpdate, logger)

		assert.Empty(t, got)
		assert.Equal(t, state.Apply.Pending, apply.State, "state should be rolled back after failed write")
	})

	t.Run("retries after rollback", func(t *testing.T) {
		apply := &storage.Apply{State: state.Apply.Pending}
		event := engine.ApplyEvent{NewState: state.Apply.PreparingBranch}

		// First attempt fails — state rolls back
		got := applyEventStateTransition(apply, event, failUpdate, logger)
		assert.Empty(t, got)
		assert.Equal(t, state.Apply.Pending, apply.State)

		// Second attempt with same event succeeds
		got = applyEventStateTransition(apply, event, succeedUpdate, logger)
		assert.Equal(t, state.Apply.PreparingBranch, got)
		assert.Equal(t, state.Apply.PreparingBranch, apply.State)
	})
}

func TestDeriveOverallState(t *testing.T) {
	tests := []struct {
		name      string
		tasks     []*storage.Task
		wantState string
	}{
		{
			name:      "empty tasks returns pending",
			tasks:     nil,
			wantState: state.Task.Pending,
		},
		{
			name: "all completed returns completed",
			tasks: []*storage.Task{
				{State: state.Task.Completed},
				{State: state.Task.Completed},
			},
			wantState: state.Task.Completed,
		},
		{
			name: "all revert_window returns revert_window",
			tasks: []*storage.Task{
				{State: state.Task.RevertWindow},
				{State: state.Task.RevertWindow},
			},
			wantState: state.Task.RevertWindow,
		},
		{
			name: "mix of revert_window and completed returns revert_window",
			tasks: []*storage.Task{
				{State: state.Task.RevertWindow},
				{State: state.Task.Completed},
			},
			wantState: state.Task.RevertWindow,
		},
		{
			name: "running takes priority over revert_window",
			tasks: []*storage.Task{
				{State: state.Task.Running},
				{State: state.Task.RevertWindow},
			},
			wantState: state.Task.Running,
		},
		{
			name: "failed takes priority over completed",
			tasks: []*storage.Task{
				{State: state.Task.Failed},
				{State: state.Task.Completed},
			},
			wantState: state.Task.Failed,
		},
		{
			name: "retryable failed waits for operator with completed work",
			tasks: []*storage.Task{
				{State: state.Task.FailedRetryable},
				{State: state.Task.Completed},
			},
			wantState: state.Task.FailedRetryable,
		},
		{
			name: "retryable failed waits for operator with pending work",
			tasks: []*storage.Task{
				{State: state.Task.FailedRetryable},
				{State: state.Task.Pending},
			},
			wantState: state.Task.FailedRetryable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deriveOverallState(tt.tasks)
			assert.Equal(t, tt.wantState, got)
		})
	}
}

// applyQuiesceDecision gates the apply-level terminal/pause side-effects on the
// rollout-projected apply state. A drive must quiesce the whole apply only when
// the projection is terminal or a retry-pause — never when a continue policy
// holds the apply running while siblings are still in flight. completed_at is
// stamped only for truly-finished states: failed_retryable (a retry pause) and
// stopped (terminal but resumable) keep completed_at nil even though they
// quiesce.
func TestApplyQuiesceDecision(t *testing.T) {
	cases := []struct {
		name           string
		projected      string
		wantQuiesce    bool
		wantRetryPause bool
		wantStamp      bool
	}{
		{"completed", state.Apply.Completed, true, false, true},
		{"failed", state.Apply.Failed, true, false, true},
		{"cancelled", state.Apply.Cancelled, true, false, true},
		{"reverted", state.Apply.Reverted, true, false, true},
		{"stopped", state.Apply.Stopped, true, false, false},
		{"failed_retryable", state.Apply.FailedRetryable, true, true, false},
		{"running", state.Apply.Running, false, false, false},
		{"pending", state.Apply.Pending, false, false, false},
		{"waiting_for_cutover", state.Apply.WaitingForCutover, false, false, false},
		{"waiting_for_deploy", state.Apply.WaitingForDeploy, false, false, false},
		{"cutting_over", state.Apply.CuttingOver, false, false, false},
		{"recovering", state.Apply.Recovering, false, false, false},
		{"revert_window", state.Apply.RevertWindow, false, false, false},
		{"empty/undetermined", "", false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			quiesce, retryPause, stamp := applyQuiesceDecision(tc.projected)
			assert.Equal(t, tc.wantQuiesce, quiesce, "quiesce for projected state %q", tc.projected)
			assert.Equal(t, tc.wantRetryPause, retryPause, "retryablePause for projected state %q", tc.projected)
			assert.Equal(t, tc.wantStamp, stamp, "stampCompletedAt for projected state %q", tc.projected)
		})
	}
}
