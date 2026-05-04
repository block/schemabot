package tern

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/statement"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestPlanNamespacesToChanges_VSchemaOnlyWhenStored(t *testing.T) {
	namespaces := map[string]*storage.NamespacePlanData{
		"ks_with_vschema": {
			Tables:  []storage.TableChange{{Table: "users", DDL: "ALTER TABLE users ADD COLUMN x INT", Operation: "alter"}},
			VSchema: json.RawMessage(`{"tables":{"users":{}}}`),
		},
		"ks_without_vschema": {
			Tables: []storage.TableChange{{Table: "orders", DDL: "ALTER TABLE orders ADD COLUMN y INT", Operation: "alter"}},
		},
	}

	changes := planNamespacesToChanges(namespaces)
	require.Len(t, changes, 2)

	byNS := make(map[string]engine.SchemaChange)
	for _, c := range changes {
		byNS[c.Namespace] = c
	}

	// Keyspace with VSchema stored should have metadata["vschema"] set
	assert.Equal(t, "true", byNS["ks_with_vschema"].Metadata["vschema_changed"])

	// Keyspace without VSchema should NOT have metadata["vschema"] set
	assert.Empty(t, byNS["ks_without_vschema"].Metadata["vschema_changed"],
		"keyspace without VSchema change should not have vschema metadata")

	// Operation field should be preserved (storage string → Spirit type via OpToStatementType)
	assert.Equal(t, statement.StatementAlterTable, byNS["ks_with_vschema"].TableChanges[0].Operation)
	assert.Equal(t, statement.StatementAlterTable, byNS["ks_without_vschema"].TableChanges[0].Operation)
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deriveOverallState(tt.tasks)
			assert.Equal(t, tt.wantState, got)
		})
	}
}

func TestVSchemaTasksCreatedAlongsideDDL(t *testing.T) {
	plan := &storage.Plan{
		Namespaces: map[string]*storage.NamespacePlanData{
			"myapp_sharded": {
				Tables:  []storage.TableChange{{Table: "users", DDL: "ALTER TABLE users ADD COLUMN phone VARCHAR(20)", Operation: "alter", Namespace: "myapp_sharded"}},
				VSchema: json.RawMessage(`{"sharded": true, "tables": {"users": {}}}`),
			},
			"myapp": {
				VSchema: json.RawMessage(`{"tables": {"users_seq": {"type": "sequence"}}}`),
			},
		},
	}

	ddlChanges := plan.FlatDDLChanges()
	require.Len(t, ddlChanges, 1, "should have 1 DDL change")

	for ns, nsData := range plan.Namespaces {
		if len(nsData.VSchema) > 0 {
			ddlChanges = append(ddlChanges, storage.TableChange{
				Table:     "VSchema: " + ns,
				Namespace: ns,
				Operation: "vschema_update",
			})
		}
	}

	assert.Len(t, ddlChanges, 3, "should have 1 DDL + 2 VSchema tasks")

	var vschemaTasks []storage.TableChange
	for _, c := range ddlChanges {
		if c.Operation == "vschema_update" {
			vschemaTasks = append(vschemaTasks, c)
		}
	}
	assert.Len(t, vschemaTasks, 2, "should have 2 VSchema tasks (one per keyspace)")
	for _, vt := range vschemaTasks {
		assert.Contains(t, vt.Table, "VSchema: ")
		assert.Equal(t, "vschema_update", vt.Operation)
	}
}

func TestDeriveVSchemaTaskState(t *testing.T) {
	c := &LocalClient{}
	now := time.Now()

	t.Run("pending task stays pending when deploy is running", func(t *testing.T) {
		task := &storage.Task{State: state.Task.Pending}
		result := &engine.ProgressResult{State: engine.StateRunning, Message: "Deploying"}
		got := c.deriveVSchemaTaskState(task, result, state.Task.Running, now)
		assert.Equal(t, state.Task.Pending, got)
	})

	t.Run("transitions to running on VSchema apply message", func(t *testing.T) {
		task := &storage.Task{State: state.Task.Pending}
		result := &engine.ProgressResult{State: engine.StateRunning, Message: "Applying VSchema changes"}
		got := c.deriveVSchemaTaskState(task, result, state.Task.Running, now)
		assert.Equal(t, state.Task.Running, got)
		assert.NotNil(t, task.StartedAt)
	})

	t.Run("transitions to revert_window when overall is revert_window", func(t *testing.T) {
		task := &storage.Task{State: state.Task.Running}
		result := &engine.ProgressResult{State: engine.StateRevertWindow}
		got := c.deriveVSchemaTaskState(task, result, state.Task.RevertWindow, now)
		assert.Equal(t, state.Task.RevertWindow, got)
	})

	t.Run("transitions to completed when overall is completed", func(t *testing.T) {
		task := &storage.Task{State: state.Task.Running}
		result := &engine.ProgressResult{State: engine.StateCompleted}
		got := c.deriveVSchemaTaskState(task, result, state.Task.Completed, now)
		assert.Equal(t, state.Task.Completed, got)
	})

	t.Run("transitions to failed on engine failure", func(t *testing.T) {
		task := &storage.Task{State: state.Task.Running}
		result := &engine.ProgressResult{State: engine.StateFailed}
		got := c.deriveVSchemaTaskState(task, result, state.Task.Failed, now)
		assert.Equal(t, state.Task.Failed, got)
	})

	t.Run("stays pending during cutover since VSchema is applied after", func(t *testing.T) {
		task := &storage.Task{State: state.Task.Pending}
		result := &engine.ProgressResult{State: engine.StateWaitingForCutover, Message: "Waiting for cutover"}
		got := c.deriveVSchemaTaskState(task, result, state.Task.WaitingForCutover, now)
		assert.Equal(t, state.Task.Pending, got)
	})

	t.Run("terminal task state is preserved", func(t *testing.T) {
		task := &storage.Task{State: state.Task.Completed}
		result := &engine.ProgressResult{State: engine.StateRunning, Message: "Applying VSchema changes"}
		got := c.deriveVSchemaTaskState(task, result, state.Task.Running, now)
		assert.Equal(t, state.Task.Completed, got)
	})
}
