package tern

import (
	"encoding/json"
	"testing"

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
			name: "creating branch transitions to creating_branch",
			event: engine.ApplyEvent{
				Message:  "Creating branch schemabot-boardgames-123",
				NewState: state.Apply.CreatingBranch,
			},
			wantState: state.Apply.CreatingBranch,
		},
		{
			name: "reusing branch transitions to creating_branch",
			event: engine.ApplyEvent{
				Message:  "Reusing branch dr-branch-reuse",
				NewState: state.Apply.CreatingBranch,
			},
			wantState: state.Apply.CreatingBranch,
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
	assert.Equal(t, "true", byNS["ks_with_vschema"].Metadata["vschema"])

	// Keyspace without VSchema should NOT have metadata["vschema"] set
	assert.Empty(t, byNS["ks_without_vschema"].Metadata["vschema"],
		"keyspace without VSchema change should not have vschema metadata")

	// Operation field should be preserved
	assert.Equal(t, "alter", byNS["ks_with_vschema"].TableChanges[0].Operation)
	assert.Equal(t, "alter", byNS["ks_without_vschema"].TableChanges[0].Operation)
}
