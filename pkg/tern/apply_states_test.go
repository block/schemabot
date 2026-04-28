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

// TestOnEventApplyStateTransitions verifies that engine lifecycle events
// trigger the correct apply state transitions for PlanetScale applies.
func TestOnEventApplyStateTransitions(t *testing.T) {
	tests := []struct {
		name         string
		message      string
		wantState    string
		wantNoChange bool
	}{
		{
			name:      "branch ready transitions to applying_branch_changes",
			message:   "Branch schemabot-boardgames-123 ready (44s)",
			wantState: state.Apply.ApplyingBranchChanges,
		},
		{
			name:      "branch schema refreshed transitions to applying_branch_changes",
			message:   "Branch dr-branch-reuse schema refreshed (5s)",
			wantState: state.Apply.ApplyingBranchChanges,
		},
		{
			name:      "applying changes transitions to applying_branch_changes",
			message:   "Applying changes to 33 keyspaces on branch dr-branch-reuse",
			wantState: state.Apply.ApplyingBranchChanges,
		},
		{
			name:         "applied keyspace does not re-transition",
			message:      "Applied keyspace commerce_sharded_015 (12/33)",
			wantNoChange: true,
		},
		{
			name:      "DDL applied transitions to creating_deploy_request",
			message:   "Applied 1 DDL changes to branch schemabot-boardgames-123",
			wantState: state.Apply.CreatingDeployRequest,
		},
		{
			name:      "multiple DDL changes transitions to creating_deploy_request",
			message:   "Applied 3 DDL changes to branch schemabot-commerce-456",
			wantState: state.Apply.CreatingDeployRequest,
		},
		{
			name:         "creating branch does not transition",
			message:      "Creating branch schemabot-boardgames-123",
			wantNoChange: true,
		},
		{
			name:         "deploy request created does not transition",
			message:      "Deploy request #77 created",
			wantNoChange: true,
		},
		{
			name:         "deploy request deployed does not transition",
			message:      "Deploy request #77 deployed",
			wantNoChange: true,
		},
		{
			name:         "no changes detected does not transition",
			message:      "Deploy request #77: no changes detected",
			wantNoChange: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := engine.ApplyEvent{Message: tt.message}
			newState := deriveApplyPhase(event.Message)

			if tt.wantNoChange {
				assert.Empty(t, newState, "expected no state change for %q", tt.message)
			} else {
				assert.Equal(t, tt.wantState, newState, "wrong state for %q", tt.message)
			}
		})
	}
}

func TestPlanNamespacesToChanges_VSchemaOnlyWhenStored(t *testing.T) {
	namespaces := map[string]*storage.NamespacePlanData{
		"ks_with_vschema": {
			Tables:  []storage.TableChange{{Table: "users", DDL: "ALTER TABLE users ADD COLUMN x INT"}},
			VSchema: json.RawMessage(`{"tables":{"users":{}}}`),
		},
		"ks_without_vschema": {
			Tables: []storage.TableChange{{Table: "orders", DDL: "ALTER TABLE orders ADD COLUMN y INT"}},
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
}
