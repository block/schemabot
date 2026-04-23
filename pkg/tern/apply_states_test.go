package tern

import (
	"testing"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
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
			name:      "DDL applied transitions to creating_deploy_request",
			message:   "Applied 1 DDL changes to branch schemabot-boardgames-123",
			wantState: state.Apply.CreatingDeployRequest,
		},
		{
			name:      "multiple DDL changes transitions to creating_deploy_request",
			message:   "Applied 3 DDL changes to branch schemabot-inventory2-456",
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
