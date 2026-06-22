package serve

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
)

// TestApplyDataPlaneClaimDefault verifies that a data-plane tern process
// (serving the Tern proto over gRPC) defaults operator claiming to the apply
// level, because it drives applies inline via LocalClient and does not maintain
// apply_operations rows. The control plane keeps operation-level claiming, and
// an explicit setting is always honored.
func TestApplyDataPlaneClaimDefault(t *testing.T) {
	enabled := true
	disabled := false

	tests := []struct {
		name        string
		current     *bool
		isDataPlane bool
		wantApplied bool
		wantClaim   bool // effective ShouldClaimOperations()
	}{
		{
			name:        "data plane with unset mode defaults to apply level",
			current:     nil,
			isDataPlane: true,
			wantApplied: true,
			wantClaim:   false,
		},
		{
			name:        "data plane honors explicit operation-level opt-in",
			current:     &enabled,
			isDataPlane: true,
			wantApplied: false,
			wantClaim:   true,
		},
		{
			name:        "data plane honors explicit apply-level setting",
			current:     &disabled,
			isDataPlane: true,
			wantApplied: false,
			wantClaim:   false,
		},
		{
			name:        "control plane with unset mode keeps operation-level default",
			current:     nil,
			isDataPlane: false,
			wantApplied: false,
			wantClaim:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &api.ServerConfig{OperatorClaimOperations: tt.current}
			applied := applyDataPlaneClaimDefault(cfg, tt.isDataPlane)
			assert.Equal(t, tt.wantApplied, applied, "applied default")
			assert.Equal(t, tt.wantClaim, cfg.ShouldClaimOperations(), "effective claim mode")
		})
	}
}
