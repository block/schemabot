package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

func TestBranchFlagValidation(t *testing.T) {
	tests := []struct {
		name    string
		branch  string
		wantErr string
	}{
		{
			name:   "empty branch is allowed",
			branch: "",
		},
		{
			name:   "development branch is allowed",
			branch: "my-feature-branch",
		},
		{
			name:    "main branch is rejected",
			branch:  "main",
			wantErr: "cannot reuse the main branch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBranchFlag(tt.branch)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBuildApplyOptionsDefersDeployOnlyForPlanetScale(t *testing.T) {
	tests := []struct {
		name         string
		engine       string
		deferDeploy  bool
		watch        bool
		format       OutputFormat
		wantDeferred bool
	}{
		{
			name:         "interactive PlanetScale apply defers deploy for review",
			engine:       storage.EnginePlanetScale,
			watch:        true,
			format:       OutputFormatInteractive,
			wantDeferred: true,
		},
		{
			name:         "explicit PlanetScale defer deploy is preserved",
			engine:       storage.EnginePlanetScale,
			deferDeploy:  true,
			format:       OutputFormatLog,
			wantDeferred: true,
		},
		{
			name:   "interactive MySQL apply does not show PlanetScale deploy controls",
			engine: storage.EngineSpirit,
			watch:  true,
			format: OutputFormatInteractive,
		},
		{
			name:        "explicit MySQL defer deploy is not sent",
			engine:      storage.EngineSpirit,
			deferDeploy: true,
			format:      OutputFormatLog,
		},
		{
			name:   "unknown engine does not auto defer deploy",
			engine: "",
			watch:  true,
			format: OutputFormatInteractive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := buildApplyOptions(&apitypes.PlanResponse{Engine: tt.engine}, false, tt.deferDeploy, false, "", tt.watch, tt.format)
			_, gotDeferred := options["defer_deploy"]
			assert.Equal(t, tt.wantDeferred, gotDeferred)
		})
	}
}

func TestBuildApplyOptionsPreservesOtherOptions(t *testing.T) {
	options := buildApplyOptions(&apitypes.PlanResponse{Engine: storage.EnginePlanetScale}, true, true, true, "feature-branch", false, OutputFormatLog)

	require.Equal(t, "true", options["defer_cutover"])
	require.Equal(t, "true", options["defer_deploy"])
	require.Equal(t, "true", options["skip_revert"])
	require.Equal(t, "feature-branch", options["branch"])
}
