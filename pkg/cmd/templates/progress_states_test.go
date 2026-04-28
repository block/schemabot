package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestStateLabel_PlanetScalePhases(t *testing.T) {
	assert.Equal(t, "Preparing branch", StateLabel(state.Apply.PreparingBranch))
	assert.Equal(t, "Applying changes to branch", StateLabel(state.Apply.ApplyingBranchChanges))
	assert.Equal(t, "Creating deploy request", StateLabel(state.Apply.CreatingDeployRequest))
	assert.Equal(t, "Cancelled", StateLabel(state.Apply.Cancelled))
}

func TestFormatProgressState_PlanetScalePhases(t *testing.T) {
	assert.Contains(t, FormatProgressState(state.Apply.PreparingBranch), "Preparing branch")
	assert.Contains(t, FormatProgressState(state.Apply.ApplyingBranchChanges), "Applying changes to branch")
	assert.Contains(t, FormatProgressState(state.Apply.CreatingDeployRequest), "Creating deploy request")
	assert.Contains(t, FormatProgressState(state.Apply.Cancelled), "Cancelled")
}

func TestStateColorFunc_PlanetScalePhases(t *testing.T) {
	for _, s := range []string{
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.CreatingDeployRequest,
		state.Apply.Cancelled,
	} {
		fn := stateColorFunc(s)
		assert.NotNil(t, fn, "expected color function for state %q", s)
	}
}
