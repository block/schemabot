package state

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyMetadata_CoversAllStates enforces that every canonical Apply.*
// constant has an entry in the registry. Renderers and gating predicates rely
// on the registry being exhaustive; a missing entry silently degrades to a
// raw state string in the CLI and to "not terminal / not setup-phase" in
// scheduling decisions.
func TestApplyMetadata_CoversAllStates(t *testing.T) {
	v := reflect.ValueOf(Apply)
	for i := 0; i < v.NumField(); i++ {
		stateValue := v.Field(i).String()
		require.NotEmpty(t, stateValue, "Apply.%s is empty", v.Type().Field(i).Name)
		info, ok := applyMetadata[stateValue]
		assert.Truef(t, ok, "Apply.%s (%q) is missing from applyMetadata", v.Type().Field(i).Name, stateValue)
		assert.NotEmptyf(t, info.Label, "Apply.%s (%q) has empty Label", v.Type().Field(i).Name, stateValue)
	}
}

// TestApplyMetadata_TerminalSet pins which states are classified as terminal.
// Changing this set affects operator claiming, reconciliation, and merge
// gating, so a deliberate test failure here is the intent when adding or
// removing a terminal state.
func TestApplyMetadata_TerminalSet(t *testing.T) {
	expected := map[string]bool{
		Apply.Completed: true,
		Apply.Failed:    true,
		Apply.Stopped:   true,
		Apply.Cancelled: true,
		Apply.Reverted:  true,
	}
	for s, info := range applyMetadata {
		assert.Equalf(t, expected[s], info.Terminal, "Terminal classification for %q", s)
	}
	// FailedRetryable must not be terminal — the operator retries it.
	assert.False(t, applyMetadata[Apply.FailedRetryable].Terminal, "FailedRetryable must not be terminal")
}

// TestApplyMetadata_SetupPhaseSet pins which states are classified as
// engine setup phases. CLI and TUI suppress the per-table list during these
// states.
func TestApplyMetadata_SetupPhaseSet(t *testing.T) {
	expected := map[string]bool{
		Apply.Pending:                 true,
		Apply.PreparingBranch:         true,
		Apply.ApplyingBranchChanges:   true,
		Apply.ValidatingBranch:        true,
		Apply.CreatingDeployRequest:   true,
		Apply.ValidatingDeployRequest: true,
		Apply.WaitingForDeploy:        true,
	}
	for s, info := range applyMetadata {
		assert.Equalf(t, expected[s], info.SetupPhase, "SetupPhase classification for %q", s)
	}
}

// TestApplyMetadata_HoldSet pins which states promise an operator something
// about the database they are holding. A state with a Hold tells a refused
// change whether to act or to wait; a state without one is rendered on its own.
// Adding a Hold to a state whose next move is not certain would have SchemaBot
// promise an outcome it cannot keep, so the set is pinned deliberately.
func TestApplyMetadata_HoldSet(t *testing.T) {
	expected := map[string]bool{
		Apply.Running:           true,
		Apply.RunningDegraded:   true,
		Apply.CatchingUp:        true,
		Apply.Checksumming:      true,
		Apply.PostChecksum:      true,
		Apply.WaitingForCutover: true,
		Apply.RevertWindow:      true,
		Apply.SkippingRevert:    true,
		Apply.Reverting:         true,
		Apply.FailedRetryable:   true,
		Apply.Stopped:           true,
	}
	for s, info := range applyMetadata {
		assert.Equalf(t, expected[s], info.Hold != "", "Hold presence for %q", s)
	}
}

// TestApplyMetadata_HoldSeparatesActFromWait pins the distinction the Hold
// exists to draw. A resting change waits on an operator's decision, so its Hold
// names the commands that end the wait; a change still under way releases the
// database itself, so its Hold promises release rather than an action.
func TestApplyMetadata_HoldSeparatesActFromWait(t *testing.T) {
	assert.Equal(t, "it holds the database until it is started or cancelled", Hold(Apply.Stopped))
	assert.Equal(t, "it holds the database until it is retried or cancelled", Hold(Apply.FailedRetryable))
	assert.Equal(t, "it holds the database until it is cut over or cancelled", Hold(Apply.WaitingForCutover))
	assert.Equal(t, "it holds the database until it is reverted or skip-reverted", Hold(Apply.RevertWindow))
	assert.Equal(t, holdUntilRevertFinishes, Hold(Apply.Reverting))
	assert.Equal(t, holdUntilApplyFinishes, Hold(Apply.Running))

	// A post-copy phase is still the apply running, so it makes the running
	// promise rather than falling through to no promise at all.
	assert.Equal(t, holdUntilApplyFinishes, Hold(Apply.Checksumming))

	// Proto and uppercase forms resolve to the same promise as the canonical one.
	assert.Equal(t, Hold(Apply.Stopped), Hold("STATE_STOPPED"))

	// States whose next move is not certain, and states the registry does not
	// know, promise nothing.
	assert.Empty(t, Hold(Apply.CuttingOver))
	assert.Empty(t, Hold(Apply.Recovering))
	assert.Empty(t, Hold(Apply.Completed))
	assert.Empty(t, Hold("not_a_state"))
	assert.Empty(t, Hold(""))
}

func TestLabel(t *testing.T) {
	assert.Equal(t, "Pending", Label(Apply.Pending))
	assert.Equal(t, "Waiting for deploy", Label(Apply.WaitingForDeploy))
	assert.Equal(t, "Retrying", Label(Apply.FailedRetryable))
	// Unknown state falls back to the raw input.
	assert.Equal(t, "unknown_state", Label("unknown_state"))
	assert.Equal(t, "", Label(""))
}

func TestLookupApply(t *testing.T) {
	info, ok := LookupApply(Apply.Completed)
	require.True(t, ok)
	assert.Equal(t, "Completed", info.Label)
	assert.True(t, info.Terminal)
	assert.False(t, info.SetupPhase)

	_, ok = LookupApply("not_a_state")
	assert.False(t, ok)
}
