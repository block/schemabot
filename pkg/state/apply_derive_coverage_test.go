package state

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every Apply state, in each format a caller hands DeriveApplyState, must
// survive normalization and derive to itself when it is the only input. A
// state with no place in the priority order would otherwise derive to pending,
// and an apply_operation row stored in it would read as not yet started.
func TestDeriveApplyState_EveryStateDerivesToItself(t *testing.T) {
	v := reflect.ValueOf(Apply)
	for i := 0; i < v.NumField(); i++ {
		name := v.Type().Field(i).Name
		s := v.Field(i).String()
		require.NotEmpty(t, s, "Apply.%s is empty", name)
		for _, input := range []string{s, strings.ToUpper(s), "STATE_" + strings.ToUpper(s)} {
			assert.Equalf(t, s, normalizeApplyState(input), "normalizeApplyState(%q) for Apply.%s", input, name)
			assert.Equalf(t, s, DeriveApplyState([]string{input}), "DeriveApplyState([%q]) for Apply.%s", input, name)
		}
	}
}

// deploySetupPhases must name every setup phase that runs before rows move:
// the registry's setup phases, less pending (nothing started) and
// waiting_for_deploy (staging is done; it ranks as its own gate).
func TestDeploySetupPhasesMatchRegistry(t *testing.T) {
	var fromRegistry []string
	for s, info := range applyMetadata {
		if info.SetupPhase && s != Apply.Pending && s != Apply.WaitingForDeploy {
			fromRegistry = append(fromRegistry, s)
		}
	}
	assert.ElementsMatch(t, fromRegistry, deploySetupPhases)
}

// A deploy setup phase ranks with a queued table: ahead of pending and of a
// later setup phase, behind any sibling that has begun copying, and it keeps a
// sibling's drain or cutover from surfacing, since its whole copy is still
// ahead. Skipping a revert ranks just behind an open revert window, a degraded
// rollout ahead of a plain running one, and a paused rollout with the other
// holds for an operator.
func TestDeriveApplyState_StatesWithoutTaskCounterparts(t *testing.T) {
	testCases := []struct {
		states   []string
		expected string
	}{
		{[]string{"preparing_branch", "pending"}, Apply.PreparingBranch},
		{[]string{"validating_branch", "creating_deploy_request"}, Apply.ValidatingBranch},
		{[]string{"completed", "validating_deploy_request", "waiting_for_deploy"}, Apply.ValidatingDeployRequest},
		{[]string{"applying_branch_changes", "running"}, Apply.Running},
		{[]string{"preparing_branch", "catching_up"}, Apply.Running},
		{[]string{"creating_deploy_request", "cutting_over"}, Apply.Running},

		{[]string{"skipping_revert", "completed"}, Apply.SkippingRevert},
		{[]string{"revert_window", "skipping_revert"}, Apply.RevertWindow},
		{[]string{"skipping_revert", "running"}, Apply.SkippingRevert},

		{[]string{"running_degraded", "running"}, Apply.RunningDegraded},
		{[]string{"running_degraded", "cutting_over"}, Apply.RunningDegraded},

		{[]string{"paused", "running"}, Apply.Paused},
		{[]string{"paused", "stopped"}, Apply.Stopped},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expected, DeriveApplyState(tc.states), "input: %v", tc.states)
	}
}
