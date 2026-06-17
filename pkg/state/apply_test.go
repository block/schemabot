package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeriveApplyState_Empty(t *testing.T) {
	assert.Equal(t, Apply.Pending, DeriveApplyState(nil))
	assert.Equal(t, Apply.Pending, DeriveApplyState([]string{}))
}

func TestDeriveApplyState_AllPending(t *testing.T) {
	states := []string{"PENDING", "PENDING", "PENDING"}
	assert.Equal(t, Apply.Pending, DeriveApplyState(states))
}

func TestDeriveApplyState_AllCompleted(t *testing.T) {
	states := []string{"COMPLETED", "COMPLETED", "COMPLETED"}
	assert.Equal(t, Apply.Completed, DeriveApplyState(states))
}

func TestDeriveApplyState_AnyFailed(t *testing.T) {
	testCases := [][]string{
		{"FAILED"},
		{"FAILED", "FAILED_RETRYABLE"},
		{"RUNNING", "FAILED"},
		{"COMPLETED", "FAILED"},
		{"WAITING_FOR_CUTOVER", "FAILED", "COMPLETED"},
		{"PENDING", "RUNNING", "FAILED"},
	}

	for _, states := range testCases {
		assert.Equal(t, Apply.Failed, DeriveApplyState(states), "input: %v", states)
	}
}

// TestDeriveApplyState_FailedRetryable verifies that a retryable task failure
// rolls the apply up to failed_retryable unless a permanent failed task exists.
func TestDeriveApplyState_FailedRetryable(t *testing.T) {
	testCases := [][]string{
		{"FAILED_RETRYABLE"},
		{"COMPLETED", "FAILED_RETRYABLE"},
		{"PENDING", "FAILED_RETRYABLE"},
		{"failed_retryable"},
	}

	for _, states := range testCases {
		assert.Equal(t, Apply.FailedRetryable, DeriveApplyState(states), "input: %v", states)
	}
}

func TestDeriveApplyState_AnyStopped(t *testing.T) {
	testCases := [][]string{
		{"STOPPED"},
		{"RUNNING", "STOPPED"},
		{"COMPLETED", "STOPPED"},
		{"WAITING_FOR_CUTOVER", "STOPPED"},
	}

	for _, states := range testCases {
		assert.Equal(t, Apply.Stopped, DeriveApplyState(states), "input: %v", states)
	}
}

func TestDeriveApplyState_AnyReverted(t *testing.T) {
	testCases := [][]string{
		{"REVERTED"},
		{"COMPLETED", "REVERTED"},
	}

	for _, states := range testCases {
		assert.Equal(t, Apply.Reverted, DeriveApplyState(states), "input: %v", states)
	}
}

func TestDeriveApplyState_AnyRunning(t *testing.T) {
	testCases := [][]string{
		{"RUNNING"},
		{"PENDING", "RUNNING"},
		{"RUNNING", "PENDING", "PENDING"},
		{"COMPLETED", "RUNNING", "PENDING"},
	}

	for _, states := range testCases {
		assert.Equal(t, Apply.Running, DeriveApplyState(states), "input: %v", states)
	}
}

func TestDeriveApplyState_AllWaitingForDeploy(t *testing.T) {
	states := []string{"WAITING_FOR_DEPLOY", "WAITING_FOR_DEPLOY"}
	assert.Equal(t, Apply.WaitingForDeploy, DeriveApplyState(states))
}

func TestDeriveApplyState_WaitingForDeployAndCompleted(t *testing.T) {
	states := []string{"COMPLETED", "WAITING_FOR_DEPLOY"}
	assert.Equal(t, Apply.WaitingForDeploy, DeriveApplyState(states))
}

func TestDeriveApplyState_AllWaitingForCutover(t *testing.T) {
	states := []string{"WAITING_FOR_CUTOVER", "WAITING_FOR_CUTOVER", "WAITING_FOR_CUTOVER"}
	assert.Equal(t, Apply.WaitingForCutover, DeriveApplyState(states))
}

func TestDeriveApplyState_WaitingAndCompleted(t *testing.T) {
	// In independent mode, some tasks may complete while others wait
	// This should still be waiting_for_cutover since not all are done
	states := []string{"COMPLETED", "WAITING_FOR_CUTOVER", "WAITING_FOR_CUTOVER"}
	assert.Equal(t, Apply.WaitingForCutover, DeriveApplyState(states))
}

func TestDeriveApplyState_CuttingOver(t *testing.T) {
	testCases := [][]string{
		{"CUTTING_OVER"},
		{"CUTTING_OVER", "CUTTING_OVER"},
		{"WAITING_FOR_CUTOVER", "CUTTING_OVER"},
		{"COMPLETED", "CUTTING_OVER"},
	}

	for _, states := range testCases {
		assert.Equal(t, Apply.CuttingOver, DeriveApplyState(states), "input: %v", states)
	}
}

func TestDeriveApplyState_RevertWindow(t *testing.T) {
	testCases := [][]string{
		{"REVERT_WINDOW"},
		{"COMPLETED", "REVERT_WINDOW"},
	}

	for _, states := range testCases {
		assert.Equal(t, Apply.RevertWindow, DeriveApplyState(states), "input: %v", states)
	}
}

func TestDeriveApplyState_MixedStates_IndependentMode(t *testing.T) {
	// Simulate independent mode: tasks complete at different times
	// Task1 completes, Task2 still running, Task3 pending
	states := []string{"COMPLETED", "RUNNING", "PENDING"}
	assert.Equal(t, Apply.Running, DeriveApplyState(states))
}

func TestDeriveApplyState_MixedStates_AtomicMode(t *testing.T) {
	// Simulate atomic mode: all tasks wait for cutover together
	states := []string{"WAITING_FOR_CUTOVER", "WAITING_FOR_CUTOVER", "WAITING_FOR_CUTOVER"}
	assert.Equal(t, Apply.WaitingForCutover, DeriveApplyState(states))
}

func TestDeriveApplyState_LowercaseInput(t *testing.T) {
	states := []string{"running", "pending"}
	assert.Equal(t, Apply.Running, DeriveApplyState(states))
}

func TestDeriveApplyState_MixedCase(t *testing.T) {
	states := []string{"RUNNING", "pending", "COMPLETED"}
	assert.Equal(t, Apply.Running, DeriveApplyState(states))
}

func TestDeriveApplyState_CompleteVariant(t *testing.T) {
	// "complete" (Vitess convention) and "completed" (storage convention) both map to Completed
	assert.Equal(t, Apply.Completed, DeriveApplyState([]string{"complete"}))
	assert.Equal(t, Apply.Completed, DeriveApplyState([]string{"COMPLETE"}))
	assert.Equal(t, Apply.Completed, DeriveApplyState([]string{"completed"}))
}

func TestDeriveApplyState_UnknownState(t *testing.T) {
	states := []string{"UNKNOWN_STATE"}
	assert.Equal(t, Apply.Pending, DeriveApplyState(states))
}

func TestIsTerminalApplyState(t *testing.T) {
	terminalStates := []string{
		Apply.Completed,
		Apply.Failed,
		Apply.Stopped,
		Apply.Reverted,
	}

	for _, s := range terminalStates {
		assert.True(t, IsTerminalApplyState(s), "%s should be terminal", s)
	}

	nonTerminalStates := []string{
		Apply.Pending,
		Apply.Running,
		Apply.RunningDegraded,
		Apply.FailedRetryable,
		Apply.WaitingForCutover,
		Apply.CuttingOver,
		Apply.RevertWindow,
	}

	for _, s := range nonTerminalStates {
		assert.False(t, IsTerminalApplyState(s), "%s should NOT be terminal", s)
	}

	// Accepts proto and uppercase forms, matching IsSetupPhase/IsState.
	assert.True(t, IsTerminalApplyState("STATE_COMPLETED"))
	assert.True(t, IsTerminalApplyState("COMPLETED"))
	assert.True(t, IsTerminalApplyState("STATE_REVERTED"))
	assert.False(t, IsTerminalApplyState("STATE_RUNNING"))
}

// TestIsRunningApplyState pins the running-family set that control gates key
// off: running and running_degraded are running-family; other non-terminal
// states (pending, waiting_for_cutover, recovering) are not.
func TestIsRunningApplyState(t *testing.T) {
	for _, s := range []string{Apply.Running, Apply.RunningDegraded, "RUNNING", "STATE_RUNNING_DEGRADED", "running_degraded"} {
		assert.Truef(t, IsRunningApplyState(s), "%s should be running-family", s)
	}
	for _, s := range []string{
		Apply.Pending,
		Apply.WaitingForDeploy,
		Apply.WaitingForCutover,
		Apply.Recovering,
		Apply.CuttingOver,
		Apply.FailedRetryable,
		Apply.Completed,
		Apply.Failed,
		Apply.Stopped,
	} {
		assert.Falsef(t, IsRunningApplyState(s), "%s should NOT be running-family", s)
	}
}

func TestNormalizeState(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"PENDING", Apply.Pending},
		{"pending", Apply.Pending},
		{"RUNNING", Apply.Running},
		{"running", Apply.Running},
		{"RUNNING_DEGRADED", Apply.RunningDegraded},
		{"running_degraded", Apply.RunningDegraded},
		{"WAITING_FOR_DEPLOY", Apply.WaitingForDeploy},
		{"waiting_for_deploy", Apply.WaitingForDeploy},
		{"WAITING_FOR_CUTOVER", Apply.WaitingForCutover},
		{"waiting_for_cutover", Apply.WaitingForCutover},
		{"RECOVERING", Apply.Recovering},
		{"recovering", Apply.Recovering},
		{"RECOVERING_CUTOVER", Apply.Recovering},
		{"recovering_cutover", Apply.Recovering},
		{"CUTTING_OVER", Apply.CuttingOver},
		{"cutting_over", Apply.CuttingOver},
		{"COMPLETED", Apply.Completed},
		{"completed", Apply.Completed},
		{"FAILED", Apply.Failed},
		{"failed", Apply.Failed},
		{"FAILED_RETRYABLE", Apply.FailedRetryable},
		{"failed_retryable", Apply.FailedRetryable},
		{"STOPPED", Apply.Stopped},
		{"stopped", Apply.Stopped},
		{"REVERTED", Apply.Reverted},
		{"reverted", Apply.Reverted},
		{"REVERT_WINDOW", Apply.RevertWindow},
		{"revert_window", Apply.RevertWindow},
		{"unknown", Apply.Pending},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expected, normalizeApplyState(tc.input), "normalizeApplyState(%q)", tc.input)
	}
}

// Test realistic scenarios

func TestDeriveApplyState_Scenario_SingleTableInstantDDL(t *testing.T) {
	states := []string{"COMPLETED"}
	assert.Equal(t, Apply.Completed, DeriveApplyState(states))
}

func TestDeriveApplyState_Scenario_SingleTableCopyMigration(t *testing.T) {
	states := []string{"RUNNING"}
	assert.Equal(t, Apply.Running, DeriveApplyState(states))
}

func TestDeriveApplyState_Scenario_MultiTableIndependent(t *testing.T) {
	// Table1: instant (completed), Table2: copying, Table3: queued
	states := []string{"COMPLETED", "RUNNING", "PENDING"}
	assert.Equal(t, Apply.Running, DeriveApplyState(states))
}

func TestDeriveApplyState_Scenario_MultiTableAtomic(t *testing.T) {
	// All tables finished copying, waiting for user to trigger cutover
	states := []string{"WAITING_FOR_CUTOVER", "WAITING_FOR_CUTOVER", "WAITING_FOR_CUTOVER"}
	assert.Equal(t, Apply.WaitingForCutover, DeriveApplyState(states))
}

func TestDeriveApplyState_Scenario_PartialFailure(t *testing.T) {
	states := []string{"COMPLETED", "FAILED", "RUNNING"}
	assert.Equal(t, Apply.Failed, DeriveApplyState(states))
}

func TestDeriveApplyState_Scenario_UserCancellation(t *testing.T) {
	states := []string{"COMPLETED", "STOPPED", "PENDING"}
	assert.Equal(t, Apply.Stopped, DeriveApplyState(states))
}

func TestIsSetupPhase(t *testing.T) {
	setupPhases := []string{
		Apply.Pending,
		Apply.PreparingBranch,
		Apply.ApplyingBranchChanges,
		Apply.ValidatingBranch,
		Apply.CreatingDeployRequest,
		Apply.ValidatingDeployRequest,
	}
	for _, s := range setupPhases {
		assert.True(t, IsSetupPhase(s), "%s should be a setup phase", s)
	}

	nonSetupPhases := []string{
		Apply.Running,
		Apply.WaitingForCutover,
		Apply.CuttingOver,
		Apply.RevertWindow,
		Apply.Completed,
		Apply.Failed,
		Apply.Stopped,
	}
	for _, s := range nonSetupPhases {
		assert.False(t, IsSetupPhase(s), "%s should NOT be a setup phase", s)
	}
}

func TestNormalizeApplyState_NewStates(t *testing.T) {
	assert.Equal(t, Apply.ValidatingBranch, normalizeApplyState("VALIDATING_BRANCH"))
	assert.Equal(t, Apply.ValidatingDeployRequest, normalizeApplyState("VALIDATING_DEPLOY_REQUEST"))
}

// rc builds a RolloutChild from a state and its continuation policy so the
// truth-table cases below read like the rollout scenario they describe.
func rc(state string, continueOnFailure bool) RolloutChild {
	return RolloutChild{State: state, ContinueOnFailure: continueOnFailure}
}

func TestDeriveRolloutApplyState_Empty(t *testing.T) {
	assert.Equal(t, Apply.Pending, DeriveRolloutApplyState(nil))
	assert.Equal(t, Apply.Pending, DeriveRolloutApplyState([]RolloutChild{}))
}

// TestDeriveRolloutApplyState_NoFailureMatchesBase verifies that when no child
// has terminally failed, the rollout projection is exactly the base projection
// regardless of any child's continuation policy.
func TestDeriveRolloutApplyState_NoFailureMatchesBase(t *testing.T) {
	cases := []struct {
		name     string
		children []RolloutChild
		want     string
	}{
		{
			name:     "all pending",
			children: []RolloutChild{rc(Apply.Pending, false), rc(Apply.Pending, true)},
			want:     Apply.Pending,
		},
		{
			name:     "all completed",
			children: []RolloutChild{rc(Apply.Completed, true), rc(Apply.Completed, true)},
			want:     Apply.Completed,
		},
		{
			name:     "running and completed",
			children: []RolloutChild{rc(Apply.Running, true), rc(Apply.Completed, true)},
			want:     Apply.Running,
		},
		{
			name:     "stopped sibling holds",
			children: []RolloutChild{rc(Apply.Stopped, true), rc(Apply.Completed, true)},
			want:     Apply.Stopped,
		},
		{
			name:     "failed_retryable not yet failed",
			children: []RolloutChild{rc(Apply.FailedRetryable, true), rc(Apply.Running, true)},
			want:     Apply.FailedRetryable,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DeriveRolloutApplyState(tc.children))
		})
	}
}

// TestDeriveRolloutApplyState_FailurePolicy is the truth table for the failed
// base case: continue holds the apply active until siblings settle, while halt,
// pause, and unrecognized policies fail closed to the failed verdict.
func TestDeriveRolloutApplyState_FailurePolicy(t *testing.T) {
	cases := []struct {
		name     string
		children []RolloutChild
		want     string
	}{
		{
			name:     "continue failure with pending sibling holds running_degraded",
			children: []RolloutChild{rc(Apply.Failed, true), rc(Apply.Pending, true)},
			want:     Apply.RunningDegraded,
		},
		{
			name:     "continue failure with running sibling holds running_degraded",
			children: []RolloutChild{rc(Apply.Failed, true), rc(Apply.Running, true)},
			want:     Apply.RunningDegraded,
		},
		{
			name:     "continue failure with all siblings terminal settles failed",
			children: []RolloutChild{rc(Apply.Failed, true), rc(Apply.Completed, true)},
			want:     Apply.Failed,
		},
		{
			name:     "continue failure with another failed continue sibling settles failed",
			children: []RolloutChild{rc(Apply.Failed, true), rc(Apply.Failed, true)},
			want:     Apply.Failed,
		},
		{
			name:     "non-continuable (halt) failure fails closed even with pending sibling",
			children: []RolloutChild{rc(Apply.Failed, false), rc(Apply.Pending, false)},
			want:     Apply.Failed,
		},
		{
			name:     "mixed: one continue failure, one halt failure fails closed",
			children: []RolloutChild{rc(Apply.Failed, true), rc(Apply.Failed, false), rc(Apply.Pending, true)},
			want:     Apply.Failed,
		},
		{
			name:     "continue failure with completed and pending holds running_degraded",
			children: []RolloutChild{rc(Apply.Failed, true), rc(Apply.Completed, true), rc(Apply.Pending, true)},
			want:     Apply.RunningDegraded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DeriveRolloutApplyState(tc.children))
		})
	}
}
