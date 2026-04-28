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
		{"RUNNING", "FAILED"},
		{"COMPLETED", "FAILED"},
		{"WAITING_FOR_CUTOVER", "FAILED", "COMPLETED"},
		{"PENDING", "RUNNING", "FAILED"},
	}

	for _, states := range testCases {
		assert.Equal(t, Apply.Failed, DeriveApplyState(states), "input: %v", states)
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
		Apply.WaitingForCutover,
		Apply.CuttingOver,
		Apply.RevertWindow,
	}

	for _, s := range nonTerminalStates {
		assert.False(t, IsTerminalApplyState(s), "%s should NOT be terminal", s)
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
		{"WAITING_FOR_CUTOVER", Apply.WaitingForCutover},
		{"waiting_for_cutover", Apply.WaitingForCutover},
		{"CUTTING_OVER", Apply.CuttingOver},
		{"cutting_over", Apply.CuttingOver},
		{"COMPLETED", Apply.Completed},
		{"completed", Apply.Completed},
		{"FAILED", Apply.Failed},
		{"failed", Apply.Failed},
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

// FailedRetryable tests

func TestDeriveApplyState_FailedRetryable(t *testing.T) {
	// FailedRetryable without permanent Failed → apply is FailedRetryable
	states := []string{"COMPLETED", "FAILED_RETRYABLE", "RUNNING"}
	assert.Equal(t, Apply.FailedRetryable, DeriveApplyState(states))
}

func TestDeriveApplyState_FailedRetryable_AllRetryable(t *testing.T) {
	states := []string{"FAILED_RETRYABLE", "FAILED_RETRYABLE"}
	assert.Equal(t, Apply.FailedRetryable, DeriveApplyState(states))
}

func TestDeriveApplyState_FailedOverridesFailedRetryable(t *testing.T) {
	// Permanent Failed takes priority over FailedRetryable
	states := []string{"FAILED_RETRYABLE", "FAILED", "COMPLETED"}
	assert.Equal(t, Apply.Failed, DeriveApplyState(states))
}

func TestDeriveApplyState_FailedRetryable_NormalizedInput(t *testing.T) {
	states := []string{"failed_retryable"}
	assert.Equal(t, Apply.FailedRetryable, DeriveApplyState(states))
}

func TestIsTerminalApplyState_FailedRetryable(t *testing.T) {
	assert.False(t, IsTerminalApplyState(Apply.FailedRetryable),
		"FailedRetryable should NOT be terminal — recovery loop can retry")
}

func TestNormalizeApplyState_FailedRetryable(t *testing.T) {
	assert.Equal(t, Apply.FailedRetryable, normalizeApplyState("FAILED_RETRYABLE"))
	assert.Equal(t, Apply.FailedRetryable, normalizeApplyState("failed_retryable"))
}
