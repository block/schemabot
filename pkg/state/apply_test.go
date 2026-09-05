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

// A revert in flight surfaces the apply as reverting, and outranks a
// fully-reverted sibling so a multi-table revert reads as in-progress until
// every table has finished reverting.
func TestDeriveApplyState_Reverting(t *testing.T) {
	testCases := [][]string{
		{"REVERTING"},
		{"COMPLETED", "REVERTING"},
		{"REVERTING", "REVERTED"},
	}

	for _, states := range testCases {
		assert.Equal(t, Apply.Reverting, DeriveApplyState(states), "input: %v", states)
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

// The post-copy phases surface at the apply level as the least-advanced
// active phase, and only once every table has started: while any table is
// still copying rows — or still queued with its whole copy ahead of it — the
// apply is Running. Any table still copying dominates a more-advanced
// sibling's phase.
func TestDeriveApplyState_PostCopyPhases(t *testing.T) {
	testCases := []struct {
		states   []string
		expected string
	}{
		{[]string{"catching_up"}, Apply.CatchingUp},
		{[]string{"CATCHING_UP"}, Apply.CatchingUp},
		{[]string{"checksumming"}, Apply.Checksumming},
		{[]string{"CHECKSUMMING"}, Apply.Checksumming},
		{[]string{"post_checksum"}, Apply.PostChecksum},
		{[]string{"POST_CHECKSUM"}, Apply.PostChecksum},
		// A table still copying dominates any sibling's post-copy phase.
		{[]string{"RUNNING", "catching_up"}, Apply.Running},
		{[]string{"RUNNING", "checksumming"}, Apply.Running},
		{[]string{"RUNNING", "post_checksum"}, Apply.Running},
		// The least-advanced phase wins across mixed drains.
		{[]string{"catching_up", "checksumming"}, Apply.CatchingUp},
		{[]string{"checksumming", "post_checksum"}, Apply.Checksumming},
		// A queued sibling keeps the apply Running: its whole copy is still
		// ahead, so naming a sibling's drain phase would overstate progress.
		{[]string{"checksumming", "PENDING"}, Apply.Running},
		{[]string{"catching_up", "PENDING"}, Apply.Running},
		{[]string{"post_checksum", "PENDING"}, Apply.Running},
		{[]string{"COMPLETED", "checksumming", "PENDING"}, Apply.Running},
		{[]string{"COMPLETED", "catching_up", "PENDING"}, Apply.Running},
		{[]string{"COMPLETED", "post_checksum", "PENDING"}, Apply.Running},
		// A completed sibling alone does not mask the active phase.
		{[]string{"COMPLETED", "checksumming"}, Apply.Checksumming},
		{[]string{"COMPLETED", "catching_up"}, Apply.CatchingUp},
		{[]string{"COMPLETED", "post_checksum"}, Apply.PostChecksum},
		// A cutover is a table's last step, so a sibling still draining or
		// verifying holds the apply on that earlier phase — least-advanced
		// active work wins, and the apply never falls back from cutting_over
		// when the cutover completes ahead of its siblings.
		{[]string{"CUTTING_OVER", "post_checksum"}, Apply.PostChecksum},
		{[]string{"CUTTING_OVER", "catching_up"}, Apply.CatchingUp},
		{[]string{"COMPLETED", "CUTTING_OVER", "checksumming"}, Apply.Checksumming},
		// A sibling that is still queued or still copying keeps a cutover from
		// surfacing the same way: a drive cuts tables over as each finishes —
		// sequentially, rolling, or across concurrent shards — so the apply
		// stays Running until every table has finished its copy.
		{[]string{"CUTTING_OVER", "PENDING"}, Apply.Running},
		{[]string{"COMPLETED", "CUTTING_OVER", "PENDING"}, Apply.Running},
		{[]string{"CUTTING_OVER", "RUNNING", "PENDING"}, Apply.Running},
		{[]string{"CUTTING_OVER", "RUNNING"}, Apply.Running},
		{[]string{"COMPLETED", "CUTTING_OVER", "RUNNING"}, Apply.Running},
		// Pending tasks with no active sibling stay Pending, not Running.
		{[]string{"PENDING", "PENDING"}, Apply.Pending},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expected, DeriveApplyState(tc.states), "input: %v", tc.states)
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

// A cutover surfaces at the apply level only once it is the least advanced
// active work; the earlier-phase sibling cases live in
// TestDeriveApplyState_PostCopyPhases. A parked WAITING_FOR_CUTOVER sibling
// does not hold a cutover back: it is waiting on a command, not working.
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
		Apply.Paused,
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
// off: running, running_degraded, and the post-copy phases (catching_up,
// checksumming, post_checksum) are running-family; other non-terminal states
// (pending, waiting_for_cutover, recovering) are not.
func TestIsRunningApplyState(t *testing.T) {
	for _, s := range []string{
		Apply.Running, Apply.RunningDegraded, "RUNNING", "STATE_RUNNING_DEGRADED", "running_degraded",
		Apply.CatchingUp, Apply.Checksumming, Apply.PostChecksum, "CATCHING_UP", "STATE_POST_CHECKSUM",
	} {
		assert.Truef(t, IsRunningApplyState(s), "%s should be running-family", s)
	}
	for _, s := range []string{
		Apply.Pending,
		Apply.Paused,
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
		{"PAUSED", Apply.Paused},
		{"paused", Apply.Paused},
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
	assert.Equal(t, Apply.CatchingUp, normalizeApplyState("CATCHING_UP"))
	assert.Equal(t, Apply.Checksumming, normalizeApplyState("CHECKSUMMING"))
	assert.Equal(t, Apply.PostChecksum, normalizeApplyState("POST_CHECKSUM"))
}

func TestInitialActiveApplyState(t *testing.T) {
	// Spirit begins copying rows immediately, so its first active phase is Running.
	for _, engine := range []string{"spirit", "Spirit", "strata", "", "unknown"} {
		assert.Equal(t, Apply.Running, InitialActiveApplyState(engine),
			"non-PlanetScale engine %q should dispatch into Running", engine)
	}

	// PlanetScale begins by preparing a branch, so its first active phase is
	// PreparingBranch — not the row-copy Running phase.
	for _, engine := range []string{"planetscale", "PlanetScale", "ENGINE_PLANETSCALE"} {
		assert.Equal(t, Apply.PreparingBranch, InitialActiveApplyState(engine),
			"PlanetScale engine %q should dispatch into PreparingBranch", engine)
	}
}

// rc builds a RolloutChild from a state and its continuation policy so the
// truth-table cases below read like the rollout scenario they describe.
func rc(state string, continueOnFailure bool) RolloutChild {
	return RolloutChild{State: state, ContinueOnFailure: continueOnFailure}
}

// rcPause builds a RolloutChild under an unreleased on_failure=pause policy.
// A released pause behaves like continue, so those cases use rc(state, true).
func rcPause(state string) RolloutChild {
	return RolloutChild{State: state, PauseOnFailure: true}
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
// base case: continue holds the apply active until siblings settle, while halt
// and unrecognized policies fail closed to the failed verdict. Failing closed
// decides the verdict, not when it is recorded — a fail-closed policy refuses
// new claims and cancels nothing, so a sibling that a driver already started still
// holds the apply degraded, while a sibling that is only pending holds nothing.
// The pause policy has its own truth table in
// TestDeriveRolloutApplyState_PausePolicy.
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
			name:     "continue failure with all siblings settled settles failed",
			children: []RolloutChild{rc(Apply.Failed, true), rc(Apply.Completed, true)},
			want:     Apply.Failed,
		},
		{
			name:     "continue failure with stopped sibling holds running_degraded",
			children: []RolloutChild{rc(Apply.Failed, true), rc(Apply.Stopped, true)},
			want:     Apply.RunningDegraded,
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
		{
			name:     "halt failure with running sibling holds running_degraded",
			children: []RolloutChild{rc(Apply.Failed, false), rc(Apply.Running, false)},
			want:     Apply.RunningDegraded,
		},
		{
			name:     "halt failure with sibling parked at the cutover barrier holds running_degraded",
			children: []RolloutChild{rc(Apply.Failed, false), rc(Apply.WaitingForCutover, false)},
			want:     Apply.RunningDegraded,
		},
		{
			name:     "halt failure with retrying sibling holds running_degraded",
			children: []RolloutChild{rc(Apply.Failed, false), rc(Apply.FailedRetryable, false)},
			want:     Apply.RunningDegraded,
		},
		{
			name:     "halt failure with running and pending siblings holds running_degraded",
			children: []RolloutChild{rc(Apply.Failed, false), rc(Apply.Running, false), rc(Apply.Pending, false)},
			want:     Apply.RunningDegraded,
		},
		{
			name:     "halt failure settles failed once the started sibling completes",
			children: []RolloutChild{rc(Apply.Failed, false), rc(Apply.Completed, false), rc(Apply.Pending, false)},
			want:     Apply.Failed,
		},
		{
			name:     "halt failure with stopped sibling holds running_degraded",
			children: []RolloutChild{rc(Apply.Failed, false), rc(Apply.Stopped, false)},
			want:     Apply.RunningDegraded,
		},
		{
			name:     "halt failure with cancelled sibling settles failed",
			children: []RolloutChild{rc(Apply.Failed, false), rc(Apply.Cancelled, false)},
			want:     Apply.Failed,
		},
		{
			name:     "halt failure with a continue sibling still running holds running_degraded",
			children: []RolloutChild{rc(Apply.Failed, false), rc(Apply.Running, true)},
			want:     Apply.RunningDegraded,
		},
		{
			name: "invalid both-flags failure holds running_degraded while a sibling works",
			children: []RolloutChild{
				{State: Apply.Failed, ContinueOnFailure: true, PauseOnFailure: true},
				rc(Apply.Running, true),
			},
			want: Apply.RunningDegraded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DeriveRolloutApplyState(tc.children))
		})
	}
}

// A fail-closed rollout holds its verdict open while any sibling still holds the
// deployment it was given, and the line is settled rather than terminal. Every
// state in the registry is classified against that rule here, so a state added
// later cannot inherit an answer: a pending sibling has touched nothing, a
// sibling whose verdict is final has released its target, and everything else
// keeps the parent's reservation alive — including stopped, which is terminal
// for claiming but resumable, so a driver may write to that target again.
func TestDeriveRolloutApplyState_HaltHoldsWhileASiblingHoldsItsTarget(t *testing.T) {
	for sibling := range applyMetadata {
		t.Run(sibling, func(t *testing.T) {
			want := Apply.Failed
			if !IsState(sibling, SettledApplyStates...) && !IsState(sibling, Apply.Pending) {
				want = Apply.RunningDegraded
			}
			children := []RolloutChild{rc(Apply.Failed, false), rc(sibling, false)}
			assert.Equal(t, want, DeriveRolloutApplyState(children),
				"a halted rollout with a %s sibling", sibling)
		})
	}
}

// A held-open rollout and a stranded parent present the same way to the
// recovery claim — a non-terminal apply over children that have all reached a
// terminal state — and only the settled line tells them apart. Every state in
// the registry is classified against that rule here, so a state added later
// cannot inherit an answer: a child that has not reached a terminal state means
// a drive can still move the parent, a child that settled has released its
// target, and a terminal-but-resumable child is the one that holds the rollout
// open for an operator's start.
func TestRolloutHeldByResumableChild(t *testing.T) {
	for child := range applyMetadata {
		t.Run(child, func(t *testing.T) {
			want := IsTerminalApplyState(child) && !IsState(child, SettledApplyStates...)
			children := []RolloutChild{rc(Apply.Failed, true), rc(child, true)}
			assert.Equal(t, want, RolloutHeldByResumableChild(Apply.RunningDegraded, children),
				"a held-open rollout with a %s child", child)
		})
	}
}

// A rollout that reached a terminal state is never held open, whatever its
// children look like: the verdict is recorded and the target released, so the
// recovery claim has nothing to reconsider.
func TestRolloutHeldByResumableChildIgnoresTerminalRollouts(t *testing.T) {
	children := []RolloutChild{rc(Apply.Failed, true), rc(Apply.Stopped, true)}
	for derived := range applyMetadata {
		if !IsTerminalApplyState(derived) {
			continue
		}
		assert.False(t, RolloutHeldByResumableChild(derived, children),
			"a %s rollout is not held open", derived)
	}
}

// TestDeriveRolloutApplyState_PausePolicy is the truth table for on_failure=pause.
// An unreleased pause failure holds the apply paused while later siblings still
// hold their targets, settles failed once nothing is left to hold, and — once
// released — behaves exactly like continue. Children are in deployment order.
func TestDeriveRolloutApplyState_PausePolicy(t *testing.T) {
	cases := []struct {
		name     string
		children []RolloutChild
		want     string
	}{
		{
			name:     "pause failure with later pending sibling holds paused",
			children: []RolloutChild{rcPause(Apply.Failed), rcPause(Apply.Pending)},
			want:     Apply.Paused,
		},
		{
			name:     "pause failure with later running sibling holds paused",
			children: []RolloutChild{rcPause(Apply.Failed), rcPause(Apply.Running)},
			want:     Apply.Paused,
		},
		{
			name:     "pause failure with all later siblings terminal settles failed",
			children: []RolloutChild{rcPause(Apply.Failed), rcPause(Apply.Completed)},
			want:     Apply.Failed,
		},
		{
			name:     "pause failure with later sibling stopped holds paused",
			children: []RolloutChild{rcPause(Apply.Failed), rcPause(Apply.Stopped)},
			want:     Apply.Paused,
		},
		{
			name:     "pause failure with later sibling cancelled settles failed",
			children: []RolloutChild{rcPause(Apply.Failed), rcPause(Apply.Cancelled)},
			want:     Apply.Failed,
		},
		{
			name:     "single pause failure with nothing to hold settles failed",
			children: []RolloutChild{rcPause(Apply.Failed)},
			want:     Apply.Failed,
		},
		{
			name:     "pause failure last in order with earlier sibling still running settles running_degraded",
			children: []RolloutChild{rcPause(Apply.Running), rcPause(Apply.Failed)},
			want:     Apply.RunningDegraded,
		},
		{
			name:     "earlier pause-held failure holds paused even when a later pause failure has nothing to hold",
			children: []RolloutChild{rcPause(Apply.Failed), rcPause(Apply.Pending), rcPause(Apply.Failed)},
			want:     Apply.Paused,
		},
		{
			name:     "released pause (continue) failure with pending sibling holds running_degraded",
			children: []RolloutChild{rc(Apply.Failed, true), rc(Apply.Pending, true)},
			want:     Apply.RunningDegraded,
		},
		{
			name:     "released pause (continue) failure with all siblings terminal settles failed",
			children: []RolloutChild{rc(Apply.Failed, true), rc(Apply.Completed, true)},
			want:     Apply.Failed,
		},
		{
			name:     "halt failure dominates a pause-held sibling and fails closed",
			children: []RolloutChild{rcPause(Apply.Failed), rc(Apply.Failed, false), rcPause(Apply.Pending)},
			want:     Apply.Failed,
		},
		{
			name:     "halt failure dominating a pause-held sibling still holds while later work runs",
			children: []RolloutChild{rcPause(Apply.Failed), rc(Apply.Failed, false), rcPause(Apply.Running)},
			want:     Apply.RunningDegraded,
		},
		{
			name: "invalid both-flags failure fails closed rather than continuing",
			children: []RolloutChild{
				{State: Apply.Failed, ContinueOnFailure: true, PauseOnFailure: true},
				rc(Apply.Pending, true),
			},
			want: Apply.Failed,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DeriveRolloutApplyState(tc.children))
		})
	}
}

// Settled is the terminal set minus Stopped, and the gap is load-bearing: a
// stopped apply is terminal but re-claimable, so anything that writes rows
// belonging to an apply it does not hold must gate on settled, never on
// terminal.
func TestSettledApplyStatesExcludeStopped(t *testing.T) {
	settled := func(s string) bool { return IsState(s, SettledApplyStates...) }

	for _, s := range SettledApplyStates {
		assert.True(t, IsTerminalApplyState(s), "%s is settled so it must also be terminal", s)
	}

	assert.True(t, IsTerminalApplyState(Apply.Stopped), "stopped is terminal for claiming")
	assert.False(t, settled(Apply.Stopped), "stopped is re-claimable, so its verdict is not final")

	for _, s := range []string{Apply.Running, Apply.Pending, Apply.Resuming, Apply.FailedRetryable} {
		assert.False(t, settled(s), "%s is not settled", s)
	}
}
