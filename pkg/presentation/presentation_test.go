package presentation

import (
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// so is the shared operation/apply state vocabulary (state.ApplyOperation == state.Apply).
var so = state.ApplyOperation

// rolling builds a rolling, halt-on-failure operation (the conservative defaults).
func rolling(dep, st string) Operation {
	return Operation{Deployment: dep, State: st, Barrier: false, HaltOnFailure: true}
}

// barrier builds a barrier, halt-on-failure operation.
func barrier(dep, st string) Operation {
	return Operation{Deployment: dep, State: st, Barrier: true, HaltOnFailure: true}
}

// continuing builds a rolling, on_failure=continue operation. HaltOnFailure and
// ContinueOnFailure are exact complements at the real boundary mappers (both
// derive from the same on_failure value), so a continue operation sets
// HaltOnFailure false and ContinueOnFailure true together.
func continuing(dep, st string) Operation {
	return Operation{Deployment: dep, State: st, HaltOnFailure: false, ContinueOnFailure: true}
}

// TestDerive_Empty: an apply with no operations rolls up to pending with no
// deployments and is not treated as multi-deployment.
func TestDerive_Empty(t *testing.T) {
	got := Derive(nil)
	assert.Equal(t, state.Apply.Pending, got.State)
	assert.Empty(t, got.Deployments)
	assert.False(t, got.MultiDeployment())
}

// TestDerive_SingleDeployment: one operation is never flagged multi-deployment,
// and the aggregate equals the single operation's state.
func TestDerive_SingleDeployment(t *testing.T) {
	got := Derive([]Operation{rolling("eu", so.Running)})
	assert.False(t, got.MultiDeployment())
	assert.Equal(t, state.Apply.Running, got.State)
	require.Len(t, got.Deployments, 1)
	assert.Equal(t, StateRunningCopy, got.Deployments[0].Presentation)
}

// TestDeriveDeployment_StateLabels: each operation state with no blocking
// siblings maps to its expected presentation state, label, emoji, and default
// expand/collapse.
func TestDeriveDeployment_StateLabels(t *testing.T) {
	cases := []struct {
		state string
		want  PresentationState
		label string
		emoji string
		open  bool
	}{
		{so.Completed, StateCompleted, "completed", "✅", false},
		{so.Running, StateRunningCopy, "running table copy", "🔄", true},
		{so.CuttingOver, StateCuttingOver, "cutting over", "🔁", true},
		{so.Failed, StateFailed, "failed", "❌", true},
		{so.FailedRetryable, StateRetrying, "retrying", "🔁", true},
		{so.Stopped, StateStopped, "stopped — resume to continue", "⏸", true},
		{so.RevertWindow, StateRevertWindow, "in revert window", "⏳", true},
		{so.Cancelled, StateCancelled, "cancelled", "⛔", false},
		{so.Reverted, StateReverted, "reverted", "↩️", false},
		{"some_engine_state", StateUnknown, "some_engine_state", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.state, func(t *testing.T) {
			// Single op so there are no earlier siblings to add ordering context.
			d := Derive([]Operation{rolling("eu", tc.state)}).Deployments[0]
			assert.Equal(t, tc.want, d.Presentation)
			assert.Equal(t, tc.label, d.Label)
			assert.Equal(t, tc.emoji, d.Emoji)
			assert.Equal(t, tc.open, d.Open)
		})
	}
}

// TestDerivePending_Ordering: a pending operation's label depends on its earlier
// siblings and the rollout policy, mirroring the claim predicate's sibling gate.
func TestDerivePending_Ordering(t *testing.T) {
	cases := []struct {
		name  string
		ops   []Operation
		want  PresentationState
		label string
	}{
		{
			name:  "rolling: earlier completed -> next in order",
			ops:   []Operation{rolling("eu", so.Completed), rolling("us", so.Pending)},
			want:  StateQueuedNext,
			label: "queued — next in order",
		},
		{
			name:  "rolling: earlier running -> waiting for it",
			ops:   []Operation{rolling("eu", so.Running), rolling("us", so.Pending)},
			want:  StateWaiting,
			label: "waiting for eu",
		},
		{
			name:  "rolling: earlier at barrier still blocks (serial)",
			ops:   []Operation{rolling("eu", so.WaitingForCutover), rolling("us", so.Pending)},
			want:  StateWaiting,
			label: "waiting for eu",
		},
		{
			name:  "barrier: earlier at barrier no longer blocks copy start",
			ops:   []Operation{barrier("eu", so.WaitingForCutover), barrier("us", so.Pending)},
			want:  StateQueuedNext,
			label: "queued — next in order",
		},
		{
			name:  "barrier: earlier in revert_window no longer blocks copy start",
			ops:   []Operation{barrier("eu", so.RevertWindow), barrier("us", so.Pending)},
			want:  StateQueuedNext,
			label: "queued — next in order",
		},
		{
			name:  "barrier: earlier still copying blocks",
			ops:   []Operation{barrier("eu", so.Running), barrier("us", so.Pending)},
			want:  StateWaiting,
			label: "waiting for eu",
		},
		{
			name:  "halt: earlier failed halts the rollout",
			ops:   []Operation{rolling("eu", so.Failed), rolling("us", so.Pending)},
			want:  StateHalted,
			label: "halted — eu failed",
		},
		{
			name: "no-halt: earlier failed no longer blocks",
			ops: []Operation{
				continuing("eu", so.Failed),
				continuing("us", so.Pending),
			},
			want:  StateQueuedNext,
			label: "queued — next in order",
		},
		{
			name:  "cancelled earlier halts regardless of halt flag",
			ops:   []Operation{continuing("eu", so.Cancelled), continuing("us", so.Pending)},
			want:  StateHalted,
			label: "halted — eu cancelled",
		},
		{
			name:  "halt naming picks the failed sibling over an in-flight one",
			ops:   []Operation{rolling("eu", so.Failed), rolling("us", so.Running), rolling("au", so.Pending)},
			want:  StateHalted,
			label: "halted — eu failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps := Derive(tc.ops).Deployments
			last := deps[len(deps)-1]
			assert.Equal(t, tc.want, last.Presentation)
			assert.Equal(t, tc.label, last.Label)
		})
	}
}

// TestDeriveWaitingForCutover_Ordering: cutover stays strictly ordered (an
// earlier sibling blocks until completed) regardless of cutover_policy, since
// the policy only relaxes copy start.
func TestDeriveWaitingForCutover_Ordering(t *testing.T) {
	cases := []struct {
		name  string
		ops   []Operation
		want  PresentationState
		label string
	}{
		{
			name:  "earlier completed -> ready, next in order",
			ops:   []Operation{rolling("eu", so.Completed), rolling("us", so.WaitingForCutover)},
			want:  StateReadyForCutoverNext,
			label: "ready for cutover — next in order",
		},
		{
			name:  "earlier still copying -> ready, waiting for it",
			ops:   []Operation{rolling("eu", so.Running), rolling("us", so.WaitingForCutover)},
			want:  StateReadyForCutoverWaiting,
			label: "ready for cutover — waiting for eu",
		},
		{
			name:  "barrier: earlier also at barrier still blocks cutover (cutover stays ordered)",
			ops:   []Operation{barrier("eu", so.WaitingForCutover), barrier("us", so.WaitingForCutover)},
			want:  StateReadyForCutoverWaiting,
			label: "ready for cutover — waiting for eu",
		},
		{
			name: "no-halt: earlier failed does not block cutover (rollout continues past it)",
			ops: []Operation{
				continuing("eu", so.Failed),
				continuing("us", so.WaitingForCutover),
			},
			want:  StateReadyForCutoverNext,
			label: "ready for cutover — next in order",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			last := Derive(tc.ops).Deployments[1]
			assert.Equal(t, tc.want, last.Presentation)
			assert.Equal(t, tc.label, last.Label)
		})
	}
}

// TestDerive_AggregateBarrierWorkedExample: the design note's barrier worked
// example — eu at the barrier, us copying, au and ca waiting — rolls up to a
// running aggregate with a cutover next-action on eu and a per-status histogram.
func TestDerive_AggregateBarrierWorkedExample(t *testing.T) {
	got := Derive([]Operation{
		barrier("eu", so.WaitingForCutover),
		barrier("us", so.Running),
		barrier("au", so.Pending),
		barrier("ca", so.Pending),
	})

	assert.True(t, got.MultiDeployment())
	assert.Equal(t, state.Apply.Running, got.State)
	assert.Equal(t, "running", got.Label)

	// eu ready (next), us running, au waiting on us, ca waiting on au.
	assert.Equal(t, StateReadyForCutoverNext, got.Deployments[0].Presentation)
	assert.Equal(t, StateRunningCopy, got.Deployments[1].Presentation)
	assert.Equal(t, StateWaiting, got.Deployments[2].Presentation)
	assert.Equal(t, "waiting for us", got.Deployments[2].Label)
	// ca names the earliest blocking sibling (us, still copying), not its
	// immediate predecessor au: under barrier eu is non-blocking and au is itself
	// pending, so the deployment whose progress unblocks the line is us — the same
	// one the claim predicate is gated on.
	assert.Equal(t, StateWaiting, got.Deployments[3].Presentation)
	assert.Equal(t, "waiting for us", got.Deployments[3].Label)

	assert.Equal(t, NextAction{Kind: NextActionCutover, Deployment: "eu"}, got.NextAction)
	assert.Equal(t, []StateCount{
		{Label: "ready for cutover", Count: 1},
		{Label: "running", Count: 1},
		{Label: "waiting", Count: 2},
	}, got.Counts)
}

// TestDerive_AggregateFailedHaltExample: with halt_on_failure on, a failed
// deployment halts the rest; the aggregate stays failed (fail-closed) and points
// the operator at the failed deployment.
func TestDerive_AggregateFailedHaltExample(t *testing.T) {
	got := Derive([]Operation{
		rolling("eu", so.WaitingForCutover),
		rolling("us", so.Failed),
		rolling("au", so.Pending),
		rolling("ca", so.Pending),
	})

	assert.Equal(t, state.Apply.Failed, got.State)
	assert.Equal(t, "failed", got.Label)
	assert.Equal(t, NextAction{Kind: NextActionReviewFailure, Deployment: "us"}, got.NextAction)
	assert.Equal(t, StateHalted, got.Deployments[2].Presentation)
	assert.Equal(t, "halted — us failed", got.Deployments[2].Label)
	assert.Equal(t, StateHalted, got.Deployments[3].Presentation)
}

// TestDerive_CompletedWithOneFailedDeployment: under on_failure continue the
// rollout runs every sibling to a terminal state, but the verdict still
// reflects the failure — once all siblings are terminal the aggregate settles
// to failed, not running_degraded.
func TestDerive_CompletedWithOneFailedDeployment(t *testing.T) {
	got := Derive([]Operation{
		continuing("eu", so.Completed),
		continuing("us", so.Completed),
		continuing("au", so.Failed),
		continuing("ca", so.Completed),
	})

	assert.Equal(t, state.Apply.Failed, got.State)
	assert.Equal(t, StateFailed, got.Deployments[2].Presentation)
	assert.Equal(t, []StateCount{
		{Label: "completed", Count: 3},
		{Label: "failed", Count: 1},
	}, got.Counts)
}

// TestDerive_StoppedAggregateNextAction: a stopped deployment makes the aggregate
// stopped and suggests resuming.
func TestDerive_StoppedAggregateNextAction(t *testing.T) {
	got := Derive([]Operation{
		rolling("eu", so.Completed),
		rolling("us", so.Stopped),
	})
	assert.Equal(t, state.Apply.Stopped, got.State)
	assert.Equal(t, NextAction{Kind: NextActionResume}, got.NextAction)
}

// TestDerive_FailedDeploymentCarriesError: the failed deployment surfaces its
// error detail for the renderer.
func TestDerive_FailedDeploymentCarriesError(t *testing.T) {
	got := Derive([]Operation{
		{Deployment: "eu", State: so.Failed, HaltOnFailure: true, Error: "lock wait timeout"},
	})
	require.Len(t, got.Deployments, 1)
	assert.Equal(t, "lock wait timeout", got.Deployments[0].Error)
}

// TestDerive_FirstFailureNoneWhenHealthy: a rollout with no failed deployment
// exposes no first failure.
func TestDerive_FirstFailureNoneWhenHealthy(t *testing.T) {
	got := Derive([]Operation{
		rolling("eu", so.Completed),
		rolling("us", so.Running),
	})
	assert.Nil(t, got.FirstFailure)
}

// TestDerive_FirstFailurePicksEarliestInOrder: with more than one failed
// deployment, the first failure is the earliest in resolved order and carries
// its error, mirroring the first-failed-operation the persisted aggregate
// ErrorMessage is stamped from.
func TestDerive_FirstFailurePicksEarliestInOrder(t *testing.T) {
	got := Derive([]Operation{
		continuing("eu", so.Completed),
		{Deployment: "us", State: so.Failed, HaltOnFailure: false, ContinueOnFailure: true, Error: "first boom"},
		{Deployment: "au", State: so.Failed, HaltOnFailure: false, ContinueOnFailure: true, Error: "second boom"},
	})
	require.NotNil(t, got.FirstFailure)
	assert.Equal(t, "us", got.FirstFailure.Deployment)
	assert.Equal(t, "first boom", got.FirstFailure.Error)
}

// TestDerive_FirstFailureWhileSiblingStillRunning: under on_failure continue an
// earlier deployment can be failed while a later one is still copying. The
// rollout is held running_degraded so the live sibling runs to completion, and
// the first failure is surfaced eagerly onto the in-progress comment rather than
// waiting for the terminal summary.
func TestDerive_FirstFailureWhileSiblingStillRunning(t *testing.T) {
	got := Derive([]Operation{
		{Deployment: "eu", State: so.Failed, HaltOnFailure: false, ContinueOnFailure: true, Error: "boom"},
		continuing("us", so.Running),
	})
	assert.Equal(t, state.Apply.RunningDegraded, got.State)
	assert.Equal(t, "running (degraded)", got.Label)
	assert.Equal(t, NextAction{Kind: NextActionNone}, got.NextAction)
	assert.Equal(t, StateRunningCopy, got.Deployments[1].Presentation)
	require.NotNil(t, got.FirstFailure)
	assert.Equal(t, "eu", got.FirstFailure.Deployment)
}

// TestDerive_HaltFailureWithRunningSiblingStaysFailed: a halt-policy failure is
// fail-closed even while a sibling is still running — only on_failure continue
// holds the rollout running_degraded.
func TestDerive_HaltFailureWithRunningSiblingStaysFailed(t *testing.T) {
	got := Derive([]Operation{
		rolling("eu", so.Failed),
		rolling("us", so.Running),
	})
	assert.Equal(t, state.Apply.Failed, got.State)
	assert.Equal(t, "failed", got.Label)
}

// TestDerive_FirstFailureExcludesRetrying: a failed_retryable deployment is
// still in progress, so it is not surfaced as a first failure.
func TestDerive_FirstFailureExcludesRetrying(t *testing.T) {
	got := Derive([]Operation{
		rolling("eu", so.Completed),
		rolling("us", so.FailedRetryable),
	})
	assert.Nil(t, got.FirstFailure)
}
