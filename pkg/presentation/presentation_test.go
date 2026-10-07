package presentation

import (
	"slices"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// so is the shared operation/apply state vocabulary (state.ApplyOperation == state.Apply).
var so = state.ApplyOperation

// rolling builds a rolling, halt-on-failure operation (the conservative
// defaults: both policy flags false).
func rolling(dep, st string) Operation {
	return Operation{Deployment: dep, State: st, Barrier: false}
}

// barrier builds a barrier, halt-on-failure operation.
func barrier(dep, st string) Operation {
	return Operation{Deployment: dep, State: st, Barrier: true}
}

// parallel builds a parallel, halt-on-failure operation. Under parallel the copy
// phase has no earlier-sibling gate, so a pending parallel operation is never
// shown waiting for or halted by an earlier sibling.
func parallel(dep, st string) Operation {
	return Operation{Deployment: dep, State: st, Parallel: true}
}

// continuing builds a rolling, on_failure=continue operation.
func continuing(dep, st string) Operation {
	return Operation{Deployment: dep, State: st, ContinueOnFailure: true}
}

// pausing builds a rolling, on_failure=pause operation with no release latch: a
// terminal-failed earlier sibling holds it paused for a human.
func pausing(dep, st string) Operation {
	return Operation{Deployment: dep, State: st, PauseOnFailure: true}
}

// released builds a rolling, on_failure=pause operation whose apply has been
// released: a terminal-failed earlier sibling is treated like continue.
func released(dep, st string) Operation {
	return Operation{Deployment: dep, State: st, PauseOnFailure: true, Released: true}
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
		{so.Stopped, StateStopped, "stopped — resume to continue", "⏹️", true},
		{so.RevertWindow, StateRevertWindow, "in revert window", "⏳", true},
		{so.Cancelled, StateCancelled, "cancelled", "🚫", false},
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
			name:  "parallel: earlier still copying does not block (concurrent copy)",
			ops:   []Operation{parallel("eu", so.Running), parallel("us", so.Pending)},
			want:  StateQueuedNext,
			label: "queued — next in order",
		},
		{
			name:  "parallel: earlier pending does not block",
			ops:   []Operation{parallel("eu", so.Pending), parallel("us", so.Pending)},
			want:  StateQueuedNext,
			label: "queued — next in order",
		},
		{
			name:  "parallel: earlier failed does not halt copy start",
			ops:   []Operation{parallel("eu", so.Failed), parallel("us", so.Pending)},
			want:  StateQueuedNext,
			label: "queued — next in order",
		},
		{
			name:  "parallel: earlier cancelled does not halt copy start",
			ops:   []Operation{parallel("eu", so.Cancelled), parallel("us", so.Pending)},
			want:  StateQueuedNext,
			label: "queued — next in order",
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
			name:  "pause: earlier failed holds the rollout paused for a human",
			ops:   []Operation{pausing("eu", so.Failed), pausing("us", so.Pending)},
			want:  StatePaused,
			label: "paused — eu failed; release or stop",
		},
		{
			name:  "released pause: earlier failed no longer blocks, like continue",
			ops:   []Operation{released("eu", so.Failed), released("us", so.Pending)},
			want:  StateQueuedNext,
			label: "queued — next in order",
		},
		{
			name:  "pause: earlier cancelled halts rather than pauses",
			ops:   []Operation{pausing("eu", so.Cancelled), pausing("us", so.Pending)},
			want:  StateHalted,
			label: "halted — eu cancelled",
		},
		{
			name:  "pause naming picks the failed sibling over an in-flight one",
			ops:   []Operation{pausing("eu", so.Failed), pausing("us", so.Running), pausing("au", so.Pending)},
			want:  StatePaused,
			label: "paused — eu failed; release or stop",
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
			name:  "parallel: earlier also at barrier still blocks cutover (cutover stays ordered)",
			ops:   []Operation{parallel("eu", so.WaitingForCutover), parallel("us", so.WaitingForCutover)},
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
		{
			name: "pause: earlier failed still blocks cutover until released",
			ops: []Operation{
				pausing("eu", so.Failed),
				pausing("us", so.WaitingForCutover),
			},
			want:  StateReadyForCutoverWaiting,
			label: "ready for cutover — waiting for eu",
		},
		{
			name: "released pause: earlier failed no longer blocks cutover",
			ops: []Operation{
				released("eu", so.Failed),
				released("us", so.WaitingForCutover),
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

	assert.Equal(t, NextAction{Kind: NextActionCutover, Deployment: "eu", Name: "eu"}, got.NextAction)
	assert.Equal(t, []StateCount{
		{Label: "ready for cutover", Count: 1},
		{Label: "running", Count: 1},
		{Label: "waiting", Count: 2},
	}, got.Counts)
}

// TestDerive_AggregateFailedHaltExample: with halt_on_failure on, a failed
// deployment halts every deployment that has not started. The queued ones render
// halted and the operator is pointed at the failure, while eu — already parked at
// the cutover barrier holding its database — keeps the aggregate degraded until
// it is resolved, since the halt refuses new claims rather than stopping work
// already under way.
func TestDerive_AggregateFailedHaltExample(t *testing.T) {
	got := Derive([]Operation{
		rolling("eu", so.WaitingForCutover),
		rolling("us", so.Failed),
		rolling("au", so.Pending),
		rolling("ca", so.Pending),
	})

	assert.Equal(t, state.Apply.RunningDegraded, got.State)
	assert.Equal(t, "running (degraded)", got.Label)
	assert.Equal(t, NextAction{Kind: NextActionReviewFailure, Deployment: "us", Name: "us"}, got.NextAction)
	assert.Equal(t, StateHalted, got.Deployments[2].Presentation)
	assert.Equal(t, "halted — us failed", got.Deployments[2].Label)
	assert.Equal(t, StateHalted, got.Deployments[3].Presentation)
}

// TestDerive_AggregateFailedHaltSettlesOnceStartedWorkEnds: the same halted
// rollout once nothing is left working. Only queued deployments remain
// non-terminal, and the halt is what keeps them queued, so the aggregate takes
// the failed verdict.
func TestDerive_AggregateFailedHaltSettlesOnceStartedWorkEnds(t *testing.T) {
	got := Derive([]Operation{
		rolling("eu", so.Completed),
		rolling("us", so.Failed),
		rolling("au", so.Pending),
		rolling("ca", so.Pending),
	})

	assert.Equal(t, state.Apply.Failed, got.State)
	assert.Equal(t, "failed", got.Label)
	assert.Equal(t, NextAction{Kind: NextActionReviewFailure, Deployment: "us", Name: "us"}, got.NextAction)
	assert.Equal(t, StateHalted, got.Deployments[2].Presentation)
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
		{Deployment: "eu", State: so.Failed, Error: "lock wait timeout"},
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
		{Deployment: "us", State: so.Failed, ContinueOnFailure: true, Error: "first boom"},
		{Deployment: "au", State: so.Failed, ContinueOnFailure: true, Error: "second boom"},
	})
	require.NotNil(t, got.FirstFailure)
	assert.Equal(t, "us", got.FirstFailure.Deployment)
	assert.Equal(t, "first boom", got.FirstFailure.Error)
}

// TestDerive_FirstFailureWhileSiblingStillRunning: under on_failure continue an
// earlier deployment can be failed while a later one is still copying. The
// rollout is held running_degraded so the live sibling runs to completion, and
// the first failure is surfaced eagerly onto the in-progress comment rather than
// waiting for the terminal summary. The rollout still has work ahead of it, so
// it offers no review-failure next action while it is in flight.
func TestDerive_FirstFailureWhileSiblingStillRunning(t *testing.T) {
	got := Derive([]Operation{
		{Deployment: "eu", State: so.Failed, ContinueOnFailure: true, Error: "boom"},
		continuing("us", so.Running),
	})
	assert.Equal(t, state.Apply.RunningDegraded, got.State)
	assert.Equal(t, "running (degraded)", got.Label)
	assert.Equal(t, NextAction{Kind: NextActionNone}, got.NextAction)
	assert.Equal(t, StateRunningCopy, got.Deployments[1].Presentation)
	require.NotNil(t, got.FirstFailure)
	assert.Equal(t, "eu", got.FirstFailure.Deployment)
}

// TestDerive_HaltFailureWithRunningSiblingRunsDegraded: a halt-policy failure is
// fail-closed on the verdict, but the halt only refuses new claims — a sibling
// already copying is unaffected by it, so the rollout runs degraded until that
// sibling is terminal and the operator can still stop it.
func TestDerive_HaltFailureWithRunningSiblingRunsDegraded(t *testing.T) {
	got := Derive([]Operation{
		rolling("eu", so.Failed),
		rolling("us", so.Running),
	})
	assert.Equal(t, state.Apply.RunningDegraded, got.State)
	assert.Equal(t, "running (degraded)", got.Label)
	assert.Equal(t, NextAction{Kind: NextActionReviewFailure, Deployment: "eu", Name: "eu"}, got.NextAction)
}

// TestDerive_BothFailurePolicyFlagsPointsAtTheFailure: the two on_failure flags
// are mutually exclusive, so a deployment carrying both is a caller bug. The
// projection resolves it the way the aggregate state does — fail closed — so a
// bad policy value cannot quietly cost the operator the pointer to the failure
// that decided the rollout.
func TestDerive_BothFailurePolicyFlagsPointsAtTheFailure(t *testing.T) {
	got := Derive([]Operation{
		{Deployment: "eu", State: so.Failed, ContinueOnFailure: true, PauseOnFailure: true, Error: "boom"},
		{Deployment: "us", State: so.Running, ContinueOnFailure: true, PauseOnFailure: true},
	})
	assert.Equal(t, state.Apply.RunningDegraded, got.State)
	assert.Equal(t, NextAction{Kind: NextActionReviewFailure, Deployment: "eu", Name: "eu"}, got.NextAction)
}

// TestDerive_PauseFailureWithPendingSiblingHoldsPaused: under on_failure pause an
// earlier failure with later work still to run holds the aggregate paused for a
// human, and the first failure is surfaced eagerly. The held sibling renders
// paused with the release-or-stop label.
func TestDerive_PauseFailureWithPendingSiblingHoldsPaused(t *testing.T) {
	got := Derive([]Operation{
		{Deployment: "eu", State: so.Failed, PauseOnFailure: true, Error: "boom"},
		pausing("us", so.Pending),
	})
	assert.Equal(t, state.Apply.Paused, got.State)
	assert.Equal(t, "paused", got.Label)
	assert.Equal(t, StatePaused, got.Deployments[1].Presentation)
	assert.Equal(t, "paused — eu failed; release or stop", got.Deployments[1].Label)
	require.NotNil(t, got.FirstFailure)
	assert.Equal(t, "eu", got.FirstFailure.Deployment)
}

// TestDerive_ReleasedPauseRunsDegradedLikeContinue: once an apply is released a
// pause failure behaves like continue — the held siblings proceed and the
// aggregate runs degraded rather than paused.
func TestDerive_ReleasedPauseRunsDegradedLikeContinue(t *testing.T) {
	got := Derive([]Operation{
		{Deployment: "eu", State: so.Failed, PauseOnFailure: true, Released: true, Error: "boom"},
		released("us", so.Running),
	})
	assert.Equal(t, state.Apply.RunningDegraded, got.State)
	assert.Equal(t, "running (degraded)", got.Label)
	assert.Equal(t, StateRunningCopy, got.Deployments[1].Presentation)
	require.NotNil(t, got.FirstFailure)
	assert.Equal(t, "eu", got.FirstFailure.Deployment)
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

// TestDerive_MultiTargetMemberNames: when one deployment addresses several
// targets, every member of that deployment is named by the routing pair, so a
// surface never labels two members identically. A sibling deployment that
// addresses a single target keeps its plain name.
func TestDerive_MultiTargetMemberNames(t *testing.T) {
	got := Derive([]Operation{
		{Deployment: "primary", Target: "testapp-001", State: so.Completed},
		{Deployment: "primary", Target: "testapp-002", State: so.Running},
		{Deployment: "eu-west", Target: "orders-eu", State: so.Pending},
	})
	require.Len(t, got.Deployments, 3)
	assert.Equal(t, "primary/testapp-001", got.Deployments[0].Name)
	assert.Equal(t, "primary/testapp-002", got.Deployments[1].Name)
	assert.Equal(t, "eu-west", got.Deployments[2].Name)
	// The routing pair travels alongside the name so a surface can still
	// address the member it just labelled.
	assert.Equal(t, "primary", got.Deployments[1].Deployment)
	assert.Equal(t, "testapp-002", got.Deployments[1].Target)
}

// TestDerive_MultiTargetLabelsNameTheBlockingMember: a member held by an earlier
// sibling names that sibling the same way the sibling's own section is named, so
// an operator reading "halted — X failed" can find X.
func TestDerive_MultiTargetLabelsNameTheBlockingMember(t *testing.T) {
	got := Derive([]Operation{
		{Deployment: "primary", Target: "testapp-001", State: so.Failed, Error: "lock wait timeout"},
		{Deployment: "primary", Target: "testapp-002", State: so.Pending},
	})
	require.Len(t, got.Deployments, 2)
	assert.Equal(t, StateHalted, got.Deployments[1].Presentation)
	assert.Equal(t, "halted — primary/testapp-001 failed", got.Deployments[1].Label)
	require.NotNil(t, got.FirstFailure)
	assert.Equal(t, "primary/testapp-001", got.FirstFailure.Name)
}

// TestDerive_MultiTargetNextActionCarriesMemberIdentity: a cutover suggestion
// names the member it applies to and carries the routing pair that addresses it,
// so the two targets of one deployment produce distinguishable next actions.
func TestDerive_MultiTargetNextActionCarriesMemberIdentity(t *testing.T) {
	got := Derive([]Operation{
		{Deployment: "primary", Target: "testapp-001", State: so.Completed},
		{Deployment: "primary", Target: "testapp-002", State: so.WaitingForCutover},
	})
	assert.Equal(t, NextAction{
		Kind:       NextActionCutover,
		Deployment: "primary",
		Target:     "testapp-002",
		Name:       "primary/testapp-002",
	}, got.NextAction)
}

// TestDerive_KeyedApplyStaysNamedByDeployment: several operations of one
// deployment against the same target are a keyed apply, not separate members.
// The target half would not tell them apart, so the plain deployment name is
// kept and the operation key does the disambiguating on the surface.
func TestDerive_KeyedApplyStaysNamedByDeployment(t *testing.T) {
	got := Derive([]Operation{
		{Deployment: "us-east", Target: "orders-us", State: so.Completed},
		{Deployment: "us-east", Target: "orders-us", State: so.Running},
	})
	require.Len(t, got.Deployments, 2)
	assert.Equal(t, "us-east", got.Deployments[0].Name)
	assert.Equal(t, "us-east", got.Deployments[1].Name)
}

// A deployment addressing many targets is one group: its members keep their
// resolved order, the group is headed by the member that most needs attention
// rather than the first one, it counts its own members only, and it opens when
// any member would. A single-target deployment is a group of one.
func TestGroups_RollsUpEachDeploymentsTargets(t *testing.T) {
	target := func(dep, tgt, st string) Operation {
		return Operation{Deployment: dep, Target: tgt, State: st, Parallel: true, ContinueOnFailure: true}
	}
	apply := Derive([]Operation{
		target("primary", "t_000", so.Completed),
		target("primary", "t_001", so.Running),
		target("primary", "t_002", so.Failed),
		target("eu", "orders_eu", so.Completed),
	})

	groups := apply.Groups()
	require.Len(t, groups, 2)

	primary := groups[0]
	assert.Equal(t, "primary", primary.Deployment)
	assert.Equal(t, []int{0, 1, 2}, primary.Members)
	assert.Equal(t, "primary/t_002", primary.Lead.Name, "the failed target heads the group")
	assert.Equal(t, []StateCount{{"completed", 1}, {"running", 1}, {"failed", 1}}, primary.Counts)
	assert.True(t, primary.Open)

	eu := groups[1]
	assert.Equal(t, "eu", eu.Deployment)
	assert.Equal(t, []int{3}, eu.Members)
	assert.Equal(t, "eu", eu.Lead.Name)
	assert.Equal(t, []StateCount{{"completed", 1}}, eu.Counts)
	assert.False(t, eu.Open)
}

// A group's progress counts the targets that ran the change apart from the
// ones that already had it, and the rest by status, so the parts always add up
// to every target the group addresses.
func TestTargetProgress_CountsRanAndAlreadyHadApart(t *testing.T) {
	target := func(tgt, st string) Operation {
		return Operation{Deployment: "primary", Target: tgt, State: st, Parallel: true, ContinueOnFailure: true}
	}
	converged := target("t_004", so.Completed)
	converged.NeverStarted = true
	apply := Derive([]Operation{
		target("t_000", so.Completed),
		target("t_001", so.Running),
		target("t_002", so.Pending),
		target("t_003", so.Failed),
		converged,
	})

	groups := apply.Groups()
	require.Len(t, groups, 1)
	assert.Equal(t, TargetProgress{
		Total:      5,
		Done:       1,
		AlreadyHad: 1,
		Others:     []StateCount{{"running", 1}, {"queued", 1}, {"failed", 1}},
	}, apply.TargetProgress(groups[0]))
}

// Members that are not distinct targets, such as keyed operations with no
// target or several operations dividing one target's work, are not rolled up:
// each stays a group of its own, so no surface counts them as targets.
func TestGroups_OnlyDistinctTargetsRollUp(t *testing.T) {
	for name, ops := range map[string][]Operation{
		"no target": {
			{Deployment: "primary", State: so.Running, Parallel: true},
			{Deployment: "primary", State: so.Running, Parallel: true},
		},
		"one target's work": {
			{Deployment: "primary", Target: "orders-001", State: so.Running, Parallel: true},
			{Deployment: "primary", Target: "orders-001", State: so.Running, Parallel: true},
		},
	} {
		t.Run(name, func(t *testing.T) {
			groups := Derive(ops).Groups()
			require.Len(t, groups, 2)
			assert.Equal(t, []int{0}, groups[0].Members)
			assert.Equal(t, []int{1}, groups[1].Members)
			assert.Equal(t, "primary", groups[1].Deployment)
		})
	}
}

// targetsRollout builds deployment payments-a's targets-list rollout of an
// orders change: for each target, two shards of work keyed
// "<target>/orders/<shard>/orders" and an orders finalizer, every row under
// the given on_failure flags. states gives each target's -80, 80- and
// finalizer states.
func targetsRollout(cont, pause bool, states map[string][3]string, targets ...string) []Operation {
	var ops []Operation
	for _, target := range targets {
		st := states[target]
		for i, shard := range []string{"-80", "80-"} {
			ops = append(ops, Operation{
				Deployment: "payments-a", Target: target,
				OperationKey: target + "/orders/" + shard + "/orders", Work: true,
				State: st[i], ContinueOnFailure: cont, PauseOnFailure: pause,
			})
		}
		ops = append(ops, Operation{
			Deployment: "payments-a", Target: target,
			OperationKey: target + "/orders/group_finalizer", Finalizer: true,
			State: st[2], ContinueOnFailure: cont, PauseOnFailure: pause,
		})
	}
	return ops
}

// TestDerive_OrphanedFinalizerSettlesLikeStorage: in a targets-list rollout,
// shard -80 of payments-001 fails and payments-002 completes, which leaves
// payments-001's finalizer pending with nothing that will ever start it.
// Storage settles that rollout failed, so the header must read failed too,
// not running (degraded) under continue or paused under an unreleased pause.
// While the finalizer's own work is only parked, it still holds the rollout.
func TestDerive_OrphanedFinalizerSettlesLikeStorage(t *testing.T) {
	orphaned := map[string][3]string{
		"payments-001": {so.Failed, so.Completed, so.Pending},
		"payments-002": {so.Completed, so.Completed, so.Completed},
	}
	for _, tc := range []struct {
		name        string
		cont, pause bool
	}{
		{name: "continue", cont: true},
		{name: "unreleased pause", pause: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ops := targetsRollout(tc.cont, tc.pause, orphaned, "payments-001", "payments-002")
			got := Derive(ops)
			assert.Equal(t, state.Apply.Failed, got.State)
			assert.Equal(t, "failed", got.Label)
			assert.Equal(t, NextActionReviewFailure, got.NextAction.Kind)
			assert.Equal(t, "payments-001", got.NextAction.Target)
		})
	}

	// payments-002's -80 is still parked at the barrier, so the rollout is
	// still live and continue keeps it degraded.
	live := map[string][3]string{
		"payments-001": {so.Failed, so.Completed, so.Pending},
		"payments-002": {so.WaitingForCutover, so.Completed, so.Pending},
	}
	got := Derive(targetsRollout(true, false, live, "payments-001", "payments-002"))
	assert.Equal(t, state.Apply.RunningDegraded, got.State)
}

// TestDerive_NeverStartedStoppedSettlesLikePending: region-a failed under
// halt, and a stop caught region-b before any driver claimed it. region-b
// counts as pending, so the header reads failed; had region-b started before
// the stop, it would still hold the rollout degraded.
func TestDerive_NeverStartedStoppedSettlesLikePending(t *testing.T) {
	failedA := Operation{Deployment: "region-a", Work: true, State: so.Failed, Error: "boom"}
	neverStarted := Operation{Deployment: "region-b", Work: true, State: so.Stopped, NeverStarted: true}
	assert.Equal(t, state.Apply.Failed, Derive([]Operation{failedA, neverStarted}).State)

	started := Operation{Deployment: "region-b", Work: true, State: so.Stopped}
	assert.Equal(t, state.Apply.RunningDegraded, Derive([]Operation{failedA, started}).State)
}

// Each member carries its own operation's data-plane identifiers and whether
// a driver ever started it, so a surface reads them from the member rather
// than pairing the model with the operations it was derived from.
func TestDerive_MemberCarriesItsOperationsIdentifiers(t *testing.T) {
	model := Derive([]Operation{
		{Deployment: "prod", Target: "payments-001", State: state.ApplyOperation.Completed, NeverStarted: true, ExternalID: "spirit-001", ExternalOperationID: "spirit-op-001"},
		{Deployment: "prod", Target: "payments-002", State: state.ApplyOperation.Failed, Error: "Error 1062: Duplicate entry", ExternalID: "spirit-002", ExternalOperationID: "spirit-op-002"},
	})
	require.Len(t, model.Deployments, 2)
	for _, want := range []struct {
		target, externalID, externalOperationID string
		neverStarted                            bool
	}{
		{"payments-001", "spirit-001", "spirit-op-001", true},
		{"payments-002", "spirit-002", "spirit-op-002", false},
	} {
		d := model.Deployments[slices.IndexFunc(model.Deployments, func(d Deployment) bool { return d.Target == want.target })]
		assert.Equal(t, want.externalID, d.ExternalID, want.target)
		assert.Equal(t, want.externalOperationID, d.ExternalOperationID, want.target)
		assert.Equal(t, want.neverStarted, d.NeverStarted, want.target)
	}
}
