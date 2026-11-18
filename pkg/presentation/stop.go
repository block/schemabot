package presentation

import "github.com/block/schemabot/pkg/state"

// RetryLabel introduces the command that retries a failed rollout. It says
// that a fresh apply resumes rather than starts over, which is what decides
// whether the operator retries or investigates first.
const RetryLabel = "To retry once the failure above is resolved — a new apply reprocesses only the tables that haven't completed"

// RetryOnceSettledNote follows stop while a failure waits on a rollout that
// is still active, where a new apply for the same targets is refused.
const RetryOnceSettledNote = "A new apply can retry the failure once this one finishes or is stopped; it reprocesses only the tables that haven't completed."

// OffersStop reports whether an apply, or one member of it, in state s is
// doing work an operator can stop: the running family, the engine setup
// phases, and an apply retrying a failed table. Every surface offers the stop
// (or cancel) command under exactly these states.
func OffersStop(s string) bool {
	return state.IsRunningApplyState(s) || state.IsState(s,
		state.Apply.FailedRetryable,
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest)
}

// HasStoppableLiveWork reports whether any member is still writing to its
// target in a state that offers stop. A member waiting for cutover is left
// out, as it is from a single apply's footer, so a rollout whose members only
// wait for cutover keeps the cutover as its one command.
func (a Apply) HasStoppableLiveWork() bool {
	for _, d := range a.Deployments {
		if OffersStop(d.State) {
			return true
		}
	}
	return false
}

// OffersRetry reports whether a rollout's footer offers a new apply to retry
// its failure. A new apply for the same targets is refused while this one is
// still active, so retry is offered only once the apply is terminal.
func (a Apply) OffersRetry() bool {
	return a.NextAction.Kind == NextActionReviewFailure && state.IsTerminalApplyState(a.State)
}

// RetryWaitsOnActiveApply reports whether a failure is the rollout's verdict
// while the apply is still active, as when a sibling a driver already started
// keeps running past a halting failure. Retry is refused until the apply
// settles, so the footer offers stop instead and says when retry opens up.
func (a Apply) RetryWaitsOnActiveApply() bool {
	return a.NextAction.Kind == NextActionReviewFailure && !state.IsTerminalApplyState(a.State)
}

// OffersRolloutStop reports whether a rollout's footer offers stop (or
// cancel). Every rollout surface decides with it, so the progress output, the
// watch view and the PR comment never disagree on whether stop is there. A
// terminal apply refuses stop. Otherwise stop is offered while the rollout is
// paused after a failure, while a member is still writing to its target,
// while a failure waits on the apply to settle, and, with no other action
// pending, whenever the aggregate state itself offers stop. A rollout whose
// live members only wait for cutover keeps the cutover as its one command.
func (a Apply) OffersRolloutStop() bool {
	if state.IsTerminalApplyState(a.State) {
		return false
	}
	if state.IsState(a.State, state.Apply.Paused) || a.HasStoppableLiveWork() || a.RetryWaitsOnActiveApply() {
		return true
	}
	return a.NextAction.Kind == NextActionNone && OffersStop(a.State)
}
