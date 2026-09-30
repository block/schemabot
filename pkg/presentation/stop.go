package presentation

import "github.com/block/schemabot/pkg/state"

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
