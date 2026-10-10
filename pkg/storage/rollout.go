package storage

import "github.com/block/schemabot/pkg/state"

// RolloutOperation maps the operation row to the rollout projection's input.
// released is the apply-level release latch
// (ApplyControlRequest.ReleasesPausedRollout): a released pause behaves like
// continue. Every projection over stored rows builds its children through
// state.RolloutChildren from this mapping, so the operator's derivation, the
// local grouped drive and the PR comment read the same rows the same way.
func (op *ApplyOperation) RolloutOperation(released bool) state.RolloutOperation {
	isPause := op.OnFailure == OnFailurePause
	return state.RolloutOperation{
		Deployment:        op.Deployment,
		OperationKey:      op.OperationKey,
		Work:              op.OperationKind == ApplyOperationKindWork,
		Finalizer:         op.OperationKind == ApplyOperationKindGroupFinalizer,
		RolloutStep:       op.RolloutStep,
		State:             op.State,
		NeverStarted:      op.StartedAt == nil,
		ContinueOnFailure: op.OnFailure == OnFailureContinue || (isPause && released),
		PauseOnFailure:    isPause && !released,
	}
}

// IsConvergedPlaceholder reports whether the row records a member that apply
// creation settled on the spot because its target already held the change:
// completed without ever starting and with no remote identity, so no driver
// claimed it and nothing was dispatched for it. Apply creation writes every
// converged member in this shape, work and finalizer alike, and a driven
// operation can never reach it: a claim stamps StartedAt before dispatch and a
// remote dispatch records the data plane's id. The remote manifest reads this
// to leave out keys that will never arrive.
func (op *ApplyOperation) IsConvergedPlaceholder() bool {
	return state.IsState(op.State, state.ApplyOperation.Completed) && op.StartedAt == nil &&
		op.RemoteApplyID() == "" && op.ExternalOperationID == ""
}
