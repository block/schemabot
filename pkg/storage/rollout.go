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
		State:             op.State,
		NeverStarted:      op.StartedAt == nil,
		ContinueOnFailure: op.OnFailure == OnFailureContinue || (isPause && released),
		PauseOnFailure:    isPause && !released,
	}
}
