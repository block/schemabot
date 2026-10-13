package tern

import (
	"context"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// adjustActiveAppliesForTransition moves the active-apply gauge for a stored
// transition from previousState to apply.State, where previousState was read
// from storage under the current claim: it releases the apply when the
// transition leaves the counted states (state.CountsAsActiveApply) and counts
// it again when the transition re-enters them.
func adjustActiveAppliesForTransition(ctx context.Context, apply *storage.Apply, previousState string) {
	switch was, is := state.CountsAsActiveApply(previousState), state.CountsAsActiveApply(apply.State); {
	case was && !is:
		metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
	case !was && is:
		metrics.AdjustActiveApplies(ctx, 1, apply.Database, apply.Deployment, apply.Environment)
	}
}

// releaseActiveApplyForDriveOutcome releases the active-apply gauge when a drive
// stores an outcome that is no longer counted, for an apply the drive holds as
// the snapshot it claimed (claimedState). The claim itself left the stored apply
// counted: a pending or retryable apply moves to running, the operator counting
// a retryable pause again on that claim, and a stopped one moves to resuming or
// stays stopped for a cancel. So the snapshot's own state does not say whether
// the apply is counted, and only a snapshot already settled was released before.
func releaseActiveApplyForDriveOutcome(ctx context.Context, apply *storage.Apply, claimedState string) {
	if state.CountsAsActiveApply(apply.State) || state.IsState(claimedState, state.SettledApplyStates...) {
		return
	}
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
}
