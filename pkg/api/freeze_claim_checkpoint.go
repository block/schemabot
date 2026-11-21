package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// heldClaimRetryAfter is how long a driver leaves an operation it held back
// unstarted out of its claims, when the freeze's stop was not recorded or the
// freezes could not be read. Without it the claim order, oldest first, would
// hand the driver the same held operation on every pass, ahead of every other
// apply's work.
const heldClaimRetryAfter = 30 * time.Second

// holdClaimForStopFreeze is the claim checkpoint. Before a driver drives an
// operation it claimed, it checks the freezes with running "stop" in effect.
// When one covers the apply, by the same rule as the change gates (one covered
// deployment holds the whole change), the freeze's stop is recorded on the
// apply. It returns true when the driver must not drive the operation.
//
// A claim that starts the operation, one that has not started or a stopped
// one a start resumes, is not started: the driver returns the claim, leaving
// the operation as it was, and stop reconciliation delivers the stop. Any
// other claim recovers or redispatches work that may already be writing, so
// the driver records the stop and drives on, and the drive delivers it. That
// includes unstarted work whose claim could not be returned: once its lease
// goes stale, its recovery passes through here again.
//
// A freeze that lets running applies continue does not act here: the change
// gates already refused every change it covers at creation and start. A pull
// request rollback is never held, an apply with a pending cancel is driven so
// the cancel is delivered (AZ-10, CO-2), and an apply admitted on an approved
// override that names the freeze runs. When the freezes cannot be read, or the
// stop cannot be recorded, the operation is not driven: a start claim goes
// back unstarted and is skipped for heldClaimRetryAfter, and any other claim
// keeps its lease until it goes stale and is recovered.
func (s *Service) holdClaimForStopFreeze(ctx context.Context, driverID int, apply *storage.Apply, op *storage.ApplyOperation, opLease storage.OperationLease) bool {
	if apply.GetOptions().PinnedRollback || s.storage.Freezes() == nil {
		return false
	}
	starts := claimStartsOperation(op)
	attrs := append(op.LogAttrs(), "driver", driverID, "checkpoint", string(changeGateCheckpointClaim), "starts_operation", starts)
	// notDriven ends the claim without driving the operation, for a claim
	// the checkpoint could not admit.
	notDriven := func() bool {
		if starts {
			s.returnUnstartedClaim(ctx, op, opLease, attrs)
			s.skipHeldOperation(op.ID)
		}
		return true
	}
	cancelling, err := s.cancelPendingForApply(ctx, apply)
	if err != nil {
		metrics.RecordChangeGateError(ctx, apply.Environment, string(changeGateCheckpointClaim))
		s.logger.Error("operator: could not read whether a cancel is pending before checking the stop freezes; the operation is not driven until the next attempt",
			append(attrs, "error", err)...)
		return notDriven()
	}
	if cancelling {
		s.logger.Debug("operator: a cancel is pending for this apply; the claim delivers it whatever the freezes", attrs...)
		return false
	}
	blocking, err := s.stopFreezesHoldingApply(ctx, apply)
	if err != nil {
		metrics.RecordChangeGateError(ctx, apply.Environment, string(changeGateCheckpointClaim))
		s.logger.Error("operator: could not check the stop freezes before driving the operation; it is not driven until the next attempt",
			append(attrs, "error", err)...)
		return notDriven()
	}
	if len(blocking) == 0 {
		return false
	}
	freeze := blocking[0]
	metrics.RecordChangeGateRefusal(ctx, apply.Environment, freezeGate{}.Name(), string(changeGateCheckpointClaim))
	attrs = append(attrs, "freeze_id", freeze.Identifier)
	if !starts {
		return s.stopRecoveredWorkForFreeze(ctx, freeze, apply, attrs)
	}
	// The claim goes back first: a stop recorded while this driver holds a
	// claim on a stopped apply is left to this driver's drive, which would
	// never come.
	s.returnUnstartedClaim(ctx, op, opLease, attrs)
	if reason, stopped := s.stopApplyForFreeze(ctx, freeze, apply); !stopped {
		s.skipHeldOperation(op.ID)
		s.logger.Warn("operator: a stop freeze covers the operation this driver claimed, and its stop was not recorded; the operation stays unstarted while the freeze is in effect",
			append(attrs, "reason", reason, "retry_after", heldClaimRetryAfter)...)
		return true
	}
	s.logger.Info("operator: a stop freeze covers the operation this driver claimed; it stopped the apply instead of starting the operation", attrs...)
	return true
}

// stopRecoveredWorkForFreeze records a covering stop freeze's stop on an
// apply whose claimed operation is recovered or redispatched rather than
// started, and reports whether the driver must not drive it. A recorded stop
// is delivered by the drive. A stop the engine refuses, such as PlanetScale's,
// leaves the work running, as the freeze's own sweep reports for it. A stop
// that could not be recorded leaves the operation undriven until its lease
// goes stale, so its recovery checks again.
func (s *Service) stopRecoveredWorkForFreeze(ctx context.Context, freeze *storage.EnvironmentFreeze, apply *storage.Apply, attrs []any) bool {
	reason, stopped := s.stopApplyForFreeze(ctx, freeze, apply)
	switch {
	case stopped:
		s.logger.Info("operator: a stop freeze covers the work this driver recovered; it recorded the freeze's stop, which this drive delivers", attrs...)
		return false
	case reason == freezeStopSweepStopFailed:
		s.logger.Error("operator: a stop freeze covers the work this driver recovered, and its stop was not recorded; the operation is not driven until its lease goes stale",
			attrs...)
		return true
	default:
		s.logger.Warn("operator: a stop freeze covers the work this driver recovered, and its stop was refused; the work keeps running",
			append(attrs, "reason", reason)...)
		return false
	}
}

// cancelPendingForApply reports whether apply has a pending cancel, which
// the claim delivers whatever the freezes: a freeze never holds a cancel.
func (s *Service) cancelPendingForApply(ctx context.Context, apply *storage.Apply) (bool, error) {
	controls := s.storage.ControlRequests()
	if controls == nil {
		return false, nil
	}
	cancel, err := controls.GetPending(ctx, apply.ID, storage.ControlOperationCancel)
	if err != nil {
		return false, fmt.Errorf("get the pending cancel of apply %s: %w", apply.ApplyIdentifier, err)
	}
	return cancel != nil, nil
}

// skipHeldOperation leaves an operation this driver held back unstarted out
// of this pod's start claims for heldClaimRetryAfter.
func (s *Service) skipHeldOperation(id int64) {
	s.freezeHeldOpsMu.Lock()
	defer s.freezeHeldOpsMu.Unlock()
	if s.freezeHeldOps == nil {
		s.freezeHeldOps = map[int64]time.Time{}
	}
	s.freezeHeldOps[id] = s.clock.Now().Add(heldClaimRetryAfter)
}

// heldOperationIDs returns the operations this pod still skips in its start
// claims, and forgets the ones whose time has passed.
func (s *Service) heldOperationIDs() []int64 {
	s.freezeHeldOpsMu.Lock()
	defer s.freezeHeldOpsMu.Unlock()
	now := s.clock.Now()
	var ids []int64
	for id, until := range s.freezeHeldOps {
		if !now.Before(until) {
			delete(s.freezeHeldOps, id)
			continue
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// returnUnstartedClaim hands back a claim the driver will not start, as the
// operation was before it. The claim moved a pending operation to running, so
// a plain release would let the next claim drive it as started work, past
// this checkpoint.
func (s *Service) returnUnstartedClaim(ctx context.Context, op *storage.ApplyOperation, opLease storage.OperationLease, attrs []any) {
	returned, err := s.storage.ApplyOperations().ReturnUnstartedClaim(ctx, opLease, op.State)
	switch {
	case err != nil:
		metrics.RecordOperatorClaimFailure(ctx, "unstarted_claim_return_error")
		s.logger.Error("operator: could not return the claim of the operation this driver will not start; it is recovered once its lease goes stale",
			append(attrs, "error", err)...)
	case !returned:
		s.logger.Warn("operator: the claim of the operation this driver will not start had already moved on; its new owner drives it",
			attrs...)
	default:
		s.logger.Debug("operator: returned the claim of the operation this driver will not start", attrs...)
	}
}

// stopFreezesHoldingApply returns the freezes with running "stop" in effect
// that cover apply and that no approved override recorded for it names.
func (s *Service) stopFreezesHoldingApply(ctx context.Context, apply *storage.Apply) ([]*storage.EnvironmentFreeze, error) {
	inEffect, err := s.storage.Freezes().ListInEffect(ctx, apply.Environment)
	if err != nil {
		return nil, fmt.Errorf("list freezes in effect in %s: %w", apply.Environment, err)
	}
	var stopFreezes []*storage.EnvironmentFreeze
	for _, freeze := range inEffect {
		if freeze.Running == storage.FreezeRunningStop {
			stopFreezes = append(stopFreezes, freeze)
		}
	}
	if len(stopFreezes) == 0 {
		return nil, nil
	}
	deployments, err := s.applyChangeGateDeployments(ctx, apply)
	if err != nil {
		return nil, err
	}
	var covering []*storage.EnvironmentFreeze
	for _, freeze := range stopFreezes {
		if freezeCoversAnyDeployment(freeze, deployments, apply.Database) {
			covering = append(covering, freeze)
		}
	}
	if len(covering) == 0 {
		return nil, nil
	}
	overrides, err := s.approvedOverridesForApply(ctx, apply)
	if err != nil {
		return nil, err
	}
	var holding []*storage.EnvironmentFreeze
	for _, freeze := range covering {
		blocker := []string{freezeGate{}.Name() + " " + freeze.Identifier}
		overridden := false
		for _, request := range overrides {
			if request.Covers(blocker) {
				overridden = true
				break
			}
		}
		if overridden {
			s.logger.Info("operator: an approved override names the stop freeze covering this apply; the operation starts",
				append(apply.LogAttrs(), "freeze_id", freeze.Identifier)...)
			continue
		}
		holding = append(holding, freeze)
	}
	return holding, nil
}

// approvedOverridesForApply returns the approved override requests that
// admitted apply: the one its creation recorded, and the one that admitted
// its pending start, which the start request names. The gate that admitted
// the change already checked who asked, so only the request that admitted
// this change counts here, never another requester's approval for the same
// apply.
func (s *Service) approvedOverridesForApply(ctx context.Context, apply *storage.Apply) ([]*storage.FreezeOverrideRequest, error) {
	store := s.storage.FreezeOverrides()
	if store == nil {
		return nil, nil
	}
	ids := []string{apply.GetOptions().FreezeOverride}
	startOverride, err := s.pendingStartFreezeOverride(ctx, apply)
	if err != nil {
		return nil, err
	}
	ids = append(ids, startOverride)
	var approved []*storage.FreezeOverrideRequest
	for _, id := range ids {
		if id == "" {
			continue
		}
		request, err := store.Get(ctx, id)
		switch {
		case errors.Is(err, storage.ErrFreezeOverrideNotFound):
		case err != nil:
			return nil, fmt.Errorf("get the override request %s that admitted apply %s: %w", id, apply.ApplyIdentifier, err)
		case request.ApprovedAt != nil:
			approved = append(approved, request)
		}
	}
	return approved, nil
}

// pendingStartFreezeOverride returns the override request that admitted
// apply's pending start, or "" when no start is pending or none admitted it.
func (s *Service) pendingStartFreezeOverride(ctx context.Context, apply *storage.Apply) (string, error) {
	controls := s.storage.ControlRequests()
	if controls == nil {
		return "", nil
	}
	start, err := controls.GetPending(ctx, apply.ID, storage.ControlOperationStart)
	if err != nil {
		return "", fmt.Errorf("get the pending start of apply %s: %w", apply.ApplyIdentifier, err)
	}
	if start == nil || len(start.Metadata) == 0 {
		return "", nil
	}
	var metadata startControlRequestMetadata
	if err := json.Unmarshal(start.Metadata, &metadata); err != nil {
		return "", fmt.Errorf("read the pending start of apply %s: %w", apply.ApplyIdentifier, err)
	}
	return metadata.FreezeOverride, nil
}
