package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// holdClaimForStopFreeze is the claim checkpoint. Before a driver starts an
// operation it claimed, one that has not started or a stopped one a start
// resumes, it checks the freezes with running "stop" in effect. When one
// covers the apply, by the same rule as the change gates (one covered
// deployment holds the whole change), the driver does not start the
// operation: it returns the claim, leaving the operation as it was before,
// and records the freeze's stop on the apply, which stop reconciliation then
// delivers. It returns true when the driver must not drive the operation.
//
// A freeze that lets running applies continue does not act here: the change
// gates already refused every change it covers at creation and start. A pull
// request rollback is never held, and an apply admitted on an approved
// override that names the freeze runs. When the freezes or the overrides
// cannot be read, or the stop cannot be recorded, the operation goes back
// unstarted and the next pass checks it again.
func (s *Service) holdClaimForStopFreeze(ctx context.Context, driverID int, apply *storage.Apply, op *storage.ApplyOperation, opLease storage.OperationLease) bool {
	if !claimStartsOperation(op) || apply.GetOptions().PinnedRollback || s.storage.Freezes() == nil {
		return false
	}
	attrs := append(op.LogAttrs(), "driver", driverID, "checkpoint", string(changeGateCheckpointClaim))
	blocking, err := s.stopFreezesHoldingApply(ctx, apply)
	if err != nil {
		metrics.RecordChangeGateError(ctx, apply.Environment, string(changeGateCheckpointClaim))
		s.logger.Error("operator: could not check the stop freezes before starting the operation; it goes back unstarted until the next pass",
			append(attrs, "error", err)...)
		s.returnUnstartedClaim(ctx, op, opLease, attrs)
		return true
	}
	if len(blocking) == 0 {
		return false
	}
	freeze := blocking[0]
	metrics.RecordChangeGateRefusal(ctx, apply.Environment, freezeGate{}.Name(), string(changeGateCheckpointClaim))
	attrs = append(attrs, "freeze_id", freeze.Identifier)
	// The claim goes back first: a stop recorded while this driver holds a
	// claim on a stopped apply is left to this driver's drive, which would
	// never come.
	s.returnUnstartedClaim(ctx, op, opLease, attrs)
	if reason, stopped := s.stopApplyForFreeze(ctx, freeze, apply); !stopped {
		s.logger.Warn("operator: a stop freeze covers the operation this driver claimed, and its stop was not recorded; the operation stays unstarted while the freeze is in effect",
			append(attrs, "reason", reason)...)
		return true
	}
	s.logger.Info("operator: a stop freeze covers the operation this driver claimed; it stopped the apply instead of starting the operation", attrs...)
	return true
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
// admitted apply: the one its creation recorded, and any unexpired one for a
// start of it. The gate that admitted the change already checked who asked,
// so any requester's approval counts here.
func (s *Service) approvedOverridesForApply(ctx context.Context, apply *storage.Apply) ([]*storage.FreezeOverrideRequest, error) {
	store := s.storage.FreezeOverrides()
	if store == nil {
		return nil, nil
	}
	var approved []*storage.FreezeOverrideRequest
	if id := apply.GetOptions().FreezeOverride; id != "" {
		request, err := store.Get(ctx, id)
		switch {
		case errors.Is(err, storage.ErrFreezeOverrideNotFound):
		case err != nil:
			return nil, fmt.Errorf("get the override request %s that admitted apply %s: %w", id, apply.ApplyIdentifier, err)
		case request.ApprovedAt != nil:
			approved = append(approved, request)
		}
	}
	starts, err := store.ListApproved(ctx, storage.FreezeOverrideTarget{
		Environment: apply.Environment, Action: storage.FreezeOverrideActionStart, TargetID: apply.ApplyIdentifier,
	})
	if err != nil {
		return nil, fmt.Errorf("list approved start overrides for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return append(approved, starts...), nil
}
