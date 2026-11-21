package api

import (
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// freezeStopSweepListFailed is what a freeze's stop sweep reports when it
// could not list the applies in scope. The freeze is stored either way, so
// it refuses new changes.
const freezeStopSweepListFailed = "SchemaBot could not list the schema changes in scope, so it stopped none of them. " +
	"Check 'schemabot status' and stop them with 'schemabot stop'."

// freezeStopSweepOperationsUnreadable is what the sweep reports for an apply
// whose operations it could not read, so it could not tell whether the
// freeze covers it.
const freezeStopSweepOperationsUnreadable = "SchemaBot could not read where this schema change runs, so it did not stop it; see server logs"

// freezeStopSweepStopFailed is what the sweep reports for an apply whose
// stop could not be recorded for a reason the server log holds.
const freezeStopSweepStopFailed = "SchemaBot could not record the stop; see server logs"

// stopAppliesForFreeze issues the durable stop on every admitted apply that a
// freeze with running "stop" covers, when the freeze is in effect at now, and
// reports what it stopped. A freeze that lets running applies continue, or
// that starts later, stops nothing here and returns nil.
//
// Each stop is the ordinary durable stop control, recorded with the freeze as
// its requester (storage.FreezeStopCaller), so its driver delivers it as it
// would an operator's and the apply resumes with start. An apply the sweep
// cannot stop is reported, never silently skipped. The sweep runs after the
// freeze is stored, so whatever it reports, the freeze already refuses new
// changes.
func (s *Service) stopAppliesForFreeze(ctx context.Context, freeze *storage.EnvironmentFreeze, now time.Time) *apitypes.FreezeStopSweep {
	if freeze.Running != storage.FreezeRunningStop {
		return nil
	}
	if !freeze.InEffectAt(now) {
		s.logger.Info("freeze stops running applies but starts later; setting it stops nothing now",
			freezeLogArgs(freeze, freeze.CreatedBy)...)
		return nil
	}
	sweep := &apitypes.FreezeStopSweep{Stopped: []string{}}
	applies, err := s.storage.Applies().GetInProgress(ctx)
	if err != nil {
		s.logger.Error("freeze could not list the applies in progress, so it stopped none of them",
			append(freezeLogArgs(freeze, freeze.CreatedBy), "error", err)...)
		sweep.Error = freezeStopSweepListFailed
		return sweep
	}
	for _, apply := range applies {
		if !freezeStopSweepCandidate(freeze, apply) {
			continue
		}
		deployments, err := s.applyChangeGateDeployments(ctx, apply)
		if err != nil {
			s.logger.Error("freeze could not read where an apply runs, so it did not stop it",
				append(apply.LogAttrs(), "freeze_id", freeze.Identifier, "error", err)...)
			sweep.NotStopped = append(sweep.NotStopped, apitypes.FreezeUnstoppedApply{
				ApplyID: apply.ApplyIdentifier, Reason: freezeStopSweepOperationsUnreadable,
			})
			continue
		}
		if !freezeCoversAnyDeployment(freeze, deployments, apply.Database) {
			s.logger.Debug("freeze does not cover this apply's deployments or database; it keeps running",
				append(apply.LogAttrs(), "freeze_id", freeze.Identifier, "deployments", deployments)...)
			continue
		}
		if reason, stopped := s.stopApplyForFreeze(ctx, freeze, apply); stopped {
			sweep.Stopped = append(sweep.Stopped, apply.ApplyIdentifier)
		} else {
			sweep.NotStopped = append(sweep.NotStopped, apitypes.FreezeUnstoppedApply{ApplyID: apply.ApplyIdentifier, Reason: reason})
		}
	}
	s.logger.Info("freeze stopped the admitted applies in its scope", append(freezeLogArgs(freeze, freeze.CreatedBy),
		"stopped", sweep.Stopped, "not_stopped_count", len(sweep.NotStopped))...)
	return sweep
}

// freezeStopSweepCandidate reports whether a freeze's stop sweep considers
// apply at all: it is in the freeze's environment, it is pending, paused or
// doing work a stop can end, and it is not a pull request rollback, which no
// gate holds. A rollout paused after a failure can still have members
// writing, so it is stopped too, as its own footer offers. An apply waiting
// for its cutover is left alone: every member is waiting or done, and the
// freeze already refuses the cutover.
func freezeStopSweepCandidate(freeze *storage.EnvironmentFreeze, apply *storage.Apply) bool {
	if storage.CanonicalKey(apply.Environment) != storage.CanonicalKey(freeze.Environment) {
		return false
	}
	if apply.GetOptions().PinnedRollback {
		return false
	}
	return state.IsState(apply.State, state.Apply.Pending, state.Apply.Paused) || presentation.OffersStop(apply.State)
}

// freezeCoversAnyDeployment reports whether the freeze covers database on any
// of the deployments. One covered deployment is enough, as it is for the
// change gate: the freeze holds the whole change.
func freezeCoversAnyDeployment(freeze *storage.EnvironmentFreeze, deployments []string, database string) bool {
	return slices.ContainsFunc(deployments, func(deployment string) bool {
		return freeze.Covers(deployment, database)
	})
}

// stopApplyForFreeze records the freeze's stop on one apply. It returns true
// when the stop was accepted, and otherwise the operator-facing reason it was
// not.
func (s *Service) stopApplyForFreeze(ctx context.Context, freeze *storage.EnvironmentFreeze, apply *storage.Apply) (string, bool) {
	caller := storage.FreezeStopCaller(freeze.Identifier)
	attrs := append(apply.LogAttrs(), "freeze_id", freeze.Identifier, "requested_by", caller)
	client, current, ternApplyID, err := s.controlTarget(ctx, "stop", apply.ApplyIdentifier, apply.Environment)
	if err == nil {
		var resp *apitypes.StopResponse
		resp, _, err = s.stopApply(ctx, client, current, ternApplyID, current.Environment, caller)
		if err == nil && !resp.Accepted {
			s.logger.Warn("freeze's stop was not accepted; the apply keeps running", append(attrs, "error_message", resp.ErrorMessage)...)
			return resp.ErrorMessage, false
		}
	}
	if err != nil {
		if controlOperationHTTPStatus(err) < http.StatusInternalServerError {
			// A refusal of the stop itself, such as an engine that cannot
			// stop or an apply that finished meanwhile, is written for the
			// operator.
			s.logger.Warn("freeze's stop was refused; the apply keeps running", append(attrs, "error", err)...)
			return err.Error(), false
		}
		s.logger.Error("freeze could not record its stop; the apply keeps running", append(attrs, "error", err)...)
		return freezeStopSweepStopFailed, false
	}
	s.logger.Info("freeze recorded a stop on an admitted apply", attrs...)
	return "", true
}
