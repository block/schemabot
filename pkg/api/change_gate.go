package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// changeGateCheckpoint names where in a change's life the change gates run.
type changeGateCheckpoint string

const (
	// changeGateCheckpointStart runs before an apply is created.
	changeGateCheckpointStart changeGateCheckpoint = "start"
	// changeGateCheckpointResume runs before a start request is recorded for
	// an apply that is stopped or waiting to be started.
	changeGateCheckpointResume changeGateCheckpoint = "resume"
	// changeGateCheckpointCutover runs before a deferred cutover is recorded.
	changeGateCheckpointCutover changeGateCheckpoint = "cutover"
)

// changeGateInput is what a change gate decides on.
type changeGateInput struct {
	Checkpoint  changeGateCheckpoint
	Environment string
	// Deployments are every deployment the change runs on.
	Deployments []string
	Database    string
	Repository  string
}

// changeGate is one check an apply, start or cutover must pass at a
// checkpoint. Each gate reports blockers. Time windows and scheduled
// cutovers plug in as further gates.
type changeGate interface {
	// Name labels the gate's blockers and metrics.
	Name() string
	// Blockers returns what refuses the change. An error means the gate
	// could not decide, and the change is refused.
	Blockers(ctx context.Context, in changeGateInput) ([]apitypes.ChangeGateBlocker, error)
}

// freezeGate blocks a change while a freeze covering it is in effect. "In
// effect" is decided by the database clock, so every pod agrees at the edges
// of a freeze.
type freezeGate struct {
	freezes storage.FreezeStore
}

func (freezeGate) Name() string { return "freeze" }

func (g freezeGate) Blockers(ctx context.Context, in changeGateInput) ([]apitypes.ChangeGateBlocker, error) {
	freezes, err := g.freezes.ListInEffect(ctx, in.Environment)
	if err != nil {
		return nil, fmt.Errorf("list freezes in effect in %s: %w", in.Environment, err)
	}
	var blockers []apitypes.ChangeGateBlocker
	for _, freeze := range freezes {
		// One frozen deployment refuses the whole change: a change that ran
		// on the others would leave the database half changed.
		covers := slices.ContainsFunc(in.Deployments, func(deployment string) bool {
			return freeze.Covers(deployment, in.Database)
		})
		if !covers {
			continue
		}
		blockers = append(blockers, apitypes.ChangeGateBlocker{
			Gate:   g.Name(),
			ID:     freeze.Identifier,
			Reason: freeze.Reason,
			EndsAt: freeze.EndsAt,
		})
	}
	return blockers, nil
}

// changeGates lists the gates every apply, start and cutover must pass. A
// server without freeze storage has no freezes to enforce.
func (s *Service) changeGates() []changeGate {
	if s.storage == nil {
		return nil
	}
	var gates []changeGate
	if freezes := s.storage.Freezes(); freezes != nil {
		gates = append(gates, freezeGate{freezes: freezes})
	}
	return gates
}

// changeGateDeployments lists the deployments a change to database runs on:
// the given one, and for a change not narrowed to one rollout member, every
// deployment of the rollout.
func (s *Service) changeGateDeployments(database, environment, deployment string, narrowed bool) []string {
	deployments := []string{deployment}
	if narrowed {
		return deployments
	}
	targets, err := s.config.ResolveDatabaseTargets(database, environment)
	if err != nil {
		// Not failing closed is deliberate. When the config does not resolve
		// the targets, apply creation runs the change on the plan's own
		// deployment alone (applyTargets), so that deployment is the whole
		// set. A start or cutover also checks the deployments of the apply's
		// stored operations.
		return deployments
	}
	for _, target := range targets {
		if !slices.Contains(deployments, target.Deployment) {
			deployments = append(deployments, target.Deployment)
		}
	}
	return deployments
}

// checkChangeGatesForApply runs the change gates for a start or cutover of an
// existing apply. A pinned PR rollback passes, as it did at creation. The
// deployments checked are the apply's own and its stored operations', with
// the configured rollout added for an apply not narrowed to one member: an
// operation keeps running on a deployment later dropped from the rollout
// configuration. If the operations cannot be read, the change is refused.
// The returned context is for the write that records the start or cutover;
// see checkChangeGates.
func (s *Service) checkChangeGatesForApply(ctx context.Context, checkpoint changeGateCheckpoint, apply *storage.Apply, caller string) (context.Context, error) {
	if apply.GetOptions().PinnedRollback {
		return ctx, nil
	}
	deployments := s.changeGateDeployments(apply.Database, apply.Environment, apply.Deployment, apply.GetOptions().NarrowedTo != "")
	if operations := s.storage.ApplyOperations(); operations != nil {
		stored, err := operations.ListByApply(ctx, apply.ID)
		if err != nil {
			metrics.RecordChangeGateError(ctx, apply.Environment, string(checkpoint))
			s.logger.Error("change gate could not read the apply's operations, so the change was refused",
				append(apply.LogAttrs(), "checkpoint", string(checkpoint), "caller", caller, "error", err)...)
			return ctx, &ChangeGateUnavailableError{Checkpoint: checkpoint, Gate: "operations", Err: fmt.Errorf("list operations of apply %s: %w", apply.ApplyIdentifier, err)}
		}
		for _, operation := range stored {
			if operation.Deployment != "" && !slices.Contains(deployments, operation.Deployment) {
				deployments = append(deployments, operation.Deployment)
			}
		}
	}
	return s.checkChangeGates(ctx, changeGateInput{
		Checkpoint:  checkpoint,
		Environment: apply.Environment,
		Deployments: deployments,
		Database:    apply.Database,
		Repository:  apply.Repository,
	}, caller)
}

// checkChangeGates runs every change gate. It returns nil when nothing
// blocks, *ChangeGateRefusedError when something does, and
// *ChangeGateUnavailableError when a gate could not decide.
//
// When nothing blocks, it also returns the context for the write that admits
// the change. The context carries the freeze epoch read before the freezes
// were listed, and that write refuses with storage.ErrFreezeEpochChanged if a
// freeze was created since; changeGateAdmissionError maps that refusal. Use
// the context for that write only.
func (s *Service) checkChangeGates(ctx context.Context, in changeGateInput, caller string) (context.Context, error) {
	attrs := []any{
		"checkpoint", string(in.Checkpoint),
		"environment", in.Environment,
		"deployments", in.Deployments,
		"database", in.Database,
		"repository", in.Repository,
		"caller", caller,
	}
	admitCtx := ctx
	if s.storage != nil {
		if freezes := s.storage.Freezes(); freezes != nil {
			// Read before the freezes are listed: a freeze created after
			// this read moves the epoch, and the admission write sees it.
			epoch, err := freezes.Epoch(ctx, in.Environment)
			if err != nil {
				metrics.RecordChangeGateError(ctx, in.Environment, string(in.Checkpoint))
				s.logger.Error("change gate could not read the freeze epoch, so the change was refused", append(attrs, "error", err)...)
				return ctx, &ChangeGateUnavailableError{Checkpoint: in.Checkpoint, Gate: "freeze", Err: err}
			}
			admitCtx = storage.WithFreezeEpochGuard(ctx, storage.FreezeEpochGuard{Environment: in.Environment, Epoch: epoch})
		}
	}
	var blockers []apitypes.ChangeGateBlocker
	for _, gate := range s.changeGates() {
		found, err := gate.Blockers(ctx, in)
		if err != nil {
			metrics.RecordChangeGateError(ctx, in.Environment, string(in.Checkpoint))
			s.logger.Error("change gate check failed, so the change was refused", append(attrs, "gate", gate.Name(), "error", err)...)
			return ctx, &ChangeGateUnavailableError{Checkpoint: in.Checkpoint, Gate: gate.Name(), Err: err}
		}
		blockers = append(blockers, found...)
	}
	if len(blockers) == 0 {
		return admitCtx, nil
	}
	for _, gate := range distinctChangeGateNames(blockers) {
		metrics.RecordChangeGateRefusal(ctx, in.Environment, gate, string(in.Checkpoint))
	}
	s.logger.Warn("change gate refused the change", append(attrs, "blockers", changeGateBlockerIDs(blockers))...)
	return ctx, &ChangeGateRefusedError{Checkpoint: in.Checkpoint, Environment: in.Environment, Blockers: blockers}
}

// changeGateAdmissionError maps the admission write's refusal of a stale
// freeze check to *ChangeGateUnavailableError, which asks the caller to try
// again: the retry checks the freezes afresh and sees the new freeze. Any
// other error is returned as it is.
func (s *Service) changeGateAdmissionError(ctx context.Context, checkpoint changeGateCheckpoint, environment string, err error) error {
	if !errors.Is(err, storage.ErrFreezeEpochChanged) {
		return err
	}
	metrics.RecordChangeGateError(ctx, environment, string(checkpoint))
	s.logger.Warn("a freeze was created while the change was being admitted, so the change was refused",
		"checkpoint", string(checkpoint), "environment", environment, "error", err)
	return &ChangeGateUnavailableError{Checkpoint: checkpoint, Gate: "freeze", Err: err}
}

func changeGateBlockerIDs(blockers []apitypes.ChangeGateBlocker) []string {
	ids := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		ids = append(ids, blocker.Gate+" "+blocker.ID)
	}
	return ids
}

func distinctChangeGateNames(blockers []apitypes.ChangeGateBlocker) []string {
	var names []string
	for _, blocker := range blockers {
		if !slices.Contains(names, blocker.Gate) {
			names = append(names, blocker.Gate)
		}
	}
	return names
}

// ChangeGateRefusedError reports an apply, start or cutover a change gate
// refused.
type ChangeGateRefusedError struct {
	Checkpoint  changeGateCheckpoint
	Environment string
	Blockers    []apitypes.ChangeGateBlocker
}

func (e *ChangeGateRefusedError) Error() string {
	parts := make([]string, 0, len(e.Blockers))
	for _, blocker := range e.Blockers {
		until := "until lifted"
		if blocker.EndsAt != nil {
			until = "until " + blocker.EndsAt.UTC().Format(time.RFC3339)
		}
		parts = append(parts, fmt.Sprintf("%s %s (%s, %s)", blocker.Gate, blocker.ID, blocker.Reason, until))
	}
	return fmt.Sprintf("blocked in %s by %s; try again after it ends or is lifted", e.Environment, strings.Join(parts, "; "))
}

// ChangeGateUnavailableError reports a change gate that could not decide. The
// change is refused; retrying may succeed.
type ChangeGateUnavailableError struct {
	Checkpoint changeGateCheckpoint
	Gate       string
	Err        error
}

// Error is the operator-facing refusal, since PR commands post it as is. The
// gate already logged the cause, and Unwrap keeps it.
func (e *ChangeGateUnavailableError) Error() string {
	return e.UserMessage()
}

func (e *ChangeGateUnavailableError) Unwrap() error { return e.Err }

// UserMessage is the operator-facing refusal. It leaves out the storage
// cause.
func (e *ChangeGateUnavailableError) UserMessage() string {
	outcome := "this did not start"
	switch e.Checkpoint {
	case changeGateCheckpointCutover:
		outcome = "the cutover did not start"
	case changeGateCheckpointResume:
		outcome = "the schema change was not started"
	}
	return fmt.Sprintf("SchemaBot could not check for freezes, so %s. Try again.", outcome)
}

// changeGateErrorStatus maps a change gate error to its HTTP status and
// error code.
func changeGateErrorStatus(err error) (status int, code string, ok bool) {
	if _, refused := errors.AsType[*ChangeGateRefusedError](err); refused {
		return http.StatusConflict, apitypes.ErrCodeChangeGateRefused, true
	}
	if _, unavailable := errors.AsType[*ChangeGateUnavailableError](err); unavailable {
		return http.StatusServiceUnavailable, apitypes.ErrCodeChangeGateUnavailable, true
	}
	return 0, "", false
}

// writeChangeGateError writes a change gate error and reports whether err was
// one. The gate already logged it.
func (s *Service) writeChangeGateError(w http.ResponseWriter, opName string, err error) bool {
	status, code, ok := changeGateErrorStatus(err)
	if !ok {
		return false
	}
	response := apitypes.ErrorResponse{Error: opName + " rejected: " + err.Error(), ErrorCode: code}
	if refused, isRefusal := errors.AsType[*ChangeGateRefusedError](err); isRefusal {
		response.Blockers = refused.Blockers
	}
	if unavailable, isUnavailable := errors.AsType[*ChangeGateUnavailableError](err); isUnavailable {
		response.Error = unavailable.UserMessage()
	}
	s.writeJSON(w, status, response)
	return true
}
