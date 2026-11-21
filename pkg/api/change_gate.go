package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/engine"
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
	// Action and TargetID name the change an override request covers: what
	// each member of an apply would run, or the apply a start or cutover acts
	// on.
	Action   string
	TargetID string
	// ResolveTargetID names an apply's target when something blocks it.
	// Resolving reads the rollout's member plans, so an apply nothing blocks
	// does not pay for it here.
	ResolveTargetID func(context.Context) (string, error)
	// Override is the requester's trimmed reason for asking an approver to
	// override the blockers. Empty when none was given.
	Override string
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

// applyChangeGateDeployments lists the deployments an existing apply runs on:
// its own and its stored operations', with the configured rollout added for
// an apply not narrowed to one member. An operation keeps running on a
// deployment later dropped from the rollout configuration, so the stored
// operations count too.
func (s *Service) applyChangeGateDeployments(ctx context.Context, apply *storage.Apply) ([]string, error) {
	deployments := s.changeGateDeployments(apply.Database, apply.Environment, apply.Deployment, apply.GetOptions().NarrowedTo != "")
	operations := s.storage.ApplyOperations()
	if operations == nil {
		return deployments, nil
	}
	stored, err := operations.ListByApply(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("list operations of apply %s: %w", apply.ApplyIdentifier, err)
	}
	for _, operation := range stored {
		if operation.Deployment != "" && !slices.Contains(deployments, operation.Deployment) {
			deployments = append(deployments, operation.Deployment)
		}
	}
	return deployments, nil
}

// checkChangeGatesForApply runs the change gates for a start or cutover of an
// existing apply. A pinned PR rollback passes, as it did at creation. The
// deployments checked are the apply's own and its stored operations', with
// the configured rollout added for an apply not narrowed to one member: an
// operation keeps running on a deployment later dropped from the rollout
// configuration. If the operations cannot be read, the change is refused.
// The returned context is for the write that records the start or cutover,
// and the returned request is the approved override that admitted it, if any;
// see checkChangeGates. override is the caller's reason for asking one.
func (s *Service) checkChangeGatesForApply(ctx context.Context, checkpoint changeGateCheckpoint, apply *storage.Apply, caller, override string) (context.Context, *storage.FreezeOverrideRequest, error) {
	if apply.GetOptions().PinnedRollback {
		return ctx, nil, nil
	}
	deployments, err := s.applyChangeGateDeployments(ctx, apply)
	if err != nil {
		metrics.RecordChangeGateError(ctx, apply.Environment, string(checkpoint))
		s.logger.Error("change gate could not read the apply's operations, so the change was refused",
			append(apply.LogAttrs(), "checkpoint", string(checkpoint), "caller", caller, "error", err)...)
		return ctx, nil, &ChangeGateUnavailableError{Checkpoint: checkpoint, Gate: "operations", Err: err}
	}
	action := storage.FreezeOverrideActionStart
	if checkpoint == changeGateCheckpointCutover {
		action = storage.FreezeOverrideActionCutover
	}
	return s.checkChangeGates(ctx, changeGateInput{
		Checkpoint:  checkpoint,
		Environment: apply.Environment,
		Deployments: deployments,
		Database:    apply.Database,
		Repository:  apply.Repository,
		Action:      action,
		TargetID:    apply.ApplyIdentifier,
		Override:    override,
	}, caller)
}

// checkChangeGates runs every change gate. It returns nil when nothing
// blocks, *ChangeGateRefusedError when something does, and
// *ChangeGateUnavailableError when a gate could not decide.
//
// Blockers are overridden by an approved override request for this
// environment, action and target that names every one of them. The request is
// returned with the admitting context so the caller can record it. Without
// one, a caller who gave an override reason has a request recorded and the
// refusal carries it. Any storage error on that path is
// *ChangeGateUnavailableError: an override never gets past a gate that could
// not decide.
//
// When nothing blocks or an override admits the change, it also returns the
// context for the write that admits the change. The context carries the freeze epoch read before the freezes
// were listed, and that write refuses with storage.ErrFreezeEpochChanged if a
// freeze was created since, or storage.ErrFreezeEpochUnreadable if it could
// not read the epoch; changeGateAdmissionError maps both refusals. Use
// the context for that write only.
func (s *Service) checkChangeGates(ctx context.Context, in changeGateInput, caller string) (context.Context, *storage.FreezeOverrideRequest, error) {
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
				return ctx, nil, &ChangeGateUnavailableError{Checkpoint: in.Checkpoint, Gate: "freeze", Err: err}
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
			return ctx, nil, &ChangeGateUnavailableError{Checkpoint: in.Checkpoint, Gate: gate.Name(), Err: err}
		}
		blockers = append(blockers, found...)
	}
	if len(blockers) == 0 {
		return admitCtx, nil, nil
	}
	if in.TargetID == "" && in.ResolveTargetID != nil {
		// A change whose members do not resolve fails as its creation would.
		targetID, err := in.ResolveTargetID(ctx)
		if err != nil {
			return ctx, nil, err
		}
		in.TargetID = targetID
	}
	approved, err := s.approvedOverride(ctx, in, blockers)
	if err != nil {
		metrics.RecordChangeGateError(ctx, in.Environment, string(in.Checkpoint))
		s.logger.Error("change gate could not read the override requests, so the change was refused", append(attrs, "error", err)...)
		return ctx, nil, &ChangeGateUnavailableError{Checkpoint: in.Checkpoint, Gate: "override", Err: err}
	}
	if approved != nil {
		for _, gate := range distinctChangeGateNames(blockers) {
			metrics.RecordChangeGateOverride(ctx, in.Environment, gate, string(in.Checkpoint))
		}
		s.logger.Info("change gate admitted the change on an approved override", append(attrs,
			"override_request", approved.Identifier, "requested_by", approved.RequestedBy, "approved_by", approved.ApprovedBy,
			"reason", approved.Reason, "blockers", approved.Blockers)...)
		return admitCtx, approved, nil
	}
	for _, gate := range distinctChangeGateNames(blockers) {
		metrics.RecordChangeGateRefusal(ctx, in.Environment, gate, string(in.Checkpoint))
	}
	refusal := &ChangeGateRefusedError{Checkpoint: in.Checkpoint, Environment: in.Environment, Blockers: blockers}
	if in.Override != "" {
		requested, err := s.requestOverride(ctx, in, blockers)
		if err != nil {
			metrics.RecordChangeGateError(ctx, in.Environment, string(in.Checkpoint))
			s.logger.Error("change gate could not record the override request, so the change was refused", append(attrs, "error", err)...)
			return ctx, nil, &ChangeGateUnavailableError{Checkpoint: in.Checkpoint, Gate: "override", Err: err}
		}
		if requested != nil {
			s.logger.Info("override requested for a change a gate refused", append(attrs,
				"override_request", requested.Identifier, "reason", requested.Reason, "blockers", requested.Blockers)...)
			refusal.OverrideRequest = requested
			refusal.OverrideReasonKept = requested.Reason != in.Override
		} else {
			refusal.OverrideUnrecorded = true
		}
	}
	s.logger.Warn("change gate refused the change", append(attrs, "blockers", changeGateBlockerIDs(blockers))...)
	return ctx, nil, refusal
}

// approvedOverride returns the first approved, unexpired override request for
// the change that names every blocker and that the caller asked for, or nil
// when there is none. Only the requester runs on an approval: anyone else asks
// for their own. Within its window the requester may use it more than once,
// for example to re-apply the same schema after a stop. A server without
// override storage has none to find.
func (s *Service) approvedOverride(ctx context.Context, in changeGateInput, blockers []apitypes.ChangeGateBlocker) (*storage.FreezeOverrideRequest, error) {
	overrides := s.storage.FreezeOverrides()
	if overrides == nil {
		return nil, nil
	}
	caller, verified := overrideCaller(ctx)
	if !verified {
		return nil, nil
	}
	approved, err := overrides.ListApproved(ctx, storage.FreezeOverrideTarget{
		Environment: in.Environment, Action: in.Action, TargetID: in.TargetID,
	})
	if err != nil {
		return nil, fmt.Errorf("list approved overrides for %s %s in %s: %w", in.Action, in.TargetID, in.Environment, err)
	}
	ids := changeGateBlockerIDs(blockers)
	for _, request := range approved {
		if request.Covers(ids) && storage.CanonicalKey(request.RequestedBy) == storage.CanonicalKey(caller) && sameOverrideOrigin(ctx, request) {
			return request, nil
		}
	}
	return nil, nil
}

// requestOverride records a request for an approver to override the blockers
// of this change, for the caller who asked, and returns nil when it may not
// record one. A pull request command records the commenter, the pull request
// and its author, all in the "github:" identity domain (see
// storage.FreezeOverrideRequest.FromPullRequest); any other request records the verified caller's
// subject, with no pull request. The plan does not store its pull request's
// author, so the pull request comes from the webhook's context, never from
// the plan.
func (s *Service) requestOverride(ctx context.Context, in changeGateInput, blockers []apitypes.ChangeGateBlocker) (*storage.FreezeOverrideRequest, error) {
	overrides := s.storage.FreezeOverrides()
	freezes := s.storage.Freezes()
	if overrides == nil || freezes == nil {
		return nil, nil
	}
	requester, verified := overrideCaller(ctx)
	if !verified {
		return nil, nil
	}
	pr, fromPullRequest := pullRequestOverrideRequester(ctx)
	if fromPullRequest && !pr.complete() {
		// Without the author the approver check could not refuse them.
		return nil, nil
	}
	// A requester who repeats the command before approval keeps the one
	// request, so the approver sees one thread per change.
	pending, err := overrides.ListPending(ctx, storage.FreezeOverrideTarget{Environment: in.Environment, Action: in.Action, TargetID: in.TargetID})
	if err != nil {
		return nil, fmt.Errorf("list pending override requests for %s %s in %s: %w", in.Action, in.TargetID, in.Environment, err)
	}
	ids := changeGateBlockerIDs(blockers)
	for _, request := range pending {
		if storage.CanonicalKey(request.RequestedBy) == storage.CanonicalKey(requester) &&
			len(request.Blockers) == len(ids) && request.Covers(ids) && sameOverrideOrigin(ctx, request) {
			return request, nil
		}
	}
	request := &storage.FreezeOverrideRequest{
		Identifier:  engine.NewFreezeOverrideID(),
		Environment: in.Environment,
		Action:      in.Action,
		TargetID:    in.TargetID,
		Database:    in.Database,
		Blockers:    ids,
		Reason:      in.Override,
		RequestedBy: requester,
	}
	if fromPullRequest {
		request.PRAuthor = storage.GitHubIdentity(pr.Author)
		request.Repository = pr.Repository
		request.PullRequest = pr.PullRequest
	}
	now, err := freezes.Now(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the database clock for an override request: %w", err)
	}
	request.ExpiresAt = now.Add(storage.FreezeOverrideMaxLifetime)
	return overrides.Create(ctx, request)
}

// sameOverrideOrigin reports whether request was asked for where this change
// comes from: on the same pull request for a pull request command, or outside
// any pull request otherwise. The target names only the schema, so the same
// change on two pull requests shares it, and without this a request asked for
// on one, approved by the other's author, would admit the other (AZ-10). A
// pull request's number fixes its author, so matching it matches the author.
func sameOverrideOrigin(ctx context.Context, request *storage.FreezeOverrideRequest) bool {
	if pr, ok := pullRequestOverrideRequester(ctx); ok {
		return request.PullRequest == pr.PullRequest && strings.EqualFold(request.Repository, pr.Repository)
	}
	return !request.FromPullRequest()
}

// overrideCaller is the verified caller who asks for an override and
// runs on its approval.
func overrideCaller(ctx context.Context) (string, bool) {
	if pr, ok := pullRequestOverrideRequester(ctx); ok {
		// The login came from a signed webhook, so it is verified.
		return storage.GitHubIdentity(pr.Login), pr.Login != ""
	}
	// A subject in the pull request domain is refused: it would pass for a
	// GitHub login.
	subject, verified := auth.VerifiedSubject(ctx)
	if !verified || storage.IsGitHubIdentity(subject) {
		return "", false
	}
	return subject, true
}

// applyOverrideTarget names what an override of an apply covers: the
// database, the rollout member it is narrowed to, and for each member it runs
// on, the schema and the ignore_tables and drop_tables that member's plan was
// reviewed under. It is not the plan's identifier, because each apply from a
// pull request or the CLI plans afresh, and a repeated command would never
// find its approval. Any change to what a member would run, or a rollout that
// gained or lost a member, is a new target and needs a new approval.
func applyOverrideTarget(database, target string, members []applyMember) string {
	type coveredMember struct {
		Member string   `json:"member"`
		Files  any      `json:"files"` // map keys marshal sorted
		Ignore []string `json:"ignore_tables"`
		Drop   []string `json:"drop_tables"`
	}
	covered := make([]coveredMember, 0, len(members))
	for _, m := range members {
		covered = append(covered, coveredMember{Member: m.MemberID(), Files: m.Plan.SchemaFiles, Ignore: m.Plan.IgnoreTables(), Drop: m.Plan.DropTables()})
	}
	slices.SortFunc(covered, func(a, b coveredMember) int { return strings.Compare(a.Member, b.Member) })
	encoded, err := json.Marshal(covered)
	if err != nil {
		// Fail safe: a target no approval names.
		return engine.NewFreezeOverrideID()
	}
	sum := sha256.Sum256(encoded)
	return fmt.Sprintf("%s/%s@%x", database, target, sum[:8])
}

// applyOverrideTargetFor resolves the members an apply would run and the plan
// each runs, the same way apply creation does, and names them as an override
// target. An apply whose members do not resolve fails here with the error its
// creation would return.
func (s *Service) applyOverrideTargetFor(ctx context.Context, plan *storage.Plan, req ApplyRequest) (string, error) {
	targets, _, err := s.applyTargets(plan, req)
	if err != nil {
		return "", err
	}
	members, err := s.resolveApplyMembers(ctx, plan, req.Environment, targets)
	if err != nil {
		return "", err
	}
	return applyOverrideTarget(plan.Database, req.Target, members), nil
}

// isFreezeAdmissionRefusal reports whether the admission write refused the
// change over its freeze check: the epoch moved, or could not be read.
func isFreezeAdmissionRefusal(err error) bool {
	return errors.Is(err, storage.ErrFreezeEpochChanged) || errors.Is(err, storage.ErrFreezeEpochUnreadable)
}

// changeGateAdmissionError maps the admission write's refusal over its freeze
// check to *ChangeGateUnavailableError, which asks the caller to try again.
// After a stale check, the retry checks the freezes afresh and sees the new
// freeze. After an unreadable epoch, the gate fails closed, as it does when
// it cannot list the freezes. Any other error is returned as it is.
func (s *Service) changeGateAdmissionError(ctx context.Context, checkpoint changeGateCheckpoint, environment string, err error) error {
	if !isFreezeAdmissionRefusal(err) {
		return err
	}
	metrics.RecordChangeGateError(ctx, environment, string(checkpoint))
	message := "a freeze was created while the change was being admitted, so the change was refused"
	if !errors.Is(err, storage.ErrFreezeEpochChanged) {
		message = "the freeze epoch could not be read while the change was being admitted, so the change was refused"
	}
	s.logger.Warn(message, "checkpoint", string(checkpoint), "environment", environment, "error", err)
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
	// OverrideRequest is the request recorded for a caller who gave an
	// override reason, awaiting an approver other than the requester.
	OverrideRequest *storage.FreezeOverrideRequest
	// OverrideReasonKept reports a repeat that found the requester's
	// pending request with a different reason. The request keeps its
	// first reason, which is the one the approver sees.
	OverrideReasonKept bool
	// OverrideUnrecorded reports an override reason given by a caller who
	// is not verified, by a pull request command with no known author, or
	// to a server with no override storage: no request was recorded.
	OverrideUnrecorded bool
}

func (e *ChangeGateRefusedError) Error() string {
	message := e.Blocked()
	if e.OverrideRequest != nil {
		message += fmt.Sprintf("; override request %s needs a freeze approver other than you", e.OverrideRequest.Identifier)
		if e.OverrideReasonKept {
			message += fmt.Sprintf(" (you asked for it earlier, and it keeps that reason: %q)", e.OverrideRequest.Reason)
		}
	}
	if e.OverrideUnrecorded {
		message += "; no override was requested, because asking for one needs a verified caller"
	}
	return message
}

// Blocked is the refusal without what it says about an override request: what
// blocks the change, and that it clears when the blockers end.
func (e *ChangeGateRefusedError) Blocked() string {
	return e.BlockedWith(func(reason string) string { return reason })
}

// BlockedWith is Blocked with each blocker's reason written by reason, for a
// caller that renders the refusal where a person's text needs escaping.
func (e *ChangeGateRefusedError) BlockedWith(reason func(string) string) string {
	parts := make([]string, 0, len(e.Blockers))
	for _, blocker := range e.Blockers {
		until := "until lifted"
		if blocker.EndsAt != nil {
			until = "until " + blocker.EndsAt.UTC().Format(time.RFC3339)
		}
		parts = append(parts, fmt.Sprintf("%s %s (%s, %s)", blocker.Gate, blocker.ID, reason(blocker.Reason), until))
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
		if refused.OverrideRequest != nil {
			overrideRequest := freezeOverrideResponse(refused.OverrideRequest)
			response.OverrideRequest = &overrideRequest
		}
	}
	if unavailable, isUnavailable := errors.AsType[*ChangeGateUnavailableError](err); isUnavailable {
		response.Error = unavailable.UserMessage()
	}
	s.writeJSON(w, status, response)
	return true
}
