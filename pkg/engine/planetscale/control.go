package planetscale

import (
	"context"
	"errors"
	"fmt"
	"strings"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/psclient"
)

var _ engine.ControlResumeValidator = (*Engine)(nil)

// Stop preserves the legacy deploy-request cancel behavior until callers move
// to Cancel.
func (e *Engine) Stop(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return e.cancelDeployRequest(ctx, engine.ControlStop, req)
}

// Cancel cancels the deploy request permanently.
func (e *Engine) Cancel(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return e.cancelDeployRequest(ctx, engine.ControlCancel, req)
}

func (e *Engine) cancelDeployRequest(ctx context.Context, operation engine.ControlOperation, req *engine.ControlRequest) (*engine.ControlResult, error) {
	r := *req
	r.Database = e.resolveDatabase(req.Credentials, req.Database)
	req = &r

	meta, client, err := e.controlClient(operation, req)
	if err != nil {
		return nil, err
	}

	if _, err := client.CancelDeployRequest(ctx, &ps.CancelDeployRequestRequest{
		Organization: credOrg(req.Credentials),
		Database:     req.Database,
		Number:       meta.DeployRequestID,
	}); err != nil {
		// Cancel must be idempotent: PlanetScale rejects a cancel of a deploy
		// request that is already closed, and a prior cancel attempt (this
		// driver's or another's) may have closed it. Read the deploy request's
		// live state and classify the rejection: "already cancelled" is the
		// goal state and reads as success; a deploy request that closed by
		// completing means the schema change landed before the cancel arrived,
		// so the caller must reconcile to the completed outcome rather than
		// retry a rejection that can never succeed; every other state stays a
		// plain error naming the live state, since reporting a successful
		// cancel there would misrepresent what happened on the target.
		dr, getErr := client.GetDeployRequest(ctx, &ps.GetDeployRequestRequest{
			Organization: credOrg(req.Credentials),
			Database:     req.Database,
			Number:       meta.DeployRequestID,
		})
		if getErr != nil {
			// A deploy request PlanetScale has no record of can never accept the
			// cancel — retrying cannot make it appear.
			var psErr *ps.Error
			if errors.As(getErr, &psErr) && psErr.Code == ps.ErrNotFound {
				return nil, engine.NewPermanentError("cancel deploy request #%d rejected (%w); deploy request not found: %w", meta.DeployRequestID, err, getErr)
			}
			return nil, fmt.Errorf("cancel deploy request #%d (may have been deleted; state read also failed: %w): %w", meta.DeployRequestID, getErr, err)
		}
		classification, classified := classifyCancelRejection(dr.DeploymentState)
		if !classified {
			// A deployment state with no explicit classification fails toward a
			// plain retryable error — never toward success or a typed completed
			// rejection. The counter surfaces the gap so an operator adds the
			// state to classifyCancelRejection instead of the drive retrying the
			// rejection indefinitely.
			e.logger.Warn("cancel rejected in a deployment state with no explicit classification; the rejection will be retried as a plain error until the state is classified",
				"database", req.Database,
				"deploy_request", meta.DeployRequestID,
				"deployment_state", dr.DeploymentState,
				"error", err)
			metrics.RecordPlanetScaleUnclassifiedCancelRejection(ctx, req.Database, dr.DeploymentState)
		}
		switch classification {
		case cancelRejectionAlreadyCancelled:
			return &engine.ControlResult{
				Accepted:    true,
				Message:     fmt.Sprintf("Deploy request #%d already cancelled", meta.DeployRequestID),
				ResumeState: req.ResumeState,
			}, nil
		case cancelRejectionAlreadyCompleted:
			return nil, engine.NewAlreadyCompletedError("cancel deploy request #%d rejected: the deploy request completed before the cancel arrived (deployment state %q): %w", meta.DeployRequestID, dr.DeploymentState, err)
		default:
			return nil, fmt.Errorf("cancel deploy request #%d rejected in deployment state %q: %w", meta.DeployRequestID, dr.DeploymentState, err)
		}
	}

	return &engine.ControlResult{
		Accepted:    true,
		Message:     "Deploy request cancelled",
		ResumeState: req.ResumeState,
	}, nil
}

// cancelRejectionClass names how a rejected cancel must be reported, given the
// deployment state the deploy request was observed in after the rejection.
type cancelRejectionClass int

const (
	// cancelRejectionAlreadyCancelled: the deploy request is already cancelled
	// or cancelling — the cancel's goal state — so the rejection reads as
	// success.
	cancelRejectionAlreadyCancelled cancelRejectionClass = iota
	// cancelRejectionAlreadyCompleted: the deploy request closed by completing
	// before the cancel arrived. The rejection is typed AlreadyCompletedError
	// so the caller reconciles stored state to the completed outcome instead
	// of retrying a rejection that can never succeed.
	cancelRejectionAlreadyCompleted
	// cancelRejectionStateError: the rejection stays a plain error naming the
	// deploy request's live state — reporting a successful cancel or a
	// completed change there would misrepresent what happened on the target.
	cancelRejectionStateError
)

// classifyCancelRejection maps the deployment state observed after a rejected
// cancel to how the rejection must be reported. Every deployment state
// PlanetScale can report has an explicit classification here; classified is
// false only for a state this enumeration has never seen (one PlanetScale
// added later), which fails toward a plain error and increments the
// unclassified-rejection counter at the call site so the gap gets classified
// instead of retried forever.
func classifyCancelRejection(deploymentState string) (class cancelRejectionClass, classified bool) {
	switch deploymentState {
	case deployState.InProgressCancel, deployState.CompleteCancel, deployState.Cancelled:
		return cancelRejectionAlreadyCancelled, true
	case deployState.Complete, deployState.NoChanges:
		return cancelRejectionAlreadyCompleted, true
	case deployState.CompletePendingRevert:
		// Deliberately not already-completed: the change is still in its revert
		// window, where the revert and skip-revert paths own the outcome. Once
		// the window closes the deploy request settles to complete and a
		// still-pending cancel reconciles then.
		return cancelRejectionStateError, true
	case deployState.Pending, deployState.Ready, deployState.Submitting, deployState.Queued,
		deployState.InProgress, deployState.PendingCutover, deployState.InProgressCutover,
		deployState.InProgressVSchema:
		// A cancel rejected while the deploy request is still live is a
		// backend surprise — surface it as an error naming the state rather
		// than guessing at an outcome.
		return cancelRejectionStateError, true
	case deployState.InProgressRevert, deployState.InProgressRevertVSchema,
		deployState.CompleteRevert, deployState.CompleteRevertError:
		// Reverting or reverted deploy requests are owned by the revert paths;
		// a rejected cancel there must not read as cancelled or completed.
		return cancelRejectionStateError, true
	case deployState.CompleteError, deployState.Error, deployState.Failed:
		// The deploy request closed by failing. The plain error keeps the
		// failed outcome visible instead of reporting a successful cancel.
		return cancelRejectionStateError, true
	default:
		return cancelRejectionStateError, false
	}
}

// Start starts a deferred deploy request. Cancelled deploy requests cannot be restarted.
func (e *Engine) Start(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	r := *req
	r.Database = e.resolveDatabase(req.Credentials, req.Database)
	req = &r

	meta, err := controlMeta(engine.ControlStart, req)
	if err != nil {
		return nil, fmt.Errorf("decode control metadata: %w", err)
	}

	if !meta.DeferredDeploy {
		return nil, fmt.Errorf("start not supported for planetscale engine: cancelled deploy requests cannot be restarted")
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	e.logger.Info("starting deferred deploy",
		"deploy_request", meta.DeployRequestID,
		"instant_ddl", meta.IsInstant,
	)
	// The stored IsInstant is trusted as-is: a ControlRequest carries no schema
	// changes to re-classify against, and every writer of this metadata (fresh
	// apply, resume, recovered deploy request) records the already-gated decision
	// — an unsafe or cutover-deferred change is stored with IsInstant=false, so
	// starting from stored state cannot widen the decision.
	dr, deployErr := e.deployDeployRequest(ctx, client, credOrg(req.Credentials), req.Database, meta.DeployRequestID, meta.IsInstant)
	if deployErr != nil {
		return nil, deployErr
	}
	return &engine.ControlResult{
		Accepted:    true,
		Message:     fmt.Sprintf("Deploy initiated for deploy request #%d", dr.Number),
		ResumeState: req.ResumeState,
	}, nil
}

// Cutover triggers the final schema swap via ApplyDeployRequest.
func (e *Engine) Cutover(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	r := *req
	r.Database = e.resolveDatabase(req.Credentials, req.Database)
	req = &r

	return e.controlDeployRequest(ctx, engine.ControlCutover, req, "cutover", "Cutover initiated for",
		func(ctx context.Context, client psclient.PSClient, number uint64) (*ps.DeployRequest, error) {
			dr, err := client.ApplyDeployRequest(ctx, &ps.ApplyDeployRequestRequest{
				Organization: credOrg(req.Credentials),
				Database:     req.Database,
				Number:       number,
			})
			if err != nil && isDeployNotStagedError(err) {
				return nil, engine.NewNotReadyError("deploy request has not staged its changes yet: %w", err)
			}
			return dr, err
		})
}

// isDeployNotStagedError reports whether the PlanetScale API rejected a
// cutover because the deploy request has not staged its changes yet. The API
// briefly reports pending_cutover before the staged changes are visible to the
// apply endpoint, so the rejection clears on its own once staging completes.
func isDeployNotStagedError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "changes have not been staged")
}

// Revert rolls back a completed schema change during the revert window.
func (e *Engine) Revert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	r := *req
	r.Database = e.resolveDatabase(req.Credentials, req.Database)
	req = &r

	return e.controlDeployRequest(ctx, engine.ControlRevert, req, "revert", "Revert initiated for",
		func(ctx context.Context, client psclient.PSClient, number uint64) (*ps.DeployRequest, error) {
			return client.RevertDeployRequest(ctx, &ps.RevertDeployRequestRequest{
				Organization: credOrg(req.Credentials),
				Database:     req.Database,
				Number:       number,
			})
		})
}

// SkipRevert closes the revert window, making the schema change permanent.
func (e *Engine) SkipRevert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	r := *req
	r.Database = e.resolveDatabase(req.Credentials, req.Database)
	req = &r

	return e.controlDeployRequest(ctx, engine.ControlSkipRevert, req, "skip revert for", "Revert window skipped for",
		func(ctx context.Context, client psclient.PSClient, number uint64) (*ps.DeployRequest, error) {
			return client.SkipRevertDeployRequest(ctx, &ps.SkipRevertDeployRequestRequest{
				Organization: credOrg(req.Credentials),
				Database:     req.Database,
				Number:       number,
			})
		})
}

// controlClient validates the resume-state metadata for a control operation and
// returns it together with a PlanetScale client for the request's credentials.
func (e *Engine) controlClient(operation engine.ControlOperation, req *engine.ControlRequest) (*psMetadata, psclient.PSClient, error) {
	meta, err := controlMeta(operation, req)
	if err != nil {
		return nil, nil, fmt.Errorf("decode control metadata: %w", err)
	}
	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, nil, fmt.Errorf("get planetscale client: %w", err)
	}
	return meta, client, nil
}

// controlDeployRequest runs a deploy-request mutation that returns the updated
// deploy request and reports a successful ControlResult. errVerb names the action
// in the failure message; messagePrefix prefixes the success message, both
// followed by "deploy request #<number>".
func (e *Engine) controlDeployRequest(
	ctx context.Context,
	operation engine.ControlOperation,
	req *engine.ControlRequest,
	errVerb string,
	messagePrefix string,
	mutate func(ctx context.Context, client psclient.PSClient, number uint64) (*ps.DeployRequest, error),
) (*engine.ControlResult, error) {
	meta, client, err := e.controlClient(operation, req)
	if err != nil {
		return nil, err
	}

	dr, err := mutate(ctx, client, meta.DeployRequestID)
	if err != nil {
		// A not-ready rejection means the deploy request exists and will accept
		// the operation once its backend catches up — the deleted-deploy-request
		// hint would misdirect whoever reads the message.
		if engine.IsNotReady(err) {
			return nil, fmt.Errorf("%s deploy request #%d: %w", errVerb, meta.DeployRequestID, err)
		}
		return nil, fmt.Errorf("%s deploy request #%d (may have been deleted): %w", errVerb, meta.DeployRequestID, err)
	}

	return &engine.ControlResult{
		Accepted:    true,
		Message:     fmt.Sprintf("%s deploy request #%d", messagePrefix, dr.Number),
		ResumeState: req.ResumeState,
	}, nil
}

// controlMeta extracts and validates psMetadata from a control request.
func controlMeta(operation engine.ControlOperation, req *engine.ControlRequest) (*psMetadata, error) {
	if req.ResumeState == nil || req.ResumeState.Metadata == "" {
		return nil, fmt.Errorf("no active schema change")
	}
	meta, err := decodePSMetadata(req.ResumeState.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode resume state: %w", err)
	}
	if err := validateControlMetadata(operation, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// ValidateControlResumeState checks that the opaque PlanetScale resume state can
// address the deploy request targeted by a control operation.
func (e *Engine) ValidateControlResumeState(operation engine.ControlOperation, resumeState *engine.ResumeState) error {
	return validateControlResumeState(operation, resumeState)
}

func validateControlResumeState(operation engine.ControlOperation, resumeState *engine.ResumeState) error {
	if resumeState == nil || resumeState.Metadata == "" {
		return fmt.Errorf("no active schema change")
	}
	meta, err := decodePSMetadata(resumeState.Metadata)
	if err != nil {
		return fmt.Errorf("decode resume state: %w", err)
	}
	return validateControlMetadata(operation, meta)
}

func validateControlMetadata(operation engine.ControlOperation, meta *psMetadata) error {
	if missing := missingControlMetadata(meta); len(missing) > 0 {
		prefix := "deploy request metadata is incomplete"
		if operation != "" {
			prefix = fmt.Sprintf("%s control resume state is incomplete", operation)
		}
		return fmt.Errorf("%s (missing %s)", prefix, strings.Join(missing, ", "))
	}
	return nil
}

func missingControlMetadata(meta *psMetadata) []string {
	var missing []string
	if meta.BranchName == "" {
		missing = append(missing, "branch_name")
	}
	if meta.DeployRequestID == 0 {
		missing = append(missing, "deploy_request_id")
	}
	if meta.DeployRequestURL == "" {
		missing = append(missing, "deploy_request_url")
	}
	return missing
}
