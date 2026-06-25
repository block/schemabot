package planetscale

import (
	"context"
	"fmt"
	"strings"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

var _ engine.ControlResumeValidator = (*Engine)(nil)

// Stop cancels the deploy request. This is permanent.
func (e *Engine) Stop(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	meta, client, err := e.controlClient(engine.ControlStop, req)
	if err != nil {
		return nil, err
	}

	if _, err := client.CancelDeployRequest(ctx, &ps.CancelDeployRequestRequest{
		Organization: credOrg(req.Credentials),
		Database:     req.Database,
		Number:       meta.DeployRequestID,
	}); err != nil {
		return nil, fmt.Errorf("cancel deploy request #%d (may have been deleted): %w", meta.DeployRequestID, err)
	}

	return &engine.ControlResult{
		Accepted:    true,
		Message:     "Deploy request cancelled",
		ResumeState: req.ResumeState,
	}, nil
}

// Start starts a deferred deploy request. Cancelled deploy requests cannot be restarted.
func (e *Engine) Start(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
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
	dr, deployErr := client.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
		Organization: credOrg(req.Credentials),
		Database:     req.Database,
		Number:       meta.DeployRequestID,
		InstantDDL:   meta.IsInstant,
	})
	if deployErr != nil {
		return nil, fmt.Errorf("deploy deploy request #%d: %w", meta.DeployRequestID, deployErr)
	}
	return &engine.ControlResult{
		Accepted:    true,
		Message:     fmt.Sprintf("Deploy initiated for deploy request #%d", dr.Number),
		ResumeState: req.ResumeState,
	}, nil
}

// Cutover triggers the final schema swap via ApplyDeployRequest.
func (e *Engine) Cutover(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return e.controlDeployRequest(ctx, engine.ControlCutover, req, "cutover", "Cutover initiated for",
		func(ctx context.Context, client psclient.PSClient, number uint64) (*ps.DeployRequest, error) {
			return client.ApplyDeployRequest(ctx, &ps.ApplyDeployRequestRequest{
				Organization: credOrg(req.Credentials),
				Database:     req.Database,
				Number:       number,
			})
		})
}

// Revert rolls back a completed schema change during the revert window.
func (e *Engine) Revert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
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
