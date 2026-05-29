package planetscale

import (
	"context"
	"fmt"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/engine"
)

// Stop cancels the deploy request. This is permanent.
func (e *Engine) Stop(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	meta, err := e.controlMeta(req)
	if err != nil {
		return nil, fmt.Errorf("decode control metadata: %w", err)
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	_, err = client.CancelDeployRequest(ctx, &ps.CancelDeployRequestRequest{
		Organization: credOrg(req.Credentials),
		Database:     req.Database,
		Number:       meta.DeployRequestID,
	})
	if err != nil {
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
	meta, err := e.controlMeta(req)
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
	meta, err := e.controlMeta(req)
	if err != nil {
		return nil, fmt.Errorf("decode control metadata: %w", err)
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	dr, err := client.ApplyDeployRequest(ctx, &ps.ApplyDeployRequestRequest{
		Organization: credOrg(req.Credentials),
		Database:     req.Database,
		Number:       meta.DeployRequestID,
	})
	if err != nil {
		return nil, fmt.Errorf("cutover deploy request #%d (may have been deleted): %w", meta.DeployRequestID, err)
	}

	return &engine.ControlResult{
		Accepted:    true,
		Message:     fmt.Sprintf("Cutover initiated for deploy request #%d", dr.Number),
		ResumeState: req.ResumeState,
	}, nil
}

// Revert rolls back a completed schema change during the revert window.
func (e *Engine) Revert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	meta, err := e.controlMeta(req)
	if err != nil {
		return nil, fmt.Errorf("decode control metadata: %w", err)
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	dr, err := client.RevertDeployRequest(ctx, &ps.RevertDeployRequestRequest{
		Organization: credOrg(req.Credentials),
		Database:     req.Database,
		Number:       meta.DeployRequestID,
	})
	if err != nil {
		return nil, fmt.Errorf("revert deploy request #%d (may have been deleted): %w", meta.DeployRequestID, err)
	}

	return &engine.ControlResult{
		Accepted:    true,
		Message:     fmt.Sprintf("Revert initiated for deploy request #%d", dr.Number),
		ResumeState: req.ResumeState,
	}, nil
}

// SkipRevert closes the revert window, making the schema change permanent.
func (e *Engine) SkipRevert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	meta, err := e.controlMeta(req)
	if err != nil {
		return nil, fmt.Errorf("decode control metadata: %w", err)
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	dr, err := client.SkipRevertDeployRequest(ctx, &ps.SkipRevertDeployRequestRequest{
		Organization: credOrg(req.Credentials),
		Database:     req.Database,
		Number:       meta.DeployRequestID,
	})
	if err != nil {
		return nil, fmt.Errorf("skip revert for deploy request #%d (may have been deleted): %w", meta.DeployRequestID, err)
	}

	return &engine.ControlResult{
		Accepted:    true,
		Message:     fmt.Sprintf("Revert window skipped for deploy request #%d", dr.Number),
		ResumeState: req.ResumeState,
	}, nil
}

// controlMeta extracts and validates psMetadata from a control request.
func (e *Engine) controlMeta(req *engine.ControlRequest) (*psMetadata, error) {
	if req.ResumeState == nil || req.ResumeState.Metadata == "" {
		return nil, fmt.Errorf("no active schema change")
	}
	meta, err := decodePSMetadata(req.ResumeState.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode resume state: %w", err)
	}
	if meta.DeployRequestID == 0 {
		return nil, fmt.Errorf("no active schema change")
	}
	return meta, nil
}
