package tern

import (
	"context"
	"fmt"
	"time"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// startStoppedDeploymentApplyBeforeAttach readies the deployment's data-plane
// apply for an operation a stop caught before its first dispatch. The stop
// moved that operation to stopped in control-plane storage alone, and the start
// claim moved it to resuming, which dispatches it for the first time into the
// apply its deployment's siblings already share. The data plane refuses an
// attach to an apply that still reads stopped, and the sibling drives that ask
// it to start race this dispatch. So when the shared apply still reads stopped,
// this drive asks for the start itself and waits for the apply to leave
// stopped, and the attach joins the resumed apply instead of failing the
// operation.
//
// It reports standDown when a stop or cancel landed while it waited; that
// request settles this undispatched operation, so nothing is dispatched. An
// error leaves the operation resuming for the next claim to retry.
func (c *GRPCClient) startStoppedDeploymentApplyBeforeAttach(ctx context.Context, apply *storage.Apply, scope applyTaskScope) (bool, error) {
	if !scope.usesOperationRemoteResume() || !state.IsState(scope.operation.State, state.Apply.Resuming) {
		return false, nil
	}
	op := scope.operation
	logger := c.applyLogger(apply)
	ops, err := c.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return false, fmt.Errorf("list operations of apply %s to find deployment %s's data-plane apply before attaching operation %s: %w", apply.ApplyIdentifier, op.Deployment, op.OperationKey, err)
	}
	remoteID, err := storage.DeploymentRemoteApplyID(ops, op.Deployment)
	if err != nil {
		return false, fmt.Errorf("resolve deployment %s's data-plane apply before attaching operation %s of apply %s: %w", op.Deployment, op.OperationKey, apply.ApplyIdentifier, err)
	}
	if remoteID == "" {
		logger.DebugContext(ctx, "no sibling operation of the deployment has dispatched; this dispatch creates its data-plane apply",
			append(apply.MutableLogAttrs(), "apply_operation_id", op.ID, "operation_deployment", op.Deployment)...)
		return false, nil
	}

	deadline := time.Now().Add(grpcStoppedAfterStartGracePeriod)
	startRequested := false
	var startErr error
	for {
		resp, err := c.client.Progress(ctx, &ternv1.ProgressRequest{ApplyId: remoteID, Environment: apply.Environment})
		if err != nil {
			return false, fmt.Errorf("check deployment %s's data-plane apply %s before attaching operation %s of apply %s: %w", op.Deployment, remoteID, op.OperationKey, apply.ApplyIdentifier, err)
		}
		remoteState := ProtoStateToStorage(resp.State)
		if !state.IsState(remoteState, state.Apply.Stopped) {
			if startRequested {
				logger.InfoContext(ctx, "deployment's data-plane apply left stopped; attaching the operation a stop caught before its first dispatch",
					append(apply.MutableLogAttrs(),
						"apply_operation_id", op.ID, "operation_deployment", op.Deployment,
						"remote_apply_id", remoteID, "remote_state", remoteState)...)
			}
			return false, nil
		}
		if standDown, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope); standDown || err != nil {
			return standDown, err
		}
		if !time.Now().Before(deadline) {
			return false, fmt.Errorf("deployment %s's data-plane apply %s still reads stopped %s after this drive asked for its start (last start error: %w); operation %s of apply %s attaches on a later claim", op.Deployment, remoteID, grpcStoppedAfterStartGracePeriod, startErr, op.OperationKey, apply.ApplyIdentifier)
		}
		if !startRequested {
			startErr = c.startDeploymentApply(ctx, apply, remoteID)
			if startErr == nil {
				startRequested = true
				logger.InfoContext(ctx, "deployment's data-plane apply still reads stopped; requested its start before attaching the operation a stop caught before its first dispatch",
					append(apply.MutableLogAttrs(),
						"apply_operation_id", op.ID, "operation_deployment", op.Deployment, "remote_apply_id", remoteID)...)
			} else {
				logger.WarnContext(ctx, "start of the deployment's stopped data-plane apply failed; retrying until it leaves stopped",
					append(apply.MutableLogAttrs(),
						"apply_operation_id", op.ID, "operation_deployment", op.Deployment,
						"remote_apply_id", remoteID, "error", startErr)...)
			}
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("wait for deployment %s's data-plane apply %s to leave stopped before attaching operation %s of apply %s: %w", op.Deployment, remoteID, op.OperationKey, apply.ApplyIdentifier, ctx.Err())
		case <-time.After(grpcProgressPollInterval):
		}
	}
}

// startDeploymentApply asks the data plane to start the deployment's shared
// apply on behalf of the operator's pending start. The data plane joins a start
// a sibling drive already asked for, so the race between drives is harmless.
func (c *GRPCClient) startDeploymentApply(ctx context.Context, apply *storage.Apply, remoteID string) error {
	startReq, err := pendingStartControlRequest(ctx, c.storage, apply)
	if err != nil {
		return fmt.Errorf("load pending start of apply %s: %w", apply.ApplyIdentifier, err)
	}
	caller := ""
	if startReq != nil {
		caller = startReq.RequestedBy
	}
	if _, err := c.client.Start(ctx, &ternv1.StartRequest{ApplyId: remoteID, Environment: apply.Environment, Caller: caller}); err != nil {
		return fmt.Errorf("start data-plane apply %s: %w", remoteID, err)
	}
	return nil
}
