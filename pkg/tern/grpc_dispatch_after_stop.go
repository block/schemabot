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
// request settles this undispatched operation, so nothing is dispatched. Only
// the operator's pending start is sent on: with none pending, a sibling drive
// already settled it, so this drive waits without starting anything. When the
// shared apply still reads stopped once the grace period runs out, the operation
// goes back to stopped and the pending start fails, so the operator sees the
// start did not take and no later claim waits on it again.
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
	var lastStartErr error
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
			return false, c.returnUndispatchedOperationToStopped(ctx, apply, op, remoteID, stillStoppedCause(startRequested, lastStartErr))
		}
		if !startRequested {
			startReq, err := pendingStartControlRequest(ctx, c.storage, apply)
			if err != nil {
				return false, fmt.Errorf("load pending start of apply %s before starting deployment %s's data-plane apply %s: %w", apply.ApplyIdentifier, op.Deployment, remoteID, err)
			}
			if startReq == nil {
				logger.DebugContext(ctx, "deployment's data-plane apply still reads stopped and no start is pending; waiting without starting it",
					append(apply.MutableLogAttrs(),
						"apply_operation_id", op.ID, "operation_deployment", op.Deployment, "remote_apply_id", remoteID)...)
			} else if _, err := c.client.Start(ctx, &ternv1.StartRequest{ApplyId: remoteID, Environment: apply.Environment, Caller: startReq.RequestedBy}); err != nil {
				lastStartErr = err
				logger.WarnContext(ctx, "start of the deployment's stopped data-plane apply failed; retrying until it leaves stopped or the grace period ends",
					append(apply.MutableLogAttrs(),
						"apply_operation_id", op.ID, "operation_deployment", op.Deployment,
						"remote_apply_id", remoteID, "error", err)...)
			} else {
				startRequested = true
				logger.InfoContext(ctx, "deployment's data-plane apply still reads stopped; requested its start before attaching the operation a stop caught before its first dispatch",
					append(apply.MutableLogAttrs(),
						"apply_operation_id", op.ID, "operation_deployment", op.Deployment, "remote_apply_id", remoteID)...)
			}
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("wait for deployment %s's data-plane apply %s to leave stopped before attaching operation %s of apply %s: %w", op.Deployment, remoteID, op.OperationKey, apply.ApplyIdentifier, ctx.Err())
		case <-time.After(grpcProgressPollInterval):
		}
	}
}

// stillStoppedCause says why the deployment's data-plane apply still reads
// stopped at the end of the grace period, so the error names the one cause that
// applies.
func stillStoppedCause(startRequested bool, lastStartErr error) string {
	switch {
	case startRequested:
		return fmt.Sprintf("it still reads stopped %s after this drive asked for its start", grpcStoppedAfterStartGracePeriod)
	case lastStartErr != nil:
		return fmt.Sprintf("every start this drive sent for %s failed (last: %v)", grpcStoppedAfterStartGracePeriod, lastStartErr)
	default:
		return fmt.Sprintf("no start was pending for %s, so nothing asked it to resume", grpcStoppedAfterStartGracePeriod)
	}
}

// returnUndispatchedOperationToStopped puts an operation a stop caught before
// its first dispatch back to stopped when its deployment's data-plane apply
// would not leave stopped, and fails the operator's pending start. Left
// resuming, the operation would be claimed again and wait out the same grace
// period on every claim with nothing telling the operator the start did not
// take; stopped, it waits for the operator to issue the start again.
func (c *GRPCClient) returnUndispatchedOperationToStopped(ctx context.Context, apply *storage.Apply, op *storage.ApplyOperation, remoteID, cause string) error {
	message := fmt.Sprintf("Deployment %s did not resume, so operation %s stays stopped and the start can be re-issued", op.Deployment, op.OperationKey)
	c.applyLogger(apply).WarnContext(ctx, "deployment's data-plane apply did not leave stopped within the grace period; returning the undispatched operation to stopped and failing the pending start",
		append(apply.MutableLogAttrs(),
			"apply_operation_id", op.ID, "operation_deployment", op.Deployment,
			"remote_apply_id", remoteID, "cause", cause)...)
	if err := c.storage.ApplyOperations().UpdateState(ctx, op.ID, state.Apply.Stopped); err != nil {
		return fmt.Errorf("return operation %s of apply %s to stopped after deployment %s's data-plane apply %s did not resume: %w", op.OperationKey, apply.ApplyIdentifier, op.Deployment, remoteID, err)
	}
	op.State = state.Apply.Stopped
	c.logApplyWarning(ctx, apply, message)
	if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, message, remoteID); err != nil {
		return fmt.Errorf("fail pending start of apply %s after deployment %s's data-plane apply %s did not resume: %w", apply.ApplyIdentifier, op.Deployment, remoteID, err)
	}
	return fmt.Errorf("deployment %s's data-plane apply %s did not resume (%s); operation %s of apply %s is stopped again", op.Deployment, remoteID, cause, op.OperationKey, apply.ApplyIdentifier)
}
