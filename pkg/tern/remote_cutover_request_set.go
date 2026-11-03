package tern

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// processPendingRemoteCutoverSet runs a remote drive's part in a pending
// cutover request bound to the operations in ids. It is the control-plane side
// of LocalClient.processPendingCutoverSet: the drive sends the command to its
// own remote apply once, naming every operation of the set that remote apply
// runs, so the data plane records one request for all of them and each of its
// drives cuts over its own. Every drive here then settles the request once the
// whole set has resolved, so it never waits on a drive that no longer exists.
func (c *GRPCClient) processPendingRemoteCutoverSet(ctx context.Context, apply *storage.Apply, scope applyTaskScope, controlReq *storage.ApplyControlRequest, ids []int64, logger *slog.Logger) error {
	switch {
	case scope.operation == nil:
		logger.WarnContext(ctx, "pending gRPC cutover request is bound to a set of operations but this drive is not scoped to one; only settling it",
			"requested_by", controlRequestCaller(controlReq), "cutover_operation_ids", ids)
	case !slices.Contains(ids, scope.applyOperationID):
		logger.DebugContext(ctx, "pending gRPC cutover request does not name this drive's operation; only settling it",
			"requested_by", controlRequestCaller(controlReq), "cutover_operation_ids", ids, "drive_operation_id", scope.applyOperationID)
	default:
		if err := c.sendRemoteCutoverSetShare(ctx, apply, scope, controlReq, ids, logger); err != nil {
			return err
		}
	}
	return settleCutoverSet(ctx, c.storage, logger, apply, controlReq, ids, nil)
}

// sendRemoteCutoverSetShare sends a cutover request set to this drive's
// remote apply, once. The drive sends only while its own operation waits at
// the cutover and no earlier member holds it. An operation already cutting
// over was reached by a sibling's send of the same set, so it sends nothing.
func (c *GRPCClient) sendRemoteCutoverSetShare(ctx context.Context, apply *storage.Apply, scope applyTaskScope, controlReq *storage.ApplyControlRequest, ids []int64, logger *slog.Logger) error {
	operationID := scope.applyOperationID
	key := newCutoverShareKey(controlReq, operationID)
	if _, taken := c.cutoverSharesTaken.Load(key); taken {
		logger.DebugContext(ctx, "this drive already sent the pending gRPC cutover request to its remote apply; waiting for the rest of the set",
			"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID)
		return nil
	}
	if state.IsState(apply.State, state.Apply.Recovering) {
		logger.InfoContext(ctx, "pending gRPC cutover request is waiting for recovery to complete",
			"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID)
		return nil
	}
	if c.storage == nil {
		return fmt.Errorf("process pending gRPC cutover for apply %s: storage is not available", apply.ApplyIdentifier)
	}
	taskStore := c.storage.Tasks()
	if taskStore == nil {
		return fmt.Errorf("process pending gRPC cutover for apply %s: task store is not available", apply.ApplyIdentifier)
	}
	tasks, err := taskStore.GetByApplyOperationID(ctx, operationID)
	if err != nil {
		return fmt.Errorf("load tasks for apply_operation %d of apply %s before its share of the pending cutover: %w", operationID, apply.ApplyIdentifier, err)
	}
	if !tasksWaitingForCutover(tasks) {
		logger.InfoContext(ctx, "pending gRPC cutover request is waiting for this drive's operation to wait at the cutover",
			"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID)
		return nil
	}
	opStore := c.storage.ApplyOperations()
	if opStore == nil {
		return fmt.Errorf("process pending gRPC cutover for apply %s: apply operation store is not available", apply.ApplyIdentifier)
	}
	blocker, err := opStore.CutoverBlocker(ctx, operationID)
	if err != nil {
		return fmt.Errorf("check the hold on apply_operation %d before sending its share of the pending cutover of apply %s: %w", operationID, apply.ApplyIdentifier, err)
	}
	if blocker != nil {
		logger.InfoContext(ctx, "this drive's share of the pending gRPC cutover request is held by an earlier member, so it waits until that member completes or recovers, or this operation ends",
			"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID,
			"blocking_operation_id", blocker.ID, "blocking_deployment", blocker.Deployment,
			"blocking_target", blocker.Target, "blocking_state", blocker.State)
		return nil
	}
	if stopReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
		return fmt.Errorf("check pending stop request before pending gRPC cutover for apply %s: %w", apply.ApplyIdentifier, err)
	} else if stopReq != nil {
		return fmt.Errorf("process pending gRPC cutover for apply %s: schema change has a pending stop request; cutover is blocked until stop is processed", apply.ApplyIdentifier)
	}
	remoteID := scope.remoteApplyID(apply)
	if remoteID == "" {
		message := "remote apply id is not available"
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, message); err != nil {
			return err
		}
		return fmt.Errorf("process pending gRPC cutover for apply %s: %s", apply.ApplyIdentifier, message)
	}
	remoteOperationID, err := scope.remoteOperationScope()
	if err != nil {
		if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, err.Error(), remoteID); failErr != nil {
			return failErr
		}
		return fmt.Errorf("process pending gRPC cutover for apply %s: %w", apply.ApplyIdentifier, err)
	}
	var remoteOperationIDs []string
	if remoteOperationID != "" {
		remoteOperationIDs, err = remoteCutoverSetOperationIDs(ctx, opStore, logger, apply, ids, remoteID, remoteOperationID)
		if err != nil {
			return err
		}
	}
	req := &ternv1.CutoverRequest{
		ApplyId:          remoteID,
		Environment:      apply.Environment,
		Caller:           controlReq.RequestedBy,
		ApplyOperationId: remoteOperationID,
	}
	if len(remoteOperationIDs) > 1 {
		req.ApplyOperationIds = remoteOperationIDs
	}
	resp, err := c.client.Cutover(ctx, req)
	if err != nil {
		if isAmbiguousRemoteCallError(err) {
			// The data plane records a cutover durably on receipt and answers
			// a re-sent set as the request it already holds, so a call that
			// ended without an answer leaves the request pending for the next
			// drive tick to re-send.
			logger.WarnContext(ctx, "remote cutover call ended without an answer; the cutover request stays pending and the next drive re-sends it",
				"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID, "remote_apply_id", remoteID, "error", err)
			return fmt.Errorf("request remote gRPC cutover for apply %s remote %s: outcome unknown, request left pending: %w", apply.ApplyIdentifier, remoteID, err)
		}
		return c.failRemoteCutoverSetShare(ctx, apply, operationID, remoteID, fmt.Sprintf("remote cutover failed: %v", err))
	}
	if resp == nil {
		return c.failRemoteCutoverSetShare(ctx, apply, operationID, remoteID, "the data plane returned neither a response nor an error")
	}
	if !resp.Accepted {
		return c.failRemoteCutoverSetShare(ctx, apply, operationID, remoteID, controlRefusalMessage(storage.ControlOperationCutover, resp.ErrorMessage))
	}
	c.cutoverSharesTaken.Store(key, struct{}{})
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered,
		fmt.Sprintf("Remote cutover accepted for apply %s (remote %s)%s", apply.ApplyIdentifier, remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	logger.InfoContext(ctx, "sent this drive's share of the pending gRPC cutover request to its remote apply; the request settles once the whole set has",
		"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID,
		"remote_apply_id", remoteID, "remote_operation_ids", req.GetApplyOperationIds())
	return nil
}

// failRemoteCutoverSetShare fails a pending cutover request set whose send
// the data plane did not take. The whole command fails, as it does when the
// local cutover path refuses one operation of a set: the operator sees the
// refusal and issues the cutover again.
func (c *GRPCClient) failRemoteCutoverSetShare(ctx context.Context, apply *storage.Apply, operationID int64, remoteID, message string) error {
	if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, message, remoteID); err != nil {
		return fmt.Errorf("request remote gRPC cutover for apply %s operation %d remote %s: %s: %w", apply.ApplyIdentifier, operationID, remoteID, message, err)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError,
		fmt.Sprintf("Remote cutover was not accepted for apply %s (remote %s): %s", apply.ApplyIdentifier, remoteID, message), "", "")
	return fmt.Errorf("request remote gRPC cutover for apply %s operation %d remote %s: %s", apply.ApplyIdentifier, operationID, remoteID, message)
}

// remoteCutoverSetOperationIDs returns the remote operation ids of the
// operations in ids that run on the remote apply remoteID, the drive's own
// remoteOperationID among them. The members of a set that run on other remote
// applies are sent by their own drives. A member on the same remote apply
// with no remote operation id recorded cannot be named, and its own drive
// refuses its scoped calls (applyTaskScope.remoteOperationScope).
func remoteCutoverSetOperationIDs(ctx context.Context, opStore storage.ApplyOperationStore, logger *slog.Logger, apply *storage.Apply, ids []int64, remoteID, remoteOperationID string) ([]string, error) {
	ops, err := opStore.ListByApply(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("list the operations the pending cutover of apply %s is for: %w", apply.ApplyIdentifier, err)
	}
	remoteIDs := []string{remoteOperationID}
	for _, op := range ops {
		if op == nil || !slices.Contains(ids, op.ID) {
			continue
		}
		switch {
		case op.RemoteApplyID() != remoteID:
			logger.DebugContext(ctx, "member of the pending gRPC cutover request runs on another remote apply; its own drive sends it there",
				"operation_id", op.ID, "operation_remote_apply_id", op.RemoteApplyID(), "remote_apply_id", remoteID)
		case op.ExternalOperationID == "":
			logger.WarnContext(ctx, "member of the pending gRPC cutover request records no remote operation id, so this send cannot name it; its own drive refuses its scoped cutover",
				"operation_id", op.ID, "remote_apply_id", remoteID)
		case !slices.Contains(remoteIDs, op.ExternalOperationID):
			remoteIDs = append(remoteIDs, op.ExternalOperationID)
		}
	}
	slices.Sort(remoteIDs)
	return remoteIDs, nil
}
