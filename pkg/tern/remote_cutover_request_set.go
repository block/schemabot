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
// remote apply. The drive sends only while its own operation waits at the
// cutover and no earlier member holds it. An operation already cutting over
// was reached by a sibling's send of the same set, so it sends nothing.
//
// Every send carries the command's id (remoteCutoverCommandID). A data plane
// that echoes it answers a re-send of a command it already settled with that
// outcome, so the drive re-sends on every tick until the set resolves, and a
// cutover the data plane failed after accepting it fails the command here. A
// data plane that does not echo it records every send as a new command, so
// the drive sends it each command only once.
func (c *GRPCClient) sendRemoteCutoverSetShare(ctx context.Context, apply *storage.Apply, scope applyTaskScope, controlReq *storage.ApplyControlRequest, ids []int64, logger *slog.Logger) error {
	operationID := scope.applyOperationID
	key := newCutoverShareKey(controlReq, operationID)
	sent, alreadySent := c.cutoverSharesTaken.Load(key)
	if alreadySent && sent == remoteCutoverSentOnce {
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
		return c.failRemoteCutoverSetShare(ctx, apply, controlReq, logger, ids, operationID, "", "remote apply id is not available")
	}
	remoteOperationID, err := scope.remoteOperationScope()
	if err != nil {
		return c.failRemoteCutoverSetShare(ctx, apply, controlReq, logger, ids, operationID, remoteID, err.Error())
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
		CommandId:        remoteCutoverCommandID(controlReq),
	}
	if len(remoteOperationIDs) > 1 {
		req.ApplyOperationIds = remoteOperationIDs
	}
	resp, err := c.client.Cutover(ctx, req)
	if err != nil {
		if isAmbiguousRemoteCallError(err) {
			// The data plane records a cutover durably on receipt and answers
			// a re-send of the command with the request it already holds or
			// the outcome it settled, so a call that ended without an answer
			// leaves the request pending for the next drive tick to re-send.
			logger.WarnContext(ctx, "remote cutover call ended without an answer; the cutover request stays pending and the next drive re-sends it",
				"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID, "remote_apply_id", remoteID, "error", err)
			return fmt.Errorf("request remote gRPC cutover for apply %s remote %s: outcome unknown, request left pending: %w", apply.ApplyIdentifier, remoteID, err)
		}
		return c.failRemoteCutoverSetShare(ctx, apply, controlReq, logger, ids, operationID, remoteID, fmt.Sprintf("remote cutover failed: %v", err))
	}
	if resp == nil {
		return c.failRemoteCutoverSetShare(ctx, apply, controlReq, logger, ids, operationID, remoteID, "the data plane returned neither a response nor an error")
	}
	if !resp.Accepted {
		return c.failRemoteCutoverSetShare(ctx, apply, controlReq, logger, ids, operationID, remoteID, controlRefusalMessage(storage.ControlOperationCutover, resp.ErrorMessage))
	}
	if alreadySent {
		logger.DebugContext(ctx, "the remote apply still holds this drive's share of the pending gRPC cutover request; waiting for the rest of the set",
			"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID, "remote_apply_id", remoteID)
		return nil
	}
	if resp.GetCommandId() == req.GetCommandId() {
		c.cutoverSharesTaken.Store(key, remoteCutoverResent)
	} else {
		logger.InfoContext(ctx, "the data plane did not echo the cutover command id, so this drive sends its share of the command only once",
			"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID, "remote_apply_id", remoteID)
		c.cutoverSharesTaken.Store(key, remoteCutoverSentOnce)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered,
		fmt.Sprintf("Remote cutover accepted for apply %s (remote %s)%s", apply.ApplyIdentifier, remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	logger.InfoContext(ctx, "sent this drive's share of the pending gRPC cutover request to its remote apply; the request settles once the whole set has",
		"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID,
		"remote_apply_id", remoteID, "remote_operation_ids", req.GetApplyOperationIds())
	return nil
}

// remoteCutoverShareSend is how a drive sends its share of one cutover
// command (cutoverShareKey) after the data plane accepted it.
type remoteCutoverShareSend int

const (
	// remoteCutoverResent: the data plane echoed the command id, so the drive
	// re-sends on every tick and learns the outcome the data plane settled.
	remoteCutoverResent remoteCutoverShareSend = iota + 1
	// remoteCutoverSentOnce: the data plane did not echo the command id and
	// would record a re-send as a new command, so the drive does not re-send.
	remoteCutoverSentOnce
)

// remoteCutoverCommandID names the operator command a pending cutover request
// carries: the request row and the revision it was armed at, the same on
// every send of the command and ordered after every earlier command on the
// row, so the data plane never takes a delayed send of an earlier command for
// a new one.
func remoteCutoverCommandID(controlReq *storage.ApplyControlRequest) string {
	return storage.NewControlCommandID(controlReq.ID, controlReq.Revision)
}

// failRemoteCutoverSetShare fails a pending cutover request set whose share
// this drive could not send, or the data plane did not take. The whole
// command fails, as it does when the local cutover path refuses one operation
// of a set: the operator sees the refusal, with how many of the set's
// operations cut over, and issues the cutover again. Only the command this
// drive sent is failed; a newer one re-armed on the same row is left to its
// own drive.
func (c *GRPCClient) failRemoteCutoverSetShare(ctx context.Context, apply *storage.Apply, controlReq *storage.ApplyControlRequest, logger *slog.Logger, ids []int64, operationID int64, remoteID, message string) error {
	var remoteIDs []string
	if remoteID != "" {
		remoteIDs = append(remoteIDs, remoteID)
	}
	if err := failCutoverSet(ctx, c.storage, logger, apply, controlReq, ids, operationID, message, remoteIDs...); err != nil {
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
