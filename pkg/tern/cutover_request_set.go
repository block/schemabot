package tern

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A cutover request bound to a set of operations (storage
// CutoverRequestSetMetadata) is one operator command for every member that was
// free to cut over when it was issued. Each member's own drive cuts over its
// own operation, and the request stays pending until no member of the set can
// still cut over on it, so it settles once, with the outcome of the whole set.

// cutoverShareKey names one operation's share of one cutover command. The
// revision tells two commands recorded on the same request row apart.
type cutoverShareKey struct {
	requestID   int64
	revision    int64
	operationID int64
}

func newCutoverShareKey(controlReq *storage.ApplyControlRequest, operationID int64) cutoverShareKey {
	return cutoverShareKey{requestID: controlReq.ID, revision: controlReq.Revision, operationID: operationID}
}

// processPendingCutoverSet runs a drive's part in a pending cutover request
// bound to the operations in ids: the drive cuts over its own operation when
// that operation is in the set, waits at the cutover, has not already taken
// its share, and no earlier member holds it. Every drive of the apply then
// settles the request once the whole set has resolved, so it never waits on a
// drive that no longer exists.
func (c *LocalClient) processPendingCutoverSet(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, controlReq *storage.ApplyControlRequest, ids []int64, logger *slog.Logger) error {
	driveOperationID, err := applyOperationIDForTasks(tasks)
	switch {
	case err != nil:
		logger.Warn("pending cutover request is bound to a set of operations but this drive's tasks do not name a single operation; only settling it",
			"requested_by", controlRequestCaller(controlReq), "cutover_operation_ids", ids, "error", err)
	case !slices.Contains(ids, driveOperationID):
		logger.Debug("pending cutover request does not name this drive's operation; only settling it",
			"requested_by", controlRequestCaller(controlReq), "cutover_operation_ids", ids, "drive_operation_id", driveOperationID)
	default:
		if err := c.takeCutoverSetShare(ctx, apply, tasks, controlReq, driveOperationID, logger); err != nil {
			return err
		}
	}
	return settleCutoverSet(ctx, c.storage, logger, apply, controlReq, ids, nil)
}

// takeCutoverSetShare cuts over the drive's own operation for a cutover
// request set, once.
func (c *LocalClient) takeCutoverSetShare(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, controlReq *storage.ApplyControlRequest, operationID int64, logger *slog.Logger) error {
	key := newCutoverShareKey(controlReq, operationID)
	if _, taken := c.cutoverSharesTaken.Load(key); taken {
		logger.Debug("this drive already cut over its operation for the pending cutover request; waiting for the rest of the set",
			"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID)
		return nil
	}
	if !tasksWaitingForCutover(tasks) {
		logger.Info("pending cutover request is waiting for this drive's operation to wait at the cutover",
			"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID)
		return nil
	}
	store := c.storage.ApplyOperations()
	if store == nil {
		return fmt.Errorf("apply operation store is not configured")
	}
	blocker, err := store.CutoverBlocker(ctx, operationID)
	if err != nil {
		return fmt.Errorf("check the hold on apply_operation %d before taking its share of the pending cutover of apply %s: %w", operationID, apply.ApplyIdentifier, err)
	}
	if blocker != nil {
		logger.Info("this drive's share of the pending cutover request is held by an earlier member, so it waits until that member completes or recovers, or this operation ends",
			"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID,
			"blocking_operation_id", blocker.ID, "blocking_deployment", blocker.Deployment,
			"blocking_target", blocker.Target, "blocking_state", blocker.State)
		return nil
	}
	accepted, err := c.cutOverBoundOperation(ctx, apply, controlReq, operationID, logger)
	if err != nil || !accepted {
		return err
	}
	c.cutoverSharesTaken.Store(key, struct{}{})
	logger.Info("cut over this drive's operation for the pending cutover request; the request settles once the whole set has",
		"requested_by", controlRequestCaller(controlReq), "drive_operation_id", operationID)
	return nil
}

// tasksWaitingForCutover reports whether any of an operation's tasks waits at
// the cutover. A task already cutting over has taken its share.
func tasksWaitingForCutover(tasks []*storage.Task) bool {
	for _, task := range tasks {
		if state.IsState(task.State, state.Task.WaitingForCutover) {
			return true
		}
	}
	return false
}

// settleCutoverSet settles a pending cutover request bound to the operations
// in ids once none of them can still cut over on it: it completes when every
// one cut over, and fails naming how far it got otherwise. notCutOver is nil
// while the apply's drives still run, so a member that has not ended keeps the
// request pending; once the apply has resolved or stopped, no drive is left to
// advance such a member, and notCutOver names why it did not cut over.
func settleCutoverSet(ctx context.Context, store storage.Storage, logger *slog.Logger, apply *storage.Apply, controlReq *storage.ApplyControlRequest, ids []int64, notCutOver func(op *storage.ApplyOperation) string) error {
	opStore := store.ApplyOperations()
	if opStore == nil {
		return fmt.Errorf("settle pending cutover of apply %s: apply operation store is not available", apply.ApplyIdentifier)
	}
	ops, err := opStore.ListByApply(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("list the operations the pending cutover of apply %s is for: %w", apply.ApplyIdentifier, err)
	}
	byID := make(map[int64]*storage.ApplyOperation, len(ops))
	for _, op := range ops {
		byID[op.ID] = op
	}
	cutOver := 0
	var firstReason string
	for _, id := range ids {
		op := byID[id]
		var reason string
		switch {
		case op == nil:
			reason = boundCutoverMissingOperationMessage(id)
		case cutoverRequestResolvedByApplyState(op.State):
			cutOver++
			continue
		case notCutOver != nil:
			reason = notCutOver(op)
		case state.IsState(op.State, state.ApplyOperation.Stopped):
			reason = cutoverSupersededByStopMessage
		case state.IsApplyOperationTerminal(op.State):
			reason = boundCutoverOperationEndedMessage(op.State)
		default:
			logger.Debug("pending cutover request waits for the rest of its set",
				"requested_by", controlRequestCaller(controlReq), "cutover_operation_ids", ids,
				"waiting_operation_id", id, "waiting_operation_state", op.State)
			return nil
		}
		if firstReason == "" {
			firstReason = reason
		}
	}
	if cutOver == len(ids) {
		logger.InfoContext(ctx, "completing pending cutover request: every operation it was sent for cut over",
			append(apply.LogAttrs(), "requested_by", controlRequestCaller(controlReq), "cutover_operation_ids", ids)...)
		return completePendingControlRequests(ctx, store, apply, storage.ControlOperationCutover)
	}
	reason := cutoverSetNotAppliedMessage(cutOver, len(ids), firstReason)
	logger.InfoContext(ctx, "failing pending cutover request: not every operation it was sent for cut over",
		append(apply.LogAttrs(), "requested_by", controlRequestCaller(controlReq), "cutover_operation_ids", ids,
			"cut_over", cutOver, "reason", reason)...)
	return failPendingControlRequests(ctx, store, apply, storage.ControlOperationCutover, reason)
}

// cutoverSetNotAppliedMessage answers a cutover request set that not every
// operation cut over on, with the reason of the first that did not.
func cutoverSetNotAppliedMessage(cutOver, total int, reason string) string {
	return fmt.Sprintf("cutover reached %d of the %d operations it was sent for; %s", cutOver, total, reason)
}
