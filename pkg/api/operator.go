package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// Operator constants.
const (
	// OperatorPollInterval is the default interval for polling applies that need attention.
	OperatorPollInterval = 10 * time.Second

	// HeartbeatTimeout is how long since last heartbeat before
	// an apply is considered to have a crashed worker and needs recovery.
	// FindNextApply uses this (via SQL: updated_at < NOW() - INTERVAL 1 MINUTE).
	HeartbeatTimeout = 1 * time.Minute

	// ApplyOperationHeartbeatInterval bounds how often the operation-row
	// heartbeat fires while ResumeApply runs. It is kept safely below
	// HeartbeatTimeout so that a large (or misconfigured) operator poll
	// interval cannot let apply_operations.updated_at go stale and allow a peer
	// worker to re-claim the operation mid-resume. The effective cadence is
	// min(operatorPollInterval, ApplyOperationHeartbeatInterval).
	ApplyOperationHeartbeatInterval = 10 * time.Second

	// DefaultOperatorWorkers is the number of concurrent operator workers
	// when not configured via operator_workers in the server config.
	DefaultOperatorWorkers = 4

	// ApplyClaimLogTimeout bounds the best-effort apply-log append recording an
	// operator claim, so a slow or hung storage layer cannot delay the resume
	// the claim is about to drive.
	ApplyClaimLogTimeout = 5 * time.Second
)

// StartOperator starts the background operator worker pool.
//
// Operator workers claim apply work from storage so one server can make
// progress across independent databases and environments concurrently. This
// includes queued applies, crash recovery for applies with stale heartbeats,
// and retry recovery for transient engine failures.
//
// Launches N concurrent workers (configured via operator_workers in config).
// Each worker independently claims applies using FOR UPDATE SKIP LOCKED.
// Call StopOperator to gracefully stop.
func (s *Service) StartOperator(ctx context.Context) {
	s.operatorMu.Lock()
	if s.stopRecovery != nil {
		s.operatorMu.Unlock()
		s.logger.Info("operator already running")
		return
	}

	workers := s.config.OperatorWorkers
	if workers <= 0 {
		workers = DefaultOperatorWorkers
	}

	stop := make(chan struct{})
	wake := make(chan struct{}, workers)
	workerCtx, cancel := context.WithCancel(ctx)
	s.stopRecovery = stop
	s.cancelRecovery = cancel
	s.operatorWake = wake
	s.operatorMu.Unlock()

	for i := range workers {
		workerID := i
		s.recoveryWg.Go(func() {
			s.operatorWorker(workerCtx, workerID, stop, wake)
		})
	}

	s.logger.Info("operator started", "workers", workers, "interval", s.operatorPollInterval)
}

// StopOperator stops the background operator and waits for all workers to finish.
// Safe to call multiple times.
func (s *Service) StopOperator() {
	s.operatorMu.Lock()
	if s.stopRecovery == nil {
		s.operatorMu.Unlock()
		return
	}
	stop := s.stopRecovery
	cancel := s.cancelRecovery
	s.stopRecovery = nil
	s.cancelRecovery = nil
	s.operatorWake = nil
	s.operatorMu.Unlock()

	close(stop)
	if cancel != nil {
		cancel()
	}
	s.recoveryWg.Wait()
}

func (s *Service) wakeOperator(applyIdentifier, database, environment string) {
	s.operatorMu.Lock()
	wake := s.operatorWake
	running := s.stopRecovery != nil
	s.operatorMu.Unlock()

	if !running || wake == nil {
		s.logger.Debug("operator wake skipped because operator is not running",
			"apply_id", applyIdentifier,
			"database", database,
			"environment", environment)
		return
	}

	select {
	case wake <- struct{}{}:
		s.logger.Debug("operator wake queued",
			"apply_id", applyIdentifier,
			"database", database,
			"environment", environment)
	default:
		s.logger.Debug("operator wake already pending",
			"apply_id", applyIdentifier,
			"database", database,
			"environment", environment)
	}
}

// operatorWorker is a single worker that claims at most one apply on startup
// and on each operator poll tick. Wake signals share the same claim path as
// polling; storage locking decides whether a worker actually owns work.
func (s *Service) operatorWorker(ctx context.Context, workerID int, stop <-chan struct{}, wake <-chan struct{}) {
	ticker := time.NewTicker(s.operatorPollInterval)
	defer ticker.Stop()

	s.logger.Debug("operator worker started", "worker", workerID)

	s.recoverApplies(ctx, workerID)

	for {
		select {
		case <-stop:
			s.logger.Debug("operator worker stopping", "worker", workerID)
			return
		case <-ctx.Done():
			s.logger.Debug("operator worker context cancelled", "worker", workerID)
			return
		case <-wake:
			s.logger.Debug("operator worker woke for queued apply", "worker", workerID)
			s.recoverApplies(ctx, workerID)
		case <-ticker.C:
			s.recoverApplies(ctx, workerID)
		}
	}
}

// recoverApplies claims and resumes applies that need attention.
// Each call claims one apply (if available) to keep the scheduling loop responsive.
func (s *Service) recoverApplies(ctx context.Context, workerID int) {
	expired, err := s.storage.Applies().ExpireRetryable(ctx)
	if err != nil {
		s.logger.Error("operator: failed to expire retryable applies", "worker", workerID, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "expire_retryable_error")
		return
	}
	for _, expiration := range expired {
		apply := expiration.Apply
		s.logger.Error("operator: retryable apply expired",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"attempt", apply.Attempt,
			"reason", expiration.Reason)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, apply.Deployment, apply.Environment, string(expiration.Reason))
	}

	owner := operatorLeaseOwner(workerID)

	if s.config.OperatorClaimOperations {
		// Service a pending stop with no claimable operation to carry it before
		// claiming new operation work, so a queued stop wins over starting more
		// deployments. When nothing needs stop reconciliation this is a cheap
		// no-op and the worker falls through to the normal operation claim.
		if s.recoverApplyPendingStop(ctx, workerID, owner) {
			return
		}
		s.recoverApplyOperation(ctx, workerID, owner)
		return
	}

	apply, err := s.storage.Applies().FindNextApply(ctx, owner)
	if err != nil {
		s.logger.Error("operator: failed to claim apply", "worker", workerID, "lease_owner", owner, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "storage_error")
		return
	}

	if apply == nil {
		s.logger.Debug("operator: no apply to claim", "worker", workerID)
		return
	}
	lease := apply.Lease()
	if !lease.Valid() {
		s.logger.Error("operator: claimed apply without a valid lease token; operator will not resume it",
			"worker", workerID,
			"lease_owner", owner,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment)
		metrics.RecordOperatorClaimFailure(ctx, "missing_lease_token")
		return
	}
	ctx = storage.WithApplyLease(ctx, lease)
	// Legacy FindNextApply path drives the whole apply (applyOperationID == 0).
	// Failures are handled inside resumeClaimedApply; the whole-apply path has no
	// operation row to terminalize, so the return values are not needed here.
	_, _ = s.resumeClaimedApply(ctx, workerID, apply, 0)
}

// recoverApplyOperation claims work at the apply_operations (per-deployment)
// level: it leases one operation row, acquires the parent apply lease that
// lease-guarded writes require, drives only that operation's tasks through the
// shared resume path while heartbeating the operation row, then marks the
// operation row terminal from the parent apply's final state. Scoping the drive
// to the claimed operation is what lets sibling deployments run concurrently
// once the multi-deployment fan-out lands; while the apply-create dual-write
// emits exactly one operation per apply, the operation-scoped drive resolves to
// the same tasks as the whole apply.
func (s *Service) recoverApplyOperation(ctx context.Context, workerID int, owner string) {
	op, err := s.storage.ApplyOperations().FindNextApplyOperation(ctx, owner)
	if err != nil {
		s.logger.Error("operator: failed to claim apply_operation", "worker", workerID, "lease_owner", owner, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "operation_storage_error")
		return
	}
	if op == nil {
		s.logger.Debug("operator: no apply_operation to claim", "worker", workerID)
		return
	}

	// The claim rotated a fresh operation lease onto the row. It is the
	// capability that guards this operation's own writes — its state
	// transitions, heartbeat, and task updates — so fail closed if it is
	// missing rather than silently degrading to the parent apply lease.
	opLease := op.Lease()
	if !opLease.Valid() {
		s.logger.Error("operator: claimed apply_operation without a valid operation lease token; operation will not be driven",
			"worker", workerID,
			"lease_owner", owner,
			"apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID,
			"deployment", op.Deployment)
		metrics.RecordOperatorClaimFailure(ctx, "missing_operation_lease_token")
		return
	}

	// The engine drive still writes the parent applies row (state RUNNING /
	// COMPLETED / FAILED), and the derived-state reconcile updates
	// applies.state, so the worker must also hold the parent apply lease — the
	// operation lease alone does not authorize parent-apply writes.
	apply, err := s.storage.Applies().ClaimApplyByID(ctx, op.ApplyID, owner)
	if err != nil {
		s.logger.Error("operator: failed to claim parent apply for operation",
			"worker", workerID,
			"lease_owner", owner,
			"apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID,
			"deployment", op.Deployment,
			"error", err)
		metrics.RecordOperatorClaimFailure(ctx, "operation_parent_claim_error")
		return
	}
	if apply == nil {
		// The parent apply is not currently claimable. Distinguish the two
		// reasons, because they need opposite handling:
		//   - terminal parent: the operation row was just leased by
		//     FindNextApplyOperation, so leaving it non-terminal would re-claim
		//     it forever. Reconcile it to the parent's terminal state now.
		//   - transiently unclaimable (a peer holds a fresh lease, or the row is
		//     locked): fail closed and let a later poll retry once it goes stale.
		s.reconcileUnclaimableParent(ctx, workerID, op)
		return
	}

	applyLease := apply.Lease()
	if !applyLease.Valid() {
		s.logger.Error("operator: claimed parent apply without a valid lease token; operation will not be driven",
			"worker", workerID,
			"lease_owner", owner,
			"apply_operation_id", op.ID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", op.Deployment,
			"environment", apply.Environment)
		metrics.RecordOperatorClaimFailure(ctx, "missing_lease_token")
		return
	}

	// Two capabilities, two scopes:
	//   - applyLeaseCtx guards parent applies writes — the engine's state
	//     transitions and the derived-state reconcile.
	//   - operationLeaseCtx guards this operation's own row and its tasks
	//     (operation state, heartbeat, task updates); the storage lease
	//     precedence prefers the operation token, so sibling operations no
	//     longer serialize on the shared parent token.
	//   - dualLeaseCtx carries both for the engine run, which writes both the
	//     operation's tasks and the parent applies row.
	applyLeaseCtx := storage.WithApplyLease(ctx, applyLease)
	operationLeaseCtx := storage.WithOperationLease(ctx, opLease)
	dualLeaseCtx := storage.WithOperationLease(applyLeaseCtx, opLease)

	// The claimed operation row's deployment is the authoritative routing key
	// for the drive, but resumeClaimedApply selects the Tern client from the
	// parent apply's stored deployment. These are identical while one operation
	// maps to one apply; if they ever diverge the worker would drive the wrong
	// deployment, so fail closed rather than route to the parent's deployment.
	if op.Deployment != apply.Deployment {
		s.logger.Error("operator: claimed operation deployment does not match parent apply deployment; operation will not be driven",
			"worker", workerID,
			"lease_owner", owner,
			"apply_operation_id", op.ID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"operation_deployment", op.Deployment,
			"apply_deployment", apply.Deployment,
			"environment", apply.Environment)
		metrics.RecordOperatorClaimFailure(ctx, "deployment_mismatch")
		return
	}

	// Heartbeat the operation row on the apply heartbeat cadence so a peer
	// worker does not re-claim it during a long drive. The heartbeat writes
	// under the operation lease, so a lost operation lease cancels the run and
	// the displaced worker stops writing.
	runCtx, cancelRun := context.WithCancel(dualLeaseCtx)
	defer cancelRun()
	stopHeartbeat := s.startApplyOperationHeartbeat(runCtx, workerID, op, apply, cancelRun)
	defer stopHeartbeat()

	resumed, resumeErr := s.resumeClaimedApply(runCtx, workerID, apply, op.ID)
	stopHeartbeat()
	if !resumed {
		if errors.Is(resumeErr, tern.ErrNoTasksForApplyOperation) {
			// The drive failed closed: the operation has no tasks, so it can
			// never make progress. Terminalize it now rather than leaving it to
			// be re-leased on every poll once its heartbeat goes stale.
			s.failOperationWithoutTasks(operationLeaseCtx, applyLeaseCtx, workerID, op, apply)
		}
		return
	}

	// Persist the operation row from its OWN drive outcome — the aggregate of
	// this operation's tasks — rather than mirroring the parent apply down.
	// Under on_failure "continue" the parent applies.state can be held running
	// (the policy-aware projection waits for siblings to settle) while this
	// operation has terminally failed; mirroring the parent down would leave the
	// failed operation claimable and re-leased on every poll, so its failure
	// would never be durably recorded. The operation row is authoritative for
	// its own deployment; the parent state is derived from the operation rows
	// afterward via updateApplyStateFromOperations.
	marked, err := s.markOperationFromOwnResult(operationLeaseCtx, workerID, op)
	if err != nil {
		s.logger.Error("operator: failed to update apply_operation from its tasks; derived apply state not updated",
			"worker", workerID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", apply.Environment, "error", err)
		return
	}
	if !marked {
		return
	}

	// If a stop is pending, terminalize any still-pending sibling operations to
	// stopped before deriving the parent. The claim gate keeps those siblings
	// from ever starting, so under on_failure "continue" a failed sibling would
	// otherwise hold the projection running with pending siblings that never
	// settle — stranding the apply. Stopping them lets the derivation below reach
	// a terminal verdict so the rollout (and the stop request) can resolve.
	if err := s.stopPendingOperationsForPendingStop(applyLeaseCtx, workerID, apply); err != nil {
		s.logger.Error("operator: failed to stop pending sibling operations for pending stop request; derived apply state not updated",
			"worker", workerID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", apply.Environment, "error", err)
		return
	}

	// Reload the parent apply so updateApplyStateFromOperations below re-derives
	// the parent from its children against the durable apply.State (its
	// terminal-to-non-terminal guard), not the in-memory object the resume path
	// started from. The reloaded row is only the target of the re-derivation;
	// the operation row was already persisted from its own tasks above.
	finalApply, err := s.storage.Applies().Get(applyLeaseCtx, apply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload parent apply after resume; derived apply state not updated",
			"worker", workerID,
			"apply_operation_id", op.ID,
			"apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment,
			"error", err)
		return
	}
	if finalApply == nil {
		s.logger.Error("operator: parent apply not found after resume; derived apply state not updated",
			"worker", workerID,
			"apply_operation_id", op.ID,
			"apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment)
		return
	}

	if err := s.updateApplyStateFromOperations(applyLeaseCtx, workerID, finalApply, allowLeaseScopedFailedReopen); err != nil {
		s.logger.Error("operator: failed to update derived apply state from apply_operations",
			"worker", workerID, "apply_operation_id", op.ID, "apply_id", finalApply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", finalApply.Environment, "error", err)
		return
	}

	// If the derived state above settled the apply terminally and a stop is still
	// pending, complete it now so the request does not linger after the rollout
	// has stopped.
	if err := s.completePendingStopIfApplyResolved(applyLeaseCtx, workerID, finalApply.ID); err != nil {
		s.logger.Error("operator: failed to complete pending stop request for resolved apply",
			"worker", workerID, "apply_operation_id", op.ID, "apply_id", finalApply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", finalApply.Environment, "error", err)
		return
	}
}

// recoverApplyPendingStop drives stop reconciliation for an apply that has a
// pending stop request but no claimable operation to carry it. Under on_failure
// "continue" a failed earlier sibling can leave an apply with only terminal and
// pending operations; the claim gate keeps the pending ones from starting, so
// the normal operation-claim path never visits the apply and its stop would
// strand forever. This path claims such an apply directly, stops its pending
// siblings, re-derives the parent, and completes the stop once the apply is
// terminal.
//
// Returns true when this tick is consumed by stop reconciliation — an apply was
// claimed (whether the reconciliation that followed succeeded or hit an error)
// or the claim itself errored or returned an invalid lease — so the caller does
// not also run the normal operation claim this tick. Returns false only when no
// apply needed reconciliation.
func (s *Service) recoverApplyPendingStop(ctx context.Context, workerID int, owner string) bool {
	apply, err := s.storage.Applies().FindNextApplyForStopReconciliation(ctx, owner)
	if err != nil {
		s.logger.Error("operator: failed to claim apply for stop reconciliation",
			"worker", workerID, "lease_owner", owner, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "stop_reconciliation_claim_error")
		return true
	}
	if apply == nil {
		s.logger.Debug("operator: no apply needs stop reconciliation", "worker", workerID)
		return false
	}

	lease := apply.Lease()
	if !lease.Valid() {
		s.logger.Error("operator: claimed apply for stop reconciliation without a valid lease token; skipping",
			"worker", workerID, "lease_owner", owner,
			"apply_id", apply.ApplyIdentifier, "database", apply.Database, "environment", apply.Environment)
		metrics.RecordOperatorClaimFailure(ctx, "stop_reconciliation_missing_lease_token")
		return true
	}
	applyLeaseCtx := storage.WithApplyLease(ctx, lease)

	if err := s.stopPendingOperationsForPendingStop(applyLeaseCtx, workerID, apply); err != nil {
		s.logger.Error("operator: failed to stop pending sibling operations during stop reconciliation",
			"worker", workerID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment, "error", err)
		return true
	}

	finalApply, err := s.storage.Applies().Get(applyLeaseCtx, apply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload apply during stop reconciliation; derived apply state not updated",
			"worker", workerID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment, "error", err)
		return true
	}
	if finalApply == nil {
		s.logger.Error("operator: apply not found during stop reconciliation; derived apply state not updated",
			"worker", workerID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment)
		return true
	}

	if err := s.updateApplyStateFromOperations(applyLeaseCtx, workerID, finalApply, allowLeaseScopedFailedReopen); err != nil {
		s.logger.Error("operator: failed to update derived apply state during stop reconciliation",
			"worker", workerID, "apply_id", finalApply.ApplyIdentifier,
			"database", finalApply.Database, "environment", finalApply.Environment, "error", err)
		return true
	}

	if err := s.completePendingStopIfApplyResolved(applyLeaseCtx, workerID, finalApply.ID); err != nil {
		s.logger.Error("operator: failed to complete pending stop request after stop reconciliation",
			"worker", workerID, "apply_id", finalApply.ApplyIdentifier,
			"database", finalApply.Database, "environment", finalApply.Environment, "error", err)
		return true
	}
	return true
}

// stopPendingOperationsForPendingStop terminalizes still-pending sibling
// operations to stopped when the apply has a pending stop request, so the
// rollout can settle instead of stranding running with siblings the claim gate
// keeps from ever starting. No-op when no stop is pending.
func (s *Service) stopPendingOperationsForPendingStop(ctx context.Context, workerID int, apply *storage.Apply) error {
	controlReq, err := s.storage.ControlRequests().GetPending(ctx, apply.ID, storage.ControlOperationStop)
	if err != nil {
		return fmt.Errorf("load pending stop request for apply %s (%d): %w", apply.ApplyIdentifier, apply.ID, err)
	}
	if controlReq == nil {
		return nil
	}

	stopped, err := s.storage.ApplyOperations().MarkPendingStoppedByApply(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("stop pending apply_operations for apply %s (%d): %w", apply.ApplyIdentifier, apply.ID, err)
	}
	if stopped > 0 {
		s.logger.Info("operator: stopped pending sibling operations for pending stop request",
			"worker", workerID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment,
			"stopped_operation_count", stopped)
	}
	return nil
}

// completePendingStopIfApplyResolved completes a pending stop control request
// once the apply has settled to a terminal state, so the request does not stay
// pending forever after the rollout stops. The apply is reloaded because the
// derived-state write operates on a copy and does not mutate the caller's row.
// No-op when the apply is still non-terminal or no stop is pending.
func (s *Service) completePendingStopIfApplyResolved(ctx context.Context, workerID int, applyID int64) error {
	apply, err := s.storage.Applies().Get(ctx, applyID)
	if err != nil {
		return fmt.Errorf("reload apply %d before completing pending stop: %w", applyID, err)
	}
	if apply == nil {
		return fmt.Errorf("reload apply %d before completing pending stop: %w", applyID, storage.ErrApplyNotFound)
	}
	if !state.IsTerminalApplyState(apply.State) {
		return nil
	}

	controlReq, err := s.storage.ControlRequests().GetPending(ctx, apply.ID, storage.ControlOperationStop)
	if err != nil {
		return fmt.Errorf("load pending stop request for resolved apply %s (%d): %w", apply.ApplyIdentifier, apply.ID, err)
	}
	if controlReq == nil {
		return nil
	}

	if err := s.storage.ControlRequests().CompletePending(ctx, apply.ID, storage.ControlOperationStop); err != nil {
		return fmt.Errorf("complete pending stop request for resolved apply %s (%d): %w", apply.ApplyIdentifier, apply.ID, err)
	}
	s.logger.Info("operator: completed pending stop request for resolved apply",
		"worker", workerID, "apply_id", apply.ApplyIdentifier,
		"database", apply.Database, "environment", apply.Environment, "state", apply.State)
	return nil
}

// reconcileUnclaimableParent handles a claimed operation whose parent apply
// ClaimApplyByID refused. If the parent is terminal, the operation row is
// reconciled to that terminal state so it stops being re-claimed on every poll
// (the write is unguarded — the operation holds no apply lease — but a terminal
// apply has no competing driver, so the mirror is safe and idempotent). If the
// parent is non-terminal (a peer holds a fresh lease, or the row was locked),
// the operation is left claimable and retried once the parent lease goes stale.
func (s *Service) reconcileUnclaimableParent(ctx context.Context, workerID int, op *storage.ApplyOperation) {
	parent, err := s.storage.Applies().Get(ctx, op.ApplyID)
	if err != nil {
		s.logger.Error("operator: failed to load unclaimable parent apply; operation will be retried",
			"worker", workerID,
			"apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID,
			"deployment", op.Deployment,
			"error", err)
		metrics.RecordOperatorClaimFailure(ctx, "operation_parent_not_claimable")
		return
	}
	if parent == nil {
		s.logger.Error("operator: parent apply not found for claimed operation; operation will be retried",
			"worker", workerID,
			"apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID,
			"deployment", op.Deployment)
		metrics.RecordOperatorClaimFailure(ctx, "operation_parent_not_claimable")
		return
	}
	if state.IsTerminalApplyState(parent.State) {
		s.logger.Info("operator: parent apply already terminal; reconciling operation to terminal state",
			"worker", workerID,
			"apply_operation_id", op.ID,
			"apply_id", parent.ApplyIdentifier,
			"deployment", op.Deployment,
			"environment", parent.Environment,
			"state", parent.State)
		marked, err := s.markOperationFromApplyState(ctx, workerID, op, parent)
		if err != nil {
			s.logger.Error("operator: failed to reconcile apply_operation from terminal parent; derived apply state not updated",
				"worker", workerID, "apply_operation_id", op.ID, "apply_id", parent.ApplyIdentifier,
				"deployment", op.Deployment, "environment", parent.Environment, "state", parent.State, "error", err)
			return
		}
		if !marked {
			return
		}
		if err := s.updateApplyStateFromOperations(ctx, workerID, parent, rejectFailedApplyReopen); err != nil {
			s.logger.Error("operator: failed to update derived apply state for terminal parent",
				"worker", workerID, "apply_operation_id", op.ID, "apply_id", parent.ApplyIdentifier,
				"deployment", op.Deployment, "environment", parent.Environment, "error", err)
			return
		}
		return
	}
	s.logger.Warn("operator: parent apply not claimable for operation; operation will be retried",
		"worker", workerID,
		"apply_operation_id", op.ID,
		"apply_id", parent.ApplyIdentifier,
		"deployment", op.Deployment,
		"environment", parent.Environment,
		"state", parent.State)
	metrics.RecordOperatorClaimFailure(ctx, "operation_parent_not_claimable")
}

// failOperationWithoutTasks terminalizes an operation whose drive failed closed
// because no tasks scope to it. Such a claim is invalid or stale: the operation
// can never make progress, so leaving the row non-terminal would re-lease it on
// every poll once its heartbeat goes stale. It marks the operation row failed
// under its own operation lease (opCtx), then re-derives the parent applies.state
// under the parent apply lease (applyCtx). The two writes target different rows
// with different guards, so they take separate lease-scoped contexts and fail
// closed if ownership has since changed.
func (s *Service) failOperationWithoutTasks(opCtx, applyCtx context.Context, workerID int, op *storage.ApplyOperation, apply *storage.Apply) {
	const reason = "operation has no tasks; invalid or stale claim"
	if err := s.storage.ApplyOperations().MarkFailed(opCtx, op.ID, reason); err != nil {
		s.logger.Error("operator: failed to mark task-less apply_operation failed; operation will be retried",
			"worker", workerID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", apply.Environment, "error", err)
		return
	}
	if err := s.updateApplyStateFromOperations(applyCtx, workerID, apply, allowLeaseScopedFailedReopen); err != nil {
		s.logger.Error("operator: failed to update derived apply state after failing task-less operation",
			"worker", workerID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", apply.Environment, "error", err)
		return
	}
}

// resumeClaimedApply drives claimed work through the engine. When
// applyOperationID is set (the operation-claim path) it drives only that
// deployment's tasks via ResumeApplyOperation with both the operation lease (for
// the operation's tasks) and the parent apply lease (for the applies.state the
// engine still writes) attached to ctx, so sibling deployments are unaffected;
// when it is 0 (the legacy whole-apply path) it drives every task of the apply
// via ResumeApply with only the apply lease attached. Returns true when the work
// resumed without error. Failures are logged and recorded as metrics internally;
// the bool lets the operation-level claim loop decide whether to mark its
// operation terminal, and the returned error lets it distinguish the fail-closed
// no-tasks case (tern.ErrNoTasksForApplyOperation) from transient failures.
func (s *Service) resumeClaimedApply(ctx context.Context, workerID int, apply *storage.Apply, applyOperationID int64) (bool, error) {
	lease := apply.Lease()
	start := s.clock.Now()
	s.logger.Info("operator: claimed apply",
		"worker", workerID,
		"lease_owner", lease.Owner,
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"deployment", apply.Deployment,
		"environment", apply.Environment,
		"state", apply.State,
		"last_heartbeat", apply.UpdatedAt)

	// Record the claim in the apply's durable log so the timeline explains
	// why new state transitions appear after a failure or a worker crash —
	// without this entry, an operator reading apply_logs sees a gap between
	// the last failure and the resumed work.
	s.logApplyResumeClaim(ctx, workerID, apply)

	previousState := apply.State

	deployment, err := storedDeploymentForApply(apply)
	if err != nil {
		s.logger.Error("operator: claimed apply is missing stored deployment metadata",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, "", apply.Environment, "missing_deployment")
		return false, err
	}
	client, err := s.RoutingTernClient()
	if err != nil {
		s.logger.Error("operator: failed to get routing client",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", deployment,
			"environment", apply.Environment,
			"error", err)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, deployment, apply.Environment, "no_client")
		return false, err
	}

	if s.OnApplyRecovered != nil {
		s.OnApplyRecovered(apply)
	}

	retryableClaim := previousState == state.Apply.FailedRetryable
	if retryableClaim {
		metrics.AdjustActiveApplies(ctx, 1, apply.Database, deployment, apply.Environment)
	}
	// The operation-claim path scopes the drive to the single deployment it
	// leased so sibling deployments are unaffected; ResumeApplyOperation fails
	// closed when no tasks scope to the operation. The legacy whole-apply path
	// (applyOperationID == 0) drives every task of the apply.
	if applyOperationID > 0 {
		err = client.ResumeApplyOperation(ctx, apply, applyOperationID)
	} else {
		err = client.ResumeApply(ctx, apply)
	}
	if err != nil {
		if errors.Is(err, storage.ErrApplyLeaseLost) {
			s.logger.Warn("operator: apply lease was lost; worker will stop writing this apply",
				"worker", workerID,
				"lease_owner", lease.Owner,
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"deployment", deployment,
				"environment", apply.Environment,
				"error", err)
			metrics.RecordOperatorResumeFailure(ctx, apply.Database, deployment, apply.Environment, "lease_lost")
			if retryableClaim {
				metrics.AdjustActiveApplies(ctx, -1, apply.Database, deployment, apply.Environment)
			}
			return false, err
		}
		if errors.Is(err, tern.ErrNoTasksForApplyOperation) {
			// Fail-closed: no tasks scope to the operation, so it is an invalid
			// or stale claim that can never make progress. The drive mutated
			// nothing; the caller terminalizes the operation row so it is not
			// re-leased on every poll once its heartbeat goes stale.
			s.logger.Error("operator: claimed operation has no tasks; failing it closed",
				"worker", workerID,
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"deployment", deployment,
				"environment", apply.Environment,
				"apply_operation_id", applyOperationID,
				"error", err)
			metrics.RecordOperatorResumeFailure(ctx, apply.Database, deployment, apply.Environment, "operation_no_tasks")
			if retryableClaim {
				metrics.AdjustActiveApplies(ctx, -1, apply.Database, deployment, apply.Environment)
			}
			return false, err
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			s.logger.Debug("operator: stopped while running claimed apply",
				"worker", workerID,
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"deployment", deployment,
				"environment", apply.Environment,
				"error", err)
			if retryableClaim {
				metrics.AdjustActiveApplies(ctx, -1, apply.Database, deployment, apply.Environment)
			}
			return false, err
		}
		s.logger.Error("operator: failed to resume apply",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", deployment,
			"environment", apply.Environment,
			"error", err)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, deployment, apply.Environment, "resume_error")
		if retryableClaim {
			metrics.AdjustActiveApplies(ctx, -1, apply.Database, deployment, apply.Environment)
		}
		return false, err
	}

	duration := s.clock.Now().Sub(start)
	s.logger.Info("operator: resumed apply",
		"worker", workerID,
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"deployment", deployment,
		"environment", apply.Environment,
		"previous_state", previousState,
		"duration", duration)
	metrics.RecordOperatorResume(ctx, apply.Database, deployment, apply.Environment, previousState)
	metrics.RecordOperatorClaimDuration(ctx, duration, apply.Database, deployment, apply.Environment, previousState)
	return true, nil
}

// logApplyResumeClaim appends a durable apply log entry recording that an
// operator worker claimed the apply to resume it. Best-effort: a failed
// append must not block the resume, so the error is logged and the claim
// proceeds.
func (s *Service) logApplyResumeClaim(ctx context.Context, workerID int, apply *storage.Apply) {
	logStore := s.storage.ApplyLogs()
	if logStore == nil {
		s.logger.Warn("operator: no apply log store configured; apply claim will not appear in apply logs",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier)
		return
	}
	logCtx, cancel := context.WithTimeout(ctx, ApplyClaimLogTimeout)
	defer cancel()
	if err := logStore.Append(logCtx, &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelInfo,
		EventType: storage.LogEventInfo,
		Source:    storage.LogSourceSchemaBot,
		Message:   fmt.Sprintf("Operator claimed apply to resume it (worker %d, state %s)", workerID, apply.State),
		OldState:  apply.State,
		NewState:  apply.State,
		CreatedAt: s.clock.Now(),
	}); err != nil {
		s.logger.Warn("operator: failed to log apply claim; apply claim will not appear in apply logs",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
	}
}

// startApplyOperationHeartbeat refreshes the claimed operation row's lease while
// ResumeApply runs, at min(operatorPollInterval, ApplyOperationHeartbeatInterval)
// so the row cannot go stale and be re-claimed by a peer even when the poll
// interval is large. The heartbeat writes under the operation lease, so a lost
// operation lease cancels the run and the displaced worker stops; other
// heartbeat errors are logged and retried on the next tick.
// Returns a stop func that is safe to call more than once.
func (s *Service) startApplyOperationHeartbeat(ctx context.Context, workerID int, op *storage.ApplyOperation, apply *storage.Apply, cancelRun context.CancelFunc) func() {
	hbCtx, stop := context.WithCancel(ctx)
	interval := min(s.operatorPollInterval, ApplyOperationHeartbeatInterval)
	s.recoveryWg.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if err := s.storage.ApplyOperations().Heartbeat(hbCtx, op.ID); err != nil {
					if errors.Is(err, storage.ErrApplyLeaseLost) {
						s.logger.Warn("operator: apply_operation heartbeat lost operation lease; worker will stop",
							"worker", workerID,
							"apply_operation_id", op.ID,
							"apply_id", apply.ApplyIdentifier,
							"database", apply.Database,
							"deployment", op.Deployment,
							"environment", apply.Environment,
							"error", err)
						cancelRun()
						return
					}
					s.logger.Warn("operator: apply_operation heartbeat failed; will retry",
						"worker", workerID,
						"apply_operation_id", op.ID,
						"apply_id", apply.ApplyIdentifier,
						"database", apply.Database,
						"deployment", op.Deployment,
						"environment", apply.Environment,
						"error", err)
				}
			}
		}
	})
	return stop
}

// markOperationFromApplyState transitions the claimed operation row to mirror
// the parent apply's final state. It is used by the unclaimable-parent
// reconciliation path, where an already-terminal parent is authoritative for its
// single operation. The drive path instead uses markOperationFromOwnResult so a
// failed operation is recorded even while the parent projection holds the apply
// running under on_failure "continue". Both delegate to persistOperationState,
// which documents the updated/error contract.
func (s *Service) markOperationFromApplyState(ctx context.Context, workerID int, op *storage.ApplyOperation, apply *storage.Apply) (updated bool, err error) {
	return s.persistOperationState(ctx, workerID, op, apply.State, apply.ErrorMessage)
}

// markOperationFromOwnResult transitions the claimed operation row to reflect
// the operation's OWN drive result, derived from its tasks via
// state.DeriveApplyState rather than mirrored from the parent apply.
//
// This is the drive-path counterpart to markOperationFromApplyState. Under the
// on_failure "continue" projection updateApplyStateFromOperations holds the
// parent apply running while sibling deployments are still in flight, so
// mirroring this operation from the parent would hit the non-terminal
// "leave claimable" branch and never persist the operation's own terminal
// outcome: a failed deployment would be silently re-claimed and the
// deployment-order gate (which keys off an earlier sibling's failed state under
// continue) would read a stale value. Deriving from the operation's own tasks
// records its real result independently of the parent projection, which
// updateApplyStateFromOperations then aggregates back into the parent apply.
//
// The returned updated flag and error carry the same contract as
// markOperationFromApplyState: updated=true when the row was durably written
// (including the resumable stopped / failed_retryable states), updated=false
// with a nil error when the operation's tasks derive a non-terminal state and
// the row is left claimable for a later poll, and a non-nil error when a read or
// write fails so the caller skips parent derivation.
func (s *Service) markOperationFromOwnResult(ctx context.Context, workerID int, op *storage.ApplyOperation) (updated bool, err error) {
	tasks, err := s.storage.Tasks().GetByApplyOperationID(ctx, op.ID)
	if err != nil {
		return false, fmt.Errorf("load tasks for apply_operation %d (deployment %q): %w", op.ID, op.Deployment, err)
	}
	taskStates := make([]string, len(tasks))
	for i, t := range tasks {
		taskStates[i] = t.State
	}
	derived := state.DeriveApplyState(taskStates)
	return s.persistOperationState(ctx, workerID, op, derived, firstFailedTaskError(tasks))
}

// firstFailedTaskError returns the ErrorMessage of the first failed task, used
// to populate the operation row's failure reason when its own tasks derive a
// failed state. Empty when no failed task carries a message.
func firstFailedTaskError(tasks []*storage.Task) string {
	for _, t := range tasks {
		if state.IsState(t.State, state.Task.Failed) && t.ErrorMessage != "" {
			return t.ErrorMessage
		}
	}
	return ""
}

// firstFailedOperationMessage returns a deployment-qualified failure reason from
// the first failed operation row that carries one. It surfaces the parent
// apply's ErrorMessage from the aggregate when the rollout settles to failed,
// rather than leaving whatever message the last-driven (possibly successful)
// operation wrote. The rollout's failure verdict is the first failure, so the
// first failed row in deployment order wins. Empty when no failed operation
// carries a message, so the caller keeps the existing apply message as fallback.
func firstFailedOperationMessage(ops []*storage.ApplyOperation) string {
	for _, op := range ops {
		if state.IsState(op.State, state.ApplyOperation.Failed) && op.ErrorMessage != "" {
			return fmt.Sprintf("deployment %s failed: %s", op.Deployment, op.ErrorMessage)
		}
	}
	return ""
}

// persistOperationState writes the claimed operation row to reflect a derived
// state, mapping each state to the appropriate row-write. The derived state and
// errorMessage come from either the parent apply (markOperationFromApplyState,
// the reconcile path) or the operation's own tasks (markOperationFromOwnResult,
// the drive path); the row-write mapping is identical regardless of source.
//
// It returns updated=true whenever the operation row was durably written —
// including resumable states (stopped, failed_retryable), not only terminal
// ones. updated=true is the signal the caller needs before deriving the parent
// apply's state from its children: the child row now reflects its outcome, so
// the derived state is current. A non-terminal derived state leaves the
// operation claimable (updated=false, nil error) so a later poll re-leases and
// resumes it; a write failure returns the error so the caller skips derivation
// rather than aggregating a stale child state.
func (s *Service) persistOperationState(ctx context.Context, workerID int, op *storage.ApplyOperation, derived, errorMessage string) (updated bool, err error) {
	opStore := s.storage.ApplyOperations()
	switch {
	case state.IsState(derived, state.Apply.Completed):
		if err := opStore.MarkCompleted(ctx, op.ID); err != nil {
			return false, fmt.Errorf("mark apply_operation %d completed (deployment %q): %w", op.ID, op.Deployment, err)
		}
		return true, nil
	case state.IsState(derived, state.Apply.Failed):
		if err := opStore.MarkFailed(ctx, op.ID, errorMessage); err != nil {
			return false, fmt.Errorf("mark apply_operation %d failed (deployment %q): %w", op.ID, op.Deployment, err)
		}
		return true, nil
	case state.IsState(derived, state.Apply.Stopped):
		// stopped is resumable, so mirror the state but leave completed_at nil
		// (matching the apply-level convention) — stopped work may resume.
		if err := opStore.UpdateState(ctx, op.ID, derived); err != nil {
			return false, fmt.Errorf("update stopped apply_operation %d state (deployment %q): %w", op.ID, op.Deployment, err)
		}
		return true, nil
	case state.IsState(derived, state.Apply.FailedRetryable):
		// failed_retryable is resumable like stopped: mirror the state (leaving
		// completed_at nil) so FindNextApplyOperation reclaims it under the
		// parent apply's recovery budget. Leaving the row in its active state
		// would instead make recovery depend on the stale-heartbeat path, which
		// has no budget and would re-claim it forever once retries are exhausted.
		if err := opStore.UpdateState(ctx, op.ID, derived); err != nil {
			return false, fmt.Errorf("update failed_retryable apply_operation %d state (deployment %q): %w", op.ID, op.Deployment, err)
		}
		return true, nil
	case state.IsTerminalApplyState(derived):
		// cancelled / reverted — non-resumable terminal states; stamp completed_at.
		if err := opStore.MarkTerminal(ctx, op.ID, derived); err != nil {
			return false, fmt.Errorf("mark terminal apply_operation %d state %q (deployment %q): %w", op.ID, derived, op.Deployment, err)
		}
		return true, nil
	default:
		s.logger.Debug("operator: derived operation state not terminal; leaving operation claimable",
			"worker", workerID, "apply_operation_id", op.ID,
			"deployment", op.Deployment, "state", derived)
		return false, nil
	}
}

// failedApplyReopenPolicy controls whether updateApplyStateFromOperations may
// reopen a terminal-failed parent apply back to running when the rollout
// projection legitimately holds it active under on_failure "continue".
//
// The reopen write is only safe when the caller holds the parent apply lease:
// reviving a failed parent through an unscoped, last-write-wins Applies().Update
// could clobber a concurrent driver. So the lease-scoped drive paths opt in and
// the unscoped terminal-parent reconciliation path opts out (it stays fail
// closed, preserving its original invariant that a terminal parent is never
// revived without a competing-driver guard).
type failedApplyReopenPolicy bool

const (
	// rejectFailedApplyReopen keeps the terminal-to-non-terminal guard fully
	// closed: a terminal parent (including failed) is never revived. Used by the
	// unscoped reconcileUnclaimableParent path, which holds no parent lease.
	rejectFailedApplyReopen failedApplyReopenPolicy = false
	// allowLeaseScopedFailedReopen permits a failed parent to reopen to running
	// when the continue projection holds it active. Used only by callers that
	// pass a lease-scoped context, so the write fails closed after ownership
	// changes.
	allowLeaseScopedFailedReopen failedApplyReopenPolicy = true
)

// updateApplyStateFromOperations re-derives applies.state from the apply's child
// apply_operations rows and persists it when it differs from the current value.
//
// This is the inverse of markOperationFromApplyState: the operator drives each
// operation row to its state, then the parent apply's state follows from the
// aggregate via state.DeriveRolloutApplyState, the policy-aware projection over
// all operation rows. Under on_failure "continue" a terminal-failed sibling no
// longer terminalizes the apply while other siblings are still in flight; the
// apply is held running until the rollout settles, then takes the failed verdict.
// Every other policy (halt, pause, unrecognized) fails closed to the failed
// verdict. While an apply has exactly one operation the derived value equals the
// value ResumeApply already persisted, so this is a no-op until the
// multi-deployment fan-out makes an apply own more than one operation.
//
// The caller is responsible for lease scoping: the active operator path passes a
// lease-scoped context so the write fails closed after ownership changes; the
// terminal-parent reconciliation path passes an unscoped context. The reopen
// parameter encodes the matching authority — a terminal parent may only be
// reopened (failed → running, for the continue hold-active projection) by a
// caller that holds the parent lease (allowLeaseScopedFailedReopen). The
// unscoped reconciliation path passes rejectFailedApplyReopen so it never
// revives a terminal parent through a last-write-wins update; every other
// terminal-to-non-terminal transition stays an error regardless.
func (s *Service) updateApplyStateFromOperations(ctx context.Context, workerID int, apply *storage.Apply, reopen failedApplyReopenPolicy) error {
	ops, err := s.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("list apply_operations for apply %s (%d): %w", apply.ApplyIdentifier, apply.ID, err)
	}
	if len(ops) == 0 {
		return fmt.Errorf("derive apply state for apply %s (%d): no apply_operations rows", apply.ApplyIdentifier, apply.ID)
	}

	childStates := make([]string, len(ops))
	children := make([]state.RolloutChild, len(ops))
	for i, op := range ops {
		childStates[i] = op.State
		children[i] = state.RolloutChild{
			State:             op.State,
			ContinueOnFailure: op.OnFailure == storage.OnFailureContinue,
		}
	}
	base := state.DeriveApplyState(childStates)
	derived := state.DeriveRolloutApplyState(children)

	// A failed parent is the one terminal state the continue projection can
	// legitimately reopen: a continuable sibling failure may have terminalized
	// the parent before the rollout settled, and re-deriving over the operation
	// rows holds it running until every sibling is terminal. Gate the exception
	// narrowly — the parent must be failed, the child base must still be failed
	// (a real continuable failure, not a stale parent over non-failed children),
	// the derived projection must be running, and the caller must hold the lease.
	reopensContinuableFailedRollout := bool(reopen) &&
		state.IsState(apply.State, state.Apply.Failed) &&
		state.IsState(base, state.Apply.Failed) &&
		state.IsState(derived, state.Apply.Running)

	if state.IsTerminalApplyState(apply.State) && !state.IsTerminalApplyState(derived) && !reopensContinuableFailedRollout {
		return fmt.Errorf("derive apply state for terminal apply %s (%d): child operations derive non-terminal state %q from parent state %q",
			apply.ApplyIdentifier, apply.ID, derived, apply.State)
	}

	if state.IsState(apply.State, derived) {
		s.logger.Debug("operator: derived apply state matches current; no update",
			"worker", workerID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment,
			"state", derived, "operation_count", len(ops))
		return nil
	}

	var completedAt *time.Time
	switch {
	case state.IsState(derived, state.Apply.Stopped):
		// stopped is resumable; keep completed_at nil to match the convention.
		completedAt = nil
	case state.IsTerminalApplyState(derived):
		if apply.CompletedAt != nil {
			completedAt = apply.CompletedAt
		} else {
			now := s.clock.Now()
			completedAt = &now
		}
	default:
		completedAt = nil
	}

	// When the rollout settles to failed, surface the failure reason from the
	// aggregate (the first failed operation) rather than leaving whatever message
	// the last-driven operation wrote — under continue the last driver may be a
	// successful sibling, which would leave the failed verdict with no matching
	// reason. Keep the existing message as a fallback when no operation carries one.
	errorMessage := apply.ErrorMessage
	if state.IsState(derived, state.Apply.Failed) {
		if msg := firstFailedOperationMessage(ops); msg != "" {
			errorMessage = msg
		}
	}

	swapped, err := s.storage.Applies().UpdateDerivedState(ctx, apply.ID, apply.State, derived, errorMessage, completedAt)
	if err != nil {
		return fmt.Errorf("update derived apply state for apply %s (%d) to %q: %w", apply.ApplyIdentifier, apply.ID, derived, err)
	}
	if !swapped {
		// Another drive advanced the apply between our read and write; our
		// projection is stale. Skip and let the next poll reconcile.
		s.logger.Info("operator: derived apply state write lost a race; skipping",
			"worker", workerID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment,
			"expected_state", apply.State, "derived_state", derived, "operation_count", len(ops))
		return nil
	}
	s.logger.Info("operator: updated derived apply state from apply_operations",
		"worker", workerID, "apply_id", apply.ApplyIdentifier,
		"database", apply.Database, "environment", apply.Environment,
		"previous_state", apply.State, "derived_state", derived, "operation_count", len(ops))
	return nil
}

func operatorLeaseOwner(workerID int) string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s/%d/worker-%d", hostname, os.Getpid(), workerID)
}
