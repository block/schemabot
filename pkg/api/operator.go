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

	// DefaultSchedulerWorkers is the number of concurrent operator workers
	// when not configured via scheduler_workers in the server config.
	DefaultSchedulerWorkers = 4
)

// StartOperator starts the background operator worker pool.
//
// Operator workers claim apply work from storage so one server can make
// progress across independent databases and environments concurrently. This
// includes queued applies, crash recovery for applies with stale heartbeats,
// and retry recovery for transient engine failures.
//
// Launches N concurrent workers (configured via scheduler_workers in config).
// Each worker independently claims applies using FOR UPDATE SKIP LOCKED.
// Call StopOperator to gracefully stop.
func (s *Service) StartOperator(ctx context.Context) {
	s.operatorMu.Lock()
	if s.stopRecovery != nil {
		s.operatorMu.Unlock()
		s.logger.Info("operator already running")
		return
	}

	workers := s.config.SchedulerWorkers
	if workers <= 0 {
		workers = DefaultSchedulerWorkers
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
		metrics.RecordSchedulerClaimFailure(ctx, "expire_retryable_error")
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
		metrics.RecordSchedulerResumeFailure(ctx, apply.Database, apply.Deployment, apply.Environment, string(expiration.Reason))
	}

	owner := operatorLeaseOwner(workerID)

	if s.config.OperatorClaimOperations {
		s.recoverApplyOperation(ctx, workerID, owner)
		return
	}

	apply, err := s.storage.Applies().FindNextApply(ctx, owner)
	if err != nil {
		s.logger.Error("operator: failed to claim apply", "worker", workerID, "lease_owner", owner, "error", err)
		metrics.RecordSchedulerClaimFailure(ctx, "storage_error")
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
		metrics.RecordSchedulerClaimFailure(ctx, "missing_lease_token")
		return
	}
	ctx = storage.WithApplyLease(ctx, lease)
	s.resumeClaimedApply(ctx, workerID, apply)
}

// recoverApplyOperation claims work at the apply_operations (per-deployment)
// level: it leases one operation row, acquires the parent apply lease that
// lease-guarded writes require, drives the apply through the shared resume path
// while heartbeating the operation row, then marks the operation row terminal
// from the parent apply's final state. While the apply-create dual-write emits
// exactly one operation per apply, driving the parent apply is equivalent to
// driving the single operation; this path is the foundation for the future
// multi-deployment fan-out.
func (s *Service) recoverApplyOperation(ctx context.Context, workerID int, owner string) {
	op, err := s.storage.ApplyOperations().FindNextApplyOperation(ctx)
	if err != nil {
		s.logger.Error("operator: failed to claim apply_operation", "worker", workerID, "lease_owner", owner, "error", err)
		metrics.RecordSchedulerClaimFailure(ctx, "operation_storage_error")
		return
	}
	if op == nil {
		s.logger.Debug("operator: no apply_operation to claim", "worker", workerID)
		return
	}

	// All lease-guarded writes (ResumeApply, MarkCompleted/MarkFailed, the
	// operation Heartbeat) key off applies.lease_token, so the operation claim
	// alone is not enough to write safely — acquire the parent apply lease.
	apply, err := s.storage.Applies().ClaimApplyByID(ctx, op.ApplyID, owner)
	if err != nil {
		s.logger.Error("operator: failed to claim parent apply for operation",
			"worker", workerID,
			"lease_owner", owner,
			"apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID,
			"deployment", op.Deployment,
			"error", err)
		metrics.RecordSchedulerClaimFailure(ctx, "operation_parent_claim_error")
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

	lease := apply.Lease()
	if !lease.Valid() {
		s.logger.Error("operator: claimed parent apply without a valid lease token; operation will not be driven",
			"worker", workerID,
			"lease_owner", owner,
			"apply_operation_id", op.ID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", op.Deployment,
			"environment", apply.Environment)
		metrics.RecordSchedulerClaimFailure(ctx, "missing_lease_token")
		return
	}
	leasedCtx := storage.WithApplyLease(ctx, lease)

	// Heartbeat the operation row on the apply heartbeat cadence so a peer
	// worker does not re-claim it during a long ResumeApply. A lost parent lease
	// cancels the run so the displaced worker stops writing.
	runCtx, cancelRun := context.WithCancel(leasedCtx)
	defer cancelRun()
	stopHeartbeat := s.startApplyOperationHeartbeat(runCtx, workerID, op, apply, cancelRun)
	defer stopHeartbeat()

	resumed := s.resumeClaimedApply(runCtx, workerID, apply)
	stopHeartbeat()
	if !resumed {
		return
	}

	// Reload the parent apply: ResumeApply persists the final state to storage,
	// and the operation row must mirror the durable outcome, not the in-memory
	// object the resume path started from.
	finalApply, err := s.storage.Applies().Get(leasedCtx, apply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload parent apply after resume; operation state not updated",
			"worker", workerID,
			"apply_operation_id", op.ID,
			"apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment,
			"error", err)
		return
	}
	if finalApply == nil {
		s.logger.Error("operator: parent apply not found after resume; operation state not updated",
			"worker", workerID,
			"apply_operation_id", op.ID,
			"apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment)
		return
	}

	s.markOperationFromApplyState(leasedCtx, workerID, op, finalApply)
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
		metrics.RecordSchedulerClaimFailure(ctx, "operation_parent_not_claimable")
		return
	}
	if parent == nil {
		s.logger.Error("operator: parent apply not found for claimed operation; operation will be retried",
			"worker", workerID,
			"apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID,
			"deployment", op.Deployment)
		metrics.RecordSchedulerClaimFailure(ctx, "operation_parent_not_claimable")
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
		s.markOperationFromApplyState(ctx, workerID, op, parent)
		return
	}
	s.logger.Warn("operator: parent apply not claimable for operation; operation will be retried",
		"worker", workerID,
		"apply_operation_id", op.ID,
		"apply_id", parent.ApplyIdentifier,
		"deployment", op.Deployment,
		"environment", parent.Environment,
		"state", parent.State)
	metrics.RecordSchedulerClaimFailure(ctx, "operation_parent_not_claimable")
}

// resumeClaimedApply drives a claimed apply through ResumeApply with the apply
// lease already attached to ctx. Returns true when the apply resumed without
// error. Failures are logged and recorded as metrics internally; the bool lets
// the operation-level claim loop decide whether to mark its operation terminal.
func (s *Service) resumeClaimedApply(ctx context.Context, workerID int, apply *storage.Apply) bool {
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

	previousState := apply.State

	deployment, err := storedDeploymentForApply(apply)
	if err != nil {
		s.logger.Error("operator: claimed apply is missing stored deployment metadata",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
		metrics.RecordSchedulerResumeFailure(ctx, apply.Database, "", apply.Environment, "missing_deployment")
		return false
	}
	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		s.logger.Error("operator: failed to get client",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", deployment,
			"environment", apply.Environment,
			"error", err)
		metrics.RecordSchedulerResumeFailure(ctx, apply.Database, deployment, apply.Environment, "no_client")
		return false
	}

	if s.OnApplyRecovered != nil {
		s.OnApplyRecovered(apply)
	}

	retryableClaim := previousState == state.Apply.FailedRetryable
	if retryableClaim {
		metrics.AdjustActiveApplies(ctx, 1, apply.Database, deployment, apply.Environment)
	}
	if err := client.ResumeApply(ctx, apply); err != nil {
		if errors.Is(err, storage.ErrApplyLeaseLost) {
			s.logger.Warn("operator: apply lease was lost; worker will stop writing this apply",
				"worker", workerID,
				"lease_owner", lease.Owner,
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"deployment", deployment,
				"environment", apply.Environment,
				"error", err)
			metrics.RecordSchedulerResumeFailure(ctx, apply.Database, deployment, apply.Environment, "lease_lost")
			if retryableClaim {
				metrics.AdjustActiveApplies(ctx, -1, apply.Database, deployment, apply.Environment)
			}
			return false
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
			return false
		}
		s.logger.Error("operator: failed to resume apply",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", deployment,
			"environment", apply.Environment,
			"error", err)
		metrics.RecordSchedulerResumeFailure(ctx, apply.Database, deployment, apply.Environment, "resume_error")
		if retryableClaim {
			metrics.AdjustActiveApplies(ctx, -1, apply.Database, deployment, apply.Environment)
		}
		return false
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
	metrics.RecordSchedulerResume(ctx, apply.Database, deployment, apply.Environment, previousState)
	metrics.RecordSchedulerClaimDuration(ctx, duration, apply.Database, deployment, apply.Environment, previousState)
	return true
}

// startApplyOperationHeartbeat refreshes the claimed operation row's lease while
// ResumeApply runs, at min(operatorPollInterval, ApplyOperationHeartbeatInterval)
// so the row cannot go stale and be re-claimed by a peer even when the poll
// interval is large. A lost parent apply lease cancels the run so the displaced
// worker stops; other heartbeat errors are logged and retried on the next tick.
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
						s.logger.Warn("operator: apply_operation heartbeat lost parent apply lease; worker will stop",
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
// the parent apply's final state after a successful resume. Non-terminal parent
// states leave the operation claimable so a later poll re-leases and resumes it.
func (s *Service) markOperationFromApplyState(ctx context.Context, workerID int, op *storage.ApplyOperation, apply *storage.Apply) {
	opStore := s.storage.ApplyOperations()
	switch {
	case state.IsState(apply.State, state.Apply.Completed):
		if err := opStore.MarkCompleted(ctx, op.ID); err != nil {
			s.logger.Error("operator: failed to mark apply_operation completed",
				"worker", workerID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
				"deployment", op.Deployment, "environment", apply.Environment, "error", err)
		}
	case state.IsState(apply.State, state.Apply.Failed):
		if err := opStore.MarkFailed(ctx, op.ID, apply.ErrorMessage); err != nil {
			s.logger.Error("operator: failed to mark apply_operation failed",
				"worker", workerID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
				"deployment", op.Deployment, "environment", apply.Environment, "error", err)
		}
	case state.IsState(apply.State, state.Apply.Stopped):
		// stopped is resumable, so mirror the state but leave completed_at nil
		// (matching the apply-level convention) — stopped work may resume.
		if err := opStore.UpdateState(ctx, op.ID, apply.State); err != nil {
			s.logger.Error("operator: failed to update stopped apply_operation state",
				"worker", workerID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
				"deployment", op.Deployment, "environment", apply.Environment, "state", apply.State, "error", err)
		}
	case state.IsTerminalApplyState(apply.State):
		// cancelled / reverted — non-resumable terminal states; stamp completed_at.
		if err := opStore.MarkTerminal(ctx, op.ID, apply.State); err != nil {
			s.logger.Error("operator: failed to mark terminal apply_operation state",
				"worker", workerID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
				"deployment", op.Deployment, "environment", apply.Environment, "state", apply.State, "error", err)
		}
	default:
		s.logger.Debug("operator: parent apply not terminal after resume; leaving operation claimable",
			"worker", workerID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", apply.Environment, "state", apply.State)
	}
}

func operatorLeaseOwner(workerID int) string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s/%d/worker-%d", hostname, os.Getpid(), workerID)
}
