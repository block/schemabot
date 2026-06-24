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
	// an apply is considered to have a crashed driver and needs recovery.
	// FindNextApply uses this (via SQL: updated_at < NOW() - INTERVAL 1 MINUTE).
	HeartbeatTimeout = 1 * time.Minute

	// ApplyOperationHeartbeatInterval bounds how often the operation-row
	// heartbeat fires while ResumeApply runs. It is kept safely below
	// HeartbeatTimeout so that a large (or misconfigured) operator poll
	// interval cannot let apply_operations.updated_at go stale and allow a peer
	// driver to re-claim the operation mid-resume. The effective cadence is
	// min(operatorPollInterval, ApplyOperationHeartbeatInterval).
	ApplyOperationHeartbeatInterval = 10 * time.Second

	// DefaultDrivers is the number of concurrent operator drivers
	// when not configured via drivers in the server config.
	DefaultDrivers = 4

	// ApplyClaimLogTimeout bounds the best-effort apply-log append recording an
	// operator claim, so a slow or hung storage layer cannot delay the resume
	// the claim is about to drive.
	ApplyClaimLogTimeout = 5 * time.Second
)

// StartOperator starts the background operator driver pool.
//
// Operator drivers claim apply work from storage so one server can make
// progress across independent databases and environments concurrently. This
// includes queued applies, crash recovery for applies with stale heartbeats,
// and retry recovery for transient engine failures.
//
// Launches N concurrent drivers (configured via drivers in config).
// Each driver independently claims applies using FOR UPDATE SKIP LOCKED.
// Call StopOperator to gracefully stop.
func (s *Service) StartOperator(ctx context.Context) {
	s.operatorMu.Lock()
	if s.stopRecovery != nil {
		s.operatorMu.Unlock()
		s.logger.Info("operator already running")
		return
	}

	driverCount := s.config.Drivers
	if driverCount <= 0 {
		driverCount = DefaultDrivers
	}

	stop := make(chan struct{})
	wake := make(chan struct{}, driverCount)
	driverCtx, cancel := context.WithCancel(ctx)
	s.stopRecovery = stop
	s.cancelRecovery = cancel
	s.operatorWake = wake
	s.operatorMu.Unlock()

	for i := range driverCount {
		driverID := i
		s.recoveryWg.Go(func() {
			s.operatorDriver(driverCtx, driverID, stop, wake)
		})
	}

	s.logger.Info("operator started", "drivers", driverCount, "interval", s.operatorPollInterval)
}

// StopOperator stops the background operator and waits for all drivers to finish.
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

// WakeOperator nudges the operator driver pool to claim queued durable work.
// Storage locking still decides ownership; this does not execute apply control
// actions directly.
func (s *Service) WakeOperator(applyIdentifier, database, environment string) {
	s.wakeOperator(applyIdentifier, database, environment)
}

// operatorDriver is a single driver that claims at most one apply on startup
// and on each operator poll tick. Wake signals share the same claim path as
// polling; storage locking decides whether a driver actually owns work.
func (s *Service) operatorDriver(ctx context.Context, driverID int, stop <-chan struct{}, wake <-chan struct{}) {
	ticker := time.NewTicker(s.operatorPollInterval)
	defer ticker.Stop()

	s.logger.Debug("operator driver started", "driver", driverID)

	s.recoverApplies(ctx, driverID)

	for {
		select {
		case <-stop:
			s.logger.Debug("operator driver stopping", "driver", driverID)
			return
		case <-ctx.Done():
			s.logger.Debug("operator driver context cancelled", "driver", driverID)
			return
		case <-wake:
			s.logger.Debug("operator driver woke for queued apply", "driver", driverID)
			s.recoverApplies(ctx, driverID)
		case <-ticker.C:
			s.recoverApplies(ctx, driverID)
		}
	}
}

// recoverApplies claims and resumes applies that need attention.
// Each call claims one apply (if available) to keep the scheduling loop responsive.
func (s *Service) recoverApplies(ctx context.Context, driverID int) {
	expired, err := s.storage.Applies().ExpireRetryable(ctx)
	if err != nil {
		s.logger.Error("operator: failed to expire retryable applies", "driver", driverID, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "expire_retryable_error")
		return
	}
	for _, expiration := range expired {
		apply := expiration.Apply
		s.logger.Error("operator: retryable apply expired",
			"driver", driverID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"attempt", apply.Attempt,
			"reason", expiration.Reason)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, apply.Deployment, apply.Environment, string(expiration.Reason))
	}

	owner := driverLeaseOwner(driverID)

	if s.config.ShouldClaimOperations() {
		// Service a pending stop with no claimable operation to carry it before
		// claiming new operation work, so a queued stop wins over starting more
		// deployments. When nothing needs stop reconciliation this is a cheap
		// no-op and the driver falls through to the normal operation claim.
		if s.recoverApplyPendingStop(ctx, driverID, owner) {
			return
		}
		// Drive a barrier-parked cutover whose deployment-ordered turn it is
		// before claiming new copy work, so the high-risk ordered swaps make
		// progress ahead of starting more copy phases. Dormant until the
		// multi-deployment fan-out lands (nothing parks at the barrier today).
		if s.recoverApplyOperationCutover(ctx, driverID, owner) {
			return
		}
		s.recoverApplyOperation(ctx, driverID, owner)
		return
	}

	apply, err := s.storage.Applies().FindNextApply(ctx, owner)
	if err != nil {
		s.logger.Error("operator: failed to claim apply", "driver", driverID, "lease_owner", owner, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "storage_error")
		return
	}

	if apply == nil {
		s.logger.Debug("operator: no apply to claim", "driver", driverID)
		return
	}
	lease := apply.Lease()
	if !lease.Valid() {
		s.logger.Error("operator: claimed apply without a valid lease token; operator will not resume it",
			"driver", driverID,
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
	_, _ = s.resumeClaimedApply(ctx, driverID, apply, 0, "")
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
func (s *Service) recoverApplyOperation(ctx context.Context, driverID int, owner string) {
	op, err := s.storage.ApplyOperations().FindNextApplyOperation(ctx, owner)
	if err != nil {
		s.logger.Error("operator: failed to claim apply_operation", "driver", driverID, "lease_owner", owner, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "operation_storage_error")
		return
	}
	if op == nil {
		s.logger.Debug("operator: no apply_operation to claim", "driver", driverID)
		return
	}

	// The claim rotated a fresh operation lease onto the row. It is the
	// capability that guards this operation's own writes — its state
	// transitions, heartbeat, and task updates — so fail closed if it is
	// missing rather than silently degrading to the parent apply lease.
	opLease := op.Lease()
	if !opLease.Valid() {
		s.logger.Error("operator: claimed apply_operation without a valid operation lease token; operation will not be driven",
			"driver", driverID,
			"lease_owner", owner,
			"apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID,
			"deployment", op.Deployment)
		metrics.RecordOperatorClaimFailure(ctx, "missing_operation_lease_token")
		return
	}

	// Branch on the operation count. A single-operation apply keeps the legacy
	// parent-lease drive byte-for-byte. A multi-operation apply drives under the
	// operation lease only: siblings must not serialize on a shared parent lease,
	// and the parent applies.state is moved solely by the projection CAS.
	ops, err := s.storage.ApplyOperations().ListByApply(ctx, op.ApplyID)
	if err != nil {
		s.logger.Error("operator: failed to list operations for claimed apply; operation will not be driven",
			"driver", driverID, "lease_owner", owner, "apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID, "deployment", op.Deployment, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "operation_set_list_error")
		return
	}
	if !operationSetContainsID(ops, op.ID) {
		s.logger.Error("operator: claimed operation is not part of its apply's operation set; operation will not be driven",
			"driver", driverID, "lease_owner", owner, "apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID, "deployment", op.Deployment, "operation_count", len(ops))
		metrics.RecordOperatorClaimFailure(ctx, "operation_set_missing")
		return
	}
	if len(ops) > 1 {
		s.recoverMultiApplyOperation(ctx, driverID, op, opLease)
		return
	}
	hasTasks, err := s.claimedOperationHasTasks(ctx, op)
	if err != nil {
		s.logger.Error("operator: failed to inspect claimed operation tasks; operation will not be driven",
			"driver", driverID, "lease_owner", owner, "apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID, "deployment", op.Deployment, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "operation_task_inspect_error")
		return
	}
	if !hasTasks {
		// Apply-level claiming is task-gated, so a valid task-less operation (for
		// example plan-level VSchema work) cannot be driven through the legacy
		// parent-lease path. Drive it under the operation lease and project the
		// parent from the operation row, the same fail-closed path multi-operation
		// applies use.
		s.driveClaimedMultiOperation(ctx, driverID, op, opLease, false)
		return
	}

	s.recoverSingleApplyOperation(ctx, driverID, owner, op, opLease)
}

func (s *Service) claimedOperationHasTasks(ctx context.Context, op *storage.ApplyOperation) (bool, error) {
	tasks, err := s.storage.Tasks().GetByApplyOperationID(ctx, op.ID)
	if err != nil {
		return false, fmt.Errorf("load tasks for apply_operation %d (deployment %q): %w", op.ID, op.Deployment, err)
	}
	return len(tasks) > 0, nil
}

// operationSetContainsID reports whether id is one of the apply's operation
// rows, used to confirm the claimed operation still belongs to the apply set
// before driving it.
func operationSetContainsID(ops []*storage.ApplyOperation, id int64) bool {
	for _, op := range ops {
		if op.ID == id {
			return true
		}
	}
	return false
}

// recoverSingleApplyOperation drives a single-operation apply on the legacy
// parent-lease path: it claims the parent apply lease, runs the engine under the
// dual (apply + operation) lease so the engine writes the parent applies row
// directly, fires the per-driver terminal observer, and re-derives the parent
// from the operation rows afterward. This path is byte-for-byte the pre-fan-out
// behavior.
func (s *Service) recoverSingleApplyOperation(ctx context.Context, driverID int, owner string, op *storage.ApplyOperation, opLease storage.OperationLease) {
	// The engine drive still writes the parent applies row (state RUNNING /
	// COMPLETED / FAILED), and the derived-state reconcile updates
	// applies.state, so the driver must also hold the parent apply lease — the
	// operation lease alone does not authorize parent-apply writes.
	apply, err := s.storage.Applies().ClaimApplyByID(ctx, op.ApplyID, owner)
	if err != nil {
		s.logger.Error("operator: failed to claim parent apply for operation",
			"driver", driverID,
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
		s.reconcileUnclaimableParent(ctx, driverID, op)
		return
	}

	applyLease := apply.Lease()
	if !applyLease.Valid() {
		s.logger.Error("operator: claimed parent apply without a valid lease token; operation will not be driven",
			"driver", driverID,
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
	// for the drive: RoutingClient.ResumeApplyOperation reloads the operation
	// row, routes by its deployment, and fails closed when no client is
	// configured for that deployment/environment. The parent apply's stored
	// deployment is only the primary deployment and is not the routing source,
	// so an operation deployment that differs from the parent is expected for
	// multi-deployment applies. An empty operation deployment is a corrupt row
	// with no routing key, so fail closed rather than fall back to a default.
	if op.Deployment == "" {
		s.logger.Error("operator: claimed operation is missing deployment metadata; operation will not be driven",
			"driver", driverID,
			"lease_owner", owner,
			"apply_operation_id", op.ID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", op.Deployment,
			"apply_deployment", apply.Deployment,
			"environment", apply.Environment)
		metrics.RecordOperatorClaimFailure(ctx, "missing_operation_deployment")
		return
	}

	// Heartbeat the operation row on the apply heartbeat cadence so a peer
	// driver does not re-claim it during a long drive. The heartbeat writes
	// under the operation lease, so a lost operation lease cancels the run and
	// the displaced driver stops writing.
	runCtx, cancelRun := context.WithCancel(dualLeaseCtx)
	defer cancelRun()
	stopHeartbeat := s.startApplyOperationHeartbeat(runCtx, driverID, op, apply, cancelRun)
	defer stopHeartbeat()

	resumed, resumeErr := s.resumeClaimedApply(runCtx, driverID, apply, op.ID, op.Deployment)
	stopHeartbeat()
	if !resumed {
		if errors.Is(resumeErr, tern.ErrNoTasksForApplyOperation) {
			// The drive failed closed: the operation has no tasks, so it can
			// never make progress. Terminalize it now rather than leaving it to
			// be re-leased on every poll once its heartbeat goes stale.
			s.failOperationWithoutTasks(operationLeaseCtx, applyLeaseCtx, driverID, op, apply)
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
	marked, err := s.markOperationFromOwnResult(operationLeaseCtx, driverID, op)
	if err != nil {
		s.logger.Error("operator: failed to update apply_operation from its tasks; derived apply state not updated",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
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
	if err := s.stopPendingOperationsForPendingStop(applyLeaseCtx, driverID, apply); err != nil {
		s.logger.Error("operator: failed to stop pending sibling operations for pending stop request; derived apply state not updated",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
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
			"driver", driverID,
			"apply_operation_id", op.ID,
			"apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment,
			"error", err)
		return
	}
	if finalApply == nil {
		s.logger.Error("operator: parent apply not found after resume; derived apply state not updated",
			"driver", driverID,
			"apply_operation_id", op.ID,
			"apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment)
		return
	}

	if _, err := s.updateApplyStateFromOperations(applyLeaseCtx, driverID, finalApply, allowLeaseScopedFailedReopen); err != nil {
		s.logger.Error("operator: failed to update derived apply state from apply_operations",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", finalApply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", finalApply.Environment, "error", err)
		return
	}

	// If the derived state above settled the apply terminally and a stop is still
	// pending, complete it now so the request does not linger after the rollout
	// has stopped.
	if err := s.completePendingStopIfApplyResolved(applyLeaseCtx, driverID, finalApply.ID); err != nil {
		s.logger.Error("operator: failed to complete pending stop request for resolved apply",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", finalApply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", finalApply.Environment, "error", err)
		return
	}
}

// recoverMultiApplyOperation drives one operation of a multi-operation apply
// under the operation lease only. It never claims the parent apply lease, so
// sibling operations no longer serialize on a shared parent token. The engine
// drive is suppressed from writing the parent applies row (the gRPC/local
// drivers skip parent writes for operation-scoped multi-op drives), and the
// per-driver terminal observer is disabled; the parent applies.state is moved
// solely by the operation-authorized projection CAS. The operation row is
// driven from its own tasks, then the parent is re-derived from the operation
// rows.
func (s *Service) recoverMultiApplyOperation(ctx context.Context, driverID int, op *storage.ApplyOperation, opLease storage.OperationLease) {
	s.driveClaimedMultiOperation(ctx, driverID, op, opLease, false)
}

// driveClaimedMultiOperation drives one claimed operation of a multi-operation
// apply under the operation lease only and settles the parent via the projection
// CAS. cutover selects the drive phase: false runs the operation's copy phase
// (ResumeApplyOperation), true forces a barrier-parked operation through its swap
// (ResumeApplyOperationCutover). The parent applies row is never written by the
// drive — the driver owns only its operation row and tasks; the parent state and
// the once-only terminal summary are this method's projection responsibility.
func (s *Service) driveClaimedMultiOperation(ctx context.Context, driverID int, op *storage.ApplyOperation, opLease storage.OperationLease, cutover bool) {
	operationLeaseCtx := storage.WithOperationLease(ctx, opLease)

	apply, err := s.storage.Applies().Get(operationLeaseCtx, op.ApplyID)
	if err != nil {
		s.logger.Error("operator: failed to load parent apply for operation drive; operation will not be driven",
			"driver", driverID, "apply_operation_id", op.ID, "apply_db_id", op.ApplyID,
			"deployment", op.Deployment, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "operation_parent_load_error")
		return
	}
	if apply == nil {
		s.logger.Error("operator: parent apply not found for claimed operation; operation will not be driven",
			"driver", driverID, "apply_operation_id", op.ID, "apply_db_id", op.ApplyID,
			"deployment", op.Deployment)
		metrics.RecordOperatorClaimFailure(ctx, "operation_parent_missing")
		return
	}
	if state.IsTerminalApplyState(apply.State) {
		s.reconcileClaimedOperationFromTerminalParent(operationLeaseCtx, driverID, op, apply)
		return
	}

	// The claimed operation row's deployment is the authoritative routing key; an
	// empty deployment is a corrupt row with no routing key, so fail closed.
	if op.Deployment == "" {
		s.logger.Error("operator: claimed operation is missing deployment metadata; operation will not be driven",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment)
		metrics.RecordOperatorClaimFailure(ctx, "missing_operation_deployment")
		return
	}

	// Move the parent pending→running via the projection CAS before the drive.
	// The claim already set this operation running, so the projection reflects an
	// in-flight rollout; the driver no longer writes the parent running for
	// multi-op, so without this the parent would linger pending during a long
	// drive. The op lease authorizes the derived-state CAS.
	if _, err := s.updateApplyStateFromOperations(operationLeaseCtx, driverID, apply, allowLeaseScopedFailedReopen); err != nil {
		s.logger.Error("operator: failed to project parent running before operation drive; operation will not be driven",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment, "error", err)
		return
	}

	// Heartbeat the operation row on the apply heartbeat cadence so a peer driver
	// does not re-claim it during a long drive. The heartbeat writes under the
	// operation lease, so a lost operation lease cancels the run.
	runCtx, cancelRun := context.WithCancel(operationLeaseCtx)
	defer cancelRun()
	stopHeartbeat := s.startApplyOperationHeartbeat(runCtx, driverID, op, apply, cancelRun)
	defer stopHeartbeat()

	// Suppress the per-driver terminal observer: the aggregate terminal summary
	// is published once by the projection CAS winner, not per deployment.
	resumed, resumeErr := s.resumeClaimedApplyWithOptions(runCtx, driverID, apply, op.ID, op.Deployment,
		resumeClaimedApplyOptions{suppressRecoveredObserver: true, cutover: cutover})
	stopHeartbeat()
	if !resumed && errors.Is(resumeErr, tern.ErrNoTasksForApplyOperation) {
		// The drive failed closed: the operation has no tasks, so it can never
		// make progress. Terminalize it now rather than leaving it to be
		// re-leased on every poll once its heartbeat goes stale.
		//
		// Reload the parent apply first: the pre-drive projection may already
		// have moved the durable parent from pending to running, and the
		// failure projection CAS expects the current durable state. Failing
		// against the stale pre-drive apply would miss the CAS and strand the
		// parent apply running.
		failApply := apply
		if reloaded, reloadErr := s.storage.Applies().Get(operationLeaseCtx, apply.ID); reloadErr != nil {
			s.logger.Error("operator: failed to reload parent apply before failing task-less operation; using pre-drive apply state",
				"driver", driverID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
				"database", apply.Database, "environment", apply.Environment, "error", reloadErr)
		} else if reloaded != nil {
			failApply = reloaded
		}
		s.failOperationWithoutTasks(operationLeaseCtx, operationLeaseCtx, driverID, op, failApply)
		return
	}

	// Persist the operation row from its OWN tasks — even when the drive returned
	// an error. Unlike the single-op path, a multi-op drive has no
	// reconcileUnclaimableParent fallback (it never claims the parent), so a
	// remote rejection that durably failed this operation's tasks must be
	// promoted to the operation row here or the operation would stay running and
	// be re-leased forever. markOperationFromOwnResult derives the operation from
	// its tasks: a still-running task set leaves it claimable (a benign no-op),
	// while a terminal task set terminalizes it.
	marked, err := s.markOperationFromOwnResult(operationLeaseCtx, driverID, op)
	if err != nil {
		s.logger.Error("operator: failed to update apply_operation from its tasks; derived apply state not updated",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", apply.Environment, "error", err)
		return
	}
	if !marked {
		return
	}

	// If a stop is pending, terminalize any still-pending sibling operations to
	// stopped before deriving the parent, so the rollout can settle. Authorized
	// by this operation's own lease (the 6a op-lease branch).
	if err := s.stopPendingOperationsForPendingStop(operationLeaseCtx, driverID, apply); err != nil {
		s.logger.Error("operator: failed to stop pending sibling operations for pending stop request; derived apply state not updated",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", apply.Environment, "error", err)
		return
	}

	// Reload the parent so the projection re-derives against the durable
	// apply.State (its terminal-to-non-terminal guard), then move it via the CAS.
	finalApply, err := s.storage.Applies().Get(operationLeaseCtx, apply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload parent apply after operation drive; derived apply state not updated",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment, "error", err)
		return
	}
	if finalApply == nil {
		s.logger.Error("operator: parent apply not found after operation drive; derived apply state not updated",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment)
		return
	}

	result, err := s.updateApplyStateFromOperations(operationLeaseCtx, driverID, finalApply, allowLeaseScopedFailedReopen)
	if err != nil {
		s.logger.Error("operator: failed to update derived apply state from apply_operations",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", finalApply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", finalApply.Environment, "error", err)
		return
	}

	// Publish the apply-level terminal summary if this drive's projection won the
	// swap that terminalized the parent. Do this before stop-request cleanup: the
	// summary depends only on the apply being terminal, and a later cleanup error
	// must not suppress it.
	s.publishTerminalSummaryIfWon(operationLeaseCtx, driverID, finalApply, result)

	if err := s.completePendingStopIfApplyResolved(operationLeaseCtx, driverID, finalApply.ID); err != nil {
		s.logger.Error("operator: failed to complete pending stop request for resolved apply",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", finalApply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", finalApply.Environment, "error", err)
		return
	}
}

// recoverApplyOperationCutover claims the next barrier-parked operation whose
// deployment-ordered turn it is to cut over and drives it through its swap. It is
// the cutover counterpart to recoverApplyOperation's copy claim: the storage
// predicate (FindNextApplyOperationCutover) only returns operations of a
// multi-operation barrier apply, so the drive always runs under the operation
// lease only and the parent applies row is settled by the projection CAS. With
// one operation per apply today nothing parks at the barrier, so this is dormant
// until the multi-deployment fan-out lands.
//
// Returns true when this tick is consumed by a cutover claim — an operation was
// claimed (whether or not the drive that followed succeeded) or the claim itself
// errored — so the caller does not also run the normal copy-operation claim this
// tick. Returns false only when nothing is ready to cut over.
func (s *Service) recoverApplyOperationCutover(ctx context.Context, driverID int, owner string) bool {
	op, err := s.storage.ApplyOperations().FindNextApplyOperationCutover(ctx, owner)
	if err != nil {
		s.logger.Error("operator: failed to claim apply_operation cutover", "driver", driverID, "lease_owner", owner, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "operation_cutover_storage_error")
		return true
	}
	if op == nil {
		s.logger.Debug("operator: no apply_operation to cut over", "driver", driverID)
		return false
	}

	// The claim rotated a fresh operation lease onto the row; it is the
	// capability that guards this operation's writes, so fail closed if missing.
	opLease := op.Lease()
	if !opLease.Valid() {
		s.logger.Error("operator: claimed apply_operation cutover without a valid operation lease token; operation will not be driven",
			"driver", driverID, "lease_owner", owner, "apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID, "deployment", op.Deployment)
		metrics.RecordOperatorClaimFailure(ctx, "missing_operation_cutover_lease_token")
		return true
	}

	// The cutover predicate already gates to multi-operation barrier applies, but
	// the operation-lease-only drive (and its parent-write suppression) is only
	// correct for a genuine multi-operation apply, so verify the set before
	// driving rather than trusting the claim alone.
	ops, err := s.storage.ApplyOperations().ListByApply(ctx, op.ApplyID)
	if err != nil {
		s.logger.Error("operator: failed to list operations for claimed cutover; operation will not be driven",
			"driver", driverID, "lease_owner", owner, "apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID, "deployment", op.Deployment, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "operation_cutover_set_list_error")
		return true
	}
	if len(ops) <= 1 || !operationSetContainsID(ops, op.ID) {
		s.logger.Error("operator: claimed cutover operation is not part of a multi-operation apply set; operation will not be driven",
			"driver", driverID, "lease_owner", owner, "apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID, "deployment", op.Deployment, "operation_count", len(ops))
		metrics.RecordOperatorClaimFailure(ctx, "operation_cutover_set_invalid")
		return true
	}

	s.driveClaimedMultiOperation(ctx, driverID, op, opLease, true)
	return true
}

// recoverApplyPendingStop drives stop reconciliation for an apply that has a
// pending stop request but no claimable operation to carry it. Two cases reach
// here: an apply whose operation row is still pending while its task is already
// running (a direct data-plane apply marks the task running before the operator
// claims the operation), and an on_failure "continue" rollout where a failed
// earlier sibling left only terminal and pending operations that the claim gate
// keeps from starting. In both cases the normal operation-claim path never
// drives the apply, so its stop would strand forever. This path claims the apply
// directly, drives the data-plane stop so the engine halts and the tasks settle
// to stopped, terminalizes the pending operation rows, re-derives the parent,
// and completes the stop once the apply is terminal.
//
// Returns true when this tick is consumed by stop reconciliation — an apply was
// claimed (whether the reconciliation that followed succeeded or hit an error)
// or the claim itself errored or returned an invalid lease — so the caller does
// not also run the normal operation claim this tick. Returns false only when no
// apply needed reconciliation.
func (s *Service) recoverApplyPendingStop(ctx context.Context, driverID int, owner string) bool {
	apply, err := s.storage.Applies().FindNextApplyForStopReconciliation(ctx, owner)
	if err != nil {
		s.logger.Error("operator: failed to claim apply for stop reconciliation",
			"driver", driverID, "lease_owner", owner, "error", err)
		metrics.RecordOperatorClaimFailure(ctx, "stop_reconciliation_claim_error")
		return true
	}
	if apply == nil {
		s.logger.Debug("operator: no apply needs stop reconciliation", "driver", driverID)
		return false
	}

	lease := apply.Lease()
	if !lease.Valid() {
		s.logger.Error("operator: claimed apply for stop reconciliation without a valid lease token; skipping",
			"driver", driverID, "lease_owner", owner,
			"apply_id", apply.ApplyIdentifier, "database", apply.Database, "environment", apply.Environment)
		metrics.RecordOperatorClaimFailure(ctx, "stop_reconciliation_missing_lease_token")
		return true
	}
	applyLeaseCtx := storage.WithApplyLease(ctx, lease)

	// Drive the pending stop through the data plane before terminalizing any
	// rows. The data-plane drive (ResumeApply -> processPendingStopControlRequest
	// -> stopOwnedApply) halts live engine work and sets the apply's tasks to
	// stopped. Without it, an apply whose operation row is still pending while its
	// task is already running would have its operation and apply rows marked
	// stopped while the task keeps running, leaving no stopped task for a later
	// start to resume. applyOperationID 0 selects the whole-apply drive: this path
	// holds the apply lease, not an operation lease, because no single operation
	// is carrying the stop.
	// resumeClaimedApply returns (true, nil) on success and (false, err) on every
	// failure, so the error is the only signal we need here.
	if _, err := s.resumeClaimedApply(applyLeaseCtx, driverID, apply, 0, ""); err != nil {
		// Fail closed: leave the pending stop and pending operation rows untouched
		// so the next tick reclaims this apply and retries the data-plane stop.
		s.logger.Error("operator: failed to drive data-plane stop during stop reconciliation",
			"driver", driverID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "deployment", apply.Deployment,
			"environment", apply.Environment, "error", err)
		return true
	}

	// The data-plane drive stopped the tasks and completed the stop control
	// request, but it does not touch apply_operations rows. Terminalize the
	// still-pending operations so the derived parent state below stays stopped and
	// the operation rows match the now-stopped tasks. The stop request is already
	// completed, so this goes through the unguarded helper rather than
	// stopPendingOperationsForPendingStop (which would no-op without a pending
	// stop).
	if err := s.markPendingOperationsStopped(applyLeaseCtx, driverID, apply); err != nil {
		s.logger.Error("operator: failed to stop pending sibling operations during stop reconciliation",
			"driver", driverID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment, "error", err)
		return true
	}

	finalApply, err := s.storage.Applies().Get(applyLeaseCtx, apply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload apply during stop reconciliation; derived apply state not updated",
			"driver", driverID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment, "error", err)
		return true
	}
	if finalApply == nil {
		s.logger.Error("operator: apply not found during stop reconciliation; derived apply state not updated",
			"driver", driverID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment)
		return true
	}

	result, err := s.updateApplyStateFromOperations(applyLeaseCtx, driverID, finalApply, allowLeaseScopedFailedReopen)
	if err != nil {
		s.logger.Error("operator: failed to update derived apply state during stop reconciliation",
			"driver", driverID, "apply_id", finalApply.ApplyIdentifier,
			"database", finalApply.Database, "environment", finalApply.Environment, "error", err)
		return true
	}

	// A multi-operation apply that settles terminal here (stop reconciliation has
	// no operation drive to publish on its behalf) still owes its single terminal
	// summary; publish it if this projection won the terminal swap.
	s.publishTerminalSummaryIfWon(applyLeaseCtx, driverID, finalApply, result)

	if err := s.completePendingStopIfApplyResolved(applyLeaseCtx, driverID, finalApply.ID); err != nil {
		s.logger.Error("operator: failed to complete pending stop request after stop reconciliation",
			"driver", driverID, "apply_id", finalApply.ApplyIdentifier,
			"database", finalApply.Database, "environment", finalApply.Environment, "error", err)
		return true
	}
	return true
}

// stopPendingOperationsForPendingStop terminalizes still-pending sibling
// operations to stopped when the apply has a pending stop request, so the
// rollout can settle instead of stranding running with siblings the claim gate
// keeps from ever starting. No-op when no stop is pending.
func (s *Service) stopPendingOperationsForPendingStop(ctx context.Context, driverID int, apply *storage.Apply) error {
	controlReq, err := s.storage.ControlRequests().GetPending(ctx, apply.ID, storage.ControlOperationStop)
	if err != nil {
		return fmt.Errorf("load pending stop request for apply %s (%d): %w", apply.ApplyIdentifier, apply.ID, err)
	}
	if controlReq == nil {
		return nil
	}
	return s.markPendingOperationsStopped(ctx, driverID, apply)
}

// markPendingOperationsStopped terminalizes every still-pending operation of the
// apply to stopped. Callers must have already established the stop intent — a
// pending stop request, or a completed data-plane stop drive — because this does
// not re-check the control request. That lets it run after the data-plane drive
// has already completed the stop request, where the pending-stop guard in
// stopPendingOperationsForPendingStop would otherwise short-circuit to a no-op.
func (s *Service) markPendingOperationsStopped(ctx context.Context, driverID int, apply *storage.Apply) error {
	stopped, err := s.storage.ApplyOperations().MarkPendingStoppedByApply(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("stop pending apply_operations for apply %s (%d): %w", apply.ApplyIdentifier, apply.ID, err)
	}
	if stopped > 0 {
		s.logger.Info("operator: stopped pending sibling operations for pending stop request",
			"driver", driverID, "apply_id", apply.ApplyIdentifier,
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
func (s *Service) completePendingStopIfApplyResolved(ctx context.Context, driverID int, applyID int64) error {
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
		"driver", driverID, "apply_id", apply.ApplyIdentifier,
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
func (s *Service) reconcileUnclaimableParent(ctx context.Context, driverID int, op *storage.ApplyOperation) {
	parent, err := s.storage.Applies().Get(ctx, op.ApplyID)
	if err != nil {
		s.logger.Error("operator: failed to load unclaimable parent apply; operation will be retried",
			"driver", driverID,
			"apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID,
			"deployment", op.Deployment,
			"error", err)
		metrics.RecordOperatorClaimFailure(ctx, "operation_parent_not_claimable")
		return
	}
	if parent == nil {
		s.logger.Error("operator: parent apply not found for claimed operation; operation will be retried",
			"driver", driverID,
			"apply_operation_id", op.ID,
			"apply_db_id", op.ApplyID,
			"deployment", op.Deployment)
		metrics.RecordOperatorClaimFailure(ctx, "operation_parent_not_claimable")
		return
	}
	if state.IsTerminalApplyState(parent.State) {
		s.reconcileClaimedOperationFromTerminalParent(ctx, driverID, op, parent)
		return
	}
	s.logger.Warn("operator: parent apply not claimable for operation; operation will be retried",
		"driver", driverID,
		"apply_operation_id", op.ID,
		"apply_id", parent.ApplyIdentifier,
		"deployment", op.Deployment,
		"environment", parent.Environment,
		"state", parent.State)
	metrics.RecordOperatorClaimFailure(ctx, "operation_parent_not_claimable")
}

func (s *Service) reconcileClaimedOperationFromTerminalParent(ctx context.Context, driverID int, op *storage.ApplyOperation, parent *storage.Apply) {
	s.logger.Info("operator: parent apply already terminal; reconciling operation to terminal state",
		"driver", driverID,
		"apply_operation_id", op.ID,
		"apply_id", parent.ApplyIdentifier,
		"deployment", op.Deployment,
		"environment", parent.Environment,
		"state", parent.State)
	marked, err := s.markOperationFromApplyState(ctx, driverID, op, parent)
	if err != nil {
		s.logger.Error("operator: failed to reconcile apply_operation from terminal parent; derived apply state not updated",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", parent.ApplyIdentifier,
			"deployment", op.Deployment, "environment", parent.Environment, "state", parent.State, "error", err)
		return
	}
	if !marked {
		return
	}
	if _, err := s.updateApplyStateFromOperations(ctx, driverID, parent, rejectFailedApplyReopen); err != nil {
		s.logger.Error("operator: failed to update derived apply state for terminal parent",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", parent.ApplyIdentifier,
			"deployment", op.Deployment, "environment", parent.Environment, "error", err)
		return
	}
}

// failOperationWithoutTasks terminalizes an operation whose drive failed closed
// because no tasks scope to it. Such a claim is invalid or stale: the operation
// can never make progress, so leaving the row non-terminal would re-lease it on
// every poll once its heartbeat goes stale. It marks the operation row failed
// under its own operation lease (opCtx), then re-derives the parent applies.state
// under the parent apply lease (applyCtx). The two writes target different rows
// with different guards, so they take separate lease-scoped contexts and fail
// closed if ownership has since changed.
func (s *Service) failOperationWithoutTasks(opCtx, applyCtx context.Context, driverID int, op *storage.ApplyOperation, apply *storage.Apply) {
	const reason = "operation has no tasks; invalid or stale claim"
	if err := s.storage.ApplyOperations().MarkFailed(opCtx, op.ID, reason); err != nil {
		s.logger.Error("operator: failed to mark task-less apply_operation failed; operation will be retried",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", apply.Environment, "error", err)
		return
	}
	result, err := s.updateApplyStateFromOperations(applyCtx, driverID, apply, allowLeaseScopedFailedReopen)
	if err != nil {
		s.logger.Error("operator: failed to update derived apply state after failing task-less operation",
			"driver", driverID, "apply_operation_id", op.ID, "apply_id", apply.ApplyIdentifier,
			"deployment", op.Deployment, "environment", apply.Environment, "error", err)
		return
	}

	// A task-less operation failure that terminalizes a multi-operation apply
	// publishes the summary here; the gate on OperationCount keeps the single-op
	// caller (which still publishes via its per-driver observer) unchanged.
	s.publishTerminalSummaryIfWon(applyCtx, driverID, apply, result)
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
func (s *Service) resumeClaimedApply(ctx context.Context, driverID int, apply *storage.Apply, applyOperationID int64, operationDeployment string) (bool, error) {
	return s.resumeClaimedApplyWithOptions(ctx, driverID, apply, applyOperationID, operationDeployment, resumeClaimedApplyOptions{})
}

// resumeClaimedApplyOptions tunes a drive for the multi-operation path.
type resumeClaimedApplyOptions struct {
	// suppressRecoveredObserver skips the per-driver progress/terminal observer
	// hook. A multi-operation drive owns only its operation; the aggregate
	// terminal summary is published once by the projection CAS winner, not per
	// deployment, so the per-driver observer must not fire.
	suppressRecoveredObserver bool
	// cutover routes the operation through ResumeApplyOperationCutover instead of
	// ResumeApplyOperation: it drives a single operation parked at the cutover
	// barrier through its high-risk swap (the deployment-ordered cutover claim)
	// rather than running its copy phase.
	cutover bool
}

func (s *Service) resumeClaimedApplyWithOptions(ctx context.Context, driverID int, apply *storage.Apply, applyOperationID int64, operationDeployment string, opts resumeClaimedApplyOptions) (bool, error) {
	lease := apply.Lease()
	start := s.clock.Now()

	// operationDeployment is observability attribution only — RoutingClient
	// reloads the operation row and routes by its own deployment. The
	// operation-claim path passes the claimed op's deployment so logs/metrics
	// name the deployment actually being driven; the legacy whole-apply path
	// passes "" and falls back to the apply's stored deployment. For single-op
	// applies the two are equal, so the attribution is unchanged.
	deployment := operationDeployment
	if deployment == "" {
		stored, err := storedDeploymentForApply(apply)
		if err != nil {
			s.logger.Error("operator: claimed apply is missing stored deployment metadata",
				"driver", driverID,
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"environment", apply.Environment,
				"error", err)
			metrics.RecordOperatorResumeFailure(ctx, apply.Database, "", apply.Environment, "missing_deployment")
			return false, err
		}
		deployment = stored
	}

	s.logger.Info("operator: claimed apply",
		"driver", driverID,
		"lease_owner", lease.Owner,
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"deployment", deployment,
		"environment", apply.Environment,
		"state", apply.State,
		"last_heartbeat", apply.UpdatedAt)

	// Record the claim in the apply's durable log so the timeline explains
	// why new state transitions appear after a failure or a driver crash —
	// without this entry, an operator reading apply_logs sees a gap between
	// the last failure and the resumed work.
	s.logApplyResumeClaim(ctx, driverID, apply)

	previousState := apply.State

	client, err := s.RoutingTernClient()
	if err != nil {
		s.logger.Error("operator: failed to get routing client",
			"driver", driverID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"deployment", deployment,
			"environment", apply.Environment,
			"error", err)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, deployment, apply.Environment, "no_client")
		return false, err
	}

	if s.OnApplyRecovered != nil && !opts.suppressRecoveredObserver {
		s.OnApplyRecovered(apply)
	}

	retryableClaim := previousState == state.Apply.FailedRetryable
	if retryableClaim {
		metrics.AdjustActiveApplies(ctx, 1, apply.Database, deployment, apply.Environment)
	}
	// The operation-claim path scopes the drive to the single deployment it
	// leased so sibling deployments are unaffected; ResumeApplyOperation fails
	// closed when no tasks scope to the operation. The cutover variant drives a
	// barrier-parked operation through its swap instead of its copy phase. The
	// legacy whole-apply path (applyOperationID == 0) drives every task.
	switch {
	case applyOperationID > 0 && opts.cutover:
		err = client.ResumeApplyOperationCutover(ctx, apply, applyOperationID)
	case applyOperationID > 0:
		err = client.ResumeApplyOperation(ctx, apply, applyOperationID)
	default:
		err = client.ResumeApply(ctx, apply)
	}
	if err != nil {
		if errors.Is(err, storage.ErrApplyLeaseLost) {
			s.logger.Warn("operator: apply lease was lost; driver will stop writing this apply",
				"driver", driverID,
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
				"driver", driverID,
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
				"driver", driverID,
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
			"driver", driverID,
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
		"driver", driverID,
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
// operator driver claimed the apply to resume it. Best-effort: a failed
// append must not block the resume, so the error is logged and the claim
// proceeds.
func (s *Service) logApplyResumeClaim(ctx context.Context, driverID int, apply *storage.Apply) {
	logStore := s.storage.ApplyLogs()
	if logStore == nil {
		s.logger.Warn("operator: no apply log store configured; apply claim will not appear in apply logs",
			"driver", driverID,
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
		Message:   fmt.Sprintf("Operator claimed apply to resume it (driver %d, state %s)", driverID, apply.State),
		OldState:  apply.State,
		NewState:  apply.State,
		CreatedAt: s.clock.Now(),
	}); err != nil {
		s.logger.Warn("operator: failed to log apply claim; apply claim will not appear in apply logs",
			"driver", driverID,
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
// operation lease cancels the run and the displaced driver stops; other
// heartbeat errors are logged and retried on the next tick.
// Returns a stop func that is safe to call more than once.
func (s *Service) startApplyOperationHeartbeat(ctx context.Context, driverID int, op *storage.ApplyOperation, apply *storage.Apply, cancelRun context.CancelFunc) func() {
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
						s.logger.Warn("operator: apply_operation heartbeat lost operation lease; driver will stop",
							"driver", driverID,
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
						"driver", driverID,
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
func (s *Service) markOperationFromApplyState(ctx context.Context, driverID int, op *storage.ApplyOperation, apply *storage.Apply) (updated bool, err error) {
	return s.persistOperationState(ctx, driverID, op, apply.State, apply.ErrorMessage)
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
func (s *Service) markOperationFromOwnResult(ctx context.Context, driverID int, op *storage.ApplyOperation) (updated bool, err error) {
	// A group_finalizer carries no tasks: its terminal state was written by the
	// drive (driveGroupFinalizer marks it completed only on an accepted apply,
	// failed otherwise). Deriving from its empty task set would overwrite that
	// outcome, so leave the row as the drive set it and let the parent derivation
	// read it.
	if op.OperationKind == storage.ApplyOperationKindGroupFinalizer {
		return true, nil
	}
	tasks, err := s.storage.Tasks().GetByApplyOperationID(ctx, op.ID)
	if err != nil {
		return false, fmt.Errorf("load tasks for apply_operation %d (deployment %q): %w", op.ID, op.Deployment, err)
	}
	if len(tasks) == 0 {
		apply, err := s.storage.Applies().Get(ctx, op.ApplyID)
		if err != nil {
			return false, fmt.Errorf("load parent apply for task-less apply_operation %d (deployment %q): %w", op.ID, op.Deployment, err)
		}
		if apply == nil {
			return false, fmt.Errorf("parent apply %d not found for task-less apply_operation %d (deployment %q)", op.ApplyID, op.ID, op.Deployment)
		}
		plan, err := s.storage.Plans().GetByID(ctx, apply.PlanID)
		if err != nil {
			return false, fmt.Errorf("load plan %d for task-less apply_operation %d (deployment %q): %w", apply.PlanID, op.ID, op.Deployment, err)
		}
		if plan != nil && op.OperationKind == storage.ApplyOperationKindWork && op.OperationKey == "" && len(plan.FlatDDLChanges()) == 0 && len(vschemaFinalizerNamespaces(plan)) > 0 {
			currentOp, getOpErr := s.storage.ApplyOperations().Get(ctx, op.ID)
			if getOpErr != nil {
				return false, fmt.Errorf("reload task-less apply_operation %d (deployment %q): %w", op.ID, op.Deployment, getOpErr)
			}
			if currentOp != nil && state.IsState(currentOp.State, state.ApplyOperation.Completed) {
				return true, nil
			}
			return s.persistOperationState(ctx, driverID, op, apply.State, apply.ErrorMessage)
		}
	}
	taskStates := make([]string, len(tasks))
	for i, t := range tasks {
		taskStates[i] = t.State
	}
	derived := state.DeriveApplyState(taskStates)
	return s.persistOperationState(ctx, driverID, op, derived, firstFailedTaskError(tasks))
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
func (s *Service) persistOperationState(ctx context.Context, driverID int, op *storage.ApplyOperation, derived, errorMessage string) (updated bool, err error) {
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
	case state.IsState(derived, state.Apply.WaitingForCutover):
		// Under an ordered-cutover policy the copy drive parks a deployment at the
		// barrier and releases it: persist waiting_for_cutover (completed_at nil,
		// the work is not done) so the row is durable and the deployment-ordered
		// cutover claim picks it up later. Without this the row would fall through
		// to the "leave claimable" default and the copy claim would re-drive it.
		if err := opStore.UpdateState(ctx, op.ID, derived); err != nil {
			return false, fmt.Errorf("update waiting_for_cutover apply_operation %d state (deployment %q): %w", op.ID, op.Deployment, err)
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
			"driver", driverID, "apply_operation_id", op.ID,
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

// applyProjectionResult reports what updateApplyStateFromOperations did to the
// parent apply. It lets callers key apply-level terminal side-effects (the
// single-publisher terminal summary in the multi-deployment fan-out work) off
// the projection outcome — "did this drive win the swap that terminalized the
// parent?" — rather than off the per-operation engine result. It carries no
// behavior today: every current caller discards it and inspects only the error.
type applyProjectionResult struct {
	// Swapped is true when the derived-state compare-and-swap actually advanced
	// the parent apply row. It is false for a no-op match (derived already
	// equals the current state with nothing to stamp) and for a lost race (the
	// CAS found the row already moved).
	Swapped bool
	// PreviousState is the parent apply state observed before the projection.
	PreviousState string
	// DerivedState is the state derived from the child apply_operations rows.
	DerivedState string
	// BecameTerminal is true when this projection won the swap and moved the
	// parent from a non-terminal previous state to a terminal derived state.
	BecameTerminal bool
	// OperationCount is the number of child apply_operations rows this projection
	// derived from. Callers use it to distinguish a legacy single-operation apply
	// (count 1, which still publishes its terminal summary via the per-driver
	// observer) from an aggregate multi-operation apply (count > 1, whose summary
	// is published once by the CAS winner) without re-listing operations.
	OperationCount int
}

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
func (s *Service) updateApplyStateFromOperations(ctx context.Context, driverID int, apply *storage.Apply, reopen failedApplyReopenPolicy) (applyProjectionResult, error) {
	ops, err := s.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return applyProjectionResult{}, fmt.Errorf("list apply_operations for apply %s (%d): %w", apply.ApplyIdentifier, apply.ID, err)
	}
	if len(ops) == 0 {
		return applyProjectionResult{}, fmt.Errorf("derive apply state for apply %s (%d): no apply_operations rows", apply.ApplyIdentifier, apply.ID)
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
	// the derived projection must be the held-running degraded state, and the
	// caller must hold the lease.
	reopensContinuableFailedRollout := bool(reopen) &&
		state.IsState(apply.State, state.Apply.Failed) &&
		state.IsState(base, state.Apply.Failed) &&
		state.IsState(derived, state.Apply.RunningDegraded)

	if state.IsTerminalApplyState(apply.State) && !state.IsTerminalApplyState(derived) && !reopensContinuableFailedRollout {
		return applyProjectionResult{}, fmt.Errorf("derive apply state for terminal apply %s (%d): child operations derive non-terminal state %q from parent state %q",
			apply.ApplyIdentifier, apply.ID, derived, apply.State)
	}

	// Stamp started_at when the projection first moves the parent out of a
	// pending state and no start time was recorded yet; UpdateDerivedState only
	// applies it while started_at is still NULL, so a recorded start is never
	// rewound. nil means "leave started_at as-is".
	var startedAt *time.Time
	if apply.StartedAt == nil && !state.IsState(derived, state.Apply.Pending) {
		now := s.clock.Now()
		startedAt = &now
	}

	if state.IsState(apply.State, derived) && startedAt == nil {
		s.logger.Debug("operator: derived apply state matches current; no update",
			"driver", driverID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment,
			"state", derived, "operation_count", len(ops))
		return applyProjectionResult{PreviousState: apply.State, DerivedState: derived, OperationCount: len(ops)}, nil
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

	swapped, err := s.storage.Applies().UpdateDerivedState(ctx, apply.ID, apply.State, derived, errorMessage, startedAt, completedAt)
	if err != nil {
		return applyProjectionResult{}, fmt.Errorf("update derived apply state for apply %s (%d) to %q: %w", apply.ApplyIdentifier, apply.ID, derived, err)
	}
	if !swapped {
		// Another drive advanced the apply between our read and write; our
		// projection is stale. Skip and let the next poll reconcile.
		s.logger.Info("operator: derived apply state write lost a race; skipping",
			"driver", driverID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment,
			"expected_state", apply.State, "derived_state", derived, "operation_count", len(ops))
		return applyProjectionResult{PreviousState: apply.State, DerivedState: derived, OperationCount: len(ops)}, nil
	}
	s.logger.Info("operator: updated derived apply state from apply_operations",
		"driver", driverID, "apply_id", apply.ApplyIdentifier,
		"database", apply.Database, "environment", apply.Environment,
		"previous_state", apply.State, "derived_state", derived, "operation_count", len(ops))

	// Own the active-apply gauge for multi-operation applies. The enqueue-time
	// increment is keyed to the parent's primary deployment, and operation-scoped
	// drives suppress the parent-level metric, so the projection that wins the
	// parent transition is the single point that must release it: -1 when the
	// rollout first reaches a terminal state, and +1 if a continuable failure
	// reopens the parent to keep it running, so the gauge tracks whether the
	// apply is still in flight. Single-operation applies keep decrementing in
	// their direct drive and are left untouched here.
	if len(ops) > 1 {
		switch {
		case !state.IsTerminalApplyState(apply.State) && state.IsTerminalApplyState(derived):
			metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
		case state.IsTerminalApplyState(apply.State) && !state.IsTerminalApplyState(derived):
			metrics.AdjustActiveApplies(ctx, 1, apply.Database, apply.Deployment, apply.Environment)
		}
	}

	return applyProjectionResult{
		Swapped:        true,
		PreviousState:  apply.State,
		DerivedState:   derived,
		BecameTerminal: !state.IsTerminalApplyState(apply.State) && state.IsTerminalApplyState(derived),
		OperationCount: len(ops),
	}, nil
}

// publishTerminalSummaryIfWon publishes the apply-level terminal summary exactly
// once, when this drive won the aggregate non-terminal→terminal projection CAS
// for a multi-operation apply. Single-operation applies (OperationCount <= 1)
// publish their summary through the per-driver observer and are skipped here, so
// this is a no-op on the legacy path. Because the parent apply is already
// durably terminal once result.BecameTerminal is true, publishing is best
// effort: every failure is logged with triage identifiers and counted, never
// reverted, and left for summary reconciliation to repair.
func (s *Service) publishTerminalSummaryIfWon(ctx context.Context, driverID int, apply *storage.Apply, result applyProjectionResult) {
	if !result.BecameTerminal || result.OperationCount <= 1 {
		return
	}
	if s.OnApplyTerminalSummary == nil {
		s.logger.Debug("operator: aggregate terminal summary publisher not configured; skipping",
			"driver", driverID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment,
			"derived_state", result.DerivedState, "operation_count", result.OperationCount)
		return
	}

	// Reload the parent at its terminal state: the input apply still carries the
	// pre-CAS state, while the summary must render the terminal state, error
	// message, and completion time the projection just stamped.
	terminalApply, err := s.storage.Applies().Get(ctx, apply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload terminal apply for aggregate summary; summary not published",
			"driver", driverID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment,
			"derived_state", result.DerivedState, "operation_count", result.OperationCount, "error", err)
		metrics.RecordOperatorTerminalSummaryFailure(ctx, "reload_apply_error")
		return
	}
	if terminalApply == nil {
		s.logger.Error("operator: terminal apply not found while publishing aggregate summary; summary not published",
			"driver", driverID, "apply_id", apply.ApplyIdentifier,
			"database", apply.Database, "environment", apply.Environment,
			"derived_state", result.DerivedState, "operation_count", result.OperationCount)
		metrics.RecordOperatorTerminalSummaryFailure(ctx, "apply_missing")
		return
	}
	if !state.IsTerminalApplyState(terminalApply.State) {
		s.logger.Error("operator: reloaded apply is no longer terminal while publishing aggregate summary; summary not published",
			"driver", driverID, "apply_id", terminalApply.ApplyIdentifier,
			"database", terminalApply.Database, "environment", terminalApply.Environment,
			"reloaded_state", terminalApply.State, "derived_state", result.DerivedState,
			"operation_count", result.OperationCount)
		metrics.RecordOperatorTerminalSummaryFailure(ctx, "apply_not_terminal_after_cas")
		return
	}

	// Reload every operation's tasks so the summary reflects the whole apply, not
	// just the operation this drive owned.
	tasks, err := s.storage.Tasks().GetByApplyID(ctx, terminalApply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload tasks for aggregate terminal summary; summary not published",
			"driver", driverID, "apply_id", terminalApply.ApplyIdentifier,
			"database", terminalApply.Database, "environment", terminalApply.Environment,
			"derived_state", result.DerivedState, "operation_count", result.OperationCount, "error", err)
		metrics.RecordOperatorTerminalSummaryFailure(ctx, "reload_tasks_error")
		return
	}

	if err := s.OnApplyTerminalSummary(ctx, terminalApply, tasks); err != nil {
		s.logger.Error("operator: aggregate terminal summary publish failed; parent state stays terminal, summary left for reconciliation",
			"driver", driverID, "apply_id", terminalApply.ApplyIdentifier,
			"database", terminalApply.Database, "environment", terminalApply.Environment,
			"derived_state", result.DerivedState, "operation_count", result.OperationCount, "error", err)
		metrics.RecordOperatorTerminalSummaryFailure(ctx, "callback_error")
		return
	}
	s.logger.Info("operator: published aggregate terminal summary for multi-operation apply",
		"driver", driverID, "apply_id", terminalApply.ApplyIdentifier,
		"database", terminalApply.Database, "environment", terminalApply.Environment,
		"derived_state", result.DerivedState, "operation_count", result.OperationCount)
}

func driverLeaseOwner(driverID int) string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s/%d/driver-%d", hostname, os.Getpid(), driverID)
}
