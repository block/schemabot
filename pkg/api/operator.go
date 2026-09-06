package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/panicsafe"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// Operator constants.
const (
	// OperatorPollInterval is the default interval for polling applies that need attention.
	OperatorPollInterval = 10 * time.Second

	// ApplyOperationHeartbeatInterval bounds how often the operation-row
	// heartbeat fires while ResumeApply runs. It is kept safely below
	// storage.ApplyLeaseStaleAfter so that a large (or misconfigured) operator
	// poll interval cannot let apply_operations.updated_at go stale and allow a
	// peer driver to re-claim the operation mid-resume. The effective cadence
	// is min(operatorPollInterval, ApplyOperationHeartbeatInterval).
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
	reaperEvery := s.strandedReaperEvery
	if reaperEvery <= 0 {
		reaperEvery = StrandedReaperInterval
	}
	s.stopRecovery = stop
	s.cancelRecovery = cancel
	s.operatorWake = wake
	s.operatorMu.Unlock()

	// Seed both occupancy gauges before any driver claims so the pool's
	// capacity is visible from startup: pool size minus busy, summed across
	// processes, is the claim capacity that remains.
	metrics.RecordOperatorDriverPoolSize(ctx, int64(driverCount))
	metrics.RecordOperatorDriversBusy(ctx, s.driversBusy.Load())

	for i := range driverCount {
		driverID := i
		s.recoveryWg.Go(func() {
			s.operatorDriver(driverCtx, driverID, stop, wake)
		})
	}

	// The reaper is maintenance, not claim work, so it runs on its own slow
	// cadence outside the driver pool. It shares the driver lifecycle: one
	// goroutine per process, stopped by StopOperator.
	s.recoveryWg.Go(func() {
		s.strandedReaperLoop(driverCtx, stop, reaperEvery)
	})

	// The fan-out cap only means something relative to the pool it bounds, and
	// an operator asking "why is my sharded apply only running two shards?" has
	// no other way to read the effective value — an omitted config key resolves
	// to the default in storage. Log the resolved cap beside the pool size.
	s.logger.Info("operator started",
		"drivers", driverCount,
		"max_drivers_per_apply", storage.BuildOptions(storage.WithMaxDriversPerApply(s.config.MaxDriversPerApply)).MaxDriversPerApply,
		"interval", s.operatorPollInterval,
		"stranded_reaper_interval", reaperEvery)
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

	// Stop claiming first, so no driver picks up new work while the in-flight
	// drives are being brought down.
	close(stop)

	// From here on a drive that returns leaves its claim registered. A drive
	// already past its stop-channel check can still take a claim after this
	// point, and a claim that registered and deregistered inside the shutdown
	// would otherwise be in neither the map nor any snapshot — handed back by
	// nobody, and idle for the whole staleness window.
	s.beginClaimDrain()

	// End the in-flight drives. A drive inside a claim only observes its
	// context, so cancelling it is what returns it; it exits leaving its apply
	// active for another driver rather than recording an operator stop.
	if cancel != nil {
		cancel()
	}
	s.recoveryWg.Wait()

	// A drive returning does not stop the engine it started. An engine that runs
	// its schema change in this process keeps copying and keeps the target's
	// lock, while nothing renews the apply's lease any more. Bring those engines
	// down before the process exits, so the lock is released rather than left
	// held for peer drivers that would reclaim the apply and be refused.
	halted := s.haltEnginesForShutdown()

	// Every drive has returned, so this collects the claims taken before the
	// stop signal and the late ones alike.
	held, heldOperations := s.drainHeldClaims()

	// Only now, with the targets released, hand the claims back. Releasing them
	// any earlier would invite a peer driver onto a target this process still
	// held; leaving them to go stale instead would idle the work for the whole
	// staleness window.
	// Hand the parent applies back before their operations. A driver reaches
	// work through its apply_operation, so releasing the operation first invites
	// a peer to claim it while this process still holds the parent apply — the
	// peer then finds an unclaimable parent and has to reconcile its way out
	// instead of simply resuming.
	s.releaseHeldClaims(held, halted)
	s.releaseHeldOperationClaims(heldOperations, halted)
}

// beginClaimDrain puts claim tracking into shutdown mode: a drive returning
// from here on leaves its claim registered for drainHeldClaims to collect.
func (s *Service) beginClaimDrain() {
	s.heldClaimsMu.Lock()
	s.heldClaimsDraining = true
	s.heldClaimsMu.Unlock()

	s.heldOperationClaimsMu.Lock()
	s.heldOperationClaimsDraining = true
	s.heldOperationClaimsMu.Unlock()
}

// drainHeldClaims takes every registered claim and leaves tracking ready for a
// later StartOperator, which is what lets a service be stopped and started
// again without the drain outliving the shutdown that opened it.
func (s *Service) drainHeldClaims() ([]heldClaim, []heldOperationClaim) {
	s.heldClaimsMu.Lock()
	applies := slices.Collect(maps.Values(s.heldClaims))
	clear(s.heldClaims)
	s.heldClaimsDraining = false
	s.heldClaimsMu.Unlock()

	s.heldOperationClaimsMu.Lock()
	operations := slices.Collect(maps.Values(s.heldOperationClaims))
	clear(s.heldOperationClaims)
	s.heldOperationClaimsDraining = false
	s.heldOperationClaimsMu.Unlock()

	return applies, operations
}

// heldClaim is an apply lease a driver is driving under, carried alongside the
// apply's triage identity. Shutdown reloads each apply before handing its claim
// back, and the reload is exactly the step that can fail — so the identity is
// captured at claim time, when the apply is in hand, rather than looked up when
// it may be unavailable.
type heldClaim struct {
	lease storage.ApplyLease
	// deployment routes the claim to the engine that holds its target's lock, so
	// shutdown can tell whether that engine came down before handing it back.
	deployment string
	logAttrs   []any
}

// trackHeldClaim registers the claim a drive is running under and returns the
// function that deregisters it when the drive returns. Shutdown snapshots the
// registered claims so it can hand back the ones still being driven when the
// process goes away.
func (s *Service) trackHeldClaim(apply *storage.Apply) func() {
	lease := apply.Lease()
	if !lease.Valid() {
		// An invalid lease authorizes no write, so there is no claim to hand
		// back. The drive's own lease validation reports the cause.
		return func() {}
	}
	if !storage.LeaseOwnedByThisProcess(lease.Owner) {
		// An operation-lease drive reads its parent apply without claiming it,
		// so the row can carry a peer's lease. Handing that back would backdate
		// a lease this process never held, out from under the peer still
		// driving its own sibling operations.
		s.logger.Debug("operator: parent apply is leased to another process; its claim is that process's to hand back",
			append(apply.IdentityLogAttrs(), "lease_owner", lease.Owner)...)
		return func() {}
	}
	s.heldClaimsMu.Lock()
	if s.heldClaims == nil {
		s.heldClaims = make(map[int64]heldClaim)
	}
	s.heldClaims[lease.ApplyID] = heldClaim{lease: lease, deployment: apply.Deployment, logAttrs: apply.IdentityLogAttrs()}
	s.heldClaimsMu.Unlock()

	return func() {
		s.heldClaimsMu.Lock()
		defer s.heldClaimsMu.Unlock()
		if s.heldClaimsDraining {
			// Shutdown is collecting the claims to hand back, and a drive
			// returning now still leaves its apply active for a peer to pick up.
			return
		}
		// Delete only this claim: a peer that rotated the lease onto itself is
		// recorded under a different token, and this drive must not deregister
		// a claim it no longer owns.
		if current, ok := s.heldClaims[lease.ApplyID]; ok && current.lease.Token == lease.Token {
			delete(s.heldClaims, lease.ApplyID)
		}
	}
}

// heldClaimsSnapshot returns the claims this process is currently driving under.
func (s *Service) heldClaimsSnapshot() []heldClaim {
	s.heldClaimsMu.Lock()
	defer s.heldClaimsMu.Unlock()
	return slices.Collect(maps.Values(s.heldClaims))
}

// releaseHeldClaims hands back the claims this process was driving under, so a
// peer driver picks the work up on its next poll rather than waiting out the
// staleness window. An apply that settled while shutdown was in progress is
// left alone: it has nothing to hand over, and backdating its heartbeat would
// misreport when it finished.
func (s *Service) releaseHeldClaims(claims []heldClaim, halted engineHaltOutcome) {
	if len(claims) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownHaltTimeout)
	defer cancel()

	for _, claim := range claims {
		attrs := append(slices.Clone(claim.logAttrs), "lease_owner", claim.lease.Owner)
		if !halted.haltedCleanly(claim.deployment) {
			s.logger.Warn("operator: this apply's engine did not come down, so its claim is left to go stale; handing it back now would put a driver onto a target this process still holds the lock on",
				attrs...)
			continue
		}
		apply, err := s.storage.Applies().Get(ctx, claim.lease.ApplyID)
		if err != nil {
			s.logger.Error("operator: could not read a claimed apply while handing back claims on shutdown; the apply stays claimed until its lease goes stale, delaying the driver that would resume it",
				append(attrs, "error", err)...)
			continue
		}
		if apply == nil {
			s.logger.Debug("operator: a claimed apply no longer exists; nothing to hand back", attrs...)
			continue
		}
		if state.IsTerminalApplyState(apply.State) {
			s.logger.Debug("operator: claimed apply settled before shutdown handed its claim back",
				append(apply.LogAttrs(), "lease_owner", claim.lease.Owner)...)
			continue
		}
		released, err := s.storage.Applies().ReleaseClaim(ctx, claim.lease)
		if err != nil {
			s.logger.Error("operator: could not hand back a claimed apply on shutdown; the apply stays claimed until its lease goes stale, delaying the driver that would resume it",
				append(apply.LogAttrs(), "lease_owner", claim.lease.Owner, "error", err)...)
			continue
		}
		if !released {
			s.logger.Debug("operator: a claimed apply's lease had already moved on; nothing to hand back",
				append(apply.LogAttrs(), "lease_owner", claim.lease.Owner)...)
			continue
		}
		s.logger.Info("operator: handed a claimed apply back on shutdown; it is claimable again on the next poll",
			append(apply.LogAttrs(), "lease_owner", claim.lease.Owner)...)
	}
}

// heldOperationClaim is an operation lease a driver is driving under, carried
// alongside the operation's triage identity for the same reason heldClaim
// carries the apply's: shutdown hands the claim back without reloading the row.
type heldOperationClaim struct {
	lease storage.OperationLease
	// deployment routes the claim to the engine that holds its target's lock,
	// the same gate the parent apply's claim passes through.
	deployment string
	logAttrs   []any
}

// trackHeldOperationClaim registers the operation claim a drive is running
// under and returns the function that deregisters it when the drive returns.
// Every claimed drive passes through here, so this is the one registration that
// covers single-operation, multi-operation, and task-less operation drives.
func (s *Service) trackHeldOperationClaim(op *storage.ApplyOperation, lease storage.OperationLease) func() {
	if !lease.Valid() {
		// An invalid lease authorizes no write, so there is no claim to hand
		// back. The caller's own lease validation reports the cause.
		return func() {}
	}
	s.heldOperationClaimsMu.Lock()
	if s.heldOperationClaims == nil {
		s.heldOperationClaims = make(map[int64]heldOperationClaim)
	}
	s.heldOperationClaims[lease.OperationID] = heldOperationClaim{lease: lease, deployment: op.Deployment, logAttrs: op.LogAttrs()}
	s.heldOperationClaimsMu.Unlock()

	return func() {
		s.heldOperationClaimsMu.Lock()
		defer s.heldOperationClaimsMu.Unlock()
		if s.heldOperationClaimsDraining {
			// Shutdown is collecting the claims to hand back, and a drive
			// returning now still leaves its operation active for a peer.
			return
		}
		// Delete only this claim: a peer that rotated the lease onto itself is
		// recorded under a different token, and this drive must not deregister
		// a claim it no longer owns.
		if current, ok := s.heldOperationClaims[lease.OperationID]; ok && current.lease.Token == lease.Token {
			delete(s.heldOperationClaims, lease.OperationID)
		}
	}
}

// heldOperationClaimsSnapshot returns the operation claims this process is
// currently driving under.
func (s *Service) heldOperationClaimsSnapshot() []heldOperationClaim {
	s.heldOperationClaimsMu.Lock()
	defer s.heldOperationClaimsMu.Unlock()
	return slices.Collect(maps.Values(s.heldOperationClaims))
}

// releaseHeldOperationClaims hands back the operation claims this process was
// driving under. A driver's next poll reaches an apply through
// FindNextApplyOperation, so an operation left claimed idles the work for the
// whole staleness window even when the parent apply is already claimable.
func (s *Service) releaseHeldOperationClaims(claims []heldOperationClaim, halted engineHaltOutcome) {
	if len(claims) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownHaltTimeout)
	defer cancel()

	for _, claim := range claims {
		attrs := append(slices.Clone(claim.logAttrs), "lease_owner", claim.lease.Owner)
		if !halted.haltedCleanly(claim.deployment) {
			s.logger.Warn("operator: this operation's engine did not come down, so its claim is left to go stale; handing it back now would put a driver onto a target this process still holds the lock on",
				attrs...)
			continue
		}
		released, err := s.storage.ApplyOperations().ReleaseClaim(ctx, claim.lease)
		if err != nil {
			s.logger.Error("operator: could not hand back a claimed apply_operation on shutdown; the operation stays claimed until its lease goes stale, delaying the driver that would resume it",
				append(attrs, "error", err)...)
			continue
		}
		if !released {
			s.logger.Debug("operator: a claimed apply_operation's lease had already moved on; nothing to hand back", attrs...)
			continue
		}
		s.logger.Info("operator: handed a claimed apply_operation back on shutdown; it is claimable again on the next poll", attrs...)
	}
}

// shutdownHaltTimeout bounds how long shutdown waits for this process's engines
// to come down. It must stay well inside storage.ApplyLeaseStaleAfter: once the
// lease goes stale, peer drivers reclaim the apply, and a target still held by
// an engine that has not come down refuses every one of them.
const shutdownHaltTimeout = 20 * time.Second

// haltEnginesForShutdown brings down every engine this process drives schema
// changes with. Clients that delegate to a separate Tern service are skipped:
// that service owns its engines and halts them on its own shutdown.
//
// Every client is attempted even after one fails, so one target that will not
// come down does not leave the rest held.
// engineHaltOutcome records which of this process's engines came down. Handing a
// claim back is only safe once the engine holding its target's lock is down, so
// shutdown consults this before releasing each claim.
type engineHaltOutcome struct {
	// failedDeployments names the deployments whose engine did not come down.
	failedDeployments map[string]bool
	// unattributedFailure records a halt failure on a client this process cannot
	// attribute to a deployment, which leaves no claim provably safe.
	unattributedFailure bool
}

// haltedCleanly reports whether the engine serving this deployment is down, and
// so whether a peer driver can safely be handed its work.
func (o engineHaltOutcome) haltedCleanly(deployment string) bool {
	if o.unattributedFailure {
		return false
	}
	if deployment == "" {
		deployment = DefaultDeployment
	}
	return !o.failedDeployments[deployment]
}

func (s *Service) haltEnginesForShutdown() engineHaltOutcome {
	// The drive contexts are already cancelled by this point, so the halt runs
	// on a fresh bounded context rather than one that is guaranteed expired.
	ctx, cancel := context.WithTimeout(context.Background(), shutdownHaltTimeout)
	defer cancel()

	s.ternMu.Lock()
	clients := maps.Clone(s.ternClients)
	defaultClient := s.defaultTernClient
	s.ternMu.Unlock()

	outcome := engineHaltOutcome{failedDeployments: make(map[string]bool)}
	for key, client := range clients {
		if err := s.haltEngine(ctx, client); err != nil {
			outcome.failedDeployments[deploymentFromTernClientKey(key)] = true
		}
	}
	if defaultClient != nil {
		if err := s.haltEngine(ctx, defaultClient); err != nil {
			outcome.unattributedFailure = true
		}
	}
	return outcome
}

// haltEngine brings one client's in-process engines down. A client that drives
// its schema changes elsewhere owns no engines here, so it reports no failure.
func (s *Service) haltEngine(ctx context.Context, client tern.Client) error {
	halter, ok := client.(tern.ShutdownHalter)
	if !ok {
		s.logger.Debug("tern client drives its schema changes outside this process; nothing to halt for shutdown",
			"endpoint", client.Endpoint())
		return nil
	}
	if err := halter.HaltForShutdown(ctx); err != nil {
		metrics.RecordOperatorShutdownHaltFailure(ctx)
		s.logger.Error("engine did not come down on shutdown; its target stays locked while this process no longer renews the apply's lease, so its claims are left in place to go stale rather than handed to a driver that would be refused the lock",
			"endpoint", client.Endpoint(),
			"halt_timeout", shutdownHaltTimeout,
			"error", err)
		return err
	}
	return nil
}

// deploymentFromTernClientKey recovers the deployment from a tern client's
// registry key, which is the deployment and environment joined by a slash.
func deploymentFromTernClientKey(key string) string {
	deployment, _, found := strings.Cut(key, "/")
	if !found {
		return key
	}
	return deployment
}

func (s *Service) wakeOperator(applyIdentifier, database, environment string) {
	s.operatorMu.Lock()
	wake := s.operatorWake
	running := s.stopRecovery != nil
	s.operatorMu.Unlock()

	if !running || wake == nil {
		// A wake reaches here for every kind of queued durable work — a queued
		// apply dispatch as well as control requests — and every drive runs under
		// an operator claim, so with no operator running none of it will be
		// picked up. Warn so an operator-less deployment shape is visible before
		// someone has to debug a queued apply that never starts.
		s.logger.Warn("operator wake skipped because the operator is not running; queued applies and control requests will not be driven until the operator starts",
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

	s.driveTick(ctx, driverID)

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
			s.driveTick(ctx, driverID)
		case <-ticker.C:
			s.driveTick(ctx, driverID)
		}
	}
}

// driveTick runs one claim-and-drive pass behind a panic boundary so a
// poisoned claim degrades only this tick: the panic is logged with its stack
// and counted, and the driver keeps polling. Panics raised inside the engine
// drive are already converted to errors (and fail the claimed apply) at the
// resumeClaimedApply seam, so a panic reaching this boundary comes from the
// claim or projection machinery itself and leaves no apply marked failed —
// that work is retried on a later tick.
func (s *Service) driveTick(ctx context.Context, driverID int) {
	// The driver's select can pick a ready ticker over an equally ready
	// ctx.Done(), so a tick can start after the operator has already been told
	// to stop. Every claim it made would fail against the cancelled context and
	// report a failure the successor driver is about to retry.
	if ctx.Err() != nil {
		s.logger.Debug("operator: skipping the claim ladder; the operator is shutting down", "driver", driverID)
		return
	}
	err := panicsafe.Call(func() error {
		s.recoverApplies(ctx, driverID)
		return nil
	})
	if err == nil {
		return
	}
	var tickPanic *panicsafe.Error
	if !errors.As(err, &tickPanic) {
		// recoverApplies handles its own failures and never returns an error, so
		// only a contained panic reaches here today; keep the signal if that
		// invariant ever changes.
		s.logger.Error("operator: drive tick failed", "driver", driverID, "error", err)
		return
	}
	s.logger.Error("operator: drive tick panicked; the driver continues and retries on the next tick",
		"driver", driverID,
		"panic", fmt.Sprint(tickPanic.Value),
		"stack", string(tickPanic.Stack))
	metrics.RecordRecoveredPanic(ctx, "operator_tick")
}

// logClaimFailure reports a failed claim or maintenance pass, separating the
// two ways a rung of the ladder fails. A pass cut short by shutdown is a
// routine deploy: the driver context is cancelled between polls, so whichever
// rung was mid-query reports it and the rungs behind it report the same
// cancellation moments later, while a successor driver reclaims all of it. Like
// the reaper's sweeps, that ending may not tick the claim-failure counter
// operators alert on, and there is no operator action it could name. Every
// other failure keeps its own reason and stays an error.
//
// The shutdown test is the driver context rather than the error, so a
// cancellation that did not come from shutdown still reports as a real failure.
//
// The shutdown branch logs at Debug, not Warn, because every deploy cancels
// every rung of every driver mid-ladder — any louder level rebuilds the noise
// floor this split exists to remove. The cost is that a failure which
// manifests only under shutdown leaves no default-level trace; a genuine,
// persisting failure is not that case, because the successor's claim replays
// it on a live context and reports it at Error with the counter. Chasing the
// shutdown-only class means enabling Debug, not promoting this branch.
func (s *Service) logClaimFailure(ctx context.Context, msg, reason string, attrs ...any) {
	if ctx.Err() != nil {
		s.logger.Debug(msg+"; the operator is shutting down and a successor driver will retry the claim", attrs...)
		return
	}
	s.logger.Error(msg, attrs...)
	metrics.RecordOperatorClaimFailure(ctx, reason)
}

// expireRetryableApplies runs the retryable-apply expiry maintenance pass,
// terminalizing failed_retryable applies that have exhausted their attempts or
// freshness window. It is best-effort: a storage error is logged and recorded
// but not returned, so the caller can still claim new work in the same tick.
func (s *Service) expireRetryableApplies(ctx context.Context, driverID int) {
	expired, err := s.storage.Applies().ExpireRetryable(ctx)
	if err != nil {
		s.logClaimFailure(ctx, "operator: failed to expire retryable applies", "expire_retryable_error",
			"driver", driverID, "error", err)
		return
	}
	for _, expiration := range expired {
		apply := expiration.Apply
		s.logger.Error("operator: retryable apply expired",
			append(apply.LogAttrs(),
				"driver", driverID,
				"attempt", apply.Attempt,
				"reason", expiration.Reason)...)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, apply.Deployment, apply.Environment, string(expiration.Reason))
		s.logApplyExpiration(ctx, apply, expiration.Reason)
	}
}

// logApplyExpiration appends a durable apply log entry recording that operator
// recovery gave up on the apply. Expiry is what makes a retryable failure
// permanent, so without it the apply log ends on the last paused attempt and an
// operator reading the CLI or the PR summary sees the apply reach a terminal
// state with nothing stating why. Best-effort: a failed append must not stop
// the driver from expiring the remaining applies.
func (s *Service) logApplyExpiration(ctx context.Context, apply *storage.Apply, reason storage.RetryableExpirationReason) {
	s.appendApplyLog(ctx, s.logger, &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelError,
		EventType: storage.LogEventError,
		Source:    storage.LogSourceSchemaBot,
		Message: fmt.Sprintf("Operator recovery gave up on the apply after %d of %d attempts (%s); it will not be retried automatically",
			apply.Attempt, storage.MaxRecoveryAttempts, reason),
		OldState:  state.Apply.FailedRetryable,
		NewState:  state.Apply.Failed,
		CreatedAt: s.clock.Now(),
	}, "why recovery stopped", apply.LogAttrs()...)
}

// appendApplyLog writes one entry to the apply's own log stream, bounded so a
// slow store cannot stall the driver. It is best-effort by contract: every
// caller has already done the work the entry describes, and an entry that
// cannot be written must not undo it. record names what an operator loses when
// the entry does not land, so the warning says which part of the apply's
// account is missing rather than only that a write failed.
func (s *Service) appendApplyLog(ctx context.Context, logger *slog.Logger, entry *storage.ApplyLog, record string, logAttrs ...any) {
	logStore := s.storage.ApplyLogs()
	if logStore == nil {
		logger.Warn("operator: no apply log store configured; the apply's own log will not state "+record, logAttrs...)
		return
	}
	logCtx, cancel := context.WithTimeout(ctx, ApplyClaimLogTimeout)
	defer cancel()
	if err := logStore.Append(logCtx, entry); err != nil {
		logger.Warn("operator: failed to append to the apply log; the apply's own log will not state "+record,
			append(slices.Clone(logAttrs), "error", err)...)
	}
}

// markDriverBusy records that a driver's claim succeeded and it now holds
// work, and returns the function that marks the driver idle again when the
// drive returns. Callers defer the returned function immediately after a
// successful claim so every exit path — a completed drive, an invalid lease,
// or a mid-drive error — releases the slot in the busy gauge.
func (s *Service) markDriverBusy(ctx context.Context) func() {
	metrics.RecordOperatorDriversBusy(ctx, s.driversBusy.Add(1))
	return func() {
		metrics.RecordOperatorDriversBusy(ctx, s.driversBusy.Add(-1))
	}
}

// recoverApplies claims and resumes work that needs attention. Each call
// claims at most one unit — a pending-stop reconciliation, a barrier-parked
// cutover, or an apply operation — to keep the drive loop responsive.
func (s *Service) recoverApplies(ctx context.Context, driverID int) {
	// Retryable-apply expiry is best-effort maintenance: run it first, but a
	// storage failure here must not stop the driver from claiming new pending
	// work in the same tick, or a transient expiry error would starve every
	// queued apply behind it.
	s.expireRetryableApplies(ctx, driverID)

	owner := driverLeaseOwner(driverID)

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
	// Settle a parent left behind its own children before claiming new
	// operation work: until it settles, the one-active-apply guard blocks
	// every new apply for that target, so the repair unblocks a database
	// rather than merely tidying a row. It matches nothing while any
	// operation is still non-terminal or any driver is still heartbeating,
	// so on a healthy plane this is a cheap no-op.
	if s.recoverApplyOperationProjection(ctx, driverID, owner) {
		return
	}
	s.recoverApplyOperation(ctx, driverID, owner)
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
		s.logClaimFailure(ctx, "operator: failed to claim apply_operation", "operation_storage_error",
			"driver", driverID, "lease_owner", owner, "error", err)
		return
	}
	if op == nil {
		s.logger.Debug("operator: no apply_operation to claim", "driver", driverID)
		return
	}
	defer s.markDriverBusy(ctx)()

	// The claim rotated a fresh operation lease onto the row. It is the
	// capability that guards this operation's own writes — its state
	// transitions, heartbeat, and task updates — so fail closed if it is
	// missing rather than silently degrading to the parent apply lease.
	opLease := op.Lease()
	if !opLease.Valid() {
		s.logger.Error("operator: claimed apply_operation without a valid operation lease token; operation will not be driven",
			append(op.LogAttrs(),
				"driver", driverID,
				"lease_owner", owner)...)
		metrics.RecordOperatorClaimFailure(ctx, "missing_operation_lease_token")
		return
	}
	// Registered once here, before the branch: every drive below runs under this
	// operation lease, and shutdown has to hand it back whichever path it took.
	defer s.trackHeldOperationClaim(op, opLease)()

	// Choose the drive mode. A single-operation apply keeps the legacy
	// parent-lease drive byte-for-byte. A multi-operation apply — by attached
	// rows or by a generation manifest still expecting siblings — drives under
	// the operation lease only: siblings must not serialize on a shared parent
	// lease, and the parent applies.state is moved solely by the projection CAS.
	ops, err := s.storage.ApplyOperations().ListByApply(ctx, op.ApplyID)
	if err != nil {
		s.logClaimFailure(ctx, "operator: failed to list operations for claimed apply; operation will not be driven", "operation_set_list_error",
			append(op.LogAttrs(),
				"driver", driverID, "lease_owner", owner, "error", err)...)
		return
	}
	if !operationSetContainsID(ops, op.ID) {
		s.logger.Error("operator: claimed operation is not part of its apply's operation set; operation will not be driven",
			append(op.LogAttrs(),
				"driver", driverID, "lease_owner", owner, "operation_count", len(ops))...)
		metrics.RecordOperatorClaimFailure(ctx, "operation_set_missing")
		return
	}
	if len(ops) > 1 {
		s.recoverMultiApplyOperation(ctx, driverID, op, opLease)
		return
	}
	// One attached operation does not prove a single-operation apply: a
	// deployment-keyed apply's operations attach one dispatch at a time, and
	// its generation manifest is the authority for whether siblings are still
	// on their way. Consult it before choosing the drive mode — a parent-lease
	// drive of the first-attached operation would terminalize the parent apply
	// while the manifest still expects siblings, and every later dispatch would
	// then be refused against the terminal apply.
	apply, err := s.storage.Applies().Get(ctx, op.ApplyID)
	if err != nil {
		s.logClaimFailure(ctx, "operator: failed to load parent apply for the drive-mode decision; operation will not be driven", "operation_parent_load_error",
			append(op.LogAttrs(),
				"driver", driverID, "lease_owner", owner, "error", err)...)
		return
	}
	if apply == nil {
		s.logger.Error("operator: parent apply not found for the drive-mode decision; operation will not be driven",
			append(op.LogAttrs(),
				"driver", driverID, "lease_owner", owner)...)
		metrics.RecordOperatorClaimFailure(ctx, "operation_parent_missing")
		return
	}
	if missing := apply.MissingExpectedOperationKeys(ops); len(missing) > 0 {
		s.logger.Info("operator: generation manifest expects operations that have not attached; driving under the operation lease so the projection owns the parent",
			append(apply.LogAttrs(),
				"driver", driverID,
				"lease_owner", owner,
				"operation_key", op.OperationKey,
				"operation_deployment", op.Deployment,
				"missing_operation_count", len(missing))...)
		s.recoverMultiApplyOperation(ctx, driverID, op, opLease)
		return
	}
	hasTasks, err := s.claimedOperationHasTasks(ctx, op)
	if err != nil {
		s.logClaimFailure(ctx, "operator: failed to inspect claimed operation tasks; operation will not be driven", "operation_task_inspect_error",
			append(op.LogAttrs(),
				"driver", driverID, "lease_owner", owner, "error", err)...)
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
		s.logClaimFailure(ctx, "operator: failed to claim parent apply for operation", "operation_parent_claim_error",
			append(op.LogAttrs(),
				"driver", driverID,
				"lease_owner", owner,
				"error", err)...)
		return
	}
	if apply == nil {
		// The parent apply is not currently claimable. Distinguish the two
		// reasons, because they need opposite handling:
		//   - terminal parent: the operation row was just leased by
		//     FindNextApplyOperation, so leaving it non-terminal would re-claim
		//     it forever. Reconcile it to the parent's terminal state now.
		//   - transiently unclaimable (a peer holds a fresh lease, or the row is
		//     locked): release the operation lease so the next poll retries,
		//     instead of squatting on it until it goes stale.
		s.reconcileUnclaimableParent(ctx, driverID, op, opLease)
		return
	}

	applyLease := apply.Lease()
	if !applyLease.Valid() {
		s.logger.Error("operator: claimed parent apply without a valid lease token; operation will not be driven",
			append(apply.LogAttrs(),
				"driver", driverID,
				"lease_owner", owner,
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment)...)
		metrics.RecordOperatorClaimFailure(ctx, "missing_lease_token")
		return
	}

	// The parent claim above is a second transaction, so a peer can rotate this
	// operation's lease onto itself between the two. Driving on would run engine
	// work under a capability this driver no longer holds, and surface as
	// whichever operation-scoped write happened to be attempted first — a
	// re-plan, a task transition — rather than as the claim race it is. Confirm
	// the operation lease survived the gap and hand the parent back when it did
	// not, so the peer holding it drives instead.
	if verdict := s.recheckOperationLease(ctx, driverID, op, opLease); verdict != operationLeaseHeld {
		s.releaseParentApplyClaim(ctx, driverID, apply, op)
		// An unproven lease is still this driver's: storage could not answer,
		// but nothing showed the lease gone. Hand it back rather than sit on a
		// row this drive is abandoning, or the claim query's heartbeat gate
		// holds the operation for a full staleness window over one failed read.
		//
		// It goes back second because releasing it backdates the row past the
		// staleness window, which makes the operation claimable again. Released
		// first, a peer would take it while this driver still held a fresh
		// parent lease, get nothing from ClaimApplyByID, and hand the row
		// straight back — a wasted round trip recorded as parent contention.
		// The reverse order leaves nothing claimable in between: the operation's
		// heartbeat is fresh until this driver gives it up.
		if verdict == operationLeaseUnproven {
			s.releaseOperationClaimBeforeDrive(ctx, driverID, op, opLease)
		}
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
			append(apply.LogAttrs(),
				"driver", driverID,
				"lease_owner", owner,
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment)...)
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
			return
		}
		var drivePanic *panicsafe.Error
		if !errors.As(resumeErr, &drivePanic) {
			// Transient drive failures leave the operation claimable so a later
			// poll retries it; resumeClaimedApply already logged the cause.
			return
		}
		// A contained drive panic already failed this operation's tasks and the
		// parent apply. Fall through so the operation row is persisted from its
		// now-failed tasks immediately instead of waiting to be re-leased once
		// its heartbeat goes stale.
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
			append(apply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID, "operation_deployment", op.Deployment, "error", err)...)
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
			append(apply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID, "operation_deployment", op.Deployment, "error", err)...)
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
			append(apply.LogAttrs(),
				"driver", driverID,
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment,
				"error", err)...)
		return
	}
	if finalApply == nil {
		s.logger.Error("operator: parent apply not found after resume; derived apply state not updated",
			append(apply.LogAttrs(),
				"driver", driverID,
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment)...)
		return
	}

	if _, err := s.updateApplyStateFromOperations(applyLeaseCtx, driverID, finalApply, allowLeaseScopedFailedReopen); err != nil {
		s.logger.Error("operator: failed to update derived apply state from apply_operations",
			append(finalApply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID, "operation_deployment", op.Deployment, "error", err)...)
		return
	}

	// If the derived state above settled the apply terminally and a stop or
	// cancel is still pending, complete it now so the request does not linger
	// after the rollout has resolved.
	if err := s.completePendingControlRequestsIfApplyResolved(applyLeaseCtx, driverID, finalApply.ID); err != nil {
		s.logger.Error("operator: failed to complete pending control requests for resolved apply",
			append(finalApply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID, "operation_deployment", op.Deployment, "error", err)...)
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
		s.logClaimFailure(ctx, "operator: failed to load parent apply for operation drive; operation will not be driven", "operation_parent_load_error",
			append(op.LogAttrs(),
				"driver", driverID, "error", err)...)
		return
	}
	if apply == nil {
		s.logger.Error("operator: parent apply not found for claimed operation; operation will not be driven",
			append(op.LogAttrs(),
				"driver", driverID)...)
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
			append(apply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID)...)
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
			append(apply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID,
				"operation_deployment", op.Deployment, "error", err)...)
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
				append(apply.LogAttrs(),
					"driver", driverID, "apply_operation_id", op.ID,
					"operation_deployment", op.Deployment, "error", reloadErr)...)
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
			append(apply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID,
				"operation_deployment", op.Deployment, "error", err)...)
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
			append(apply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID,
				"operation_deployment", op.Deployment, "error", err)...)
		return
	}

	// Reload the parent so the projection re-derives against the durable
	// apply.State (its terminal-to-non-terminal guard), then move it via the CAS.
	finalApply, err := s.storage.Applies().Get(operationLeaseCtx, apply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload parent apply after operation drive; derived apply state not updated",
			append(apply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID,
				"operation_deployment", op.Deployment, "error", err)...)
		return
	}
	if finalApply == nil {
		s.logger.Error("operator: parent apply not found after operation drive; derived apply state not updated",
			append(apply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID,
				"operation_deployment", op.Deployment)...)
		return
	}

	result, err := s.updateApplyStateFromOperations(operationLeaseCtx, driverID, finalApply, allowLeaseScopedFailedReopen)
	if err != nil {
		s.logger.Error("operator: failed to update derived apply state from apply_operations",
			append(finalApply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID,
				"operation_deployment", op.Deployment, "error", err)...)
		return
	}

	// Publish the apply-level terminal summary if this drive's projection won the
	// swap that terminalized the parent. Do this before control-request cleanup:
	// the summary depends only on the apply being terminal, and a later cleanup
	// error must not suppress it.
	s.publishTerminalSummaryIfWon(operationLeaseCtx, driverID, finalApply, result)

	if err := s.completePendingControlRequestsIfApplyResolved(operationLeaseCtx, driverID, finalApply.ID); err != nil {
		s.logger.Error("operator: failed to complete pending control requests for resolved apply",
			append(finalApply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID,
				"operation_deployment", op.Deployment, "error", err)...)
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
		s.logClaimFailure(ctx, "operator: failed to claim apply_operation cutover", "operation_cutover_storage_error",
			"driver", driverID, "lease_owner", owner, "error", err)
		return true
	}
	if op == nil {
		s.logger.Debug("operator: no apply_operation to cut over", "driver", driverID)
		return false
	}
	defer s.markDriverBusy(ctx)()

	// The claim rotated a fresh operation lease onto the row; it is the
	// capability that guards this operation's writes, so fail closed if missing.
	opLease := op.Lease()
	if !opLease.Valid() {
		s.logger.Error("operator: claimed apply_operation cutover without a valid operation lease token; operation will not be driven",
			append(op.LogAttrs(),
				"driver", driverID, "lease_owner", owner)...)
		metrics.RecordOperatorClaimFailure(ctx, "missing_operation_cutover_lease_token")
		return true
	}

	// The cutover predicate already gates to multi-operation barrier applies, but
	// the operation-lease-only drive (and its parent-write suppression) is only
	// correct for a genuine multi-operation apply, so verify the set before
	// driving rather than trusting the claim alone.
	ops, err := s.storage.ApplyOperations().ListByApply(ctx, op.ApplyID)
	if err != nil {
		s.logClaimFailure(ctx, "operator: failed to list operations for claimed cutover; operation will not be driven", "operation_cutover_set_list_error",
			append(op.LogAttrs(),
				"driver", driverID, "lease_owner", owner, "error", err)...)
		return true
	}
	if len(ops) <= 1 || !operationSetContainsID(ops, op.ID) {
		s.logger.Error("operator: claimed cutover operation is not part of a multi-operation apply set; operation will not be driven",
			append(op.LogAttrs(),
				"driver", driverID, "lease_owner", owner, "operation_count", len(ops))...)
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
// claimed, whether the reconciliation that followed succeeded, hit an error, or
// the claim returned an invalid lease — so the caller does not also run the
// normal operation claim this tick. Returns false when no apply needed
// reconciliation, and when the claim itself errored: a failed claim did no work
// and holds no lease, so the tick falls through to the operation claim instead
// of letting a persistent storage error on this path starve every other claim
// the driver ladder runs after it.
func (s *Service) recoverApplyPendingStop(ctx context.Context, driverID int, owner string) bool {
	apply, err := s.storage.Applies().FindNextApplyForStopReconciliation(ctx, owner)
	if err != nil {
		s.logClaimFailure(ctx, "operator: failed to claim apply for stop reconciliation", "stop_reconciliation_claim_error",
			"driver", driverID, "lease_owner", owner, "error", err)
		return false
	}
	if apply == nil {
		s.logger.Debug("operator: no apply needs stop reconciliation", "driver", driverID)
		return false
	}
	defer s.markDriverBusy(ctx)()

	lease := apply.Lease()
	if !lease.Valid() {
		s.logger.Error("operator: claimed apply for stop reconciliation without a valid lease token; skipping",
			append(apply.LogAttrs(),
				"driver", driverID, "lease_owner", owner)...)
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
			append(apply.LogAttrs(),
				"driver", driverID, "error", err)...)
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
			append(apply.LogAttrs(),
				"driver", driverID, "error", err)...)
		return true
	}

	finalApply, err := s.storage.Applies().Get(applyLeaseCtx, apply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload apply during stop reconciliation; derived apply state not updated",
			append(apply.LogAttrs(),
				"driver", driverID, "error", err)...)
		return true
	}
	if finalApply == nil {
		s.logger.Error("operator: apply not found during stop reconciliation; derived apply state not updated",
			append(apply.LogAttrs(),
				"driver", driverID)...)
		return true
	}

	result, err := s.updateApplyStateFromOperations(applyLeaseCtx, driverID, finalApply, allowLeaseScopedFailedReopen)
	if err != nil {
		s.logger.Error("operator: failed to update derived apply state during stop reconciliation",
			append(finalApply.LogAttrs(),
				"driver", driverID, "error", err)...)
		return true
	}

	// A multi-operation apply that settles terminal here (stop reconciliation has
	// no operation drive to publish on its behalf) still owes its single terminal
	// summary; publish it if this projection won the terminal swap.
	s.publishTerminalSummaryIfWon(applyLeaseCtx, driverID, finalApply, result)

	if err := s.completePendingControlRequestsIfApplyResolved(applyLeaseCtx, driverID, finalApply.ID); err != nil {
		s.logger.Error("operator: failed to complete pending control requests after stop reconciliation",
			append(finalApply.LogAttrs(),
				"driver", driverID, "error", err)...)
		return true
	}
	return true
}

// recoverApplyOperationProjection settles an apply whose operation rows have all
// reached a terminal state while the apply itself is still non-terminal: a
// parent left behind its own children because a drive stopped between writing
// its terminal operation row and projecting that outcome onto the parent.
//
//	apply_operations              applies
//	┌──────────────────┐          ┌──────────────────────────┐
//	│ id=8  completed  │          │ id=7  running  ← stranded│
//	│ id=9  completed  │  ──✗──▶  └──────────────────────────┘
//	└──────────────────┘            the projection never ran
//	  every child settled           the target stays blocked
//
// Nothing else recovers it. No operation-level claim arm matches a fully
// terminal operation set, the parent's own state never moves again, and the
// one-active-apply guard keeps refusing new applies for that target until it
// does. Re-deriving the parent from its operations is the entire repair — no
// engine work is driven, because the operations already carry their outcomes.
//
// Returns true when this driver spent its tick here, including on failure, so a
// repair that could not complete is retried rather than falling through to
// claim new work behind a target that is still blocked. Returns false when the
// claimed apply turned out to be a manifest-held deployment-keyed rollout —
// nothing needs repair there, so the tick falls through to claim operation
// work instead.
func (s *Service) recoverApplyOperationProjection(ctx context.Context, driverID int, owner string) bool {
	apply, err := s.storage.Applies().FindNextApplyForOperationProjection(ctx, owner)
	if err != nil {
		s.logClaimFailure(ctx, "operator: failed to claim apply for operation projection", "operation_projection_claim_error",
			"driver", driverID, "lease_owner", owner, "error", err)
		return true
	}
	if apply == nil {
		s.logger.Debug("operator: no apply needs operation projection", "driver", driverID)
		return false
	}
	defer s.markDriverBusy(ctx)()

	lease := apply.Lease()
	if !lease.Valid() {
		s.logger.Error("operator: claimed apply for operation projection without a valid lease token; the apply stays non-terminal and its target stays blocked until a later tick reclaims it",
			append(apply.LogAttrs(),
				"driver", driverID, "lease_owner", owner)...)
		metrics.RecordOperatorClaimFailure(ctx, "operation_projection_missing_lease_token")
		return true
	}
	applyLeaseCtx := storage.WithApplyLease(ctx, lease)

	s.logger.Info("operator: claimed an apply whose operations have all settled while it stayed non-terminal; deriving its state from them",
		append(apply.LogAttrs(),
			"driver", driverID, "lease_owner", owner)...)

	result, err := s.updateApplyStateFromOperations(applyLeaseCtx, driverID, apply, allowLeaseScopedFailedReopen)
	if err != nil {
		s.logger.Error("operator: failed to derive apply state from its settled operations; the apply stays non-terminal and its target stays blocked until a later tick reclaims it",
			append(apply.LogAttrs(),
				"driver", driverID, "error", err)...)
		return true
	}
	if !result.Swapped && result.ManifestHeld {
		// A deployment-keyed apply whose attached operations have all settled
		// matches the claim predicate while its generation manifest still
		// expects siblings. That is a healthy rollout awaiting dispatches, not
		// a stranded parent: the derive held it open, the claim refreshed the
		// heartbeat so it is not reconsidered until the staleness window
		// elapses again, and this driver's tick moves on to claim real
		// operation work — including the very dispatches the manifest waits for.
		s.logger.Info("operator: claimed apply is a deployment-keyed rollout whose generation manifest still expects operations that have not attached; it stays open for the declared dispatches and needs no repair",
			append(apply.LogAttrs(),
				"driver", driverID, "operation_count", result.OperationCount)...)
		return false
	}
	if !result.Swapped && result.HeldByResumableChild {
		// Every operation is terminal, which is what the claim predicate matches
		// on, but one of them is stopped and an operator can start it again. The
		// parent is already showing the state that says so, and holding it open
		// is what keeps that start claimable, so there is nothing to repair here
		// and no warning to raise. The claim refreshed the heartbeat; this
		// driver's tick moves on to claim real work.
		s.logger.Info("operator: claimed apply is held open by an operation an operator can start again; it stays open for that start and needs no repair",
			append(apply.LogAttrs(),
				"driver", driverID, "derived_state", result.DerivedState,
				"operation_count", result.OperationCount)...)
		return false
	}
	if !result.Swapped {
		// The claim refreshed the heartbeat, so this apply is not reconsidered
		// until the staleness window elapses again. Warn rather than debug: a
		// terminal operation set that does not derive a terminal parent means the
		// projection and the claim predicate disagree, and the target stays
		// blocked in the meantime.
		s.logger.Warn("operator: settled operations did not move the apply out of its non-terminal state; its target stays blocked until the state changes or an operator reconciles it",
			append(apply.LogAttrs(),
				"driver", driverID, "derived_state", result.DerivedState,
				"operation_count", result.OperationCount)...)
		return true
	}
	metrics.RecordOperatorOperationProjectionRepair(ctx, apply.Database, apply.Deployment, apply.Environment, result.DerivedState)

	// No operation drive is left to publish on this apply's behalf, so this
	// projection owes the single terminal summary if it won the terminal swap.
	s.publishTerminalSummaryIfWon(applyLeaseCtx, driverID, apply, result)

	if err := s.completePendingControlRequestsIfApplyResolved(applyLeaseCtx, driverID, apply.ID); err != nil {
		s.logger.Error("operator: failed to complete pending control requests after deriving apply state from its settled operations",
			append(apply.LogAttrs(),
				"driver", driverID, "error", err)...)
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
			append(apply.LogAttrs(),
				"driver", driverID, "stopped_operation_count", stopped)...)
	}
	return nil
}

// completePendingControlRequestsIfApplyResolved completes pending stop and
// cancel control requests once the apply has settled to a terminal state, so
// the requests do not stay pending forever after the rollout resolves. The
// apply is reloaded because the derived-state write operates on a copy and
// does not mutate the caller's row. A pending stop completes at any terminal
// state. A pending cancel completes only when the terminal state is not
// stopped: a stopped apply remains cancellable, so its pending cancel must
// stay deliverable for the next drive. A still-non-terminal apply resolves
// only its stop, and only once that stop has reached every operation. No-op
// when nothing is pending.
func (s *Service) completePendingControlRequestsIfApplyResolved(ctx context.Context, driverID int, applyID int64) error {
	apply, err := s.storage.Applies().Get(ctx, applyID)
	if err != nil {
		return fmt.Errorf("reload apply %d before completing pending control requests: %w", applyID, err)
	}
	if apply == nil {
		return fmt.Errorf("reload apply %d before completing pending control requests: %w", applyID, storage.ErrApplyNotFound)
	}
	if !state.IsTerminalApplyState(apply.State) {
		return s.completeLandedStopForHeldOpenApply(ctx, driverID, apply)
	}

	if err := s.completePendingRequestForResolvedApply(ctx, driverID, apply, storage.ControlOperationStop); err != nil {
		return err
	}
	if state.IsState(apply.State, state.Apply.Stopped) {
		s.logger.Debug("operator: leaving pending cancel request deliverable for stopped apply",
			append(apply.LogAttrs(),
				"driver", driverID)...)
		return nil
	}
	return s.completePendingRequestForResolvedApply(ctx, driverID, apply, storage.ControlOperationCancel)
}

// completeLandedStopForHeldOpenApply completes a pending stop request on an
// apply the rollout projection is still holding open.
//
// A stopped operation still holds its target, so a rollout an operator stopped
// can carry a running-family verdict with every one of its operations already
// terminal. The stop has landed all the same, and its request is what start
// consults: leaving it pending refuses the very start that resumes the stopped
// operation, which is the only way that rollout ever settles. Cancel is not
// completed here — the apply is not resolved, and the cancel is still
// deliverable to the next drive.
//
// Fails closed on an incomplete generation. While the manifest still declares
// operations that have not attached, a later dispatch can bring work this stop
// must halt, so the request stays pending for it.
func (s *Service) completeLandedStopForHeldOpenApply(ctx context.Context, driverID int, apply *storage.Apply) error {
	controlReq, err := s.storage.ControlRequests().GetPending(ctx, apply.ID, storage.ControlOperationStop)
	if err != nil {
		return fmt.Errorf("load pending stop request for held-open apply %s (%d): %w", apply.ApplyIdentifier, apply.ID, err)
	}
	if controlReq == nil {
		return nil
	}

	ops, err := s.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("list apply_operations for held-open apply %s (%d): %w", apply.ApplyIdentifier, apply.ID, err)
	}
	if len(ops) == 0 {
		s.logger.Debug("operator: leaving pending stop request for held-open apply with no operation rows",
			append(apply.LogAttrs(), "driver", driverID)...)
		return nil
	}
	if missing := apply.MissingExpectedOperationKeys(ops); len(missing) > 0 {
		s.logger.Debug("operator: leaving pending stop request deliverable for operations the generation manifest still expects",
			append(apply.LogAttrs(),
				"driver", driverID, "missing_operation_keys", missing)...)
		return nil
	}
	for _, op := range ops {
		if !state.IsApplyOperationTerminal(op.State) {
			s.logger.Debug("operator: leaving pending stop request for held-open apply with an operation the stop has not reached",
				append(apply.LogAttrs(),
					"driver", driverID, "operation_deployment", op.Deployment,
					"operation_state", op.State)...)
			return nil
		}
	}
	return s.completePendingRequestForResolvedApply(ctx, driverID, apply, storage.ControlOperationStop)
}

// completePendingRequestForResolvedApply completes one pending control request
// for an apply the caller has already established as resolved for that
// operation. No-op when no request of that operation is pending.
func (s *Service) completePendingRequestForResolvedApply(ctx context.Context, driverID int, apply *storage.Apply, op storage.ControlOperation) error {
	controlReq, err := s.storage.ControlRequests().GetPending(ctx, apply.ID, op)
	if err != nil {
		return fmt.Errorf("load pending %s request for resolved apply %s (%d): %w", op, apply.ApplyIdentifier, apply.ID, err)
	}
	if controlReq == nil {
		return nil
	}

	if err := s.storage.ControlRequests().CompletePending(ctx, apply.ID, op); err != nil {
		return fmt.Errorf("complete pending %s request for resolved apply %s (%d): %w", op, apply.ApplyIdentifier, apply.ID, err)
	}
	s.logger.Info("operator: completed pending control request for resolved apply",
		append(apply.LogAttrs(),
			"driver", driverID, "operation", op)...)
	return nil
}

// operationLeaseVerdict is what a pre-drive lease re-check concluded. The three
// answers differ in what this driver still holds, which is what decides whether
// it has an operation lease to hand back.
type operationLeaseVerdict int

const (
	// operationLeaseHeld: the row still carries this drive's token.
	operationLeaseHeld operationLeaseVerdict = iota
	// operationLeaseUnproven: storage could not answer. Nothing showed the lease
	// gone, so this driver still holds it and owes a release.
	operationLeaseUnproven
	// operationLeaseLost: a successful read showed the lease is no longer this
	// driver's — the row is gone, a peer rotated the token onto itself, or a
	// peer released it. There is nothing left to release.
	operationLeaseLost
)

// recheckOperationLease re-reads the operation row to see whether it still
// carries this drive's lease token. It spans the gap between the operation claim
// and the parent apply claim, so a peer that rotated the token in that gap is
// detected before any engine work runs rather than through the first refused
// write.
//
// It narrows that race rather than closing it: the read takes no row lock, so a
// peer can still rotate between this read and the drive's first write, where the
// lease-guarded write catches it (OW-2). What the claim query's own heartbeat
// gate closes is the window that produced the rotation in the first place; this
// re-check is what turns the remainder into an early, correctly named claim race
// instead of a late failure attributed to whichever write ran first.
//
// Uncertainty is not displacement (OW-4): a storage failure answers unproven,
// not lost, so the caller hands the lease back for an immediate retry instead of
// treating a blip as a peer.
func (s *Service) recheckOperationLease(ctx context.Context, driverID int, op *storage.ApplyOperation, opLease storage.OperationLease) operationLeaseVerdict {
	current, err := s.storage.ApplyOperations().Get(ctx, op.ID)
	if err != nil {
		s.logClaimFailure(ctx, "operator: could not re-read the claimed operation to confirm its lease survived the parent claim; operation will not be driven", "operation_lease_recheck_error",
			append(op.LogAttrs(),
				"driver", driverID,
				"error", err)...)
		return operationLeaseUnproven
	}
	if current == nil {
		s.logger.Error("operator: claimed operation disappeared before its drive started; operation will not be driven",
			append(op.LogAttrs(), "driver", driverID)...)
		metrics.RecordOperatorClaimFailure(ctx, "operation_lease_recheck_missing")
		return operationLeaseLost
	}
	// An empty token is a peer that released the row rather than one that took
	// it, and the two need different log lines: nobody is driving the operation
	// now, so the truthful outcome is that the next poll offers it again.
	if current.LeaseToken == "" {
		s.logger.Warn("operator: a peer released the operation lease between this driver's operation claim and its parent apply claim; the operation is offered again on the next poll",
			append(op.LogAttrs(), "driver", driverID)...)
		metrics.RecordOperatorClaimFailure(ctx, "operation_lease_released_by_peer")
		return operationLeaseLost
	}
	if current.LeaseToken != opLease.Token {
		s.logger.Warn("operator: a peer rotated the operation lease between this driver's operation claim and its parent apply claim; the peer drives the operation",
			append(op.LogAttrs(),
				"driver", driverID,
				"current_lease_owner", current.LeaseOwner)...)
		metrics.RecordOperatorClaimFailure(ctx, "operation_lease_rotated")
		return operationLeaseLost
	}
	return operationLeaseHeld
}

// releaseOperationClaimBeforeDrive hands back an operation lease this driver
// holds but will not drive under, so the next poll can offer the row
// immediately. Without it the claim query's heartbeat gate — refreshed by this
// driver's own claim — keeps the row unclaimable for a full staleness window,
// which is the stall the release exists to avoid.
func (s *Service) releaseOperationClaimBeforeDrive(ctx context.Context, driverID int, op *storage.ApplyOperation, opLease storage.OperationLease) {
	released, err := s.storage.ApplyOperations().ReleaseClaim(ctx, opLease)
	if err != nil {
		s.logClaimFailure(ctx, "operator: failed to release the operation lease this driver will not drive under; the operation is retried once its lease goes stale", "operation_lease_release_error",
			append(op.LogAttrs(),
				"driver", driverID,
				"error", err)...)
		return
	}
	if !released {
		s.logger.Warn("operator: operation lease already rotated before this driver could release it; the new lease owner drives the operation",
			append(op.LogAttrs(), "driver", driverID)...)
		return
	}
	s.logger.Info("operator: released the operation lease this driver will not drive under; the operation is offered on the next poll",
		append(op.LogAttrs(), "driver", driverID)...)
}

// releaseParentApplyClaim hands back a parent apply lease this driver acquired
// but will not drive under, so the peer that owns the operation can claim it on
// its next poll instead of waiting out the staleness window. A parent left
// leased here is exactly the stall the release avoids: the operation claim only
// re-offers the row while the parent's heartbeat is stale.
func (s *Service) releaseParentApplyClaim(ctx context.Context, driverID int, apply *storage.Apply, op *storage.ApplyOperation) {
	released, err := s.storage.Applies().ReleaseClaim(ctx, apply.Lease())
	if err != nil {
		s.logClaimFailure(ctx, "operator: failed to release the parent apply claim this driver will not drive; the parent is retried once its lease goes stale", "operation_parent_release_error",
			append(apply.LogAttrs(),
				"driver", driverID,
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment,
				"error", err)...)
		return
	}
	if !released {
		s.logger.Warn("operator: parent apply lease already rotated before this driver could release it; the new lease owner drives the apply",
			append(apply.LogAttrs(),
				"driver", driverID,
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment)...)
		return
	}
	s.logger.Info("operator: released the parent apply claim for an operation this driver no longer leases",
		append(apply.LogAttrs(),
			"driver", driverID,
			"apply_operation_id", op.ID,
			"operation_deployment", op.Deployment)...)
}

// reconcileUnclaimableParent handles a claimed operation whose parent apply
// ClaimApplyByID refused. If the parent is terminal, the operation row is
// reconciled to that terminal state so it stops being re-claimed on every poll
// (the write is unguarded — the operation holds no apply lease — but a terminal
// apply has no competing driver, so the mirror is safe and idempotent). If the
// parent is non-terminal (a peer holds a fresh lease, or the row was locked),
// the just-acquired operation lease is released so the next poll retries the
// operation immediately — retaining it would stall the operation for the full
// lease staleness window over a refusal that typically clears in milliseconds.
func (s *Service) reconcileUnclaimableParent(ctx context.Context, driverID int, op *storage.ApplyOperation, opLease storage.OperationLease) {
	parent, err := s.storage.Applies().Get(ctx, op.ApplyID)
	if err != nil {
		s.logClaimFailure(ctx, "operator: failed to load unclaimable parent apply; operation will be retried once its lease goes stale", "operation_parent_load_error",
			append(op.LogAttrs(),
				"driver", driverID,
				"error", err)...)
		return
	}
	if parent == nil {
		s.logger.Error("operator: parent apply not found for claimed operation; operation will be retried once its lease goes stale",
			append(op.LogAttrs(),
				"driver", driverID)...)
		metrics.RecordOperatorClaimFailure(ctx, "operation_parent_missing")
		return
	}
	if state.IsTerminalApplyState(parent.State) {
		s.reconcileClaimedOperationFromTerminalParent(ctx, driverID, op, parent)
		return
	}
	metrics.RecordOperatorClaimFailure(ctx, "operation_parent_not_claimable")
	released, err := s.storage.ApplyOperations().ReleaseClaim(ctx, opLease)
	if err != nil {
		s.logClaimFailure(ctx, "operator: parent apply not claimable and operation lease release failed; operation will be retried once its lease goes stale", "operation_lease_release_error",
			append(parent.LogAttrs(),
				"driver", driverID,
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment,
				"error", err)...)
		return
	}
	if !released {
		s.logger.Warn("operator: parent apply not claimable and operation lease already rotated; the new lease owner drives the operation",
			append(parent.LogAttrs(),
				"driver", driverID,
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment)...)
		return
	}
	s.logger.Warn("operator: parent apply not claimable for operation; operation lease released for retry on the next poll",
		append(parent.LogAttrs(),
			"driver", driverID,
			"apply_operation_id", op.ID,
			"operation_deployment", op.Deployment)...)
}

func (s *Service) reconcileClaimedOperationFromTerminalParent(ctx context.Context, driverID int, op *storage.ApplyOperation, parent *storage.Apply) {
	s.logger.Info("operator: parent apply already terminal; reconciling operation to terminal state",
		append(parent.LogAttrs(),
			"driver", driverID,
			"apply_operation_id", op.ID,
			"operation_deployment", op.Deployment)...)
	marked, err := s.markOperationFromApplyState(ctx, driverID, op, parent)
	if err != nil {
		s.logger.Error("operator: failed to reconcile apply_operation from terminal parent; derived apply state not updated",
			append(parent.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID,
				"operation_deployment", op.Deployment, "error", err)...)
		return
	}
	if !marked {
		return
	}
	if _, err := s.updateApplyStateFromOperations(ctx, driverID, parent, rejectFailedApplyReopen); err != nil {
		s.logger.Error("operator: failed to update derived apply state for terminal parent",
			append(parent.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID,
				"operation_deployment", op.Deployment, "error", err)...)
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
			append(apply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID,
				"operation_deployment", op.Deployment, "error", err)...)
		return
	}
	result, err := s.updateApplyStateFromOperations(applyCtx, driverID, apply, allowLeaseScopedFailedReopen)
	if err != nil {
		s.logger.Error("operator: failed to update derived apply state after failing task-less operation",
			append(apply.LogAttrs(),
				"driver", driverID, "apply_operation_id", op.ID,
				"operation_deployment", op.Deployment, "error", err)...)
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
// when it is 0 (the whole-apply drive the stop-reconciliation claim uses) it
// drives every task of the apply via ResumeApply with only the apply lease
// attached. Returns true when the work
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
	defer s.trackHeldClaim(apply)()
	start := s.clock.Now()

	// Bind the apply's identity once so every line of this drive is
	// filterable by apply_id/repo/pr without hand-listing the attrs per call.
	// Mutable attrs (state, the driven deployment) are bound or appended
	// where they are known to be current.
	logger := s.logger.With(append(apply.IdentityLogAttrs(), "driver", driverID)...)

	// operationDeployment is observability attribution only — RoutingClient
	// reloads the operation row and routes by its own deployment. The
	// operation-claim path passes the claimed op's deployment so logs/metrics
	// name the deployment actually being driven; the whole-apply
	// stop-reconciliation drive passes "" and falls back to the apply's stored
	// deployment. For single-op
	// applies the two are equal, so the attribution is unchanged.
	deployment := operationDeployment
	if deployment == "" {
		stored, err := storedDeploymentForApply(apply)
		if err != nil {
			logger.Error("operator: claimed apply is missing stored deployment metadata",
				"error", err)
			metrics.RecordOperatorResumeFailure(ctx, apply.Database, "", apply.Environment, "missing_deployment")
			return false, err
		}
		deployment = stored
	}
	logger = logger.With("deployment", deployment)

	logger.Info("operator: claimed apply",
		"lease_owner", lease.Owner,
		"state", apply.State,
		"last_heartbeat", apply.UpdatedAt)

	// Record the claim in the apply's durable log so the timeline explains
	// why new state transitions appear after a failure or a driver crash —
	// without this entry, an operator reading apply_logs sees a gap between
	// the last failure and the resumed work.
	s.logApplyResumeClaim(ctx, logger, driverID, apply)

	previousState := apply.State

	client, err := s.RoutingTernClient()
	if err != nil {
		logger.Error("operator: failed to get routing client",
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
	// whole-apply stop-reconciliation drive (applyOperationID == 0) drives
	// every task.
	// The drive runs behind a panic boundary: engine and resume code process
	// stored metadata, so one poisoned row must degrade only this apply, not
	// the process. A contained panic surfaces as a *panicsafe.Error and is
	// handled as a permanent failure below.
	err = panicsafe.Call(func() error {
		switch {
		case applyOperationID > 0 && opts.cutover:
			return client.ResumeApplyOperationCutover(ctx, apply, applyOperationID)
		case applyOperationID > 0:
			return client.ResumeApplyOperation(ctx, apply, applyOperationID)
		default:
			return client.ResumeApply(ctx, apply)
		}
	})
	if err != nil {
		var drivePanic *panicsafe.Error
		if errors.As(err, &drivePanic) {
			logger.Error("operator: engine drive panicked; the apply will be marked failed so it is not re-claimed and panicked again",
				"state", apply.State,
				"apply_operation_id", applyOperationID,
				"panic", fmt.Sprint(drivePanic.Value),
				"stack", string(drivePanic.Stack))
			metrics.RecordRecoveredPanic(ctx, "apply_drive")
			metrics.RecordOperatorResumeFailure(ctx, apply.Database, deployment, apply.Environment, "drive_panic")
			// The failed transition below owns the active-apply gauge release, so
			// the retryable-claim decrement the other failure branches perform is
			// intentionally omitted here.
			s.failClaimedApplyAfterDrivePanic(ctx, driverID, apply, applyOperationID, deployment, drivePanic)
			return false, err
		}
		if errors.Is(err, tern.ErrApplyLeasePresumedLost) {
			logger.Warn("operator: apply lease presumed lost after heartbeat failures spanning the staleness window; driver will stop writing this apply and a peer will reclaim it",
				"lease_owner", lease.Owner,
				"error", err)
			metrics.RecordOperatorResumeFailure(ctx, apply.Database, deployment, apply.Environment, "lease_presumed_lost")
			if retryableClaim {
				metrics.AdjustActiveApplies(ctx, -1, apply.Database, deployment, apply.Environment)
			}
			return false, err
		}
		if errors.Is(err, storage.ErrApplyLeaseLost) {
			logger.Warn("operator: apply lease was lost; driver will stop writing this apply",
				"lease_owner", lease.Owner,
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
			logger.Error("operator: claimed operation has no tasks; failing it closed",
				"apply_operation_id", applyOperationID,
				"error", err)
			metrics.RecordOperatorResumeFailure(ctx, apply.Database, deployment, apply.Environment, "operation_no_tasks")
			if retryableClaim {
				metrics.AdjustActiveApplies(ctx, -1, apply.Database, deployment, apply.Environment)
			}
			return false, err
		}
		if errors.Is(err, tern.ErrApplyTasksNotLoaded) {
			// Fail-closed: the apply owns task rows the whole-apply drive did not
			// load, so completing it would report success for schema changes that
			// never ran. The drive mutated nothing; the apply stays claimable and
			// this driver will re-attempt it on later polls, so the work stays
			// visibly stuck (and this metric fires) until the rows load or an
			// operator intervenes.
			logger.Error("operator: apply owns task rows the drive did not load; refusing task-less completion and leaving the apply claimable",
				"error", err)
			metrics.RecordOperatorResumeFailure(ctx, apply.Database, deployment, apply.Environment, "apply_tasks_not_loaded")
			if retryableClaim {
				metrics.AdjustActiveApplies(ctx, -1, apply.Database, deployment, apply.Environment)
			}
			return false, err
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			logger.Debug("operator: stopped while running claimed apply",
				"error", err)
			if retryableClaim {
				metrics.AdjustActiveApplies(ctx, -1, apply.Database, deployment, apply.Environment)
			}
			return false, err
		}
		logger.Error("operator: failed to resume apply",
			"error", err)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, deployment, apply.Environment, "resume_error")
		if retryableClaim {
			metrics.AdjustActiveApplies(ctx, -1, apply.Database, deployment, apply.Environment)
		}
		return false, err
	}

	duration := s.clock.Now().Sub(start)
	logger.Info("operator: resumed apply",
		"previous_state", previousState,
		"duration", duration)
	metrics.RecordOperatorResume(ctx, apply.Database, deployment, apply.Environment, previousState)
	metrics.RecordOperatorClaimDuration(ctx, duration, apply.Database, deployment, apply.Environment, previousState)
	return true, nil
}

// logApplyResumeClaim appends a durable apply log entry recording that an
// operator driver claimed the apply to resume it. Best-effort: a failed
// append must not block the resume, so the error is logged on the caller's
// drive-scoped logger and the claim proceeds.
func (s *Service) logApplyResumeClaim(ctx context.Context, logger *slog.Logger, driverID int, apply *storage.Apply) {
	s.appendApplyLog(ctx, logger, &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelInfo,
		EventType: storage.LogEventInfo,
		Source:    storage.LogSourceSchemaBot,
		Message:   fmt.Sprintf("Operator claimed apply to resume it (driver %d, state %s)", driverID, apply.State),
		OldState:  apply.State,
		NewState:  apply.State,
		CreatedAt: s.clock.Now(),
	}, "that a driver claimed it to resume it")
}

// failClaimedApplyAfterDrivePanic contains an engine-drive panic to the
// claimed work. The tasks in the drive's scope are marked failed so the
// operation row can settle to a terminal state from its own tasks, and — when
// this driver holds the parent apply lease — the apply row is marked failed so
// neither the stale-heartbeat nor the retry recovery path re-claims the same
// row and panics again. failed (permanent) is deliberate: failed_retryable
// would queue the apply straight back into the panicking code path. Returns
// true when this call transitioned the apply row to failed.
//
// The apply requires operator intervention afterwards: fix the underlying code
// or data fault the panic log identifies, then start a new apply for the
// schema change.
func (s *Service) failClaimedApplyAfterDrivePanic(ctx context.Context, driverID int, apply *storage.Apply, applyOperationID int64, deployment string, drivePanic *panicsafe.Error) bool {
	errMsg := fmt.Sprintf("apply drive panicked: %v", drivePanic.Value)
	now := s.clock.Now()

	// Fail the tasks the panicked drive was scoped to. The operation-claim
	// paths derive the operation row from its tasks (markOperationFromOwnResult),
	// so failing the tasks is what lets the operation row — and through the
	// rollout projection, the parent apply — settle terminally.
	var tasks []*storage.Task
	var tasksErr error
	if applyOperationID > 0 {
		tasks, tasksErr = s.storage.Tasks().GetByApplyOperationID(ctx, applyOperationID)
	} else {
		tasks, tasksErr = s.storage.Tasks().GetByApplyID(ctx, apply.ID)
	}
	if tasksErr != nil {
		s.logger.Error("operator: failed to load tasks while containing drive panic; the operation row will settle from a later reconcile instead",
			append(apply.LogAttrs(),
				"driver", driverID,
				"apply_operation_id", applyOperationID,
				"operation_deployment", deployment,
				"error", tasksErr)...)
	}
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			s.logger.Debug("operator: task already terminal while containing drive panic; leaving its state",
				append(task.LogAttrs(), "driver", driverID)...)
			continue
		}
		if task.ErrorMessage == "" {
			task.ErrorMessage = errMsg
		}
		task.State = state.Task.Failed
		task.CompletedAt = &now
		task.UpdatedAt = now
		if err := s.storage.Tasks().Update(ctx, task); err != nil {
			s.logger.Error("operator: failed to mark task failed while containing drive panic; the task will settle from a later reconcile instead",
				append(task.LogAttrs(), "driver", driverID, "error", err)...)
		}
	}

	// A multi-operation drive holds only its operation lease; the parent
	// applies row is owned by the rollout projection, which settles it from the
	// now-failed operation row. Only a driver holding the parent apply lease
	// writes the apply row directly.
	if _, hasApplyLease := storage.ApplyLeaseFromContext(ctx); !hasApplyLease {
		s.logger.Info("operator: contained drive panic under the operation lease; the parent apply will settle via the rollout projection",
			append(apply.LogAttrs(),
				"driver", driverID,
				"apply_operation_id", applyOperationID,
				"operation_deployment", deployment)...)
		return false
	}

	// Reload before writing: a stop or a competing driver may have already
	// terminalized the apply between the claim and the panic, and a terminal
	// state must not be overwritten.
	fresh, err := s.storage.Applies().Get(ctx, apply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload apply while containing drive panic; the apply is not marked failed and will be re-claimed on a later poll",
			append(apply.LogAttrs(), "driver", driverID, "error", err)...)
		return false
	}
	if fresh == nil {
		s.logger.Error("operator: apply not found while containing drive panic; the apply is not marked failed",
			append(apply.LogAttrs(), "driver", driverID)...)
		return false
	}
	if state.IsTerminalApplyState(fresh.State) {
		s.logger.Info("operator: apply already terminal while containing drive panic; leaving its state",
			append(apply.LogAttrs(), "driver", driverID, "current_state", fresh.State)...)
		return false
	}

	// Write the reloaded row, not the claim-time pointer: fields a stop or a
	// peer updated between the claim and the panic must survive this write.
	previousState := fresh.State
	fresh.State = state.Apply.Failed
	fresh.ErrorMessage = errMsg
	fresh.CompletedAt = &now
	fresh.UpdatedAt = now
	if err := s.storage.Applies().Update(ctx, fresh); err != nil {
		s.logger.Error("operator: failed to mark apply failed while containing drive panic; the apply will be re-claimed on a later poll",
			append(apply.LogAttrs(), "driver", driverID, "error", err)...)
		return false
	}
	// The failed transition releases the active-apply gauge the same way an
	// engine-driven failure would have.
	metrics.AdjustActiveApplies(ctx, -1, fresh.Database, deployment, fresh.Environment)
	s.logApplyDrivePanicFailure(ctx, driverID, fresh, previousState, errMsg)
	return true
}

// logApplyDrivePanicFailure appends a durable apply log entry recording that a
// contained drive panic failed the apply, so the timeline explains the terminal
// state without server logs. Best-effort: a failed append must not block
// containment.
func (s *Service) logApplyDrivePanicFailure(ctx context.Context, driverID int, apply *storage.Apply, previousState, errMsg string) {
	s.appendApplyLog(ctx, s.logger, &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelError,
		EventType: storage.LogEventError,
		Source:    storage.LogSourceSchemaBot,
		Message:   fmt.Sprintf("Apply failed after a drive panic was contained by operator driver %d: %s. Fix the underlying fault, then start a new apply.", driverID, errMsg),
		OldState:  previousState,
		NewState:  apply.State,
		CreatedAt: s.clock.Now(),
	}, "that a contained drive panic failed it", append(apply.LogAttrs(), "driver", driverID)...)
}

// startApplyOperationHeartbeat refreshes the claimed operation row's lease while
// ResumeApply runs, at min(operatorPollInterval, ApplyOperationHeartbeatInterval)
// so the row cannot go stale and be re-claimed by a peer even when the poll
// interval is large. The heartbeat writes under the operation lease, so a lost
// operation lease cancels the run and the displaced driver stops. Other
// heartbeat errors are logged and retried on the next tick — until they have
// persisted for the full storage.ApplyLeaseStaleAfter window, at which point
// the lease is presumed lost (a peer can already have reclaimed the stale row)
// and the run is cancelled the same way.
//
// Each successful heartbeat also checks the inverse hazard: a drive goroutine
// that has wedged while its heartbeat stays fresh. The heartbeat alone would
// keep the lease renewed forever, so no peer could ever reclaim the stuck
// work; operationDriveStalled watches the drive's task mirror writes and
// cancels the run when they stop for the full stall window.
// Returns a stop func that is safe to call more than once.
func (s *Service) startApplyOperationHeartbeat(ctx context.Context, driverID int, op *storage.ApplyOperation, apply *storage.Apply, cancelRun context.CancelFunc) func() {
	hbCtx, stop := context.WithCancel(ctx)
	interval := min(s.operatorPollInterval, ApplyOperationHeartbeatInterval)
	s.recoveryWg.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		driveStart := s.clock.Now()
		lastSuccess := driveStart
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				err := s.storage.ApplyOperations().Heartbeat(hbCtx, op.ID)
				if err == nil {
					lastSuccess = s.clock.Now()
					if s.operationDriveStalled(hbCtx, driverID, op, apply, driveStart) {
						cancelRun()
						return
					}
					continue
				}
				if hbCtx.Err() != nil {
					// The run is shutting down; the failed write is
					// cancellation fallout, not lease trouble.
					return
				}
				if s.operationHeartbeatFailureStopsDrive(hbCtx, driverID, op, apply, err, lastSuccess) {
					cancelRun()
					return
				}
			}
		}
	})
	return stop
}

// operationHeartbeatFailureStopsDrive classifies a failed operation heartbeat
// write and reports whether the driver must stop driving: either storage
// reported the operation lease definitively lost, or heartbeat failures have
// persisted since lastSuccess for the full lease staleness window, so a peer
// driver can already have reclaimed the stale row. A transient failure inside
// the window keeps the run going and is retried on the next tick.
func (s *Service) operationHeartbeatFailureStopsDrive(ctx context.Context, driverID int, op *storage.ApplyOperation, apply *storage.Apply, hbErr error, lastSuccess time.Time) bool {
	logAttrs := append(append(op.LogAttrs(), apply.IdentityLogAttrs()...),
		"driver", driverID)
	if errors.Is(hbErr, storage.ErrApplyLeaseLost) {
		s.logger.Warn("operator: apply_operation heartbeat lost operation lease; driver will stop",
			append(logAttrs, "error", hbErr)...)
		return true
	}
	if s.clock.Now().Sub(lastSuccess) >= storage.ApplyLeaseStaleAfter {
		s.logger.Warn("operator: apply_operation heartbeat has failed for the full lease staleness window; a peer driver can reclaim the operation, so this driver will stop",
			append(logAttrs, "last_successful_heartbeat", lastSuccess, "error", hbErr)...)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, op.Deployment, apply.Environment, "lease_presumed_lost")
		return true
	}
	s.logger.Warn("operator: apply_operation heartbeat failed; will retry",
		append(logAttrs, "error", hbErr)...)
	return false
}

// operationDriveStalled reports whether the drive holding this operation has
// stopped making observable progress while its heartbeat stays fresh. The
// drive's poll loop mirrors every task row to storage on every poll tick, so
// task updated_at advances continuously while the drive goroutine is alive. A
// drive that has mirrored nothing for the full storage.ApplyDriveStallAfter window is
// wedged — for example blocked in a target-database read that outlives its
// cancelled context — and will never finish on its own, yet its heartbeat
// keeps the operation lease fresh so no peer driver can reclaim the work.
// Cancelling the run reuses the lost-lease semantics: a cancellation-aware
// drive unwinds and records a retryable failure, and a drive blocked beyond
// cancellation stops being heartbeated so the lease goes stale and a peer
// driver reclaims the operation.
//
// The window is measured from the later of drive start and the newest task
// mirror write, so a fresh drive is never judged by a predecessor's writes and
// slow pre-poll phases get the full window before their first mirror. An
// operation that carries no task rows by design gives the check no mirror
// signal to judge, so it is exempt: group finalizers never carry tasks, and a
// VSchema-only plan's work operation is the one task-less work shape (every
// other task-less claim fails closed at dispatch, before a drive exists to
// watch). A liveness read failure keeps the drive going — the heartbeat
// failure path already stops the drive when storage itself is unhealthy.
func (s *Service) operationDriveStalled(ctx context.Context, driverID int, op *storage.ApplyOperation, apply *storage.Apply, driveStart time.Time) bool {
	if op.OperationKind == storage.ApplyOperationKindGroupFinalizer {
		return false
	}
	now := s.clock.Now()
	if now.Sub(driveStart) < storage.ApplyDriveStallAfter {
		return false
	}
	logAttrs := append(append(op.LogAttrs(), apply.IdentityLogAttrs()...),
		"driver", driverID)
	tasks, err := s.storage.Tasks().GetByApplyOperationID(ctx, op.ID)
	if err != nil {
		s.logger.Warn("operator: failed to read task rows for the drive liveness check; the check will retry on the next heartbeat tick",
			append(logAttrs, "error", err)...)
		return false
	}
	// Task rows are inserted with the apply, so a work operation with none is
	// the task-less VSchema-only shape: it mirrors nothing by design, and the
	// check has no signal to judge it by.
	if len(tasks) == 0 {
		return false
	}
	lastMirror := driveStart
	for _, task := range tasks {
		if task.UpdatedAt.After(lastMirror) {
			lastMirror = task.UpdatedAt
		}
	}
	if now.Sub(lastMirror) < storage.ApplyDriveStallAfter {
		return false
	}
	s.logger.Warn("operator: drive has mirrored no task progress for the full stall window while its heartbeat stayed fresh; the run will be cancelled so the operation can be re-claimed",
		append(logAttrs,
			"drive_started", driveStart,
			"last_task_mirror", lastMirror,
			"stall_window", storage.ApplyDriveStallAfter)...)
	metrics.RecordOperatorResumeFailure(ctx, apply.Database, op.Deployment, apply.Environment, "drive_stalled")
	return true
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
		if op.IsTasklessVSchemaOnlyWork(plan) {
			currentOp, getOpErr := s.storage.ApplyOperations().Get(ctx, op.ID)
			if getOpErr != nil {
				return false, fmt.Errorf("reload task-less apply_operation %d (deployment %q): %w", op.ID, op.Deployment, getOpErr)
			}
			// The drive owns a task-less operation's outcome, the same way it owns
			// a group_finalizer's: there are no task rows to derive it from, so
			// whatever terminal state the drive wrote is the result. Report it as
			// written so the caller goes on to project the parent. Re-deriving from
			// the parent instead would read the still-running parent of an
			// operation-lease-only drive, take the "leave claimable" branch, and
			// return updated=false — stranding a failed or stopped operation with
			// its parent never projected and its target blocked.
			if currentOp != nil && state.IsTerminalApplyState(currentOp.State) {
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
// projection legitimately holds it active: under on_failure "continue" because
// later siblings still get their turn, and under a fail-closed policy because a
// sibling that a driver already started is still working.
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
	// when the rollout projection holds it active. Used only by callers that
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
	// ManifestHeld is true when the attached operations derived a
	// whole-generation verdict that the generation manifest gated back to
	// running because declared operations have not attached yet. The apply is
	// a healthy deployment-keyed rollout awaiting its remaining dispatches,
	// not a stranded parent.
	ManifestHeld bool
	// HeldByResumableChild is true when every child reached a terminal state but
	// the derived parent stayed non-terminal because one of them can still be
	// started again. The apply is waiting on the operator who stopped it, not
	// stranded behind a projection that never ran.
	HeldByResumableChild bool
}

// anyPauseOnFailure reports whether any operation uses the on_failure=pause
// policy. It gates the release-latch read: only a pause rollout's projection
// depends on whether an operator has released it.
func anyPauseOnFailure(ops []*storage.ApplyOperation) bool {
	return slices.ContainsFunc(ops, func(op *storage.ApplyOperation) bool {
		return op.OnFailure == storage.OnFailurePause
	})
}

// projectionOwnsActiveAppliesGauge reports whether the winning projection owns
// adjusting the active-applies gauge for this apply. Drives that run under the
// operation lease only — a multi-operation rollout, or a deployment-keyed
// apply whose generation manifest still expects unattached operations —
// suppress the parent-level gauge in their own drive, so the projection that
// wins the parent transition is the single point that must adjust it. A legacy
// single-operation apply drives under the parent lease and releases the gauge
// in its direct drive, so its projection leaves the gauge alone.
func projectionOwnsActiveAppliesGauge(apply *storage.Apply, ops []*storage.ApplyOperation) bool {
	return len(ops) > 1 || len(apply.MissingExpectedOperationKeys(ops)) > 0
}

// manifestGatedVerdict reports whether a derived apply state asserts a
// whole-generation outcome: completed claims every declared operation applied,
// and reverted claims every declared operation was unwound. Neither claim is
// honest while manifest-declared operations have not attached, so the
// generation-manifest hold gates both. Failure verdicts are not gated: a
// failed generation must not wait for siblings that may never dispatch.
func manifestGatedVerdict(derived string) bool {
	return state.IsState(derived, state.Apply.Completed) ||
		state.IsState(derived, state.Apply.Reverted)
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
// verdict, and holds the apply degraded until any sibling that a driver already
// started settles, because refusing new claims does not stop work already under
// way. While an apply has exactly one operation the derived value equals the
// value ResumeApply already persisted, so this is a no-op until the
// multi-deployment fan-out makes an apply own more than one operation.
//
// The caller is responsible for lease scoping: the active operator path passes a
// lease-scoped context so the write fails closed after ownership changes; the
// terminal-parent reconciliation path passes an unscoped context. The reopen
// parameter encodes the matching authority — a terminal parent may only be
// reopened (failed → running, for the hold-active rollout projection) by a
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

	// Load the release latch only when some operation uses on_failure=pause: it
	// is the only policy whose projection depends on whether an operator has
	// released the rollout, so an apply without a pause operation never pays the
	// read or risks a latch read error blocking its derived-state update. A
	// released pause behaves like continue, so a paused apply that an operator
	// has released no longer holds at paused; a failed release does not latch
	// (fail-closed), per ApplyControlRequest.ReleasesPausedRollout. With no
	// control-request store a release latch cannot exist, so an unreleased pause
	// stays held.
	released := false
	if anyPauseOnFailure(ops) {
		if requests := s.storage.ControlRequests(); requests != nil {
			releaseReq, err := requests.GetByOperation(ctx, apply.ID, storage.ControlOperationRelease)
			if err != nil {
				return applyProjectionResult{}, fmt.Errorf("load release latch for apply %s (%d): %w", apply.ApplyIdentifier, apply.ID, err)
			}
			released = releaseReq.ReleasesPausedRollout()
		}
	}

	childStates := make([]string, len(ops))
	children := make([]state.RolloutChild, len(ops))
	for i, op := range ops {
		childStates[i] = op.State
		isContinue := op.OnFailure == storage.OnFailureContinue
		isPause := op.OnFailure == storage.OnFailurePause
		children[i] = state.RolloutChild{
			State:             op.State,
			ContinueOnFailure: isContinue || (isPause && released),
			PauseOnFailure:    isPause && !released,
		}
	}
	base := state.DeriveApplyState(childStates)
	derived := state.DeriveRolloutApplyState(children)
	heldByResumableChild := state.RolloutHeldByResumableChild(derived, children)

	// A deployment-keyed apply's operations attach one dispatch at a time, so
	// the attached rows alone cannot prove the generation is done. The stored
	// generation manifest is the completion authority: the apply may derive a
	// whole-generation verdict only when every declared operation has attached
	// (and, via the child derivation above, reached a terminal state). Until
	// then the honest projection over an incomplete generation is running —
	// the work the dispatcher declared is still on its way, and terminalizing
	// early would make later dispatches refuse to attach, silently diverging
	// the declared shards from the recorded outcome.
	manifestHeld := false
	if manifestGatedVerdict(derived) {
		if missing := apply.MissingExpectedOperationKeys(ops); len(missing) > 0 {
			derived = state.Apply.Running
			manifestHeld = true
			s.logger.Debug("operator: holding apply open; manifest operations have not attached yet",
				append(apply.LogAttrs(),
					"driver", driverID,
					"missing_operation_keys", missing,
					"operation_count", len(ops))...)
			metrics.RecordApplyManifestHold(ctx, apply.Database, apply.Deployment, apply.Environment)
		}
	}

	// A failed parent is the one terminal state the rollout projection can
	// legitimately reopen: a sibling failure may have terminalized the parent
	// before the rollout settled, and re-deriving over the operation rows holds
	// it degraded until every sibling settles. This covers both policies
	// that hold — continue, which lets later siblings still run, and a
	// fail-closed policy whose verdict landed while an already-started sibling
	// was working. Gate the exception narrowly: the parent must be failed, the
	// child base must still be failed (a real failure, not a stale parent over
	// non-failed children), the derived projection must be the held degraded
	// state, and the caller must hold the lease.
	reopensHeldFailedRollout := bool(reopen) &&
		state.IsState(apply.State, state.Apply.Failed) &&
		state.IsState(base, state.Apply.Failed) &&
		state.IsState(derived, state.Apply.RunningDegraded)

	if state.IsTerminalApplyState(apply.State) && !state.IsTerminalApplyState(derived) && !reopensHeldFailedRollout {
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
			append(apply.LogAttrs(),
				"driver", driverID, "operation_count", len(ops))...)
		return applyProjectionResult{PreviousState: apply.State, DerivedState: derived, OperationCount: len(ops), ManifestHeld: manifestHeld, HeldByResumableChild: heldByResumableChild}, nil
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
			append(apply.LogAttrs(),
				"driver", driverID, "derived_state", derived, "operation_count", len(ops))...)
		return applyProjectionResult{PreviousState: apply.State, DerivedState: derived, OperationCount: len(ops), ManifestHeld: manifestHeld, HeldByResumableChild: heldByResumableChild}, nil
	}
	s.logger.Info("operator: updated derived apply state from apply_operations",
		append(apply.LogAttrs(),
			"driver", driverID, "derived_state", derived, "operation_count", len(ops))...)

	// The projection is often the last writer standing on an apply — the drives
	// that produced the operation states may be gone (a crashed driver, a
	// lease-lost settle, stop reconciliation). Record the transition in the
	// apply's own durable log so its timeline states how it reached the derived
	// state instead of ending at the last claim entry. A swap that only stamps
	// started_at while the state holds is not a transition, so it writes no
	// timeline entry.
	if state.IsState(apply.State, derived) {
		s.logger.Debug("operator: derived apply state swap stamped started_at only; no state transition to record",
			append(apply.LogAttrs(),
				"driver", driverID, "derived_state", derived)...)
	} else {
		s.appendApplyLog(ctx, s.logger, &storage.ApplyLog{
			ApplyID:   apply.ID,
			Level:     storage.LogLevelInfo,
			EventType: storage.LogEventStateTransition,
			Source:    storage.LogSourceSchemaBot,
			Message:   fmt.Sprintf("Apply state derived from its %d operation row(s): %s", len(ops), derived),
			OldState:  apply.State,
			NewState:  derived,
			CreatedAt: s.clock.Now(),
		}, "how the derived apply state was reached", append(apply.LogAttrs(), "driver", driverID)...)
	}

	// Own the active-apply gauge when the apply's drives suppressed it. The
	// enqueue-time increment is keyed to the parent's primary deployment, and
	// operation-lease-only drives suppress the parent-level metric, so the
	// projection that wins the parent transition is the single point that must
	// release it: -1 when the rollout first reaches a terminal state, and +1
	// if a held rollout reopens the parent to keep it running, so the
	// gauge tracks whether the apply is still in flight. A legacy
	// single-operation apply keeps decrementing in its direct parent-lease
	// drive and is left untouched here.
	if projectionOwnsActiveAppliesGauge(apply, ops) {
		switch {
		case !state.IsTerminalApplyState(apply.State) && state.IsTerminalApplyState(derived):
			metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
		case state.IsTerminalApplyState(apply.State) && !state.IsTerminalApplyState(derived):
			metrics.AdjustActiveApplies(ctx, 1, apply.Database, apply.Deployment, apply.Environment)
		}
	}

	return applyProjectionResult{
		Swapped:              true,
		PreviousState:        apply.State,
		DerivedState:         derived,
		BecameTerminal:       !state.IsTerminalApplyState(apply.State) && state.IsTerminalApplyState(derived),
		OperationCount:       len(ops),
		ManifestHeld:         manifestHeld,
		HeldByResumableChild: heldByResumableChild,
	}, nil
}

// publishTerminalSummaryIfWon publishes the apply-level terminal summary when
// this drive won the aggregate non-terminal→terminal projection CAS, whatever
// the apply's operation count. A live single-operation drive usually has its
// per-driver observer publish first, but when no live observer is left — for
// example stop reconciliation terminalizing an apply whose driver is gone —
// this is the only publisher; the atomic summary-marker claim inside the
// publish path keeps the two exactly-once. Because the parent apply is already
// durably terminal once result.BecameTerminal is true, publishing is best
// effort: every failure is logged with triage identifiers and counted, never
// reverted, and left for summary reconciliation to repair.
func (s *Service) publishTerminalSummaryIfWon(ctx context.Context, driverID int, apply *storage.Apply, result applyProjectionResult) {
	if !result.BecameTerminal {
		return
	}
	if s.OnApplyTerminalSummary == nil {
		s.logger.Debug("operator: aggregate terminal summary publisher not configured; skipping",
			append(apply.LogAttrs(),
				"driver", driverID,
				"derived_state", result.DerivedState, "operation_count", result.OperationCount)...)
		return
	}

	// Reload the parent at its terminal state: the input apply still carries the
	// pre-CAS state, while the summary must render the terminal state, error
	// message, and completion time the projection just stamped.
	terminalApply, err := s.storage.Applies().Get(ctx, apply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload terminal apply for aggregate summary; summary not published",
			append(apply.LogAttrs(),
				"driver", driverID,
				"derived_state", result.DerivedState, "operation_count", result.OperationCount, "error", err)...)
		metrics.RecordOperatorTerminalSummaryFailure(ctx, "reload_apply_error")
		return
	}
	if terminalApply == nil {
		s.logger.Error("operator: terminal apply not found while publishing aggregate summary; summary not published",
			append(apply.LogAttrs(),
				"driver", driverID,
				"derived_state", result.DerivedState, "operation_count", result.OperationCount)...)
		metrics.RecordOperatorTerminalSummaryFailure(ctx, "apply_missing")
		return
	}
	if !state.IsTerminalApplyState(terminalApply.State) {
		s.logger.Error("operator: reloaded apply is no longer terminal while publishing aggregate summary; summary not published",
			append(terminalApply.LogAttrs(),
				"driver", driverID,
				"reloaded_state", terminalApply.State, "derived_state", result.DerivedState,
				"operation_count", result.OperationCount)...)
		metrics.RecordOperatorTerminalSummaryFailure(ctx, "apply_not_terminal_after_cas")
		return
	}

	// Reload every operation's tasks so the summary reflects the whole apply, not
	// just the operation this drive owned.
	tasks, err := s.storage.Tasks().GetByApplyID(ctx, terminalApply.ID)
	if err != nil {
		s.logger.Error("operator: failed to reload tasks for aggregate terminal summary; summary not published",
			append(terminalApply.LogAttrs(),
				"driver", driverID,
				"derived_state", result.DerivedState, "operation_count", result.OperationCount, "error", err)...)
		metrics.RecordOperatorTerminalSummaryFailure(ctx, "reload_tasks_error")
		return
	}

	if err := s.OnApplyTerminalSummary(ctx, terminalApply, tasks); err != nil {
		s.logger.Error("operator: aggregate terminal summary publish failed; parent state stays terminal, summary left for reconciliation",
			append(terminalApply.LogAttrs(),
				"driver", driverID,
				"derived_state", result.DerivedState, "operation_count", result.OperationCount, "error", err)...)
		metrics.RecordOperatorTerminalSummaryFailure(ctx, "callback_error")
		return
	}
	s.logger.Info("operator: published aggregate terminal summary",
		append(terminalApply.LogAttrs(),
			"driver", driverID,
			"derived_state", result.DerivedState, "operation_count", result.OperationCount)...)
}

func driverLeaseOwner(driverID int) string {
	return fmt.Sprintf("%s/driver-%d", storage.LeaseOwnerProcess(), driverID)
}
