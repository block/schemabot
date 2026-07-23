package api

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/metrics"
)

const (
	// OperatorStuckPendingApplyThreshold is how long a pending apply may carry
	// its claimable child rows before the monitor treats it as stuck. Apply
	// creation rejects a second active apply for the same target rather than
	// queuing it, so a pending apply is never waiting its turn; once a driver
	// claims it, it leaves the pending state. A pending apply unclaimed this long
	// therefore means no driver reached it — a saturated, operator-less, or
	// wedged driver pool. It is set well above the claim cadence so ordinary
	// claim latency never trips it.
	OperatorStuckPendingApplyThreshold = 15 * time.Minute

	// OperatorStuckPendingScanInterval is how often the monitor scans for stuck
	// pending applies. It is coarse relative to the threshold: the scan only
	// answers "has an unclaimed pending apply aged past the threshold", which
	// does not need sub-minute resolution.
	OperatorStuckPendingScanInterval = time.Minute

	// OperatorStuckPendingScanTimeout bounds a single scan so a slow or
	// contended database cannot stall the monitor loop.
	OperatorStuckPendingScanTimeout = 5 * time.Second

	// operatorStuckPendingSampleLimit caps how many stuck rows a single scan
	// pulls back. The gauge counts what the scan returns, so a value at the cap
	// means "at least this many"; the cap keeps a pathological backlog from
	// loading an unbounded result set into memory every scan.
	operatorStuckPendingSampleLimit = 500
)

// StartOperatorStuckPendingMonitor starts a background loop that periodically
// scans for pending applies a driver should have claimed and emits the
// stuck-pending gauge. It is a no-op when storage is unavailable (nothing to
// observe).
func (s *Service) StartOperatorStuckPendingMonitor(ctx context.Context) {
	if s.storage == nil {
		s.logger.Debug("operator stuck-pending monitor not started because storage is unavailable")
		return
	}

	s.stuckPendingMu.Lock()
	if s.stuckPendingCancel != nil {
		s.stuckPendingMu.Unlock()
		s.logger.Info("operator stuck-pending monitor already running")
		return
	}
	monitorCtx, cancel := context.WithCancel(ctx)
	s.stuckPendingCancel = cancel
	s.stuckPendingMu.Unlock()

	s.stuckPendingWg.Go(func() {
		s.operatorStuckPendingMonitor(monitorCtx, OperatorStuckPendingScanInterval)
	})
	s.logger.Info("operator stuck-pending monitor started",
		"interval", OperatorStuckPendingScanInterval,
		"threshold", OperatorStuckPendingApplyThreshold,
		"timeout", OperatorStuckPendingScanTimeout)
}

// StopOperatorStuckPendingMonitor stops the background stuck-pending monitor.
// Safe to call multiple times.
func (s *Service) StopOperatorStuckPendingMonitor() {
	s.stuckPendingMu.Lock()
	cancel := s.stuckPendingCancel
	if cancel == nil {
		s.stuckPendingMu.Unlock()
		return
	}
	s.stuckPendingCancel = nil
	s.stuckPendingMu.Unlock()

	cancel()
	s.stuckPendingWg.Wait()
}

func (s *Service) operatorStuckPendingMonitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.CollectOperatorStuckPendingMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("operator stuck-pending monitor stopping", "error", ctx.Err())
			return
		case <-ticker.C:
			s.CollectOperatorStuckPendingMetrics(ctx)
		}
	}
}

// CollectOperatorStuckPendingMetrics scans once for stuck pending applies and
// records the gauge. It is exported so tests and diagnostics can run a single
// synchronous collection without starting the background monitor.
func (s *Service) CollectOperatorStuckPendingMetrics(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		s.logger.Debug("operator stuck-pending scan skipped because context is done", "error", err)
		return
	}
	if s.storage == nil || s.storage.Applies() == nil {
		s.logger.Debug("operator stuck-pending scan skipped because apply storage is unavailable")
		return
	}

	scanCtx, cancel := context.WithTimeout(ctx, OperatorStuckPendingScanTimeout)
	defer cancel()

	stuck, err := s.storage.Applies().FindStuckPendingApplies(scanCtx, OperatorStuckPendingApplyThreshold, operatorStuckPendingSampleLimit)
	if err != nil {
		// The gauge is a last-value instrument: leaving it untouched here
		// re-exports the last-good value with a fresh timestamp, which reads as a
		// healthy operator. The failure counter is the liveness signal that tells
		// operators the gauge is stale.
		metrics.RecordOperatorStuckPendingScanFailure(ctx)
		s.logger.Warn("operator stuck-pending scan failed", "error", err)
		return
	}

	metrics.RecordOperatorStuckPendingApplies(ctx, int64(len(stuck)))

	if len(stuck) == 0 {
		return
	}

	// stuck is ordered oldest first, so the first row is the longest-waiting one
	// and the most useful triage handle. A driver should already have claimed
	// these; log the count and the oldest so an operator can start there.
	oldest := stuck[0]
	oldestAge := s.clock.Now().Sub(oldest.CreatedAt)
	s.logger.Warn("operator has pending applies a driver should have claimed; check driver pool liveness and saturation",
		append(oldest.LogAttrs(),
			"stuck_pending", len(stuck),
			"threshold", OperatorStuckPendingApplyThreshold,
			"oldest_pending_age", oldestAge)...)
}
