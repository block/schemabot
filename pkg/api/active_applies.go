package api

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/metrics"
)

const (
	// ActiveAppliesScanInterval is how often the monitor samples the active
	// apply population from storage. Applies run for minutes to hours, so a
	// minute of gauge resolution is enough to see long-running work without
	// putting a grouped count query on a hot path.
	ActiveAppliesScanInterval = time.Minute

	// ActiveAppliesScanTimeout bounds a single scan so a slow or contended
	// database cannot stall the monitor loop.
	ActiveAppliesScanTimeout = 5 * time.Second
)

// activeAppliesTarget keys one active-applies gauge series: the attribute set
// the storage scan groups by.
type activeAppliesTarget struct {
	database    string
	deployment  string
	environment string
}

// StartActiveAppliesMonitor starts a background loop that periodically counts
// non-terminal applies from storage and emits the active-applies gauge per
// database/deployment/environment. Sampling storage rather than adjusting a
// counter in-process keeps the gauge truthful across pod restarts and lease
// handovers: an apply that is still running keeps being reported however many
// times its driver changes. It is a no-op when storage is unavailable
// (nothing to observe).
func (s *Service) StartActiveAppliesMonitor(ctx context.Context) {
	if s.storage == nil {
		s.logger.Debug("active-applies monitor not started because storage is unavailable")
		return
	}

	s.activeAppliesMu.Lock()
	if s.activeAppliesCancel != nil {
		s.activeAppliesMu.Unlock()
		s.logger.Info("active-applies monitor already running")
		return
	}
	monitorCtx, cancel := context.WithCancel(ctx)
	s.activeAppliesCancel = cancel
	s.activeAppliesMu.Unlock()

	s.activeAppliesWg.Go(func() {
		s.activeAppliesMonitor(monitorCtx, ActiveAppliesScanInterval)
	})
	s.logger.Info("active-applies monitor started",
		"interval", ActiveAppliesScanInterval,
		"timeout", ActiveAppliesScanTimeout)
}

// StopActiveAppliesMonitor stops the background active-applies monitor. Safe
// to call multiple times.
func (s *Service) StopActiveAppliesMonitor() {
	s.activeAppliesMu.Lock()
	cancel := s.activeAppliesCancel
	if cancel == nil {
		s.activeAppliesMu.Unlock()
		return
	}
	s.activeAppliesCancel = nil
	s.activeAppliesMu.Unlock()

	cancel()
	s.activeAppliesWg.Wait()
}

func (s *Service) activeAppliesMonitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.CollectActiveAppliesMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("active-applies monitor stopping", "error", ctx.Err())
			return
		case <-ticker.C:
			s.CollectActiveAppliesMetrics(ctx)
		}
	}
}

// CollectActiveAppliesMetrics samples the non-terminal apply population once
// and records the active-applies gauge for every target. It is exported so
// tests and diagnostics can run a single synchronous collection without
// starting the background monitor.
func (s *Service) CollectActiveAppliesMetrics(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		s.logger.Debug("active-applies scan skipped because context is done", "error", err)
		return
	}
	if s.storage == nil || s.storage.Applies() == nil {
		s.logger.Debug("active-applies scan skipped because apply storage is unavailable")
		return
	}

	scanCtx, cancel := context.WithTimeout(ctx, ActiveAppliesScanTimeout)
	defer cancel()

	counts, err := s.storage.Applies().CountActiveApplies(scanCtx)
	if err != nil {
		// A shutdown that lands mid-scan cancels the monitor context and fails
		// the query with a cancellation error. That is orderly teardown, not a
		// stale gauge: the failure counter is documented as "the gauge must not
		// be trusted", and a routine deploy must not be able to tick it.
		if ctx.Err() != nil {
			s.logger.Debug("active-applies scan aborted because context is done", "error", err)
			return
		}
		// The gauge is a last-value instrument: leaving every target untouched
		// here re-exports the last-good values with fresh timestamps, which
		// reads as a stable population. The failure counter is the liveness
		// signal that tells operators the gauge is stale.
		metrics.RecordActiveAppliesScanFailure(ctx)
		s.logger.Warn("active-applies scan failed; the active-applies gauge is stale until a scan succeeds", "error", err)
		return
	}

	current := make(map[activeAppliesTarget]int64, len(counts))
	for _, count := range counts {
		current[activeAppliesTarget{
			database:    count.Database,
			deployment:  count.Deployment,
			environment: count.Environment,
		}] = count.Count
	}

	// Swap the seen set under the lock, then record outside it: the recorded
	// values come from this scan's snapshot, so a concurrent collection cannot
	// interleave stale values into it.
	s.activeAppliesMu.Lock()
	previous := s.activeAppliesSeen
	seen := make(map[activeAppliesTarget]struct{}, len(current))
	for target := range current {
		seen[target] = struct{}{}
	}
	s.activeAppliesSeen = seen
	s.activeAppliesMu.Unlock()

	for target, count := range current {
		metrics.RecordActiveApplies(ctx, count, target.database, target.deployment, target.environment)
	}
	// A target the last scan reported but this scan did not has no active
	// applies left. Record 0 once so the last-value series reflects that the
	// work finished instead of freezing at its final nonzero count.
	for target := range previous {
		if _, ok := current[target]; !ok {
			metrics.RecordActiveApplies(ctx, 0, target.database, target.deployment, target.environment)
		}
	}
}
