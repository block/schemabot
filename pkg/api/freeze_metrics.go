package api

import (
	"context"
	"sort"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

const (
	// FreezeMetricsInterval is how often SchemaBot records which environments
	// are frozen. A planned freeze starts and ends by time rather than by an
	// API call, so only a periodic snapshot keeps the gauge current.
	FreezeMetricsInterval = 30 * time.Second

	// FreezeMetricsTimeout bounds a single snapshot so a slow database cannot
	// stall the monitor loop.
	FreezeMetricsTimeout = 5 * time.Second
)

// StartFreezeMonitor starts a background loop that records, per environment,
// whether a freeze is in effect. It is a no-op without freeze storage.
func (s *Service) StartFreezeMonitor(ctx context.Context) {
	if s.storage == nil {
		s.logger.Debug("freeze monitor not started because storage is unavailable")
		return
	}

	s.freezeMonitorMu.Lock()
	if s.freezeMonitorCancel != nil {
		s.freezeMonitorMu.Unlock()
		s.logger.Info("freeze monitor already running")
		return
	}
	monitorCtx, cancel := context.WithCancel(ctx)
	s.freezeMonitorCancel = cancel
	s.freezeMonitorMu.Unlock()

	s.freezeMonitorWg.Go(func() {
		ticker := time.NewTicker(FreezeMetricsInterval)
		defer ticker.Stop()
		s.CollectFreezeMetrics(monitorCtx)
		for {
			select {
			case <-monitorCtx.Done():
				s.logger.Debug("freeze monitor stopping", "error", monitorCtx.Err())
				return
			case <-ticker.C:
				s.CollectFreezeMetrics(monitorCtx)
			}
		}
	})
	s.logger.Info("freeze monitor started", "interval", FreezeMetricsInterval, "timeout", FreezeMetricsTimeout)
}

// StopFreezeMonitor stops the background freeze monitor. Safe to call
// multiple times.
func (s *Service) StopFreezeMonitor() {
	s.freezeMonitorMu.Lock()
	cancel := s.freezeMonitorCancel
	if cancel == nil {
		s.freezeMonitorMu.Unlock()
		return
	}
	s.freezeMonitorCancel = nil
	s.freezeMonitorMu.Unlock()

	cancel()
	s.drainMonitor(&s.freezeMonitorWg, "freeze_monitor")
}

// CollectFreezeMetrics records schemabot.environment.frozen once: 1 for each
// environment with a freeze in effect, whole or narrowed to some deployments
// or databases, and 0 for every other known environment. It is exported so
// tests can run one synchronous collection.
func (s *Service) CollectFreezeMetrics(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		s.logger.Debug("freeze metrics collection skipped because context is done", "error", err)
		return
	}
	if s.storage == nil || s.storage.Freezes() == nil {
		s.logger.Debug("freeze metrics collection skipped because freeze storage is unavailable")
		return
	}

	collectCtx, cancel := context.WithTimeout(ctx, FreezeMetricsTimeout)
	defer cancel()

	inEffect, err := s.storage.Freezes().ListInEffect(collectCtx, "", s.clock.Now().UTC())
	if err != nil {
		// The gauge is last-value: leaving it untouched re-exports the last
		// snapshot. The failure counter tells operators it is stale.
		metrics.RecordEnvironmentFreezeCollectionFailure(ctx)
		s.logger.Warn("freeze metrics collection failed", "error", err)
		return
	}

	frozen := make(map[string]bool)
	for _, environment := range s.config.KnownEnvironments() {
		frozen[storage.CanonicalKey(environment)] = false
	}
	for _, freeze := range inEffect {
		frozen[storage.CanonicalKey(freeze.Environment)] = true
	}
	// Record every environment that was ever reported frozen too, so an
	// environment dropped from config after its freeze ends returns to 0
	// instead of re-exporting 1 forever.
	s.freezeMonitorMu.Lock()
	if s.freezeMonitorSeen == nil {
		s.freezeMonitorSeen = make(map[string]bool)
	}
	for environment := range s.freezeMonitorSeen {
		if _, ok := frozen[environment]; !ok {
			frozen[environment] = false
		}
	}
	for environment, isFrozen := range frozen {
		if isFrozen {
			s.freezeMonitorSeen[environment] = true
		}
	}
	s.freezeMonitorMu.Unlock()

	environments := make([]string, 0, len(frozen))
	for environment := range frozen {
		environments = append(environments, environment)
	}
	sort.Strings(environments)
	for _, environment := range environments {
		metrics.RecordEnvironmentFrozen(ctx, environment, frozen[environment])
	}
}
