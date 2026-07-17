package api

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

const (
	// WebhookInboxMetricsInterval is how often SchemaBot snapshots the durable
	// webhook inbox for steady-state depth/backlog metrics. With durable
	// dispatch, backpressure shows up as inbox depth rather than request
	// latency, so this cadence must be tight enough to catch a growing backlog
	// well within the dispatch expectation.
	WebhookInboxMetricsInterval = 30 * time.Second

	// WebhookInboxMetricsTimeout bounds a single inbox snapshot so a slow or
	// contended database cannot stall the monitor loop.
	WebhookInboxMetricsTimeout = 5 * time.Second
)

// StartWebhookInboxMonitor starts a background loop that periodically snapshots
// the durable webhook inbox and emits depth, backlog-age, and stuck-processing
// gauges. It is a no-op when webhook storage is unavailable (nothing to
// observe).
func (s *Service) StartWebhookInboxMonitor(ctx context.Context) {
	if s.storage == nil {
		s.logger.Debug("webhook inbox monitor not started because storage is unavailable")
		return
	}

	s.webhookInboxMu.Lock()
	if s.webhookInboxCancel != nil {
		s.webhookInboxMu.Unlock()
		s.logger.Info("webhook inbox monitor already running")
		return
	}
	monitorCtx, cancel := context.WithCancel(ctx)
	s.webhookInboxCancel = cancel
	interval := s.webhookInboxInterval
	s.webhookInboxMu.Unlock()

	s.webhookInboxWg.Go(func() {
		s.webhookInboxMonitor(monitorCtx, interval)
	})
	s.logger.Info("webhook inbox monitor started", "interval", interval, "timeout", WebhookInboxMetricsTimeout)
}

// StopWebhookInboxMonitor stops the background webhook inbox monitor. Safe to
// call multiple times.
func (s *Service) StopWebhookInboxMonitor() {
	s.webhookInboxMu.Lock()
	cancel := s.webhookInboxCancel
	if cancel == nil {
		s.webhookInboxMu.Unlock()
		return
	}
	s.webhookInboxCancel = nil
	s.webhookInboxMu.Unlock()

	cancel()
	s.webhookInboxWg.Wait()
}

func (s *Service) webhookInboxMonitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.CollectWebhookInboxMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("webhook inbox monitor stopping", "error", ctx.Err())
			return
		case <-ticker.C:
			s.CollectWebhookInboxMetrics(ctx)
		}
	}
}

// CollectWebhookInboxMetrics snapshots the inbox once and records its gauges. It
// is exported so tests and diagnostics can run a single synchronous collection
// without starting the background monitor.
func (s *Service) CollectWebhookInboxMetrics(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		s.logger.Debug("webhook inbox metrics collection skipped because context is done", "error", err)
		return
	}
	if s.storage == nil || s.storage.WebhookEvents() == nil {
		s.logger.Debug("webhook inbox metrics collection skipped because webhook storage is unavailable")
		return
	}

	collectCtx, cancel := context.WithTimeout(ctx, WebhookInboxMetricsTimeout)
	defer cancel()

	stats, err := s.storage.WebhookEvents().InboxStats(collectCtx)
	if err != nil {
		// The depth/backlog gauges are last-value instruments: leaving them
		// untouched here re-exports the last-good values with fresh timestamps,
		// which reads as a healthy inbox. The failure counter is the liveness
		// signal that tells operators the gauges are stale.
		metrics.RecordWebhookInboxStatsCollectionFailure(ctx)
		s.logger.Warn("webhook inbox metrics collection failed", "error", err)
		return
	}

	recorded := make(map[string]bool, len(stats.CountsByState))
	for _, state := range storage.WebhookEventStatesAll {
		metrics.RecordWebhookInboxDepth(ctx, state, stats.CountsByState[state])
		recorded[state] = true
	}
	// The state column is a free-form varchar, not an enum, so a value outside
	// the canonical set could appear. Sum any such rows into a single unknown
	// series rather than dropping them, so an unexpected state still surfaces.
	var unknownCount int64
	for state, count := range stats.CountsByState {
		if !recorded[state] {
			unknownCount += count
		}
	}
	// Always record unknown, including 0, like the canonical states above:
	// inbox_depth is a last-value gauge, so skipping the record when the count
	// drops back to 0 would leave the gauge re-exporting its last nonzero value
	// forever, showing a phantom unknown-state population that never resolves.
	metrics.RecordWebhookInboxDepth(ctx, "unknown", unknownCount)
	metrics.RecordWebhookInboxOldestClaimableAge(ctx, stats.OldestClaimableAge)
	metrics.RecordWebhookInboxStuckProcessing(ctx, stats.StuckProcessing)

	if stats.StuckProcessing > 0 {
		s.logger.Warn("durable webhook inbox has rows stuck in processing past the attempt cap",
			"stuck_processing", stats.StuckProcessing)
	}
}
