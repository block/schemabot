package api

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/storage"
)

// The storage retention pass deletes old rows from SchemaBot's own tables so
// they do not grow without bound. Like the reaper's sweeps, it runs only on the
// elected instance and only from the operator's maintenance loop, never from a
// request handler. It deletes only rows whose work is finished, and the store
// keeps any row the webhook reconciler still reads as proof that a pull
// request's head was planned.
//
//	every storage_retention.interval
//	     │
//	     ▼
//	elect one instance (advisory lock) ─── held elsewhere ──▶ skip the pass
//	     │ elected
//	     ▼
//	dry run? ── yes ──▶ count what a purge would delete, delete nothing
//	     │ no
//	     ▼
//	delete one batch of old finished rows ─── fewer than a full batch ──▶ done
//	     │ a full batch
//	     ▼
//	pause, then delete the next batch, up to the cap on batches per pass
//
// The election is an efficiency gate, not a safety one. Each delete repeats its
// own state and age predicate and is idempotent, so two instances running the
// pass at once would be correct, just wasteful.
//
// There is deliberately no metric for the pass: it runs too rarely for a rate
// to mean anything. Each pass logs one line
// naming the table, how many rows it deleted or would delete, how many batches
// it took, the retention window, and whether it was a dry run.

const (
	// storageRetentionMaxBatches caps how many batches one pass deletes, so a
	// pass that finds a large backlog cannot hold the election or the database
	// for long. The cap leaves a first backlog to drain over many hourly passes,
	// while a table that is already kept trimmed is emptied by the first batch
	// or two. A pass that stops at the cap says so in its log line, and the next
	// pass continues where it stopped.
	storageRetentionMaxBatches = 100

	// storageRetentionBatchPause is how long a pass waits between batches, so a
	// backlog drains in small steps that leave the database room for the
	// webhook inbox's own claims and writes.
	storageRetentionBatchPause = 200 * time.Millisecond
)

// storageRetentionSettings is what one pass reads from the config and the
// package's pacing constants. It is resolved once when the pass is built, so
// the pass itself never re-parses config.
type storageRetentionSettings struct {
	dryRun        bool
	batchSize     int
	webhookEvents time.Duration

	// maxBatches and batchPause pace a pass that deletes. They are fields so a
	// test can shrink them; storageRetentionPass sets them to the constants.
	maxBatches int
	batchPause time.Duration
}

// storageRetentionPass returns the pass to run, how often to run it, and true,
// or false when the pass should not start. Each reason it declines is logged
// separately, because each asks something different of the operator.
func (s *Service) storageRetentionPass() (func(context.Context), time.Duration, bool) {
	if !s.config.StorageRetentionEnabled() {
		s.logger.Debug("storage retention pass not started because it is disabled; old rows are kept until deleted by hand")
		return nil, 0, false
	}
	// Validate() rejects invalid values before the server starts, so these
	// guard direct embedders that skip config validation.
	interval, err := s.config.StorageRetentionInterval()
	if err != nil {
		s.logger.Error("storage retention pass not started because its interval is invalid; old rows are kept until the config is fixed", "error", err)
		return nil, 0, false
	}
	batchSize, err := s.config.StorageRetentionBatchSize()
	if err != nil {
		s.logger.Error("storage retention pass not started because its batch size is invalid; old rows are kept until the config is fixed", "error", err)
		return nil, 0, false
	}
	webhookEvents, err := s.config.StorageRetentionWebhookEvents()
	if err != nil {
		s.logger.Error("storage retention pass not started because its webhook_events window is invalid; old rows are kept until the config is fixed", "error", err)
		return nil, 0, false
	}
	settings := storageRetentionSettings{
		dryRun:        s.config.StorageRetentionDryRun(),
		batchSize:     batchSize,
		webhookEvents: webhookEvents,
		maxBatches:    storageRetentionMaxBatches,
		batchPause:    storageRetentionBatchPause,
	}
	s.logger.Info("storage retention pass enabled",
		"dry_run", settings.dryRun, "interval", interval.String(), "batch_size", batchSize,
		"max_batches_per_pass", settings.maxBatches, "webhook_events_window", webhookEvents.String())
	return func(ctx context.Context) {
		s.runStorageRetentionPass(ctx, settings)
	}, interval, true
}

// runStorageRetentionPass purges every table the pass covers on the elected
// instance. Losing the election is the expected outcome on every instance but
// one, and a pass cut short by shutdown is a routine deploy, so neither is
// logged as a failure. A purge that fails has already logged itself with the
// table and window; only a failure to take part in the election, which no purge
// has logged, is logged here. The next interval retries either way.
func (s *Service) runStorageRetentionPass(ctx context.Context, settings storageRetentionSettings) {
	purged := false
	err := s.storage.Applies().WithStorageRetentionElection(ctx, func(ctx context.Context) error {
		purged = true
		return s.purgeWebhookEvents(ctx, settings)
	})
	switch {
	case err == nil:
	case errors.Is(err, storage.ErrStorageRetentionBusy):
		s.logger.Debug("storage retention: another instance is purging old rows; skipping this pass")
	case ctx.Err() != nil:
		s.logger.Debug("storage retention: pass interrupted by shutdown", "error", err)
	case !purged:
		s.logger.Error("storage retention pass could not take part in the election; old rows are kept until the next pass", "error", err)
	}
}

// purgeWebhookEvents counts or deletes finished webhook inbox rows older than
// the window. A dry run reads only the count. A real pass deletes batch after
// batch until one comes back short, the cap is reached, or shutdown arrives. It
// returns the error it logged so the election wrapper does not log it again.
func (s *Service) purgeWebhookEvents(ctx context.Context, settings storageRetentionSettings) error {
	const table = "webhook_events"
	// The window is logged as text because a JSON log handler would otherwise
	// render a duration as a bare nanosecond count.
	logger := s.logger.With("table", table, "window", settings.webhookEvents.String(), "dry_run", settings.dryRun)
	events := s.storage.WebhookEvents()

	if settings.dryRun {
		wouldDelete, err := events.CountTerminalOlderThan(ctx, settings.webhookEvents)
		if err != nil {
			return logStorageRetentionFailure(ctx, logger, 0, 0, err)
		}
		logger.Info("storage retention pass complete", "rows_would_delete", wouldDelete, "batches", 0)
		return nil
	}

	var deleted int64
	batches := 0
	for {
		n, err := events.DeleteTerminalOlderThan(ctx, settings.webhookEvents, settings.batchSize)
		if err != nil {
			return logStorageRetentionFailure(ctx, logger, deleted, batches, err)
		}
		deleted += n
		batches++

		if n < int64(settings.batchSize) {
			logger.Info("storage retention pass complete", "rows_deleted", deleted, "batches", batches, "stopped_at_batch_cap", false)
			return nil
		}
		if batches >= settings.maxBatches {
			logger.Info("storage retention pass stopped at its batch cap; the next pass continues",
				"rows_deleted", deleted, "batches", batches, "stopped_at_batch_cap", true)
			return nil
		}
		if !waitBetweenBatches(ctx, settings.batchPause) {
			logger.Info("storage retention pass interrupted by shutdown; the next pass continues",
				"rows_deleted", deleted, "batches", batches)
			return ctx.Err()
		}
	}
}

// logStorageRetentionFailure logs a purge that ended on an error and returns
// that error. A purge cut short because the server is shutting down is a
// routine deploy, so it is logged at debug rather than as a failure.
func logStorageRetentionFailure(ctx context.Context, logger *slog.Logger, deleted int64, batches int, err error) error {
	if ctx.Err() != nil {
		logger.Debug("storage retention pass interrupted by shutdown; the next pass continues",
			"rows_deleted", deleted, "batches", batches, "error", err)
		return err
	}
	logger.Error("storage retention pass failed; the pass ends and the next interval retries",
		"rows_deleted", deleted, "batches", batches, "error", err)
	return err
}

// waitBetweenBatches pauses for d and reports whether the pass should go on.
// It returns false as soon as ctx ends, so shutdown never waits out a pause.
func waitBetweenBatches(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
