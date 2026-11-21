package notifications

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/panicsafe"
	"github.com/block/schemabot/pkg/storage"
)

// Runner catches up within the lookback and observes backlog independently.
// One Runner belongs to one server lifecycle.
type Runner struct {
	observationInterval time.Duration
	store               storage.NotificationStore
	publisher           Publisher
	logger              *slog.Logger
	lookback            time.Duration
	interval            time.Duration
	batch               int
	sweep               *observationSweep
}

type observationSweep struct {
	through, after int64
	at             time.Time
	oldest         *time.Time
	outside        int64
}

func New(store storage.NotificationStore, publisher Publisher, cfg Config, logger *slog.Logger) (*Runner, error) {
	lookback, interval, batch, err := cfg.settings()
	if err != nil {
		return nil, err
	}
	if store == nil || publisher == nil {
		return nil, fmt.Errorf("notification runner requires storage and publisher")
	}
	observationInterval, err := cfg.observationInterval()
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{observationInterval: observationInterval, store: store, publisher: publisher, logger: logger, lookback: lookback, interval: interval, batch: batch}, nil
}

// Run starts delivery and observation immediately. Each has its own cadence;
// historical observation never blocks recent delivery. Cancellation stops both.
func (r *Runner) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Go(func() { r.loop(ctx, r.observationInterval, r.observe) })
	r.loop(ctx, r.interval, r.Pass)
	wg.Wait()
	metrics.PublishApplyNotificationBacklog(nil)
}

func (r *Runner) loop(ctx context.Context, interval time.Duration, pass func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			r.logger.Debug("apply notification loop stopped", "error", ctx.Err())
			return
		}
		panicErr, err := panicsafe.Catch(func() error { return pass(ctx) })
		if panicErr != nil {
			r.logger.Error("apply notification pass panicked; next tick retries", "error", panicErr)
		}
		if err != nil {
			r.logger.Warn("apply notification pass failed; next tick retries", "error", err)
		}
		select {
		case <-ctx.Done():
			r.logger.Debug("apply notification loop stopping", "error", ctx.Err())
			return
		case <-ticker.C:
		}
	}
}

// Pass publishes one bounded batch. It is also the restart catch-up operation:
// all authority comes from storage, not memory, logs, or GitHub observers.
func (r *Runner) Pass(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, QueryTimeout)
	applies, err := r.store.FindUnpublished(queryCtx, r.lookback, r.batch)
	cancel()
	if err != nil {
		return fmt.Errorf("scan completed applies: %w", err)
	}
	var errs []error
	for _, apply := range applies {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		if err := r.publish(ctx, apply); err != nil {
			r.logger.Warn("apply completion publication failed; claim release or expiry permits retry", append(apply.LogAttrs(), "error", err)...)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Runner) publish(ctx context.Context, apply *storage.Apply) error {
	if apply.CompletedAt == nil {
		return fmt.Errorf("completed apply %s has no completion time", apply.ApplyIdentifier)
	}
	token := uuid.NewString()
	claimCtx, cancel := context.WithTimeout(ctx, QueryTimeout)
	won, err := r.store.Claim(claimCtx, apply.ID, token, ClaimTTL)
	cancel()
	if err != nil {
		return fmt.Errorf("claim apply %s publication: %w", apply.ApplyIdentifier, err)
	}
	if !won {
		r.logger.Debug("apply notification claimed by another publisher", apply.LogAttrs()...)
		return nil
	}
	sendCtx, cancel := context.WithTimeout(ctx, SendTimeout)
	err = r.publisher.Publish(sendCtx, Event{
		ApplyIdentifier: apply.ApplyIdentifier, Database: apply.Database, DatabaseType: apply.DatabaseType,
		Environment: apply.Environment, Repository: apply.Repository, PullRequest: apply.PullRequest, CompletedAt: apply.CompletedAt.UTC(),
	})
	cancel()
	finishCtx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	var changed bool
	var finishErr error
	if err != nil {
		// Store the reason for the latest failed send, not an attempt history.
		changed, finishErr = r.store.Release(finishCtx, apply.ID, token, err.Error())
	} else {
		changed, finishErr = r.store.MarkPublished(finishCtx, apply.ID, token)
	}
	if finishErr != nil {
		return errors.Join(err, fmt.Errorf("finish apply %s publication: %w", apply.ApplyIdentifier, finishErr))
	}
	if !changed {
		return errors.Join(err, fmt.Errorf("apply %s notification claim expired or changed before finish", apply.ApplyIdentifier))
	}
	return err
}

// observe consumes one primary-key page per tick, never a filtered all-history
// COUNT. Cutoff/ceiling stay fixed through a sweep; concurrent state changes are
// observed by the next sweep. An unfinished sweep never replaces a good gauge.
func (r *Runner) observe(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	if r.sweep == nil {
		through, at, err := r.store.ObservationCeiling(queryCtx)
		if err != nil {
			return err
		}
		r.sweep = &observationSweep{through: through, at: at}
	}
	sweep := r.sweep
	page, err := r.store.ObservePage(queryCtx, sweep.after, sweep.through, ObservationPageSize)
	if err != nil {
		return err
	}
	cutoff := sweep.at.Add(-r.lookback)
	for _, sample := range page {
		sweep.after = sample.ID
		if !sample.Completed || sample.CompletedAt == nil || sample.Published || sample.CompletedAt.After(sweep.at) {
			continue
		}
		if sweep.oldest == nil || sample.CompletedAt.Before(*sweep.oldest) {
			oldest := *sample.CompletedAt
			sweep.oldest = &oldest
		}
		if sample.CompletedAt.Before(cutoff) {
			sweep.outside++
		}
	}
	if len(page) < ObservationPageSize || sweep.after >= sweep.through {
		metrics.PublishApplyNotificationBacklog(&metrics.ApplyNotificationBacklog{Oldest: sweep.oldest, OutOfWindow: sweep.outside})
		r.sweep = nil
	}
	return nil
}
