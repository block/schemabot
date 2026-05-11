package api

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Scheduler constants.
const (
	// SchedulerPollInterval is how often each worker polls for applies that need attention.
	SchedulerPollInterval = 10 * time.Second

	// HeartbeatTimeout is how long since last heartbeat before
	// an apply is considered to have a crashed worker and needs recovery.
	// FindNextApply uses this (via SQL: updated_at < NOW() - INTERVAL 1 MINUTE).
	HeartbeatTimeout = 1 * time.Minute

	// DefaultSchedulerWorkers is the number of concurrent scheduler workers
	// when not configured via scheduler_workers in the server config.
	DefaultSchedulerWorkers = 1
)

// StartScheduler starts the background scheduler that handles:
//   - Crash recovery: resumes applies whose workers crashed (stale heartbeats)
//   - Retryable recovery: re-dispatches failed_retryable applies within attempt limit
//   - Retry expiration: transitions exhausted failed_retryable applies to permanent failed
//
// Launches N concurrent workers (configured via scheduler_workers in config).
// Each worker independently claims applies using FOR UPDATE SKIP LOCKED.
// Call StopScheduler to gracefully stop.
func (s *Service) StartScheduler(ctx context.Context) {
	if s.stopRecovery != nil {
		s.logger.Info("scheduler already running")
		return
	}

	workers := s.config.SchedulerWorkers
	if workers <= 0 {
		workers = DefaultSchedulerWorkers
	}

	s.stopRecovery = make(chan struct{})

	for i := range workers {
		workerID := i
		s.recoveryWg.Go(func() {
			s.schedulerWorker(ctx, workerID)
		})
	}

	s.logger.Info("scheduler started", "workers", workers, "interval", SchedulerPollInterval)
}

// StopScheduler stops the background scheduler and waits for all workers to finish.
// Safe to call multiple times.
func (s *Service) StopScheduler() {
	if s.stopRecovery == nil {
		return
	}
	close(s.stopRecovery)
	s.stopRecovery = nil
	s.recoveryWg.Wait()
}

// schedulerWorker is a single worker that polls for applies on each tick.
func (s *Service) schedulerWorker(ctx context.Context, workerID int) {
	ticker := time.NewTicker(SchedulerPollInterval)
	defer ticker.Stop()

	s.logger.Debug("scheduler worker started", "worker", workerID)

	// Run immediately on startup, then on each tick
	s.schedulerTick(ctx, workerID)

	for {
		select {
		case <-s.stopRecovery:
			s.logger.Debug("scheduler worker stopping", "worker", workerID)
			return
		case <-ctx.Done():
			s.logger.Debug("scheduler worker context cancelled", "worker", workerID)
			return
		case <-ticker.C:
			s.schedulerTick(ctx, workerID)
		}
	}
}

// schedulerTick runs one cycle: expire exhausted retries, then claim and resume applies.
func (s *Service) schedulerTick(ctx context.Context, workerID int) {
	// Only one worker needs to run expiry per cycle (idempotent, but avoid redundant work)
	if workerID == 0 {
		s.expireExhaustedRetries(ctx)
	}
	s.recoverApplies(ctx, workerID)
}

// expireExhaustedRetries transitions failed_retryable applies that have
// exhausted their retry budget to permanent failed.
func (s *Service) expireExhaustedRetries(ctx context.Context) {
	n, err := s.storage.Applies().ExpireRetryable(ctx)
	if err != nil {
		s.logger.Error("scheduler: expire retryable failed", "error", err)
		return
	}
	if n > 0 {
		s.logger.Info("scheduler: expired exhausted retryable applies", "count", n)
		metrics.RecordSchedulerExpired(ctx, n)
	}
}

// recoverApplies claims and resumes applies that need attention.
// Each call claims one apply (if available) to keep the scheduling loop responsive.
func (s *Service) recoverApplies(ctx context.Context, workerID int) {
	apply, err := s.storage.Applies().FindNextApply(ctx)
	if err != nil {
		s.logger.Error("scheduler: failed to claim apply", "worker", workerID, "error", err)
		return
	}

	if apply == nil {
		return
	}

	start := time.Now()
	s.logger.Info("scheduler: claimed apply",
		"worker", workerID,
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"environment", apply.Environment,
		"state", apply.State,
		"attempt", apply.Attempt,
		"last_heartbeat", apply.UpdatedAt)

	previousState := apply.State

	deployment := s.resolveDeployment(apply.Database, apply.Deployment)
	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		s.logger.Error("scheduler: failed to get client",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
		if apply.State == state.Apply.FailedRetryable {
			s.failApply(ctx, apply, "scheduler: no client available for retry")
		}
		metrics.RecordSchedulerResumeFailure(ctx, apply.Database, apply.Environment, "no_client")
		return
	}

	if err := client.ResumeApply(ctx, apply); err != nil {
		s.logger.Error("scheduler: failed to resume apply",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"error", err)
		metrics.RecordSchedulerResumeFailure(ctx, apply.Database, apply.Environment, "resume_error")
		return
	}

	duration := time.Since(start)
	s.logger.Info("scheduler: resumed apply",
		"worker", workerID,
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"environment", apply.Environment,
		"previous_state", previousState,
		"duration", duration)
	metrics.RecordSchedulerResume(ctx, apply.Database, apply.Environment, previousState)
	metrics.RecordSchedulerClaimDuration(ctx, duration, apply.Database, apply.Environment, previousState)
}

// failApply permanently fails an apply that can't be recovered.
func (s *Service) failApply(ctx context.Context, apply *storage.Apply, errMsg string) {
	now := time.Now()
	apply.State = state.Apply.Failed
	apply.ErrorMessage = errMsg
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	if err := s.storage.Applies().Update(ctx, apply); err != nil {
		s.logger.Error("scheduler: failed to mark apply as permanently failed",
			"apply_id", apply.ApplyIdentifier, "error", err)
	}
}
