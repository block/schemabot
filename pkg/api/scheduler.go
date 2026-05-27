package api

import (
	"context"
	"errors"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
)

// Scheduler constants.
const (
	// SchedulerPollInterval is the default interval for polling applies that need attention.
	SchedulerPollInterval = 10 * time.Second

	// HeartbeatTimeout is how long since last heartbeat before
	// an apply is considered to have a crashed worker and needs recovery.
	// FindNextApply uses this (via SQL: updated_at < NOW() - INTERVAL 1 MINUTE).
	HeartbeatTimeout = 1 * time.Minute

	// DefaultSchedulerWorkers is the number of concurrent scheduler workers
	// when not configured via scheduler_workers in the server config.
	DefaultSchedulerWorkers = 4
)

// StartScheduler starts the background scheduler worker pool.
//
// Scheduler workers claim apply work from storage so one server can make
// progress across independent databases and environments concurrently. This
// includes queued applies, crash recovery for applies with stale heartbeats,
// and retry recovery for transient engine failures.
//
// Launches N concurrent workers (configured via scheduler_workers in config).
// Each worker independently claims applies using FOR UPDATE SKIP LOCKED.
// Call StopScheduler to gracefully stop.
func (s *Service) StartScheduler(ctx context.Context) {
	s.schedulerMu.Lock()
	if s.stopRecovery != nil {
		s.schedulerMu.Unlock()
		s.logger.Info("scheduler already running")
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
	s.schedulerWake = wake
	s.schedulerMu.Unlock()

	for i := range workers {
		workerID := i
		s.recoveryWg.Go(func() {
			s.schedulerWorker(workerCtx, workerID, stop, wake)
		})
	}

	s.logger.Info("scheduler started", "workers", workers, "interval", s.schedulerPollInterval)
}

// StopScheduler stops the background scheduler and waits for all workers to finish.
// Safe to call multiple times.
func (s *Service) StopScheduler() {
	s.schedulerMu.Lock()
	if s.stopRecovery == nil {
		s.schedulerMu.Unlock()
		return
	}
	stop := s.stopRecovery
	cancel := s.cancelRecovery
	s.stopRecovery = nil
	s.cancelRecovery = nil
	s.schedulerWake = nil
	s.schedulerMu.Unlock()

	close(stop)
	if cancel != nil {
		cancel()
	}
	s.recoveryWg.Wait()
}

func (s *Service) wakeScheduler(applyIdentifier, database, environment string) {
	s.schedulerMu.Lock()
	wake := s.schedulerWake
	running := s.stopRecovery != nil
	s.schedulerMu.Unlock()

	if !running || wake == nil {
		s.logger.Debug("scheduler wake skipped because scheduler is not running",
			"apply_id", applyIdentifier,
			"database", database,
			"environment", environment)
		return
	}

	select {
	case wake <- struct{}{}:
		s.logger.Debug("scheduler wake queued",
			"apply_id", applyIdentifier,
			"database", database,
			"environment", environment)
	default:
		s.logger.Debug("scheduler wake already pending",
			"apply_id", applyIdentifier,
			"database", database,
			"environment", environment)
	}
}

// schedulerWorker is a single worker that claims at most one apply on startup
// and on each scheduler poll tick. Wake signals share the same claim path as
// polling; storage locking decides whether a worker actually owns work.
func (s *Service) schedulerWorker(ctx context.Context, workerID int, stop <-chan struct{}, wake <-chan struct{}) {
	ticker := time.NewTicker(s.schedulerPollInterval)
	defer ticker.Stop()

	s.logger.Debug("scheduler worker started", "worker", workerID)

	s.recoverApplies(ctx, workerID)

	for {
		select {
		case <-stop:
			s.logger.Debug("scheduler worker stopping", "worker", workerID)
			return
		case <-ctx.Done():
			s.logger.Debug("scheduler worker context cancelled", "worker", workerID)
			return
		case <-wake:
			s.logger.Debug("scheduler worker woke for queued apply", "worker", workerID)
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
		s.logger.Error("scheduler: failed to expire retryable applies", "worker", workerID, "error", err)
		metrics.RecordSchedulerClaimFailure(ctx, "expire_retryable_error")
		return
	}
	for _, expiration := range expired {
		apply := expiration.Apply
		s.logger.Error("scheduler: retryable apply expired",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"attempt", apply.Attempt,
			"reason", expiration.Reason)
		metrics.RecordSchedulerResumeFailure(ctx, apply.Database, apply.Environment, string(expiration.Reason))
	}

	apply, err := s.storage.Applies().FindNextApply(ctx)
	if err != nil {
		s.logger.Error("scheduler: failed to claim apply", "worker", workerID, "error", err)
		metrics.RecordSchedulerClaimFailure(ctx, "storage_error")
		return
	}

	if apply == nil {
		s.logger.Debug("scheduler: no apply to claim", "worker", workerID)
		return
	}

	start := time.Now()
	s.logger.Info("scheduler: claimed apply",
		"worker", workerID,
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"environment", apply.Environment,
		"state", apply.State,
		"last_heartbeat", apply.UpdatedAt)

	previousState := apply.State

	deployment, err := storedDeploymentForApply(apply)
	if err != nil {
		s.logger.Error("scheduler: claimed apply is missing stored deployment metadata",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
		metrics.RecordSchedulerResumeFailure(ctx, apply.Database, apply.Environment, "missing_deployment")
		return
	}
	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		s.logger.Error("scheduler: failed to get client",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
		metrics.RecordSchedulerResumeFailure(ctx, apply.Database, apply.Environment, "no_client")
		return
	}

	if s.OnApplyRecovered != nil {
		s.OnApplyRecovered(apply)
	}

	retryableClaim := previousState == state.Apply.FailedRetryable
	if retryableClaim {
		metrics.AdjustActiveApplies(ctx, 1, apply.Database, apply.Environment)
	}
	if err := client.ResumeApply(ctx, apply); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			s.logger.Debug("scheduler: stopped while running claimed apply",
				"worker", workerID,
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"environment", apply.Environment,
				"error", err)
			if retryableClaim {
				metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Environment)
			}
			return
		}
		s.logger.Error("scheduler: failed to resume apply",
			"worker", workerID,
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"error", err)
		metrics.RecordSchedulerResumeFailure(ctx, apply.Database, apply.Environment, "resume_error")
		if retryableClaim {
			metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Environment)
		}
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
