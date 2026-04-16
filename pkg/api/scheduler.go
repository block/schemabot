package api

import (
	"context"
	"time"
)

// Recovery worker constants.
const (
	// RecoveryPollInterval is how often we poll for in-progress applies that need recovery.
	RecoveryPollInterval = 10 * time.Second

	// HeartbeatTimeout is how long since last heartbeat before
	// an apply is considered to have a crashed worker and needs recovery.
	// ClaimForRecovery uses this (via SQL: updated_at < NOW() - INTERVAL 1 MINUTE).
	HeartbeatTimeout = 1 * time.Minute
)

// StartRecoveryWorker starts a background worker that finds and resumes
// in-progress applies whose workers crashed.
//
//   - Runs immediately on startup, then polls every 10 seconds
//   - Finds applies with no heartbeat for 1+ minute (worker crashed)
//   - Claims them using FOR UPDATE SKIP LOCKED (prevents races)
//   - Resumes the apply via the appropriate Tern client
//   - STOPPED applies are NOT auto-resumed (user must call `schemabot start`)
//
// This unified approach (vs separate startup + background recovery) ensures:
//   - No race window if server restarts quickly (within 1 minute of last heartbeat)
//   - In-progress applies are always recovered once their heartbeat times out
//
// Call StopRecoveryWorker to gracefully stop the worker.
func (s *Service) StartRecoveryWorker(ctx context.Context) {
	if s.stopRecovery != nil {
		s.logger.Info("recovery worker already running")
		return
	}
	s.stopRecovery = make(chan struct{})
	s.recoveryWg.Go(func() {
		ticker := time.NewTicker(RecoveryPollInterval)
		defer ticker.Stop()

		s.logger.Info("started recovery worker", "interval", RecoveryPollInterval)

		// Run immediately on startup, then on each tick
		s.resumeInProgressApplies(ctx)

		for {
			select {
			case <-s.stopRecovery:
				s.logger.Info("stopping recovery worker")
				return
			case <-ctx.Done():
				s.logger.Info("recovery worker context cancelled")
				return
			case <-ticker.C:
				s.resumeInProgressApplies(ctx)
			}
		}
	})
}

// StopRecoveryWorker stops the background recovery worker and waits for it to finish.
// Safe to call multiple times.
func (s *Service) StopRecoveryWorker() {
	if s.stopRecovery == nil {
		return
	}
	close(s.stopRecovery)
	s.stopRecovery = nil
	s.recoveryWg.Wait()
}

// resumeInProgressApplies finds and resumes all in-progress applies whose workers crashed.
// Loops until no more applies need recovery.
func (s *Service) resumeInProgressApplies(ctx context.Context) {
	var recovered, failed int

	// Loop until no more applies to recover
	for {
		apply, err := s.storage.Applies().ClaimForRecovery(ctx)
		if err != nil {
			s.logger.Error("recovery worker: failed to claim apply", "error", err)
			break
		}

		if apply == nil {
			// No more applies need recovery
			break
		}

		s.logger.Info("recovery worker: found in-progress apply with crashed worker",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"state", apply.State,
			"last_heartbeat", apply.UpdatedAt)

		// Get Tern client using the deployment stored on the apply.
		deployment := s.resolveDeployment(apply.Database, apply.Deployment)
		client, err := s.TernClient(deployment, apply.Environment)
		if err != nil {
			s.logger.Error("recovery worker: failed to get client",
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"environment", apply.Environment,
				"error", err)
			failed++
			continue
		}

		// Resume the apply
		if err := client.ResumeApply(ctx, apply); err != nil {
			s.logger.Error("recovery worker: failed to resume apply",
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"error", err)
			failed++
			continue
		}

		s.logger.Info("recovery worker: resumed apply",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"previous_state", apply.State)
		recovered++
	}

	if recovered > 0 || failed > 0 {
		s.logger.Info("recovery worker: cycle complete",
			"recovered", recovered,
			"failed", failed)
	}
}
