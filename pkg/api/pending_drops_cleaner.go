package api

import (
	"context"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/pendingdrops"
	"github.com/block/schemabot/pkg/storage"
)

// PendingDropsCleanupInterval is how often the pending drops cleaner runs a
// cleanup pass. Quarantined tables are retained for days, so a coarse interval
// is sufficient; the per-target advisory lock keeps concurrent instances from
// duplicating work.
const PendingDropsCleanupInterval = 6 * time.Hour

// StartPendingDropsCleaner starts the background loop that permanently drops
// expired quarantined tables from local-mode MySQL databases. It does not
// start when cleanup is disabled for this process or when no local MySQL
// targets are configured (gRPC-mode targets are cleaned by the deployment that
// executes the schema changes).
func (s *Service) StartPendingDropsCleaner(ctx context.Context) {
	if !s.config.PendingDropsCleanupEnabled() {
		s.logger.Info("pending drops cleaner not started because cleanup is disabled for this process")
		return
	}

	retention, err := s.config.PendingDropsRetention()
	if err != nil {
		// Validate() rejects invalid retention before the server starts, so
		// this guards direct embedders that skip config validation.
		s.logger.Error("pending drops cleaner not started because retention is invalid; quarantined tables will accumulate until the config is fixed", "error", err)
		return
	}

	if !s.hasPendingDropsLocalTargets() {
		s.logger.Info("pending drops cleaner not started because no local MySQL database targets are configured")
		return
	}

	s.pendingDropsMu.Lock()
	if s.pendingDropsCancel != nil {
		s.pendingDropsMu.Unlock()
		s.logger.Info("pending drops cleaner already running")
		return
	}
	cleanerCtx, cancel := context.WithCancel(ctx)
	s.pendingDropsCancel = cancel
	s.pendingDropsMu.Unlock()

	s.pendingDropsWg.Go(func() {
		s.pendingDropsCleanerLoop(cleanerCtx, retention, s.config.PendingDrops.DryRun)
	})
	s.logger.Info("pending drops cleaner started",
		"retention", retention,
		"dry_run", s.config.PendingDrops.DryRun,
		"interval", PendingDropsCleanupInterval,
	)
}

// StopPendingDropsCleaner stops the background pending drops cleaner.
// Safe to call multiple times.
func (s *Service) StopPendingDropsCleaner() {
	s.pendingDropsMu.Lock()
	cancel := s.pendingDropsCancel
	if cancel == nil {
		s.pendingDropsMu.Unlock()
		return
	}
	s.pendingDropsCancel = nil
	s.pendingDropsMu.Unlock()

	cancel()
	s.pendingDropsWg.Wait()
	s.logger.Info("pending drops cleaner stopped")
}

func (s *Service) pendingDropsCleanerLoop(ctx context.Context, retention time.Duration, dryRun bool) {
	ticker := time.NewTicker(PendingDropsCleanupInterval)
	defer ticker.Stop()

	if err := s.runPendingDropsCleanupPass(ctx, retention, dryRun); err != nil {
		s.logger.Error("pending drops cleanup pass was incomplete; failed targets retry on the next pass", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("pending drops cleaner stopping", "error", ctx.Err())
			return
		case <-ticker.C:
			if err := s.runPendingDropsCleanupPass(ctx, retention, dryRun); err != nil {
				s.logger.Error("pending drops cleanup pass was incomplete; failed targets retry on the next pass", "error", err)
			}
		}
	}
}

func (s *Service) runPendingDropsCleanupPass(ctx context.Context, retention time.Duration, dryRun bool) error {
	targets, unresolved := s.pendingDropsTargets(ctx)
	if len(targets) == 0 {
		if unresolved > 0 {
			return fmt.Errorf("resolve pending drops cleanup targets: %d local MySQL target(s) could not resolve DSN", unresolved)
		}
		s.logger.Warn("pending drops cleanup pass found no resolved local MySQL targets; targets with unresolved DSNs will retry on the next pass")
		return nil
	}
	cleaner := pendingdrops.NewCleaner(targets, retention, dryRun, s.logger)
	if err := cleaner.Run(ctx); err != nil {
		if unresolved > 0 {
			return fmt.Errorf("clean resolved pending drops targets; %d local MySQL target(s) could not resolve DSN: %w", unresolved, err)
		}
		return err
	}
	if unresolved > 0 {
		return fmt.Errorf("resolve pending drops cleanup targets: %d local MySQL target(s) could not resolve DSN", unresolved)
	}
	return nil
}

func (s *Service) hasPendingDropsLocalTargets() bool {
	for _, dbConfig := range s.config.Databases {
		if dbConfig.Type != storage.DatabaseTypeMySQL {
			continue
		}
		for _, envConfig := range dbConfig.Environments {
			if envConfig.HasLocalDSN() {
				return true
			}
		}
	}
	return false
}

// pendingDropsTargets resolves the local-mode MySQL databases the cleaner
// inspects. Databases without a local DSN are executed by a remote deployment,
// which owns quarantine cleanup for its own targets.
func (s *Service) pendingDropsTargets(ctx context.Context) ([]pendingdrops.Target, int) {
	var targets []pendingdrops.Target
	var unresolved int
	for dbName, dbConfig := range s.config.Databases {
		if dbConfig.Type != storage.DatabaseTypeMySQL {
			continue
		}
		for envName, envConfig := range dbConfig.Environments {
			if !envConfig.HasLocalDSN() {
				continue
			}
			dsn, err := envConfig.ResolveDSN()
			if err != nil {
				unresolved++
				s.logger.Error("pending drops cleaner skipping target because its DSN could not be resolved; quarantined tables on this target will not be cleaned",
					"database", dbName,
					"environment", envName,
					"error", err,
				)
				metrics.RecordPendingDropsCleanupError(ctx, dbName, envName, "dsn_resolution_error")
				continue
			}
			targets = append(targets, pendingdrops.Target{
				Database:    dbName,
				Environment: envName,
				DSN:         dsn,
			})
		}
	}
	return targets, unresolved
}
