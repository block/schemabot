package api

import (
	"context"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/pendingdrops"
	"github.com/block/schemabot/pkg/storage"
)

// PendingDropsCleanupInterval is how often the pending drops cleaner runs a
// cleanup pass. Quarantined tables are retained for days, so a coarse interval
// is sufficient; the per-target advisory lock keeps concurrent instances from
// duplicating work.
const PendingDropsCleanupInterval = 6 * time.Hour

// StartPendingDropsCleaner starts the background loop that permanently drops
// expired quarantined tables from local-mode MySQL databases. It declines to
// start when the quarantine is off, when cleanup is disabled for this process,
// when the configured retention cannot be parsed, or when no local MySQL
// targets are configured (gRPC-mode targets are cleaned by the deployment that
// executes the schema changes). Each cause logs its own line.
func (s *Service) StartPendingDropsCleaner(ctx context.Context) {
	// The ways cleanup can be off mean different things to an operator and need
	// different responses, so they are reported separately rather than through
	// the single PendingDropsCleanupEnabled predicate. Each message states its
	// own consequence: quarantining without reaping is what makes tables
	// accumulate on a target, so the reason a process is not reaping is the
	// thing an operator needs to see.
	if !s.config.PendingDropsEnabled() {
		// Disabling the quarantine disables its cleaner with it, and this loop
		// is the only reaper, so tables quarantined while it was on are left
		// where they are. Saying "nothing to reap" would be false for a
		// deployment that turned the quarantine off with tables outstanding.
		s.logger.Info("pending drops cleaner not started because the quarantine is disabled; DROP TABLE executes as written, and any tables quarantined before it was disabled are not reaped")
		return
	}

	if !s.config.PendingDropsCleanupEnabled() {
		s.logger.Info("pending drops cleaner not started because cleanup is disabled for this process; another deployment must reap this deployment's targets or quarantined tables will accumulate on them")
		return
	}

	retention, err := s.config.PendingDropsRetention()
	if err != nil {
		// Validate() rejects invalid retention before the server starts, so
		// this guards direct embedders that skip config validation.
		s.logger.Error("pending drops cleaner not started because retention is invalid; quarantined tables will accumulate until the config is fixed", "error", err)
		return
	}

	local, routed := s.pendingDropsTargetCounts()
	if local == 0 {
		// A control plane that routes every target over gRPC executes nothing
		// itself, so it has nothing to reap and this is its expected state.
		// The counts separate that from a process that executes against
		// targets it never configured, which quarantines tables no cleaner
		// will reach.
		s.logger.Info("pending drops cleaner not started because no local MySQL database targets are configured; the deployment that executes against each target reaps it",
			"routed_mysql_targets", routed,
		)
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

// pendingDropsTargetCounts returns how many configured MySQL database targets
// this process executes against itself (local) and how many it routes to
// another deployment (routed). Only local targets are reapable from here; the
// routed count tells an operator whether a process with no local targets is a
// control plane whose deployments reap their own targets, or a process with no
// MySQL topology at all.
//
// A multi-deployment environment routes to each of its deployments, so it
// contributes one target per deployment: the count is of execution targets,
// which is what has to be reaped somewhere, not of environments. An
// environment configured with neither a local DSN nor any routing executes
// nowhere and is counted as neither; Validate() rejects that shape before
// startup, so it is only reachable for an embedder that skips validation, and
// counting it as routed would claim a deployment reaps it when none does.
func (s *Service) pendingDropsTargetCounts() (local, routed int) {
	for _, dbConfig := range s.config.Databases {
		if dbConfig.Type != storage.DatabaseTypeMySQL {
			continue
		}
		for _, envConfig := range dbConfig.Environments {
			switch {
			case envConfig.HasLocalDSN():
				local++
			case len(envConfig.Deployments) > 0:
				routed += len(envConfig.Deployments)
			case envConfig.Target != "" || envConfig.Deployment != "":
				routed++
			}
		}
	}
	return local, routed
}

// pendingDropsTargets resolves the local-mode MySQL databases the cleaner
// inspects. The mysql type filter is a deliberate capability boundary, not an
// incidental selection: the pending-drops quarantine is implemented with the
// Go MySQL driver and MySQL-specific discovery, advisory locking, and DROP
// statements, so targets of any other database family must never reach the
// cleaner. Databases without a local DSN are executed by a remote deployment,
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
				Locker:      namedlock.MySQL{},
			})
		}
	}
	return targets, unresolved
}
