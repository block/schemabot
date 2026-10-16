package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// BranchReconcileInterval is how often the branch reconcile pass runs. A
// branch it can reclaim has already outlived the grace period, so a slow
// cadence finds everything a fast one would, a little later, while listing
// each database's branches far less often.
const BranchReconcileInterval = 1 * time.Hour

// branchReconcileSweep identifies the branch reconcile pass to the outcome
// handling it shares with the reaper's sweeps.
var branchReconcileSweep = reapSweep{
	busy:          storage.ErrBranchReconcileBusy,
	subject:       "orphaned engine branches",
	failureReason: "branch_reconcile_error",
}

// branchReconciler is the tern client capability the pass drives. The local
// client for a PlanetScale database has it; a remote client does not, because
// the deployment it routes to owns that database's branches.
type branchReconciler interface {
	ReconcileOrphanedBranches(ctx context.Context, opts tern.BranchReconcileOptions) (tern.BranchReconcileResult, error)
}

// recentTargetsBranchReconciler is the capability of a data plane's default
// client that resolves targets per request (tern.TargetRouter). It has no
// static databases to visit, so it reconciles the PlanetScale targets its own
// applies recently ran against.
type recentTargetsBranchReconciler interface {
	ReconcileRecentTargetBranches(ctx context.Context, opts tern.BranchReconcileOptions) (tern.RecentTargetsReconcileResult, error)
}

// branchReconcileTarget is one PlanetScale database environment this process
// executes against itself.
type branchReconcileTarget struct {
	database    string
	environment string
}

// branchReconcilePass returns the pass to run on the reaper's cadence, or
// false when the pass should not start. Each reason it declines is logged
// separately, because each asks something different of the operator.
//
// The pass deletes branches that belong to applies no driver is coming back
// for, so it runs with the operator. Each pod runs it on its own ticker, and
// the election keeps two pods from running it at once. What makes each delete
// safe is not the election but the apply target hold the reconcile takes
// around it (OW-10).
//
// The targets are read on every pass rather than here, because a local server
// can register databases while it runs; a process with none yet still starts
// the pass so it picks them up. A data plane whose default client resolves
// targets per request also reconciles the targets its applies recently ran
// against, through that client.
func (s *Service) branchReconcilePass() (func(context.Context), bool) {
	if !s.config.BranchReconcile.Enabled {
		s.logger.Debug("branch reconcile pass not started because it is disabled; orphaned PlanetScale branches are left until deleted by hand")
		return nil, false
	}
	grace, err := s.config.BranchReconcileGrace()
	if err != nil {
		// Validate() rejects an invalid grace before the server starts, so
		// this guards direct embedders that skip config validation.
		s.logger.Error("branch reconcile pass not started because its grace period is invalid; orphaned PlanetScale branches are left until the config is fixed", "error", err)
		return nil, false
	}
	deleteOrphans := s.config.BranchReconcile.Delete
	s.logger.Info("branch reconcile pass enabled",
		"delete", deleteOrphans, "grace", grace, "interval", BranchReconcileInterval,
		"local_planetscale_targets", len(s.branchReconcileTargets()),
		"reconciles_recently_applied_targets", s.recentTargetsBranchReconciler() != nil)
	return func(ctx context.Context) {
		s.runBranchReconcilePass(ctx, grace, deleteOrphans)
	}, true
}

// branchReconcileTargets returns the PlanetScale database environments this
// process executes against itself, in a stable order. One with no local DSN is
// routed to another deployment, which owns its branches.
func (s *Service) branchReconcileTargets() []branchReconcileTarget {
	var targets []branchReconcileTarget
	for dbName, dbConfig := range s.config.DatabaseConfigs() {
		if dbConfig.Type != storage.DatabaseTypeVitess {
			continue
		}
		for envName, envConfig := range dbConfig.Environments {
			if !envConfig.HasLocalDSN() {
				continue
			}
			targets = append(targets, branchReconcileTarget{database: dbName, environment: envName})
		}
	}
	slices.SortFunc(targets, func(a, b branchReconcileTarget) int {
		return cmp.Or(strings.Compare(a.database, b.database), strings.Compare(a.environment, b.environment))
	})
	return targets
}

// runBranchReconcilePass reconciles every current target on the elected
// instance: each local PlanetScale environment, then, on a data plane that
// resolves targets per request, each target its applies recently ran against.
// The targets are independent databases, so one that fails is logged and the
// pass moves on to the next; the failures are joined into the pass's outcome.
func (s *Service) runBranchReconcilePass(ctx context.Context, grace time.Duration, deleteOrphans bool) {
	targets := s.branchReconcileTargets()
	resolved := s.recentTargetsBranchReconciler()
	if len(targets) == 0 && resolved == nil {
		s.logger.Debug("branch reconcile pass skipped: no local PlanetScale database targets; the deployment that executes against each database reconciles its branches")
		return
	}
	deleted := 0
	err := s.storage.Applies().WithBranchReconcileElection(ctx, func(ctx context.Context) error {
		var errs []error
		for _, target := range targets {
			n, err := s.reconcileTargetBranches(ctx, target, grace, deleteOrphans)
			deleted += n
			if err != nil {
				errs = append(errs, err)
			}
		}
		if resolved != nil {
			result, err := resolved.ReconcileRecentTargetBranches(ctx, tern.BranchReconcileOptions{
				Grace:  grace,
				Delete: deleteOrphans,
				Now:    s.clock.Now(),
			})
			deleted += result.Deleted
			s.logger.Info("branch reconcile visited the PlanetScale targets this data plane recently applied to",
				"targets", result.Targets, "unreachable", result.Unreachable, "deleted", result.Deleted, "delete", deleteOrphans)
			if err != nil {
				errs = append(errs, fmt.Errorf("reconcile branches of recently applied targets: %w", err))
			}
		}
		if len(errs) == 0 {
			s.logger.Info("branch reconcile pass complete", "deleted", deleted, "delete", deleteOrphans)
		}
		return errors.Join(errs...)
	})
	s.recordSweepOutcome(ctx, branchReconcileSweep, deleted, err)
}

// recentTargetsBranchReconciler returns the default tern client when it
// reconciles recently applied targets, and nil otherwise.
func (s *Service) recentTargetsBranchReconciler() recentTargetsBranchReconciler {
	s.ternMu.Lock()
	defer s.ternMu.Unlock()
	reconciler, _ := s.defaultTernClient.(recentTargetsBranchReconciler)
	return reconciler
}

// reconcileTargetBranches runs the reconcile on one target and returns how
// many branches it deleted.
func (s *Service) reconcileTargetBranches(ctx context.Context, target branchReconcileTarget, grace time.Duration, deleteOrphans bool) (int, error) {
	logger := s.logger.With("database", target.database, "environment", target.environment, "database_type", storage.DatabaseTypeVitess)
	client, err := s.TernClient(target.database, target.environment)
	if err != nil {
		logger.Error("branch reconcile skipped this database because its client could not be created; it is retried on the next pass", "error", err)
		return 0, fmt.Errorf("reconcile branches of %s/%s: %w", target.database, target.environment, err)
	}
	reconciler, ok := client.(branchReconciler)
	if !ok {
		logger.Error("branch reconcile skipped this database because its client cannot reconcile branches", "client_type", fmt.Sprintf("%T", client))
		return 0, fmt.Errorf("reconcile branches of %s/%s: client %T cannot reconcile branches", target.database, target.environment, client)
	}
	result, err := reconciler.ReconcileOrphanedBranches(ctx, tern.BranchReconcileOptions{
		Grace:       grace,
		Delete:      deleteOrphans,
		Now:         s.clock.Now(),
		Environment: target.environment,
	})
	if err != nil {
		logger.Error("branch reconcile on this database was incomplete; branches it could not judge or delete are kept and retried on the next pass",
			"planetscale_database", result.Database, "deleted", result.Deleted, "error", err)
		return result.Deleted, fmt.Errorf("reconcile branches of %s/%s: %w", target.database, target.environment, err)
	}
	return result.Deleted, nil
}
