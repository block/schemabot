package localscale

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/spirit/pkg/utils"
)

// dr is a shorthand alias for PlanetScale deploy request state constants.
var dr = state.DeployRequest

// vitessCancelledSignal is the transient value returned by deriveDeployState when Vitess
// migrations are cancelled. It is not a real PlanetScale deploy state — the processor
// routes it through InProgressCancel → CompleteCancel.
const vitessCancelledSignal = "cancelled"

// terminalDeployStates is the set of deploy request states where no further progress will occur.
// CompletePendingRevert is NOT terminal — the revert window is still open and the user
// can revert or skip-revert. A new deploy cannot proceed until the revert window closes.
var terminalDeployStates = map[string]bool{
	dr.Complete:            true,
	dr.CompleteError:       true,
	dr.CompleteRevert:      true,
	dr.CompleteCancel:      true,
	dr.CompleteRevertError: true,
	dr.Error:               true,
	dr.NoChanges:           true,
}

// runStateProcessor is a background goroutine that drives deploy request state
// transitions by polling Vitess migration statuses every 500ms. This replaces
// the previous approach of deriving state lazily on each GET request.
func (s *Server) runStateProcessor(ctx context.Context) {
	defer close(s.processorDone)
	ticker := time.NewTicker(processorTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.processActiveDeployRequests(ctx)
		}
	}
}

// activeDeployRow holds one row from the active deploy requests query.
type activeDeployRow struct {
	org                    string
	database               string
	number                 uint64
	deployState            string
	migrationContext       string
	revertMigrationContext string
	vschemaDataSQL         sql.NullString
	vschemaApplied         bool
	autoCutover            bool
	branch                 string
	revertExpiresAtStr     sql.NullString
	createdAtStr           string
}

// processActiveDeployRequests queries deploy requests in active (non-terminal)
// states and advances their state based on Vitess migration progress.
func (s *Server) processActiveDeployRequests(ctx context.Context) {
	rows, err := s.metadataDB.QueryContext(ctx,
		`SELECT org, database_name, number, deployment_state, migration_context, revert_migration_context,
		        vschema_data, vschema_applied, auto_cutover, branch, revert_expires_at, created_at
		 FROM localscale_deploy_requests
		 WHERE deployment_state IN ('submitting','queued','in_progress','pending_cutover','in_progress_cutover','in_progress_vschema','in_progress_cancel','in_progress_revert','in_progress_revert_vschema','complete_pending_revert')
		 AND deployed = TRUE`)
	if err != nil {
		s.logger.Warn("processor: query active deploy requests", "error", err)
		return
	}

	var activeRows []activeDeployRow
	for rows.Next() {
		var r activeDeployRow
		if err := rows.Scan(&r.org, &r.database, &r.number, &r.deployState, &r.migrationContext, &r.revertMigrationContext,
			&r.vschemaDataSQL, &r.vschemaApplied, &r.autoCutover, &r.branch, &r.revertExpiresAtStr, &r.createdAtStr); err != nil {
			s.logger.Warn("processor: scan deploy request row", "error", err)
			continue
		}
		activeRows = append(activeRows, r)
	}
	if err := rows.Err(); err != nil {
		s.logger.Warn("processor: iterate deploy request rows", "error", err)
		utils.CloseAndLog(rows)
		return
	}
	utils.CloseAndLog(rows)

	for _, r := range activeRows {
		backend, err := s.backendFor(r.org, r.database)
		if err != nil {
			s.logger.Warn("unknown backend for deploy request", "number", r.number, "org", r.org, "database", r.database, "error", err)
			continue
		}

		hasVSchema := hasVSchemaData(r.vschemaDataSQL)

		switch r.deployState {
		case dr.Submitting:
			// Crash recovery: if the background goroutine panicked or exited without
			// advancing the state, detect stale "submitting" rows and transition to error.
			if createdAt, err := time.Parse("2006-01-02 15:04:05", r.createdAtStr); err == nil {
				if time.Since(createdAt) > 2*time.Minute {
					s.logger.Error("deploy request stuck in submitting (possible crash)", "number", r.number)
					if err := s.updateDeployState(ctx, r.number, dr.Error); err != nil {
						s.logger.Error("processor: failed to transition stuck deploy to error", "number", r.number, "error", err)
					}
				}
			} else {
				s.logger.Warn("processor: parse created_at", "number", r.number, "raw", r.createdAtStr, "error", err)
			}

		case dr.PendingCutover:
			// Auto-cutover: issue COMPLETE for all migrations so Vitess completes the cutover.
			// Without this, Vitess may leave migrations in ready_to_complete indefinitely
			// (e.g. after previous cancelled migrations affect executor state in vtcombo).
			if r.autoCutover && r.migrationContext != "" {
				if err := s.alterVitessMigrations(ctx, backend, r.migrationContext, "COMPLETE"); err != nil {
					s.logger.Error("processor: auto-cutover COMPLETE failed", "number", r.number, "error", err)
					continue
				}
				if err := s.updateDeployState(ctx, r.number, dr.InProgressCutover); err != nil {
					s.logger.Error("processor: failed to transition to in_progress_cutover", "number", r.number, "error", err)
				}
			}

		case dr.Queued, dr.InProgress, dr.InProgressCutover:
			// DDL phase: derive state from Vitess migrations
			if r.migrationContext == "" {
				s.logger.Warn("processor: deploy request has no migration context", "number", r.number, "state", r.deployState)
				continue
			}
			migrations := s.getMigrationInfos(ctx, backend, r.migrationContext)
			if len(migrations) == 0 {
				s.logger.Debug("processor: no migrations visible yet", "number", r.number, "migration_context", r.migrationContext)
				continue
			}
			cutoverRequested := r.deployState == dr.InProgressCutover

			// Issue COMPLETE for migrations that are newly ready_to_complete.
			// With --in-order-completion, migrations become ready_to_complete
			// one at a time — each needs its own COMPLETE command issued.
			// alterVitessMigrations issues per-UUID commands, so already-completed
			// migrations are safely ignored by Vitess.
			if cutoverRequested {
				hasWaiting := false
				for _, m := range migrations {
					if m.readyToComplete && m.status != state.Vitess.Complete {
						hasWaiting = true
						break
					}
				}
				if hasWaiting {
					if err := s.alterVitessMigrations(ctx, backend, r.migrationContext, "COMPLETE"); err != nil {
						s.logger.Error("processor: COMPLETE migrations failed", "number", r.number, "error", err)
					}
				}
			}

			newState := deriveDeployState(migrations, cutoverRequested)

			// If the processor detects cancelled migrations while the deploy is
			// still in queued/in_progress, route through the cancel flow instead
			// of setting "cancelled" directly. This avoids a race where the
			// processor beats the cancel handler's DB update to in_progress_cancel.
			if newState == vitessCancelledSignal {
				newState = dr.InProgressCancel
			}

			// Log migration details on failure for debugging
			if newState == dr.CompleteError {
				for _, m := range migrations {
					s.logger.Warn("migration failed", "number", r.number, "status", m.status, "message", m.message)
				}
			}

			// If DDL complete and unapplied VSchema exists, transition to VSchema phase
			if newState == dr.CompletePendingRevert && hasVSchema && !r.vschemaApplied {
				newState = dr.InProgressVSchema
			}

			if newState != r.deployState {
				if err := s.updateDeployState(ctx, r.number, newState); err != nil {
					s.logger.Error("processor: failed to update deploy state", "number", r.number, "new_state", newState, "error", err)
				}
			}

		case dr.InProgressVSchema:
			// VSchema phase: apply pending VSchema
			if err := s.applyPendingVSchema(ctx, backend, r.number, r.vschemaDataSQL.String); err != nil {
				s.logger.Error("apply vschema failed", "number", r.number, "error", err)
				if err := s.updateDeployState(ctx, r.number, dr.CompleteError); err != nil {
					s.logger.Error("processor: failed to transition to complete_error", "number", r.number, "error", err)
				}
			} else {
				if err := s.updateDeployState(ctx, r.number, dr.CompletePendingRevert); err != nil {
					s.logger.Error("processor: failed to transition to complete_pending_revert", "number", r.number, "error", err)
				}
			}

		case dr.InProgressCancel:
			// Cancel phase: wait for all Vitess migrations to reach terminal state.
			if r.migrationContext == "" {
				// No migrations to cancel (e.g., cancelled during submitting)
				if err := s.execLog(ctx,
					"UPDATE localscale_deploy_requests SET cancelled = TRUE, deployment_state = ? WHERE number = ?",
					dr.CompleteCancel, r.number); err != nil {
					s.logger.Error("processor: update cancel state", "number", r.number, "error", err)
				} else {
					s.logger.Info("deploy state transition", "number", r.number, "new_state", dr.CompleteCancel)
				}
				continue
			}
			// Re-issue CANCEL on every tick to handle the race where migrations
			// become visible after the initial cancel request.
			if err := s.alterVitessMigrations(ctx, backend, r.migrationContext, "CANCEL"); err != nil {
				s.logger.Warn("processor: re-cancel migrations", "number", r.number, "error", err)
			}
			migrations := s.getMigrationInfos(ctx, backend, r.migrationContext)
			if len(migrations) == 0 {
				s.logger.Debug("processor: cancel waiting for migrations to appear", "number", r.number, "migration_context", r.migrationContext)
				continue
			}
			allTerminal := true
			for _, m := range migrations {
				switch m.status {
				case state.Vitess.Complete, state.Vitess.Failed, state.Vitess.Cancelled:
					// terminal
				default:
					allTerminal = false
				}
			}
			if allTerminal {
				if err := s.execLog(ctx,
					"UPDATE localscale_deploy_requests SET cancelled = TRUE, deployment_state = ? WHERE number = ?",
					dr.CompleteCancel, r.number); err != nil {
					s.logger.Error("processor: update cancel state", "number", r.number, "error", err)
				} else {
					s.logger.Info("deploy state transition", "number", r.number, "new_state", dr.CompleteCancel)
				}
			}

		case dr.InProgressRevert:
			// Revert phase: track reverse DDL progress by revert migration context
			newState := s.deriveRevertState(ctx, backend, r.revertMigrationContext)
			if newState != r.deployState {
				if err := s.updateDeployState(ctx, r.number, newState); err != nil {
					s.logger.Error("processor: failed to update revert state", "number", r.number, "new_state", newState, "error", err)
				}
			}

		case dr.InProgressRevertVSchema:
			// VSchema revert is synchronous in the HTTP handler — no processor action needed.
			// The handler transitions through this state and advances it within the same request.

		case dr.CompletePendingRevert:
			// Auto-expire the revert window after the configured duration
			if r.revertExpiresAtStr.Valid {
				if expiresAt, err := time.Parse("2006-01-02 15:04:05", r.revertExpiresAtStr.String); err == nil && time.Now().After(expiresAt) {
					s.logger.Info("revert window expired", "number", r.number)
					if err := s.updateDeployState(ctx, r.number, dr.Complete); err != nil {
						s.logger.Error("processor: failed to complete expired revert", "number", r.number, "error", err)
						continue
					}
					s.dropBranchDatabases(ctx, backend, r.branch)
				}
			}
		}
	}
}

func (s *Server) updateDeployState(ctx context.Context, number uint64, newState string) error {
	var err error
	if newState == dr.CompletePendingRevert {
		_, err = s.metadataDB.ExecContext(ctx,
			`UPDATE localscale_deploy_requests
			 SET deployment_state = ?, revert_expires_at = DATE_ADD(NOW(), INTERVAL ? SECOND)
			 WHERE number = ?`,
			newState, int(s.revertWindowDuration.Seconds()), number)
	} else {
		_, err = s.metadataDB.ExecContext(ctx,
			"UPDATE localscale_deploy_requests SET deployment_state = ? WHERE number = ?",
			newState, number)
	}
	if err != nil {
		s.logger.Error("update deploy state failed", "number", number, "new_state", newState, "error", err)
		return fmt.Errorf("update deploy state to %s for %d: %w", newState, number, err)
	}
	s.logger.Info("deploy state transition", "number", number, "new_state", newState)
	return nil
}

// migrationInfo holds the key fields from a Vitess migration needed for state derivation.
type migrationInfo struct {
	status          string
	readyToComplete bool
	ddlAction       string // "create", "alter", "drop"
	message         string
}

// deriveDeployState maps a set of Vitess migration statuses to a single PlanetScale
// deploy request state. This provides realistic deploy request status tracking that
// reflects what Vitess is actually doing.
//
// PlanetScale deploy request states:
// https://planetscale.com/docs/api/reference/create_deploy_request#response-deployment-state
//
// Vitess migration statuses (from vitess.io/vitess/go/vt/schema):
//
//	requested        → Migration submitted to tablet, not yet picked up by scheduler
//	queued           → Picked up by scheduler, waiting for execution slot
//	ready            → Ready to execute but waiting (e.g., for --in-order-completion)
//	running          → Actively copying rows (or cutting over if ready_to_complete=true)
//	ready_to_complete→ Row copy done, waiting for cutover (explicit or auto)
//	complete         → Migration finished successfully
//	failed           → Migration failed
//	cancelled        → Migration was cancelled
//
// The readyToComplete field is the authoritative signal for whether a migration is
// waiting for cutover — it is true even when migration_status is still "running"
// (brief race) or "queued" (immediate operations like CREATE/DROP TABLE).
// Instant DDL (ALGORITHM=INSTANT) skips ready_to_complete entirely and goes
// straight to complete. Terminal states (complete, failed, cancelled) take
// precedence over readyToComplete, which can remain true after cancel/fail.
//
// cutoverRequested tracks whether apply-deploy (cutover) has been triggered, enabling
// the distinction between pending_cutover and in_progress_cutover states.
func deriveDeployState(migrations []migrationInfo, cutoverRequested bool) string {
	var failed, cancelled, complete, running, waitingCutover, queued int

	for _, m := range migrations {
		switch m.status {
		case state.Vitess.Complete:
			complete++
		case state.Vitess.Failed:
			failed++
		case state.Vitess.Cancelled:
			cancelled++
		case state.Vitess.Running:
			if m.readyToComplete {
				waitingCutover++
			} else {
				running++
			}
		case state.Vitess.ReadyToComplete:
			waitingCutover++
		case state.Vitess.Queued, state.Vitess.Requested, state.Vitess.Ready:
			// Early states. Immediate operations (CREATE/DROP TABLE) set
			// ready_to_complete=true before transitioning out of queued.
			// Instant DDL (ALGORITHM=INSTANT ALTER) skips this entirely
			// and goes straight to complete.
			if m.readyToComplete {
				waitingCutover++
			} else {
				queued++
			}
		default:
			// Unknown Vitess status — count as queued so the processor keeps
			// polling until Vitess resolves it to a known status.
			slog.Warn("unknown vitess migration status, treating as pending", "status", m.status, "ddl_action", m.ddlAction)
			queued++
		}
	}

	total := len(migrations)

	// Map aggregate Vitess migration state to PlanetScale deploy request state.
	// Priority matches PlanetScale's real behavior.
	switch {
	case failed > 0:
		return dr.CompleteError
	case cancelled > 0:
		// Note: "cancelled" is a transient signal from Vitess, not a real PlanetScale
		// deploy state. The processor routes this through InProgressCancel → CompleteCancel.
		return vitessCancelledSignal
	case complete == total:
		return dr.CompletePendingRevert
	case cutoverRequested && complete > 0:
		return dr.InProgressCutover
	case cutoverRequested && waitingCutover > 0 && running == 0 && queued == 0:
		return dr.InProgressCutover
	case cutoverRequested && running > 0:
		return dr.InProgressCutover
	case waitingCutover > 0 && running == 0 && queued == 0:
		return dr.PendingCutover
	case running > 0:
		return dr.InProgress
	case queued > 0 && complete == 0 && waitingCutover == 0:
		return dr.Queued
	default:
		return dr.InProgress
	}
}
