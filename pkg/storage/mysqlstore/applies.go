// applies.go implements ApplyStore for tracking schema change executions.
// Each apply is a top-level container that holds one or more tasks.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// applyColumns lists all columns for SELECT queries.
const applyColumns = `id, apply_identifier, lock_id, plan_id, database_name, database_type,
	repository, pull_request, environment, deployment, caller, installation_id, external_id, engine,
	state, error_message, options,
	created_at, started_at, completed_at, updated_at`

// applyStore implements storage.ApplyStore using MySQL.
type applyStore struct {
	db *sql.DB
}

// Create stores a new apply and returns its ID.
func (s *applyStore) Create(ctx context.Context, apply *storage.Apply) (int64, error) {
	// Ensure options has valid JSON (empty object if nil)
	options := apply.Options
	if len(options) == 0 {
		options = []byte("{}")
	}

	result, err := s.db.ExecContext(ctx, `
		INSERT INTO applies (
			apply_identifier, lock_id, plan_id, database_name, database_type,
			repository, pull_request, environment, deployment, caller, installation_id, external_id, engine,
			state, error_message, options
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		apply.ApplyIdentifier, apply.LockID, apply.PlanID, apply.Database, apply.DatabaseType,
		apply.Repository, apply.PullRequest, apply.Environment, apply.Deployment, apply.Caller, apply.InstallationID, apply.ExternalID, apply.Engine,
		apply.State, apply.ErrorMessage, string(options),
	)
	if err != nil {
		if isDuplicateKeyError(err) {
			return 0, storage.ErrApplyIDExists
		}
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return id, nil
}

// Get returns an apply by ID, or nil if not found.
func (s *applyStore) Get(ctx context.Context, id int64) (*storage.Apply, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE id = ?
	`, id)

	return scanApply(row)
}

// GetByApplyIdentifier returns an apply by apply_identifier, or nil if not found.
func (s *applyStore) GetByApplyIdentifier(ctx context.Context, applyIdentifier string) (*storage.Apply, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE apply_identifier = ?
	`, applyIdentifier)

	return scanApply(row)
}

// GetByPlan returns the apply for a plan_id, or nil if not found.
func (s *applyStore) GetByPlan(ctx context.Context, planID int64) (*storage.Apply, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE plan_id = ?
	`, planID)

	return scanApply(row)
}

// GetByLock returns applies for a lock (0-2: staging + production).
func (s *applyStore) GetByLock(ctx context.Context, lockID int64) ([]*storage.Apply, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE lock_id = ?
		ORDER BY created_at DESC
	`, lockID)
	if err != nil {
		return nil, fmt.Errorf("query applies for lock %d: %w", lockID, err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplies(rows)
}

// Update updates apply state and fields.
func (s *applyStore) Update(ctx context.Context, apply *storage.Apply) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE applies
		SET state = ?, error_message = ?,
		    external_id = ?, started_at = ?, completed_at = ?, updated_at = NOW()
		WHERE id = ?
	`, apply.State, apply.ErrorMessage,
		apply.ExternalID, apply.StartedAt, apply.CompletedAt, apply.ID)
	return err
}

// GetInProgress returns all applies in non-terminal states.
// Note: For recovery, use ClaimForRecovery which handles locking and heartbeat staleness.
func (s *applyStore) GetInProgress(ctx context.Context) ([]*storage.Apply, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE state NOT IN ('completed', 'failed', 'stopped', 'reverted')
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	return scanApplies(rows)
}

// GetRecent returns the most recent applies across all databases, ordered by start time desc.
func (s *applyStore) GetRecent(ctx context.Context, limit int) ([]*storage.Apply, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		ORDER BY COALESCE(started_at, created_at) DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	return scanApplies(rows)
}

// ClaimForRecovery atomically claims an apply for recovery using heartbeat-based leasing.
// It uses FOR UPDATE SKIP LOCKED to prevent race conditions between workers.
// Only applies with stale heartbeats (updated_at > 1 minute ago) are claimed.
// Returns the claimed apply, or nil if no apply is available to claim.
//
// The caller MUST call Heartbeat periodically (every 10 seconds) to maintain the lease.
func (s *applyStore) ClaimForRecovery(ctx context.Context) (*storage.Apply, error) {
	// Start a transaction for the claim
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Find an apply that:
	// 1. Is in an active (non-terminal) state:
	//    - pending: created but not started (server crashed before Apply spawned worker)
	//    - running: actively copying rows
	//    - waiting_for_cutover: copy complete, waiting for cutover trigger
	//    - cutting_over: actively swapping tables
	//    - revert_window: completed but monitoring for revert (optional recovery)
	// 2. Has a stale heartbeat (updated_at > 1 minute ago) = crashed worker
	// 3. Use FOR UPDATE SKIP LOCKED to prevent race conditions
	row := tx.QueryRowContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE state IN (?, ?, ?, ?, ?, ?)
		  AND updated_at < NOW() - INTERVAL 1 MINUTE
		ORDER BY created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`,
		state.Apply.Pending,
		state.Apply.Running,
		state.Apply.WaitingForDeploy,
		state.Apply.WaitingForCutover,
		state.Apply.CuttingOver,
		state.Apply.RevertWindow,
	)

	apply, err := scanApplyInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // No apply to claim
	}
	if err != nil {
		return nil, err
	}

	// Claim by updating updated_at to now (this is our heartbeat)
	_, err = tx.ExecContext(ctx, `
		UPDATE applies SET updated_at = NOW() WHERE id = ?
	`, apply.ID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return apply, nil
}

// Heartbeat updates the apply's updated_at timestamp to maintain the lease.
// Should be called every 10 seconds while working on an apply.
// If not called for > 1 minute, another worker can claim the apply via ClaimForRecovery.
// Does not check RowsAffected — if the apply was deleted, the UPDATE matches 0 rows
// and returns nil.
func (s *applyStore) Heartbeat(ctx context.Context, applyID int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE applies SET updated_at = NOW() WHERE id = ?
	`, applyID)
	return err
}

// GetByDatabase returns applies for a specific database and optionally filtered by dbType and environment.
// If dbType or environment are empty strings, they are not used as filters.
func (s *applyStore) GetByDatabase(ctx context.Context, database, dbType, environment string) ([]*storage.Apply, error) {
	query := `
		SELECT ` + applyColumns + `
		FROM applies
		WHERE database_name = ?`
	args := []any{database}

	if dbType != "" {
		query += " AND database_type = ?"
		args = append(args, dbType)
	}
	if environment != "" {
		query += " AND environment = ?"
		args = append(args, environment)
	}
	query += " ORDER BY created_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query applies for database %s: %w", database, err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplies(rows)
}

// GetByPR returns all applies for a PR.
func (s *applyStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Apply, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE repository = ? AND pull_request = ?
		ORDER BY created_at DESC
	`, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("query applies for %s#%d: %w", repo, pr, err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplies(rows)
}

// Delete removes an apply by ID.
func (s *applyStore) Delete(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM applies WHERE id = ?
	`, id)
	if err != nil {
		return err
	}

	return checkRowsAffected(result, storage.ErrApplyNotFound)
}

// DeleteByPR removes all applies for a PR.
func (s *applyStore) DeleteByPR(ctx context.Context, repo string, pr int) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM applies WHERE repository = ? AND pull_request = ?
	`, repo, pr)
	return err
}

// scanApply scans a single apply row, returning nil if not found.
func scanApply(row *sql.Row) (*storage.Apply, error) {
	apply, err := scanApplyInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return apply, err
}

// scanApplies scans multiple apply rows.
func scanApplies(rows *sql.Rows) ([]*storage.Apply, error) {
	var applies []*storage.Apply
	for rows.Next() {
		apply, err := scanApplyInto(rows)
		if err != nil {
			return nil, err
		}
		applies = append(applies, apply)
	}
	return applies, rows.Err()
}

// scanApplyInto scans apply data from any scanner (Row or Rows).
func scanApplyInto(s scanner) (*storage.Apply, error) {
	var apply storage.Apply
	var startedAt, completedAt sql.NullTime
	var options []byte

	err := s.Scan(
		&apply.ID, &apply.ApplyIdentifier, &apply.LockID, &apply.PlanID,
		&apply.Database, &apply.DatabaseType,
		&apply.Repository, &apply.PullRequest, &apply.Environment, &apply.Deployment,
		&apply.Caller, &apply.InstallationID, &apply.ExternalID, &apply.Engine,
		&apply.State, &apply.ErrorMessage, &options,
		&apply.CreatedAt, &startedAt, &completedAt, &apply.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	apply.Options = options

	if startedAt.Valid {
		apply.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		apply.CompletedAt = &completedAt.Time
	}

	return &apply, nil
}
