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
	state, error_message, options, attempt,
	created_at, started_at, completed_at, updated_at`

// maxRecoveryAttempts is the maximum number of times a failed_retryable apply
// will be re-dispatched before transitioning to permanent failed.
const maxRecoveryAttempts = 5

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
		SET state = ?, error_message = ?, attempt = ?,
		    external_id = ?, started_at = ?, completed_at = ?, updated_at = NOW()
		WHERE id = ?
	`, apply.State, apply.ErrorMessage, apply.Attempt,
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

	// Only claim failed_retryable applies within the attempt limit.
	// Stale active applies (crashed workers) are handled separately by
	// the existing heartbeat/conflict detection in Apply().
	// Uses FOR UPDATE SKIP LOCKED to prevent race conditions between workers.
	row := tx.QueryRowContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies
		WHERE state = ? AND attempt < ?
		ORDER BY created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`,
		state.Apply.FailedRetryable,
		maxRecoveryAttempts,
	)

	apply, err := scanApplyInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // No apply to claim
	}
	if err != nil {
		return nil, err
	}

	// Claim by updating updated_at and incrementing attempt counter
	_, err = tx.ExecContext(ctx, `
		UPDATE applies SET updated_at = NOW(), attempt = attempt + 1 WHERE id = ?
	`, apply.ID)
	apply.Attempt++ // reflect the increment in the returned object
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

// ExpireRetryable transitions failed_retryable applies that have exhausted
// their retry budget to permanent failed. Returns the number of rows affected.
func (s *applyStore) ExpireRetryable(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE applies
		SET state = ?, completed_at = NOW(), updated_at = NOW()
		WHERE state = ? AND attempt >= ?
	`, state.Apply.Failed, state.Apply.FailedRetryable, maxRecoveryAttempts)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
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
		&apply.State, &apply.ErrorMessage, &options, &apply.Attempt,
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
