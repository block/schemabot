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
const maxRecoveryAttempts = 10

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
			state, error_message, options, attempt
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		apply.ApplyIdentifier, apply.LockID, apply.PlanID, apply.Database, apply.DatabaseType,
		apply.Repository, apply.PullRequest, apply.Environment, apply.Deployment, apply.Caller, apply.InstallationID, apply.ExternalID, apply.Engine,
		apply.State, apply.ErrorMessage, string(options), apply.Attempt,
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
// Note: For recovery, use FindNextApply which handles locking and heartbeat staleness.
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

// FindNextApply atomically claims the next apply that needs attention.
// Returns the claimed apply, or nil if nothing needs work.
//
// Matches two types of applies:
//   - Stale active applies (crashed workers): heartbeat expired > 1 minute
//   - failed_retryable applies within attempt limit: transient failures to retry
//
// Skips applies where another active apply is running on the same database
// (database exclusion). Uses FOR UPDATE SKIP LOCKED to prevent race conditions
// between concurrent scheduler workers.
func (s *applyStore) FindNextApply(ctx context.Context) (*storage.Apply, error) {
	// Start a transaction for the claim
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Claim applies that need attention:
	// 1. Stale active applies (crashed workers): heartbeat expired > 1 minute
	// 2. failed_retryable applies within attempt limit: retry immediately on next poll
	//
	// Database exclusion: skip if another active apply is running on the same
	// database (prevents concurrent schema changes and metadata lock conflicts).
	//
	// Uses FOR UPDATE SKIP LOCKED to prevent race conditions between instances.
	row := tx.QueryRowContext(ctx, `
		SELECT `+applyColumns+`
		FROM applies a
		WHERE (
			(a.state IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AND a.updated_at < NOW() - INTERVAL 1 MINUTE)
			OR (a.state = ? AND a.attempt < ?)
		)
		AND NOT EXISTS (
			SELECT 1 FROM applies a2
			WHERE a2.database_name = a.database_name
			AND a2.database_type = a.database_type
			AND a2.state IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			AND a2.updated_at >= NOW() - INTERVAL 1 MINUTE
			AND a2.id != a.id
		)
		ORDER BY a.created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`,
		// Stale active states (all non-terminal states that have a heartbeat)
		state.Apply.Pending,
		state.Apply.Running,
		state.Apply.WaitingForDeploy,
		state.Apply.WaitingForCutover,
		state.Apply.CuttingOver,
		state.Apply.RevertWindow,
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest,
		// Retryable
		state.Apply.FailedRetryable,
		maxRecoveryAttempts,
		// Exclusion subquery: same active states with fresh heartbeat
		state.Apply.Pending,
		state.Apply.Running,
		state.Apply.WaitingForDeploy,
		state.Apply.WaitingForCutover,
		state.Apply.CuttingOver,
		state.Apply.RevertWindow,
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest,
	)

	apply, err := scanApplyInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // No apply to claim
	}
	if err != nil {
		return nil, err
	}

	// Claim by updating updated_at and incrementing attempt counter.
	// Retryable applies move back to pending while the worker dispatches them so
	// other workers see a fresh active lease instead of the old retryable row.
	if apply.State == state.Apply.FailedRetryable {
		var result sql.Result
		result, err = tx.ExecContext(ctx, `
			UPDATE applies
			SET state = ?, updated_at = NOW(), attempt = attempt + 1, completed_at = NULL
			WHERE id = ? AND state = ?
		`, state.Apply.Pending, apply.ID, state.Apply.FailedRetryable)
		if err != nil {
			return nil, err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if rows == 0 {
			return nil, nil
		}
	} else {
		_, err = tx.ExecContext(ctx, `
			UPDATE applies SET updated_at = NOW(), attempt = attempt + 1 WHERE id = ?
		`, apply.ID)
		if err != nil {
			return nil, err
		}
	}
	apply.Attempt++ // reflect the increment in the returned object

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return apply, nil
}

// Heartbeat updates the apply's updated_at timestamp to maintain the lease.
// Should be called every 10 seconds while working on an apply.
// If not called for > 1 minute, another worker can claim the apply via FindNextApply.
// Does not check RowsAffected — if the apply was deleted, the UPDATE matches 0 rows
// and returns nil.
func (s *applyStore) Heartbeat(ctx context.Context, applyID int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE applies SET updated_at = NOW() WHERE id = ?
	`, applyID)
	if err != nil {
		return fmt.Errorf("heartbeat apply %d: %w", applyID, err)
	}
	return nil
}

// ExpireRetryable transitions failed_retryable applies that have exhausted
// their retry budget to permanent failed. Returns the number of rows affected.
func (s *applyStore) ExpireRetryable(ctx context.Context) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `
		UPDATE tasks t
		JOIN applies a ON t.apply_id = a.id
		SET t.state = ?, t.completed_at = COALESCE(t.completed_at, NOW()), t.updated_at = NOW()
		WHERE a.state = ? AND a.attempt >= ? AND t.state = ?
	`, state.Task.Failed, state.Apply.FailedRetryable, maxRecoveryAttempts, state.Task.FailedRetryable)
	if err != nil {
		return 0, err
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE applies
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE state = ? AND attempt >= ?
	`, state.Apply.Failed, state.Apply.FailedRetryable, maxRecoveryAttempts)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return rowsAffected, nil
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
