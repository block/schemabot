// checks.go implements CheckStore for SchemaBot's stored check state.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// checkColumns lists all columns for SELECT queries.
const checkColumns = `id, repository, pull_request, head_sha,
	environment, database_type, database_name,
	check_run_id, apply_id, has_changes, status, conclusion,
	blocking_reason, error_message, created_at, updated_at`

const checkStatusInProgress = "in_progress"

// checkStore implements storage.CheckStore using MySQL.
type checkStore struct {
	db *sql.DB
}

// Upsert creates or updates stored check state.
func (s *checkStore) Upsert(ctx context.Context, check *storage.Check) error {
	// Convert CheckRunID=0 to NULL (0 is Go's zero value, not a valid check run ID)
	var checkRunID any
	if check.CheckRunID != 0 {
		checkRunID = check.CheckRunID
	}
	var applyID any
	if check.ApplyID != 0 {
		applyID = check.ApplyID
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO checks (
			repository, pull_request, head_sha,
			environment, database_type, database_name,
			check_run_id, apply_id, has_changes, status, conclusion, blocking_reason, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			head_sha = VALUES(head_sha),
			check_run_id = VALUES(check_run_id),
			apply_id = VALUES(apply_id),
			has_changes = VALUES(has_changes),
			status = VALUES(status),
			conclusion = VALUES(conclusion),
			blocking_reason = VALUES(blocking_reason),
			error_message = VALUES(error_message)
	`, check.Repository, check.PullRequest, check.HeadSHA,
		check.Environment, check.DatabaseType, check.DatabaseName,
		checkRunID, applyID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage)
	return err
}

// UpsertPlanResult stores plan-derived check state without overwriting
// in-progress apply state for the same PR/environment/database/head SHA.
func (s *checkStore) UpsertPlanResult(ctx context.Context, check *storage.Check) error {
	var checkRunID any
	if check.CheckRunID != 0 {
		checkRunID = check.CheckRunID
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO checks (
			repository, pull_request, head_sha,
			environment, database_type, database_name,
			check_run_id, apply_id, has_changes, status, conclusion, blocking_reason, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
	`, check.Repository, check.PullRequest, check.HeadSHA,
		check.Environment, check.DatabaseType, check.DatabaseName,
		checkRunID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage)
	// Fast path: no existing check state for this PR/environment/database, so the
	// insert is the complete write. Any non-duplicate error is a real storage
	// failure; duplicate key means the row exists and needs the guarded update
	// below.
	if err == nil {
		return nil
	}
	if !isDuplicateKeyError(err) {
		return err
	}

	// Preserve in-progress apply-owned state for the same PR commit. A plan
	// result for that same commit is stale relative to the apply that already
	// started.
	_, err = s.db.ExecContext(ctx, `
		UPDATE checks
		SET head_sha = ?,
		    check_run_id = ?,
		    apply_id = NULL,
		    has_changes = ?,
		    status = ?,
		    conclusion = ?,
		    blocking_reason = ?,
		    error_message = ?
		WHERE repository = ? AND pull_request = ?
		  AND environment = ? AND database_type = ? AND database_name = ?
		  AND NOT (status = ? AND head_sha = ? AND apply_id IS NOT NULL)
	`, check.HeadSHA, checkRunID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage,
		check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName,
		checkStatusInProgress, check.HeadSHA)
	return err
}

// CompleteForApply updates stored check state to a terminal state only if it
// still belongs to the apply being completed.
func (s *checkStore) CompleteForApply(ctx context.Context, check *storage.Check, apply *storage.Apply) (bool, error) {
	var checkRunID any
	if check.CheckRunID != 0 {
		checkRunID = check.CheckRunID
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE checks
		SET head_sha = ?,
		    check_run_id = ?,
		    apply_id = ?,
		    has_changes = ?,
		    status = ?,
		    conclusion = ?,
		    blocking_reason = ?,
		    error_message = ?
		WHERE repository = ? AND pull_request = ?
		  AND environment = ? AND database_type = ? AND database_name = ?
		  AND status = ?
		  AND apply_id = ?
		  AND NOT EXISTS (
		    SELECT 1
		    FROM applies newer
		    WHERE newer.repository = checks.repository
		      AND newer.pull_request = checks.pull_request
		      AND newer.environment = checks.environment
		      AND newer.database_type = checks.database_type
		      AND newer.database_name = checks.database_name
		      AND newer.id > ?
		  )
	`, check.HeadSHA, checkRunID, apply.ID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage,
		check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName,
		checkStatusInProgress, apply.ID, apply.ID)
	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// MarkActionRequiredForApply marks stored check state action_required after a
// rollback only if it still belongs to that rollback apply and no newer apply
// exists for the same PR/environment/database.
func (s *checkStore) MarkActionRequiredForApply(ctx context.Context, check *storage.Check, apply *storage.Apply) (bool, error) {
	var checkRunID any
	if check.CheckRunID != 0 {
		checkRunID = check.CheckRunID
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE checks
		SET head_sha = ?,
		    check_run_id = ?,
		    apply_id = NULL,
		    has_changes = ?,
		    status = ?,
		    conclusion = ?,
		    blocking_reason = ?,
		    error_message = ?
		WHERE repository = ? AND pull_request = ?
		  AND environment = ? AND database_type = ? AND database_name = ?
		  AND apply_id = ?
		  AND NOT EXISTS (
		    SELECT 1
		    FROM applies newer
		    WHERE newer.repository = checks.repository
		      AND newer.pull_request = checks.pull_request
		      AND newer.environment = checks.environment
		      AND newer.database_type = checks.database_type
		      AND newer.database_name = checks.database_name
		      AND newer.id > ?
		  )
	`, check.HeadSHA, checkRunID, check.HasChanges, check.Status, check.Conclusion, check.BlockingReason, check.ErrorMessage,
		check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName,
		apply.ID, apply.ID)
	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// Get returns a check by its unique key (PR + env + database), or nil if not found.
func (s *checkStore) Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*storage.Check, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+checkColumns+`
		FROM checks
		WHERE repository = ? AND pull_request = ?
		  AND environment = ? AND database_type = ? AND database_name = ?
	`, repo, pr, environment, dbType, database)

	return scanCheck(row)
}

// GetByCheckRunID returns a check by GitHub's check run ID, or nil if not found.
func (s *checkStore) GetByCheckRunID(ctx context.Context, checkRunID int64) (*storage.Check, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+checkColumns+`
		FROM checks
		WHERE check_run_id = ?
	`, checkRunID)

	return scanCheck(row)
}

// GetByPR returns all checks for a PR.
func (s *checkStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Check, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+checkColumns+`
		FROM checks
		WHERE repository = ? AND pull_request = ?
		ORDER BY environment, database_type, database_name
	`, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("query checks for %s#%d: %w", repo, pr, err)
	}
	defer utils.CloseAndLog(rows)

	return scanChecks(rows)
}

// GetByDatabase returns all checks for a database across all PRs.
// Used for cross-PR coordination (blocking other PRs when one is applying).
func (s *checkStore) GetByDatabase(ctx context.Context, repo, environment, dbType, database string) ([]*storage.Check, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+checkColumns+`
		FROM checks
		WHERE repository = ? AND environment = ?
		  AND database_type = ? AND database_name = ?
		ORDER BY pull_request
	`, repo, environment, dbType, database)
	if err != nil {
		return nil, fmt.Errorf("query checks for database %s: %w", database, err)
	}
	defer utils.CloseAndLog(rows)

	return scanChecks(rows)
}

// Delete removes stored check state by ID.
func (s *checkStore) Delete(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM checks WHERE id = ?`, id)
	if err != nil {
		return err
	}

	return checkRowsAffected(result, storage.ErrCheckNotFound)
}

// DeleteByPR removes all stored check state for a PR.
func (s *checkStore) DeleteByPR(ctx context.Context, repo string, pr int) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM checks WHERE repository = ? AND pull_request = ?`, repo, pr)
	return err
}

// scanCheck scans a single check row, returning nil if not found.
func scanCheck(row *sql.Row) (*storage.Check, error) {
	check, err := scanCheckInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return check, err
}

// scanChecks scans multiple check rows.
func scanChecks(rows *sql.Rows) ([]*storage.Check, error) {
	var checks []*storage.Check
	for rows.Next() {
		check, err := scanCheckInto(rows)
		if err != nil {
			return nil, err
		}
		checks = append(checks, check)
	}
	return checks, rows.Err()
}

// scanCheckInto scans check data from any scanner (Row or Rows).
func scanCheckInto(s scanner) (*storage.Check, error) {
	var check storage.Check
	var checkRunID, applyID sql.NullInt64
	var conclusion, blockingReason, errorMessage sql.NullString

	err := s.Scan(
		&check.ID, &check.Repository, &check.PullRequest, &check.HeadSHA,
		&check.Environment, &check.DatabaseType, &check.DatabaseName,
		&checkRunID, &applyID, &check.HasChanges, &check.Status, &conclusion,
		&blockingReason, &errorMessage, &check.CreatedAt, &check.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if checkRunID.Valid {
		check.CheckRunID = checkRunID.Int64
	}
	if applyID.Valid {
		check.ApplyID = applyID.Int64
	}
	if conclusion.Valid {
		check.Conclusion = conclusion.String
	}
	if blockingReason.Valid {
		check.BlockingReason = blockingReason.String
	}
	if errorMessage.Valid {
		check.ErrorMessage = errorMessage.String
	}

	return &check, nil
}
