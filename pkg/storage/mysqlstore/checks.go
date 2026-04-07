// checks.go implements CheckStore for GitHub status check tracking.
// One check per (PR, environment, database) combination.
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
	check_run_id, has_changes, status, conclusion,
	error_message, created_at, updated_at`

// checkStore implements storage.CheckStore using MySQL.
type checkStore struct {
	db *sql.DB
}

// Upsert creates or updates a check record.
func (s *checkStore) Upsert(ctx context.Context, check *storage.Check) error {
	// Convert CheckRunID=0 to NULL (0 is Go's zero value, not a valid check run ID)
	var checkRunID any
	if check.CheckRunID != 0 {
		checkRunID = check.CheckRunID
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO checks (
			repository, pull_request, head_sha,
			environment, database_type, database_name,
			check_run_id, has_changes, status, conclusion, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			head_sha = VALUES(head_sha),
			check_run_id = VALUES(check_run_id),
			has_changes = VALUES(has_changes),
			status = VALUES(status),
			conclusion = VALUES(conclusion),
			error_message = VALUES(error_message)
	`, check.Repository, check.PullRequest, check.HeadSHA,
		check.Environment, check.DatabaseType, check.DatabaseName,
		checkRunID, check.HasChanges, check.Status, check.Conclusion, check.ErrorMessage)
	return err
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

// Delete removes a check record by ID.
func (s *checkStore) Delete(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM checks WHERE id = ?`, id)
	if err != nil {
		return err
	}

	return checkRowsAffected(result, storage.ErrCheckNotFound)
}

// DeleteByPR removes all check records for a PR.
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
	var checkRunID sql.NullInt64
	var conclusion, errorMessage sql.NullString

	err := s.Scan(
		&check.ID, &check.Repository, &check.PullRequest, &check.HeadSHA,
		&check.Environment, &check.DatabaseType, &check.DatabaseName,
		&checkRunID, &check.HasChanges, &check.Status, &conclusion,
		&errorMessage, &check.CreatedAt, &check.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if checkRunID.Valid {
		check.CheckRunID = checkRunID.Int64
	}
	if conclusion.Valid {
		check.Conclusion = conclusion.String
	}
	if errorMessage.Valid {
		check.ErrorMessage = errorMessage.String
	}

	return &check, nil
}
