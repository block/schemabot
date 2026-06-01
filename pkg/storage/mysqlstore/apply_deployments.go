// apply_deployments.go implements ApplyDeploymentStore for per-(apply,
// deployment) child rows under a multi-deployment apply.
//
// Introduced in MD-2. This file ships the storage primitive only; no other
// code path in SchemaBot reads or writes these rows yet. MD-5 will start
// dual-writing them at apply-create time, MD-6 will move scheduler claim +
// locking down to this granularity.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// applyDeploymentColumns lists all columns for SELECT queries.
const applyDeploymentColumns = `id, apply_id, deployment, target, state, error_message,
	started_at, completed_at, created_at, updated_at`

// mysqlErrDupEntry is MySQL's error number for a duplicate-key violation.
// Used to translate unique-index conflicts into typed storage errors.
const mysqlErrDupEntry = 1062

// applyDeploymentStore implements storage.ApplyDeploymentStore using MySQL.
type applyDeploymentStore struct {
	db *sql.DB
}

// Insert stores a new apply_deployments row and returns its ID.
// Translates a unique-key conflict on (apply_id, deployment) into
// storage.ErrApplyDeploymentExists so callers can branch cleanly.
func (s *applyDeploymentStore) Insert(ctx context.Context, ad *storage.ApplyDeployment) (int64, error) {
	stateVal := ad.State
	if stateVal == "" {
		stateVal = state.ApplyDeployment.Pending
	}

	result, err := s.db.ExecContext(ctx, `
		INSERT INTO apply_deployments (
			apply_id, deployment, target, state, error_message,
			started_at, completed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
		ad.ApplyID, ad.Deployment, ad.Target, stateVal, nullString(ad.ErrorMessage),
		ad.StartedAt, ad.CompletedAt,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDupEntry {
			return 0, storage.ErrApplyDeploymentExists
		}
		return 0, fmt.Errorf("insert apply_deployments (apply=%d, deployment=%s): %w", ad.ApplyID, ad.Deployment, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last insert id: %w", err)
	}
	ad.ID = id
	ad.State = stateVal
	return id, nil
}

// Get returns a child row by ID, or nil if not found.
func (s *applyDeploymentStore) Get(ctx context.Context, id int64) (*storage.ApplyDeployment, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyDeploymentColumns+`
		FROM apply_deployments
		WHERE id = ?
	`, id)
	return scanApplyDeployment(row)
}

// GetByApplyDeployment returns the child row for (apply_id, deployment), or nil if not found.
func (s *applyDeploymentStore) GetByApplyDeployment(ctx context.Context, applyID int64, deployment string) (*storage.ApplyDeployment, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+applyDeploymentColumns+`
		FROM apply_deployments
		WHERE apply_id = ? AND deployment = ?
	`, applyID, deployment)
	return scanApplyDeployment(row)
}

// ListByApply returns all child rows for an apply, ordered by id ascending.
func (s *applyDeploymentStore) ListByApply(ctx context.Context, applyID int64) ([]*storage.ApplyDeployment, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyDeploymentColumns+`
		FROM apply_deployments
		WHERE apply_id = ?
		ORDER BY id
	`, applyID)
	if err != nil {
		return nil, fmt.Errorf("query apply_deployments for apply %d: %w", applyID, err)
	}
	defer utils.CloseAndLog(rows)

	return scanApplyDeployments(rows)
}

// UpdateState transitions a child row to the given state.
// Returns storage.ErrApplyDeploymentNotFound if no row matches the ID.
func (s *applyDeploymentStore) UpdateState(ctx context.Context, id int64, newState string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_deployments SET state = ? WHERE id = ?
	`, newState, id)
	if err != nil {
		return fmt.Errorf("update apply_deployments state (id=%d): %w", id, err)
	}
	return checkRowsAffected(result, storage.ErrApplyDeploymentNotFound)
}

// MarkStarted sets state=in_progress and stamps started_at=NOW().
// Returns storage.ErrApplyDeploymentNotFound if no row matches the ID.
func (s *applyDeploymentStore) MarkStarted(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_deployments
		SET state = ?, started_at = COALESCE(started_at, NOW())
		WHERE id = ?
	`, state.ApplyDeployment.InProgress, id)
	if err != nil {
		return fmt.Errorf("mark apply_deployment started (id=%d): %w", id, err)
	}
	return checkRowsAffected(result, storage.ErrApplyDeploymentNotFound)
}

// MarkCompleted sets state=completed and stamps completed_at=NOW().
// Returns storage.ErrApplyDeploymentNotFound if no row matches the ID.
func (s *applyDeploymentStore) MarkCompleted(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_deployments
		SET state = ?, completed_at = NOW()
		WHERE id = ?
	`, state.ApplyDeployment.Completed, id)
	if err != nil {
		return fmt.Errorf("mark apply_deployment completed (id=%d): %w", id, err)
	}
	return checkRowsAffected(result, storage.ErrApplyDeploymentNotFound)
}

// MarkFailed sets state=failed, error_message, and stamps completed_at=NOW().
// Returns storage.ErrApplyDeploymentNotFound if no row matches the ID.
func (s *applyDeploymentStore) MarkFailed(ctx context.Context, id int64, errMsg string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_deployments
		SET state = ?, error_message = ?, completed_at = NOW()
		WHERE id = ?
	`, state.ApplyDeployment.Failed, nullString(errMsg), id)
	if err != nil {
		return fmt.Errorf("mark apply_deployment failed (id=%d): %w", id, err)
	}
	return checkRowsAffected(result, storage.ErrApplyDeploymentNotFound)
}

// DeleteByApply removes all child rows for an apply.
func (s *applyDeploymentStore) DeleteByApply(ctx context.Context, applyID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM apply_deployments WHERE apply_id = ?`, applyID)
	if err != nil {
		return fmt.Errorf("delete apply_deployments for apply %d: %w", applyID, err)
	}
	return nil
}

// scanApplyDeployment scans a single apply_deployments row, returning nil if not found.
func scanApplyDeployment(row *sql.Row) (*storage.ApplyDeployment, error) {
	ad, err := scanApplyDeploymentInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return ad, err
}

// scanApplyDeployments scans multiple apply_deployments rows.
func scanApplyDeployments(rows *sql.Rows) ([]*storage.ApplyDeployment, error) {
	var out []*storage.ApplyDeployment
	for rows.Next() {
		ad, err := scanApplyDeploymentInto(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ad)
	}
	return out, rows.Err()
}

// scanApplyDeploymentInto scans apply_deployments data from any scanner.
func scanApplyDeploymentInto(s scanner) (*storage.ApplyDeployment, error) {
	var ad storage.ApplyDeployment
	var errMsg sql.NullString
	var startedAt, completedAt sql.NullTime

	if err := s.Scan(
		&ad.ID, &ad.ApplyID, &ad.Deployment, &ad.Target, &ad.State, &errMsg,
		&startedAt, &completedAt, &ad.CreatedAt, &ad.UpdatedAt,
	); err != nil {
		return nil, err
	}

	if errMsg.Valid {
		ad.ErrorMessage = errMsg.String
	}
	if startedAt.Valid {
		t := startedAt.Time
		ad.StartedAt = &t
	}
	if completedAt.Valid {
		t := completedAt.Time
		ad.CompletedAt = &t
	}
	return &ad, nil
}
