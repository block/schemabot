package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/schemabot/pkg/storage"
)

const controlRequestColumns = `id, apply_id, operation, status,
	requested_by, error_message, metadata, completed_at, created_at, updated_at`

type controlRequestStore struct {
	db *sql.DB
}

func (s *controlRequestStore) RequestPending(ctx context.Context, req *storage.ApplyControlRequest) (*storage.ApplyControlRequest, bool, error) {
	var controlReq *storage.ApplyControlRequest
	var alreadyPending bool
	op := fmt.Sprintf("request control request for apply %d operation %s", req.ApplyID, req.Operation)
	err := withLockRetry(ctx, op, func() error {
		var attemptErr error
		controlReq, alreadyPending, attemptErr = s.requestPending(ctx, req)
		if attemptErr == nil || !isDuplicateKeyError(attemptErr) {
			return attemptErr
		}

		slog.DebugContext(ctx, "retrying control request after duplicate insert",
			"apply_id", req.ApplyID,
			"operation", req.Operation)

		// requestPending opens its transaction at READ COMMITTED. The unique key
		// on apply_id + operation is the durable guard when two first-time
		// callers both observe no row and race to insert. Re-read once so the
		// losing insert returns the winning row as "already requested"; if the
		// re-read also fails, return that storage error instead of hiding an
		// unexpected conflict.
		controlReq, alreadyPending, attemptErr = s.requestPending(ctx, req)
		if attemptErr != nil {
			return fmt.Errorf("retry control request after duplicate insert for apply %d operation %s: %w", req.ApplyID, req.Operation, attemptErr)
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return controlReq, alreadyPending, nil
}

func (s *controlRequestStore) requestPending(ctx context.Context, req *storage.ApplyControlRequest) (*storage.ApplyControlRequest, bool, error) {
	metadata := nullJSON(req.Metadata)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, false, fmt.Errorf("begin control request transaction for apply %d operation %s: %w", req.ApplyID, req.Operation, err)
	}
	defer rollbackTx(ctx, tx, "request pending")

	existing, err := s.getByApplyOperationForUpdate(ctx, tx, req.ApplyID, req.Operation)
	if err != nil {
		return nil, false, err
	}
	if existing != nil && existing.Status == storage.ControlRequestPending {
		if err := tx.Commit(); err != nil {
			return nil, false, fmt.Errorf("commit pending control request read for apply %d operation %s: %w", req.ApplyID, req.Operation, err)
		}
		return existing, true, nil
	}
	if existing != nil {
		_, err := tx.ExecContext(ctx, `
			UPDATE apply_control_requests
			SET status = ?, requested_by = ?, error_message = NULL, metadata = ?,
				completed_at = NULL, updated_at = NOW()
			WHERE id = ?
		`, storage.ControlRequestPending, req.RequestedBy, metadata, existing.ID)
		if err != nil {
			return nil, false, fmt.Errorf("reset control request for apply %d operation %s to pending: %w", req.ApplyID, req.Operation, err)
		}
		updated, err := s.getByIDForUpdate(ctx, tx, existing.ID)
		if err != nil {
			return nil, false, err
		}
		if err := tx.Commit(); err != nil {
			return nil, false, fmt.Errorf("commit reset control request for apply %d operation %s: %w", req.ApplyID, req.Operation, err)
		}
		return updated, false, nil
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO apply_control_requests (
			apply_id, operation, status, requested_by, error_message, metadata
		) VALUES (?, ?, ?, ?, ?, ?)
	`,
		req.ApplyID, req.Operation, storage.ControlRequestPending,
		req.RequestedBy, nullString(req.ErrorMessage), metadata,
	)
	if err != nil {
		return nil, false, fmt.Errorf("create control request for apply %d operation %s: %w", req.ApplyID, req.Operation, err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, false, fmt.Errorf("read control request id for apply %d operation %s: %w", req.ApplyID, req.Operation, err)
	}
	req.ID = id
	created, err := s.getByIDForUpdate(ctx, tx, id)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit control request for apply %d operation %s: %w", req.ApplyID, req.Operation, err)
	}
	return created, false, nil
}

func (s *controlRequestStore) GetPending(ctx context.Context, applyID int64, operation storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+controlRequestColumns+`
		FROM apply_control_requests
		WHERE apply_id = ? AND operation = ? AND status = ?
	`, applyID, operation, storage.ControlRequestPending)
	return scanControlRequest(row)
}

func (s *controlRequestStore) GetByOperation(ctx context.Context, applyID int64, operation storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+controlRequestColumns+`
		FROM apply_control_requests
		WHERE apply_id = ? AND operation = ?
	`, applyID, operation)
	req, err := scanControlRequest(row)
	if err != nil {
		return nil, fmt.Errorf("get control request for apply %d operation %s: %w", applyID, operation, err)
	}
	return req, nil
}

func (s *controlRequestStore) CompletePending(ctx context.Context, applyID int64, operation storage.ControlOperation) error {
	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return err
	}
	leaseJoin := ""
	leasePredicate := ""
	args := []any{storage.ControlRequestCompleted, applyID, operation, storage.ControlRequestPending}
	if hasLease {
		leaseJoin = " JOIN applies a ON a.id = cr.apply_id"
		leasePredicate = " AND a.lease_token = ?"
		args = append(args, lease.Token)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE apply_control_requests cr
		`+leaseJoin+`
		SET cr.status = ?, cr.completed_at = COALESCE(cr.completed_at, NOW()), cr.updated_at = NOW()
			WHERE cr.apply_id = ? AND cr.operation = ? AND cr.status = ?`+leasePredicate+`
		`, args...)
	if err != nil {
		return fmt.Errorf("complete pending control requests for apply %d operation %s: %w", applyID, operation, err)
	}
	if hasLease {
		if _, err := confirmLeaseOnZeroRows(ctx, s.db, result, lease, "completed control request", fmt.Sprintf("apply %d operation %s", applyID, operation)); err != nil {
			return err
		}
	}
	return nil
}

func (s *controlRequestStore) FailPending(ctx context.Context, applyID int64, operation storage.ControlOperation, errorMessage string) error {
	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return err
	}
	leaseJoin := ""
	leasePredicate := ""
	args := []any{storage.ControlRequestFailed, nullString(errorMessage), applyID, operation, storage.ControlRequestPending}
	if hasLease {
		leaseJoin = " JOIN applies a ON a.id = cr.apply_id"
		leasePredicate = " AND a.lease_token = ?"
		args = append(args, lease.Token)
	}
	result, err := s.db.ExecContext(ctx, `
			UPDATE apply_control_requests cr
			`+leaseJoin+`
			SET cr.status = ?, cr.error_message = ?, cr.completed_at = COALESCE(cr.completed_at, NOW()), cr.updated_at = NOW()
			WHERE cr.apply_id = ? AND cr.operation = ? AND cr.status = ?`+leasePredicate+`
		`, args...)
	if err != nil {
		return fmt.Errorf("fail pending control requests for apply %d operation %s: %w", applyID, operation, err)
	}
	if hasLease {
		if _, err := confirmLeaseOnZeroRows(ctx, s.db, result, lease, "failed control request", fmt.Sprintf("apply %d operation %s", applyID, operation)); err != nil {
			return err
		}
	}
	return nil
}

func (s *controlRequestStore) getByIDForUpdate(ctx context.Context, tx *sql.Tx, id int64) (*storage.ApplyControlRequest, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT `+controlRequestColumns+`
		FROM apply_control_requests
		WHERE id = ?
		FOR UPDATE
	`, id)
	req, err := scanControlRequest(row)
	if err != nil {
		return nil, fmt.Errorf("get control request %d: %w", id, err)
	}
	return req, nil
}

func (s *controlRequestStore) getByApplyOperationForUpdate(
	ctx context.Context,
	tx *sql.Tx,
	applyID int64,
	operation storage.ControlOperation,
) (*storage.ApplyControlRequest, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT `+controlRequestColumns+`
		FROM apply_control_requests
		WHERE apply_id = ? AND operation = ?
		FOR UPDATE
	`, applyID, operation)
	req, err := scanControlRequest(row)
	if err != nil {
		return nil, fmt.Errorf("get control request for apply %d operation %s: %w", applyID, operation, err)
	}
	return req, nil
}

func scanControlRequest(s scanner) (*storage.ApplyControlRequest, error) {
	var req storage.ApplyControlRequest
	var errorMessage sql.NullString
	var completedAt sql.NullTime

	err := s.Scan(
		&req.ID, &req.ApplyID, &req.Operation, &req.Status,
		&req.RequestedBy, &errorMessage, &req.Metadata, &completedAt,
		&req.CreatedAt, &req.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if errorMessage.Valid {
		req.ErrorMessage = errorMessage.String
	}
	if completedAt.Valid {
		req.CompletedAt = &completedAt.Time
	}
	return &req, nil
}
