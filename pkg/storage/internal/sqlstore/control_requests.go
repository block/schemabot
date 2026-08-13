package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

const controlRequestColumns = `id, apply_id, operation, status,
	requested_by, error_message, metadata, completed_at, created_at, updated_at`

type controlRequestStore struct {
	db         *rebindDB
	identity   identityInserter
	classifier ErrorClassifier
	dialect    Dialect
}

func (s *controlRequestStore) RequestPending(ctx context.Context, req *storage.ApplyControlRequest) (*storage.ApplyControlRequest, bool, error) {
	var controlReq *storage.ApplyControlRequest
	var alreadyPending bool
	op := fmt.Sprintf("request control request for apply %d operation %s", req.ApplyID, req.Operation)
	err := withLockRetry(ctx, s.classifier, op, func() error {
		var attemptErr error
		controlReq, alreadyPending, attemptErr = s.requestPending(ctx, req)
		if attemptErr == nil || !s.classifier.IsDuplicateKey(attemptErr) {
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

	id, err := s.identity.InsertID(ctx, tx, `
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
	if !hasLease {
		_, err := s.db.ExecContext(ctx, `
			UPDATE apply_control_requests
			SET status = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
			WHERE apply_id = ? AND operation = ? AND status = ?
		`, storage.ControlRequestCompleted, applyID, operation, storage.ControlRequestPending)
		if err != nil {
			return fmt.Errorf("complete pending control requests for apply %d operation %s: %w", applyID, operation, err)
		}
		return nil
	}
	query := s.dialect.JoinedUpdate(
		"apply_control_requests", "cr", "applies", "a", "a.id = cr.apply_id",
		[]JoinedUpdateAssignment{
			{Column: "status", Expr: "?"},
			{Column: "completed_at", Expr: "COALESCE(cr.completed_at, NOW())"},
			{Column: "updated_at", Expr: "NOW()"},
		},
		"cr.apply_id = ? AND cr.operation = ? AND cr.status = ? AND "+s.dialect.LeaseTokenFence("applies", "a", "id", "lease_token"),
	)
	result, err := s.db.ExecContext(ctx, query, storage.ControlRequestCompleted, applyID, operation, storage.ControlRequestPending, lease.Token)
	if err != nil {
		return fmt.Errorf("complete pending control requests for apply %d operation %s: %w", applyID, operation, err)
	}
	if _, err := confirmLeaseOnZeroRows(ctx, s.db, result, lease, "completed control request", fmt.Sprintf("apply %d operation %s", applyID, operation)); err != nil {
		return err
	}
	return nil
}

func (s *controlRequestStore) FailPending(ctx context.Context, applyID int64, operation storage.ControlOperation, errorMessage string) error {
	lease, hasLease, err := applyLeaseFromContext(ctx, applyID)
	if err != nil {
		return err
	}
	if !hasLease {
		_, err := s.db.ExecContext(ctx, `
			UPDATE apply_control_requests
			SET status = ?, error_message = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
			WHERE apply_id = ? AND operation = ? AND status = ?
		`, storage.ControlRequestFailed, nullString(errorMessage), applyID, operation, storage.ControlRequestPending)
		if err != nil {
			return fmt.Errorf("fail pending control requests for apply %d operation %s: %w", applyID, operation, err)
		}
		return nil
	}
	query := s.dialect.JoinedUpdate(
		"apply_control_requests", "cr", "applies", "a", "a.id = cr.apply_id",
		[]JoinedUpdateAssignment{
			{Column: "status", Expr: "?"},
			{Column: "error_message", Expr: "?"},
			{Column: "completed_at", Expr: "COALESCE(cr.completed_at, NOW())"},
			{Column: "updated_at", Expr: "NOW()"},
		},
		"cr.apply_id = ? AND cr.operation = ? AND cr.status = ? AND "+s.dialect.LeaseTokenFence("applies", "a", "id", "lease_token"),
	)
	result, err := s.db.ExecContext(ctx, query, storage.ControlRequestFailed, nullString(errorMessage), applyID, operation, storage.ControlRequestPending, lease.Token)
	if err != nil {
		return fmt.Errorf("fail pending control requests for apply %d operation %s: %w", applyID, operation, err)
	}
	if _, err := confirmLeaseOnZeroRows(ctx, s.db, result, lease, "failed control request", fmt.Sprintf("apply %d operation %s", applyID, operation)); err != nil {
		return err
	}
	return nil
}

func (s *controlRequestStore) ListSettled(ctx context.Context, applyID int64) ([]*storage.ApplyControlRequest, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+controlRequestColumns+`
		FROM apply_control_requests
		WHERE apply_id = ? AND status IN (?, ?)
		ORDER BY operation
	`, applyID, storage.ControlRequestCompleted, storage.ControlRequestFailed)
	if err != nil {
		return nil, fmt.Errorf("list settled control requests for apply %d: %w", applyID, err)
	}
	defer utils.CloseAndLog(rows)

	var requests []*storage.ApplyControlRequest
	for rows.Next() {
		req, err := scanControlRequest(rows)
		if err != nil {
			return nil, fmt.Errorf("scan settled control request for apply %d: %w", applyID, err)
		}
		requests = append(requests, req)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read settled control requests for apply %d: %w", applyID, err)
	}
	return requests, nil
}

func (s *controlRequestStore) RecordRemoteFailure(ctx context.Context, req *storage.ApplyControlRequest) (bool, error) {
	var changed bool
	op := fmt.Sprintf("record remote failure for apply %d operation %s", req.ApplyID, req.Operation)
	err := withLockRetry(ctx, s.classifier, op, func() error {
		var attemptErr error
		changed, attemptErr = s.recordRemoteFailure(ctx, req)
		return attemptErr
	})
	if err != nil {
		return false, err
	}
	return changed, nil
}

func (s *controlRequestStore) recordRemoteFailure(ctx context.Context, req *storage.ApplyControlRequest) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, fmt.Errorf("begin remote failure transaction for apply %d operation %s: %w", req.ApplyID, req.Operation, err)
	}
	defer rollbackTx(ctx, tx, "record remote failure")

	existing, err := s.getByApplyOperationForUpdate(ctx, tx, req.ApplyID, req.Operation)
	if err != nil {
		return false, err
	}
	// A pending row is a live request this plane has not handed to the serving
	// plane yet, so it is necessarily newer than anything the serving plane has
	// already settled — the report in hand describes a superseded attempt. The
	// local plane completes a request the moment the serving plane accepts it,
	// so the row a real rejection lands on is completed or absent, never
	// pending. Overwriting a pending row would drop the operator's command:
	// nothing forwards it once it stops being pending.
	if existing != nil && existing.Status == storage.ControlRequestPending {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit superseded remote failure read for apply %d operation %s: %w", req.ApplyID, req.Operation, err)
		}
		slog.DebugContext(ctx, "ignoring remote control rejection for an operation that has been requested again",
			"apply_id", req.ApplyID, "operation", req.Operation)
		return false, nil
	}
	// A mirrored report crosses the control RPC boundary, which carries no
	// operator identity, so its requester can name the forwarding path instead
	// of a person. A row this plane queued does name the operator, and the
	// notice exists to tell them which of their commands did not take effect —
	// so keep whichever value names an operator rather than letting the mirror
	// replace one with the forwarder.
	requestedBy := req.RequestedBy
	if existing != nil && !storage.ControlRequestNamesAnOperator(requestedBy) && existing.RequestedBy != "" {
		requestedBy = existing.RequestedBy
	}
	// The same rejection is reported on every poll until the operator retries
	// the operation, so an unchanged row means the failure is already recorded.
	// The requester is part of that identity: a second operator whose re-issued
	// command fails for the same reason is a distinct rejection, and the notice
	// names who issued the command that did not take effect.
	if existing != nil && existing.Status == storage.ControlRequestFailed &&
		existing.ErrorMessage == req.ErrorMessage && existing.RequestedBy == requestedBy {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit unchanged remote failure read for apply %d operation %s: %w", req.ApplyID, req.Operation, err)
		}
		return false, nil
	}
	if existing != nil {
		if _, err := tx.ExecContext(ctx, `
			UPDATE apply_control_requests
			SET status = ?, requested_by = ?, error_message = ?, completed_at = NOW(), updated_at = NOW()
			WHERE id = ?
		`, storage.ControlRequestFailed, requestedBy, nullString(req.ErrorMessage), existing.ID); err != nil {
			return false, fmt.Errorf("record remote failure on control request %d for apply %d operation %s: %w", existing.ID, req.ApplyID, req.Operation, err)
		}
	} else if _, err := s.identity.InsertID(ctx, tx, `
		INSERT INTO apply_control_requests (
			apply_id, operation, status, requested_by, error_message, metadata, completed_at
		) VALUES (?, ?, ?, ?, ?, ?, NOW())
	`,
		req.ApplyID, req.Operation, storage.ControlRequestFailed,
		req.RequestedBy, nullString(req.ErrorMessage), nullJSON(storage.MirroredControlRequestMetadata()),
	); err != nil {
		return false, fmt.Errorf("create failed control request for apply %d operation %s: %w", req.ApplyID, req.Operation, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit remote failure for apply %d operation %s: %w", req.ApplyID, req.Operation, err)
	}
	return true, nil
}

func (s *controlRequestStore) ClearRemoteFailure(ctx context.Context, applyID int64, operation storage.ControlOperation) (bool, error) {
	var changed bool
	op := fmt.Sprintf("clear remote failure for apply %d operation %s", applyID, operation)
	err := withLockRetry(ctx, s.classifier, op, func() error {
		var attemptErr error
		changed, attemptErr = s.clearRemoteFailure(ctx, applyID, operation)
		return attemptErr
	})
	if err != nil {
		return false, err
	}
	return changed, nil
}

func (s *controlRequestStore) clearRemoteFailure(ctx context.Context, applyID int64, operation storage.ControlOperation) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, fmt.Errorf("begin clear remote failure transaction for apply %d operation %s: %w", applyID, operation, err)
	}
	defer rollbackTx(ctx, tx, "clear remote failure")

	existing, err := s.getByApplyOperationForUpdate(ctx, tx, applyID, operation)
	if err != nil {
		return false, err
	}
	// Only a row the mirror itself created is the mirror's to clear. A row this
	// plane queued carries its own lifecycle — re-requesting it resets it to
	// pending — so clearing one here could erase a failure this plane recorded
	// for its own reason.
	clearable := existing != nil &&
		existing.Status == storage.ControlRequestFailed &&
		existing.IsMirroredRemoteRejection()
	if !clearable {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit clear remote failure read for apply %d operation %s: %w", applyID, operation, err)
		}
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE apply_control_requests
		SET status = ?, error_message = NULL, completed_at = NOW(), updated_at = NOW()
		WHERE id = ?
	`, storage.ControlRequestCompleted, existing.ID); err != nil {
		return false, fmt.Errorf("clear remote failure on control request %d for apply %d operation %s: %w", existing.ID, applyID, operation, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit clear remote failure for apply %d operation %s: %w", applyID, operation, err)
	}
	return true, nil
}

func (s *controlRequestStore) getByIDForUpdate(ctx context.Context, tx *rebindTx, id int64) (*storage.ApplyControlRequest, error) {
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
	tx *rebindTx,
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
