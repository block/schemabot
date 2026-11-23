package sqlstore

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
)

const logEventOperationDispatch = "operation_dispatch"

func operationDispatchMessage(attempt int) string {
	return fmt.Sprintf("Dispatched operation attempt %d", attempt+1)
}

// RecordOperationDispatch appends a durable marker for the attempt under the
// lease that admitted it. A later lease sees the same marker and reattaches.
func (s *applyLogStore) RecordOperationDispatch(ctx context.Context, lease storage.OperationLease, attempt int) (bool, error) {
	if !lease.Valid() {
		return false, storage.ErrApplyLeaseLost
	}
	message := operationDispatchMessage(attempt)
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO apply_logs (apply_id, apply_operation_id, level, event_type, source, message)
		SELECT o.apply_id, o.id, ?, ?, ?, ?
		FROM apply_operations o
		WHERE o.id = ? AND o.apply_id = ? AND o.attempt = ?
		  AND `+s.dialect.LeaseSourceFence("apply_operations", "o", "id", "lease_token")+`
		  AND NOT EXISTS (
		      SELECT 1 FROM apply_logs l
		      WHERE l.apply_id = o.apply_id AND l.apply_operation_id = o.id AND l.event_type = ? AND l.message = ?
		  )
	`, storage.LogLevelInfo, logEventOperationDispatch, storage.LogSourceSchemaBot, message,
		lease.OperationID, lease.ApplyID, attempt, lease.Token, logEventOperationDispatch, message)
	if err != nil {
		return false, fmt.Errorf("record operation dispatch: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read operation dispatch rows affected: %w", err)
	}
	return rows == 1, nil
}

func (s *applyLogStore) HasOperationDispatch(ctx context.Context, lease storage.OperationLease, attempt int) (bool, error) {
	var dispatched bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM apply_logs
		    WHERE apply_id = ? AND apply_operation_id = ? AND event_type = ? AND message = ?
		)
	`, lease.ApplyID, lease.OperationID, logEventOperationDispatch, operationDispatchMessage(attempt)).Scan(&dispatched)
	if err != nil {
		return false, fmt.Errorf("check operation dispatch: %w", err)
	}
	return dispatched, nil
}
