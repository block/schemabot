package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/storage"
)

type shardProgressStore struct {
	db *sql.DB
}

// Upsert inserts or updates one shard's progress row, keyed by
// (apply_operation_id, namespace, table_name, shard). Only the display fields are
// updated on conflict; the key columns and created_at are preserved.
func (s *shardProgressStore) Upsert(ctx context.Context, sp *storage.ShardProgress) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO shard_progress
			(apply_operation_id, namespace, table_name, shard, state, progress_percent, rows_copied, rows_total, eta_seconds, cutover_attempts, ready_to_complete)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			state = VALUES(state),
			progress_percent = VALUES(progress_percent),
			rows_copied = VALUES(rows_copied),
			rows_total = VALUES(rows_total),
			eta_seconds = VALUES(eta_seconds),
			cutover_attempts = VALUES(cutover_attempts),
			ready_to_complete = VALUES(ready_to_complete)`,
		sp.ApplyOperationID, sp.Namespace, sp.TableName, sp.Shard, sp.State,
		sp.ProgressPercent, sp.RowsCopied, sp.RowsTotal, sp.ETASeconds, sp.CutoverAttempts, sp.ReadyToComplete,
	)
	if err != nil {
		return fmt.Errorf("upsert shard progress for operation %d %s.%s shard %s: %w",
			sp.ApplyOperationID, sp.Namespace, sp.TableName, sp.Shard, err)
	}
	return nil
}

// GetByApplyOperationID returns all shard-progress rows for an operation, ordered
// by namespace, table_name, shard so the renderer sees a stable order.
func (s *shardProgressStore) GetByApplyOperationID(ctx context.Context, applyOperationID int64) ([]*storage.ShardProgress, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT apply_operation_id, namespace, table_name, shard, state, progress_percent, rows_copied, rows_total, eta_seconds, cutover_attempts, ready_to_complete
		FROM shard_progress
		WHERE apply_operation_id = ?
		ORDER BY namespace, table_name, shard`, applyOperationID,
	)
	if err != nil {
		return nil, fmt.Errorf("query shard progress for operation %d: %w", applyOperationID, err)
	}
	defer utils.CloseAndLog(rows)

	var result []*storage.ShardProgress
	for rows.Next() {
		var sp storage.ShardProgress
		if err := rows.Scan(
			&sp.ApplyOperationID, &sp.Namespace, &sp.TableName, &sp.Shard, &sp.State,
			&sp.ProgressPercent, &sp.RowsCopied, &sp.RowsTotal, &sp.ETASeconds, &sp.CutoverAttempts, &sp.ReadyToComplete,
		); err != nil {
			return nil, fmt.Errorf("scan shard progress for operation %d: %w", applyOperationID, err)
		}
		result = append(result, &sp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate shard progress for operation %d: %w", applyOperationID, err)
	}
	return result, nil
}
