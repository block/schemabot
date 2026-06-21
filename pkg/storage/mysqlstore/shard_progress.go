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
//
// When the caller holds an operation lease (the operator's reconciler does), the
// write is scoped to that lease via the source SELECT, so a displaced driver that
// lost the lease fails closed instead of overwriting the read-model with stale
// progress — matching the guard on other operation-scoped writes (tasks.Update).
// Without a lease on the context the write is unguarded, as for any other store call.
func (s *shardProgressStore) Upsert(ctx context.Context, sp *storage.ShardProgress) error {
	// The row values come from a SELECT so the optional lease predicate can gate
	// the whole insert/update: an empty source row (lease lost) writes nothing.
	source := "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
	args := []any{
		sp.ApplyOperationID, sp.Namespace, sp.TableName, sp.Shard, sp.State,
		sp.ProgressPercent, sp.RowsCopied, sp.RowsTotal, sp.ETASeconds, sp.CutoverAttempts, sp.ReadyToComplete,
	}

	var verifyLeaseStillOwned func() error
	if opLease, ok := storage.OperationLeaseFromContext(ctx); ok {
		if !opLease.Valid() {
			return fmt.Errorf("invalid operation lease for shard progress (operation %d): %w", sp.ApplyOperationID, storage.ErrApplyLeaseLost)
		}
		source += " FROM apply_operations ao WHERE ao.id = ? AND ao.lease_token = ?"
		args = append(args, opLease.OperationID, opLease.Token)
		verifyLeaseStillOwned = func() error { return ensureOperationLeaseStillOwned(ctx, s.db, opLease) }
	}

	result, err := s.db.ExecContext(ctx, `
		INSERT INTO shard_progress
			(apply_operation_id, namespace, table_name, shard, state, progress_percent, rows_copied, rows_total, eta_seconds, cutover_attempts, ready_to_complete)
		`+source+`
		ON DUPLICATE KEY UPDATE
			state = VALUES(state),
			progress_percent = VALUES(progress_percent),
			rows_copied = VALUES(rows_copied),
			rows_total = VALUES(rows_total),
			eta_seconds = VALUES(eta_seconds),
			cutover_attempts = VALUES(cutover_attempts),
			ready_to_complete = VALUES(ready_to_complete)`,
		args...,
	)
	if err != nil {
		return fmt.Errorf("upsert shard progress for operation %d %s.%s shard %s: %w",
			sp.ApplyOperationID, sp.Namespace, sp.TableName, sp.Shard, err)
	}
	if verifyLeaseStillOwned == nil {
		return nil
	}
	// Zero rows affected is either a no-op update (same values) or a lost lease;
	// verifyLeaseStillOwned disambiguates — it returns ErrApplyLeaseLost only when
	// the lease is actually gone, and nil for a benign no-op.
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read shard progress upsert rows affected for operation %d: %w", sp.ApplyOperationID, err)
	}
	if rows == 0 {
		return verifyLeaseStillOwned()
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
