// apply_logs.go implements ApplyLogStore for audit and debugging log entries.
// Captures state transitions, errors, and progress events during applies.
package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// applyLogColumns lists all columns for SELECT queries.
const applyLogColumns = `id, apply_id, task_id, apply_operation_id, level, event_type, source, message,
	old_state, new_state, metadata, created_at`

// applyLogStore implements storage.ApplyLogStore using MySQL.
type applyLogStore struct {
	db       *rebindDB
	dialect  Dialect
	identity identityInserter
}

// applyLogLeasedAppendStatement renders Append's statement for a caller holding
// the apply lease. It binds the ten inserted columns, then the apply ID, then
// the lease token. The row is selected from the leased applies row, so a
// displaced driver's statement selects nothing and inserts nothing; the token
// check goes through LeaseSourceFence so it serializes against a concurrent
// steal instead of passing against a token the statement's snapshot still
// holds, at whatever isolation level the storage session runs.
func applyLogLeasedAppendStatement(d Dialect) string {
	return "INSERT INTO apply_logs (apply_id, task_id, apply_operation_id, level, event_type, source, message, old_state, new_state, metadata) " +
		"SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM applies a WHERE a.id = ? AND " + d.LeaseSourceFence("applies", "a", "id", "lease_token")
}

// Append adds a new log entry.
func (s *applyLogStore) Append(ctx context.Context, log *storage.ApplyLog) error {
	lease, hasLease, err := applyLeaseFromContext(ctx, log.ApplyID)
	if err != nil {
		return err
	}
	if log.ApplyOperationID == nil && !log.ApplyWide {
		log.ApplyOperationID = drivingOperationID(ctx, log.ApplyID)
	}
	// Default source to schemabot if not set
	source := log.Source
	if source == "" {
		source = storage.LogSourceSchemaBot
	}

	if hasLease {
		id, inserted, err := s.identity.InsertGuardedID(ctx, s.db, applyLogLeasedAppendStatement(s.dialect),
			log.ApplyID, nullInt64Ptr(log.TaskID), nullInt64Ptr(log.ApplyOperationID), log.Level, log.EventType, source, log.Message,
			nullString(log.OldState), nullString(log.NewState), nullJSON(log.Metadata),
			lease.ApplyID, lease.Token,
		)
		if err != nil {
			return fmt.Errorf("append apply log for apply %d: %w", log.ApplyID, err)
		}
		if !inserted {
			if err := ensureApplyLeaseStillOwned(ctx, s.db, lease); err != nil {
				return err
			}
			return fmt.Errorf("append apply log for apply %d matched no rows despite current lease", log.ApplyID)
		}
		log.ID = id
		return nil
	}

	id, err := s.identity.InsertID(ctx, s.db, `
		INSERT INTO apply_logs (
			apply_id, task_id, apply_operation_id, level, event_type, source, message,
			old_state, new_state, metadata
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		log.ApplyID, nullInt64Ptr(log.TaskID), nullInt64Ptr(log.ApplyOperationID), log.Level, log.EventType, source, log.Message,
		nullString(log.OldState), nullString(log.NewState), nullJSON(log.Metadata),
	)
	if err != nil {
		return err
	}
	log.ID = id

	return nil
}

// drivingOperationID returns the operation whose drive is writing to applyID,
// read off the operation lease on ctx: an operation-scoped drive carries its
// operation's lease for every write it makes. Nil when ctx carries no
// operation lease, or one for another apply.
func drivingOperationID(ctx context.Context, applyID int64) *int64 {
	lease, ok := storage.OperationLeaseFromContext(ctx)
	if !ok || lease.ApplyID != applyID || lease.OperationID <= 0 {
		return nil
	}
	id := lease.OperationID
	return &id
}

// GetByApply returns all logs for an apply, ordered by created_at ascending.
// Ties on created_at are broken by id so entries keep their insertion order.
func (s *applyLogStore) GetByApply(ctx context.Context, applyID int64) ([]*storage.ApplyLog, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyLogColumns+`
		FROM apply_logs
		WHERE apply_id = ?
		ORDER BY created_at ASC, id ASC
	`, applyID)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	return scanApplyLogs(rows)
}

// GetRecentByApply returns the newest limit logs for an apply, ordered by
// created_at ascending. Ties on created_at (second precision) are broken by
// id so entries keep their insertion order.
func (s *applyLogStore) GetRecentByApply(ctx context.Context, applyID int64, limit int) ([]*storage.ApplyLog, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyLogColumns+`
		FROM apply_logs
		WHERE apply_id = ?
		ORDER BY created_at DESC, id DESC
		LIMIT ?
	`, applyID, limit)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	logs, err := scanApplyLogs(rows)
	if err != nil {
		return nil, err
	}
	slices.Reverse(logs)
	return logs, nil
}

// List returns the newest Limit logs matching the filter criteria, ordered by
// created_at ascending so the result reads chronologically. Ties on created_at
// (second precision) are broken by id so entries keep their insertion order.
// Callers rendering a bounded tail (like the CLI logs command) need the newest
// entries — a long-running apply accumulates far more log rows than the window.
func (s *applyLogStore) List(ctx context.Context, filter storage.ApplyLogFilter) ([]*storage.ApplyLog, error) {
	// Build query with filters
	query := `
		SELECT ` + applyLogColumns + `
		FROM apply_logs
		WHERE apply_id = ?
	`
	args := []any{filter.ApplyID}

	if filter.TaskID != nil {
		query += " AND task_id = ?"
		args = append(args, *filter.TaskID)
	}
	if len(filter.OperationIDs) > 0 {
		// An entry belongs to an operation when its drive wrote it, or when it
		// is about one of the operation's tasks: a task-scoped entry written
		// outside the operation's drive still names the task. An entry that
		// names neither is about the whole apply, and so about every operation.
		marks := placeholders(len(filter.OperationIDs))
		query += " AND (apply_operation_id IN (" + marks + ") OR task_id IN (SELECT id FROM tasks WHERE apply_operation_id IN (" + marks + ")) OR (apply_operation_id IS NULL AND task_id IS NULL))"
		for range 2 {
			for _, id := range filter.OperationIDs {
				args = append(args, id)
			}
		}
	}
	if filter.Level != "" {
		query += " AND level = ?"
		args = append(args, filter.Level)
	}
	if filter.EventType != "" {
		query += " AND event_type = ?"
		args = append(args, filter.EventType)
	}

	query += " ORDER BY created_at DESC, id DESC"

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	query += " LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	logs, err := scanApplyLogs(rows)
	if err != nil {
		return nil, err
	}
	slices.Reverse(logs)
	return logs, nil
}

// scanApplyLogs scans multiple apply log rows.
func scanApplyLogs(rows *sql.Rows) ([]*storage.ApplyLog, error) {
	var logs []*storage.ApplyLog
	for rows.Next() {
		log, err := scanApplyLogInto(rows)
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	return logs, rows.Err()
}

// scanApplyLogInto scans apply log data from a scanner.
func scanApplyLogInto(s scanner) (*storage.ApplyLog, error) {
	var log storage.ApplyLog
	var taskID, operationID sql.NullInt64
	var oldState, newState sql.NullString
	var metadata []byte

	err := s.Scan(
		&log.ID, &log.ApplyID, &taskID, &operationID, &log.Level, &log.EventType, &log.Source, &log.Message,
		&oldState, &newState, &metadata, &log.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	if taskID.Valid {
		log.TaskID = &taskID.Int64
	}
	if operationID.Valid {
		log.ApplyOperationID = &operationID.Int64
	}
	if oldState.Valid {
		log.OldState = oldState.String
	}
	if newState.Valid {
		log.NewState = newState.String
	}
	log.Metadata = metadata

	return &log, nil
}
