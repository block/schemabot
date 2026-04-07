// apply_logs.go implements ApplyLogStore for audit and debugging log entries.
// Captures state transitions, errors, and progress events during applies.
package mysqlstore

import (
	"context"
	"database/sql"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// applyLogColumns lists all columns for SELECT queries.
const applyLogColumns = `id, apply_id, task_id, level, event_type, source, message,
	old_state, new_state, metadata, created_at`

// applyLogStore implements storage.ApplyLogStore using MySQL.
type applyLogStore struct {
	db *sql.DB
}

// Append adds a new log entry.
func (s *applyLogStore) Append(ctx context.Context, log *storage.ApplyLog) error {
	// Default source to schemabot if not set
	source := log.Source
	if source == "" {
		source = storage.LogSourceSchemaBot
	}

	result, err := s.db.ExecContext(ctx, `
		INSERT INTO apply_logs (
			apply_id, task_id, level, event_type, source, message,
			old_state, new_state, metadata
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		log.ApplyID, nullInt64Ptr(log.TaskID), log.Level, log.EventType, source, log.Message,
		nullString(log.OldState), nullString(log.NewState), nullJSON(log.Metadata),
	)
	if err != nil {
		return err
	}

	// Set the auto-generated ID
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	log.ID = id

	return nil
}

// GetByApply returns all logs for an apply, ordered by created_at.
func (s *applyLogStore) GetByApply(ctx context.Context, applyID int64) ([]*storage.ApplyLog, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+applyLogColumns+`
		FROM apply_logs
		WHERE apply_id = ?
		ORDER BY created_at ASC
	`, applyID)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	return scanApplyLogs(rows)
}

// List returns logs matching the filter criteria, ordered by created_at.
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
	if filter.Level != "" {
		query += " AND level = ?"
		args = append(args, filter.Level)
	}
	if filter.EventType != "" {
		query += " AND event_type = ?"
		args = append(args, filter.EventType)
	}

	query += " ORDER BY created_at ASC"

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

	return scanApplyLogs(rows)
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
	var taskID sql.NullInt64
	var oldState, newState sql.NullString
	var metadata []byte

	err := s.Scan(
		&log.ID, &log.ApplyID, &taskID, &log.Level, &log.EventType, &log.Source, &log.Message,
		&oldState, &newState, &metadata, &log.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	if taskID.Valid {
		log.TaskID = &taskID.Int64
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
