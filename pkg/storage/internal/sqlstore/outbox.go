package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
	"github.com/google/uuid"
)

const applyCompletedEventType = "apply_completed"

// enqueueApplyCompleted reads the winning stored outcome, never a caller's stale
// apply snapshot. Its insert shares the transaction that records completion.
func enqueueApplyCompleted(ctx context.Context, tx *rebindTx, applyID int64) error {
	var event storage.ApplyCompletedEvent
	err := tx.QueryRowContext(ctx, `
		SELECT apply_identifier, database_name, database_type, environment,
		       repository, pull_request, COALESCE(completed_at, updated_at)
		FROM applies WHERE id = ? AND state = ?
	`, applyID, state.Apply.Completed).Scan(&event.ID, &event.Database, &event.DatabaseType,
		&event.Environment, &event.Repository, &event.PullRequest, &event.CompletedAt)
	if err != nil {
		return fmt.Errorf("read completed apply %d for notification: %w", applyID, err)
	}
	event.CompletedAt = event.CompletedAt.UTC()
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode completion notification for apply %s: %w", event.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_events (apply_id, event_type, payload, status) VALUES (?, ?, ?, ?)
	`, applyID, applyCompletedEventType, string(payload), storage.OutboxStatusPending); err != nil {
		return fmt.Errorf("enqueue completion notification for apply %s: %w", event.ID, err)
	}
	return nil
}

type outboxStore struct {
	db *rebindDB
}

// MaxID bounds a scan independently of later inserts and their commit order.
func (s *outboxStore) MaxID(ctx context.Context) (int64, error) {
	var id int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id), 0) FROM outbox_events`).Scan(&id); err != nil {
		return 0, fmt.Errorf("read completion notification high-water ID: %w", err)
	}
	return id, nil
}

func (s *outboxStore) ListUnpublished(ctx context.Context, afterID int64, limit int) ([]*storage.OutboxEvent, error) {
	if limit < 1 || limit > 100 {
		return nil, fmt.Errorf("list unpublished notifications: limit must be between 1 and 100")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, apply_id, event_type, payload, created_at
		FROM outbox_events WHERE status = ? AND id > ? ORDER BY id LIMIT ?
	`, storage.OutboxStatusPending, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("list unpublished notifications: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var events []*storage.OutboxEvent
	for rows.Next() {
		event := &storage.OutboxEvent{}
		if err := rows.Scan(&event.ID, &event.ApplyID, &event.EventType, &event.Payload, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan unpublished notification: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read unpublished notifications: %w", err)
	}
	return events, nil
}

func (s *outboxStore) MarkPublished(ctx context.Context, id int64) error {
	// A prefetched send may finish after quarantine or a future permanent skip.
	// Record that acceptance without clearing the hold or its retained audit.
	if _, err := s.db.ExecContext(ctx, `
		UPDATE outbox_events
		SET status = CASE WHEN status = ? THEN ? ELSE status END,
		    published_at = COALESCE(published_at, NOW())
		WHERE id = ?
	`, storage.OutboxStatusPending, storage.OutboxStatusPublished, id); err != nil {
		return fmt.Errorf("mark notification %d published: %w", id, err)
	}
	return nil
}

func (s *outboxStore) OldestUnpublishedAt(ctx context.Context) (*time.Time, error) {
	var oldest sql.NullTime
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(created_at) FROM outbox_events WHERE status IN (?, ?)`, storage.OutboxStatusPending, storage.OutboxStatusQuarantined).Scan(&oldest); err != nil {
		return nil, fmt.Errorf("read oldest unpublished notification time: %w", err)
	}
	if !oldest.Valid {
		return nil, nil
	}
	return &oldest.Time, nil
}

// Quarantine leaves transport acceptance and the completed apply unchanged.
// Its token identifies this incident even if the same row is released and
// quarantined again within the storage clock's timestamp precision.
func (s *outboxStore) Quarantine(ctx context.Context, id int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin quarantine for notification %d: %w", id, err)
	}
	defer rollbackTx(ctx, tx, "quarantine completion notification")
	var applyID int64
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT apply_id, status FROM outbox_events WHERE id = ? FOR UPDATE`, id).Scan(&applyID, &status); err != nil {
		return fmt.Errorf("read notification %d for quarantine: %w", id, err)
	}
	if status != storage.OutboxStatusPending {
		slog.DebugContext(ctx, "completion notification is not pending; no quarantine recorded", "outbox_id", id, "status", status)
		return nil
	}
	token := uuid.NewString()
	if _, err := tx.ExecContext(ctx, `UPDATE outbox_events SET status = ?, quarantine_token = ? WHERE id = ?`, storage.OutboxStatusQuarantined, token, id); err != nil {
		return fmt.Errorf("quarantine notification %d: %w", id, err)
	}
	metadata, err := json.Marshal(struct {
		OutboxID        int64  `json:"outbox_id"`
		QuarantineToken string `json:"quarantine_token"`
	}{id, token})
	if err != nil {
		return fmt.Errorf("encode quarantine diagnostic for notification %d: %w", id, err)
	}
	if _, err := tx.ExecContext(ctx, `
  INSERT INTO apply_logs (apply_id, level, event_type, source, message, metadata)
  VALUES (?, ?, ?, ?, ?, ?)
 `, applyID, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
		"Completion notification quarantined after a panic. It remains held until an authorized release operation is available.", string(metadata)); err != nil {
		return fmt.Errorf("record quarantine diagnostic for notification %d: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit quarantine for notification %d: %w", id, err)
	}
	return nil
}
