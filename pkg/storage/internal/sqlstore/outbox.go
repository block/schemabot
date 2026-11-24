package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
	"github.com/google/uuid"
)

const applyFinishedEventType = "apply.finished"

func notifiesApplyOutcome(outcome string) bool {
	return state.IsState(outcome, state.SettledApplyStates...)
}

// enqueueApplyFinished reads the winning stored outcome, never a caller's stale
// apply snapshot. Its insert shares the transaction that records that outcome.
func (s *applyStore) enqueueApplyFinished(ctx context.Context, tx *rebindTx, applyID int64) (*outcomeRecordingFailure, error) {
	var event storage.ApplyFinishedEvent
	err := tx.QueryRowContext(ctx, `
		SELECT apply_identifier, database_name, database_type, environment,
		       repository, pull_request, COALESCE(completed_at, updated_at), state
		FROM applies WHERE id = ? AND state IN (?, ?, ?, ?)
	`, applyID, state.Apply.Completed, state.Apply.Failed, state.Apply.Cancelled, state.Apply.Reverted).Scan(&event.ID, &event.Database, &event.DatabaseType,
		&event.Environment, &event.Repository, &event.PullRequest, &event.CompletedAt, &event.Outcome)
	if err != nil {
		return nil, fmt.Errorf("read finished apply %d for notification: %w", applyID, err)
	}
	event.CompletedAt = event.CompletedAt.UTC()
	event.Type = applyFinishedEventType
	event.EventID = uuid.NewString()
	event.OccurredAtNanos = time.Now().UnixNano()
	if s.changeEventApp != nil {
		event.App = s.changeEventApp(event.Database)
	}
	if event.App == "" {
		event.App = event.Database
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("encode apply outcome notification for apply %s: %w", event.ID, err)
	}
	return recordOutcomeIntent(ctx, tx, applyID, event, payload, s.recordingErrorPolicy)
}

// recordOutcomeIntent applies the installation policy only to the outbox insert.
func recordOutcomeIntent(ctx context.Context, tx *rebindTx, applyID int64, event storage.ApplyFinishedEvent, payload []byte, policy storage.RecordingErrorPolicy) (*outcomeRecordingFailure, error) {
	continueOnError := policy == storage.RecordingErrorContinue
	if continueOnError {
		// PostgreSQL aborts the transaction after a statement error. Recover only
		// the intent insert; state writes and their lease/CAS fences precede it.
		if _, err := tx.ExecContext(ctx, "SAVEPOINT completion_notification"); err != nil {
			return nil, fmt.Errorf("save notification recording for apply %s: %w", event.ID, err)
		}
	}
	_, recordingErr := tx.ExecContext(ctx, `
  INSERT INTO outbox_events (apply_id, event_type, payload, status) VALUES (?, ?, ?, ?)
 `, applyID, event.Type, string(payload), storage.OutboxStatusPending)
	if recordingErr != nil {
		if event.Type == operationFinishedEventType {
			recordingErr = fmt.Errorf("enqueue operation outcome notification for apply %s: %w", event.ID, recordingErr)
		} else {
			recordingErr = fmt.Errorf("enqueue apply outcome notification for apply %s: %w", event.ID, recordingErr)
		}
		if !continueOnError {
			return nil, recordingErr
		}
		if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT completion_notification"); err != nil {
			return nil, fmt.Errorf("recover notification recording for apply %s (%w): %w", event.ID, recordingErr, err)
		}
	}
	if continueOnError {
		if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT completion_notification"); err != nil {
			return nil, fmt.Errorf("release notification recording for apply %s: %w", event.ID, err)
		}
	}
	if recordingErr != nil {
		return &outcomeRecordingFailure{event: event, err: recordingErr}, nil
	}
	return nil, nil
}

// outcomeRecordingFailure is reported only after the outer transaction commits.
// A failed commit must not be counted as a committed outcome without intent.
type outcomeRecordingFailure struct {
	event     storage.ApplyFinishedEvent
	operation *storage.ApplyOperation
	err       error
}

func (f *outcomeRecordingFailure) report(ctx context.Context) {
	if f == nil {
		return
	}
	apply := storage.Apply{ApplyIdentifier: f.event.ID, Database: f.event.Database,
		DatabaseType: f.event.DatabaseType, Environment: f.event.Environment,
		Repository: f.event.Repository, PullRequest: f.event.PullRequest, State: f.event.Outcome}
	if f.operation != nil {
		slog.ErrorContext(ctx, "Operation outcome committed without notification intent; recording failed and will not be retried",
			append(append(apply.IdentityLogAttrs(), f.operation.LogAttrs()...), "event_type", f.event.Type, "attempt", f.operation.Attempt+1, "recording_error_policy", storage.RecordingErrorContinue, "error", f.err)...)
		metrics.RecordOutboxRecordingFailure(ctx, f.event.Environment)
		return
	}
	slog.ErrorContext(ctx, "Apply outcome committed without notification intent; recording failed and will not be retried",
		append(apply.LogAttrs(), "recording_error_policy", storage.RecordingErrorContinue, "error", f.err)...)
	metrics.RecordOutboxRecordingFailure(ctx, f.event.Environment)
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
