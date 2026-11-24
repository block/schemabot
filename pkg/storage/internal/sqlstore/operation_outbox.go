package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/google/uuid"
)

const operationFinishedEventType = "operation.finished"

func notifiesOperationOutcome(outcome string) bool {
	return notifiesApplyOutcome(outcome) || state.IsState(outcome, state.ApplyOperation.FailedRetryable)
}

// writeFinishedOperation retains the existing fenced state write. The operation
// lock pins its deployment and retry count to the claim's token until commit;
// neither a sibling retry nor a parent projection supplies the event's attempt.
func (s *applyOperationStore) writeFinishedOperation(ctx context.Context, id int64, guard operationWriteGuard, statement string, args []any) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin operation outcome transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, "operation outcome")
	var applyID int64
	var deployment, token string
	var attempt int
	var recorded sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT apply_id, deployment, attempt, lease_token, finished_event_attempt
 FROM apply_operations WHERE id = ? FOR UPDATE`, id).Scan(&applyID, &deployment, &attempt, &token, &recorded)
	if errors.Is(err, sql.ErrNoRows) {
		return applyOperationMissingResult(id, false)
	}
	if err != nil {
		return fmt.Errorf("read operation outcome identity (id=%d): %w", id, err)
	}
	if token != guard.opLease.Token {
		return fmt.Errorf("apply_operation %d is no longer owned by its operation lease: %w", id, storage.ErrApplyLeaseLost)
	}
	if _, err := tx.ExecContext(ctx, statement, args...); err != nil {
		return fmt.Errorf("write operation outcome (id=%d): %w", id, err)
	}
	var failure *outcomeRecordingFailure
	if !recorded.Valid || recorded.Int64 != int64(attempt) {
		failure, err = s.enqueueOperationFinished(ctx, tx, id, applyID, deployment, attempt)
		if err != nil {
			return err
		}
		// Under continue this also marks a deliberately lost intent. Replaying the
		// state write must not silently backfill a notification after that decision.
		if _, err := tx.ExecContext(ctx, `UPDATE apply_operations SET finished_event_attempt = ?, updated_at = updated_at WHERE id = ? AND lease_token = ?`, attempt, id, guard.opLease.Token); err != nil {
			return fmt.Errorf("record operation outcome attempt (id=%d): %w", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit operation outcome (id=%d): %w", id, err)
	}
	failure.report(ctx)
	return nil
}

func (s *applyOperationStore) enqueueOperationFinished(ctx context.Context, tx *rebindTx, id, applyID int64, deployment string, attempt int) (*outcomeRecordingFailure, error) {
	var event storage.OperationFinishedEvent
	err := tx.QueryRowContext(ctx, `SELECT a.apply_identifier, a.database_name, a.database_type, a.environment,
 a.repository, a.pull_request, COALESCE(ao.completed_at, ao.updated_at), ao.state
 FROM apply_operations ao JOIN applies a ON a.id = ao.apply_id WHERE ao.id = ?`, id).Scan(
		&event.ID, &event.Database, &event.DatabaseType, &event.Environment, &event.Repository,
		&event.PullRequest, &event.CompletedAt, &event.Outcome)
	if err != nil {
		return nil, fmt.Errorf("read finished operation %d for notification: %w", id, err)
	}
	event.Type = operationFinishedEventType
	event.EventID = uuid.NewString()
	event.OccurredAtNanos = time.Now().UnixNano()
	event.CompletedAt = event.CompletedAt.UTC()
	event.OperationID, event.Deployment, event.Attempt = id, deployment, attempt+1
	if s.changeEventApp != nil {
		event.App = s.changeEventApp(event.Database)
	}
	if event.App == "" {
		event.App = event.Database
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("encode operation outcome notification (id=%d): %w", id, err)
	}
	failure, err := recordOutcomeIntent(ctx, tx, applyID, event.ApplyFinishedEvent, payload, s.recordingErrorPolicy)
	if failure != nil {
		failure.operation = &storage.ApplyOperation{ID: id, ApplyID: applyID, Deployment: deployment, Attempt: attempt, State: event.Outcome}
	}
	return failure, err
}
