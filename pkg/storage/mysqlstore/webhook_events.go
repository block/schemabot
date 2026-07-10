// webhook_events.go implements WebhookEventStore for durable webhook ingestion.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/block/schemabot/pkg/storage"
)

const webhookEventColumns = `id, provider, delivery_id, event, action, repository, pull_request, head_sha, tenant_id,
	payload, state, attempts, lease_owner, lease_token, lease_expires_at, retry_after, last_error,
	received_at, started_at, completed_at, created_at, updated_at`

type webhookEventStore struct {
	db *sql.DB
}

func (s *webhookEventStore) Create(ctx context.Context, event *storage.WebhookEvent) (bool, error) {
	if event.DeliveryID == "" {
		return false, fmt.Errorf("webhook delivery ID is required")
	}
	if event.Event == "" {
		return false, fmt.Errorf("webhook event type is required")
	}
	provider := event.Provider
	if provider == "" {
		provider = storage.WebhookProviderGitHub
	}
	payload := event.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	state := event.State
	if state == "" {
		state = storage.WebhookEventPending
	}
	receivedAt := event.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now()
	}

	result, err := s.db.ExecContext(ctx, `
		INSERT INTO webhook_events (
			provider, delivery_id, event, action, repository, pull_request, head_sha, tenant_id,
			payload, state, attempts, received_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, provider, event.DeliveryID, event.Event, event.Action, event.Repository, event.PullRequest, event.HeadSHA, event.TenantID,
		string(payload), state, event.Attempts, receivedAt)
	if err != nil {
		if isDuplicateKeyError(err) {
			return false, nil
		}
		return false, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return false, err
	}
	event.ID = id
	event.Provider = provider
	event.State = state
	event.Payload = payload
	event.ReceivedAt = receivedAt
	return true, nil
}

func (s *webhookEventStore) GetByDeliveryID(ctx context.Context, provider, deliveryID string) (*storage.WebhookEvent, error) {
	if provider == "" {
		provider = storage.WebhookProviderGitHub
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT `+webhookEventColumns+`
		FROM webhook_events
		WHERE provider = ? AND delivery_id = ?
	`, provider, deliveryID)
	return scanWebhookEvent(row)
}

func (s *webhookEventStore) FindNext(ctx context.Context, owner string, leaseDuration time.Duration) (*storage.WebhookEvent, error) {
	if owner == "" {
		return nil, fmt.Errorf("webhook driver owner is required")
	}
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("webhook lease duration must be positive")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim webhook event transaction: %w", err)
	}
	defer rollbackTx(ctx, tx, "claim webhook event")

	row := tx.QueryRowContext(ctx, `
		SELECT `+webhookEventColumns+`
		FROM webhook_events
		WHERE state = ?
			OR (state = ? AND (retry_after IS NULL OR retry_after <= NOW()))
			OR (state = ? AND lease_expires_at <= NOW())
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, storage.WebhookEventPending, storage.WebhookEventFailedRetryable, storage.WebhookEventProcessing)

	event, err := scanWebhookEventInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query next claimable webhook event: %w", err)
	}

	leaseToken := uuid.NewString()
	_, err = tx.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, attempts = attempts + 1, lease_owner = ?, lease_token = ?,
			lease_expires_at = DATE_ADD(NOW(), INTERVAL ? MICROSECOND),
			retry_after = NULL, started_at = COALESCE(started_at, NOW()), updated_at = NOW()
		WHERE id = ?
	`, storage.WebhookEventProcessing, owner, leaseToken, leaseDuration.Microseconds(), event.ID)
	if err != nil {
		return nil, fmt.Errorf("claim webhook event %d: %w", event.ID, err)
	}
	event, err = scanWebhookEventInto(tx.QueryRowContext(ctx, `
		SELECT `+webhookEventColumns+`
		FROM webhook_events
		WHERE id = ?
	`, event.ID))
	if err != nil {
		return nil, fmt.Errorf("reload claimed webhook event %d: %w", event.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim webhook event %d: %w", event.ID, err)
	}

	return event, nil
}

func (s *webhookEventStore) Heartbeat(ctx context.Context, id int64, leaseToken string, leaseDuration time.Duration) error {
	if leaseToken == "" {
		return fmt.Errorf("webhook event lease token is required: %w", storage.ErrWebhookEventLeaseLost)
	}
	if leaseDuration <= 0 {
		return fmt.Errorf("webhook lease duration must be positive")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET lease_expires_at = DATE_ADD(NOW(), INTERVAL ? MICROSECOND), updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, leaseDuration.Microseconds(), id, leaseToken)
	if err != nil {
		return fmt.Errorf("heartbeat webhook event %d: %w", id, err)
	}
	return s.checkWebhookEventLeaseResult(ctx, result, id, leaseToken, true)
}

func (s *webhookEventStore) MarkCompleted(ctx context.Context, id int64, leaseToken string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, completed_at = NOW(), lease_owner = NULL, lease_token = NULL,
			lease_expires_at = NULL, retry_after = NULL, updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, storage.WebhookEventCompleted, id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark webhook event %d completed: %w", id, err)
	}
	return s.checkWebhookEventLeaseResult(ctx, result, id, leaseToken, false)
}

func (s *webhookEventStore) MarkFailed(ctx context.Context, id int64, leaseToken string, errMsg string, retryAfter *time.Time) error {
	state := storage.WebhookEventFailed
	if retryAfter != nil {
		state = storage.WebhookEventFailedRetryable
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, last_error = ?, retry_after = ?, lease_owner = NULL, lease_token = NULL,
			lease_expires_at = NULL, completed_at = CASE WHEN ? THEN NOW() ELSE completed_at END,
			updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, state, nullString(errMsg), retryAfter, retryAfter == nil, id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark webhook event %d failed: %w", id, err)
	}
	return s.checkWebhookEventLeaseResult(ctx, result, id, leaseToken, false)
}

func scanWebhookEvent(row *sql.Row) (*storage.WebhookEvent, error) {
	event, err := scanWebhookEventInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return event, err
}

func scanWebhookEventInto(row scanner) (*storage.WebhookEvent, error) {
	var event storage.WebhookEvent
	var leaseOwner, leaseToken, lastError sql.NullString
	var leaseExpiresAt, retryAfter, startedAt, completedAt sql.NullTime
	err := row.Scan(
		&event.ID, &event.Provider, &event.DeliveryID, &event.Event, &event.Action, &event.Repository, &event.PullRequest,
		&event.HeadSHA, &event.TenantID, &event.Payload, &event.State, &event.Attempts,
		&leaseOwner, &leaseToken, &leaseExpiresAt, &retryAfter, &lastError,
		&event.ReceivedAt, &startedAt, &completedAt, &event.CreatedAt, &event.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	event.LeaseOwner = leaseOwner.String
	event.LeaseToken = leaseToken.String
	event.LastError = lastError.String
	if leaseExpiresAt.Valid {
		event.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	if retryAfter.Valid {
		event.RetryAfter = &retryAfter.Time
	}
	if startedAt.Valid {
		event.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		event.CompletedAt = &completedAt.Time
	}
	return &event, nil
}

func (s *webhookEventStore) checkWebhookEventLeaseResult(ctx context.Context, result sql.Result, id int64, leaseToken string, missingOK bool) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows > 0 {
		return nil
	}
	var currentToken sql.NullString
	err = s.db.QueryRowContext(ctx, `SELECT lease_token FROM webhook_events WHERE id = ?`, id).Scan(&currentToken)
	if errors.Is(err, sql.ErrNoRows) {
		if missingOK {
			return nil
		}
		return storage.ErrWebhookEventNotFound
	}
	if err != nil {
		return err
	}
	if currentToken.Valid && currentToken.String == leaseToken {
		return nil
	}
	return storage.ErrWebhookEventLeaseLost
}
