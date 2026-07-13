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
	payload := nullJSON(event.Payload)
	// New deliveries are always pending. Accepting any other state here would
	// let a caller persist a row that no lifecycle path can move — e.g. a
	// "processing" row with NULL lease columns is never claimable and never
	// expires, yet its delivery GUID dedups all future redeliveries into
	// no-ops, silently wedging that delivery forever.
	state := event.State
	if state == "" {
		state = storage.WebhookEventPending
	}
	if state != storage.WebhookEventPending {
		return false, fmt.Errorf("create webhook event (delivery_id=%s): new deliveries must be pending, got %q", event.DeliveryID, state)
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
		payload, state, event.Attempts, receivedAt)
	if err != nil {
		if isDuplicateKeyError(err) {
			return s.reopenTerminalWebhookEvent(ctx, provider, event.DeliveryID, payload, receivedAt)
		}
		return false, fmt.Errorf("insert webhook event (provider=%s, delivery_id=%s): %w", provider, event.DeliveryID, err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return false, fmt.Errorf("read webhook event last insert id (delivery_id=%s): %w", event.DeliveryID, err)
	}
	event.ID = id
	event.Provider = provider
	event.State = state
	event.Payload = []byte(payload)
	event.ReceivedAt = receivedAt
	return true, nil
}

// reopenTerminalWebhookEvent handles the duplicate-GUID branch of Create.
// GitHub's "Redeliver" reuses the original delivery GUID, so plain dedup would
// make redelivery a permanent no-op for a terminally failed row — the one case
// where an operator most needs it to work. Re-open only terminal failures:
// pending/retryable/processing rows are genuinely in flight (dedup, return
// false), and completed rows must not re-run. last_error is kept for forensics
// until the next attempt overwrites it.
func (s *webhookEventStore) reopenTerminalWebhookEvent(ctx context.Context, provider, deliveryID, payload string, receivedAt time.Time) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, attempts = 0, payload = ?, received_at = ?,
			lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			retry_after = NULL, completed_at = NULL, started_at = NULL, updated_at = NOW()
		WHERE provider = ? AND delivery_id = ? AND state = ?
	`, storage.WebhookEventPending, payload, receivedAt, provider, deliveryID, storage.WebhookEventFailed)
	if err != nil {
		return false, fmt.Errorf("reopen terminally failed webhook delivery (provider=%s, delivery_id=%s): %w", provider, deliveryID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read reopened webhook delivery rows affected (provider=%s, delivery_id=%s): %w", provider, deliveryID, err)
	}
	return rows > 0, nil
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
			OR (state = ? AND (retry_after IS NULL OR retry_after <= NOW()) AND attempts < ?)
			OR (state = ? AND lease_expires_at <= NOW(6) AND attempts < ?)
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, storage.WebhookEventPending,
		storage.WebhookEventFailedRetryable, storage.MaxWebhookEventAttempts,
		storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts)

	event, err := scanWebhookEventInto(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query next claimable webhook event: %w", err)
	}

	leaseToken := uuid.NewString()
	now := time.Now()
	leaseExpiresAt := now.Add(leaseDuration)
	_, err = tx.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, attempts = attempts + 1, lease_owner = ?, lease_token = ?,
			lease_expires_at = DATE_ADD(NOW(6), INTERVAL ? MICROSECOND),
			retry_after = NULL, started_at = COALESCE(started_at, NOW()), updated_at = NOW()
		WHERE id = ?
	`, storage.WebhookEventProcessing, owner, leaseToken, leaseDuration.Microseconds(), event.ID)
	if err != nil {
		return nil, fmt.Errorf("claim webhook event %d: %w", event.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim webhook event %d: %w", event.ID, err)
	}

	// Reflect the committed claim on the scanned row instead of reloading it,
	// avoiding a second round trip that would re-transfer the full payload. The
	// row was held under FOR UPDATE for the whole transaction, so no other writer
	// could have changed it. lease_expires_at is the application-clock estimate of
	// the database's NOW(6)+leaseDuration; callers schedule heartbeats from
	// leaseDuration, not from this absolute value.
	event.State = storage.WebhookEventProcessing
	event.Attempts++
	event.LeaseOwner = owner
	event.LeaseToken = leaseToken
	event.LeaseExpiresAt = &leaseExpiresAt
	event.RetryAfter = nil
	if event.StartedAt == nil {
		event.StartedAt = &now
	}
	event.UpdatedAt = now

	return event, nil
}

func (s *webhookEventStore) Heartbeat(ctx context.Context, id int64, leaseToken string, leaseDuration time.Duration) error {
	if leaseToken == "" {
		return fmt.Errorf("webhook event lease token is required")
	}
	if leaseDuration <= 0 {
		return fmt.Errorf("webhook lease duration must be positive")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET lease_expires_at = DATE_ADD(NOW(6), INTERVAL ? MICROSECOND), updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, leaseDuration.Microseconds(), id, leaseToken)
	if err != nil {
		return fmt.Errorf("heartbeat webhook event %d: %w", id, err)
	}
	return s.checkWebhookEventLeaseResult(ctx, result, id, leaseToken)
}

// MarkCompleted marks a claimed event terminal-successful.
//
// Idempotent: the lease token is retained (not cleared) and completed_at is
// COALESCE-preserved, so a retry after a committed-but-unacknowledged first
// attempt still matches the row and is a no-op that returns nil, rather than
// misreporting the completion as a lost lease. A genuine reclaim rotates the
// token, so a write with a stale token still returns ErrWebhookEventLeaseLost.
func (s *webhookEventStore) MarkCompleted(ctx context.Context, id int64, leaseToken string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, storage.WebhookEventCompleted, id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark webhook event %d completed: %w", id, err)
	}
	return s.checkWebhookEventLeaseResult(ctx, result, id, leaseToken)
}

// MarkFailed marks a claimed event failed. A non-nil retryAfter keeps it
// retryable after that time; nil makes the failure terminal.
//
// Idempotent for the same lease token, on the same rationale as MarkCompleted:
// the token is retained and completed_at is COALESCE-preserved for terminal
// failures. A retryable failure keeps completed_at NULL; the row becomes
// claimable again via retry_after, and FindNext rotates a fresh token on the
// next claim so the retained token cannot be reused to claim.
func (s *webhookEventStore) MarkFailed(ctx context.Context, id int64, leaseToken string, errMsg string, retryAfter *time.Time) error {
	state := storage.WebhookEventFailed
	if retryAfter != nil {
		state = storage.WebhookEventFailedRetryable
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, last_error = ?, retry_after = ?,
			completed_at = CASE WHEN ? THEN COALESCE(completed_at, NOW()) ELSE completed_at END,
			updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, state, nullString(errMsg), retryAfter, retryAfter == nil, id, leaseToken)
	if err != nil {
		return fmt.Errorf("mark webhook event %d failed: %w", id, err)
	}
	return s.checkWebhookEventLeaseResult(ctx, result, id, leaseToken)
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

func (s *webhookEventStore) checkWebhookEventLeaseResult(ctx context.Context, result sql.Result, id int64, leaseToken string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read webhook event %d lease write rows affected: %w", id, err)
	}
	if rows > 0 {
		return nil
	}
	var currentToken sql.NullString
	err = s.db.QueryRowContext(ctx, `SELECT lease_token FROM webhook_events WHERE id = ?`, id).Scan(&currentToken)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrWebhookEventNotFound
	}
	if err != nil {
		return fmt.Errorf("verify webhook event %d lease token: %w", id, err)
	}
	if currentToken.Valid && currentToken.String == leaseToken {
		return nil
	}
	return storage.ErrWebhookEventLeaseLost
}
