// webhook_events.go implements WebhookEventStore for durable webhook ingestion.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/block/spirit/pkg/utils"

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
// make redelivery a permanent no-op for a terminal row — the one case where an
// operator most needs it to work. Re-open:
//   - failed and completed rows: a completed row can still have lost its
//     follow-on work if the process died after the delivery was marked completed
//     but before the detached plan goroutines durably recorded their plans, and
//     re-running auto-plan on the same head SHA is idempotent.
//   - processing rows whose lease has expired: a driver hard-killed on its final
//     attempt leaves the row parked in processing with attempts at the cap,
//     which FindNext never reclaims — so it would otherwise be recoverable only
//     by the reconciler's periodic sweep. Matching it here makes Redeliver an
//     immediate recovery lever. An expired lease means no live owner, so the
//     reopen can't race a real driver, and the lease-token guard on
//     MarkCompleted/MarkFailed still rejects a returning zombie driver.
//
// pending/retryable rows and processing rows with a live (unexpired) lease are
// genuinely in flight, so they dedup (return false). last_error is kept for
// forensics until the next attempt overwrites it.
func (s *webhookEventStore) reopenTerminalWebhookEvent(ctx context.Context, provider, deliveryID, payload string, receivedAt time.Time) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, attempts = 0, payload = ?, received_at = ?,
			lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			retry_after = NULL, completed_at = NULL, started_at = NULL, updated_at = NOW()
		WHERE provider = ? AND delivery_id = ?
			AND (state IN (?, ?) OR (state = ? AND lease_expires_at <= NOW(6)))
	`, storage.WebhookEventPending, payload, receivedAt, provider, deliveryID,
		storage.WebhookEventFailed, storage.WebhookEventCompleted, storage.WebhookEventProcessing)
	if err != nil {
		return false, fmt.Errorf("reopen webhook delivery for redelivery (provider=%s, delivery_id=%s): %w", provider, deliveryID, err)
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

// webhookClaimablePredicate matches exactly the rows a driver would claim: a
// pending row, a retryable row whose retry window has elapsed and is under the
// attempt cap, or a processing row whose lease has expired and is under the
// attempt cap. FindNext and the InboxStats backlog-age query derive from this
// single source so the "ready to claim" definition cannot drift between what a
// driver picks up and what the backlog gauge measures. Bind its placeholders
// with webhookClaimableArgs.
const webhookClaimablePredicate = `(
			state = ?
			OR (state = ? AND (retry_after IS NULL OR retry_after <= NOW()) AND attempts < ?)
			OR (state = ? AND lease_expires_at <= NOW(6) AND attempts < ?)
		)`

// webhookClaimableArgs returns the placeholder bindings for
// webhookClaimablePredicate, in order.
func webhookClaimableArgs() []any {
	return []any{
		storage.WebhookEventPending,
		storage.WebhookEventFailedRetryable, storage.MaxWebhookEventAttempts,
		storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts,
	}
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
		WHERE `+webhookClaimablePredicate+`
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, webhookClaimableArgs()...)

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

// Release re-queues a claimed event as pending and refunds the attempt the
// claim consumed. When this undoes the first claim (attempts == 1), started_at
// is cleared so a later claim re-derives it via COALESCE(started_at, NOW()) —
// otherwise an interrupted first claim would permanently pin started_at to the
// cancelled attempt's time and misreport when processing actually began. The
// started_at reset is ordered before the attempts decrement so the CASE reads
// the pre-decrement value.
func (s *webhookEventStore) Release(ctx context.Context, id int64, leaseToken string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?,
			started_at = CASE WHEN attempts <= 1 THEN NULL ELSE started_at END,
			attempts = GREATEST(attempts - 1, 0), lease_owner = NULL,
			lease_token = NULL, lease_expires_at = NULL, retry_after = NULL, updated_at = NOW()
		WHERE id = ? AND lease_token = ?
	`, storage.WebhookEventPending, id, leaseToken)
	if err != nil {
		return fmt.Errorf("release webhook event %d: %w", id, err)
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

// InboxStats returns a read-only snapshot of the inbox for observability. It
// runs three cheap aggregates rather than one query: heterogeneous aggregates
// (grouped counts, a MIN age over a filtered subset, and a filtered count) do
// not combine into a single index-friendly statement, and each is served by an
// existing index.
func (s *webhookEventStore) InboxStats(ctx context.Context) (*storage.WebhookInboxStats, error) {
	stats := &storage.WebhookInboxStats{CountsByState: make(map[string]int64, len(storage.WebhookEventStatesAll))}
	for _, state := range storage.WebhookEventStatesAll {
		stats.CountsByState[state] = 0
	}

	rows, err := s.db.QueryContext(ctx, `SELECT state, COUNT(*) FROM webhook_events GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("count webhook inbox rows by state: %w", err)
	}
	defer utils.CloseAndLog(rows)
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return nil, fmt.Errorf("scan webhook inbox state count: %w", err)
		}
		stats.CountsByState[state] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate webhook inbox state counts: %w", err)
	}

	// Oldest ready-to-claim row, using the exact predicate a driver claims by
	// (webhookClaimablePredicate) so the backlog gauge and dispatch agree on
	// what is claimable: a cap-exhausted retryable row is not counted (a driver
	// won't take it, so it isn't backlog), and an expired-lease processing row a
	// driver would reclaim is counted (real backlog when its driver crashed).
	// NULL (nothing waiting) scans into a zero age.
	var oldestAgeSeconds sql.NullFloat64
	err = s.db.QueryRowContext(ctx, `
		SELECT TIMESTAMPDIFF(MICROSECOND, MIN(received_at), NOW(6)) / 1e6
		FROM webhook_events
		WHERE `+webhookClaimablePredicate+`
	`, webhookClaimableArgs()...).Scan(&oldestAgeSeconds)
	if err != nil {
		return nil, fmt.Errorf("measure oldest claimable webhook inbox row: %w", err)
	}
	if oldestAgeSeconds.Valid && oldestAgeSeconds.Float64 > 0 {
		stats.OldestClaimableAge = time.Duration(oldestAgeSeconds.Float64 * float64(time.Second))
	}

	err = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM webhook_events
		WHERE state = ? AND lease_expires_at <= NOW(6) AND attempts >= ?
	`, storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts).Scan(&stats.StuckProcessing)
	if err != nil {
		return nil, fmt.Errorf("count stuck processing webhook inbox rows: %w", err)
	}

	return stats, nil
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
