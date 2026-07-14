//go:build integration

package mysqlstore

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

func TestWebhookEventStore_CreateDeduplicatesDeliveryID(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	event := &storage.WebhookEvent{
		DeliveryID:  "delivery-1",
		Event:       "pull_request",
		Action:      "opened",
		Repository:  "block/example",
		PullRequest: 123,
		HeadSHA:     "abc123",
		TenantID:    "456",
		Payload:     []byte(`{"action":"opened"}`),
	}
	inserted, err := store.WebhookEvents().Create(ctx, event)
	require.NoError(t, err)
	require.True(t, inserted)
	require.NotZero(t, event.ID)

	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "delivery-1",
		Event:      "pull_request",
		Payload:    []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, inserted)

	got, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, storage.WebhookProviderGitHub, got.Provider)
	assert.Equal(t, storage.WebhookEventPending, got.State)
	assert.Equal(t, "block/example", got.Repository)
	assert.Equal(t, 123, got.PullRequest)
	assert.Equal(t, "456", got.TenantID)
	assert.JSONEq(t, `{"action":"opened"}`, string(got.Payload))
}

func TestWebhookEventStore_CreateDeduplicatesByProviderAndDeliveryID(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)
	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.False(t, inserted)
	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{Provider: "gitlab", DeliveryID: "delivery-1", Event: "merge_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)
}

func TestWebhookEventStore_FindNextClaimsOldestPendingEvent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)
	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-2", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, "delivery-1", claimed.DeliveryID)
	assert.Equal(t, storage.WebhookEventProcessing, claimed.State)
	assert.Equal(t, 1, claimed.Attempts)
	assert.Equal(t, "driver-a", claimed.LeaseOwner)
	assert.NotEmpty(t, claimed.LeaseToken)

	next, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, next)
	assert.Equal(t, "delivery-2", next.DeliveryID)
}

func TestWebhookEventStore_FindNextRequiresOwnerWithoutLeaseLostSentinel(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	claimed, err := store.WebhookEvents().FindNext(ctx, "", time.Minute)
	require.Nil(t, claimed)
	require.Error(t, err)
	assert.False(t, errors.Is(err, storage.ErrWebhookEventLeaseLost))
}

func TestWebhookEventStore_FindNextSkipsFreshLeaseAndReclaimsExpiredLease(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	none, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, none)

	_, err = testDB.ExecContext(ctx, `UPDATE webhook_events SET lease_expires_at = NOW() - INTERVAL 1 SECOND WHERE id = ?`, claimed.ID)
	require.NoError(t, err)

	reclaimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	assert.Equal(t, claimed.ID, reclaimed.ID)
	assert.Equal(t, 2, reclaimed.Attempts)
	assert.Equal(t, "driver-b", reclaimed.LeaseOwner)
	assert.NotEqual(t, claimed.LeaseToken, reclaimed.LeaseToken)
}

func TestWebhookEventStore_MarkFailedRetryableAndCompleted(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	retryAfter := time.Now().Add(-time.Minute)
	require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "temporary failure", &retryAfter))

	retryable, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, retryable)
	assert.Equal(t, storage.WebhookEventFailedRetryable, retryable.State)
	assert.Equal(t, "temporary failure", retryable.LastError)
	assert.NotNil(t, retryable.RetryAfter)
	assert.Nil(t, retryable.CompletedAt)

	reclaimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, reclaimed.ID, reclaimed.LeaseToken))

	completed, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, storage.WebhookEventCompleted, completed.State)
	assert.NotNil(t, completed.CompletedAt)
	// The lease token is retained on terminal rows so a committed-but-unacked
	// retry of MarkCompleted stays idempotent rather than reporting a lost lease.
	assert.Equal(t, reclaimed.LeaseToken, completed.LeaseToken)
}

func TestWebhookEventStore_LeaseTokenGuardsWrites(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	require.ErrorIs(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, "stale-token", time.Minute), storage.ErrWebhookEventLeaseLost)
	require.ErrorIs(t, store.WebhookEvents().MarkCompleted(ctx, claimed.ID, "stale-token"), storage.ErrWebhookEventLeaseLost)
	require.NoError(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Minute))
	require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))
}

func TestWebhookEventStore_CreateReopensTerminalDelivery(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{"attempt":1}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "permanent failure", nil))

	// GitHub "Redeliver" reuses the original delivery GUID; it must re-open a
	// terminally failed row instead of being deduplicated into a no-op.
	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{"attempt":2}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	reopened, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, reopened)
	assert.Equal(t, storage.WebhookEventPending, reopened.State)
	assert.Equal(t, 0, reopened.Attempts)
	assert.Empty(t, reopened.LeaseToken)
	assert.Nil(t, reopened.CompletedAt)
	assert.Nil(t, reopened.StartedAt)
	assert.JSONEq(t, `{"attempt":2}`, string(reopened.Payload))

	reclaimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, reclaimed.ID, reclaimed.LeaseToken))

	// A completed row is also re-opened on redelivery: completion is recorded
	// before the detached plan goroutines have durably persisted their work, so
	// a crash in that window can lose the auto-plan while leaving the row
	// completed. Re-running auto-plan on the same head SHA is idempotent, so an
	// operator Redeliver is a safe recovery lever rather than a permanent no-op.
	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{"attempt":3}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	reopenedAgain, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, reopenedAgain)
	assert.Equal(t, storage.WebhookEventPending, reopenedAgain.State)
	assert.Equal(t, 0, reopenedAgain.Attempts)
	assert.Nil(t, reopenedAgain.CompletedAt)
	assert.JSONEq(t, `{"attempt":3}`, string(reopenedAgain.Payload))
}

func TestWebhookEventStore_ReleaseRefundsAttemptAndRequeues(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, 1, claimed.Attempts)

	require.ErrorIs(t, store.WebhookEvents().Release(ctx, claimed.ID, "stale-token"), storage.ErrWebhookEventLeaseLost)
	require.NoError(t, store.WebhookEvents().Release(ctx, claimed.ID, claimed.LeaseToken))

	released, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, released)
	assert.Equal(t, storage.WebhookEventPending, released.State)
	assert.Equal(t, 0, released.Attempts, "release must refund the attempt consumed by the claim")
	assert.Empty(t, released.LeaseToken)
	assert.Nil(t, released.StartedAt, "releasing the first claim must clear started_at so it re-derives on reclaim")

	reclaimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	assert.Equal(t, 1, reclaimed.Attempts)
	require.NotNil(t, reclaimed.StartedAt, "reclaim must set started_at to the actual processing start")
}

// A release that undoes a later claim (attempts > 1) must keep started_at,
// which records when the earliest attempt began processing.
func TestWebhookEventStore_ReleaseKeepsStartedAtAfterFirstAttempt(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	// First claim + retryable failure so the row is claimable again.
	first, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NotNil(t, first.StartedAt)
	firstStartedAt := *first.StartedAt
	retryAfter := time.Now().Add(-time.Minute)
	require.NoError(t, store.WebhookEvents().MarkFailed(ctx, first.ID, first.LeaseToken, "boom", &retryAfter))

	// Second claim (attempts == 2), then release it.
	second, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, 2, second.Attempts)
	require.NoError(t, store.WebhookEvents().Release(ctx, second.ID, second.LeaseToken))

	released, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, released)
	assert.Equal(t, 1, released.Attempts, "release must refund only the second attempt")
	require.NotNil(t, released.StartedAt, "releasing a later claim must preserve the original started_at")
	assert.WithinDuration(t, firstStartedAt, *released.StartedAt, time.Second)
}

func TestWebhookEventStore_TerminalWritesReturnNotFoundAfterDelete(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	_, err = testDB.ExecContext(ctx, `DELETE FROM webhook_events WHERE id = ?`, claimed.ID)
	require.NoError(t, err)

	require.ErrorIs(t, store.WebhookEvents().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken), storage.ErrWebhookEventNotFound)
	require.ErrorIs(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "failed", nil), storage.ErrWebhookEventNotFound)
	require.ErrorIs(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Minute), storage.ErrWebhookEventNotFound)
}

func TestWebhookEventStore_CreateRejectsNonPendingState(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// A row created directly in "processing" would have NULL lease columns:
	// never claimable, never expiring, yet deduplicating every future
	// redelivery. Create must only ever persist pending rows.
	for _, state := range []string{
		storage.WebhookEventProcessing,
		storage.WebhookEventCompleted,
		storage.WebhookEventFailedRetryable,
		storage.WebhookEventFailed,
		"bogus",
	} {
		inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
			DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`), State: state,
		})
		require.ErrorContains(t, err, "must be pending", "state %q", state)
		require.False(t, inserted)
	}

	stored, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.Nil(t, stored)
}

func TestWebhookEventStore_FindNextStopsReclaimingAtAttemptsCeiling(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	// A poison event that hard-kills drivers before MarkFailed leaves an
	// expired-lease processing row. Once attempts reaches the ceiling it must
	// stop being reclaimed instead of blocking the queue forever.
	_, err = testDB.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, attempts = ?, lease_owner = 'driver-a', lease_token = 'token-a',
			lease_expires_at = DATE_SUB(NOW(6), INTERVAL 1 HOUR)
		WHERE delivery_id = 'delivery-1'
	`, storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts)
	require.NoError(t, err)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.Nil(t, claimed, "expired-lease row at the attempts ceiling must not be reclaimed")

	// Same ceiling applies to retryable failures.
	_, err = testDB.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, retry_after = NULL, lease_expires_at = NULL
		WHERE delivery_id = 'delivery-1'
	`, storage.WebhookEventFailedRetryable)
	require.NoError(t, err)

	claimed, err = store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.Nil(t, claimed, "retryable row at the attempts ceiling must not be reclaimed")

	// One attempt below the ceiling is still claimable.
	_, err = testDB.ExecContext(ctx, `
		UPDATE webhook_events SET attempts = ? WHERE delivery_id = 'delivery-1'
	`, storage.MaxWebhookEventAttempts-1)
	require.NoError(t, err)

	claimed, err = store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, storage.MaxWebhookEventAttempts, claimed.Attempts)
}

func TestWebhookEventStore_HeartbeatTreatsUnchangedMatchingLeaseAsSuccess(t *testing.T) {
	clearTables(t)
	ctx := t.Context()

	db, err := sql.Open("mysql", testDSNChangedRows)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := New(db)
	_, err = db.ExecContext(ctx, `SET timestamp = 1700000000`)
	require.NoError(t, err)
	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	// With production-style changed-rows semantics and a pinned NOW(), this
	// heartbeat matches the row but writes the same second-precision values.
	// It must still count as a live lease, not a lost lease.
	require.NoError(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Minute))
}

// A committed-but-unacknowledged terminal write, retried within the same
// DATETIME second under production changed-rows semantics, must remain an
// idempotent success rather than misreporting the driver's own completion as a
// lost lease.
func TestWebhookEventStore_TerminalWritesAreIdempotentOnRetry(t *testing.T) {
	clearTables(t)
	ctx := t.Context()

	db, err := sql.Open("mysql", testDSNChangedRows)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := New(db)
	_, err = db.ExecContext(ctx, `SET timestamp = 1700000000`)
	require.NoError(t, err)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-complete", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)
	completeClaim, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, completeClaim)
	require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, completeClaim.ID, completeClaim.LeaseToken))
	require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, completeClaim.ID, completeClaim.LeaseToken))

	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-fail", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)
	failClaim, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, failClaim)
	require.NoError(t, store.WebhookEvents().MarkFailed(ctx, failClaim.ID, failClaim.LeaseToken, "boom", nil))
	require.NoError(t, store.WebhookEvents().MarkFailed(ctx, failClaim.ID, failClaim.LeaseToken, "boom", nil))
}

// A sub-second lease must not collapse to an already-expired lease: a second
// driver must not immediately reclaim an event the first driver is processing.
func TestWebhookEventStore_SubSecondLeaseIsNotImmediatelyReclaimable(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", 200*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	none, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, none)
}
