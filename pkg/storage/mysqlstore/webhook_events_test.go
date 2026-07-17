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

func TestWebhookEventStore_HasEventForHead(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	_, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID:  "delivery-head-1",
		Event:       "pull_request",
		Action:      "synchronize",
		Repository:  "block/example",
		PullRequest: 7,
		HeadSHA:     "head-sha-1",
		Payload:     []byte(`{}`),
	})
	require.NoError(t, err)

	found, err := store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, "block/example", 7, "head-sha-1")
	require.NoError(t, err)
	assert.True(t, found)

	// Provider defaults to GitHub.
	found, err = store.WebhookEvents().HasEventForHead(ctx, "", "block/example", 7, "head-sha-1")
	require.NoError(t, err)
	assert.True(t, found)

	for _, tc := range []struct {
		name    string
		repo    string
		pr      int
		headSHA string
	}{
		{"different head SHA", "block/example", 7, "head-sha-2"},
		{"different PR", "block/example", 8, "head-sha-1"},
		{"different repo", "block/other", 7, "head-sha-1"},
	} {
		found, err = store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, tc.repo, tc.pr, tc.headSHA)
		require.NoError(t, err, tc.name)
		assert.False(t, found, tc.name)
	}

	_, err = store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, "", 7, "head-sha-1")
	require.Error(t, err, "missing repository must be rejected")
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

func TestWebhookEventStore_CreateReopensStuckProcessingDelivery(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{"attempt":1}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	// A driver hard-killed on its final attempt leaves the row in processing
	// with an expired lease at the attempts ceiling. FindNext never reclaims it,
	// so absent the reconciler's periodic sweep an operator Redeliver is the
	// only immediate recovery lever — which is the path this exercises.
	_, err = testDB.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, attempts = ?, lease_owner = 'driver-a', lease_token = 'token-a',
			lease_expires_at = DATE_SUB(NOW(6), INTERVAL 1 HOUR), started_at = NOW(6)
		WHERE delivery_id = 'delivery-1'
	`, storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts)
	require.NoError(t, err)

	stuck, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.Nil(t, stuck, "cap-exhausted processing row must not be reclaimable by FindNext")

	// Redeliver reuses the delivery GUID; it must re-open the wedged row.
	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{"attempt":2}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	reopened, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, reopened)
	assert.Equal(t, storage.WebhookEventPending, reopened.State)
	assert.Equal(t, 0, reopened.Attempts)
	assert.Empty(t, reopened.LeaseToken)
	assert.Nil(t, reopened.StartedAt)
	assert.JSONEq(t, `{"attempt":2}`, string(reopened.Payload))

	reclaimed, err := store.WebhookEvents().FindNext(ctx, "driver-c", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed, "reopened row must be claimable again")
	assert.Equal(t, 1, reclaimed.Attempts)
}

func TestWebhookEventStore_CreateDedupesProcessingWithLiveLease(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{"attempt":1}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	// A live driver owns the row (unexpired lease), so it is genuinely in
	// flight — a duplicate delivery must dedup rather than yank it away.
	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{"attempt":2}`)})
	require.NoError(t, err)
	require.False(t, inserted, "a processing row with a live lease must dedup, not reopen")

	current, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, storage.WebhookEventProcessing, current.State)
	assert.Equal(t, claimed.LeaseToken, current.LeaseToken, "the live driver's lease must be untouched")
	assert.JSONEq(t, `{"attempt":1}`, string(current.Payload), "payload must not be overwritten while in flight")
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

// The backlog-age gauge must count exactly the rows a driver would claim: a
// retryable row past the attempt cap is not backlog (FindNext skips it forever),
// while an expired-lease processing row under the cap is (a driver reclaims it).
func TestWebhookEventStore_InboxStatsBacklogMatchesClaimable(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// A cap-exhausted retryable row whose retry window has long elapsed. FindNext
	// never reclaims it, so it must not inflate the backlog age.
	capExhausted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "cap-exhausted", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, capExhausted)
	_, err = testDB.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, attempts = ?, retry_after = NOW() - INTERVAL 1 HOUR,
			received_at = NOW(6) - INTERVAL 600 SECOND
		WHERE provider = ? AND delivery_id = ?
	`, storage.WebhookEventFailedRetryable, storage.MaxWebhookEventAttempts, storage.WebhookProviderGitHub, "cap-exhausted")
	require.NoError(t, err)

	onlyExhausted, err := store.WebhookEvents().InboxStats(ctx)
	require.NoError(t, err)
	assert.Zero(t, onlyExhausted.OldestClaimableAge, "a cap-exhausted retryable row must not count as backlog")

	// An expired-lease processing row under the cap is genuinely reclaimable
	// backlog: its driver crashed and another will pick it up.
	reclaimable, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "reclaimable", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, reclaimable)
	_, err = testDB.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, attempts = ?, lease_owner = 'driver', lease_token = 'tok',
			lease_expires_at = NOW(6) - INTERVAL 1 SECOND,
			received_at = NOW(6) - INTERVAL 200 SECOND
		WHERE provider = ? AND delivery_id = ?
	`, storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts-1, storage.WebhookProviderGitHub, "reclaimable")
	require.NoError(t, err)

	stats, err := store.WebhookEvents().InboxStats(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, stats.OldestClaimableAge, 190*time.Second, "an expired-lease processing row under the cap is claimable backlog")
	assert.Less(t, stats.OldestClaimableAge, 300*time.Second, "the 600s cap-exhausted retryable row must not drive the age")

	// FindNext agrees with the gauge: it reclaims the processing row and never
	// the cap-exhausted retryable one.
	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, "reclaimable", claimed.DeliveryID)
}

// InboxStats gives operators a steady-state view of the durable inbox: how many
// rows sit in each state, how long the oldest ready-to-claim delivery has waited
// (backlog latency), and how many are wedged in processing past the attempt cap.
func TestWebhookEventStore_InboxStats(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Empty inbox: every state present at zero, no backlog, nothing stuck.
	empty, err := store.WebhookEvents().InboxStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, empty)
	assert.Equal(t, int64(0), empty.CountsByState[storage.WebhookEventPending])
	assert.Equal(t, int64(0), empty.CountsByState[storage.WebhookEventProcessing])
	assert.Zero(t, empty.OldestClaimableAge)
	assert.Equal(t, int64(0), empty.StuckProcessing)

	// Two pending rows; the older one drives the backlog age.
	insertPending := func(deliveryID string, receivedAgo time.Duration) {
		t.Helper()
		inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: deliveryID, Event: "pull_request", Payload: []byte(`{}`)})
		require.NoError(t, err)
		require.True(t, inserted)
		_, err = testDB.ExecContext(ctx, `UPDATE webhook_events SET received_at = NOW(6) - INTERVAL ? SECOND WHERE provider = ? AND delivery_id = ?`,
			int(receivedAgo.Seconds()), storage.WebhookProviderGitHub, deliveryID)
		require.NoError(t, err)
	}
	insertPending("pending-old", 120*time.Second)
	insertPending("pending-new", 5*time.Second)

	// A completed row: counts toward completed, never toward backlog. Set its
	// state directly so the FIFO claim order can't reassign it to another row.
	completedDelivery, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "completed-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, completedDelivery)
	_, err = testDB.ExecContext(ctx, `UPDATE webhook_events SET state = ?, completed_at = NOW() WHERE provider = ? AND delivery_id = ?`,
		storage.WebhookEventCompleted, storage.WebhookProviderGitHub, "completed-1")
	require.NoError(t, err)

	// A stuck processing row: at the attempt cap with an expired lease.
	stuck, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "stuck-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, stuck)
	_, err = testDB.ExecContext(ctx, `
		UPDATE webhook_events
		SET state = ?, attempts = ?, lease_owner = 'driver', lease_token = 'tok',
			lease_expires_at = NOW(6) - INTERVAL 1 SECOND
		WHERE provider = ? AND delivery_id = ?
	`, storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts, storage.WebhookProviderGitHub, "stuck-1")
	require.NoError(t, err)

	stats, err := store.WebhookEvents().InboxStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(2), stats.CountsByState[storage.WebhookEventPending])
	assert.Equal(t, int64(1), stats.CountsByState[storage.WebhookEventProcessing])
	assert.Equal(t, int64(1), stats.CountsByState[storage.WebhookEventCompleted])
	assert.Equal(t, int64(1), stats.StuckProcessing)
	// The oldest pending row is ~120s old; allow slack for execution time.
	assert.GreaterOrEqual(t, stats.OldestClaimableAge, 110*time.Second)
	assert.Less(t, stats.OldestClaimableAge, 5*time.Minute)
}

// TerminateStuckProcessing sweeps out rows a hard-killed driver left parked in
// processing at the attempt cap with an expired lease — FindNext never reclaims
// those, so the reconciler must terminalize them. Rows below the cap, or whose
// lease is still fresh, must be left alone.
func TestWebhookEventStore_TerminateStuckProcessing(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Seed three processing rows, then drive each into the exact state under
	// test via SQL so the assertions don't depend on FindNext claim ordering.
	setProcessing := func(deliveryID string, attempts int, leaseExpiresAt string) {
		t.Helper()
		inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: deliveryID, Event: "pull_request", Payload: []byte(`{}`)})
		require.NoError(t, err)
		require.True(t, inserted)
		_, err = testDB.ExecContext(ctx, `
			UPDATE webhook_events
			SET state = ?, attempts = ?, lease_owner = 'driver', lease_token = ?,
				lease_expires_at = `+leaseExpiresAt+`
			WHERE provider = ? AND delivery_id = ?
		`, storage.WebhookEventProcessing, attempts, deliveryID, storage.WebhookProviderGitHub, deliveryID)
		require.NoError(t, err)
	}

	// stuck: at the attempt cap, lease expired -> terminated by the sweep.
	setProcessing("stuck", storage.MaxWebhookEventAttempts, "NOW(6) - INTERVAL 1 SECOND")
	// belowCap: expired lease but under the cap -> FindNext reclaims it, not the sweep.
	setProcessing("below-cap", storage.MaxWebhookEventAttempts-1, "NOW(6) - INTERVAL 1 SECOND")
	// fresh: at the cap but lease still valid -> a driver is still working it.
	setProcessing("fresh", storage.MaxWebhookEventAttempts, "NOW(6) + INTERVAL 1 MINUTE")

	terminated, err := store.WebhookEvents().TerminateStuckProcessing(ctx, "reconciler sweep")
	require.NoError(t, err)
	assert.Equal(t, int64(1), terminated, "only the cap-exhausted expired-lease row should be terminated")

	got, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "stuck")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, storage.WebhookEventFailed, got.State)
	assert.Equal(t, "reconciler sweep", got.LastError)
	assert.Empty(t, got.LeaseToken)
	assert.Nil(t, got.LeaseExpiresAt)
	assert.NotNil(t, got.CompletedAt)

	belowGot, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "below-cap")
	require.NoError(t, err)
	require.NotNil(t, belowGot)
	assert.Equal(t, storage.WebhookEventProcessing, belowGot.State)

	freshGot, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "fresh")
	require.NoError(t, err)
	require.NotNil(t, freshGot)
	assert.Equal(t, storage.WebhookEventProcessing, freshGot.State)

	// A terminated stuck row is redeliverable: reusing its GUID re-opens it.
	reopened, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "stuck", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, reopened)
}
