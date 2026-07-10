//go:build integration

package mysqlstore

import (
	"database/sql"
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
		DeliveryID:     "delivery-1",
		Event:          "pull_request",
		Action:         "opened",
		Repository:     "block/example",
		PullRequest:    123,
		HeadSHA:        "abc123",
		InstallationID: 456,
		Payload:        []byte(`{"action":"opened"}`),
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

	got, err := store.WebhookEvents().GetByDeliveryID(ctx, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, storage.WebhookEventPending, got.State)
	assert.Equal(t, "block/example", got.Repository)
	assert.Equal(t, 123, got.PullRequest)
	assert.JSONEq(t, `{"action":"opened"}`, string(got.Payload))
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

	claimed, err := store.WebhookEvents().FindNext(ctx, "worker-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, "delivery-1", claimed.DeliveryID)
	assert.Equal(t, storage.WebhookEventProcessing, claimed.State)
	assert.Equal(t, 1, claimed.Attempts)
	assert.Equal(t, "worker-a", claimed.LeaseOwner)
	assert.NotEmpty(t, claimed.LeaseToken)

	next, err := store.WebhookEvents().FindNext(ctx, "worker-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, next)
	assert.Equal(t, "delivery-2", next.DeliveryID)
}

func TestWebhookEventStore_FindNextSkipsFreshLeaseAndReclaimsExpiredLease(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "worker-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	none, err := store.WebhookEvents().FindNext(ctx, "worker-b", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, none)

	_, err = testDB.ExecContext(ctx, `UPDATE webhook_events SET lease_expires_at = NOW() - INTERVAL 1 SECOND WHERE id = ?`, claimed.ID)
	require.NoError(t, err)

	reclaimed, err := store.WebhookEvents().FindNext(ctx, "worker-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	assert.Equal(t, claimed.ID, reclaimed.ID)
	assert.Equal(t, 2, reclaimed.Attempts)
	assert.Equal(t, "worker-b", reclaimed.LeaseOwner)
	assert.NotEqual(t, claimed.LeaseToken, reclaimed.LeaseToken)
}

func TestWebhookEventStore_MarkFailedRetryableAndCompleted(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "worker-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	retryAfter := time.Now().Add(-time.Minute)
	require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "temporary failure", &retryAfter))

	retryable, err := store.WebhookEvents().GetByDeliveryID(ctx, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, retryable)
	assert.Equal(t, storage.WebhookEventFailedRetryable, retryable.State)
	assert.Equal(t, "temporary failure", retryable.LastError)
	assert.NotNil(t, retryable.RetryAfter)
	assert.Nil(t, retryable.CompletedAt)

	reclaimed, err := store.WebhookEvents().FindNext(ctx, "worker-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, reclaimed.ID, reclaimed.LeaseToken))

	completed, err := store.WebhookEvents().GetByDeliveryID(ctx, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, storage.WebhookEventCompleted, completed.State)
	assert.NotNil(t, completed.CompletedAt)
	assert.Empty(t, completed.LeaseToken)
}

func TestWebhookEventStore_LeaseTokenGuardsWrites(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "worker-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	require.ErrorIs(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, "stale-token", time.Minute), storage.ErrWebhookEventLeaseLost)
	require.ErrorIs(t, store.WebhookEvents().MarkCompleted(ctx, claimed.ID, "stale-token"), storage.ErrWebhookEventLeaseLost)
	require.NoError(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Minute))
	require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))
}

func TestWebhookEventStore_HeartbeatTreatsUnchangedMatchingLeaseAsSuccess(t *testing.T) {
	clearTables(t)
	ctx := t.Context()

	db, err := sql.Open("mysql", testDSNChangedRows)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := New(db)
	_, err = db.ExecContext(ctx, `SET timestamp = 1700000000`)
	require.NoError(t, err)
	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "worker-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	// With production-style changed-rows semantics and a pinned NOW(), this
	// heartbeat matches the row but writes the same second-precision values.
	// It must still count as a live lease, not a lost lease.
	require.NoError(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Minute))
}
