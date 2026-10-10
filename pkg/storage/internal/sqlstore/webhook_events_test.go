//go:build integration

package sqlstore

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// The behavioral suite for WebhookEventStore lives in
// pkg/storage/storagetest and runs against every dialect via parity_test.go.
// The tests here cover only behaviors that require raw SQL or
// database-specific conditions.

// A terminally failed row's coverage depends on its origin: a failed organic
// delivery still covers its head because the operator's GitHub Redeliver
// lever exists for it — synthesizing over it would loop a deterministically
// failing head through a fresh claim budget every pass — while a failed
// synthesized row must not cover, because there is no Redeliver lever for a
// synthesized GUID and re-synthesis is the only recovery path. A dead-lettered
// row covers regardless of origin: the driver proved the head can never
// succeed, so re-synthesis would replay the identical failure every
// reconciler pass. A superseded row never covers regardless of origin: its
// work was discarded on the promise that a covering successor performs it, so
// it cannot itself attest that the head was planned.
func TestWebhookEventStore_HasEventForHeadExcludesFailedStateSynthesized(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	for i, tc := range []struct {
		state       string
		synthesized bool
		covers      bool
	}{
		{storage.WebhookEventPending, false, true},
		{storage.WebhookEventProcessing, false, true},
		{storage.WebhookEventFailedRetryable, false, true},
		{storage.WebhookEventCompleted, false, true},
		{storage.WebhookEventFailed, false, true},
		{storage.WebhookEventFailedPermanent, false, true},
		{storage.WebhookEventSuperseded, false, false},
		{storage.WebhookEventPending, true, true},
		{storage.WebhookEventFailed, true, false},
		{storage.WebhookEventFailedPermanent, true, true},
		{storage.WebhookEventSuperseded, true, false},
	} {
		deliveryID := fmt.Sprintf("delivery-state-%d", i)
		if tc.synthesized {
			deliveryID = storage.SynthesizedWebhookDeliveryIDPrefix + deliveryID
		}
		headSHA := fmt.Sprintf("state-head-%d", i)
		_, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
			DeliveryID:  deliveryID,
			Event:       "pull_request",
			Action:      "synchronize",
			Repository:  "block/example",
			PullRequest: 7,
			HeadSHA:     headSHA,
			Payload:     []byte(`{}`),
		})
		require.NoError(t, err, tc.state)
		_, err = testDB.ExecContext(ctx, `UPDATE webhook_events SET state = ? WHERE delivery_id = ?`, tc.state, deliveryID)
		require.NoError(t, err, tc.state)

		found, err := store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, "block/example", 7, headSHA)
		require.NoError(t, err, tc.state)
		assert.Equal(t, tc.covers, found, "state %s", tc.state)
	}
}

// createCoalescingEvent inserts a delivery for the SupersedeIfCovered tests
// with receivedAgo controlling its position in the (received_at, id) newness
// ordering, then optionally drives it into a non-pending state via SQL.
// leaseExpiresAt is a SQL expression for the successor's lease column; empty
// means NULL.
func createCoalescingEvent(t *testing.T, store storage.Storage, deliveryID, event, action, repo string, pr int, headSHA string, receivedAgo time.Duration, state string, attempts int, leaseExpiresAt string) {
	t.Helper()
	ctx := t.Context()
	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID:  deliveryID,
		Event:       event,
		Action:      action,
		Repository:  repo,
		PullRequest: pr,
		HeadSHA:     headSHA,
		Payload:     []byte(`{}`),
		ReceivedAt:  time.Now().Add(-receivedAgo),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	if state == storage.WebhookEventPending && attempts == 0 && leaseExpiresAt == "" {
		return
	}
	lease := "NULL"
	if leaseExpiresAt != "" {
		lease = leaseExpiresAt
	}
	_, err = testDB.ExecContext(ctx, `
		UPDATE webhook_events SET state = ?, attempts = ?, lease_expires_at = `+lease+`
		WHERE provider = ? AND delivery_id = ?
	`, state, attempts, storage.WebhookProviderGitHub, deliveryID)
	require.NoError(t, err)
}

// claimCoalescingEvent claims the next delivery and asserts it is the one the
// test expects, so supersede assertions never run against the wrong claim.
func claimCoalescingEvent(t *testing.T, store storage.Storage, wantDeliveryID string) *storage.WebhookEvent {
	t.Helper()
	claimed, err := store.WebhookEvents().FindNext(t.Context(), "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, wantDeliveryID, claimed.DeliveryID)
	return claimed
}

// A claimed auto-plan delivery is discarded only when a newer delivery for
// the same PR will perform, is performing, or has performed the work. States
// whose work runs (pending, live or reclaimable processing, retryable under
// the attempt cap, completed) cover; dead ends (terminally failed,
// superseded, cap-exhausted rows) do not — discarding old work on the
// strength of a successor that will never run would lose the PR's plan.
func TestWebhookEventStore_SupersedeIfCoveredSuccessorStates(t *testing.T) {
	for _, tc := range []struct {
		name           string
		event          string
		action         string
		state          string
		attempts       int
		leaseExpiresAt string
		covers         bool
	}{
		{"pending auto-plan covers", "pull_request", "synchronize", storage.WebhookEventPending, 0, "", true},
		{"processing with live lease covers", "pull_request", "synchronize", storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts, "NOW(6) + INTERVAL 1 MINUTE", true},
		{"expired processing under the cap covers", "pull_request", "synchronize", storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts - 1, "NOW(6) - INTERVAL 1 MINUTE", true},
		{"expired processing at the cap does not cover", "pull_request", "synchronize", storage.WebhookEventProcessing, storage.MaxWebhookEventAttempts, "NOW(6) - INTERVAL 1 MINUTE", false},
		{"retryable under the cap covers", "pull_request", "synchronize", storage.WebhookEventFailedRetryable, storage.MaxWebhookEventAttempts - 1, "", true},
		{"retryable at the cap does not cover", "pull_request", "synchronize", storage.WebhookEventFailedRetryable, storage.MaxWebhookEventAttempts, "", false},
		{"completed covers", "pull_request", "synchronize", storage.WebhookEventCompleted, 1, "", true},
		{"terminally failed does not cover", "pull_request", "synchronize", storage.WebhookEventFailed, storage.MaxWebhookEventAttempts, "", false},
		{"superseded does not cover", "pull_request", "synchronize", storage.WebhookEventSuperseded, 1, "", false},
		{"closed covers", "pull_request", "closed", storage.WebhookEventPending, 0, "", true},
		{"non-plan pull_request action does not cover", "pull_request", "labeled", storage.WebhookEventPending, 0, "", false},
		{"non-pull_request event does not cover", "check_run", "created", storage.WebhookEventPending, 0, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := NewMySQL(testDB)

			createCoalescingEvent(t, store, "old", "pull_request", "synchronize", "block/example", 7, "old-head", time.Minute, storage.WebhookEventPending, 0, "")
			createCoalescingEvent(t, store, "new", tc.event, tc.action, "block/example", 7, "new-head", 0, tc.state, tc.attempts, tc.leaseExpiresAt)

			claimed := claimCoalescingEvent(t, store, "old")
			covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, claimed)
			require.NoError(t, err)
			assert.Equal(t, tc.covers, covered, "the advisory probe must agree with the guarded write's predicate")
			superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
			require.NoError(t, err)
			assert.Equal(t, tc.covers, superseded)

			got, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "old")
			require.NoError(t, err)
			require.NotNil(t, got)
			if tc.covers {
				assert.Equal(t, storage.WebhookEventSuperseded, got.State)
				assert.Equal(t, storage.WebhookEventSuperseded, claimed.State, "the claimed struct must reflect the supersede")
				assert.NotNil(t, got.CompletedAt)
			} else {
				assert.Equal(t, storage.WebhookEventProcessing, got.State, "an uncovered claim must stay processing")
			}
		})
	}
}

// A pending delivery created with a not-before time is durable immediately but
// invisible to dispatch until the time passes: a deferred producer — a
// redundant convergence signal that should lose the race to the primary
// delivery — must not have its row claimed early, while a past or unset
// not-before time leaves the row immediately claimable. The backlog gauge
// agrees: a deferred row is not backlog, because no driver would take it yet.
func TestWebhookEventStore_FindNextHonorsPendingNotBefore(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	future := time.Now().Add(time.Hour)
	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "deferred", Event: "check_suite", Payload: []byte(`{}`), RetryAfter: &future,
	})
	require.NoError(t, err)
	require.True(t, inserted)

	deferred, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "deferred")
	require.NoError(t, err)
	require.NotNil(t, deferred)
	require.NotNil(t, deferred.RetryAfter, "the not-before time must be persisted with the row")
	assert.WithinDuration(t, future, *deferred.RetryAfter, 2*time.Second)

	none, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.Nil(t, none, "a pending row with a future not-before time must not be claimable")

	stats, err := store.WebhookEvents().InboxStats(ctx)
	require.NoError(t, err)
	assert.Zero(t, stats.OldestClaimableAge, "a deferred pending row is not claimable backlog")
	assert.Equal(t, int64(1), stats.CountsByState[storage.WebhookEventPending])

	// An older sibling whose not-before time has already elapsed is claimed
	// while the deferred row stays invisible.
	past := time.Now().Add(-time.Hour)
	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "due", Event: "check_suite", Payload: []byte(`{}`), RetryAfter: &past,
	})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, "due", claimed.DeliveryID, "an elapsed not-before time leaves the row claimable")

	none, err = store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.Nil(t, none, "the deferred row must stay invisible while its not-before time is in the future")

	// Once the not-before time elapses, the deferred row becomes ordinary
	// pending backlog and is claimed like any other row.
	_, err = testDB.ExecContext(ctx, `
		UPDATE webhook_events SET retry_after = NOW() - INTERVAL 1 SECOND
		WHERE provider = ? AND delivery_id = ?
	`, storage.WebhookProviderGitHub, "deferred")
	require.NoError(t, err)

	ready, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, ready)
	assert.Equal(t, "deferred", ready.DeliveryID, "an elapsed not-before time makes the deferred row claimable")
	assert.Equal(t, storage.WebhookEventProcessing, ready.State)
	assert.Nil(t, ready.RetryAfter, "the claim consumes the not-before time")

	persisted, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "deferred")
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Nil(t, persisted.RetryAfter, "the claim consumes the persisted not-before time, not just the returned mirror")
}

// A retryable failure's retry window is advisory, not structural: the claim
// predicate treats a missing window as immediately due, so a retryable row
// whose retry_after was cleared by a manual operator repair is claimed like
// any other due row rather than staying invisible forever. The lifecycle
// never produces this shape itself — MarkFailed persists the state and the
// window together — so raw SQL is required to set it up.
func TestWebhookEventStore_FindNextClaimsRetryableRowWithoutRetryWindow(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "repaired", Event: "check_suite", Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	future := time.Now().Add(time.Hour)
	require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "temporary failure", &future))

	none, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.Nil(t, none, "a retryable row with an open retry window is not claimable")

	_, err = testDB.ExecContext(ctx, `
		UPDATE webhook_events SET retry_after = NULL
		WHERE provider = ? AND delivery_id = ?
	`, storage.WebhookProviderGitHub, "repaired")
	require.NoError(t, err)

	reclaimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed, "a retryable row without a retry window is immediately due")
	assert.Equal(t, "repaired", reclaimed.DeliveryID)
	assert.Equal(t, 2, reclaimed.Attempts)
}

// A delivery's claimability must not depend on its payload width. Ordering the
// claimable set must keep the payload out of the sort, so a single delivery
// whose payload exceeds the server's sort buffer is claimed like any other row
// instead of failing every claim attempt and wedging the inbox behind it.
func TestWebhookEventStore_FindNextClaimsPayloadWiderThanSortBuffer(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	var sortBufferSize int
	require.NoError(t, testDB.QueryRowContext(ctx, `SELECT @@sort_buffer_size`).Scan(&sortBufferSize))

	payload := fmt.Sprintf(`{"data":%q}`, strings.Repeat("a", 2*sortBufferSize))
	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "delivery-wide",
		Event:      "push",
		Repository: "block/example",
		Payload:    []byte(payload),
	})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, "delivery-wide", claimed.DeliveryID)
	assert.Equal(t, storage.WebhookEventProcessing, claimed.State)
	assert.Equal(t, 1, claimed.Attempts)
	assert.Equal(t, "driver-a", claimed.LeaseOwner)
	assert.JSONEq(t, payload, string(claimed.Payload))
}

func TestWebhookEventStore_CreateReopensStuckProcessingDelivery(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

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

func TestWebhookEventStore_TerminalWritesReturnNotFoundAfterDelete(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "delivery-1", Event: "pull_request", Repository: "block/example", PullRequest: 7, Payload: []byte(`{}`),
	})
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
	require.ErrorIs(t, store.WebhookEvents().Release(ctx, claimed.ID, claimed.LeaseToken), storage.ErrWebhookEventNotFound)
	require.ErrorIs(t, store.WebhookEvents().MarkFailedPermanent(ctx, claimed.ID, claimed.LeaseToken, "failed"), storage.ErrWebhookEventNotFound)
	_, err = store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
	require.ErrorIs(t, err, storage.ErrWebhookEventNotFound)
}

func TestWebhookEventStore_HeartbeatTreatsUnchangedMatchingLeaseAsSuccess(t *testing.T) {
	clearTables(t)
	ctx := t.Context()

	db, err := sql.Open("block-mysql", testDSNChangedRows)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := NewMySQL(db)
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

	db, err := sql.Open("block-mysql", testDSNChangedRows)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := NewMySQL(db)
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

// The backlog-age gauge must count exactly the rows a driver would claim: a
// retryable row past the attempt cap is not backlog (FindNext skips it forever),
// while an expired-lease processing row under the cap is (a driver reclaims it).
func TestWebhookEventStore_InboxStatsBacklogMatchesClaimable(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

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

// The webhook driver claim query orders candidates by (created_at, id) while
// its eligibility filter ORs across several states, so no state-prefixed index
// can serve the ordering. Without a dedicated index on the ordering pair the
// planner sorts the full candidate set — and on InnoDB that sort runs under
// FOR UPDATE, locking every claimable row before LIMIT 1 applies and turning
// SKIP LOCKED into a serializer that makes drivers contend instead of claiming
// distinct rows in parallel.
//
// This asserts the index as the embedded MySQL schema file declares it, on the
// MySQL store this package's tests run against, under the name MySQL's
// table-scoped naming gives it. The PostgreSQL counterpart carries a different
// name because its index names are schema-wide; the schema parity tests pin it
// by shape rather than by name.
func TestWebhookEventClaimOrderingIsIndexed(t *testing.T) {
	ctx := t.Context()

	rows, err := testDB.QueryContext(ctx, `
		SELECT COLUMN_NAME FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'webhook_events' AND INDEX_NAME = 'idx_created_id'
		ORDER BY SEQ_IN_INDEX`)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

	var indexColumns []string
	for rows.Next() {
		var column string
		require.NoError(t, rows.Scan(&column))
		indexColumns = append(indexColumns, column)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []string{"created_at", "id"}, indexColumns,
		"the claim ordering needs an index on exactly its ordering pair, in that order")
}
