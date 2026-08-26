//go:build integration

package sqlstore

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// The behavioral suite for WebhookEventStore lives in
// pkg/storage/storagetest and runs against every dialect via parity_test.go.
// The tests here cover only behaviors that require raw SQL, database-specific
// conditions, or methods not yet represented in the cross-dialect suite.

func TestWebhookEventStore_HasEventForHead(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

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

// Only auto-plannable pull_request rows cover a head. Rows of other event
// types or actions can carry the same PR + head SHA without planning it — a
// pull_request.closed row from before a reopen, or a check_run row — so they
// must not suppress the reconciler's recovery of a lost auto-plan delivery.
func TestWebhookEventStore_HasEventForHeadRequiresAutoPlanPullRequestRow(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	for i, tc := range []struct {
		name   string
		event  string
		action string
		covers bool
	}{
		{"opened plans the head", "pull_request", "opened", true},
		{"synchronize plans the head", "pull_request", "synchronize", true},
		{"reopened plans the head", "pull_request", "reopened", true},
		{"closed does not plan the head", "pull_request", "closed", false},
		{"check_run does not plan the head", "check_run", "rerequested", false},
		{"issue_comment does not plan the head", "issue_comment", "created", false},
	} {
		deliveryID := fmt.Sprintf("delivery-event-%d", i)
		headSHA := fmt.Sprintf("event-head-%d", i)
		_, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
			DeliveryID:  deliveryID,
			Event:       tc.event,
			Action:      tc.action,
			Repository:  "block/example",
			PullRequest: 7,
			HeadSHA:     headSHA,
			Payload:     []byte(`{}`),
		})
		require.NoError(t, err, tc.name)

		found, err := store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, "block/example", 7, headSHA)
		require.NoError(t, err, tc.name)
		assert.Equal(t, tc.covers, found, tc.name)
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

// Coalescing is keyed by (provider, repository, pull request): deliveries for
// a different PR, repository, or provider never cover, and two PRs sharing
// the same head SHA remain independent. A newer delivery for the same PR
// covers even when it targets a different head — the PR's newest head is the
// only one worth planning.
func TestWebhookEventStore_SupersedeIfCoveredScopesToPullRequest(t *testing.T) {
	for _, tc := range []struct {
		name    string
		repo    string
		pr      int
		headSHA string
		covers  bool
	}{
		{"different PR does not cover", "block/example", 8, "old-head", false},
		{"different repository does not cover", "block/other", 7, "old-head", false},
		{"same PR with a different head covers", "block/example", 7, "other-head", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := NewMySQL(testDB)

			createCoalescingEvent(t, store, "old", "pull_request", "synchronize", "block/example", 7, "old-head", time.Minute, storage.WebhookEventPending, 0, "")
			createCoalescingEvent(t, store, "new", "pull_request", "synchronize", tc.repo, tc.pr, tc.headSHA, 0, storage.WebhookEventPending, 0, "")

			claimed := claimCoalescingEvent(t, store, "old")
			covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, claimed)
			require.NoError(t, err)
			assert.Equal(t, tc.covers, covered, "the advisory probe must agree with the guarded write's predicate")
			superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
			require.NoError(t, err)
			assert.Equal(t, tc.covers, superseded)
		})
	}
}

// Newness is a strictly greater received_at: deliveries received in the same
// instant never cover each other, regardless of insertion order, so two
// simultaneous deliveries both process instead of one being discarded on an
// ordering guess.
func TestWebhookEventStore_SupersedeIfCoveredIgnoresEqualTimestamps(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	// second (the later insert, in a covering pending state) shares
	// received_at with first — it must not count as newer.
	createCoalescingEvent(t, store, "first", "pull_request", "synchronize", "block/example", 7, "head-1", 0, storage.WebhookEventPending, 0, "")
	createCoalescingEvent(t, store, "second", "pull_request", "synchronize", "block/example", 7, "head-2", 0, storage.WebhookEventPending, 0, "")
	_, err := testDB.ExecContext(ctx, `
		UPDATE webhook_events SET received_at = '2024-01-01 00:00:00'
		WHERE provider = ? AND delivery_id IN ('first', 'second')
	`, storage.WebhookProviderGitHub)
	require.NoError(t, err)

	claimed := claimCoalescingEvent(t, store, "first")
	covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, claimed)
	require.NoError(t, err)
	assert.False(t, covered, "an equal-timestamp delivery must not count as a covering successor")
	superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
	require.NoError(t, err)
	assert.False(t, superseded, "an equal-timestamp delivery must not cover")

	claimedSecond := claimCoalescingEvent(t, store, "second")
	covered, err = store.WebhookEvents().HasCoveringSuccessor(ctx, claimedSecond)
	require.NoError(t, err)
	assert.False(t, covered, "equal-timestamp deliveries must not count as covering in either direction")
	superseded, err = store.WebhookEvents().SupersedeIfCovered(ctx, claimedSecond)
	require.NoError(t, err)
	assert.False(t, superseded, "equal-timestamp deliveries must not cover in either direction")
}

// Closed deliveries participate asymmetrically: a newer closed delivery
// covers older auto-plan work (planning a closed PR is pointless), but a
// claimed closed delivery is never itself superseded — its cleanup must
// always run, even when a newer auto-plan delivery exists for the same PR.
func TestWebhookEventStore_SupersedeIfCoveredNeverSupersedesClosed(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	createCoalescingEvent(t, store, "closed-old", "pull_request", "closed", "block/example", 7, "old-head", time.Minute, storage.WebhookEventPending, 0, "")
	createCoalescingEvent(t, store, "reopened-new", "pull_request", "reopened", "block/example", 7, "new-head", 0, storage.WebhookEventPending, 0, "")

	claimed := claimCoalescingEvent(t, store, "closed-old")
	// The probe reports only successor existence — it knows nothing about
	// the claimed row's action — so a true answer here is advisory and the
	// guarded write still refuses to supersede the closed claim.
	covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, claimed)
	require.NoError(t, err)
	assert.True(t, covered, "a newer auto-plan delivery is a covering successor by the successor predicate")
	superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
	require.NoError(t, err)
	assert.False(t, superseded, "a closed delivery must never be superseded")

	got, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "closed-old")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, storage.WebhookEventProcessing, got.State)
}

// The SQL guards the updated row's event type independently of the
// dispatcher's gate: a claimed non-pull_request row that carries the
// coalescing key (repository + PR) — and so passes the Go precondition —
// must never be superseded, even when a newer auto-plan delivery exists.
func TestWebhookEventStore_SupersedeIfCoveredNeverSupersedesNonPullRequestEvents(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	createCoalescingEvent(t, store, "check-run-old", "check_run", "created", "block/example", 7, "old-head", time.Minute, storage.WebhookEventPending, 0, "")
	createCoalescingEvent(t, store, "plan-new", "pull_request", "synchronize", "block/example", 7, "new-head", 0, storage.WebhookEventPending, 0, "")

	claimed := claimCoalescingEvent(t, store, "check-run-old")
	superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
	require.NoError(t, err)
	assert.False(t, superseded, "a non-pull_request claim must never be superseded")

	got, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "check-run-old")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, storage.WebhookEventProcessing, got.State, "the non-pull_request claim must stay processing")
}

// SupersedeIfCovered is a lease-guarded write: a stale token reports the lost
// lease, a deleted row reports not-found, and a claim without the coalescing
// key fields is rejected before touching storage.
func TestWebhookEventStore_SupersedeIfCoveredGuardsLeaseAndInputs(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	createCoalescingEvent(t, store, "old", "pull_request", "synchronize", "block/example", 7, "old-head", time.Minute, storage.WebhookEventPending, 0, "")
	createCoalescingEvent(t, store, "new", "pull_request", "synchronize", "block/example", 7, "new-head", 0, storage.WebhookEventPending, 0, "")

	claimed := claimCoalescingEvent(t, store, "old")

	missingToken := *claimed
	missingToken.LeaseToken = ""
	_, err := store.WebhookEvents().SupersedeIfCovered(ctx, &missingToken)
	require.ErrorContains(t, err, "lease token is required")

	// The read-only probe carries no lease semantics: it answers without a
	// lease token but still rejects a claim missing the coalescing key.
	covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, &missingToken)
	require.NoError(t, err)
	assert.True(t, covered, "the probe must answer without a lease token")

	missingKey := *claimed
	missingKey.Repository = ""
	_, err = store.WebhookEvents().SupersedeIfCovered(ctx, &missingKey)
	require.ErrorContains(t, err, "repository and pull request are required")
	_, err = store.WebhookEvents().HasCoveringSuccessor(ctx, &missingKey)
	require.ErrorContains(t, err, "repository and pull request are required")

	staleToken := *claimed
	staleToken.LeaseToken = "some-other-driver's-token"
	_, err = store.WebhookEvents().SupersedeIfCovered(ctx, &staleToken)
	require.ErrorIs(t, err, storage.ErrWebhookEventLeaseLost)

	got, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "old")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, storage.WebhookEventProcessing, got.State, "a stale token must not supersede the row")

	_, err = testDB.ExecContext(ctx, `DELETE FROM webhook_events WHERE provider = ? AND delivery_id = 'old'`, storage.WebhookProviderGitHub)
	require.NoError(t, err)
	_, err = store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
	require.ErrorIs(t, err, storage.ErrWebhookEventNotFound)
}

// Redelivering a superseded delivery reopens it as the PR's newest delivery:
// the reopen refreshes received_at, so pre-existing rows cannot immediately
// supersede the operator's explicit request — instead the reopened row now
// covers the rows that predate it.
func TestWebhookEventStore_SupersedeIfCoveredRedeliveryReopensAndWins(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	// delivery-b's receipt sits a few whole seconds in the past so the
	// second-precision received_at comparison is deterministic: it is
	// strictly newer than delivery-a's original receipt and strictly older
	// than delivery-a's redelivery below.
	createCoalescingEvent(t, store, "delivery-a", "pull_request", "synchronize", "block/example", 7, "head-a", time.Minute, storage.WebhookEventPending, 0, "")
	createCoalescingEvent(t, store, "delivery-b", "pull_request", "synchronize", "block/example", 7, "head-b", 5*time.Second, storage.WebhookEventPending, 0, "")

	claimed := claimCoalescingEvent(t, store, "delivery-a")
	superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
	require.NoError(t, err)
	require.True(t, superseded)

	// Operator Redeliver reuses the GUID; the superseded row reopens pending
	// with a fresh received_at.
	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-a", Event: "pull_request", Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	// The reopened row is claimed first (FIFO by created_at) and is now the
	// PR's newest delivery, so the still-pending delivery-b cannot cover it.
	reclaimed := claimCoalescingEvent(t, store, "delivery-a")
	covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, reclaimed)
	require.NoError(t, err)
	assert.False(t, covered, "rows predating the redelivery must not count as covering successors")
	superseded, err = store.WebhookEvents().SupersedeIfCovered(ctx, reclaimed)
	require.NoError(t, err)
	assert.False(t, superseded, "a redelivered row must not be re-superseded by rows that predate the redelivery")

	// The reopened row's live claim now covers delivery-b instead.
	claimedB := claimCoalescingEvent(t, store, "delivery-b")
	covered, err = store.WebhookEvents().HasCoveringSuccessor(ctx, claimedB)
	require.NoError(t, err)
	assert.True(t, covered, "the redelivered row must count as delivery-b's covering successor")
	superseded, err = store.WebhookEvents().SupersedeIfCovered(ctx, claimedB)
	require.NoError(t, err)
	assert.True(t, superseded, "the redelivered row's live claim must cover the older delivery")
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

// A row that spent time deferred measures its backlog age and dispatch-lag
// basis from when it became claimable, not from receipt: the deferral's grace
// period is by design, and reporting it as backlog would spike the gauge to
// the full grace duration the instant the row becomes due.
func TestWebhookEventStore_DeferredRowAgeMeasuredFromDueTime(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	future := time.Now().Add(time.Hour)
	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "was-deferred", Event: "check_suite", Payload: []byte(`{}`), RetryAfter: &future,
	})
	require.NoError(t, err)
	require.True(t, inserted)

	// The row was received 60s ago and its not-before time elapsed 10s ago:
	// only the 10s spent claimable is backlog, not the 50s grace period.
	_, err = testDB.ExecContext(ctx, `
		UPDATE webhook_events
		SET received_at = NOW() - INTERVAL 60 SECOND, retry_after = NOW() - INTERVAL 10 SECOND
		WHERE provider = ? AND delivery_id = ?
	`, storage.WebhookProviderGitHub, "was-deferred")
	require.NoError(t, err)

	stats, err := store.WebhookEvents().InboxStats(ctx)
	require.NoError(t, err)
	assert.Greater(t, stats.OldestClaimableAge, 5*time.Second, "age must count the time spent claimable")
	assert.Less(t, stats.OldestClaimableAge, 40*time.Second, "age must not count the deferral's grace period since receipt")

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.WithinDuration(t, time.Now().Add(-10*time.Second), claimed.ClaimableSince, 5*time.Second,
		"the claimed event carries when it became dispatchable, not its receipt time")

	// A row that was never deferred is dispatchable from receipt.
	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "never-deferred", Event: "check_suite", Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.True(t, inserted)

	prompt, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, prompt)
	require.Equal(t, "never-deferred", prompt.DeliveryID)
	assert.WithinDuration(t, prompt.ReceivedAt, prompt.ClaimableSince, 2*time.Second,
		"an undeferred row is dispatchable from receipt")
}

// A duplicate Create whose GUID matches a terminally failed row reopens that
// row immediately claimable even when the incoming event carries a future
// not-before time: redelivery is an operator recovery lever, so the reopened
// row must never be re-deferred behind the very deferral whose loss it
// recovers from.
func TestWebhookEventStore_CreateReopenIgnoresIncomingNotBefore(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{"attempt":1}`),
	})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "permanent failure", nil))

	future := time.Now().Add(time.Hour)
	inserted, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{"attempt":2}`), RetryAfter: &future,
	})
	require.NoError(t, err)
	require.True(t, inserted)

	reopened, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, reopened)
	assert.Equal(t, storage.WebhookEventPending, reopened.State)
	assert.Nil(t, reopened.RetryAfter, "the reopen discards the incoming not-before time")

	reclaimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	assert.Equal(t, "delivery-1", reclaimed.DeliveryID, "the reopened row is immediately claimable")
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

func TestWebhookEventStore_FindNextRequiresOwnerWithoutLeaseLostSentinel(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	claimed, err := store.WebhookEvents().FindNext(ctx, "", time.Minute)
	require.Nil(t, claimed)
	require.Error(t, err)
	assert.False(t, errors.Is(err, storage.ErrWebhookEventLeaseLost))
}

// A dead-lettered delivery is terminal and sticky: MarkFailedPermanent records
// the error that proved the head can never succeed, FindNext never reclaims
// the row, and only a redelivery of the same GUID (GitHub Redeliver) reopens
// it with a fresh claim budget.
func TestWebhookEventStore_MarkFailedPermanentDeadLetters(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	inserted, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{DeliveryID: "delivery-1", Event: "pull_request", Payload: []byte(`{"attempt":1}`)})
	require.NoError(t, err)
	require.True(t, inserted)

	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	require.ErrorIs(t, store.WebhookEvents().MarkFailedPermanent(ctx, claimed.ID, "stale-token", "unused"), storage.ErrWebhookEventLeaseLost)
	require.NoError(t, store.WebhookEvents().MarkFailedPermanent(ctx, claimed.ID, claimed.LeaseToken, "PR file listing hit the GitHub cap"))

	deadLettered, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, deadLettered)
	assert.Equal(t, storage.WebhookEventFailedPermanent, deadLettered.State)
	assert.Equal(t, "PR file listing hit the GitHub cap", deadLettered.LastError)
	assert.Nil(t, deadLettered.RetryAfter)
	assert.NotNil(t, deadLettered.CompletedAt)
	// The lease token is retained on terminal rows so a committed-but-unacked
	// retry of MarkFailedPermanent stays idempotent rather than reporting a
	// lost lease.
	assert.Equal(t, claimed.LeaseToken, deadLettered.LeaseToken)
	require.NoError(t, store.WebhookEvents().MarkFailedPermanent(ctx, claimed.ID, claimed.LeaseToken, "PR file listing hit the GitHub cap"))

	reclaimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.Nil(t, reclaimed, "a dead-lettered row must never be reclaimed")

	// GitHub Redeliver reuses the delivery GUID and is the one lever that
	// revives a dead-lettered delivery — after the author shrinks the PR or the
	// deterministic cause is otherwise fixed.
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
	assert.JSONEq(t, `{"attempt":2}`, string(reopened.Payload))

	revived, err := store.WebhookEvents().FindNext(ctx, "driver-c", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, revived, "a redelivered dead-lettered row must be claimable again")
	assert.Equal(t, 1, revived.Attempts)
}

func TestWebhookEventStore_LeaseTokenGuardsWrites(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

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

func TestWebhookEventStore_ReleaseRefundsAttemptAndRequeues(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

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
	store := NewMySQL(testDB)

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
	store := NewMySQL(testDB)

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
	store := NewMySQL(testDB)

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
	store := NewMySQL(testDB)

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

	db, err := sql.Open("mysql", testDSNChangedRows)
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

// A sub-second lease must not collapse to an already-expired lease: a second
// driver must not immediately reclaim an event the first driver is processing.
func TestWebhookEventStore_SubSecondLeaseIsNotImmediatelyReclaimable(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

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

// InboxStats gives operators a steady-state view of the durable inbox: how many
// rows sit in each state, how long the oldest ready-to-claim delivery has waited
// (backlog latency), and how many are wedged in processing past the attempt cap.
func TestWebhookEventStore_InboxStats(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

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
	store := NewMySQL(testDB)

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
