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

// A terminally failed row's coverage depends on its origin: a freshly failed
// organic delivery covers its head because the operator's GitHub Redeliver
// lever exists for it (TestWebhookEventStore_FailedOrganicRowCoversOnlyDuringCoolOff
// covers how long), while a failed synthesized row must not cover, because
// there is no Redeliver lever for a synthesized GUID and re-synthesis is the
// only recovery path. A dead-lettered
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

// A PR's plan delivery that exhausted its transient retries covers its head
// only for WebhookEventFailedCoverageCoolOff, so once the window passes the
// reconciler re-plans the PR instead of leaving its check blocked until
// someone pushes or comments. A dead-lettered row covers however old it is,
// and a failed synthesized row never covers.
func TestWebhookEventStore_FailedOrganicRowCoversOnlyDuringCoolOff(t *testing.T) {
	clearTables(t)
	assertFailedRowCoverageCoolOff(t, NewMySQL(testDB), testDB, MySQLDialect{}, func(query string) string { return query })
}

// assertFailedRowCoverageCoolOff ages terminal rows on the database clock and
// checks which still cover their head. The cutoff is rendered SQL, so each
// dialect suite runs it; rebind adapts the raw aging statement's placeholders.
func assertFailedRowCoverageCoolOff(t *testing.T, store storage.Storage, db *sql.DB, dialect Dialect, rebind func(string) string) {
	t.Helper()
	ctx := t.Context()
	coolOff := storage.WebhookEventFailedCoverageCoolOff
	ago := func(d time.Duration) string {
		return dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
			LiteralIntervalAmount(uint64(d.Microseconds())), IntervalMicrosecond)
	}

	for i, tc := range []struct {
		name        string
		state       string
		synthesized bool
		failedAgo   time.Duration
		// noCompletedAt leaves completed_at unset, so updated_at dates the failure.
		noCompletedAt bool
		covers        bool
	}{
		{name: "organic failed a minute ago", state: storage.WebhookEventFailed, failedAgo: time.Minute, covers: true},
		{name: "organic failed just inside the cool-off", state: storage.WebhookEventFailed, failedAgo: coolOff - time.Minute, covers: true},
		{name: "organic failed past the cool-off", state: storage.WebhookEventFailed, failedAgo: coolOff + time.Minute, covers: false},
		{name: "organic failed past the cool-off without completed_at", state: storage.WebhookEventFailed, failedAgo: coolOff + time.Minute, noCompletedAt: true, covers: false},
		{name: "synthesized failed a minute ago", state: storage.WebhookEventFailed, synthesized: true, failedAgo: time.Minute, covers: false},
		{name: "organic dead-lettered past the cool-off", state: storage.WebhookEventFailedPermanent, failedAgo: coolOff + time.Minute, covers: true},
		{name: "synthesized dead-lettered past the cool-off", state: storage.WebhookEventFailedPermanent, synthesized: true, failedAgo: coolOff + time.Minute, covers: true},
		{name: "organic completed past the cool-off", state: storage.WebhookEventCompleted, failedAgo: coolOff + time.Minute, covers: true},
	} {
		deliveryID := fmt.Sprintf("delivery-cool-off-%d", i)
		if tc.synthesized {
			deliveryID = storage.SynthesizedWebhookDeliveryIDPrefix + deliveryID
		}
		headSHA := fmt.Sprintf("cool-off-head-%d", i)
		_, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
			DeliveryID:  deliveryID,
			Event:       "pull_request",
			Action:      "synchronize",
			Repository:  "block/example",
			PullRequest: 7,
			HeadSHA:     headSHA,
			Payload:     []byte(`{}`),
		})
		require.NoError(t, err, tc.name)
		completedAt := ago(tc.failedAgo)
		if tc.noCompletedAt {
			completedAt = "NULL"
		}
		_, err = db.ExecContext(ctx, rebind(`UPDATE webhook_events SET state = ?, completed_at = `+completedAt+
			`, updated_at = `+ago(tc.failedAgo)+` WHERE delivery_id = ?`), tc.state, deliveryID)
		require.NoError(t, err, tc.name)

		found, err := store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, "block/example", 7, headSHA)
		require.NoError(t, err, tc.name)
		assert.Equal(t, tc.covers, found, tc.name)
	}
}

// Retention deletes only rows no driver or sweep will touch again. An old row
// in each terminal state goes; an old pending, processing, or retryable row
// stays however old it is, and so does a terminal row still inside the window.
func TestWebhookEventStore_DeleteTerminalOlderThan(t *testing.T) {
	clearTables(t)
	assertDeleteTerminalOlderThan(t, NewMySQL(testDB), testDB, MySQLDialect{}, func(query string) string { return query })
}

// assertDeleteTerminalOlderThan ages rows on the database clock and checks
// which survive a purge. The aging statement is rendered SQL, so each dialect
// suite runs it; rebind adapts that raw statement's placeholders.
func assertDeleteTerminalOlderThan(t *testing.T, store storage.Storage, db *sql.DB, dialect Dialect, rebind func(string) string) {
	t.Helper()
	ctx := t.Context()
	retention := 30 * 24 * time.Hour
	ago := func(d time.Duration) string {
		return dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
			LiteralIntervalAmount(uint64(d.Microseconds())), IntervalMicrosecond)
	}
	seed := func(deliveryID, state string, receivedAgo time.Duration) {
		t.Helper()
		_, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
			DeliveryID: deliveryID,
			Event:      "pull_request",
			Action:     "synchronize",
			Repository: "block/example",
			Payload:    []byte(`{}`),
		})
		require.NoError(t, err, deliveryID)
		_, err = db.ExecContext(ctx, rebind(`UPDATE webhook_events SET state = ?, received_at = `+ago(receivedAgo)+
			` WHERE delivery_id = ?`), state, deliveryID)
		require.NoError(t, err, deliveryID)
	}
	exists := func(deliveryID string) bool {
		t.Helper()
		event, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, deliveryID)
		require.NoError(t, err, deliveryID)
		return event != nil
	}

	seed("invalid-args-row", storage.WebhookEventCompleted, retention+time.Hour)
	for _, tc := range []struct {
		name  string
		age   time.Duration
		limit int
	}{
		{"zero age", 0, 10},
		{"negative age", -time.Hour, 10},
		{"zero limit", retention, 0},
		{"negative limit", retention, -1},
		{"limit above the largest batch", retention, storage.MaxWebhookEventPurgeBatch + 1},
	} {
		deleted, err := store.WebhookEvents().DeleteTerminalOlderThan(ctx, tc.age, tc.limit)
		require.Error(t, err, tc.name)
		assert.Equal(t, int64(0), deleted, tc.name)
	}
	require.True(t, exists("invalid-args-row"), "an invalid call must delete nothing")
	deleted, err := store.WebhookEvents().DeleteTerminalOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	old := retention + time.Hour
	young := retention - time.Hour
	oldTerminal := []string{
		storage.WebhookEventCompleted, storage.WebhookEventFailed,
		storage.WebhookEventFailedPermanent, storage.WebhookEventSuperseded,
	}
	oldInFlight := []string{
		storage.WebhookEventPending, storage.WebhookEventProcessing, storage.WebhookEventFailedRetryable,
	}
	for _, state := range oldTerminal {
		seed("old-"+state, state, old)
	}
	for _, state := range oldInFlight {
		seed("old-"+state, state, old)
	}
	for _, state := range oldTerminal {
		seed("young-"+state, state, young)
	}

	counted, err := store.WebhookEvents().CountTerminalOlderThan(ctx, retention)
	require.NoError(t, err)
	assert.Equal(t, int64(len(oldTerminal)), counted, "the count sees the rows a purge may delete, and no others")
	for _, age := range []time.Duration{0, -time.Hour} {
		_, err = store.WebhookEvents().CountTerminalOlderThan(ctx, age)
		require.Error(t, err, "age %s", age)
	}
	require.True(t, exists("old-"+storage.WebhookEventCompleted), "counting must delete nothing")

	deleted, err = store.WebhookEvents().DeleteTerminalOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(len(oldTerminal)), deleted, "one old row per terminal state")
	counted, err = store.WebhookEvents().CountTerminalOlderThan(ctx, retention)
	require.NoError(t, err)
	assert.Equal(t, int64(0), counted, "nothing terminal and old is left to count")
	for _, state := range oldTerminal {
		assert.False(t, exists("old-"+state), "old %s row must be deleted", state)
		assert.True(t, exists("young-"+state), "young %s row must be kept", state)
	}
	for _, state := range oldInFlight {
		assert.True(t, exists("old-"+state), "old %s row must be kept", state)
	}
	deleted, err = store.WebhookEvents().DeleteTerminalOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted, "a second purge finds nothing left")

	// The limit caps one call, and the oldest rows go first.
	seed("batch-oldest", storage.WebhookEventCompleted, old+2*time.Hour)
	seed("batch-middle", storage.WebhookEventCompleted, old+time.Hour)
	seed("batch-newest", storage.WebhookEventCompleted, old)
	deleted, err = store.WebhookEvents().DeleteTerminalOlderThan(ctx, retention, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
	assert.False(t, exists("batch-oldest"))
	assert.False(t, exists("batch-middle"))
	assert.True(t, exists("batch-newest"), "the limit leaves the newest row for the next call")
	deleted, err = store.WebhookEvents().DeleteTerminalOlderThan(ctx, retention, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted, "the next call takes the rest")
	assert.False(t, exists("batch-newest"))

	// A Redeliver that reopens a row after the purge selected it keeps the row:
	// the delete checks state and age again.
	inbox := store.WebhookEvents().(*webhookEventStore)
	t.Cleanup(func() { inbox.hooks = webhookEventHooks{} })
	seed("race-reopened", storage.WebhookEventCompleted, old)
	seed("race-purged", storage.WebhookEventCompleted, old)
	inbox.hooks.afterPurgeSelect = func() {
		inbox.hooks.afterPurgeSelect = nil
		created, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
			DeliveryID: "race-reopened", Event: "pull_request", Action: "synchronize",
			Repository: "block/example", Payload: []byte(`{}`),
		})
		require.NoError(t, err)
		require.True(t, created, "the redelivery reopens the completed row")
	}
	deleted, err = store.WebhookEvents().DeleteTerminalOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted, "only the row nobody reopened is deleted")
	assert.False(t, exists("race-purged"))
	reopened, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "race-reopened")
	require.NoError(t, err)
	require.NotNil(t, reopened, "a row reopened between the select and the delete must survive")
	assert.Equal(t, storage.WebhookEventPending, reopened.State)

	// A purge that deletes a row between a redelivery's duplicate insert and
	// its reopen does not lose the redelivery: Create inserts the row again.
	seed("race-recreated", storage.WebhookEventCompleted, old)
	inbox.hooks.afterDuplicateInsert = func() {
		inbox.hooks.afterDuplicateInsert = nil
		_, err := db.ExecContext(ctx, rebind(`DELETE FROM webhook_events WHERE delivery_id = ?`), "race-recreated")
		require.NoError(t, err)
	}
	created, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "race-recreated", Event: "pull_request", Action: "synchronize",
		Repository: "block/example", Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	assert.True(t, created, "the redelivery is accepted, so it must have a row")
	recreated, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "race-recreated")
	require.NoError(t, err)
	require.NotNil(t, recreated, "an accepted redelivery has a row to drive")
	assert.Equal(t, storage.WebhookEventPending, recreated.State)
}

// Retention keeps the newest covering row of each pull request, the row the
// reconciler reads for its newest planned head: an old completed or
// dead-lettered row survives unless a newer covering row exists for the same
// pull request, and rows that cover nothing are deleted on age alone.
func TestWebhookEventStore_PurgeKeepsNewestCoveringRow(t *testing.T) {
	clearTables(t)
	assertPurgeKeepsNewestCoveringRow(t, NewMySQL(testDB), testDB, MySQLDialect{}, func(query string) string { return query })
}

// assertPurgeKeepsNewestCoveringRow seeds one pull request per scenario with
// every row older than the retention window, purges, and checks which rows
// survive, that HasEventForHead still reports the heads that must stay
// covered, and that the dry-run count equals what the purge deleted.
func assertPurgeKeepsNewestCoveringRow(t *testing.T, store storage.Storage, db *sql.DB, dialect Dialect, rebind func(string) string) {
	t.Helper()
	ctx := t.Context()
	const repo = "block/coverage"
	retention := 30 * 24 * time.Hour
	old := retention + time.Hour
	ago := func(d time.Duration) string {
		return dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
			LiteralIntervalAmount(uint64(d.Microseconds())), IntervalMicrosecond)
	}
	// seedRow inserts a delivery in order, so later seeds have higher ids. The
	// row was received receivedAgo ago and reached its state finishedAgo ago.
	seedRow := func(deliveryID, repository, event string, pr int, head, action, state string, receivedAgo, finishedAgo time.Duration) {
		t.Helper()
		_, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
			DeliveryID: deliveryID, Event: event, Action: action,
			Repository: repository, PullRequest: pr, HeadSHA: head, Payload: []byte(`{}`),
		})
		require.NoError(t, err, deliveryID)
		_, err = db.ExecContext(ctx, rebind(`UPDATE webhook_events SET state = ?, received_at = `+ago(receivedAgo)+
			`, completed_at = `+ago(finishedAgo)+`, updated_at = `+ago(finishedAgo)+` WHERE delivery_id = ?`), state, deliveryID)
		require.NoError(t, err, deliveryID)
	}
	// seed inserts a pull_request delivery in the test's repository.
	seed := func(deliveryID string, pr int, head, action, state string, receivedAgo, finishedAgo time.Duration) {
		t.Helper()
		seedRow(deliveryID, repo, "pull_request", pr, head, action, state, receivedAgo, finishedAgo)
	}
	exists := func(deliveryID string) bool {
		t.Helper()
		event, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, deliveryID)
		require.NoError(t, err, deliveryID)
		return event != nil
	}
	covered := func(pr int, head string) bool {
		t.Helper()
		found, err := store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, repo, pr, head)
		require.NoError(t, err)
		return found
	}
	completed, permanent := storage.WebhookEventCompleted, storage.WebhookEventFailedPermanent

	// 1: the only covering row of its pull request.
	seed("only-completed", 1, "head-1", "synchronize", completed, old, old)
	// 2: a completed row with a newer completed row for another head.
	seed("completed-older", 2, "head-1", "synchronize", completed, old, old)
	seed("completed-newest", 2, "head-2", "synchronize", completed, old, old)
	// 3: the same for dead-lettered rows, plus a synthesized-GUID one on its own.
	seed("permanent-older", 3, "head-1", "synchronize", permanent, old, old)
	seed("permanent-newest", 3, "head-2", "synchronize", permanent, old, old)
	seed(storage.SynthesizedWebhookDeliveryIDPrefix+"permanent-only", 4, "head-1", "synchronize", permanent, old, old)
	// 5: rows that cover nothing are deleted even when they are the newest for
	// their pull request, and they do not displace the covering row beneath them.
	seed("kept-beneath-failed", 5, "head-1", "synchronize", completed, old, old)
	seed("failed-newest", 5, "head-2", "synchronize", storage.WebhookEventFailed, old, old)
	seed("superseded-newest", 5, "head-3", "synchronize", storage.WebhookEventSuperseded, old, old)
	seed("failed-alone", 6, "head-1", "synchronize", storage.WebhookEventFailed, old, old)
	seed("superseded-alone", 7, "head-1", "synchronize", storage.WebhookEventSuperseded, old, old)
	// 8: an action that never plans a head does not cover it.
	seed("closed-alone", 8, "head-1", "closed", completed, old, old)
	// 9: a newer row still in flight covers its head now but can lose that
	// coverage (it can fail), so it does not justify deleting the older row until
	// it completes.
	seed("older-than-pending", 9, "head-1", "synchronize", completed, old, old)
	seed("pending-newest", 9, "head-2", "synchronize", storage.WebhookEventPending, old, old)
	// 10: a failed row still inside its cool-off covers its head, so it stays.
	seed("failed-in-cool-off", 10, "head-1", "synchronize", storage.WebhookEventFailed, old, time.Minute)
	// 11: Redeliver of an older head resets its received_at but keeps its id, so
	// the current head's row, the newest by id, must still be kept.
	seed("redelivered-older-head", 11, "head-1", "synchronize", completed, old, old)
	seed("current-head", 11, "head-2", "synchronize", completed, old, old)
	reopened, err := store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "redelivered-older-head", Event: "pull_request", Action: "synchronize",
		Repository: repo, PullRequest: 11, HeadSHA: "head-1", Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.True(t, reopened, "the redelivery reopens the completed row with a fresh received_at")
	_, err = db.ExecContext(ctx, rebind(`UPDATE webhook_events SET state = ? WHERE delivery_id = ?`), completed, "redelivered-older-head")
	require.NoError(t, err)
	// 16: a delivery for the earlier head that arrives after the current head
	// was planned finds the head moved and ends superseded, so it is not a
	// newer covering row and the current head's row is kept.
	seed("before-late-arrival", 16, "head-1", "synchronize", completed, old, old)
	seed("current-before-late-arrival", 16, "head-2", "synchronize", completed, old, old)
	seed("late-arrival-earlier-head", 16, "head-1", "synchronize", storage.WebhookEventSuperseded, old, old)
	// 17: a newer row of another event type on the same pull request plans
	// nothing, so it does not justify deleting the covering row, even when its
	// action shares a name with an auto-plan action.
	seed("beneath-check-run", 17, "head-1", "synchronize", completed, old, old)
	seedRow("check-run-newest", repo, "check_run", 17, "head-1", "completed", completed, old, old)
	seedRow("issue-reopened-newest", repo, "issues", 17, "head-1", "reopened", completed, old, old)
	// 18: nor does a newer pull_request row whose action never plans a head.
	seed("beneath-closed", 18, "head-1", "synchronize", completed, old, old)
	seed("closed-newest", 18, "head-1", "closed", completed, old, old)
	// 19: a newer covering row in another repository with the same pull request
	// number belongs to another pull request.
	seed("beneath-other-repo", 19, "head-1", "synchronize", completed, old, old)
	seedRow("other-repo-newest", "block/other", "pull_request", 19, "head-2", "synchronize", completed, old, old)
	// 20: a covering-shaped row with no pull request can never cover a head.
	seed("no-pull-request", 20, "head-1", "synchronize", completed, old, old)
	_, err = db.ExecContext(ctx, rebind(`UPDATE webhook_events SET pull_request = 0 WHERE delivery_id = ?`), "no-pull-request")
	require.NoError(t, err)

	wantDeleted := []string{
		"completed-older", "permanent-older", "failed-newest", "superseded-newest",
		"failed-alone", "superseded-alone", "closed-alone",
		"before-late-arrival", "late-arrival-earlier-head",
		"check-run-newest", "issue-reopened-newest", "closed-newest", "no-pull-request",
	}
	wantKept := []string{
		"only-completed", "completed-newest", "permanent-newest", "older-than-pending",
		storage.SynthesizedWebhookDeliveryIDPrefix + "permanent-only", "kept-beneath-failed",
		"pending-newest", "failed-in-cool-off", "redelivered-older-head", "current-head",
		"current-before-late-arrival", "beneath-check-run", "beneath-closed",
		"beneath-other-repo", "other-repo-newest",
	}

	counted, err := store.WebhookEvents().CountTerminalOlderThan(ctx, retention)
	require.NoError(t, err)
	assert.Equal(t, int64(len(wantDeleted)), counted, "the count sees what the purge may delete")
	for _, deliveryID := range append(append([]string{}, wantDeleted...), wantKept...) {
		require.True(t, exists(deliveryID), "counting must delete nothing: %s", deliveryID)
	}

	deleted, err := store.WebhookEvents().DeleteTerminalOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(len(wantDeleted)), deleted, "the purge deletes exactly the rows the count reported")
	for _, deliveryID := range wantDeleted {
		assert.False(t, exists(deliveryID), "%s is not the newest covering row of its pull request, so it must be deleted", deliveryID)
	}
	for _, deliveryID := range wantKept {
		assert.True(t, exists(deliveryID), "%s must be kept", deliveryID)
	}
	counted, err = store.WebhookEvents().CountTerminalOlderThan(ctx, retention)
	require.NoError(t, err)
	assert.Equal(t, int64(0), counted, "what is left is all kept")

	assert.True(t, covered(1, "head-1"), "the only completed row still covers its head")
	assert.True(t, covered(2, "head-2"), "the newest completed row still covers its head")
	assert.True(t, covered(3, "head-2"), "the newest dead-lettered row still covers its head")
	assert.True(t, covered(4, "head-1"), "a synthesized dead-lettered row still covers its head")
	assert.True(t, covered(5, "head-1"), "the covering row beneath uncovering newer rows still covers its head")
	assert.True(t, covered(10, "head-1"), "a failed row in its cool-off still covers its head")
	assert.True(t, covered(11, "head-2"), "a redelivered older head must not expose the current head")
	assert.True(t, covered(16, "head-2"), "a late delivery for an earlier head must not expose the current head")

	// Once the newer row completes, it justifies deleting the older one.
	assert.True(t, exists("older-than-pending"), "a newer pending row must not justify the delete")
	assert.True(t, covered(9, "head-1"))
	_, err = db.ExecContext(ctx, rebind(`UPDATE webhook_events SET state = ? WHERE delivery_id = ?`), completed, "pending-newest")
	require.NoError(t, err)
	counted, err = store.WebhookEvents().CountTerminalOlderThan(ctx, retention)
	require.NoError(t, err)
	assert.Equal(t, int64(1), counted, "the older row is deletable once the newer row completes")
	deleted, err = store.WebhookEvents().DeleteTerminalOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	assert.False(t, exists("older-than-pending"))
	assert.True(t, exists("pending-newest"))
	assert.True(t, covered(9, "head-2"), "the completed newest row still covers its head")

	// A newer row that stops covering permanently between the page select and the
	// delete (a Redeliver reopens it) no longer justifies deleting the older row:
	// the delete checks again.
	inbox := store.WebhookEvents().(*webhookEventStore)
	t.Cleanup(func() { inbox.hooks = webhookEventHooks{} })
	seed("guard-oldest", 12, "head-0", "synchronize", completed, old, old)
	seed("guard-older", 12, "head-1", "synchronize", completed, old, old)
	seed("guard-newest", 12, "head-2", "synchronize", completed, old, old)
	inbox.hooks.afterPurgeSelect = func() {
		inbox.hooks.afterPurgeSelect = nil
		_, hookErr := db.ExecContext(ctx, rebind(`UPDATE webhook_events SET state = ? WHERE delivery_id = ?`),
			storage.WebhookEventPending, "guard-newest")
		require.NoError(t, hookErr)
	}
	deleted, err = store.WebhookEvents().DeleteTerminalOlderThan(ctx, retention, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted, "only the row still justified by a newer covering row is deleted")
	assert.False(t, exists("guard-oldest"), "guard-older still covers beneath it")
	assert.True(t, exists("guard-older"), "the row left with no newer covering row must survive")
	assert.True(t, covered(12, "head-1"))

	// Rows the purge must keep never fill its page: two protected rows older than
	// a deletable one leave room for it even when the page holds one row.
	seed("crowd-protected-1", 13, "head-1", "synchronize", completed, old+3*time.Hour, old+3*time.Hour)
	seed("crowd-protected-2", 14, "head-1", "synchronize", completed, old+3*time.Hour, old+3*time.Hour)
	seed("crowd-deletable", 15, "head-1", "synchronize", storage.WebhookEventFailed, old+2*time.Hour, old+2*time.Hour)
	deleted, err = store.WebhookEvents().DeleteTerminalOlderThan(ctx, retention, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted, "the page holds the deletable row, not the protected rows older than it")
	assert.False(t, exists("crowd-deletable"))
	assert.True(t, exists("crowd-protected-1"))
	assert.True(t, exists("crowd-protected-2"))
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
