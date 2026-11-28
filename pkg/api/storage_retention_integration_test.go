//go:build integration

package api

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/storage/postgresstore"
)

// An operator turns storage retention on and runs a pass against a real inbox.
// The inbox holds finished deliveries older than the window, deliveries older
// than the window that are still pending, processing, or waiting to retry, and a
// finished delivery inside the window. A dry run only reports; the real pass
// then deletes exactly the old finished rows, in batches smaller than the
// backlog, and leaves every row a driver or the reconciler still needs.
func TestStorageRetentionPassPurgesOnlyOldFinishedRowsOnMySQL(t *testing.T) {
	db := openMatrixStorage(t)
	runStorageRetentionPassAgainstRealStorage(t, mysqlstore.New(db), db, func(int) string { return "?" })
}

// The same scenario on PostgreSQL storage, which elects the pass with a
// different advisory lock mechanism and deletes through different SQL.
func TestStorageRetentionPassPurgesOnlyOldFinishedRowsOnPostgres(t *testing.T) {
	dsn, _ := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	runStorageRetentionPassAgainstRealStorage(t, postgresstore.New(db), db, func(n int) string { return fmt.Sprintf("$%d", n) })
}

func runStorageRetentionPassAgainstRealStorage(t *testing.T, stor storage.Storage, db *sql.DB, placeholder func(int) string) {
	t.Helper()
	ctx := t.Context()
	window := 168 * time.Hour

	// A pull request of 0 seeds a row that plans no pull request head, so only
	// its state and age decide whether the pass deletes it.
	seedForPR := func(deliveryID, state string, receivedAgo time.Duration, pullRequest int) {
		t.Helper()
		_, err := stor.WebhookEvents().Create(ctx, &storage.WebhookEvent{
			DeliveryID:  deliveryID,
			Event:       "pull_request",
			Action:      "synchronize",
			Repository:  "block/example",
			PullRequest: pullRequest,
			HeadSHA:     "head-" + deliveryID,
			Payload:     []byte(`{}`),
		})
		require.NoError(t, err, deliveryID)
		_, err = db.ExecContext(ctx,
			"UPDATE webhook_events SET state = "+placeholder(1)+", received_at = "+placeholder(2)+" WHERE delivery_id = "+placeholder(3),
			state, time.Now().UTC().Add(-receivedAgo), deliveryID)
		require.NoError(t, err, deliveryID)
	}
	seed := func(deliveryID, state string, receivedAgo time.Duration) {
		t.Helper()
		seedForPR(deliveryID, state, receivedAgo, 0)
	}
	exists := func(deliveryID string) bool {
		t.Helper()
		event, err := stor.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, deliveryID)
		require.NoError(t, err, deliveryID)
		return event != nil
	}

	old := window + 24*time.Hour
	young := window - 24*time.Hour
	// Pull request 7 has two old planned heads. The older one is deleted; the
	// newer one is kept, because it is what tells the reconciler that the pull
	// request's latest head was planned.
	seedForPR("old-pr7-earlier-head", storage.WebhookEventCompleted, old, 7)
	seedForPR("old-pr7-latest-head", storage.WebhookEventCompleted, old, 7)
	oldFinished := []string{"old-pr7-earlier-head", "old-completed-1", "old-failed-1", "old-superseded-1"}
	seed(oldFinished[1], storage.WebhookEventCompleted, old)
	seed(oldFinished[2], storage.WebhookEventFailedPermanent, old)
	seed(oldFinished[3], storage.WebhookEventSuperseded, old)
	oldUnfinished := []string{"old-pending", "old-processing", "old-retryable"}
	seed(oldUnfinished[0], storage.WebhookEventPending, old)
	seed(oldUnfinished[1], storage.WebhookEventProcessing, old)
	seed(oldUnfinished[2], storage.WebhookEventFailedRetryable, old)
	seed("young-completed", storage.WebhookEventCompleted, young)

	retentionLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	run := func(cfg StorageRetentionConfig) {
		t.Helper()
		cfg.Enabled = true
		cfg.WebhookEvents = window.String()
		svc := New(stor, &ServerConfig{StorageRetention: cfg}, nil, retentionLogger)
		pass, _, ok := svc.storageRetentionPass()
		require.True(t, ok)
		pass(ctx)
	}
	requireRowsRemaining := func(want int, message string) {
		t.Helper()
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM webhook_events").Scan(&count))
		assert.Equal(t, want, count, message)
	}

	// The first pass an operator sees is a dry run, so enabling the pass alone
	// changes nothing.
	run(StorageRetentionConfig{})
	requireRowsRemaining(9, "a dry run deletes nothing")

	// A batch size of two takes three batches to clear four old finished rows:
	// two full batches, then one that comes back empty.
	run(StorageRetentionConfig{DryRun: new(false), BatchSize: 2})
	requireRowsRemaining(5, "only the old finished rows are deleted")
	for _, id := range oldFinished {
		assert.False(t, exists(id), "%s is old and finished, so it is deleted", id)
	}
	assert.True(t, exists("old-pr7-latest-head"), "the newest planned head of a pull request is kept however old")
	for _, id := range oldUnfinished {
		assert.True(t, exists(id), "%s is still work a driver owns, so it is kept", id)
	}
	assert.True(t, exists("young-completed"), "a finished row inside the window is kept")

	// A pass with nothing left to delete is harmless and repeatable.
	run(StorageRetentionConfig{DryRun: new(false), BatchSize: 2})
	requireRowsRemaining(5, "a second pass finds nothing to delete")

	// Two instances running the pass at once is also safe: the second steps aside
	// while the first holds the election.
	electionHeld := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- stor.Applies().WithStorageRetentionElection(ctx, func(context.Context) error {
			close(electionHeld)
			<-finish
			return nil
		})
	}()
	select {
	case <-electionHeld:
	case err := <-done:
		require.FailNow(t, "the election ended before the test could hold it", "%v", err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the election was never taken")
	}
	seed("old-while-elected", storage.WebhookEventCompleted, old)
	run(StorageRetentionConfig{DryRun: new(false), BatchSize: 2})
	assert.True(t, exists("old-while-elected"), "a pass that loses the election deletes nothing")
	close(finish)
	require.NoError(t, <-done)
	run(StorageRetentionConfig{DryRun: new(false), BatchSize: 2})
	assert.False(t, exists("old-while-elected"), "the next pass, once the election is free, deletes it")
}
