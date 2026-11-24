//go:build integration

package serve_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/notifications"
	"github.com/block/schemabot/pkg/serve"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type acceptingHostPublisher struct {
	accepted chan json.RawMessage
}

func (p acceptingHostPublisher) Publish(ctx context.Context, payload json.RawMessage) error {
	select {
	case p.accepted <- append(json.RawMessage(nil), payload...):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// An embedding host's selected publisher receives the durable outcome after
// Build and Start assemble the server; accepting it retires the pending row.
func TestBuildStartsHostPublisher(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	dsn, db := testutil.StartPostgres(t, "publisher_host")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cfg := &api.ServerConfig{
		Storage: api.StorageConfig{DSN: dsn, Dialect: "postgres"},
		Databases: map[string]api.DatabaseConfig{
			"orders": {Type: "mysql", Environments: map[string]api.EnvironmentConfig{
				"staging": {DSN: "root@tcp(localhost)/orders"},
			}},
		},
		ApplyNotifications: notifications.Config{
			Publisher: "sqs", QueueURL: "https://sqs.us-west-2.amazonaws.com/123456789012/outcomes",
		},
	}
	accepted := make(chan json.RawMessage, 1)
	factoryCalls := 0
	server, err := serve.Build(ctx, cfg, serve.WithLogger(slog.New(slog.DiscardHandler)),
		serve.WithPublisher("sqs", func(_ context.Context, destination notifications.Destination) (notifications.ResolvedPublisher, error) {
			factoryCalls++
			assert.Equal(t, "sqs", destination.Type)
			assert.Equal(t, cfg.ApplyNotifications.QueueURL, destination.Settings["queue_url"])
			return notifications.ResolvedPublisher{Identity: "host-outcomes", Publisher: acceptingHostPublisher{accepted}}, nil
		}))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, server.Close()) })
	require.Equal(t, 1, factoryCalls)

	store := server.Service().Storage()
	lock := storagetest.CreateLock(t, store, "orders", storage.DatabaseTypeMySQL)
	apply := &storage.Apply{ApplyIdentifier: "apply_host_publisher", LockID: lock.ID,
		Database: "orders", DatabaseType: storage.DatabaseTypeMySQL,
		Environment: "staging", State: state.Apply.Running}
	apply.ID, err = store.Applies().Create(ctx, apply)
	require.NoError(t, err)
	now := time.Now()
	apply.State, apply.CompletedAt = state.Apply.Completed, &now
	require.NoError(t, store.Applies().Update(ctx, apply))
	pending, err := store.OutboxEvents().ListUnpublished(ctx, 0, 100)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	server.Start(ctx)
	select {
	case payload := <-accepted:
		assert.JSONEq(t, string(pending[0].Payload), string(payload))
		var event storage.ApplyFinishedEvent
		require.NoError(t, json.Unmarshal(payload, &event))
		assert.Equal(t, "apply_host_publisher", event.ID)
		assert.Equal(t, "apply.finished", event.Type)
		assert.Equal(t, state.Apply.Completed, event.Outcome)
	case <-ctx.Done():
		require.FailNow(t, "host publisher did not receive the pending outcome before the startup deadline")
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var status string
		var publishedAt sql.NullTime
		err := db.QueryRowContext(ctx, "SELECT status, published_at FROM outbox_events WHERE id = $1", pending[0].ID).Scan(&status, &publishedAt)
		require.NoError(c, err)
		assert.Equal(c, storage.OutboxStatusPublished, status)
		assert.True(c, publishedAt.Valid)
	}, 5*time.Second, 10*time.Millisecond)
}
