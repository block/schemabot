package serve

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/notifications"
	"github.com/block/schemabot/pkg/storage"
)

type notificationLifecycleStore struct {
	storage.NotificationStore
	scans chan struct{}
}

func (s *notificationLifecycleStore) FindUnpublished(context.Context, time.Duration, int) ([]*storage.Apply, error) {
	select {
	case s.scans <- struct{}{}:
	default:
	}
	return nil, nil
}
func (s *notificationLifecycleStore) ObservationCeiling(context.Context) (int64, time.Time, error) {
	return 0, time.Now(), nil
}
func (s *notificationLifecycleStore) ObservePage(context.Context, int64, int64, int) ([]storage.NotificationObservation, error) {
	return nil, nil
}

type unusedPublisher struct{}

func (unusedPublisher) Publish(context.Context, notifications.Event) error { return nil }

func TestNotificationsLifecycleDoesNotRequireWebhook(t *testing.T) {
	store := &notificationLifecycleStore{scans: make(chan struct{}, 1)}
	runner, err := notifications.New(store, unusedPublisher{}, notifications.Config{}, slog.Default())
	require.NoError(t, err)
	server := &Server{notificationRunner: runner, logger: slog.Default()}
	// No webhook runtime, GitHub credentials, HTTP request or observer is present.
	server.startNotifications(t.Context())
	t.Cleanup(server.stopNotifications)
	select {
	case <-store.scans:
	case <-time.After(time.Second):
		require.FailNow(t, "startup catch-up did not run without GitHub")
	}
	done := server.notificationDone
	server.stopNotifications()
	select {
	case <-done:
	default:
		require.FailNow(t, "notification loop was not drained")
	}
	assert.Nil(t, server.notificationCancel)
	server.stopNotifications()
}

func TestUnsupportedPublisherFailsBuildBeforeStorage(t *testing.T) {
	_, err := Build(t.Context(), &api.ServerConfig{Notifications: notifications.Config{Type: "kafka"}})
	require.ErrorContains(t, err, "unsupported notifications.type")
}
