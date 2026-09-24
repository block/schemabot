package webhook

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// durableClosedTestStorage provides the webhook inbox plus the lock/apply/check
// stores runPRCloseCleanup reads, so the driver can drive a claimed close
// delivery end to end.
type durableClosedTestStorage struct {
	emptyStorage
	webhookEvents storage.WebhookEventStore
	locks         *prClosedLockStore
	applies       storage.ApplyStore
	checks        *prClosedCheckStore
}

func (s *durableClosedTestStorage) WebhookEvents() storage.WebhookEventStore { return s.webhookEvents }
func (s *durableClosedTestStorage) Locks() storage.LockStore                 { return s.locks }
func (s *durableClosedTestStorage) Applies() storage.ApplyStore              { return s.applies }
func (s *durableClosedTestStorage) Checks() storage.CheckStore               { return s.checks }

func durableClosedEvent() *storage.WebhookEvent {
	payload := []byte(`{
		"action": "closed",
		"pull_request": {"number": 1, "merged": true, "head": {"sha": "head-sha", "ref": "feature"}},
		"repository": {"full_name": "octocat/hello-world"},
		"installation": {"id": 12345}
	}`)
	return &storage.WebhookEvent{
		Provider:    storage.ProviderGitHub,
		DeliveryID:  "delivery-closed-1",
		Event:       "pull_request",
		Action:      "closed",
		Repository:  "octocat/hello-world",
		PullRequest: 1,
		HeadSHA:     "head-sha",
		TenantID:    "12345",
		Payload:     payload,
	}
}

// With durable dispatch enabled, a PR-close delivery is persisted and ACKed
// fast: a leased driver runs the lock/check cleanup later, so a restart mid
// cleanup cannot drop the delivery and leave a lock held or stale check state.
func TestDurablePRClosedWebhookQueuesAndAcks(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
	}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "closed", merged: true}, nil)
	req.Header.Set(headerDeliveryID, "delivery-closed-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"PR close cleanup queued"}`, rr.Body.String())

	event, err := events.GetByDeliveryID(t.Context(), storage.ProviderGitHub, "delivery-closed-1")
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, "pull_request", event.Event)
	require.Equal(t, "closed", event.Action)
	require.Equal(t, 1, event.PullRequest)
}

// The driver runs PR-close cleanup for a claimed delivery: with all applies
// terminal it releases the locks, deletes stored check state, and completes.
func TestDurablePRClosedDriverRunsCleanup(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders")}}
	checkStore := &prClosedCheckStore{}
	scripted := newScriptedWebhookEventStore(durableClosedEvent())
	st := &durableClosedTestStorage{
		webhookEvents: scripted,
		locks:         lockStore,
		applies:       &prClosedApplyStore{applies: []*storage.Apply{prClosedApply("orders", state.Apply.Completed)}},
		checks:        checkStore,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(st, &api.ServerConfig{}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Equal(t, []string{"orders"}, lockStore.released, "the driver must release locks for a merged closed PR")
	require.Equal(t, 1, checkStore.deleteCalls, "the driver must delete stored check state")
	require.Equal(t, []bool{true}, checkStore.deleteMerged, "the delete must run in merged mode")
	select {
	case <-scripted.completed:
	default:
		t.Fatal("expected the close delivery to be marked completed")
	}
	require.Empty(t, scripted.failed)
}

// A failed apply lookup fails closed — nothing is released or deleted — and the
// delivery stays retryable so a transient storage blip does not permanently
// skip cleanup and leave a lock stranded.
func TestDurablePRClosedDriverRetriesOnApplyLookupFailure(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders")}}
	checkStore := &prClosedCheckStore{}
	scripted := newScriptedWebhookEventStore(durableClosedEvent())
	st := &durableClosedTestStorage{
		webhookEvents: scripted,
		locks:         lockStore,
		applies:       &failingPRApplyLookupStore{},
		checks:        checkStore,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(st, &api.ServerConfig{}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, lockStore.released, "no lock is released when apply state is unknown")
	require.Equal(t, 0, checkStore.deleteCalls, "no check state is deleted when apply state is unknown")
	select {
	case failure := <-scripted.failed:
		require.NotNil(t, failure.retryAfter, "a failed apply lookup must stay retryable")
	default:
		t.Fatal("expected the close delivery to be marked failed and retryable")
	}
	require.Empty(t, scripted.completed)
}
