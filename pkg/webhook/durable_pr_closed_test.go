package webhook

import (
	"fmt"
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
	return durableClosedEventWithMerged(true)
}

func durableClosedEventWithMerged(merged bool) *storage.WebhookEvent {
	payload := fmt.Appendf(nil, `{
		"action": "closed",
		"pull_request": {"number": 1, "merged": %t, "head": {"sha": "head-sha", "ref": "feature"}},
		"repository": {"full_name": "octocat/hello-world"},
		"installation": {"id": 12345}
	}`, merged)
	return &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
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

	event, err := events.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-closed-1")
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

// A close delivery retried after the PR was reopened must not undo the
// reopened PR's state: the operator took a lock owned by repo#pr after the
// reopen, so the driver sees the live PR is open, releases and deletes
// nothing, and completes the stale delivery instead of retrying it.
func TestDurablePRClosedDriverSkipsCleanupForReopenedPR(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders")}}
	checkStore := &prClosedCheckStore{}
	scripted := newScriptedWebhookEventStore(durableClosedEventWithMerged(false))
	st := &durableClosedTestStorage{
		webhookEvents: scripted,
		locks:         lockStore,
		applies:       &prClosedApplyStore{},
		checks:        checkStore,
	}
	factory, fetches := prClosedGitHub(t, "open", 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(st, &api.ServerConfig{}, nil, logger)
	h := NewHandler(service, factory, nil, logger, WithDurableWebhookDispatch())

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Equal(t, int32(1), fetches.Load(), "the driver reads the live PR once")
	require.Empty(t, lockStore.released, "the lock taken after the reopen must stay held")
	require.Equal(t, 0, checkStore.deleteCalls, "stored check state must survive the stale close delivery")
	select {
	case <-scripted.completed:
	default:
		t.Fatal("expected the stale close delivery to be marked completed")
	}
	require.Empty(t, scripted.failed)
}

// A close delivery whose live PR read fails fails closed — nothing is released
// or deleted — and stays retryable, so the driver re-checks the PR on the next
// attempt instead of cleaning up on an unverified state.
func TestDurablePRClosedDriverRetriesWhenLivePRReadFails(t *testing.T) {
	lockStore := &prClosedLockStore{locks: []*storage.Lock{prClosedLock("orders")}}
	checkStore := &prClosedCheckStore{}
	scripted := newScriptedWebhookEventStore(durableClosedEventWithMerged(false))
	st := &durableClosedTestStorage{
		webhookEvents: scripted,
		locks:         lockStore,
		applies:       &prClosedApplyStore{},
		checks:        checkStore,
	}
	factory, _ := prClosedGitHub(t, "closed", http.StatusForbidden)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(st, &api.ServerConfig{}, nil, logger)
	h := NewHandler(service, factory, nil, logger, WithDurableWebhookDispatch())

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, lockStore.released, "no lock is released when the live PR state is unknown")
	require.Equal(t, 0, checkStore.deleteCalls, "no check state is deleted when the live PR state is unknown")
	select {
	case failure := <-scripted.failed:
		require.NotNil(t, failure.retryAfter, "a failed live PR read must stay retryable")
	default:
		t.Fatal("expected the close delivery to be marked failed and retryable")
	}
	require.Empty(t, scripted.completed)
}
