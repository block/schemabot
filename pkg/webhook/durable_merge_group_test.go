package webhook

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

func durableMergeGroupEvent() *storage.WebhookEvent {
	payload := []byte(`{
		"action": "checks_requested",
		"merge_group": {"head_sha": "mergesha123"},
		"repository": {"full_name": "octocat/hello-world"},
		"installation": {"id": 12345}
	}`)
	return &storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-mg-1",
		Event:      "merge_group",
		Action:     "checks_requested",
		Repository: "octocat/hello-world",
		HeadSHA:    "mergesha123",
		TenantID:   "12345",
		Payload:    payload,
	}
}

// With durable dispatch enabled, a merge_group delivery is persisted and ACKed
// fast: no GitHub client is created on the request path, and a leased driver
// posts the check later. This closes the window where a restart mid-post drops
// a merge-queue check and wedges the queue.
func TestDurableMergeGroupWebhookQueuesAndAcks(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
	}, nil, logger)
	clientFactory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := NewHandler(service, clientFactory, nil, logger, WithDurableWebhookDispatch())

	req := buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil)
	req.Header.Set(headerDeliveryID, "delivery-mg-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"merge_group check queued"}`, rr.Body.String())

	event, err := events.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-mg-1")
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, "merge_group", event.Event)
	require.Equal(t, "checks_requested", event.Action)
	require.Equal(t, "octocat/hello-world", event.Repository)
	require.Equal(t, "mergesha123", event.HeadSHA)
	require.Equal(t, "12345", event.TenantID)
	require.NotEmpty(t, event.Payload)

	select {
	case <-clientFactory.forInstallationStarted:
		t.Fatal("durable request path should not create a GitHub client")
	default:
	}
}

// A webhook redelivery reuses the delivery GUID, so the inbox deduplicates it to
// a single row rather than queuing the same merge-group check twice.
func TestDurableMergeGroupWebhookDeduplicatesDelivery(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
	}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	for range 2 {
		req := buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil)
		req.Header.Set(headerDeliveryID, "delivery-mg-1")
		rr := httptest.NewRecorder()

		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
	}

	events.mu.Lock()
	defer events.mu.Unlock()
	require.Len(t, events.events, 1)
}

// The driver posts the passing aggregate check on the merge-group head for each
// gated environment, then marks the delivery completed.
func TestDurableMergeGroupDriverPostsChecks(t *testing.T) {
	client, mux := setupGitHubServer(t)
	created := make(chan createdCheckRun, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var c createdCheckRun
		require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
		created <- c
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 555})
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())

	store := newScriptedWebhookEventStore(durableMergeGroupEvent())
	config := &api.ServerConfig{
		AllowedEnvironments: []string{"production"},
		Repos:               map[string]api.RepoConfig{"octocat/hello-world": {}},
	}
	h := newDurableDriverHandler(t, store, config, &fakeClientFactory{client: installClient})

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case c := <-created:
		assert.Equal(t, "SchemaBot (production)", c.Name)
		assert.Equal(t, "mergesha123", c.HeadSHA)
		assert.Equal(t, "success", c.Conclusion)
	case <-time.After(durableWebhookTestDeadline):
		t.Fatal("expected the driver to post a merge_group check")
	}
	select {
	case <-store.completed:
	case <-time.After(durableWebhookTestDeadline):
		t.Fatal("expected the delivery to be marked completed")
	}
	require.Empty(t, store.failed)
}

// A merge_group delivery for a repo this deployment does not manage completes as
// a no-op — its check is not required there — without creating a GitHub client.
func TestDurableMergeGroupDriverCompletesUnregisteredRepo(t *testing.T) {
	store := newScriptedWebhookEventStore(durableMergeGroupEvent())
	config := &api.ServerConfig{Repos: map[string]api.RepoConfig{"other/repo": {}}}
	factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := newDurableDriverHandler(t, store, config, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected unregistered-repo merge_group delivery to be marked completed")
	}
	require.Empty(t, store.failed)
	select {
	case <-factory.forInstallationStarted:
		t.Fatal("unregistered repo must not create a GitHub client")
	default:
	}
}

// An aggregate participant stays silent on merge-group commits — the leader
// posts the required check — so the driver completes the delivery as a no-op.
func TestDurableMergeGroupDriverParticipantStaysSilent(t *testing.T) {
	store := newScriptedWebhookEventStore(durableMergeGroupEvent())
	config := &api.ServerConfig{
		AllowedEnvironments: []string{"production"},
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {
			Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant},
		}},
	}
	factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := newDurableDriverHandler(t, store, config, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected participant merge_group delivery to be marked completed")
	}
	require.Empty(t, store.failed)
	select {
	case <-factory.forInstallationStarted:
		t.Fatal("participant must not create a GitHub client for a merge_group check")
	default:
	}
}

// A GitHub client failure while posting the check is retryable: the merge queue
// blocks until the check lands, so the delivery must be retried, not dropped.
func TestDurableMergeGroupDriverRetriesClientFailure(t *testing.T) {
	store := newScriptedWebhookEventStore(durableMergeGroupEvent())
	config := &api.ServerConfig{Repos: map[string]api.RepoConfig{"octocat/hello-world": {}}}
	factory := &fakeClientFactory{forInstallationErr: errors.New("installation token unavailable")}
	h := newDurableDriverHandler(t, store, config, factory)

	before := time.Now()
	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.NotNil(t, failure.retryAfter, "client failure must stay retryable")
		require.True(t, failure.retryAfter.After(before), "retry must be scheduled in the future")
		require.Contains(t, failure.errMsg, "installation token unavailable")
	default:
		t.Fatal("expected client failure to be marked failed")
	}
	require.Empty(t, store.completed)
}
