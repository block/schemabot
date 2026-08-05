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

func durablePushEvent() *storage.WebhookEvent {
	payload := []byte(`{
		"ref": "refs/heads/main",
		"after": "pushsha123",
		"deleted": false,
		"repository": {"full_name": "octocat/hello-world", "default_branch": "main"},
		"installation": {"id": 12345}
	}`)
	return &storage.WebhookEvent{
		Provider:   storage.ProviderGitHub,
		DeliveryID: "delivery-push-1",
		Event:      "push",
		Repository: "octocat/hello-world",
		HeadSHA:    "pushsha123",
		TenantID:   "12345",
		Payload:    payload,
	}
}

// With durable dispatch enabled, a default-branch push is persisted and ACKed
// fast: no GitHub client is created on the request path, and a leased driver
// posts the ruleset-source check later, surviving a restart mid-post.
func TestDurablePushWebhookQueuesAndAcks(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
	}, nil, logger)
	clientFactory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := NewHandler(service, clientFactory, nil, logger, WithDurableWebhookDispatch())

	req := buildPushWebhookRequest(t, "refs/heads/main", "pushsha123", false)
	req.Header.Set(headerDeliveryID, "delivery-push-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"default-branch check queued"}`, rr.Body.String())

	event, err := events.GetByDeliveryID(t.Context(), storage.ProviderGitHub, "delivery-push-1")
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, "push", event.Event)
	require.Equal(t, "octocat/hello-world", event.Repository)
	require.Equal(t, "pushsha123", event.HeadSHA)
	require.Equal(t, "12345", event.TenantID)
	require.NotEmpty(t, event.Payload)

	select {
	case <-clientFactory.forInstallationStarted:
		t.Fatal("durable request path should not create a GitHub client")
	default:
	}
}

// A non-default-branch push is filtered before enqueue, so no inbox row is
// created — only default-branch commits keep the ruleset check source current.
func TestDurablePushWebhookIgnoresFeatureBranch(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
	}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	req := buildPushWebhookRequest(t, "refs/heads/feature-branch", "pushsha123", false)
	req.Header.Set(headerDeliveryID, "delivery-push-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	events.mu.Lock()
	defer events.mu.Unlock()
	require.Empty(t, events.events, "a feature-branch push must not be enqueued")
}

// The driver posts the passing default-branch check on the pushed commit for
// each gated environment, then marks the delivery completed.
func TestDurablePushDriverPostsChecks(t *testing.T) {
	client, mux := setupGitHubServer(t)
	created := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var c checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
		created <- c
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 555})
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())

	store := newScriptedWebhookEventStore(durablePushEvent())
	config := &api.ServerConfig{
		AllowedEnvironments: []string{"production"},
		Repos:               map[string]api.RepoConfig{"octocat/hello-world": {}},
	}
	h := newDurableDriverHandler(t, store, config, &fakeClientFactory{client: installClient})

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case c := <-created:
		assert.Equal(t, "SchemaBot (production)", c.Name)
		assert.Equal(t, "pushsha123", c.HeadSHA)
		assert.Equal(t, "success", c.Conclusion)
	case <-time.After(durableWebhookTestDeadline):
		t.Fatal("expected the driver to post a default-branch check")
	}
	select {
	case <-store.completed:
	case <-time.After(durableWebhookTestDeadline):
		t.Fatal("expected the delivery to be marked completed")
	}
	require.Empty(t, store.failed)
}

// A push delivery for a repo this deployment does not manage completes as a
// no-op — there is no check source to maintain — without creating a client.
func TestDurablePushDriverCompletesUnregisteredRepo(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePushEvent())
	config := &api.ServerConfig{Repos: map[string]api.RepoConfig{"other/repo": {}}}
	factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := newDurableDriverHandler(t, store, config, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected unregistered-repo push delivery to be marked completed")
	}
	require.Empty(t, store.failed)
	select {
	case <-factory.forInstallationStarted:
		t.Fatal("unregistered repo must not create a GitHub client")
	default:
	}
}

// A push row with an empty repo or an empty head SHA is a corrupted/replayed
// row (the deletion sentinel is an all-zeros SHA, not an empty one), so the
// driver fails it terminally — even when the corruption also blanks the fields
// a routine skip would match, such as the ref and default branch — rather than
// silently completing it as a non-default-branch or unregistered-repo skip.
func TestDurablePushDriverFailsMalformedRowTerminally(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{
			name: "empty head SHA",
			payload: `{
				"ref": "refs/heads/main",
				"after": "",
				"deleted": false,
				"repository": {"full_name": "octocat/hello-world", "default_branch": "main"},
				"installation": {"id": 12345}
			}`,
		},
		{
			name: "empty head SHA with empty ref and default branch",
			payload: `{
				"ref": "",
				"after": "",
				"deleted": false,
				"repository": {"full_name": "octocat/hello-world", "default_branch": ""},
				"installation": {"id": 12345}
			}`,
		},
		{
			name: "empty repo",
			payload: `{
				"ref": "refs/heads/main",
				"after": "abc123def456",
				"deleted": false,
				"repository": {"full_name": "", "default_branch": "main"},
				"installation": {"id": 12345}
			}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := durablePushEvent()
			event.Payload = []byte(tt.payload)
			store := newScriptedWebhookEventStore(event)
			config := &api.ServerConfig{Repos: map[string]api.RepoConfig{"octocat/hello-world": {}}}
			h := newDurableDriverHandler(t, store, config, &fakeClientFactory{})

			h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

			select {
			case failure := <-store.failed:
				require.Nil(t, failure.retryAfter, "a malformed push row must not be retried")
				require.Contains(t, failure.errMsg, "missing repo or head SHA")
			default:
				t.Fatal("expected malformed push delivery to be marked failed")
			}
			require.Empty(t, store.completed)
		})
	}
}

// A GitHub client failure while posting the check is retryable so the ruleset
// check source is refreshed rather than left to age until the next push.
func TestDurablePushDriverRetriesClientFailure(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePushEvent())
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
