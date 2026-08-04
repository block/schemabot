package webhook

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// durableCheckRunRerequestEvent returns a claimable check_run rerequest inbox
// row for the default SchemaBot aggregate check, mirroring what the HTTP path
// enqueues.
func durableCheckRunRerequestEvent(t *testing.T) *storage.WebhookEvent {
	t.Helper()
	payload := []byte(`{
		"action": "rerequested",
		"check_run": {"id": 123, "name": "SchemaBot", "head_sha": "head-sha", "pull_requests": [{"number": 7}]},
		"repository": {"full_name": "octocat/hello-world"},
		"installation": {"id": 12345}
	}`)
	return &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  "delivery-check-run-1",
		Event:       "check_run",
		Action:      "rerequested",
		Repository:  "octocat/hello-world",
		PullRequest: 7,
		HeadSHA:     "head-sha",
		TenantID:    "12345",
		Payload:     payload,
	}
}

// durableCheckRunCompletedEvent returns a claimable check_run completion inbox
// row for a participant check on a repo the deployment leads, mirroring what the
// HTTP path enqueues.
func durableCheckRunCompletedEvent(t *testing.T) *storage.WebhookEvent {
	t.Helper()
	payload := []byte(`{
		"action": "completed",
		"check_run": {"id": 123, "name": "SchemaBot Tenant B", "status": "completed", "conclusion": "success", "head_sha": "head-sha", "pull_requests": [{"number": 7}]},
		"repository": {"full_name": "octocat/hello-world"},
		"installation": {"id": 12345}
	}`)
	return &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  "delivery-check-run-completed-1",
		Event:       "check_run",
		Action:      "completed",
		Repository:  "octocat/hello-world",
		PullRequest: 7,
		HeadSHA:     "head-sha",
		TenantID:    "12345",
		Payload:     payload,
	}
}

// With durable dispatch enabled, a participant check_run completion on a led
// repo acks fast by persisting an inbox row rather than re-folding the aggregate
// synchronously on the request path, so a deploy mid-fold cannot drop the
// leader's aggregate re-fold.
func TestDurableCheckRunCompletionQueuesAndAcks(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, aggregateLeaderConfig(), nil, logger)
	clientFactory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := NewHandler(service, clientFactory, nil, logger, WithDurableWebhookDispatch())

	req := buildCheckRunWebhookRequest(t, checkRunWebhookPayloadOpts{action: "completed", checkName: "SchemaBot Tenant B", headSHA: "head-sha", pr: 7}, nil)
	req.Header.Set(headerDeliveryID, "delivery-check-run-completed-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"check_run completion queued"}`, rr.Body.String())
	event, err := events.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-check-run-completed-1")
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, "check_run", event.Event)
	require.Equal(t, "completed", event.Action)
	require.Equal(t, "octocat/hello-world", event.Repository)
	require.Equal(t, 7, event.PullRequest)
	require.Equal(t, "head-sha", event.HeadSHA)
	require.Equal(t, "12345", event.TenantID)
	require.NotEmpty(t, event.Payload)

	select {
	case <-clientFactory.forInstallationStarted:
		t.Fatal("durable request path should not create a GitHub client")
	default:
	}
}

// A redelivered participant check_run completion (same delivery GUID) is
// deduplicated to a single inbox row.
func TestDurableCheckRunCompletionDeduplicatesDelivery(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, aggregateLeaderConfig(), nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	for range 2 {
		req := buildCheckRunWebhookRequest(t, checkRunWebhookPayloadOpts{action: "completed", checkName: "SchemaBot Tenant B", headSHA: "head-sha", pr: 7}, nil)
		req.Header.Set(headerDeliveryID, "delivery-check-run-completed-1")
		rr := httptest.NewRecorder()

		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
	}

	events.mu.Lock()
	defer events.mu.Unlock()
	require.Len(t, events.events, 1)
}

// A participant completion for a repo this deployment does not lead is rejected
// before enqueue, so no inbox row is created even with durable dispatch enabled.
func TestDurableCheckRunCompletionSkipsNonLeaderRepo(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{Repos: map[string]api.RepoConfig{"octocat/hello-world": {}}}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	req := buildCheckRunWebhookRequest(t, checkRunWebhookPayloadOpts{action: "completed", checkName: "SchemaBot Tenant B", headSHA: "head-sha", pr: 7}, nil)
	req.Header.Set(headerDeliveryID, "delivery-check-run-completed-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	events.mu.Lock()
	defer events.mu.Unlock()
	require.Empty(t, events.events, "a completion on a non-leader repo must not be enqueued")
}

// With durable dispatch enabled, a check_run rerequest for a SchemaBot
// aggregate check acks fast by persisting an inbox row rather than running
// auto-plan synchronously on the request path, so a deploy mid-run cannot drop
// the re-plan.
func TestDurableCheckRunRerequestQueuesAndAcks(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{}, nil, logger)
	clientFactory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := NewHandler(service, clientFactory, nil, logger, WithDurableWebhookDispatch())

	req := buildCheckRunWebhookRequest(t, checkRunWebhookPayloadOpts{action: "rerequested", headSHA: "head-sha", pr: 7}, nil)
	req.Header.Set(headerDeliveryID, "delivery-check-run-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"check_run rerequest queued"}`, rr.Body.String())
	event, err := events.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-check-run-1")
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, "check_run", event.Event)
	require.Equal(t, "rerequested", event.Action)
	require.Equal(t, "octocat/hello-world", event.Repository)
	require.Equal(t, 7, event.PullRequest)
	require.Equal(t, "head-sha", event.HeadSHA)
	require.Equal(t, "12345", event.TenantID)
	require.NotEmpty(t, event.Payload)

	select {
	case <-clientFactory.forInstallationStarted:
		t.Fatal("durable request path should not create a GitHub client")
	default:
	}
}

// A redelivered check_run rerequest (same delivery GUID) is deduplicated to a
// single inbox row.
func TestDurableCheckRunRerequestDeduplicatesDelivery(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	for range 2 {
		req := buildCheckRunWebhookRequest(t, checkRunWebhookPayloadOpts{action: "rerequested"}, nil)
		req.Header.Set(headerDeliveryID, "delivery-check-run-1")
		rr := httptest.NewRecorder()

		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
	}

	events.mu.Lock()
	defer events.mu.Unlock()
	require.Len(t, events.events, 1)
}

// A rerequest for a check SchemaBot does not own is rejected before enqueue, so
// no inbox row is created even with durable dispatch enabled.
func TestDurableCheckRunRerequestSkipsNonSchemaBotCheck(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	req := buildCheckRunWebhookRequest(t, checkRunWebhookPayloadOpts{action: "rerequested", checkName: "Some Other Check"}, nil)
	req.Header.Set(headerDeliveryID, "delivery-check-run-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	events.mu.Lock()
	defer events.mu.Unlock()
	require.Empty(t, events.events, "a non-SchemaBot check rerequest must not be enqueued")
}

// A claimed check_run delivery whose action is neither rerequested nor completed
// is ignored and completed rather than retried, so a replayed or future-producer
// row cannot wedge the queue.
func TestDurableCheckRunDriverCompletesUnsupportedAction(t *testing.T) {
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-check-run-created",
		Event:      "check_run",
		Action:     "created",
		Payload:    []byte(`{"action":"created"}`),
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case outcome := <-store.completed:
		require.Equal(t, int64(1), outcome.id)
		require.Equal(t, "token-1", outcome.leaseToken)
	default:
		t.Fatal("expected unsupported check_run action to be marked completed")
	}
	require.Empty(t, store.failed)
}

// A malformed check_run payload cannot be decoded, so the driver fails it
// terminally (no retry) rather than crash-looping the fleet on a poison row.
func TestDurableCheckRunDriverFailsMalformedTerminally(t *testing.T) {
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-check-run-malformed",
		Event:      "check_run",
		Payload:    []byte(`{not json`),
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.Nil(t, failure.retryAfter, "malformed payload must not be retried")
		require.Contains(t, failure.errMsg, "decode durable check_run delivery")
	default:
		t.Fatal("expected malformed check_run event to be marked failed")
	}
	require.Empty(t, store.completed)
}

// A check_run rerequest for a repo outside the allowlist is completed without
// running auto-plan or creating a GitHub client.
func TestDurableCheckRunDriverCompletesUnregisteredRepo(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckRunRerequestEvent(t))
	config := &api.ServerConfig{Repos: map[string]api.RepoConfig{"other/repo": {}}}
	factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := newDurableDriverHandler(t, store, config, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected unregistered-repo check_run event to be marked completed")
	}
	require.Empty(t, store.failed)
	select {
	case <-factory.forInstallationStarted:
		t.Fatal("unregistered repo must not create a GitHub client")
	default:
	}
}

// A check_run rerequest for a check SchemaBot does not own is completed without
// running auto-plan — the driver re-validates the same guard the request path
// applies.
func TestDurableCheckRunDriverCompletesNonSchemaBotCheck(t *testing.T) {
	event := durableCheckRunRerequestEvent(t)
	event.Payload = []byte(`{
		"action": "rerequested",
		"check_run": {"id": 123, "name": "Some Other Check", "head_sha": "head-sha", "pull_requests": [{"number": 7}]},
		"repository": {"full_name": "octocat/hello-world"},
		"installation": {"id": 12345}
	}`)
	store := newScriptedWebhookEventStore(event)
	factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := newDurableDriverHandler(t, store, nil, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected non-SchemaBot check_run rerequest to be marked completed")
	}
	require.Empty(t, store.failed)
	select {
	case <-factory.forInstallationStarted:
		t.Fatal("non-SchemaBot check must not create a GitHub client")
	default:
	}
}

// serveCheckRunPRHead registers the PR lookup the driver's pre-dispatch head
// check performs, reporting currentHeadSHA as the PR's current head.
func serveCheckRunPRHead(t *testing.T, mux *http.ServeMux, currentHeadSHA string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"sha": currentHeadSHA, "ref": "feature-branch"},
			"base": map[string]any{"sha": "def456", "ref": "main"},
			"user": map[string]any{"login": "testuser"},
		}))
	})
}

// A durable rerequest whose head is no longer the PR's current head is completed
// without running discovery: planning the stale head would list the current
// PR's files against a stale config snapshot.
func TestDurableCheckRunDriverCompletesStaleHead(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckRunRerequestEvent(t))
	ghClient, mux := setupGitHubServer(t)
	serveCheckRunPRHead(t, mux, "newer-sha-999")
	var fileListCalls atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7/files", func(w http.ResponseWriter, _ *http.Request) {
		fileListCalls.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{}))
	})
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghClient, testLogger())}
	h := newDurableDriverHandler(t, store, nil, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected a stale-head rerequest to be marked completed")
	}
	require.Empty(t, store.failed)
	require.Equal(t, int64(0), fileListCalls.Load(), "a stale rerequest must stop before config discovery")
}

// A GitHub failure verifying the current head is uncertainty, not staleness, so
// the delivery stays retryable rather than silently completing and dropping the
// re-plan.
func TestDurableCheckRunDriverRetriesHeadVerificationFailure(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckRunRerequestEvent(t))
	ghClient, mux := setupGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghClient, testLogger())}
	h := newDurableDriverHandler(t, store, nil, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.NotNil(t, failure.retryAfter, "head-verification failure must stay retryable")
		require.Contains(t, failure.errMsg, "verify current head for durable check_run rerequest")
	default:
		t.Fatal("expected head-verification failure to be marked failed")
	}
	require.Empty(t, store.completed)
}

// A durable rerequest whose head is still current proceeds through discovery and
// completes: a no-schema PR has nothing to plan, so the delivery is done once
// auto-plan runs.
func TestDurableCheckRunDriverReplansCurrentHead(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckRunRerequestEvent(t))
	ghClient, mux := setupGitHubServer(t)
	serveCheckRunPRHead(t, mux, "head-sha")
	var fileListCalls atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7/files", func(w http.ResponseWriter, _ *http.Request) {
		fileListCalls.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{
			{"filename": "README.md", "status": "modified"},
		}))
	})
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghClient, testLogger())}
	h := newDurableDriverHandler(t, store, nil, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected a current-head rerequest to be marked completed")
	}
	require.Empty(t, store.failed)
	require.Equal(t, int64(1), fileListCalls.Load(), "a current rerequest must run config discovery")
}

// A bootstrap failure on a valid check_run rerequest stays retryable so a
// transient installation-token outage does not drop the re-plan.
func TestDurableCheckRunDriverRetriesBootstrapFailure(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckRunRerequestEvent(t))
	factory := &fakeClientFactory{forInstallationErr: errors.New("installation token unavailable")}
	h := newDurableDriverHandler(t, store, nil, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.NotNil(t, failure.retryAfter, "bootstrap failure must stay retryable")
		require.Contains(t, failure.errMsg, "installation token unavailable")
	default:
		t.Fatal("expected bootstrap failure to be marked failed")
	}
	require.Empty(t, store.completed)
}

// A claimed check_run completion for a repo this deployment does not lead is
// completed without folding (or creating a GitHub client): only the aggregate
// leader re-folds, so a non-leader row is inapplicable rather than retryable.
func TestDurableCheckRunDriverCompletionSkipsNonLeader(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckRunCompletedEvent(t))
	factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := newDurableDriverHandler(t, store, nil, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected a non-leader check_run completion to be marked completed")
	}
	require.Empty(t, store.failed)
	select {
	case <-factory.forInstallationStarted:
		t.Fatal("a non-leader completion must not create a GitHub client")
	default:
	}
}

// A GitHub failure while the leader re-folds is an operational error, not a
// terminal one, so the delivery stays retryable rather than silently dropping
// the aggregate re-fold.
func TestDurableCheckRunDriverCompletionRetriesFoldFailure(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckRunCompletedEvent(t))
	ghClient, mux := setupGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghClient, testLogger())}
	h := newDurableDriverHandler(t, store, aggregateLeaderConfig(), factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.NotNil(t, failure.retryAfter, "a fold failure must stay retryable")
		require.Contains(t, failure.errMsg, "re-fold aggregate for durable check_run completion")
	default:
		t.Fatal("expected a fold failure to be marked failed")
	}
	require.Empty(t, store.completed)
}

// The producer always stores a resolved positive installation ID in the row's
// tenant, so a completion whose tenant does not resolve is a corrupted or
// hand-crafted row: retrying cannot repair it, so the delivery fails
// terminally instead of burning retry attempts on a deterministic failure.
func TestDurableCheckRunDriverCompletionFailsUnresolvableTenantTerminally(t *testing.T) {
	tests := []struct {
		name     string
		tenantID string
		wantErr  string
	}{
		{name: "empty tenant", tenantID: "", wantErr: "unparseable tenant ID"},
		{name: "corrupted tenant", tenantID: "not-a-number", wantErr: "unparseable tenant ID"},
		{name: "negative tenant", tenantID: "-42", wantErr: "non-positive installation ID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := durableCheckRunCompletedEvent(t)
			event.TenantID = tt.tenantID
			store := newScriptedWebhookEventStore(event)
			factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
			h := newDurableDriverHandler(t, store, aggregateLeaderConfig(), factory)

			h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

			select {
			case failure := <-store.failed:
				require.Nil(t, failure.retryAfter, "an unresolvable persisted tenant must not be retried")
				require.Contains(t, failure.errMsg, tt.wantErr)
			default:
				t.Fatal("expected an unresolvable tenant to be marked failed")
			}
			require.Empty(t, store.completed)
			select {
			case <-factory.forInstallationStarted:
				t.Fatal("an unresolvable tenant must not create a GitHub client")
			default:
			}
		})
	}
}

// A leader's re-fold for a head that is no longer the PR's current head is
// completed without folding participant state: the fold core hands back a
// leader re-fold disposition, so a fresh pass against the then-current head is
// armed instead of retrying the superseded delivery.
func TestDurableCheckRunDriverCompletionCompletesStaleHeadAndArmsRefold(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckRunCompletedEvent(t))
	ghClient, mux := setupGitHubServer(t)
	serveCheckRunPRHead(t, mux, "newer-sha-999")
	var fileListCalls atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7/files", func(w http.ResponseWriter, _ *http.Request) {
		fileListCalls.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{}))
	})
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghClient, testLogger())}
	h := newDurableDriverHandler(t, store, aggregateLeaderConfig(), factory)
	// Keep the armed re-fold timer from firing inside the test process;
	// pending timers at process exit are inert.
	h.participantRefoldDelayOverride = time.Hour

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected a stale-head completion re-fold to be marked completed")
	}
	require.Empty(t, store.failed)
	require.Equal(t, int64(0), fileListCalls.Load(), "a stale-head fold must stop before participant discovery")

	h.participantRefoldMu.Lock()
	refoldAttempts := h.participantRefoldAttempts[participantRefoldKey("octocat/hello-world", 7)]
	h.participantRefoldMu.Unlock()
	require.Equal(t, 1, refoldAttempts, "a stale-head completion must arm a leader re-fold for the current head")
}

// A leader's re-fold on a current head runs discovery (proving the leader fold
// executed) and completes: a PR touching no participant paths has no aggregate
// to publish, so the delivery is done once the fold runs.
func TestDurableCheckRunDriverCompletionRefoldsCurrentHead(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckRunCompletedEvent(t))
	ghClient, mux := setupGitHubServer(t)
	serveCheckRunPRHead(t, mux, "head-sha")
	var fileListCalls atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7/files", func(w http.ResponseWriter, _ *http.Request) {
		fileListCalls.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{}))
	})
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghClient, testLogger())}
	h := newDurableDriverHandler(t, store, aggregateLeaderConfig(), factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected a current-head completion re-fold to be marked completed")
	}
	require.Empty(t, store.failed)
	require.Equal(t, int64(1), fileListCalls.Load(), "the leader re-fold must run participant discovery")
}
