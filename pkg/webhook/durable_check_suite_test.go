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

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// durableCheckSuiteRerequestEvent returns a claimable check_suite rerequest
// inbox row, mirroring what the HTTP path enqueues for the GitHub UI's
// "Re-run all checks".
func durableCheckSuiteRerequestEvent(t *testing.T) *storage.WebhookEvent {
	t.Helper()
	payload := []byte(`{
		"action": "rerequested",
		"check_suite": {"id": 456, "head_sha": "head-sha", "pull_requests": [{"number": 7}]},
		"repository": {"full_name": "octocat/hello-world"},
		"installation": {"id": 12345}
	}`)
	return &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  "delivery-check-suite-1",
		Event:       "check_suite",
		Action:      "rerequested",
		Repository:  "octocat/hello-world",
		PullRequest: 7,
		HeadSHA:     "head-sha",
		TenantID:    "12345",
		Payload:     payload,
	}
}

// With durable dispatch enabled, a check_suite rerequest ("Re-run all checks",
// the only re-run affordance on a merged PR) acks fast by persisting an inbox
// row rather than running auto-plan synchronously on the request path, so a
// deploy mid-run cannot drop the re-plan.
func TestDurableCheckSuiteRerequestQueuesAndAcks(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{}, nil, logger)
	clientFactory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := NewHandler(service, clientFactory, nil, logger, WithDurableWebhookDispatch())

	req := buildCheckSuiteWebhookRequest(t, checkSuiteWebhookPayloadOpts{action: "rerequested", headSHA: "head-sha", pr: 7}, nil)
	req.Header.Set(headerDeliveryID, "delivery-check-suite-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"check_suite rerequest queued"}`, rr.Body.String())
	event, err := events.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-check-suite-1")
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, "check_suite", event.Event)
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

// A redelivered check_suite rerequest (same delivery GUID) is deduplicated to a
// single inbox row.
func TestDurableCheckSuiteRerequestDeduplicatesDelivery(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	for range 2 {
		req := buildCheckSuiteWebhookRequest(t, checkSuiteWebhookPayloadOpts{action: "rerequested"}, nil)
		req.Header.Set(headerDeliveryID, "delivery-check-suite-1")
		rr := httptest.NewRecorder()

		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
	}

	events.mu.Lock()
	defer events.mu.Unlock()
	require.Len(t, events.events, 1)
}

// A check_suite rerequest without an associated pull request (e.g. a suite on
// a branch with no open PR) has nothing to re-plan, so it is acknowledged
// before enqueue and no inbox row is created.
func TestDurableCheckSuiteRerequestIgnoresWithoutPullRequest(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	req := buildCheckSuiteWebhookRequest(t, checkSuiteWebhookPayloadOpts{action: "rerequested", noPR: true}, nil)
	req.Header.Set(headerDeliveryID, "delivery-check-suite-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"check_suite rerequest ignored without pull request"}`, rr.Body.String())
	events.mu.Lock()
	defer events.mu.Unlock()
	require.Empty(t, events.events, "a PR-less check_suite rerequest must not be enqueued")
}

// check_suite actions other than rerequested (created, completed) fire on
// every commit as part of the normal check lifecycle and carry no operator
// intent, so the request path acknowledges them without enqueueing work.
func TestCheckSuiteNonRerequestActionIgnored(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	req := buildCheckSuiteWebhookRequest(t, checkSuiteWebhookPayloadOpts{action: "completed"}, nil)
	req.Header.Set(headerDeliveryID, "delivery-check-suite-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"check_suite action ignored"}`, rr.Body.String())
	events.mu.Lock()
	defer events.mu.Unlock()
	require.Empty(t, events.events, "a non-rerequested check_suite action must not be enqueued")
}

// A claimed check_suite delivery whose action is not rerequested (only
// rerequested is enqueued today) is ignored and completed rather than retried,
// so a replayed or future-producer row cannot wedge the queue.
func TestDurableCheckSuiteDriverCompletesUnsupportedAction(t *testing.T) {
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-check-suite-completed",
		Event:      "check_suite",
		Action:     "completed",
		Payload:    []byte(`{"action":"completed"}`),
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case outcome := <-store.completed:
		require.Equal(t, int64(1), outcome.id)
		require.Equal(t, "token-1", outcome.leaseToken)
	default:
		t.Fatal("expected unsupported check_suite action to be marked completed")
	}
	require.Empty(t, store.failed)
}

// A malformed check_suite payload cannot be decoded, so the driver fails it
// terminally (no retry) rather than crash-looping the fleet on a poison row.
func TestDurableCheckSuiteDriverFailsMalformedTerminally(t *testing.T) {
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-check-suite-malformed",
		Event:      "check_suite",
		Payload:    []byte(`{not json`),
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.Nil(t, failure.retryAfter, "malformed payload must not be retried")
		require.Contains(t, failure.errMsg, "decode durable check_suite delivery")
	default:
		t.Fatal("expected malformed check_suite event to be marked failed")
	}
	require.Empty(t, store.completed)
}

// A check_suite rerequest for a repo outside the allowlist is completed without
// running auto-plan or creating a GitHub client.
func TestDurableCheckSuiteDriverCompletesUnregisteredRepo(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckSuiteRerequestEvent(t))
	config := &api.ServerConfig{Repos: map[string]api.RepoConfig{"other/repo": {}}}
	factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := newDurableDriverHandler(t, store, config, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected unregistered-repo check_suite event to be marked completed")
	}
	require.Empty(t, store.failed)
	select {
	case <-factory.forInstallationStarted:
		t.Fatal("unregistered repo must not create a GitHub client")
	default:
	}
}

// A durable check_suite rerequest whose head is no longer the PR's current head
// is completed without running discovery: planning the stale head would list
// the current PR's files against a stale config snapshot.
func TestDurableCheckSuiteDriverCompletesStaleHead(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckSuiteRerequestEvent(t))
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
		t.Fatal("expected a stale-head check_suite rerequest to be marked completed")
	}
	require.Empty(t, store.failed)
	require.Equal(t, int64(0), fileListCalls.Load(), "a stale rerequest must stop before config discovery")
}

// A durable check_suite rerequest for a closed (merged) PR is completed without
// running discovery: closed PRs are read-only history — apply commands are
// rejected on them and PR-close cleanup removed their stored check state — so
// a re-plan would recreate deleted state and post a plan nobody can act on.
func TestDurableCheckSuiteDriverCompletesClosedPR(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckSuiteRerequestEvent(t))
	ghClient, mux := setupGitHubServer(t)
	serveMergedCheckRunPR(t, mux, "head-sha")
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
		t.Fatal("expected a closed-PR check_suite rerequest to be marked completed")
	}
	require.Empty(t, store.failed)
	require.Equal(t, int64(0), fileListCalls.Load(), "a closed-PR rerequest must stop before config discovery")
}

// A GitHub failure verifying the current head is uncertainty, not staleness, so
// the delivery stays retryable rather than silently completing and dropping the
// re-plan.
func TestDurableCheckSuiteDriverRetriesHeadVerificationFailure(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckSuiteRerequestEvent(t))
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
		require.Contains(t, failure.errMsg, "verify re-run target for durable check_suite rerequest")
	default:
		t.Fatal("expected head-verification failure to be marked failed")
	}
	require.Empty(t, store.completed)
}

// A durable check_suite rerequest whose head is still current proceeds through
// discovery and completes: a no-schema PR has nothing to plan, so the delivery
// is done once auto-plan runs.
func TestDurableCheckSuiteDriverReplansCurrentHead(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckSuiteRerequestEvent(t))
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
		t.Fatal("expected a current-head check_suite rerequest to be marked completed")
	}
	require.Empty(t, store.failed)
	require.Equal(t, int64(1), fileListCalls.Load(), "a current rerequest must run config discovery")
}

// A bootstrap failure on a valid check_suite rerequest stays retryable so a
// transient installation-token outage does not drop the re-plan.
func TestDurableCheckSuiteDriverRetriesBootstrapFailure(t *testing.T) {
	store := newScriptedWebhookEventStore(durableCheckSuiteRerequestEvent(t))
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
