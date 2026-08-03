package webhook

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// durableIssueCommentPayload renders an issue_comment created payload carrying
// the given PR comment body, mirroring what GitHub delivers for a user comment
// on a pull request.
func durableIssueCommentPayload(comment string) []byte {
	return fmt.Appendf(nil, `{
		"action": "created",
		"issue": {"number": 7, "pull_request": {"url": "https://api.github.com/repos/octocat/hello-world/pulls/7"}},
		"comment": {"id": 42, "body": %q, "user": {"login": "testuser", "type": "User"}},
		"repository": {"full_name": "octocat/hello-world"},
		"installation": {"id": 12345}
	}`, comment)
}

// newDurableIssueCommentEnqueueHandler builds a durable-dispatch handler whose
// GitHub client points at a permissive stub server, so the synchronous
// acknowledgment reaction the request path posts before enqueueing does not
// dereference a nil client.
func newDurableIssueCommentEnqueueHandler(t *testing.T, events storage.WebhookEventStore) *Handler {
	t.Helper()
	client, mux := setupGitHubServer(t)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{}, nil, logger)
	return NewHandler(service, &fakeClientFactory{client: installClient}, nil, logger, WithDurableWebhookDispatch())
}

// durableIssueCommentEvent returns a claimable issue_comment inbox row for an
// apply command, mirroring what the HTTP path enqueues.
func durableIssueCommentEvent(comment string) *storage.WebhookEvent {
	return &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  "delivery-issue-comment-1",
		Event:       "issue_comment",
		Action:      "created",
		Repository:  "octocat/hello-world",
		PullRequest: 7,
		TenantID:    "12345",
		Payload:     durableIssueCommentPayload(comment),
	}
}

// With durable dispatch enabled, an apply or apply-confirm command acks fast
// by persisting an inbox row rather than dispatching an in-process goroutine,
// so a deploy after the ACK cannot drop the acknowledged command.
func TestDurableIssueCommentCommandQueuesAndAcks(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		message string
	}{
		{name: "apply", comment: "schemabot apply -e production", message: "apply queued"},
		{name: "apply-confirm", comment: "schemabot apply-confirm -e production", message: "apply-confirm queued"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := newRecordingWebhookEventStore()
			h := newDurableIssueCommentEnqueueHandler(t, events)

			req := buildWebhookRequest(t, webhookPayloadOpts{comment: tt.comment, isPR: true}, nil)
			req.Header.Set(headerDeliveryID, "delivery-issue-comment-1")
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)
			h.DrainInProcessWebhookWork(t.Context())

			require.Equal(t, http.StatusOK, rr.Code)
			require.JSONEq(t, fmt.Sprintf(`{"message":%q}`, tt.message), rr.Body.String())
			event, err := events.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-issue-comment-1")
			require.NoError(t, err)
			require.NotNil(t, event)
			require.Equal(t, "issue_comment", event.Event)
			require.Equal(t, "created", event.Action)
			require.Equal(t, "octocat/hello-world", event.Repository)
			require.Equal(t, 1, event.PullRequest)
			require.Equal(t, "12345", event.TenantID)
			require.NotEmpty(t, event.Payload)
		})
	}
}

// A redelivered apply command (same delivery GUID) is deduplicated to a single
// inbox row, so a GitHub redelivery cannot double-run the command.
func TestDurableIssueCommentDeduplicatesDelivery(t *testing.T) {
	events := newRecordingWebhookEventStore()
	h := newDurableIssueCommentEnqueueHandler(t, events)

	for range 2 {
		req := buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply -e production", isPR: true}, nil)
		req.Header.Set(headerDeliveryID, "delivery-issue-comment-1")
		rr := httptest.NewRecorder()

		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
	}
	h.DrainInProcessWebhookWork(t.Context())

	events.mu.Lock()
	defer events.mu.Unlock()
	require.Len(t, events.events, 1)
}

// Commands without a durable driver keep their in-process dispatch even with
// durable dispatch enabled, so enabling the flag changes nothing for them.
func TestDurableIssueCommentOtherCommandsStayInProcess(t *testing.T) {
	events := newRecordingWebhookEventStore()
	h := newDurableIssueCommentEnqueueHandler(t, events)

	req := buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot stop -e production", isPR: true}, nil)
	req.Header.Set(headerDeliveryID, "delivery-issue-comment-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)
	h.DrainInProcessWebhookWork(t.Context())

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"stop started"}`, rr.Body.String())
	events.mu.Lock()
	defer events.mu.Unlock()
	require.Empty(t, events.events)
}

// A GitHub client failure while bootstrapping the command is retryable: the
// command was acknowledged, so it must be re-driven rather than dropped on a
// transient token or connectivity blip.
func TestDurableIssueCommentDriverRetriesBootstrapFailure(t *testing.T) {
	store := newScriptedWebhookEventStore(durableIssueCommentEvent("schemabot apply -e production"))
	factory := &fakeClientFactory{forInstallationErr: fmt.Errorf("installation token unavailable")}
	h := newDurableDriverHandler(t, store, nil, factory)

	before := time.Now()
	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.NotNil(t, failure.retryAfter, "bootstrap failure must stay retryable")
		require.True(t, failure.retryAfter.After(before), "retry must be scheduled in the future")
		require.Contains(t, failure.errMsg, "installation token unavailable")
	default:
		t.Fatal("expected bootstrap failure to be marked failed")
	}
	require.Empty(t, store.completed)
}

// An inbox row whose payload no longer decodes is a corrupted or hand-crafted
// delivery: retrying cannot repair it, so the driver fails it terminally.
func TestDurableIssueCommentDriverFailsMalformedRowTerminally(t *testing.T) {
	event := durableIssueCommentEvent("schemabot apply -e production")
	event.Payload = []byte("{not json")
	store := newScriptedWebhookEventStore(event)
	h := newDurableDriverHandler(t, store, nil, &fakeClientFactory{})

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.Nil(t, failure.retryAfter, "a malformed issue_comment row must not be retried")
		require.Contains(t, failure.errMsg, "decode durable issue_comment delivery")
	default:
		t.Fatal("expected malformed issue_comment delivery to be marked failed")
	}
	require.Empty(t, store.completed)
}

// An inbox row with an unparseable tenant cannot resolve an installation, and
// driver work runs outside any HTTP request, so there is no out-of-band
// resolution to recover with: the driver fails it terminally.
func TestDurableIssueCommentDriverFailsUnparseableTenantTerminally(t *testing.T) {
	event := durableIssueCommentEvent("schemabot apply -e production")
	event.TenantID = "not-a-number"
	store := newScriptedWebhookEventStore(event)
	h := newDurableDriverHandler(t, store, nil, &fakeClientFactory{})

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.Nil(t, failure.retryAfter, "an unparseable tenant must not be retried")
		require.Contains(t, failure.errMsg, "unparseable tenant ID")
	default:
		t.Fatal("expected unparseable-tenant delivery to be marked failed")
	}
	require.Empty(t, store.completed)
}

// Replayed or hand-crafted rows that the request path would never enqueue —
// a bot comment, or a comment carrying a command without a durable driver —
// complete as no-ops instead of running work fail-open.
func TestDurableIssueCommentDriverCompletesNonDispatchableRows(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{
			name: "bot comment",
			payload: []byte(`{
				"action": "created",
				"issue": {"number": 7, "pull_request": {"url": "https://api.github.com/repos/octocat/hello-world/pulls/7"}},
				"comment": {"id": 42, "body": "schemabot apply -e production", "user": {"login": "some-bot", "type": "Bot"}},
				"repository": {"full_name": "octocat/hello-world"},
				"installation": {"id": 12345}
			}`),
		},
		{
			name:    "not a PR comment",
			payload: []byte(`{"action": "created", "issue": {"number": 7}, "comment": {"id": 42, "body": "schemabot apply -e production", "user": {"login": "testuser", "type": "User"}}, "repository": {"full_name": "octocat/hello-world"}}`),
		},
		{
			name:    "command without a durable driver",
			payload: durableIssueCommentPayload("schemabot stop -e production"),
		},
		{
			name:    "apply missing environment flag",
			payload: durableIssueCommentPayload("schemabot apply"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := durableIssueCommentEvent("")
			event.Payload = tt.payload
			store := newScriptedWebhookEventStore(event)
			factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
			h := newDurableDriverHandler(t, store, nil, factory)

			h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

			select {
			case <-store.completed:
			default:
				t.Fatal("expected non-dispatchable issue_comment delivery to be marked completed")
			}
			require.Empty(t, store.failed)
			select {
			case <-factory.forInstallationStarted:
				t.Fatal("a non-dispatchable delivery must not create a GitHub client")
			default:
			}
		})
	}
}

// A delivery for a repo this deployment does not manage completes as a no-op
// without creating a client — config can drop a repo between enqueue and
// drive, and the driver re-validates the allowlist fail-closed.
func TestDurableIssueCommentDriverCompletesUnregisteredRepo(t *testing.T) {
	store := newScriptedWebhookEventStore(durableIssueCommentEvent("schemabot apply -e production"))
	config := &api.ServerConfig{Repos: map[string]api.RepoConfig{"other/repo": {}}}
	factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := newDurableDriverHandler(t, store, config, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected unregistered-repo issue_comment delivery to be marked completed")
	}
	require.Empty(t, store.failed)
	select {
	case <-factory.forInstallationStarted:
		t.Fatal("unregistered repo must not create a GitHub client")
	default:
	}
}
