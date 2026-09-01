package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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

// TestIssueCommentGateBlockParity pins the shared gate ladder reason by
// reason: every block reason has a row whose comment trips exactly that gate,
// so a reordering or a dropped gate fails a named row. The handler carries a
// tenanted, environment-restricted config so the routing and ownership gates
// — tenant addressing, tenant-target requirement, environment ownership — are
// reachable, not skipped behind a nil service. The test drives the ladder
// directly; the entry points' handling of each reason is exercised by the
// request-path and durable-driver tests.
func TestIssueCommentGateBlockParity(t *testing.T) {
	parser := NewCommandParser()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// A deployment that owns tenant "alpha" and environment "production" only,
	// with no aggregate role: unscoped work commands require a tenant target.
	h := &Handler{service: api.New(nil, &api.ServerConfig{
		Tenant:              "alpha",
		AllowedEnvironments: []string{"production"},
	}, nil, logger)}
	// An aggregate participant for the repo: unscoped work commands fan out
	// instead of requiring a tenant target.
	participant := &Handler{service: api.New(nil, &api.ServerConfig{
		Tenant:              "alpha",
		AllowedEnvironments: []string{"production"},
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}},
		},
	}, nil, logger)}
	tests := []struct {
		name    string
		comment string
		handler *Handler
		want    issueCommentGateBlockReason
	}{
		{name: "apply passes", comment: "schemabot apply -e production -t alpha", want: issueCommentGatePass},
		{name: "apply-confirm passes", comment: "schemabot apply-confirm -e production -t alpha", want: issueCommentGatePass},
		{name: "unlock passes without environment", comment: "schemabot unlock -t alpha", want: issueCommentGatePass},
		{name: "unscoped apply fans out on a participant repo", comment: "schemabot apply -e production", handler: participant, want: issueCommentGatePass},
		{name: "invalid tenant", comment: "schemabot apply -e production -t", want: issueCommentGateInvalidTenant},
		{name: "tenant handled by another deployment", comment: "schemabot apply -e production -t other", want: issueCommentGateTenantNotOwned},
		{name: "tenant target required", comment: "schemabot apply -e production", want: issueCommentGateTenantRequired},
		{name: "invalid environment", comment: "schemabot apply -e production--yes -t alpha", want: issueCommentGateInvalidEnvironment},
		{name: "missing environment", comment: "schemabot apply -t alpha", want: issueCommentGateMissingEnvironment},
		{name: "environment handled by another instance", comment: "schemabot apply -e staging -t alpha", want: issueCommentGateEnvironmentNotOwned},
		{name: "rollback missing apply ID", comment: "schemabot rollback -e production -t alpha", want: issueCommentGateMissingApplyID},
		{name: "command not found", comment: "schemabot frobnicate", want: issueCommentGateCommandNotFound},
		{name: "unsupported auto-confirm", comment: "schemabot apply-confirm -e production -y -t alpha", want: issueCommentGateAutoConfirm},
		{name: "unsupported auto-confirm on apply", comment: "schemabot apply -e production --yes -t alpha", want: issueCommentGateAutoConfirm},
		{name: "rollback misplaced defer-cutover", comment: "schemabot rollback apply_123 -e production --defer-cutover -t alpha", want: issueCommentGateDeferCutover},
		{name: "unsupported database", comment: "schemabot stop -e production -d accounts -t alpha", want: issueCommentGateDatabase},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := tt.handler
			if handler == nil {
				handler = h
			}
			result := parser.ParseCommand(tt.comment)
			reason := handler.issueCommentGateBlock("octocat/hello-world", result, parser, tt.comment)

			require.Equal(t, tt.want, reason)
		})
	}
}

// durableIssueCommentEvent returns a claimable issue_comment inbox row for a
// command comment, mirroring what the HTTP path enqueues. ReceivedAt is set
// the way the store populates it on insert, so drivers that bound their work
// to the delivery's receipt time see a realistic value.
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
		ReceivedAt:  time.Now(),
	}
}

// With durable dispatch enabled, a durably dispatched command acks fast by
// persisting an inbox row rather than dispatching an in-process goroutine,
// so a deploy after the ACK cannot drop the acknowledged command.
func TestDurableIssueCommentCommandQueuesAndAcks(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		message string
	}{
		{name: "apply", comment: "schemabot apply -e production", message: "apply queued"},
		{name: "apply-confirm", comment: "schemabot apply-confirm -e production", message: "apply-confirm queued"},
		{name: "unlock", comment: "schemabot unlock", message: "unlock queued"},
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

func TestIssueCommentWebhookCanonicalizesRepository(t *testing.T) {
	events := newRecordingWebhookEventStore()
	h := newDurableIssueCommentEnqueueHandler(t, events)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e production", repo: "MixedCase/Sample-Repo",
		userLogin: "MixedCaseUser", isPR: true,
	}, nil)
	req.Header.Set(headerDeliveryID, "mixed-case-issue-comment")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)
	h.DrainInProcessWebhookWork(t.Context())

	require.Equal(t, http.StatusOK, rr.Code)
	event, err := events.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "mixed-case-issue-comment")
	require.NoError(t, err)
	require.NotNil(t, event)
	assert.Equal(t, "mixedcase/sample-repo", event.Repository)
	assert.Equal(t, "mixedcase/sample-repo#1", fmt.Sprintf("%s#%d", event.Repository, event.PullRequest))

	var payload webhookPayload
	require.NoError(t, json.Unmarshal(event.Payload, &payload))
	require.NotNil(t, payload.Comment)
	require.NotNil(t, payload.Comment.User)
	assert.Equal(t, "MixedCaseUser", payload.Comment.User.Login)
	assert.Equal(t, "schemabot apply -e production", payload.Comment.Body)
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
// delivery: retrying cannot repair it, so the driver dead-letters it.
func TestDurableIssueCommentDriverFailsMalformedRowTerminally(t *testing.T) {
	event := durableIssueCommentEvent("schemabot apply -e production")
	event.Payload = []byte("{not json")
	store := newScriptedWebhookEventStore(event)
	h := newDurableDriverHandler(t, store, nil, &fakeClientFactory{})

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failedPermanent:
		require.Contains(t, failure.errMsg, "decode durable issue_comment delivery")
	default:
		t.Fatal("expected malformed issue_comment delivery to be dead-lettered")
	}
	require.Empty(t, store.failed, "a malformed row is deterministic and must not burn retry budget")
	require.Empty(t, store.completed)
}

// An inbox row with an unparseable tenant cannot resolve an installation, and
// driver work runs outside any HTTP request, so there is no out-of-band
// resolution to recover with: the driver dead-letters it.
func TestDurableIssueCommentDriverFailsUnparseableTenantTerminally(t *testing.T) {
	event := durableIssueCommentEvent("schemabot apply -e production")
	event.TenantID = "not-a-number"
	store := newScriptedWebhookEventStore(event)
	h := newDurableDriverHandler(t, store, nil, &fakeClientFactory{})

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failedPermanent:
		require.Contains(t, failure.errMsg, "unparseable tenant ID")
	default:
		t.Fatal("expected unparseable-tenant delivery to be dead-lettered")
	}
	require.Empty(t, store.failed, "an unparseable tenant is deterministic and must not burn retry budget")
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
		{
			name:    "apply-confirm with unsupported auto-confirm flag",
			payload: durableIssueCommentPayload("schemabot apply-confirm -e production -y"),
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

// A delivery whose command this deployment does not route — a tenant target
// addressed to a sibling deployment, an unscoped command on a tenanted
// deployment, or an environment owned by another instance — completes as a
// no-op without creating a client. The request path routes these away before
// enqueueing, so a claimed row tripping one of the gates is a replayed or
// hand-crafted delivery, or one enqueued before a configuration change moved
// the ownership.
func TestDurableIssueCommentDriverCompletesUnroutedCommands(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		config  *api.ServerConfig
	}{
		{
			name:    "tenant addressed to another deployment",
			comment: "schemabot apply -e production -t other",
			config:  &api.ServerConfig{Tenant: "mine"},
		},
		{
			name:    "unscoped command on a tenanted deployment",
			comment: "schemabot apply -e production",
			config:  &api.ServerConfig{Tenant: "mine"},
		},
		{
			name:    "environment owned by another instance",
			comment: "schemabot apply -e production",
			config:  &api.ServerConfig{AllowedEnvironments: []string{"staging"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newScriptedWebhookEventStore(durableIssueCommentEvent(tt.comment))
			factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
			h := newDurableDriverHandler(t, store, tt.config, factory)

			h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

			select {
			case <-store.completed:
			default:
				t.Fatal("expected unrouted issue_comment delivery to be marked completed")
			}
			require.Empty(t, store.failed)
			select {
			case <-factory.forInstallationStarted:
				t.Fatal("an unrouted delivery must not create a GitHub client")
			default:
			}
		})
	}
}

// An aggregate participant serves unscoped apply commands by fanning out onto
// the work it owns, so the driver's tenant-target gate must let the command
// through to the command core rather than completing it as a no-op.
func TestDurableIssueCommentDriverDispatchesParticipantFanOut(t *testing.T) {
	store := newScriptedWebhookEventStore(durableIssueCommentEvent("schemabot apply -e production"))
	config := &api.ServerConfig{
		Tenant: "mine",
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}},
		},
	}
	factory := &fakeClientFactory{
		forInstallationStarted: make(chan struct{}),
		forInstallationErr:     fmt.Errorf("installation token unavailable"),
	}
	h := newDurableDriverHandler(t, store, config, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-factory.forInstallationStarted:
	default:
		t.Fatal("expected participant fan-out apply to reach the command core")
	}
	require.Empty(t, store.completed, "fan-out apply must dispatch, not complete as a no-op")
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

// durableIssueCommentCoreConfig returns a server config where the test repo is
// registered and the databases registry configures "orders" for production
// only, so an apply targeting another environment reaches the command core and
// draws a user-facing targeting rejection naming the database and environment.
func durableIssueCommentCoreConfig() *api.ServerConfig {
	return &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
		Databases: map[string]api.DatabaseConfig{
			"orders": {Environments: map[string]api.EnvironmentConfig{"production": {}}},
		},
	}
}

// newDurableIssueCommentCoreHarness builds a durable-driver handler whose stub
// GitHub server carries the routes the apply command core walks for the
// driver's PR — pull request, changed files, and a schemabot.yaml resolving to
// database "orders" — plus a recorder capturing posted PR comments. onComment,
// when non-nil, runs before each comment post is recorded.
func newDurableIssueCommentCoreHarness(t *testing.T, store storage.WebhookEventStore, cfg *api.ServerConfig, onComment func()) (*Handler, chan string) {
	t.Helper()
	st := &durableWebhookTestStorage{webhookEvents: store}
	return newDurableDriverHarness(t, st, cfg, func(mux *http.ServeMux, comments chan string) {
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"head": map[string]any{"sha": "abc123", "ref": "feature-branch"},
				"base": map[string]any{"sha": "def456", "ref": "main"},
				"user": map[string]any{"login": "testuser"},
			}))
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7/files", func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode([]map[string]string{{
				"filename": "schemabot.yaml",
				"status":   "modified",
			}}))
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/schemabot.yaml", func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
				"type":     "file",
				"encoding": "base64",
				"content":  base64.StdEncoding.EncodeToString([]byte("database: orders\ntype: mysql\n")),
			}))
		})
		recorder := commentRecorder(t, comments)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/7/comments", func(w http.ResponseWriter, r *http.Request) {
			if onComment != nil {
				onComment()
			}
			recorder(w, r)
		})
	})
}

// A routed apply command must reach the command core with the payload's
// environment and database threaded intact: the driver takes the enqueued row
// through config discovery to the core's user-facing answer — here a targeting
// rejection naming both the discovered database and the requested environment
// — and completes the delivery as the command's terminal answer.
func TestDurableIssueCommentDriverAnswersApplyThroughCore(t *testing.T) {
	store := newScriptedWebhookEventStore(durableIssueCommentEvent("schemabot apply -e staging"))
	h, comments := newDurableIssueCommentCoreHarness(t, store, durableIssueCommentCoreConfig(), nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected the driven apply command to complete its delivery")
	}
	require.Empty(t, store.failed)
	body := requireComment(t, comments, "environment-not-configured apply answer")
	require.Contains(t, body, `database &#34;orders&#34; environment &#34;staging&#34; is not configured on this server`)
}

// A routed unlock command must reach the command core through the driver: with
// no locks held by the PR, the core posts the no-locks answer — the command's
// terminal answer — and the delivery completes.
func TestDurableIssueCommentDriverAnswersUnlockThroughCore(t *testing.T) {
	store := newScriptedWebhookEventStore(durableIssueCommentEvent("schemabot unlock"))
	st := &durableWebhookTestStorage{webhookEvents: store, locks: &unlockTestLockStore{}, applies: &noActiveAppliesStore{}}
	h, comments := newDurableDriverHarness(t, st, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected the driven unlock command to complete its delivery")
	}
	require.Empty(t, store.failed)
	body := requireComment(t, comments, "no-locks unlock answer")
	require.Contains(t, body, "No Locks Found")
	require.Contains(t, body, "Nothing to unlock")
}

// A storage failure while looking up the PR's locks is retryable: the unlock
// was acknowledged, so the driver re-drives it rather than dropping it on a
// transient storage blip.
func TestDurableIssueCommentDriverRetriesUnlockLockLookupFailure(t *testing.T) {
	store := newScriptedWebhookEventStore(durableIssueCommentEvent("schemabot unlock"))
	st := &durableWebhookTestStorage{
		webhookEvents: store,
		locks:         &unlockTestLockStore{getByPRErr: errors.New("lock lookup failed")},
		applies:       &noActiveAppliesStore{},
	}
	h, comments := newDurableDriverHarness(t, st, nil, nil)

	before := time.Now()
	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.NotNil(t, failure.retryAfter, "lock lookup failure must stay retryable")
		require.True(t, failure.retryAfter.After(before), "retry must be scheduled in the future")
		require.Contains(t, failure.errMsg, "lock lookup failed")
	default:
		t.Fatal("expected unlock lock-lookup failure to be marked failed")
	}
	require.Empty(t, store.completed)
	require.Empty(t, comments, "an intermediate durable attempt must not post a retry comment")
}

// The final durable attempt still gives the user exactly one answer: the core
// stays silent and the driver posts after recording the terminal disposition.
func TestDurableIssueCommentDriverFinalAttemptPostsOneRetryFailure(t *testing.T) {
	event := durableIssueCommentEvent("schemabot unlock")
	event.Attempts = maxDurableWebhookAttempts - 1
	store := newScriptedWebhookEventStore(event)
	st := &durableWebhookTestStorage{
		webhookEvents: store,
		locks:         &unlockTestLockStore{getByPRErr: errors.New("lock lookup failed")},
		applies:       &noActiveAppliesStore{},
	}
	h, comments := newDurableDriverHarness(t, st, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	failure := <-store.failed
	require.Nil(t, failure.retryAfter)
	body := requireComment(t, comments, "terminal unlock failure")
	require.Contains(t, body, "Unlock Failed")
	require.Contains(t, body, "could not complete this command after retrying")
	require.Empty(t, comments, "the final attempt must post only one answer")
}

// cancelOnLookupLockStore cancels the drive context during the lock lookup and,
// like a real store, refuses to mutate lock state once its context is
// cancelled. It pins that the driver's context reaches the unlock core's
// storage calls: a core detached from the drive context would release the lock
// anyway.
type cancelOnLookupLockStore struct {
	storage.LockStore
	cancel   context.CancelFunc
	locks    []*storage.Lock
	released int
}

func (s *cancelOnLookupLockStore) GetByPR(_ context.Context, _ string, _ int) ([]*storage.Lock, error) {
	s.cancel()
	return s.locks, nil
}

func (s *cancelOnLookupLockStore) Release(ctx context.Context, _, _, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.released++
	return nil
}

// Cancelling the driver's context (shutdown or lease loss) mid-unlock must
// stop the core's lock releases: once the lease cannot be proven, this driver
// must not keep mutating lock state, and the claim is released so the next
// claimant re-drives the command from current state.
func TestDurableIssueCommentDriverCancelledUnlockStopsLockReleases(t *testing.T) {
	store := newScriptedWebhookEventStore(durableIssueCommentEvent("schemabot unlock"))
	driveCtx, cancelDrive := context.WithCancel(t.Context())
	t.Cleanup(cancelDrive)
	lock := prOwnedOrdersLock()
	lock.CreatedAt = time.Now().Add(-time.Hour)
	locks := &cancelOnLookupLockStore{cancel: cancelDrive, locks: []*storage.Lock{lock}}
	st := &durableWebhookTestStorage{webhookEvents: store, locks: locks, applies: &noActiveAppliesStore{}}
	h, _ := newDurableDriverHarness(t, st, nil, nil)

	h.driveNextDurableWebhook(driveCtx, 0, "test-host/1/webhook-driver-0")

	require.Zero(t, locks.released, "a cancelled run must not release locks it can no longer prove it owns")
	select {
	case released := <-store.released:
		require.Equal(t, "token-1", released.leaseToken)
	default:
		t.Fatal("expected the cancelled run to release its claim for re-drive")
	}
	require.Empty(t, store.completed, "a cancelled run must not complete the delivery")
}

// Cancelling the driver's context (shutdown or lease loss) must reach the
// in-flight command work: the run cannot prove it finished dispatching, so the
// claim is released for another driver to re-drive rather than completed as if
// the command had been answered.
func TestDurableIssueCommentDriverCancelledRunReleasesClaim(t *testing.T) {
	store := newScriptedWebhookEventStore(durableIssueCommentEvent("schemabot apply -e staging"))
	driveCtx, cancelDrive := context.WithCancel(t.Context())
	t.Cleanup(cancelDrive)
	h, _ := newDurableIssueCommentCoreHarness(t, store, durableIssueCommentCoreConfig(), cancelDrive)

	h.driveNextDurableWebhook(driveCtx, 0, "test-host/1/webhook-driver-0")

	select {
	case released := <-store.released:
		require.Equal(t, "token-1", released.leaseToken)
	default:
		t.Fatal("expected the cancelled run to release its claim for re-drive")
	}
	require.Empty(t, store.completed, "a cancelled run must not complete the delivery")
}
