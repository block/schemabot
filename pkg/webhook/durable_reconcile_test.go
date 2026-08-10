package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// newReconcileTestHandler builds a reconciler-enabled handler whose GitHub
// client talks to a fake server; register PR list responses on the returned
// mux. Extra options are appended so tests can enable synthesis.
func newReconcileTestHandler(t *testing.T, store storage.WebhookEventStore, repos map[string]api.RepoConfig, opts ...HandlerOption) (*Handler, *http.ServeMux) {
	t.Helper()
	ghc, mux := setupGitHubServer(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: store}, &api.ServerConfig{Repos: repos}, nil, logger)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghc, logger)}
	h := NewHandler(service, factory, nil, logger, append([]HandlerOption{WithDurableWebhookDispatch(), WithWebhookReconciler()}, opts...)...)
	return h, mux
}

func openPR(number int, headSHA string, updatedAt time.Time) map[string]any {
	return map[string]any{
		"number":     number,
		"title":      fmt.Sprintf("PR %d", number),
		"updated_at": updatedAt.UTC().Format(time.RFC3339),
		"head":       map[string]any{"sha": headSHA, "ref": "feature"},
		"base":       map[string]any{"ref": "main"},
		"user":       map[string]any{"login": "octocat"},
	}
}

func writeOpenPRs(t *testing.T, w http.ResponseWriter, prs ...map[string]any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if prs == nil {
		prs = []map[string]any{}
	}
	require.NoError(t, json.NewEncoder(w).Encode(prs))
}

func TestWebhookReconcilerReportsMissingInboxRow(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w, openPR(7, "head-sha", time.Now().Add(-time.Hour)))
	})

	scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, 1, scanned)
	require.Equal(t, 1, missing)
	require.Equal(t, 0, synthesized)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "head-sha"))
	require.NoError(t, err)
	require.Nil(t, row, "report-only scan must not synthesize inbox rows")
}

func TestWebhookReconcilerSkipsRecordedHead(t *testing.T) {
	store := newRecordingWebhookEventStore()
	_, err := store.Create(t.Context(), &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  "delivery-recorded",
		Event:       "pull_request",
		Action:      "synchronize",
		Repository:  "octocat/hello-world",
		PullRequest: 7,
		HeadSHA:     "head-sha",
		Payload:     []byte(`{}`),
	})
	require.NoError(t, err)
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w, openPR(7, "head-sha", time.Now().Add(-time.Hour)))
	})

	scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, 1, scanned)
	require.Equal(t, 0, missing)
	require.Equal(t, 0, synthesized)
}

func TestWebhookReconcilerSkipsGraceAndLookbackWindows(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		// Listing is newest-updated first: one PR inside the grace window (a
		// delivery may still be in flight), one past the lookback window (its
		// activity predates the inbox's coverage).
		writeOpenPRs(t, w,
			openPR(1, "fresh-sha", time.Now().Add(-time.Minute)),
			openPR(2, "stale-sha", time.Now().Add(-72*time.Hour)),
		)
	})

	scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, 0, scanned)
	require.Equal(t, 0, missing)
	require.Equal(t, 0, synthesized)
}

// TestWebhookReconcilerSynthesizesMissingHeadDelivery exercises the enforcing
// scan: an open PR head with no inbox delivery gets a synthesized
// pull_request-equivalent row whose GUID embeds repo/PR/SHA, whose tenant
// carries the resolved installation, and whose payload decodes into the
// auto-plannable shape the durable dispatcher routes — so a delivery lost
// upstream of the inbox is recovered without operator action.
func TestWebhookReconcilerSynthesizesMissingHeadDelivery(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}}, WithWebhookReconcileSynthesis())
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w, openPR(7, "head-sha", time.Now().Add(-time.Hour)))
	})

	scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, 1, scanned)
	require.Equal(t, 1, missing)
	require.Equal(t, 1, synthesized)

	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "head-sha"))
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, "pull_request", row.Event)
	require.Equal(t, "octocat/hello-world", row.Repository)
	require.Equal(t, 7, row.PullRequest)
	require.Equal(t, "head-sha", row.HeadSHA)
	require.Equal(t, "12345", row.TenantID)

	var payload pullRequestPayload
	require.NoError(t, json.Unmarshal(row.Payload, &payload))
	require.True(t, isAutoPlannablePullRequestAction(payload.Action))
	require.Equal(t, "octocat/hello-world", payload.Repository.FullName)
	require.Equal(t, 7, payload.PullRequest.Number)
	require.Equal(t, "head-sha", payload.PullRequest.Head.SHA)
}

// TestWebhookReconcilerSynthesisDedupesOnLaterPasses exercises the natural
// dedup of synthesized recovery rows: once a pass has enqueued one for a head,
// later passes see the head as covered and enqueue nothing, so a lost delivery
// is recovered exactly once rather than re-planned every interval.
func TestWebhookReconcilerSynthesisDedupesOnLaterPasses(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}}, WithWebhookReconcileSynthesis())
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w, openPR(7, "head-sha", time.Now().Add(-time.Hour)))
	})

	_, _, firstSynthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	require.Equal(t, 1, firstSynthesized)

	scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, 1, scanned)
	require.Equal(t, 0, missing)
	require.Equal(t, 0, synthesized)
}

func TestWebhookReconcilerSkipsAllowAllRegistry(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newReconcileTestHandler(t, store, nil)
	listed := false
	mux.HandleFunc("/", func(http.ResponseWriter, *http.Request) {
		listed = true
	})

	h.reconcileWebhookInbox(t.Context())

	require.False(t, listed, "allow-all registry is not enumerable; no GitHub calls expected")
}

func TestWebhookReconcilerTerminatesStuckProcessingEvent(t *testing.T) {
	store := newRecordingWebhookEventStore()
	leaseExpired := time.Now().Add(-time.Minute)
	_, err := store.Create(t.Context(), &storage.WebhookEvent{
		Provider:       storage.WebhookProviderGitHub,
		DeliveryID:     "delivery-stuck",
		Event:          "pull_request",
		Repository:     "octocat/hello-world",
		PullRequest:    7,
		HeadSHA:        "head-sha",
		State:          storage.WebhookEventProcessing,
		Attempts:       storage.MaxWebhookEventAttempts,
		LeaseExpiresAt: &leaseExpired,
		Payload:        []byte(`{}`),
	})
	require.NoError(t, err)
	// An allow-all registry: the sweep must still run even though the
	// missing-delivery scan cannot enumerate repos.
	h, _ := newReconcileTestHandler(t, store, nil)

	h.reconcileWebhookInbox(t.Context())

	got, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-stuck")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, storage.WebhookEventFailed, got.State)
	require.NotEmpty(t, got.LastError)
}

func TestWebhookReconcilerRunsOnDispatchLifecycle(t *testing.T) {
	store := newScriptedWebhookEventStore()
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	h.webhookReconcileInterval = 10 * time.Millisecond
	listed := make(chan struct{}, 4)
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		select {
		case listed <- struct{}{}:
		default:
		}
		writeOpenPRs(t, w)
	})

	h.StartDurableWebhookDispatch(t.Context())
	select {
	case <-listed:
	case <-time.After(durableWebhookTestDeadline):
		require.FailNow(t, "expected the reconciler to list open PRs on the dispatch lifecycle")
	}
	h.StopDurableWebhookDispatch()
}

// TestSynthesizedDeliveryGUIDFitsDeliveryColumn pins the GUID contract for
// synthesized recovery deliveries: the dedup key must be deterministic per
// (repo, PR, head), distinct across heads, and always fit the webhook_events
// delivery_id column even for long repository names, large PR numbers, and
// full 40-character head SHAs.
func TestSynthesizedDeliveryGUIDFitsDeliveryColumn(t *testing.T) {
	longRepo := strings.Repeat("organization-with-a-very-long-name-", 4) + "/repository-with-a-long-name"
	fullSHA := "0123456789abcdef0123456789abcdef01234567"

	guid := synthesizedDeliveryGUID(longRepo, 987654321, fullSHA)
	require.LessOrEqual(t, len(guid), 64, "GUID must fit the delivery_id column")
	require.Equal(t, guid, synthesizedDeliveryGUID(longRepo, 987654321, fullSHA),
		"GUID must be deterministic so re-scans of the same head dedup")

	require.NotEqual(t, guid, synthesizedDeliveryGUID(longRepo, 987654321, "fedcba9876543210fedcba9876543210fedcba98"),
		"a new push must mint a fresh recovery candidate")
	require.NotEqual(t, guid, synthesizedDeliveryGUID(longRepo, 123456789, fullSHA),
		"different PRs must not collide")
	require.NotEqual(t, guid, synthesizedDeliveryGUID("octocat/hello-world", 987654321, fullSHA),
		"different repositories must not collide")
}

// failingCreateWebhookEventStore rejects every insert so tests can pin the
// reconciler's behavior when the inbox refuses a synthesized recovery row.
type failingCreateWebhookEventStore struct {
	*recordingWebhookEventStore
}

func (s *failingCreateWebhookEventStore) Create(context.Context, *storage.WebhookEvent) (bool, error) {
	return false, errors.New("insert rejected")
}

// TestWebhookReconcilerSynthesisInsertFailureLeavesHeadRecoverable exercises
// the synthesis-failure branch: when the inbox rejects the recovery row, the
// pass reports the miss without counting a synthesis, records nothing, and the
// head stays uncovered so the next pass retries it.
func TestWebhookReconcilerSynthesisInsertFailureLeavesHeadRecoverable(t *testing.T) {
	store := &failingCreateWebhookEventStore{newRecordingWebhookEventStore()}
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}}, WithWebhookReconcileSynthesis())
	fullSHA := "0123456789abcdef0123456789abcdef01234567"
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w, openPR(7, fullSHA, time.Now().Add(-time.Hour)))
	})

	scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, 1, scanned)
	require.Equal(t, 1, missing)
	require.Equal(t, 0, synthesized, "a rejected insert must not count as synthesized")
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, fullSHA))
	require.NoError(t, err)
	require.Nil(t, row)

	scanned, missing, synthesized = h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, 1, scanned)
	require.Equal(t, 1, missing, "the head must stay visible as missing so later passes retry")
	require.Equal(t, 0, synthesized)
}

// TestWebhookReconcilerResynthesizesTerminallyFailedRecoveryRow exercises
// recovery from a recovery: a synthesized row that exhausted its attempts must
// not cover its head forever. The next pass sees the head as missing, and the
// deterministic GUID lands in the store's duplicate branch, which reopens the
// failed row as a fresh pending delivery.
func TestWebhookReconcilerResynthesizesTerminallyFailedRecoveryRow(t *testing.T) {
	store := newRecordingWebhookEventStore()
	guid := synthesizedDeliveryGUID("octocat/hello-world", 7, "head-sha")
	_, err := store.Create(t.Context(), &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  guid,
		Event:       "pull_request",
		Action:      webhookReconcileSynthesizedAction,
		Repository:  "octocat/hello-world",
		PullRequest: 7,
		HeadSHA:     "head-sha",
		State:       storage.WebhookEventFailed,
		Attempts:    storage.MaxWebhookEventAttempts,
		Payload:     []byte(`{}`),
	})
	require.NoError(t, err)
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}}, WithWebhookReconcileSynthesis())
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w, openPR(7, "head-sha", time.Now().Add(-time.Hour)))
	})

	scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, 1, scanned)
	require.Equal(t, 1, missing, "a terminally failed row must not cover its head")
	require.Equal(t, 1, synthesized)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, guid)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, storage.WebhookEventPending, row.State, "the failed row must be reopened as a fresh pending delivery")
	require.Equal(t, 0, row.Attempts)
}

// TestWebhookReconcilerNonPlanRowDoesNotCoverHead exercises the
// reopened-at-the-same-head scenario: a PR closed at head H leaves a
// pull_request.closed inbox row, and when the PR is reopened without a new
// push, the lost reopened delivery is the only thing that would have planned
// the head. The closed row must not count as coverage, so the reconciler
// synthesizes the recovery delivery.
func TestWebhookReconcilerNonPlanRowDoesNotCoverHead(t *testing.T) {
	store := newRecordingWebhookEventStore()
	_, err := store.Create(t.Context(), &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  "delivery-closed",
		Event:       "pull_request",
		Action:      "closed",
		Repository:  "octocat/hello-world",
		PullRequest: 7,
		HeadSHA:     "head-sha",
		State:       storage.WebhookEventCompleted,
		Payload:     []byte(`{}`),
	})
	require.NoError(t, err)
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}}, WithWebhookReconcileSynthesis())
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w, openPR(7, "head-sha", time.Now().Add(-time.Hour)))
	})

	scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, 1, scanned)
	require.Equal(t, 1, missing, "a closed row must not cover the reopened head")
	require.Equal(t, 1, synthesized)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "head-sha"))
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, storage.WebhookEventPending, row.State)
}

// TestWebhookReconcilerCompletedRowStillCoversHead pins the other side of the
// terminal-state contract: a completed delivery is a processed head, so the
// reconciler must not re-plan it on every pass.
func TestWebhookReconcilerCompletedRowStillCoversHead(t *testing.T) {
	store := newRecordingWebhookEventStore()
	_, err := store.Create(t.Context(), &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  "delivery-completed",
		Event:       "pull_request",
		Action:      "synchronize",
		Repository:  "octocat/hello-world",
		PullRequest: 7,
		HeadSHA:     "head-sha",
		State:       storage.WebhookEventCompleted,
		Payload:     []byte(`{}`),
	})
	require.NoError(t, err)
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}}, WithWebhookReconcileSynthesis())
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w, openPR(7, "head-sha", time.Now().Add(-time.Hour)))
	})

	scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, 1, scanned)
	require.Equal(t, 0, missing)
	require.Equal(t, 0, synthesized)
}
