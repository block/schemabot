package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
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
	h, mux := newReconcileTestHandlerWithSettings(t, store, newMemorySettingsStore(), repos, opts...)
	return h, mux
}

// newReconcileTestHandlerWithSettings builds a reconciler-enabled handler
// backed by the given settings store, so tests can inspect the persisted scan
// cursor or share it across handler instances to model a process restart.
func newReconcileTestHandlerWithSettings(t *testing.T, store storage.WebhookEventStore, settings storage.SettingsStore, repos map[string]api.RepoConfig, opts ...HandlerOption) (*Handler, *http.ServeMux) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return newReconcileTestHandlerWithLoggerAndSettings(t, logger, store, settings, repos, opts...)
}

// newReconcileTestHandlerWithLogger is newReconcileTestHandler with a
// caller-supplied logger, for tests that assert on emitted log records.
func newReconcileTestHandlerWithLogger(t *testing.T, logger *slog.Logger, store storage.WebhookEventStore, repos map[string]api.RepoConfig, opts ...HandlerOption) (*Handler, *http.ServeMux) {
	t.Helper()
	return newReconcileTestHandlerWithLoggerAndSettings(t, logger, store, newMemorySettingsStore(), repos, opts...)
}

func newReconcileTestHandlerWithLoggerAndSettings(t *testing.T, logger *slog.Logger, store storage.WebhookEventStore, settings storage.SettingsStore, repos map[string]api.RepoConfig, opts ...HandlerOption) (*Handler, *http.ServeMux) {
	t.Helper()
	ghc, mux := setupGitHubServer(t)
	service := api.New(&durableWebhookTestStorage{webhookEvents: store, settings: settings}, &api.ServerConfig{Repos: repos}, nil, logger)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghc, logger)}
	h := NewHandler(service, factory, nil, logger, append([]HandlerOption{WithDurableWebhookDispatch(), WithWebhookReconciler()}, opts...)...)
	// Tests drive consecutive passes back to back on one handler, which the
	// per-repository scan claim would otherwise skip until the claim expired.
	// Tests of the claim itself set their own duration.
	h.webhookReconcileScanClaim = 0
	return h, mux
}

// memorySettingsStore is an in-memory storage.SettingsStore for reconciler
// tests that exercise the persisted scan cursor.
type memorySettingsStore struct {
	mu       sync.Mutex
	settings map[string]string
}

func newMemorySettingsStore() *memorySettingsStore {
	return &memorySettingsStore{settings: make(map[string]string)}
}

func (s *memorySettingsStore) Get(_ context.Context, key string) (*storage.Setting, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.settings[key]
	if !ok {
		return nil, nil
	}
	return &storage.Setting{Key: key, Value: value}, nil
}

func (s *memorySettingsStore) Set(_ context.Context, key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settings[key] = value
	return nil
}

func (s *memorySettingsStore) CompareAndSet(_ context.Context, key string, previous *storage.Setting, value string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.settings[key]
	if previous == nil {
		if exists {
			return false, nil
		}
	} else if !exists || current != previous.Value {
		return false, nil
	}
	s.settings[key] = value
	return true, nil
}

func (s *memorySettingsStore) List(_ context.Context) ([]*storage.Setting, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	settings := make([]*storage.Setting, 0, len(s.settings))
	for key, value := range s.settings {
		settings = append(settings, &storage.Setting{Key: key, Value: value})
	}
	return settings, nil
}

func (s *memorySettingsStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.settings[key]; !ok {
		return storage.ErrSettingNotFound
	}
	delete(s.settings, key)
	return nil
}

// pageFetchLog records which listing pages the reconciler fetched, in order,
// so tests can assert exactly how a pass spent its page budget.
type pageFetchLog struct {
	mu    sync.Mutex
	pages []int
}

func (l *pageFetchLog) record(page int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pages = append(l.pages, page)
}

// take returns the pages fetched since the last call and clears the log, so a
// test can assert per-pass fetch patterns.
func (l *pageFetchLog) take() []int {
	l.mu.Lock()
	defer l.mu.Unlock()
	pages := l.pages
	l.pages = nil
	return pages
}

// registerPagedOpenPRs serves a multi-page open-PR listing on the fake GitHub
// server, emulating the Link-header pagination ListOpenPullRequestsPage
// consumes: every page but the last advertises rel="next". Returns a log of
// the pages fetched.
func registerPagedOpenPRs(t *testing.T, mux *http.ServeMux, repo string, pages ...[]map[string]any) *pageFetchLog {
	t.Helper()
	return registerMutablePagedOpenPRs(t, mux, repo, func() [][]map[string]any { return pages })
}

// registerMutablePagedOpenPRs is registerPagedOpenPRs over a listing the test
// may change between passes, so it can model PRs entering the listing while a
// scan cycle is in progress.
func registerMutablePagedOpenPRs(t *testing.T, mux *http.ServeMux, repo string, listing func() [][]map[string]any) *pageFetchLog {
	t.Helper()
	log := &pageFetchLog{}
	mux.HandleFunc("/repos/"+repo+"/pulls", func(w http.ResponseWriter, r *http.Request) {
		pages := listing()
		page, err := strconv.Atoi(r.URL.Query().Get("page"))
		if err != nil || page < 1 {
			page = 1
		}
		log.record(page)
		if page > len(pages) {
			writeOpenPRs(t, w)
			return
		}
		if page < len(pages) {
			w.Header().Set("Link", fmt.Sprintf(
				`<https://api.github.test/repos/%s/pulls?page=%d>; rel="next", <https://api.github.test/repos/%s/pulls?page=%d>; rel="last"`,
				repo, page+1, repo, len(pages)))
		}
		writeOpenPRs(t, w, pages[page-1]...)
	})
	return log
}

// storedScanCursor reads the repository's persisted scan cursor back from the
// settings store, failing the test when none is stored.
func storedScanCursor(t *testing.T, settings storage.SettingsStore, repo string) webhookReconcileScanCursor {
	t.Helper()
	setting, err := settings.Get(t.Context(), webhookReconcileScanCursorKey(repo))
	require.NoError(t, err)
	require.NotNil(t, setting, "expected a persisted scan cursor for %s", repo)
	var cursor webhookReconcileScanCursor
	require.NoError(t, json.Unmarshal([]byte(setting.Value), &cursor))
	return cursor
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

// TestWebhookReconcilerMissingHeadSeverityByMode pins the severity of the
// per-head line for a missing inbox delivery to what an operator can act on.
// In report-only mode the reconciler cannot recover the head, so the same line
// repeats every pass until synthesis is enabled or an organic delivery arrives:
// it stays at info and the missing-event metric plus the per-pass summary carry
// the signal. Once synthesis is enabled the line is emitted at most once per
// head and names a recovery that is actually happening, so it warns.
func TestWebhookReconcilerMissingHeadSeverityByMode(t *testing.T) {
	scenarios := []struct {
		name      string
		opts      []HandlerOption
		wantLevel slog.Level
		wantMsg   string
	}{
		{
			name:      "report-only mode logs the repeating per-head line at info",
			wantLevel: slog.LevelInfo,
			wantMsg:   "webhook reconciler found open PR head with no inbox delivery (report-only; synthesis disabled)",
		},
		{
			name:      "synthesis mode warns because a recovery delivery is being enqueued",
			opts:      []HandlerOption{WithWebhookReconcileSynthesis()},
			wantLevel: slog.LevelWarn,
			wantMsg:   "webhook reconciler found open PR head with no inbox delivery; synthesizing recovery delivery",
		},
	}
	for _, tc := range scenarios {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			store := newRecordingWebhookEventStore()
			h, mux := newReconcileTestHandlerWithLogger(t, logger, store, map[string]api.RepoConfig{"octocat/hello-world": {}}, tc.opts...)
			mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
				writeOpenPRs(t, w, openPR(7, "head-sha", time.Now().Add(-time.Hour)))
			})

			_, missing, _ := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
			require.Equal(t, 1, missing)

			record := findLogRecord(t, logs.Bytes(), tc.wantMsg)
			require.Equal(t, tc.wantLevel.String(), record["level"])
			require.Equal(t, "octocat/hello-world", record["repo"])
			require.Equal(t, float64(7), record["pr"])
			require.Equal(t, "head-sha", record["head_sha"])
		})
	}
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

// TestWebhookReconcilerDeadLetteredRecoveryRowCoversHead pins the dead-letter
// contract at the reconciler: a synthesized recovery row the driver proved can
// never succeed for its head covers that head, so the enforcing scan neither
// reports it missing nor reopens it — reopening would replay the identical
// deterministic failure through a fresh claim budget every pass. The head is
// only planned again when a new push mints a new head SHA.
func TestWebhookReconcilerDeadLetteredRecoveryRowCoversHead(t *testing.T) {
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
		State:       storage.WebhookEventFailedPermanent,
		Attempts:    1,
		Payload:     []byte(`{}`),
	})
	require.NoError(t, err)
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}}, WithWebhookReconcileSynthesis())
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w, openPR(7, "head-sha", time.Now().Add(-time.Hour)))
	})

	scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, 1, scanned)
	require.Equal(t, 0, missing, "a dead-lettered recovery row must cover its head")
	require.Equal(t, 0, synthesized)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, guid)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, storage.WebhookEventFailedPermanent, row.State, "the dead-lettered row must stay dead-lettered")
	require.Equal(t, 1, row.Attempts)
}

// TestWebhookReconcilerFailedOrganicRowCoversHead pins the organic side of
// the terminal-failure contract: a terminally failed organic delivery still
// covers its head, because the operator's remediation for it is GitHub
// Redeliver — the reconciler synthesizing over it would put a
// deterministically failing head through a fresh claim budget on every pass.
func TestWebhookReconcilerFailedOrganicRowCoversHead(t *testing.T) {
	store := newRecordingWebhookEventStore()
	_, err := store.Create(t.Context(), &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  "organic-github-guid",
		Event:       "pull_request",
		Action:      "synchronize",
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
	require.Equal(t, 0, missing, "a terminally failed organic row must cover its head")
	require.Equal(t, 0, synthesized)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "head-sha"))
	require.NoError(t, err)
	require.Nil(t, row, "no recovery row may be synthesized over a failed organic delivery")
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

// TestWebhookReconcilerScanCursorResumesAcrossPasses exercises the persisted
// scan cursor under a page budget too small to reach the lookback cutoff in
// one pass: the first pass covers the fresh window plus the start of the deep
// scan and records where it stopped; the next pass resumes there instead of
// restarting from the newest PRs, so every head in the lookback window is
// examined within a bounded number of passes. Once the listing end is
// reached, the cursor resets and a new cycle begins at the newest page.
func TestWebhookReconcilerScanCursorResumesAcrossPasses(t *testing.T) {
	store := newRecordingWebhookEventStore()
	settings := newMemorySettingsStore()
	h, mux := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
	// Budget of 2: the fresh-window walk may spend 1 page, the resumed deep
	// scan the other.
	h.webhookReconcileMaxPages = 2
	// Three pages of PRs all updated before the fresh window but inside the
	// lookback window, so covering them requires the deep scan.
	fetches := registerPagedOpenPRs(t, mux, "octocat/hello-world",
		[]map[string]any{openPR(1, "sha-1", time.Now().Add(-2*time.Hour))},
		[]map[string]any{openPR(2, "sha-2", time.Now().Add(-3*time.Hour))},
		[]map[string]any{openPR(3, "sha-3", time.Now().Add(-4*time.Hour))},
	)

	scanned, missing, _ := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, []int{1, 2}, fetches.take(), "first pass: fresh walk page 1, deep scan page 2")
	require.Equal(t, 2, scanned)
	require.Equal(t, 2, missing)
	cursor := storedScanCursor(t, settings, "octocat/hello-world")
	require.Equal(t, 3, cursor.Page, "budget exhausted before page 3; cursor records the resume point")
	require.Equal(t, 1, cursor.CyclePasses)

	scanned, missing, _ = h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, []int{1, 3}, fetches.take(), "second pass: fresh walk page 1, deep scan resumes at page 3")
	require.Equal(t, 2, scanned)
	require.Equal(t, 2, missing)
	cursor = storedScanCursor(t, settings, "octocat/hello-world")
	require.Equal(t, 1, cursor.Page, "listing end reached; the cursor resets for a new cycle")
	require.Equal(t, 0, cursor.CyclePasses)
}

// TestWebhookReconcilerFreshTrafficCannotStarveDeepScan pins the page-budget
// reserve: a repository whose fresh PR traffic alone exceeds the fresh walk's
// budget share every pass must still advance the deep-scan cursor, so heads
// deeper in the listing are examined within a bounded number of passes
// instead of being starved forever by new activity.
func TestWebhookReconcilerFreshTrafficCannotStarveDeepScan(t *testing.T) {
	store := newRecordingWebhookEventStore()
	settings := newMemorySettingsStore()
	h, mux := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
	h.webhookReconcileMaxPages = 2
	// Pages 1-2 hold PRs updated inside the fresh window (past the grace
	// window, so they are examined) on every pass; pages 3-5 hold older heads
	// only the deep scan can reach.
	fresh := time.Now().Add(-20 * time.Minute)
	fetches := registerPagedOpenPRs(t, mux, "octocat/hello-world",
		[]map[string]any{openPR(1, "sha-1", fresh)},
		[]map[string]any{openPR(2, "sha-2", fresh)},
		[]map[string]any{openPR(3, "sha-3", time.Now().Add(-3*time.Hour))},
		[]map[string]any{openPR(4, "sha-4", time.Now().Add(-4*time.Hour))},
		[]map[string]any{openPR(5, "sha-5", time.Now().Add(-5*time.Hour))},
	)

	h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	require.Equal(t, []int{1, 2}, fetches.take())
	require.Equal(t, 3, storedScanCursor(t, settings, "octocat/hello-world").Page)

	h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	require.Equal(t, []int{1, 3}, fetches.take(), "deep scan resumes past the fresh pages")
	require.Equal(t, 4, storedScanCursor(t, settings, "octocat/hello-world").Page)

	h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	require.Equal(t, []int{1, 4}, fetches.take())
	require.Equal(t, 5, storedScanCursor(t, settings, "octocat/hello-world").Page)

	h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	require.Equal(t, []int{1, 5}, fetches.take(), "deep scan reaches the listing end despite constant fresh traffic")
	cursor := storedScanCursor(t, settings, "octocat/hello-world")
	require.Equal(t, 1, cursor.Page, "cycle complete; cursor resets")
	require.Equal(t, 0, cursor.CyclePasses)
}

// TestWebhookReconcilerScanCursorPersistsAcrossRestart pins that the scan
// cursor lives in settings storage, not handler memory: a new handler (a
// restarted process) resumes the deep scan where the previous one stopped.
func TestWebhookReconcilerScanCursorPersistsAcrossRestart(t *testing.T) {
	store := newRecordingWebhookEventStore()
	settings := newMemorySettingsStore()
	pages := [][]map[string]any{
		{openPR(1, "sha-1", time.Now().Add(-2*time.Hour))},
		{openPR(2, "sha-2", time.Now().Add(-3*time.Hour))},
		{openPR(3, "sha-3", time.Now().Add(-4*time.Hour))},
	}

	h1, mux1 := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
	h1.webhookReconcileMaxPages = 2
	registerPagedOpenPRs(t, mux1, "octocat/hello-world", pages...)
	h1.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	require.Equal(t, 3, storedScanCursor(t, settings, "octocat/hello-world").Page)

	h2, mux2 := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
	h2.webhookReconcileMaxPages = 2
	fetches := registerPagedOpenPRs(t, mux2, "octocat/hello-world", pages...)
	h2.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, []int{1, 3}, fetches.take(), "restarted handler resumes the deep scan from the persisted cursor")
}

// TestWebhookReconcilerScanCursorResetsOnCutoffCrossing pins cycle completion
// via the lookback cutoff: when the deep scan reaches a PR older than the
// lookback window, everything deeper is outside coverage, so the cycle is
// complete and the cursor resets even though the listing has more pages.
func TestWebhookReconcilerScanCursorResetsOnCutoffCrossing(t *testing.T) {
	store := newRecordingWebhookEventStore()
	settings := newMemorySettingsStore()
	h, mux := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
	h.webhookReconcileMaxPages = 2
	fetches := registerPagedOpenPRs(t, mux, "octocat/hello-world",
		[]map[string]any{openPR(1, "sha-1", time.Now().Add(-2*time.Hour))},
		[]map[string]any{openPR(2, "sha-2", time.Now().Add(-72*time.Hour))},
		[]map[string]any{openPR(3, "sha-3", time.Now().Add(-73*time.Hour))},
	)

	h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, []int{1, 2}, fetches.take(), "page 3 is beyond the cutoff and never fetched")
	cursor := storedScanCursor(t, settings, "octocat/hello-world")
	require.Equal(t, 1, cursor.Page, "cutoff crossed; cycle complete and cursor reset")
	require.Equal(t, 0, cursor.CyclePasses)
}

// failingSettingsStore errors on every operation, modeling unavailable
// settings storage.
type failingSettingsStore struct{}

func (failingSettingsStore) Get(context.Context, string) (*storage.Setting, error) {
	return nil, errors.New("settings storage unavailable")
}

func (failingSettingsStore) Set(context.Context, string, string) error {
	return errors.New("settings storage unavailable")
}

func (failingSettingsStore) CompareAndSet(context.Context, string, *storage.Setting, string) (bool, error) {
	return false, errors.New("settings storage unavailable")
}

func (failingSettingsStore) List(context.Context) ([]*storage.Setting, error) {
	return nil, errors.New("settings storage unavailable")
}

func (failingSettingsStore) Delete(context.Context, string) error {
	return errors.New("settings storage unavailable")
}

// TestWebhookReconcilerScanDegradesWithoutSettingsStorage pins the fallback
// when the cursor cannot be loaded or persisted (nil or failing settings
// storage): each pass degrades to the pre-cursor behavior — scan from the
// newest page under the budget — rather than skipping the repository.
func TestWebhookReconcilerScanDegradesWithoutSettingsStorage(t *testing.T) {
	for name, settings := range map[string]storage.SettingsStore{
		"nil":     nil,
		"failing": failingSettingsStore{},
	} {
		t.Run(name, func(t *testing.T) {
			store := newRecordingWebhookEventStore()
			h, mux := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
			h.webhookReconcileMaxPages = 2
			fetches := registerPagedOpenPRs(t, mux, "octocat/hello-world",
				[]map[string]any{openPR(1, "sha-1", time.Now().Add(-2*time.Hour))},
				[]map[string]any{openPR(2, "sha-2", time.Now().Add(-3*time.Hour))},
				[]map[string]any{openPR(3, "sha-3", time.Now().Add(-4*time.Hour))},
			)

			scanned, missing, _ := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
			require.Equal(t, []int{1, 2}, fetches.take())
			require.Equal(t, 2, scanned)
			require.Equal(t, 2, missing)

			scanned, _, _ = h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
			require.Equal(t, []int{1, 2}, fetches.take(), "without a persisted cursor every pass restarts from the newest page")
			require.Equal(t, 2, scanned)
		})
	}
}

// readFailingSettingsStore fails every read while delegating writes, modeling
// a settings store that is reachable but whose Get errors during the pass.
type readFailingSettingsStore struct {
	*memorySettingsStore
}

func (readFailingSettingsStore) Get(context.Context, string) (*storage.Setting, error) {
	return nil, errors.New("settings storage read failed")
}

// TestWebhookReconcilerStoredCursorSurvivesLoadFailure pins that a pass which
// could not read the repository's cursor does not write one back: it scans
// from the newest page for this pass only, and the cursor a healthier pass
// stored — with the cycle's accumulated progress — is still there for the
// next pass to resume from.
func TestWebhookReconcilerStoredCursorSurvivesLoadFailure(t *testing.T) {
	inner := newMemorySettingsStore()
	stored := webhookReconcileScanCursor{Page: 40, CycleStartedAt: time.Now().Add(-6 * time.Hour), CyclePasses: 7}
	encoded, err := json.Marshal(stored)
	require.NoError(t, err)
	require.NoError(t, inner.Set(t.Context(), webhookReconcileScanCursorKey("octocat/hello-world"), string(encoded)))

	store := newRecordingWebhookEventStore()
	h, mux := newReconcileTestHandlerWithSettings(t, store, readFailingSettingsStore{inner}, map[string]api.RepoConfig{"octocat/hello-world": {}})
	h.webhookReconcileMaxPages = 2
	fetches := registerPagedOpenPRs(t, mux, "octocat/hello-world",
		[]map[string]any{openPR(1, "sha-1", time.Now().Add(-2*time.Hour))},
		[]map[string]any{openPR(2, "sha-2", time.Now().Add(-3*time.Hour))},
		[]map[string]any{openPR(3, "sha-3", time.Now().Add(-4*time.Hour))},
	)

	scanned, _, _ := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, []int{1, 2}, fetches.take(), "the pass scans from the newest page under the budget")
	require.Equal(t, 2, scanned)
	after := storedScanCursor(t, inner, "octocat/hello-world")
	require.Equal(t, 40, after.Page, "a pass that never saw the stored cursor must not overwrite it")
	require.Equal(t, 7, after.CyclePasses)
}

// TestWebhookReconcilerScanClaimSerializesReplicas pins that two replicas
// ticking on the same repository do not both walk its listing: the first pass
// claims the repository's scan, and a second pass that finds the live claim
// skips the repository without fetching a page or touching the cursor.
func TestWebhookReconcilerScanClaimSerializesReplicas(t *testing.T) {
	store := newRecordingWebhookEventStore()
	settings := newMemorySettingsStore()
	pages := [][]map[string]any{
		{openPR(1, "sha-1", time.Now().Add(-2*time.Hour))},
		{openPR(2, "sha-2", time.Now().Add(-3*time.Hour))},
		{openPR(3, "sha-3", time.Now().Add(-4*time.Hour))},
	}

	h1, mux1 := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
	h1.webhookReconcileMaxPages = 2
	h1.webhookReconcileScanClaim = time.Hour
	registerPagedOpenPRs(t, mux1, "octocat/hello-world", pages...)
	scanned, _, _ := h1.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	require.Equal(t, 2, scanned)
	claimed := storedScanCursor(t, settings, "octocat/hello-world")
	require.Equal(t, 3, claimed.Page)
	require.WithinDuration(t, time.Now().Add(time.Hour), claimed.ClaimedUntil, time.Minute, "the pass records its claim on the cursor")

	h2, mux2 := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
	h2.webhookReconcileMaxPages = 2
	h2.webhookReconcileScanClaim = time.Hour
	fetches := registerPagedOpenPRs(t, mux2, "octocat/hello-world", pages...)
	scanned, missing, synthesized := h2.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Zero(t, scanned)
	require.Zero(t, missing)
	require.Zero(t, synthesized)
	require.Empty(t, fetches.take(), "a pass that finds a live claim fetches nothing")
	require.Equal(t, claimed, storedScanCursor(t, settings, "octocat/hello-world"), "the skipped pass leaves the cursor untouched")
}

// TestWebhookReconcilerScanClaimIsCompareAndSet pins the race the claim
// exists for: two passes that load the same cursor before either claims it.
// Only the first claim lands; the second sees the cursor changed underneath
// it and skips the repository. A later save against the losing pass's stale
// view is likewise refused, so it cannot overwrite the winner's progress.
func TestWebhookReconcilerScanClaimIsCompareAndSet(t *testing.T) {
	store := newRecordingWebhookEventStore()
	settings := newMemorySettingsStore()
	h, _ := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
	h.webhookReconcileScanClaim = time.Hour
	now := time.Now()

	first := h.loadWebhookReconcileScanCursor(t.Context(), "octocat/hello-world", now)
	second := h.loadWebhookReconcileScanCursor(t.Context(), "octocat/hello-world", now)
	require.True(t, first.persistable)
	require.Nil(t, first.stored, "no cursor is stored yet, so both passes load a fresh one")

	require.True(t, h.claimWebhookReconcileScan(t.Context(), "octocat/hello-world", &first, now))
	require.False(t, h.claimWebhookReconcileScan(t.Context(), "octocat/hello-world", &second, now), "the second claim finds the cursor already claimed")

	winner := first.cursor
	winner.Page = 4
	h.saveWebhookReconcileScanCursor(t.Context(), "octocat/hello-world", first, winner)
	require.Equal(t, 4, storedScanCursor(t, settings, "octocat/hello-world").Page)

	stale := second
	stale.persistable = true
	loser := second.cursor
	loser.Page = 2
	h.saveWebhookReconcileScanCursor(t.Context(), "octocat/hello-world", stale, loser)
	require.Equal(t, 4, storedScanCursor(t, settings, "octocat/hello-world").Page, "a save against a stale cursor view is refused")
}

// TestWebhookReconcilerScanWatermarkSkipsAlreadyExaminedHeads pins how the
// deep scan copes with the listing shifting under it. Between passes two PRs
// enter the listing at the front, pushing every older head one page deeper,
// so the page the cursor resumes at now holds a head the previous pass
// already examined. The watermark recognizes it as newer than the deep scan's
// progress and passes over it instead of examining it again, and the cycle
// still reaches every head down to the listing end exactly once.
func TestWebhookReconcilerScanWatermarkSkipsAlreadyExaminedHeads(t *testing.T) {
	store := newRecordingWebhookEventStore()
	settings := newMemorySettingsStore()
	h, mux := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
	// Budget of 3: the fresh walk may spend up to 2 pages, the deep scan the
	// rest.
	h.webhookReconcileMaxPages = 3
	var mu sync.Mutex
	pages := [][]map[string]any{
		{openPR(1, "sha-1", time.Now().Add(-2*time.Hour))},
		{openPR(2, "sha-2", time.Now().Add(-3*time.Hour))},
		{openPR(3, "sha-3", time.Now().Add(-4*time.Hour))},
		{openPR(4, "sha-4", time.Now().Add(-5*time.Hour))},
	}
	fetches := registerMutablePagedOpenPRs(t, mux, "octocat/hello-world", func() [][]map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return pages
	})

	scanned, _, _ := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	require.Equal(t, []int{1, 2, 3}, fetches.take(), "first pass: fresh walk page 1, deep scan pages 2-3")
	require.Equal(t, 3, scanned)
	cursor := storedScanCursor(t, settings, "octocat/hello-world")
	require.Equal(t, 4, cursor.Page)
	require.WithinDuration(t, time.Now().Add(-4*time.Hour), cursor.Watermark, time.Minute, "the watermark is the oldest head the deep scan examined")

	// Two PRs are updated just now and enter the listing at the front; the
	// head on page 4 is now PR 2, which the first pass already examined.
	mu.Lock()
	pages = append([][]map[string]any{
		{openPR(5, "sha-5", time.Now())},
		{openPR(6, "sha-6", time.Now())},
	}, pages...)
	mu.Unlock()

	scanned, _, _ = h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	require.Equal(t, []int{1, 2, 4}, fetches.take(), "second pass: fresh walk spends its share on the new PRs, deep scan resumes at page 4")
	require.Zero(t, scanned, "PRs 5 and 6 are inside the grace window and PR 2 is above the watermark, so nothing is examined")
	cursor = storedScanCursor(t, settings, "octocat/hello-world")
	require.Equal(t, 5, cursor.Page)
	require.WithinDuration(t, time.Now().Add(-4*time.Hour), cursor.Watermark, time.Minute, "passing over an examined head does not move the watermark")

	scanned, _, _ = h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	require.Equal(t, []int{1, 2, 5}, fetches.take())
	require.Equal(t, 1, scanned, "PR 3 sits at the watermark and is examined")

	scanned, _, _ = h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	require.Equal(t, []int{1, 2, 6}, fetches.take(), "deep scan reaches the listing end")
	require.Equal(t, 1, scanned, "PR 4 is examined")
	cursor = storedScanCursor(t, settings, "octocat/hello-world")
	require.Equal(t, 1, cursor.Page, "cycle complete; cursor resets")
	require.True(t, cursor.Watermark.IsZero(), "a new cycle starts with no watermark")
}

// TestWebhookReconcilerFreshWindowWidensWhenCutShort pins that a fresh walk
// stopped by the page reserve before reaching its floor does not advance the
// fresh coverage point: the next pass's fresh window still begins where the
// last complete fresh walk left off, so heads updated in between are examined
// once fresh traffic subsides rather than falling into a gap between passes.
func TestWebhookReconcilerFreshWindowWidensWhenCutShort(t *testing.T) {
	store := newRecordingWebhookEventStore()
	settings := newMemorySettingsStore()
	h, mux := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
	h.webhookReconcileMaxPages = 2
	// Both pages hold heads inside the fresh window, so the fresh walk's
	// single-page share cannot reach its floor.
	fetches := registerPagedOpenPRs(t, mux, "octocat/hello-world",
		[]map[string]any{openPR(1, "sha-1", time.Now().Add(-20*time.Minute))},
		[]map[string]any{openPR(2, "sha-2", time.Now().Add(-25*time.Minute))},
	)

	h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")

	require.Equal(t, []int{1, 2}, fetches.take())
	cursor := storedScanCursor(t, settings, "octocat/hello-world")
	require.Equal(t, 1, cursor.Page, "the deep scan reached the listing end")
	require.WithinDuration(t, time.Now().Add(-h.webhookReconcileGrace), cursor.FreshCoveredAt, time.Minute,
		"the fresh walk was cut short, so coverage advances only to the completed cycle's start")
}

// TestWebhookReconcilerDeletesOrphanedScanCursors pins that a repository
// removed from the registry does not leave its scan cursor behind: the next
// pass deletes it, while cursors of registered repositories are kept.
func TestWebhookReconcilerDeletesOrphanedScanCursors(t *testing.T) {
	store := newRecordingWebhookEventStore()
	settings := newMemorySettingsStore()
	for _, repo := range []string{"octocat/hello-world", "octocat/retired"} {
		encoded, err := json.Marshal(webhookReconcileScanCursor{Page: 3, CycleStartedAt: time.Now(), CyclePasses: 1})
		require.NoError(t, err)
		require.NoError(t, settings.Set(t.Context(), webhookReconcileScanCursorKey(repo), string(encoded)))
	}
	require.NoError(t, settings.Set(t.Context(), "unrelated_setting", "kept"))
	h, mux := newReconcileTestHandlerWithSettings(t, store, settings, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w)
	})

	h.reconcileWebhookInbox(t.Context())

	retired, err := settings.Get(t.Context(), webhookReconcileScanCursorKey("octocat/retired"))
	require.NoError(t, err)
	require.Nil(t, retired, "the deregistered repository's cursor is deleted")
	require.Equal(t, 1, storedScanCursor(t, settings, "octocat/hello-world").Page, "the registered repository's cursor is kept and advanced by the pass")
	unrelated, err := settings.Get(t.Context(), "unrelated_setting")
	require.NoError(t, err)
	require.NotNil(t, unrelated)
	require.Equal(t, "kept", unrelated.Value)
}

func TestWithWebhookReconcileScanBounds(t *testing.T) {
	h := &Handler{webhookReconcileMaxPages: defaultWebhookReconcileMaxPages, webhookReconcileLookback: defaultWebhookReconcileLookback}

	WithWebhookReconcileScanBounds(12, 6*time.Hour)(h)
	require.Equal(t, 12, h.webhookReconcileMaxPages)
	require.Equal(t, 6*time.Hour, h.webhookReconcileLookback)

	WithWebhookReconcileScanBounds(0, 0)(h)
	require.Equal(t, 12, h.webhookReconcileMaxPages, "a non-positive budget leaves the current value in place")
	require.Equal(t, 6*time.Hour, h.webhookReconcileLookback, "a non-positive lookback leaves the current value in place")
}
