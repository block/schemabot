package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// durableWebhookTestDeadline bounds waits for async driver work in these
// unit tests so a hang fails fast instead of stalling the suite.
const durableWebhookTestDeadline = 5 * time.Second

type durableWebhookTestStorage struct {
	emptyStorage
	webhookEvents storage.WebhookEventStore
}

func (s *durableWebhookTestStorage) WebhookEvents() storage.WebhookEventStore {
	return s.webhookEvents
}

type recordingWebhookEventStore struct {
	mu     sync.Mutex
	nextID int64
	events map[string]*storage.WebhookEvent
}

func newRecordingWebhookEventStore() *recordingWebhookEventStore {
	return &recordingWebhookEventStore{events: make(map[string]*storage.WebhookEvent)}
}

func (s *recordingWebhookEventStore) Create(_ context.Context, event *storage.WebhookEvent) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := event.Provider + ":" + event.DeliveryID
	if existing, ok := s.events[key]; ok {
		// Mirror the real store's duplicate-GUID branch: terminal rows are
		// reopened as fresh pending deliveries; live rows dedup.
		if existing.State == storage.WebhookEventFailed || existing.State == storage.WebhookEventFailedPermanent ||
			existing.State == storage.WebhookEventCompleted || existing.State == storage.WebhookEventSuperseded {
			existing.State = storage.WebhookEventPending
			existing.Attempts = 0
			existing.Payload = append([]byte(nil), event.Payload...)
			existing.LeaseOwner = ""
			existing.LeaseToken = ""
			existing.LeaseExpiresAt = nil
			existing.RetryAfter = nil
			existing.StartedAt = nil
			existing.CompletedAt = nil
			return true, nil
		}
		return false, nil
	}
	// Mirror the real store's insert contract: a fresh insert populates the
	// caller's event.ID, while the reopen branch above leaves it zero — the
	// discriminator callers use to tell the two apart.
	s.nextID++
	event.ID = s.nextID
	copy := *event
	copy.Payload = append([]byte(nil), event.Payload...)
	if copy.State == "" {
		copy.State = storage.WebhookEventPending
	}
	s.events[key] = &copy
	return true, nil
}

func (s *recordingWebhookEventStore) GetByDeliveryID(_ context.Context, provider, deliveryID string) (*storage.WebhookEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	event := s.events[provider+":"+deliveryID]
	if event == nil {
		return nil, nil
	}
	copy := *event
	copy.Payload = append([]byte(nil), event.Payload...)
	return &copy, nil
}

func (s *recordingWebhookEventStore) HasEventForHead(_ context.Context, provider, repository string, pullRequest int, headSHA string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range s.events {
		if event.Provider == provider && event.Repository == repository &&
			event.PullRequest == pullRequest && event.HeadSHA == headSHA &&
			event.Event == "pull_request" && isAutoPlannablePullRequestAction(event.Action) &&
			event.State != storage.WebhookEventSuperseded &&
			(event.State != storage.WebhookEventFailed ||
				!strings.HasPrefix(event.DeliveryID, storage.SynthesizedWebhookDeliveryIDPrefix)) {
			return true, nil
		}
	}
	return false, nil
}

func (s *recordingWebhookEventStore) FindNext(context.Context, string, time.Duration) (*storage.WebhookEvent, error) {
	return nil, errors.New("FindNext not implemented by recordingWebhookEventStore")
}

func (s *recordingWebhookEventStore) Heartbeat(context.Context, int64, string, time.Duration) error {
	return errors.New("Heartbeat not implemented by recordingWebhookEventStore")
}

func (s *recordingWebhookEventStore) MarkCompleted(context.Context, int64, string) error {
	return errors.New("MarkCompleted not implemented by recordingWebhookEventStore")
}

func (s *recordingWebhookEventStore) MarkFailed(context.Context, int64, string, string, *time.Time) error {
	return errors.New("MarkFailed not implemented by recordingWebhookEventStore")
}

func (s *recordingWebhookEventStore) MarkFailedPermanent(context.Context, int64, string, string) error {
	return errors.New("MarkFailedPermanent not implemented by recordingWebhookEventStore")
}

func (s *recordingWebhookEventStore) Release(context.Context, int64, string) error {
	return errors.New("Release not implemented by recordingWebhookEventStore")
}

func (s *recordingWebhookEventStore) HasCoveringSuccessor(context.Context, *storage.WebhookEvent) (bool, error) {
	return false, errors.New("HasCoveringSuccessor not implemented by recordingWebhookEventStore")
}

func (s *recordingWebhookEventStore) SupersedeIfCovered(context.Context, *storage.WebhookEvent) (bool, error) {
	return false, errors.New("SupersedeIfCovered not implemented by recordingWebhookEventStore")
}

func (s *recordingWebhookEventStore) InboxStats(context.Context) (*storage.WebhookInboxStats, error) {
	return nil, errors.New("InboxStats not implemented by recordingWebhookEventStore")
}

func (s *recordingWebhookEventStore) TerminateStuckProcessing(_ context.Context, reason string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var terminated int64
	for _, event := range s.events {
		leaseExpired := event.LeaseExpiresAt != nil && !event.LeaseExpiresAt.After(now)
		if event.State == storage.WebhookEventProcessing && leaseExpired &&
			event.Attempts >= storage.MaxWebhookEventAttempts {
			event.State = storage.WebhookEventFailed
			event.LastError = reason
			event.LeaseOwner = ""
			event.LeaseToken = ""
			event.LeaseExpiresAt = nil
			event.RetryAfter = nil
			// Mirror the real SQL's completed_at = COALESCE(completed_at, NOW()).
			if event.CompletedAt == nil {
				completedAt := now
				event.CompletedAt = &completedAt
			}
			terminated++
		}
	}
	return terminated, nil
}

func TestDurablePullRequestWebhookQueuesAndAcks(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{}, nil, logger)
	clientFactory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := NewHandler(service, clientFactory, nil, logger, WithDurableWebhookDispatch())

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened", headSHA: "head-sha"}, nil)
	req.Header.Set(headerDeliveryID, "delivery-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"auto-plan queued"}`, rr.Body.String())
	event, err := events.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-1")
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, "pull_request", event.Event)
	require.Equal(t, "opened", event.Action)
	require.Equal(t, "octocat/hello-world", event.Repository)
	require.Equal(t, 1, event.PullRequest)
	require.Equal(t, "head-sha", event.HeadSHA)
	require.Equal(t, "12345", event.TenantID)
	require.NotEmpty(t, event.Payload)

	select {
	case <-clientFactory.forInstallationStarted:
		t.Fatal("durable request path should not create a GitHub client")
	default:
	}
}

func TestDurablePullRequestWebhookDeduplicatesDelivery(t *testing.T) {
	events := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: events}, &api.ServerConfig{}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	for range 2 {
		req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "synchronize"}, nil)
		req.Header.Set(headerDeliveryID, "delivery-1")
		rr := httptest.NewRecorder()

		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
	}

	events.mu.Lock()
	defer events.mu.Unlock()
	require.Len(t, events.events, 1)
}

// scriptedWebhookEventStore feeds queued events to the driver claim path and
// records terminal outcomes so tests can assert on MarkCompleted/MarkFailed.
type scriptedWebhookEventStore struct {
	recordingWebhookEventStore

	queueMu      sync.Mutex
	queue        []*storage.WebhookEvent
	nextID       int64
	heartbeatErr error

	// coveringSuccessor makes HasCoveringSuccessor report every claimed
	// auto-plannable pull_request event as having a covering successor, so
	// tests can exercise the live-head confirmation and supersede paths.
	coveringSuccessor bool
	// coveringSuccessorErr is returned by HasCoveringSuccessor so tests can
	// exercise the driver's fail-open (process anyway) probe-error path.
	coveringSuccessorErr error
	// coveringSuccessorCalls counts HasCoveringSuccessor invocations so
	// tests can assert the driver only probes auto-plannable pull_request
	// claims.
	coveringSuccessorCalls int
	// supersedeCovered makes SupersedeIfCovered report every claimed
	// auto-plannable pull_request event as covered by a newer delivery, so
	// tests can exercise the driver's superseded claim path.
	supersedeCovered bool
	// supersedeErr is returned by SupersedeIfCovered so tests can exercise
	// the driver's fail-open (process anyway) coalescing-error path.
	supersedeErr error
	// supersedeCalls counts SupersedeIfCovered invocations so tests can
	// assert the guarded write only runs after live-head confirmation.
	supersedeCalls int

	// Optional injected finish errors, returned before the corresponding
	// outcome channel send so tests can exercise the storage-failure paths.
	markCompletedErr       error
	markFailedErr          error
	markFailedPermanentErr error
	releaseErr             error

	completed       chan scriptedWebhookOutcome
	failed          chan scriptedWebhookFailure
	failedPermanent chan scriptedWebhookPermanentFailure
	released        chan scriptedWebhookOutcome
}

type scriptedWebhookOutcome struct {
	id         int64
	leaseToken string
}

type scriptedWebhookFailure struct {
	id         int64
	leaseToken string
	errMsg     string
	retryAfter *time.Time
}

type scriptedWebhookPermanentFailure struct {
	id         int64
	leaseToken string
	errMsg     string
}

func newScriptedWebhookEventStore(queue ...*storage.WebhookEvent) *scriptedWebhookEventStore {
	return &scriptedWebhookEventStore{
		recordingWebhookEventStore: *newRecordingWebhookEventStore(),
		queue:                      queue,
		completed:                  make(chan scriptedWebhookOutcome, 8),
		failed:                     make(chan scriptedWebhookFailure, 8),
		failedPermanent:            make(chan scriptedWebhookPermanentFailure, 8),
		released:                   make(chan scriptedWebhookOutcome, 8),
	}
}

func (s *scriptedWebhookEventStore) FindNext(_ context.Context, owner string, _ time.Duration) (*storage.WebhookEvent, error) {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()

	if len(s.queue) == 0 {
		return nil, nil
	}
	event := s.queue[0]
	s.queue = s.queue[1:]
	s.nextID++
	claimed := *event
	if claimed.ID == 0 {
		claimed.ID = s.nextID
	}
	claimed.LeaseOwner = owner
	claimed.LeaseToken = fmt.Sprintf("token-%d", claimed.ID)
	claimed.Attempts = event.Attempts + 1
	// Mirror the real claim contract: the claim consumes retry_after and the
	// returned event carries ClaimableSince, the later of receipt and the
	// consumed not-before time.
	claimed.ClaimableSince = claimed.ReceivedAt
	if claimed.RetryAfter != nil && claimed.RetryAfter.After(claimed.ClaimableSince) {
		claimed.ClaimableSince = *claimed.RetryAfter
	}
	claimed.RetryAfter = nil
	return &claimed, nil
}

func (s *scriptedWebhookEventStore) Heartbeat(context.Context, int64, string, time.Duration) error {
	return s.heartbeatErr
}

func (s *scriptedWebhookEventStore) HasCoveringSuccessor(context.Context, *storage.WebhookEvent) (bool, error) {
	s.queueMu.Lock()
	s.coveringSuccessorCalls++
	s.queueMu.Unlock()
	if s.coveringSuccessorErr != nil {
		return false, s.coveringSuccessorErr
	}
	return s.coveringSuccessor, nil
}

func (s *scriptedWebhookEventStore) SupersedeIfCovered(_ context.Context, event *storage.WebhookEvent) (bool, error) {
	s.queueMu.Lock()
	s.supersedeCalls++
	s.queueMu.Unlock()
	if s.supersedeErr != nil {
		return false, s.supersedeErr
	}
	if !s.supersedeCovered {
		return false, nil
	}
	event.State = storage.WebhookEventSuperseded
	return true, nil
}

func (s *scriptedWebhookEventStore) MarkCompleted(_ context.Context, id int64, leaseToken string) error {
	if s.markCompletedErr != nil {
		return s.markCompletedErr
	}
	s.completed <- scriptedWebhookOutcome{id: id, leaseToken: leaseToken}
	return nil
}

func (s *scriptedWebhookEventStore) MarkFailed(_ context.Context, id int64, leaseToken string, errMsg string, retryAfter *time.Time) error {
	if s.markFailedErr != nil {
		return s.markFailedErr
	}
	s.failed <- scriptedWebhookFailure{id: id, leaseToken: leaseToken, errMsg: errMsg, retryAfter: retryAfter}
	return nil
}

func (s *scriptedWebhookEventStore) MarkFailedPermanent(_ context.Context, id int64, leaseToken string, errMsg string) error {
	if s.markFailedPermanentErr != nil {
		return s.markFailedPermanentErr
	}
	s.failedPermanent <- scriptedWebhookPermanentFailure{id: id, leaseToken: leaseToken, errMsg: errMsg}
	return nil
}

func (s *scriptedWebhookEventStore) Release(_ context.Context, id int64, leaseToken string) error {
	if s.releaseErr != nil {
		return s.releaseErr
	}
	s.released <- scriptedWebhookOutcome{id: id, leaseToken: leaseToken}
	return nil
}

func newDurableDriverHandler(t *testing.T, store storage.WebhookEventStore, config *api.ServerConfig, factory *fakeClientFactory) *Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if config == nil {
		config = &api.ServerConfig{}
	}
	service := api.New(&durableWebhookTestStorage{webhookEvents: store}, config, nil, logger)
	if factory == nil {
		factory = &fakeClientFactory{}
	}
	return NewHandler(service, factory, nil, logger, WithDurableWebhookDispatch())
}

func durablePullRequestEvent(t *testing.T) *storage.WebhookEvent {
	t.Helper()
	payload := []byte(`{
		"action": "opened",
		"pull_request": {"number": 7, "head": {"sha": "head-sha", "ref": "feature"}},
		"repository": {"full_name": "octocat/hello-world"},
		"installation": {"id": 12345}
	}`)
	return &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  "delivery-driver-1",
		Event:       "pull_request",
		Action:      "opened",
		Repository:  "octocat/hello-world",
		PullRequest: 7,
		HeadSHA:     "head-sha",
		TenantID:    "12345",
		Payload:     payload,
		ReceivedAt:  time.Now(),
	}
}

// newLiveHeadGitHubFactory wires a fakeClientFactory whose installation
// client is backed by a fake GitHub server serving the claimed PR
// (octocat/hello-world#7) with the given live head SHA and state, so tests
// can script the live-head confirmation that gates coalescing. The returned
// counter reports how many live PR fetches the driver issued.
func newLiveHeadGitHubFactory(t *testing.T, liveHeadSHA, state string) (*fakeClientFactory, *atomic.Int32) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	fetches := &atomic.Int32{}
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"number": 7,
			"state":  state,
			"head":   map[string]any{"sha": liveHeadSHA, "ref": "feature"},
		}))
	})
	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
	return &fakeClientFactory{client: installClient}, fetches
}

func TestDurableWebhookDriverCompletesUnsupportedEvent(t *testing.T) {
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-unsupported",
		Event:      "issue_comment",
		Payload:    []byte(`{}`),
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case outcome := <-store.completed:
		require.Equal(t, int64(1), outcome.id)
		require.Equal(t, "token-1", outcome.leaseToken)
	default:
		t.Fatal("expected unsupported event to be marked completed")
	}
	require.Empty(t, store.failed)
}

func TestDurableWebhookDriverFailsMalformedPullRequestTerminally(t *testing.T) {
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-malformed",
		Event:      "pull_request",
		Payload:    []byte(`{not json`),
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failedPermanent:
		require.Contains(t, failure.errMsg, "decode durable pull_request delivery")
	default:
		t.Fatal("expected malformed event to be dead-lettered")
	}
	require.Empty(t, store.failed, "a malformed payload is deterministic and must not burn retry budget")
	require.Empty(t, store.completed)
}

// autoPlanFailureIsPermanent gates dead-lettering, so it must recognize the
// file-listing cap sentinel through the wrapping the fetch path applies, and
// must leave transient failures — GitHub outages, timeouts — retryable.
func TestAutoPlanFailureIsPermanent(t *testing.T) {
	truncatedListing := fmt.Errorf("fetch PR files for octocat/hello-world#7: %w",
		fmt.Errorf("list PR files for octocat/hello-world#7 reached GitHub API limit: %w", ghclient.ErrPRFilesIncomplete))
	assert.True(t, autoPlanFailureIsPermanent(truncatedListing),
		"the file-listing cap is deterministic for the head and must dead-letter")

	assert.False(t, autoPlanFailureIsPermanent(errors.New("github unavailable: connection refused")),
		"a transient GitHub failure must stay retryable")
	assert.False(t, autoPlanFailureIsPermanent(fmt.Errorf("list PR files: %w", ghclient.ErrGitHubUnavailable)),
		"a classified GitHub outage must stay retryable")
	assert.False(t, autoPlanFailureIsPermanent(context.DeadlineExceeded),
		"a timeout must stay retryable")
}

func TestDurableInstallationID(t *testing.T) {
	tests := []struct {
		name        string
		event       string
		pullRequest int
		tenantID    string
		wantID      int64
		wantErr     string
	}{
		{name: "valid tenant", event: "pull_request", pullRequest: 7, tenantID: "12345", wantID: 12345},
		{name: "unparseable tenant", event: "pull_request", pullRequest: 7, tenantID: "not-an-installation-id", wantErr: "unparseable tenant ID"},
		{name: "missing tenant", event: "pull_request", pullRequest: 7, tenantID: "", wantErr: "missing its stored tenant"},
		{name: "zero tenant", event: "pull_request", pullRequest: 7, tenantID: "0", wantErr: "non-positive installation ID"},
		{name: "negative tenant", event: "pull_request", pullRequest: 7, tenantID: "-42", wantErr: "non-positive installation ID"},
		{name: "zero tenant on PR-less event", event: "push", tenantID: "0", wantErr: "non-positive installation ID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &storage.WebhookEvent{
				DeliveryID:  "delivery-tenant",
				Event:       tt.event,
				Repository:  "octocat/hello-world",
				PullRequest: tt.pullRequest,
				TenantID:    tt.tenantID,
			}
			id, err := durableInstallationID(event)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.ErrorContains(t, err, "octocat/hello-world")
				if tt.pullRequest > 0 {
					require.ErrorContains(t, err, "octocat/hello-world#7")
				} else {
					require.NotContains(t, err.Error(), "#0", "PR-less deliveries must not render a fabricated #0 location")
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantID, id)
		})
	}
}

func TestDurableWebhookDriverFailsUnparseableTenantTerminally(t *testing.T) {
	event := durablePullRequestEvent(t)
	event.TenantID = "not-an-installation-id"
	store := newScriptedWebhookEventStore(event)
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failedPermanent:
		require.Contains(t, failure.errMsg, "unparseable tenant ID")
	default:
		t.Fatal("expected unparseable tenant to be dead-lettered")
	}
	require.Empty(t, store.failed, "a corrupted persisted tenant is deterministic and must not burn retry budget")
	require.Empty(t, store.completed)
}

// servePRFileListPastCap registers a PR-files endpoint that keeps handing out
// another page, the way GitHub answers a pull request whose diff is larger than
// it will report in full: SchemaBot walks pages until it reaches the per-PR file
// cap and then refuses to plan from the partial list. The fake never encodes the
// cap's value, so it cannot go stale, and it stops paginating far past the cap so
// a lost cap check fails the test instead of hanging it.
//
// visibleSchemaPath, when non-empty, is served as the first file of the first
// page, so the truncated listing SchemaBot sees contains that schema change.
// When empty, every reported file is a plain source file — the shape of a large
// refactor PR with no visible schema change.
func servePRFileListPastCap(t *testing.T, mux *http.ServeMux, pr int, pages *atomic.Int64, visibleSchemaPath string) {
	t.Helper()
	const (
		filesPerPage = 100
		lastPage     = 500
	)
	mux.HandleFunc(fmt.Sprintf("GET /repos/octocat/hello-world/pulls/%d/files", pr), func(w http.ResponseWriter, r *http.Request) {
		pages.Add(1)
		page, err := strconv.Atoi(r.URL.Query().Get("page"))
		if err != nil || page < 1 {
			page = 1
		}
		files := make([]map[string]any, filesPerPage)
		for i := range files {
			files[i] = map[string]any{
				"filename": fmt.Sprintf("apps/service/src/file_%d_%d.go", page, i),
				"status":   "modified",
			}
		}
		if page == 1 && visibleSchemaPath != "" {
			files[0] = map[string]any{
				"filename": visibleSchemaPath,
				"status":   "modified",
			}
		}
		if page < lastPage {
			w.Header().Set("Link", fmt.Sprintf(`<%s?page=%d>; rel="next"`, r.URL.Path, page+1))
		}
		require.NoError(t, json.NewEncoder(w).Encode(files))
	})
}

// checksDisabledRepoConfig scopes a driver test to the delivery's retry
// classification: with check publishing off, the run stops at the discovery
// failure instead of reaching the aggregate publish path.
func checksDisabledRepoConfig() *api.ServerConfig {
	return &api.ServerConfig{Repos: map[string]api.RepoConfig{
		"octocat/hello-world": {EnableChecks: new(false)},
	}}
}

// A pull request that changes more files than GitHub will report for a single
// PR dead-letters on its first attempt instead of retrying. The cap is a
// property of the PR, so every retry would list the same truncated diff, spend
// the retry budget, and end in a terminal-drop log that reads like a lost
// delivery.
func TestDurableWebhookDriverFailsPRFileCapTerminally(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	ghClient, mux := setupGitHubServer(t)
	serveCheckRunPRHead(t, mux, "head-sha")
	var filePages atomic.Int64
	servePRFileListPastCap(t, mux, 7, &filePages, "")
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghClient, testLogger())}
	h := newDurableDriverHandler(t, store, checksDisabledRepoConfig(), factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failedPermanent:
		require.Contains(t, failure.errMsg, "reached GitHub API limit")
		require.Contains(t, failure.errMsg, "config discovery is incomplete")
	default:
		t.Fatal("expected an over-cap PR to be dead-lettered")
	}
	require.Empty(t, store.failed, "GitHub's per-PR file cap is deterministic and must not burn retry budget")
	require.Empty(t, store.completed, "a delivery SchemaBot could not plan must not be completed")
	require.Positive(t, filePages.Load(), "the driver must have listed PR files before failing closed")
}

func TestDurableWebhookDriverRetriesBootstrapFailure(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	factory := &fakeClientFactory{forInstallationErr: errors.New("installation token unavailable")}
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

func TestDurableWebhookDriverCompletesUnregisteredRepo(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	config := &api.ServerConfig{Repos: map[string]api.RepoConfig{"other/repo": {}}}
	factory := &fakeClientFactory{forInstallationStarted: make(chan struct{})}
	h := newDurableDriverHandler(t, store, config, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected unregistered-repo event to be marked completed")
	}
	require.Empty(t, store.failed)
	select {
	case <-factory.forInstallationStarted:
		t.Fatal("unregistered repo must not create a GitHub client")
	default:
	}
}

// A claimed auto-plan delivery covered by a newer delivery for the same PR —
// with GitHub confirming its head is no longer the PR's current head — is
// discarded at claim time: the driver marks it superseded and never plans the
// stale head, so a burst of pushes plans only the newest head.
func TestDurableWebhookDriverSupersedesCoveredAutoPlan(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.coveringSuccessor = true
	store.supersedeCovered = true
	factory, fetches := newLiveHeadGitHubFactory(t, "newer-head", "open")
	h := newDurableDriverHandler(t, store, nil, factory)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		t.Error("a superseded delivery must not be processed")
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.completed, "a superseded delivery must not be marked completed")
	require.Empty(t, store.failed, "a superseded delivery must not be marked failed")
	require.Equal(t, int32(1), fetches.Load(), "staleness must be confirmed against the live PR")
}

// A closed PR's auto-plan delivery is coalesced without a head comparison:
// planning a closed PR is pointless, and the covering successor (the closed
// delivery) runs the cleanup.
func TestDurableWebhookDriverSupersedesAutoPlanForClosedPR(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.coveringSuccessor = true
	store.supersedeCovered = true
	factory, _ := newLiveHeadGitHubFactory(t, "head-sha", "closed")
	h := newDurableDriverHandler(t, store, nil, factory)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		t.Error("a superseded delivery must not be processed")
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.completed)
	require.Empty(t, store.failed)
}

// received_at is arrival order, not push order: a delayed or redelivered
// old-head delivery can arrive after the delivery that carries the PR's real
// current head. When GitHub confirms the claimed head IS the live head, the
// claim must survive — discarding it could leave the newest head unplanned in
// configurations without reconciler synthesis.
func TestDurableWebhookDriverKeepsAutoPlanCarryingLiveHead(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.coveringSuccessor = true
	store.supersedeCovered = true
	factory, fetches := newLiveHeadGitHubFactory(t, "head-sha", "open")
	h := newDurableDriverHandler(t, store, nil, factory)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1, "the delivery carrying the live head must process normally")
	require.Empty(t, store.failed)
	require.Equal(t, int32(1), fetches.Load())
	store.queueMu.Lock()
	supersedes := store.supersedeCalls
	store.queueMu.Unlock()
	require.Zero(t, supersedes, "a delivery confirmed to carry the live head must never reach the supersede write")
}

// Without a covering successor there is nothing to coalesce against, so the
// driver must not spend a GitHub call verifying the head: the common
// no-successor claim stays storage-only.
func TestDurableWebhookDriverSkipsHeadCheckWithoutCoveringSuccessor(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	factory, fetches := newLiveHeadGitHubFactory(t, "newer-head", "open")
	h := newDurableDriverHandler(t, store, nil, factory)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1)
	require.Zero(t, fetches.Load(), "no covering successor must mean no live PR fetch")
	store.queueMu.Lock()
	supersedes := store.supersedeCalls
	store.queueMu.Unlock()
	require.Zero(t, supersedes)
}

// Coalescing is an optimization: when the covering-successor probe itself
// fails, the claim stays alive and the delivery processes normally — planning
// a possibly stale head is idempotent, dropping the claim on a query error is
// not safe.
func TestDurableWebhookDriverProcessesWhenSuccessorProbeFails(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.coveringSuccessorErr = errors.New("storage briefly unavailable")
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1, "a delivery whose coalescing probe failed must process normally")
	require.Empty(t, store.failed)
}

// When the live-PR fetch that would confirm staleness fails, the driver
// cannot know whether the claimed head is current, so it fails open and
// processes the delivery normally instead of discarding it.
func TestDurableWebhookDriverProcessesWhenLiveHeadFetchFails(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.coveringSuccessor = true
	store.supersedeCovered = true
	client, _ := setupGitHubServer(t) // no PR route: the live fetch 404s
	factory := &fakeClientFactory{client: ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")}
	h := newDurableDriverHandler(t, store, nil, factory)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1, "a delivery whose live-head check failed must process normally")
	require.Empty(t, store.failed)
	store.queueMu.Lock()
	supersedes := store.supersedeCalls
	store.queueMu.Unlock()
	require.Zero(t, supersedes, "an unconfirmed head must never reach the supersede write")
}

// A claimed delivery without a stored head SHA cannot be verified against the
// live PR, so it processes normally rather than being discarded on an
// unverifiable comparison.
func TestDurableWebhookDriverProcessesAutoPlanWithoutHeadSHA(t *testing.T) {
	event := durablePullRequestEvent(t)
	event.HeadSHA = ""
	store := newScriptedWebhookEventStore(event)
	store.coveringSuccessor = true
	store.supersedeCovered = true
	factory, fetches := newLiveHeadGitHubFactory(t, "newer-head", "open")
	h := newDurableDriverHandler(t, store, nil, factory)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1)
	require.Zero(t, fetches.Load(), "a delivery without a head SHA must not attempt live-head verification")
}

// The covering successor can disappear between the advisory probe and the
// guarded write — the write re-evaluates the predicate atomically and answers
// false, and the claim processes normally.
func TestDurableWebhookDriverProcessesWhenSuccessorVanishesBeforeWrite(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.coveringSuccessor = true
	store.supersedeCovered = false
	factory, _ := newLiveHeadGitHubFactory(t, "newer-head", "open")
	h := newDurableDriverHandler(t, store, nil, factory)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1, "a claim whose successor vanished must process normally")
	require.Empty(t, store.failed)
	store.queueMu.Lock()
	supersedes := store.supersedeCalls
	store.queueMu.Unlock()
	require.Equal(t, 1, supersedes, "the guarded write must have re-evaluated the predicate")
}

// When the guarded supersede write fails on storage, the claim stays alive
// and the delivery processes normally.
func TestDurableWebhookDriverProcessesWhenSupersedeWriteFails(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.coveringSuccessor = true
	store.supersedeErr = errors.New("storage briefly unavailable")
	factory, _ := newLiveHeadGitHubFactory(t, "newer-head", "open")
	h := newDurableDriverHandler(t, store, nil, factory)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1, "a delivery whose supersede write failed must process normally")
	require.Empty(t, store.failed)
}

// Losing the claim while superseding means another driver owns the delivery;
// this driver walks away without processing or writing a terminal state.
func TestDurableWebhookDriverStopsWhenSupersedeLosesLease(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.coveringSuccessor = true
	store.supersedeErr = storage.ErrWebhookEventLeaseLost
	factory, _ := newLiveHeadGitHubFactory(t, "newer-head", "open")
	h := newDurableDriverHandler(t, store, nil, factory)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		t.Error("a lease-lost delivery must not be processed")
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.completed)
	require.Empty(t, store.failed)
}

// Only auto-plannable pull_request claims are coalesced. A closed delivery's
// cleanup must always run, and non-PR events have no per-PR successor notion,
// so neither may reach the covering-successor probe.
func TestDurableWebhookDriverSkipsSupersedeForNonAutoPlanClaims(t *testing.T) {
	closed := durablePullRequestEvent(t)
	closed.Action = "closed"
	store := newScriptedWebhookEventStore(closed, &storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-non-pr",
		Event:      "issue_comment",
		Payload:    []byte(`{}`),
	})
	store.coveringSuccessor = true
	store.supersedeCovered = true
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")
	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	store.queueMu.Lock()
	probes := store.coveringSuccessorCalls
	calls := store.supersedeCalls
	store.queueMu.Unlock()
	require.Zero(t, probes, "closed and non-pull_request claims must never be probed for a covering successor")
	require.Zero(t, calls, "closed and non-pull_request claims must never reach the supersede write")
	require.Len(t, store.completed, 2, "both deliveries must process to completion")
}

func TestDurableWebhookHeartbeatLossCancelsRun(t *testing.T) {
	store := newScriptedWebhookEventStore()
	store.heartbeatErr = storage.ErrWebhookEventLeaseLost
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookLeaseDuration = 30 * time.Millisecond

	runCtx, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	event := durablePullRequestEvent(t)
	event.ID = 1
	event.LeaseToken = "token-1"

	stop := h.startDurableWebhookHeartbeat(runCtx, 0, event, cancelRun)

	select {
	case <-runCtx.Done():
	case <-time.After(durableWebhookTestDeadline):
		t.Fatal("expected heartbeat lease loss to cancel the run context")
	}
	require.ErrorIs(t, stop(), storage.ErrWebhookEventLeaseLost)
}

func TestDurableWebhookHeartbeatLossSkipsCompletion(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.heartbeatErr = storage.ErrWebhookEventLeaseLost
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookLeaseDuration = 30 * time.Millisecond
	h.durableWebhookProcessOverride = func(ctx context.Context, _ *storage.WebhookEvent) (bool, error) {
		// Simulate work that finishes successfully just as the lease is lost:
		// wait for the failed heartbeat to cancel the run, then report success.
		<-ctx.Done()
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.completed, "lease-lost delivery must not be marked completed")
	require.Empty(t, store.failed, "delivery must be left processing so lease expiry hands it to another driver")
}

func TestDurableWebhookDriverStopsRetryingAtAttemptCap(t *testing.T) {
	event := durablePullRequestEvent(t)
	event.Attempts = maxDurableWebhookAttempts - 1 // FindNext claim increments to the cap
	store := newScriptedWebhookEventStore(event)
	factory := &fakeClientFactory{forInstallationErr: errors.New("installation token unavailable")}
	h := newDurableDriverHandler(t, store, nil, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.Nil(t, failure.retryAfter, "delivery at the attempt cap must fail terminally")
		require.Contains(t, failure.errMsg, "installation token unavailable")
	default:
		t.Fatal("expected exhausted delivery to be marked failed")
	}
	require.Empty(t, store.completed)
}

// A single wake drains the whole backlog rather than one delivery per signal, so
// a burst of queued deliveries is worked down without waiting for the next tick.
func TestDurableWebhookDrainProcessesBacklogInOnePass(t *testing.T) {
	store := newScriptedWebhookEventStore(
		&storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: "d1", Event: "issue_comment", Payload: []byte(`{}`)},
		&storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: "d2", Event: "issue_comment", Payload: []byte(`{}`)},
		&storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: "d3", Event: "issue_comment", Payload: []byte(`{}`)},
	)
	h := newDurableDriverHandler(t, store, nil, nil)

	h.drainDurableWebhooks(t.Context(), 0, "test-host/1/webhook-driver-0")

	for i := range 3 {
		select {
		case <-store.completed:
		default:
			t.Fatalf("expected delivery %d of 3 to be drained in a single pass", i+1)
		}
	}
	require.Empty(t, store.failed)
}

func TestDurableWebhookDriverRecoversPanic(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		panic("poison payload")
	}

	// Must not crash the process: a panicking delivery stays claimable after
	// lease expiry, so an unrecovered panic would crash-loop every replica.
	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failed:
		require.NotNil(t, failure.retryAfter, "recovered panic must consume the normal retry budget")
		require.Contains(t, failure.errMsg, "panic processing durable webhook delivery")
		require.Contains(t, failure.errMsg, "poison payload")
	default:
		t.Fatal("expected recovered panic to be recorded as a delivery failure")
	}
	require.Empty(t, store.completed)
}

func TestDurableWebhookShutdownReleasesClaim(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	h := newDurableDriverHandler(t, store, nil, nil)

	driverCtx, cancelDriver := context.WithCancel(t.Context())
	defer cancelDriver()
	h.durableWebhookProcessOverride = func(ctx context.Context, _ *storage.WebhookEvent) (bool, error) {
		// Simulate shutdown arriving mid-run: the driver context is cancelled
		// while the delivery is being processed.
		cancelDriver()
		<-ctx.Done()
		return true, fmt.Errorf("auto-plan interrupted: %w", ctx.Err())
	}

	h.driveNextDurableWebhook(driverCtx, 0, "test-host/1/webhook-driver-0")

	select {
	case released := <-store.released:
		require.Equal(t, "token-1", released.leaseToken)
	default:
		t.Fatal("expected shutdown-cancelled claim to be released")
	}
	require.Empty(t, store.failed, "shutdown cancellation must not consume the attempt budget")
	require.Empty(t, store.completed)
}

func TestDurableWebhookShutdownReleasesClaimWhenCancelErrorIsStringified(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	h := newDurableDriverHandler(t, store, nil, nil)

	driverCtx, cancelDriver := context.WithCancel(t.Context())
	defer cancelDriver()
	h.durableWebhookProcessOverride = func(ctx context.Context, _ *storage.WebhookEvent) (bool, error) {
		cancelDriver()
		<-ctx.Done()
		// A client that stringifies the cancellation drops the context.Canceled
		// sentinel from the error chain. The refund must still fire off the
		// driver-context cancellation rather than an errors.Is unwrap, or the
		// interrupted claim burns an attempt via MarkFailed.
		return true, errors.New(ctx.Err().Error())
	}

	h.driveNextDurableWebhook(driverCtx, 0, "test-host/1/webhook-driver-0")

	select {
	case released := <-store.released:
		require.Equal(t, "token-1", released.leaseToken)
	default:
		t.Fatal("expected shutdown-cancelled claim to be released even when the cancel error is stringified")
	}
	require.Empty(t, store.failed, "shutdown cancellation must not consume the attempt budget")
	require.Empty(t, store.completed)
}

func TestDurableWebhookHeartbeatTransientErrorKeepsRunAlive(t *testing.T) {
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-transient-heartbeat",
		Event:      "push",
		Payload:    []byte(`{}`),
	})
	store.heartbeatErr = errors.New("transient store blip")
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookLeaseDuration = 30 * time.Millisecond
	h.durableWebhookProcessOverride = func(ctx context.Context, _ *storage.WebhookEvent) (bool, error) {
		// Outlast several failing heartbeats; a transient heartbeat error must
		// not cancel the run or block completion.
		select {
		case <-ctx.Done():
			return true, fmt.Errorf("run cancelled by transient heartbeat error: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
			return false, nil
		}
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case <-store.completed:
	default:
		t.Fatal("expected delivery to complete despite transient heartbeat errors")
	}
	require.Empty(t, store.failed)
}

func TestDurableWebhookDispatchLifecycleDrainsQueuedEvent(t *testing.T) {
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-lifecycle",
		Event:      "push",
		Payload:    []byte(`{"ref": "refs/heads/feature", "after": "abc123def456", "repository": {"full_name": "octocat/hello-world", "default_branch": "main"}}`),
	})
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookPollInterval = 10 * time.Millisecond

	h.StartDurableWebhookDispatch(t.Context())

	select {
	case outcome := <-store.completed:
		require.Equal(t, "token-1", outcome.leaseToken)
	case <-time.After(durableWebhookTestDeadline):
		t.Fatal("expected driver pool to drain the queued event")
	}

	h.StopDurableWebhookDispatch()
	// Stop must be idempotent.
	h.StopDurableWebhookDispatch()
}
