package webhook

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
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
	events map[string]*storage.WebhookEvent
}

func newRecordingWebhookEventStore() *recordingWebhookEventStore {
	return &recordingWebhookEventStore{events: make(map[string]*storage.WebhookEvent)}
}

func (s *recordingWebhookEventStore) Create(_ context.Context, event *storage.WebhookEvent) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := event.Provider + ":" + event.DeliveryID
	if _, ok := s.events[key]; ok {
		return false, nil
	}
	copy := *event
	copy.Payload = append([]byte(nil), event.Payload...)
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

func (s *recordingWebhookEventStore) Release(context.Context, int64, string) error {
	return errors.New("Release not implemented by recordingWebhookEventStore")
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

	completed chan scriptedWebhookOutcome
	failed    chan scriptedWebhookFailure
	released  chan scriptedWebhookOutcome
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

func newScriptedWebhookEventStore(queue ...*storage.WebhookEvent) *scriptedWebhookEventStore {
	return &scriptedWebhookEventStore{
		recordingWebhookEventStore: *newRecordingWebhookEventStore(),
		queue:                      queue,
		completed:                  make(chan scriptedWebhookOutcome, 8),
		failed:                     make(chan scriptedWebhookFailure, 8),
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
	return &claimed, nil
}

func (s *scriptedWebhookEventStore) Heartbeat(context.Context, int64, string, time.Duration) error {
	return s.heartbeatErr
}

func (s *scriptedWebhookEventStore) MarkCompleted(_ context.Context, id int64, leaseToken string) error {
	s.completed <- scriptedWebhookOutcome{id: id, leaseToken: leaseToken}
	return nil
}

func (s *scriptedWebhookEventStore) MarkFailed(_ context.Context, id int64, leaseToken string, errMsg string, retryAfter *time.Time) error {
	s.failed <- scriptedWebhookFailure{id: id, leaseToken: leaseToken, errMsg: errMsg, retryAfter: retryAfter}
	return nil
}

func (s *scriptedWebhookEventStore) Release(_ context.Context, id int64, leaseToken string) error {
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
	}
}

func TestDurableWebhookDriverCompletesUnsupportedEvent(t *testing.T) {
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-unsupported",
		Event:      "push",
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
	case failure := <-store.failed:
		require.Nil(t, failure.retryAfter, "malformed payload must not be retried")
		require.Contains(t, failure.errMsg, "decode durable pull_request delivery")
	default:
		t.Fatal("expected malformed event to be marked failed")
	}
	require.Empty(t, store.completed)
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
		&storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: "d1", Event: "push", Payload: []byte(`{}`)},
		&storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: "d2", Event: "push", Payload: []byte(`{}`)},
		&storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: "d3", Event: "push", Payload: []byte(`{}`)},
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
		Payload:    []byte(`{}`),
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
