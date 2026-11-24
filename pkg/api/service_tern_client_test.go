package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/clock"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// ternClientTestDeadline bounds every wait in these tests. A wait that reaches
// it fails the test; it is never the way a test expects to proceed.
const ternClientTestDeadline = 30 * time.Second

// closeCountingTernClient is a tern client that counts how often it is closed.
type closeCountingTernClient struct {
	tern.Client
	closes atomic.Int32
}

func (c *closeCountingTernClient) Close() error {
	c.closes.Add(1)
	return nil
}

// ternResolver stands in for local client construction. Each call to build
// records the deployment, optionally waits on a gate, then returns the next
// scripted outcome.
type ternResolver struct {
	calls   atomic.Int32
	started chan string
	gates   map[string]chan struct{}
	failErr atomic.Pointer[error]
	built   []*closeCountingTernClient
	mu      sync.Mutex
}

func newTernResolver(gatedDeployments ...string) *ternResolver {
	r := &ternResolver{started: make(chan string, 64), gates: make(map[string]chan struct{})}
	for _, deployment := range gatedDeployments {
		r.gates[deployment] = make(chan struct{})
	}
	return r
}

func (r *ternResolver) fail(err error) { r.failErr.Store(&err) }

func (r *ternResolver) succeed() { r.failErr.Store(nil) }

func (r *ternResolver) build(_, database, _ string, _ EnvironmentConfig) (tern.Client, error) {
	r.calls.Add(1)
	r.started <- database
	if gate, ok := r.gates[database]; ok {
		<-gate
	}
	if errPtr := r.failErr.Load(); errPtr != nil {
		return nil, *errPtr
	}
	client := &closeCountingTernClient{}
	r.mu.Lock()
	r.built = append(r.built, client)
	r.mu.Unlock()
	return client, nil
}

func newTernClientTestService(t *testing.T, resolver *ternResolver, deployments ...string) *Service {
	t.Helper()
	databases := make(map[string]DatabaseConfig, len(deployments))
	for _, deployment := range deployments {
		databases[deployment] = DatabaseConfig{
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {DSN: "root@tcp(localhost:3306)/" + deployment},
			},
		}
	}
	svc := New(&mockStorage{}, &ServerConfig{Databases: databases}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.newLocalTernClientFn = resolver.build
	return svc
}

type ternClientResult struct {
	client tern.Client
	err    error
}

func ternClientAsync(svc *Service, deployment string) <-chan ternClientResult {
	out := make(chan ternClientResult, 1)
	go func() {
		client, err := svc.TernClient(deployment, "staging")
		out <- ternClientResult{client: client, err: err}
	}()
	return out
}

func receiveTernClientResult(t *testing.T, ch <-chan ternClientResult) ternClientResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), ternClientTestDeadline)
	defer cancel()
	select {
	case result := <-ch:
		return result
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for a TernClient call to return")
		return ternClientResult{}
	}
}

func receiveStartedResolution(t *testing.T, resolver *ternResolver) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), ternClientTestDeadline)
	defer cancel()
	select {
	case database := <-resolver.started:
		return database
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for a resolution to start")
		return ""
	}
}

// While deployment "slow" is stuck resolving its credentials, a request for
// deployment "other" gets its client without waiting: the lock is not held
// across the slow resolution.
func TestService_TernClientSlowResolutionDoesNotBlockOtherDeployments(t *testing.T) {
	resolver := newTernResolver("slow")
	svc := newTernClientTestService(t, resolver, "slow", "other")

	slowResult := ternClientAsync(svc, "slow")
	assert.Equal(t, "slow", receiveStartedResolution(t, resolver))

	otherResult := receiveTernClientResult(t, ternClientAsync(svc, "other"))
	require.NoError(t, otherResult.err)
	assert.NotNil(t, otherResult.client)

	// The cached client for the other deployment is served while "slow" is
	// still unresolved.
	cached, err := svc.TernClient("other", "staging")
	require.NoError(t, err)
	assert.Same(t, otherResult.client, cached)

	close(resolver.gates["slow"])
	result := receiveTernClientResult(t, slowResult)
	require.NoError(t, result.err)
	assert.NotNil(t, result.client)
}

// Concurrent first callers of one deployment share a single resolution and all
// receive the same client.
func TestService_TernClientConcurrentFirstCallersResolveOnce(t *testing.T) {
	const callers = 16
	resolver := newTernResolver("slow")
	svc := newTernClientTestService(t, resolver, "slow")

	results := make([]<-chan ternClientResult, 0, callers)
	for range callers {
		results = append(results, ternClientAsync(svc, "slow"))
	}
	assert.Equal(t, "slow", receiveStartedResolution(t, resolver))
	close(resolver.gates["slow"])

	var first tern.Client
	for _, ch := range results {
		result := receiveTernClientResult(t, ch)
		require.NoError(t, result.err)
		if first == nil {
			first = result.client
		}
		assert.Same(t, first, result.client)
	}
	assert.Equal(t, int32(1), resolver.calls.Load(), "one resolution serves every concurrent first caller")
}

// A failed resolution is remembered: a second call inside the retry window gets
// the cached failure without calling the resolver, and the first call after the
// window resolves again and recovers.
func TestService_TernClientFailureIsCachedUntilTTL(t *testing.T) {
	resolver := newTernResolver()
	svc := newTernClientTestService(t, resolver, "orders")
	fake := clock.NewFake(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	require.NoError(t, svc.SetClock(fake))

	secretErr := errors.New("secret store timed out")
	resolver.fail(secretErr)

	_, err := svc.TernClient("orders", "staging")
	require.ErrorIs(t, err, secretErr)
	assert.NotContains(t, err.Error(), "cached failure")
	assert.Equal(t, int32(1), resolver.calls.Load())

	fake.Advance(ternClientFailureTTL - time.Second)
	_, err = svc.TernClient("orders", "staging")
	require.ErrorIs(t, err, secretErr)
	assert.Contains(t, err.Error(), "cached failure from the last attempt")
	assert.Equal(t, int32(1), resolver.calls.Load(), "a call inside the window does not resolve again")

	resolver.succeed()
	fake.Advance(time.Second)
	client, err := svc.TernClient("orders", "staging")
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, int32(2), resolver.calls.Load(), "the first call after the window resolves again")

	cached, err := svc.TernClient("orders", "staging")
	require.NoError(t, err)
	assert.Same(t, client, cached)
	assert.Equal(t, int32(2), resolver.calls.Load())
}

// Registering a client for a deployment drops its remembered failure, so a
// fixed credential or a supplied client takes effect at once.
func TestService_RegisterTernClientClearsCachedFailure(t *testing.T) {
	resolver := newTernResolver()
	svc := newTernClientTestService(t, resolver, "orders")
	fake := clock.NewFake(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	require.NoError(t, svc.SetClock(fake))

	resolver.fail(errors.New("secret store timed out"))
	_, err := svc.TernClient("orders", "staging")
	require.Error(t, err)
	_, err = svc.TernClient("orders", "staging")
	require.ErrorContains(t, err, "cached failure")

	registered := &closeCountingTernClient{}
	svc.RegisterTernClient("orders", "staging", registered)
	client, err := svc.TernClient("orders", "staging")
	require.NoError(t, err)
	assert.Same(t, tern.Client(registered), client)
	assert.Equal(t, int32(1), resolver.calls.Load())

	svc.ternMu.Lock()
	_, remembered := svc.ternFailures["orders/staging"]
	svc.ternMu.Unlock()
	assert.False(t, remembered, "registering clears the remembered failure")
}

// A resolution cancelled with its context says nothing about the deployment, so
// the next call resolves again instead of receiving a cached failure.
func TestService_TernClientCancelledResolutionIsNotCached(t *testing.T) {
	resolver := newTernResolver()
	svc := newTernClientTestService(t, resolver, "orders")
	fake := clock.NewFake(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	require.NoError(t, svc.SetClock(fake))

	resolver.fail(fmt.Errorf("resolve token for orders/staging: %w", context.Canceled))
	_, err := svc.TernClient("orders", "staging")
	require.ErrorIs(t, err, context.Canceled)

	resolver.succeed()
	client, err := svc.TernClient("orders", "staging")
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, int32(2), resolver.calls.Load(), "the cancelled attempt left nothing behind to serve")
}

// When a client is registered while a resolution is still running, the
// registered client wins and the one the resolution built is closed rather than
// leaked.
func TestService_TernClientClosesClientThatLosesInstallRace(t *testing.T) {
	resolver := newTernResolver("orders")
	svc := newTernClientTestService(t, resolver, "orders")

	pending := ternClientAsync(svc, "orders")
	assert.Equal(t, "orders", receiveStartedResolution(t, resolver))

	registered := &closeCountingTernClient{}
	svc.RegisterTernClient("orders", "staging", registered)
	close(resolver.gates["orders"])

	result := receiveTernClientResult(t, pending)
	require.NoError(t, result.err)
	assert.Same(t, tern.Client(registered), result.client)

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	require.Len(t, resolver.built, 1)
	assert.Equal(t, int32(1), resolver.built[0].closes.Load(), "the client that lost the race is closed once")
	assert.Equal(t, int32(0), registered.closes.Load())
}

// When a client is registered while a resolution is still running and that
// resolution then fails, every caller waiting on it gets the registered client,
// not the failure, and nothing is remembered: registration wins over a failed
// build the same way it wins over a successful one.
func TestService_TernClientFailedResolutionServesClientRegisteredMeanwhile(t *testing.T) {
	resolver := newTernResolver("orders")
	svc := newTernClientTestService(t, resolver, "orders")
	resolver.fail(errors.New("secret store timed out"))

	pending := ternClientAsync(svc, "orders")
	assert.Equal(t, "orders", receiveStartedResolution(t, resolver))

	registered := &closeCountingTernClient{}
	svc.RegisterTernClient("orders", "staging", registered)
	close(resolver.gates["orders"])

	result := receiveTernClientResult(t, pending)
	require.NoError(t, result.err)
	assert.Same(t, tern.Client(registered), result.client)

	svc.ternMu.Lock()
	_, remembered := svc.ternFailures["orders/staging"]
	svc.ternMu.Unlock()
	assert.False(t, remembered, "a failure answered by the registered client is not remembered")
}

// An engine registered while a resolution is still running clears the
// remembered failures, and the older resolution's failure, arriving after the
// registration, is not remembered in their place: the next call tries again
// with the new factory instead of getting a cached failure for the rest of the
// window.
func TestService_TernClientFailureFromBeforeRegisterEngineIsNotCached(t *testing.T) {
	resolver := newTernResolver("orders")
	svc := newTernClientTestService(t, resolver, "orders")
	fake := clock.NewFake(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	require.NoError(t, svc.SetClock(fake))
	missingEngine := errors.New("no engine registered for this database type")
	resolver.fail(missingEngine)

	pending := ternClientAsync(svc, "orders")
	assert.Equal(t, "orders", receiveStartedResolution(t, resolver))

	factory := func(tern.LocalConfig, *slog.Logger) (engine.Engine, error) {
		return &fakeRegisteredEngine{}, nil
	}
	require.NoError(t, svc.RegisterEngine(storage.DatabaseTypeMySQL, factory))
	close(resolver.gates["orders"])

	result := receiveTernClientResult(t, pending)
	require.ErrorIs(t, result.err, missingEngine, "the attempt that failed still reports its own failure")

	resolver.succeed()
	client, err := svc.TernClient("orders", "staging")
	require.NoError(t, err, "the failure from before the registration must not be served from the cache")
	assert.NotNil(t, client)
	assert.Equal(t, int32(2), resolver.calls.Load(), "the next call resolves again")
}

// A resolution that finishes after the service is closed closes its client and
// reports the closed service instead of installing a client nothing will close.
func TestService_TernClientResolutionAfterCloseClosesClient(t *testing.T) {
	resolver := newTernResolver("orders")
	svc := newTernClientTestService(t, resolver, "orders")

	pending := ternClientAsync(svc, "orders")
	assert.Equal(t, "orders", receiveStartedResolution(t, resolver))

	require.NoError(t, svc.Close())
	close(resolver.gates["orders"])

	result := receiveTernClientResult(t, pending)
	require.ErrorContains(t, result.err, "service is closed")

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	require.Len(t, resolver.built, 1)
	assert.Equal(t, int32(1), resolver.built[0].closes.Load())
}

// An embedder can register an engine while a local client is being built,
// since construction runs without ternMu. The client is built from a copy of
// the factory registry, so the two never touch the same map; run under -race,
// this test fails if construction reads the registry RegisterEngine writes.
func TestService_RegisterEngineDuringLocalClientConstruction(t *testing.T) {
	// A discarding handler takes no lock, so logging adds no ordering between
	// the two goroutines that could hide a race from the detector.
	svc := New(nil, &ServerConfig{}, nil, slog.New(slog.DiscardHandler))
	factory := func(tern.LocalConfig, *slog.Logger) (engine.Engine, error) {
		return &fakeRegisteredEngine{}, nil
	}
	require.NoError(t, svc.RegisterEngine("strata", factory))

	envConfig := EnvironmentConfig{DSN: "root@tcp(localhost:3306)/customdb"}
	const rounds = 50
	var wg sync.WaitGroup
	errs := make(chan error, rounds)
	wg.Go(func() {
		for range rounds {
			_, err := svc.newLocalTernClient("customdb/staging", "customdb", "strata", envConfig)
			errs <- err
		}
	})
	wg.Go(func() {
		for range rounds {
			assert.NoError(t, svc.RegisterEngine("strata", factory))
		}
	})
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
