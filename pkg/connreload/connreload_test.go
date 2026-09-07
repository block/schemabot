package connreload

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fakes below stand in for a driver. Nothing in this package knows what a
// DSN means, so a test driver can be as simple as "the DSN is the credential":
// every dial records the DSN it was resolved from, and errRefused is the only
// error the connector is told to treat as a credentials refusal.

var errRefused = errors.New("credentials refused")

func refused(err error) bool { return errors.Is(err, errRefused) }

// stubConn is the minimal driver.Conn a fake dial can hand back.
type stubConn struct{}

func (stubConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (stubConn) Close() error                        { return nil }
func (stubConn) Begin() (driver.Tx, error)           { return nil, errors.New("not implemented") }

type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) { return nil, errors.New("not implemented") }

// fakeConnector dials for one resolved DSN, recording each attempt into the
// shared slice and taking its outcome from results, consumed in order across
// every generation.
type fakeConnector struct {
	t       *testing.T
	dsn     string
	dials   *[]string
	results []error
	mu      *sync.Mutex
}

func (f *fakeConnector) Connect(context.Context) (driver.Conn, error) {
	f.mu.Lock()
	*f.dials = append(*f.dials, f.dsn)
	i := len(*f.dials) - 1
	f.mu.Unlock()
	// assert (not require): this runs on the dialing goroutine, and testify's
	// FailNow is only valid on the test goroutine. The error return fails the
	// dial cleanly instead.
	if !assert.Less(f.t, i, len(f.results), "unexpected extra dial attempt") {
		return nil, errors.New("unexpected extra dial attempt")
	}
	if err := f.results[i]; err != nil {
		return nil, err
	}
	return stubConn{}, nil
}

func (f *fakeConnector) Driver() driver.Driver { return stubDriver{} }

// dialRecorder builds a Resolve that hands out fakeConnectors sharing one
// attempt log, and returns the log.
func dialRecorder(t *testing.T, results []error) (func(string) (driver.Connector, error), *[]string) {
	t.Helper()
	var dials []string
	var mu sync.Mutex
	resolve := func(dsn string) (driver.Connector, error) {
		return &fakeConnector{t: t, dsn: dsn, dials: &dials, results: results, mu: &mu}, nil
	}
	return resolve, &dials
}

// newTestConnector builds a Connector over the fake driver. results are the
// dial outcomes in order; reload supplies the rotated DSN.
func newTestConnector(t *testing.T, dsn string, results []error, reload func() (string, error)) (*Connector, *[]string) {
	t.Helper()
	resolve, dials := dialRecorder(t, results)
	c, err := New(dsn, Config{
		Resolve: resolve,
		Refused: refused,
		Reload:  reload,
		Driver:  stubDriver{},
		Name:    "test",
	})
	require.NoError(t, err)
	return c, dials
}

func TestNewRequiresCallbacks(t *testing.T) {
	resolve := func(string) (driver.Connector, error) { return nil, nil }
	reload := func() (string, error) { return "", nil }

	_, err := New("dsn", Config{Refused: refused, Reload: reload})
	require.Error(t, err, "a nil Resolve must not produce a connector that panics on first dial")
	_, err = New("dsn", Config{Resolve: resolve, Reload: reload})
	require.Error(t, err)
	_, err = New("dsn", Config{Resolve: resolve, Refused: refused})
	require.Error(t, err)
}

func TestNewRejectsUnresolvableDSN(t *testing.T) {
	resolveErr := errors.New("malformed DSN")
	_, err := New("bad", Config{
		Resolve: func(string) (driver.Connector, error) { return nil, resolveErr },
		Refused: refused,
		Reload:  func() (string, error) { return "", nil },
	})
	require.ErrorIs(t, err, resolveErr, "a bad boot DSN must fail the open, not the first dial")
}

func TestReloadsOnRefusal(t *testing.T) {
	var reloads atomic.Int32
	c, dials := newTestConnector(t, "old", []error{errRefused, nil, nil}, func() (string, error) {
		reloads.Add(1)
		return "rotated", nil
	})

	conn, err := c.Connect(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Equal(t, []string{"old", "rotated"}, *dials, "retry must dial with the reloaded credentials")
	assert.Equal(t, int32(1), reloads.Load())

	// The reloaded credentials stick for subsequent dials without another reload.
	conn, err = c.Connect(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Equal(t, []string{"old", "rotated", "rotated"}, *dials)
	assert.Equal(t, int32(1), reloads.Load())
}

func TestKeepsCredentialsWhenReloadFails(t *testing.T) {
	c, dials := newTestConnector(t, "old", []error{errRefused}, func() (string, error) {
		return "", errors.New("secret backend unavailable")
	})

	conn, err := c.Connect(t.Context())
	require.ErrorIs(t, err, errRefused, "the original refusal surfaces, not the reload error")
	assert.Nil(t, conn)
	assert.Equal(t, []string{"old"}, *dials, "no retry without fresh credentials")

	_, gen := c.snapshot()
	assert.Equal(t, uint64(0), gen, "current credentials are kept")
}

func TestIgnoresNonRefusalErrors(t *testing.T) {
	dialErr := errors.New("connection refused")
	reloadCalled := false
	c, dials := newTestConnector(t, "old", []error{dialErr}, func() (string, error) {
		reloadCalled = true
		return "rotated", nil
	})

	_, err := c.Connect(t.Context())
	require.ErrorIs(t, err, dialErr)
	assert.False(t, reloadCalled, "only a credentials refusal may trigger a reload")
	assert.Equal(t, []string{"old"}, *dials)
}

func TestSurfacesRetryFailure(t *testing.T) {
	var reloads atomic.Int32
	c, dials := newTestConnector(t, "old", []error{errRefused, errRefused}, func() (string, error) {
		reloads.Add(1)
		return "rotated", nil
	})

	// The reload succeeds but the retry dial is also refused — for example a
	// reloaded secret that is itself stale. The retry's error surfaces and the
	// reload runs exactly once for the failed attempt.
	conn, err := c.Connect(t.Context())
	require.ErrorIs(t, err, errRefused)
	assert.Nil(t, conn)
	assert.Equal(t, []string{"old", "rotated"}, *dials, "the retry dials with the reloaded credentials")
	assert.Equal(t, int32(1), reloads.Load())
}

// A reloaded DSN that Resolve rejects keeps the pool on the working
// generation, rather than swapping in credentials nothing can dial with.
func TestRejectsUnresolvableReloadedDSN(t *testing.T) {
	var dials []string
	var mu sync.Mutex
	resolveErr := errors.New("malformed DSN")
	c, err := New("old", Config{
		Resolve: func(dsn string) (driver.Connector, error) {
			if dsn != "old" {
				return nil, resolveErr
			}
			return &fakeConnector{t: t, dsn: dsn, dials: &dials, results: []error{nil}, mu: &mu}, nil
		},
		Refused: refused,
		Reload:  func() (string, error) { return "rotated", nil },
	})
	require.NoError(t, err)

	fresh, _, ok := c.refresh(t.Context(), 0)
	require.False(t, ok)
	assert.Nil(t, fresh)

	_, gen := c.snapshot()
	assert.Equal(t, uint64(0), gen, "the pool stays on the working generation")
}

func TestRefreshConcurrent(t *testing.T) {
	var reloads atomic.Int32
	c, _ := newTestConnector(t, "old", nil, func() (string, error) {
		reloads.Add(1)
		return "rotated", nil
	})

	// Concurrent dials that failed on the same generation trigger exactly one
	// reload; the rest reuse the swapped credentials.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			_, gen, ok := c.refresh(t.Context(), 0)
			assert.True(t, ok)
			assert.Equal(t, uint64(1), gen)
		})
	}
	wg.Wait()
	assert.Equal(t, int32(1), reloads.Load(), "concurrent same-generation failures must reload once")
}

func TestRefreshDedupesStaleGeneration(t *testing.T) {
	var reloads atomic.Int32
	c, _ := newTestConnector(t, "old", nil, func() (string, error) {
		reloads.Add(1)
		return "rotated", nil
	})

	_, gen, ok := c.refresh(t.Context(), 0)
	require.True(t, ok)
	require.Equal(t, uint64(1), gen)
	require.Equal(t, int32(1), reloads.Load())

	// A dial that failed against the already-superseded generation reuses the
	// swapped credentials instead of reloading again.
	_, gen, ok = c.refresh(t.Context(), 0)
	require.True(t, ok)
	assert.Equal(t, uint64(1), gen)
	assert.Equal(t, int32(1), reloads.Load(), "stale-generation refresh must not reload")
}

// A reload that succeeds but returns credentials the server still refuses arms
// the cooldown too: without it, each refused dial advances the generation and
// triggers a fresh resolve — one per new connection — for as long as the secret
// store keeps answering with a refused credential.
func TestArmsCooldownWhenReloadedCredentialsRefused(t *testing.T) {
	var reloads atomic.Int32
	c, dials := newTestConnector(t, "old",
		[]error{errRefused, errRefused, errRefused, errRefused, errRefused},
		func() (string, error) {
			reloads.Add(1)
			return "stale", nil
		})
	clock := time.Now()
	c.cfg.now = func() time.Time { return clock }

	// The dial fails, the reload succeeds, and the retry is refused too: the
	// cooldown arms.
	_, err := c.Connect(t.Context())
	require.Error(t, err)
	require.Equal(t, int32(1), reloads.Load())
	assert.Equal(t, []string{"old", "stale"}, *dials)

	// The next refused dial surfaces without another resolve.
	_, err = c.Connect(t.Context())
	require.Error(t, err)
	assert.Equal(t, int32(1), reloads.Load(), "a refused reloaded credential must not cost one resolve per connection")
	assert.Equal(t, []string{"old", "stale", "stale"}, *dials)

	// After the window elapses the reload is retried; another refused retry
	// re-arms the cooldown.
	clock = clock.Add(DefaultCooldown)
	_, err = c.Connect(t.Context())
	require.Error(t, err)
	assert.Equal(t, int32(2), reloads.Load())
	assert.Equal(t, []string{"old", "stale", "stale", "stale", "stale"}, *dials)
}

func TestReloadCooldown(t *testing.T) {
	var reloads atomic.Int32
	c, _ := newTestConnector(t, "old", nil, func() (string, error) {
		reloads.Add(1)
		if reloads.Load() < 3 {
			return "", errors.New("secret backend unavailable")
		}
		return "rotated", nil
	})
	clock := time.Now()
	c.cfg.now = func() time.Time { return clock }

	// The first failed reload arms the cooldown.
	_, _, ok := c.refresh(t.Context(), 0)
	require.False(t, ok)
	require.Equal(t, int32(1), reloads.Load())

	// Failed dials inside the window surface without reloading again.
	_, _, ok = c.refresh(t.Context(), 0)
	require.False(t, ok)
	assert.Equal(t, int32(1), reloads.Load(), "reload must not run during the cooldown")

	// After the window elapses the reload is retried; another failure re-arms.
	clock = clock.Add(DefaultCooldown)
	_, _, ok = c.refresh(t.Context(), 0)
	require.False(t, ok)
	require.Equal(t, int32(2), reloads.Load())

	// A successful reload swaps credentials and clears the cooldown.
	clock = clock.Add(DefaultCooldown)
	_, _, ok = c.refresh(t.Context(), 0)
	require.True(t, ok)
	require.Equal(t, int32(3), reloads.Load())
	assert.True(t, c.lastReloadFail.IsZero(), "a successful reload must clear the cooldown")
}

func TestCooldownOverride(t *testing.T) {
	var reloads atomic.Int32
	resolve, _ := dialRecorder(t, nil)
	c, err := New("old", Config{
		Resolve:  resolve,
		Refused:  refused,
		Reload:   func() (string, error) { reloads.Add(1); return "", errors.New("unavailable") },
		Cooldown: time.Hour,
	})
	require.NoError(t, err)
	clock := time.Now()
	c.cfg.now = func() time.Time { return clock }

	_, _, ok := c.refresh(t.Context(), 0)
	require.False(t, ok)
	require.Equal(t, int32(1), reloads.Load())

	// The default window would have elapsed; the override's has not.
	clock = clock.Add(2 * DefaultCooldown)
	_, _, ok = c.refresh(t.Context(), 0)
	require.False(t, ok)
	assert.Equal(t, int32(1), reloads.Load(), "Cooldown must override DefaultCooldown")
}

// waitClosed fails the test when ch does not close within a bounded deadline.
func waitClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

// hungReload returns a Reload that signals started, then blocks until release
// closes before returning result and err.
func hungReload(started, release chan struct{}, reloads *atomic.Int32, result string, err error) func() (string, error) {
	return func() (string, error) {
		reloads.Add(1)
		close(started)
		<-release
		return result, err
	}
}

func TestSnapshotNotBlockedByHungReload(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var reloads atomic.Int32
	c, _ := newTestConnector(t, "old", nil, hungReload(started, release, &reloads, "rotated", nil))

	var wg sync.WaitGroup
	wg.Go(func() {
		_, gen, ok := c.refresh(t.Context(), 0)
		assert.True(t, ok)
		assert.Equal(t, uint64(1), gen)
	})
	waitClosed(t, started, "reload never started")

	// The hung reload must not hold the connector mutex: healthy dials keep
	// snapshotting the current credentials while a new one resolves.
	snapshotDone := make(chan struct{})
	go func() {
		defer close(snapshotDone)
		_, gen := c.snapshot()
		assert.Equal(t, uint64(0), gen)
	}()
	waitClosed(t, snapshotDone, "snapshot blocked behind an in-flight reload")

	close(release)
	wg.Wait()
	assert.Equal(t, int32(1), reloads.Load())
}

func TestConnectNotBlockedByHungReload(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var reloads atomic.Int32
	c, dials := newTestConnector(t, "old", []error{errRefused, nil, nil},
		hungReload(started, release, &reloads, "rotated", nil))

	var wg sync.WaitGroup
	wg.Go(func() {
		conn, err := c.Connect(t.Context())
		assert.NoError(t, err)
		assert.NotNil(t, conn)
	})
	waitClosed(t, started, "reload never started")

	// A dial that authenticates with the current credentials completes while
	// the refused dial's reload hangs on secret resolution.
	connected := make(chan struct{})
	go func() {
		defer close(connected)
		conn, err := c.Connect(t.Context())
		assert.NoError(t, err)
		assert.NotNil(t, conn)
	}()
	waitClosed(t, connected, "healthy dial blocked behind an in-flight reload")

	close(release)
	wg.Wait()
	assert.Equal(t, []string{"old", "old", "rotated"}, *dials)
	assert.Equal(t, int32(1), reloads.Load())
}

func TestRefreshWaiterRespectsDialContext(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var reloads atomic.Int32
	c, _ := newTestConnector(t, "old", nil, hungReload(started, release, &reloads, "rotated", nil))

	var wg sync.WaitGroup
	wg.Go(func() {
		_, _, ok := c.refresh(t.Context(), 0)
		assert.True(t, ok)
	})
	waitClosed(t, started, "reload never started")

	// A waiter whose dial context ends while the leader's reload hangs gives up
	// and surfaces its dial error instead of blocking indefinitely.
	ctx, cancel := context.WithCancel(t.Context())
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		fresh, _, ok := c.refresh(ctx, 0)
		assert.False(t, ok)
		assert.Nil(t, fresh)
	}()
	cancel()
	waitClosed(t, waiterDone, "waiter did not honor its dial context")

	close(release)
	wg.Wait()
	assert.Equal(t, int32(1), reloads.Load(), "the canceled waiter must not trigger its own reload")
}

// The dial that elects a reload is not pinned by it: the reload runs detached,
// so cancelling the electing dial's context returns it promptly with its dial
// error — it cannot hold a pool connection slot for as long as a hung secret
// resolution takes — while the reload finishes in the background and publishes
// the rotated credentials for later dials.
func TestRefreshElectingDialRespectsDialContext(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var reloads atomic.Int32
	c, _ := newTestConnector(t, "old", nil, hungReload(started, release, &reloads, "rotated", nil))

	ctx, cancel := context.WithCancel(t.Context())
	electorDone := make(chan struct{})
	go func() {
		defer close(electorDone)
		fresh, _, ok := c.refresh(ctx, 0)
		assert.False(t, ok)
		assert.Nil(t, fresh)
	}()
	waitClosed(t, started, "reload never started")
	cancel()
	waitClosed(t, electorDone, "the electing dial did not honor its own dial context")

	// The detached reload finishes and publishes: a later same-generation
	// refresh picks up the rotated credentials without reloading again.
	close(release)
	_, gen, ok := c.refresh(t.Context(), 0)
	require.True(t, ok)
	assert.Equal(t, uint64(1), gen)
	assert.Equal(t, int32(1), reloads.Load(), "the abandoned reload's outcome must be reused, not re-resolved")
}

func TestRefreshWaiterObservesLeaderFailure(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var reloads atomic.Int32
	c, _ := newTestConnector(t, "old", nil,
		hungReload(started, release, &reloads, "", errors.New("secret backend unavailable")))

	// The clock seam doubles as an entered-refresh gate: an expired cooldown
	// stamp makes every refresh iteration consult the clock under the mutex, so
	// the second consult is the waiter's. Once past it, the waiter can only
	// reach the select on the leader's done channel — releasing the leader
	// after that pins the wake-then-recheck path instead of leaving it to the
	// scheduler.
	c.lastReloadFail = time.Now().Add(-2 * DefaultCooldown)
	var clockCalls atomic.Int32
	waiterEntered := make(chan struct{})
	c.cfg.now = func() time.Time {
		if clockCalls.Add(1) == 2 {
			close(waiterEntered)
		}
		return time.Now()
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		_, _, ok := c.refresh(t.Context(), 0)
		assert.False(t, ok)
	})
	waitClosed(t, started, "reload never started")

	// A same-generation waiter observes the leader's failed reload through the
	// armed cooldown and reports failure without reloading again.
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		fresh, _, ok := c.refresh(t.Context(), 0)
		assert.False(t, ok)
		assert.Nil(t, fresh)
	}()
	waitClosed(t, waiterEntered, "waiter never entered refresh")
	close(release)
	wg.Wait()
	waitClosed(t, waiterDone, "waiter did not observe the leader's failed reload")
	assert.Equal(t, int32(1), reloads.Load(), "the waiter must not reload during the cooldown the failure armed")
}

// A Reload that panics must not wedge the connector or crash the process: the
// recover in the publish defer converts the panic into a failed reload — the
// reloading guard is released, waiters are unblocked, the cooldown is armed,
// and the current credentials are kept.
func TestReloadPanicUnblocksWaiters(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var reloads atomic.Int32
	c, _ := newTestConnector(t, "old", nil, func() (string, error) {
		reloads.Add(1)
		close(started)
		<-release
		panic("secret backend panicked")
	})

	var wg sync.WaitGroup
	wg.Go(func() {
		fresh, _, ok := c.refresh(t.Context(), 0)
		assert.False(t, ok, "a panicking reload must surface as a failed reload")
		assert.Nil(t, fresh)
	})
	waitClosed(t, started, "reload never started")

	// A same-generation waiter blocked on the reload's outcome is unblocked by
	// the publish defer and observes the armed cooldown.
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		fresh, _, ok := c.refresh(t.Context(), 0)
		assert.False(t, ok)
		assert.Nil(t, fresh)
	}()
	close(release)
	wg.Wait()
	waitClosed(t, waiterDone, "waiter was not unblocked by the panicking reload")

	// The guard is released and the cooldown armed: a later same-generation
	// refresh backs off without reloading rather than deadlocking on a stale
	// guard.
	_, _, ok := c.refresh(t.Context(), 0)
	assert.False(t, ok)
	assert.Equal(t, int32(1), reloads.Load(), "the panicked reload must arm the cooldown; no further reloads")
	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Nil(t, c.reloading, "the reloading guard must be released after a panic")
}

// A refusal verdict on superseded credentials must not arm the cooldown: while
// a retry dial is in flight, another rotation can swap in newer credentials,
// and stamping the late refusal onto that newer generation would suppress its
// legitimate reloads for a full window.
func TestStaleRefusalDoesNotArmCooldown(t *testing.T) {
	var c *Connector
	var dials atomic.Int32
	resolve := func(string) (driver.Connector, error) {
		return connectFunc(func(context.Context) (driver.Conn, error) {
			switch dials.Add(1) {
			case 1:
				// The initial dial is refused, triggering the reload.
				return nil, errRefused
			case 2:
				// While the retry with reloaded credentials is in flight, a
				// concurrent rotation advances the generation; the retry then
				// comes back refused — a verdict on already-superseded
				// credentials.
				c.mu.Lock()
				c.gen++
				c.mu.Unlock()
				return nil, errRefused
			default:
				return stubConn{}, nil
			}
		}), nil
	}
	var err error
	c, err = New("old", Config{
		Resolve: resolve,
		Refused: refused,
		Reload:  func() (string, error) { return "rotated", nil },
	})
	require.NoError(t, err)

	_, err = c.Connect(t.Context())
	require.Error(t, err)
	c.mu.Lock()
	defer c.mu.Unlock()
	assert.True(t, c.lastReloadFail.IsZero(), "a stale refusal must not arm the cooldown against the newer generation")
}

// connectFunc adapts a dial function to driver.Connector.
type connectFunc func(context.Context) (driver.Conn, error)

func (f connectFunc) Connect(ctx context.Context) (driver.Conn, error) { return f(ctx) }
func (connectFunc) Driver() driver.Driver                              { return stubDriver{} }

func TestDriverIsReported(t *testing.T) {
	c, _ := newTestConnector(t, "old", nil, func() (string, error) { return "", nil })
	assert.Equal(t, stubDriver{}, c.Driver())
}

// Two pools must not share reload state. The driver this package replaced
// registered its reload callback process-globally, so opening a second
// reloadable pool silently repointed the first one's credential reload at the
// second one's secret.
func TestPoolsDoNotShareReloadState(t *testing.T) {
	var first, second atomic.Int32
	firstConn, _ := newTestConnector(t, "first-old", []error{errRefused, nil}, func() (string, error) {
		first.Add(1)
		return "first-new", nil
	})
	secondConn, _ := newTestConnector(t, "second-old", []error{errRefused, nil}, func() (string, error) {
		second.Add(1)
		return "second-new", nil
	})

	_, err := firstConn.Connect(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(1), first.Load())
	assert.Equal(t, int32(0), second.Load(), "one pool's refusal must not reload another pool's secret")

	_, err = secondConn.Connect(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(1), first.Load())
	assert.Equal(t, int32(1), second.Load())
}

func TestUnnamedPoolLogsWithoutPanicking(t *testing.T) {
	// Name is optional; the logger path must handle both.
	resolve, _ := dialRecorder(t, nil)
	c, err := New("old", Config{
		Resolve: resolve,
		Refused: refused,
		Reload:  func() (string, error) { return "", errors.New("unavailable") },
	})
	require.NoError(t, err)
	require.NotNil(t, c.log())

	_, _, ok := c.refresh(t.Context(), 0)
	assert.False(t, ok)
}

func TestConnectSurfacesResolveErrorShape(t *testing.T) {
	// Sanity: a wrapped refusal is still a refusal, because Refused unwraps.
	c, dials := newTestConnector(t, "old",
		[]error{fmt.Errorf("dial tcp: %w", errRefused), nil},
		func() (string, error) { return "rotated", nil })

	conn, err := c.Connect(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Equal(t, []string{"old", "rotated"}, *dials)
}
