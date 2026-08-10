package postgresconn

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionDSN(t *testing.T) {
	tests := []struct {
		name       string
		dsn        string
		want       string
		wantErrSub string
	}{
		{
			name: "RDS URL host gets sslmode=require",
			dsn:  "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app",
			want: "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app?sslmode=require",
		},
		{
			name: "RDS URL host with explicit sslmode is unchanged",
			dsn:  "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app?sslmode=verify-full",
			want: "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app?sslmode=verify-full",
		},
		{
			name: "RDS URL host with explicit sslmode=disable is unchanged",
			dsn:  "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app?sslmode=disable",
			want: "postgres://schemabot:secret@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app?sslmode=disable",
		},
		{
			name: "RDS keyword host gets sslmode=require",
			dsn:  "host=database.cluster-abc123.us-west-2.rds.amazonaws.com port=5432 user=schemabot password=secret dbname=app",
			want: "host=database.cluster-abc123.us-west-2.rds.amazonaws.com port=5432 user=schemabot password=secret dbname=app sslmode=require",
		},
		{
			name: "RDS keyword host with explicit sslmode is unchanged",
			dsn:  "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode=disable",
			want: "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode=disable",
		},
		{
			name: "RDS keyword host with sslmode = disable (spaces around =) is unchanged",
			dsn:  "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode = disable",
			want: "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode = disable",
		},
		{
			name: "RDS keyword host with sslmode = verify-full (spaces around =) is unchanged",
			dsn:  "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode = verify-full",
			want: "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode = verify-full",
		},
		{
			name: "RDS keyword host with sslmode =disable (space before =) is unchanged",
			dsn:  "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode =disable",
			want: "host=database.cluster-abc123.us-west-2.rds.amazonaws.com user=schemabot password=secret dbname=app sslmode =disable",
		},
		{
			name: "non-RDS URL host is unchanged",
			dsn:  "postgres://schemabot:secret@localhost:5432/app",
			want: "postgres://schemabot:secret@localhost:5432/app",
		},
		{
			// RDS detection considers only the first host, so a multi-host
			// DSN whose RDS host is a fallback gets no injection — spell out
			// sslmode explicitly for multi-host DSNs.
			name: "multi-host DSN with RDS fallback host gets no injection",
			dsn:  "postgres://schemabot:secret@localhost:5432,database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app",
			want: "postgres://schemabot:secret@localhost:5432,database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app",
		},
		{
			name: "non-RDS keyword host is unchanged",
			dsn:  "host=localhost user=schemabot password=secret dbname=app",
			want: "host=localhost user=schemabot password=secret dbname=app",
		},
		{
			name:       "invalid DSN returns context",
			dsn:        "postgres://schemabot:secret@localhost:not-a-port/app",
			wantErrSub: "parse PostgreSQL DSN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConnectionDSN(tt.dsn)
			if tt.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrSub)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			// The normalized DSN must itself parse.
			_, err = pgx.ParseConfig(got)
			require.NoError(t, err)
		})
	}
}

func TestWithConnectTimeout(t *testing.T) {
	cfg, err := connectionConfig("postgres://schemabot:secret@localhost:5432/app", WithConnectTimeout(7*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 7*time.Second, cfg.ConnectTimeout)

	cfg, err = connectionConfig("postgres://schemabot:secret@localhost:5432/app", WithConnectTimeout(0))
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), cfg.ConnectTimeout)
}

// stubConn is the minimal driver.Conn a fake dial can hand back.
type stubConn struct{}

func (stubConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (stubConn) Close() error                        { return nil }
func (stubConn) Begin() (driver.Tx, error)           { return nil, errors.New("not implemented") }

// fakeDial replaces the connectConfig seam for the test and records the
// password of every dial attempt. Each attempt's outcome comes from results,
// consumed in order.
func fakeDial(t *testing.T, results []error) *[]string {
	t.Helper()
	var passwords []string
	original := connectConfig
	t.Cleanup(func() { connectConfig = original })
	connectConfig = func(_ context.Context, cfg pgx.ConnConfig) (driver.Conn, error) {
		passwords = append(passwords, cfg.Password)
		// assert (not require): this seam runs on the dialing goroutine, and
		// testify's FailNow is only valid on the test goroutine. The error
		// return fails the dial cleanly instead.
		if !assert.Less(t, len(passwords)-1, len(results), "unexpected extra dial attempt") {
			return nil, errors.New("unexpected extra dial attempt")
		}
		if err := results[len(passwords)-1]; err != nil {
			return nil, err
		}
		return stubConn{}, nil
	}
	return &passwords
}

func authError() error {
	return fmt.Errorf("connect: %w", &pgconn.PgError{Code: "28P01", Message: "password authentication failed"})
}

func newReloadableConnector(t *testing.T, dsn string, reload func() (string, error)) *reloadableConnector {
	t.Helper()
	cfg, err := connectionConfig(dsn)
	require.NoError(t, err)
	return &reloadableConnector{cfg: cfg, reload: reload}
}

func TestReloadableConnectorReloadsOnAuthFailure(t *testing.T) {
	passwords := fakeDial(t, []error{authError(), nil, nil})
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		reloads.Add(1)
		return "postgres://schemabot:rotated@localhost:5432/app", nil
	})

	conn, err := c.Connect(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Equal(t, []string{"old", "rotated"}, *passwords, "retry must dial with the reloaded credentials")
	assert.Equal(t, int32(1), reloads.Load())

	// The reloaded credentials stick for subsequent dials without another reload.
	conn, err = c.Connect(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Equal(t, []string{"old", "rotated", "rotated"}, *passwords)
	assert.Equal(t, int32(1), reloads.Load())
}

func TestReloadableConnectorKeepsCredentialsWhenReloadFails(t *testing.T) {
	passwords := fakeDial(t, []error{authError()})
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		return "", errors.New("secret backend unavailable")
	})

	conn, err := c.Connect(t.Context())
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "password authentication failed", "the original auth error surfaces, not the reload error")
	assert.Equal(t, []string{"old"}, *passwords, "no retry without fresh credentials")

	cfg, gen := c.snapshot()
	assert.Equal(t, "old", cfg.Password, "current credentials are kept")
	assert.Equal(t, uint64(0), gen)
}

func TestReloadableConnectorIgnoresNonAuthErrors(t *testing.T) {
	dialErr := errors.New("connection refused")
	passwords := fakeDial(t, []error{dialErr})
	reloadCalled := false
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		reloadCalled = true
		return "postgres://schemabot:rotated@localhost:5432/app", nil
	})

	_, err := c.Connect(t.Context())
	require.ErrorIs(t, err, dialErr)
	assert.False(t, reloadCalled, "a non-authentication failure must not trigger a reload")
	assert.Equal(t, []string{"old"}, *passwords)
}

func TestReloadableConnectorSurfacesRetryFailure(t *testing.T) {
	passwords := fakeDial(t, []error{authError(), authError()})
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		reloads.Add(1)
		return "postgres://schemabot:rotated@localhost:5432/app", nil
	})

	// The reload succeeds but the retry dial is also rejected — for example a
	// reloaded secret that is itself stale. The retry's error surfaces and the
	// reload runs exactly once for the failed attempt.
	conn, err := c.Connect(t.Context())
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "password authentication failed")
	assert.Equal(t, []string{"old", "rotated"}, *passwords, "the retry dials with the reloaded credentials")
	assert.Equal(t, int32(1), reloads.Load())
}

func TestReloadableConnectorReloadReappliesOptionsAndNormalization(t *testing.T) {
	// The reloaded raw DSN gets the same treatment as the boot DSN: caller
	// options are re-applied and an RDS host gets TLS injected.
	opts := []Option{WithConnectTimeout(7 * time.Second)}
	cfg, err := connectionConfig("postgres://schemabot:old@localhost:5432/app", opts...)
	require.NoError(t, err)
	c := &reloadableConnector{cfg: cfg, opts: opts, reload: func() (string, error) {
		return "postgres://schemabot:rotated@database.cluster-abc123.us-west-2.rds.amazonaws.com:5432/app", nil
	}}

	fresh, _, ok := c.refresh(t.Context(), 0)
	require.True(t, ok)
	assert.Equal(t, "rotated", fresh.Password)
	assert.Equal(t, 7*time.Second, fresh.ConnectTimeout, "options must flow through the reload path")
	assert.NotNil(t, fresh.TLSConfig, "a reloaded RDS DSN must get sslmode=require injected")
	// sslmode=prefer would also set TLSConfig but keep a plaintext fallback;
	// require is distinguished by that fallback's absence.
	assert.Empty(t, fresh.Fallbacks, "sslmode=require must not leave a plaintext fallback")
}

func TestReloadableConnectorRefreshConcurrent(t *testing.T) {
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		reloads.Add(1)
		return "postgres://schemabot:rotated@localhost:5432/app", nil
	})

	// Concurrent dials that failed on the same generation trigger exactly one
	// reload; the rest reuse the swapped config.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			cfg, _, ok := c.refresh(t.Context(), 0)
			assert.True(t, ok)
			assert.Equal(t, "rotated", cfg.Password)
		})
	}
	wg.Wait()
	assert.Equal(t, int32(1), reloads.Load(), "concurrent same-generation failures must reload once")
}

// A reload that succeeds but returns credentials the server still rejects
// arms the cooldown too: without it, each rejected dial advances the
// generation and triggers a fresh resolve — one per new connection — for as
// long as the secret store keeps answering with a refused credential.
func TestReloadableConnectorArmsCooldownWhenReloadedCredentialsRejected(t *testing.T) {
	passwords := fakeDial(t, []error{authError(), authError(), authError(), authError(), authError()})
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		reloads.Add(1)
		return "postgres://schemabot:stale@localhost:5432/app", nil
	})
	clock := time.Now()
	c.now = func() time.Time { return clock }

	// The dial fails, the reload succeeds, and the retry is rejected too:
	// the cooldown arms.
	_, err := c.Connect(t.Context())
	require.Error(t, err)
	require.Equal(t, int32(1), reloads.Load())
	assert.Equal(t, []string{"old", "stale"}, *passwords)

	// The next rejected dial surfaces without another resolve.
	_, err = c.Connect(t.Context())
	require.Error(t, err)
	assert.Equal(t, int32(1), reloads.Load(), "a rejected reloaded credential must not cost one resolve per connection")
	assert.Equal(t, []string{"old", "stale", "stale"}, *passwords)

	// After the window elapses the reload is retried; another rejected retry
	// re-arms the cooldown.
	clock = clock.Add(reloadCooldown)
	_, err = c.Connect(t.Context())
	require.Error(t, err)
	assert.Equal(t, int32(2), reloads.Load())
	assert.Equal(t, []string{"old", "stale", "stale", "stale", "stale"}, *passwords)
}

func TestReloadableConnectorReloadCooldown(t *testing.T) {
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		reloads.Add(1)
		if reloads.Load() < 3 {
			return "", errors.New("secret backend unavailable")
		}
		return "postgres://schemabot:rotated@localhost:5432/app", nil
	})
	clock := time.Now()
	c.now = func() time.Time { return clock }

	// The first failed reload arms the cooldown.
	_, _, ok := c.refresh(t.Context(), 0)
	require.False(t, ok)
	require.Equal(t, int32(1), reloads.Load())

	// Failed dials inside the window surface without reloading again.
	_, _, ok = c.refresh(t.Context(), 0)
	require.False(t, ok)
	assert.Equal(t, int32(1), reloads.Load(), "reload must not run during the cooldown")

	// After the window elapses the reload is retried; another failure re-arms.
	clock = clock.Add(reloadCooldown)
	_, _, ok = c.refresh(t.Context(), 0)
	require.False(t, ok)
	require.Equal(t, int32(2), reloads.Load())

	// A successful reload swaps credentials and clears the cooldown.
	clock = clock.Add(reloadCooldown)
	cfg, _, ok := c.refresh(t.Context(), 0)
	require.True(t, ok)
	assert.Equal(t, "rotated", cfg.Password)
	require.Equal(t, int32(3), reloads.Load())
	assert.True(t, c.lastReloadFail.IsZero(), "a successful reload must clear the cooldown")
}

func TestDSNParseErrorsRedactCredentials(t *testing.T) {
	// A DSN whose URL fails to parse must not have its password echoed in the
	// error: *url.Error reproduces the full URL, password included.
	const password = "sup3r-secret"
	badDSN := "postgres://schemabot:" + password + "@localhost:not-a-port/app"

	t.Run("ConnectionDSN", func(t *testing.T) {
		_, err := ConnectionDSN(badDSN)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), password)
	})

	t.Run("Open", func(t *testing.T) {
		_, err := Open(badDSN)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), password)
	})

	t.Run("refresh with unparseable reloaded DSN", func(t *testing.T) {
		c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
			return badDSN, nil
		})
		_, _, ok := c.refresh(t.Context(), 0)
		require.False(t, ok)
	})
}

func TestReloadableConnectorRefreshDedupesConcurrentFailures(t *testing.T) {
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		reloads.Add(1)
		return "postgres://schemabot:rotated@localhost:5432/app", nil
	})

	cfg, _, ok := c.refresh(t.Context(), 0)
	require.True(t, ok)
	assert.Equal(t, "rotated", cfg.Password)
	require.Equal(t, int32(1), reloads.Load())

	// A dial that failed against the already-superseded generation reuses the
	// swapped config instead of reloading again.
	cfg, _, ok = c.refresh(t.Context(), 0)
	require.True(t, ok)
	assert.Equal(t, "rotated", cfg.Password)
	assert.Equal(t, int32(1), reloads.Load(), "stale-generation refresh must not reload")
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

// hungReload returns a reload callback that signals started, then blocks
// until release closes before returning result and err.
func hungReload(started, release chan struct{}, reloads *atomic.Int32, result string, err error) func() (string, error) {
	return func() (string, error) {
		reloads.Add(1)
		close(started)
		<-release
		return result, err
	}
}

func TestReloadableConnectorSnapshotNotBlockedByHungReload(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app",
		hungReload(started, release, &reloads, "postgres://schemabot:rotated@localhost:5432/app", nil))

	var wg sync.WaitGroup
	wg.Go(func() {
		cfg, _, ok := c.refresh(t.Context(), 0)
		assert.True(t, ok)
		assert.Equal(t, "rotated", cfg.Password)
	})
	waitClosed(t, started, "reload never started")

	// The hung reload must not hold the connector mutex: healthy dials keep
	// snapshotting the current config while credentials resolve.
	snapshotDone := make(chan struct{})
	go func() {
		defer close(snapshotDone)
		cfg, gen := c.snapshot()
		assert.Equal(t, "old", cfg.Password)
		assert.Equal(t, uint64(0), gen)
	}()
	waitClosed(t, snapshotDone, "snapshot blocked behind an in-flight reload")

	close(release)
	wg.Wait()
	assert.Equal(t, int32(1), reloads.Load())
}

func TestReloadableConnectorConnectNotBlockedByHungReload(t *testing.T) {
	passwords := fakeDial(t, []error{authError(), nil, nil})
	started := make(chan struct{})
	release := make(chan struct{})
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app",
		hungReload(started, release, &reloads, "postgres://schemabot:rotated@localhost:5432/app", nil))

	var wg sync.WaitGroup
	wg.Go(func() {
		conn, err := c.Connect(t.Context())
		assert.NoError(t, err)
		assert.NotNil(t, conn)
	})
	waitClosed(t, started, "reload never started")

	// A dial that authenticates with the current credentials completes while
	// the rejected dial's reload hangs on secret resolution.
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
	assert.Equal(t, []string{"old", "old", "rotated"}, *passwords)
	assert.Equal(t, int32(1), reloads.Load())
}

func TestReloadableConnectorRefreshWaiterRespectsDialContext(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app",
		hungReload(started, release, &reloads, "postgres://schemabot:rotated@localhost:5432/app", nil))

	var wg sync.WaitGroup
	wg.Go(func() {
		cfg, _, ok := c.refresh(t.Context(), 0)
		assert.True(t, ok)
		assert.Equal(t, "rotated", cfg.Password)
	})
	waitClosed(t, started, "reload never started")

	// A waiter whose dial context ends while the leader's reload hangs gives
	// up and surfaces its dial error instead of blocking indefinitely.
	ctx, cancel := context.WithCancel(t.Context())
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		cfg, _, ok := c.refresh(ctx, 0)
		assert.False(t, ok)
		assert.Nil(t, cfg)
	}()
	cancel()
	waitClosed(t, waiterDone, "waiter did not honor its dial context")

	close(release)
	wg.Wait()
	assert.Equal(t, int32(1), reloads.Load(), "the canceled waiter must not trigger its own reload")
}

func TestReloadableConnectorRefreshWaiterObservesLeaderFailure(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app",
		hungReload(started, release, &reloads, "", errors.New("secret backend unavailable")))

	// The clock seam doubles as an entered-refresh gate: an expired cooldown
	// stamp makes every refresh iteration consult the clock under the mutex,
	// so the second consult is the waiter's. Once past it, the waiter can
	// only reach the select on the leader's done channel — releasing the
	// leader after that pins the wake-then-recheck path instead of leaving
	// it to the scheduler.
	c.lastReloadFail = time.Now().Add(-2 * reloadCooldown)
	var clockCalls atomic.Int32
	waiterEntered := make(chan struct{})
	c.now = func() time.Time {
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

	// A same-generation waiter observes the leader's failed reload through
	// the armed cooldown and reports failure without reloading again.
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		cfg, _, ok := c.refresh(t.Context(), 0)
		assert.False(t, ok)
		assert.Nil(t, cfg)
	}()
	waitClosed(t, waiterEntered, "waiter never entered refresh")
	close(release)
	wg.Wait()
	waitClosed(t, waiterDone, "waiter did not observe the leader's failed reload")
	assert.Equal(t, int32(1), reloads.Load(), "the waiter must not reload during the cooldown the failure armed")
}

// A reload callback that panics must not wedge the connector: the publish
// defer releases the reloading guard, unblocks waiters, and arms the
// cooldown before the panic propagates to the leader's caller.
func TestReloadableConnectorReloadPanicUnblocksWaiters(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var reloads atomic.Int32
	c := newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		reloads.Add(1)
		close(started)
		<-release
		panic("secret backend panicked")
	})

	var wg sync.WaitGroup
	wg.Go(func() {
		defer func() {
			assert.Equal(t, "secret backend panicked", recover(), "refresh must propagate the reload panic")
		}()
		_, _, _ = c.refresh(t.Context(), 0)
		assert.Fail(t, "refresh must not return normally from a panicking reload")
	})
	waitClosed(t, started, "reload never started")

	// A same-generation waiter blocked on the leader's outcome is unblocked
	// by the publish defer and observes the armed cooldown.
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		cfg, _, ok := c.refresh(t.Context(), 0)
		assert.False(t, ok)
		assert.Nil(t, cfg)
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

// A rejection verdict on superseded credentials must not arm the cooldown:
// while a retry dial is in flight, another rotation can swap in newer
// credentials, and stamping the late rejection onto that newer generation
// would suppress its legitimate reloads for a full window.
func TestReloadableConnectorStaleRejectionDoesNotArmCooldown(t *testing.T) {
	var c *reloadableConnector
	original := connectConfig
	t.Cleanup(func() { connectConfig = original })
	var dials atomic.Int32
	connectConfig = func(_ context.Context, _ pgx.ConnConfig) (driver.Conn, error) {
		switch dials.Add(1) {
		case 1:
			// The initial dial is rejected, triggering the reload.
			return nil, authError()
		case 2:
			// While the retry with reloaded credentials is in flight, a
			// concurrent rotation advances the generation; the retry then
			// comes back rejected — a verdict on already-superseded
			// credentials.
			c.mu.Lock()
			c.gen++
			c.mu.Unlock()
			return nil, authError()
		default:
			return stubConn{}, nil
		}
	}
	c = newReloadableConnector(t, "postgres://schemabot:old@localhost:5432/app", func() (string, error) {
		return "postgres://schemabot:rotated@localhost:5432/app", nil
	})

	_, err := c.Connect(t.Context())
	require.Error(t, err)
	c.mu.Lock()
	defer c.mu.Unlock()
	assert.True(t, c.lastReloadFail.IsZero(), "a stale rejection must not arm the cooldown against the newer generation")
}

func TestIsAuthError(t *testing.T) {
	assert.True(t, isAuthError(&pgconn.PgError{Code: "28P01"}))
	assert.True(t, isAuthError(&pgconn.PgError{Code: "28000"}))
	assert.True(t, isAuthError(fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "28P01"})))
	assert.False(t, isAuthError(&pgconn.PgError{Code: "55P03"}))
	assert.False(t, isAuthError(errors.New("connection refused")))
	assert.False(t, isAuthError(nil))
}
