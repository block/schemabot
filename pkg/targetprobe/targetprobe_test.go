package targetprobe

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/storage"
)

const (
	testTimeout  = 5 * time.Second
	testPoolSize = 2
)

type fakeResolver struct {
	requests []inventory.ProbeRequest
	target   *inventory.Target
	err      error
}

func (r *fakeResolver) ResolveTarget(context.Context, inventory.Request) (*inventory.Target, error) {
	return r.target, r.err
}

func (r *fakeResolver) Enumerate(context.Context) ([]inventory.ProbeRequest, error) {
	return r.requests, nil
}

func (r *fakeResolver) UnenumerableDatabaseTypes() []string { return nil }

// funcResolver enumerates fixed requests and delegates resolution to a
// function, so a test can make one target's resolution misbehave.
type funcResolver struct {
	requests []inventory.ProbeRequest
	resolve  func(context.Context, inventory.Request) (*inventory.Target, error)
}

func (r *funcResolver) ResolveTarget(ctx context.Context, req inventory.Request) (*inventory.Target, error) {
	return r.resolve(ctx, req)
}

func (r *funcResolver) Enumerate(context.Context) ([]inventory.ProbeRequest, error) {
	return r.requests, nil
}

func (r *funcResolver) UnenumerableDatabaseTypes() []string { return nil }

type resolverOnly struct{}

func (resolverOnly) ResolveTarget(context.Context, inventory.Request) (*inventory.Target, error) {
	return nil, errors.New("must not resolve")
}

// typedResolverOnly is a discovery resolver that serves one database type
// without enumerating targets.
type typedResolverOnly struct {
	resolverOnly
	databaseType string
}

func (r typedResolverOnly) DatabaseType() string { return r.databaseType }

func newTestProber(resolver inventory.Resolver, timeout time.Duration) (*Prober, *bytes.Buffer) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	return New(resolver, logger, timeout, testPoolSize), &logs
}

// newDebugTestProber is newTestProber at debug level, for scenarios whose
// outcome is a skipped or discarded probe that only the debug log records.
func newDebugTestProber(resolver inventory.Resolver, timeout time.Duration) (*Prober, *bytes.Buffer) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return New(resolver, logger, timeout, testPoolSize), &logs
}

func TestRunResolverWithoutEnumerator(t *testing.T) {
	prober, logs := newTestProber(resolverOnly{}, time.Second)
	prober.Run(t.Context())
	assert.Equal(t, 1, bytes.Count(logs.Bytes(), []byte("level=INFO")))
	assert.Contains(t, logs.String(), "discovery-resolved targets are outside probe coverage")
	assert.NotContains(t, logs.String(), "database_types")
}

// A discovery resolver that serves one engine names it in the coverage log,
// so an operator reading startup logs knows which database type has no probe
// results rather than only that some type is uncovered.
func TestRunResolverWithoutEnumeratorNamesItsDatabaseType(t *testing.T) {
	prober, logs := newTestProber(typedResolverOnly{databaseType: storage.DatabaseTypePostgres}, time.Second)
	prober.Run(t.Context())
	assert.Contains(t, logs.String(), "discovery-resolved targets are outside probe coverage")
	assert.Contains(t, logs.String(), "database_types=[postgres]")
}

func TestRunResolveError(t *testing.T) {
	resolver := &fakeResolver{requests: []inventory.ProbeRequest{{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL}}, err: errors.New("resolve failed")}
	prober, logs := newTestProber(resolver, time.Second)
	prober.Run(t.Context())
	assert.Contains(t, logs.String(), "outcome=resolve_error")
}

func TestRunClassifiesOpenErrors(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		outcome string
	}{
		{name: "authentication", err: &mysql.MySQLError{Number: 1045, Message: "access denied"}, outcome: "auth_invalid_credentials"},
		{name: "connection", err: errors.New("dial failed"), outcome: "connection_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := &fakeResolver{
				requests: []inventory.ProbeRequest{{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL}},
				target:   &inventory.Target{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL, DSN: "user:secretpw@tcp(host)/"},
			}
			prober, logs := newTestProber(resolver, time.Second)
			prober.open = func(context.Context, *inventory.Target) (*sql.DB, error) { return nil, tt.err }
			prober.Run(t.Context())
			assert.Contains(t, logs.String(), "outcome="+tt.outcome)
			assert.NotContains(t, logs.String(), "secretpw")
		})
	}
}

func TestRunTimeout(t *testing.T) {
	resolver := &fakeResolver{
		requests: []inventory.ProbeRequest{{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL}},
		target:   &inventory.Target{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL, DSN: "secretpw"},
	}
	prober, logs := newTestProber(resolver, time.Millisecond)
	prober.open = func(ctx context.Context, _ *inventory.Target) (*sql.DB, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	prober.Run(t.Context())
	assert.Contains(t, logs.String(), "outcome=timeout")
	assert.NotContains(t, logs.String(), "secretpw")
}

// Resolution that runs past the per-target deadline (a slow secret lookup, an
// unreachable inventory service) is a timeout, not a resolver error: the
// deadline is what ended it, and the outcome should say so.
func TestRunResolveTimeout(t *testing.T) {
	resolver := &funcResolver{
		requests: []inventory.ProbeRequest{{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL}},
		resolve: func(ctx context.Context, _ inventory.Request) (*inventory.Target, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	prober, logs := newTestProber(resolver, time.Millisecond)
	prober.open = func(context.Context, *inventory.Target) (*sql.DB, error) {
		return nil, errors.New("must not open an unresolved target")
	}
	prober.Run(t.Context())
	assert.Contains(t, logs.String(), "outcome=timeout")
	assert.NotContains(t, logs.String(), "outcome=resolve_error")
}

// A resolver that scopes targets by environment refuses a request without one,
// so the probe resolves each enumerated target under the environment it was
// enumerated with, and the outcome it records carries that environment.
func TestRunPropagatesEnumeratedEnvironment(t *testing.T) {
	resolver := &funcResolver{
		requests: []inventory.ProbeRequest{{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging"}},
		resolve: func(_ context.Context, req inventory.Request) (*inventory.Target, error) {
			if req.Environment != "staging" {
				return nil, fmt.Errorf("target %q is scoped to an environment; got %q", req.Target, req.Environment)
			}
			return &inventory.Target{Target: req.Target, DatabaseType: req.DatabaseType, DSN: "secretpw"}, nil
		},
	}
	prober, logs := newTestProber(resolver, time.Second)
	prober.open = func(context.Context, *inventory.Target) (*sql.DB, error) {
		return nil, errors.New("dial failed")
	}
	prober.Run(t.Context())
	assert.Contains(t, logs.String(), "environment=staging")
	assert.Contains(t, logs.String(), "outcome=connection_error")
	assert.NotContains(t, logs.String(), "outcome=resolve_error")
	assert.NotContains(t, logs.String(), "secretpw")
}

// A resolver that answers with neither a target nor an error has not resolved
// anything; the probe records that as a resolver failure instead of dialing
// nothing and reporting the target healthy.
func TestRunResolverReturnsNoTarget(t *testing.T) {
	resolver := &funcResolver{
		requests: []inventory.ProbeRequest{{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL}},
		resolve: func(context.Context, inventory.Request) (*inventory.Target, error) {
			return nil, nil
		},
	}
	prober, logs := newTestProber(resolver, time.Second)
	opened := false
	prober.open = func(context.Context, *inventory.Target) (*sql.DB, error) {
		opened = true
		return nil, errors.New("must not open an unresolved target")
	}
	prober.Run(t.Context())
	assert.False(t, opened)
	assert.Contains(t, logs.String(), "outcome=resolve_error")
	assert.Contains(t, logs.String(), "returned no target")
}

// blockingEnumerator enumerates only when its context lets it, standing in for
// an inventory that does not answer.
type blockingEnumerator struct {
	resolverOnly
}

func (blockingEnumerator) Enumerate(ctx context.Context) ([]inventory.ProbeRequest, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (blockingEnumerator) UnenumerableDatabaseTypes() []string { return nil }

// Enumeration runs under the per-target bound, so an inventory that never
// answers fails the run within that bound instead of holding the probe
// goroutine, and the shutdown that waits on it, open indefinitely.
func TestRunBoundsEnumeration(t *testing.T) {
	prober, logs := newTestProber(blockingEnumerator{}, time.Millisecond)
	done := make(chan struct{})
	go func() {
		prober.Run(t.Context())
		close(done)
	}()
	waitCtx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	select {
	case <-done:
	case <-waitCtx.Done():
		require.NoError(t, waitCtx.Err(), "waiting for the bounded enumeration to fail the run")
	}
	assert.Contains(t, logs.String(), "enumerate targets failed")
	assert.Contains(t, logs.String(), context.DeadlineExceeded.Error())
	assert.NotContains(t, logs.String(), "probing enumerated targets")
}

// Targets whose database type has no DSN the probe can dial are skipped before
// resolution and left out of the outcome counts: Vitess connects through an
// API, and an unfamiliar type has nothing the probe can vouch for. Neither is
// a connection failure.
func TestRunSkipsTypesWithoutDSNProbe(t *testing.T) {
	for _, databaseType := range []string{storage.DatabaseTypeVitess, "custom-engine"} {
		t.Run(databaseType, func(t *testing.T) {
			resolver := &funcResolver{
				requests: []inventory.ProbeRequest{{Target: "target-a", DatabaseType: databaseType}},
				resolve: func(context.Context, inventory.Request) (*inventory.Target, error) {
					return nil, errors.New("must not resolve a skipped target")
				},
			}
			prober, logs := newDebugTestProber(resolver, time.Second)
			called := false
			prober.open = func(context.Context, *inventory.Target) (*sql.DB, error) {
				called = true
				return nil, errors.New("unexpected open")
			}
			prober.Run(t.Context())
			assert.False(t, called)
			assert.Contains(t, logs.String(), "outside DSN probe coverage")
			assert.NotContains(t, logs.String(), "target probe: failed")
			assert.NotContains(t, logs.String(), "outcome=")
			assert.Contains(t, logs.String(), "completed enumerated targets")
		})
	}
}

// A probe the shutdown interrupts mid-dial says nothing about the target, so
// its result is discarded rather than recorded as a connection failure that
// would send an operator chasing a healthy database.
func TestRunDiscardsProbeCutShortByShutdown(t *testing.T) {
	resolver := &fakeResolver{
		requests: []inventory.ProbeRequest{{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL}},
		target:   &inventory.Target{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL, DSN: "secretpw"},
	}
	prober, logs := newDebugTestProber(resolver, testTimeout)
	dialing := make(chan struct{})
	prober.open = func(ctx context.Context, _ *inventory.Target) (*sql.DB, error) {
		close(dialing)
		<-ctx.Done()
		return nil, errors.New("dial interrupted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		prober.Run(ctx)
		close(done)
	}()
	waitCtx, cancelWait := context.WithTimeout(t.Context(), testTimeout)
	defer cancelWait()
	select {
	case <-dialing:
	case <-waitCtx.Done():
		require.NoError(t, waitCtx.Err(), "waiting for the probe to start dialing")
	}
	cancel()
	select {
	case <-done:
	case <-waitCtx.Done():
		require.NoError(t, waitCtx.Err(), "waiting for the cancelled probe to finish")
	}
	assert.Contains(t, logs.String(), "discarding the in-flight result")
	assert.NotContains(t, logs.String(), "outcome=")
	assert.NotContains(t, logs.String(), "secretpw")
}

// A panic while probing one target costs that target its result and nothing
// else: the other targets are still probed, the completion log still lands,
// and the panic value and stack are in the log for triage.
func TestRunContainsPerTargetPanic(t *testing.T) {
	resolver := &funcResolver{
		requests: []inventory.ProbeRequest{
			{Target: "panics", DatabaseType: storage.DatabaseTypeMySQL},
			{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL},
		},
		resolve: func(_ context.Context, req inventory.Request) (*inventory.Target, error) {
			if req.Target == "panics" {
				panic("resolver bug for " + req.Target)
			}
			return &inventory.Target{Target: req.Target, DatabaseType: req.DatabaseType, DSN: "secretpw"}, nil
		},
	}
	prober, logs := newTestProber(resolver, time.Second)
	prober.open = func(context.Context, *inventory.Target) (*sql.DB, error) {
		return nil, errors.New("dial failed")
	}
	prober.Run(t.Context())
	assert.Contains(t, logs.String(), "probe panicked; the remaining targets continue")
	assert.Contains(t, logs.String(), `panic="resolver bug for panics"`)
	assert.Contains(t, logs.String(), "stack=")
	assert.Contains(t, logs.String(), "target=target-a")
	assert.Contains(t, logs.String(), "outcome=connection_error")
	assert.Contains(t, logs.String(), "completed enumerated targets")
	assert.NotContains(t, logs.String(), "secretpw")
}

// The pool admits at most its size in probes at once: with two probes held open,
// the remaining targets wait for a worker rather than dialing alongside them,
// and every target is still probed once the held probes return.
func TestRunBoundsConcurrency(t *testing.T) {
	requests := make([]inventory.ProbeRequest, 5)
	for i := range requests {
		requests[i] = inventory.ProbeRequest{Target: "target", DatabaseType: storage.DatabaseTypeMySQL}
	}
	resolver := &fakeResolver{requests: requests, target: &inventory.Target{Target: "target", DatabaseType: storage.DatabaseTypeMySQL, DSN: "secretpw"}}
	prober, logs := newTestProber(resolver, testTimeout)
	release := make(chan struct{})
	entered := make(chan struct{}, len(requests))
	var current atomic.Int32
	var maximum atomic.Int32
	var probed atomic.Int32
	prober.open = func(context.Context, *inventory.Target) (*sql.DB, error) {
		now := current.Add(1)
		for old := maximum.Load(); now > old && !maximum.CompareAndSwap(old, now); old = maximum.Load() {
		}
		probed.Add(1)
		entered <- struct{}{}
		<-release
		current.Add(-1)
		return nil, errors.New("expected open failure")
	}
	done := make(chan struct{})
	go func() {
		prober.Run(t.Context())
		close(done)
	}()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	for range testPoolSize {
		select {
		case <-entered:
		case <-ctx.Done():
			require.NoError(t, ctx.Err(), "waiting for the pool to fill")
		}
	}
	// With both workers held, the pool is full: a third probe entering now is
	// the bound being ignored, whatever the goroutine scheduling. The window is
	// long enough for an unbounded pool to dial the remaining targets.
	select {
	case <-entered:
		t.Fatal("a probe entered while the pool was full")
	case <-time.After(100 * time.Millisecond):
	}
	assert.Equal(t, int32(testPoolSize), current.Load(), "probes in flight while the pool is held")
	close(release)
	select {
	case <-done:
	case <-ctx.Done():
		require.NoError(t, ctx.Err(), "waiting for probes to complete")
	}
	assert.Equal(t, int32(testPoolSize), maximum.Load(), "most probes ever in flight at once")
	assert.Equal(t, int32(len(requests)), probed.Load(), "every enumerated target is probed")
	assert.NotContains(t, logs.String(), "secretpw")
}

func TestRunSkipsTargetsOnceShutdownBegins(t *testing.T) {
	resolver := &fakeResolver{
		requests: []inventory.ProbeRequest{{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL}},
		target:   &inventory.Target{Target: "target-a", DatabaseType: storage.DatabaseTypeMySQL, DSN: "secretpw"},
	}
	var logs bytes.Buffer
	prober := New(resolver, slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})), time.Second, 2)
	opened := false
	prober.open = func(context.Context, *inventory.Target) (*sql.DB, error) {
		opened = true
		return nil, errors.New("unexpected open")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	prober.Run(ctx)
	assert.False(t, opened)
	assert.NotContains(t, logs.String(), "outcome=")
	assert.Contains(t, logs.String(), "shutting down")
}

// A PostgreSQL target dials under the trust its CA reference names, exactly as
// the engine does before a plan: a reference the engine refuses (a bundle that
// is not there, a relative path) fails the probe before any dial, and a
// readable pinned bundle opens the pool.
func TestOpenDatabasePostgresHonorsCAReference(t *testing.T) {
	dsn := "postgres://user:secretpw@db.example.com:5432/app?sslmode=verify-full"
	target := func(caRef string) *inventory.Target {
		return &inventory.Target{Target: "target-a", DatabaseType: storage.DatabaseTypePostgres, DSN: dsn, Metadata: map[string]string{"postgres_ca_ref": caRef}}
	}

	t.Run("missing bundle fails before dialing", func(t *testing.T) {
		db, err := openDatabase(t.Context(), target("file:"+filepath.Join(t.TempDir(), "absent.pem")))
		require.Error(t, err)
		assert.Nil(t, db)
		assert.Contains(t, err.Error(), "read PostgreSQL CA bundle")
		assert.NotContains(t, err.Error(), "secretpw")
	})

	t.Run("relative bundle path is refused", func(t *testing.T) {
		db, err := openDatabase(t.Context(), target("file:certs/ca.pem"))
		require.Error(t, err)
		assert.Nil(t, db)
		assert.Contains(t, err.Error(), "requires an absolute path")
	})

	t.Run("pinned bundle opens the pool", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ca.pem")
		require.NoError(t, os.WriteFile(path, testCAPEM(t), 0o600))
		db, err := openDatabase(t.Context(), target("file:"+path))
		require.NoError(t, err)
		defer utils.CloseAndLog(db)
		assert.NotNil(t, db)
	})
}

// testCAPEM returns a self-signed CA certificate in PEM form.
func testCAPEM(t *testing.T) []byte {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "schemabot test ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestOpenDatabaseRejectsNonDSNTypes(t *testing.T) {
	db, err := openDatabase(t.Context(), &inventory.Target{Target: "target-a", DatabaseType: storage.DatabaseTypeVitess, DSN: "secretpw"})
	require.Error(t, err)
	assert.Nil(t, db)
	assert.NotContains(t, err.Error(), "secretpw")
}
