package targetprobe

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/storage"
)

const testTimeout = 5 * time.Second

type fakeResolver struct {
	requests []inventory.ProbeRequest
	target   *inventory.Target
	err      error
}

func (r *fakeResolver) ResolveTarget(context.Context, inventory.Request) (*inventory.Target, error) {
	return r.target, r.err
}

func (r *fakeResolver) Enumerate() []inventory.ProbeRequest { return r.requests }

type resolverOnly struct{}

func (resolverOnly) ResolveTarget(context.Context, inventory.Request) (*inventory.Target, error) {
	return nil, errors.New("must not resolve")
}

func newTestProber(resolver inventory.Resolver, timeout time.Duration) (*Prober, *bytes.Buffer) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	return New(resolver, logger, timeout, 2), &logs
}

func TestRunResolverWithoutEnumerator(t *testing.T) {
	prober, logs := newTestProber(resolverOnly{}, time.Second)
	prober.Run(t.Context())
	assert.Equal(t, 1, bytes.Count(logs.Bytes(), []byte("level=INFO")))
	assert.Contains(t, logs.String(), "discovery-resolved targets are outside probe coverage")
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
			prober.open = func(context.Context, string, string) (*sql.DB, error) { return nil, tt.err }
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
	prober.open = func(ctx context.Context, _, _ string) (*sql.DB, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	prober.Run(t.Context())
	assert.Contains(t, logs.String(), "outcome=timeout")
	assert.NotContains(t, logs.String(), "secretpw")
}

func TestRunSkipsVitess(t *testing.T) {
	resolver := &fakeResolver{requests: []inventory.ProbeRequest{{Target: "target-a", DatabaseType: storage.DatabaseTypeVitess}}}
	prober, logs := newTestProber(resolver, time.Second)
	called := false
	prober.open = func(context.Context, string, string) (*sql.DB, error) {
		called = true
		return nil, errors.New("unexpected open")
	}
	prober.Run(t.Context())
	assert.False(t, called)
	assert.NotContains(t, logs.String(), "target probe: failed")
}

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
	prober.open = func(context.Context, string, string) (*sql.DB, error) {
		now := current.Add(1)
		for old := maximum.Load(); now > old && !maximum.CompareAndSwap(old, now); old = maximum.Load() {
		}
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
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			require.NoError(t, ctx.Err(), "waiting for bounded probes")
		}
	}
	assert.Equal(t, int32(2), maximum.Load())
	close(release)
	select {
	case <-done:
	case <-ctx.Done():
		require.NoError(t, ctx.Err(), "waiting for probes to complete")
	}
	assert.LessOrEqual(t, maximum.Load(), int32(2))
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
	prober.open = func(context.Context, string, string) (*sql.DB, error) {
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

func TestOpenDatabaseRejectsNonDSNTypes(t *testing.T) {
	db, err := openDatabase(t.Context(), storage.DatabaseTypeVitess, "secretpw")
	require.Error(t, err)
	assert.Nil(t, db)
	assert.NotContains(t, err.Error(), "secretpw")
}
