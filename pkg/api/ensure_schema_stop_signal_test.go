package api

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

// convergenceStopDeadline bounds how long a stopped convergence may take to
// return. It is far below the convergence budget the test runs under, and below
// the connect timeout every SchemaBot-managed pool carries, so a convergence
// that ignored the stop could not satisfy it by any route.
const convergenceStopDeadline = 10 * time.Second

// blackholeListener accepts TCP connections and then never speaks, so a
// database driver blocks reading the server's handshake until its context ends.
// It returns the address to point a DSN at, and a channel closed once a
// connection has been accepted — the point at which a convergence is provably
// inside a blocking call rather than about to enter one.
func blackholeListener(t *testing.T) (string, <-chan struct{}) {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		assert.NoError(t, listener.Close())
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			assert.NoError(t, conn.Close())
		}
	})

	accepted := make(chan struct{})
	var once sync.Once
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				// The listener was closed at test cleanup; nothing left to accept.
				return
			}
			// Hold the connection open and silent. Closing it would let the
			// driver fail on its own, and a convergence that returned for that
			// reason would prove nothing about the stop.
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			once.Do(func() { close(accepted) })
		}
	}()
	return listener.Addr().String(), accepted
}

// An instance told to stop while its storage convergence is in flight stops
// there. The convergence carries a budget of its own measured in minutes and is
// the longest step of startup, so an instance that kept converging past the
// signal would ignore it for minutes while holding its scheduling slot. What
// comes back says it was stopped rather than naming the budget, so an operator
// reading it is not sent looking for a database too slow to converge.
func TestEnsureSchemaStopsWhenItsStopSignalCloses(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		dialect schema.Dialect
		dsn     func(addr string) string
	}{
		{
			name:    "mysql",
			dialect: schema.DialectMySQL,
			dsn:     func(addr string) string { return "user:pass@tcp(" + addr + ")/schemabot" },
		},
		{
			name:    "postgres",
			dialect: schema.DialectPostgres,
			dsn:     func(addr string) string { return "postgres://user:pass@" + addr + "/schemabot?sslmode=disable" },
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			addr, accepted := blackholeListener(t)
			stop := make(chan struct{})

			done := make(chan error, 1)
			go func() {
				done <- EnsureSchema(tt.dsn(addr), slog.New(slog.DiscardHandler),
					WithDialect(tt.dialect),
					WithStopSignal(stop))
			}()

			select {
			case <-accepted:
			case err := <-done:
				t.Fatalf("convergence returned before it reached the database: %v", err)
			case <-time.After(convergenceStopDeadline):
				t.Fatal("convergence never connected to the storage database")
			}
			close(stop)

			select {
			case err := <-done:
				require.Error(t, err)
				require.ErrorIs(t, err, context.Canceled)
				assert.NotErrorIs(t, err, context.DeadlineExceeded)
				assert.NotContains(t, err.Error(), EnsureSchemaTimeout.String())
			case <-time.After(convergenceStopDeadline):
				t.Fatalf("convergence did not return within %s of the stop", convergenceStopDeadline)
			}
		})
	}
}

// A convergence given no stop signal is one nothing but its budget can end.
// This is every caller that has not been told to stop by something outside
// itself, and the default: the option has to be passed for a convergence to
// become stoppable at all, so no caller acquires the ability by threading a
// context that happened to be in scope.
func TestEnsureSchemaWithoutAStopSignalRunsToItsBudget(t *testing.T) {
	t.Parallel()

	addr, accepted := blackholeListener(t)

	const budget = 2 * time.Second
	done := make(chan error, 1)
	go func() {
		done <- EnsureSchema("user:pass@tcp("+addr+")/schemabot", slog.New(slog.DiscardHandler),
			WithDialect(schema.DialectMySQL),
			WithConvergenceTimeout(budget))
	}()

	select {
	case <-accepted:
	case err := <-done:
		t.Fatalf("convergence returned before it reached the database: %v", err)
	case <-time.After(convergenceStopDeadline):
		t.Fatal("convergence never connected to the storage database")
	}

	select {
	case err := <-done:
		require.Error(t, err)
		assert.NotErrorIs(t, err, context.Canceled)
	case <-time.After(convergenceStopDeadline):
		t.Fatalf("convergence did not return within %s of its budget", convergenceStopDeadline)
	}
}
