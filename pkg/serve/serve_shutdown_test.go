package serve

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

// buildCancelDeadline bounds a Build that is expected to return on its
// cancelled context. It is far below the storage boot budget, so a Build that
// waits out any part of that budget fails here rather than passing slowly.
const buildCancelDeadline = 15 * time.Second

// unresponsiveAddr returns the address of a listener that accepts connections
// and never answers, standing in for a database that is reachable but not
// responding — what keeps a boot attempt blocked instead of failing fast.
func unresponsiveAddr(t *testing.T) string {
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

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()

	return listener.Addr().String()
}

// Build runs on its caller's context, so cancelling it returns from a storage
// boot instead of finishing it. Storage boot is where a server spends its
// startup — it retries for minutes against a database that is not answering —
// and a caller that has stopped wanting the server should not have to wait out
// that budget to say so.
func TestBuildStopsWhenItsContextIsCanceled(t *testing.T) {
	addr := unresponsiveAddr(t)
	cfg := &api.ServerConfig{
		Storage:   api.StorageConfig{DSN: "root:test@tcp(" + addr + ")/schemabot"},
		Databases: map[string]api.DatabaseConfig{"app": {Type: "mysql", Environments: map[string]api.EnvironmentConfig{"production": {DSN: "root:test@tcp(" + addr + ")/app"}}}},
	}

	ctx, cancel := context.WithCancel(t.Context())
	built := make(chan error, 1)
	go func() {
		_, err := Build(ctx, cfg, WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
		built <- err
	}()

	cancel()

	select {
	case err := <-built:
		require.Error(t, err)
		// The distinction is the point: a deadline reports a database that was
		// too slow, and an operator chases the database. Cancellation reports a
		// process that was told to stop, and there is nothing to chase.
		require.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, context.DeadlineExceeded)
		assert.NotContains(t, err.Error(), api.EnsureSchemaTimeout.String())
	case <-time.After(buildCancelDeadline):
		t.Fatalf("Build did not return within %s of its context being canceled", buildCancelDeadline)
	}
}

// Close waits for the detached reconciliation pass, but only for as long as its
// drain allows: a pass that will not finish is cancelled and left behind rather
// than held onto, because the comments it did not reach are reconciled by the
// next process to start, and a process that cannot close is not that process.
func TestReconcilePassStopIsBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// pass reports whether it returns when its context is cancelled.
		cooperative bool
		wantWithin  time.Duration
		wantLogs    []string
		notWantLog  string
	}{
		{
			name:        "pass returns when canceled",
			cooperative: true,
			wantWithin:  missingSummaryReconcileDrainTimeout + missingSummaryReconcileCancelGrace,
			wantLogs:    []string{"did not finish within the shutdown drain"},
			notWantLog:  "has not returned since it was canceled",
		},
		{
			name:        "pass ignores cancellation",
			cooperative: false,
			wantWithin:  missingSummaryReconcileDrainTimeout + missingSummaryReconcileCancelGrace + 5*time.Second,
			wantLogs: []string{
				"did not finish within the shutdown drain",
				"has not returned since it was canceled",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logs bytes.Buffer
			var mu sync.Mutex
			logger := slog.New(slog.NewTextHandler(&lockedWriter{mu: &mu, w: &logs}, nil))

			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			runtime := webhookRuntime{reconcileMissingSummaryComments: func(ctx context.Context) {
				if tt.cooperative {
					select {
					case <-ctx.Done():
					case <-release:
					}
					return
				}
				<-release
			}}

			pass := runtime.StartMissingSummaryReconciliation(t.Context(), logger)
			require.NotNil(t, pass)

			start := time.Now()
			pass.stop(logger)
			elapsed := time.Since(start)

			assert.GreaterOrEqual(t, elapsed, missingSummaryReconcileDrainTimeout,
				"the pass gets its full drain before being canceled")
			assert.Less(t, elapsed, tt.wantWithin, "stop returns on its own bound, not on the pass")

			mu.Lock()
			output := logs.String()
			mu.Unlock()
			for _, want := range tt.wantLogs {
				assert.Contains(t, output, want)
			}
			if tt.notWantLog != "" {
				assert.NotContains(t, output, tt.notWantLog)
			}
		})
	}
}

// lockedWriter serializes writes from the reconciliation goroutine and the test
// that reads them.
type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
