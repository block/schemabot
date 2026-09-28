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
	"github.com/block/schemabot/pkg/drain"
)

// A server that is never told what budget to keep keeps the default, and one
// that asks for none runs unbudgeted. The two are different answers to
// different questions, and both leave options.shutdownBudget at zero, which is
// why the option records that it was set at all.
func TestShutdownBudgetFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []Option
		want time.Duration
	}{
		{name: "unset", want: shutdownBudgetTotal},
		{name: "set", opts: []Option{WithShutdownBudget(90 * time.Second)}, want: 90 * time.Second},
		{name: "set to zero runs unbudgeted", opts: []Option{WithShutdownBudget(0)}, want: 0},
		{name: "negative runs unbudgeted", opts: []Option{WithShutdownBudget(-time.Second)}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var o options
			for _, opt := range tt.opts {
				opt(&o)
			}

			assert.Equal(t, tt.want, shutdownBudgetFor(o))
		})
	}
}

// Shutdown begins wherever it is first noticed, and only once. Run reaches it
// at the top of its own shutdown and Close reaches it for an embedder that
// never ran the server, so both call it — and the second call must measure
// itself against the first call's deadline rather than starting a fresh one,
// which would restore the summing the budget exists to stop.
func TestBeginShutdownArmsOneBudgetForEveryStage(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handlerBudgets := make(chan *drain.Budget, 4)
	srv := &Server{
		logger:              logger,
		svc:                 api.New(nil, &api.ServerConfig{}, nil, logger),
		webhook:             webhookRuntime{setShutdownBudget: func(b *drain.Budget) { handlerBudgets <- b }},
		shutdownBudgetTotal: time.Minute,
	}

	first := srv.beginShutdown()
	require.NotNil(t, first)
	time.Sleep(50 * time.Millisecond)
	assert.Same(t, first, srv.beginShutdown(), "the second stage shares the first stage's deadline")

	assert.Less(t, srv.shutdownAllot(time.Minute), time.Minute,
		"a stage asking for the whole budget gets only what the elapsed time left")

	require.Len(t, handlerBudgets, 1, "the webhook pool is told once")
	assert.Same(t, first, <-handlerBudgets)
}

// An unbudgeted server hands every stage its own bound in full, and says so,
// because an operator reading a shutdown that overran needs to know whether
// there was a budget at all.
func TestBeginShutdownLeavesAnUnbudgetedServerUnbounded(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	srv := &Server{logger: slog.New(slog.NewTextHandler(&logs, nil))}

	assert.Nil(t, srv.beginShutdown())
	assert.Equal(t, time.Minute, srv.shutdownAllot(time.Minute))
	assert.Contains(t, logs.String(), "shutdown is unbudgeted")
}

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
			pass.stop(logger, nil)
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

// A shutdown whose earlier stages already spent the budget does not then wait
// the reconciliation pass's own seven seconds on top. The stage still runs, and
// still says what it gave up; it just gets what is left rather than a fresh
// bound of its own, which is what keeps a shutdown where several stages hang
// inside the deployment's termination grace period instead of the sum of every
// bound.
func TestReconciliationStopSpendsOnlyWhatIsLeftOfTheShutdownBudget(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&lockedWriter{mu: &mu, w: &logs}, nil))

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	runtime := webhookRuntime{reconcileMissingSummaryComments: func(context.Context) { <-release }}

	pass := runtime.StartMissingSummaryReconciliation(t.Context(), logger)
	require.NotNil(t, pass)

	budget := drain.NewBudget(time.Nanosecond)
	require.True(t, budget.Spent())

	start := time.Now()
	pass.stop(logger, budget)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, missingSummaryReconcileDrainTimeout,
		"a spent budget leaves the stage less than its own bound")

	mu.Lock()
	output := logs.String()
	mu.Unlock()
	assert.Contains(t, output, "did not finish within the shutdown drain")
	assert.Contains(t, output, "budget_spent=true")
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
