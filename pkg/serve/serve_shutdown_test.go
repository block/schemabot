package serve

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
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

// runShutdownDeadline bounds each wait on a served server in the shutdown test:
// reaching Start, and returning once signalled. Neither waits on anything that
// is slow by design, so a wait that outlasts it is a hang rather than a slow
// machine.
const runShutdownDeadline = 30 * time.Second

// A pod driving a schema change receives SIGTERM during a rolling deploy. The signal stops the listeners at once, but the background work Start
// launched — the operator's drives among it — keeps running until Close ends it.
// A drive that ended on the signal itself would deregister its claim before
// Close opened the claim drain, so Close would have nothing to hand back and a
// peer would wait out the whole staleness window while this pod's engine could
// still be copying. Close ends the drives in its own order instead, engines down
// and then claims handed back, and nothing Start launched outlives the run.
//
// The durable webhook dispatch stands in for the operator here: Start hands
// every loop it launches the same context, and the dispatch is the one whose
// start and stop a test can observe without a database.
func TestServeKeepsBackgroundWorkRunningUntilCloseAfterShutdownSignal(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// A lazily-opened handle: never connected, so the background loops fail
	// their storage reads fast and svc.Close can close it without a database.
	db, err := sql.Open("block-mysql", "schemabot@tcp(127.0.0.1:1)/schemabot")
	require.NoError(t, err)
	// Close (via svc.Close) owns the handle; this cleanup only prevents a leak
	// when the test fails before the server shuts down.
	serverClosed := false
	t.Cleanup(func() {
		if !serverClosed {
			utils.CloseAndLog(db)
		}
	})
	authorizer, err := auth.NewLocalAuthorizer(strings.Repeat("a", 64), logger)
	require.NoError(t, err)

	started := make(chan context.Context, 1)
	liveWhenCloseStoppedIt := make(chan bool, 1)
	var backgroundCtx context.Context
	srv := &Server{
		cfg:       &api.ServerConfig{MetricsPort: freeTCPPort(t)},
		svc:       api.New(mysqlstore.New(db), &api.ServerConfig{}, nil, logger),
		logger:    logger,
		authz:     authorizer,
		telemetry: &api.Telemetry{MetricsHandler: http.NotFoundHandler()},
		webhook: webhookRuntime{
			handler: http.NotFoundHandler(),
			// Start and Close both run on the serving goroutine, so the context
			// captured here is read there without a race.
			startDurableWebhookDispatch: func(ctx context.Context) {
				backgroundCtx = ctx
				started <- ctx
			},
			stopDurableWebhookDispatch: func() {
				liveWhenCloseStoppedIt <- backgroundCtx.Err() == nil
			},
		},
	}

	runCtx, signalled, stopWatching := watchForShutdownSignal(t.Context())
	defer stopWatching()

	served := make(chan error, 1)
	go func() { served <- serveUntilShutdown(runCtx, srv, signalled, "0", "") }()

	var background context.Context
	select {
	case background = <-started:
	case <-time.After(runShutdownDeadline):
		t.Fatalf("the server did not start its background work within %s", runShutdownDeadline)
	}

	self, err := os.FindProcess(os.Getpid())
	require.NoError(t, err)
	require.NoError(t, self.Signal(syscall.SIGTERM))

	select {
	case err := <-served:
		serverClosed = true
		require.NoError(t, err, "a signalled server shuts down cleanly")
	case <-time.After(runShutdownDeadline):
		t.Fatalf("the server did not shut down within %s of SIGTERM", runShutdownDeadline)
	}

	require.ErrorIs(t, runCtx.Err(), context.Canceled, "the signal ends the run")
	select {
	case live := <-liveWhenCloseStoppedIt:
		assert.True(t, live, "the signal must not end the background work before Close stops it")
	default:
		t.Fatal("Close did not stop the background work")
	}
	assert.ErrorIs(t, background.Err(), context.Canceled, "the background work does not outlive the run")
}

// A signalled server keeps its in-flight drives running until Close hands their
// claims back, but it must not take on new work in the meantime: an idle driver
// that claims a pending apply during the listener drains starts an engine only
// for Close to halt it seconds later and hand the apply to a peer. The claim
// gate closes at the signal, so the drains that follow see no new claims.
//
// Storage here is a handle that never connects, so every claim attempt fails
// fast and leaves a log line; the count of those lines is the claim activity.
// The durable webhook dispatch's stop hook stands in for the drains Close runs
// before it stops the operator, and reads the count on either side of a window
// long enough for many polls.
func TestServeStopsClaimingNewWorkOnShutdownSignal(t *testing.T) {
	var logs bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&lockedWriter{mu: &mu, w: &logs}, nil))
	claimAttempts := func() int {
		mu.Lock()
		defer mu.Unlock()
		return strings.Count(logs.String(), "failed to claim apply_operation")
	}

	db, err := sql.Open("block-mysql", "schemabot@tcp(127.0.0.1:1)/schemabot")
	require.NoError(t, err)
	serverClosed := false
	t.Cleanup(func() {
		if !serverClosed {
			utils.CloseAndLog(db)
		}
	})
	authorizer, err := auth.NewLocalAuthorizer(strings.Repeat("a", 64), logger)
	require.NoError(t, err)

	const pollInterval = 20 * time.Millisecond
	svc := api.New(mysqlstore.New(db), &api.ServerConfig{}, nil, logger)
	require.NoError(t, svc.SetOperatorPollInterval(pollInterval))

	started := make(chan struct{}, 1)
	claimsAcrossDrain := make(chan [2]int, 1)
	srv := &Server{
		cfg:       &api.ServerConfig{MetricsPort: freeTCPPort(t)},
		svc:       svc,
		logger:    logger,
		authz:     authorizer,
		telemetry: &api.Telemetry{MetricsHandler: http.NotFoundHandler()},
		webhook: webhookRuntime{
			handler:                     http.NotFoundHandler(),
			startDurableWebhookDispatch: func(context.Context) { started <- struct{}{} },
			stopDurableWebhookDispatch: func() {
				before := settledClaimAttempts(claimAttempts, pollInterval)
				time.Sleep(20 * pollInterval)
				claimsAcrossDrain <- [2]int{before, claimAttempts()}
			},
		},
	}

	runCtx, signalled, stopWatching := watchForShutdownSignal(t.Context())
	defer stopWatching()

	served := make(chan error, 1)
	go func() { served <- serveUntilShutdown(runCtx, srv, signalled, "0", "") }()

	select {
	case <-started:
	case <-time.After(runShutdownDeadline):
		t.Fatalf("the server did not start its background work within %s", runShutdownDeadline)
	}
	require.Eventually(t, func() bool { return claimAttempts() > 0 }, runShutdownDeadline, 10*time.Millisecond, "drivers poll for work before the signal")

	self, err := os.FindProcess(os.Getpid())
	require.NoError(t, err)
	require.NoError(t, self.Signal(syscall.SIGTERM))

	select {
	case err := <-served:
		serverClosed = true
		require.NoError(t, err, "a signalled server shuts down cleanly")
	case <-time.After(runShutdownDeadline):
		t.Fatalf("the server did not shut down within %s of SIGTERM", runShutdownDeadline)
	}

	select {
	case counts := <-claimsAcrossDrain:
		assert.Equal(t, counts[0], counts[1], "idle drivers must not claim new work after the shutdown signal")
	default:
		t.Fatal("Close did not stop the durable webhook dispatch")
	}
}

// settledClaimAttempts returns the claim count once it has held still for two
// poll intervals, or after ten if it never does. The shutdown signal closes the
// claim gate, but a driver already inside a tick finishes it, and each rung of
// its claim ladder logs its failure whenever that dial returns; a baseline read
// before those lines land would count a tick that began before the signal as
// a claim made after it. A driver that really keeps claiming moves the count
// throughout, so the cap hands that back as the baseline and the window that
// follows still catches it.
func settledClaimAttempts(claimAttempts func() int, pollInterval time.Duration) int {
	deadline := time.Now().Add(10 * pollInterval)
	count := claimAttempts()
	stableSince := time.Now()
	for time.Since(stableSince) < 2*pollInterval && time.Now().Before(deadline) {
		time.Sleep(pollInterval / 4)
		if next := claimAttempts(); next != count {
			count, stableSince = next, time.Now()
		}
	}
	return count
}

// freeTCPPort returns a loopback port that was free a moment ago, for a listener
// whose address is fixed by configuration rather than chosen at bind time.
func freeTCPPort(t *testing.T) int {
	t.Helper()

	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok, "a TCP listener reports a TCP address")
	require.NoError(t, listener.Close())
	return addr.Port
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
