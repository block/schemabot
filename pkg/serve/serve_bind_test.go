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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// occupiedPort binds a wildcard TCP port and holds it for the test, standing in
// for another process that already listens there.
func occupiedPort(t *testing.T) int {
	t.Helper()

	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", ":0")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, listener.Close()) })

	addr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok, "a TCP listener reports a TCP address")
	return addr.Port
}

// newBindTestServer builds a Server whose storage handle points at a closed
// port, so nothing reaches a database. The handle is opened lazily, and
// Server.Close closes it once the run returns; serverClosed tells the cleanup
// whether the run got that far.
func newBindTestServer(t *testing.T, logger *slog.Logger, metricsPort int, metrics http.Handler, dispatchStarted func()) (*Server, *bool) {
	t.Helper()

	db, err := sql.Open("block-mysql", "schemabot@tcp(127.0.0.1:1)/schemabot")
	require.NoError(t, err)
	serverClosed := false
	t.Cleanup(func() {
		if !serverClosed {
			assert.NoError(t, db.Close())
		}
	})
	authorizer, err := auth.NewLocalAuthorizer(strings.Repeat("a", 64), logger)
	require.NoError(t, err)

	store := mysqlstore.New(db)
	return &Server{
		cfg:             &api.ServerConfig{MetricsPort: metricsPort},
		svc:             api.New(store, &api.ServerConfig{}, nil, logger),
		storage:         store,
		dataPlaneClient: stubTernClient{},
		logger:          logger,
		authz:           authorizer,
		telemetry:       &api.Telemetry{MetricsHandler: metrics},
		webhook: webhookRuntime{
			handler:                     http.NotFoundHandler(),
			startDurableWebhookDispatch: func(context.Context) { dispatchStarted() },
		},
	}, &serverClosed
}

// requirePortFree fails the test unless port can be bound again, which shows
// that the run that held it has released it.
func requirePortFree(t *testing.T, port int, name string) {
	t.Helper()

	var config net.ListenConfig
	rebound, err := config.Listen(t.Context(), "tcp", ":"+strconv.Itoa(port))
	require.NoError(t, err, "the %s port is free again once the run has failed", name)
	require.NoError(t, rebound.Close())
}

// A second `schemabot serve` starts while another process already holds its
// gRPC, HTTP or metrics port. The run must fail on the bind and name the port
// before anything serves or the background work starts: a process that is about
// to exit must not first accept an RPC or claim an apply, a webhook delivery or
// a lease, because that is work a peer then has to wait for or take back. The
// ports it did bind are released, so the failed run leaves no socket behind.
//
// Starting the durable webhook dispatch and the operator both happen in
// Server.Start, so a dispatch that was never started shows that no claimer ran.
func TestServeFailsOnTakenPortBeforeStartingBackgroundWork(t *testing.T) {
	tests := []struct {
		name string
		// taken says which of the three listeners finds its port occupied.
		taken string
	}{
		{name: "grpc port in use", taken: "grpc"},
		{name: "http port in use", taken: "http"},
		{name: "metrics port in use", taken: "metrics"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			var mu sync.Mutex
			logger := slog.New(slog.NewTextHandler(&lockedWriter{mu: &mu, w: &logs}, nil))

			ports := map[string]int{"grpc": freeTCPPort(t), "http": freeTCPPort(t), "metrics": freeTCPPort(t)}
			taken := occupiedPort(t)
			ports[tt.taken] = taken

			var dispatchStarted bool
			srv, serverClosed := newBindTestServer(t, logger, ports["metrics"], http.NotFoundHandler(), func() { dispatchStarted = true })

			runCtx, signalled, stopWatching := watchForShutdownSignal(t.Context())
			defer stopWatching()

			served := make(chan error, 1)
			go func() {
				served <- serveUntilShutdown(runCtx, srv, signalled, strconv.Itoa(ports["http"]), strconv.Itoa(ports["grpc"]))
			}()

			var serveErr error
			select {
			case serveErr = <-served:
				*serverClosed = true
			case <-time.After(runShutdownDeadline):
				require.FailNowf(t, "run did not fail", "the server did not return within %s of a taken port", runShutdownDeadline)
			}

			require.Error(t, serveErr)
			require.ErrorIs(t, serveErr, syscall.EADDRINUSE)
			assert.Contains(t, serveErr.Error(), "listen for "+tt.taken+" server on port "+strconv.Itoa(taken))

			for name, port := range ports {
				if name != tt.taken {
					requirePortFree(t, port, name)
				}
			}

			assert.False(t, dispatchStarted, "the background work must not start behind a port that is taken")
			mu.Lock()
			output := logs.String()
			mu.Unlock()
			assert.NotContains(t, output, "starting gRPC server", "nothing may serve behind a port that is taken")
			assert.NotContains(t, output, "operator started", "the operator must not start behind a port that is taken")
			assert.NotContains(t, output, "failed to claim", "no driver may attempt a claim behind a port that is taken")
		})
	}
}

// The listeners are bound before Start, so each must still end up serving its
// own handler: the API on the HTTP port and the metrics handler on the metrics
// port, and not the other way round.
func TestServeServesEachHandlerOnItsOwnBoundPort(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	httpPort, metricsPort := freeTCPPort(t), freeTCPPort(t)
	started := make(chan struct{}, 1)
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := io.WriteString(w, "metrics-body"); err != nil {
			t.Errorf("write metrics body: %v", err)
		}
	})
	srv, serverClosed := newBindTestServer(t, logger, metricsPort, metrics, func() { started <- struct{}{} })

	runCtx, signalled, stopWatching := watchForShutdownSignal(t.Context())
	defer stopWatching()
	served := make(chan error, 1)
	go func() { served <- serveUntilShutdown(runCtx, srv, signalled, strconv.Itoa(httpPort), "") }()

	select {
	case <-started:
	case <-time.After(runShutdownDeadline):
		require.FailNowf(t, "run did not start", "the server did not start within %s", runShutdownDeadline)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	get := func(port int, path string) (int, string) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://127.0.0.1:"+strconv.Itoa(port)+path, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(body)
	}

	code, _ := get(httpPort, "/livez")
	assert.Equal(t, http.StatusUnauthorized, code, "the API, behind its authorizer, answers on the HTTP port")
	code, body := get(metricsPort, "/metrics")
	assert.Equal(t, http.StatusOK, code, "metrics answer on the metrics port")
	assert.Equal(t, "metrics-body", body)
	code, _ = get(metricsPort, "/livez")
	assert.Equal(t, http.StatusNotFound, code, "the API is not served on the metrics port")

	self, err := os.FindProcess(os.Getpid())
	require.NoError(t, err)
	require.NoError(t, self.Signal(syscall.SIGTERM))
	select {
	case err := <-served:
		*serverClosed = true
		require.NoError(t, err)
	case <-time.After(runShutdownDeadline):
		require.FailNowf(t, "run did not stop", "the server did not shut down within %s", runShutdownDeadline)
	}
}
