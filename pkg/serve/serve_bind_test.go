package serve

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
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

// heldPorts are loopback ports a test configures on a Server. Each one stays
// bound from the moment the test chooses it until the server takes it over, so
// no other process can claim the port in between. A port the test released and
// the server bound again by number could be taken by a test running in another
// package, and the run would then fail on a listener the test never meant to
// occupy.
//
// The listeners map is filled by holdPorts and never changes afterwards, so it
// is read without a lock; each listener guards its own state.
type heldPorts struct {
	listeners map[string]*heldListener
}

// heldListener records whether the server took the listener and whether it was
// closed afterwards. mu guards both taken and closed.
type heldListener struct {
	net.Listener
	mu     sync.Mutex
	taken  bool
	closed bool
}

func (l *heldListener) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	if err := l.Listener.Close(); err != nil {
		return fmt.Errorf("close held listener on %s: %w", l.Addr(), err)
	}
	return nil
}

// take marks the listener as handed to the server and reports whether it was
// still free to hand over.
func (l *heldListener) take() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.taken {
		return false
	}
	l.taken = true
	return true
}

// state reports whether the server took the listener and whether it was
// closed.
func (l *heldListener) state() (taken, closed bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.taken, l.closed
}

// holdPorts binds n loopback ports and holds them for the test. Listeners the
// server never takes are closed when the test ends; the ones it takes are its
// own to close.
func holdPorts(t *testing.T, n int) (*heldPorts, []int) {
	t.Helper()

	held := &heldPorts{listeners: make(map[string]*heldListener, n)}
	ports := make([]int, 0, n)
	var config net.ListenConfig
	for range n {
		listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr, ok := listener.Addr().(*net.TCPAddr)
		require.True(t, ok, "a TCP listener reports a TCP address")
		held.listeners[strconv.Itoa(addr.Port)] = &heldListener{Listener: listener}
		ports = append(ports, addr.Port)
	}
	t.Cleanup(func() {
		for _, listener := range held.listeners {
			if taken, _ := listener.state(); !taken {
				assert.NoError(t, listener.Close())
			}
		}
	})
	return held, ports
}

// listen is a Server.listen that hands over the held listener for a held port
// and binds any other port for real, so a port another listener occupies still
// fails the way it does in production.
func (h *heldPorts) listen(ctx context.Context, name, port string) (net.Listener, error) {
	if listener, ok := h.listeners[port]; ok && listener.take() {
		return listener, nil
	}
	return listenForServe(ctx, name, port)
}

// requireReleased checks what a failed run did with the listener for port.
// A port bound before the failing one must have been taken and then closed,
// so the failed run holds no socket; a port after it must never have been
// taken, since binding stops at the first failure.
func (h *heldPorts) requireReleased(t *testing.T, port int, name string, boundBeforeFailure bool) {
	t.Helper()

	listener, ok := h.listeners[strconv.Itoa(port)]
	require.True(t, ok, "the %s port is one the test holds", name)
	taken, closed := listener.state()
	if !boundBeforeFailure {
		assert.False(t, taken, "the run must not bind the %s port after an earlier bind failed", name)
		return
	}
	assert.True(t, taken, "the run binds the %s port before the one that is taken", name)
	assert.True(t, closed, "the run closes the %s listener it bound once it has failed", name)
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
		// boundBefore names the listeners the run binds before the taken one,
		// in the order gRPC, HTTP, metrics.
		boundBefore []string
	}{
		{name: "grpc port in use", taken: "grpc"},
		{name: "http port in use", taken: "http", boundBefore: []string{"grpc"}},
		{name: "metrics port in use", taken: "metrics", boundBefore: []string{"grpc", "http"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			var mu sync.Mutex
			logger := slog.New(slog.NewTextHandler(&lockedWriter{mu: &mu, w: &logs}, nil))

			held, free := holdPorts(t, 3)
			ports := map[string]int{"grpc": free[0], "http": free[1], "metrics": free[2]}
			taken := occupiedPort(t)
			ports[tt.taken] = taken

			var dispatchStarted bool
			srv, serverClosed := newBindTestServer(t, logger, ports["metrics"], http.NotFoundHandler(), func() { dispatchStarted = true })
			srv.listen = held.listen

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
					held.requireReleased(t, port, name, slices.Contains(tt.boundBefore, name))
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
	held, ports := holdPorts(t, 2)
	httpPort, metricsPort := ports[0], ports[1]
	started := make(chan struct{}, 1)
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := io.WriteString(w, "metrics-body"); err != nil {
			t.Errorf("write metrics body: %v", err)
		}
	})
	srv, serverClosed := newBindTestServer(t, logger, metricsPort, metrics, func() { started <- struct{}{} })
	srv.listen = held.listen

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
