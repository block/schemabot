package serve

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"net"
	"net/http"
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

// A second `schemabot serve` starts while another process already holds its HTTP
// or metrics port. The run must fail on the bind and name the port, before the
// background work starts: a process that is about to exit must not first claim
// an apply, a webhook delivery or a lease, because a claim it has just taken is
// work a peer then has to wait for or take back.
//
// Starting the durable webhook dispatch and the operator both happen in
// Server.Start, so a dispatch that was never started shows that no claimer ran.
func TestServeFailsOnTakenPortBeforeStartingBackgroundWork(t *testing.T) {
	tests := []struct {
		name string
		// taken says which of the two listeners finds its port occupied.
		taken string
		// wantServer is the name the bind error gives the listener that failed.
		wantServer string
	}{
		{name: "http port in use", taken: "http", wantServer: "http"},
		{name: "metrics port in use", taken: "metrics", wantServer: "metrics"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			var mu sync.Mutex
			logger := slog.New(slog.NewTextHandler(&lockedWriter{mu: &mu, w: &logs}, nil))

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

			taken := occupiedPort(t)
			httpPort, metricsPort := strconv.Itoa(taken), freeTCPPort(t)
			if tt.taken == "metrics" {
				httpPort, metricsPort = strconv.Itoa(freeTCPPort(t)), taken
			}

			dispatchStarted := false
			srv := &Server{
				cfg:       &api.ServerConfig{MetricsPort: metricsPort},
				svc:       api.New(mysqlstore.New(db), &api.ServerConfig{}, nil, logger),
				logger:    logger,
				authz:     authorizer,
				telemetry: &api.Telemetry{MetricsHandler: http.NotFoundHandler()},
				webhook: webhookRuntime{
					handler:                     http.NotFoundHandler(),
					startDurableWebhookDispatch: func(context.Context) { dispatchStarted = true },
				},
			}

			runCtx, signalled, stopWatching := watchForShutdownSignal(t.Context())
			defer stopWatching()

			served := make(chan error, 1)
			go func() { served <- serveUntilShutdown(runCtx, srv, signalled, httpPort, "") }()

			var serveErr error
			select {
			case serveErr = <-served:
				serverClosed = true
			case <-time.After(runShutdownDeadline):
				t.Fatalf("the server did not return within %s of a taken port", runShutdownDeadline)
			}

			require.Error(t, serveErr)
			require.ErrorIs(t, serveErr, syscall.EADDRINUSE)
			assert.Contains(t, serveErr.Error(), "listen for "+tt.wantServer+" server on port "+strconv.Itoa(taken))

			assert.False(t, dispatchStarted, "the background work must not start behind a port that is taken")
			mu.Lock()
			output := logs.String()
			mu.Unlock()
			assert.NotContains(t, output, "operator started", "the operator must not start behind a port that is taken")
			assert.NotContains(t, output, "failed to claim", "no driver may attempt a claim behind a port that is taken")
		})
	}
}
