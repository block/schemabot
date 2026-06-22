package serve

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// stubTernClient satisfies tern.Client for tests that only need a non-nil client
// to register; its methods are never invoked (no RPC is made).
type stubTernClient struct{ tern.Client }

// RegisterGRPC registers the Tern service on an embedder-supplied gRPC server,
// reusing the prebuilt data-plane client. This is the gRPC half of the embedding
// seam: a data plane attaches SchemaBot to its own server rather than letting
// Run own the listener.
func TestServerRegisterGRPCRegistersTernService(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	srv := &Server{
		dataPlaneClient: stubTernClient{},
		svc:             api.New(mysqlstore.New(nil), &api.ServerConfig{}, nil, logger),
		logger:          logger,
	}

	gs := grpc.NewServer()
	require.NoError(t, srv.RegisterGRPC(t.Context(), gs))

	_, ok := gs.GetServiceInfo()["tern.v1.Tern"]
	assert.True(t, ok, "RegisterGRPC must register the Tern service on the embedder's gRPC server")
}

// Handler returns the SchemaBot HTTP handler an embedder mounts on its own mux.
// It exposes /metrics through the auth middleware, so a request reaches the
// metrics endpoint without Run owning the HTTP server.
func TestServerHandlerServesMetrics(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := &api.ServerConfig{}
	svc := api.New(mysqlstore.New(nil), cfg, nil, logger)

	webhook, err := buildWebhookRuntime(cfg, svc, logger)
	require.NoError(t, err)
	authz, err := buildAuthorizer(t.Context(), cfg.Auth, nil, logger)
	require.NoError(t, err)
	telemetry, err := api.SetupTelemetry(logger)
	require.NoError(t, err)
	// SetupTelemetry installs global OTel providers; shut them down so state
	// does not leak into later tests.
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_ = telemetry.Shutdown(shutdownCtx)
	})

	srv := &Server{cfg: cfg, svc: svc, logger: logger, webhook: webhook, telemetry: telemetry, authz: authz}

	handler := srv.Handler()
	require.NotNil(t, handler)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, rec.Code, "the handler serves /metrics through the auth middleware")
}
