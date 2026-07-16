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

// Prometheus metrics live on their own handler (served by Run on the dedicated
// metrics listener), not on the API handler an embedder mounts on its own mux.
func TestServerMetricsHandlerSeparateFromAPIHandler(t *testing.T) {
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
	assert.Equal(t, http.StatusNotFound, rec.Code, "the API handler does not serve /metrics")

	metricsHandler := srv.MetricsHandler()
	require.NotNil(t, metricsHandler)

	rec = httptest.NewRecorder()
	metricsHandler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, rec.Code, "the metrics handler serves /metrics")
}

// Enabling auth.type: forward_auth wires the forward-auth authorizer into the
// real HTTP handler so the API enforces the read/write tiers per request: an
// unauthenticated caller is rejected, an authenticated read-tier caller cannot
// write, and an authenticated write-tier caller passes the auth gate. This
// exercises the full config → buildAuthorizer → middleware → tier path, not just
// the authorizer in isolation.
func TestForwardAuthEnforcedThroughServerHandler(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := &api.ServerConfig{
		Auth: api.AuthConfig{
			Type: "forward_auth",
			ForwardAuth: api.ForwardAuthSettings{
				// httptest's default RemoteAddr (192.0.2.1) falls in this range,
				// so it is the trusted proxy; other sources are untrusted.
				TrustedProxyCIDRs: []string{"192.0.2.0/24"},
				GroupsHeader:      "X-Forwarded-Capabilities",
				WriteGroups:       []string{"owners"},
			},
		},
	}
	svc := api.New(mysqlstore.New(nil), cfg, nil, logger)
	webhook, err := buildWebhookRuntime(cfg, svc, logger)
	require.NoError(t, err)
	authz, err := buildAuthorizer(t.Context(), cfg.Auth, nil, logger)
	require.NoError(t, err)
	telemetry, err := api.SetupTelemetry(logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_ = telemetry.Shutdown(shutdownCtx)
	})

	srv := &Server{cfg: cfg, svc: svc, logger: logger, webhook: webhook, telemetry: telemetry, authz: authz}
	handler := srv.Handler()

	const trusted = "192.0.2.1:1234"
	const untrusted = "203.0.113.5:1234"

	t.Run("unauthenticated request is rejected with 401", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
		req.RemoteAddr = untrusted
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("authenticated read-tier caller is denied a write with 403", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", nil)
		req.RemoteAddr = trusted
		req.Header.Set("X-Forwarded-User", "alice")
		req.Header.Set("X-Forwarded-Capabilities", "readers")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("authenticated write-tier caller is authorized and reaches the write handler", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", nil)
		req.RemoteAddr = trusted
		req.Header.Set("X-Forwarded-User", "bob")
		req.Header.Set("X-Forwarded-Capabilities", "owners")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		// The write is authorized — not rejected at auth — so it reaches the plan
		// handler, which rejects this empty body with 400. The contrast with the
		// read-tier caller above (403, blocked before the handler) is the proof
		// that a write-group member is permitted to perform the write.
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}
