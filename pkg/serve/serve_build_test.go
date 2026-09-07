package serve

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/testctx"
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
	authz, err := buildServerAuthorizer(t.Context(), cfg, logger)
	require.NoError(t, err)
	telemetry, err := api.SetupTelemetry(logger)
	require.NoError(t, err)
	// SetupTelemetry installs global OTel providers; shut them down so state
	// does not leak into later tests.
	t.Cleanup(func() {
		shutdownCtx, cancel := testctx.Cleanup(t, 5*time.Second)
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
	authz, err := buildServerAuthorizer(t.Context(), cfg, logger)
	require.NoError(t, err)
	telemetry, err := api.SetupTelemetry(logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdownCtx, cancel := testctx.Cleanup(t, 5*time.Second)
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

// Configuring the service-caller lane (trusted_gateway_spiffe +
// read_service_spiffe + caller_spiffe_header) wires all three settings through
// buildAuthorizer into the real HTTP handler: a listed caller arriving through
// a listed gateway under the configured header gets read access, and stays
// read-only. This proves the config-to-middleware plumbing end to end, so a
// dropped or swapped field in the authorizer construction fails behaviorally.
func TestForwardAuthServiceCallerLaneThroughServerHandler(t *testing.T) {
	const (
		gatewaySVID  = "spiffe://example.org/ns/service-ingress/sa/gateway"
		callerSVID   = "spiffe://example.org/ns/reporting/sa/reporting"
		callerHeader = "X-Custom-Caller-Id"
	)
	logger := slog.New(slog.DiscardHandler)
	cfg := &api.ServerConfig{
		Auth: api.AuthConfig{
			Type: "forward_auth",
			ForwardAuth: api.ForwardAuthSettings{
				TrustedProxySPIFFE:   []string{"spiffe://example.org/ns/ingress/sa/proxy"},
				WriteGroups:          []string{"owners"},
				TrustedGatewaySPIFFE: []string{gatewaySVID},
				ReadServiceSPIFFE:    []string{callerSVID},
				CallerSPIFFEHeader:   callerHeader,
			},
		},
	}
	svc := api.New(mysqlstore.New(nil), cfg, nil, logger)
	webhook, err := buildWebhookRuntime(cfg, svc, logger)
	require.NoError(t, err)
	authz, err := buildServerAuthorizer(t.Context(), cfg, logger)
	require.NoError(t, err)
	telemetry, err := api.SetupTelemetry(logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdownCtx, cancel := testctx.Cleanup(t, 5*time.Second)
		defer cancel()
		_ = telemetry.Shutdown(shutdownCtx)
	})

	srv := &Server{cfg: cfg, svc: svc, logger: logger, webhook: webhook, telemetry: telemetry, authz: authz}
	handler := srv.Handler()

	gatewayRequest := func(t *testing.T, method, path string) *http.Request {
		t.Helper()
		req := httptest.NewRequestWithContext(t.Context(), method, path, nil)
		req.RemoteAddr = "203.0.113.5:1234"
		req.Header.Set("X-Forwarded-Client-Cert", "URI="+gatewaySVID)
		req.Header.Set(callerHeader, callerSVID)
		return req
	}

	t.Run("listed service caller passes the auth gate on a read", func(t *testing.T) {
		// The read is authorized — not rejected at auth — so it reaches the
		// pull handler, which rejects this empty body with 400.
		req := gatewayRequest(t, http.MethodPost, "/api/pull")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("service caller is denied a write with 403", func(t *testing.T) {
		req := gatewayRequest(t, http.MethodPost, "/api/plan")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "read-only")
	})

	t.Run("caller under the default header name is rejected when a custom header is configured", func(t *testing.T) {
		req := gatewayRequest(t, http.MethodGet, "/api/status")
		req.Header.Del(callerHeader)
		req.Header.Set("X-Forwarded-Caller-Spiffe-Id", callerSVID)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})
}

// Per-database operator scoping only works if the server wires the
// operator-group union from database config into the forward-auth middleware.
// This drives the real server handler with a caller who is in an operator
// group but no write group: the middleware must admit them to the write tier
// (the union wiring), the handler must allow their granted database and deny
// another — so a build that drops the union, or a handler that skips the
// scope check, fails behaviorally.
func TestForwardAuthOperatorScopingThroughServerHandler(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := &api.ServerConfig{
		Auth: api.AuthConfig{
			Type: "forward_auth",
			ForwardAuth: api.ForwardAuthSettings{
				// httptest's default RemoteAddr (192.0.2.1) falls in this range,
				// so it is the trusted proxy; other sources are untrusted.
				TrustedProxyCIDRs:    []string{"192.0.2.0/24"},
				GroupsHeader:         "X-Forwarded-Capabilities",
				WriteGroups:          []string{"owners"},
				OperatorEnvironments: []string{"staging"},
			},
		},
		Databases: map[string]api.DatabaseConfig{
			"payments": {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/payments"},
				},
				OperatorGroups: []string{"payments-team"},
			},
			"orders": {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/orders"},
				},
			},
		},
	}
	svc := api.New(mysqlstore.New(nil), cfg, nil, logger)
	webhook, err := buildWebhookRuntime(cfg, svc, logger)
	require.NoError(t, err)
	authz, err := buildServerAuthorizer(t.Context(), cfg, logger)
	require.NoError(t, err)
	telemetry, err := api.SetupTelemetry(logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdownCtx, cancel := testctx.Cleanup(t, 5*time.Second)
		defer cancel()
		_ = telemetry.Shutdown(shutdownCtx)
	})

	srv := &Server{cfg: cfg, svc: svc, logger: logger, webhook: webhook, telemetry: telemetry, authz: authz}
	handler := srv.Handler()

	operatorRequest := func(t *testing.T, method, path, body string) *http.Request {
		t.Helper()
		req := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
		req.RemoteAddr = "192.0.2.1:1234"
		req.Header.Set("X-Forwarded-User", "bob")
		req.Header.Set("X-Forwarded-Capabilities", "users,payments-team")
		return req
	}

	planBody := func(database string) string {
		return `{"database":"` + database + `","environment":"staging","type":"mysql","schema_files":{"default":{"files":{"t.sql":"CREATE TABLE t (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"}}}}`
	}

	t.Run("operator is admitted to the write tier and allowed on their granted database", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, operatorRequest(t, http.MethodPost, "/api/plan", planBody("payments")))
		// Both authorization layers pass — middleware admission via the
		// operator-group union, handler scope via the payments grant — so the
		// request reaches plan execution, which fails downstream in this
		// harness (no engine wired). Any 401/403 means an auth layer denied.
		assert.NotEqual(t, http.StatusUnauthorized, rec.Code, "middleware must authenticate the operator: %s", rec.Body.String())
		assert.NotEqual(t, http.StatusForbidden, rec.Code, "both authorization layers must allow the granted database: %s", rec.Body.String())
	})

	t.Run("the same operator is denied another database at the handler", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, operatorRequest(t, http.MethodPost, "/api/plan", planBody("orders")))
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "owners", "the denial names the write groups that could act instead")
	})

	t.Run("operator gets the read tier deployment-wide", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, operatorRequest(t, http.MethodGet, "/api/databases", ""))
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}
