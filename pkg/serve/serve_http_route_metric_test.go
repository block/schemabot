package serve

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// An operator reading the HTTP request metrics needs to know which endpoint a
// burst of errors hit. Every request through the server handler is labeled
// with the route it matched: one the auth middleware let through to its
// handler, one the middleware rejected, and the unauthenticated webhook alike.
// A request that matches no route carries no route label. Mounted under an
// embedder's mux, the handler labels its own metrics without changing the
// pattern the embedder's request carries.
func TestServerHandlerLabelsHTTPMetricsWithRoute(t *testing.T) {
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
	// The OTel HTTP handler takes its meter from the global provider when it is
	// built, so the reader is installed before Handler is called.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		assert.NoError(t, mp.Shutdown(shutdownCtx))
	})

	srv := &Server{cfg: cfg, svc: svc, logger: logger, webhook: webhook, authz: authz}
	handler := srv.Handler()

	serveVia := func(h http.Handler, method, path, remoteAddr, groups string) int {
		req := httptest.NewRequestWithContext(t.Context(), method, path, nil)
		req.RemoteAddr = remoteAddr
		if groups != "" {
			req.Header.Set("X-Forwarded-User", "bob")
			req.Header.Set("X-Forwarded-Capabilities", groups)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	serve := func(method, path, remoteAddr, groups string) int {
		return serveVia(handler, method, path, remoteAddr, groups)
	}

	// Authorized, so it reaches the plan handler, which rejects the empty body.
	require.Equal(t, http.StatusBadRequest, serve(http.MethodPost, "/api/plan", "192.0.2.1:1234", "owners"))
	// Rejected by the auth middleware before any handler runs.
	require.Equal(t, http.StatusUnauthorized, serve(http.MethodGet, "/api/status", "203.0.113.5:1234", ""))
	// A path with a wildcard is labeled with its pattern, not the request path.
	require.Equal(t, http.StatusUnauthorized, serve(http.MethodGet, "/api/history/orders", "203.0.113.5:1234", ""))
	// Bypasses the auth middleware; with no GitHub App configured the webhook
	// answers 503.
	require.Equal(t, http.StatusServiceUnavailable, serve(http.MethodPost, "/webhook", "203.0.113.5:1234", ""))
	// Matches no route.
	require.Equal(t, http.StatusNotFound, serve(http.MethodGet, "/api/nope", "192.0.2.1:1234", "owners"))

	// Mounted under an embedder's mux, a routed path is labeled with this
	// handler's pattern and an unrouted one stays unlabeled rather than
	// inheriting the mount pattern. The embedder's request keeps its own
	// pattern either way.
	var embedderPatterns []string
	outer := http.NewServeMux()
	outer.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
		embedderPatterns = append(embedderPatterns, r.Pattern)
	})
	require.Equal(t, http.StatusBadRequest, serveVia(outer, http.MethodPost, "/api/plan", "192.0.2.1:1234", "owners"))
	require.Equal(t, http.StatusNotFound, serveVia(outer, http.MethodGet, "/api/missing", "192.0.2.1:1234", "owners"))
	assert.Equal(t, []string{"/", "/"}, embedderPatterns)

	assert.Equal(t, map[string]int64{
		"POST 400 /api/plan":              2,
		"GET 401 /api/status":             1,
		"GET 401 /api/history/{database}": 1,
		"POST 503 /webhook":               1,
		"GET 404 ":                        2,
	}, requestCountsByRoute(t, reader))
}

// requestCountsByRoute sums the recorded HTTP server requests, keyed by
// "<method> <status> <route>" with an empty route when none was recorded.
func requestCountsByRoute(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	counts := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "http.server.request.duration" {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "http.server.request.duration is a float64 histogram, got %T", m.Data)
			for _, dp := range hist.DataPoints {
				method, _ := dp.Attributes.Value(attribute.Key("http.request.method"))
				code, _ := dp.Attributes.Value(attribute.Key("http.response.status_code"))
				route, _ := dp.Attributes.Value(attribute.Key("http.route"))
				key := fmt.Sprintf("%s %d %s", method.AsString(), code.AsInt64(), route.AsString())
				counts[key] += int64(dp.Count)
			}
		}
	}
	return counts
}
