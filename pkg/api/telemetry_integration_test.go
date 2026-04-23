//go:build integration

package api

import (
	"database/sql"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/testutil"
)

// TestMetricsAfterRequests starts a real service with MySQL storage, hits
// several API endpoints, then scrapes /metrics and verifies that HTTP server
// metrics appear in the Prometheus text output.
func TestMetricsAfterRequests(t *testing.T) {
	ctx := t.Context()

	container, err := mysql.Run(ctx,
		"mysql:8.4",
		mysql.WithDatabase("schemabot_test"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
	)
	require.NoError(t, err, "failed to start mysql")
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	dsn, err := testutil.ContainerConnectionString(ctx, container, "parseTime=true")
	require.NoError(t, err, "failed to get connection string")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	require.NoError(t, EnsureSchema(dsn, logger), "failed to ensure schema")

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))

	storage := mysqlstore.New(db)
	serverConfig := &ServerConfig{
		TernDeployments: TernConfig{
			"default": {"staging": "tern-staging:9090"},
		},
	}
	svc := New(storage, serverConfig, nil, logger)
	defer utils.CloseAndLog(svc)

	// Set up telemetry and routes exactly as serve.go does.
	tel, err := SetupTelemetry(logger)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tel.Shutdown(t.Context())) })

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	mux.Handle("GET /metrics", tel.MetricsHandler)
	handler := otelhttp.NewHandler(mux, "schemabot")

	ts := httptest.NewServer(handler)
	defer ts.Close()

	// Hit several endpoints to generate HTTP metrics.
	endpoints := []struct {
		method string
		path   string
	}{
		{"GET", "/health"},
		{"GET", "/api/status"},
		{"GET", "/api/locks"},
		{"GET", "/api/settings"},
		{"GET", "/api/logs"},
	}

	client := ts.Client()
	for _, ep := range endpoints {
		req, err := http.NewRequestWithContext(ctx, ep.method, ts.URL+ep.path, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
	}

	// Scrape /metrics and verify HTTP server metrics appear.
	metricsReq, err := http.NewRequestWithContext(ctx, "GET", ts.URL+"/metrics", nil)
	require.NoError(t, err)
	resp, err := client.Do(metricsReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	metricsText := string(body)

	// otelhttp produces these standard metrics.
	assert.True(t, strings.Contains(metricsText, "http_server_request_duration"),
		"/metrics should contain http_server_request_duration")
	assert.True(t, strings.Contains(metricsText, "http_server_request_body_size"),
		"/metrics should contain http_server_request_body_size")
	assert.True(t, strings.Contains(metricsText, "http_server_response_body_size"),
		"/metrics should contain http_server_response_body_size")

	// The custom plans counter only appears after its first increment,
	// so we don't assert it here — it's tested in TestRecordPlanMetric.
}
