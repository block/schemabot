package serve

import (
	"bytes"
	"database/sql"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// A collector outage must not fail graceful shutdown: when the final telemetry
// flush is rejected, Close logs a warning and still returns nil, so embedders
// that treat a Close error as fatal do not turn a routine SIGTERM rotation
// into a reported crash.
func TestServerCloseSucceedsWhenTelemetryFlushFails(t *testing.T) {
	rejectingCollector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
	}))
	t.Cleanup(rejectingCollector.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", rejectingCollector.URL)

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	telemetry, err := api.SetupTelemetry(logger)
	require.NoError(t, err)

	// A lazily-opened handle: never connected, so svc.Close can close it
	// without a reachable database.
	db, err := sql.Open("block-mysql", "schemabot@tcp(127.0.0.1:1)/schemabot")
	require.NoError(t, err)
	// srv.Close (via svc.Close) owns the handle; this cleanup only prevents a
	// leak when the test fails before Close runs.
	serverClosed := false
	t.Cleanup(func() {
		if !serverClosed {
			utils.CloseAndLog(db)
		}
	})
	svc := api.New(mysqlstore.New(db), &api.ServerConfig{}, nil, logger)

	srv := &Server{cfg: &api.ServerConfig{}, svc: svc, logger: logger, telemetry: telemetry}

	// Record a metric so the shutdown flush has data the collector rejects.
	metrics.RecordPlan(t.Context(), "org/repo", "testdb", "pie", "staging", "success")

	require.NoError(t, srv.Close(), "a failed telemetry flush must not fail Close")
	serverClosed = true
	assert.Contains(t, logs.String(), "telemetry shutdown failed",
		"the dropped flush must be visible to operators as a warning")
}
