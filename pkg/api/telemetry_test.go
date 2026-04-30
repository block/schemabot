package api

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
)

func TestSetupTelemetry(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	tel, err := SetupTelemetry(logger)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tel.Shutdown(t.Context())) })

	require.NotNil(t, tel.MetricsHandler)
	assert.Nil(t, tel.tracerProvider, "tracerProvider should be nil without OTLP endpoint")
}

func TestSetupTelemetryWithOTLP(t *testing.T) {
	// Start a fake OTLP endpoint that records which paths receive data.
	var mu sync.Mutex
	receivedPaths := make(map[string]int)
	fakeOTLP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedPaths[r.URL.Path]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", fakeOTLP.URL)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	tel, err := SetupTelemetry(logger)
	require.NoError(t, err)

	require.NotNil(t, tel.MetricsHandler)
	assert.NotNil(t, tel.tracerProvider, "tracerProvider should be set with OTLP endpoint")

	// Record a metric so there's data to push.
	metrics.RecordPlan(t.Context(), "testdb", "staging", "success")

	// Create a trace span so there's trace data to push.
	tracer := otel.Tracer("test")
	_, span := tracer.Start(t.Context(), "test-span")
	span.End()

	// Shutdown flushes all pending data to the OTLP endpoint.
	require.NoError(t, tel.Shutdown(t.Context()))
	fakeOTLP.Close()

	mu.Lock()
	defer mu.Unlock()

	// OTLP HTTP exporter POSTs to /v1/metrics and /v1/traces.
	assert.Greater(t, receivedPaths["/v1/traces"], 0,
		"expected OTLP trace export to /v1/traces, got paths: %v", receivedPaths)
	assert.Greater(t, receivedPaths["/v1/metrics"], 0,
		"expected OTLP metric export to /v1/metrics, got paths: %v", receivedPaths)
}

func TestMetricsEndpoint(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	tel, err := SetupTelemetry(logger)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tel.Shutdown(t.Context())) })

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", tel.MetricsHandler)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/metrics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/plain")

	body := w.Body.String()
	assert.Contains(t, body, "target_info")
}

func TestRecordPlanMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordPlan(t.Context(), "testdb", "staging", "success")
	metrics.RecordPlan(t.Context(), "testdb", "staging", "success")
	metrics.RecordPlan(t.Context(), "testdb", "staging", "error")
	metrics.RecordPlan(t.Context(), "other", "production", "success")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	// Find the plans counter and assert with metricdatatest (the OTel-recommended pattern).
	var plansMetric metricdata.Metrics
	var found bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "schemabot.plans.total" {
				plansMetric = m
				found = true
			}
		}
	}
	require.True(t, found, "schemabot.plans.total metric not found")

	want := metricdata.Metrics{
		Name:        "schemabot.plans.total",
		Description: "Total number of plan operations",
		Unit:        "{plan}",
		Data: metricdata.Sum[int64]{
			IsMonotonic: true,
			Temporality: metricdata.CumulativeTemporality,
			DataPoints: []metricdata.DataPoint[int64]{
				{
					Value:      2,
					Attributes: attribute.NewSet(attribute.String("database", "testdb"), attribute.String("environment", "staging"), attribute.String("status", "success")),
				},
				{
					Value:      1,
					Attributes: attribute.NewSet(attribute.String("database", "testdb"), attribute.String("environment", "staging"), attribute.String("status", "error")),
				},
				{
					Value:      1,
					Attributes: attribute.NewSet(attribute.String("database", "other"), attribute.String("environment", "production"), attribute.String("status", "success")),
				},
			},
		},
	}
	metricdatatest.AssertEqual(t, want, plansMetric, metricdatatest.IgnoreTimestamp())
}

func TestOtelHTTPMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	svc := newTestService()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	handler := otelhttp.NewHandler(mux, "schemabot")

	// Hit /health — the one route guaranteed to work with mock storage.
	req := httptest.NewRequestWithContext(t.Context(), "GET", "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	// Verify otelhttp produced the standard HTTP server metrics.
	metricNames := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			metricNames[m.Name] = true
		}
	}
	assert.True(t, metricNames["http.server.request.duration"], "expected http.server.request.duration metric")
	assert.True(t, metricNames["http.server.request.body.size"], "expected http.server.request.body.size metric")
	assert.True(t, metricNames["http.server.response.body.size"], "expected http.server.response.body.size metric")

	// Verify the duration histogram has data points with expected attributes.
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "http.server.request.duration" {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(hist.DataPoints), 1, "expected at least one duration data point")

			// Verify data points have standard HTTP attributes.
			for _, dp := range hist.DataPoints {
				_, hasMethod := dp.Attributes.Value(attribute.Key("http.request.method"))
				assert.True(t, hasMethod, "expected http.request.method attribute on duration data point")
				_, hasStatus := dp.Attributes.Value(attribute.Key("http.response.status_code"))
				assert.True(t, hasStatus, "expected http.response.status_code attribute on duration data point")
			}
		}
	}
}

// collectMetricNames returns all metric names from the reader.
func collectMetricNames(t *testing.T, reader *sdkmetric.ManualReader) map[string]bool {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	names := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}
	return names
}

func TestRecordApplyMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordApply(t.Context(), "mydb", "staging", "success")
	metrics.RecordApply(t.Context(), "mydb", "staging", "error")
	metrics.RecordApplyDuration(t.Context(), 2*time.Second, "mydb", "staging", "success")

	names := collectMetricNames(t, reader)
	assert.True(t, names["schemabot.applies.total"], "expected schemabot.applies.total")
	assert.True(t, names["schemabot.apply.duration_seconds"], "expected schemabot.apply.duration_seconds")
}

func TestRecordPlanDurationMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordPlanDuration(t.Context(), 500*time.Millisecond, "mydb", "staging", "success")

	names := collectMetricNames(t, reader)
	assert.True(t, names["schemabot.plan.duration_seconds"], "expected schemabot.plan.duration_seconds")
}

func TestRecordWebhookEventMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordWebhookEvent(t.Context(), "issue_comment", "created", "org/repo", "processed")
	metrics.RecordWebhookEvent(t.Context(), "pull_request", "opened", "org/repo", "processed")
	metrics.RecordWebhookEvent(t.Context(), "pull_request", "closed", "org/repo", "processed")
	metrics.RecordWebhookEvent(t.Context(), "ping", "", "", "ignored")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var found bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "schemabot.webhook.events_total" {
				found = true
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				assert.Len(t, sum.DataPoints, 4, "expected 4 data points (one per event_type/action/status combo)")
			}
		}
	}
	assert.True(t, found, "schemabot.webhook.events_total metric not found")
}
