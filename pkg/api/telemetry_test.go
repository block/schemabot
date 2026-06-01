package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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
	metrics.RecordPlan(t.Context(), "testrepo", "testdb", "pie", "staging", "success")

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

	metrics.RecordPlan(t.Context(), "testrepo", "testdb", "pie", "staging", "success")
	metrics.RecordPlan(t.Context(), "testrepo", "testdb", "pie", "staging", "success")
	metrics.RecordPlan(t.Context(), "testrepo", "testdb", "pie", "staging", "error")
	metrics.RecordPlan(t.Context(), "testrepo", "other", "bakery", "production", "success")

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
					Attributes: attribute.NewSet(attribute.String("repository", "testrepo"), attribute.String("database", "testdb"), attribute.String("deployment", "pie"), attribute.String("environment", "staging"), attribute.String("status", "success")),
				},
				{
					Value:      1,
					Attributes: attribute.NewSet(attribute.String("repository", "testrepo"), attribute.String("database", "testdb"), attribute.String("deployment", "pie"), attribute.String("environment", "staging"), attribute.String("status", "error")),
				},
				{
					Value:      1,
					Attributes: attribute.NewSet(attribute.String("repository", "testrepo"), attribute.String("database", "other"), attribute.String("deployment", "bakery"), attribute.String("environment", "production"), attribute.String("status", "success")),
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
	handler := otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		labeler, _ := otelhttp.LabelerFromContext(r.Context())
		labeler.Add(metrics.EnvironmentAttribute(""))
		mux.ServeHTTP(w, r)
	}), "schemabot")

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

	// Verify HTTP server metrics have expected attributes.
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if !strings.HasPrefix(m.Name, "http.server.") {
				continue
			}
			assertMetricDataPointsHaveEnvironment(t, m)
			if m.Name != "http.server.request.duration" {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(hist.DataPoints), 1, "expected at least one data point for %s", m.Name)

			for _, dp := range hist.DataPoints {
				_, hasMethod := dp.Attributes.Value(attribute.Key("http.request.method"))
				assert.True(t, hasMethod, "expected http.request.method attribute on %s data point", m.Name)
				_, hasStatus := dp.Attributes.Value(attribute.Key("http.response.status_code"))
				assert.True(t, hasStatus, "expected http.response.status_code attribute on %s data point", m.Name)
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

func TestSchemaBotMetricsIncludeEnvironmentAttribute(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordPlan(t.Context(), "org/repo", "mydb", "pie", "staging", "success")
	metrics.RecordPlanDuration(t.Context(), time.Second, "org/repo", "mydb", "pie", "staging", "success")
	metrics.RecordApply(t.Context(), "org/repo", "mydb", "pie", "staging", "success")
	metrics.RecordApplyDuration(t.Context(), time.Second, "org/repo", "mydb", "pie", "staging", "success")
	metrics.RecordSchemaFreshnessRejected(t.Context(), "apply", "staging")
	metrics.RecordStalePlanRejected(t.Context(), "staging")
	metrics.RecordSourcePolicyBlock(t.Context(), "plan", "mydb", "staging", "unauthorized_repo")
	metrics.RecordPRCommandActorAuthorization(t.Context(), "apply", "mydb", "staging", "org/repo", "allowed", "allowed_admin_user")
	metrics.RecordCheckOwnershipMiss(t.Context(), "apply_finished", "org/repo", "mydb", "mysql", "pie", "staging")
	metrics.AdjustActiveApplies(t.Context(), 1, "mydb", "pie", "staging")
	metrics.RecordControlOperation(t.Context(), "stop", "mydb", "pie", "staging", "success")
	metrics.RecordRemoteDeploymentHealth(t.Context(), "pie", "staging", true)
	metrics.RecordRemoteDeploymentHealthCheck(t.Context(), "pie", "staging", "success", "healthy")
	metrics.RecordLockOperation(t.Context(), "acquire", "mydb", "success")
	metrics.RecordSchedulerResume(t.Context(), "mydb", "pie", "staging", "running")
	metrics.RecordSchedulerResumeFailure(t.Context(), "mydb", "pie", "staging", "no_client")
	metrics.RecordSchedulerClaimFailure(t.Context(), "storage_error")
	metrics.RecordSchedulerClaimDuration(t.Context(), time.Second, "mydb", "pie", "staging", "running")
	metrics.RecordSchemaRequestError(t.Context(), "org/repo", "apply", "mydb", "staging", "invalid_config")
	metrics.RecordGitHubRequest(t.Context(), metrics.GitHubRequestSample{
		Operation:  metrics.GitHubOperationFetchPullRequest,
		Category:   metrics.GitHubRequestCategoryRead,
		Resource:   metrics.GitHubRateLimitResourceCore,
		Status:     metrics.GitHubRequestStatusSuccess,
		Repository: "org/repo",
	})
	metrics.RecordGitHubRateLimit(t.Context(), metrics.GitHubRateLimitSample{
		Operation:  metrics.GitHubOperationFetchPullRequest,
		Resource:   metrics.GitHubRateLimitResourceCore,
		Repository: "org/repo",
		Limit:      100,
		Remaining:  99,
		Used:       1,
	})
	metrics.RecordWebhookEvent(t.Context(), "issue_comment", "created", "org/repo", "processed")
	metrics.RecordStatusCheckOperation(t.Context(), metrics.StatusCheckOperation{
		Operation:  "aggregate_check_sync",
		Repository: "org/repo",
		Status:     "blocked",
	})

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var checked int
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if !strings.HasPrefix(m.Name, "schemabot.") {
				continue
			}
			checked++
			assertMetricDataPointsHaveEnvironment(t, m)
			if metricHasDeploymentAttribute(m.Name) {
				assertMetricDataPointsHaveDeployment(t, m)
			}
		}
	}
	require.NotZero(t, checked, "expected SchemaBot metrics to be collected")
}

func metricHasDeploymentAttribute(metricName string) bool {
	switch metricName {
	case "schemabot.plans.total",
		"schemabot.plan.duration_seconds",
		"schemabot.applies.total",
		"schemabot.apply.duration_seconds",
		"schemabot.check_ownership_misses_total",
		"schemabot.active_applies",
		"schemabot.control_operations_total",
		"schemabot.remote_deployment.health",
		"schemabot.remote_deployment.health_checks_total",
		"schemabot.scheduler.resumed_total",
		"schemabot.scheduler.resume_failures_total",
		"schemabot.scheduler.claim_duration_seconds":
		return true
	default:
		return false
	}
}

func assertMetricDataPointsHaveEnvironment(t *testing.T, m metricdata.Metrics) {
	t.Helper()
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, dp := range data.DataPoints {
			assertMetricAttributesHaveEnvironment(t, m.Name, dp.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, dp := range data.DataPoints {
			assertMetricAttributesHaveEnvironment(t, m.Name, dp.Attributes)
		}
	case metricdata.Histogram[float64]:
		for _, dp := range data.DataPoints {
			assertMetricAttributesHaveEnvironment(t, m.Name, dp.Attributes)
		}
	case metricdata.Histogram[int64]:
		for _, dp := range data.DataPoints {
			assertMetricAttributesHaveEnvironment(t, m.Name, dp.Attributes)
		}
	default:
		t.Fatalf("unsupported metric data type %T for %s", m.Data, m.Name)
	}
}

func assertMetricAttributesHaveEnvironment(t *testing.T, metricName string, attrs attribute.Set) {
	t.Helper()
	environment, ok := attrs.Value(attribute.Key("environment"))
	require.True(t, ok, "%s metric data point missing environment attribute: %v", metricName, attrs)
	assert.NotEmpty(t, environment.AsString(), "%s metric environment attribute must be non-empty", metricName)
}

func assertMetricDataPointsHaveDeployment(t *testing.T, m metricdata.Metrics) {
	t.Helper()
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, dp := range data.DataPoints {
			assertMetricAttributesHaveDeployment(t, m.Name, dp.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, dp := range data.DataPoints {
			assertMetricAttributesHaveDeployment(t, m.Name, dp.Attributes)
		}
	case metricdata.Histogram[float64]:
		for _, dp := range data.DataPoints {
			assertMetricAttributesHaveDeployment(t, m.Name, dp.Attributes)
		}
	case metricdata.Histogram[int64]:
		for _, dp := range data.DataPoints {
			assertMetricAttributesHaveDeployment(t, m.Name, dp.Attributes)
		}
	default:
		t.Fatalf("unsupported metric data type %T for %s", m.Data, m.Name)
	}
}

func assertMetricAttributesHaveDeployment(t *testing.T, metricName string, attrs attribute.Set) {
	t.Helper()
	deployment, ok := attrs.Value(attribute.Key("deployment"))
	require.True(t, ok, "%s metric data point missing deployment attribute: %v", metricName, attrs)
	assert.NotEmpty(t, deployment.AsString(), "%s metric deployment attribute must be non-empty", metricName)
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

	metrics.RecordApply(t.Context(), "testrepo", "mydb", "pie", "staging", "success")
	metrics.RecordApply(t.Context(), "testrepo", "mydb", "pie", "staging", "error")
	metrics.RecordApply(t.Context(), "testrepo", "mydb", "pie", "staging", "conflict")
	metrics.RecordApplyDuration(t.Context(), 2*time.Second, "testrepo", "mydb", "pie", "staging", "success")

	names := collectMetricNames(t, reader)
	assert.True(t, names["schemabot.applies.total"], "expected schemabot.applies.total")
	assert.True(t, names["schemabot.apply.duration_seconds"], "expected schemabot.apply.duration_seconds")
}

func TestRecordCheckOwnershipMissMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordCheckOwnershipMiss(t.Context(), "apply_finished", "org/repo", "mydb", "mysql", "pie", "staging")
	metrics.RecordCheckOwnershipMiss(t.Context(), "rollback_finished", "org/repo", "mydb", "mysql", "pie", "staging")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var found bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "schemabot.check_ownership_misses_total" {
				found = true
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				assert.Len(t, sum.DataPoints, 2, "expected one data point per operation")
			}
		}
	}
	assert.True(t, found, "schemabot.check_ownership_misses_total metric not found")
}

func TestRecordSourcePolicyBlockMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordSourcePolicyBlock(t.Context(), "plan", "mydb", "staging", "unauthorized_repo")
	metrics.RecordSourcePolicyBlock(t.Context(), "apply", "mydb", "production", "missing_database_config")
	metrics.RecordSourcePolicyBlock(t.Context(), "not_real", "mydb", "staging", "not_real")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var found bool
	var sawUnknown bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.source_policy.blocks_total" {
				continue
			}
			found = true
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			assert.Len(t, sum.DataPoints, 3, "expected one data point per operation/reason attribute set")
			for _, dp := range sum.DataPoints {
				operation, hasOperation := dp.Attributes.Value(attribute.Key("operation"))
				reason, hasReason := dp.Attributes.Value(attribute.Key("reason"))
				require.True(t, hasOperation)
				require.True(t, hasReason)
				if operation.AsString() == "unknown" && reason.AsString() == "unknown" {
					sawUnknown = true
				}
			}
		}
	}
	assert.True(t, found, "schemabot.source_policy.blocks_total metric not found")
	assert.True(t, sawUnknown, "expected unknown operation and reason to be normalized")
}

func TestRecordPRCommandActorAuthorizationMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordPRCommandActorAuthorization(t.Context(), "apply", "mydb", "staging", "org/repo", "allowed", "allowed_admin_user")
	metrics.RecordPRCommandActorAuthorization(t.Context(), "apply_confirm", "mydb", "production", "org/repo", "denied", "not_authorized")
	metrics.RecordPRCommandActorAuthorization(t.Context(), "not_real", "mydb", "staging", "org/repo", "not_real", "not_real")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var found bool
	var sawUnknown bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.pr_command_actor_authorization.total" {
				continue
			}
			found = true
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			assert.Len(t, sum.DataPoints, 3, "expected one data point per command/status/reason attribute set")
			for _, dp := range sum.DataPoints {
				command, hasCommand := dp.Attributes.Value(attribute.Key("command"))
				status, hasStatus := dp.Attributes.Value(attribute.Key("status"))
				reason, hasReason := dp.Attributes.Value(attribute.Key("reason"))
				require.True(t, hasCommand)
				require.True(t, hasStatus)
				require.True(t, hasReason)
				if command.AsString() == "unknown" && status.AsString() == "unknown" && reason.AsString() == "unknown" {
					sawUnknown = true
				}
			}
		}
	}
	assert.True(t, found, "schemabot.pr_command_actor_authorization.total metric not found")
	assert.True(t, sawUnknown, "expected unknown command, status, and reason to be normalized")
}

func TestRecordStatusCheckOperationMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordStatusCheckOperation(t.Context(), metrics.StatusCheckOperation{
		Operation:   "aggregate_check_sync",
		Repository:  "org/repo",
		Environment: "staging",
		Status:      "blocked",
	})
	metrics.RecordStatusCheckOperation(t.Context(), metrics.StatusCheckOperation{
		Operation:    "stale_check_cleanup",
		Repository:   "org/repo",
		Database:     "mydb",
		DatabaseType: "mysql",
		Environment:  "staging",
		Status:       "success",
	})
	metrics.RecordStatusCheckOperation(t.Context(), metrics.StatusCheckOperation{
		Operation:   "schema_config_environment_validation",
		Repository:  "org/repo",
		Environment: "staging",
		Status:      "error",
	})
	metrics.RecordStatusCheckOperation(t.Context(), metrics.StatusCheckOperation{
		Operation:    "not_real",
		Repository:   "org/repo",
		Database:     "mydb",
		DatabaseType: "mysql",
		Environment:  "staging",
		Status:       "not_real",
	})

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var found bool
	var sawUnknown bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.status_check_operations_total" {
				continue
			}
			found = true
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			assert.Len(t, sum.DataPoints, 4, "expected one data point per operation/status attribute set")
			for _, dp := range sum.DataPoints {
				operation, hasOperation := dp.Attributes.Value(attribute.Key("operation"))
				status, hasStatus := dp.Attributes.Value(attribute.Key("status"))
				require.True(t, hasOperation)
				require.True(t, hasStatus)
				if operation.AsString() == "unknown" && status.AsString() == "unknown" {
					sawUnknown = true
				}
			}
		}
	}
	assert.True(t, found, "schemabot.status_check_operations_total metric not found")
	assert.True(t, sawUnknown, "expected unknown operation and status to be normalized")
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

	metrics.RecordPlanDuration(t.Context(), 500*time.Millisecond, "testrepo", "mydb", "pie", "staging", "success")

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

func TestRecordControlOperationMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordControlOperation(t.Context(), "cutover", "mydb", "pie", "staging", "success")
	metrics.RecordControlOperation(t.Context(), "stop", "mydb", "pie", "staging", "success")
	metrics.RecordControlOperation(t.Context(), "start", "mydb", "pie", "staging", "error")

	names := collectMetricNames(t, reader)
	assert.True(t, names["schemabot.control_operations_total"], "expected schemabot.control_operations_total")
}

func TestRecordLockOperationMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordLockOperation(t.Context(), "acquire", "mydb", "success")
	metrics.RecordLockOperation(t.Context(), "acquire", "mydb", "conflict")
	metrics.RecordLockOperation(t.Context(), "release", "mydb", "success")

	names := collectMetricNames(t, reader)
	assert.True(t, names["schemabot.lock_operations_total"], "expected schemabot.lock_operations_total")
}

func TestRecordSchedulerMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordSchedulerResume(t.Context(), "testdb", "pie", "staging", "running")
	metrics.RecordSchedulerResumeFailure(t.Context(), "testdb", "pie", "staging", "no_client")
	metrics.RecordSchedulerClaimFailure(t.Context(), "storage_error")
	metrics.RecordSchedulerClaimFailure(t.Context(), "expire_retryable_error")
	metrics.RecordSchedulerClaimDuration(t.Context(), 50*time.Millisecond, "testdb", "pie", "staging", "running")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	names := make(map[string]bool)
	var sawExpireRetryableError bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
			if m.Name != "schemabot.scheduler.claim_failures_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				reason, hasReason := dp.Attributes.Value(attribute.Key("reason"))
				require.True(t, hasReason)
				if reason.AsString() == "expire_retryable_error" {
					sawExpireRetryableError = true
				}
			}
		}
	}
	assert.True(t, names["schemabot.scheduler.resumed_total"], "expected schemabot.scheduler.resumed_total")
	assert.True(t, names["schemabot.scheduler.resume_failures_total"], "expected schemabot.scheduler.resume_failures_total")
	assert.True(t, names["schemabot.scheduler.claim_failures_total"], "expected schemabot.scheduler.claim_failures_total")
	assert.True(t, names["schemabot.scheduler.claim_duration_seconds"], "expected schemabot.scheduler.claim_duration_seconds")
	assert.True(t, sawExpireRetryableError, "expected expire_retryable_error claim failure reason to be preserved")
}

func TestRecordRemoteDeploymentHealthMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	metrics.RecordRemoteDeploymentHealth(t.Context(), "pie", "staging", true)
	metrics.RecordRemoteDeploymentHealth(t.Context(), "sled", "staging", false)
	metrics.RecordRemoteDeploymentHealthCheck(t.Context(), "pie", "staging", "success", "healthy")
	metrics.RecordRemoteDeploymentHealthCheck(t.Context(), "sled", "staging", "error", "unavailable")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var sawHealthyGauge, sawUnhealthyGauge, sawHealthCheckCounter bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "schemabot.remote_deployment.health":
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok)
				for _, dp := range gauge.DataPoints {
					deployment, hasDeployment := dp.Attributes.Value(attribute.Key("deployment"))
					require.True(t, hasDeployment)
					environment, hasEnvironment := dp.Attributes.Value(attribute.Key("environment"))
					require.True(t, hasEnvironment)
					assert.Equal(t, "staging", environment.AsString())
					switch deployment.AsString() {
					case "pie":
						assert.Equal(t, int64(1), dp.Value)
						sawHealthyGauge = true
					case "sled":
						assert.Equal(t, int64(0), dp.Value)
						sawUnhealthyGauge = true
					}
				}
			case "schemabot.remote_deployment.health_checks_total":
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				assert.Len(t, sum.DataPoints, 2)
				sawHealthCheckCounter = true
			}
		}
	}
	assert.True(t, sawHealthyGauge, "expected healthy deployment gauge")
	assert.True(t, sawUnhealthyGauge, "expected unhealthy deployment gauge")
	assert.True(t, sawHealthCheckCounter, "expected health check counter")
}

// setupTraceTest creates an in-memory trace exporter and configures the global
// TracerProvider. Returns the exporter for span inspection.
func setupTraceTest(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		if err := tp.Shutdown(t.Context()); err != nil {
			t.Logf("tracer shutdown: %v", err)
		}
	})
	return exporter
}

// findSpan returns the first span with the given name, or nil.
func findSpan(spans tracetest.SpanStubs, name string) *tracetest.SpanStub {
	for i := range spans {
		if spans[i].Name == name {
			return &spans[i]
		}
	}
	return nil
}

// spanAttrs returns a map of string attribute values from a span.
func spanAttrs(s *tracetest.SpanStub) map[string]string {
	attrs := make(map[string]string)
	for _, a := range s.Attributes {
		attrs[string(a.Key)] = a.Value.Emit()
	}
	return attrs
}

func TestExecutePlanTrace(t *testing.T) {
	exporter := setupTraceTest(t)

	svc := newTestService()

	// ExecutePlan will fail (mock tern client), but the span is still recorded.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, _ = svc.ExecutePlan(ctx, PlanRequest{
		Database:    "testdb",
		Environment: "staging",
		Type:        "mysql",
	})

	s := findSpan(exporter.GetSpans(), "ExecutePlan")
	require.NotNil(t, s, "expected ExecutePlan span")

	attrs := spanAttrs(s)
	assert.Equal(t, "testdb", attrs["database"])
	assert.Equal(t, "staging", attrs["environment"])
	assert.Equal(t, "mysql", attrs["type"])
}

// mockPlanStore returns nil for all lookups, simulating "plan not found".
type mockPlanStore struct{}

func (m *mockPlanStore) Create(context.Context, *storage.Plan) (int64, error)      { return 0, nil }
func (m *mockPlanStore) Get(context.Context, string) (*storage.Plan, error)        { return nil, nil }
func (m *mockPlanStore) GetByID(context.Context, int64) (*storage.Plan, error)     { return nil, nil }
func (m *mockPlanStore) GetByLock(context.Context, int64) ([]*storage.Plan, error) { return nil, nil }
func (m *mockPlanStore) GetByPR(context.Context, string, int) ([]*storage.Plan, error) {
	return nil, nil
}
func (m *mockPlanStore) Delete(context.Context, int64) error           { return nil }
func (m *mockPlanStore) DeleteByPR(context.Context, string, int) error { return nil }

// mockStorageWithPlans wraps mockStorage but returns a real PlanStore.
type mockStorageWithPlans struct {
	mockStorage
}

func (m *mockStorageWithPlans) Plans() storage.PlanStore { return &mockPlanStore{} }

func TestExecuteApplyTrace(t *testing.T) {
	exporter := setupTraceTest(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithPlans{}, testServerConfig(), nil, logger)

	// ExecuteApply returns "plan not found" — no panic, span is recorded cleanly.
	_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "nonexistent",
		Environment: "staging",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plan not found")

	s := findSpan(exporter.GetSpans(), "ExecuteApply")
	require.NotNil(t, s, "expected ExecuteApply span")

	attrs := spanAttrs(s)
	assert.Equal(t, "nonexistent", attrs["plan_id"])
	assert.Equal(t, "staging", attrs["environment"])
}
