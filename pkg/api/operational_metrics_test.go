package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/block/schemabot/pkg/metrics"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

func newOperationalTelemetry(t *testing.T) (*Telemetry, func()) {
	t.Helper()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	previous := otel.GetMeterProvider()
	tel, err := SetupTelemetry(slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	shutdown := sync.OnceFunc(func() {
		require.NoError(t, tel.Shutdown(context.WithoutCancel(t.Context())))
	})
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		shutdown()
	})
	return tel, shutdown
}

func scrapeOperationalMetrics(t *testing.T, tel *Telemetry) map[string]*dto.MetricFamily {
	t.Helper()
	response := httptest.NewRecorder()
	tel.MetricsHandler.ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, response.Code)
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(response.Body.String()))
	require.NoError(t, err)
	return families
}

func exportedLabels(m *dto.Metric) map[string]string {
	labels := make(map[string]string)
	for _, label := range m.Label {
		labels[label.GetName()] = label.GetValue()
	}
	return labels
}

// The real scrape endpoint stays observable while idle and while inbox storage
// fails; its heartbeat is independent of the last-good inbox snapshot gauges.
func TestOperationalHeartbeatSurvivesStorageFailure(t *testing.T) {
	tel, _ := newOperationalTelemetry(t)
	for _, storageFailed := range []bool{false, true} {
		if storageFailed {
			svc := newInboxMetricsTestService(t, &fakeWebhookEventStore{err: errors.New("storage unavailable")})
			svc.CollectWebhookInboxMetrics(t.Context())
		}
		families := scrapeOperationalMetrics(t, tel)
		heartbeat := families["schemabot_telemetry_heartbeat_ratio"]
		require.NotNil(t, heartbeat, "exported families: %v", families)
		assert.Equal(t, dto.MetricType_GAUGE, heartbeat.GetType())
		require.Len(t, heartbeat.Metric, 1)
		assert.Equal(t, float64(1), heartbeat.Metric[0].GetGauge().GetValue())
		labels := exportedLabels(heartbeat.Metric[0])
		assert.Equal(t, "unknown", labels["environment"])
		for _, key := range []string{"database", "repository", "pr", "delivery_id", "request_id"} {
			assert.NotContains(t, labels, key)
		}
		if storageFailed {
			failures := families["schemabot_webhook_inbox_stats_collection_failures_total"]
			require.NotNil(t, failures)
			require.Len(t, failures.Metric, 1)
			assert.Equal(t, float64(1), failures.Metric[0].GetCounter().GetValue())
		}
	}
}

// Each provider owns its heartbeat; replacing one process leaves another live
// process exporting a single heartbeat on its own registry.
func TestOperationalHeartbeatProviderReplacement(t *testing.T) {
	_, stopFirst := newOperationalTelemetry(t)
	second, _ := newOperationalTelemetry(t)
	stopFirst()
	heartbeat := scrapeOperationalMetrics(t, second)["schemabot_telemetry_heartbeat_ratio"]
	require.NotNil(t, heartbeat)
	require.Len(t, heartbeat.Metric, 1)
	assert.Equal(t, float64(1), heartbeat.Metric[0].GetGauge().GetValue())
}

// Permanent reasons split the existing event counter, preserve its aggregate,
// and bound unknown classifications without adding labels to ordinary events.
func TestOperationalPermanentFailureExport(t *testing.T) {
	tel, _ := newOperationalTelemetry(t)
	reasons := []metrics.WebhookFailureReason{
		metrics.WebhookFailurePRFileCap, metrics.WebhookFailurePRFileCap,
		metrics.WebhookFailureInvalidPayload, metrics.WebhookFailureUnresolvedRoute,
		metrics.WebhookFailureOther, "unexpected error text", "",
	}
	for _, reason := range reasons {
		metrics.RecordWebhookPermanentFailure(t.Context(), "example-app", "pull_request", "synchronize", "octocat/hello-world", reason)
	}
	metrics.RecordWebhookEvent(t.Context(), "example-app", "pull_request", "synchronize", "octocat/hello-world", "processed")
	family := scrapeOperationalMetrics(t, tel)["schemabot_webhook_events_total"]
	require.NotNil(t, family)
	counts := make(map[string]float64)
	var permanentTotal float64
	for _, m := range family.Metric {
		labels := exportedLabels(m)
		assert.Equal(t, "example-app", labels["app_name"])
		assert.Equal(t, "unknown", labels["environment"])
		assert.Equal(t, "pull_request", labels["event_type"])
		assert.Equal(t, "synchronize", labels["action"])
		assert.Equal(t, "octocat/hello-world", labels["repository"])
		assert.NotContains(t, labels, "origin")
		if labels["status"] == "processed" {
			assert.NotContains(t, labels, "failure_reason")
			assert.Equal(t, float64(1), m.GetCounter().GetValue())
			continue
		}
		assert.Equal(t, "durable_dispatch_failed_permanent", labels["status"])
		counts[labels["failure_reason"]] += m.GetCounter().GetValue()
		permanentTotal += m.GetCounter().GetValue()
	}
	assert.Equal(t, map[string]float64{"pr_file_cap": 2, "invalid_payload": 1, "unresolved_route": 1, "other": 3}, counts)
	assert.Equal(t, float64(len(reasons)), permanentTotal)
}

// Exceptional command counters stay event-driven, retain the target's route
// and schema environment, and can be scraped without pre-seeding combinations.
func TestOperationalRemoteControlCounterExport(t *testing.T) {
	tel, _ := newOperationalTelemetry(t)
	families := scrapeOperationalMetrics(t, tel)
	for _, name := range []string{
		"schemabot_remote_control_requests_rejected_total",
		"schemabot_remote_control_requests_stale_resends_total",
	} {
		assert.NotContains(t, families, name)
	}
	metrics.RecordRemoteControlRequestRejected(t.Context(), "stop", "planetscale", "example", "remote", "production")
	metrics.RecordRemoteControlRequestStale(t.Context(), "cancel", "example", "remote", "staging")
	families = scrapeOperationalMetrics(t, tel)
	for _, tc := range []struct{ name, operation, environment, engine string }{
		{"schemabot_remote_control_requests_rejected_total", "stop", "production", "planetscale"},
		{"schemabot_remote_control_requests_stale_resends_total", "cancel", "staging", ""},
	} {
		family := families[tc.name]
		require.NotNil(t, family, "missing %s", tc.name)
		require.Len(t, family.Metric, 1)
		m := family.Metric[0]
		assert.Equal(t, float64(1), m.GetCounter().GetValue())
		labels := exportedLabels(m)
		assert.Equal(t, tc.operation, labels["operation"])
		assert.Equal(t, tc.environment, labels["environment"])
		assert.Equal(t, "example", labels["database"])
		assert.Equal(t, "remote", labels["deployment"])
		if tc.engine != "" {
			assert.Equal(t, tc.engine, labels["engine"])
		}
	}
}
