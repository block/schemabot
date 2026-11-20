package metrics

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// notARealValue stands in for a typo, a new caller value nobody added to an
// allowlist, or a value copied from a request.
const notARealValue = "not-a-real-value"

// Every recorder that allowlists a label must record an unrecognized value as
// its fallback ("unknown" for most labels, "other" for webhook failure reasons,
// and "error" and "unavailable" for remote deployment health checks), never as
// the raw value. Otherwise one unexpected caller value opens a new time series
// per distinct input.
func TestRecordersFoldUnrecognizedLabelValues(t *testing.T) {
	tests := []struct {
		name    string
		record  func(ctx context.Context)
		metrics []string
		want    map[string]string
	}{
		{
			name: "schema freshness rejection action",
			record: func(ctx context.Context) {
				RecordSchemaFreshnessRejected(ctx, notARealValue, "staging")
			},
			metrics: []string{"schemabot.schema_freshness.rejected.total"},
			want:    map[string]string{"action": "unknown"},
		},
		{
			name: "base schema freshness rejection action",
			record: func(ctx context.Context) {
				RecordBaseSchemaFreshnessRejected(ctx, notARealValue, "staging", "rejected")
			},
			metrics: []string{"schemabot.command.rejected_base_schema_freshness.total"},
			want:    map[string]string{"action": "unknown"},
		},
		{
			name: "review drift classification",
			record: func(ctx context.Context) {
				RecordReviewDrift(ctx, "orders", "staging", "default", notARealValue)
			},
			metrics: []string{"schemabot.review_drift.total"},
			want:    map[string]string{"classification": "unknown"},
		},
		{
			name: "source policy block operation and reason",
			record: func(ctx context.Context) {
				RecordSourcePolicyBlock(ctx, notARealValue, "orders", "staging", notARealValue)
			},
			metrics: []string{"schemabot.source_policy.blocks_total"},
			want:    map[string]string{"operation": "unknown", "reason": "unknown"},
		},
		{
			name: "PR command actor authorization command, status and reason",
			record: func(ctx context.Context) {
				RecordPRCommandActorAuthorization(ctx, notARealValue, "orders", "staging",
					"octo/app", notARealValue, notARealValue)
			},
			metrics: []string{"schemabot.pr_command_actor_authorization.total"},
			want:    map[string]string{"command": "unknown", "status": "unknown", "reason": "unknown"},
		},
		{
			name: "direct write authorization operation, status, reason and empty database",
			record: func(ctx context.Context) {
				RecordDirectWriteAuthorization(ctx, notARealValue, "", "staging", notARealValue, notARealValue)
			},
			metrics: []string{"schemabot.direct_write_authorization.total"},
			want: map[string]string{
				"operation": "unknown", "status": "unknown", "reason": "unknown", "database": "unknown",
			},
		},
		{
			name: "check ownership miss operation",
			record: func(ctx context.Context) {
				RecordCheckOwnershipMiss(ctx, notARealValue, "octo/app", "orders", "mysql", "default", "staging")
			},
			metrics: []string{"schemabot.check_ownership_misses_total"},
			want:    map[string]string{"operation": "unknown"},
		},
		{
			name: "control operation",
			record: func(ctx context.Context) {
				RecordControlOperation(ctx, notARealValue, "orders", "default", "staging", "success")
			},
			metrics: []string{"schemabot.control_operations_total"},
			want:    map[string]string{"operation": "unknown"},
		},
		{
			name: "stale remote control request operation",
			record: func(ctx context.Context) {
				RecordRemoteControlRequestStale(ctx, notARealValue, "orders", "default", "staging")
			},
			metrics: []string{"schemabot.remote_control_requests.stale_resends_total"},
			want:    map[string]string{"operation": "unknown"},
		},
		{
			name: "rejected remote control request operation",
			record: func(ctx context.Context) {
				RecordRemoteControlRequestRejected(ctx, notARealValue, "spirit", "orders", "default", "staging")
			},
			metrics: []string{"schemabot.remote_control_requests.rejected_total"},
			want:    map[string]string{"operation": "unknown"},
		},
		{
			name: "unsupported control decline operation",
			record: func(ctx context.Context) {
				RecordControlRequestUnsupportedDecline(ctx, notARealValue, "spirit", "orders", "default", "staging")
			},
			metrics: []string{"schemabot.control.unsupported_declines_total"},
			want:    map[string]string{"operation": "unknown"},
		},
		{
			name: "taskless settle deferral operation",
			record: func(ctx context.Context) {
				RecordTasklessSettleDeferred(ctx, notARealValue, "orders", "default", "staging")
			},
			metrics: []string{"schemabot.control.taskless_settle_deferrals_total"},
			want:    map[string]string{"operation": "unknown"},
		},
		{
			name: "remote deployment health check status and reason",
			record: func(ctx context.Context) {
				RecordRemoteDeploymentHealthCheck(ctx, "default", "staging", notARealValue, notARealValue)
			},
			metrics: []string{"schemabot.remote_deployment.health_checks_total"},
			want:    map[string]string{"status": "error", "reason": "unavailable"},
		},
		{
			name: "remote apply dedup outcome",
			record: func(ctx context.Context) {
				RecordRemoteApplyDedup(ctx, "orders", "staging", notARealValue)
			},
			metrics: []string{"schemabot.remote_apply_dedup_total"},
			want:    map[string]string{"outcome": "unknown"},
		},
		{
			name: "remote apply attach outcome",
			record: func(ctx context.Context) {
				RecordRemoteApplyAttach(ctx, "orders", "staging", notARealValue)
			},
			metrics: []string{"schemabot.remote_apply_attach_total"},
			want:    map[string]string{"outcome": "unknown"},
		},
		{
			name: "operator claim failure reason, on the operator name and its scheduler alias",
			record: func(ctx context.Context) {
				RecordOperatorClaimFailure(ctx, notARealValue)
			},
			metrics: []string{
				"schemabot.operator.claim_failures_total",
				"schemabot.scheduler.claim_failures_total",
			},
			want: map[string]string{"reason": "unknown"},
		},
		{
			name: "operator terminal summary failure reason, on the operator name and its scheduler alias",
			record: func(ctx context.Context) {
				RecordOperatorTerminalSummaryFailure(ctx, notARealValue)
			},
			metrics: []string{
				"schemabot.operator.terminal_summary_publish_failures_total",
				"schemabot.scheduler.terminal_summary_publish_failures_total",
			},
			want: map[string]string{"reason": "unknown"},
		},
		{
			name: "recovered panic operation",
			record: func(ctx context.Context) {
				RecordRecoveredPanic(ctx, notARealValue)
			},
			metrics: []string{"schemabot.panic.recovered_total"},
			want:    map[string]string{"operation": "unknown"},
		},
		{
			name: "webhook permanent failure reason",
			record: func(ctx context.Context) {
				RecordWebhookPermanentFailure(ctx, "", "pull_request", "opened", "octo/app",
					WebhookFailureReason(notARealValue))
			},
			metrics: []string{"schemabot.webhook.events_total"},
			want:    map[string]string{"failure_reason": string(WebhookFailureOther)},
		},
		{
			name: "status check operation and status",
			record: func(ctx context.Context) {
				RecordStatusCheckOperation(ctx, StatusCheckOperation{
					Operation:   notARealValue,
					Repository:  "octo/app",
					Database:    "orders",
					Environment: "staging",
					Status:      notARealValue,
				})
			},
			metrics: []string{"schemabot.status_check_operations_total"},
			want:    map[string]string{"operation": "unknown", "status": "unknown"},
		},
		{
			name: "direct execution outcome",
			record: func(ctx context.Context) {
				RecordDirectExecution(ctx, "orders", notARealValue)
			},
			metrics: []string{"schemabot.direct_execution.statements_total"},
			want:    map[string]string{"outcome": "unknown"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := useManualReader(t)

			tt.record(t.Context())

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &rm))
			for _, name := range tt.metrics {
				points := counterAttributeSets(t, rm, name)
				require.Len(t, points, 1, "metric %s should have one data point", name)
				attrs := points[0]
				for key, want := range tt.want {
					got, ok := attrs.Value(attribute.Key(key))
					require.True(t, ok, "metric %s has no %q label", name, key)
					assert.Equal(t, want, got.AsString(), "metric %s label %q", name, key)
				}
				for _, kv := range attrs.ToSlice() {
					assert.NotEqual(t, notARealValue, kv.Value.AsString(),
						"metric %s recorded the raw value on label %q", name, kv.Key)
				}
			}
		})
	}
}

// useManualReader installs a fresh meter provider for the test and restores
// the previous one when the test ends.
func useManualReader(t *testing.T) *metric.ManualReader {
	t.Helper()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	// t.Context() is canceled before cleanups run, so shut down on a context
	// that outlives it.
	shutdownCtx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, mp.Shutdown(shutdownCtx))
	})
	return reader
}

// counterAttributeSets returns the attribute set of every data point the named
// int64 counter collected.
func counterAttributeSets(t *testing.T, rm metricdata.ResourceMetrics, name string) []attribute.Set {
	t.Helper()
	var sets []attribute.Set
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "metric %s is not an int64 counter", name)
			for _, dp := range sum.DataPoints {
				sets = append(sets, dp.Attributes)
			}
		}
	}
	return sets
}
