package api

import (
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/testctx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// installManualMetricReader routes the global meter provider through a manual
// reader for the duration of the test so recorded counters can be inspected.
func installManualMetricReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		cleanupCtx, cancel := testctx.Cleanup(t, 30*time.Second)
		defer cancel()
		require.NoError(t, mp.Shutdown(cleanupCtx))
	})
	return reader
}

// collectCounterPoints returns the data points recorded so far for one counter.
func collectCounterPoints(t *testing.T, reader *sdkmetric.ManualReader, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s is not an int64 sum", name)
			return sum.DataPoints
		}
	}
	return nil
}

func attributeValue(t *testing.T, dp metricdata.DataPoint[int64], key string) string {
	t.Helper()
	v, ok := dp.Attributes.Value(attribute.Key(key))
	require.True(t, ok, "attribute %q missing", key)
	return v.AsString()
}

// Every actor-authorization reason the PR command door can produce must be
// recorded under its own name. A reason the metric does not recognize is
// collapsed to "unknown", which would hide which principal granted a command
// or why it was blocked.
func TestPRCommandActorAuthorizationReasonsRecordAsThemselves(t *testing.T) {
	reasons := []string{
		ActorAuthReasonDisabled,
		ActorAuthReasonAllowedAdminTeam,
		ActorAuthReasonAllowedAdminUser,
		ActorAuthReasonAllowedRepoAdminTeam,
		ActorAuthReasonAllowedRepoAdminUser,
		ActorAuthReasonAllowedOperatorTeam,
		ActorAuthReasonAllowedOperatorUser,
		ActorAuthReasonMissingActor,
		ActorAuthReasonMissingServerConfig,
		ActorAuthReasonMissingDatabaseConfig,
		ActorAuthReasonNoConfiguredPrincipal,
		ActorAuthReasonNotAuthorized,
		ActorAuthReasonGitHubError,
	}
	reader := installManualMetricReader(t)
	for _, reason := range reasons {
		metrics.RecordPRCommandActorAuthorization(t.Context(), "apply", "mydb", "staging", "org/repo", "allowed", reason)
	}

	recorded := map[string]bool{}
	for _, dp := range collectCounterPoints(t, reader, "schemabot.pr_command_actor_authorization.total") {
		recorded[attributeValue(t, dp, "reason")] = true
	}
	for _, reason := range reasons {
		assert.True(t, recorded[reason], "reason %q was not recorded under its own name", reason)
	}
	assert.False(t, recorded[ActorAuthReasonUnknown], "no reason in the vocabulary collapsed to unknown")
}

// A direct write whose stored plan cannot be loaded never authorizes, and the
// decision still lands on the metric: status skipped with the target_unresolved
// reason, so a run of storage failures on the authorization path is countable
// rather than visible only in the logs.
func TestUnresolvedDirectWriteTargetIsCountedAsSkipped(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	operator := &auth.User{Subject: "bob", Groups: []string{"payments-team"}}

	cases := map[string]*mockPlanLookupStore{
		"plan lookup fails":   {err: assert.AnError},
		"plan does not exist": {},
	}
	for name, plans := range cases {
		t.Run(name, func(t *testing.T) {
			reader := installManualMetricReader(t)
			svc := New(&mockStorageWithPlanLookup{plans: plans}, scopedWriteConfig(), nil, logger)
			rec := scopedDenialRequest(t, svc.handleApply, operator, http.MethodPost, "/api/apply",
				`{"plan_id":"plan-1","environment":"staging"}`)
			require.Equal(t, http.StatusInternalServerError, rec.Code)

			points := collectCounterPoints(t, reader, "schemabot.direct_write_authorization.total")
			require.Len(t, points, 1, "exactly one direct-write decision is recorded")
			assert.Equal(t, "apply", attributeValue(t, points[0], "operation"))
			assert.Equal(t, "skipped", attributeValue(t, points[0], "status"))
			assert.Equal(t, DirectWriteReasonTargetUnresolved, attributeValue(t, points[0], "reason"))
			assert.Equal(t, "staging", attributeValue(t, points[0], "environment"))
			assert.Equal(t, "unknown", attributeValue(t, points[0], "database"),
				"no target resolved, so the database attribute carries the sentinel")
		})
	}
}
