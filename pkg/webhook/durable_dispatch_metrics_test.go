package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// newDispatchMetricsReader installs a manual OTel reader for the test's
// lifetime so the driver's dispatch metrics can be collected and asserted.
func newDispatchMetricsReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		// t.Context() is already cancelled when cleanup runs; detach so the
		// provider shutdown is not spuriously aborted.
		require.NoError(t, mp.Shutdown(context.WithoutCancel(t.Context())))
	})
	return reader
}

// collectCounterPoints returns the named int64 counter's data points from the
// reader, failing the test when the counter was never recorded.
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
			require.True(t, ok, "metric %s must be an int64 sum", name)
			return sum.DataPoints
		}
	}
	t.Fatalf("metric %s was never recorded", name)
	return nil
}

// collectDispatchHistogramPoints collects the named float64 histogram's data
// points from the reader.
func collectDispatchHistogramPoints(t *testing.T, reader *sdkmetric.ManualReader, name string) []metricdata.HistogramDataPoint[float64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var points []metricdata.HistogramDataPoint[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "metric %s must be a float64 histogram", name)
			points = append(points, hist.DataPoints...)
		}
	}
	return points
}

// A first claim records the started status, the inbox dispatch lag measured
// from receipt, and exactly one duration observation with the completed
// outcome — the started/outcome pair is the ledger that surfaces drivers dying
// mid-claim.
func TestDurableWebhookDispatchMetricsFirstClaimCompleted(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-metrics-1",
		Event:      "issue_comment",
		Payload:    []byte(`{}`),
		ReceivedAt: time.Now().Add(-90 * time.Second),
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1, "the no-op issue_comment delivery must be marked completed")

	lagPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.inbox_dispatch_lag_seconds")
	require.Len(t, lagPoints, 1, "first claim must record inbox dispatch lag")
	assert.Equal(t, uint64(1), lagPoints[0].Count)
	assert.GreaterOrEqual(t, lagPoints[0].Sum, 90.0)
	assertStringAttr(t, lagPoints[0].Attributes, "event_type", "issue_comment")

	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1, "the claim must record exactly one outcome")
	assert.Equal(t, uint64(1), durationPoints[0].Count)
	assertStringAttr(t, durationPoints[0].Attributes, "event_type", "issue_comment")
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "completed")
}

// A delivery that spent time deferred by a not-before time measures dispatch
// lag from when it became dispatchable, not from receipt: the grace period is
// by design, and counting it as lag would saturate the histogram's upper
// percentiles and mask real backlog regressions.
func TestDurableWebhookDispatchMetricsDeferredClaimMeasuresLagFromDueTime(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	notBefore := time.Now().Add(-10 * time.Second)
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-metrics-deferred",
		Event:      "issue_comment",
		Payload:    []byte(`{}`),
		ReceivedAt: time.Now().Add(-90 * time.Second),
		RetryAfter: &notBefore,
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1)
	lagPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.inbox_dispatch_lag_seconds")
	require.Len(t, lagPoints, 1, "first claim must record inbox dispatch lag")
	assert.GreaterOrEqual(t, lagPoints[0].Sum, 10.0, "lag must count the wait since the not-before time")
	assert.Less(t, lagPoints[0].Sum, 60.0, "lag must not count the deferral's grace period since receipt")
}

// A retry claim measures the retry window, not backlog latency, so only the
// first claim records inbox dispatch lag.
func TestDurableWebhookDispatchMetricsRetryClaimSkipsInboxLag(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-metrics-retry",
		Event:      "issue_comment",
		Payload:    []byte(`{}`),
		Attempts:   1, // FindNext claim increments to 2
		ReceivedAt: time.Now().Add(-90 * time.Second),
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1)
	lagPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.inbox_dispatch_lag_seconds")
	assert.Empty(t, lagPoints, "a retry claim must not record inbox dispatch lag")

	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "completed")
}

// A retryable processing failure below the attempt cap records the retrying
// outcome.
func TestDurableWebhookDispatchMetricsRetryingOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return true, errors.New("transient auto-plan failure")
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.failed, 1)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "retrying")
}

// A non-retryable processing failure dead-letters the delivery on the first
// attempt and records the failed_permanent outcome — no retry budget is
// burned replaying a failure the processor proved deterministic.
func TestDurableWebhookDispatchMetricsFailedPermanentOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, errors.New("deterministic rejection")
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.failed, "a dead-lettered delivery must not take the retryable failure path")
	require.Len(t, store.failedPermanent, 1)
	failure := <-store.failedPermanent
	assert.Equal(t, "deterministic rejection", failure.errMsg)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "failed_permanent")
}

// A retryable failure on the final budgeted attempt records the failed
// outcome: the delivery is terminal but stays eligible for reconciler
// resurrection and GitHub Redeliver, unlike a dead-lettered one.
func TestDurableWebhookDispatchMetricsBudgetExhaustedFailedOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	event := durablePullRequestEvent(t)
	event.Attempts = maxDurableWebhookAttempts - 1 // the FindNext claim increments to the cap
	store := newScriptedWebhookEventStore(event)
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return true, errors.New("transient auto-plan failure")
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.failedPermanent, "budget exhaustion must not dead-letter the delivery")
	require.Len(t, store.failed, 1)
	failure := <-store.failed
	assert.Nil(t, failure.retryAfter, "the terminal failure must not schedule another retry")
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "failed")
}

// A shutdown-interrupted claim that is refunded records the released outcome —
// not failed or retrying, since no genuine attempt completed.
func TestDurableWebhookDispatchMetricsReleasedOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	h := newDurableDriverHandler(t, store, nil, nil)

	driverCtx, cancelDriver := context.WithCancel(t.Context())
	defer cancelDriver()
	h.durableWebhookProcessOverride = func(ctx context.Context, _ *storage.WebhookEvent) (bool, error) {
		cancelDriver()
		<-ctx.Done()
		return true, ctx.Err()
	}

	h.driveNextDurableWebhook(driverCtx, 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.released, 1)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "released")
}

// Storage failing to record a completion leaves the row processing until
// lease expiry and records the finish_error outcome — the storage-health
// signal.
func TestDurableWebhookDispatchMetricsFinishErrorOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.markCompletedErr = errors.New("storage unavailable")
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.completed)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "finish_error")
}

// Storage failing to record a delivery failure also records finish_error, so
// the ledger stays intact on the failure-finish path.
func TestDurableWebhookDispatchMetricsFailureFinishErrorOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.markFailedErr = errors.New("storage unavailable")
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return true, errors.New("transient auto-plan failure")
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.failed)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "finish_error")
}

// Storage failing to record a dead-letter also records finish_error: the row
// stays processing until lease expiry and the next claim re-derives the
// permanent classification.
func TestDurableWebhookDispatchMetricsPermanentFailureFinishErrorOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.markFailedPermanentErr = errors.New("storage unavailable")
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, errors.New("deterministic rejection")
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.failedPermanent)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "finish_error")
}

// A shutdown release that fails because the lease was already lost records
// lease_lost, not finish_error: storage is healthy and another driver owns
// the row.
func TestDurableWebhookDispatchMetricsShutdownReleaseLeaseLost(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.releaseErr = storage.ErrWebhookEventLeaseLost
	h := newDurableDriverHandler(t, store, nil, nil)

	driverCtx, cancelDriver := context.WithCancel(t.Context())
	defer cancelDriver()
	h.durableWebhookProcessOverride = func(ctx context.Context, _ *storage.WebhookEvent) (bool, error) {
		cancelDriver()
		<-ctx.Done()
		return true, ctx.Err()
	}

	h.driveNextDurableWebhook(driverCtx, 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.released)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "lease_lost")
}

// A shutdown release that fails for a reason other than lease loss records
// finish_error: the claim could not be refunded and the row stays processing
// until lease expiry — the storage-health signal on the shutdown path.
func TestDurableWebhookDispatchMetricsShutdownReleaseFinishError(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.releaseErr = errors.New("storage unavailable")
	h := newDurableDriverHandler(t, store, nil, nil)

	driverCtx, cancelDriver := context.WithCancel(t.Context())
	defer cancelDriver()
	h.durableWebhookProcessOverride = func(ctx context.Context, _ *storage.WebhookEvent) (bool, error) {
		cancelDriver()
		<-ctx.Done()
		return true, ctx.Err()
	}

	h.driveNextDurableWebhook(driverCtx, 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.released)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "finish_error")
}

// A failure finish that loses the lease records lease_lost, not finish_error:
// another driver owns the row, so this claim's failure result is simply
// discarded.
func TestDurableWebhookDispatchMetricsFailureLeaseLost(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.markFailedErr = storage.ErrWebhookEventLeaseLost
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return true, errors.New("transient auto-plan failure")
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.failed)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "lease_lost")
}

// A completion finish that loses the lease records lease_lost, not
// finish_error or completed: the row belongs to another driver and will be
// re-processed there.
func TestDurableWebhookDispatchMetricsCompletionLeaseLost(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.markCompletedErr = storage.ErrWebhookEventLeaseLost
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.completed)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "lease_lost")
}

// Losing the lease heartbeat after successful processing leaves the row for
// reclaim and records the lease_lost outcome.
func TestDurableWebhookDispatchMetricsLeaseLostOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.heartbeatErr = storage.ErrWebhookEventLeaseLost
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookLeaseDuration = 30 * time.Millisecond
	h.durableWebhookProcessOverride = func(ctx context.Context, _ *storage.WebhookEvent) (bool, error) {
		<-ctx.Done()
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.completed)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "lease_lost")
}

// A claim discarded in favor of a newer covering delivery for the same PR
// records exactly one duration observation with the superseded outcome, so
// coalescing stays visible in the started/outcome ledger.
func TestDurableWebhookDispatchMetricsSupersededOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.coveringSuccessor = true
	store.supersedeCovered = true
	factory, _ := newLiveHeadGitHubFactory(t, "newer-head", "open")
	h := newDurableDriverHandler(t, store, nil, factory)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.completed)
	require.Empty(t, store.failed)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "superseded")
}

// Committed dead letters carry bounded reasons derived from typed processing
// errors. The same reason and delivery origin let logs explain recovery.
func TestDurablePermanentFailureReasons(t *testing.T) {
	type failureCase struct {
		name       string
		eventType  string
		tenant     string
		processErr error
		want       metrics.WebhookFailureReason
	}
	cases := []failureCase{
		{name: "file cap", processErr: fmt.Errorf("auto-plan: %w", ghclient.ErrPRFilesIncomplete), want: metrics.WebhookFailurePRFileCap},
		{name: "unknown", processErr: errors.New("decode durable invalid_payload pr_file_cap unresolved_route"), want: metrics.WebhookFailureOther},
		{name: "missing tenant", tenant: "", want: metrics.WebhookFailureUnresolvedRoute},
		{name: "invalid tenant", tenant: "garbage", want: metrics.WebhookFailureUnresolvedRoute},
		{name: "nonpositive tenant", tenant: "0", want: metrics.WebhookFailureUnresolvedRoute},
	}
	for _, eventType := range []string{"pull_request", "check_run", "check_suite", "issue_comment", "push", "merge_group"} {
		cases = append(cases, failureCase{name: "decode " + eventType, eventType: eventType, want: metrics.WebhookFailureInvalidPayload})
	}
	for _, tc := range cases {
		for _, origin := range []string{"organic", "synthesized"} {
			t.Run(tc.name+"/"+origin, func(t *testing.T) {
				reader := newDispatchMetricsReader(t)
				event := durablePullRequestEvent(t)
				event.TenantID = tc.tenant
				if tc.eventType != "" {
					event.Event = tc.eventType
					event.Payload = []byte(`{not json`)
				}
				if origin == "synthesized" {
					event.DeliveryID = storage.SynthesizedWebhookDeliveryIDPrefix + event.DeliveryID
				}
				store := newScriptedWebhookEventStore(event)
				h := newDurableDriverHandler(t, store, nil, nil)
				var logs bytes.Buffer
				h.logger = slog.New(slog.NewJSONHandler(&logs, nil))
				if tc.processErr != nil {
					h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
						return false, fmt.Errorf("wrapped processing error: %w", tc.processErr)
					}
				}
				h.driveNextDurableWebhook(t.Context(), 0, "test-driver")
				require.Len(t, store.failedPermanent, 1)
				var total int64
				for _, dp := range collectCounterPoints(t, reader, "schemabot.webhook.events_total") {
					status, _ := dp.Attributes.Value("status")
					reason, hasReason := dp.Attributes.Value("failure_reason")
					if status.AsString() != "durable_dispatch_failed_permanent" {
						assert.False(t, hasReason)
						continue
					}
					assert.Equal(t, string(tc.want), reason.AsString())
					assertStringAttr(t, dp.Attributes, "repository", event.Repository)
					assertStringAttr(t, dp.Attributes, "event_type", event.Event)
					assert.Equal(t, int64(1), dp.Value)
					total += dp.Value
				}
				assert.Equal(t, int64(1), total)
				var matched int
				for line := range strings.Lines(logs.String()) {
					var entry map[string]any
					require.NoError(t, json.Unmarshal([]byte(line), &entry))
					if entry["failure_reason"] == nil {
						continue
					}
					matched++
					assert.Equal(t, string(tc.want), entry["failure_reason"])
					assert.Equal(t, origin, entry["origin"])
					assert.Equal(t, event.DeliveryID, entry["delivery_id"])
					assert.Equal(t, event.Repository, entry["repo"])
					assert.Equal(t, float64(event.PullRequest), entry["pr"])
				}
				assert.Equal(t, 1, matched)
			})
		}
	}
}

// A retry or unsuccessful terminal write never claims that a committed dead
// letter exists. Failed writes retain the processing cause and permanent reason
// in logs, including when a peer has already replaced the delivery lease.
func TestDurablePermanentFailureRequiresSuccessfulWrite(t *testing.T) {
	for _, tc := range []struct {
		name     string
		retry    bool
		writeErr error
	}{
		{name: "storage error", writeErr: errors.New("storage unavailable")},
		{name: "lease lost", writeErr: storage.ErrWebhookEventLeaseLost},
		{name: "row removed", writeErr: storage.ErrWebhookEventNotFound},
		{name: "retry", retry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := newDispatchMetricsReader(t)
			store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
			store.markFailedPermanentErr = tc.writeErr
			h := newDurableDriverHandler(t, store, nil, nil)
			var logs bytes.Buffer
			h.logger = slog.New(slog.NewJSONHandler(&logs, nil))
			h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
				return tc.retry, ghclient.ErrPRFilesIncomplete
			}
			h.driveNextDurableWebhook(t.Context(), 0, "test-driver")
			require.Empty(t, store.failedPermanent)
			for _, dp := range collectCounterPoints(t, reader, "schemabot.webhook.events_total") {
				status, _ := dp.Attributes.Value("status")
				assert.NotEqual(t, "durable_dispatch_failed_permanent", status.AsString())
				_, hasReason := dp.Attributes.Value("failure_reason")
				assert.False(t, hasReason)
			}
			assert.NotContains(t, logs.String(), "dead-lettered")
			if tc.writeErr != nil {
				message := "durable webhook driver failed to record delivery failure"
				outcome := "finish_error"
				if errors.Is(tc.writeErr, storage.ErrWebhookEventLeaseLost) || errors.Is(tc.writeErr, storage.ErrWebhookEventNotFound) {
					message = "durable webhook driver lost the delivery lease before recording failure; another driver owns the delivery or the row is gone"
					outcome = "lease_lost"
				}
				record := findLogRecord(t, logs.Bytes(), message)
				assert.Equal(t, ghclient.ErrPRFilesIncomplete.Error(), record["process_error"])
				assert.Equal(t, tc.writeErr.Error(), record["error"])
				assert.Equal(t, "pr_file_cap", record["failure_reason"])
				durations := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
				require.Len(t, durations, 1)
				assertStringAttr(t, durations[0].Attributes, "outcome", outcome)
			}
		})
	}
}
