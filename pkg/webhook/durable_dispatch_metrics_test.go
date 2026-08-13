package webhook

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
