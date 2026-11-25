package metrics

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	otelmetric "go.opentelemetry.io/otel/metric"
)

// ApplyNotificationBacklog is a completed, eventually refreshed observation
// sweep, not an exact durable count of lost events. Includes pre-feature history.
type ApplyNotificationBacklog struct {
	Oldest      *time.Time
	OutOfWindow int64
}

var applyNotificationBacklog atomic.Pointer[ApplyNotificationBacklog]

// PublishApplyNotificationBacklog replaces both gauges atomically. Nil clears
// them when the publisher stops. Callers must not mutate a published snapshot.
func PublishApplyNotificationBacklog(snapshot *ApplyNotificationBacklog) {
	applyNotificationBacklog.Store(snapshot)
}

func RegisterApplyNotificationMetrics(provider otelmetric.MeterProvider) error {
	meter := provider.Meter(meterName)
	lag, err := meter.Float64ObservableGauge("schemabot.apply_notifications.oldest_unpublished_age_seconds", otelmetric.WithUnit("s"), otelmetric.WithDescription("Age of the oldest unpublished completed apply observed by the last completed backlog sweep"))
	if err != nil {
		return fmt.Errorf("register apply notification lag: %w", err)
	}
	outside, err := meter.Int64ObservableGauge("schemabot.apply_notifications.out_of_window", otelmetric.WithDescription("Unpublished completions older than the delivery lookback observed over the last completed sweep; includes pre-feature history"))
	if err != nil {
		return fmt.Errorf("register apply notification out-of-window count: %w", err)
	}
	_, err = meter.RegisterCallback(func(_ context.Context, observer otelmetric.Observer) error {
		snapshot := applyNotificationBacklog.Load()
		if snapshot == nil {
			return nil
		}
		age := float64(0)
		if snapshot.Oldest != nil {
			age = max(time.Since(*snapshot.Oldest).Seconds(), 0)
		}
		observer.ObserveFloat64(lag, age, otelmetric.WithAttributes(EnvironmentAttribute("")))
		observer.ObserveInt64(outside, snapshot.OutOfWindow, otelmetric.WithAttributes(EnvironmentAttribute("")))
		return nil
	}, lag, outside)
	if err != nil {
		return fmt.Errorf("register apply notification gauges callback: %w", err)
	}
	return nil
}
