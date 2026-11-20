package metrics

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

// EnvironmentFreezeSample is one environment's freeze gauges in a snapshot.
type EnvironmentFreezeSample struct {
	Environment string
	// Whole is true while a freeze of the whole environment is in effect.
	Whole bool
	// Partial is true while a freeze narrowed to some deployments or
	// databases is in effect.
	Partial bool
	// OpenFor is the age of the oldest open-ended freeze in effect, or 0.
	OpenFor time.Duration
}

// activeEnvironmentFreezes is the snapshot every collection reads. The
// freeze monitor replaces it whole, so one collection never mixes two
// snapshots.
var activeEnvironmentFreezes atomic.Pointer[[]EnvironmentFreezeSample]

// PublishEnvironmentFreezes replaces the freeze snapshot the gauges report.
// A collection reads either the old snapshot or the new one, never a mix, so
// a freeze that holds throughout, such as a whole-environment freeze replaced
// by a partial one, never reads as both scopes at 0. Until the next publish,
// every collection re-reports this snapshot; a nil snapshot reports nothing.
func PublishEnvironmentFreezes(samples []EnvironmentFreezeSample) {
	if samples == nil {
		activeEnvironmentFreezes.Store(nil)
		return
	}
	snapshot := slices.Clone(samples)
	activeEnvironmentFreezes.Store(&snapshot)
}

// RegisterEnvironmentFreezeMetrics registers the freeze gauges on provider.
// Call it once during metrics-provider setup.
//
// schemabot.environment.frozen is 1 while a freeze of the scope is in effect
// in the environment, else 0. Scope is FreezeScopeEnvironment or
// FreezeScopePartial and both are reported for every environment in the
// snapshot, so max by environment answers "is anything frozen". The unit is
// empty so the exporter keeps the bare schemabot_environment_frozen name
// rather than adding _ratio.
//
// schemabot.environment.freeze_open_seconds is the age of the oldest
// open-ended freeze in effect, or 0. A planned freeze has an end time and
// cannot be forgotten, so it is not counted.
func RegisterEnvironmentFreezeMetrics(provider otelmetric.MeterProvider) error {
	meter := provider.Meter(meterName)
	frozen, err := meter.Int64ObservableGauge("schemabot.environment.frozen",
		otelmetric.WithDescription("1 while a freeze of this scope is in effect in the environment, else 0"),
		otelmetric.WithUnit(""))
	if err != nil {
		return fmt.Errorf("register environment frozen gauge: %w", err)
	}
	openSeconds, err := meter.Int64ObservableGauge("schemabot.environment.freeze_open_seconds",
		otelmetric.WithDescription("Age in seconds of the oldest open-ended freeze in effect in the environment, 0 when none is"),
		otelmetric.WithUnit("s"))
	if err != nil {
		return fmt.Errorf("register environment freeze open seconds gauge: %w", err)
	}
	_, err = meter.RegisterCallback(func(_ context.Context, observer otelmetric.Observer) error {
		snapshot := activeEnvironmentFreezes.Load()
		if snapshot == nil {
			return nil
		}
		for _, sample := range *snapshot {
			environment := EnvironmentAttribute(sample.Environment)
			observer.ObserveInt64(frozen, boolGaugeValue(sample.Whole),
				otelmetric.WithAttributes(environment, attribute.String("scope", FreezeScopeEnvironment)))
			observer.ObserveInt64(frozen, boolGaugeValue(sample.Partial),
				otelmetric.WithAttributes(environment, attribute.String("scope", FreezeScopePartial)))
			observer.ObserveInt64(openSeconds, int64(max(sample.OpenFor, 0).Seconds()),
				otelmetric.WithAttributes(environment))
		}
		return nil
	}, frozen, openSeconds)
	if err != nil {
		return fmt.Errorf("register environment freeze gauges callback: %w", err)
	}
	return nil
}

func boolGaugeValue(v bool) int64 {
	if v {
		return 1
	}
	return 0
}
