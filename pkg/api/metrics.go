package api

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

// Meter name used for all SchemaBot metrics.
const meterName = "schemabot"

// RecordPlan increments the plans counter with database, environment, and status attributes.
// Status should be "success" or "error".
//
// The OTel SDK deduplicates instruments with the same name, so repeated calls
// to Int64Counter are cheap after the first registration.
func RecordPlan(ctx context.Context, database, environment, status string) {
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.plans.total",
		otelmetric.WithDescription("Total number of plan operations"),
		otelmetric.WithUnit("{plan}"),
	)
	if err != nil {
		slog.Warn("failed to create plans counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("status", status),
		),
	)
}
