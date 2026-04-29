package api

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// setupOTLP creates OTLP HTTP exporters for metrics and traces.
// Configuration (endpoint, headers, protocol) is read from standard OTel
// environment variables by the SDK — no manual parsing needed.
//
// Only OTEL_EXPORTER_OTLP_ENDPOINT is checked to gate OTLP enablement.
// Signal-specific vars (OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, etc.) are
// not checked but are still respected by the SDK if the generic endpoint is set.
func setupOTLP(ctx context.Context) (sdkmetric.Reader, sdktrace.SpanExporter, error) {
	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}

	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		if shutdownErr := metricExporter.Shutdown(ctx); shutdownErr != nil {
			return nil, nil, fmt.Errorf("create OTLP trace exporter: %w (also failed to shut down metric exporter: %w)", err, shutdownErr)
		}
		return nil, nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}

	// Use the SDK default interval (60s). Override with OTEL_METRIC_EXPORT_INTERVAL.
	reader := sdkmetric.NewPeriodicReader(metricExporter)

	return reader, traceExporter, nil
}
