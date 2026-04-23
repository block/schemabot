package api

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	oteloprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// Telemetry holds the OTel providers and the Prometheus HTTP handler.
// Call Shutdown to flush and release resources on server exit.
type Telemetry struct {
	// MetricsHandler serves Prometheus metrics at /metrics.
	MetricsHandler http.Handler

	meterProvider *sdkmetric.MeterProvider
}

// SetupTelemetry initializes OpenTelemetry with a Prometheus metrics exporter.
// The returned Telemetry exposes a MetricsHandler that should be registered on
// the HTTP mux, and a Shutdown method for graceful cleanup.
func SetupTelemetry(logger *slog.Logger) (*Telemetry, error) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("schemabot"),
		),
	)
	if err != nil {
		return nil, err
	}

	// Use a dedicated Prometheus registry to avoid coupling to global state
	// and prevent duplicate collector registration if called more than once.
	registry := prometheus.NewRegistry()
	promExporter, err := oteloprom.New(oteloprom.WithRegisterer(registry))
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExporter),
	)
	otel.SetMeterProvider(mp)

	logger.Info("telemetry initialized", "metrics_endpoint", "/metrics")

	return &Telemetry{
		MetricsHandler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		meterProvider:  mp,
	}, nil
}

// Shutdown flushes pending telemetry data and releases resources.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil || t.meterProvider == nil {
		return nil
	}
	return t.meterProvider.Shutdown(ctx)
}
