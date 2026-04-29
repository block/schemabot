package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	oteloprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// Telemetry holds the OTel providers and the Prometheus HTTP handler.
// Call Shutdown to flush and release resources on server exit.
type Telemetry struct {
	// MetricsHandler serves Prometheus metrics at /metrics.
	MetricsHandler http.Handler

	meterProvider  *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
}

// SetupTelemetry initializes OpenTelemetry with a Prometheus metrics exporter
// (always on) and optional OTLP exporters for metrics and traces when
// OTEL_EXPORTER_OTLP_ENDPOINT is set.
func SetupTelemetry(logger *slog.Logger) (*Telemetry, error) {
	ctx := context.Background()

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("schemabot"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	// Prometheus exporter: always on, serves /metrics for pull-based scraping.
	registry := prometheus.NewRegistry()
	promExporter, err := oteloprom.New(oteloprom.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("create prometheus exporter: %w", err)
	}

	meterOpts := []sdkmetric.Option{
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExporter),
	}

	var tp *sdktrace.TracerProvider

	// OTLP exporters: enabled when OTEL_EXPORTER_OTLP_ENDPOINT is set.
	// The OTel SDK reads endpoint, headers, and protocol from standard env vars:
	//   OTEL_EXPORTER_OTLP_ENDPOINT   (e.g., https://otlp-gateway-us.grafana.net/otlp)
	//   OTEL_EXPORTER_OTLP_HEADERS    (e.g., Authorization=Basic ...)
	//   OTEL_EXPORTER_OTLP_PROTOCOL   (default: http/protobuf)
	if otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); otlpEndpoint != "" {
		otlpMeterReader, otlpTraceExporter, err := setupOTLP(ctx)
		if err != nil {
			return nil, fmt.Errorf("setup OTLP exporters: %w", err)
		}

		meterOpts = append(meterOpts, sdkmetric.WithReader(otlpMeterReader))

		tp = sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithBatcher(otlpTraceExporter),
		)
		otel.SetTracerProvider(tp)

		// Log only scheme+host to avoid leaking credentials from URL userinfo.
		redacted := otlpEndpoint
		if u, err := url.Parse(otlpEndpoint); err == nil {
			redacted = u.Scheme + "://" + u.Host
		}
		logger.Info("telemetry initialized",
			"metrics_endpoint", "/metrics",
			"otlp_endpoint", redacted,
		)
	} else {
		logger.Info("telemetry initialized", "metrics_endpoint", "/metrics")
	}

	mp := sdkmetric.NewMeterProvider(meterOpts...)
	otel.SetMeterProvider(mp)

	return &Telemetry{
		MetricsHandler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		meterProvider:  mp,
		tracerProvider: tp,
	}, nil
}

// Shutdown flushes pending telemetry data and releases resources.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	var firstErr error
	if t.tracerProvider != nil {
		if err := t.tracerProvider.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("tracer provider shutdown: %w", err)
		}
	}
	if t.meterProvider != nil {
		if err := t.meterProvider.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("meter provider shutdown: %w", err)
		}
	}
	return firstErr
}
