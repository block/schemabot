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

// telemetryResource merges the base resource with the schemabot service-name
// override. The override is schemaless so the merge never conflicts with the
// base resource's schema URL, which is owned by the host binary's SDK
// version. Host binaries embed this server and upgrade their OTel SDK
// independently; a pinned schema URL here would make that upgrade fail
// resource creation.
func telemetryResource(base *resource.Resource) (*resource.Resource, error) {
	return resource.Merge(
		base,
		resource.NewSchemaless(semconv.ServiceName("schemabot")),
	)
}

// SetupTelemetry initializes OpenTelemetry with a Prometheus metrics exporter
// (always on) and optional OTLP exporters for metrics and traces when
// OTEL_EXPORTER_OTLP_ENDPOINT is set.
func SetupTelemetry(logger *slog.Logger) (*Telemetry, error) {
	ctx := context.Background()

	res, err := telemetryResource(resource.Default())
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
	// Endpoint and headers are read from standard env vars by the SDK; the
	// transport protocol is resolved by setupOTLP:
	//   OTEL_EXPORTER_OTLP_ENDPOINT   (e.g., https://otlp-gateway-us.grafana.net/otlp)
	//   OTEL_EXPORTER_OTLP_HEADERS    (e.g., Authorization=Basic ...)
	//   OTEL_EXPORTER_OTLP_PROTOCOL   (grpc or http/protobuf; default: http/protobuf)
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
			"otlp_metrics_protocol", otlpProtocol("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"),
			"otlp_traces_protocol", otlpProtocol("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"),
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
