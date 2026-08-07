package api

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// OTLP transport protocols from the OTel exporter specification. http/json is
// not implemented by the Go SDK exporters, so it is rejected at setup rather
// than silently mapped to another transport.
const (
	otlpProtocolGRPC         = "grpc"
	otlpProtocolHTTPProtobuf = "http/protobuf"
)

// otlpProtocol resolves the OTLP transport protocol for one signal: the
// signal-specific env var overrides the generic OTEL_EXPORTER_OTLP_PROTOCOL,
// and the OTel default is http/protobuf.
func otlpProtocol(signalVar string) string {
	if p := os.Getenv(signalVar); p != "" {
		return p
	}
	if p := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); p != "" {
		return p
	}
	return otlpProtocolHTTPProtobuf
}

// setupOTLP creates OTLP exporters for metrics and traces. Endpoint and
// headers are read from the standard OTel env vars by the SDK; the transport
// is chosen here from OTEL_EXPORTER_OTLP_PROTOCOL (and its signal-specific
// overrides) because the Go SDK fixes the transport at exporter construction
// and does not read the protocol env vars itself. A collector listening with
// gRPC (conventionally port 4317) requires protocol "grpc" — the HTTP exporter
// would POST protobuf to it and be rejected.
func setupOTLP(ctx context.Context) (sdkmetric.Reader, sdktrace.SpanExporter, error) {
	metricExporter, err := newOTLPMetricExporter(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}

	traceExporter, err := newOTLPTraceExporter(ctx)
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

func newOTLPMetricExporter(ctx context.Context) (sdkmetric.Exporter, error) {
	switch proto := otlpProtocol("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"); proto {
	case otlpProtocolGRPC:
		return otlpmetricgrpc.New(ctx)
	case otlpProtocolHTTPProtobuf:
		return otlpmetrichttp.New(ctx)
	default:
		return nil, fmt.Errorf("unsupported OTLP metrics protocol %q: supported protocols are %q and %q", proto, otlpProtocolGRPC, otlpProtocolHTTPProtobuf)
	}
}

func newOTLPTraceExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	switch proto := otlpProtocol("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"); proto {
	case otlpProtocolGRPC:
		return otlptracegrpc.New(ctx)
	case otlpProtocolHTTPProtobuf:
		return otlptracehttp.New(ctx)
	default:
		return nil, fmt.Errorf("unsupported OTLP traces protocol %q: supported protocols are %q and %q", proto, otlpProtocolGRPC, otlpProtocolHTTPProtobuf)
	}
}
