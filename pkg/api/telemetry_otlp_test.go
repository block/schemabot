package api

import (
	"context"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

// fakeGRPCMetricsCollector counts OTLP metric export RPCs.
type fakeGRPCMetricsCollector struct {
	collectormetricspb.UnimplementedMetricsServiceServer
	mu      sync.Mutex
	exports int
}

func (f *fakeGRPCMetricsCollector) Export(_ context.Context, _ *collectormetricspb.ExportMetricsServiceRequest) (*collectormetricspb.ExportMetricsServiceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exports++
	return &collectormetricspb.ExportMetricsServiceResponse{}, nil
}

func (f *fakeGRPCMetricsCollector) exportCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.exports
}

// fakeGRPCTraceCollector counts OTLP trace export RPCs.
type fakeGRPCTraceCollector struct {
	collectortracepb.UnimplementedTraceServiceServer
	mu      sync.Mutex
	exports int
}

func (f *fakeGRPCTraceCollector) Export(_ context.Context, _ *collectortracepb.ExportTraceServiceRequest) (*collectortracepb.ExportTraceServiceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exports++
	return &collectortracepb.ExportTraceServiceResponse{}, nil
}

func (f *fakeGRPCTraceCollector) exportCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.exports
}

// A collector that only speaks gRPC (conventionally port 4317) is supported by
// setting OTEL_EXPORTER_OTLP_PROTOCOL=grpc: the exporters then send OTLP over
// gRPC instead of POSTing protobuf over HTTP, which such a collector rejects
// with 415 Unsupported Media Type.
func TestSetupTelemetryWithOTLPGRPCProtocol(t *testing.T) {
	lis, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	metricsCollector := &fakeGRPCMetricsCollector{}
	traceCollector := &fakeGRPCTraceCollector{}
	gs := grpc.NewServer()
	collectormetricspb.RegisterMetricsServiceServer(gs, metricsCollector)
	collectortracepb.RegisterTraceServiceServer(gs, traceCollector)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	// An http:// scheme tells the gRPC exporter to dial without TLS.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+lis.Addr().String())
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	tel, err := SetupTelemetry(logger)
	require.NoError(t, err)

	require.NotNil(t, tel.MetricsHandler)
	assert.NotNil(t, tel.tracerProvider, "tracerProvider should be set with OTLP endpoint")

	// Record a metric and a span so the shutdown flush has data to export.
	metrics.RecordPlan(t.Context(), "testrepo", "testdb", "pie", "staging", "success")
	tracer := otel.Tracer("test")
	_, span := tracer.Start(t.Context(), "grpc-test-span")
	span.End()

	require.NoError(t, tel.Shutdown(t.Context()))

	assert.Positive(t, metricsCollector.exportCount(), "expected OTLP metric export over gRPC")
	assert.Positive(t, traceCollector.exportCount(), "expected OTLP trace export over gRPC")
}

// A protocol the Go SDK exporters cannot speak must fail setup rather than be
// silently mapped to a transport the collector may reject.
func TestSetupTelemetryRejectsUnsupportedOTLPProtocol(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	_, err := SetupTelemetry(logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unsupported OTLP metrics protocol "http/json"`)
}

func TestOTLPProtocolResolution(t *testing.T) {
	tests := []struct {
		name    string
		generic string
		signal  string
		want    string
	}{
		{name: "default is http/protobuf", want: "http/protobuf"},
		{name: "generic protocol applies to all signals", generic: "grpc", want: "grpc"},
		{name: "signal-specific overrides generic", generic: "grpc", signal: "http/protobuf", want: "http/protobuf"},
		{name: "signal-specific alone", signal: "grpc", want: "grpc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", tt.generic)
			t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", tt.signal)
			assert.Equal(t, tt.want, otlpProtocol("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"))
		})
	}
}
