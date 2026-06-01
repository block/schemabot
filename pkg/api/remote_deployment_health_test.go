package api

import (
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/block/schemabot/pkg/tern"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Remote deployment health checks emit one steady-state gauge per configured
// deployment/environment so dashboards can show status even when no schema
// changes are running.
func TestCheckRemoteDeploymentHealthRecordsEachDeployment(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	config := &ServerConfig{
		TernDeployments: TernConfig{
			"pie":  TernEndpoints{"staging": "pie-staging:9090"},
			"sled": TernEndpoints{"staging": "sled-staging:9090"},
		},
	}
	ternClients := map[string]tern.Client{
		"pie/staging":  &mockTernClient{},
		"sled/staging": &mockTernClient{healthErr: errors.New("connection refused")},
	}
	svc := New(&mockStorage{}, config, ternClients, logger)

	svc.CheckRemoteDeploymentHealth(t.Context())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	healthByDeployment := make(map[string]int64)
	checkStatusByDeployment := make(map[string]string)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "schemabot.remote_deployment.health":
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok)
				for _, dp := range gauge.DataPoints {
					deployment, ok := dp.Attributes.Value(attribute.Key("deployment"))
					require.True(t, ok)
					environment, ok := dp.Attributes.Value(attribute.Key("environment"))
					require.True(t, ok)
					assert.Equal(t, "staging", environment.AsString())
					healthByDeployment[deployment.AsString()] = dp.Value
				}
			case "schemabot.remote_deployment.health_checks_total":
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				for _, dp := range sum.DataPoints {
					deployment, ok := dp.Attributes.Value(attribute.Key("deployment"))
					require.True(t, ok)
					status, ok := dp.Attributes.Value(attribute.Key("status"))
					require.True(t, ok)
					checkStatusByDeployment[deployment.AsString()] = status.AsString()
				}
			}
		}
	}

	assert.Equal(t, map[string]int64{"pie": 1, "sled": 0}, healthByDeployment)
	assert.Equal(t, map[string]string{"pie": "success", "sled": "error"}, checkStatusByDeployment)
}

func TestStartRemoteDeploymentHealthMonitorSkipsEmptyConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorage{}, &ServerConfig{}, nil, logger)

	svc.StartRemoteDeploymentHealthMonitor(t.Context())

	svc.remoteHealthMu.Lock()
	defer svc.remoteHealthMu.Unlock()
	assert.Nil(t, svc.remoteHealthCancel)
}
