// Package consumermodule verifies schemabot's startup surface from the
// perspective of a host binary that embeds schemabot as a Go module. This
// module intentionally pins a newer OpenTelemetry SDK than the parent module:
// host binaries resolve their own SDK version, so schemabot's telemetry setup
// must tolerate a default resource whose semconv schema URL differs from the
// one schemabot compiles against.
package consumermodule

import (
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/api"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/resource"
	// Must match the semconv version imported by pkg/api/telemetry.go so the
	// precondition below compares against schemabot's actual pin.
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// TestSetupTelemetryWithNewerHostSDK proves that a host binary whose OTel SDK
// is newer than schemabot's can initialize telemetry during startup. The host
// SDK's resource.Default() carries its own semconv schema URL; SetupTelemetry
// must succeed even when that URL differs from schemabot's pinned semconv
// version, otherwise every embedding service fails to start whenever the two
// modules upgrade OTel independently.
func TestSetupTelemetryWithNewerHostSDK(t *testing.T) {
	// Guard the scenario: if these ever match, this module's OTel SDK pin no
	// longer produces a schema URL conflict and the test proves nothing. Bump
	// the go.opentelemetry.io/otel/sdk requirement in this module's go.mod so
	// it is ahead of the semconv version imported by pkg/api/telemetry.go.
	require.NotEqual(t, semconv.SchemaURL, resource.Default().SchemaURL(),
		"host SDK default resource must carry a different schema URL than schemabot's semconv pin; bump this module's go.opentelemetry.io/otel/sdk requirement")

	tel, err := api.SetupTelemetry(slog.Default())
	require.NoError(t, err)
	require.NotNil(t, tel)

	require.NoError(t, tel.Shutdown(t.Context()))
}
