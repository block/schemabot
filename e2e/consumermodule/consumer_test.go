// Package consumermodule verifies schemabot's startup surface from the
// perspective of a host binary that embeds schemabot as a Go module. This
// module intentionally pins a newer OpenTelemetry SDK than the parent module:
// host binaries resolve their own SDK version, so schemabot's telemetry setup
// must tolerate a default resource whose semconv schema URL differs from the
// one schemabot compiles against.
package consumermodule

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/api"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/resource"
)

// TestSetupTelemetryWithNewerHostSDK proves that a host binary whose OTel SDK
// is newer than schemabot's can initialize telemetry during startup. The host
// SDK's resource.Default() carries its own semconv schema URL; SetupTelemetry
// must succeed even when that URL differs from schemabot's pinned semconv
// version, otherwise every embedding service fails to start whenever the two
// modules upgrade OTel independently.
func TestSetupTelemetryWithNewerHostSDK(t *testing.T) {
	// Pin the exporter selection to the Prometheus-only path regardless of
	// the developer's environment: SetupTelemetry builds OTLP exporters when
	// OTEL_EXPORTER_OTLP_ENDPOINT is non-empty, which is a different code
	// path than the one this test guards.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	// Guard the scenario: the host SDK's default resource must carry a
	// non-empty schema URL that differs from schemabot's semconv pin,
	// otherwise this module no longer produces a schema URL conflict and the
	// test proves nothing. Bump the go.opentelemetry.io/otel/sdk requirement
	// in this module's go.mod so it is ahead of schemabot's pin.
	hostSchemaURL := resource.Default().SchemaURL()
	require.NotEmpty(t, hostSchemaURL,
		"host SDK default resource must carry a schema URL for the conflict scenario to exist")
	require.NotEqual(t, api.TelemetrySchemaURL, hostSchemaURL,
		"host SDK default resource must carry a different schema URL than schemabot's semconv pin; bump this module's go.opentelemetry.io/otel/sdk requirement")

	tel, err := api.SetupTelemetry(slog.Default())
	require.NoError(t, err)
	require.NotNil(t, tel)

	// The merged resource must keep schemabot's service identity even when
	// the base resource comes from the newer host SDK. The Prometheus
	// exporter surfaces resource attributes on the target_info metric.
	rec := httptest.NewRecorder()
	tel.MetricsHandler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `service_name="schemabot"`,
		"merged telemetry resource must retain schemabot's service.name")

	require.NoError(t, tel.Shutdown(t.Context()))
}

// TestReplaceDirectivesMirrorParent guards the module's hand-copied replace
// directives against drifting from the parent module's. Replace directives do
// not propagate across module boundaries, so this module mirrors the parent's
// fork pins by hand; a stale mirror silently builds this module against a
// different fork revision than the repo actually ships.
func TestReplaceDirectivesMirrorParent(t *testing.T) {
	parentReplaces := parseReplaceDirectives(t, filepath.Join("..", "..", "go.mod"))
	localReplaces := parseReplaceDirectives(t, "go.mod")

	// The schemabot self-replace points this module at the enclosing checkout
	// and has no parent counterpart.
	delete(localReplaces, "github.com/block/schemabot")

	require.Equal(t, parentReplaces, localReplaces,
		"e2e/consumermodule/go.mod must mirror the parent module's replace directives; update the mirrored replace lines to match the parent go.mod")
}

// parseReplaceDirectives returns a go.mod's replace directives keyed by the
// replaced module path, with the replacement target as the value. Both the
// single-line and block forms are handled.
func parseReplaceDirectives(t *testing.T, path string) map[string]string {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	replaces := make(map[string]string)
	inBlock := false
	for line := range strings.SplitSeq(string(data), "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		switch {
		case line == "replace (":
			inBlock = true
		case inBlock && line == ")":
			inBlock = false
		case inBlock || strings.HasPrefix(line, "replace "):
			spec := strings.TrimSpace(strings.TrimPrefix(line, "replace "))
			lhs, rhs, ok := strings.Cut(spec, "=>")
			if !ok {
				continue
			}
			lhsFields := strings.Fields(lhs)
			if len(lhsFields) == 0 {
				continue
			}
			replaces[lhsFields[0]] = strings.Join(strings.Fields(rhs), " ")
		}
	}
	return replaces
}
