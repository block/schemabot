package serve

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

// syncBuffer collects log output from Build, which may write from more than one
// goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Split(strings.TrimSpace(b.buf.String()), "\n")
}

// buildAndCaptureLogs runs Build with a logger that writes JSON records into a
// buffer and returns the records it emitted. Build is expected to fail — the
// config names no storage — but it logs before it reaches storage resolution,
// which is the part under test.
func buildAndCaptureLogs(t *testing.T, withLogger func(*slog.Logger) *slog.Logger, opts ...Option) []string {
	t.Helper()
	// Force storage resolution to fail regardless of the developer's
	// environment, so Build never reaches a real database.
	t.Setenv("STORAGE_DSN", "")
	t.Setenv("MYSQL_DSN", "")

	var out syncBuffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	if withLogger != nil {
		logger = withLogger(logger)
	}

	_, err := Build(t.Context(), &api.ServerConfig{}, append([]Option{WithLogger(logger)}, opts...)...)
	require.Error(t, err, "Build must fail without a storage DSN rather than opening a connection")

	lines := out.lines()
	require.NotEmpty(t, lines[0], "Build must emit log records before it fails")
	return lines
}

func schemaBotVersionOf(t *testing.T, line string) string {
	t.Helper()
	var record map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &record), "log record must be valid JSON: %s", line)
	value, ok := record["schemabot_version"]
	require.True(t, ok, "record is missing schemabot_version: %s", line)
	version, ok := value.(string)
	require.True(t, ok, "schemabot_version must be a string: %s", line)
	return version
}

// A host binary that embeds SchemaBot supplies its own logger and passes no
// build info, so it cannot attach SchemaBot's version itself. Build derives the
// version from the module graph and attaches it to every record it logs, so an
// embedded deployment's logs still identify which SchemaBot is running.
func TestBuildAttachesSchemabotVersionForEmbeddedHosts(t *testing.T) {
	lines := buildAndCaptureLogs(t, nil)

	for _, line := range lines {
		version := schemaBotVersionOf(t, line)
		assert.NotEmpty(t, version, "every record must carry a schemabot_version: %s", line)
		assert.Equal(t, 1, strings.Count(line, `"schemabot_version":`),
			"exactly one schemabot_version per record: %s", line)
	}
}

// The standalone CLI attaches schemabot_version to the logger it passes in and
// supplies build info. Build must leave that path alone: deriving a second
// value would emit the key twice on every record.
func TestBuildDoesNotDuplicateSchemabotVersionWhenBuildInfoIsSet(t *testing.T) {
	const cliVersion = "v9.9.9-cli"

	lines := buildAndCaptureLogs(t,
		func(logger *slog.Logger) *slog.Logger {
			return logger.With("schemabot_version", cliVersion)
		},
		WithBuildInfo(cliVersion, "abc1234", "2026-01-01"),
	)

	for _, line := range lines {
		assert.Equal(t, 1, strings.Count(line, `"schemabot_version":`),
			"the caller's schemabot_version must not be duplicated: %s", line)
		assert.Equal(t, cliVersion, schemaBotVersionOf(t, line),
			"the caller's value must stand: %s", line)
	}
}

// versionFromBuildInfo reads SchemaBot's version out of the main module's
// dependency list, which is where the Go build records it when SchemaBot is
// embedded. A replace directive reports what the host actually builds against.
func TestVersionFromBuildInfo(t *testing.T) {
	tests := []struct {
		name string
		info *debug.BuildInfo
		want string
	}{
		{
			name: "reports the dependency version when schemabot is embedded",
			info: &debug.BuildInfo{Deps: []*debug.Module{
				{Path: "github.com/other/dep", Version: "v1.2.3"},
				{Path: schemabotModulePath, Version: "v0.1.68"},
			}},
			want: "v0.1.68",
		},
		{
			name: "prefers the replacement version",
			info: &debug.BuildInfo{Deps: []*debug.Module{{
				Path:    schemabotModulePath,
				Version: "v0.1.68",
				Replace: &debug.Module{Path: schemabotModulePath, Version: "v0.2.0-fork"},
			}}},
			want: "v0.2.0-fork",
		},
		{
			name: "reports a local path replacement as a development build",
			info: &debug.BuildInfo{Deps: []*debug.Module{{
				Path:    schemabotModulePath,
				Version: "v0.1.68",
				Replace: &debug.Module{Path: "../schemabot", Version: "(devel)"},
			}}},
			want: "(devel)",
		},
		{
			name: "falls back when a replacement names no version",
			info: &debug.BuildInfo{Deps: []*debug.Module{{
				Path:    schemabotModulePath,
				Version: "v0.1.68",
				Replace: &debug.Module{Path: "../schemabot"},
			}}},
			want: unknownModuleVersion,
		},
		{
			name: "falls back when the dependency names no version",
			info: &debug.BuildInfo{Deps: []*debug.Module{
				{Path: schemabotModulePath},
			}},
			want: unknownModuleVersion,
		},
		{
			name: "falls back when schemabot is not a dependency",
			info: &debug.BuildInfo{Deps: []*debug.Module{
				{Path: "github.com/other/dep", Version: "v1.2.3"},
			}},
			want: unknownModuleVersion,
		},
		{
			name: "falls back when the module graph is empty",
			info: &debug.BuildInfo{},
			want: unknownModuleVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, versionFromBuildInfo(tt.info))
		})
	}
}

// moduleVersion always names something. In this repo's own test binary
// SchemaBot is the main module rather than a dependency, so it reports the
// fallback; the value is never empty, which is what keeps the log field
// queryable.
func TestModuleVersionIsNeverEmpty(t *testing.T) {
	assert.NotEmpty(t, moduleVersion())
}
