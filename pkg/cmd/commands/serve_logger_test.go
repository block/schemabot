package commands

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A server entrypoint supplies its own build info to serve, which means serve
// leaves the version field to the caller. Every record the entrypoint's logger
// emits therefore has to name the version itself, exactly once.
func TestNewServerLoggerNamesTheVersion(t *testing.T) {
	var out bytes.Buffer
	newServerLogger(&out, "v1.2.3").Info("building server")

	line := strings.TrimSpace(out.String())
	assert.Equal(t, 1, strings.Count(line, `"schemabot_version":`),
		"exactly one schemabot_version per record: %s", line)

	var record map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &record))
	assert.Equal(t, "v1.2.3", record["schemabot_version"])
}
