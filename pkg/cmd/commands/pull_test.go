package commands

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

func TestWritePullSchemaResponseReturnsSchemaAsJSON(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, writePullSchemaResponse(&out, validPullSchemaResponse()))

	var got apitypes.PullSchemaResponse
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	assert.Equal(t, "orders", got.Database)
	assert.Equal(t, "mysql", got.Type)
	assert.Equal(t, "production", got.Environment)
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint NOT NULL);\n", got.Namespaces["orders"].Tables["users"])
}

// The pull command resolves the database type from the server's registered
// config, so --type is optional (defaults to empty) and accepts any engine the
// server supports rather than being pinned to one type.
func TestPullCmdTypeIsOptionalAndAcceptsAnyEngine(t *testing.T) {
	var cli struct {
		Pull PullCmd `cmd:""`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	_, err = parser.Parse([]string{"pull", "-d", "boardgames", "-e", "staging"})
	require.NoError(t, err)
	assert.Empty(t, cli.Pull.Type, "type must default to empty so the server resolves it from config")

	_, err = parser.Parse([]string{"pull", "-d", "boardgames", "-e", "staging", "--type", "vitess"})
	require.NoError(t, err, "a non-mysql type must be accepted")
	assert.Equal(t, "vitess", cli.Pull.Type)
}
