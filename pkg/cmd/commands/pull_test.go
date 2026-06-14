package commands

import (
	"bytes"
	"encoding/json"
	"testing"

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
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint NOT NULL);\n", got.SchemaFiles["orders"].Files["users.sql"])
}
