package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
)

// A table's ETA travels from the progress proto through the HTTP response and
// its JSON encoding unchanged, so CLI and API consumers see the same figure
// the driver stored.
func TestProgressResponseFromProtoCarriesTableETA(t *testing.T) {
	resp := progressResponseFromProto(&ternv1.ProgressResponse{
		State:  ternv1.State_STATE_RUNNING,
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Tables: []*ternv1.TableProgress{
			{
				TableName:       "users",
				Namespace:       "testdb",
				Status:          "running",
				RowsCopied:      250000,
				RowsTotal:       500000,
				PercentComplete: 50,
				EtaSeconds:      540,
			},
		},
	})

	require.Len(t, resp.Tables, 1)
	assert.Equal(t, int64(540), resp.Tables[0].ETASeconds)

	encoded, err := json.Marshal(resp)
	require.NoError(t, err)
	var decoded apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	require.Len(t, decoded.Tables, 1)
	assert.Equal(t, "users", decoded.Tables[0].TableName)
	assert.Equal(t, state.Apply.Running, decoded.State)
	assert.Equal(t, int64(250000), decoded.Tables[0].RowsCopied)
	assert.Equal(t, int64(500000), decoded.Tables[0].RowsTotal)
	assert.Equal(t, int32(50), decoded.Tables[0].PercentComplete)
	assert.Equal(t, int64(540), decoded.Tables[0].ETASeconds)
}
