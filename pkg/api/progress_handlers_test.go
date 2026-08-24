package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
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

// Each stored apply_operation's operation key travels through the progress
// response and its JSON encoding, so an operator triaging a multi-operation
// apply with `status --json` can correlate each JSON operation row with the
// stored apply_operation and its data-plane work.
func TestProgressOperationsCarryOperationKey(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{
			ID:           1,
			Deployment:   "region-a",
			OperationKey: "commerce/-80/users",
			State:        state.Apply.Running,
		},
		{
			ID:           2,
			Deployment:   "region-a",
			OperationKey: "commerce/80-/users",
			State:        state.Apply.Completed,
		},
	}

	responses, _ := progressOperationsFromRows(ops)
	require.Len(t, responses, 2)
	assert.Equal(t, "commerce/-80/users", responses[0].OperationKey)
	assert.Equal(t, "commerce/80-/users", responses[1].OperationKey)

	encoded, err := json.Marshal(&apitypes.ProgressResponse{
		State:      state.Apply.Running,
		Operations: responses,
	})
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"operation_key":"commerce/-80/users"`)

	var decoded apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded.Operations, 2)
	assert.Equal(t, "commerce/-80/users", decoded.Operations[0].OperationKey)
	assert.Equal(t, "commerce/80-/users", decoded.Operations[1].OperationKey)
}
