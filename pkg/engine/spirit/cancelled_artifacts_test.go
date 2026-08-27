package spirit

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

func TestEngine_ReleaseCancelledArtifacts_RejectsIncompleteRequests(t *testing.T) {
	tests := []struct {
		name    string
		req     *engine.ReleaseArtifactsRequest
		wantErr string
	}{
		{
			name:    "nil request",
			req:     nil,
			wantErr: "release artifacts request is required",
		},
		{
			name:    "no credentials",
			req:     &engine.ReleaseArtifactsRequest{Database: "shop"},
			wantErr: "DSN credentials required",
		},
		{
			name:    "empty DSN",
			req:     &engine.ReleaseArtifactsRequest{Database: "shop", Credentials: &engine.Credentials{}},
			wantErr: "DSN credentials required",
		},
		{
			name: "no database in the DSN or the request",
			req: &engine.ReleaseArtifactsRequest{
				Credentials: &engine.Credentials{DSN: "user:pass@tcp(127.0.0.1:3306)/"},
			},
			wantErr: "database is required",
		},
	}

	eng := New(Config{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eng.ReleaseCancelledArtifacts(t.Context(), tt.req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Nil(t, result)
		})
	}
}

// The artifact names must come from Spirit's own helpers so they match what a
// run actually created, including the deterministic truncation Spirit applies
// to keep a name within MySQL's identifier limit.
func TestArtifactNames_MatchSpiritNaming(t *testing.T) {
	longTable := strings.Repeat("a", 70)
	tables := []string{"orders", longTable}

	data := dataBearingArtifacts(tables)
	assert.Equal(t, []string{
		utils.NewTableName("orders"),
		utils.OldTableName("orders"),
		utils.NewTableName(longTable),
		utils.OldTableName(longTable),
	}, data)

	metadata := metadataArtifacts(tables)
	assert.Equal(t, []string{
		utils.CheckpointTableName("orders"),
		utils.CheckpointTableName(longTable),
		sharedCheckpointTable,
		deferredCutoverSentinelTable,
	}, metadata)

	for _, name := range append(data, metadata...) {
		assert.LessOrEqual(t, len(name), utils.MaxTableNameLength,
			"artifact name %q must fit MySQL's identifier limit", name)
	}
}

// The two schema-level artifacts belong to the schema change rather than to any
// one table, so they are released even when the change touched no tables.
func TestMetadataArtifacts_AlwaysIncludesSchemaLevelTables(t *testing.T) {
	assert.Equal(t,
		[]string{sharedCheckpointTable, deferredCutoverSentinelTable},
		metadataArtifacts(nil))
	assert.Empty(t, dataBearingArtifacts(nil))
}
