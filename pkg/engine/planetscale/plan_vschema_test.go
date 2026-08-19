package planetscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

func TestVSchemaDeletionsMetadata(t *testing.T) {
	current := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}, "email_lookup": {"type": "consistent_lookup_unique"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
	desired := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`

	meta, err := vschemaDeletionsMetadata(current, desired)
	require.NoError(t, err)
	require.NotEmpty(t, meta)

	deletions, err := apitypes.ParseVSchemaDeletions(map[string]string{apitypes.VSchemaDeletionsMetadataKey: meta})
	require.NoError(t, err)
	require.Len(t, deletions, 1)
	assert.Equal(t, "vindex", deletions[0].Kind)
	assert.Equal(t, "email_lookup", deletions[0].Name)
}

func TestVSchemaDeletionsMetadata_NoRemovals(t *testing.T) {
	current := `{"tables": {"widgets": {}}}`
	desired := `{"tables": {"widgets": {}, "gadgets": {}}}`

	meta, err := vschemaDeletionsMetadata(current, desired)
	require.NoError(t, err)
	assert.Empty(t, meta)
}

func TestVSchemaDeletionsMetadata_UnparseableFailsClosed(t *testing.T) {
	_, err := vschemaDeletionsMetadata("{corrupt", `{"tables": {}}`)
	require.Error(t, err)
}

func TestVSchemaMutationsMetadata(t *testing.T) {
	current := `{"sharded": true, "vindexes": {"user_idx": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "user_idx"}]}}}`
	desired := `{"sharded": true, "vindexes": {"user_idx": {"type": "xxhash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "user_idx"}]}}}`

	meta, err := vschemaMutationsMetadata(current, desired)
	require.NoError(t, err)
	require.NotEmpty(t, meta)

	mutations, err := apitypes.ParseVSchemaMutations(map[string]string{apitypes.VSchemaMutationsMetadataKey: meta})
	require.NoError(t, err)
	require.Len(t, mutations, 1)
	assert.Equal(t, "vindex_type", mutations[0].Kind)
	assert.Equal(t, "user_idx", mutations[0].Name)
}

func TestVSchemaMutationsMetadata_NoMutations(t *testing.T) {
	current := `{"sharded": true, "vindexes": {"user_idx": {"type": "hash"}}, "tables": {}}`
	desired := `{"sharded": true, "vindexes": {"user_idx": {"type": "hash"}, "extra": {"type": "hash"}}, "tables": {}}`

	meta, err := vschemaMutationsMetadata(current, desired)
	require.NoError(t, err)
	assert.Empty(t, meta)
}

func TestVSchemaMutationsMetadata_UnparseableFailsClosed(t *testing.T) {
	_, err := vschemaMutationsMetadata("{corrupt", `{"tables": {}}`)
	require.Error(t, err)
}
