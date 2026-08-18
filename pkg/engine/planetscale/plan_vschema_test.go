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
