package apitypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A VSchema change that removes a vindex, table routing entry, or
// column-vindex association gates the apply behind the unsafe opt-in, the
// same as destructive DDL: the removal changes Vitess query routing the
// moment it lands.
func TestPlanResponse_UnsafeChangesIncludesVSchemaDeletions(t *testing.T) {
	meta, err := EncodeVSchemaDeletions([]VSchemaDeletion{
		{Kind: "vindex", Name: "email_lookup", Reason: `vindex "email_lookup" is removed: Vitess immediately stops using it`},
		{Kind: "column_vindex", Name: "users.email_lookup", Reason: `table "users" no longer uses vindex "email_lookup"`},
	})
	require.NoError(t, err)

	resp := &PlanResponse{
		Changes: []*SchemaChangeResponse{{
			Namespace: "commerce",
			Metadata: map[string]string{
				VSchemaChangedMetadataKey:   "true",
				VSchemaDeletionsMetadataKey: meta,
			},
		}},
	}

	changes := resp.UnsafeChanges()
	require.Len(t, changes, 2)
	assert.Equal(t, "commerce/vschema.json", changes[0].Table)
	assert.Equal(t, VSchemaChangeType, changes[0].ChangeType)
	assert.Contains(t, changes[0].Reason, "email_lookup")
	assert.Contains(t, changes[1].Reason, `table "users"`)
}

func TestPlanResponse_UnsafeChangesVSchemaAdditionsOnlyStaysSafe(t *testing.T) {
	resp := &PlanResponse{
		Changes: []*SchemaChangeResponse{{
			Namespace: "commerce",
			Metadata: map[string]string{
				VSchemaChangedMetadataKey: "true",
				VSchemaDiffMetadataKey:    "+ added lines only",
			},
		}},
	}
	assert.Empty(t, resp.UnsafeChanges())
}

func TestPlanResponse_UnsafeChangesUndecodableVSchemaDeletionsFailClosed(t *testing.T) {
	resp := &PlanResponse{
		Changes: []*SchemaChangeResponse{{
			Namespace: "commerce",
			Metadata: map[string]string{
				VSchemaChangedMetadataKey:   "true",
				VSchemaDeletionsMetadataKey: "{corrupt",
			},
		}},
	}

	changes := resp.UnsafeChanges()
	require.Len(t, changes, 1)
	assert.Equal(t, "commerce/vschema.json", changes[0].Table)
	assert.Contains(t, changes[0].Reason, "could not be decoded")
}

func TestEncodeVSchemaDeletions_EmptyOmitsKey(t *testing.T) {
	meta, err := EncodeVSchemaDeletions(nil)
	require.NoError(t, err)
	assert.Empty(t, meta)
}
