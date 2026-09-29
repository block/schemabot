package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

// A stored plan whose only work is a finalize the engine asked for is read
// back with the request under the key it was planned with, so the stored plan
// still reads as having changes.
func TestPlanContentFromStorageReportsFinalizeRequest(t *testing.T) {
	resp := planContentFromStorage(&storage.Plan{
		PlanIdentifier: "plan-finalize-only",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeStrata,
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {Finalize: true},
		},
	})

	require.Len(t, resp.Changes, 1)
	assert.Equal(t, map[string]string{apitypes.NeedsFinalizerMetadataKey: "true"}, resp.Changes[0].Metadata)
	assert.True(t, resp.HasChanges())
	assert.False(t, resp.Changes[0].HasVSchemaChange())
}

// A stored plan whose only work is a finalize lists with a finalize count, so
// GET /api/plans does not present it as a plan with nothing to apply.
func TestPlanSummaryFromStorageCountsFinalizeRequests(t *testing.T) {
	summary := planSummaryFromStorage(&storage.Plan{
		PlanIdentifier: "plan-finalize-only",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeStrata,
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {Finalize: true},
			"ledger":   {},
		},
	})

	assert.Equal(t, 1, summary.FinalizeCount)
	assert.Empty(t, summary.ChangeCounts)
	assert.Zero(t, summary.VSchemaChangeCount)
}
