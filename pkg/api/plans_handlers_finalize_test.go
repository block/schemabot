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

// A stored plan whose VSchema change the engine generated entirely from the
// plan's DDL lists and reads back as a finalize, the way the live plan showed
// it, while still reporting the VSchema change to anything that acts on it.
func TestStoredPlanShowsGeneratedVSchemaChangeAsFinalize(t *testing.T) {
	plan := &storage.Plan{
		PlanIdentifier: "plan-generated-vschema",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeStrata,
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {
				Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true", storage.PlanMetadataVSchemaGeneratedOnly: "true"},
				Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables": {"refund_notes": {}}}`},
				Finalize:  true,
			},
		},
	}

	summary := planSummaryFromStorage(plan)
	assert.Equal(t, 1, summary.FinalizeCount)
	assert.Zero(t, summary.VSchemaChangeCount)

	resp := planContentFromStorage(plan)
	require.Len(t, resp.Changes, 1)
	assert.True(t, resp.Changes[0].HasVSchemaChange())
	assert.False(t, resp.Changes[0].ShowsVSchemaChange())
}

// A stored VSchema change that records a deletion is shown and counted as a
// VSchema change even when it is marked generated from the DDL, so an unsafe
// VSchema change never reads as a plain finalize.
func TestStoredPlanShowsGeneratedVSchemaChangeWithDeletion(t *testing.T) {
	plan := &storage.Plan{
		PlanIdentifier: "plan-generated-vschema-drop",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeStrata,
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {
				Metadata: map[string]string{
					storage.PlanMetadataVSchemaChanged:       "true",
					storage.PlanMetadataVSchemaGeneratedOnly: "true",
					storage.PlanMetadataVSchemaDeletions:     `[{"kind":"table","name":"refund_notes","reason":"removing table refund_notes changes query routing"}]`,
				},
				Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables": {}}`},
				Finalize:  true,
			},
		},
	}

	assert.Equal(t, 1, planSummaryFromStorage(plan).VSchemaChangeCount)
	resp := planContentFromStorage(plan)
	require.Len(t, resp.Changes, 1)
	assert.True(t, resp.Changes[0].ShowsVSchemaChange())
}
