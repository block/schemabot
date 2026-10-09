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
	resp := PlanContentFromStorage(&storage.Plan{
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

// A stored plan creates a table in one namespace, which the engine finalizes
// as part of that DDL, and has nothing but a finalize in another. Only the
// namespace whose only work is the finalize counts under finalize_count, so
// GET /api/plans lists the plan the way its plan comment summarizes it.
func TestPlanSummaryFromStorageCountsOnlyFinalizeOnlyNamespaces(t *testing.T) {
	summary := planSummaryFromStorage(&storage.Plan{
		PlanIdentifier: "plan-finalize-beside-ddl",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeStrata,
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {
				Tables:   []storage.TableChange{{Table: "refund_notes", DDL: "CREATE TABLE `refund_notes` (`id` bigint NOT NULL, PRIMARY KEY (`id`))", Operation: "create"}},
				Finalize: true,
			},
			"ledger": {Finalize: true},
			"audit": {
				Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
				Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables": {"events": {}}}`},
				Finalize:  true,
			},
		},
	})

	assert.Equal(t, map[string]int{"create": 1}, summary.ChangeCounts)
	assert.Equal(t, 1, summary.VSchemaChangeCount)
	assert.Equal(t, 1, summary.FinalizeCount)
}

// A stored plan whose VSchema change the engine generated entirely from the
// plan's DDL lists by that DDL alone, with neither a VSchema change nor a
// finalize count, and reads back without a VSchema change to show, the way the
// live plan showed it, while still reporting the VSchema change to anything
// that acts on it.
func TestStoredPlanShowsGeneratedVSchemaChangeAsItsDDL(t *testing.T) {
	plan := &storage.Plan{
		PlanIdentifier: "plan-generated-vschema",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeStrata,
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {
				Tables:    []storage.TableChange{{Table: "refund_notes", DDL: "CREATE TABLE `refund_notes` (`id` bigint NOT NULL, PRIMARY KEY (`id`))", Operation: "create"}},
				Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true", storage.PlanMetadataVSchemaGeneratedOnly: "true"},
				Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables": {"refund_notes": {}}}`},
				Finalize:  true,
			},
		},
	}

	summary := planSummaryFromStorage(plan)
	assert.Equal(t, map[string]int{"create": 1}, summary.ChangeCounts)
	assert.Zero(t, summary.FinalizeCount)
	assert.Zero(t, summary.VSchemaChangeCount)

	resp := PlanContentFromStorage(plan)
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
	resp := PlanContentFromStorage(plan)
	require.Len(t, resp.Changes, 1)
	assert.True(t, resp.Changes[0].ShowsVSchemaChange())
}

// A planner can record the blocked verdict in any case. The summary counts it
// the way apply admission reads it, so GET /api/plans never lists a plan whose
// apply is refused as having nothing blocked.
func TestPlanSummaryFromStorageCountsBlockedVerdictInAnyCase(t *testing.T) {
	summary := planSummaryFromStorage(&storage.Plan{
		PlanIdentifier: "plan-blocked-upper",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {Tables: []storage.TableChange{
				{Table: "orders", Operation: "alter", ExecutionMode: "BLOCKED"},
				{Table: "refunds", Operation: "alter", ExecutionMode: "direct"},
			}},
		},
	})

	assert.Equal(t, 1, summary.BlockedCount)
}
