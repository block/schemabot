package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVSchemaPlanMetadata(t *testing.T) {
	t.Run("no vschema work returns nil", func(t *testing.T) {
		assert.Nil(t, VSchemaPlanMetadata(nil))
		assert.Nil(t, VSchemaPlanMetadata(map[string]string{}))
		assert.Nil(t, VSchemaPlanMetadata(map[string]string{"vschema": "some rendered diff"}))
	})

	t.Run("flag without deletions persists the flag alone", func(t *testing.T) {
		got := VSchemaPlanMetadata(map[string]string{
			PlanMetadataVSchemaChanged: "true",
		})
		assert.Equal(t, map[string]string{PlanMetadataVSchemaChanged: "true"}, got)
	})

	t.Run("flag with deletions and mutations persists all three and drops display metadata", func(t *testing.T) {
		got := VSchemaPlanMetadata(map[string]string{
			PlanMetadataVSchemaChanged:   "true",
			PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
			PlanMetadataVSchemaMutations: `[{"kind":"vindex_type","name":"user_idx","reason":"changing vindex user_idx type re-computes keyspace ids"}]`,
			"vschema":                    "some rendered diff",
		})
		assert.Equal(t, map[string]string{
			PlanMetadataVSchemaChanged:   "true",
			PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
			PlanMetadataVSchemaMutations: `[{"kind":"vindex_type","name":"user_idx","reason":"changing vindex user_idx type re-computes keyspace ids"}]`,
		}, got)
	})
}

// TestPlanUnsafeVSchemaChanges verifies the stored-plan unsafe-change view of
// VSchema work: recorded deletions and vindex mutations surface with their
// operator-facing reasons, an additive VSchema change surfaces nothing, and
// uncertainty — a VSchema change without persisted change-metadata, or a
// record that cannot be decoded — fails closed as unsafe so the apply-time
// gate always sees it.
func TestPlanUnsafeVSchemaChanges(t *testing.T) {
	vschemaArtifact := map[string]string{VSchemaArtifactName: `{"sharded": true}`}

	t.Run("nil plan has no changes", func(t *testing.T) {
		var plan *Plan
		assert.Nil(t, plan.UnsafeVSchemaChanges())
	})

	t.Run("recorded deletions are unsafe with their reasons", func(t *testing.T) {
		plan := &Plan{Namespaces: map[string]*NamespacePlanData{
			"payments": {
				Artifacts: vschemaArtifact,
				Metadata: map[string]string{
					PlanMetadataVSchemaChanged:   "true",
					PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"},{"kind":"table","name":"users","reason":"removing table users from the VSchema stops routing its queries"}]`,
				},
			},
		}}

		changes := plan.UnsafeVSchemaChanges()

		require.Len(t, changes, 2)
		assert.Equal(t, "payments", changes[0].Namespace)
		assert.Equal(t, "removing vindex email_idx changes query routing", changes[0].Reason)
		assert.Equal(t, "payments", changes[1].Namespace)
		assert.Equal(t, "removing table users from the VSchema stops routing its queries", changes[1].Reason)
	})

	t.Run("recorded mutations are unsafe with their reasons", func(t *testing.T) {
		plan := &Plan{Namespaces: map[string]*NamespacePlanData{
			"payments": {
				Artifacts: vschemaArtifact,
				Metadata: map[string]string{
					PlanMetadataVSchemaChanged:   "true",
					PlanMetadataVSchemaMutations: `[{"kind":"vindex_type","name":"user_idx","reason":"changing vindex user_idx type re-computes keyspace ids"}]`,
				},
			},
		}}

		changes := plan.UnsafeVSchemaChanges()

		require.Len(t, changes, 1)
		assert.Equal(t, "payments", changes[0].Namespace)
		assert.Equal(t, "changing vindex user_idx type re-computes keyspace ids", changes[0].Reason)
	})

	t.Run("additive vschema change is safe", func(t *testing.T) {
		plan := &Plan{Namespaces: map[string]*NamespacePlanData{
			"payments": {
				Artifacts: vschemaArtifact,
				Metadata:  map[string]string{PlanMetadataVSchemaChanged: "true"},
			},
		}}

		assert.Empty(t, plan.UnsafeVSchemaChanges())
	})

	t.Run("vschema change without metadata fails closed", func(t *testing.T) {
		plan := &Plan{Namespaces: map[string]*NamespacePlanData{
			"payments": {Artifacts: vschemaArtifact},
		}}

		changes := plan.UnsafeVSchemaChanges()

		require.Len(t, changes, 1)
		assert.Equal(t, "payments", changes[0].Namespace)
		assert.Contains(t, changes[0].Reason, "does not record whether this VSchema change removes or mutates vindexes or routing entries")
		assert.Contains(t, changes[0].Reason, "re-plan")
	})

	t.Run("metadata without vschema document fails closed", func(t *testing.T) {
		plan := &Plan{Namespaces: map[string]*NamespacePlanData{
			"payments": {
				Metadata: map[string]string{
					PlanMetadataVSchemaChanged:   "true",
					PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
				},
			},
		}}

		changes := plan.UnsafeVSchemaChanges()

		require.Len(t, changes, 1)
		assert.Equal(t, "payments", changes[0].Namespace)
		assert.Contains(t, changes[0].Reason, "records VSchema change-metadata for this namespace but no desired VSchema document")
		assert.Contains(t, changes[0].Reason, "re-plan")
	})

	t.Run("undecodable deletions and mutations each fail closed", func(t *testing.T) {
		plan := &Plan{Namespaces: map[string]*NamespacePlanData{
			"payments": {
				Artifacts: vschemaArtifact,
				Metadata: map[string]string{
					PlanMetadataVSchemaChanged:   "true",
					PlanMetadataVSchemaDeletions: "not json",
					PlanMetadataVSchemaMutations: "also not json",
				},
			},
		}}

		changes := plan.UnsafeVSchemaChanges()

		require.Len(t, changes, 2)
		assert.Contains(t, changes[0].Reason, "VSchema deletions were recorded on this plan but could not be decoded")
		assert.Contains(t, changes[1].Reason, "VSchema mutations were recorded on this plan but could not be decoded")
	})

	t.Run("undecodable deletions fail closed without hiding mutations", func(t *testing.T) {
		plan := &Plan{Namespaces: map[string]*NamespacePlanData{
			"payments": {
				Artifacts: vschemaArtifact,
				Metadata: map[string]string{
					PlanMetadataVSchemaChanged:   "true",
					PlanMetadataVSchemaDeletions: "not json",
					PlanMetadataVSchemaMutations: `[{"kind":"vindex_type","name":"user_idx","reason":"changing vindex user_idx type re-computes keyspace ids"}]`,
				},
			},
		}}

		changes := plan.UnsafeVSchemaChanges()

		require.Len(t, changes, 2)
		assert.Equal(t, "payments", changes[0].Namespace)
		assert.Contains(t, changes[0].Reason, "VSchema deletions were recorded on this plan but could not be decoded")
		assert.Contains(t, changes[0].Reason, "re-plan")
		assert.Equal(t, "changing vindex user_idx type re-computes keyspace ids", changes[1].Reason)
	})

	t.Run("undecodable mutations fail closed", func(t *testing.T) {
		plan := &Plan{Namespaces: map[string]*NamespacePlanData{
			"payments": {
				Artifacts: vschemaArtifact,
				Metadata: map[string]string{
					PlanMetadataVSchemaChanged:   "true",
					PlanMetadataVSchemaMutations: "not json",
				},
			},
		}}

		changes := plan.UnsafeVSchemaChanges()

		require.Len(t, changes, 1)
		assert.Equal(t, "payments", changes[0].Namespace)
		assert.Contains(t, changes[0].Reason, "VSchema mutations were recorded on this plan but could not be decoded")
		assert.Contains(t, changes[0].Reason, "re-plan")
	})

	t.Run("namespaces without vschema changes are skipped", func(t *testing.T) {
		plan := &Plan{Namespaces: map[string]*NamespacePlanData{
			"orders": {Tables: []TableChange{{Table: "orders", Operation: "alter"}}},
			"payments": {
				Artifacts: vschemaArtifact,
				Metadata: map[string]string{
					PlanMetadataVSchemaChanged:   "true",
					PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
				},
			},
		}}

		changes := plan.UnsafeVSchemaChanges()

		require.Len(t, changes, 1)
		assert.Equal(t, "payments", changes[0].Namespace)
	})
}
