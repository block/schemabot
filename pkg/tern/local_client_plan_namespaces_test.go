package tern

import (
	"testing"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func planNamespacesTestClient() *LocalClient {
	return &LocalClient{config: LocalConfig{
		Database: "commerce",
		Type:     storage.DatabaseTypeVitess,
	}}
}

// TestNamespacesFromEngineChangesPersistsVSchemaMetadata verifies the local
// plan-persistence path stores the same apply-time VSchema change-metadata as
// the gRPC path: the changed flag, the recorded deletions and mutations the
// unsafe gate reads, and the rendered diff apply-time display shows survive
// into the namespace's plan data, alongside the desired-VSchema artifact.
func TestNamespacesFromEngineChangesPersistsVSchemaMetadata(t *testing.T) {
	client := planNamespacesTestClient()
	metadata := map[string]string{
		storage.PlanMetadataVSchemaChanged:   "true",
		storage.PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
		storage.PlanMetadataVSchemaMutations: `[{"kind":"vindex_type","name":"user_idx","reason":"changing vindex user_idx type re-computes keyspace ids"}]`,
		storage.PlanMetadataVSchemaDiff:      "rendered diff for display",
		"branch":                             "engine-internal detail",
	}
	desiredVSchema := `{"sharded":true,"tables":{"users":{}}}`

	namespaces, shardPlans := client.namespacesFromEngineChanges([]engine.SchemaChange{{
		Namespace: "payments",
		TableChanges: []engine.TableChange{{
			Table:     "users",
			DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			Operation: ddl.StatementAlterTable,
		}},
		Metadata: metadata,
	}}, schema.SchemaFiles{
		"payments": {Files: map[string]string{storage.VSchemaArtifactName: desiredVSchema}},
	})

	assert.Empty(t, shardPlans)
	require.Contains(t, namespaces, "payments")
	nsData := namespaces["payments"]
	require.Len(t, nsData.Tables, 1)
	assert.Equal(t, "users", nsData.Tables[0].Table)
	assert.Equal(t, desiredVSchema, nsData.Artifacts[storage.VSchemaArtifactName])
	assert.Equal(t, map[string]string{
		storage.PlanMetadataVSchemaChanged:   "true",
		storage.PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
		storage.PlanMetadataVSchemaMutations: `[{"kind":"vindex_type","name":"user_idx","reason":"changing vindex user_idx type re-computes keyspace ids"}]`,
		storage.PlanMetadataVSchemaDiff:      "rendered diff for display",
	}, nsData.Metadata, "only the VSchema plan keys persist; engine-internal metadata does not leak")
}

// TestNamespacesFromEngineChangesSiblingShardKeepsVSchemaMetadata verifies a
// sharded keyspace's per-shard changes cannot erode its persisted VSchema
// change-metadata: the shards share one namespace record, so a sibling shard
// carrying no metadata — or only the changed flag without the deletion record
// — must not clear or overwrite what another shard already persisted.
func TestNamespacesFromEngineChangesSiblingShardKeepsVSchemaMetadata(t *testing.T) {
	client := planNamespacesTestClient()
	alterUsers := engine.TableChange{
		Table:     "users",
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		Operation: ddl.StatementAlterTable,
	}
	metadata := map[string]string{
		storage.PlanMetadataVSchemaChanged:   "true",
		storage.PlanMetadataVSchemaDeletions: `[{"kind":"table","name":"users","reason":"removing table users from the VSchema stops routing its queries"}]`,
	}

	namespaces, shardPlans := client.namespacesFromEngineChanges([]engine.SchemaChange{
		{
			Namespace:    "payments",
			Shard:        engine.Shard{Name: "-80"},
			TableChanges: []engine.TableChange{alterUsers},
			Metadata:     metadata,
		},
		{
			Namespace:    "payments",
			Shard:        engine.Shard{Name: "80-"},
			TableChanges: []engine.TableChange{alterUsers},
		},
		{
			Namespace:    "payments",
			Shard:        engine.Shard{Name: "c0-"},
			TableChanges: []engine.TableChange{alterUsers},
			Metadata:     map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
		},
	}, schema.SchemaFiles{
		"payments": {Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
	})

	require.Contains(t, namespaces, "payments")
	nsData := namespaces["payments"]
	assert.Equal(t, storage.VSchemaPlanMetadata(metadata), nsData.Metadata)

	// Namespace-level tables dedupe across shards; per-shard rows keep each
	// shard's own changes.
	require.Len(t, nsData.Tables, 1)
	assert.Equal(t, "users", nsData.Tables[0].Table)
	require.Len(t, nsData.Shards, 3)
	require.Len(t, shardPlans, 3)
	assert.Equal(t, "-80", shardPlans[0].Shard)
	assert.Equal(t, "80-", shardPlans[1].Shard)
	assert.Equal(t, "c0-", shardPlans[2].Shard)
	for _, sp := range shardPlans {
		assert.Equal(t, "payments", sp.Namespace)
		require.Len(t, sp.Changes, 1)
		assert.Equal(t, "users", sp.Changes[0].Table)
	}
}

// TestNamespacesFromEngineChangesMergesDisjointSiblingMetadata verifies that
// per-shard changes contributing different pieces of a namespace's VSchema
// change-metadata all land in the persisted record: a shard recording only
// deletions and a sibling recording only mutations must merge into one record
// carrying both, so the apply-time gate sees every disclosure.
func TestNamespacesFromEngineChangesMergesDisjointSiblingMetadata(t *testing.T) {
	client := planNamespacesTestClient()
	alterUsers := engine.TableChange{
		Table:     "users",
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		Operation: ddl.StatementAlterTable,
	}
	deletions := `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`
	mutations := `[{"kind":"vindex_type","name":"user_idx","reason":"changing vindex user_idx type re-computes keyspace ids"}]`

	namespaces, _ := client.namespacesFromEngineChanges([]engine.SchemaChange{
		{
			Namespace:    "payments",
			Shard:        engine.Shard{Name: "-80"},
			TableChanges: []engine.TableChange{alterUsers},
			Metadata: map[string]string{
				storage.PlanMetadataVSchemaChanged:   "true",
				storage.PlanMetadataVSchemaDeletions: deletions,
			},
		},
		{
			Namespace:    "payments",
			Shard:        engine.Shard{Name: "80-"},
			TableChanges: []engine.TableChange{alterUsers},
			Metadata: map[string]string{
				storage.PlanMetadataVSchemaChanged:   "true",
				storage.PlanMetadataVSchemaMutations: mutations,
			},
		},
	}, schema.SchemaFiles{
		"payments": {Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
	})

	require.Contains(t, namespaces, "payments")
	assert.Equal(t, map[string]string{
		storage.PlanMetadataVSchemaChanged:   "true",
		storage.PlanMetadataVSchemaDeletions: deletions,
		storage.PlanMetadataVSchemaMutations: mutations,
	}, namespaces["payments"].Metadata)
}

// TestNamespacesFromApplyRequestCarriesVSchemaMetadata verifies a deployment
// materializing a plan from a dispatch request recovers the namespace's
// VSchema change-metadata from the request's VSchema change, so its apply
// admission gate reads the same recorded deletions and mutations as a
// deployment loading its own stored plan — an additive change queues freely
// while a recorded deletion still requires opt-in.
func TestNamespacesFromApplyRequestCarriesVSchemaMetadata(t *testing.T) {
	client := planNamespacesTestClient()
	metadata := map[string]string{
		storage.PlanMetadataVSchemaChanged:   "true",
		storage.PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
	}

	namespaces, err := client.namespacesFromApplyRequest([]*ternv1.TableChange{{
		Namespace:  "payments",
		TableName:  "VSchema: payments",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
		Metadata:   metadata,
	}}, schema.SchemaFiles{
		"payments": {Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
	})
	require.NoError(t, err)

	require.Contains(t, namespaces, "payments")
	nsData := namespaces["payments"]
	assert.Equal(t, `{"sharded":true}`, nsData.Artifacts[storage.VSchemaArtifactName])
	assert.Equal(t, metadata, nsData.Metadata)
}

// TestPlanResultToProtoChangesSiblingShardKeepsVSchemaMetadata verifies the
// wire conversion gives sharded keyspaces the same guarantee as plan
// persistence: the per-shard changes collapse into one proto SchemaChange, and
// the namespace's VSchema change-metadata survives no matter which shard's
// change carries it — a metadata-free first shard must not drop a later
// sibling's recorded deletions.
func TestPlanResultToProtoChangesSiblingShardKeepsVSchemaMetadata(t *testing.T) {
	client := planNamespacesTestClient()
	alterUsers := engine.TableChange{
		Table:     "users",
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		Operation: ddl.StatementAlterTable,
	}
	metadata := map[string]string{
		storage.PlanMetadataVSchemaChanged:   "true",
		storage.PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
	}

	changes, violations, shards := client.planResultToProtoChanges(&engine.PlanResult{
		Changes: []engine.SchemaChange{
			{
				Namespace:    "payments",
				Shard:        engine.Shard{Name: "-80"},
				TableChanges: []engine.TableChange{alterUsers},
			},
			{
				Namespace:    "payments",
				Shard:        engine.Shard{Name: "80-"},
				TableChanges: []engine.TableChange{alterUsers},
				Metadata:     metadata,
			},
		},
	})

	assert.Empty(t, violations)
	require.Len(t, shards, 2)
	require.Len(t, changes, 1)
	assert.Equal(t, "payments", changes[0].Namespace)
	assert.Equal(t, metadata, changes[0].Metadata)
}

// TestNamespacesFromEngineChangesWithoutVSchemaWork verifies a DDL-only change
// persists no VSchema change-metadata and no artifact, so the stored plan's
// unsafe-VSchema gate has nothing to inspect for it.
func TestNamespacesFromEngineChangesWithoutVSchemaWork(t *testing.T) {
	client := planNamespacesTestClient()

	namespaces, shardPlans := client.namespacesFromEngineChanges([]engine.SchemaChange{{
		Namespace: "payments",
		TableChanges: []engine.TableChange{{
			Table:     "users",
			DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			Operation: ddl.StatementAlterTable,
		}},
	}}, schema.SchemaFiles{
		"payments": {Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
	})

	assert.Empty(t, shardPlans)
	require.Contains(t, namespaces, "payments")
	nsData := namespaces["payments"]
	assert.Nil(t, nsData.Metadata)
	assert.Empty(t, nsData.Artifacts)
}
