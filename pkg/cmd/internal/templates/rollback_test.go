package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/charmbracelet/x/ansi"
	"github.com/stretchr/testify/assert"
)

// Preview and progress retain the same SQL while leaving the supplied plan intact.
func TestRollbackAndProgressPreservePlanSQL(t *testing.T) {
	for _, databaseType := range []string{"mysql", "postgres", "custom"} {
		t.Run(databaseType, func(t *testing.T) {
			raw := `ALTER TABLE "INT" ADD COLUMN "TEXT" text DEFAULT 'KEEP INT, DEFAULT NULL';`
			if databaseType == "mysql" {
				raw = "ALTER TABLE `INT` ADD COLUMN `TEXT` varchar(64) DEFAULT 'KEEP INT, DEFAULT NULL';"
			}
			table := &apitypes.TableChangeResponse{TableName: "INT", ChangeType: "alter", DDL: raw}
			plan := &apitypes.PlanResponse{Database: "shop", DatabaseType: databaseType, Environment: "staging", Changes: []*apitypes.SchemaChangeResponse{{Namespace: "shop", TableChanges: []*apitypes.TableChangeResponse{table}}}}
			preview := captureStdout(t, func() { WriteRollbackPlan(plan, "apply-example-85") })
			assert.Equal(t, raw, table.DDL)
			assert.Contains(t, ansi.Strip(preview), raw)
			progress := FormatTableProgress(TableProgress{TableName: "INT", ChangeType: "alter", DDL: raw, Dialect: schema.DialectForDatabaseType(databaseType), Status: state.Apply.Running})
			assert.Contains(t, ansi.Strip(progress), raw)
			assert.Equal(t, raw, table.DDL)
		})
	}
}

// A rollback whose only work in payments is the engine's finalize, and whose
// commerce keyspace reverts its VSchema, lists both under the changes the
// operator is asked to confirm, rather than an empty list.
func TestRollbackPlanListsFinalizeOnlyKeyspaces(t *testing.T) {
	plan := &apitypes.PlanResponse{Database: "shop", DatabaseType: "vitess", Environment: "staging", Changes: []*apitypes.SchemaChangeResponse{
		{Namespace: "commerce", Metadata: map[string]string{apitypes.VSchemaChangedMetadataKey: "true", apitypes.NeedsFinalizerMetadataKey: "true"}},
		{Namespace: "payments", Metadata: map[string]string{apitypes.NeedsFinalizerMetadataKey: "true"}},
	}}

	preview := ansi.Strip(captureStdout(t, func() { WriteRollbackPlan(plan, "apply-example-85") }))

	assert.Contains(t, preview, "The following changes will be applied to rollback:\n\n  commerce: VSchema update\n  payments: finalized by the engine once every shard's DDL has landed\n")
}

// A rollback that recreates a table in a keyspace the engine then finalizes
// lists the CREATE alone: the finalize is part of that work, as the PR comment
// shows it.
func TestRollbackPlanListsOnlyTheDDLOfAFinalizedKeyspace(t *testing.T) {
	plan := &apitypes.PlanResponse{Database: "shop", DatabaseType: "vitess", Environment: "staging", Changes: []*apitypes.SchemaChangeResponse{
		{
			Namespace:    "payments",
			TableChanges: []*apitypes.TableChangeResponse{{TableName: "refund_notes", Namespace: "payments", ChangeType: "create", DDL: "CREATE TABLE `refund_notes` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}},
			Metadata: map[string]string{
				apitypes.VSchemaChangedMetadataKey:       "true",
				apitypes.VSchemaGeneratedOnlyMetadataKey: "true",
				apitypes.NeedsFinalizerMetadataKey:       "true",
			},
		},
	}}

	preview := ansi.Strip(captureStdout(t, func() { WriteRollbackPlan(plan, "apply-example-85") }))

	assert.Contains(t, preview, "  refund_notes (create):\n")
	assert.NotContains(t, preview, "finalized by the engine")
	assert.NotContains(t, preview, "VSchema update")
}

// A keyspace whose shards diverged lists what each shard will run: the drop
// only one shard needs is shown beside the ALTER its sibling runs, instead of
// the one entry per table the namespace-level changes keep. A statement
// uniform across shards appears once.
func TestRollbackPlanListsADivergentShardsStatement(t *testing.T) {
	safeAlter := "ALTER TABLE `orders` ADD INDEX `idx_status` (`status`);"
	shardDrop := "ALTER TABLE `orders` DROP COLUMN `legacy`;"
	plan := &apitypes.PlanResponse{Database: "shop", DatabaseType: "vitess", Environment: "staging",
		Changes: []*apitypes.SchemaChangeResponse{{Namespace: "shop", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders", ChangeType: "alter", DDL: safeAlter}}}},
		Shards: []*apitypes.ShardPlanResponse{
			{Namespace: "shop", Shard: "-80", Changes: []*apitypes.TableChangeResponse{{TableName: "orders", ChangeType: "alter", DDL: safeAlter}}},
			{Namespace: "shop", Shard: "80-", Changes: []*apitypes.TableChangeResponse{{TableName: "orders", ChangeType: "alter", DDL: safeAlter}, {TableName: "orders", ChangeType: "alter", DDL: shardDrop}}},
		}}

	preview := ansi.Strip(captureStdout(t, func() { WriteRollbackPlan(plan, "apply-example-85") }))

	assert.Contains(t, preview, "DROP COLUMN `legacy`")
	assert.Equal(t, 1, strings.Count(preview, "ADD INDEX `idx_status`"))
	assert.Equal(t, 2, strings.Count(preview, "  orders (alter):\n"))
}
