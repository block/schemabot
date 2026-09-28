package sqlstore

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// The plan store rebuilds each namespace's plan data field by field before it
// writes plan_data, so a field it does not copy is silently dropped from every
// stored plan. Every field is set here, so a field added to NamespacePlanData
// without being copied fails this test instead of vanishing from storage.
func TestNamespacesWithShardPlansKeepsEveryNamespaceField(t *testing.T) {
	nsData := &storage.NamespacePlanData{
		Tables:                []storage.TableChange{{Namespace: "payments", Table: "refunds", DDL: "ALTER TABLE `refunds` ADD COLUMN `note` text", Operation: "alter"}},
		Shards:                []storage.ShardPlan{{Namespace: "payments", Shard: "-"}},
		OriginalFiles:         map[string]string{"refunds.sql": "CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"},
		OriginalFilesCaptured: true,
		Artifacts:             map[string]string{storage.VSchemaArtifactName: `{"tables":{}}`},
		Metadata:              map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
		IgnoreTables:          []string{"_archive"},
		Finalize:              true,
	}
	fields := reflect.ValueOf(nsData).Elem()
	for i := range fields.NumField() {
		require.False(t, fields.Field(i).IsZero(), "set NamespacePlanData.%s in this test so its persistence is checked", fields.Type().Field(i).Name)
	}

	got := namespacesWithShardPlans(&storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"payments": nsData}})

	assert.Equal(t, nsData, got["payments"])
}
