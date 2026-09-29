package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/webhook/templates"
)

const refundNotesDDL = "CREATE TABLE `refund_notes` (`refund_id` bigint NOT NULL, PRIMARY KEY (`refund_id`))"

func renderStrataPlan(t *testing.T, metadata map[string]string) string {
	t.Helper()
	ks := templates.KeyspaceChangeData{Keyspace: "payments_001", Statements: []string{refundNotesDDL}}
	setNamespaceWork(&ks, &apitypes.SchemaChangeResponse{Namespace: "payments_001", Metadata: metadata})
	return templates.RenderPlanComment(templates.PlanCommentData{
		Database: "payments", Environment: "staging", DatabaseType: "strata",
		Changes: []templates.KeyspaceChangeData{ks},
	})
}

// A Strata keyspace gains a table without a hand-written VSchema change: the
// engine generates the table's VSchema entry from its DDL and finalizes the
// keyspace. The plan comment shows no VSchema section at all, only the DDL and
// the finalize line, and counts the keyspace as a finalize.
func TestPlanComment_GeneratedVSchemaChangeShowsOnlyTheFinalize(t *testing.T) {
	out := renderStrataPlan(t, map[string]string{
		apitypes.VSchemaChangedMetadataKey:       "true",
		apitypes.VSchemaGeneratedOnlyMetadataKey: "true",
		apitypes.NeedsFinalizerMetadataKey:       "true",
	})

	assert.NotContains(t, out, "VSchema")
	assert.NotContains(t, out, "diff not available")
	assert.Contains(t, out, "_Finalized by the engine once every shard's DDL has landed._")
	assert.Contains(t, out, "📋 **Plan**: **1** table to create, **1** keyspace to finalize\n")
}

// A VSchema change the engine sent no diff for, and did not mark generated
// from the DDL, still reads as unavailable: the comment cannot vouch that
// nothing hand-written is hidden.
func TestPlanComment_VSchemaWithoutDiffOrMarkerIsUnavailable(t *testing.T) {
	out := renderStrataPlan(t, map[string]string{
		apitypes.VSchemaChangedMetadataKey: "true",
		apitypes.NeedsFinalizerMetadataKey: "true",
	})

	assert.Contains(t, out, "#### VSchema\n_(diff not available)_\n")
	assert.Contains(t, out, "**1** vschema update")
}
