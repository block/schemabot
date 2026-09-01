package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Table-size rendering in the plan comment: an info section above the plan
// summary shows the scale of each table gaining an index — rows, on-disk
// bytes, the shard span, and the largest single shard. A missing estimate is
// stated explicitly so a failed size probe never reads as a small table.

func tableSizePlanData(sizes []TableSizeData) PlanCommentData {
	return PlanCommentData{
		Database:    "testapp",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"},
			TableSizes: sizes,
		}},
	}
}

func TestRenderPlanComment_TableSizes(t *testing.T) {
	out := RenderPlanComment(tableSizePlanData([]TableSizeData{
		{Table: "mutes", EstimatedRows: previewRows(2_340_000), EstimatedBytes: previewRows(1_130_000_000)},
	}))

	assert.Contains(t, out, "📊 **Table sizes**:")
	// The line ends right after the byte clause: a non-sharded target renders
	// no shard clause.
	assert.Contains(t, out, "- `mutes`: ~2.3M rows · ~1.1 GB\n")
}

func TestRenderPlanComment_TableSizesRenderAbovePlanSummary(t *testing.T) {
	data := tableSizePlanData([]TableSizeData{
		{Table: "mutes", EstimatedRows: previewRows(2_340_000), EstimatedBytes: previewRows(1_130_000_000)},
	})
	data.LintViolations = []LintViolationData{
		{Message: "Index should be invisible first", Table: "mutes", LinterName: "invisible_index_before_drop"},
	}
	out := RenderPlanComment(data)

	summaryAt := strings.Index(out, "📋 **Plan**:")
	sizesAt := strings.Index(out, "📊 **Table sizes**")
	lintAt := strings.Index(out, "💡 **Lint Warnings**")
	ddlAt := strings.Index(out, "```sql")
	assert.Greater(t, sizesAt, ddlAt, "sizes render after the DDL, not above it")
	assert.Greater(t, sizesAt, lintAt, "sizes render below the lint warnings")
	assert.Less(t, sizesAt, summaryAt, "sizes render above the plan summary")
}

func TestRenderPlanComment_TableSizesWithoutBytesOmitsByteClause(t *testing.T) {
	out := RenderPlanComment(tableSizePlanData([]TableSizeData{
		{Table: "mutes", EstimatedRows: previewRows(2_340_000)},
	}))

	assert.Contains(t, out, "- `mutes`: ~2.3M rows\n")
}

// PlanetScale's branch metrics report storage bytes with no row counts, so a
// PlanetScale change renders its byte estimate and shard span rather than
// falling to the size-unavailable line.
func TestRenderPlanComment_TableSizesBytesOnly(t *testing.T) {
	out := RenderPlanComment(tableSizePlanData([]TableSizeData{
		{Table: "mutes", EstimatedBytes: previewRows(23_400_000_000), ShardCount: 4},
	}))

	assert.Contains(t, out, "- `mutes`: ~23.4 GB across 4 shards\n")
}

func TestRenderPlanComment_TableSizesSharded(t *testing.T) {
	out := RenderPlanComment(tableSizePlanData([]TableSizeData{
		{Table: "mutes", EstimatedRows: previewRows(48_200_000), EstimatedBytes: previewRows(23_400_000_000), ShardCount: 4, LargestShardRows: previewRows(13_100_000)},
	}))

	assert.Contains(t, out, "- `mutes`: ~48.2M rows · ~23.4 GB across 4 shards (largest shard ~13.1M rows)\n")
}

func TestRenderPlanComment_TableSizesSingleShardOmitsLargest(t *testing.T) {
	out := RenderPlanComment(tableSizePlanData([]TableSizeData{
		{Table: "mutes", EstimatedRows: previewRows(15_249), ShardCount: 1, LargestShardRows: previewRows(15_249)},
	}))

	assert.Contains(t, out, "- `mutes`: ~15.2k rows across 1 shard\n")
	assert.NotContains(t, out, "largest shard", "a single-shard span has no distinct largest shard")
}

func TestRenderPlanComment_TableSizeUnavailableIsExplicit(t *testing.T) {
	out := RenderPlanComment(tableSizePlanData([]TableSizeData{
		{Table: "mutes", ShardCount: 3},
		{Table: "orders"},
	}))

	assert.Contains(t, out, "- `mutes`: size estimate unavailable · 3 shards\n")
	assert.Contains(t, out, "- `orders`: size estimate unavailable\n")
}

func TestRenderPlanComment_TableSizesQualifiedAcrossKeyspaces(t *testing.T) {
	data := PlanCommentData{
		Database:     "testapp",
		Environment:  "staging",
		DatabaseType: "vitess",
		Changes: []KeyspaceChangeData{
			{
				Keyspace:   "commerce",
				Statements: []string{"ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"},
				TableSizes: []TableSizeData{{Table: "mutes", EstimatedRows: previewRows(2_340_000), EstimatedBytes: previewRows(1_130_000_000)}},
			},
			{
				Keyspace:   "commerce_sharded",
				Statements: []string{"ALTER TABLE `customers` ADD COLUMN `tier` varchar(20)"},
				TableSizes: []TableSizeData{{Table: "customers", EstimatedRows: previewRows(48_200_000), EstimatedBytes: previewRows(23_400_000_000), ShardCount: 2, LargestShardRows: previewRows(24_600_000)}},
			},
		},
	}
	out := RenderPlanComment(data)

	assert.Contains(t, out, "- `commerce.mutes`: ~2.3M rows · ~1.1 GB\n")
	assert.Contains(t, out, "- `commerce_sharded.customers`: ~48.2M rows · ~23.4 GB across 2 shards (largest shard ~24.6M rows)\n")
}

func TestRenderPlanComment_NoTableSizesOmitsSection(t *testing.T) {
	out := RenderPlanComment(tableSizePlanData(nil))

	assert.False(t, strings.Contains(out, "Table sizes"), "a plan without size data renders no size section")
}
