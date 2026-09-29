package templates

import (
	"slices"
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

// sizedTables returns one sized table per name, each with the given bytes and
// ten rows per byte unit so rows and bytes rank the same way.
func sizedTables(bytesByTable map[string]int64, order []string) []TableSizeData {
	sizes := make([]TableSizeData, 0, len(order))
	for _, name := range order {
		b, ok := bytesByTable[name]
		if !ok {
			sizes = append(sizes, TableSizeData{Table: name})
			continue
		}
		sizes = append(sizes, TableSizeData{Table: name, EstimatedRows: previewRows(b * 10), EstimatedBytes: previewRows(b)})
	}
	return sizes
}

// Up to the inline limit every table is listed in plan order, with no fold.
func TestRenderPlanComment_TableSizesAtInlineLimitStayInPlanOrder(t *testing.T) {
	order := []string{"t01", "t02", "t03", "t04", "t05", "t06", "t07", "t08", "t09", "t10"}
	bytes := map[string]int64{}
	for i, name := range order {
		bytes[name] = int64(i+1) * 1_000_000
	}
	out := RenderPlanComment(tableSizePlanData(sizedTables(bytes, order)))

	assert.Contains(t, out, "📊 **Table sizes**:\n- `t01`: ~10M rows · ~1 MB\n- `t02`:")
	assert.Contains(t, out, "- `t10`: ~100M rows · ~10 MB\n\n")
	assert.NotContains(t, out, "<details>\n<summary>", "a plan at the inline limit does not fold its sizes")
	assert.Less(t, strings.Index(out, "`t01`"), strings.Index(out, "`t10`"), "inline sizes keep plan order")
}

// A plan that changes more tables than the inline limit leads with the count,
// shows the largest tables, and folds the rest largest first. A table whose
// size probe failed is counted in the visible heading and listed last.
func TestRenderPlanComment_TableSizesOverInlineLimitFoldLargestFirst(t *testing.T) {
	order := []string{"accounts", "audit_events", "carts", "coupons", "events_raw", "invoices", "ledger", "orders", "payments", "sessions", "users", "webhooks"}
	bytes := map[string]int64{
		"accounts":     6_000_000,
		"audit_events": 90_000_000,
		"carts":        3_000_000,
		"coupons":      1_000_000,
		"invoices":     40_000_000,
		"ledger":       80_000_000,
		"orders":       50_000_000,
		"payments":     60_000_000,
		"users":        20_000_000,
		"webhooks":     2_000_000,
	}
	out := RenderPlanComment(tableSizePlanData(sizedTables(bytes, order)))

	want := "📊 **Table sizes** (12 tables, largest first; 2 without a size estimate):\n" +
		"- `audit_events`: ~900M rows · ~90 MB\n" +
		"- `ledger`: ~800M rows · ~80 MB\n" +
		"- `payments`: ~600M rows · ~60 MB\n" +
		"- `orders`: ~500M rows · ~50 MB\n" +
		"- `invoices`: ~400M rows · ~40 MB\n" +
		"\n<details>\n<summary>7 more tables</summary>\n\n" +
		"- `users`: ~200M rows · ~20 MB\n" +
		"- `accounts`: ~60M rows · ~6 MB\n" +
		"- `carts`: ~30M rows · ~3 MB\n" +
		"- `webhooks`: ~20M rows · ~2 MB\n" +
		"- `coupons`: ~10M rows · ~1 MB\n" +
		"- `events_raw`: size estimate unavailable\n" +
		"- `sessions`: size estimate unavailable\n" +
		"\n</details>\n\n"
	assert.Contains(t, out, want)
}

// When every table has an estimate the folded heading carries only the count.
func TestRenderPlanComment_TableSizesFoldedHeadingOmitsZeroUnavailable(t *testing.T) {
	order := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}
	bytes := map[string]int64{}
	for i, name := range order {
		bytes[name] = int64(i+1) * 1_000_000
	}
	out := RenderPlanComment(tableSizePlanData(sizedTables(bytes, order)))

	assert.Contains(t, out, "📊 **Table sizes** (11 tables, largest first):\n- `k`:")
	assert.Contains(t, out, "<summary>6 more tables</summary>")
}

// The fold ranks tables with bytes by bytes, then tables reporting rows alone
// by rows, then tables with no estimate: rows and bytes are never compared
// with each other, so the order is the same whatever order the plan listed
// the tables in.
func TestCompareTableSizesLargestFirst(t *testing.T) {
	bigBytes := TableSizeData{Table: "big_bytes", EstimatedRows: previewRows(100), EstimatedBytes: previewRows(10_000)}
	smallBytes := TableSizeData{Table: "small_bytes", EstimatedRows: previewRows(5_000), EstimatedBytes: previewRows(10)}
	bytesOnly := TableSizeData{Table: "bytes_only", EstimatedBytes: previewRows(500)}
	manyRows := TableSizeData{Table: "many_rows", EstimatedRows: previewRows(9_000)}
	fewRows := TableSizeData{Table: "few_rows", EstimatedRows: previewRows(3)}
	unknown := TableSizeData{Table: "unknown"}

	want := []string{"big_bytes", "bytes_only", "small_bytes", "many_rows", "few_rows", "unknown"}
	inputs := [][]TableSizeData{
		{unknown, fewRows, manyRows, smallBytes, bytesOnly, bigBytes},
		{manyRows, smallBytes, unknown, bigBytes, fewRows, bytesOnly},
	}
	for _, in := range inputs {
		sorted := slices.Clone(in)
		slices.SortStableFunc(sorted, compareTableSizesLargestFirst)
		got := make([]string, 0, len(sorted))
		for _, ts := range sorted {
			got = append(got, ts.Table)
		}
		assert.Equal(t, want, got)
	}
}
