package templates

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Table-size rendering in the plan comment: an info section above the plan
// summary shows the on-disk footprint of each table gaining an index, and the
// shard span on a sharded target. A missing estimate is stated explicitly so a
// failed size probe never reads as a small table, but a plan where no table has
// an estimate renders no section, since its engine did not estimate sizes.

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
		{Table: "mutes", EstimatedBytes: previewBytes(1_130_000_000)},
	}))

	assert.Contains(t, out, "📊 **Table sizes**:")
	// The line ends right after the byte clause: a non-sharded target renders
	// no shard clause.
	assert.Contains(t, out, "- `mutes`: ~1.1 GB\n")
}

func TestRenderPlanComment_TableSizesRenderAbovePlanSummary(t *testing.T) {
	data := tableSizePlanData([]TableSizeData{
		{Table: "mutes", EstimatedBytes: previewBytes(1_130_000_000)},
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

func TestRenderPlanComment_TableSizesSharded(t *testing.T) {
	out := RenderPlanComment(tableSizePlanData([]TableSizeData{
		{Table: "mutes", EstimatedBytes: previewBytes(23_400_000_000), ShardCount: 4},
		{Table: "orders", EstimatedBytes: previewBytes(15_249_000), ShardCount: 1},
	}))

	assert.Contains(t, out, "- `mutes`: ~23.4 GB across 4 shards\n")
	assert.Contains(t, out, "- `orders`: ~15.2 MB across 1 shard\n")
}

func TestRenderPlanComment_TableSizeUnavailableIsExplicit(t *testing.T) {
	out := RenderPlanComment(tableSizePlanData([]TableSizeData{
		{Table: "audits", EstimatedBytes: previewBytes(1_130_000_000)},
		{Table: "mutes", ShardCount: 3},
		{Table: "orders"},
	}))

	assert.Contains(t, out, "- `mutes`: size estimate unavailable · 3 shards\n")
	assert.Contains(t, out, "- `orders`: size estimate unavailable\n")
}

// An engine that does not estimate sizes leaves every table without one, and
// "unavailable" on every line would read as a failed probe when none ran.
func TestRenderPlanComment_NoEstimatesOmitsSection(t *testing.T) {
	out := RenderPlanComment(tableSizePlanData([]TableSizeData{
		{Table: "mutes", ShardCount: 3},
		{Table: "orders"},
	}))

	assert.NotContains(t, out, "Table sizes")
	assert.NotContains(t, out, "size estimate unavailable")
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
				TableSizes: []TableSizeData{{Table: "mutes", EstimatedBytes: previewBytes(1_130_000_000)}},
			},
			{
				Keyspace:   "commerce_sharded",
				Statements: []string{"ALTER TABLE `customers` ADD COLUMN `tier` varchar(20)"},
				TableSizes: []TableSizeData{{Table: "customers", EstimatedBytes: previewBytes(23_400_000_000), ShardCount: 2}},
			},
		},
	}
	out := RenderPlanComment(data)

	assert.Contains(t, out, "- `commerce.mutes`: ~1.1 GB\n")
	assert.Contains(t, out, "- `commerce_sharded.customers`: ~23.4 GB across 2 shards\n")
}

func TestRenderPlanComment_NoTableSizesOmitsSection(t *testing.T) {
	out := RenderPlanComment(tableSizePlanData(nil))

	assert.False(t, strings.Contains(out, "Table sizes"), "a plan without size data renders no size section")
}

// sizedTables returns one sized table per name, each with the given bytes, and
// an unsized table for a name with no bytes.
func sizedTables(bytesByTable map[string]int64, order []string) []TableSizeData {
	sizes := make([]TableSizeData, 0, len(order))
	for _, name := range order {
		b, ok := bytesByTable[name]
		if !ok {
			sizes = append(sizes, TableSizeData{Table: name})
			continue
		}
		sizes = append(sizes, TableSizeData{Table: name, EstimatedBytes: previewBytes(b)})
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

	assert.Contains(t, out, "📊 **Table sizes**:\n- `t01`: ~1 MB\n- `t02`:")
	assert.Contains(t, out, "- `t10`: ~10 MB\n\n")
	assert.NotContains(t, out, "<details>\n<summary>", "a plan at the inline limit does not fold its sizes")
	assert.Less(t, strings.Index(out, "`t01`"), strings.Index(out, "`t10`"), "inline sizes keep plan order")
}

// A folded section lists at most tableSizesListedLimit tables, since collapsed
// lines still count toward the comment size limit, and counts the rest in a
// closing line. The heading still counts every table.
func TestRenderPlanComment_TableSizesFoldedListIsCapped(t *testing.T) {
	const tables = tableSizesListedLimit + 25
	order := make([]string, 0, tables)
	bytes := map[string]int64{}
	for i := range tables {
		name := fmt.Sprintf("t%03d", i)
		order = append(order, name)
		bytes[name] = int64(tables-i) * 1_000_000
	}
	out := RenderPlanComment(tableSizePlanData(sizedTables(bytes, order)))

	assert.Contains(t, out, fmt.Sprintf("📊 **Table sizes** (%d tables, largest first):\n", tables))
	assert.Contains(t, out, fmt.Sprintf("<summary>%d more tables</summary>", tables-tableSizesLargestShown))
	assert.Contains(t, out, fmt.Sprintf("- `t%03d`:", tableSizesListedLimit-1), "the smallest listed table is the last under the cap")
	assert.NotContains(t, out, fmt.Sprintf("- `t%03d`:", tableSizesListedLimit), "tables past the cap are not listed")
	assert.Contains(t, out, "- …and 25 more tables\n\n</details>")
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
		"- `audit_events`: ~90 MB\n" +
		"- `ledger`: ~80 MB\n" +
		"- `payments`: ~60 MB\n" +
		"- `orders`: ~50 MB\n" +
		"- `invoices`: ~40 MB\n" +
		"\n<details>\n<summary>7 more tables</summary>\n\n" +
		"- `users`: ~20 MB\n" +
		"- `accounts`: ~6 MB\n" +
		"- `carts`: ~3 MB\n" +
		"- `webhooks`: ~2 MB\n" +
		"- `coupons`: ~1 MB\n" +
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

// The fold ranks tables by bytes, largest first, and tables with no estimate
// last, so the order is the same whatever order the plan listed them in. A
// multi-target line ranks by its total across targets.
func TestCompareTableSizesLargestFirst(t *testing.T) {
	sized := func(name string, bytes int64) tableSizeEntry {
		return tableSizeEntry{name: name, size: TableSizeData{Table: name, EstimatedBytes: previewBytes(bytes)}}
	}
	big := sized("big", 10_000)
	mid := sized("mid", 500)
	small := sized("small", 10)
	unknown := tableSizeEntry{name: "unknown", size: TableSizeData{Table: "unknown"}}
	spread := tableSizeEntry{name: "spread", perTarget: []TargetTableSize{
		targetSize("primary/testapp_1", "spread", 6_000),
		targetSize("primary/testapp_2", "spread", 6_000),
	}}

	want := []string{"spread", "big", "mid", "small", "unknown"}
	inputs := [][]tableSizeEntry{
		{unknown, small, mid, spread, big},
		{mid, unknown, big, small, spread},
	}
	for _, in := range inputs {
		sorted := slices.Clone(in)
		slices.SortStableFunc(sorted, compareTableSizesLargestFirst)
		got := make([]string, 0, len(sorted))
		for _, e := range sorted {
			got = append(got, e.name)
		}
		assert.Equal(t, want, got)
	}
}

// multiTargetSizePlanData is a clean rollout of independent targets that all
// run the same index build, with the given per-target sizes.
func multiTargetSizePlanData(sizes []TargetTableSize) PlanCommentData {
	changes := []KeyspaceChangeData{{
		Keyspace:   "testapp",
		Statements: []string{"ALTER TABLE `orders` ADD INDEX `created_at`(`created_at`)"},
		TableSizes: []TableSizeData{{Table: "orders", EstimatedBytes: previewBytes(100_000)}},
	}}
	return PlanCommentData{
		Database:    "testapp",
		Environment: "production",
		IsMySQL:     true,
		Changes:     changes,
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: previewRolloutMembers(),
			Plans: []DeploymentPlanGroup{{
				Members: []string{"primary/testapp_1", "primary/testapp_2", "primary/testapp_3"}, Primary: true, Changes: changes,
			}},
			TableSizes: sizes,
		},
	}
}

func targetSize(target, table string, bytes int64) TargetTableSize {
	return TargetTableSize{Target: target, Keyspace: "testapp", Size: TableSizeData{Table: table, EstimatedBytes: previewBytes(bytes)}}
}

func unsizedTarget(target, table string) TargetTableSize {
	return TargetTableSize{Target: target, Keyspace: "testapp", Size: TableSizeData{Table: table}}
}

// Each target of a rollout copies its own data. A table two targets change is
// shown with its total and its largest size with that target named, so the
// operator sees where the build runs longest rather than the reviewed
// target's size alone, and a collapsed breakdown lists both targets' sizes.
func TestRenderPlanComment_TwoTargetSizesNameLargestTarget(t *testing.T) {
	out := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		targetSize("primary/testapp_1", "orders", 1_130_000_000),
		targetSize("primary/testapp_2", "orders", 23_400_000_000),
	}))

	assert.Contains(t, out, "📊 **Table sizes**:\n- `orders`: ~24.5 GB across 2 targets · largest ~23.4 GB on `primary/testapp_2` · smallest ~1.1 GB\n\n")
	assert.Contains(t, out, "<details>\n<summary>Size on each target</summary>\n\n"+
		"- `orders`\n"+
		"  - `primary/testapp_2`: ~23.4 GB\n"+
		"  - `primary/testapp_1`: ~1.1 GB\n"+
		"\n</details>\n\n📋 **Plan**:")
}

// When one of two targets reports no estimate, the known size is shown on its
// target and the other target is named as unavailable.
func TestRenderPlanComment_TwoTargetSizesNameTargetWithoutEstimate(t *testing.T) {
	out := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		unsizedTarget("primary/testapp_1", "orders"),
		targetSize("primary/testapp_2", "orders", 1_130_000_000),
	}))

	assert.Contains(t, out, "- `orders`: ~1.1 GB on `primary/testapp_2` · size estimate unavailable on `primary/testapp_1`\n")
	assert.Contains(t, out, "  - `primary/testapp_2`: ~1.1 GB\n  - `primary/testapp_1`: size estimate unavailable\n",
		"a target with no estimate is listed after every sized target")
	assert.NotContains(t, out, "largest", "one sized target has no largest or smallest to compare")
}

// At three or more targets the line still names the largest target, which is
// the one that bounds how long the rollout's copy runs, whatever position it
// holds in rollout order.
func TestRenderPlanComment_ThreeTargetSizesNameLargestTarget(t *testing.T) {
	out := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		targetSize("primary/testapp_1", "orders", 100_000_000),
		targetSize("primary/testapp_2", "orders", 23_400_000_000),
		targetSize("primary/testapp_3", "orders", 1_130_000_000),
	}))

	assert.Contains(t, out, "- `orders`: ~24.6 GB across 3 targets · largest ~23.4 GB on `primary/testapp_2` · smallest ~100 MB\n")
	assert.Contains(t, out, "- `orders`\n"+
		"  - `primary/testapp_2`: ~23.4 GB\n"+
		"  - `primary/testapp_3`: ~1.1 GB\n"+
		"  - `primary/testapp_1`: ~100 MB\n")
}

// A target with no estimate is named, since the total then covers only the
// targets that reported one; several such targets are counted instead.
func TestRenderPlanComment_ThreeTargetSizesNameTargetsWithoutEstimate(t *testing.T) {
	one := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		targetSize("primary/testapp_1", "orders", 1_130_000_000),
		unsizedTarget("primary/testapp_2", "orders"),
		targetSize("primary/testapp_3", "orders", 23_400_000_000),
	}))
	assert.Contains(t, one, "- `orders`: ~24.5 GB across 2 of 3 targets · largest ~23.4 GB on `primary/testapp_3` · smallest ~1.1 GB · size estimate unavailable on `primary/testapp_2`\n")

	two := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		unsizedTarget("primary/testapp_1", "orders"),
		unsizedTarget("primary/testapp_2", "orders"),
		targetSize("primary/testapp_3", "orders", 23_400_000_000),
	}))
	assert.Contains(t, two, "- `orders`: ~23.4 GB on `primary/testapp_3` · size estimate unavailable on 2 targets\n")
}

// The per-target breakdown lists at most tableSizesListedLimit target sizes,
// largest first, since collapsed lines still count toward the comment size
// limit, and counts the rest: the targets of the table it stopped in, then
// the tables it did not reach.
func TestRenderPlanComment_TableSizesByTargetIsCapped(t *testing.T) {
	const targets = tableSizesListedLimit + 10
	var sizes []TargetTableSize
	for _, table := range []string{"orders", "users", "carts"} {
		for i := range targets {
			sizes = append(sizes, targetSize(fmt.Sprintf("primary/orders-%03d", i+1), table, int64(i+1)*1_000_000))
		}
	}
	out := RenderPlanComment(multiTargetSizePlanData(sizes))

	assert.Contains(t, out, fmt.Sprintf("- `orders`: ~1.8 GB across %d targets · largest ~60 MB on `primary/orders-%03d` · smallest ~1 MB\n", targets, targets))
	assert.Contains(t, out, fmt.Sprintf("- `orders`\n  - `primary/orders-%03d`: ~60 MB\n", targets), "the breakdown leads with the largest target")
	assert.Contains(t, out, "  - `primary/orders-011`: ~11 MB\n  - …and 10 more targets\n- …and 2 more tables\n\n</details>")
	assert.NotContains(t, out, "  - `primary/orders-010`:", "targets past the cap are not listed")
}

// A table only one target changes is left out of the breakdown, since its
// line already names that target; a rollout with no such table renders none.
func TestRenderPlanComment_TableSizesByTargetSkipsSingleTargetTables(t *testing.T) {
	out := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		targetSize("primary/testapp_1", "orders", 100_000),
		targetSize("primary/testapp_3", "users", 1_130_000_000),
	}))

	assert.NotContains(t, out, "Size on each target")
}

func TestRenderPlanComment_MultiTargetSizesUnavailableEverywhere(t *testing.T) {
	out := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		targetSize("primary/testapp_1", "accounts", 100_000),
		unsizedTarget("primary/testapp_1", "orders"),
		unsizedTarget("primary/testapp_2", "orders"),
		unsizedTarget("primary/testapp_3", "orders"),
		unsizedTarget("primary/testapp_3", "users"),
	}))

	assert.Contains(t, out, "- `orders`: size estimate unavailable on all 3 targets\n")
	assert.Contains(t, out, "- `users`: size estimate unavailable on `primary/testapp_3`\n")
}

// A multi-target plan where no target estimated any table renders no section.
func TestRenderPlanComment_MultiTargetNoEstimatesOmitsSection(t *testing.T) {
	out := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		unsizedTarget("primary/testapp_1", "orders"),
		unsizedTarget("primary/testapp_2", "orders"),
	}))

	assert.NotContains(t, out, "Table sizes")
}

// A table only one target changes is shown on that target alone.
func TestRenderPlanComment_MultiTargetSizesSingleTargetTable(t *testing.T) {
	out := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		targetSize("primary/testapp_1", "orders", 100_000),
		targetSize("primary/testapp_3", "users", 1_130_000_000),
	}))

	assert.Contains(t, out, "- `orders`: ~100 KB on `primary/testapp_1`\n")
	assert.Contains(t, out, "- `users`: ~1.1 GB on `primary/testapp_3`\n")
}

// A folded multi-target section counts every table missing an estimate on
// any target, since its total may understate it.
func TestRenderPlanComment_MultiTargetFoldedHeadingCountsIncompleteTables(t *testing.T) {
	var sizes []TargetTableSize
	for i := range 11 {
		table := fmt.Sprintf("t%02d", i+1)
		sizes = append(sizes, targetSize("primary/testapp_1", table, int64(i+1)*100_000))
		if i == 0 {
			sizes = append(sizes, unsizedTarget("primary/testapp_2", table))
			continue
		}
		sizes = append(sizes, targetSize("primary/testapp_2", table, int64(i+1)*100_000))
	}
	out := RenderPlanComment(multiTargetSizePlanData(sizes))

	assert.Contains(t, out, "📊 **Table sizes** (11 tables, largest first; 1 missing an estimate on at least one target):\n- `t11`:")
}

// A blocked rollup carries no per-target sizes, so the section falls back to
// the reviewed plan's own, and the heading names the reviewed target so the
// sizes are not read as the whole rollout's.
func TestRenderPlanComment_MultiTargetWithoutTargetSizesNamesReviewedTarget(t *testing.T) {
	data := multiTargetSizePlanData(nil)
	data.DeploymentDrift = &DeploymentDriftData{Computed: true, Clean: false, Deployments: previewRolloutMembers()}
	out := RenderPlanComment(data)

	assert.Contains(t, out, "📊 **Table sizes** (reviewed target `primary/testapp_1` only; other targets not shown):\n- `orders`: ~100 KB\n\n")
	assert.NotContains(t, out, "Size on each target")
}

// A rollup that could not be computed names no members, so the heading scopes
// the sizes to the reviewed target without naming it.
func TestRenderPlanComment_UncomputedRollupScopesSizesToReviewedTarget(t *testing.T) {
	data := multiTargetSizePlanData(nil)
	data.DeploymentDrift = &DeploymentDriftData{Computed: false}
	out := RenderPlanComment(data)

	assert.Contains(t, out, "📊 **Table sizes** (reviewed target only; other targets not shown):\n")
}

// A folded section on the reviewed-target fallback carries both the table
// count and the target scope in its heading.
func TestRenderPlanComment_FoldedReviewedTargetSizesKeepScope(t *testing.T) {
	order := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}
	bytes := map[string]int64{}
	for i, name := range order {
		bytes[name] = int64(i+1) * 1_000_000
	}
	data := tableSizePlanData(sizedTables(bytes, order))
	data.DeploymentDrift = &DeploymentDriftData{Computed: true, Clean: false, Deployments: previewRolloutMembers()}
	out := RenderPlanComment(data)

	assert.Contains(t, out, "📊 **Table sizes** (11 tables, largest first; reviewed target `primary/testapp_1` only; other targets not shown):\n- `k`:")
}
