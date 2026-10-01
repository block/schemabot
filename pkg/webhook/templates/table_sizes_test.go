package templates

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// The sizes describe the tables the DDL touches, so they sit directly under
// the DDL rather than below the warnings, next to the plan summary.
func TestRenderPlanComment_TableSizesRenderUnderDDL(t *testing.T) {
	data := tableSizePlanData([]TableSizeData{
		{Table: "mutes", EstimatedBytes: previewBytes(1_130_000_000)},
	})
	data.LintViolations = []LintViolationData{
		{Message: "Index should be invisible first", Table: "mutes", LinterName: "invisible_index_before_drop"},
	}
	out := RenderPlanComment(data)

	ddlAt := strings.Index(out, "```sql")
	require.NotEqual(t, -1, ddlAt, "plan comment renders the DDL")
	ddlEnd := ddlAt + strings.Index(out[ddlAt:], "\n```\n")
	sizesAt := strings.Index(out, "📊 **Table sizes**")
	lintAt := strings.Index(out, "💡 **Lint Warnings**")
	summaryAt := strings.Index(out, "📋 **Plan**:")
	require.NotEqual(t, -1, sizesAt, "plan comment renders the size section")
	require.NotEqual(t, -1, lintAt, "plan comment renders the lint warnings")
	assert.Greater(t, sizesAt, ddlEnd, "sizes render after the DDL")
	assert.Less(t, sizesAt, lintAt, "sizes render above the lint warnings")
	assert.Less(t, sizesAt, summaryAt, "sizes render above the plan summary")
}

// The locked apply comment follows a plan comment that already showed the
// sizes, so it leaves them out whether it applies automatically or waits for
// apply-confirm.
func TestRenderPlanComment_LockedApplyOmitsTableSizes(t *testing.T) {
	for _, pending := range []bool{false, true} {
		data := tableSizePlanData([]TableSizeData{
			{Table: "mutes", EstimatedBytes: previewBytes(1_130_000_000)},
		})
		data.IsLocked = true
		data.LockOwner = "octocat/hello-world#1"
		data.PendingManualConfirmation = pending
		out := RenderPlanComment(data)

		assert.Contains(t, out, "## Schema Change Apply", "pending confirmation %v", pending)
		assert.Contains(t, out, "📋 **Plan**:", "pending confirmation %v", pending)
		assert.NotContains(t, out, "Table sizes", "pending confirmation %v", pending)
	}
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

// tableSizesSection returns the size section of a rendered plan comment, from
// its first line through the blank line that closes it.
func tableSizesSection(t *testing.T, out string) string {
	t.Helper()
	at := strings.Index(out, "📊")
	require.GreaterOrEqual(t, at, 0, "the comment carries a size section")
	start := strings.LastIndex(out[:at], "\n\n") + 2
	end := strings.Index(out, "📋 **Plan**:")
	require.Greater(t, end, start, "the size section renders above the plan summary")
	return out[start:end]
}

// rankedTables names n tables in plan order, each smaller than the one before
// it is listed when ranked, so the largest is the last in plan order.
func rankedTables(n int) ([]string, map[string]int64) {
	order := make([]string, 0, n)
	bytes := map[string]int64{}
	for i := range n {
		name := fmt.Sprintf("t%03d", i+1)
		order = append(order, name)
		bytes[name] = int64(i+1) * 1_000_000
	}
	return order, bytes
}

// sizeLines renders the section line for each of tables, ranked from first to
// last, with the bytes rankedTables gave it.
func sizeLines(bytes map[string]int64, tables []string) string {
	var sb strings.Builder
	for _, name := range tables {
		fmt.Fprintf(&sb, "- `%s`: ~%d MB\n", name, bytes[name]/1_000_000)
	}
	return sb.String()
}

// largestFirst returns order reversed: rankedTables makes each table larger
// than the one before it.
func largestFirst(order []string) []string {
	reversed := slices.Clone(order)
	slices.Reverse(reversed)
	return reversed
}

// Up to the inline limit every table is listed in the open, in plan order.
func TestRenderPlanComment_TableSizesAtInlineLimitStayInPlanOrder(t *testing.T) {
	order, bytes := rankedTables(tableSizesInlineLimit)
	out := RenderPlanComment(tableSizePlanData(sizedTables(bytes, order)))

	assert.Equal(t, "📊 **Table sizes**:\n"+
		"- `t001`: ~1 MB\n"+
		"- `t002`: ~2 MB\n"+
		"- `t003`: ~3 MB\n"+
		"- `t004`: ~4 MB\n"+
		"- `t005`: ~5 MB\n"+
		"\n", tableSizesSection(t, out))
}

// Past the inline limit the whole section collapses, so the sizes do not
// stand a wall of lines above the plan summary, and lists the largest tables
// first, since they bound how long the apply runs. A table whose size probe
// failed is listed last, as unavailable rather than small.
func TestRenderPlanComment_TableSizesOverInlineLimitCollapseLargestFirst(t *testing.T) {
	order := []string{"accounts", "audit_events", "carts", "events_raw", "ledger", "orders"}
	bytes := map[string]int64{
		"accounts":     6_000_000,
		"audit_events": 90_000_000,
		"carts":        3_000_000,
		"ledger":       80_000_000,
		"orders":       50_000_000,
	}
	out := RenderPlanComment(tableSizePlanData(sizedTables(bytes, order)))

	assert.Equal(t, "<details>\n<summary>📊 <b>Table sizes</b></summary>\n\n"+
		"- `audit_events`: ~90 MB\n"+
		"- `ledger`: ~80 MB\n"+
		"- `orders`: ~50 MB\n"+
		"- `accounts`: ~6 MB\n"+
		"- `carts`: ~3 MB\n"+
		"- `events_raw`: size estimate unavailable\n"+
		"\n</details>\n\n", tableSizesSection(t, out))
}

// A collapsed section lists every table up to the listed cap, with no line
// counting tables left out.
func TestRenderPlanComment_TableSizesAtListedCapListEveryTable(t *testing.T) {
	order, bytes := rankedTables(tableSizesShown)
	out := RenderPlanComment(tableSizePlanData(sizedTables(bytes, order)))

	assert.Equal(t, "<details>\n<summary>📊 <b>Table sizes</b></summary>\n\n"+
		sizeLines(bytes, largestFirst(order))+
		"\n</details>\n\n", tableSizesSection(t, out))
}

// Past the listed cap the collapsed section lists only the largest tables,
// since collapsed lines still count toward the comment size limit, and counts
// the rest in a closing line.
func TestRenderPlanComment_TableSizesPastListedCapCountTheRest(t *testing.T) {
	order, bytes := rankedTables(100)
	out := RenderPlanComment(tableSizePlanData(sizedTables(bytes, order)))

	assert.Equal(t, "<details>\n<summary>📊 <b>Table sizes</b></summary>\n\n"+
		sizeLines(bytes, largestFirst(order)[:tableSizesShown])+
		"- …and 36 more tables\n"+
		"\n</details>\n\n", tableSizesSection(t, out))
}

// One table past the listed cap is counted in the closing line, in the
// singular.
func TestRenderPlanComment_TableSizesOnePastListedCap(t *testing.T) {
	order, bytes := rankedTables(tableSizesShown + 1)
	out := RenderPlanComment(tableSizePlanData(sizedTables(bytes, order)))

	assert.Equal(t, "<details>\n<summary>📊 <b>Table sizes</b></summary>\n\n"+
		sizeLines(bytes, largestFirst(order)[:tableSizesShown])+
		"- …and 1 more table\n"+
		"\n</details>\n\n", tableSizesSection(t, out))
}

// The collapsed section ranks tables by bytes, largest first, and tables with
// no estimate last, so the order is the same whatever order the plan listed
// them in. A multi-target line ranks by its total across targets.
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
// target's size alone.
func TestRenderPlanComment_TwoTargetSizesNameLargestTarget(t *testing.T) {
	out := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		targetSize("primary/testapp_1", "orders", 1_130_000_000),
		targetSize("primary/testapp_2", "orders", 23_400_000_000),
	}))

	assert.Equal(t, "📊 **Table sizes**:\n- `orders`: ~24.5 GB across 2 targets · largest ~23.4 GB on `primary/testapp_2` · smallest ~1.1 GB\n\n",
		tableSizesSection(t, out))
}

// When one of two targets reports no estimate, the known size is shown on its
// target and the other target is named as unavailable.
func TestRenderPlanComment_TwoTargetSizesNameTargetWithoutEstimate(t *testing.T) {
	out := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		unsizedTarget("primary/testapp_1", "orders"),
		targetSize("primary/testapp_2", "orders", 1_130_000_000),
	}))

	assert.Contains(t, out, "- `orders`: ~1.1 GB on `primary/testapp_2` · size estimate unavailable on `primary/testapp_1`\n")
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

// A blocked rollup carries no per-target sizes, so the section falls back to
// the reviewed plan's own, and the heading names the reviewed target so the
// sizes are not read as the whole rollout's.
func TestRenderPlanComment_MultiTargetWithoutTargetSizesNamesReviewedTarget(t *testing.T) {
	data := multiTargetSizePlanData(nil)
	data.DeploymentDrift = &DeploymentDriftData{Computed: true, Clean: false, Deployments: previewRolloutMembers()}
	out := RenderPlanComment(data)

	assert.Contains(t, out, "📊 **Table sizes** (reviewed target `primary/testapp_1` only; other targets not shown):\n- `orders`: ~100 KB\n\n")
}

// A rollup that could not be computed names no members, so the heading scopes
// the sizes to the reviewed target without naming it, and without claiming
// other targets exist: the database may have only the reviewed one.
func TestRenderPlanComment_UncomputedRollupScopesSizesToReviewedTarget(t *testing.T) {
	data := multiTargetSizePlanData(nil)
	data.DeploymentDrift = &DeploymentDriftData{Computed: false}
	out := RenderPlanComment(data)

	assert.Contains(t, out, "📊 **Table sizes** (reviewed target only; targets could not be listed):\n")
	assert.NotContains(t, out, "other targets")
}

// A rollup of a single target has no other target the sizes could be read
// for, so the heading carries no scope.
func TestRenderPlanComment_SingleTargetRollupSizesHaveNoScope(t *testing.T) {
	data := multiTargetSizePlanData(nil)
	data.DeploymentDrift = &DeploymentDriftData{Computed: true, Clean: false, Deployments: previewRolloutMembers()[:1]}
	out := RenderPlanComment(data)

	assert.Contains(t, out, "📊 **Table sizes**:\n- `orders`: ~100 KB\n\n")
}

// Target and table names come from server config and the PR, so each renders
// as a code span it cannot close: a name carrying a backtick or a line break
// stays inside its span instead of writing markdown into the comment.
func TestRenderPlanComment_TableSizeNamesStayInCodeSpans(t *testing.T) {
	members := previewRolloutMembers()
	members[0].Deployment = "prim`ary"
	data := multiTargetSizePlanData(nil)
	data.DeploymentDrift = &DeploymentDriftData{Computed: true, Clean: false, Deployments: members}
	assert.Contains(t, RenderPlanComment(data), "(reviewed target `` prim`ary `` only; other targets not shown)")

	out := RenderPlanComment(multiTargetSizePlanData([]TargetTableSize{
		targetSize("primary/a`b", "ord`ers", 23_400_000_000),
		targetSize("primary/c\n# d", "ord`ers", 1_130_000_000),
		unsizedTarget("primary/e`f", "ord`ers"),
		targetSize("primary/g`h", "us`ers", 1_000_000),
	}))
	assert.Contains(t, out, "- `` ord`ers ``: ~24.5 GB across 2 of 3 targets · largest ~23.4 GB on `` primary/a`b `` · smallest ~1.1 GB · size estimate unavailable on `` primary/e`f ``\n")
	assert.Contains(t, out, "- `` us`ers ``: ~1 MB on `` primary/g`h ``\n")
}

// A collapsed section on the reviewed-target fallback keeps the target scope
// in its summary, where GitHub reads HTML rather than markdown, so the
// reviewed target's name is escaped and set in a <code> tag.
func TestRenderPlanComment_CollapsedReviewedTargetSizesKeepScope(t *testing.T) {
	order, bytes := rankedTables(tableSizesInlineLimit + 1)
	data := tableSizePlanData(sizedTables(bytes, order))
	data.DeploymentDrift = &DeploymentDriftData{Computed: true, Clean: false, Deployments: previewRolloutMembers()}
	out := RenderPlanComment(data)

	assert.Equal(t, "<details>\n<summary>📊 <b>Table sizes</b> (reviewed target <code>primary/testapp_1</code> only; other targets not shown)</summary>\n\n"+
		sizeLines(bytes, largestFirst(order))+
		"\n</details>\n\n", tableSizesSection(t, out))

	members := previewRolloutMembers()
	members[0].Deployment = "<b>prim`ary</b>"
	data.DeploymentDrift = &DeploymentDriftData{Computed: true, Clean: false, Deployments: members}
	assert.Contains(t, RenderPlanComment(data),
		"<summary>📊 <b>Table sizes</b> (reviewed target <code>&lt;b&gt;prim`ary&lt;/b&gt;</code> only; other targets not shown)</summary>")
}
