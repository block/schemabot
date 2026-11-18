package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A wide group's names pack as many to a row as fit the grid, in equal-width
// columns, so short shard key ranges fill a row and long target names take
// few. A name wider than the grid is never shortened and gets a row to
// itself, without spreading the other names out.
func TestNameGrid(t *testing.T) {
	t.Run("short key ranges pack many to a row", func(t *testing.T) {
		ranges := []string{"-08", "08-10", "18-20", "20-28", "28-30", "30-38", "38-40", "40-48", "48-50", "50-58", "58-60", "60-68", "68-70", "70-"}
		assert.Equal(t,
			"-08    08-10  18-20  20-28  28-30  30-38  38-40  40-48  48-50  50-58  58-60\n"+
				"60-68  68-70  70-\n",
			nameGrid(ranges),
			"columns are as wide as the widest range, so the missing range after `08-10` shows as a break in the sequence")
	})

	t.Run("long target names pack few to a row", func(t *testing.T) {
		var names []string
		for n := 5; n <= 9; n++ {
			names = append(names, fmt.Sprintf("acme/orders-archive-replica-%03d", n))
		}
		assert.Equal(t,
			"acme/orders-archive-replica-005  acme/orders-archive-replica-006\n"+
				"acme/orders-archive-replica-007  acme/orders-archive-replica-008\n"+
				"acme/orders-archive-replica-009\n",
			nameGrid(names))
	})

	t.Run("a name wider than the grid gets its own row and leaves the others packed", func(t *testing.T) {
		long := strings.Repeat("x", nameGridWidth+5)
		var names []string
		for n := 1; n <= 7; n++ {
			names = append(names, fmt.Sprintf("acme/orders-%03d", n))
		}
		names = append(names[:3], append([]string{long}, names[3:]...)...)
		assert.Equal(t,
			"acme/orders-001  acme/orders-002  acme/orders-003\n"+
				long+"\n"+
				"acme/orders-004  acme/orders-005  acme/orders-006  acme/orders-007\n",
			nameGrid(names),
			"the over-wide name ends the row before it, and the names after it still pack four to a row")
		assert.Equal(t, "a\n"+long+"\n", nameGrid([]string{"a", long}), "a row ends at its last name, with no padding after it")
	})

	t.Run("a line break or format character in a name cannot add a row or disguise it", func(t *testing.T) {
		assert.Equal(t,
			"db/orders-001   db/orders- 002\n",
			nameGrid([]string{"db/orders-001", "db/orders-\n002"}),
			"the line break collapses to one space, so the name stays on its row")
		assert.Equal(t,
			"db/orders-001  db/orders-002\n",
			nameGrid([]string{"db/orders-001", "db/\u202eorders-002"}),
			"the right-to-left override is removed, so the name reads as written")
	})

	t.Run("columns are measured in terminal cells", func(t *testing.T) {
		var wide []string
		for n := 1; n <= 9; n++ {
			wide = append(wide, fmt.Sprintf("db/订单-%03d", n))
		}
		grid := nameGrid(wide)
		assert.Equal(t,
			"db/订单-001  db/订单-002  db/订单-003  db/订单-004  db/订单-005  db/订单-006\n"+
				"db/订单-007  db/订单-008  db/订单-009\n",
			grid,
			"each ideograph takes two cells, so six 11-cell names fill the 80-cell row")

		assert.Equal(t,
			"db/cafe\u0301-01   db/cafe-002\n",
			nameGrid([]string{"db/café-01", "db/cafe-002"}),
			"a combining accent takes no cell, so the padding after it lines up the next column")
	})
}

// A wide set of targets that raised several findings is named once, as its
// coverage with the names in a collapsed grid, in the folded lint section as
// in the inline one.
func TestWriteLintViolations_FoldedSharedWideTargetsNamedOnce(t *testing.T) {
	var targets []string
	for n := 5; n <= 32; n++ {
		targets = append(targets, fmt.Sprintf("acme/orders-%03d", n))
	}
	warnings := []LintViolationData{{Table: "accounts", Message: "table has no primary key"}}
	for _, column := range []string{"created_at", "updated_at", "ended_at", "started_at", "recorded_at"} {
		warnings = append(warnings, LintViolationData{
			Table:        "trips",
			Message:      "Column " + column + " uses TIMESTAMP",
			Targets:      targets,
			TotalTargets: 32,
		})
	}

	var sb strings.Builder
	writeLintViolations(&sb, warnings)
	out := sb.String()

	assert.Contains(t, out, "<summary>💡 <b>Lint Warnings</b>: 6 advisory findings</summary>\n\n"+
		"**`accounts`**\n- table has no primary key\n\n"+
		"On 28 of 32 targets:\n\n"+
		"<details>\n<summary>Target names</summary>\n\n```\n"+
		"acme/orders-005  acme/orders-006  acme/orders-007  acme/orders-008\n"+
		"acme/orders-009  acme/orders-010  acme/orders-011  acme/orders-012\n",
		"the finding every target raised comes first, then the shared targets lead their findings")
	assert.Contains(t, out, "</details>\n\n**`trips`**\n- Column created_at uses TIMESTAMP\n- Column updated_at uses TIMESTAMP\n")
	assert.Equal(t, 1, strings.Count(out, "acme/orders-032"), "the shared targets are listed once, not once per finding")
	assert.NotContains(t, out, "on 28 of 32 targets", "no finding repeats the targets on its own line")
}

// Findings that name the same targets out of the same total share one
// heading whatever order each finding lists them in, and findings that name
// the same targets out of different totals do not.
func TestGroupLintWarningsByTargets_SetsIgnoreTargetOrder(t *testing.T) {
	forward := []string{"acme/orders-001", "acme/orders-002"}
	reversed := []string{"acme/orders-002", "acme/orders-001"}
	warnings := []LintViolationData{
		{Table: "bikes", Message: "created", Targets: forward, TotalTargets: 4},
		{Table: "bikes", Message: "updated", Targets: reversed, TotalTargets: 4},
		{Table: "docks", Message: "shared", Targets: forward, TotalTargets: 8},
	}

	labeled, shared := groupLintWarningsByTargets(warnings)

	if assert.Len(t, shared, 1) {
		assert.Equal(t, forward, shared[0].targets, "the heading lists the targets in the order the first finding gave")
		assert.Equal(t, 4, shared[0].total)
		assert.Equal(t, []string{"created", "updated"}, []string{shared[0].warnings[0].Message, shared[0].warnings[1].Message})
	}
	if assert.Len(t, labeled, 1, "the same targets out of a different total are a different set") {
		assert.Equal(t, "shared", labeled[0].Message)
		assert.Equal(t, forward, labeled[0].Targets)
	}
	assert.Equal(t, reversed, warnings[1].Targets, "grouping leaves the caller's findings as they were")
}
