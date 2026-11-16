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
// itself.
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
			names = append(names, fmt.Sprintf("ski/aurora-staging-orders-%03d", n))
		}
		assert.Equal(t,
			"ski/aurora-staging-orders-005  ski/aurora-staging-orders-006\n"+
				"ski/aurora-staging-orders-007  ski/aurora-staging-orders-008\n"+
				"ski/aurora-staging-orders-009\n",
			nameGrid(names))
	})

	t.Run("a name wider than the grid gets its own row", func(t *testing.T) {
		long := strings.Repeat("x", nameGridWidth+5)
		assert.Equal(t, "a\n"+long+"\n", nameGrid([]string{"a", long}), "a row ends at its last name, with no padding after it")
	})
}

// A wide set of targets that raised several findings is named once, as its
// coverage with the names in a collapsed grid, in the folded lint section as
// in the inline one.
func TestWriteLintViolations_FoldedSharedWideTargetsNamedOnce(t *testing.T) {
	var targets []string
	for n := 5; n <= 32; n++ {
		targets = append(targets, fmt.Sprintf("ski/orders-%03d", n))
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
		"ski/orders-005  ski/orders-006  ski/orders-007  ski/orders-008  ski/orders-009\n",
		"the finding every target raised comes first, then the shared targets lead their findings")
	assert.Contains(t, out, "</details>\n\n**`trips`**\n- Column created_at uses TIMESTAMP\n- Column updated_at uses TIMESTAMP\n")
	assert.Equal(t, 1, strings.Count(out, "ski/orders-032"), "the shared targets are listed once, not once per finding")
	assert.NotContains(t, out, "on 28 of 32 targets", "no finding repeats the targets on its own line")
}
