package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runningTable(name string, copied, total int64, percent int) TableProgressData {
	return TableProgressData{
		Namespace:       "orders",
		TableName:       name,
		DDL:             "ALTER TABLE " + name + " ADD COLUMN c int",
		Status:          state.Task.Running,
		RowsCopied:      copied,
		RowsTotal:       total,
		PercentComplete: percent,
	}
}

// progressSummaryLine returns the one-line "📊 ..." summary of a rendered
// apply comment.
func progressSummaryLine(t *testing.T, out string) string {
	t.Helper()
	for line := range strings.SplitSeq(out, "\n") {
		if strings.HasPrefix(line, "📊") {
			return line
		}
	}
	require.FailNow(t, "no progress summary line in rendered comment", out)
	return ""
}

// With several tables copying, the summary reports the rows copied over the
// rows to copy across all of them, so the figure is the same whichever order
// the tables are listed in.
func TestRenderApplyStatusComment_RunningSummaryWeighsRowsAcrossTables(t *testing.T) {
	completed := TableProgressData{
		Namespace: "orders", TableName: "audit_log", DDL: "CREATE TABLE audit_log (id bigint)", Status: state.Task.Completed,
	}
	large := runningTable("events", 400000, 1000000, 40)
	small := runningTable("users", 9000, 10000, 90)

	t.Run("large table listed first", func(t *testing.T) {
		out := RenderApplyStatusComment(multiStatementApplyData(state.Apply.Running, []TableProgressData{completed, large, small}))

		assert.Equal(t, "📊 1/3 complete · 2 running (40.50%)", progressSummaryLine(t, out))
	})

	t.Run("small table listed first", func(t *testing.T) {
		out := RenderApplyStatusComment(multiStatementApplyData(state.Apply.Running, []TableProgressData{completed, small, large}))

		assert.Equal(t, "📊 1/3 complete · 2 running (40.50%)", progressSummaryLine(t, out))
	})
}

// A single copying table keeps its own percent; several show no percent when it
// cannot be computed from rows.
func TestRenderApplyStatusComment_RunningSummaryOmitsPercentItCannotWeigh(t *testing.T) {
	queued := TableProgressData{
		Namespace: "orders", TableName: "refunds", DDL: "ALTER TABLE refunds ADD COLUMN c int", Status: state.Task.Pending,
	}

	t.Run("one running table shows its own percent", func(t *testing.T) {
		out := RenderApplyStatusComment(multiStatementApplyData(state.Apply.Running, []TableProgressData{
			runningTable("users", 9000, 10000, 90), queued,
		}))

		assert.Equal(t, "📊 1 running (90.00%) · 1 queued", progressSummaryLine(t, out))
	})

	t.Run("one running table that has copied no rows shows no percent", func(t *testing.T) {
		out := RenderApplyStatusComment(multiStatementApplyData(state.Apply.Running, []TableProgressData{
			runningTable("users", 0, 10000, 0), queued,
		}))

		assert.Equal(t, "📊 1 running · 1 queued", progressSummaryLine(t, out))
	})

	t.Run("a running table with no row total yet", func(t *testing.T) {
		out := RenderApplyStatusComment(multiStatementApplyData(state.Apply.Running, []TableProgressData{
			runningTable("events", 0, 0, 0), runningTable("users", 9000, 10000, 90),
		}))

		assert.Equal(t, "📊 2 running", progressSummaryLine(t, out))
	})

	t.Run("no running table has copied a row yet", func(t *testing.T) {
		out := RenderApplyStatusComment(multiStatementApplyData(state.Apply.Running, []TableProgressData{
			runningTable("events", 0, 1000000, 0), runningTable("users", 0, 10000, 0),
		}))

		assert.Equal(t, "📊 2 running", progressSummaryLine(t, out))
	})
}

// A table that reports more rows copied than its total counts as fully copied,
// so it cannot inflate the figure for the others. The summary never reaches
// this, because such a table switches the label to "(finalizing copy)" first,
// so it is checked on the function.
func TestRunningPercentTextCountsAnOvershootAsFullyCopied(t *testing.T) {
	got := runningPercentText([]TableProgressData{
		runningTable("events", 1500, 1000, 100), runningTable("users", 0, 1000, 0),
	})

	assert.Equal(t, "50.00%", got)
}
