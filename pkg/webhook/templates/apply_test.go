package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderApplyStatusComment_Running(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       "running",
		Engine:      "Spirit",
		Tables: []TableProgressData{
			{TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: "completed"},
			{TableName: "users", DDL: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)", Status: "running", RowsCopied: 45000, RowsTotal: 100000, PercentComplete: 45, ETASeconds: 195},
			{TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: "pending"},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "## Schema Change In Progress")
	assert.Contains(t, result, "@aparajon")
	assert.Contains(t, result, "`testapp`")
	assert.Contains(t, result, "`staging`")
	// Progress summary
	assert.Contains(t, result, "📊 1/3 complete")
	assert.Contains(t, result, "1 running (45%)")
	assert.Contains(t, result, "1 queued")
	assert.Contains(t, result, "### Table Progress")

	// Per-table checks
	assert.Contains(t, result, "**`orders`**")
	assert.Contains(t, result, "✓ Complete")
	assert.Contains(t, result, "🟩") // green bar for completed

	assert.Contains(t, result, "**`users`**")
	assert.Contains(t, result, "45%")
	assert.Contains(t, result, "🟦") // blue bar for running
	assert.Contains(t, result, "45,000 / 100,000")
	assert.Contains(t, result, "ETA: 3m 15s")

	assert.Contains(t, result, "**`products`**")
	assert.Contains(t, result, "Queued")
}

func TestRenderApplyStatusComment_Completed(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       "completed",
		Engine:      "Spirit",
		Tables: []TableProgressData{
			{TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: "completed"},
			{TableName: "users", DDL: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)", Status: "completed"},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "## ✅ Schema Change Applied")
	assert.Contains(t, result, "### Table Progress")
	// Progress summary line
	assert.Contains(t, result, "📊 2/2 complete")
	// Each table has "✓ Complete" = 2 total
	assert.Equal(t, 2, strings.Count(result, "Complete"))
}

func TestRenderApplyStatusComment_Failed(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:     "testapp",
		Environment:  "staging",
		RequestedBy:  "aparajon",
		State:        "failed",
		Engine:       "Spirit",
		ErrorMessage: "lock wait timeout exceeded",
		Tables: []TableProgressData{
			{TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: "completed"},
			{TableName: "users", DDL: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)", Status: "failed", PercentComplete: 30},
			{TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_price` (`price_cents`)", Status: state.Task.Cancelled},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "## ❌ Schema Change Failed")
	assert.Contains(t, result, "⚠️ **Error:**")
	assert.Contains(t, result, "lock wait timeout exceeded")
	assert.Contains(t, result, "🟥") // red bar for failed table
	assert.Contains(t, result, "❌ Failed")
	assert.Contains(t, result, "⊘ Cancelled (not started)")
	// Progress summary
	assert.Contains(t, result, "📊 1/3 complete")
	assert.Contains(t, result, "1 failed")
	assert.Contains(t, result, "1 cancelled")
	assert.Contains(t, result, "To retry:")
	assert.Contains(t, result, "schemabot apply -e staging")
}

func TestRenderApplyStatusComment_Stopped(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       "stopped",
		Engine:      "Spirit",
		Tables: []TableProgressData{
			{TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: "completed"},
			{TableName: "users", DDL: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)", Status: "stopped", RowsCopied: 72000, RowsTotal: 100000, PercentComplete: 72},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "## ⏹️ Schema Change Stopped")
	assert.Contains(t, result, "🟧") // orange bar for stopped
	assert.Contains(t, result, "⏹️ Stopped at 72%")
	assert.Contains(t, result, "72,000 / 100,000")
	// Progress summary
	assert.Contains(t, result, "📊 1/2 complete")
	assert.Contains(t, result, "1 stopped")
	assert.Contains(t, result, "schemabot start")
}

func TestRenderApplyStatusComment_WaitingForCutover(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       "waiting_for_cutover",
		Engine:      "Spirit",
		Tables: []TableProgressData{
			{TableName: "orders", Status: "waiting_for_cutover"},
			{TableName: "users", Status: "waiting_for_cutover"},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "Waiting for Cutover")
	assert.Contains(t, result, "🟨") // yellow bar
	assert.Contains(t, result, "schemabot cutover")
}

func TestRenderApplyStatusComment_CuttingOver(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       "cutting_over",
		Engine:      "Spirit",
		Tables: []TableProgressData{
			{TableName: "orders", Status: "cutting_over"},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "Cutting Over")
	assert.Contains(t, result, "Cutting over...")
}

func TestRenderApplyStatusComment_NoTables(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       "running",
		Engine:      "Spirit",
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "## Schema Change In Progress")
	assert.NotContains(t, result, "### Table Progress")
}

func TestRenderApplyStatusComment_NoRequestedBy(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		State:       "running",
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "*Started at")
	assert.NotContains(t, result, "@")
}

func TestApplyStatusFromProgress(t *testing.T) {
	resp := &apitypes.ProgressResponse{
		State:       "running",
		Engine:      "Spirit",
		ApplyID:     "apply_abc123",
		Database:    "testapp",
		Environment: "staging",
		Tables: []*apitypes.TableProgressResponse{
			{
				TableName:       "users",
				DDL:             "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)",
				Status:          "running",
				RowsCopied:      5000,
				RowsTotal:       10000,
				PercentComplete: 50,
				ETASeconds:      120,
			},
			{
				TableName: "", // empty table name should be filtered
			},
		},
	}

	data := ApplyStatusFromProgress(resp, "aparajon")

	assert.Equal(t, "testapp", data.Database)
	assert.Equal(t, "staging", data.Environment)
	assert.Equal(t, "aparajon", data.RequestedBy)
	assert.Equal(t, "running", data.State)
	assert.Equal(t, "Spirit", data.Engine)
	assert.Equal(t, "apply_abc123", data.ApplyID)
	require.Len(t, data.Tables, 1) // empty table name filtered
	assert.Equal(t, "users", data.Tables[0].TableName)
	assert.Equal(t, int64(5000), data.Tables[0].RowsCopied)
	assert.Equal(t, 50, data.Tables[0].PercentComplete)
}

func TestPreviewCommentApplyProgress(t *testing.T) {
	result := PreviewCommentApplyProgress()

	assert.Contains(t, result, "Schema Change In Progress")
	assert.Contains(t, result, "### Table Progress")
	assert.Contains(t, result, "**`orders`**")
	assert.Contains(t, result, "**`users`**")
	assert.Contains(t, result, "**`products`**")
	assert.Contains(t, result, "62%")
	assert.Contains(t, result, "Queued")
}

func TestPreviewCommentApplyCompleted(t *testing.T) {
	result := PreviewCommentApplyCompleted()

	assert.Contains(t, result, "Schema Change Applied")
	assert.Contains(t, result, "### Table Progress")
}

func TestPreviewCommentApplyFailed(t *testing.T) {
	result := PreviewCommentApplyFailed()

	assert.Contains(t, result, "Schema Change Failed")
	assert.Contains(t, result, "lock wait timeout")
	assert.Contains(t, result, "Cancelled (not started)")
}

func TestPreviewCommentApplyStopped(t *testing.T) {
	result := PreviewCommentApplyStopped()

	assert.Contains(t, result, "Schema Change Stopped")
	assert.Contains(t, result, "Stopped at 72%")
	assert.Contains(t, result, "schemabot start")
}

func TestPreviewCommentApplyWaitingForCutover(t *testing.T) {
	result := PreviewCommentApplyWaitingForCutover()

	assert.Contains(t, result, "Waiting for Cutover")
	assert.Contains(t, result, "schemabot cutover")
}

func TestPreviewCommentApplyCuttingOver(t *testing.T) {
	result := PreviewCommentApplyCuttingOver()

	assert.Contains(t, result, "Cutting Over")
	assert.Contains(t, result, "Cutting over...")
}

func TestPreviewCommentSummaryCompleted(t *testing.T) {
	result := PreviewCommentSummaryCompleted()

	assert.Contains(t, result, "Schema Change Applied")
	assert.Contains(t, result, "All 3 tables applied successfully")
	// Single namespace matching database name — header skipped
	assert.NotContains(t, result, "### ")
	assert.Contains(t, result, "**`orders`**")
	assert.Contains(t, result, "```sql")
}

func TestPreviewCommentSummaryFailed(t *testing.T) {
	result := PreviewCommentSummaryFailed()

	assert.Contains(t, result, "Schema Change Failed")
	assert.Contains(t, result, "unsafe warning")
	assert.Contains(t, result, "1 of 3 tables completed before failure.")
	// Single namespace — no header, but table entries present
	assert.NotContains(t, result, "### ")
	assert.Contains(t, result, "**`users`** — Failed at 30%")
	assert.Contains(t, result, "**`orders`**")
	assert.Contains(t, result, "**`products`** — Cancelled")
}

func TestPreviewCommentSummaryStopped(t *testing.T) {
	result := PreviewCommentSummaryStopped()

	assert.Contains(t, result, "⏹️ Schema Change Stopped")
	assert.Contains(t, result, "1 of 2 tables completed before stop.")
	// Single namespace — no header
	assert.NotContains(t, result, "### ")
	assert.Contains(t, result, "**`users`** — Stopped at 72%")
	assert.Contains(t, result, "**`orders`**")
}

func TestTruncateDDL(t *testing.T) {
	assert.Equal(t, "ALTER TABLE orders ADD INDEX idx_user_id (user_id)", truncateDDL("ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", 80))
	assert.Equal(t, "short", truncateDDL("short", 80))
	assert.Equal(t, "", truncateDDL("", 80))

	long := "ALTER TABLE `very_long_table_name` ADD INDEX `idx_very_long_column_name_that_goes_on_forever` (`very_long_column_name_that_goes_on_forever`)"
	result := truncateDDL(long, 80)
	assert.Len(t, result, 80)
	assert.True(t, strings.HasSuffix(result, "..."))
}
