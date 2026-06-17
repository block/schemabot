package templates

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withTemplateTimestamp(t *testing.T, timestamp string) {
	t.Helper()
	original := TimestampFunc
	TimestampFunc = func() string { return timestamp }
	t.Cleanup(func() { TimestampFunc = original })
}

func TestRenderApplyBlockedByCLILockUsesValidUnlockCommand(t *testing.T) {
	rendered := RenderApplyBlockedByOtherPR(ApplyLockConflictData{
		Database:    "example-db",
		Environment: "staging",
		LockOwner:   "cli:testuser@example.local",
		LockCreated: time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC),
	})

	assert.Contains(t, rendered, "schemabot unlock -d example-db --force")
	assert.NotContains(t, rendered, "schemabot unlock -d example-db -e staging --force")
}

func TestUnsafeDropUsageTarget(t *testing.T) {
	tests := []struct {
		name    string
		changes []UnsafeChangeData
		want    string
		wantOK  bool
	}{
		{
			name: "drop column",
			changes: []UnsafeChangeData{
				{Table: "customers", Reason: "Unsafe operation detected: DROP COLUMN `nickname`"},
			},
			want:   "the dropped column",
			wantOK: true,
		},
		{
			name: "multiple drop columns",
			changes: []UnsafeChangeData{
				{Table: "customers", Reason: "Unsafe operation detected: DROP COLUMN `nickname`; Unsafe operation detected: DROP COLUMN `legacy_code`"},
			},
			want:   "any dropped columns",
			wantOK: true,
		},
		{
			name: "drop table",
			changes: []UnsafeChangeData{
				{Table: "archived_orders", Reason: "Unsafe operation detected: DROP TABLE"},
			},
			want:   "the dropped table",
			wantOK: true,
		},
		{
			name: "multiple drop tables",
			changes: []UnsafeChangeData{
				{Table: "archived_orders", Reason: "Unsafe operation detected: DROP TABLE"},
				{Table: "legacy_orders", Reason: "Unsafe operation detected: DROP TABLE"},
			},
			want:   "any dropped tables",
			wantOK: true,
		},
		{
			name: "drop column and table",
			changes: []UnsafeChangeData{
				{Table: "customers", Reason: "Unsafe operation detected: DROP COLUMN `nickname`"},
				{Table: "archived_orders", Reason: "Unsafe operation detected: DROP TABLE"},
			},
			want:   "the dropped table and column",
			wantOK: true,
		},
		{
			name: "multiple drop columns and one drop table",
			changes: []UnsafeChangeData{
				{Table: "customers", Reason: "Unsafe operation detected: DROP COLUMN `nickname`; Unsafe operation detected: DROP COLUMN `legacy_code`"},
				{Table: "archived_orders", Reason: "Unsafe operation detected: DROP TABLE"},
			},
			want:   "the dropped table and any dropped columns",
			wantOK: true,
		},
		{
			name: "multiple drop tables and one drop column",
			changes: []UnsafeChangeData{
				{Table: "customers", Reason: "Unsafe operation detected: DROP COLUMN `nickname`"},
				{Table: "archived_orders", Reason: "Unsafe operation detected: DROP TABLE"},
				{Table: "legacy_orders", Reason: "Unsafe operation detected: DROP TABLE"},
			},
			want:   "any dropped tables and the dropped column",
			wantOK: true,
		},
		{
			name: "multiple drop columns and tables",
			changes: []UnsafeChangeData{
				{Table: "customers", Reason: "Unsafe operation detected: DROP COLUMN `nickname`; Unsafe operation detected: DROP COLUMN `legacy_code`"},
				{Table: "archived_orders", Reason: "Unsafe operation detected: DROP TABLE"},
				{Table: "legacy_orders", Reason: "Unsafe operation detected: DROP TABLE"},
			},
			want:   "any dropped tables or columns",
			wantOK: true,
		},
		{
			name: "other unsafe change",
			changes: []UnsafeChangeData{
				{Table: "customers", Reason: "Unsafe operation detected: MODIFY COLUMN"},
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := unsafeDropUsageTarget(tt.changes)

			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRenderApplyStatusComment_Running(t *testing.T) {
	withTemplateTimestamp(t, "2026-06-16 19:42:00 UTC")
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
	assert.Contains(t, result, "_Last updated: <relative-time datetime=\"2026-06-16T19:42:00Z\">2026-06-16 19:42:00 UTC</relative-time> (2026-06-16 19:42:00 UTC)_")
	assert.NotContains(t, result, "**Last updated**")
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

func TestRenderApplyStatusComment_RowCopyDisplaysOnePercentAfterCopyStarts(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       state.Apply.Running,
		Engine:      "Spirit",
		Tables: []TableProgressData{
			{TableName: "users", Status: state.Task.Completed},
			{TableName: "orders", Status: state.Task.Running, RowsCopied: 3_000, RowsTotal: 1_604_159, PercentComplete: 0},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "1 running (1%)")
	assert.Contains(t, result, "**`orders`**: "+ui.ProgressBarRowCopy(1)+" 1%")
	assert.Contains(t, result, "Rows: 3,000 / 1,604,159")
	assert.NotContains(t, result, " 0%")
}

func TestRenderApplyStatusComment_EstimateExceeded(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       state.Apply.Running,
		Engine:      "Spirit",
		Tables: []TableProgressData{
			{TableName: "orders", Status: state.Task.Completed},
			{TableName: "users", DDL: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)", Status: state.Task.Running, RowsCopied: 145000, RowsTotal: 100000, PercentComplete: 145},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "1 running (Active)")
	assert.Contains(t, result, ui.ProgressBarActivity()+" Active")
	assert.Contains(t, result, "- Rows copied: 145,000 so far\n- ℹ️ _"+ui.EstimateExceededTooltip+"_")
	assert.NotContains(t, result, "[ℹ️](##")
	assert.NotContains(t, result, "<br>")
	assert.NotContains(t, result, "145%")
	assert.NotContains(t, result, "100%")
	assert.NotContains(t, result, "100,000 / 100,000")
}

func TestRenderApplyStatusComment_UsesOneRenderTimestamp(t *testing.T) {
	original := TimestampFunc
	timestamps := []string{"2026-06-16 19:42:00 UTC", "2026-06-16 19:42:01 UTC"}
	TimestampFunc = func() string {
		ts := timestamps[0]
		timestamps = timestamps[1:]
		return ts
	}
	t.Cleanup(func() { TimestampFunc = original })

	result := RenderApplyStatusComment(ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       state.Apply.Running,
		ApplyID:     "apply-abc123",
	})

	assert.Contains(t, result, "*Applied by @aparajon at 2026-06-16 19:42:00 UTC*")
	assert.Contains(t, result, "<relative-time datetime=\"2026-06-16T19:42:00Z\">2026-06-16 19:42:00 UTC</relative-time>")
	assert.NotContains(t, result, "2026-06-16 19:42:01 UTC")
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
	assert.NotContains(t, result, "Last updated")
}

func TestRenderApplyStatusComment_SQLFencesAreTopLevel(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		State:       state.Apply.Completed,
		Tables: []TableProgressData{
			{
				TableName: "example_cursor",
				DDL: "CREATE TABLE `example_cursor` (" +
					"`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, " +
					"`source` VARCHAR(64) NOT NULL, " +
					"PRIMARY KEY (`id`)" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				Status: state.Task.Completed,
			},
			{
				TableName: "example_state",
				DDL:       "ALTER TABLE `example_state` MODIFY COLUMN `version` VARCHAR(100) NOT NULL",
				Status:    state.Task.Completed,
			},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "### Table Progress")
	assert.NotContains(t, result, "\n- **`")
	assert.NotContains(t, result, "\n  ```sql")
	assert.NotContains(t, result, "\n  CREATE TABLE")
	assert.Contains(t, result, "**`example_cursor`**:")
	assert.Contains(t, result, "\n```sql\nCREATE TABLE `example_cursor`")
	assert.Contains(t, result, "\n```\n\n**`example_state`**:")
	assert.Contains(t, result, "\n```sql\nALTER TABLE `example_state`")
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

// A retryable failure is operator-recovery state, not a user-facing outcome:
// the comment keeps the in-progress headline, surfaces the retry and its last
// error on the affected table, and tells the user SchemaBot retries
// automatically.
func TestRenderApplyStatusComment_FailedRetryable(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       state.Apply.FailedRetryable,
		Engine:      "Spirit",
		ApplyID:     "apply-abc123",
		Tables: []TableProgressData{
			{TableName: "orders", DDL: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", Status: state.Task.Completed},
			{
				TableName:       "users",
				DDL:             "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
				Status:          state.Task.FailedRetryable,
				PercentComplete: 35,
				ErrorMessage:    "remote deployment unavailable",
			},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "## Schema Change In Progress")
	assert.NotContains(t, result, "Failed_retryable")
	assert.NotContains(t, result, "failed_retryable")
	// The retry detail lives on the affected table, not in the headline.
	assert.Contains(t, result, "🔄 Interrupted — retrying automatically")
	assert.Contains(t, result, "> ⚠️ Last error: remote deployment unavailable")
	assert.Contains(t, result, "🟧") // orange bar for the interrupted table
	// Progress summary counts the retrying table.
	assert.Contains(t, result, "1/2 complete")
	assert.Contains(t, result, "1 retrying")
	// Footer explains automatic retry — including the failure outcome when
	// retries are exhausted — and offers stop, not a manual re-apply.
	assert.Contains(t, result, "SchemaBot retries automatically and marks it failed if retries are exhausted")
	assert.Contains(t, result, "schemabot stop apply-abc123")
	assert.NotContains(t, result, "transient")
	assert.NotContains(t, result, "schemabot apply -e staging")
}

// Every apply state must render a human-readable headline. Raw snake_case
// state constants are internal vocabulary and must never appear in a PR
// comment title, including for states added after this test.
func TestApplyHeaderNeverLeaksRawStateConstants(t *testing.T) {
	applyStates := reflect.ValueOf(state.Apply)
	for _, field := range applyStates.Fields() {
		stateValue := field.String()
		t.Run(stateValue, func(t *testing.T) {
			var sb strings.Builder
			writeApplyHeader(&sb, ApplyStatusCommentData{State: stateValue})
			header := sb.String()
			require.NotEmpty(t, header)
			assert.NotContains(t, header, "_", "header for state %q leaks a raw state constant", stateValue)
		})
	}
}

// Storage task states arrive uppercase; the renderer must normalize them so a
// retrying task never falls through to the running renderer.
func TestRenderApplyStatusComment_FailedRetryableUppercaseStatus(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		State:       state.Apply.FailedRetryable,
		Tables: []TableProgressData{
			{TableName: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Status: "FAILED_RETRYABLE"},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "🔄 Interrupted — retrying automatically")
	assert.NotContains(t, result, "Running...")
}

// Engine errors can span multiple lines; every line must stay inside the
// blockquote so the error cannot break the structure of the rest of the
// comment.
func TestRenderApplyStatusComment_FailedRetryableMultilineError(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		State:       state.Apply.FailedRetryable,
		Tables: []TableProgressData{
			{
				TableName:    "users",
				DDL:          "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
				Status:       state.Task.FailedRetryable,
				ErrorMessage: "rpc error: code = Unavailable\ndesc = upstream connect error",
			},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "> ⚠️ Last error: rpc error: code = Unavailable\n> desc = upstream connect error")
}

func TestRenderApplyStatusComment_Stopped(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:     "testapp",
		Environment:  "staging",
		RequestedBy:  "aparajon",
		State:        "stopped",
		Engine:       "Spirit",
		ErrorMessage: "remote apply remote-123 remained stopped after start grace period 30s",
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
	assert.Contains(t, result, "remote apply remote-123 remained stopped after start grace period 30s")
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

func TestRenderApplyStatusComment_Recovering(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       state.Apply.Recovering,
		Engine:      "Spirit",
		Tables: []TableProgressData{
			{TableName: "orders", Status: state.Task.Completed},
			{TableName: "users", Status: state.Task.Recovering},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "Recovering")
	assert.Contains(t, result, "1 recovering")
	assert.Contains(t, result, "Recovering state...")
	assert.Contains(t, result, "Cutover will be available once recovery completes")
	assert.NotContains(t, result, "schemabot cutover")
}

func TestRenderApplyStatusComment_RecoveringCopyingRows(t *testing.T) {
	data := ApplyStatusCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "aparajon",
		State:       state.Apply.Recovering,
		Engine:      "Spirit",
		Tables: []TableProgressData{
			{TableName: "users", Status: state.Task.Recovering, RowsCopied: 420, RowsTotal: 1000, PercentComplete: 42, ETASeconds: 120},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "Row copy in progress (42%)")
	assert.Contains(t, result, "Rows: 420 / 1,000 · ETA: 2m")
	assert.Contains(t, result, "Row copy is in progress (42%)")
	assert.Contains(t, result, "progress returns to the normal row-copy view")
	assert.Contains(t, result, "Recovering after restart")
	assert.NotContains(t, result, "Cutover will be available once recovery completes")
	assert.NotContains(t, result, "schemabot cutover")
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

func TestRenderPRCommandNotAuthorized(t *testing.T) {
	result := RenderPRCommandNotAuthorized(ActorAuthorizationCommentData{
		RequestedBy: "mona",
		CommandName: "apply",
		Database:    "orders",
		Environment: "staging",
	})

	assert.Contains(t, result, "SchemaBot Command Not Authorized")
	assert.Contains(t, result, "`orders`")
	assert.Contains(t, result, "`staging`")
	assert.Contains(t, result, "@mona is not authorized")
	assert.Contains(t, result, "`schemabot apply`")
	assert.Contains(t, result, "SchemaBot admin/database operator")
}

func TestRenderPRCommandAuthorizationUnavailable(t *testing.T) {
	result := RenderPRCommandAuthorizationUnavailable(ActorAuthorizationCommentData{
		RequestedBy: "mona",
		CommandName: "apply-confirm",
		Database:    "orders",
		Environment: "production",
	})

	assert.Contains(t, result, "SchemaBot Authorization Check Failed")
	assert.Contains(t, result, "`orders`")
	assert.Contains(t, result, "`production`")
	assert.Contains(t, result, "could not verify authorization")
	assert.Contains(t, result, "No schema change was started")
	assert.Contains(t, result, "GitHub App can read organization members")
	assert.Contains(t, result, "inspect SchemaBot authorization logs")
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

func TestPreviewCommentApplyEstimateExceeded(t *testing.T) {
	result := PreviewCommentApplyEstimateExceeded()

	assert.Contains(t, result, "Schema Change In Progress")
	assert.Contains(t, result, "1 running (Active)")
	assert.Contains(t, result, "Active")
	assert.Contains(t, result, "Rows copied: 145,000,000 so far")
	assert.NotContains(t, result, "145%")
	assert.NotContains(t, result, "100,000,000 / 100,000,000")
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

func TestPreviewCommentSummaryCompletedLargeSingleNamespaceKeepsApplyIDInsideSection(t *testing.T) {
	result := PreviewCommentSummaryCompletedLarge()

	assert.Contains(t, result, "All 8 tables applied successfully")
	assert.Contains(t, result, "_Apply ID: `apply-a1b2c3d4e5f6`_")
	assert.Equal(t, 0, strings.Count(result, "</details>"))
}

func TestRenderApplySummaryCommentCompletedCollapsedGroupSeparatesApplyID(t *testing.T) {
	tableNames := []string{"users", "orders", "products", "invoices", "payments", "shipments"}
	tables := make([]TableProgressData, 0, len(tableNames))
	for _, tableName := range tableNames {
		tables = append(tables, TableProgressData{
			Namespace: "testapp_primary",
			TableName: tableName,
			DDL: fmt.Sprintf(
				"CREATE TABLE `%s` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				tableName,
			),
			Status: state.Task.Completed,
		})
	}
	data := ApplyStatusCommentData{
		ApplyID:     "apply-a1b2c3d4e5f6",
		Database:    "testapp",
		Environment: "staging",
		State:       state.Apply.Completed,
		Tables:      tables,
	}

	result := RenderApplySummaryComment(data)

	assert.Contains(t, result, "<details><summary>✅ <strong>testapp_primary</strong> (6 tables)</summary>")
	assert.Contains(t, result, "</details>\n\n_Apply ID: `apply-a1b2c3d4e5f6`_")
	assert.NotContains(t, result, "</details>\n_Apply ID")
}

func TestRenderApplyBlockedByNonPassingChecks(t *testing.T) {
	notPassing := []BlockingCheck{
		{Name: "CI / unit-tests", State: "failure"},
		{Name: "CI / lint", State: "timed_out"},
	}

	result := RenderApplyBlockedByNonPassingChecks("staging", notPassing)

	assert.Contains(t, result, "## ❌ Apply Blocked")
	assert.Contains(t, result, "`staging`")
	assert.Contains(t, result, "Cannot apply while PR checks are not passing")
	assert.Contains(t, result, "| Check | Status |")
	assert.Contains(t, result, "| `CI / unit-tests` | failure |")
	assert.Contains(t, result, "| `CI / lint` | timed_out |")
	assert.Contains(t, result, "schemabot apply -e staging")
}

func TestRenderApplyBlockedByNonPassingChecks_SingleCheck(t *testing.T) {
	notPassing := []BlockingCheck{
		{Name: "security-scan", State: "error"},
	}

	result := RenderApplyBlockedByNonPassingChecks("production", notPassing)

	assert.Contains(t, result, "`production`")
	assert.Contains(t, result, "| `security-scan` | error |")
	assert.Contains(t, result, "schemabot apply -e production")
}

func TestRenderApplyBlockedByNonPassingChecks_EmptyList(t *testing.T) {
	// Defensive guard: rendering with an empty slice must not emit an empty
	// Markdown table (header row with zero data rows). It should fall back
	// to a generic message that still preserves the header, environment,
	// and retry block.
	for _, notPassing := range [][]BlockingCheck{nil, {}} {
		result := RenderApplyBlockedByNonPassingChecks("staging", notPassing)

		assert.Contains(t, result, "## ❌ Apply Blocked")
		assert.Contains(t, result, "**Environment**: `staging`")
		assert.Contains(t, result, "Cannot apply while PR checks are not passing.")
		assert.Contains(t, result, "Get the checks passing — fix failures and re-run cancelled or stale checks — then retry:\n```\nschemabot apply -e staging\n```",
			"retry command must be inside a fenced code block immediately after the retry copy")
		assert.NotContains(t, result, "| Check | Status |",
			"empty-list branch must not emit a table header with no data rows")
		assert.NotContains(t, result, "|-------|--------|",
			"empty-list branch must not emit a table separator with no data rows")
	}
}

func TestRenderApplyBlockedByCheckStatusError(t *testing.T) {
	t.Run("generic error is shown verbatim with retry block", func(t *testing.T) {
		err := errors.New("get combined commit status: 500 Internal Server Error")

		result := RenderApplyBlockedByCheckStatusError("staging", err, nil)

		assert.Contains(t, result, "## ❌ Apply Blocked")
		assert.Contains(t, result, "**Environment**: `staging`")
		assert.Contains(t, result, "Unable to verify PR check statuses")
		assert.Contains(t, result, "get combined commit status: 500 Internal Server Error")
		assert.Contains(t, result, "Resolve the issue and retry:\n```\nschemabot apply -e staging\n```",
			"retry command must be inside a fenced code block immediately after the retry copy")
	})

	t.Run("permission error surfaces a targeted hint with retry block", func(t *testing.T) {
		err := errors.New("GET https://api.github.com/...: 403 Resource not accessible by integration")

		result := RenderApplyBlockedByCheckStatusError("production", err, &CheckStatusAccessDetails{
			GitHubApp:          "schemabot-prod",
			MissingPermissions: []string{"Checks: Read"},
		})

		assert.Contains(t, result, "## ❌ Apply Blocked")
		assert.Contains(t, result, "**Environment**: `production`")
		assert.Contains(t, result, "SchemaBot GitHub App `schemabot-prod`")
		assert.Contains(t, result, "cannot read PR check statuses")
		assert.Contains(t, result, "**Checks: Read**")
		assert.NotContains(t, result, "**Commit statuses: Read**")
		assert.Contains(t, result, "then retry:\n```\nschemabot apply -e production\n```",
			"retry command must be inside a fenced code block immediately after the retry copy")
		assert.NotContains(t, result, "Unable to verify PR check statuses",
			"permission branch should replace the generic verbatim message")
		assert.NotContains(t, result, "Resolve the issue and retry:",
			"permission branch should not also emit the generic-branch retry copy")
	})

	t.Run("permission error explains ambiguous check-status failure when REST probes pass", func(t *testing.T) {
		err := errors.New("read check statuses for abc123: Resource not accessible by integration")

		result := RenderApplyBlockedByCheckStatusError("production", err, &CheckStatusAccessDetails{
			GitHubApp:              "schemabot-prod",
			ChecksReadable:         true,
			CommitStatusesReadable: true,
		})

		assert.Contains(t, result, "SchemaBot GitHub App `schemabot-prod`")
		assert.Contains(t, result, "Diagnostic REST probes could read both **Checks** and **Commit statuses**")
		assert.Contains(t, result, "inspect the SchemaBot logs")
		assert.NotContains(t, result, "Grant or accept those permissions")
	})

	t.Run("nil error skips empty fence and uses concise retry copy", func(t *testing.T) {
		result := RenderApplyBlockedByCheckStatusError("staging", nil, nil)

		assert.Contains(t, result, "## ❌ Apply Blocked")
		assert.Contains(t, result, "**Environment**: `staging`")
		assert.Contains(t, result, "Unable to verify PR check statuses.")
		assert.Contains(t, result, "Retry:\n```\nschemabot apply -e staging\n```",
			"retry command must be inside a fenced code block immediately after the retry copy")
		assert.NotContains(t, result, "```\n```",
			"nil-error branch should not emit an empty fenced code block")
		assert.NotContains(t, result, "Resolve the issue and retry:",
			"nil-error branch should not reference an issue that was not surfaced")
	})
}

func TestRenderApplyBlockedByPriorEnvCheckError(t *testing.T) {
	t.Run("renders reason and wrapped error verbatim", func(t *testing.T) {
		err := errors.New("404 Not Found")

		result := RenderApplyBlockedByPriorEnvCheckError("staging", "fetch PR details", err)

		assert.Contains(t, result, "## ❌ Apply Blocked")
		assert.Contains(t, result, "Could not verify staging status: failed to fetch PR details. Retry the apply command.")
		assert.Contains(t, result, "_Error: 404 Not Found_")
	})

	t.Run("each reason variant produces matching body", func(t *testing.T) {
		err := errors.New("boom")

		for _, reason := range []string{"create GitHub client", "fetch PR details", "query check runs"} {
			result := RenderApplyBlockedByPriorEnvCheckError("production", reason, err)
			assert.Contains(t, result, "Could not verify production status: failed to "+reason+". Retry the apply command.")
		}
	})

	t.Run("nil error renders <nil>", func(t *testing.T) {
		result := RenderApplyBlockedByPriorEnvCheckError("staging", "query check runs", nil)

		assert.Contains(t, result, "## ❌ Apply Blocked")
		assert.Contains(t, result, "_Error: <nil>_")
	})

	t.Run("output matches prior inline rendering byte-for-byte", func(t *testing.T) {
		err := errors.New("rate limited")
		priorEnv := "staging"
		reason := "create GitHub client"

		expected := "## ❌ Apply Blocked\n\nCould not verify " + priorEnv + " status: failed to " + reason + ". Retry the apply command.\n\n_Error: " + err.Error() + "_"

		assert.Equal(t, expected, RenderApplyBlockedByPriorEnvCheckError(priorEnv, reason, err))
	})
}

func TestRenderApplyBlockedByMissingPriorEnvCheck(t *testing.T) {
	result := RenderApplyBlockedByMissingPriorEnvCheck("staging")

	assert.Contains(t, result, "## ❌ Apply Blocked")
	assert.Contains(t, result, "could not find a completed `staging` check")
	assert.Contains(t, result, "schemabot plan -e staging")
	assert.Contains(t, result, "apply `staging`")
	assert.NotContains(t, result, "Retry the apply command")
}

func TestRenderApplyBlockedByUntrustedPriorEnvCheck(t *testing.T) {
	result := RenderApplyBlockedByUntrustedPriorEnvCheck("staging", "SchemaBot (staging)", []string{"schemabot-staging"})

	assert.Contains(t, result, "## ❌ Apply Blocked")
	assert.Contains(t, result, "`SchemaBot (staging)`")
	assert.Contains(t, result, "- `schemabot-staging`")
	assert.Contains(t, result, "does not trust")
	assert.Contains(t, result, "trusted-check-app-slugs")
	assert.Contains(t, result, "Re-running `schemabot plan -e staging` will not resolve this")
	assert.NotContains(t, result, "could not find a completed")
}

func TestRenderApplyBlockedByInProgressChecks(t *testing.T) {
	inProgress := []BlockingCheck{
		{Name: "CI / unit-tests", State: "in_progress"},
		{Name: "CI / integration", State: "queued"},
	}

	result := RenderApplyBlockedByInProgressChecks("staging", inProgress, nil)

	assert.Contains(t, result, "⏳ Apply Blocked")
	assert.Contains(t, result, "`staging`")
	assert.Contains(t, result, "still running")
	assert.Contains(t, result, "| `CI / unit-tests` | in_progress |")
	assert.Contains(t, result, "| `CI / integration` | queued |")
	assert.Contains(t, result, "Wait for checks to complete")
	assert.Contains(t, result, "schemabot apply -e staging")
	assert.NotContains(t, result, "not reported",
		"in-progress-only render must not surface the never-reported remediation")
}

// A configured required check that has never reported on the commit gets
// remediation distinct from the still-running checks: waiting will not unblock
// it, so the operator is told to verify the name and that the check runs on the
// PR rather than to wait indefinitely.
func TestRenderApplyBlockedByInProgressChecks_NotReported(t *testing.T) {
	notReported := []BlockingCheck{
		{Name: "Security scan", State: "not reported"},
	}

	result := RenderApplyBlockedByInProgressChecks("production", nil, notReported)

	assert.Contains(t, result, "⏳ Apply Blocked")
	assert.Contains(t, result, "`production`")
	assert.Contains(t, result, "have not reported on this commit")
	assert.Contains(t, result, "| `Security scan` | not reported |")
	assert.Contains(t, result, "If a check never reports, waiting will not unblock the apply.")
	assert.Contains(t, result, "Verify the name in `required_checks` matches the check exactly and that it runs on this PR")
	assert.Contains(t, result, "schemabot apply -e production")
	assert.NotContains(t, result, "Cannot apply while PR checks are still running",
		"not-reported-only render must not surface the wait-and-retry headline")
}

// When both still-running checks and never-reported required checks block the
// same apply, each cause keeps its own remediation: the running checks get the
// wait-and-retry guidance and the missing required checks get the verify-the-name
// guidance, so the operator does not wait indefinitely on a check that will
// never report.
func TestRenderApplyBlockedByInProgressChecks_InProgressAndNotReported(t *testing.T) {
	inProgress := []BlockingCheck{
		{Name: "CI / unit-tests", State: "in_progress"},
	}
	notReported := []BlockingCheck{
		{Name: "Security scan", State: "not reported"},
	}

	result := RenderApplyBlockedByInProgressChecks("staging", inProgress, notReported)

	assert.Contains(t, result, "Cannot apply while PR checks are still running:")
	assert.Contains(t, result, "| `CI / unit-tests` | in_progress |")
	assert.Contains(t, result, "Wait for checks to complete and retry:")
	assert.Contains(t, result, "These required checks have not reported on this commit:")
	assert.Contains(t, result, "| `Security scan` | not reported |")
	assert.Contains(t, result, "Verify the name in `required_checks` matches the check exactly and that it runs on this PR")
}

func TestRenderApplyBlockedByInProgressChecks_EmptyList(t *testing.T) {
	// Defensive guard: rendering with empty slices must not emit an empty
	// Markdown table (header row with zero data rows). It should fall back
	// to a generic message that still preserves the header, environment,
	// and retry block.
	for _, inProgress := range [][]BlockingCheck{nil, {}} {
		result := RenderApplyBlockedByInProgressChecks("staging", inProgress, nil)

		assert.Contains(t, result, "## ⏳ Apply Blocked")
		assert.Contains(t, result, "**Environment**: `staging`")
		assert.Contains(t, result, "Cannot apply until PR checks finish verifying this commit.")
		assert.Contains(t, result, "Wait for checks to complete and retry:\n```\nschemabot apply -e staging\n```",
			"retry command must be inside a fenced code block immediately after the retry copy")
		assert.NotContains(t, result, "| Check | Status |",
			"empty-list branch must not emit a table header with no data rows")
		assert.NotContains(t, result, "|-------|--------|",
			"empty-list branch must not emit a table separator with no data rows")
	}
}

func TestRenderApplyStatusComment_WaitingForCutover_ReadyNotReady(t *testing.T) {
	data := ApplyStatusCommentData{
		ApplyID:     "apply-abc123",
		Database:    "testapp",
		Environment: "staging",
		State:       state.Apply.WaitingForCutover,
		Tables: []TableProgressData{
			{TableName: "users", Status: state.Task.WaitingForCutover, ReadyToComplete: true, DDL: "ALTER TABLE users ADD INDEX idx_email (email)"},
			{TableName: "orders", Status: state.Task.WaitingForCutover, ReadyToComplete: true, DDL: "ALTER TABLE orders ADD INDEX idx_status (status)"},
			{TableName: "items", Status: state.Task.WaitingForCutover, ReadyToComplete: false, DDL: "ALTER TABLE items ADD INDEX idx_price (price_cents)"},
		},
	}

	result := RenderApplyStatusComment(data)

	// Header
	assert.Contains(t, result, "Waiting for Cutover")

	// Cutover summary shows ready/waiting counts
	assert.Contains(t, result, "2/3")
	assert.Contains(t, result, "waiting on 1")

	// Per-table: ready tables show checkmark, non-ready show plain waiting
	assert.Contains(t, result, "Ready for cutover")
	assert.Contains(t, result, "Waiting for cutover")

	// Footer has cutover command
	assert.Contains(t, result, "schemabot cutover apply-abc123")
}

func TestRenderApplyStatusComment_WaitingForCutover_AllReady(t *testing.T) {
	data := ApplyStatusCommentData{
		ApplyID:     "apply-abc123",
		Database:    "testapp",
		Environment: "staging",
		State:       state.Apply.WaitingForCutover,
		Tables: []TableProgressData{
			{TableName: "users", Status: state.Task.WaitingForCutover, ReadyToComplete: true},
			{TableName: "orders", Status: state.Task.WaitingForCutover, ReadyToComplete: true},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "2/2")
	assert.NotContains(t, result, "waiting on")
}

func TestRenderApplyStatusComment_RevertWindow(t *testing.T) {
	data := ApplyStatusCommentData{
		ApplyID:     "apply-abc123",
		Database:    "testapp",
		Environment: "staging",
		State:       state.Apply.RevertWindow,
		Tables: []TableProgressData{
			{TableName: "users", Status: state.Task.RevertWindow},
		},
	}

	result := RenderApplyStatusComment(data)

	assert.Contains(t, result, "Pending Revert")
	assert.Contains(t, result, "Complete (pending revert)")
	assert.Contains(t, result, "schemabot revert apply-abc123")
	assert.Contains(t, result, "schemabot skip-revert apply-abc123")
}
