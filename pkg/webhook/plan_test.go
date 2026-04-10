package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

func TestBuildPlanCommentData_UnsafeChangesPopulated(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{
		Database: "testdb",
		Type:     "mysql",
	}

	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "testdb",
			TableChanges: []*apitypes.TableChangeResponse{{
				TableName:    "orders",
				DDL:          "ALTER TABLE `orders` DROP INDEX `idx_status`",
				ChangeType:   "alter",
				IsUnsafe:     true,
				UnsafeReason: "DROP INDEX without making invisible first",
			}},
		}},
		LintResults: []*apitypes.LintWarningResponse{{
			Message:  "Index 'idx_status' should be made invisible before dropping",
			Table:    "orders",
			Linter:   "invisible_index_before_drop",
			Severity: "error",
		}},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "testuser")

	assert.True(t, data.HasUnsafeChanges, "expected HasUnsafeChanges=true when plan contains unsafe table changes")
	require.Len(t, data.UnsafeChanges, 1)
	assert.Equal(t, "orders", data.UnsafeChanges[0].Table)
	assert.Equal(t, "DROP INDEX without making invisible first", data.UnsafeChanges[0].Reason)
}

func TestBuildPlanCommentData_NoUnsafeChanges(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{
		Database: "testdb",
		Type:     "mysql",
	}

	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "testdb",
			TableChanges: []*apitypes.TableChangeResponse{{
				TableName:  "orders",
				DDL:        "ALTER TABLE `orders` ADD COLUMN `status2` varchar(50)",
				ChangeType: "alter",
				IsUnsafe:   false,
			}},
		}},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "testuser")

	assert.False(t, data.HasUnsafeChanges)
	assert.Empty(t, data.UnsafeChanges)
}

func TestBuildPlanCommentData_MixedSafeAndUnsafe(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{
		Database: "testdb",
		Type:     "mysql",
	}

	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "testdb",
			TableChanges: []*apitypes.TableChangeResponse{
				{
					TableName:  "orders",
					DDL:        "ALTER TABLE `orders` ADD COLUMN `status2` varchar(50)",
					ChangeType: "alter",
					IsUnsafe:   false,
				},
				{
					TableName:    "users",
					DDL:          "DROP TABLE `users`",
					ChangeType:   "drop",
					IsUnsafe:     true,
					UnsafeReason: "DROP TABLE removes all data",
				},
			},
		}},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "testuser")

	assert.True(t, data.HasUnsafeChanges)
	require.Len(t, data.UnsafeChanges, 1)
	assert.Equal(t, "users", data.UnsafeChanges[0].Table)
}

func TestRenderPlanComment_ShowsUnsafeWarning(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `orders` DROP INDEX `idx_status`"},
		}},
		HasUnsafeChanges: true,
		UnsafeChanges: []templates.UnsafeChangeData{{
			Table:  "orders",
			Reason: "DROP INDEX without making invisible first",
		}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "⛔ Unsafe Changes Detected")
	assert.Contains(t, rendered, "`orders`")
	assert.Contains(t, rendered, "DROP INDEX without making invisible first")
	// Plan comment should NOT say "--allow-unsafe enabled" since it wasn't
	assert.NotContains(t, rendered, "--allow-unsafe` enabled")
}

func TestRenderPlanComment_UnsafeWithAllowUnsafe(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		IsLocked:    true,
		AllowUnsafe: true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `users`"},
		}},
		HasUnsafeChanges: true,
		UnsafeChanges: []templates.UnsafeChangeData{{
			Table:  "users",
			Reason: "DROP TABLE removes all data",
		}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "--allow-unsafe` enabled")
	assert.Contains(t, rendered, "`users`")
	assert.Contains(t, rendered, "apply-confirm -e staging --allow-unsafe")
}

func TestRenderPlanComment_NoUnsafe_NoWarning(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `orders` ADD COLUMN `x` INT"},
		}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.NotContains(t, rendered, "Unsafe")
}

func TestRenderUnsafeChangesBlocked_UsedByApplyFlow(t *testing.T) {
	// Verify RenderUnsafeChangesBlocked produces the expected blocking content
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `users`"},
		}},
		HasUnsafeChanges: true,
		UnsafeChanges: []templates.UnsafeChangeData{{
			Table:  "users",
			Reason: "DROP TABLE removes all data",
		}},
	}

	rendered := templates.RenderUnsafeChangesBlocked(data)

	assert.Contains(t, rendered, "⛔ Unsafe Changes Detected")
	assert.Contains(t, rendered, "`users`")
	assert.Contains(t, rendered, "DROP TABLE removes all data")
	assert.Contains(t, rendered, "--allow-unsafe")
	assert.Contains(t, rendered, "schemabot apply -e staging --allow-unsafe")
}
