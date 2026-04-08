package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderRollbackPlanComment_WithChanges(t *testing.T) {
	data := PlanCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "testuser",
		IsMySQL:     true,
		Changes: []KeyspaceChangeData{
			{
				Keyspace: "testapp",
				Statements: []string{
					"ALTER TABLE `users` DROP INDEX `idx_email`",
					"ALTER TABLE `orders` DROP COLUMN `notes`",
				},
			},
		},
	}

	rendered := RenderRollbackPlanComment(data)
	assert.Contains(t, rendered, "## MySQL Schema Rollback Plan")
	assert.Contains(t, rendered, "@testuser")
	assert.Contains(t, rendered, "`testapp`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "DROP INDEX")
	assert.Contains(t, rendered, "DROP COLUMN")
	assert.Contains(t, rendered, "destructive changes")
	assert.Contains(t, rendered, "schemabot rollback-confirm -e staging")
	assert.Contains(t, rendered, "schemabot unlock")
}

func TestRenderRollbackPlanComment_NoChanges(t *testing.T) {
	data := PlanCommentData{
		Database:    "testapp",
		Environment: "production",
		RequestedBy: "testuser",
		IsMySQL:     true,
		Changes:     nil,
	}

	rendered := RenderRollbackPlanComment(data)
	assert.Contains(t, rendered, "## MySQL Schema Rollback Plan")
	assert.Contains(t, rendered, "already matches the original schema")
	assert.NotContains(t, rendered, "schemabot rollback-confirm")
}

func TestRenderRollbackPlanComment_Vitess(t *testing.T) {
	data := PlanCommentData{
		Database:    "myks",
		Environment: "staging",
		RequestedBy: "admin",
		IsMySQL:     false,
		Changes: []KeyspaceChangeData{
			{
				Keyspace:   "myks",
				Statements: []string{"ALTER TABLE `t1` DROP INDEX `idx_foo`"},
			},
		},
	}

	rendered := RenderRollbackPlanComment(data)
	assert.Contains(t, rendered, "## Vitess Schema Rollback Plan")
	assert.Contains(t, rendered, "schemabot rollback-confirm -e staging")
}

func TestRenderRollbackPlanComment_WithLintWarnings(t *testing.T) {
	data := PlanCommentData{
		Database:    "testapp",
		Environment: "staging",
		RequestedBy: "testuser",
		IsMySQL:     true,
		Changes: []KeyspaceChangeData{
			{
				Keyspace:   "testapp",
				Statements: []string{"ALTER TABLE `users` DROP INDEX `idx_email`"},
			},
		},
		LintWarnings: []LintWarningData{
			{Message: "Dropping index may impact queries", Table: "users"},
		},
	}

	rendered := RenderRollbackPlanComment(data)
	assert.Contains(t, rendered, "Lint Warnings")
	assert.Contains(t, rendered, "[users] Dropping index may impact queries")
}

func TestRenderRollbackNoCompletedApply(t *testing.T) {
	rendered := RenderRollbackNoCompletedApply("testapp", "staging")
	assert.Contains(t, rendered, "## ℹ️ No Completed Schema Change to Rollback")
	assert.Contains(t, rendered, "`testapp`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "no completed schema change")
}

func TestRenderRollbackConfirmNoLock(t *testing.T) {
	rendered := RenderRollbackConfirmNoLock("testapp", "staging")
	assert.Contains(t, rendered, "## 🔒 No Lock Found")
	assert.Contains(t, rendered, "`testapp`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "schemabot rollback -e staging")
}
