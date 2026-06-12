package templates

import (
	"strings"
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

func TestRenderRollbackPlanComment_WithLintViolations(t *testing.T) {
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
		LintViolations: []LintViolationData{
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
	assert.Contains(t, rendered, "schemabot rollback <apply-id> -e staging")
}

func TestRenderRollbackMissingApplyID(t *testing.T) {
	rendered := RenderRollbackMissingApplyID()
	assert.Contains(t, rendered, "## Missing Apply ID")
	assert.Contains(t, rendered, "schemabot rollback <apply-id> -e <environment>")
	assert.Contains(t, rendered, "schemabot status")
}

func TestRenderRollbackApplyNotFound(t *testing.T) {
	rendered := RenderRollbackApplyNotFound("apply-abc123")
	assert.Contains(t, rendered, "## Apply Not Found")
	assert.Contains(t, rendered, "`apply-abc123`")
	assert.Contains(t, rendered, "Check the ID and try again")
}

func TestRenderRollbackBlockedByLock(t *testing.T) {
	t.Run("PR-owned lock renders as link", func(t *testing.T) {
		rendered := RenderRollbackBlockedByLock("testapp", "staging", "block/myapp#42", "block/myapp", 42)

		assert.Contains(t, rendered, "## Rollback Blocked")
		assert.Contains(t, rendered, "`testapp`")
		assert.Contains(t, rendered, "`staging`")
		assert.Contains(t, rendered, "[block/myapp#42](https://github.com/block/myapp/pull/42)")
		assert.Contains(t, rendered, "schemabot unlock")
		assert.NotContains(t, rendered, "`block/myapp#42`",
			"PR-link variant should not render the owner as a bare backticked string")
	})

	t.Run("non-PR lock renders bare owner", func(t *testing.T) {
		rendered := RenderRollbackBlockedByLock("testapp", "staging", "cli:alice@laptop", "", 0)

		assert.Contains(t, rendered, "## Rollback Blocked")
		assert.Contains(t, rendered, "`cli:alice@laptop`")
		assert.Contains(t, rendered, "ask the lock owner to release it")
		assert.NotContains(t, rendered, "github.com",
			"bare-owner variant should not include any github.com link")
		assert.NotContains(t, rendered, "schemabot unlock",
			"bare-owner variant should not suggest schemabot unlock")
	})

	t.Run("missing repo falls back to bare owner even with PR > 0", func(t *testing.T) {
		rendered := RenderRollbackBlockedByLock("testapp", "staging", "stale-owner", "", 99)
		assert.Contains(t, rendered, "`stale-owner`")
		assert.NotContains(t, rendered, "github.com")
	})
}

func TestRenderRollbackNothingToDo(t *testing.T) {
	rendered := RenderRollbackNothingToDo("testapp", "staging", "apply-xyz")
	assert.Contains(t, rendered, "## Nothing to Rollback")
	assert.Contains(t, rendered, "`testapp`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "before apply `apply-xyz`")
}

func TestRenderRollbackLockNotOwned(t *testing.T) {
	rendered := RenderRollbackLockNotOwned("testapp", "production", "block/other#7")
	assert.Contains(t, rendered, "## Lock Not Owned")
	assert.Contains(t, rendered, "`testapp`")
	assert.Contains(t, rendered, "`production`")
	assert.Contains(t, rendered, "held by `block/other#7`, not this PR")
}

func TestRenderRollbackAlreadyRolledBack(t *testing.T) {
	rendered := RenderRollbackAlreadyRolledBack("testapp", "staging")
	assert.Contains(t, rendered, "## Already Rolled Back")
	assert.Contains(t, rendered, "`testapp`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "Lock released")
}

func TestRenderRollbackAlreadyRolledBackLockHeld(t *testing.T) {
	rendered := RenderRollbackAlreadyRolledBackLockHeld("testapp", "staging", "block/myapp#42")
	assert.Contains(t, rendered, "## Already Rolled Back")
	assert.Contains(t, rendered, "`testapp`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "failed to release the lock held by `block/myapp#42`")
	assert.Contains(t, rendered, "Applies on this database will be blocked until the lock is released")
	assert.Contains(t, rendered, "schemabot unlock")
	assert.Contains(t, rendered, "schemabot unlock -d testapp --force")
	assert.NotContains(t, rendered, "Lock released",
		"a failed release must not claim the lock was released")
}

func TestRenderRollbackNotAccepted(t *testing.T) {
	rendered := RenderRollbackNotAccepted("testapp", "staging", "plan not found")
	assert.Contains(t, rendered, "## Rollback Not Accepted")
	assert.Contains(t, rendered, "`testapp`")
	assert.Contains(t, rendered, "`staging`")
	assert.Contains(t, rendered, "plan not found")
}

func TestRollbackTemplates_NoStrayWhitespace(t *testing.T) {
	for name, body := range map[string]string{
		"MissingApplyID":            RenderRollbackMissingApplyID(),
		"ApplyNotFound":             RenderRollbackApplyNotFound("a"),
		"BlockedByLockPR":           RenderRollbackBlockedByLock("d", "e", "o", "r", 1),
		"BlockedByLockOwner":        RenderRollbackBlockedByLock("d", "e", "o", "", 0),
		"NothingToDo":               RenderRollbackNothingToDo("d", "e", "a"),
		"LockNotOwned":              RenderRollbackLockNotOwned("d", "e", "o"),
		"AlreadyRolledBack":         RenderRollbackAlreadyRolledBack("d", "e"),
		"AlreadyRolledBackLockHeld": RenderRollbackAlreadyRolledBackLockHeld("d", "e", "o"),
		"NotAccepted":               RenderRollbackNotAccepted("d", "e", "x"),
	} {
		assert.False(t, strings.HasPrefix(body, " ") || strings.HasPrefix(body, "\n"),
			"%s body should not start with whitespace", name)
	}
}
