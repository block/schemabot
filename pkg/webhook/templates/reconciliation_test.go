package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderSchemaChangeReconciliationRequiredInProgress(t *testing.T) {
	rendered := RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{
		RequestedBy: "alice",
		Timestamp:   "2026-06-14 12:34:56",
		Items: []SchemaChangeReconciliationItem{
			{
				Database:    "orders",
				Environment: "staging",
				ApplyID:     "apply-1234",
				State:       "running",
				InProgress:  true,
			},
		},
	})

	assert.Contains(t, rendered, "## ⚠️ Schema Change Reconciliation Required")
	assert.Contains(t, rendered, "SchemaBot is still applying a schema change from this PR")
	assert.Contains(t, rendered, "The live database operation was already started")
	assert.Contains(t, rendered, "schemabot stop apply-1234 -e staging")
	assert.Contains(t, rendered, "schemabot rollback apply-1234 -e staging")
	assert.Contains(t, rendered, "schemabot plan -e staging -d orders")
	assert.Contains(t, rendered, "push a no-op `schemabot.yaml` edit to trigger a fresh plan")
	assert.NotContains(t, rendered, "ask an operator")
	assert.NotContains(t, rendered, "Git reverting")
	assert.NotContains(t, rendered, "Removing the schema change")
}

func TestRenderSchemaChangeReconciliationRequiredCompleted(t *testing.T) {
	rendered := RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{
		RequestedBy: "alice",
		Timestamp:   "2026-06-14 12:34:56",
		Items: []SchemaChangeReconciliationItem{
			{
				Database:    "orders",
				Environment: "staging",
				ApplyID:     "apply-1234",
				State:       "completed",
			},
		},
	})

	assert.Contains(t, rendered, "## ⚠️ Schema Change Reconciliation Required")
	assert.Contains(t, rendered, "SchemaBot already applied a schema change from this PR")
	assert.Contains(t, rendered, "The live database was already updated")
	assert.Contains(t, rendered, "Keep the live schema change")
	assert.Contains(t, rendered, "Undo the live schema change")
	assert.Contains(t, rendered, "schemabot rollback apply-1234 -e staging")
	assert.Contains(t, rendered, "schemabot plan -e staging -d orders")
	assert.Contains(t, rendered, "push a no-op `schemabot.yaml` edit to trigger a fresh plan")
	assert.NotContains(t, rendered, "ask an operator")
	assert.NotContains(t, rendered, "schemabot status")
	assert.NotContains(t, rendered, "Git reverting")
}

func TestRenderNoManagedSchemaChangesChecksRefreshed(t *testing.T) {
	t.Run("plain refresh explains why no plan ran and how a nonce edit gets one", func(t *testing.T) {
		rendered := RenderNoManagedSchemaChangesChecksRefreshed(NoManagedSchemaChangesChecksRefreshedData{
			RequestedBy: "alice",
			Repository:  "acme/payments",
			HeadSHA:     "abcdef1234567890abcdef1234567890abcdef12",
		})

		assert.Contains(t, rendered, "## ℹ️ No Schema Files Changed")
		assert.Contains(t, rendered, "refreshed as passing on [`abcdef1`](https://github.com/acme/payments/commit/abcdef1234567890abcdef1234567890abcdef12).")
		assert.Contains(t, rendered, "<summary>Expected a plan?</summary>")
		assert.Contains(t, rendered, "This PR changes none, so no database was compared against its schema directory.")
		assert.Contains(t, rendered, "- **A new database.** Its schema directory merged before the database was configured")
		assert.Contains(t, rendered, "- **An existing database with drift.** The live schema was changed outside SchemaBot: DDL run by hand during an incident")
		assert.Contains(t, rendered, "add or toggle a `# nonce` comment line in its `schemabot.yaml` and push")
		assert.Contains(t, rendered, "the plan comment carries the apply command")
		assert.True(t, strings.HasSuffix(rendered, "\n_Requested by @alice_\n"), "attribution closes the comment: %q", rendered)
		assert.NotContains(t, rendered, "UTC")
	})

	t.Run("refresh gated on tenants does not offer the named-database plan", func(t *testing.T) {
		rendered := RenderNoManagedSchemaChangesChecksRefreshed(NoManagedSchemaChangesChecksRefreshedData{
			RequestedBy:    "alice",
			HeadSHA:        "abc123",
			GatedOnTenants: true,
		})

		assert.Contains(t, rendered, "refreshed on `abc123` and will pass once every tenant deployment's own check succeeds")
		assert.NotContains(t, rendered, "nonce")
		assert.Contains(t, rendered, "\n_Requested by @alice_\n")
	})
}

func TestRenderNoManagedSchemaChanges(t *testing.T) {
	rendered := RenderNoManagedSchemaChanges(SchemaErrorData{
		RequestedBy: "alice",
		Timestamp:   "2026-06-14 12:34:56",
		Environment: "staging",
	})

	assert.Contains(t, rendered, "## ℹ️ No Schema Files Changed")
	assert.Contains(t, rendered, "no changes to managed schema files in this PR and no apply-owned state")
}
