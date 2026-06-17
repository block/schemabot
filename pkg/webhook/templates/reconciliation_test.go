package templates

import (
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

func TestRenderNoManagedSchemaChanges(t *testing.T) {
	rendered := RenderNoManagedSchemaChanges(SchemaErrorData{
		RequestedBy: "alice",
		Timestamp:   "2026-06-14 12:34:56",
		Environment: "staging",
	})

	assert.Contains(t, rendered, "## ✅ No Managed Schema Changes")
	assert.Contains(t, rendered, "SchemaBot did not find any apply-owned state")
}
