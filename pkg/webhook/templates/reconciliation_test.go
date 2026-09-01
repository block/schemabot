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
	assert.Contains(t, rendered, "SchemaBot refreshes this PR's checks automatically")
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
	assert.Contains(t, rendered, "SchemaBot refreshes this PR's checks automatically")
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

// A system-triggered reconciliation comment (posted by stale-check cleanup,
// not a user command) attributes itself to the trigger instead of a requester.
func TestRenderSchemaChangeReconciliationRequiredTriggerAttribution(t *testing.T) {
	rendered := RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{
		Trigger:   "Triggered automatically by a pull request update",
		Timestamp: "2026-06-14 12:34:56",
		Items: []SchemaChangeReconciliationItem{
			{Database: "orders", Environment: "staging", ApplyID: "apply-1234", State: "running", InProgress: true},
		},
	})

	assert.Contains(t, rendered, "*Triggered automatically by a pull request update at 2026-06-14 12:34:56 UTC*")
	assert.NotContains(t, rendered, "Requested")
	assert.Contains(t, rendered, "SchemaBot is still applying a schema change from this PR")
}

func TestRenderNoManagedSchemaChangesChecksRefreshedTriggerAttribution(t *testing.T) {
	rendered := RenderNoManagedSchemaChangesChecksRefreshed(NoManagedSchemaChangesChecksRefreshedData{
		Trigger:   "Triggered automatically after the rollback completed",
		Timestamp: "2026-06-14 12:34:56",
		HeadSHA:   "abc123",
	})

	assert.Contains(t, rendered, "*Triggered automatically after the rollback completed at 2026-06-14 12:34:56 UTC*")
	assert.NotContains(t, rendered, "Requested")
	assert.Contains(t, rendered, "refreshed as passing on `abc123`")
}
