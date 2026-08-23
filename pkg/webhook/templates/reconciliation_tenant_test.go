package templates

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The reconciliation comment's pasteable stop/plan/rollback hints address a
// specific deployment, so on a tenant deployment they carry its tenant flag.
func TestReconciliationHintsCarryTenant(t *testing.T) {
	out := RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{
		Tenant: "acme",
		Items:  []SchemaChangeReconciliationItem{{Database: "orders", Environment: "staging", ApplyID: "apply_abc123", State: "completed"}},
	})
	require.Contains(t, out, "schemabot rollback apply_abc123 -e staging --tenant acme")
	require.Contains(t, out, "schemabot plan -e staging -d orders --tenant acme")

	untenanted := RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{
		Items: []SchemaChangeReconciliationItem{{Database: "orders", Environment: "staging", ApplyID: "apply_abc123", State: "completed"}},
	})
	require.Contains(t, untenanted, "schemabot rollback apply_abc123 -e staging")
	require.Contains(t, untenanted, "schemabot plan -e staging -d orders")
	require.NotContains(t, untenanted, "--tenant")
}
