package templates

import (
	"strings"
	"testing"
)

func TestReconciliationHintsCarryTenant(t *testing.T) {
	out := RenderSchemaChangeReconciliationRequired(SchemaChangeReconciliationData{
		Tenant: "acme",
		Items:  []SchemaChangeReconciliationItem{{Database: "orders", Environment: "staging", ApplyID: "apply_abc123", State: "completed"}},
	})
	if !strings.Contains(out, "schemabot rollback apply_abc123 -e staging --tenant acme") {
		t.Fatalf("rollback hint missing tenant:\n%s", out)
	}
}
