package templates

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// WriteRollbackPlan presents the source apply and the newly planned SQL before
// confirmation. SQL is rendered under the target database's grammar.
func WriteRollbackPlan(plan *apitypes.PlanResponse, sourceApplyID string) {
	fmt.Println("\nRollback Plan")
	WriteBox([]BoxRow{
		{Label: "Database", Value: plan.Database},
		{Label: "Environment", Value: plan.Environment},
		{Label: "Source apply", Value: sourceApplyID},
	}, "", nil)
	fmt.Println("\nThe following changes will be applied to rollback:")
	fmt.Println()
	dialect := schema.DialectForDatabaseType(plan.DatabaseType)
	for _, table := range plan.FlatTables() {
		fmt.Printf("  %s (%s):\n", table.TableName, table.ChangeType)
		fmt.Println(IndentSQL(ddl.FormatDDLForDialect(dialect, table.DDL), "    "))
	}
	for _, change := range plan.Changes {
		if change.HasVSchemaChange() {
			fmt.Printf("  %s: VSchema update\n", change.Namespace)
		}
	}
}
