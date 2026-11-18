package templates

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// WriteRollbackPlan presents the source apply and the newly planned SQL before
// confirmation. SQL is rendered under the target database's grammar. The
// statements listed are the plan's rendered set, so a change only one shard of
// a divergent keyspace needs is shown rather than hidden behind the one entry
// per table the namespace-level changes keep; the operator is asked to confirm
// what will run, not a summary of it.
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
	for _, table := range plan.RenderedTables() {
		fmt.Printf("  %s (%s):\n", table.TableName, table.ChangeType)
		fmt.Println(IndentSQL(ddl.FormatDDLForDialect(dialect, table.DDL), "    "))
	}
	for _, change := range plan.Changes {
		switch {
		case change.ShowsVSchemaChange():
			fmt.Printf("  %s: VSchema update\n", change.Namespace)
		case plan.FinalizesOnly(change):
			fmt.Printf("  %s: finalized by the engine once every shard's DDL has landed\n", change.Namespace)
		}
	}
}
