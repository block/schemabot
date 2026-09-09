package templates

import (
	"fmt"
	"log/slog"
	"strings"

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
		fmt.Println(IndentSQL(rollbackDisplayDDL(dialect, table.DDL), "    "))
	}
	for _, change := range plan.Changes {
		if change.HasVSchemaChange() {
			fmt.Printf("  %s: VSchema update\n", change.Namespace)
		}
	}
}

// A display transformation must not alter quoted names or values. Canonical
// comparison also catches layout changes inside literals and expressions.
func rollbackDisplayDDL(dialect schema.Dialect, raw string) string {
	raw = strings.TrimRight(strings.TrimSpace(raw), ";") + ";"
	formatted := ddl.FormatDDLForDialect(dialect, raw)
	parser, err := ddl.ParserForDialect(dialect)
	if err != nil {
		// FormatDDLForDialect logs unsupported dialects and preserves their SQL.
		return formatted
	}
	if parser.Canonicalize(raw) != parser.Canonicalize(formatted) {
		slog.Debug("Rollback SQL display normalization changed the statement; preserving original SQL", "dialect", dialect)
		return raw
	}
	return formatted
}
