package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/charmbracelet/x/ansi"
	"github.com/stretchr/testify/assert"
)

// Preview and progress retain the same SQL while leaving the supplied plan intact.
func TestRollbackAndProgressPreservePlanSQL(t *testing.T) {
	for _, databaseType := range []string{"mysql", "postgres", "custom"} {
		t.Run(databaseType, func(t *testing.T) {
			raw := `ALTER TABLE "INT" ADD COLUMN "TEXT" text DEFAULT 'KEEP INT, DEFAULT NULL';`
			if databaseType == "mysql" {
				raw = "ALTER TABLE `INT` ADD COLUMN `TEXT` varchar(64) DEFAULT 'KEEP INT, DEFAULT NULL';"
			}
			table := &apitypes.TableChangeResponse{TableName: "INT", ChangeType: "alter", DDL: raw}
			plan := &apitypes.PlanResponse{Database: "shop", DatabaseType: databaseType, Environment: "staging", Changes: []*apitypes.SchemaChangeResponse{{Namespace: "shop", TableChanges: []*apitypes.TableChangeResponse{table}}}}
			preview := captureStdout(t, func() { WriteRollbackPlan(plan, "apply-example-85") })
			assert.Equal(t, raw, table.DDL)
			assert.Contains(t, ansi.Strip(preview), raw)
			progress := FormatTableProgress(TableProgress{TableName: "INT", ChangeType: "alter", DDL: raw, Dialect: schema.DialectForDatabaseType(databaseType), Status: state.Apply.Running})
			assert.Contains(t, ansi.Strip(progress), raw)
			assert.Equal(t, raw, table.DDL)
		})
	}
}
