package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

const notValidReason = "holds a foreign key added NOT VALID, which a schema file cannot declare"

// rollbackBlockersSurfaces renders the same plan, with one changed namespace
// whose current schema could not be saved, through the CLI and the PR plan
// comment.
func rollbackBlockersSurfaces(t *testing.T, blockers []apitypes.SkippedTable) (cli, comment string) {
	t.Helper()
	cli = captureStdout(t, func() {
		WriteRollbackBlockers([]*apitypes.SchemaChangeResponse{{Namespace: "public", RollbackBlockers: blockers}})
	})
	tables := make([]webhooktemplates.RollbackBlockerData, 0, len(blockers))
	for _, b := range blockers {
		tables = append(tables, webhooktemplates.RollbackBlockerData{Table: b.Table, Reason: b.Reason})
	}
	comment = webhooktemplates.RenderPlanComment(webhooktemplates.PlanCommentData{
		Database: "testapp", Environment: "staging", DatabaseType: "postgres",
		Changes:          []webhooktemplates.KeyspaceChangeData{{Keyspace: "public", Statements: []string{"ALTER TABLE users ADD COLUMN email text"}}},
		RollbackBlockers: []webhooktemplates.RollbackBlockersData{{Namespace: "public", Tables: tables}},
	})
	return cli, comment
}

// The CLI and the PR plan comment say the same sentence about a plan whose
// apply cannot be rolled back, with the same glyph, and name the same table
// with the same reason (UX-5). Only the quoting differs.
func TestRollbackBlockersSurfacesSaySameSentence(t *testing.T) {
	cli, comment := rollbackBlockersSurfaces(t, []apitypes.SkippedTable{{Table: "orders", Reason: notValidReason}})

	sentence := "No rollback for namespace public: rollback needs a schema file for every table in the namespace, and SchemaBot can't write one yet for the tables below. The apply still runs. To undo it, revert the schema files in a new pull request."
	assert.Contains(t, cli, "⚠️  "+sentence+"\n     - orders: "+notValidReason+"\n")
	assert.Contains(t, strings.ReplaceAll(comment, "`", ""), "⚠️ "+sentence+"\n\n- orders: "+notValidReason+"\n")
}

// A namespace with more blockers than the limit names the same first tables
// and counts the rest the same way on both surfaces.
func TestManyRollbackBlockersSurfacesCountTheSameRest(t *testing.T) {
	var blockers []apitypes.SkippedTable
	for i := range presentation.RollbackBlockersListLimit + 2 {
		blockers = append(blockers, apitypes.SkippedTable{Table: fmt.Sprintf("events_%02d", i), Reason: notValidReason})
	}

	cli, comment := rollbackBlockersSurfaces(t, blockers)

	assert.Contains(t, cli, "     - 2 more tables\n")
	assert.Contains(t, comment, "- 2 more tables\n")
	last := blockers[presentation.RollbackBlockersListLimit-1].Table
	assert.Contains(t, cli, last)
	assert.Contains(t, comment, last)
	assert.NotContains(t, cli, blockers[presentation.RollbackBlockersListLimit].Table)
	assert.NotContains(t, comment, blockers[presentation.RollbackBlockersListLimit].Table)
}

// Nothing is written when every changed namespace's current schema was saved.
func TestWriteRollbackBlockersWritesNothingWhenEveryNamespaceWasSaved(t *testing.T) {
	assert.Empty(t, captureStdout(t, func() {
		WriteRollbackBlockers([]*apitypes.SchemaChangeResponse{{Namespace: "public"}})
	}))
}
