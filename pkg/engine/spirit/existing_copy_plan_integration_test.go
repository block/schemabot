//go:build integration

package spirit

import (
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// planTarget is a target holding one table and the desired schema that widens
// it, which is the shape every disclosure case here needs: a plan that produces
// exactly one ALTER the engine runs itself.
type planTarget struct {
	eng   *Engine
	dsn   string
	table string
	files map[string]string
}

func newPlanTarget(t *testing.T, tableName string) planTarget {
	t.Helper()

	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `"+tableName+"` ("+
		"id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, "+
		"name VARCHAR(50) NOT NULL"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "create table %s", tableName)

	return planTarget{
		eng:   New(Config{Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))}),
		dsn:   dsn,
		table: tableName,
		files: map[string]string{
			tableName + ".sql": "CREATE TABLE `" + tableName + "` (" +
				"id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, " +
				"name VARCHAR(100) NOT NULL" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
	}
}

// plan runs the plan the operator would run, and returns it along with the
// single ALTER it produced — the batch an apply hands the engine, and so the
// batch a copy on the target has to have been made for to be continued.
func (p planTarget) plan(t *testing.T) (*engine.PlanResult, string) {
	t.Helper()

	result, err := p.eng.Plan(t.Context(), &engine.PlanRequest{
		Database:    "testdb",
		SchemaFiles: testSchemaFiles(p.files),
		Credentials: &engine.Credentials{DSN: p.dsn},
	})
	require.NoError(t, err, "Plan()")
	require.False(t, result.NoChanges, "the desired schema widens a column, so the plan has a change")

	ddl := result.FlatDDL()
	require.Len(t, ddl, 1, "the plan produces one ALTER")
	return result, ddl[0]
}

// A plan against a clean target discloses nothing: there is no copy to continue
// or destroy, which is the ordinary case and must stay quiet.
func TestPlanDisclosesNoCopyOnACleanTarget(t *testing.T) {
	target := newPlanTarget(t, "plan_clean_target")

	result, _ := target.plan(t)
	assert.Empty(t, result.ExistingCopies, "a clean target has no copy at stake")
}

// A plan whose ALTER matches a copy already on the target discloses that
// applying continues that copy. Nothing is destroyed, so the disclosure informs
// and the plan is otherwise unchanged.
func TestPlanDisclosesAContinuedCopy(t *testing.T) {
	target := newPlanTarget(t, "plan_continued_copy")
	_, alter := target.plan(t)

	_, db := setupTestMySQL(t)
	seedCopy(t, db, []string{target.table}, utils.CheckpointTableName(target.table), alter)

	result, _ := target.plan(t)
	require.Len(t, result.ExistingCopies, 1, "the plan meets a copy of its own table")
	disclosed := result.ExistingCopies[0]
	assert.Equal(t, "testdb", disclosed.Namespace, "the disclosure names where the copy sits")
	assert.Equal(t, engine.CopyAdopt, disclosed.Disposition)
	assert.Empty(t, disclosed.Reason)
	assert.Equal(t, []string{target.table}, disclosed.Tables)
	assert.Positive(t, disclosed.Age, "the disclosure carries how long the copy has been sitting there")
}

// A plan whose ALTER differs from the one a copy on the target was made for
// discloses that applying destroys that copy and re-copies the table from zero.
// This is the case that costs an operator days of copying, so the plan names it
// before anyone confirms.
func TestPlanDisclosesADiscardedCopy(t *testing.T) {
	target := newPlanTarget(t, "plan_discarded_copy")

	_, db := setupTestMySQL(t)
	seedCopy(t, db, []string{target.table}, utils.CheckpointTableName(target.table),
		"ALTER TABLE `"+target.table+"` ADD INDEX idx_name (name)")

	result, _ := target.plan(t)
	require.Len(t, result.ExistingCopies, 1, "the plan meets a copy of its own table")
	disclosed := result.ExistingCopies[0]
	assert.Equal(t, "testdb", disclosed.Namespace, "the disclosure names where the copy sits")
	assert.Equal(t, engine.CopyDiscard, disclosed.Disposition)
	assert.Equal(t, engine.DiscardStatementDiffers, disclosed.Reason)
	assert.Equal(t, []string{target.table}, disclosed.Tables)
}
