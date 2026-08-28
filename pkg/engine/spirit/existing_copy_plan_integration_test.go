//go:build integration

package spirit

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// planTarget is a target holding the named tables and the desired schema that
// widens each of them, which is the shape every disclosure case here needs: a
// plan that produces exactly one ALTER per table, all of them run by the engine.
type planTarget struct {
	eng     *Engine
	dsn     string
	db      *sql.DB
	tables  []string
	files   map[string]string
	grouped bool
}

func newPlanTarget(t *testing.T, tables ...string) planTarget {
	t.Helper()

	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	files := make(map[string]string, len(tables))
	for _, tableName := range tables {
		_, err := db.ExecContext(t.Context(), "CREATE TABLE `"+tableName+"` ("+
			"id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, "+
			"name VARCHAR(50) NOT NULL"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
		require.NoError(t, err, "create table %s", tableName)

		files[tableName+".sql"] = "CREATE TABLE `" + tableName + "` (" +
			"id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, " +
			"name VARCHAR(100) NOT NULL" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	}

	return planTarget{
		eng:    newPlanEngine(Settings{}),
		dsn:    dsn,
		db:     db,
		tables: tables,
		files:  files,
	}
}

func newPlanEngine(settings Settings) *Engine {
	return New(Config{
		Logger:   slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Settings: settings,
	})
}

// groupedApply plans for an apply that hands the engine every ALTER at once
// rather than driving one table at a time. It changes which checkpoint the
// prediction reads and how much of the plan has to be copied for the copy to
// survive, so a multi-table case has to say which shape it is describing.
func (p planTarget) groupedApply() planTarget {
	p.grouped = true
	return p
}

// expireCheckpoints replans against an engine whose checkpoint bound sits below
// the age of any checkpoint a test can write, so a copy recorded moments ago is
// already too old to replay from rather than the test waiting out a real expiry.
func (p planTarget) expireCheckpoints() planTarget {
	p.eng = newPlanEngine(Settings{CheckpointMaxAge: -time.Second})
	return p
}

// plan runs the plan the operator would run, and returns it along with the
// batch it produced — every ALTER joined the way an apply hands them to the
// engine, and so the statement a copy on the target has to have been made for
// to be continued.
func (p planTarget) plan(t *testing.T) (*engine.PlanResult, string) {
	t.Helper()

	result, err := p.eng.Plan(t.Context(), &engine.PlanRequest{
		Database:         "testdb",
		SchemaFiles:      testSchemaFiles(p.files),
		Credentials:      &engine.Credentials{DSN: p.dsn},
		GroupedExecution: p.grouped,
	})
	require.NoError(t, err, "Plan()")
	require.False(t, result.NoChanges, "the desired schema widens a column on every table, so the plan has changes")

	ddl := result.FlatDDL()
	require.Len(t, ddl, len(p.tables), "the plan produces one ALTER per table")
	return result, strings.Join(ddl, "; ")
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
	_, batch := target.plan(t)

	seedCopy(t, target.db, target.tables, utils.CheckpointTableName(target.tables[0]), batch)

	result, _ := target.plan(t)
	require.Len(t, result.ExistingCopies, 1, "the plan meets a copy of its own table")
	disclosed := result.ExistingCopies[0]
	assert.Equal(t, "testdb", disclosed.Namespace, "the disclosure names where the copy sits")
	assert.Equal(t, engine.CopyAdopt, disclosed.Disposition)
	assert.Empty(t, disclosed.Reason)
	assert.Equal(t, target.tables, disclosed.Tables)
	assert.Positive(t, disclosed.Age, "the disclosure carries how long the copy has been sitting there")
}

// A plan whose ALTER differs from the one a copy on the target was made for
// discloses that applying destroys that copy and re-copies the table from zero.
// This is the case that costs an operator days of copying, so the plan names it
// before anyone confirms.
func TestPlanDisclosesADiscardedCopy(t *testing.T) {
	target := newPlanTarget(t, "plan_discarded_copy")
	started := "ALTER TABLE `" + target.tables[0] + "` ADD INDEX idx_name (name)"

	seedCopy(t, target.db, target.tables, utils.CheckpointTableName(target.tables[0]), started)

	result, _ := target.plan(t)
	require.Len(t, result.ExistingCopies, 1, "the plan meets a copy of its own table")
	disclosed := result.ExistingCopies[0]
	assert.Equal(t, "testdb", disclosed.Namespace, "the disclosure names where the copy sits")
	assert.Equal(t, engine.CopyDiscard, disclosed.Disposition)
	assert.Equal(t, engine.DiscardStatementDiffers, disclosed.Reason)
	assert.Equal(t, target.tables, disclosed.Tables)
	assert.Equal(t, started, disclosed.Statement,
		"a discard for a differing statement carries the one the copy was started for, "+
			"so the disclosure can name what the plan differs from")
}

// A copy whose statement still matches this plan but whose checkpoint is too
// old to replay from is destroyed all the same. The plan discloses the expiry
// as the cause, because it is the one discard re-running the same schema change
// cannot avoid.
func TestPlanDisclosesADiscardedCopyWithAnExpiredCheckpoint(t *testing.T) {
	target := newPlanTarget(t, "plan_expired_checkpoint").expireCheckpoints()
	_, batch := target.plan(t)

	seedCopy(t, target.db, target.tables, utils.CheckpointTableName(target.tables[0]), batch)

	result, _ := target.plan(t)
	require.Len(t, result.ExistingCopies, 1, "the plan meets a copy of its own table")
	disclosed := result.ExistingCopies[0]
	assert.Equal(t, engine.CopyDiscard, disclosed.Disposition)
	assert.Equal(t, engine.DiscardCheckpointExpired, disclosed.Reason,
		"the statement matches; only the age disqualifies the copy")
	assert.Equal(t, target.tables, disclosed.Tables)
}

// A grouped apply that alters two tables where only one was ever copied
// destroys the copy that exists: the engine resumes all of a batch or none of
// it. Nothing in the schema change hints at this, so the plan discloses the
// whole batch's copy as lost and names the partial copy as the cause.
func TestPlanDisclosesADiscardedCopyCoveringPartOfTheBatch(t *testing.T) {
	target := newPlanTarget(t, "plan_partial_copy_one", "plan_partial_copy_two").groupedApply()
	_, batch := target.plan(t)

	// The checkpoint is for the whole batch and matches it byte for byte; only
	// the second table's shadow table never got built.
	seedCopy(t, target.db, target.tables[:1], sharedCheckpointTable, batch)

	result, _ := target.plan(t)
	require.Len(t, result.ExistingCopies, 1, "one target, so one disclosure covering the batch")
	disclosed := result.ExistingCopies[0]
	assert.Equal(t, engine.CopyDiscard, disclosed.Disposition)
	assert.Equal(t, engine.DiscardCopyIncomplete, disclosed.Reason)
	assert.Equal(t, target.tables[:1], disclosed.Tables,
		"only the copied table is disclosed as work lost; the other never started")
}

// A target that cannot be read leaves the plan exactly as it would be without
// the disclosure. A plan describes a target and decides nothing, so failing to
// read one must never fail the plan an operator is waiting on.
func TestPlanDisclosesNothingWhenTheTargetCannotBeRead(t *testing.T) {
	eng := newPlanEngine(Settings{})

	ctx, cancel := context.WithTimeout(t.Context(), copyDetectionTimeout)
	defer cancel()
	target := &lazyTargetDB{dsn: "root:nopass@tcp(127.0.0.1:1)/absent"}
	defer target.close()

	disclosed := eng.plannedExistingCopies(ctx, target, "absent", []engine.TableChange{
		engineRun("xfers", "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"),
	}, false)
	assert.Nil(t, disclosed, "an unreadable target is logged, not disclosed and not returned as an error")
}

// An apply that drives one table at a time meets each table's copy on its own
// terms, so a plan covering several tables where only one is partway through
// discloses that one copy as continued. The tables that have not started have
// no copy to lose, and the copy that exists is resumed from its own checkpoint:
// telling the operator that confirming destroys it would name a cost the apply
// does not pay.
func TestPlanDisclosesAContinuedCopyWhenTablesAreDrivenOneAtATime(t *testing.T) {
	target := newPlanTarget(t, "plan_sequential_copy_one", "plan_sequential_copy_two")

	result, _ := target.plan(t)
	alters := result.FlatDDL()
	require.Len(t, alters, 2, "the plan produces one ALTER per table")

	// The first table is copying under its own single-statement batch, which is
	// what an apply driving a table at a time hands the engine. The second has
	// not started.
	seedCopy(t, target.db, target.tables[:1], utils.CheckpointTableName(target.tables[0]), alters[0])

	result, _ = target.plan(t)
	require.Len(t, result.ExistingCopies, 1, "only the table already copying has anything at stake")
	disclosed := result.ExistingCopies[0]
	assert.Equal(t, "testdb", disclosed.Namespace)
	assert.Equal(t, engine.CopyAdopt, disclosed.Disposition,
		"the apply hands the engine this table's own ALTER, which is what its checkpoint records")
	assert.Empty(t, disclosed.Reason)
	assert.Equal(t, target.tables[:1], disclosed.Tables)
	assert.Equal(t, alters[0], disclosed.Statement)
}
