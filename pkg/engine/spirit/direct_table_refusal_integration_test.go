//go:build integration

package spirit

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// createForeignKeyPair creates the shape a declarative onboarding meets when
// an environment kept a foreign key the schema files later dropped: a parent
// table, a child holding a foreign key to it, and a table outside the
// relationship. Every table carries a TIMESTAMP column the schema files
// declare as DATETIME.
func createForeignKeyPair(t *testing.T, db *sql.DB) {
	t.Helper()
	dropTablesOnCleanup(t, db, "fk_owners", "fk_groups", "fk_standalone")
	for _, stmt := range []string{
		"CREATE TABLE fk_groups (id BIGINT NOT NULL AUTO_INCREMENT, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id))",
		"CREATE TABLE fk_owners (id BIGINT NOT NULL AUTO_INCREMENT, group_id BIGINT NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
			"PRIMARY KEY (id), KEY idx_group_id (group_id), " +
			"CONSTRAINT fk_owners_group_id FOREIGN KEY (group_id) REFERENCES fk_groups (id) ON DELETE CASCADE)",
		"CREATE TABLE fk_standalone (id BIGINT NOT NULL AUTO_INCREMENT, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id))",
		"INSERT INTO fk_groups (id) VALUES (1), (2)",
		"INSERT INTO fk_owners (group_id) VALUES (1), (2)",
		"INSERT INTO fk_standalone (id) VALUES (1)",
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}
	for _, table := range []string{"fk_groups", "fk_owners", "fk_standalone"} {
		_, err := db.ExecContext(t.Context(), "ANALYZE TABLE `"+table+"`")
		require.NoError(t, err, "analyze %s", table)
	}
}

// foreignKeyPairSchema is the declared schema: DATETIME columns and no foreign
// key.
func foreignKeyPairSchema() map[string]string {
	return map[string]string{
		"fk_groups.sql": "CREATE TABLE fk_groups (id BIGINT NOT NULL AUTO_INCREMENT, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id))",
		"fk_owners.sql": "CREATE TABLE fk_owners (id BIGINT NOT NULL AUTO_INCREMENT, group_id BIGINT NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
			"PRIMARY KEY (id), KEY idx_group_id (group_id))",
		"fk_standalone.sql": "CREATE TABLE fk_standalone (id BIGINT NOT NULL AUTO_INCREMENT, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id))",
	}
}

func planForeignKeyPair(t *testing.T, dsn string, metadata map[string]string) map[string]engine.TableChange {
	t.Helper()
	changes := planTables(t, dsn, metadata, foreignKeyPairSchema())
	require.Len(t, changes, 3, "the plan alters all three tables")
	return changes
}

// planTables plans files against the target and returns the plan's changes
// keyed by table.
func planTables(t *testing.T, dsn string, metadata map[string]string, files map[string]string) map[string]engine.TableChange {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database:    "testdb",
		SchemaFiles: testSchemaFiles(files),
		Credentials: &engine.Credentials{DSN: dsn, Metadata: metadata},
	})
	require.NoError(t, err, "Plan()")
	changes := make(map[string]engine.TableChange)
	for _, change := range result.FlatTableChanges() {
		changes[change.Table] = change
	}
	return changes
}

// applyAlters runs the plan's ALTER statements for tables, in that order,
// through the engine's apply path under policy, and requires the schema
// change to complete. It returns the target's database name.
func applyAlters(t *testing.T, dsn string, changes map[string]engine.TableChange, tables []string, policy directPolicy) string {
	t.Helper()
	ddlStatements := make([]string, 0, len(tables))
	for _, table := range tables {
		require.NotEmpty(t, changes[table].DDL, "the plan alters table %s", table)
		ddlStatements = append(ddlStatements, changes[table].DDL)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})
	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   tables,
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false, policy)

	eng.mu.Lock()
	finalState, errorMessage := eng.runningSchemaChange.state, eng.runningSchemaChange.errorMessage
	eng.mu.Unlock()
	require.Equal(t, engine.StateCompleted, finalState, "schema change error: %s", errorMessage)
	return database
}

// foreignKeyCount returns how many foreign keys table holds on the target.
func foreignKeyCount(t *testing.T, db *sql.DB, database, table string) int {
	t.Helper()
	var foreignKeys int
	require.NoError(t, db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.REFERENTIAL_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ?`, database, table).Scan(&foreignKeys))
	return foreignKeys
}

// rowCount returns the number of rows in table.
func rowCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var rows int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM `"+table+"`").Scan(&rows))
	return rows
}

// A plan that alters both ends of a foreign key, and drops the key, routes
// both tables to direct execution when they are within the policy's bound:
// the copy process refuses either end of a foreign key, and the plan says so
// before the operator confirms. The table outside the relationship stays on
// the engine's default path.
func TestEngine_Plan_ForeignKeyTablesRouteDirect(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createForeignKeyPair(t, db)

	changes := planForeignKeyPair(t, dsn, byteBoundPolicyMetadata(100<<20))

	assert.Contains(t, changes["fk_owners"].DDL, "DROP FOREIGN KEY `fk_owners_group_id`")
	for _, table := range []string{"fk_groups", "fk_owners"} {
		assert.Equal(t, engine.ExecutionModeDirect, changes[table].ExecutionMode, "table %s", table)
		assert.Regexp(t, `^the table has .+ of data and indexes$`, changes[table].ModeReason, "table %s", table)
	}
	assert.Empty(t, changes["fk_standalone"].ExecutionMode, "a table outside the relationship stays on the default path")
	assert.Empty(t, changes["fk_standalone"].ModeReason)
}

// A table the copy process refuses but direct execution cannot take, because
// it is above the policy's bound, is not blocked: the engine may still
// complete the statement with MySQL's native DDL, so the plan leaves it on
// the default path with no verdict, as it did before the table check existed.
func TestEngine_Plan_ForeignKeyTablesAboveBoundStayOnEngine(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createForeignKeyPair(t, db)

	changes := planForeignKeyPair(t, dsn, byteBoundPolicyMetadata(1))

	for table, change := range changes {
		assert.Empty(t, change.ExecutionMode, "table %s", table)
		assert.Empty(t, change.ModeReason, "table %s", table)
	}
}

// Without a direct execution policy the table check does not apply, and the
// plan records no verdict for any of the tables.
func TestEngine_Plan_ForeignKeyTablesPolicyOff(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createForeignKeyPair(t, db)

	changes := planForeignKeyPair(t, dsn, nil)

	for table, change := range changes {
		assert.Empty(t, change.ExecutionMode, "table %s", table)
	}
}

// An apply that alters both ends of a foreign key and drops the key completes
// under an enabled policy: both ends run as native DDL, so the copy process
// never meets the foreign key, and the table outside the relationship runs
// through the engine as before. Every row survives, the columns land as
// DATETIME, and the foreign key is gone.
func TestEngine_ExecuteAlterPhase_ForeignKeyTablesRunDirect(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createForeignKeyPair(t, db)

	changes := planForeignKeyPair(t, dsn, byteBoundPolicyMetadata(100<<20))
	database := applyAlters(t, dsn, changes, []string{"fk_groups", "fk_owners", "fk_standalone"},
		directPolicy{Enabled: true, MaxTableBytes: 100 << 20})

	for table, wantRows := range map[string]int{"fk_groups": 2, "fk_owners": 2, "fk_standalone": 1} {
		assert.Equal(t, "datetime", columnType(t, db, table, "created_at"), "table %s", table)
		assert.Equal(t, wantRows, rowCount(t, db, table), "table %s keeps its rows", table)
	}
	assert.Zero(t, foreignKeyCount(t, db, database, "fk_owners"), "the foreign key is dropped")
}

// A foreign key's parent above the policy's bound, gaining a nullable column,
// completes through the schema change engine at apply time: the copy process
// would refuse the table, but the engine completes the statement with MySQL's
// native DDL first, so leaving it on the default path, as the plan said, is
// what lets it run. The foreign key is untouched.
func TestEngine_ExecuteAlterPhase_ForeignKeyParentAboveBoundCompletesOnEngine(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createForeignKeyPair(t, db)

	files := map[string]string{
		"fk_groups.sql": "CREATE TABLE fk_groups (id BIGINT NOT NULL AUTO_INCREMENT, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
			"note VARCHAR(20) DEFAULT NULL, PRIMARY KEY (id))",
		"fk_owners.sql": "CREATE TABLE fk_owners (id BIGINT NOT NULL AUTO_INCREMENT, group_id BIGINT NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
			"PRIMARY KEY (id), KEY idx_group_id (group_id), " +
			"CONSTRAINT fk_owners_group_id FOREIGN KEY (group_id) REFERENCES fk_groups (id) ON DELETE CASCADE)",
		"fk_standalone.sql": "CREATE TABLE fk_standalone (id BIGINT NOT NULL AUTO_INCREMENT, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id))",
	}
	changes := planTables(t, dsn, byteBoundPolicyMetadata(1), files)
	require.Len(t, changes, 1, "the plan alters only the parent")
	assert.Contains(t, changes["fk_groups"].DDL, "ADD COLUMN `note`")
	assert.Empty(t, changes["fk_groups"].ExecutionMode, "above the bound, the plan leaves the parent on the default path")

	database := applyAlters(t, dsn, changes, []string{"fk_groups"}, directPolicy{Enabled: true, MaxTableBytes: 1})

	assert.Equal(t, "varchar(20)", columnType(t, db, "fk_groups", "note"))
	assert.Equal(t, 2, rowCount(t, db, "fk_groups"))
	assert.Equal(t, 1, foreignKeyCount(t, db, database, "fk_owners"), "the foreign key is untouched")
}

// A small child dropping its foreign key runs directly, and the parent above
// the policy's bound is copied by the schema change engine afterwards. Direct
// statements run first, so by the time the engine checks the parent its foreign
// key is gone and the copy proceeds. The plan says the same: a direct verdict
// on the child, none on the parent.
func TestEngine_ExecuteAlterPhase_ForeignKeyChildDirectBeforeParentCopies(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createForeignKeyPair(t, db)
	_, err := db.ExecContext(t.Context(), "INSERT INTO fk_groups (id) VALUES (3), (4), (5), (6), (7), (8), (9), (10), (11), (12)")
	require.NoError(t, err, "seed the parent above the row bound")

	const maxRows = 5
	changes := planForeignKeyPair(t, dsn, directPolicyMetadata(maxRows))
	assert.Equal(t, engine.ExecutionModeDirect, changes["fk_owners"].ExecutionMode, "the child is within the bound")
	assert.Empty(t, changes["fk_groups"].ExecutionMode, "the parent is above the bound and stays on the default path")
	assert.Empty(t, changes["fk_standalone"].ExecutionMode)

	database := applyAlters(t, dsn, changes, []string{"fk_groups", "fk_owners", "fk_standalone"},
		directPolicy{Enabled: true, MaxTableRows: maxRows})

	for table, wantRows := range map[string]int{"fk_groups": 12, "fk_owners": 2, "fk_standalone": 1} {
		assert.Equal(t, "datetime", columnType(t, db, table, "created_at"), "table %s", table)
		assert.Equal(t, wantRows, rowCount(t, db, table), "table %s keeps its rows", table)
	}
	assert.Zero(t, foreignKeyCount(t, db, database, "fk_owners"), "the foreign key is dropped")
}

// createTriggerTable creates a table whose trigger fills a column on insert,
// with a TIMESTAMP column the schema files declare as DATETIME.
func createTriggerTable(t *testing.T, db *sql.DB) {
	t.Helper()
	dropTablesOnCleanup(t, db, "trg_audited")
	for _, stmt := range []string{
		"CREATE TABLE trg_audited (id BIGINT NOT NULL AUTO_INCREMENT, note VARCHAR(20) NOT NULL DEFAULT '', " +
			"created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id))",
		"CREATE TRIGGER trg_audited_bi BEFORE INSERT ON trg_audited FOR EACH ROW SET NEW.note = 'fired'",
		"INSERT INTO trg_audited (id) VALUES (1), (2)",
		"ANALYZE TABLE trg_audited",
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}
}

// A table with a trigger, within the policy's bound, routes to direct
// execution: the plan records a direct verdict, and the apply runs the
// statement as native DDL, which keeps the trigger. Rows survive, the column
// lands as DATETIME, and a row inserted afterwards still fires the trigger.
func TestEngine_TriggerTableRunsDirect(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	createTriggerTable(t, db)

	files := map[string]string{
		"trg_audited.sql": "CREATE TABLE trg_audited (id BIGINT NOT NULL AUTO_INCREMENT, note VARCHAR(20) NOT NULL DEFAULT '', " +
			"created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id))",
	}
	changes := planTables(t, dsn, byteBoundPolicyMetadata(100<<20), files)
	require.Len(t, changes, 1)
	assert.Equal(t, engine.ExecutionModeDirect, changes["trg_audited"].ExecutionMode)
	assert.Regexp(t, `^the table has .+ of data and indexes$`, changes["trg_audited"].ModeReason)

	database := applyAlters(t, dsn, changes, []string{"trg_audited"}, directPolicy{Enabled: true, MaxTableBytes: 100 << 20})

	assert.Equal(t, "datetime", columnType(t, db, "trg_audited", "created_at"))
	assert.Equal(t, 2, rowCount(t, db, "trg_audited"))

	var triggers int
	require.NoError(t, db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.TRIGGERS
		WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = 'trg_audited'`, database).Scan(&triggers))
	assert.Equal(t, 1, triggers, "the trigger survives the statement")

	_, err := db.ExecContext(t.Context(), "INSERT INTO trg_audited (id) VALUES (3)")
	require.NoError(t, err)
	var note string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT note FROM trg_audited WHERE id = 3").Scan(&note))
	assert.Equal(t, "fired", note, "the trigger still fires")
}
