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
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database:    "testdb",
		SchemaFiles: testSchemaFiles(foreignKeyPairSchema()),
		Credentials: &engine.Credentials{DSN: dsn, Metadata: metadata},
	})
	require.NoError(t, err, "Plan()")
	changes := make(map[string]engine.TableChange)
	for _, change := range result.FlatTableChanges() {
		changes[change.Table] = change
	}
	require.Len(t, changes, 3, "the plan alters all three tables")
	return changes
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
	ddlStatements := []string{changes["fk_groups"].DDL, changes["fk_owners"].DDL, changes["fk_standalone"].DDL}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})
	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"fk_groups", "fk_owners", "fk_standalone"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false,
		directPolicy{Enabled: true, MaxTableBytes: 100 << 20})

	eng.mu.Lock()
	finalState, errorMessage := eng.runningSchemaChange.state, eng.runningSchemaChange.errorMessage
	eng.mu.Unlock()
	require.Equal(t, engine.StateCompleted, finalState, "schema change error: %s", errorMessage)

	for table, wantRows := range map[string]int{"fk_groups": 2, "fk_owners": 2, "fk_standalone": 1} {
		var dataType string
		require.NoError(t, db.QueryRowContext(t.Context(), `
			SELECT DATA_TYPE FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'created_at'`,
			database, table).Scan(&dataType))
		assert.Equal(t, "datetime", dataType, "table %s", table)

		var rows int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM `"+table+"`").Scan(&rows))
		assert.Equal(t, wantRows, rows, "table %s keeps its rows", table)
	}

	var foreignKeys int
	require.NoError(t, db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.REFERENTIAL_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = 'fk_owners'`, database).Scan(&foreignKeys))
	assert.Zero(t, foreignKeys, "the foreign key is dropped")
}
