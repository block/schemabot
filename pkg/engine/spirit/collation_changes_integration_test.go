//go:build integration

package spirit

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// A schema file that moves a column onto another charset without naming a
// collation leaves the collation to the server, and the plan reads that
// default from the target, so the reviewer sees the collation the column
// really ends up under instead of an unknown.
func TestEngine_Plan_CollationChangeReadsTheTargetsCharsetDefault(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	dropTablesOnCleanup(t, db, "products")

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `products` ("+
		"`id` bigint unsigned NOT NULL AUTO_INCREMENT, "+
		"`title` varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL, "+
		"PRIMARY KEY (`id`)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "create products")

	var serverDefault string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT DEFAULT_COLLATE_NAME FROM information_schema.CHARACTER_SETS WHERE CHARACTER_SET_NAME = 'utf8mb4'").Scan(&serverDefault))
	require.Equal(t, "utf8mb4_0900_ai_ci", serverDefault, "the test container's default collation for utf8mb4")

	eng := New(Config{Logger: slog.New(slog.DiscardHandler)})
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"products.sql": "CREATE TABLE `products` (" +
				"`id` bigint unsigned NOT NULL AUTO_INCREMENT, " +
				"`title` varchar(255) CHARACTER SET utf8mb4 DEFAULT NULL, " +
				"PRIMARY KEY (`id`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		}),
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Plan()")

	changes := result.FlatTableChanges()
	require.Len(t, changes, 1, "DDL: %v", result.FlatDDL())
	assert.Equal(t, []engine.CollationChange{{
		Column: "title", From: "latin1_swedish_ci", To: "utf8mb4_0900_ai_ci",
		Case:           engine.ComparisonUnchanged,
		TrailingSpaces: engine.ComparisonBecomesSensitive,
		CanMergeValues: true,
	}}, changes[0].CollationChanges, "DDL: %s", changes[0].DDL)
}
