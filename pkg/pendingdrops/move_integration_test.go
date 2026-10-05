//go:build integration

package pendingdrops

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSourceTable creates schema.table holding one row with the given id, so
// a test can tell which quarantined copy came from which source.
func createSourceTable(t *testing.T, db *sql.DB, schema, table string, id int) {
	t.Helper()
	ctx := t.Context()
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", schema))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", schema, table))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE `%s`.`%s` (id INT PRIMARY KEY)", schema, table))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (%d)", schema, table, id))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", schema, table))
	})
}

// quarantinedRow reads the single id in a quarantined copy.
func quarantinedRow(t *testing.T, db *sql.DB, moved QuarantinedTable) int {
	t.Helper()
	var id int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT id FROM `%s`.`%s`", moved.QuarantineSchema, moved.QuarantineTable)).Scan(&id),
		"read quarantined copy of %s.%s", moved.SchemaName, moved.TableName)
	return id
}

// The RENAME is where the bug lived: before destinations were made unique,
// MySQL refused the statement outright and every retry built the same one.
// These cases run the real statement for the shapes that used to fail and
// check that each source's rows land in its own quarantined copy.
func TestMoveTablesQuarantinesEveryTableUnderItsOwnName(t *testing.T) {
	t.Run("Spirit's long-named shadow and cutover pair", func(t *testing.T) {
		// A cancelled schema change on a 44-character table preserves both
		// `_<t>_new` and `_<t>_old`; both are 49 characters, so both used to be
		// cut to the same 46 and collide in one RENAME.
		db := setupCleanerTest(t)
		long := strings.Repeat("t", 44)
		names := []string{"_" + long + "_new", "_" + long + "_old"}
		moves := make([]TableMove, 0, len(names))
		for i, name := range names {
			createSourceTable(t, db, "testdb", name, 7+i)
			moves = append(moves, TableMove{SchemaName: "testdb", TableName: name})
		}

		moved, err := MoveTables(t.Context(), db, moves, time.Now())
		require.NoError(t, err)

		require.Len(t, moved, 2)
		assert.Equal(t, 7, quarantinedRow(t, db, moved[0]))
		assert.Equal(t, 8, quarantinedRow(t, db, moved[1]))
		assert.Len(t, quarantinedTables(t, db), 2)
	})

	t.Run("same table name dropped from two schemas", func(t *testing.T) {
		db := setupCleanerTest(t)
		createSourceTable(t, db, "testdb", "users", 1)
		createSourceTable(t, db, "testdb_2", "users", 2)
		t.Cleanup(func() {
			_, _ = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `testdb_2`")
		})

		moved, err := MoveTables(t.Context(), db, []TableMove{
			{SchemaName: "testdb", TableName: "users"},
			{SchemaName: "testdb_2", TableName: "users"},
		}, time.Now())
		require.NoError(t, err)

		require.Len(t, moved, 2)
		assert.Equal(t, 1, quarantinedRow(t, db, moved[0]))
		assert.Equal(t, 2, quarantinedRow(t, db, moved[1]))
		assert.Len(t, quarantinedTables(t, db), 2)
	})

	t.Run("multibyte name at the character limit keeps its full name", func(t *testing.T) {
		// 40 two-byte characters fit MySQL's 64-character limit after the
		// 18-character prefix, so the name must move unshortened rather than
		// be cut by bytes.
		db := setupCleanerTest(t)
		name := strings.Repeat("é", 40)
		createSourceTable(t, db, "testdb", name, 3)

		moved, err := MoveTables(t.Context(), db, []TableMove{{SchemaName: "testdb", TableName: name}}, time.Now())
		require.NoError(t, err)

		require.Len(t, moved, 1)
		assert.True(t, strings.HasSuffix(moved[0].QuarantineTable, name), "quarantined as %q", moved[0].QuarantineTable)
		assert.Equal(t, 3, quarantinedRow(t, db, moved[0]))
	})
}
