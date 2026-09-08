//go:build e2e || integration

package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// OpenMySQL opens and verifies a MySQL handle for direct test-side access to a
// plane's storage or target database. The handle is closed when the test ends.
func OpenMySQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "open mysql")
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()), "ping mysql")
	return db
}

// CreateTestTable creates a table on the given DSN and returns a cleanup function
// that drops it. The cleanup function opens a new connection so it works even
// after the test context is cancelled.
func CreateTestTable(t *testing.T, dsn, tableName, ddl string) func() {
	t.Helper()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "open mysql for create table")
	require.NoError(t, db.PingContext(t.Context()), "ping mysql for create table")

	_, err = db.ExecContext(t.Context(), ddl)
	utils.CloseAndLog(db)
	require.NoError(t, err, "create table %s", tableName)

	return func() {
		db2, err := sql.Open("block-mysql", dsn)
		if err != nil {
			return
		}
		defer utils.CloseAndLog(db2)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		if err := db2.PingContext(ctx); err != nil {
			t.Logf("warning: could not ping db to drop test table %s: %v", tableName, err)
			return
		}
		if _, err := db2.ExecContext(ctx, "DROP TABLE IF EXISTS `"+tableName+"`"); err != nil {
			t.Logf("warning: could not drop test table %s: %v", tableName, err)
		}
	}
}

// ColumnExists checks if a column exists on a table by loading the schema via
// Spirit's lint package and inspecting the parsed CREATE TABLE.
func ColumnExists(t *testing.T, dsn, tableName, columnName string) bool {
	t.Helper()
	tables, err := lint.LoadSchemaFromDSN(t.Context(), dsn)
	require.NoError(t, err, "load schema")
	for _, tbl := range tables {
		if tbl.TableName == tableName {
			return tbl.Columns.ByName(columnName) != nil
		}
	}
	return false
}

// FindTable returns the parsed CREATE TABLE for a given table name, or nil.
func FindTable(tables []*statement.CreateTable, name string) *statement.CreateTable {
	for _, t := range tables {
		if t.TableName == name {
			return t
		}
	}
	return nil
}

// CreateTestTableWithCleanup creates a test table and registers cleanup that drops
// the table and clears all rows from the given storage DSNs. Use this when the
// test needs both a clean target table and clean storage state.
func CreateTestTableWithCleanup(t *testing.T, targetDSN, tableName, ddl string, storageDSNs ...string) {
	t.Helper()
	t.Cleanup(CreateTestTable(t, targetDSN, tableName, ddl))
	t.Cleanup(func() {
		for _, dsn := range storageDSNs {
			ClearAllTables(t, dsn)
		}
	})
}

// UniqueTableName generates a table name with a timestamp suffix for test isolation.
func UniqueTableName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano()%100000)
}

// maxSeedRows bounds what a single insert is asked to generate. A request past it
// is a mistake in the caller rather than a slow test, so it fails outright instead
// of waiting on a statement that will not finish — and bounding the request keeps
// the generator's capacity arithmetic well inside the range of an int.
const maxSeedRows = 10_000_000

// SeedRows inserts rowCount rows into the given table using cross-joined sequences.
func SeedRows(t *testing.T, dsn, tableName, columns, valueTemplate string, rowCount int) {
	t.Helper()
	require.Positive(t, rowCount, "seed row count for %s", tableName)
	require.LessOrEqual(t, rowCount, maxSeedRows, "seed row count for %s", tableName)
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "open mysql for seeding")
	defer utils.CloseAndLog(db)

	query := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s LIMIT %d`,
		tableName, columns, valueTemplate, sequenceGenerator(rowCount), rowCount)

	res, err := db.ExecContext(t.Context(), query)
	require.NoError(t, err, "seed %d rows into %s", rowCount, tableName)

	// Callers size their table to how long the resulting schema change must run.
	// A table seeded short finishes its row copy before the assertions that mean
	// to observe it mid-flight, so the shortfall fails here rather than surfacing
	// as an unexplained timing failure in the test that asked for the rows.
	seeded, err := res.RowsAffected()
	require.NoError(t, err, "seeded row count for %s", tableName)
	require.EqualValues(t, rowCount, seeded, "seeded row count for %s", tableName)
}

// sequenceGenerator returns a derived table of sequence values numbering at least
// rowCount, built by cross-joining one ten-row digit table per decimal digit of
// rowCount so the generator widens with what the caller asks for. Callers bound
// rowCount by maxSeedRows, which keeps the capacity below where it could overflow.
func sequenceGenerator(rowCount int) string {
	const digits = `(SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9)`

	var joins []string
	for capacity := 1; capacity < rowCount || len(joins) == 0; capacity *= 10 {
		joins = append(joins, fmt.Sprintf("%s d%d", digits, len(joins)))
	}
	return `(SELECT @row := @row + 1 AS seq FROM ` + strings.Join(joins, ", ") +
		`, (SELECT @row := 0) init) nums`
}

// ClearAllTables deletes all rows from all tables in the given database.
// Used to reset state between e2e tests. Logs warnings instead of failing
// since this is cleanup code. Uses context.Background() because this is
// typically called from t.Cleanup where t.Context() is already canceled.
func ClearAllTables(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("block-mysql", dsn)
	if err != nil {
		t.Logf("warning: could not open db to clear tables: %v", err)
		return
	}
	defer utils.CloseAndLog(db)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Logf("warning: could not ping db to clear tables: %v", err)
		return
	}

	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		t.Logf("warning: could not list tables: %v", err)
		return
	}
	defer utils.CloseAndLog(rows)

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Logf("warning: could not scan table name: %v", err)
			return
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Logf("warning: could not iterate tables: %v", err)
		return
	}

	for _, table := range tables {
		if _, err := db.ExecContext(ctx, "DELETE FROM `"+table+"`"); err != nil {
			t.Logf("warning: could not clear table %s: %v", table, err)
		}
	}
}
