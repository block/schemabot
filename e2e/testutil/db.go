//go:build e2e || integration

package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// CreateTestTable creates a table on the given DSN and returns a cleanup function
// that drops it. The cleanup function opens a new connection so it works even
// after the test context is cancelled.
func CreateTestTable(t *testing.T, dsn, tableName, ddl string) func() {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open mysql for create table")

	_, err = db.ExecContext(t.Context(), ddl)
	utils.CloseAndLog(db)
	require.NoError(t, err, "create table %s", tableName)

	return func() {
		db2, err := sql.Open("mysql", dsn)
		if err != nil {
			return
		}
		defer utils.CloseAndLog(db2)
		//nolint:usetesting // cleanup runs after test context cancelled
		_, _ = db2.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+tableName+"`")
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

// UniqueTableName generates a table name with a timestamp suffix for test isolation.
func UniqueTableName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano()%100000)
}
