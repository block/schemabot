//go:build integration

package localscale_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/utils"
	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tinyIntWidthsDDL declares a tinyint(1) column the way an application would.
// The (1) is a display width, so the column holds -128..127 and values outside
// 0/1 are ordinary stored data, not a misuse of the type.
const tinyIntWidthsDDL = "CREATE TABLE tinyint_widths (id bigint NOT NULL PRIMARY KEY, flag tinyint(1) NOT NULL) " +
	"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// tinyIntWidthsSeed stores both the values that look boolean and the values
// that are destroyed if the column is read as one.
const tinyIntWidthsSeed = "INSERT INTO tinyint_widths (id, flag) VALUES (1, 0), (2, 1), (3, 2), (4, 127)"

// wantTinyIntFlags is the stored data as a reader must see it: every value
// intact, including the two that a bool cannot represent.
var wantTinyIntFlags = []string{"0", "1", "2", "127"}

// A client of a branch proxy reads a tinyint(1) column and gets the stored
// numbers. The proxy scans arbitrary user columns into an any and forwards what
// it finds, so a value the driver had already reduced to a Go bool would reach
// go-mysql's resultset builder, which has no bool case — the SELECT would fail
// outright rather than return a wrong number.
func TestBranchProxyReadsTinyInt1AsNumber(t *testing.T) {
	ctx := t.Context()
	branch := createBranch(t, ctx, "tinyint-proxy")
	applyBranchDDL(t, ctx, branch, map[string][]string{"testapp": {tinyIntWidthsDDL}})

	pw, err := testClient.CreateBranchPassword(ctx, &ps.DatabaseBranchPasswordRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       branch,
	})
	require.NoError(t, err, "CreateBranchPassword")

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/testapp", pw.Username, pw.PlainText, pw.Hostname)
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "open branch proxy connection")
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx), "ping branch proxy")

	_, err = db.ExecContext(ctx, tinyIntWidthsSeed)
	require.NoError(t, err, "seed tinyint(1) values through the branch proxy")

	rows, err := db.QueryContext(ctx, "SELECT id, flag FROM tinyint_widths ORDER BY id")
	require.NoError(t, err, "SELECT a tinyint(1) column through the branch proxy")
	defer utils.CloseAndLog(rows)

	var flags []string
	for rows.Next() {
		var id int64
		var flag string
		require.NoError(t, rows.Scan(&id, &flag), "scan row")
		flags = append(flags, flag)
	}
	require.NoError(t, rows.Err(), "iterate rows")
	assert.Equal(t, wantTinyIntFlags, flags, "proxied tinyint(1) values")
}

// The branch database query endpoint returns the stored numbers for a
// tinyint(1) column. It scans every column into a sql.NullString, which turns a
// Go bool into the literal "true" — a silent loss with no error for an operator
// to notice, so the endpoint's own output is what has to be asserted.
func TestBranchDBQueryReadsTinyInt1AsNumber(t *testing.T) {
	ctx := t.Context()
	branch := createBranch(t, ctx, "tinyint-admin")
	applyBranchDDL(t, ctx, branch, map[string][]string{"testapp": {tinyIntWidthsDDL}})

	seeded, err := testContainer.BranchDBQuery(ctx, branch, "testapp", tinyIntWidthsSeed)
	require.NoError(t, err, "seed tinyint(1) values")
	assert.Equal(t, int64(4), seeded.RowsAffected)

	result, err := testContainer.BranchDBQuery(ctx, branch, "testapp",
		"SELECT id, flag FROM tinyint_widths ORDER BY id")
	require.NoError(t, err, "query tinyint(1) column")
	assert.Equal(t, []string{"id", "flag"}, result.Columns)
	require.Len(t, result.Rows, len(wantTinyIntFlags))

	flags := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(t, row, 2)
		flag, ok := row[1].(string)
		require.True(t, ok, "flag column should decode as a string, got %T", row[1])
		flags = append(flags, flag)
	}
	assert.Equal(t, wantTinyIntFlags, flags, "tinyint(1) values from the query endpoint")
}
