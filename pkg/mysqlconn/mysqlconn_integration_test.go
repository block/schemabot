//go:build integration

package mysqlconn

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testutil.MySQLContainerRequest("mysql:8.0", "testdb"),
		Started:          true,
	})
	if err != nil {
		log.Fatalf("start mysql container: %v", err)
	}

	sharedDSN, err = testutil.MySQLDSN(ctx, container, "testdb")
	if err != nil {
		log.Fatalf("build mysql dsn: %v", err)
	}

	code := m.Run()

	if err := container.Terminate(ctx); err != nil {
		log.Printf("terminate mysql container: %v", err)
	}
	os.Exit(code)
}

// tinyIntWidthsDDL declares a tinyint(1) column the way an application would.
// The (1) is a display width, so the column holds -128..127 and values outside
// 0/1 are ordinary stored data rather than a misuse of the type.
const tinyIntWidthsDDL = "CREATE TABLE tinyint_widths (id bigint NOT NULL PRIMARY KEY, flag tinyint(1) NOT NULL) " +
	"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// A pool opened through this package reads a signed tinyint(1) column as the
// number stored in it. A target database's tinyint(1) columns are application
// data, so a value the driver had reduced to a Go bool would reach a caller as
// true for every non-zero number, with the original value unrecoverable.
func TestOpenReadsTinyInt1AsNumber(t *testing.T) {
	ctx := t.Context()
	db, err := Open(sharedDSN)
	require.NoError(t, err, "open pool")
	defer utils.CloseAndLog(db)
	require.NoError(t, testutil.PingMySQL(ctx, db), "ping")

	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS tinyint_widths")
	require.NoError(t, err, "drop table")
	_, err = db.ExecContext(ctx, tinyIntWidthsDDL)
	require.NoError(t, err, "create table")
	_, err = db.ExecContext(ctx, "INSERT INTO tinyint_widths (id, flag) VALUES (1, 0), (2, 1), (3, 2), (4, 127)")
	require.NoError(t, err, "seed values")

	// Scanning into an any is what a caller reading columns of unknown type
	// does, and it is where a bool substitution would be invisible.
	rows, err := db.QueryContext(ctx, "SELECT flag FROM tinyint_widths ORDER BY id")
	require.NoError(t, err, "select")
	defer utils.CloseAndLog(rows)

	var flags []string
	for rows.Next() {
		var flag any
		require.NoError(t, rows.Scan(&flag), "scan")
		require.IsTypef(t, int64(0), flag, "tinyint(1) should not decode as %T", flag)
		flags = append(flags, fmt.Sprintf("%v", flag))
	}
	require.NoError(t, rows.Err(), "iterate")
	assert.Equal(t, []string{"0", "1", "2", "127"}, flags, "stored tinyint(1) values")
}

// SchemaBot's own tinyint(1) columns hold 0 or 1 and are scanned into a Go
// bool, which keeps working because database/sql converts the number back. A
// value no bool can represent now fails the scan instead of reading as true,
// so a column that has been corrupted is reported rather than guessed at.
func TestOpenScansBooleanColumnsIntoBool(t *testing.T) {
	ctx := t.Context()
	db, err := Open(sharedDSN)
	require.NoError(t, err, "open pool")
	defer utils.CloseAndLog(db)
	require.NoError(t, testutil.PingMySQL(ctx, db), "ping")

	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS tinyint_bools")
	require.NoError(t, err, "drop table")
	_, err = db.ExecContext(ctx,
		"CREATE TABLE tinyint_bools (id bigint NOT NULL PRIMARY KEY, flag tinyint(1) NOT NULL) "+
			"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "create table")
	_, err = db.ExecContext(ctx, "INSERT INTO tinyint_bools (id, flag) VALUES (1, 0), (2, 1), (3, 2)")
	require.NoError(t, err, "seed values")

	var flag bool
	require.NoError(t,
		db.QueryRowContext(ctx, "SELECT flag FROM tinyint_bools WHERE id = 1").Scan(&flag), "scan 0")
	assert.False(t, flag, "0 scans as false")
	require.NoError(t,
		db.QueryRowContext(ctx, "SELECT flag FROM tinyint_bools WHERE id = 2").Scan(&flag), "scan 1")
	assert.True(t, flag, "1 scans as true")

	err = db.QueryRowContext(ctx, "SELECT flag FROM tinyint_bools WHERE id = 3").Scan(&flag)
	require.Error(t, err, "a stored 2 must not scan into a bool")
	assert.Contains(t, err.Error(), "couldn't convert 2 into type bool")
}
