//go:build integration

package livee2e

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/testutil"
)

var targetDSN string

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testutil.MySQLContainerRequest("mysql:8.0", "testdb"),
		Started:          true,
	})
	if err != nil {
		log.Fatalf("start mysql container: %v", err)
	}

	targetDSN, err = testutil.MySQLDSN(ctx, container, "testdb")
	if err != nil {
		log.Fatalf("build mysql dsn: %v", err)
	}

	code := m.Run()

	if err := container.Terminate(ctx); err != nil {
		log.Printf("terminate mysql container: %v", err)
	}
	os.Exit(code)
}

// targetDatabase holds a backtick so every query proves it quotes the
// database name.
const targetDatabase = "live`e2e"

// openTestTarget recreates targetDatabase with a seed table of seedRows rows
// and opens a Target on it whose seed batches hold batchRows rows.
func openTestTarget(t *testing.T, seedRows int, batchRows int64) (*mysqlTarget, *sql.DB) {
	t.Helper()
	ctx := t.Context()

	db, err := mysqlconn.Open(targetDSN)
	require.NoError(t, err, "connect to mysql")
	t.Cleanup(func() { utils.CloseAndLog(db) })

	quoted := quoteIdentifier(targetDatabase)
	for _, stmt := range []string{
		"DROP DATABASE IF EXISTS " + quoted,
		"CREATE DATABASE " + quoted,
		"CREATE TABLE " + quoted + ".seed (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, payload VARCHAR(64) NOT NULL, created_at DATETIME NOT NULL)",
	} {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	for i := range seedRows {
		_, err := db.ExecContext(ctx, "INSERT INTO "+quoted+".seed (payload, created_at) VALUES (?, NOW())", fmt.Sprintf("row-%d", i))
		require.NoError(t, err, "insert seed row")
	}
	// A gap in the seed table's IDs, so a batch cannot assume they are dense.
	_, err = db.ExecContext(ctx, "DELETE FROM "+quoted+".seed WHERE id IN (4, 5)")
	require.NoError(t, err, "punch a gap in the seed table")

	target, err := OpenMySQLTarget(ctx, targetDSN, targetDatabase, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(target) })
	mt, ok := target.(*mysqlTarget)
	require.True(t, ok)
	mt.batchRows = batchRows
	return mt, db
}

// createRunTable creates table in targetDatabase with the columns Seed fills.
func createRunTable(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), "CREATE TABLE "+quoteIdentifier(targetDatabase)+"."+quoteIdentifier(table)+
		" (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, payload VARCHAR(64) NOT NULL, created_at DATETIME NOT NULL)")
	require.NoError(t, err, "create %s", table)
}

func TestOpenMySQLTargetRefusesAMissingDatabase(t *testing.T) {
	_, err := OpenMySQLTarget(t.Context(), targetDSN, "no_such_database", slog.New(slog.DiscardHandler))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestMySQLTargetSeedsAcrossBatches(t *testing.T) {
	target, db := openTestTarget(t, 27, 10)
	ctx := t.Context()
	const table = "e2e_20261105103001"
	createRunTable(t, db, table)

	require.NoError(t, target.Seed(ctx, table, "seed", 23))
	n, err := target.RowCount(ctx, table)
	require.NoError(t, err)
	assert.Equal(t, int64(23), n, "three batches, the last one partial")

	require.NoError(t, target.Seed(ctx, table, "seed", 25), "a retried seed copies only what is missing")
	n, err = target.RowCount(ctx, table)
	require.NoError(t, err)
	assert.Equal(t, int64(25), n)

	err = target.Seed(ctx, table, "seed", 60)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has only 25 rows")

	err = target.Seed(ctx, table, "seed", 10)
	require.Error(t, err, "a table that already holds more rows is not truncated")
}

func TestMySQLTargetRefusesToSeedATableARunDidNotCreate(t *testing.T) {
	target, _ := openTestTarget(t, 5, 10)
	err := target.Seed(t.Context(), "seed", "seed", 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to seed")
	n, err := target.RowCount(t.Context(), "seed")
	require.NoError(t, err)
	assert.Equal(t, int64(3), n, "the seed table is untouched")
}

func TestMySQLTargetReadsTheSchema(t *testing.T) {
	target, db := openTestTarget(t, 3, 10)
	ctx := t.Context()
	const table = "e2e_20261105103001"
	createRunTable(t, db, table)

	exists, err := target.TableExists(ctx, table)
	require.NoError(t, err)
	assert.True(t, exists)
	exists, err = target.TableExists(ctx, "e2e_20261105103002")
	require.NoError(t, err)
	assert.False(t, exists)

	columnType, err := target.ColumnType(ctx, table, "payload")
	require.NoError(t, err)
	assert.Equal(t, "varchar(64)", columnType)
	_, err = target.ColumnType(ctx, table, "missing")
	require.Error(t, err)

	leftover, err := target.LeftoverCopyTables(ctx, table)
	require.NoError(t, err)
	assert.Empty(t, leftover)
	createRunTable(t, db, "_"+table+"_new")
	leftover, err = target.LeftoverCopyTables(ctx, table)
	require.NoError(t, err)
	assert.Equal(t, []string{"_" + table + "_new"}, leftover)
}

func TestMySQLTargetListsAndDropsOnlyRunTables(t *testing.T) {
	target, db := openTestTarget(t, 3, 10)
	ctx := t.Context()
	for _, table := range []string{
		"e2e_20261105103001",
		"e2e_20261105101500",
		"e2e_orders",          // the e2e_ prefix without a timestamp
		"e2eX20261105103001",  // matches LIKE 'e2e_%' unless the underscore is escaped
		"e2e_202611051030011", // one digit too many
	} {
		createRunTable(t, db, table)
	}

	tables, err := target.RunTables(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"e2e_20261105103001", "e2e_20261105101500"}, tables)

	for _, table := range []string{"seed", "e2e_orders", "e2eX20261105103001"} {
		err := target.DropTable(ctx, table)
		require.Error(t, err, "%s is not a run table", table)
		exists, err := target.TableExists(ctx, table)
		require.NoError(t, err)
		assert.True(t, exists, "%s survives the refused drop", table)
	}

	require.NoError(t, target.DropTable(ctx, "e2e_20261105103001"))
	require.NoError(t, target.DropTable(ctx, "e2e_20261105103001"), "dropping a run table twice is not an error")
	tables, err = target.RunTables(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"e2e_20261105101500"}, tables)
}
