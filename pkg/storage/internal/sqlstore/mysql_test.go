//go:build integration

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

const testDatabaseName = "schemabot_test"

var (
	testDB             *sql.DB
	testDSN            string
	testDSNChangedRows string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := startMySQLContainer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start MySQL container: %v\n", err)
		os.Exit(1)
	}

	// testDSN sets clientFoundRows=true so RowsAffected reflects matched rows.
	// testDSNChangedRows omits it to mirror production, where RowsAffected
	// reflects changed rows: a matched row whose values are unchanged reports
	// zero affected rows. Storage paths that infer existence or ownership from
	// RowsAffected must be correct under the changed-rows connection.
	testDSN, err = testutil.MySQLDSN(ctx, container, testDatabaseName,
		"parseTime=true", "clientFoundRows=true", "multiStatements=true")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build MySQL DSN: %v\n", err)
		os.Exit(1)
	}
	testDSNChangedRows, err = testutil.MySQLDSN(ctx, container, testDatabaseName,
		"parseTime=true", "multiStatements=true")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build MySQL changed-rows DSN: %v\n", err)
		os.Exit(1)
	}

	testDB, err = sql.Open("block-mysql", testDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to MySQL: %v\n", err)
		os.Exit(1)
	}
	if err := testutil.PingMySQL(ctx, testDB); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to reach MySQL: %v\n", err)
		os.Exit(1)
	}

	// Apply schema by executing embedded SQL files directly
	if err := applyTestSchema(testDB); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to apply schema: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	_ = testDB.Close()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

func applyTestSchema(db *sql.DB) error {
	entries, err := schema.MySQLFS.ReadDir("mysql")
	if err != nil {
		return fmt.Errorf("read schema directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, err := schema.MySQLFS.ReadFile("mysql/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		if _, err := db.ExecContext(context.Background(), string(content)); err != nil {
			return fmt.Errorf("execute %s: %w", entry.Name(), err)
		}
	}
	return nil
}

func startMySQLContainer(ctx context.Context) (testcontainers.Container, error) {
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testutil.MySQLContainerRequest("mysql:8.0", testDatabaseName),
		Started:          true,
	})
}

func clearTables(t *testing.T) {
	t.Helper()
	rows, err := testDB.QueryContext(t.Context(), "SHOW TABLES")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

	var tables []string
	for rows.Next() {
		var table string
		require.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}

	for _, table := range tables {
		_, err := testDB.ExecContext(t.Context(), "DELETE FROM "+table)
		require.NoError(t, err, "failed to clear table %s", table)
	}
}
