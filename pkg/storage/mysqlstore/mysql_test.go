//go:build integration

package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

var (
	testDB  *sql.DB
	testDSN string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := startMySQLContainer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start MySQL container: %v\n", err)
		os.Exit(1)
	}

	host, err := testutil.ContainerHost(ctx, container)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get container host: %v\n", err)
		os.Exit(1)
	}

	port, err := testutil.ContainerPort(ctx, container, "3306")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get container port: %v\n", err)
		os.Exit(1)
	}

	// clientFoundRows=true: return number of matched rows, not changed rows
	// This is needed because UPDATE ... SET updated_at = NOW() may not change
	// the value if called within the same second, but we still want to know
	// if the row was found.
	testDSN = fmt.Sprintf("root:testpassword@tcp(%s:%d)/schemabot_test?parseTime=true&clientFoundRows=true&multiStatements=true", host, port)

	testDB, err = sql.Open("mysql", testDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to MySQL: %v\n", err)
		os.Exit(1)
	}

	// Wait for MySQL to be ready to accept connections
	for range 30 {
		if err := testDB.PingContext(ctx); err == nil {
			break
		}
		time.Sleep(time.Second)
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
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "testpassword",
			"MYSQL_DATABASE":      "schemabot_test",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(120*time.Second),
			wait.ForListeningPort("3306/tcp"),
		),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
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
