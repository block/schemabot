//go:build integration

package schema

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/block/schemabot/pkg/testutil"
)

// TestPostgresSchemaFilesExecuteAndMirrorMySQL executes every embedded
// postgres schema file against a real PostgreSQL server, then verifies each
// table mirrors its MySQL counterpart column-for-column: same column names and
// same nullability. The MySQL side is parsed from the embedded files; the
// PostgreSQL side is read back from information_schema after the DDL runs, so
// the comparison covers what the server actually created, not what the file
// claims.
func TestPostgresSchemaFilesExecuteAndMirrorMySQL(t *testing.T) {
	ctx := t.Context()

	container, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("schemabot_test"),
		postgres.WithUsername("schemabot"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err, "failed to start postgres")
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	dsn, err := testutil.ContainerConnectionString(ctx, container, "sslmode=disable")
	require.NoError(t, err, "failed to get connection string")

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(ctx))

	for name, content := range readSchemaDir(t, "postgres") {
		for stmt := range strings.SplitSeq(content, ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err, "%s: statement failed:\n%s", name, stmt)
		}
	}

	for name, content := range readSchemaDir(t, "mysql") {
		parsed, err := statement.ParseCreateTable(content)
		require.NoError(t, err, "parse mysql schema file %s", name)

		pgColumns := postgresColumns(t, db, parsed.TableName)
		require.NotEmpty(t, pgColumns, "table %s missing from postgres schema", parsed.TableName)

		mysqlNames := make([]string, 0, len(parsed.Columns))
		for _, col := range parsed.Columns {
			mysqlNames = append(mysqlNames, col.Name)
			nullable, ok := pgColumns[col.Name]
			if assert.True(t, ok, "table %s: column %s missing from postgres schema", parsed.TableName, col.Name) {
				assert.Equal(t, col.Nullable, nullable,
					"table %s: column %s nullability differs (mysql nullable=%v)", parsed.TableName, col.Name, col.Nullable)
			}
		}
		pgNames := make([]string, 0, len(pgColumns))
		for colName := range pgColumns {
			pgNames = append(pgNames, colName)
		}
		assert.ElementsMatch(t, mysqlNames, pgNames, "table %s: column sets differ", parsed.TableName)
	}
}

// postgresColumns returns column name → nullable for a table in the public
// schema of the connected PostgreSQL database.
func postgresColumns(t *testing.T, db *sql.DB, tableName string) map[string]bool {
	t.Helper()

	rows, err := db.QueryContext(t.Context(),
		`SELECT column_name, is_nullable FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = $1`,
		tableName,
	)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	columns := make(map[string]bool)
	for rows.Next() {
		var colName, isNullable string
		require.NoError(t, rows.Scan(&colName, &isNullable))
		columns[colName] = isNullable == "YES"
	}
	require.NoError(t, rows.Err())
	return columns
}
