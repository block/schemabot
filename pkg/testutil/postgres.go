package testutil

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/jackc/pgx/v5/stdlib" // pgx database/sql driver for the returned connection
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// StartPostgres starts a PostgreSQL container with the given database name
// and returns its DSN plus an open connection for assertions. The container
// and connection are cleaned up when the test ends.
func StartPostgres(t *testing.T, database string) (string, *sql.DB) {
	t.Helper()
	ctx := t.Context()

	container, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase(database),
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

	dsn, err := ContainerConnectionString(ctx, container, "sslmode=disable")
	require.NoError(t, err, "failed to get connection string")

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(ctx))

	return dsn, db
}

// PostgresTableExists reports whether tableName exists in schemaName on a
// PostgreSQL connection. It mirrors TableExists, whose `?` placeholders only
// bind on MySQL.
func PostgresTableExists(t *testing.T, db *sql.DB, schemaName, tableName string) bool {
	t.Helper()

	var count int
	err := db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
		schemaName, tableName,
	).Scan(&count)
	require.NoError(t, err)
	return count > 0
}
