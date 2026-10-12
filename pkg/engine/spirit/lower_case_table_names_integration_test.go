//go:build integration

package spirit

import (
	"database/sql"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/block/spirit/pkg/utils"
)

// A server started with lower_case_table_names=1 treats `orders` and `Orders`
// as one table. The plan compares table names the same way:
//   - a schema file that writes `Orders` for the live `orders` plans an ALTER
//     of `orders`, not a CREATE the server refuses and a DROP of the live table;
//   - two schema files declaring `orders` and `Orders` are refused before the
//     diff, naming both files.
func TestEngine_Plan_MatchesTableNamesTheWayTheTargetDoes(t *testing.T) {
	req := testutil.MySQLContainerRequest("mysql:8.0", testDatabase)
	req.Cmd = []string{"--lower-case-table-names=1"}
	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "start mysql with lower_case_table_names=1")
	testcontainers.CleanupContainer(t, container)
	dsn, err := testutil.MySQLDSN(t.Context(), container, testDatabase, "parseTime=true")
	require.NoError(t, err)
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	var lctn int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@lower_case_table_names").Scan(&lctn))
	require.Equal(t, 1, lctn)
	_, err = db.ExecContext(t.Context(), "CREATE TABLE orders (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL)")
	require.NoError(t, err)
	eng := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})

	t.Run("a case-only rename alters the live table", func(t *testing.T) {
		result, err := eng.Plan(t.Context(), &engine.PlanRequest{
			Database: testDatabase,
			SchemaFiles: testSchemaFiles(map[string]string{
				"orders.sql": "CREATE TABLE Orders (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL, total INT NULL)",
			}),
			Credentials: &engine.Credentials{DSN: dsn},
		})
		require.NoError(t, err, "Plan()")

		ddl := result.FlatDDL()
		require.Len(t, ddl, 1, "one statement, got %v", ddl)
		assert.Contains(t, ddl[0], "ALTER TABLE `orders`")
		assert.True(t, containsAddColumn(ddl[0], "total"), "expected an ADD COLUMN for total, got %s", ddl[0])
	})

	t.Run("two files declaring one table in two cases are refused", func(t *testing.T) {
		_, err := eng.Plan(t.Context(), &engine.PlanRequest{
			Database: testDatabase,
			SchemaFiles: testSchemaFiles(map[string]string{
				"a_orders.sql": "CREATE TABLE orders (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL)",
				"b_orders.sql": "CREATE TABLE Orders (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL)",
			}),
			Credentials: &engine.Credentials{DSN: dsn},
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, `tables "orders" (schema file "testdb/a_orders.sql") and "Orders" (schema file "testdb/b_orders.sql") are the same table on this target`)
	})
}
