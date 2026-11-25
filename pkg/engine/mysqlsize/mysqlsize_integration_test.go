//go:build integration

package mysqlsize

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/block/spirit/pkg/utils"
)

// One read sizes tables spread over several schemas on a server that compares
// names case-sensitively. Each estimate lands on the Table the caller named,
// a table asked for in one schema is not reported for another schema that
// also has it, and a view, a missing table, or a name that differs from a
// live table only in case gets no estimate.
func TestReadSizesTablesAcrossSchemas(t *testing.T) {
	conn := startShopServer(t)

	ordersA := Table{Schema: "shop_a", Name: "orders"}
	ordersB := Table{Schema: "shop_b", Name: "Orders"}
	refundsA := Table{Schema: "shop_a", Name: "refunds"}
	view := Table{Schema: "shop_b", Name: "order_ids"}
	missing := Table{Schema: "shop_b", Name: "invoices"}
	estimates, err := Read(t.Context(), conn, discardLogger(), []Table{ordersA, ordersB, refundsA, view, missing})
	require.NoError(t, err)

	assert.Len(t, estimates, 2, "orders and refunds in shop_a, but not shop_b's refunds, the view, the missing table, or shop_b.Orders")
	require.Contains(t, estimates, ordersA)
	require.NotNil(t, estimates[ordersA].Rows)
	assert.Equal(t, int64(3), *estimates[ordersA].Rows)
	require.NotNil(t, estimates[ordersA].Bytes)
	assert.Positive(t, *estimates[ordersA].Bytes)
	assert.Contains(t, estimates, refundsA)
	assert.NotContains(t, estimates, ordersB, "shop_b.Orders is not shop_b.orders on this server, so it does not take that table's size")
	assert.NotContains(t, estimates, Table{Schema: "shop_b", Name: "refunds"})

	var expiry int
	require.NoError(t, conn.QueryRowContext(t.Context(), "SELECT @@SESSION.information_schema_stats_expiry").Scan(&expiry))
	assert.Equal(t, 0, expiry, "the read disables cached statistics on its session")

	t.Run("a row that does not scan fails the read", func(t *testing.T) {
		garbled := rewriteSizeQuery{conn: conn, to: "SELECT ?, ?, 'not a count', 1"}
		_, err := Read(t.Context(), garbled, discardLogger(), []Table{ordersA})
		require.Error(t, err)
		assert.ErrorContains(t, err, "scan table size estimate")
	})
}

// On a server that compares names ignoring case, a table the caller names in
// a different case from the server's still gets its size, keyed by the case
// the caller wrote.
func TestReadMatchesNamesIgnoringCaseWhenTheServerDoes(t *testing.T) {
	conn := startShopServer(t, "--lower-case-table-names=1")

	ordersA := Table{Schema: "shop_a", Name: "orders"}
	ordersB := Table{Schema: "Shop_B", Name: "Orders"}
	estimates, err := Read(t.Context(), conn, discardLogger(), []Table{ordersA, ordersB})
	require.NoError(t, err)

	require.Contains(t, estimates, ordersA)
	require.NotNil(t, estimates[ordersA].Rows)
	assert.Equal(t, int64(3), *estimates[ordersA].Rows)
	require.Contains(t, estimates, ordersB, "the estimate is keyed by the case the caller wrote")
	require.NotNil(t, estimates[ordersB].Rows)
	assert.Equal(t, int64(1), *estimates[ordersB].Rows)
}

// startShopServer starts a MySQL server with mysqld args, creates the shop_a
// and shop_b schemas with an orders and a refunds table each and a view in
// shop_b, and returns a dedicated connection to it.
func startShopServer(t *testing.T, mysqldArgs ...string) *sql.Conn {
	t.Helper()
	req := testutil.MySQLContainerRequest("mysql:8.0", "shop_a")
	req.Cmd = append(req.Cmd, mysqldArgs...)
	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "start mysql")
	testcontainers.CleanupContainer(t, container)
	dsn, err := testutil.MySQLDSN(t.Context(), container, "shop_a")
	require.NoError(t, err)
	db, err := mysqlconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, testutil.PingMySQL(t.Context(), db))

	for _, stmt := range []string{
		"CREATE DATABASE `shop_b`",
		"CREATE TABLE `shop_a`.`orders` (id INT PRIMARY KEY)",
		"CREATE TABLE `shop_a`.`refunds` (id INT PRIMARY KEY)",
		"CREATE TABLE `shop_b`.`orders` (id INT PRIMARY KEY)",
		"CREATE TABLE `shop_b`.`refunds` (id INT PRIMARY KEY)",
		"CREATE VIEW `shop_b`.`order_ids` AS SELECT id FROM `shop_b`.`orders`",
		"INSERT INTO `shop_a`.`orders` VALUES (1), (2), (3)",
		"INSERT INTO `shop_b`.`orders` VALUES (1)",
		"ANALYZE TABLE `shop_a`.`orders`, `shop_b`.`orders`",
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(conn) })
	return conn
}

// rewriteSizeQuery replaces the size query with another statement that binds
// the same arguments, and passes every other statement through unchanged.
type rewriteSizeQuery struct {
	conn *sql.Conn
	to   string
}

func (c rewriteSizeQuery) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(ctx, query, args...)
}

func (c rewriteSizeQuery) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if strings.Contains(query, "information_schema.tables") {
		query = c.to
	}
	return c.conn.QueryContext(ctx, query, args...)
}
