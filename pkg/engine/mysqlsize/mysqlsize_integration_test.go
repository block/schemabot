//go:build integration

package mysqlsize

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/block/spirit/pkg/utils"
)

// One read sizes tables spread over several schemas on one server. Each
// estimate lands on the Table the caller named, a table asked for in one
// schema is not reported for another schema that also has it, and a view or
// a missing table gets no estimate.
func TestReadSizesTablesAcrossSchemas(t *testing.T) {
	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testutil.MySQLContainerRequest("mysql:8.0", "shop_a"),
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

	ordersA := Table{Schema: "shop_a", Name: "orders"}
	ordersB := Table{Schema: "shop_b", Name: "Orders"}
	refundsA := Table{Schema: "shop_a", Name: "refunds"}
	view := Table{Schema: "shop_b", Name: "order_ids"}
	missing := Table{Schema: "shop_b", Name: "invoices"}
	estimates, err := Read(t.Context(), conn, discardLogger(), []Table{ordersA, ordersB, refundsA, view, missing})
	require.NoError(t, err)

	assert.Len(t, estimates, 3, "orders in both schemas and refunds in shop_a, but not shop_b's refunds, the view or the missing table")
	require.Contains(t, estimates, ordersA)
	require.NotNil(t, estimates[ordersA].Rows)
	assert.Equal(t, int64(3), *estimates[ordersA].Rows)
	require.NotNil(t, estimates[ordersA].Bytes)
	assert.Positive(t, *estimates[ordersA].Bytes)
	require.Contains(t, estimates, ordersB, "the estimate is keyed by the case the caller wrote")
	require.NotNil(t, estimates[ordersB].Rows)
	assert.Equal(t, int64(1), *estimates[ordersB].Rows)
	assert.Contains(t, estimates, refundsA)
	assert.NotContains(t, estimates, Table{Schema: "shop_b", Name: "refunds"})

	var expiry int
	require.NoError(t, conn.QueryRowContext(t.Context(), "SELECT @@SESSION.information_schema_stats_expiry").Scan(&expiry))
	assert.Equal(t, 0, expiry, "the read disables cached statistics on its session")
}
