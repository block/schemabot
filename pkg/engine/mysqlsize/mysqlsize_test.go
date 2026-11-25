package mysqlsize

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// Statistics that report nothing, or a negative sentinel, become an absent
// estimate rather than a zero or a negative size, while a real value, zero
// included, is kept.
func TestValidStatistic(t *testing.T) {
	orders := Table{Schema: "shop", Name: "orders"}
	assert.Nil(t, validStatistic(discardLogger(), orders, "table_rows", sql.NullInt64{}))
	assert.Nil(t, validStatistic(discardLogger(), orders, "table_rows", sql.NullInt64{Int64: -1, Valid: true}))

	zero := validStatistic(discardLogger(), orders, "table_rows", sql.NullInt64{Int64: 0, Valid: true})
	require.NotNil(t, zero)
	assert.Equal(t, int64(0), *zero)

	bytes := validStatistic(discardLogger(), orders, "data_length + index_length", sql.NullInt64{Int64: 6_200_000, Valid: true})
	require.NotNil(t, bytes)
	assert.Equal(t, int64(6_200_000), *bytes)
}

// Each caller's Table keeps its own estimate. On a server that compares names
// case-sensitively, Shop.orders and shop.orders are two tables and each is
// returned under its own name, so neither takes the other's size. On one that
// compares them case-insensitively, a table returned in the server's case
// reaches the Table the caller named, unless another request matched that row
// exactly or more than one row matches ignoring case.
func TestMatchEstimates(t *testing.T) {
	row := func(schema, name string, rows int64) sizeRow {
		return sizeRow{table: Table{Schema: schema, Name: name}, rows: sql.NullInt64{Int64: rows, Valid: true}, bytes: sql.NullInt64{Int64: rows * 100, Valid: true}}
	}
	rowsOf := func(est map[Table]Estimate, tbl Table) any {
		e, ok := est[tbl]
		if !ok || e.Rows == nil {
			return nil
		}
		return *e.Rows
	}
	upper := Table{Schema: "Shop", Name: "orders"}
	lower := Table{Schema: "shop", Name: "orders"}

	got := matchEstimates(discardLogger(), []Table{upper, lower}, []sizeRow{row("shop", "orders", 1), row("Shop", "orders", 2)})
	assert.Equal(t, int64(2), rowsOf(got, upper), "case-sensitive server: Shop.orders keeps its own size")
	assert.Equal(t, int64(1), rowsOf(got, lower), "case-sensitive server: shop.orders keeps its own size")

	got = matchEstimates(discardLogger(), []Table{upper, lower}, []sizeRow{row("shop", "orders", 1)})
	assert.Equal(t, int64(1), rowsOf(got, lower))
	assert.Nil(t, rowsOf(got, upper), "a row another request matched exactly is not lent to a table that differs in case")

	named := Table{Schema: "Shop_A", Name: "Orders"}
	got = matchEstimates(discardLogger(), []Table{named}, []sizeRow{row("shop_a", "orders", 7), row("shop_a", "refunds", 9)})
	assert.Equal(t, map[Table]Estimate{named: {Rows: new(int64(7)), Bytes: new(int64(700))}}, got,
		"case-insensitive server: the row reaches the Table the caller named, and a pair nobody asked for is dropped")

	got = matchEstimates(discardLogger(), []Table{named}, []sizeRow{row("shop_a", "orders", 7), row("SHOP_A", "ORDERS", 8)})
	assert.Empty(t, got, "more than one row matching ignoring case gives no estimate")
}

// The read filters on each schema and each table name once, in the order the
// caller first names them.
func TestDistinctSchemasAndNames(t *testing.T) {
	schemas, names := distinctSchemasAndNames([]Table{
		{Schema: "shop_b", Name: "orders"},
		{Schema: "shop_a", Name: "orders"},
		{Schema: "shop_a", Name: "refunds"},
	})
	assert.Equal(t, []string{"shop_b", "shop_a"}, schemas)
	assert.Equal(t, []string{"orders", "refunds"}, names)
	assert.Equal(t, "?, ?, ?", placeholders(3))
}

// Nothing to size reads nothing, so a nil connection is never touched.
func TestReadWithNoTables(t *testing.T) {
	estimates, err := Read(t.Context(), nil, discardLogger(), nil)
	require.NoError(t, err)
	assert.Nil(t, estimates)
}
