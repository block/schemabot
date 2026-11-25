package mysqlsize

import (
	"context"
	"database/sql"
	"errors"
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
// case-sensitively, a row that differs from a request only in case is another
// table: Shop.orders and shop.orders each keep their own size, and a row the
// query returned only because another schema asked for that name never
// reaches a request in a different case. On a server that compares names
// ignoring case, a table returned in the server's case reaches the Table the
// caller named, unless another request matched that row exactly or more than
// one row matches ignoring case.
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

	t.Run("case-sensitive server", func(t *testing.T) {
		got := matchEstimates(discardLogger(), []Table{upper, lower}, []sizeRow{row("shop", "orders", 1), row("Shop", "orders", 2)}, false)
		assert.Equal(t, int64(2), rowsOf(got, upper), "Shop.orders keeps its own size")
		assert.Equal(t, int64(1), rowsOf(got, lower), "shop.orders keeps its own size")

		missing := Table{Schema: "shop_b", Name: "Orders"}
		asked := Table{Schema: "shop_a", Name: "orders"}
		got = matchEstimates(discardLogger(), []Table{asked, missing}, []sizeRow{row("shop_a", "orders", 3), row("shop_b", "orders", 1)}, false)
		assert.Equal(t, map[Table]Estimate{asked: {Rows: new(int64(3)), Bytes: new(int64(300))}}, got,
			"shop_b.orders came back only because shop_a asked for orders, and it is not shop_b.Orders")
	})

	t.Run("case-insensitive server", func(t *testing.T) {
		got := matchEstimates(discardLogger(), []Table{upper, lower}, []sizeRow{row("shop", "orders", 1)}, true)
		assert.Equal(t, int64(1), rowsOf(got, lower))
		assert.Nil(t, rowsOf(got, upper), "a row another request matched exactly is not lent to a table that differs in case")

		named := Table{Schema: "Shop_A", Name: "Orders"}
		got = matchEstimates(discardLogger(), []Table{named}, []sizeRow{row("shop_a", "orders", 7), row("shop_a", "refunds", 9)}, true)
		assert.Equal(t, map[Table]Estimate{named: {Rows: new(int64(7)), Bytes: new(int64(700))}}, got,
			"the row reaches the Table the caller named, and a pair nobody asked for is dropped")

		got = matchEstimates(discardLogger(), []Table{named}, []sizeRow{row("shop_a", "orders", 7), row("SHOP_A", "ORDERS", 8)}, true)
		assert.Empty(t, got, "more than one row matching ignoring case gives no estimate")
	})
}

// A request whose distinct schemas and names need more placeholders than one
// statement allows is split, in order, into batches that each fit, and a
// request that fits stays one batch.
func TestBatchesWithin(t *testing.T) {
	tables := []Table{
		{Schema: "s1", Name: "orders"},
		{Schema: "s2", Name: "orders"},
		{Schema: "s3", Name: "orders"},
		{Schema: "s3", Name: "refunds"},
	}
	assert.Equal(t, [][]Table{tables}, batchesWithin(tables, 5))
	assert.Equal(t, [][]Table{tables[:2], tables[2:]}, batchesWithin(tables, 3),
		"s1, s2 and orders fill the first batch; s3, orders and refunds the second")
	for _, batch := range batchesWithin(tables, 2) {
		schemas, names := distinctSchemasAndNames(batch)
		assert.LessOrEqual(t, len(schemas)+len(names), 2)
	}
}

// A failure to set up the session or to run the read fails the whole read,
// naming the step, so the caller plans without sizes.
func TestReadReturnsEachStepsError(t *testing.T) {
	orders := []Table{{Schema: "shop", Name: "orders"}}
	boom := errors.New("connection reset")

	_, err := Read(t.Context(), failingConn{execErr: boom}, nil, orders)
	require.ErrorIs(t, err, boom)
	assert.ErrorContains(t, err, "disable cached statistics for size estimates")

	_, err = Read(t.Context(), failingConn{queryErr: boom}, nil, orders)
	require.ErrorIs(t, err, boom)
	assert.ErrorContains(t, err, "read lower_case_table_names for size estimates")
}

// failingConn fails the statement kinds given an error and succeeds the rest.
type failingConn struct {
	execErr, queryErr error
}

func (c failingConn) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, c.execErr
}

func (c failingConn) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, c.queryErr
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
