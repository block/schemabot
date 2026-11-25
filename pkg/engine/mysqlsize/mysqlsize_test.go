package mysqlsize

import (
	"database/sql"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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

// A row information_schema returns in the server's case maps back to the
// Table the caller named, schema and name alike.
func TestRequestedTables(t *testing.T) {
	requested := requestedTables([]Table{{Schema: "Shop_A", Name: "Orders"}, {Schema: "shop_b", Name: "line_items"}})
	assert.Equal(t, Table{Schema: "Shop_A", Name: "Orders"}, requested[Table{Schema: "shop_a", Name: "orders"}])
	assert.Equal(t, Table{Schema: "shop_b", Name: "line_items"}, requested[Table{Schema: "shop_b", Name: "line_items"}])
	_, ok := requested[Table{Schema: "shop_b", Name: "orders"}]
	assert.False(t, ok, "a name asked for in one schema is not asked for in another")
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
