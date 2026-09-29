package spirit

import (
	"database/sql"
	"testing"

	spiritlint "github.com/block/spirit/pkg/lint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The size probe reads each planned table once, however many statements the
// plan carries for it, and in the order the plan first names it. A statement
// that names no table is left out rather than sent as an empty parameter.
func TestPlannedTableNames(t *testing.T) {
	eng := New(Config{})
	names := eng.plannedTableNames("shop", []spiritlint.PlannedChange{
		{TableName: "orders", Statement: "ALTER TABLE `orders` REMOVE PARTITIONING"},
		{TableName: "customers", Statement: "ALTER TABLE `customers` ADD INDEX `idx_email` (`email`)"},
		{TableName: "orders", Statement: "ALTER TABLE `orders` PARTITION BY HASH (`id`) PARTITIONS 4"},
		{TableName: "", Statement: "ALTER TABLE `unnamed` ADD COLUMN `x` int"},
	})
	assert.Equal(t, []string{"orders", "customers"}, names)
}

// Statistics that report nothing, or a negative sentinel, become an absent
// estimate rather than a zero or a negative size, while a real value, zero
// included, is kept.
func TestValidSizeStatistic(t *testing.T) {
	eng := New(Config{})

	assert.Nil(t, eng.validSizeStatistic("shop", "orders", "table_rows", sql.NullInt64{}))
	assert.Nil(t, eng.validSizeStatistic("shop", "orders", "table_rows", sql.NullInt64{Int64: -1, Valid: true}))

	zero := eng.validSizeStatistic("shop", "orders", "table_rows", sql.NullInt64{Int64: 0, Valid: true})
	require.NotNil(t, zero)
	assert.Equal(t, int64(0), *zero)

	bytes := eng.validSizeStatistic("shop", "orders", "data_length + index_length", sql.NullInt64{Int64: 6_200_000, Valid: true})
	require.NotNil(t, bytes)
	assert.Equal(t, int64(6_200_000), *bytes)
}
