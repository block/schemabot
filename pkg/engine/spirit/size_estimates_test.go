package spirit

import (
	"testing"

	spiritlint "github.com/block/spirit/pkg/lint"
	"github.com/stretchr/testify/assert"
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

// Only tables already in the current schema are probed for sizes; a table the
// plan creates has none to read, and plan order is kept within each group.
func TestPartitionByExistence(t *testing.T) {
	current := map[string]string{"orders": "CREATE TABLE `orders` ...", "customers": "CREATE TABLE `customers` ..."}
	existing, created := partitionByExistence([]string{"refunds", "orders", "audit_log", "customers"}, current)
	assert.Equal(t, []string{"orders", "customers"}, existing)
	assert.Equal(t, []string{"refunds", "audit_log"}, created)
}
