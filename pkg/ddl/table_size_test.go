package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableCostScalesWithSize(t *testing.T) {
	assert.True(t, TableCostScalesWithSize("mysql", []string{"ALTER TABLE `orders` ADD INDEX `idx_region` (`region`)"}),
		"an index build scales with the table")
	assert.False(t, TableCostScalesWithSize("mysql", []string{"CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}),
		"creating a table has no existing data to scale with")
	assert.True(t, TableCostScalesWithSize("mysql", []string{"ALTER TABLE `orders` ADD COLUMN `region` varchar(32);", "ALTER TABLE `orders` FROB"}),
		"DDL the parser cannot inspect keeps its size line rather than understating the work")
}
