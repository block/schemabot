package templates

import (
	"github.com/block/schemabot/pkg/apitypes"
)

// samplePlanChanges returns reusable DDL changes for plan preview functions.
func samplePlanChanges() []DDLChange {
	return []DDLChange{
		{ChangeType: "CREATE", TableName: "users", DDL: "CREATE TABLE `users` (`id` bigint NOT NULL AUTO_INCREMENT, `email` varchar(255) NOT NULL, `created_at` timestamp DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`), INDEX `idx_email` (`email`))"},
		{ChangeType: "CREATE", TableName: "orders", DDL: "CREATE TABLE `orders` (`id` bigint NOT NULL AUTO_INCREMENT, `user_id` bigint NOT NULL, `total_cents` bigint NOT NULL, `status` varchar(50) NOT NULL DEFAULT 'pending', PRIMARY KEY (`id`), INDEX `idx_user_id` (`user_id`))"},
		{ChangeType: "ALTER", TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_category_price` (`category`, `price`)"},
	}
}

// samplePartitionedPlanChanges returns plan changes for partitioned tables: a
// new table partitioned by month, a REORGANIZE PARTITION that splits the
// catch-all partition, and an ALTER that adds a column and repartitions.
func samplePartitionedPlanChanges() []DDLChange {
	return []DDLChange{
		{ChangeType: "CREATE", TableName: "ledger_entries", DDL: "CREATE TABLE `ledger_entries` (`id` bigint NOT NULL AUTO_INCREMENT, `settlement_date` date NOT NULL, `amount_cents` bigint NOT NULL, " +
			"PRIMARY KEY (`id`,`settlement_date`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci " +
			"PARTITION BY RANGE COLUMNS(`settlement_date`) (" +
			"PARTITION `p202601` VALUES LESS THAN ('2026-02-01') ENGINE = InnoDB," +
			"PARTITION `p202602` VALUES LESS THAN ('2026-03-01') ENGINE = InnoDB," +
			"PARTITION `p202603` VALUES LESS THAN ('2026-04-01') ENGINE = InnoDB," +
			"PARTITION `future` VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB)"},
		{ChangeType: "ALTER", TableName: "events", DDL: "ALTER TABLE `events` REORGANIZE PARTITION `future` INTO (" +
			"PARTITION `p202611` VALUES LESS THAN ('2026-12-01'), " +
			"PARTITION `p202612` VALUES LESS THAN ('2027-01-01'), " +
			"PARTITION `future` VALUES LESS THAN (MAXVALUE))"},
		{ChangeType: "ALTER", TableName: "payouts", DDL: "ALTER TABLE `payouts` ADD COLUMN `note` varchar(64) NULL DEFAULT NULL " +
			"PARTITION BY RANGE COLUMNS (`settlement_date`) (" +
			"PARTITION `p2025` VALUES LESS THAN ('2026-01-01')," +
			"PARTITION `p2026` VALUES LESS THAN ('2027-01-01')," +
			"PARTITION `future` VALUES LESS THAN (MAXVALUE))"},
	}
}

// samplePlanLintViolations returns reusable lint violations for plan preview functions.
func samplePlanLintViolations() []apitypes.LintViolationResponse {
	return []apitypes.LintViolationResponse{
		{Message: "has_float: New column uses floating-point data type", Table: "orders", Linter: "has_float"},
		{Message: "no_default: Column added without DEFAULT value", Table: "users", Linter: "no_default"},
	}
}

// seqDDLs are common DDLs used across sequential mode preview examples.
var seqDDLs = []struct {
	table string
	ddl   string
}{
	{"users", "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)"},
	{"orders", "ALTER TABLE `orders` ADD INDEX `idx_user_status` (`user_id`, `status`)"},
	{"products", "ALTER TABLE `products` ADD COLUMN `weight_grams` INT DEFAULT 0"},
}
