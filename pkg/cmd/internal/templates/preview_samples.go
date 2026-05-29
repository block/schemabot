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
