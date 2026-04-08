package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatDDL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single ADD INDEX canonicalized",
			input:    "ALTER TABLE `orders` ADD INDEX `idx_user` (`user_id`)",
			expected: "ALTER TABLE `orders` ADD INDEX `idx_user`(`user_id`);",
		},
		{
			name:  "multiple ADD INDEX formatted",
			input: "ALTER TABLE `orders` ADD INDEX `idx_user` (`user_id`), ADD INDEX `idx_status` (`status`)",
			expected: "ALTER TABLE `orders`\n" +
				"    ADD INDEX `idx_user`(`user_id`),\n" +
				"    ADD INDEX `idx_status`(`status`);",
		},
		{
			name:  "multiple ADD INDEX with compound keys",
			input: "ALTER TABLE `orders` ADD INDEX `idx_user_status` (`user_id`, `status`), ADD INDEX `idx_created` (`created_at`)",
			expected: "ALTER TABLE `orders`\n" +
				"    ADD INDEX `idx_user_status`(`user_id`, `status`),\n" +
				"    ADD INDEX `idx_created`(`created_at`);",
		},
		{
			name:  "mixed ADD and DROP",
			input: "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255), DROP COLUMN `old_field`",
			expected: "ALTER TABLE `users`\n" +
				"    ADD COLUMN `email` varchar(255),\n" +
				"    DROP COLUMN `old_field`;",
		},
		{
			name:  "three clauses",
			input: "ALTER TABLE `t` ADD INDEX `a` (`a`), ADD INDEX `b` (`b`), ADD INDEX `c` (`c`)",
			expected: "ALTER TABLE `t`\n" +
				"    ADD INDEX `a`(`a`),\n" +
				"    ADD INDEX `b`(`b`),\n" +
				"    ADD INDEX `c`(`c`);",
		},
		{
			name:     "CREATE TABLE single column unchanged",
			input:    "CREATE TABLE `users` (`id` INT PRIMARY KEY)",
			expected: "CREATE TABLE `users` (`id` int PRIMARY KEY);",
		},
		{
			name:  "CREATE TABLE multiple columns formatted",
			input: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), created_at TIMESTAMP)",
			expected: "CREATE TABLE `users` (\n" +
				"    `id` int PRIMARY KEY,\n" +
				"    `name` varchar(255),\n" +
				"    `created_at` timestamp\n" +
				");",
		},
		{
			name:  "CREATE TABLE with indexes formatted",
			input: "CREATE TABLE users (id INT, name VARCHAR(255), INDEX idx_name (name)) ENGINE=InnoDB",
			expected: "CREATE TABLE `users` (\n" +
				"    `id` int,\n" +
				"    `name` varchar(255),\n" +
				"    INDEX `idx_name`(`name`)\n" +
				") ENGINE InnoDB;",
		},
		{
			name:     "DROP TABLE canonicalized",
			input:    "DROP TABLE `users`",
			expected: "DROP TABLE `users`;",
		},
		{
			name:  "unquoted table name canonicalized",
			input: "ALTER TABLE orders ADD INDEX idx_user (user_id), ADD INDEX idx_status (status)",
			expected: "ALTER TABLE `orders`\n" +
				"    ADD INDEX `idx_user`(`user_id`),\n" +
				"    ADD INDEX `idx_status`(`status`);",
		},
		{
			name:  "MODIFY and CHANGE clauses",
			input: "ALTER TABLE `t` MODIFY COLUMN `a` INT, CHANGE COLUMN `b` `c` VARCHAR(100)",
			expected: "ALTER TABLE `t`\n" +
				"    MODIFY COLUMN `a` int,\n" +
				"    CHANGE COLUMN `b` `c` varchar(100);",
		},
		{
			name:     "input with semicolon not doubled",
			input:    "ALTER TABLE `t` ADD INDEX `idx`(`col`);",
			expected: "ALTER TABLE `t` ADD INDEX `idx`(`col`);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatDDL(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatDDL_LowercaseTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains []string
	}{
		{
			name:  "data types lowercased",
			input: "CREATE TABLE `t` (`id` BIGINT UNSIGNED NOT NULL, `name` VARCHAR(255), `price` DECIMAL(10,2), `data` JSON)",
			contains: []string{
				"bigint unsigned NOT NULL",
				"varchar(255)",
				"decimal(10,2)",
				"json",
			},
		},
		{
			name:  "timestamp and functions lowercased",
			input: "CREATE TABLE `t` (`created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(), `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP())",
			contains: []string{
				"timestamp DEFAULT current_timestamp()",
				"ON UPDATE current_timestamp()",
			},
		},
		{
			name:  "charset and collate lowercased on separate lines",
			input: "CREATE TABLE `t` (`id` INT) ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_0900_AI_CI",
			contains: []string{
				"ENGINE InnoDB",
				"CHARSET utf8mb4",
				"COLLATE utf8mb4_0900_ai_ci",
			},
		},
		{
			name:  "charset literal introducer stripped",
			input: "CREATE TABLE `t` (`status` VARCHAR(50) DEFAULT _UTF8MB4'pending')",
			contains: []string{
				"DEFAULT 'pending'",
				"varchar(50)",
			},
		},
		{
			name:  "DEFAULT NULL stripped",
			input: "CREATE TABLE `t` (`id` BIGINT NOT NULL, `name` VARCHAR(255) DEFAULT NULL, `count` INT DEFAULT NULL)",
			contains: []string{
				"`name` varchar(255),",
				"`count` int",
			},
		},
		{
			name:  "table options on separate lines",
			input: "CREATE TABLE `t` (`id` BIGINT NOT NULL) ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_0900_AI_CI",
			contains: []string{
				") ENGINE InnoDB,\n  CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_ai_ci;",
			},
		},
		{
			name:  "SQL keywords stay uppercase",
			input: "CREATE TABLE `t` (`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(255) NOT NULL DEFAULT _UTF8MB4'unknown')",
			contains: []string{
				"CREATE TABLE",
				"NOT NULL",
				"AUTO_INCREMENT",
				"PRIMARY KEY",
				"DEFAULT 'unknown'",
				"bigint",
				"varchar(255)",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatDDL(tt.input)
			for _, substr := range tt.contains {
				assert.Contains(t, result, substr, "should contain %q in:\n%s", substr, result)
			}
		})
	}
}

func TestCanonicalize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "ALTER TABLE gets canonicalized",
			input:    "alter table orders add index idx (col)",
			expected: "ALTER TABLE `orders` ADD INDEX `idx`(`col`)",
		},
		{
			name:     "already canonical unchanged",
			input:    "ALTER TABLE `orders` ADD INDEX `idx`(`col`)",
			expected: "ALTER TABLE `orders` ADD INDEX `idx`(`col`)",
		},
		{
			name:     "CREATE TABLE gets canonicalized",
			input:    "CREATE TABLE users (id INT)",
			expected: "CREATE TABLE `users` (`id` INT)",
		},
		{
			name:     "DROP TABLE gets canonicalized",
			input:    "DROP TABLE users",
			expected: "DROP TABLE `users`",
		},
		{
			name:     "invalid SQL returns original",
			input:    "not valid sql",
			expected: "not valid sql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Canonicalize(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitAlterClauses(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single clause",
			input:    "ALTER TABLE `t` ADD INDEX `idx`(`col`)",
			expected: []string{"ALTER TABLE `t` ADD INDEX `idx`(`col`)"},
		},
		{
			name:  "two clauses",
			input: "ALTER TABLE `t` ADD INDEX `a`(`a`), ADD INDEX `b`(`b`)",
			expected: []string{
				"ALTER TABLE `t` ADD INDEX `a`(`a`)",
				"ADD INDEX `b`(`b`)",
			},
		},
		{
			name:  "compound key not split",
			input: "ALTER TABLE `t` ADD INDEX `idx`(`a`, `b`, `c`)",
			expected: []string{
				"ALTER TABLE `t` ADD INDEX `idx`(`a`, `b`, `c`)",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitAlterClauses(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
