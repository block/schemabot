package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A pull request approved on one base and rebased onto a newer one keeps its
// approval only when its edit to the table generates the same statements on
// both bases and leaves everything the base changed in between as the base
// has it.
func TestSameTableChange(t *testing.T) {
	const (
		base = "CREATE TABLE `variants` (\n" +
			"  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
			"  `token` varchar(255) NOT NULL,\n" +
			"  `amount` int DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`),\n" +
			"  UNIQUE KEY `u_token` (`token`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
		// newerBase is base after the default branch added two columns.
		newerBase = "CREATE TABLE `variants` (\n" +
			"  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
			"  `token` varchar(255) NOT NULL,\n" +
			"  `amount` int DEFAULT NULL,\n" +
			"  `bounty_amount` bigint DEFAULT NULL,\n" +
			"  `bounty_currency` varchar(3) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`),\n" +
			"  UNIQUE KEY `u_token` (`token`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	)
	tests := []struct {
		name                                   string
		approvedBase, approved, headBase, head string
		wantSame                               bool
		wantReason                             []string
	}{
		{
			name:         "a column the PR appends below columns the base appended",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: newerBase,
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`bounty_amount` bigint DEFAULT NULL, `bounty_currency` varchar(3) DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantSame: true,
		},
		{
			name:         "a column the PR inserts above columns the base appended",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `sku` varchar(255) DEFAULT NULL, `token` varchar(255) NOT NULL, " +
				"`amount` int DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: newerBase,
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `sku` varchar(255) DEFAULT NULL, `token` varchar(255) NOT NULL, " +
				"`amount` int DEFAULT NULL, `bounty_amount` bigint DEFAULT NULL, `bounty_currency` varchar(3) DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantSame: true,
		},
		{
			name:         "an index the PR adds while the base changed the table comment",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`), KEY `i_amount` (`amount`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='campaign variants'",
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`), KEY `i_amount` (`amount`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='campaign variants'",
			wantSame: true,
		},
		{
			name:         "a reformatted file on the same base",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: base,
			head: "-- campaign variants\ncreate table variants (\n  id bigint not null auto_increment,\n  token varchar(255) not null,\n  amount int default null,\n" +
				"  sku varchar(255) default null,\n  primary key (id),\n  unique key u_token (token)\n) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_0900_ai_ci;\n",
			wantSame: true,
		},
		{
			name:         "a column the PR places differently at the head",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: newerBase,
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, `bounty_amount` bigint DEFAULT NULL, `bounty_currency` varchar(3) DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: []string{"table variants", "ADD COLUMN `sku` varchar(255) NULL DEFAULT NULL AFTER `amount`"},
		},
		{
			name:         "a column the PR defines differently at the head",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: newerBase,
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`bounty_amount` bigint DEFAULT NULL, `bounty_currency` varchar(3) DEFAULT NULL, " +
				"`sku` varchar(64) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: []string{"table variants", "varchar(64)"},
		},
		{
			name:         "a head that drops a column the base added",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: newerBase,
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`bounty_amount` bigint DEFAULT NULL, `sku` varchar(255) DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: []string{"table variants", "DROP COLUMN `bounty_currency`"},
		},
		{
			// Both bases are modified by the same statement, but on the newer
			// base that statement also undoes the base's NOT NULL.
			name:         "a head that keeps the PR's version of a column the base made NOT NULL",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` bigint DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int NOT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` bigint DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: []string{"table variants", "the base changed column amount", "replaces it"},
		},
		{
			name:         "a head that keeps the PR's version of an index the base changed",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`, `amount`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`) COMMENT 'lookup by token') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`, `amount`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: []string{"table variants", "the base changed index u_token", "replaces it"},
		},
		{
			name:         "a file that renames its table at the head",
			approvedBase: base,
			approved:     base,
			headBase:     base,
			head:         "CREATE TABLE `variants_v2` (`id` bigint NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason:   []string{"declares table variants at the approved commit's base but variants_v2 at the head"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			same, reason, err := SameTableChange(tt.approvedBase, tt.approved, tt.headBase, tt.head)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, same)
			if tt.wantSame {
				assert.Empty(t, reason)
				return
			}
			for _, want := range tt.wantReason {
				assert.Contains(t, reason, want)
			}
		})
	}
}

// Files the comparison cannot read as one CREATE TABLE prove nothing, so the
// caller counts them as changed.
func TestSameTableChange_CannotCompare(t *testing.T) {
	table := "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	tests := []struct {
		name    string
		head    string
		wantErr string
	}{
		{name: "two statements", head: table + ";\nCREATE TABLE `u` (`id` bigint NOT NULL, PRIMARY KEY (`id`));", wantErr: "parse the schema file at the head: the file holds 2 statements"},
		{name: "an empty file", head: "", wantErr: "parse the schema file at the head: the file holds 0 statements"},
		{name: "only a comment", head: "-- nothing here\n", wantErr: "parse the schema file at the head: failed to parse SQL statements"},
		{name: "not a CREATE TABLE", head: "ALTER TABLE `t` ADD COLUMN `x` int", wantErr: "parse the schema file at the head: parse CREATE TABLE"},
		{name: "unparseable", head: "CREATE TABLE `t` (`id` bigint NOT NULL,,)", wantErr: "parse the schema file at the head"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			same, reason, err := SameTableChange(table, table, table, tt.head)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.False(t, same)
			assert.Empty(t, reason)
		})
	}
}
