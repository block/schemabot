package ddl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A pull request approved on one base and rebased onto a newer one keeps its
// approval only when its edit to the table generates no statement on the
// newer base that the reviewer did not approve, and leaves everything the
// base changed in between as the base has it.
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
			wantReason: []string{"table variants: the approved commit generates", "ADD COLUMN `sku` varchar(255)", "but the head generates", "ADD COLUMN `sku` varchar(64)"},
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
			wantReason: []string{"table variants", "but the head generates", "DROP COLUMN `bounty_currency`"},
		},
		{
			// Both commits run the same MODIFY COLUMN statement, so the head
			// runs only what the reviewer approved.
			name:         "a head that keeps the PR's version of a column the base made NOT NULL",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` bigint DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int NOT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` bigint DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantSame: true,
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
			wantSame: true,
		},
		{
			name:         "a head that reverts a column the base changed and the PR did not",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int NOT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: []string{"table variants", "but the head generates", "MODIFY COLUMN `amount` int NULL DEFAULT NULL"},
		},
		{
			name:         "a table option the PR sets beside one the base set",
			approvedBase: base,
			approved:     base + " COMMENT='campaign variants'",
			headBase:     base + " STATS_PERSISTENT=1",
			head:         base + " STATS_PERSISTENT=1 COMMENT='campaign variants'",
			wantSame:     true,
		},
		{
			name:         "a head that drops a table option the base set",
			approvedBase: base,
			approved:     base + " COMMENT='campaign variants'",
			headBase:     base + " STATS_PERSISTENT=1",
			head:         base + " COMMENT='campaign variants'",
			wantReason:   []string{"table variants", "but the head generates", "STATS_PERSISTENT=DEFAULT"},
		},
		{
			// The diff does not act on the TEMPORARY modifier, so the head
			// runs the approved statement.
			name:         "a head that makes the table TEMPORARY",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: base,
			head: "CREATE TEMPORARY TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantSame: true,
		},
		{
			// Both commits run MODIFY COLUMN `b` ... FIRST, the statement the
			// reviewer approved.
			name:         "a head that undoes the base's column order",
			approvedBase: "CREATE TABLE `t` (`id` int NOT NULL, `b` int, `a` int, PRIMARY KEY (`id`))",
			approved:     "CREATE TABLE `t` (`b` int, `id` int NOT NULL, `a` int, PRIMARY KEY (`id`))",
			headBase:     "CREATE TABLE `t` (`id` int NOT NULL, `a` int, `b` int, PRIMARY KEY (`id`))",
			head:         "CREATE TABLE `t` (`b` int, `id` int NOT NULL, `a` int, PRIMARY KEY (`id`))",
			wantSame:     true,
		},
		{
			name:         "a head that orders the PR's columns differently",
			approvedBase: "CREATE TABLE `t` (`id` int NOT NULL, PRIMARY KEY (`id`))",
			approved:     "CREATE TABLE `t` (`id` int NOT NULL, `x` int, `y` int, PRIMARY KEY (`id`))",
			headBase:     "CREATE TABLE `t` (`id` int NOT NULL, PRIMARY KEY (`id`))",
			head:         "CREATE TABLE `t` (`id` int NOT NULL, `y` int, `x` int, PRIMARY KEY (`id`))",
			wantReason:   []string{"table t", "but the head generates", "ADD COLUMN `y` int NULL AFTER `id`"},
		},
		{
			// A stacked pull request below this one merged the column change,
			// so the head runs only the index change, which was approved.
			name:         "a column change the base now has from a pull request below this one",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) DEFAULT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`) INVISIBLE) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) DEFAULT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) DEFAULT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`) INVISIBLE) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantSame: true,
		},
		{
			// The head runs nothing for the column: the base's own version
			// stands, and the PR's version is no longer part of the change.
			name:         "a column the PR adds that the base has since added with another definition",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`), KEY `i_amount` (`amount`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(64) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(64) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`), KEY `i_amount` (`amount`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantSame: true,
		},
		{
			// The base made the index INVISIBLE before the PR drops it; the
			// head runs the same drop that was approved.
			name:         "an index the PR drops that the base changed first",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`) INVISIBLE) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantSame: true,
		},
		{
			// The head no longer adds `sku`, and the base never added it, so
			// the head drops part of the approved change.
			name:         "a head that stops making a change the base does not have",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`), KEY `i_amount` (`amount`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: newerBase,
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`bounty_amount` bigint DEFAULT NULL, `bounty_currency` varchar(3) DEFAULT NULL, " +
				"PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`), KEY `i_amount` (`amount`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: []string{"table variants", "but the head generates", "and the head's base already runs", "ADD COLUMN `bounty_amount`"},
		},
		{
			// The base added the column the PR adds, but the head declares a
			// third version of it, which nobody approved.
			name:         "a head that redefines a column the base has since added",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(64) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			head: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(128) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: []string{"table variants", "but the head generates", "MODIFY COLUMN `sku` varchar(128)"},
		},
		{
			// The base now has `x`, so the head runs only the approved clause
			// that adds `y` after it.
			name:         "a column the PR adds after one the base has since added",
			approvedBase: "CREATE TABLE `t` (`id` int NOT NULL, `a` int, PRIMARY KEY (`id`))",
			approved:     "CREATE TABLE `t` (`id` int NOT NULL, `a` int, `x` int, `y` int, PRIMARY KEY (`id`))",
			headBase:     "CREATE TABLE `t` (`id` int NOT NULL, `a` int, `x` int, PRIMARY KEY (`id`))",
			head:         "CREATE TABLE `t` (`id` int NOT NULL, `a` int, `x` int, `y` int, PRIMARY KEY (`id`))",
			wantSame:     true,
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

// A file that declares a column twice is rejected before anything is
// diffed, at whichever commit it appears.
func TestSameTableChange_DuplicateDeclaration(t *testing.T) {
	table := "CREATE TABLE `t` (`id` bigint NOT NULL, `x` int, PRIMARY KEY (`id`))"
	duplicated := "CREATE TABLE `t` (`id` bigint NOT NULL, `x` int, `X` int, PRIMARY KEY (`id`))"
	for i, side := range []string{"approved commit's base", "approved commit", "head's base", "head"} {
		t.Run(side, func(t *testing.T) {
			files := [4]string{table, table, table, table}
			files[i] = duplicated
			same, reason, err := SameTableChange(files[0], files[1], files[2], files[3])
			require.Error(t, err)
			assert.Contains(t, err.Error(), "read table t at the "+side+": column x is declared more than once")
			assert.False(t, same)
			assert.Empty(t, reason)
		})
	}
}

// A table file the pull request adds is absent at both bases, so its change
// is the CREATE TABLE itself. Reformatting the file or editing its comments
// keeps the approval; any change to what the table is built from does not.
func TestSameTableChange_AddedTable(t *testing.T) {
	const approved = "CREATE TABLE `overrides` (\n" +
		"  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
		"  `program_token` varchar(255) NOT NULL,\n" +
		"  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `idx_program_token` (`program_token`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	tests := []struct {
		name       string
		head       string
		wantSame   bool
		wantReason string
	}{
		{
			name: "a reformatted file with a comment added",
			head: "-- Per-program overrides.\nCREATE TABLE `overrides` (`id` bigint NOT NULL AUTO_INCREMENT, `program_token` varchar(255) NOT NULL, " +
				"`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`), KEY `idx_program_token` (`program_token`)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n",
			wantSame: true,
		},
		{
			name: "a column whose type changes at the head",
			head: "CREATE TABLE `overrides` (`id` bigint NOT NULL AUTO_INCREMENT, `program_token` varchar(255) NOT NULL, " +
				"`created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`), KEY `idx_program_token` (`program_token`)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: "but the head runs \"CREATE TABLE `overrides` (`id` BIGINT NOT NULL AUTO_INCREMENT,`program_token` VARCHAR(255) NOT NULL,`created_at` DATETIME",
		},
		{
			name: "a column the head adds",
			head: "CREATE TABLE `overrides` (`id` bigint NOT NULL AUTO_INCREMENT, `program_token` varchar(255) NOT NULL, " +
				"`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `note` text, PRIMARY KEY (`id`), KEY `idx_program_token` (`program_token`)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: "`note` TEXT",
		},
		{
			name: "an index the head drops",
			head: "CREATE TABLE `overrides` (`id` bigint NOT NULL AUTO_INCREMENT, `program_token` varchar(255) NOT NULL, " +
				"`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: "but the head runs \"CREATE TABLE `overrides` (`id` BIGINT NOT NULL AUTO_INCREMENT,`program_token` VARCHAR(255) NOT NULL,`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),PRIMARY KEY(`id`)) ENGINE",
		},
		{
			name: "a table option the head changes",
			head: "CREATE TABLE `overrides` (`id` bigint NOT NULL AUTO_INCREMENT, `program_token` varchar(255) NOT NULL, " +
				"`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`), KEY `idx_program_token` (`program_token`)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=COMPRESSED",
			wantReason: "ROW_FORMAT = COMPRESSED",
		},
		{
			name: "columns the head reorders",
			head: "CREATE TABLE `overrides` (`id` bigint NOT NULL AUTO_INCREMENT, `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
				"`program_token` varchar(255) NOT NULL, PRIMARY KEY (`id`), KEY `idx_program_token` (`program_token`)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: "but the head runs \"CREATE TABLE `overrides` (`id` BIGINT NOT NULL AUTO_INCREMENT,`created_at` TIMESTAMP",
		},
		{
			name: "an option Spirit's model does not carry that the head changes",
			head: "CREATE TABLE `overrides` (`id` bigint NOT NULL AUTO_INCREMENT, `program_token` varchar(255) NOT NULL, " +
				"`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`), KEY `idx_program_token` (`program_token`)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ENCRYPTION='Y'",
			wantReason: "ENCRYPTION = 'Y'",
		},
		{
			name:       "a head that names another table",
			head:       "CREATE TABLE `program_overrides` (`id` bigint NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: "but the head runs \"CREATE TABLE `program_overrides`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			same, reason, err := SameTableChange("", approved, "", tt.head)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, same)
			if tt.wantSame {
				assert.Empty(t, reason)
				return
			}
			assert.Contains(t, reason, "table overrides: the approved commit runs")
			assert.Contains(t, reason, tt.wantReason)
		})
	}
}

// MySQL gives foreign keys and CHECK constraints separate namespaces, so one
// of each may share a name, while two of the same kind may not.
func TestSameTableChange_ConstraintNamespaces(t *testing.T) {
	const (
		fk       = "CONSTRAINT `c` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`)"
		check    = "CONSTRAINT `c` CHECK (`parent_id` > 0)"
		otherFK  = "CONSTRAINT `C` FOREIGN KEY (`id`) REFERENCES `parent` (`id`)"
		otherChk = "CONSTRAINT `C` CHECK (`id` > 0)"
	)
	table := func(withX bool, constraints ...string) string {
		cols := "`id` bigint NOT NULL, `parent_id` bigint NOT NULL"
		if withX {
			cols += ", `x` int"
		}
		return "CREATE TABLE `t` (" + cols + ", PRIMARY KEY (`id`), " + strings.Join(constraints, ", ") + ")"
	}

	t.Run("a foreign key and a CHECK sharing a name", func(t *testing.T) {
		base, edited := table(false, fk, check), table(true, fk, check)
		same, reason, err := SameTableChange(base, edited, base, edited)
		require.NoError(t, err)
		assert.True(t, same)
		assert.Empty(t, reason)
	})

	tests := []struct {
		name        string
		constraints []string
		wantErr     string
	}{
		{name: "two foreign keys sharing a name", constraints: []string{fk, otherFK}, wantErr: "read table t at the head: foreign key c is declared more than once"},
		{name: "two CHECKs sharing a name", constraints: []string{check, otherChk}, wantErr: "read table t at the head: check constraint c is declared more than once"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := table(false, fk)
			same, reason, err := SameTableChange(base, table(true, fk), base, table(true, tt.constraints...))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.False(t, same)
			assert.Empty(t, reason)
		})
	}
}

// A file absent at only one base has no single change to compare, and one
// that cannot be read as one CREATE TABLE proves nothing; both are errors the
// caller counts as changed.
func TestSameTableChange_AddedTableCannotCompare(t *testing.T) {
	table := "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	tests := []struct {
		name                                   string
		approvedBase, approved, headBase, head string
		wantErr                                string
	}{
		{name: "absent at the head's base only", approvedBase: table, approved: table, head: table, wantErr: "the file is absent at one base but not the other"},
		{name: "absent at the approved commit's base only", approved: table, headBase: table, head: table, wantErr: "the file is absent at one base but not the other"},
		{name: "two statements at the head", approved: table, head: table + ";\nCREATE TABLE `u` (`id` bigint NOT NULL, PRIMARY KEY (`id`));", wantErr: "parse the schema file at the head: the file holds 2 statements"},
		{name: "a column declared twice at the approved commit", approved: "CREATE TABLE `t` (`id` bigint NOT NULL, `x` int, `X` int, PRIMARY KEY (`id`))", head: table, wantErr: "read table t at the approved commit: column x is declared more than once"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			same, reason, err := SameTableChange(tt.approvedBase, tt.approved, tt.headBase, tt.head)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.False(t, same)
			assert.Empty(t, reason)
		})
	}
}

// A head on the same base as the approved commit runs every clause the
// reviewer approved or loses the approval: dropping half of an approved
// redefinition runs a statement nobody reviewed on its own. On a base that
// already made part of the change, the head may skip that part, but never by
// touching the same element itself.
func TestSameTableChange_HeadDropsPartOfApprovedChange(t *testing.T) {
	const (
		base = "CREATE TABLE `t` (`id` bigint NOT NULL, `a` int, `b` int, `email` varchar(255), PRIMARY KEY (`id`), KEY `idx_ab` (`a`)) ENGINE=InnoDB"
		// The approved commit widens idx_ab to (a, b), which Spirit runs as a
		// DROP INDEX and an ADD INDEX of the same name.
		widened = "CREATE TABLE `t` (`id` bigint NOT NULL, `a` int, `b` int, `email` varchar(255), PRIMARY KEY (`id`), KEY `idx_ab` (`a`,`b`)) ENGINE=InnoDB"
		noIndex = "CREATE TABLE `t` (`id` bigint NOT NULL, `a` int, `b` int, `email` varchar(255), PRIMARY KEY (`id`)) ENGINE=InnoDB"
		// The approved commit replaces email with email_v2.
		replaced = "CREATE TABLE `t` (`id` bigint NOT NULL, `a` int, `b` int, `email_v2` varchar(255), PRIMARY KEY (`id`), KEY `idx_ab` (`a`)) ENGINE=InnoDB"
		dropped  = "CREATE TABLE `t` (`id` bigint NOT NULL, `a` int, `b` int, PRIMARY KEY (`id`), KEY `idx_ab` (`a`)) ENGINE=InnoDB"
		// A base that has since added an unrelated column.
		movedBase    = "CREATE TABLE `t` (`id` bigint NOT NULL, `a` int, `b` int, `email` varchar(255), `c` int, PRIMARY KEY (`id`), KEY `idx_ab` (`a`)) ENGINE=InnoDB"
		movedNoIndex = "CREATE TABLE `t` (`id` bigint NOT NULL, `a` int, `b` int, `email` varchar(255), `c` int, PRIMARY KEY (`id`)) ENGINE=InnoDB"
	)
	tests := []struct {
		name                                   string
		approvedBase, approved, headBase, head string
		wantSame                               bool
	}{
		{name: "the head deletes the index on the same base", approvedBase: base, approved: widened, headBase: base, head: noIndex},
		{name: "the head deletes the replacement column on the same base", approvedBase: base, approved: replaced, headBase: base, head: dropped},
		{name: "the head deletes the index on a base that moved elsewhere", approvedBase: base, approved: widened, headBase: movedBase, head: movedNoIndex},
		{name: "the head deletes the index on a base that already widened it", approvedBase: base, approved: widened, headBase: widened, head: noIndex},
		{name: "the head runs the approved change on the same base", approvedBase: base, approved: widened, headBase: base, head: widened, wantSame: true},
		{name: "the head runs nothing on a base that already widened the index", approvedBase: base, approved: widened, headBase: widened, head: widened, wantSame: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			same, reason, err := SameTableChange(tt.approvedBase, tt.approved, tt.headBase, tt.head)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, same, reason)
		})
	}
}

// The DDL is what the planner runs. On a file both commits edit, it is the
// diff, so a clause the diff does not act on runs on neither side. On a file
// the pull request adds, it is the CREATE TABLE as written, so every clause
// counts, including a LIKE source or an AS SELECT query.
func TestSameTableChange_ClausesOutsideTheDiff(t *testing.T) {
	const table = "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB"
	tests := []struct {
		name                                   string
		approvedBase, approved, headBase, head string
		wantSame                               bool
		wantReason                             string
	}{
		{
			name:         "an ENCRYPTION option the head changes on a file both sides edited",
			approvedBase: table, headBase: table,
			approved: "CREATE TABLE `t` (`id` bigint NOT NULL, `x` int, PRIMARY KEY (`id`)) ENGINE=InnoDB ENCRYPTION='Y'",
			head:     "CREATE TABLE `t` (`id` bigint NOT NULL, `x` int, PRIMARY KEY (`id`)) ENGINE=InnoDB ENCRYPTION='N'",
			wantSame: true,
		},
		{
			name:         "a COMPRESSION option the base set and the head drops",
			approvedBase: table, approved: "CREATE TABLE `t` (`id` bigint NOT NULL, `x` int, PRIMARY KEY (`id`)) ENGINE=InnoDB",
			headBase: "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB COMPRESSION='zlib'",
			head:     "CREATE TABLE `t` (`id` bigint NOT NULL, `x` int, PRIMARY KEY (`id`)) ENGINE=InnoDB",
			wantSame: true,
		},
		{
			name:       "an ENCRYPTION option the head changes on a file the PR adds",
			approved:   "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`)) ENCRYPTION='Y'",
			head:       "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`)) ENCRYPTION='N'",
			wantReason: "table t: the approved commit runs \"CREATE TABLE `t` (`id` BIGINT NOT NULL,PRIMARY KEY(`id`)) ENCRYPTION = 'Y'\" but the head runs \"CREATE TABLE `t` (`id` BIGINT NOT NULL,PRIMARY KEY(`id`)) ENCRYPTION = 'N'\"",
		},
		{
			name:       "a LIKE source the head changes on a file the PR adds",
			approved:   "CREATE TABLE `t` LIKE `source_a`",
			head:       "CREATE TABLE `t` LIKE `source_b`",
			wantReason: "table t: the approved commit runs \"CREATE TABLE `t` LIKE `source_a`\" but the head runs \"CREATE TABLE `t` LIKE `source_b`\"",
		},
		{
			name:       "an AS SELECT query the head changes on a file the PR adds",
			approved:   "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`)) AS SELECT `id` FROM `source` WHERE `id` > 1",
			head:       "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`)) AS SELECT `id` FROM `source` WHERE `id` > 2",
			wantReason: "table t: the approved commit runs \"CREATE TABLE `t` (`id` BIGINT NOT NULL,PRIMARY KEY(`id`)) AS SELECT `id` FROM `source` WHERE `id`>1\" but the head runs \"CREATE TABLE `t` (`id` BIGINT NOT NULL,PRIMARY KEY(`id`)) AS SELECT `id` FROM `source` WHERE `id`>2\"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			same, reason, err := SameTableChange(tt.approvedBase, tt.approved, tt.headBase, tt.head)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, same)
			assert.Equal(t, tt.wantReason, reason)
		})
	}
}

// The head's statements are covered only when each clause they run is one
// the approved statements run, in the same order, and each approved clause
// they skip is one the head's base already ran.
func TestClausesCovered(t *testing.T) {
	const (
		addX = "ALTER TABLE `t` ADD COLUMN `x` int NULL DEFAULT NULL AFTER `a`"
		addY = "ALTER TABLE `t` ADD COLUMN `y` int NULL DEFAULT NULL AFTER `x`"
		both = "ALTER TABLE `t` ADD COLUMN `x` int NULL DEFAULT NULL AFTER `a`, ADD COLUMN `y` int NULL DEFAULT NULL AFTER `x`"
		addZ = "ALTER TABLE `t` ADD COLUMN `z` int NULL DEFAULT NULL"
	)
	tests := []struct {
		name                     string
		approved, head, baseMove []string
		want                     bool
	}{
		{name: "the same statement", approved: []string{both}, head: []string{both}, want: true},
		{name: "one approved clause, the other already on the head's base", approved: []string{both}, head: []string{addY}, baseMove: []string{addX}, want: true},
		{name: "one approved clause on the same base", approved: []string{both}, head: []string{addY}},
		{name: "one approved clause, the base moved by another clause", approved: []string{both}, head: []string{addY}, baseMove: []string{addZ}},
		{name: "one approved clause, the other column already on the head's base with another definition", approved: []string{both}, head: []string{addY},
			baseMove: []string{"ALTER TABLE `t` ADD COLUMN `x` bigint NULL DEFAULT NULL"}, want: true},
		{name: "no statement at the head, all of it already on the head's base", approved: []string{both}, baseMove: []string{both}, want: true},
		{name: "no statement at the head on the same base", approved: []string{both}},
		{name: "a clause nobody approved", approved: []string{addX}, head: []string{both}},
		{name: "approved clauses in another order", approved: []string{both},
			head: []string{"ALTER TABLE `t` ADD COLUMN `y` int NULL DEFAULT NULL AFTER `x`, ADD COLUMN `x` int NULL DEFAULT NULL AFTER `a`"}},
		{name: "a clause the approved statement runs with another definition", approved: []string{both},
			head: []string{"ALTER TABLE `t` ADD COLUMN `y` bigint NULL DEFAULT NULL AFTER `x`"}, baseMove: []string{addX}},
		{name: "two statements regrouped into one", approved: []string{addX, addY}, head: []string{both}},
		{name: "the same two statements", approved: []string{addX, addY}, head: []string{addX, addY}, want: true},
		{name: "no statement at the head, two approved statements already on the head's base", approved: []string{addX, addY}, baseMove: []string{both}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := clausesCovered(tt.approved, tt.head, tt.baseMove)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// The DDL shown for a changed schema file is the statement the review gate
// compares: the strict diff from the file's base, the CREATE TABLE of an
// added file, or a DROP TABLE for a removed one.
func TestTableChangeStatements(t *testing.T) {
	const (
		base = "CREATE TABLE `shipments` (\n" +
			"  `id` bigint NOT NULL,\n" +
			"  `label` binary(32) NOT NULL,\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
		commented = "CREATE TABLE `shipments` (\n" +
			"  `id` bigint NOT NULL,\n" +
			"  `label` binary(32) DEFAULT NULL COMMENT 'NULL until printed',\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
		uncommented = "CREATE TABLE `shipments` (\n" +
			"  `id` bigint NOT NULL,\n" +
			"  `label` binary(32) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	)
	for _, tc := range []struct {
		name       string
		base, file string
		want       []string
	}{
		{
			name: "modified column with a comment",
			base: base, file: commented,
			want: []string{"ALTER TABLE `shipments` MODIFY COLUMN `label` binary(32) NULL DEFAULT NULL COMMENT 'NULL until printed'"},
		},
		{
			name: "modified column without the comment",
			base: base, file: uncommented,
			want: []string{"ALTER TABLE `shipments` MODIFY COLUMN `label` binary(32) NULL DEFAULT NULL"},
		},
		{
			name: "unchanged file",
			base: base, file: base,
			want: []string{},
		},
		{
			name: "added file",
			file: base,
			want: []string{"CREATE TABLE `shipments` (`id` BIGINT NOT NULL,`label` BINARY(32) NOT NULL,PRIMARY KEY(`id`)) ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_BIN"},
		},
		{
			name: "removed file",
			base: base,
			want: []string{"DROP TABLE `shipments`"},
		},
		{
			name: "absent at both",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TableChangeStatements(tc.base, tc.file)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// A file that does not hold exactly one CREATE TABLE has no DDL to show.
func TestTableChangeStatements_CannotParse(t *testing.T) {
	const table = "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	for _, tc := range []struct{ name, base, file string }{
		{name: "two statements in the file", base: table, file: table + ";\n" + table},
		{name: "not a CREATE TABLE at the base", base: "ALTER TABLE `t` ADD COLUMN `x` int", file: table},
		{name: "added file that does not parse", file: "CREATE TABLE"},
		{name: "removed file that does not parse", base: "CREATE TABLE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := TableChangeStatements(tc.base, tc.file)
			require.Error(t, err)
		})
	}
}
