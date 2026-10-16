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
			wantReason: []string{"table variants: the head declares column sku differently from the approved commit"},
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
			wantReason: []string{"table variants: the base changed column bounty_currency since the approved commit, and the head replaces it"},
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
			wantReason: []string{"table variants", "both the approved commit and the base changed column amount"},
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
			wantReason: []string{"table variants", "both the approved commit and the base changed index u_token"},
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
			wantReason: []string{"table variants", "the base changed column amount since the approved commit, and the head replaces it"},
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
			wantReason:   []string{"table variants", "the base changed table option stats_persistent"},
		},
		{
			// The statements match, since the diff does not compare the
			// TEMPORARY modifier, but the head declares a different table.
			name:         "a head that makes the table TEMPORARY",
			approvedBase: base,
			approved: "CREATE TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			headBase: base,
			head: "CREATE TEMPORARY TABLE `variants` (`id` bigint NOT NULL AUTO_INCREMENT, `token` varchar(255) NOT NULL, `amount` int DEFAULT NULL, " +
				"`sku` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `u_token` (`token`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: []string{"table variants", "the head declares the TEMPORARY modifier differently from the approved commit"},
		},
		{
			// Both commits generate MODIFY COLUMN `b` ... FIRST, but the
			// head also puts `b` back before `a`, undoing the base's order.
			name:         "a head that undoes the base's column order",
			approvedBase: "CREATE TABLE `t` (`id` int NOT NULL, `b` int, `a` int, PRIMARY KEY (`id`))",
			approved:     "CREATE TABLE `t` (`b` int, `id` int NOT NULL, `a` int, PRIMARY KEY (`id`))",
			headBase:     "CREATE TABLE `t` (`id` int NOT NULL, `a` int, `b` int, PRIMARY KEY (`id`))",
			head:         "CREATE TABLE `t` (`b` int, `id` int NOT NULL, `a` int, PRIMARY KEY (`id`))",
			wantReason:   []string{"table t: the base reordered columns since the approved commit"},
		},
		{
			name:         "a head that orders the PR's columns differently",
			approvedBase: "CREATE TABLE `t` (`id` int NOT NULL, PRIMARY KEY (`id`))",
			approved:     "CREATE TABLE `t` (`id` int NOT NULL, `x` int, `y` int, PRIMARY KEY (`id`))",
			headBase:     "CREATE TABLE `t` (`id` int NOT NULL, PRIMARY KEY (`id`))",
			head:         "CREATE TABLE `t` (`id` int NOT NULL, `y` int, `x` int, PRIMARY KEY (`id`))",
			wantReason:   []string{"table t: the head orders columns differently from the approved commit"},
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
			wantReason: "table overrides: the head declares column created_at differently from the approved commit",
		},
		{
			name: "a column the head adds",
			head: "CREATE TABLE `overrides` (`id` bigint NOT NULL AUTO_INCREMENT, `program_token` varchar(255) NOT NULL, " +
				"`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `note` text, PRIMARY KEY (`id`), KEY `idx_program_token` (`program_token`)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: "table overrides: the head declares column note differently from the approved commit",
		},
		{
			name: "an index the head drops",
			head: "CREATE TABLE `overrides` (`id` bigint NOT NULL AUTO_INCREMENT, `program_token` varchar(255) NOT NULL, " +
				"`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: "table overrides: the head declares index idx_program_token differently from the approved commit",
		},
		{
			name: "a table option the head changes",
			head: "CREATE TABLE `overrides` (`id` bigint NOT NULL AUTO_INCREMENT, `program_token` varchar(255) NOT NULL, " +
				"`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`), KEY `idx_program_token` (`program_token`)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=COMPRESSED",
			wantReason: "table overrides: the head declares table option",
		},
		{
			name: "columns the head reorders",
			head: "CREATE TABLE `overrides` (`id` bigint NOT NULL AUTO_INCREMENT, `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
				"`program_token` varchar(255) NOT NULL, PRIMARY KEY (`id`), KEY `idx_program_token` (`program_token`)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: "table overrides: the head orders columns differently from the approved commit",
		},
		{
			name:       "a head that names another table",
			head:       "CREATE TABLE `program_overrides` (`id` bigint NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			wantReason: "the file declares table overrides at the approved commit but program_overrides at the head",
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
			assert.Contains(t, reason, tt.wantReason)
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
		{name: "absent at the approved commit's base only", approved: table, headBase: table, head: table, wantErr: "absent at one base but not the other"},
		{name: "absent at the head's base only", approvedBase: table, approved: table, head: table, wantErr: "absent at one base but not the other"},
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
