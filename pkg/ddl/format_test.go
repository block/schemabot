package ddl

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

func TestFormatSchemaFileForDialect(t *testing.T) {
	tests := []struct {
		name     string
		dialect  schema.Dialect
		input    string
		expected string
	}{
		{
			name:     "single-column MySQL table",
			dialect:  schema.DialectMySQL,
			input:    "CREATE TABLE `users` (`id` BIGINT NOT NULL)",
			expected: "CREATE TABLE `users` (\n    `id` bigint NOT NULL\n);\n",
		},
		{
			name:     "quoted MySQL content",
			dialect:  schema.DialectMySQL,
			input:    "CREATE TABLE `events` (`id` INT, `note` VARCHAR(64) DEFAULT 'Keep INT, comma')",
			expected: "CREATE TABLE `events` (\n    `id` int,\n    `note` varchar(64) DEFAULT 'Keep INT, comma'\n);\n",
		},
		{
			name:     "single-column PostgreSQL table",
			dialect:  schema.DialectPostgres,
			input:    "CREATE TABLE users (id bigint PRIMARY KEY)",
			expected: "CREATE TABLE users (\n    id bigint PRIMARY KEY\n);\n",
		},
		{
			name:     "existing multiline SQL is preserved",
			dialect:  schema.DialectPostgres,
			input:    "CREATE TABLE \"Order\" (\n  \"id\" bigint PRIMARY KEY\n);\nCREATE INDEX \"idx_id\" ON \"Order\" (\"id\");\n\n",
			expected: "CREATE TABLE \"Order\" (\n  \"id\" bigint PRIMARY KEY\n);\nCREATE INDEX \"idx_id\" ON \"Order\" (\"id\");\n",
		},
		{
			name:     "PostgreSQL list partition keeps one bound value per line",
			dialect:  schema.DialectPostgres,
			input:    "CREATE TABLE orders_pending PARTITION OF orders FOR VALUES IN ('PENDING','HELD')",
			expected: "CREATE TABLE orders_pending PARTITION OF orders FOR VALUES IN (\n    'PENDING',\n    'HELD'\n);\n",
		},
		{
			name:    "PostgreSQL table and indexes",
			dialect: schema.DialectPostgres,
			input:   "CREATE TABLE users (id bigint PRIMARY KEY); CREATE INDEX users_id_idx ON users (id)",
			expected: "CREATE TABLE users (\n    id bigint PRIMARY KEY\n);\n\n" +
				"CREATE INDEX users_id_idx ON users USING btree (id);\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FormatSchemaFileForDialect(tt.dialect, tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
			assert.Contains(t, strings.TrimSuffix(got, "\n"), "\n")

			parser, err := ParserForDialect(tt.dialect)
			require.NoError(t, err)
			before, err := ParseCreateSet(parser, tt.input)
			require.NoError(t, err)
			after, err := ParseCreateSet(parser, got)
			require.NoError(t, err)
			require.Len(t, after.Statements, len(before.Statements))
			for i := range before.Statements {
				assert.Equal(t, parser.Canonicalize(before.Statements[i]), parser.Canonicalize(after.Statements[i]))
			}
		})
	}
}

func TestFormatSchemaFileForDialectRejectsNonTableContent(t *testing.T) {
	_, err := FormatSchemaFileForDialect(schema.DialectMySQL, "ALTER TABLE users ADD COLUMN email text")
	require.ErrorContains(t, err, "must start with CREATE TABLE")

	_, err = FormatSchemaFileForDialect(schema.DialectMySQL, "CREATE TABLE")
	require.ErrorContains(t, err, "parse declarative schema file")
}

func TestFormatSchemaFileForDialectRequiresMultilineTable(t *testing.T) {
	// UNLOGGED tables pass desired-schema admission but have no multiline
	// layout. An index separator cannot satisfy the table's layout contract.
	for _, suffix := range []string{"", "; CREATE INDEX t_id ON t (id)"} {
		t.Run(suffix, func(t *testing.T) {
			got, err := FormatSchemaFileForDialect(schema.DialectPostgres, "CREATE UNLOGGED TABLE t (id bigint)"+suffix)
			require.EqualError(t, err, "formatter produced a single-line CREATE TABLE")
			assert.Empty(t, got)
		})
	}
}

func TestFormatSchemaFileForDialectPostgresRequiresTable(t *testing.T) {
	for _, input := range []string{
		"CREATE INDEX t_id ON t (id)",
		"ALTER TABLE t ENABLE ROW LEVEL SECURITY; CREATE POLICY readers ON t USING (true)",
	} {
		t.Run(input, func(t *testing.T) {
			got, err := FormatSchemaFileForDialect(schema.DialectPostgres, input)
			require.ErrorContains(t, err, "parse declarative schema file")
			require.ErrorContains(t, err, "CREATE TABLE")
			assert.Empty(t, got)
		})
	}
}

func TestFormatSchemaFileForDialectPostgresOrdersTableFirst(t *testing.T) {
	for _, suffix := range []string{"", "; ALTER TABLE t ENABLE ROW LEVEL SECURITY; CREATE POLICY readers ON t USING (true)"} {
		t.Run(suffix, func(t *testing.T) {
			input := "CREATE INDEX t_id ON t (id); CREATE TABLE t (id bigint)" + suffix
			got, err := FormatSchemaFileForDialect(schema.DialectPostgres, input)
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(got, "CREATE TABLE t (\n    id bigint\n);\n\nCREATE INDEX t_id ON t USING btree (id);\n"), got)
			before, err := postgresSchemaFileStatements(input)
			require.NoError(t, err)
			after, err := postgresSchemaFileStatements(got)
			require.NoError(t, err)
			assert.Equal(t, before, after)
		})
	}
}

func TestFormatSchemaFileForDialectRefusesCommentLoss(t *testing.T) {
	for _, input := range []string{
		"CREATE TABLE t (id int /* keep me */)",
		"CREATE TABLE t (id int) /* keep me */",
		"CREATE TABLE t (id int); -- keep me",
		"CREATE TABLE t (id int); # keep me",
		"CREATE TABLE t (id int); --",
		"CREATE TABLE t (id int); --\tkeep me",
		"CREATE TABLE t (id int); --\xA0keep me",
		"CREATE TABLE t (id int) /*!50100 PARTITION BY HASH (id) PARTITIONS 4 */",
		"CREATE TABLE t (id int) /*+ keep me */",
		`CREATE TABLE t (note varchar(64) DEFAULT 'escaped\' quote') /* keep me */`,
	} {
		t.Run(input, func(t *testing.T) {
			got, err := FormatSchemaFileForDialect(schema.DialectMySQL, input)
			require.ErrorContains(t, err, "comments")
			assert.Empty(t, got)
		})
	}

	got, err := FormatSchemaFileForDialect(schema.DialectPostgres, "CREATE TABLE t (id bigint PRIMARY KEY /* keep me */)")
	require.ErrorContains(t, err, "format PostgreSQL declarative schema file")
	assert.Empty(t, got)
}

func TestFormatSchemaFileForDialectPreservesMySQLQuotedCommentMarkers(t *testing.T) {
	for _, input := range []string{
		"CREATE TABLE `/* -- # */` (`id` int) COMMENT='/* -- # */'",
		`CREATE TABLE t (note varchar(64) DEFAULT 'it''s /* -- # */')`,
		`CREATE TABLE t (note varchar(64) DEFAULT 'it\'s /* -- # */')`,
		`CREATE TABLE t (note varchar(64) DEFAULT "it's /* -- # */")`,
		"CREATE TABLE `a``/* -- # */` (`id` int)",
		"CREATE TABLE t (id int DEFAULT (1--2))",
		`CREATE TABLE t (note varchar(64) DEFAULT 'path\\') COMMENT='/* keep me */'`,
		"CREATE TABLE t (id int) COMMENT='/* TableOptionStatsPersistent is not supported */'",
	} {
		t.Run(input, func(t *testing.T) {
			got, err := FormatSchemaFileForDialect(schema.DialectMySQL, input)
			require.NoError(t, err)
			assert.Contains(t, strings.TrimSuffix(got, "\n"), "\n")
			assert.Equal(t, Canonicalize(input), Canonicalize(got))
		})
	}
}

func TestFormatSchemaFileForDialectPreservesMySQLTableOptions(t *testing.T) {
	for _, option := range []string{
		"ENCRYPTION = 'Y'",
		"MAX_ROWS = 1000",
		"STATS_SAMPLE_PAGES = 32",
		"SECONDARY_ENGINE = NULL",
		"CHECKSUM = 1",
		"DELAY_KEY_WRITE = 1",
		"MIN_ROWS = 10",
		"STATS_AUTO_RECALC = 0",
		"STATS_PERSISTENT = 0",
		"STATS_PERSISTENT = 1",
		"PACK_KEYS = 0",
		"PACK_KEYS = 1",
	} {
		t.Run(option, func(t *testing.T) {
			input := "CREATE TABLE t (id int) ENGINE=InnoDB " + option + " COMMENT='Keep INT, comma' PARTITION BY HASH (id) PARTITIONS 4"
			got, err := FormatSchemaFileForDialect(schema.DialectMySQL, input)
			require.NoError(t, err)
			assert.Contains(t, got, "\n    `id` int\n)")
			assert.Contains(t, got, option)
			assert.Contains(t, got, "COMMENT = 'Keep INT, comma'")
			assert.Contains(t, got, "PARTITION BY HASH")
			assert.Equal(t, Canonicalize(input), Canonicalize(got))
		})
	}
}

// A canonical form in which Restore wrote a placeholder comment instead of an
// option's value is flagged, while comment openers inside quoted content are not.
func TestContainsMySQLCommentFlagsRestorePlaceholders(t *testing.T) {
	assert.True(t, containsMySQLComment("CREATE TABLE `t` (`id` INT) ENGINE = InnoDB /* TableOptionStatsPersistent is not supported */ ", false))
	assert.False(t, containsMySQLComment("CREATE TABLE `t` (`id` INT) COMMENT = '/* TableOptionStatsPersistent is not supported */'", false))
	assert.False(t, containsMySQLComment("CREATE TABLE `/* -- # */` (`id` INT)", false))
}

func TestFormatSchemaFileForDialectPreservesMultilineMySQL(t *testing.T) {
	input := "CREATE TABLE t (\n  id int /* keep me */\n) ENGINE=InnoDB STATS_PERSISTENT=0 PACK_KEYS=1\n/*!50100 PARTITION BY HASH (id) PARTITIONS 4 */;\n"
	got, err := FormatSchemaFileForDialect(schema.DialectMySQL, input)
	require.NoError(t, err)
	assert.Equal(t, input, got)
}

func TestFormatSchemaFileForDialectRefusesUnformattableTable(t *testing.T) {
	got, err := FormatSchemaFileForDialect(schema.DialectMySQL, "CREATE TABLE t LIKE other_table")
	require.Error(t, err)
	assert.Empty(t, got)
}

func TestFormatSchemaFileForDialectPreservesPostgresRowSecurity(t *testing.T) {
	input := "CREATE TABLE documents (\n  id bigint PRIMARY KEY\n);\nALTER TABLE documents ENABLE ROW LEVEL SECURITY;\nCREATE POLICY readers ON documents FOR SELECT USING (id = 1);\nCOMMENT ON POLICY readers ON documents IS 'Read your documents';\n\n"

	got, err := FormatSchemaFileForDialect(schema.DialectPostgres, input)
	require.NoError(t, err)
	assert.Equal(t, strings.TrimRight(input, "\r\n")+"\n", got)
}

func TestFormatSchemaFileForDialectFormatsSingleLinePostgresRowSecurity(t *testing.T) {
	input := "CREATE TABLE documents (id bigint PRIMARY KEY); ALTER TABLE documents ENABLE ROW LEVEL SECURITY; CREATE POLICY readers ON documents FOR SELECT USING (id = 1); COMMENT ON POLICY readers ON documents IS 'Read your documents'"

	got, err := FormatSchemaFileForDialect(schema.DialectPostgres, input)
	require.NoError(t, err)
	assert.Contains(t, strings.TrimSuffix(got, "\n"), "\n")
	assert.Contains(t, got, "ALTER TABLE documents ENABLE ROW LEVEL SECURITY;")
	assert.Contains(t, got, "CREATE POLICY readers ON documents")
	assert.Contains(t, got, "COMMENT ON POLICY readers ON documents")

	before, err := postgresSchemaFileStatements(input)
	require.NoError(t, err)
	after, err := postgresSchemaFileStatements(got)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

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
			name: "table options each on their own line",
			input: "ALTER TABLE `products` MODIFY COLUMN `sku` VARCHAR(64) COLLATE utf8mb4_0900_ai_ci NOT NULL, " +
				"DEFAULT CHARSET = utf8mb4, COLLATE = utf8mb4_0900_ai_ci",
			expected: "ALTER TABLE `products`\n" +
				"    MODIFY COLUMN `sku` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL,\n" +
				"    DEFAULT CHARACTER SET = utf8mb4,\n" +
				"    DEFAULT COLLATE = utf8mb4_0900_ai_ci;",
		},
		{
			name:  "engine and auto-increment options on their own lines",
			input: "ALTER TABLE `t` ADD COLUMN `a` INT, ENGINE = InnoDB, AUTO_INCREMENT = 10",
			expected: "ALTER TABLE `t`\n" +
				"    ADD COLUMN `a` int,\n" +
				"    ENGINE = InnoDB,\n" +
				"    AUTO_INCREMENT = 10;",
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
			name:  "CREATE TABLE comma in default literal",
			input: "CREATE TABLE t (id int, note varchar(10) DEFAULT 'x, y')",
			expected: "CREATE TABLE `t` (\n" +
				"    `id` int,\n" +
				"    `note` varchar(10) DEFAULT 'x, y'\n" +
				");",
		},
		{
			// The MySQL parser resolves the \b escape to a backspace once; the
			// canonical form carries the character itself and round-trips, so
			// the statement formats with the comma still inside the literal.
			name:  "backslash escape in default literal is resolved once and the statement formats",
			input: "CREATE TABLE t (id int, note varchar(10) DEFAULT 'a\\b, c')",
			expected: "CREATE TABLE `t` (\n" +
				"    `id` int,\n" +
				"    `note` varchar(10) DEFAULT 'a\b, c'\n" +
				");",
		},
		{
			// A literal holding a backslash character keeps its escape in the
			// canonical form, so the parser reads the same value back and the
			// statement formats with the comma still inside the literal.
			name:  "literal containing a backslash character keeps its escape and formats",
			input: "CREATE TABLE t (id int, note varchar(10) DEFAULT 'a\\\\b, c')",
			expected: "CREATE TABLE `t` (\n" +
				"    `id` int,\n" +
				"    `note` varchar(10) DEFAULT 'a\\\\b, c'\n" +
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
		{
			name: "CREATE TABLE with options and PARTITION BY LIST",
			input: "CREATE TABLE `t` (`id` bigint NOT NULL AUTO_INCREMENT, `run_partition_id` bigint NOT NULL, " +
				"PRIMARY KEY (`run_partition_id`,`id`), KEY `idx_id` (`id`)) " +
				"ENGINE = InnoDB, CHARSET utf8mb4, COLLATE utf8mb4_0900_ai_ci " +
				"PARTITION BY LIST (`run_partition_id`) (PARTITION p_seed VALUES IN (0))",
			expected: "CREATE TABLE `t` (\n" +
				"    `id` bigint NOT NULL AUTO_INCREMENT,\n" +
				"    `run_partition_id` bigint NOT NULL,\n" +
				"    PRIMARY KEY(`run_partition_id`, `id`),\n" +
				"    INDEX `idx_id`(`id`)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET utf8mb4,\n" +
				"  COLLATE utf8mb4_0900_ai_ci\n" +
				"  PARTITION BY LIST (`run_partition_id`) (PARTITION `p_seed` VALUES IN (0));",
		},
		{
			name:  "CREATE TABLE with PARTITION BY HASH and no other options",
			input: "CREATE TABLE `t2` (`id` bigint NOT NULL, `k` bigint NOT NULL) PARTITION BY HASH (`k`) PARTITIONS 8",
			expected: "CREATE TABLE `t2` (\n" +
				"    `id` bigint NOT NULL,\n" +
				"    `k` bigint NOT NULL\n" +
				") PARTITION BY HASH (`k`) PARTITIONS 8;",
		},
		{
			name:  "single column CREATE TABLE with PARTITION BY",
			input: "CREATE TABLE `t3` (`id` BIGINT NOT NULL) ENGINE=InnoDB PARTITION BY HASH (`id`) PARTITIONS 4",
			expected: "CREATE TABLE `t3` (`id` bigint NOT NULL) ENGINE InnoDB\n" +
				"  PARTITION BY HASH (`id`) PARTITIONS 4;",
		},
		{
			name:     "COMMENT containing PARTITION BY is not split",
			input:    "CREATE TABLE `t4` (`id` BIGINT NOT NULL) ENGINE=InnoDB COMMENT='do not PARTITION BY hand'",
			expected: "CREATE TABLE `t4` (`id` bigint NOT NULL) ENGINE InnoDB,\n  COMMENT 'do not PARTITION BY hand';",
		},
		{
			name:  "COMMENT with doubled quote and PARTITION BY before the clause",
			input: "CREATE TABLE `t4` (`id` BIGINT NOT NULL) ENGINE=InnoDB COMMENT='don''t PARTITION BY hand' PARTITION BY HASH (`id`) PARTITIONS 2",
			expected: "CREATE TABLE `t4` (`id` bigint NOT NULL) ENGINE InnoDB,\n" +
				"  COMMENT 'don''t PARTITION BY hand'\n" +
				"  PARTITION BY HASH (`id`) PARTITIONS 2;",
		},
		{
			name:  "type words inside a column COMMENT literal keep their case",
			input: "CREATE TABLE t (id INT, note TEXT COMMENT 'stored as INT, not TEXT')",
			expected: "CREATE TABLE `t` (\n" +
				"    `id` int,\n" +
				"    `note` text COMMENT 'stored as INT, not TEXT'\n" +
				");",
		},
		{
			name:  "backticked identifiers that spell a type keep their case",
			input: "CREATE TABLE `DATE` (`INT` INT, `TEXT` TEXT DEFAULT NULL)",
			expected: "CREATE TABLE `DATE` (\n" +
				"    `INT` int,\n" +
				"    `TEXT` text DEFAULT NULL\n" +
				");",
		},
		{
			name:     "table COMMENT with doubled quote and a type word",
			input:    "CREATE TABLE `t` (`id` BIGINT NOT NULL) ENGINE=InnoDB COMMENT='it''s a VARCHAR(255) thing'",
			expected: "CREATE TABLE `t` (`id` bigint NOT NULL) ENGINE InnoDB,\n  COMMENT 'it''s a VARCHAR(255) thing';",
		},
		{
			name:  "ALTER clauses with type words in identifier and literal",
			input: "ALTER TABLE `t` ADD COLUMN `INT` INT COMMENT 'was a BIGINT, then TEXT', ADD COLUMN `b` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()",
			expected: "ALTER TABLE `t`\n" +
				"    ADD COLUMN `INT` int COMMENT 'was a BIGINT, then TEXT',\n" +
				"    ADD COLUMN `b` timestamp DEFAULT current_timestamp();",
		},
		{
			name:  "non-ASCII COMMENT before PARTITION BY",
			input: "CREATE TABLE `t5` (`id` BIGINT NOT NULL) ENGINE=InnoDB COMMENT='ılık ıslak' PARTITION BY HASH (`id`) PARTITIONS 2",
			expected: "CREATE TABLE `t5` (`id` bigint NOT NULL) ENGINE InnoDB,\n" +
				"  COMMENT 'ılık ıslak'\n" +
				"  PARTITION BY HASH (`id`) PARTITIONS 2;",
		},
		{
			name: "PARTITION BY RANGE expression with one definition per line",
			input: "CREATE TABLE `events` (`id` bigint NOT NULL AUTO_INCREMENT, `created_at` datetime(3) NOT NULL, " +
				"PRIMARY KEY (`created_at`,`id`), KEY `idx_id` (`id`)) " +
				"ENGINE = InnoDB, CHARSET utf8mb4 " +
				"PARTITION BY RANGE (TO_DAYS(`created_at`)) (" +
				"PARTITION p2026_01 VALUES LESS THAN (TO_DAYS('2026-02-01'))," +
				"PARTITION p2026_02 VALUES LESS THAN (TO_DAYS('2026-03-01'))," +
				"PARTITION pmax VALUES LESS THAN (MAXVALUE))",
			expected: "CREATE TABLE `events` (\n" +
				"    `id` bigint NOT NULL AUTO_INCREMENT,\n" +
				"    `created_at` datetime(3) NOT NULL,\n" +
				"    PRIMARY KEY(`created_at`, `id`),\n" +
				"    INDEX `idx_id`(`id`)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET utf8mb4\n" +
				"  PARTITION BY RANGE (TO_DAYS(`created_at`)) (\n" +
				"      PARTITION `p2026_01` VALUES LESS THAN (TO_DAYS('2026-02-01')),\n" +
				"      PARTITION `p2026_02` VALUES LESS THAN (TO_DAYS('2026-03-01')),\n" +
				"      PARTITION `pmax` VALUES LESS THAN (MAXVALUE)\n" +
				"  );",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatDDL(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatCreateTableQuotedContent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:  "comma in default literal",
			input: "CREATE TABLE t (id int, note varchar(10) DEFAULT 'x, y')",
			expected: "CREATE TABLE t (\n" +
				"    id int,\n" +
				"    note varchar(10) DEFAULT 'x, y'\n" +
				")",
		},
		{
			name:  "check list and literal default",
			input: "CREATE TABLE t (c text CHECK (c IN ('a','b')), note varchar(10) DEFAULT 'x, y')",
			expected: "CREATE TABLE t (\n" +
				"    c text CHECK (c IN ('a','b')),\n" +
				"    note varchar(10) DEFAULT 'x, y'\n" +
				")",
		},
		{
			name:  "parentheses and comma in comment literal",
			input: "CREATE TABLE t (id int COMMENT 'see (a) and (b), then c', note text)",
			expected: "CREATE TABLE t (\n" +
				"    id int COMMENT 'see (a) and (b), then c',\n" +
				"    note text\n" +
				")",
		},
		{
			name:  "doubled quote and comma in literal",
			input: "CREATE TABLE t (id int, note varchar(10) DEFAULT 'it''s, ok')",
			expected: "CREATE TABLE t (\n" +
				"    id int,\n" +
				"    note varchar(10) DEFAULT 'it''s, ok'\n" +
				")",
		},
		{
			name:  "backslash before the closing quote is content",
			input: `CREATE TABLE t (id int, note varchar(10) DEFAULT 'a\', c int)`,
			expected: "CREATE TABLE t (\n" +
				"    id int,\n" +
				`    note varchar(10) DEFAULT 'a\',` + "\n" +
				"    c int\n" +
				")",
		},
		{
			name:  "backslash followed by doubled quote",
			input: `CREATE TABLE t (id int, note varchar(10) DEFAULT 'a\''b, (c', c int)`,
			expected: "CREATE TABLE t (\n" +
				"    id int,\n" +
				`    note varchar(10) DEFAULT 'a\''b, (c',` + "\n" +
				"    c int\n" +
				")",
		},
		{
			name:  "escape string literal with doubled quote",
			input: `CREATE TABLE t (id int, note text DEFAULT E'a\\''b, (c', c int)`,
			expected: "CREATE TABLE t (\n" +
				"    id int,\n" +
				`    note text DEFAULT E'a\\''b, (c',` + "\n" +
				"    c int\n" +
				")",
		},
		{
			name:     "unterminated literal",
			input:    "CREATE TABLE t (id int, note varchar(10) DEFAULT 'x, y)",
			expected: "CREATE TABLE t (id int, note varchar(10) DEFAULT 'x, y)",
		},
		{
			name:  "backtick identifier with punctuation",
			input: "CREATE TABLE `t(a` (`id` int, `note,value` text)",
			expected: "CREATE TABLE `t(a` (\n" +
				"    `id` int,\n" +
				"    `note,value` text\n" +
				")",
		},
		{
			name:  "double quoted identifier with punctuation",
			input: `CREATE TABLE "t(a" ("id" int, "note,value" text)`,
			expected: "CREATE TABLE \"t(a\" (\n" +
				"    \"id\" int,\n" +
				"    \"note,value\" text\n" +
				")",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, formatCreateTable(tt.input))
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
			name:  "explicit NULL defaults preserved",
			input: "CREATE TABLE `t` (`id` BIGINT NOT NULL, `name` VARCHAR(255) DEFAULT NULL, `count` INT DEFAULT NULL)",
			contains: []string{
				"`name` varchar(255) DEFAULT NULL,",
				"`count` int DEFAULT NULL",
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
		// Canonicalization is a spelling of the statement, not a judgement
		// about it: it quotes identifiers, uppercases keywords and collapses
		// whitespace, and otherwise reprints the parse tree as written. An
		// expression therefore keeps whatever parentheses it arrived with,
		// including ones a user would call redundant — whether two spellings
		// mean the same table is decided when the schemas are diffed, not
		// here.
		//
		// That spelling is worth pinning because it is load-bearing twice
		// over. A plan comment must never show a generated column or DEFAULT
		// expression that differs from the one being applied. And the drift
		// guard uses the canonical form as a comparison key
		// (canonicalDDLForDrift), holding a form produced at plan time
		// against one recomputed at apply time — a gap that can span a
		// deploy. A silent change to canonical spelling, a parser that begins
		// folding redundant parentheses away say, would put every in-flight
		// dispatch out of agreement with its own recomputation and fail
		// closed on someone's schema change. Pinned here, that change fails
		// in CI instead.
		{
			name:     "generated column keeps its expression",
			input:    "CREATE TABLE t (a INT, b INT AS (a * 2) STORED)",
			expected: "CREATE TABLE `t` (`a` INT,`b` INT GENERATED ALWAYS AS(`a`*2) STORED)",
		},
		{
			name:     "generated column keeps redundant parentheses",
			input:    "CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS ((a * 2)) VIRTUAL)",
			expected: "CREATE TABLE `t` (`a` INT,`b` INT GENERATED ALWAYS AS((`a`*2)) VIRTUAL)",
		},
		{
			name:     "parenthesized DEFAULT expression stays an expression",
			input:    "CREATE TABLE t (a INT DEFAULT (1 + 2))",
			expected: "CREATE TABLE `t` (`a` INT DEFAULT (1+2))",
		},
		{
			name:     "nested parentheses in a DEFAULT expression are preserved",
			input:    "CREATE TABLE t (a INT DEFAULT ((1 + 2)))",
			expected: "CREATE TABLE `t` (`a` INT DEFAULT ((1+2)))",
		},
		{
			name:     "function call in a DEFAULT expression keeps its call syntax",
			input:    "CREATE TABLE t (a CHAR(36) DEFAULT (uuid()))",
			expected: "CREATE TABLE `t` (`a` CHAR(36) DEFAULT (UUID()))",
		},
		// The two statement kinds are spelled by different code. Canonicalize
		// branches before the parser: an ALTER is reconstructed from Spirit's
		// normalized Alter field, while CREATE and DROP go through TiDB's
		// Restore. The cases above therefore pin only one of the two, and the
		// ALTER path is the one carrying more risk — a schema change on an
		// existing table is an ALTER, so it is the shape that dominates what
		// the drift guard compares, and its spelling moves with the Spirit
		// dependency rather than with anything in this repository.
		{
			name:     "ALTER adding a generated column spells out GENERATED ALWAYS",
			input:    "ALTER TABLE t ADD COLUMN b INT AS (a * 2) STORED",
			expected: "ALTER TABLE `t` ADD COLUMN `b` INT GENERATED ALWAYS AS(`a`*2) STORED",
		},
		{
			name:     "ALTER keeps redundant parentheses in a generated column",
			input:    "ALTER TABLE t ADD COLUMN b INT GENERATED ALWAYS AS ((a * 2)) VIRTUAL",
			expected: "ALTER TABLE `t` ADD COLUMN `b` INT GENERATED ALWAYS AS((`a`*2)) VIRTUAL",
		},
		{
			name:     "ALTER keeps a parenthesized DEFAULT expression",
			input:    "ALTER TABLE t ADD COLUMN c INT DEFAULT ((1 + 2))",
			expected: "ALTER TABLE `t` ADD COLUMN `c` INT DEFAULT ((1+2))",
		},
		{
			name:     "ALTER modifying a generated column keeps its expression",
			input:    "ALTER TABLE t MODIFY COLUMN b BIGINT AS (a * 2) STORED",
			expected: "ALTER TABLE `t` MODIFY COLUMN `b` BIGINT GENERATED ALWAYS AS(`a`*2) STORED",
		},
		{
			name:     "ALTER keeps an expression in every clause",
			input:    "ALTER TABLE t ADD COLUMN b INT AS (a * 2) STORED, ADD COLUMN c INT DEFAULT (1 + 2)",
			expected: "ALTER TABLE `t` ADD COLUMN `b` INT GENERATED ALWAYS AS(`a`*2) STORED, ADD COLUMN `c` INT DEFAULT (1+2)",
		},
		{
			name:     "schema-qualified ALTER keeps its expression",
			input:    "ALTER TABLE d.t ADD COLUMN b INT AS (a * 2) STORED",
			expected: "ALTER TABLE `d`.`t` ADD COLUMN `b` INT GENERATED ALWAYS AS(`a`*2) STORED",
		},
		{
			// DROP CONSTRAINT and DROP CHECK are not synonyms: MySQL resolves
			// the first against the table's CHECK, FOREIGN KEY and UNIQUE
			// constraints and the second against check constraints alone. The
			// engine submits this canonical text rather than what the author
			// wrote, so folding the two spellings together sends the server a
			// statement it rejects.
			name:     "ALTER dropping a named constraint keeps the CONSTRAINT spelling",
			input:    "ALTER TABLE t DROP CONSTRAINT uq_name",
			expected: "ALTER TABLE `t` DROP CONSTRAINT `uq_name`",
		},
		{
			name:     "ALTER dropping a check constraint keeps the CHECK spelling",
			input:    "ALTER TABLE t DROP CHECK chk_positive",
			expected: "ALTER TABLE `t` DROP CHECK `chk_positive`",
		},
		{
			name:     "invalid SQL returns original",
			input:    "not valid sql",
			expected: "not valid sql",
		},
		{
			name:     "multi-statement CREATE input returned unchanged",
			input:    "CREATE TABLE users (id INT); CREATE TABLE orders (id INT)",
			expected: "CREATE TABLE users (id INT); CREATE TABLE orders (id INT)",
		},
		{
			name:     "multi-statement DROP input returned unchanged",
			input:    "DROP TABLE users; DROP TABLE orders",
			expected: "DROP TABLE users; DROP TABLE orders",
		},
		{
			name:     "multi-statement ALTER input returned unchanged",
			input:    "alter table orders add index idx (col); drop table users",
			expected: "alter table orders add index idx (col); drop table users",
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
		{
			name:  "clause keyword inside a literal not split",
			input: "ALTER TABLE `t` ADD COLUMN `note` varchar(10) DEFAULT 'x, ADD y (', ADD INDEX `b`(`b`)",
			expected: []string{
				"ALTER TABLE `t` ADD COLUMN `note` varchar(10) DEFAULT 'x, ADD y ('",
				"ADD INDEX `b`(`b`)",
			},
		},
		{
			name:  "unterminated literal left whole",
			input: "ALTER TABLE `t` ADD COLUMN `note` varchar(10) DEFAULT 'x, ADD INDEX `b`(`b`)",
			expected: []string{
				"ALTER TABLE `t` ADD COLUMN `note` varchar(10) DEFAULT 'x, ADD INDEX `b`(`b`)",
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

func TestFormatDDLForDialect(t *testing.T) {
	t.Run("mysql dialect gets full FormatDDL treatment", func(t *testing.T) {
		got := FormatDDLForDialect(schema.DialectMySQL,
			"ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255), DROP COLUMN `old_field`")
		assert.Equal(t, "ALTER TABLE `users`\n"+
			"    ADD COLUMN `email` varchar(255),\n"+
			"    DROP COLUMN `old_field`;", got)
	})

	t.Run("postgres dialect renders its own canonical form", func(t *testing.T) {
		got := FormatDDLForDialect(schema.DialectPostgres,
			"alter   table users\n  add column session_id uuid")
		assert.Equal(t, "ALTER TABLE users ADD COLUMN session_id uuid;", got)
	})

	t.Run("postgres types are never mangled under the MySQL grammar", func(t *testing.T) {
		got := FormatDDLForDialect(schema.DialectPostgres,
			"CREATE TABLE sessions (id uuid PRIMARY KEY, payload jsonb, created_at timestamptz)")
		assert.Contains(t, got, "uuid")
		assert.Contains(t, got, "jsonb")
		assert.NotContains(t, got, "`")
	})

	t.Run("postgres CREATE TABLE is line-broken like the MySQL layout", func(t *testing.T) {
		got := FormatDDLForDialect(schema.DialectPostgres,
			"CREATE TABLE sessions (id uuid PRIMARY KEY, payload jsonb, created_at timestamptz)")
		assert.Equal(t, "CREATE TABLE sessions (\n"+
			"    id uuid PRIMARY KEY,\n"+
			"    payload jsonb,\n"+
			"    created_at timestamptz\n"+
			");", got)
	})

	t.Run("postgres escape-string literal remains intact in formatted output", func(t *testing.T) {
		got := FormatDDLForDialect(schema.DialectPostgres,
			"CREATE TABLE t (id int, note text DEFAULT E'a\\b, c')")
		assert.Equal(t, "CREATE TABLE t (\n"+
			"    id int,\n"+
			"    note text DEFAULT 'a\b, c'\n"+
			");", got)
	})

	t.Run("postgres dollar-quoted literal remains intact in formatted output", func(t *testing.T) {
		got := FormatDDLForDialect(schema.DialectPostgres,
			`CREATE TABLE t (id int, note text DEFAULT $$x, (y$$)`)
		assert.Equal(t, "CREATE TABLE t (\n"+
			"    id int,\n"+
			"    note text DEFAULT 'x, (y'\n"+
			");", got)
	})

	t.Run("postgres multi-clause ALTER renders one clause per line", func(t *testing.T) {
		got := FormatDDLForDialect(schema.DialectPostgres,
			"alter table users add column a integer, add column b text")
		assert.Equal(t, "ALTER TABLE users\n"+
			"    ADD COLUMN a int,\n"+
			"    ADD COLUMN b text;", got)
	})

	t.Run("postgres quoted identifiers with punctuation stay whole", func(t *testing.T) {
		got := FormatDDLForDialect(schema.DialectPostgres,
			`CREATE TABLE "t(a" ("id" int, "note,value" text)`)
		assert.Equal(t, "CREATE TABLE \"t(a\" (\n"+
			"    id int,\n"+
			"    \"note,value\" text\n"+
			");", got)
	})

	t.Run("postgres ALTER splits clauses around quoted names and literals", func(t *testing.T) {
		got := FormatDDLForDialect(schema.DialectPostgres,
			`ALTER TABLE "Orders" ADD COLUMN "INT" text DEFAULT 'a, (b', ADD COLUMN "x,y" int`)
		assert.Equal(t, "ALTER TABLE \"Orders\"\n"+
			"    ADD COLUMN \"INT\" text DEFAULT 'a, (b',\n"+
			"    ADD COLUMN \"x,y\" int;", got)
	})

	t.Run("postgres DROP COLUMN keeps the explicit COLUMN keyword", func(t *testing.T) {
		got := FormatDDLForDialect(schema.DialectPostgres,
			"ALTER TABLE public.users DROP COLUMN legacy")
		assert.Equal(t, "ALTER TABLE public.users DROP COLUMN legacy;", got)
	})

	t.Run("statement the dialect parser rejects renders as-is", func(t *testing.T) {
		got := FormatDDLForDialect(schema.DialectPostgres, "THIS IS NOT SQL")
		assert.Equal(t, "THIS IS NOT SQL;", got)
	})

	t.Run("dialect with no registered parser renders as-is", func(t *testing.T) {
		got := FormatDDLForDialect(schema.Dialect("customengine"),
			"CREATE TABLE t (id INT);")
		assert.Equal(t, "CREATE TABLE t (id INT);", got)
	})
}

// Every display entry point preserves quoted names, literal contents, and
// statement boundaries, even when layout formatting cannot be applied safely.
func TestDisplayFormattingPreservesSQL(t *testing.T) {
	cases := []struct {
		name    string
		dialect schema.Dialect
		raw     string
	}{
		{"mysql quoted names and literal", schema.DialectMySQL, "ALTER TABLE `INT` ADD COLUMN `TEXT` varchar(64) DEFAULT 'KEEP INT, DEFAULT NULL'"},
		{"postgres literal comma", schema.DialectPostgres, `ALTER TABLE "OrderHistory" ADD COLUMN "Label" text DEFAULT 'Keep INT, Case', ADD COLUMN payload jsonb`},
		{"statement boundaries", schema.DialectMySQL, "ALTER TABLE orders ADD COLUMN note text; DROP TABLE orders"},
		{"unknown dialect", schema.Dialect("custom"), `ALTER TABLE "INT" ADD COLUMN "TEXT" text DEFAULT 'KEEP INT, DEFAULT NULL'`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expected := tc.raw + ";"
			if tc.dialect == schema.DialectPostgres {
				expected = "ALTER TABLE \"OrderHistory\"\n    ADD COLUMN \"Label\" text DEFAULT 'Keep INT, Case',\n    ADD COLUMN payload jsonb;"
			}
			assert.Equal(t, expected, FormatDDLForDialect(tc.dialect, " \n"+tc.raw+" \n"))
			if tc.dialect == schema.DialectMySQL {
				assert.Equal(t, tc.raw+";", FormatDDL(tc.raw))
			}
		})
	}
}

// A displayed ENUM or SET list, or PostgreSQL list partition bound, too long
// for a line of its own wraps onto indented lines no wider than
// valueListWrapWidth, so the DDL reads without scrolling and GitHub still
// highlights it. Short lists stay inline, and the wrapped statement parses to
// the same SQL.
func TestFormatDDLForDialectWrapsLongValueLists(t *testing.T) {
	values := func(n int) string {
		v := make([]string, n)
		for i := range v {
			v[i] = fmt.Sprintf("'STATUS_%02d_VALUE'", i)
		}
		return strings.Join(v, ",")
	}
	tests := []struct {
		name     string
		dialect  schema.Dialect
		input    string
		expected string
	}{
		{
			name:    "single-clause ALTER wraps under the statement",
			dialect: schema.DialectMySQL,
			input:   "ALTER TABLE `orders` MODIFY COLUMN `status` enum(" + values(12) + ") NOT NULL",
			expected: "ALTER TABLE `orders` MODIFY COLUMN `status` enum(\n" +
				"    'STATUS_00_VALUE', 'STATUS_01_VALUE', 'STATUS_02_VALUE', 'STATUS_03_VALUE', 'STATUS_04_VALUE',\n" +
				"    'STATUS_05_VALUE', 'STATUS_06_VALUE', 'STATUS_07_VALUE', 'STATUS_08_VALUE', 'STATUS_09_VALUE',\n" +
				"    'STATUS_10_VALUE', 'STATUS_11_VALUE'\n" +
				") NOT NULL;",
		},
		{
			name:    "multi-clause ALTER wraps under its clause",
			dialect: schema.DialectMySQL,
			input:   "ALTER TABLE `orders` MODIFY COLUMN `status` enum(" + values(8) + ") NOT NULL, ADD COLUMN `note` text",
			expected: "ALTER TABLE `orders`\n" +
				"    MODIFY COLUMN `status` enum(\n" +
				"        'STATUS_00_VALUE', 'STATUS_01_VALUE', 'STATUS_02_VALUE', 'STATUS_03_VALUE',\n" +
				"        'STATUS_04_VALUE', 'STATUS_05_VALUE', 'STATUS_06_VALUE', 'STATUS_07_VALUE'\n" +
				"    ) NOT NULL,\n" +
				"    ADD COLUMN `note` text;",
		},
		{
			name:    "CREATE TABLE SET column with list-like comment",
			dialect: schema.DialectMySQL,
			input:   "CREATE TABLE `orders` (`id` bigint NOT NULL, `flags` set(" + values(6) + ") NOT NULL COMMENT 'enum(a,b)', PRIMARY KEY (`id`))",
			expected: "CREATE TABLE `orders` (\n" +
				"    `id` bigint NOT NULL,\n" +
				"    `flags` SET(\n" +
				"        'STATUS_00_VALUE', 'STATUS_01_VALUE', 'STATUS_02_VALUE', 'STATUS_03_VALUE',\n" +
				"        'STATUS_04_VALUE', 'STATUS_05_VALUE'\n" +
				"    ) NOT NULL COMMENT 'enum(a,b)',\n" +
				"    PRIMARY KEY(`id`)\n" +
				");",
		},
		{
			name:    "values holding commas and quotes stay whole",
			dialect: schema.DialectMySQL,
			input:   "ALTER TABLE `orders` MODIFY COLUMN `status` enum('a,b','it''s'," + values(5) + ") NOT NULL",
			expected: "ALTER TABLE `orders` MODIFY COLUMN `status` enum(\n" +
				"    'a,b', 'it''s', 'STATUS_00_VALUE', 'STATUS_01_VALUE', 'STATUS_02_VALUE', 'STATUS_03_VALUE',\n" +
				"    'STATUS_04_VALUE'\n" +
				") NOT NULL;",
		},
		{
			name:     "short list on a long line stays inline",
			dialect:  schema.DialectMySQL,
			input:    "ALTER TABLE `orders` MODIFY COLUMN `status` enum('A','B') NOT NULL COMMENT '" + strings.Repeat("x", 100) + "'",
			expected: "ALTER TABLE `orders` MODIFY COLUMN `status` enum('A','B') NOT NULL COMMENT '" + strings.Repeat("x", 100) + "';",
		},
		{
			name:    "postgres enum type",
			dialect: schema.DialectPostgres,
			input:   "CREATE TYPE order_status AS ENUM (" + values(6) + ")",
			expected: "CREATE TYPE order_status AS ENUM (\n" +
				"    'STATUS_00_VALUE', 'STATUS_01_VALUE', 'STATUS_02_VALUE', 'STATUS_03_VALUE', 'STATUS_04_VALUE',\n" +
				"    'STATUS_05_VALUE'\n" +
				");",
		},
		{
			name:    "postgres list partition bound",
			dialect: schema.DialectPostgres,
			input:   "CREATE TABLE orders_pending PARTITION OF orders FOR VALUES IN (" + values(6) + ")",
			expected: "CREATE TABLE orders_pending PARTITION OF orders FOR VALUES IN (\n" +
				"    'STATUS_00_VALUE', 'STATUS_01_VALUE', 'STATUS_02_VALUE', 'STATUS_03_VALUE', 'STATUS_04_VALUE',\n" +
				"    'STATUS_05_VALUE'\n" +
				");",
		},
		{
			name:    "postgres list partition bound before the partition's own PARTITION BY",
			dialect: schema.DialectPostgres,
			input:   "CREATE TABLE orders_pending PARTITION OF orders FOR VALUES IN (" + values(6) + ") PARTITION BY HASH (id)",
			expected: "CREATE TABLE orders_pending PARTITION OF orders FOR VALUES IN (\n" +
				"    'STATUS_00_VALUE', 'STATUS_01_VALUE', 'STATUS_02_VALUE', 'STATUS_03_VALUE', 'STATUS_04_VALUE',\n" +
				"    'STATUS_05_VALUE'\n" +
				") PARTITION BY HASH (id);",
		},
		{
			name:    "postgres ATTACH PARTITION bound",
			dialect: schema.DialectPostgres,
			input:   "ALTER TABLE orders ATTACH PARTITION orders_pending FOR VALUES IN (" + values(6) + ")",
			expected: "ALTER TABLE orders ATTACH PARTITION orders_pending FOR VALUES IN (\n" +
				"    'STATUS_00_VALUE', 'STATUS_01_VALUE', 'STATUS_02_VALUE', 'STATUS_03_VALUE', 'STATUS_04_VALUE',\n" +
				"    'STATUS_05_VALUE'\n" +
				");",
		},
		{
			name:     "postgres short partition bound stays on the statement line",
			dialect:  schema.DialectPostgres,
			input:    "CREATE TABLE orders_pending PARTITION OF orders FOR VALUES IN ('PENDING','HELD')",
			expected: "CREATE TABLE orders_pending PARTITION OF orders FOR VALUES IN ('PENDING', 'HELD');",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			formatted := FormatDDLForDialect(tc.dialect, tc.input)
			assert.Equal(t, tc.expected, formatted)
			for line := range strings.SplitSeq(formatted, "\n") {
				if !strings.Contains(line, strings.Repeat("x", 100)) {
					assert.LessOrEqual(t, len(line), valueListWrapWidth, "line %q", line)
				}
			}
			parser, err := ParserForDialect(tc.dialect)
			require.NoError(t, err)
			assert.Equal(t, parser.Canonicalize(tc.input), parser.Canonicalize(formatted), "wrapping must not change the statement")
		})
	}
}

// The wrap scanner reads one line at a time and knows only single, double,
// and backtick quotes, so list-like text inside a PostgreSQL literal that
// spans lines, or inside a dollar-quoted body, looks like a value list to it.
// Wrapping that text would change the literal; the displayed statement keeps
// its unwrapped form instead.
func TestFormatDDLForDialectKeepsUnwrappedFormWhenWrappingChangesSQL(t *testing.T) {
	listText := func(quote string) string {
		v := make([]string, 8)
		for i := range v {
			v[i] = quote + fmt.Sprintf("STATUS_%02d_VALUE", i) + quote
		}
		return "enum(" + strings.Join(v, ",") + ")"
	}
	tests := []struct {
		name    string
		dialect schema.Dialect
		input   string
	}{
		{
			name:    "postgres comment literal spanning lines",
			dialect: schema.DialectPostgres,
			input:   "COMMENT ON TABLE orders IS 'allowed values:\n" + listText("''") + "\nend of list'",
		},
		{
			name:    "postgres dollar-quoted function body",
			dialect: schema.DialectPostgres,
			input:   "CREATE FUNCTION order_statuses() RETURNS text LANGUAGE sql AS $$ SELECT 'statuses' WHERE " + listText("'") + " IS NOT NULL $$",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parser, err := ParserForDialect(tc.dialect)
			require.NoError(t, err)
			unwrapped, equivalent := formatDDLForDialect(tc.dialect, parser, tc.input, false)
			require.True(t, equivalent)
			wrapped := wrapLongValueLists(unwrapped, valueListPatternFor(tc.dialect))
			require.NotEqual(t, unwrapped, wrapped, "the scanner must misread the literal for this case to exercise the guard")
			require.NotEqual(t, parser.Canonicalize(unwrapped), parser.Canonicalize(wrapped), "wrapping must change the literal for this case to exercise the guard")

			assert.Equal(t, unwrapped, FormatDDLForDialect(tc.dialect, tc.input))
		})
	}
}

// SET is a column type only in the MySQL family. A long PostgreSQL SET (...)
// storage-parameter list is not a value list and stays on its line.
func TestFormatDDLForDialectLeavesPostgresSetParametersInline(t *testing.T) {
	input := "ALTER TABLE orders SET (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.01, autovacuum_analyze_scale_factor = 0.005, toast_tuple_target = 4096)"
	formatted := FormatDDLForDialect(schema.DialectPostgres, input)
	assert.NotContains(t, formatted, "\n", "storage parameters must not wrap")
	assert.Contains(t, formatted, "toast_tuple_target")
}

// A displayed partition definition list puts each definition on an indented
// line of its own, whether it follows PARTITION BY in a CREATE TABLE or an
// ALTER TABLE, or lists the new partitions of ADD PARTITION or REORGANIZE
// PARTITION. A partitioning clause trailing other ALTER TABLE clauses gets a
// line of its own after them. Statements with no definition list keep their
// layout, and every wrapped statement parses to the same SQL.
func TestFormatDDLWrapsPartitionDefinitions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "CREATE TABLE RANGE COLUMNS after table options",
			input: "CREATE TABLE `ledger_entries` (`id` bigint NOT NULL AUTO_INCREMENT, `settlement_date` date NOT NULL, " +
				"PRIMARY KEY (`id`,`settlement_date`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci " +
				"PARTITION BY RANGE COLUMNS(`settlement_date`) (" +
				"PARTITION p20240101 VALUES LESS THAN ('2024-01-01') ENGINE = InnoDB, " +
				"PARTITION p20240201 VALUES LESS THAN ('2024-02-01') ENGINE = InnoDB, " +
				"PARTITION future VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB)",
			expected: "CREATE TABLE `ledger_entries` (\n" +
				"    `id` bigint NOT NULL AUTO_INCREMENT,\n" +
				"    `settlement_date` date NOT NULL,\n" +
				"    PRIMARY KEY(`id`, `settlement_date`)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET utf8mb4,\n" +
				"  COLLATE utf8mb4_0900_ai_ci\n" +
				"  PARTITION BY RANGE COLUMNS (`settlement_date`) (\n" +
				"      PARTITION `p20240101` VALUES LESS THAN ('2024-01-01') ENGINE = InnoDB,\n" +
				"      PARTITION `p20240201` VALUES LESS THAN ('2024-02-01') ENGINE = InnoDB,\n" +
				"      PARTITION `future` VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB\n" +
				"  );",
		},
		{
			name: "CREATE TABLE from SHOW CREATE TABLE with a versioned partition comment",
			input: "CREATE TABLE `ledger_entries` (\n" +
				"  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
				"  `settlement_date` date NOT NULL,\n" +
				"  PRIMARY KEY (`id`,`settlement_date`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n" +
				"/*!50500 PARTITION BY RANGE  COLUMNS(settlement_date)\n" +
				"(PARTITION p20240101 VALUES LESS THAN ('2024-01-01') ENGINE = InnoDB,\n" +
				" PARTITION future VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB) */",
			expected: "CREATE TABLE `ledger_entries` (\n" +
				"    `id` bigint NOT NULL AUTO_INCREMENT,\n" +
				"    `settlement_date` date NOT NULL,\n" +
				"    PRIMARY KEY(`id`, `settlement_date`)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET utf8mb4,\n" +
				"  COLLATE utf8mb4_0900_ai_ci\n" +
				"  PARTITION BY RANGE COLUMNS (`settlement_date`) (\n" +
				"      PARTITION `p20240101` VALUES LESS THAN ('2024-01-01') ENGINE = InnoDB,\n" +
				"      PARTITION `future` VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB\n" +
				"  );",
		},
		{
			name: "CREATE TABLE LIST",
			input: "CREATE TABLE `shipments` (`id` bigint NOT NULL, `bucket` int NOT NULL) " +
				"PARTITION BY LIST (`bucket`) (PARTITION p_low VALUES IN (1,2,3), PARTITION p_high VALUES IN (4,5,6))",
			expected: "CREATE TABLE `shipments` (\n" +
				"    `id` bigint NOT NULL,\n" +
				"    `bucket` int NOT NULL\n" +
				") PARTITION BY LIST (`bucket`) (\n" +
				"    PARTITION `p_low` VALUES IN (1, 2, 3),\n" +
				"    PARTITION `p_high` VALUES IN (4, 5, 6)\n" +
				");",
		},
		{
			name: "CREATE TABLE LIST COLUMNS with multi-column tuples",
			input: "CREATE TABLE `stores` (`id` bigint NOT NULL, `region` varchar(8) NOT NULL, `tier` int NOT NULL) " +
				"PARTITION BY LIST COLUMNS (`region`,`tier`) (" +
				"PARTITION p_west VALUES IN (('west',1),('west',2)), PARTITION p_east VALUES IN (('east',1),('east',2)))",
			expected: "CREATE TABLE `stores` (\n" +
				"    `id` bigint NOT NULL,\n" +
				"    `region` varchar(8) NOT NULL,\n" +
				"    `tier` int NOT NULL\n" +
				") PARTITION BY LIST COLUMNS (`region`,`tier`) (\n" +
				"    PARTITION `p_west` VALUES IN (('west', 1), ('west', 2)),\n" +
				"    PARTITION `p_east` VALUES IN (('east', 1), ('east', 2))\n" +
				");",
		},
		{
			name: "CREATE TABLE subpartitions stay on their partition's line",
			input: "CREATE TABLE `readings` (`id` int, `r` int) PARTITION BY RANGE (`r`) SUBPARTITION BY HASH(`id`) (" +
				"PARTITION p_a VALUES LESS THAN (10) (SUBPARTITION s0, SUBPARTITION s1), " +
				"PARTITION p_b VALUES LESS THAN MAXVALUE (SUBPARTITION s2, SUBPARTITION s3))",
			expected: "CREATE TABLE `readings` (\n" +
				"    `id` int,\n" +
				"    `r` int\n" +
				") PARTITION BY RANGE (`r`) SUBPARTITION BY HASH (`id`) SUBPARTITIONS 2 (\n" +
				"    PARTITION `p_a` VALUES LESS THAN (10) (SUBPARTITION `s0`,SUBPARTITION `s1`),\n" +
				"    PARTITION `p_b` VALUES LESS THAN (MAXVALUE) (SUBPARTITION `s2`,SUBPARTITION `s3`)\n" +
				");",
		},
		{
			name: "partition COMMENT holding a definition-like list stays whole",
			input: "CREATE TABLE `readings` (`id` int, `r` int) PARTITION BY RANGE (`r`) (" +
				"PARTITION p_a VALUES LESS THAN (10) COMMENT = 'was (PARTITION a, PARTITION b)', PARTITION p_b VALUES LESS THAN MAXVALUE)",
			expected: "CREATE TABLE `readings` (\n" +
				"    `id` int,\n" +
				"    `r` int\n" +
				") PARTITION BY RANGE (`r`) (\n" +
				"    PARTITION `p_a` VALUES LESS THAN (10) COMMENT = 'was (PARTITION a, PARTITION b)',\n" +
				"    PARTITION `p_b` VALUES LESS THAN (MAXVALUE)\n" +
				");",
		},
		{
			name:  "CREATE TABLE HASH with a partition count has no list to wrap",
			input: "CREATE TABLE `sessions` (`id` bigint NOT NULL, `k` bigint NOT NULL) PARTITION BY HASH (`k`) PARTITIONS 8",
			expected: "CREATE TABLE `sessions` (\n" +
				"    `id` bigint NOT NULL,\n" +
				"    `k` bigint NOT NULL\n" +
				") PARTITION BY HASH (`k`) PARTITIONS 8;",
		},
		{
			name:  "CREATE TABLE KEY with a partition count has no list to wrap",
			input: "CREATE TABLE `sessions` (`id` bigint NOT NULL, `k` bigint NOT NULL) ENGINE=InnoDB PARTITION BY KEY (`k`) PARTITIONS 4",
			expected: "CREATE TABLE `sessions` (\n" +
				"    `id` bigint NOT NULL,\n" +
				"    `k` bigint NOT NULL\n" +
				") ENGINE InnoDB\n" +
				"  PARTITION BY KEY (`k`) PARTITIONS 4;",
		},
		{
			name: "REORGANIZE one partition into several",
			input: "ALTER TABLE `events` REORGANIZE PARTITION `future` INTO (" +
				"PARTITION `p20290201` VALUES LESS THAN ('2029-02-01'), " +
				"PARTITION `p20290301` VALUES LESS THAN ('2029-03-01'), " +
				"PARTITION `future` VALUES LESS THAN (MAXVALUE))",
			expected: "ALTER TABLE `events` REORGANIZE PARTITION `future` INTO (\n" +
				"    PARTITION `p20290201` VALUES LESS THAN ('2029-02-01'),\n" +
				"    PARTITION `p20290301` VALUES LESS THAN ('2029-03-01'),\n" +
				"    PARTITION `future` VALUES LESS THAN (MAXVALUE)\n" +
				");",
		},
		{
			name:  "REORGANIZE several partitions into one too long for its line",
			input: "ALTER TABLE `events` REORGANIZE PARTITION `p20221231`,`p20230401` INTO (PARTITION `p20230401` VALUES LESS THAN ('2023-04-01'))",
			expected: "ALTER TABLE `events` REORGANIZE PARTITION `p20221231`,`p20230401` INTO (\n" +
				"    PARTITION `p20230401` VALUES LESS THAN ('2023-04-01')\n" +
				");",
		},
		{
			name: "ADD PARTITION",
			input: "ALTER TABLE `events` ADD PARTITION (" +
				"PARTITION `p20290201` VALUES LESS THAN ('2029-02-01'), PARTITION `p20290301` VALUES LESS THAN ('2029-03-01'))",
			expected: "ALTER TABLE `events` ADD PARTITION (\n" +
				"    PARTITION `p20290201` VALUES LESS THAN ('2029-02-01'),\n" +
				"    PARTITION `p20290301` VALUES LESS THAN ('2029-03-01')\n" +
				");",
		},
		{
			name:     "ADD PARTITION of one short definition stays inline",
			input:    "ALTER TABLE `events` ADD PARTITION (PARTITION `p20290201` VALUES LESS THAN ('2029-02-01'))",
			expected: "ALTER TABLE `events` ADD PARTITION (PARTITION `p20290201` VALUES LESS THAN ('2029-02-01'));",
		},
		{
			name:     "COALESCE PARTITION",
			input:    "ALTER TABLE `sessions` COALESCE PARTITION 2",
			expected: "ALTER TABLE `sessions` COALESCE PARTITION 2;",
		},
		{
			name:     "ADD PARTITION by count",
			input:    "ALTER TABLE `sessions` ADD PARTITION PARTITIONS 4",
			expected: "ALTER TABLE `sessions` ADD PARTITION PARTITIONS 4;",
		},
		{
			name: "PARTITION BY trailing an ADD COLUMN",
			input: "ALTER TABLE `events` ADD COLUMN `note` varchar(64) NULL DEFAULT NULL PARTITION BY RANGE COLUMNS (`settlement_date`) (" +
				"PARTITION `p20230401` VALUES LESS THAN ('2023-04-01'),PARTITION `future` VALUES LESS THAN (MAXVALUE))",
			expected: "ALTER TABLE `events`\n" +
				"    ADD COLUMN `note` varchar(64) NULL DEFAULT NULL\n" +
				"    PARTITION BY RANGE COLUMNS (`settlement_date`) (\n" +
				"        PARTITION `p20230401` VALUES LESS THAN ('2023-04-01'),\n" +
				"        PARTITION `future` VALUES LESS THAN (MAXVALUE)\n" +
				"    );",
		},
		{
			name: "PARTITION BY trailing several clauses",
			input: "ALTER TABLE `events` ADD COLUMN `note` varchar(64), ADD INDEX `idx_note` (`note`), ALGORITHM=INPLACE " +
				"PARTITION BY KEY(`id`) PARTITIONS 4",
			expected: "ALTER TABLE `events`\n" +
				"    ADD COLUMN `note` varchar(64),\n" +
				"    ADD INDEX `idx_note`(`note`),\n" +
				"    ALGORITHM = INPLACE\n" +
				"    PARTITION BY KEY (`id`) PARTITIONS 4;",
		},
		{
			name:  "REMOVE PARTITIONING trailing a clause",
			input: "ALTER TABLE `events` ADD COLUMN `note` varchar(64) REMOVE PARTITIONING",
			expected: "ALTER TABLE `events`\n" +
				"    ADD COLUMN `note` varchar(64)\n" +
				"    REMOVE PARTITIONING;",
		},
		{
			name:  "COMMENT mentioning PARTITION BY before the clause",
			input: "ALTER TABLE `events` COMMENT 'split by PARTITION BY month' PARTITION BY HASH(`id`) PARTITIONS 4",
			expected: "ALTER TABLE `events`\n" +
				"    COMMENT = 'split by PARTITION BY month'\n" +
				"    PARTITION BY HASH (`id`) PARTITIONS 4;",
		},
		{
			name: "PARTITION BY alone stays on the statement line",
			input: "ALTER TABLE `events` PARTITION BY RANGE COLUMNS (`settlement_date`) (" +
				"PARTITION `p20230401` VALUES LESS THAN ('2023-04-01'),PARTITION `future` VALUES LESS THAN (MAXVALUE))",
			expected: "ALTER TABLE `events` PARTITION BY RANGE COLUMNS (`settlement_date`) (\n" +
				"    PARTITION `p20230401` VALUES LESS THAN ('2023-04-01'),\n" +
				"    PARTITION `future` VALUES LESS THAN (MAXVALUE)\n" +
				");",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			formatted := FormatDDL(tc.input)
			assert.Equal(t, tc.expected, formatted)
			assert.Equal(t, Canonicalize(tc.input), Canonicalize(formatted), "wrapping must not change the statement")
		})
	}
}

// A table with many partitions displays one definition per line, so the
// statement stays readable however long the list grows.
func TestFormatDDLWrapsLongPartitionList(t *testing.T) {
	var definitions, expected []string
	for month := range 60 {
		bound := fmt.Sprintf("'%04d-%02d-01'", 2024+month/12, month%12+1)
		name := fmt.Sprintf("`p%02d`", month)
		definitions = append(definitions, "PARTITION "+name+" VALUES LESS THAN ("+bound+")")
		expected = append(expected, "    PARTITION "+name+" VALUES LESS THAN ("+bound+")")
	}
	input := "ALTER TABLE `events` PARTITION BY RANGE COLUMNS (`settlement_date`) (" + strings.Join(definitions, ",") + ")"

	formatted := FormatDDL(input)

	assert.Equal(t, "ALTER TABLE `events` PARTITION BY RANGE COLUMNS (`settlement_date`) (\n"+
		strings.Join(expected, ",\n")+"\n);", formatted)
	assert.Equal(t, Canonicalize(input), Canonicalize(formatted))
}

// A LIST partition's VALUES IN list too long for a line of its own wraps onto
// indented lines under its definition, the way a long ENUM list does. A LIST
// COLUMNS partition's tuples pack whole, and short lists stay inline.
func TestFormatDDLWrapsLongListPartitionValues(t *testing.T) {
	codes := func(from, n int) string {
		v := make([]string, 0, n)
		for i := from; i < from+n; i++ {
			v = append(v, strconv.Itoa(1000+i))
		}
		return strings.Join(v, ",")
	}
	tuples := func(region string, n int) string {
		v := make([]string, 0, n)
		for i := range n {
			v = append(v, fmt.Sprintf("('%s',%d)", region, i))
		}
		return strings.Join(v, ",")
	}
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "LIST wraps a long value list and keeps a short one inline",
			input: "CREATE TABLE `stores` (`id` bigint NOT NULL, `store_code` int NOT NULL) PARTITION BY LIST (`store_code`) (" +
				"PARTITION p_west VALUES IN (" + codes(0, 30) + "), PARTITION p_east VALUES IN (" + codes(30, 3) + "))",
			expected: "CREATE TABLE `stores` (\n" +
				"    `id` bigint NOT NULL,\n" +
				"    `store_code` int NOT NULL\n" +
				") PARTITION BY LIST (`store_code`) (\n" +
				"    PARTITION `p_west` VALUES IN (\n" +
				"        1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014,\n" +
				"        1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029\n" +
				"    ),\n" +
				"    PARTITION `p_east` VALUES IN (1030, 1031, 1032)\n" +
				");",
		},
		{
			name: "LIST COLUMNS packs whole tuples",
			input: "CREATE TABLE `stores` (`id` bigint NOT NULL, `region` varchar(8) NOT NULL, `tier` int NOT NULL) " +
				"PARTITION BY LIST COLUMNS (`region`,`tier`) (" +
				"PARTITION p_west VALUES IN (" + tuples("west", 12) + "), PARTITION p_east VALUES IN (('east',1)))",
			expected: "CREATE TABLE `stores` (\n" +
				"    `id` bigint NOT NULL,\n" +
				"    `region` varchar(8) NOT NULL,\n" +
				"    `tier` int NOT NULL\n" +
				") PARTITION BY LIST COLUMNS (`region`,`tier`) (\n" +
				"    PARTITION `p_west` VALUES IN (\n" +
				"        ('west', 0), ('west', 1), ('west', 2), ('west', 3), ('west', 4), ('west', 5), ('west', 6),\n" +
				"        ('west', 7), ('west', 8), ('west', 9), ('west', 10), ('west', 11)\n" +
				"    ),\n" +
				"    PARTITION `p_east` VALUES IN (('east', 1))\n" +
				");",
		},
		{
			name:  "ADD PARTITION of one long LIST definition",
			input: "ALTER TABLE `stores` ADD PARTITION (PARTITION p_north VALUES IN (" + codes(50, 30) + "))",
			expected: "ALTER TABLE `stores` ADD PARTITION (\n" +
				"    PARTITION `p_north` VALUES IN (\n" +
				"        1050, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1060, 1061, 1062, 1063, 1064,\n" +
				"        1065, 1066, 1067, 1068, 1069, 1070, 1071, 1072, 1073, 1074, 1075, 1076, 1077, 1078, 1079\n" +
				"    )\n" +
				");",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			formatted := FormatDDL(tc.input)
			assert.Equal(t, tc.expected, formatted)
			for line := range strings.SplitSeq(formatted, "\n") {
				assert.LessOrEqual(t, len(line), valueListWrapWidth, "line %q", line)
			}
			assert.Equal(t, Canonicalize(tc.input), Canonicalize(formatted), "wrapping must not change the statement")
		})
	}
}
