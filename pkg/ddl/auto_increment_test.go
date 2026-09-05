package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStripTableAutoIncrement(t *testing.T) {
	tests := []struct {
		name string
		stmt string
		want string
	}{
		{
			name: "counter between other table options",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name: "counter as the last table option",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=42",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name: "no counter is returned byte for byte",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name: "keyword inside a column default, comment and identifier survives the counter removal",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `note` varchar(64) DEFAULT 'AUTO_INCREMENT=123',\n" +
				"  `x` int DEFAULT NULL COMMENT 'reset AUTO_INCREMENT 1000 on rollover',\n" +
				"  `auto_increment_2024` int DEFAULT NULL,\n" +
				"  `AUTO_INCREMENT=789` int DEFAULT NULL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='keep auto_increment=321'",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `note` varchar(64) DEFAULT 'AUTO_INCREMENT=123',\n" +
				"  `x` int DEFAULT NULL COMMENT 'reset AUTO_INCREMENT 1000 on rollover',\n" +
				"  `auto_increment_2024` int DEFAULT NULL,\n" +
				"  `AUTO_INCREMENT=789` int DEFAULT NULL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='keep auto_increment=321'",
		},
		{
			name: "keyword inside literals with no counter present is untouched",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `note` varchar(64) DEFAULT 'AUTO_INCREMENT=123',\n" +
				"  `x` int DEFAULT NULL COMMENT 'reset AUTO_INCREMENT 1000 on rollover'\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			want: "CREATE TABLE `orders` (\n" +
				"  `note` varchar(64) DEFAULT 'AUTO_INCREMENT=123',\n" +
				"  `x` int DEFAULT NULL COMMENT 'reset AUTO_INCREMENT 1000 on rollover'\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name: "lowercase counter written without an equals sign",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB auto_increment 77 DEFAULT CHARSET=utf8mb4",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		},
		{
			name: "table comment spelling the counter is skipped to reach the real one",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB COMMENT='rolls over at auto_increment=999' AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB COMMENT='rolls over at auto_increment=999' DEFAULT CHARSET=utf8mb4",
		},
		{
			name: "table name spelling the counter is skipped to reach the real one",
			stmt: "CREATE TABLE `AUTO_INCREMENT=1` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4",
			want: "CREATE TABLE `AUTO_INCREMENT=1` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		},
		{
			name: "schema-file form with a trailing semicolon and newline",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4;\n",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n",
		},
		{
			name: "counter on a partitioned table",
			stmt: "CREATE TABLE `events` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=900 DEFAULT CHARSET=utf8mb4\n" +
				"/*!50100 PARTITION BY RANGE (`id`)\n" +
				"(PARTITION p0 VALUES LESS THAN (100) ENGINE = InnoDB,\n" +
				" PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */",
			want: "CREATE TABLE `events` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
				"/*!50100 PARTITION BY RANGE (`id`)\n" +
				"(PARTITION p0 VALUES LESS THAN (100) ENGINE = InnoDB,\n" +
				" PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StripTableAutoIncrement(tt.stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			// Stripping an already-stripped statement is a no-op, so a schema
			// pulled twice produces the same file both times.
			again, err := StripTableAutoIncrement(got)
			require.NoError(t, err)
			assert.Equal(t, tt.want, again)
		})
	}
}

// The column-level attribute is what makes ids generate, so it must survive
// even when the table-level counter next to it is removed.
func TestStripTableAutoIncrementKeepsColumnAttribute(t *testing.T) {
	stmt := "CREATE TABLE `orders` (\n" +
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4"

	got, err := StripTableAutoIncrement(stmt)
	require.NoError(t, err)
	assert.Contains(t, got, "`id` bigint unsigned NOT NULL AUTO_INCREMENT,")
	assert.NotContains(t, got, "AUTO_INCREMENT=500")
}

func TestStripTableAutoIncrementRejectsUnparseableStatements(t *testing.T) {
	_, err := StripTableAutoIncrement("CREATE TABLE `orders` (")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse CREATE TABLE to strip its auto-increment counter")
}
