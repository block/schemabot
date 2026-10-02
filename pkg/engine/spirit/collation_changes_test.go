package spirit

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

const productsTable = "CREATE TABLE `products` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `sku` varchar(64) COLLATE utf8mb4_general_ci NOT NULL,\n" +
	"  `title` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,\n" +
	"  `stock` int NOT NULL,\n" +
	"  PRIMARY KEY (`id`),\n" +
	"  UNIQUE KEY `uk_sku` (`sku`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"

func TestPlannedCollationChanges(t *testing.T) {
	tests := []struct {
		name    string
		current string
		alter   string
		desired string
		want    []engine.CollationChange
	}{
		{
			name:    "a unique column moving onto the charset's binary collation risks no collision",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `sku` varchar(64) COLLATE utf8mb4_bin NOT NULL",
			desired: productsTable,
			want: []engine.CollationChange{{
				Column: "sku", From: "utf8mb4_general_ci", To: "utf8mb4_bin",
				Case:           engine.ComparisonBecomesSensitive,
				TrailingSpaces: engine.ComparisonUnchanged,
			}},
		},
		{
			name:    "a unique column moving onto a weight-based collation can collide",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `sku` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL",
			desired: productsTable,
			want: []engine.CollationChange{{
				Column: "sku", From: "utf8mb4_general_ci", To: "utf8mb4_0900_ai_ci",
				Case:           engine.ComparisonUnchanged,
				TrailingSpaces: engine.ComparisonBecomesSensitive,
				UniqueIndexes:  []string{"uk_sku"},
			}},
		},
		{
			name: "a binary collation that starts ignoring trailing spaces can collide",
			current: "CREATE TABLE `products` (`sku` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL, UNIQUE KEY `uk_sku` (`sku`)) " +
				"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `sku` varchar(64) COLLATE utf8mb4_bin NOT NULL",
			desired: "CREATE TABLE `products` (`sku` varchar(64) COLLATE utf8mb4_bin NOT NULL, UNIQUE KEY `uk_sku` (`sku`))",
			want: []engine.CollationChange{{
				Column: "sku", From: "utf8mb4_0900_ai_ci", To: "utf8mb4_bin",
				Case:           engine.ComparisonBecomesSensitive,
				TrailingSpaces: engine.ComparisonBecomesInsensitive,
				UniqueIndexes:  []string{"uk_sku"},
			}},
		},
		{
			name: "a new table default reaches the columns the ALTER redeclares",
			alter: "ALTER TABLE `products` MODIFY COLUMN `title` varchar(255) DEFAULT NULL, " +
				"DEFAULT CHARSET=utf8mb4, COLLATE=utf8mb4_0900_ai_ci",
			desired: productsTable,
			want: []engine.CollationChange{{
				Column: "title", From: "utf8mb4_general_ci", To: "utf8mb4_0900_ai_ci",
				Case:           engine.ComparisonUnchanged,
				TrailingSpaces: engine.ComparisonBecomesSensitive,
			}},
		},
		{
			name:    "CONVERT TO CHARACTER SET re-collates every character column, and a new charset can collide",
			alter:   "ALTER TABLE `products` CONVERT TO CHARACTER SET latin1 COLLATE latin1_bin",
			desired: productsTable,
			want: []engine.CollationChange{
				{
					Column: "sku", From: "utf8mb4_general_ci", To: "latin1_bin",
					Case:           engine.ComparisonBecomesSensitive,
					TrailingSpaces: engine.ComparisonUnchanged,
					UniqueIndexes:  []string{"uk_sku"},
				},
				{
					Column: "title", From: "utf8mb4_general_ci", To: "latin1_bin",
					Case:           engine.ComparisonBecomesSensitive,
					TrailingSpaces: engine.ComparisonUnchanged,
				},
			},
		},
		{
			name:    "a collation left to the server default is reported as unknown",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `title` varchar(255) CHARACTER SET utf8mb4 DEFAULT NULL",
			desired: productsTable,
			want: []engine.CollationChange{{
				Column: "title", From: "utf8mb4_general_ci", To: "",
				Case:           engine.ComparisonUnknown,
				TrailingSpaces: engine.ComparisonUnknown,
			}},
		},
		{
			name:    "a primary key column is covered by PRIMARY",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `sku` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL",
			desired: "CREATE TABLE `products` (`sku` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL, PRIMARY KEY (`sku`))",
			want: []engine.CollationChange{{
				Column: "sku", From: "utf8mb4_general_ci", To: "utf8mb4_0900_ai_ci",
				Case:           engine.ComparisonUnchanged,
				TrailingSpaces: engine.ComparisonBecomesSensitive,
				UniqueIndexes:  []string{"PRIMARY"},
			}},
		},
		{
			name:    "a widened column keeps its collation",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `title` varchar(512) COLLATE utf8mb4_general_ci DEFAULT NULL",
			desired: productsTable,
		},
		{
			name:    "a character column changed to a numeric type is a type change",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `title` int DEFAULT NULL",
			desired: productsTable,
		},
		{
			name:    "an added column re-collates nothing",
			alter:   "ALTER TABLE `products` ADD COLUMN `note` text COLLATE utf8mb4_bin",
			desired: productsTable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := tt.current
			if current == "" {
				current = productsTable
			}
			got, err := plannedCollationChanges(slog.New(slog.DiscardHandler), tt.alter, current, tt.desired)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// A collation the plan does not know reports unknown comparisons, and the
// unique index covering the column, since the plan cannot rule out a collision.
func TestPlannedCollationChangesUnknownCollationFailsClosed(t *testing.T) {
	current := "CREATE TABLE `notes` (`body` varchar(64) COLLATE utf8mb4_general_ci NOT NULL, UNIQUE KEY `uk_body` (`body`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
	got, err := plannedCollationChanges(slog.New(slog.DiscardHandler),
		"ALTER TABLE `notes` MODIFY COLUMN `body` varchar(64) COLLATE utf8mb4_made_up_ci NOT NULL", current, current)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "utf8mb4_made_up_ci", got[0].To)
	assert.Equal(t, engine.ComparisonUnknown, got[0].Case)
	assert.Equal(t, engine.ComparisonUnknown, got[0].TrailingSpaces)
	assert.Equal(t, []string{"uk_body"}, got[0].UniqueIndexes)
}
