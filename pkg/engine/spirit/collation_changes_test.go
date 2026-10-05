package spirit

import (
	"errors"
	"fmt"
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
				CanMergeValues: false,
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
				CanMergeValues: true,
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
				CanMergeValues: true,
				UniqueIndexes:  []string{"uk_sku"},
			}},
		},
		{
			name: "a case-sensitive unique column moving onto a case-insensitive collation can collide",
			current: "CREATE TABLE `products` (`sku` varchar(64) COLLATE utf8mb4_bin NOT NULL, UNIQUE KEY `uk_sku` (`sku`)) " +
				"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `sku` varchar(64) COLLATE utf8mb4_general_ci NOT NULL",
			desired: "CREATE TABLE `products` (`sku` varchar(64) COLLATE utf8mb4_general_ci NOT NULL, UNIQUE KEY `uk_sku` (`sku`))",
			want: []engine.CollationChange{{
				Column: "sku", From: "utf8mb4_bin", To: "utf8mb4_general_ci",
				Case:           engine.ComparisonBecomesInsensitive,
				TrailingSpaces: engine.ComparisonUnchanged,
				CanMergeValues: true,
				UniqueIndexes:  []string{"uk_sku"},
			}},
		},
		{
			name: "a NO PAD unique column moving onto a PAD SPACE collation can collide",
			current: "CREATE TABLE `products` (`sku` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL, UNIQUE KEY `uk_sku` (`sku`)) " +
				"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `sku` varchar(64) COLLATE utf8mb4_general_ci NOT NULL",
			desired: "CREATE TABLE `products` (`sku` varchar(64) COLLATE utf8mb4_general_ci NOT NULL, UNIQUE KEY `uk_sku` (`sku`))",
			want: []engine.CollationChange{{
				Column: "sku", From: "utf8mb4_0900_ai_ci", To: "utf8mb4_general_ci",
				Case:           engine.ComparisonUnchanged,
				TrailingSpaces: engine.ComparisonBecomesInsensitive,
				CanMergeValues: true,
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
				CanMergeValues: true,
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
					CanMergeValues: true,
					UniqueIndexes:  []string{"uk_sku"},
				},
				{
					Column: "title", From: "utf8mb4_general_ci", To: "latin1_bin",
					Case:           engine.ComparisonBecomesSensitive,
					TrailingSpaces: engine.ComparisonUnchanged,
					CanMergeValues: true,
				},
			},
		},
		{
			name:    "a charset named without a collation takes the target's default for it",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `title` varchar(255) CHARACTER SET utf8mb4 DEFAULT NULL",
			desired: productsTable,
			want: []engine.CollationChange{{
				Column: "title", From: "utf8mb4_general_ci", To: "utf8mb4_0900_ai_ci",
				Case:           engine.ComparisonUnchanged,
				TrailingSpaces: engine.ComparisonBecomesSensitive,
				CanMergeValues: true,
			}},
		},
		{
			name: "a new charset named without a collation takes the target's default for it",
			current: "CREATE TABLE `products` (`title` varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL) " +
				"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `title` varchar(255) CHARACTER SET utf8mb4 DEFAULT NULL",
			desired: "CREATE TABLE `products` (`title` varchar(255) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			want: []engine.CollationChange{{
				Column: "title", From: "latin1_swedish_ci", To: "utf8mb4_0900_ai_ci",
				Case:           engine.ComparisonUnchanged,
				TrailingSpaces: engine.ComparisonBecomesSensitive,
				CanMergeValues: true,
			}},
		},
		{
			name: "a charset named without a collation whose default is the current collation re-collates nothing",
			current: "CREATE TABLE `products` (`title` varchar(255) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL) " +
				"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `title` varchar(255) CHARACTER SET utf8mb4 DEFAULT NULL",
			desired: "CREATE TABLE `products` (`title` varchar(255) DEFAULT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name:    "a primary key column is covered by PRIMARY",
			alter:   "ALTER TABLE `products` MODIFY COLUMN `sku` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL",
			desired: "CREATE TABLE `products` (`sku` varchar(64) COLLATE utf8mb4_0900_ai_ci NOT NULL, PRIMARY KEY (`sku`))",
			want: []engine.CollationChange{{
				Column: "sku", From: "utf8mb4_general_ci", To: "utf8mb4_0900_ai_ci",
				Case:           engine.ComparisonUnchanged,
				TrailingSpaces: engine.ComparisonBecomesSensitive,
				CanMergeValues: true,
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
			got, err := plannedCollationChanges(slog.New(slog.DiscardHandler), tt.alter, current, tt.desired, serverDefaultCollations)
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
		"ALTER TABLE `notes` MODIFY COLUMN `body` varchar(64) COLLATE utf8mb4_made_up_ci NOT NULL", current, current, serverDefaultCollations)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "utf8mb4_made_up_ci", got[0].To)
	assert.Equal(t, engine.ComparisonUnknown, got[0].Case)
	assert.Equal(t, engine.ComparisonUnknown, got[0].TrailingSpaces)
	assert.True(t, got[0].CanMergeValues)
	assert.Equal(t, []string{"uk_body"}, got[0].UniqueIndexes)
}

// A charset default the target cannot answer leaves the new collation unknown,
// and the column's comparisons unknown with it, rather than failing the plan.
func TestPlannedCollationChangesUnreadableServerDefaultFailsClosed(t *testing.T) {
	unreachable := func(string) (string, error) { return "", errors.New("connect: connection refused") }
	got, err := plannedCollationChanges(slog.New(slog.DiscardHandler),
		"ALTER TABLE `products` MODIFY COLUMN `sku` varchar(64) CHARACTER SET utf8mb4 NOT NULL", productsTable, productsTable, unreachable)
	require.NoError(t, err)
	assert.Equal(t, []engine.CollationChange{{
		Column: "sku", From: "utf8mb4_general_ci", To: "",
		Case:           engine.ComparisonUnknown,
		TrailingSpaces: engine.ComparisonUnknown,
		CanMergeValues: true,
		UniqueIndexes:  []string{"uk_sku"},
	}}, got)
}

// serverDefaultCollations answers each charset's default the way MySQL 8.0
// and later do.
func serverDefaultCollations(charset string) (string, error) {
	switch charset {
	case "utf8mb4":
		return "utf8mb4_0900_ai_ci", nil
	case "latin1":
		return "latin1_swedish_ci", nil
	}
	return "", fmt.Errorf("no charset %q", charset)
}
