package planetscale

import (
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/spirit/pkg/table"
)

func TestDiffKeyspace_DetectsSchemaChanges(t *testing.T) {
	e := &Engine{
		linter: lint.New(),
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	t.Run("matching schemas produce no changes", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {
				{Name: "users", Schema: "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
			},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		assert.Empty(t, changes, "matching schemas should produce no changes")
	})

	t.Run("missing column detected as ALTER", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {
				{Name: "users", Schema: "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
			},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  `phone` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		require.Len(t, changes, 1, "should detect one ALTER TABLE change")
		assert.Equal(t, "users", changes[0].Table)
		assert.Contains(t, changes[0].DDL, "phone")
	})

	t.Run("extra column on branch detected as ALTER DROP", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {
				{Name: "users", Schema: "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  `stale_col` varchar(100) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
			},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		require.Len(t, changes, 1, "should detect DROP COLUMN for stale column")
		assert.Equal(t, "users", changes[0].Table)
		assert.Contains(t, changes[0].DDL, "stale_col")
	})

	t.Run("missing table detected as CREATE", func(t *testing.T) {
		currentSchema := map[string][]table.TableSchema{
			"myapp": {},
		}
		desired := &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
			},
		}
		changes, _, _, err := e.diffKeyspace(t.Context(), nil, "", "", "", "myapp", desired, currentSchema)
		require.NoError(t, err)
		require.Len(t, changes, 1, "should detect CREATE TABLE")
		assert.Equal(t, "users", changes[0].Table)
		assert.Equal(t, statement.StatementCreateTable, changes[0].Operation)
	})
}
