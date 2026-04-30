package spirit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

func TestNamespaceForTable(t *testing.T) {
	sf := schema.SchemaFiles{
		"billing": {
			Files: map[string]string{
				"invoices.sql": "CREATE TABLE `invoices` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			},
		},
		"users": {
			Files: map[string]string{
				"users.sql":   "CREATE TABLE `users` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				"orders.sql":  "CREATE TABLE `orders` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				"migrate.sql": "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			},
		},
	}

	t.Run("matches by filename", func(t *testing.T) {
		ns, err := namespaceForTable("invoices", sf)
		require.NoError(t, err)
		assert.Equal(t, "billing", ns)
	})

	t.Run("matches by CREATE TABLE content", func(t *testing.T) {
		ns, err := namespaceForTable("orders", sf)
		require.NoError(t, err)
		assert.Equal(t, "users", ns)
	})

	t.Run("only matches CREATE TABLE, not other statement types", func(t *testing.T) {
		mixed := schema.SchemaFiles{
			"ns1": {
				Files: map[string]string{
					"alter.sql":  "ALTER TABLE `target` ADD COLUMN `x` INT",
					"drop.sql":   "DROP TABLE `target`",
					"rename.sql": "RENAME TABLE `target` TO `target_old`",
				},
			},
			"ns2": {
				Files: map[string]string{
					"target.sql": "CREATE TABLE `target` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				},
			},
		}
		ns, err := namespaceForTable("target", mixed)
		require.NoError(t, err)
		assert.Equal(t, "ns2", ns, "should match the namespace with CREATE TABLE, not ALTER/DROP/RENAME")
	})

	t.Run("single namespace fallback", func(t *testing.T) {
		single := schema.SchemaFiles{
			"default": {
				Files: map[string]string{
					"other.sql": "CREATE TABLE `other` (`id` bigint NOT NULL) ENGINE=InnoDB",
				},
			},
		}
		ns, err := namespaceForTable("nonexistent", single)
		require.NoError(t, err)
		assert.Equal(t, "default", ns)
	})

	t.Run("no match falls back to single namespace", func(t *testing.T) {
		// When no CREATE TABLE matches but there's only one namespace, it's returned as fallback
		single := schema.SchemaFiles{
			"only": {Files: map[string]string{"x.sql": "CREATE TABLE `x` (`id` bigint NOT NULL) ENGINE=InnoDB"}},
		}
		ns, err := namespaceForTable("nonexistent", single)
		require.NoError(t, err)
		assert.Equal(t, "only", ns)
	})

	t.Run("no namespace found with empty schema files", func(t *testing.T) {
		empty := schema.SchemaFiles{}
		_, err := namespaceForTable("nonexistent", empty)
		assert.Error(t, err)
	})
}
