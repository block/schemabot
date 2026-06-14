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
				"users.sql":  "CREATE TABLE `users` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				"orders.sql": "CREATE TABLE `orders` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				"change.sql": "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
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

	// A single CREATE TABLE match in a multi-namespace setup always resolves to
	// the same namespace, regardless of Go's randomized map iteration order.
	t.Run("multi-namespace match is deterministic across map orderings", func(t *testing.T) {
		for range 50 {
			ns, err := namespaceForTable("orders", sf)
			require.NoError(t, err)
			require.Equal(t, "users", ns)
		}
	})

	// A schema file containing several CREATE TABLE statements maps every table
	// it defines to that file's namespace, not just the first statement.
	t.Run("multi-statement file maps all defined tables", func(t *testing.T) {
		multi := schema.SchemaFiles{
			"catalog": {
				Files: map[string]string{
					"schema.sql": "CREATE TABLE `products` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\nCREATE TABLE `categories` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
				},
			},
			"accounts": {
				Files: map[string]string{
					"users.sql": "CREATE TABLE `users` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				},
			},
		}

		first, err := namespaceForTable("products", multi)
		require.NoError(t, err)
		assert.Equal(t, "catalog", first)

		// The second CREATE TABLE in the same file also resolves, proving every
		// statement in the file is classified.
		second, err := namespaceForTable("categories", multi)
		require.NoError(t, err)
		assert.Equal(t, "catalog", second)
	})

	// With exactly one namespace, a table whose CREATE is absent (such as a
	// DROP TABLE plan) is attributed to that sole namespace.
	t.Run("single namespace fallback returns the only namespace", func(t *testing.T) {
		single := schema.SchemaFiles{
			"default": {
				Files: map[string]string{
					"other.sql": "CREATE TABLE `other` (\n  `id` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				},
			},
		}
		ns, err := namespaceForTable("dropped_table", single)
		require.NoError(t, err)
		assert.Equal(t, "default", ns)
	})

	// With two or more namespaces and no defining statement, the namespace is
	// ambiguous. Returning an arbitrary map key would make per-table progress
	// matching nondeterministic, so the lookup fails loudly instead.
	t.Run("multi-namespace with no match returns an error", func(t *testing.T) {
		for range 50 {
			ns, err := namespaceForTable("dropped_table", sf)
			require.Error(t, err)
			assert.Empty(t, ns)
			assert.Contains(t, err.Error(), "dropped_table")
			assert.Contains(t, err.Error(), "billing")
			assert.Contains(t, err.Error(), "users")
		}
	})

	t.Run("empty schema files returns an error", func(t *testing.T) {
		ns, err := namespaceForTable("nonexistent", schema.SchemaFiles{})
		require.Error(t, err)
		assert.Empty(t, ns)
	})
}
