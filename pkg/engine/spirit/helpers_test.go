package spirit

import (
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

func TestNamespaceForTable(t *testing.T) {
	sf := schema.SchemaFiles{
		"billing": {Files: map[string]string{"invoices.sql": ""}},
		"users":   {Files: map[string]string{"users.sql": "", "orders.sql": ""}},
	}
	declaredIn := map[string]string{"invoices": "billing", "users": "users", "orders": "users"}

	t.Run("returns the namespace recorded for the declaration", func(t *testing.T) {
		ns, err := namespaceForTable("orders", declaredIn, sf)
		require.NoError(t, err)
		assert.Equal(t, "users", ns)
	})

	// With exactly one namespace, a table no schema file declares (such as a
	// DROP TABLE plan) is attributed to that sole namespace.
	t.Run("single namespace fallback returns the only namespace", func(t *testing.T) {
		single := schema.SchemaFiles{"default": {Files: map[string]string{"other.sql": ""}}}
		ns, err := namespaceForTable("dropped_table", map[string]string{"other": "default"}, single)
		require.NoError(t, err)
		assert.Equal(t, "default", ns)
	})

	// With two or more namespaces and no declaration, the namespace is
	// ambiguous. Returning an arbitrary map key would make per-table progress
	// matching nondeterministic, so the lookup fails with the same error on
	// every run instead.
	t.Run("multi-namespace with no declaration returns an error", func(t *testing.T) {
		for range 50 {
			ns, err := namespaceForTable("dropped_table", declaredIn, sf)
			require.EqualError(t, err, `no namespace defines table "dropped_table" among 2 schema namespaces [billing users]`)
			assert.Empty(t, ns)
		}
	})

	t.Run("empty schema files returns an error", func(t *testing.T) {
		ns, err := namespaceForTable("nonexistent", nil, schema.SchemaFiles{})
		require.EqualError(t, err, `no namespace defines table "nonexistent" among 0 schema namespaces []`)
		assert.Empty(t, ns)
	})
}

// A schema file's name does not decide which namespace a table belongs to:
// billing/orders.sql declares `invoices`, and the `orders` table is declared
// by testdb/x.sql. The plan's ALTER on `orders` belongs to testdb and the
// CREATE of `invoices` to billing, on every run, with the namespaces in
// sorted order.
func TestGroupChangesByNamespace_FollowsDeclarations(t *testing.T) {
	sf := schema.SchemaFiles{
		"billing": {Files: map[string]string{"orders.sql": "CREATE TABLE `invoices` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}},
		"testdb":  {Files: map[string]string{"x.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"}},
	}
	declaredIn := map[string]string{"invoices": "billing", "orders": "testdb"}
	liveOrders := "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	alterOrders := engine.TableChange{Table: "orders", Operation: ddl.StatementAlterTable, DDL: "ALTER TABLE `orders` ADD COLUMN `note` text"}
	createInvoices := engine.TableChange{Table: "invoices", Operation: ddl.StatementCreateTable, DDL: sf["billing"].Files["orders.sql"]}

	want := []engine.SchemaChange{
		{
			Namespace:             "billing",
			TableChanges:          []engine.TableChange{createInvoices},
			OriginalFiles:         map[string]string{},
			OriginalFilesCaptured: true,
		},
		{
			Namespace:             "testdb",
			TableChanges:          []engine.TableChange{alterOrders},
			OriginalFiles:         map[string]string{"orders.sql": liveOrders},
			OriginalFilesCaptured: true,
		},
	}
	for range 50 {
		got, err := groupChangesByNamespace(
			[]engine.TableChange{alterOrders, createInvoices},
			[]table.TableSchema{{Name: "orders", Schema: liveOrders}},
			declaredIn, sf)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}
