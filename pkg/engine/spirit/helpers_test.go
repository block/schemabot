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
		ns, err := namespaceForTable("orders", declaredIn, sf, "shop")
		require.NoError(t, err)
		assert.Equal(t, "users", ns)
	})

	// With exactly one namespace, a table no schema file declares (such as a
	// DROP TABLE plan) is attributed to that sole namespace.
	t.Run("single namespace fallback returns the only namespace", func(t *testing.T) {
		single := schema.SchemaFiles{"default": {Files: map[string]string{"other.sql": ""}}}
		ns, err := namespaceForTable("dropped_table", map[string]string{"other": "default"}, single, "shop")
		require.NoError(t, err)
		assert.Equal(t, "default", ns)
	})

	// With two or more namespaces diffed against one database, an undeclared
	// table is live in every namespace's schema at once, so no namespace can
	// be shown to own its drop. Returning an arbitrary map key would make
	// per-table progress matching nondeterministic, so the lookup refuses
	// with the same error, naming both namespaces and the remedy, on every
	// run instead.
	t.Run("multi-namespace with no declaration refuses naming every namespace", func(t *testing.T) {
		for range 50 {
			ns, err := namespaceForTable("dropped_table", declaredIn, sf, "shop")
			require.EqualError(t, err, `DROP TABLE "dropped_table" cannot be placed: namespaces [billing users] all diff against database "shop". Keep this database's schema files in one namespace directory, or ask a SchemaBot operator to use a target with no database in its DSN`)
			assert.Empty(t, ns)
		}
	})

	t.Run("empty schema files returns an error", func(t *testing.T) {
		ns, err := namespaceForTable("nonexistent", nil, schema.SchemaFiles{}, "shop")
		require.EqualError(t, err, `DROP TABLE "nonexistent" cannot be placed: the request carries no schema files for database "shop". Restore this database's schema files, or remove the database from the SchemaBot config`)
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
			declaredIn, sf, "testdb")
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

// A single-namespace request owns every live table, so an undeclared table's
// DROP and its original definition both land in that namespace. An empty
// change list yields no SchemaChange at all, even with live tables present,
// because originals are captured only for namespaces the plan changes.
func TestGroupChangesByNamespace_SingleNamespace(t *testing.T) {
	sf := schema.SchemaFiles{"shop": {Files: map[string]string{"orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}}}
	declaredIn := map[string]string{"orders": "shop"}
	liveOrders := "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	liveOld := "CREATE TABLE `orders_old` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	current := []table.TableSchema{{Name: "orders", Schema: liveOrders}, {Name: "orders_old", Schema: liveOld}}

	t.Run("an undeclared table's drop and original belong to the only namespace", func(t *testing.T) {
		dropOld := engine.TableChange{Table: "orders_old", Operation: ddl.StatementDropTable, DDL: "DROP TABLE `orders_old`"}
		got, err := groupChangesByNamespace([]engine.TableChange{dropOld}, current, declaredIn, sf, "shop")
		require.NoError(t, err)
		require.Equal(t, []engine.SchemaChange{{
			Namespace:             "shop",
			TableChanges:          []engine.TableChange{dropOld},
			OriginalFiles:         map[string]string{"orders.sql": liveOrders, "orders_old.sql": liveOld},
			OriginalFilesCaptured: true,
		}}, got)
	})

	t.Run("an empty change list yields no schema change", func(t *testing.T) {
		got, err := groupChangesByNamespace(nil, current, declaredIn, sf, "shop")
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}
