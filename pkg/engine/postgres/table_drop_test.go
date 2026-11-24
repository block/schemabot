package postgres

import (
	"testing"

	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// referencing returns a model of table with one foreign key named constraint.
func referencing(table, constraint string, referencedBy ...string) schemadiff.Model {
	return schemadiff.Model{
		Table:        table,
		Constraints:  []schemadiff.Constraint{{Name: constraint, Def: "FOREIGN KEY (ref_id) REFERENCES other(id)"}},
		ReferencedBy: referencedBy,
	}
}

// A chain of foreign keys among dropped tables orders the drops so each
// table goes after every table that references it, and each definition
// leaves out the references that are gone by the time the drop runs.
func TestPlanTableDropsOrdersReferencingTablesFirst(t *testing.T) {
	plan := planTableDrops("public", map[string]schemadiff.Model{
		"a_regions":    {Table: "a_regions", ReferencedBy: []string{"warehouses.warehouses_region_fkey"}},
		"warehouses":   referencing("warehouses", "warehouses_region_fkey", "z_shipments.z_shipments_warehouse_fkey"),
		"z_shipments":  referencing("z_shipments", "z_shipments_warehouse_fkey"),
		"m_standalone": {Table: "m_standalone"},
	})
	assert.Equal(t, []string{"m_standalone", "z_shipments", "warehouses", "a_regions"}, plan.order)
	assert.Empty(t, plan.blocked)
	assert.Equal(t, "table a_regions\n", plan.definitions["a_regions"])
	assert.NotContains(t, plan.definitions["warehouses"], "referenced by")
}

// Introspection leaves a table's foreign key to itself out of the tables
// that reference it, so a reference that resolves to the table itself
// belongs to a same-named table in another schema, which stays. The table
// is blocked rather than dropped into the server's refusal.
func TestPlanTableDropsBlocksAReferenceThatResolvesToItself(t *testing.T) {
	plan := planTableDrops("public", map[string]schemadiff.Model{
		"categories": referencing("categories", "categories_parent_fkey", "categories.categories_parent_fkey"),
	})
	assert.Empty(t, plan.order)
	assert.Contains(t, plan.blocked["categories"], "its foreign keys form a cycle")
}

// A table that a kept table references is blocked, and blocking it keeps
// the tables it references in turn: tags is referenced by a table that
// stays, so tags stays, and so does colors, which tags references.
func TestPlanTableDropsBlocksTablesThatStayReferenced(t *testing.T) {
	plan := planTableDrops("public", map[string]schemadiff.Model{
		"colors": {Table: "colors", ReferencedBy: []string{"tags.tags_color_fkey"}},
		"tags":   referencing("tags", "tags_color_fkey", "posts.posts_tag_fkey"),
	})
	assert.Empty(t, plan.order)
	assert.Contains(t, plan.blocked["tags"], `foreign key constraint(s) "posts.posts_tag_fkey" of tables that stay reference it`)
	assert.Contains(t, plan.blocked["colors"], `"tags.tags_color_fkey"`)
}

// Introspection names a referencing table without its schema, so the same
// name twice means two tables in different schemas. Only one of them can be
// the dropped table, so the table they reference stays.
func TestPlanTableDropsBlocksAReferenceItCannotAttribute(t *testing.T) {
	plan := planTableDrops("public", map[string]schemadiff.Model{
		"users":  {Table: "users", ReferencedBy: []string{"orders.orders_user_fkey", "orders.orders_user_fkey"}},
		"orders": referencing("orders", "orders_user_fkey"),
	})
	assert.Equal(t, []string{"orders"}, plan.order)
	assert.Contains(t, plan.blocked["users"], `"orders.orders_user_fkey"`)
}

// Partitioned tables, partitions, and inheritance parents and children are
// blocked, and so is a cycle of foreign keys, which no order satisfies.
func TestPlanTableDropsBlocksHierarchiesAndCycles(t *testing.T) {
	plan := planTableDrops("public", map[string]schemadiff.Model{
		"events":  {Table: "events", PartitionKey: "RANGE (day)"},
		"base":    {Table: "base", InheritanceChildren: []string{"public.derived"}},
		"derived": {Table: "derived", InheritsParents: []string{"public.base"}},
		"ping":    referencing("ping", "ping_pong_fkey", "pong.pong_ping_fkey"),
		"pong":    referencing("pong", "pong_ping_fkey", "ping.ping_pong_fkey"),
	})
	assert.Empty(t, plan.order)
	for _, table := range []string{"events", "base", "derived"} {
		assert.Contains(t, plan.blocked[table], "does not drop a partitioned table or a table in an inheritance hierarchy", table)
	}
	for _, table := range []string{"ping", "pong"} {
		assert.Contains(t, plan.blocked[table], "its foreign keys form a cycle", table)
	}
}

// The apply admits a DROP TABLE only when it is exactly the plan's drop of
// the change's table in the apply's schema and carries the plan's record of
// the table; every other statement goes on to the native path.
func TestTableDropForApply(t *testing.T) {
	recorded := map[string]string{reviewedTableDropMetadataKey: "table legacy\n"}
	tests := []struct {
		name     string
		ddl      string
		metadata map[string]string
		wantDrop bool
		wantErr  string
	}{
		{name: "the plan's drop", ddl: "DROP TABLE public.legacy", metadata: recorded, wantDrop: true},
		{name: "another statement", ddl: "ALTER TABLE public.legacy ADD COLUMN note text", metadata: recorded},
		{name: "unrecorded", ddl: "DROP TABLE public.legacy", wantErr: "the plan does not record the table this statement drops"},
		{name: "cascade", ddl: "DROP TABLE public.legacy CASCADE", metadata: recorded, wantErr: "does not drop exactly this table"},
		{name: "another schema", ddl: "DROP TABLE billing.legacy", metadata: recorded, wantErr: "does not drop exactly this table"},
		{name: "two tables", ddl: "DROP TABLE public.legacy, public.other", metadata: recorded, wantErr: "does not drop exactly this table"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			change, ok, err := tableDropForApply("app", "public", engine.TableChange{Table: "legacy", DDL: tt.ddl, Metadata: tt.metadata}, []string{tt.ddl})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantDrop, ok)
			if tt.wantDrop {
				assert.Equal(t, "table legacy\n", change.droppedTable)
				assert.Equal(t, "public", change.namespace)
				assert.Equal(t, "app", change.database)
			}
		})
	}
}
