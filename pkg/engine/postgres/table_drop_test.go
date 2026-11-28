package postgres

import (
	"testing"

	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// referencing returns a model of table with one foreign key named constraint,
// referenced by the given foreign keys of other tables.
func referencing(table, constraint string, referencedBy ...schemadiff.IncomingForeignKey) schemadiff.Model {
	return schemadiff.Model{
		Table:               table,
		Constraints:         []schemadiff.Constraint{{Name: constraint, Def: "FOREIGN KEY (ref_id) REFERENCES other(id)"}},
		IncomingForeignKeys: referencedBy,
	}
}

// fk names a foreign key of a table in the public schema.
func fk(table, constraint string) schemadiff.IncomingForeignKey {
	return schemadiff.IncomingForeignKey{Schema: "public", Table: table, Constraint: constraint}
}

// A chain of foreign keys among dropped tables orders the drops so each
// table goes after every table that references it, and each definition
// leaves out the references that are gone by the time the drop runs.
func TestPlanTableDropsOrdersReferencingTablesFirst(t *testing.T) {
	plan := planTableDrops("public", map[string]schemadiff.Model{
		"a_regions": {
			Table:               "a_regions",
			ReferencedBy:        []string{"warehouses.warehouses_region_fkey"},
			IncomingForeignKeys: []schemadiff.IncomingForeignKey{fk("warehouses", "warehouses_region_fkey")},
		},
		"warehouses":   referencing("warehouses", "warehouses_region_fkey", fk("z_shipments", "z_shipments_warehouse_fkey")),
		"z_shipments":  referencing("z_shipments", "z_shipments_warehouse_fkey"),
		"m_standalone": {Table: "m_standalone"},
	}, nil)
	assert.Equal(t, []string{"m_standalone", "z_shipments", "warehouses", "a_regions"}, plan.order)
	assert.Empty(t, plan.blocked)
	assert.Equal(t, "table a_regions\n", plan.definitions["a_regions"])
	assert.NotContains(t, plan.definitions["warehouses"], "referenced by")
}

// A table and a constraint can share their names with a table and a
// constraint in another schema. A foreign key of billing.orders keeps
// accounts, even though public.orders, which the plan drops, has a foreign
// key of the same name. The reason says the referrer is in another
// namespace, whose drops this plan cannot order its own after.
func TestPlanTableDropsKeepsATableReferencedFromAnotherSchema(t *testing.T) {
	plan := planTableDrops("public", map[string]schemadiff.Model{
		"accounts": {Table: "accounts", IncomingForeignKeys: []schemadiff.IncomingForeignKey{
			{Schema: "billing", Table: "orders", Constraint: "orders_user_fkey"},
		}},
		"orders": referencing("orders", "orders_user_fkey"),
	}, nil)
	assert.Equal(t, []string{"orders"}, plan.order)
	assert.Contains(t, plan.blocked["accounts"][0],
		`foreign key constraint(s) "billing.orders.orders_user_fkey" in other namespaces reference it, and drops in different namespaces have no guaranteed order`)
}

// A table that a kept table references is blocked, and blocking it keeps
// the tables it references in turn: tags is referenced by a table that
// stays, so tags stays, and so does colors, which tags references.
func TestPlanTableDropsBlocksTablesThatStayReferenced(t *testing.T) {
	plan := planTableDrops("public", map[string]schemadiff.Model{
		"colors": {Table: "colors", IncomingForeignKeys: []schemadiff.IncomingForeignKey{fk("tags", "tags_color_fkey")}},
		"tags":   referencing("tags", "tags_color_fkey", fk("posts", "posts_tag_fkey")),
	}, nil)
	assert.Empty(t, plan.order)
	assert.Contains(t, plan.blocked["tags"][0], `foreign key constraint(s) "posts.posts_tag_fkey" of tables that stay reference it`)
	assert.Contains(t, plan.blocked["colors"][0], `"tags.tags_color_fkey"`)
}

// A table the target refuses to drop, such as one a view reads, is
// blocked with the target's reasons, and the tables it references stay
// with it.
func TestPlanTableDropsBlocksATableTheTargetRefuses(t *testing.T) {
	plan := planTableDrops("public", map[string]schemadiff.Model{
		"users":  {Table: "users", IncomingForeignKeys: []schemadiff.IncomingForeignKey{fk("orders", "orders_user_fkey")}},
		"orders": referencing("orders", "orders_user_fkey"),
	}, map[string][]string{"orders": {"view public.open_orders depends on it", "the engine role cannot drop it"}})
	assert.Empty(t, plan.order)
	assert.Equal(t, []string{
		`table "orders" exists on the target but no schema file in namespace "public" declares it; view public.open_orders depends on it`,
		"the engine role cannot drop it",
	}, plan.blocked["orders"])
	assert.Contains(t, plan.blocked["users"][0], `"orders.orders_user_fkey"`)
}

// Partitioned tables, partitions, and inheritance parents and children are
// blocked, and so is a cycle of foreign keys, which no order satisfies. A
// table the cycle references is not on it: colors is blocked because ping,
// which stays, references it.
func TestPlanTableDropsBlocksHierarchiesAndCycles(t *testing.T) {
	plan := planTableDrops("public", map[string]schemadiff.Model{
		"events":  {Table: "events", PartitionKey: "RANGE (day)"},
		"base":    {Table: "base", InheritanceChildren: []string{"public.derived"}},
		"derived": {Table: "derived", InheritsParents: []string{"public.base"}},
		"ping": {Table: "ping", Constraints: []schemadiff.Constraint{
			{Name: "ping_pong_fkey", Def: "FOREIGN KEY (pong_id) REFERENCES pong(id)"},
			{Name: "ping_color_fkey", Def: "FOREIGN KEY (color_id) REFERENCES colors(id)"},
		}, IncomingForeignKeys: []schemadiff.IncomingForeignKey{fk("pong", "pong_ping_fkey")}},
		"pong":   referencing("pong", "pong_ping_fkey", fk("ping", "ping_pong_fkey")),
		"colors": {Table: "colors", IncomingForeignKeys: []schemadiff.IncomingForeignKey{fk("ping", "ping_color_fkey")}},
	}, nil)
	assert.Empty(t, plan.order)
	for _, table := range []string{"events", "base", "derived"} {
		assert.Contains(t, plan.blocked[table][0], "does not drop a partitioned table or a table in an inheritance hierarchy", table)
	}
	for _, table := range []string{"ping", "pong"} {
		assert.Contains(t, plan.blocked[table][0], "its foreign keys form a cycle", table)
	}
	assert.Contains(t, plan.blocked["colors"][0], `foreign key constraint(s) "ping.ping_color_fkey" of tables that stay reference it`)
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

// The reason a table drop is refused for want of ownership ends in the
// statement to run and, when the grant targets another role, the hint that
// says so. The catalog check that found the access missing is left out.
func TestPrivilegeDropReason(t *testing.T) {
	privilege := &preflight.PrivilegeError{
		Tier:  tableDropTier,
		Check: "pg_has_role(drop_limited, app_owner, 'USAGE')",
		Grant: `GRANT "app_owner" TO "drop_limited"`,
	}
	assert.Equal(t, `the engine role cannot drop it, because only the table's owner can drop a table — provision with: GRANT "app_owner" TO "drop_limited"`,
		privilegeDropReason(privilege))

	privilege.Hint = "the grant targets the owning role"
	assert.Equal(t, `the engine role cannot drop it, because only the table's owner can drop a table — provision with: GRANT "app_owner" TO "drop_limited"; the grant targets the owning role`,
		privilegeDropReason(privilege))
}
