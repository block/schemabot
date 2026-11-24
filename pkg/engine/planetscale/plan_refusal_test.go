package planetscale

import (
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

// A declared schema the PlanetScale engine will not plan as written is
// reported as a plan refusal, not as a failure of the engine, and keeps its
// message word for word: the message tells whoever edits the schema files or
// the config what to change. Each case is a refusal the engine reaches without
// calling PlanetScale.
func TestPlanRefusalsAreTyped(t *testing.T) {
	const orders = "CREATE TABLE orders (id bigint NOT NULL PRIMARY KEY)"

	tests := []struct {
		name    string
		refuse  func() error
		wantErr string
	}{
		{
			name: "drop_tables on a PlanetScale target",
			refuse: func() error {
				_, err := New(nil).Plan(t.Context(), &engine.PlanRequest{
					Database:   "commerce",
					DropTables: []string{"orders_old"},
				})
				return err
			},
			wantErr: "plan database commerce: drop_tables is supported only on MySQL, and this is a PlanetScale target. Remove drop_tables from schemabot.yaml",
		},
		{
			name: "a declared table the config ignores",
			refuse: func() error {
				req := ignoreTablesRequest(map[string]map[string]string{"commerce": {"orders.sql": orders}}, "orders")
				currentSchema := map[string][]table.TableSchema{"commerce": {{Name: "orders", Schema: orders}}}
				_, err := New(nil).withholdIgnoredTables(mustIgnoredTables(t, req.IgnoreTables), req, []string{"commerce"}, currentSchema)
				return err
			},
			wantErr: `ignore_tables entry "orders" is also declared by a schema file in namespace "commerce". Remove the entry or the schema file`,
		},
		{
			name: "a table declared by two schema files",
			refuse: func() error {
				_, err := parseDesiredSchemas("commerce", &schema.Namespace{Files: map[string]string{
					"orders.sql":        orders,
					"orders_extra.sql":  orders,
					"products.sql":      "CREATE TABLE products (id bigint NOT NULL PRIMARY KEY)",
					"not_a_schema.json": "{}",
				}})
				return err
			},
			wantErr: `table "orders" is declared by both schema files "commerce/orders.sql" and "commerce/orders_extra.sql". Declare each table in exactly one schema file`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.refuse()
			require.Error(t, err)
			assert.True(t, engine.IsPlanRefused(err), "the refusal must be typed so consumers do not treat it as an engine failure")
			assert.EqualError(t, err, tc.wantErr)
		})
	}
}
