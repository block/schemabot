//go:build integration

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// A schema whose files declare foreign keys plans: the change a foreign key
// does not touch is planned to run, while each foreign-key change, and a new
// table that declares one, is shown for review and blocked, since SchemaBot
// does not apply foreign keys on PostgreSQL yet.
func TestEnginePlanForeignKeys(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_foreign_keys_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL);
		CREATE TABLE public.payments (id bigint PRIMARY KEY, order_id bigint NOT NULL REFERENCES public.orders (id))`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: "plan_foreign_keys_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql":     "CREATE TABLE users (id bigint PRIMARY KEY)",
				"orders.sql":    "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id), note text)",
				"payments.sql":  "CREATE TABLE payments (id bigint PRIMARY KEY, order_id bigint NOT NULL)",
				"shipments.sql": "CREATE TABLE shipments (id bigint PRIMARY KEY, order_id bigint NOT NULL REFERENCES orders (id))",
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)

	byDDL := map[string]engine.TableChange{}
	assert.Len(t, result.Changes, 1)
	for _, sc := range result.Changes {
		for _, tc := range sc.TableChanges {
			byDDL[tc.DDL] = tc
		}
	}

	addColumn, ok := byDDL["ALTER TABLE public.orders ADD COLUMN note text"]
	require.True(t, ok, "the column add is planned")
	assert.NotEqual(t, engine.ExecutionModeBlocked, addColumn.ExecutionMode)

	addFK, ok := byDDL["ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id)"]
	require.True(t, ok, "the foreign-key add is planned")
	assert.Equal(t, engine.ExecutionModeBlocked, addFK.ExecutionMode)
	assert.Equal(t, `statement for table "orders" adds a foreign key, which SchemaBot plans but does not apply on PostgreSQL yet; run this statement outside SchemaBot`, addFK.ModeReason)

	dropFK, ok := byDDL["ALTER TABLE public.payments DROP CONSTRAINT payments_order_id_fkey"]
	require.True(t, ok, "the undeclared live foreign key is planned as a drop")
	assert.Equal(t, engine.ExecutionModeBlocked, dropFK.ExecutionMode)
	assert.Contains(t, dropFK.ModeReason, `statement for table "payments" drops a foreign key, which SchemaBot plans but does not apply on PostgreSQL yet`)

	create, ok := byDDL["CREATE TABLE public.shipments (id bigint PRIMARY KEY, order_id bigint NOT NULL REFERENCES orders (id))"]
	require.True(t, ok, "the new table is planned")
	assert.Equal(t, engine.ExecutionModeBlocked, create.ExecutionMode)
	assert.Contains(t, create.ModeReason, `statement for table "shipments" declares a foreign key, which SchemaBot plans but does not apply on PostgreSQL yet`)
}
