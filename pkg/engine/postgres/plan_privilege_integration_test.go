//go:build integration

package postgres

import (
	"database/sql"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// planningRoleDSN creates a login role with the grants given and returns the
// DSN that logs in as it. The password is the role's name.
func planningRoleDSN(t *testing.T, db *sql.DB, dsn, role, grants string) string {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `CREATE ROLE `+role+` LOGIN PASSWORD '`+role+`'; `+grants)
	require.NoError(t, err)
	planner, err := url.Parse(dsn)
	require.NoError(t, err)
	planner.User = url.UserPassword(role, role)
	return planner.String()
}

// SchemaBot's role is missing one of the grants planning needs: USAGE on a
// schema created after it was provisioned, SELECT on a table a foreign key
// references, or CREATE on the database for the plan's scratch schema. Each
// plan is refused with a reason that names the privilege and ends in the
// grant to make, rather than failing as an internal error that a retry would
// only repeat.
func TestEnginePlanRefusesWhatThePlanningRoleCannotRead(t *testing.T) {
	const database = "plan_privilege"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.customers (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, customer_id bigint);
		CREATE SCHEMA billing;
		CREATE TABLE billing.invoices (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	plan := func(t *testing.T, plannerDSN string, files schema.SchemaFiles) error {
		t.Helper()
		_, err := New().Plan(t.Context(), &engine.PlanRequest{
			Database:    database,
			SchemaFiles: files,
			Credentials: &engine.Credentials{DSN: plannerDSN},
		})
		require.Error(t, err)
		return err
	}

	t.Run("a schema the role has no USAGE on", func(t *testing.T) {
		planner := planningRoleDSN(t, db, dsn, "plan_no_usage",
			`GRANT CONNECT, CREATE ON DATABASE `+database+` TO plan_no_usage; GRANT USAGE ON SCHEMA public TO plan_no_usage`)
		err := plan(t, planner, schema.SchemaFiles{
			"billing": {Files: map[string]string{"invoices.sql": "CREATE TABLE invoices (id bigint PRIMARY KEY, total bigint)"}},
			"public":  {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text)"}},
		})
		assert.True(t, engine.IsPlanRefused(err), "%v", err)
		assert.EqualError(t, err, `grant SchemaBot's role the privilege this error names, then re-plan: permission denied for schema billing (SQLSTATE 42501; file "billing"/"invoices.sql")`)
	})

	t.Run("a table a foreign key references that the role cannot read", func(t *testing.T) {
		planner := planningRoleDSN(t, db, dsn, "plan_no_select",
			`GRANT CONNECT, CREATE ON DATABASE `+database+` TO plan_no_select; GRANT USAGE ON SCHEMA public TO plan_no_select`)
		err := plan(t, planner, schema.SchemaFiles{"public": {Files: map[string]string{
			"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, customer_id bigint REFERENCES customers (id))",
		}}})
		assert.True(t, engine.IsPlanRefused(err), "%v", err)
		assert.Contains(t, err.Error(), `plan PostgreSQL schema in "public"/"orders.sql": `)
		assert.Contains(t, err.Error(), "; grant the planning role SELECT on the referenced table, then re-plan")
	})

	t.Run("a database the role cannot create the scratch schema in", func(t *testing.T) {
		planner := planningRoleDSN(t, db, dsn, "plan_no_create",
			`GRANT CONNECT ON DATABASE `+database+` TO plan_no_create; GRANT USAGE ON SCHEMA public TO plan_no_create`)
		err := plan(t, planner, schema.SchemaFiles{"public": {Files: map[string]string{
			"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text)",
		}}})
		assert.True(t, engine.IsPlanRefused(err), "%v", err)
		assert.EqualError(t, err, `grant SchemaBot's role the privilege this error names, then re-plan: permission denied for database `+database+` (SQLSTATE 42501; file "public"/"users.sql")`)
	})
}
