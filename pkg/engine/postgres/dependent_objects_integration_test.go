//go:build integration

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/testutil"
)

// An owner drops a column that a publication names in its column list, a
// dependent the plan does not read. PostgreSQL refuses the drop while the
// publication depends on the column. The apply ends at once rather
// than retrying, since the dependency stays until someone removes it, and
// its error names the publication, which the owner of a hosted deployment
// could not otherwise find. The column stays.
func TestEngineApplyDropOfAColumnAPublicationNames(t *testing.T) {
	const database = "apply_dependent_objects_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.orders (id bigint PRIMARY KEY, amt numeric NOT NULL, note text);
		INSERT INTO public.orders VALUES (1, 10, 'first');
		CREATE PUBLICATION orders_pub FOR TABLE public.orders (id, note)`)
	require.NoError(t, err)
	eng := New()

	plan := planForeignKeyFiles(t, eng, database, dsn, map[string]string{
		"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, amt numeric NOT NULL)",
	})
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	require.Equal(t, "ALTER TABLE public.orders DROP note", plan.Changes[0].TableChanges[0].DDL)

	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.False(t, outcomes[0].Retryable, "the publication keeps depending on the column, so a retry meets the same refusal")
	assert.Equal(t, `other objects depend on what the statement on "orders" drops: publication of table orders in publication orders_pub depends on column note of table orders; remove the dependency, then apply again`, outcomes[0].ErrorMessage)

	var columns int
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'note'`).Scan(&columns))
	assert.Equal(t, 1, columns, "the column stays")
}
