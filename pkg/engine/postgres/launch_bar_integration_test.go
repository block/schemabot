//go:build integration

package postgres

import (
	"strings"
	"testing"

	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/testutil"
)

// launchBarSchema is a typical application's schema: tables that reference
// each other, a table tuned with storage parameters, and one table for each
// shape a schema file cannot describe yet. orders is the biggest table.
const launchBarSchema = `
	CREATE TABLE public.users (id bigint PRIMARY KEY, email text NOT NULL UNIQUE);
	CREATE TABLE public.orders (
		id bigint PRIMARY KEY,
		user_id bigint NOT NULL REFERENCES public.users (id) ON DELETE CASCADE,
		total numeric(12,2) NOT NULL);
	CREATE TABLE public.events (id bigint PRIMARY KEY, payload text) WITH (fillfactor = 70);

	CREATE TABLE public.accounts (id bigint PRIMARY KEY, updated_at timestamptz NOT NULL DEFAULT now());
	CREATE FUNCTION public.set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN NEW.updated_at := now(); RETURN NEW; END $$;
	CREATE TRIGGER accounts_set_updated_at BEFORE UPDATE ON public.accounts
		FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
	CREATE TABLE public.notes (id bigint PRIMARY KEY, body text);
	COMMENT ON TABLE public.notes IS 'free-form notes';
	COMMENT ON COLUMN public.notes.body IS 'markdown';
	CREATE TABLE public.tags (id bigint PRIMARY KEY, name text COLLATE "C" NOT NULL);
	CREATE UNLOGGED TABLE public.sessions (id bigint PRIMARY KEY);
	CREATE TABLE public.records (id bigint PRIMARY KEY);
	CREATE TABLE public.archived_records (archived_at timestamptz) INHERITS (public.records);
	CREATE SEQUENCE public.invoice_numbers;
	CREATE TABLE public.invoices (id bigint PRIMARY KEY, number bigint NOT NULL DEFAULT nextval('public.invoice_numbers'));

	INSERT INTO public.users SELECT g, 'user' || g || '@example.com' FROM generate_series(1, 100) AS g;
	INSERT INTO public.orders SELECT g, 1 + g % 100, g FROM generate_series(1, 20000) AS g;`

// A team brings an existing database to SchemaBot. pull writes every table a
// schema file can describe, foreign keys and storage parameters included, and
// skips each table it cannot describe, naming what it carries. The pulled
// files, with the skipped tables ignored, plan no change. The team's first
// pull request adds a nullable column to the biggest table, with the size
// limit set below that table's size. The column changes only the catalog, so
// it plans as runnable, the apply completes, and the column exists.
func TestEnginePostgresLaunchBar(t *testing.T) {
	const database = "launch_bar_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), launchBarSchema)
	require.NoError(t, err)
	credentials := &engine.Credentials{DSN: dsn}

	response, err := NewForTarget(0, 0, database, credentials).PullSchema(t.Context(),
		&ternv1.PullSchemaRequest{Namespace: "public", SkipUnrepresentableTables: true})
	require.NoError(t, err)
	pulled := response.Namespaces["public"]
	require.NotNil(t, pulled)

	assert.Equal(t, []string{"events", "orders", "users"}, sortedKeys(pulled.Tables))
	assert.Contains(t, pulled.Tables["orders"], `CONSTRAINT "orders_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE`)
	assert.Contains(t, pulled.Tables["events"], "WITH (fillfactor='70')")

	skipped := map[string]string{}
	for _, table := range pulled.SkippedTables {
		skipped[table.GetTable()] = table.GetReason()
	}
	assert.Equal(t, map[string]string{
		"accounts":         "schema files cannot describe its trigger",
		"notes":            "schema files cannot describe its comment",
		"tags":             schemadiff.ErrUnrenderableCollation.Error(),
		"sessions":         schemadiff.ErrUnrenderableUnlogged.Error(),
		"archived_records": "schema files cannot describe its table inheritance",
		"records":          schemadiff.ErrUnrenderableInheritance.Error(),
		"invoices":         schemadiff.ErrUnrenderableDefault.Error(),
	}, skipped)

	ignored := sortedKeys(skipped)
	files := pulledSchemaFiles(response)
	baseline, err := New().Plan(t.Context(), &engine.PlanRequest{
		Database: database, SchemaFiles: files, Credentials: credentials, IgnoreTables: ignored,
	})
	require.NoError(t, err)
	assert.Empty(t, baseline.Changes, "the pulled files and the skipped tables' ignore_tables entries plan nothing")

	var ordersBytes int64
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT pg_total_relation_size('public.orders')").Scan(&ordersBytes))
	sizeLimit := ordersBytes / 2
	eng := NewWithTableSizeLimit(sizeLimit)

	files["public"].Files["orders.sql"] = withColumn(t, pulled.Tables["orders"], `"total" numeric(12,2) NOT NULL`, `"shipped_at" timestamp with time zone`)
	planned, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: database, SchemaFiles: files, Credentials: credentials, IgnoreTables: ignored,
	})
	require.NoError(t, err)
	require.Len(t, planned.Changes, 1)
	require.Len(t, planned.Changes[0].TableChanges, 1)
	change := planned.Changes[0].TableChanges[0]
	assert.Equal(t, "orders", change.Table)
	assert.Contains(t, change.DDL, "ADD COLUMN")
	assert.Contains(t, change.DDL, "shipped_at")
	assert.Empty(t, change.ExecutionMode, "a catalog-only column runs on a table past the size limit: %s", change.ModeReason)

	result, err := eng.Apply(t.Context(), plannedApplyRequest(dsn, change))
	require.NoError(t, err)
	assert.True(t, result.Accepted)
	progress := awaitPostgresProgress(t, eng, "orders")
	assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)

	var dataType string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT data_type FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'shipped_at'`).Scan(&dataType))
	assert.Equal(t, "timestamp with time zone", dataType)
}

// withColumn adds a column definition after the named one in a pulled
// CREATE TABLE, as a developer editing the file would.
func withColumn(t *testing.T, file, after, column string) string {
	t.Helper()
	require.Contains(t, file, after)
	return strings.Replace(file, after, after+",\n    "+column, 1)
}
