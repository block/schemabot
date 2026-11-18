package planetscale

import (
	"io"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
)

// mustIgnoredTables indexes ignore_tables entries the test knows are valid.
func mustIgnoredTables(t *testing.T, entries []string) engine.IgnoredTables {
	t.Helper()
	ignored, err := engine.NewIgnoredTables(entries)
	require.NoError(t, err)
	return ignored
}

func ignoreTablesRequest(files map[string]map[string]string, ignore ...string) *engine.PlanRequest {
	schemaFiles := make(schema.SchemaFiles, len(files))
	for keyspace, keyspaceFiles := range files {
		schemaFiles[keyspace] = &schema.Namespace{Files: keyspaceFiles}
	}
	return &engine.PlanRequest{
		Database:     "commerce",
		SchemaFiles:  schemaFiles,
		IgnoreTables: ignore,
	}
}

// Each keyspace's live schema is filtered on its own, and the disclosure names
// the keyspace the table was withheld from. An entry is not keyspace-qualified,
// so a table name occurring in two keyspaces is withheld in both.
func TestWithholdIgnoredTablesFiltersEachKeyspace(t *testing.T) {
	req := ignoreTablesRequest(map[string]map[string]string{
		"commerce":         {"orders.sql": "CREATE TABLE orders (id bigint NOT NULL PRIMARY KEY)"},
		"commerce_sharded": {"products.sql": "CREATE TABLE products (id bigint NOT NULL PRIMARY KEY)"},
	}, "flyway_schema_history")
	currentSchema := map[string][]table.TableSchema{
		"commerce": {
			{Name: "orders", Schema: "CREATE TABLE orders (id bigint NOT NULL PRIMARY KEY)"},
			{Name: "flyway_schema_history", Schema: "CREATE TABLE flyway_schema_history (installed_rank int NOT NULL PRIMARY KEY)"},
		},
		"commerce_sharded": {
			{Name: "products", Schema: "CREATE TABLE products (id bigint NOT NULL PRIMARY KEY)"},
			{Name: "flyway_schema_history", Schema: "CREATE TABLE flyway_schema_history (installed_rank int NOT NULL PRIMARY KEY)"},
		},
	}

	exempt, err := New(nil).withholdIgnoredTables(
		mustIgnoredTables(t, req.IgnoreTables), req,
		[]string{"commerce", "commerce_sharded"}, currentSchema)
	require.NoError(t, err)

	require.Len(t, exempt, 2)
	assert.Equal(t, "commerce", exempt[0].Namespace)
	assert.Equal(t, []string{"flyway_schema_history"}, exempt[0].Tables)
	assert.Equal(t, engine.ExemptReasonIgnoreTables, exempt[0].Reason)
	assert.Equal(t, "commerce_sharded", exempt[1].Namespace)
	assert.Equal(t, []string{"flyway_schema_history"}, exempt[1].Tables)

	// The withheld table is gone from the schema the diff will see, so it has
	// no declaring file to miss and is never proposed for DROP TABLE.
	assert.Equal(t, []string{"orders"}, tableSchemaNames(currentSchema["commerce"]))
	assert.Equal(t, []string{"products"}, tableSchemaNames(currentSchema["commerce_sharded"]))
}

// A config that withholds nothing leaves the live schema untouched and
// discloses nothing, which is the ordinary plan.
func TestWithholdIgnoredTablesWithoutEntriesIsANoOp(t *testing.T) {
	req := ignoreTablesRequest(map[string]map[string]string{
		"commerce": {"orders.sql": "CREATE TABLE orders (id bigint NOT NULL PRIMARY KEY)"},
	})
	currentSchema := map[string][]table.TableSchema{
		"commerce": {{Name: "flyway_schema_history", Schema: "CREATE TABLE flyway_schema_history (installed_rank int NOT NULL PRIMARY KEY)"}},
	}

	exempt, err := New(nil).withholdIgnoredTables(
		mustIgnoredTables(t, nil), req, []string{"commerce"}, currentSchema)
	require.NoError(t, err)

	assert.Empty(t, exempt)
	assert.Equal(t, []string{"flyway_schema_history"}, tableSchemaNames(currentSchema["commerce"]),
		"the table stays visible to the plan, which is free to propose dropping it")
}

// An entry matching no live table withholds nothing in that keyspace, so no
// disclosure is made for an exclusion that is not happening. Withholding is
// exact and case-sensitive: an entry must never withhold a table it does not
// name, so a differently-cased live table stays visible to the plan and the
// entry is left for the caller to report as unmatched.
func TestWithholdIgnoredTablesMatchingNothingDisclosesNothing(t *testing.T) {
	req := ignoreTablesRequest(map[string]map[string]string{
		"commerce": {"orders.sql": "CREATE TABLE orders (id bigint NOT NULL PRIMARY KEY)"},
	}, "never_existed", "Flyway_Schema_History")
	currentSchema := map[string][]table.TableSchema{
		"commerce": {
			{Name: "orders", Schema: "CREATE TABLE orders (id bigint NOT NULL PRIMARY KEY)"},
			{Name: "flyway_schema_history", Schema: "CREATE TABLE flyway_schema_history (installed_rank int NOT NULL PRIMARY KEY)"},
		},
	}

	exempt, err := New(nil).withholdIgnoredTables(
		mustIgnoredTables(t, req.IgnoreTables), req, []string{"commerce"}, currentSchema)
	require.NoError(t, err)

	assert.Empty(t, exempt, "matching is exact and case-sensitive")
	assert.Equal(t, []string{"orders", "flyway_schema_history"}, tableSchemaNames(currentSchema["commerce"]),
		"the plan is free to propose dropping the table the entry failed to name")
}

// A keyspace whose schema files declare a withheld table is refused: the
// declaration and the ignore contradict each other, and the diff would resolve
// that by proposing to create a table that already exists.
func TestWithholdIgnoredTablesRefusesDeclaredTable(t *testing.T) {
	req := ignoreTablesRequest(map[string]map[string]string{
		"commerce": {"bookkeeping.sql": "CREATE TABLE flyway_schema_history (installed_rank int NOT NULL PRIMARY KEY)"},
	}, "flyway_schema_history")
	currentSchema := map[string][]table.TableSchema{
		"commerce": {{Name: "flyway_schema_history", Schema: "CREATE TABLE flyway_schema_history (installed_rank int NOT NULL PRIMARY KEY)"}},
	}

	_, err := New(nil).withholdIgnoredTables(
		mustIgnoredTables(t, req.IgnoreTables), req, []string{"commerce"}, currentSchema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flyway_schema_history")
	assert.Contains(t, err.Error(), `namespace "commerce"`)
	assert.Contains(t, err.Error(), "Remove the entry or the schema file")

	assert.Equal(t, []string{"flyway_schema_history"}, tableSchemaNames(currentSchema["commerce"]),
		"a refused plan leaves the live schema as it found it")
}

// A keyspace whose files declare a table the config names in a different case
// is refused too: where a database folds identifiers the two spellings are one
// table, and the plan does not ask the target which it is — a contradiction
// through is an apply that fails part way, a contradiction refused is a plan
// and an error naming both spellings.
func TestWithholdIgnoredTablesRefusesDeclaredTableWhateverTheCase(t *testing.T) {
	req := ignoreTablesRequest(map[string]map[string]string{
		"commerce": {"bookkeeping.sql": "CREATE TABLE Flyway_Schema_History (installed_rank int NOT NULL PRIMARY KEY)"},
	}, "flyway_schema_history")
	currentSchema := map[string][]table.TableSchema{
		"commerce": {{Name: "flyway_schema_history", Schema: "CREATE TABLE flyway_schema_history (installed_rank int NOT NULL PRIMARY KEY)"}},
	}

	_, err := New(nil).withholdIgnoredTables(
		mustIgnoredTables(t, req.IgnoreTables), req, []string{"commerce"}, currentSchema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"flyway_schema_history" (the file spells it "Flyway_Schema_History")`)

	assert.Equal(t, []string{"flyway_schema_history"}, tableSchemaNames(currentSchema["commerce"]),
		"a refused plan leaves the live schema as it found it")
}

// A keyspace the plan covers with no schema files at all cannot be checked for
// the declared-and-ignored contradiction, so the plan fails closed rather than
// withholding tables from a keyspace whose declarations it never read.
func TestWithholdIgnoredTablesRefusesKeyspaceWithoutFiles(t *testing.T) {
	req := ignoreTablesRequest(map[string]map[string]string{
		"commerce": {"orders.sql": "CREATE TABLE orders (id bigint NOT NULL PRIMARY KEY)"},
	}, "flyway_schema_history")
	currentSchema := map[string][]table.TableSchema{
		"commerce_sharded": {{Name: "flyway_schema_history", Schema: "CREATE TABLE flyway_schema_history (installed_rank int NOT NULL PRIMARY KEY)"}},
	}

	_, err := New(nil).withholdIgnoredTables(
		mustIgnoredTables(t, req.IgnoreTables), req,
		[]string{"commerce", "commerce_sharded"}, currentSchema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `plan keyspace "commerce_sharded": schema files are required`)
}

// An application that creates one table per configured trigger leaves a
// family of undeclared tables in a keyspace, and a pattern entry withholds the
// whole family there and discloses each member, while a declared table the
// pattern does not match stays in the diff.
func TestWithholdIgnoredTablesWithholdsPatternFamily(t *testing.T) {
	req := ignoreTablesRequest(map[string]map[string]string{
		"commerce":         {"orders.sql": "CREATE TABLE orders (id bigint NOT NULL PRIMARY KEY)"},
		"commerce_sharded": {"products.sql": "CREATE TABLE products (id bigint NOT NULL PRIMARY KEY)"},
	}, `/^relay_\d+_feed$/`)
	feed := func(name string) table.TableSchema {
		return table.TableSchema{Name: name, Schema: "CREATE TABLE " + name + " (id bigint NOT NULL PRIMARY KEY)"}
	}
	currentSchema := map[string][]table.TableSchema{
		"commerce":         {feed("orders"), feed("relay_2_feed"), feed("relay_10_feed"), feed("relay_feed_settings")},
		"commerce_sharded": {feed("products"), feed("relay_7_feed")},
	}

	exempt, err := New(nil).withholdIgnoredTables(
		mustIgnoredTables(t, req.IgnoreTables), req,
		[]string{"commerce", "commerce_sharded"}, currentSchema)
	require.NoError(t, err)

	require.Len(t, exempt, 2)
	assert.Equal(t, "commerce", exempt[0].Namespace)
	assert.Equal(t, []string{"relay_10_feed", "relay_2_feed"}, exempt[0].Tables)
	assert.Equal(t, engine.ExemptReasonIgnoreTables, exempt[0].Reason)
	assert.Equal(t, "commerce_sharded", exempt[1].Namespace)
	assert.Equal(t, []string{"relay_7_feed"}, exempt[1].Tables)

	assert.Equal(t, []string{"orders", "relay_feed_settings"}, tableSchemaNames(currentSchema["commerce"]),
		"a table the pattern does not match in full stays in the diff")
	assert.Equal(t, []string{"products"}, tableSchemaNames(currentSchema["commerce_sharded"]))
}

// A pattern that also reaches a table a keyspace's files declare is refused,
// the same as a plain entry naming it.
func TestWithholdIgnoredTablesRefusesPatternMatchingDeclaredTable(t *testing.T) {
	req := ignoreTablesRequest(map[string]map[string]string{
		"commerce": {"relay_1_feed.sql": "CREATE TABLE relay_1_feed (id bigint NOT NULL PRIMARY KEY)"},
	}, `/^relay_\d+_feed$/`)
	currentSchema := map[string][]table.TableSchema{
		"commerce": {{Name: "relay_1_feed", Schema: "CREATE TABLE relay_1_feed (id bigint NOT NULL PRIMARY KEY)"}},
	}

	_, err := New(nil).withholdIgnoredTables(
		mustIgnoredTables(t, req.IgnoreTables), req, []string{"commerce"}, currentSchema)
	require.Error(t, err)
	assert.Equal(t,
		`ignore_tables entry "/^relay_\d+_feed$/" matches "relay_1_feed", which a schema file in namespace "commerce" declares. Narrow the pattern so it no longer matches it, or remove the schema file`,
		err.Error())
}

// The apply-side comparison of a branch against the declared schema leaves out
// the same pattern-matched tables the plan withheld, so a family member on the
// branch is not read as drift.
func TestWithoutIgnoredTablesHonorsPatterns(t *testing.T) {
	live := map[string][]table.TableSchema{
		"commerce": {{Name: "orders"}, {Name: "relay_3_feed"}, {Name: "relay_3_feed_old"}},
	}
	kept := withoutIgnoredTables(live, mustIgnoredTables(t, []string{`/^relay_\d+_feed$/`}))
	assert.Equal(t, []string{"orders", "relay_3_feed_old"}, tableSchemaNames(kept["commerce"]))
}

// A pattern that does not compile fails the plan and the apply that carry it,
// and the apply fails before it creates a branch it would have to clean up.
func TestInvalidIgnoreTablesPatternFailsPlanAndApply(t *testing.T) {
	e := NewWithClient(slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(_, _ string) (psclient.PSClient, error) { return &emptyMainBranchClient{}, nil })
	_, err := e.Plan(t.Context(), &engine.PlanRequest{
		Database:     "commerce",
		SchemaFiles:  schema.SchemaFiles{"commerce": &schema.Namespace{Files: map[string]string{"orders.sql": "CREATE TABLE orders (id bigint NOT NULL PRIMARY KEY)"}}},
		Credentials:  &engine.Credentials{Metadata: map[string]string{"organization": "org", "token_name": "tn", "token_value": "tv"}},
		IgnoreTables: []string{`/relay_(\d+_feed/`},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `ignore_tables entry "/relay_(\d+_feed/" is not a valid regular expression`)

	client := &branchLifecycleClient{}
	_, err = conformanceEngine(client).Apply(t.Context(), &engine.ApplyRequest{
		PlanID:       "plan-0123456789abcdef",
		Database:     "commerce",
		Credentials:  conformanceCredentials(),
		IgnoreTables: []string{`/relay_(\d+_feed/`},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `ignore_tables entry "/relay_(\d+_feed/" is not a valid regular expression`)
	created, _ := client.snapshot()
	assert.Empty(t, created, "no branch is created for an apply that cannot read its exclusions")
}

func tableSchemaNames(schemas []table.TableSchema) []string {
	names := make([]string, 0, len(schemas))
	for _, ts := range schemas {
		names = append(names, ts.Name)
	}
	return names
}
