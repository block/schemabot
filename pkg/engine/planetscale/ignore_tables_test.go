package planetscale

import (
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

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
		engine.NewIgnoredTables(req.IgnoreTables), req,
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
		engine.NewIgnoredTables(nil), req, []string{"commerce"}, currentSchema)
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
		engine.NewIgnoredTables(req.IgnoreTables), req, []string{"commerce"}, currentSchema)
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
		engine.NewIgnoredTables(req.IgnoreTables), req, []string{"commerce"}, currentSchema)
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
		engine.NewIgnoredTables(req.IgnoreTables), req, []string{"commerce"}, currentSchema)
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
		engine.NewIgnoredTables(req.IgnoreTables), req,
		[]string{"commerce", "commerce_sharded"}, currentSchema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `plan keyspace "commerce_sharded": schema files are required`)
}

func tableSchemaNames(schemas []table.TableSchema) []string {
	names := make([]string, 0, len(schemas))
	for _, ts := range schemas {
		names = append(names, ts.Name)
	}
	return names
}
