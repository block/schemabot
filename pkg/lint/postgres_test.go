package lint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The primary key rule judges the type the column stores: an integer key is
// flagged whether it is declared inline or at table level, a bigint or uuid
// key passes, and a composite key is judged column by column so only the
// offending column is named.
func TestLintPostgresSchema_PrimaryKeyType(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
		want []Result
	}{
		{
			name: "inline integer key",
			ddl:  `CREATE TABLE orders (id integer PRIMARY KEY, total numeric(12,2));`,
			want: []Result{{
				Table: "orders", Column: "id", Linter: "primary_key", Severity: "warning",
				Message: `Primary key column "id" in table "orders" uses "integer"; allowed types: bigint, uuid`,
			}},
		},
		{
			name: "table-level bigint key",
			ddl:  `CREATE TABLE orders (id bigint NOT NULL, total numeric(12,2), CONSTRAINT orders_pkey PRIMARY KEY (id));`,
			want: nil,
		},
		{
			name: "uuid key",
			ddl:  `CREATE TABLE orders (id uuid PRIMARY KEY);`,
			want: nil,
		},
		{
			name: "composite key flags only the integer column",
			ddl:  `CREATE TABLE order_lines (tenant_id integer, id bigint, PRIMARY KEY (tenant_id, id));`,
			want: []Result{{
				Table: "order_lines", Column: "tenant_id", Linter: "primary_key", Severity: "warning",
				Message: `Primary key column "tenant_id" in table "order_lines" uses "integer"; allowed types: bigint, uuid`,
			}},
		},
		{
			name: "serial is judged as the integer it stores",
			ddl:  `CREATE TABLE a (id serial PRIMARY KEY);`,
			want: []Result{{
				Table: "a", Column: "id", Linter: "primary_key", Severity: "warning",
				Message: `Primary key column "id" in table "a" uses "serial"; allowed types: bigint, uuid`,
			}},
		},
		{
			name: "bigserial is judged as bigint",
			ddl:  `CREATE TABLE b (id bigserial PRIMARY KEY);`,
			want: nil,
		},
		{
			name: "identity column keeps its declared type",
			ddl:  `CREATE TABLE orders (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY);`,
			want: nil,
		},
		{
			name: "no primary key",
			ddl:  `CREATE TABLE events (occurred_at timestamptz NOT NULL, payload jsonb);`,
			want: []Result{{
				Table: "events", Linter: "primary_key", Severity: "warning",
				Message: "No primary key defined",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := lintOnePostgresEntry(t, tt.ddl)
			require.NoError(t, err)
			assert.Equal(t, tt.want, results)
		})
	}
}

// Binary floating point columns are flagged under every spelling the grammar
// accepts; exact numeric types are not.
func TestLintPostgresSchema_HasFloat(t *testing.T) {
	results, err := lintOnePostgresEntry(t, `CREATE TABLE prices (
		id bigint PRIMARY KEY,
		unit real,
		ratio double precision,
		narrow float(10),
		wide float,
		exact numeric(12,2),
		cents bigint
	);`)
	require.NoError(t, err)
	assert.Equal(t, []Result{
		{Table: "prices", Column: "narrow", Linter: "has_float", Severity: "warning", Message: `Column "narrow" in table "prices" uses "real" data type`},
		{Table: "prices", Column: "ratio", Linter: "has_float", Severity: "warning", Message: `Column "ratio" in table "prices" uses "double precision" data type`},
		{Table: "prices", Column: "unit", Linter: "has_float", Severity: "warning", Message: `Column "unit" in table "prices" uses "real" data type`},
		{Table: "prices", Column: "wide", Linter: "has_float", Severity: "warning", Message: `Column "wide" in table "prices" uses "double precision" data type`},
	}, results)
}

// A quoted mixed-case table name is flagged; PostgreSQL folds unquoted names
// to lowercase so those never trigger the rule.
func TestLintPostgresSchema_NameCase(t *testing.T) {
	results, err := lintOnePostgresEntry(t, `CREATE TABLE "Orders" (id bigint PRIMARY KEY);`)
	require.NoError(t, err)
	assert.Equal(t, []Result{{
		Table: "Orders", Linter: "name_case", Severity: "warning",
		Message: `table name "Orders" is not lowercase`,
	}}, results)

	results, err = lintOnePostgresEntry(t, `CREATE TABLE Orders (id bigint PRIMARY KEY);`)
	require.NoError(t, err)
	assert.Empty(t, results)
}

// A pulled entry is the rendered table: CREATE TABLE followed by that table's
// CREATE INDEX statements. The indexes are skipped rather than rejected.
func TestLintPostgresSchema_IndexStatementsAreSkipped(t *testing.T) {
	results, err := lintOnePostgresEntry(t, `CREATE TABLE orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL);
CREATE INDEX orders_customer_id_idx ON orders (customer_id);
CREATE UNIQUE INDEX orders_id_customer_idx ON orders (id, customer_id);`)
	require.NoError(t, err)
	assert.Empty(t, results)
}

// Tables in the ignore list are parsed but produce no findings, matching the
// MySQL audit's treatment of SchemaBot's own bookkeeping table.
func TestLintPostgresSchema_IgnoredTable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IgnoreTables = []string{"schema_version"}
	results, err := NewWithConfig(cfg).LintPostgresSchema(map[string]string{
		"schema_version": `CREATE TABLE schema_version (version integer PRIMARY KEY);`,
	})
	require.NoError(t, err)
	assert.Empty(t, results)
}

// The allowed primary key set is configuration: widening it to integer makes
// the inline integer key pass.
func TestLintPostgresSchema_AllowedPKTypesFromConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AllowedPostgresPKTypes = "bigint, integer"
	results, err := NewWithConfig(cfg).LintPostgresSchema(map[string]string{
		"orders": `CREATE TABLE orders (id integer PRIMARY KEY);`,
	})
	require.NoError(t, err)
	assert.Empty(t, results)
}

// Results span every table in the namespace and are sorted by table then
// column so the response body is stable regardless of map iteration order.
func TestLintPostgresSchema_SortedAcrossTables(t *testing.T) {
	results, err := New().LintPostgresSchema(map[string]string{
		"zeta":  `CREATE TABLE zeta (id integer PRIMARY KEY);`,
		"alpha": `CREATE TABLE alpha (weight real);`,
	})
	require.NoError(t, err)
	require.Len(t, results, 3)
	assert.Equal(t, []string{"alpha", "alpha", "zeta"}, []string{results[0].Table, results[1].Table, results[2].Table})
	assert.Equal(t, "primary_key", results[0].Linter)
	assert.Equal(t, "has_float", results[1].Linter)
	assert.Equal(t, "primary_key", results[2].Linter)
}

// An entry the audit cannot cover in full fails the whole call with a typed
// error naming the table, rather than producing a partial result.
func TestLintPostgresSchema_UnlintableEntries(t *testing.T) {
	tests := []struct {
		name       string
		ddl        string
		wantDetail string
	}{
		{
			name:       "ALTER TABLE",
			ddl:        `ALTER TABLE orders ADD COLUMN note text;`,
			wantDetail: "contains a AlterTable statement, expected CREATE TABLE",
		},
		{
			name:       "DROP TABLE alongside the table",
			ddl:        `CREATE TABLE orders (id bigint PRIMARY KEY); DROP TABLE legacy;`,
			wantDetail: "contains a Drop statement, expected CREATE TABLE",
		},
		{
			name:       "two CREATE TABLE statements",
			ddl:        `CREATE TABLE a (id bigint PRIMARY KEY); CREATE TABLE b (id bigint PRIMARY KEY);`,
			wantDetail: "contains more than one CREATE TABLE statement",
		},
		{
			name:       "only an index",
			ddl:        `CREATE INDEX orders_id_idx ON orders (id);`,
			wantDetail: "contains no CREATE TABLE statement",
		},
		{
			name:       "unparsable",
			ddl:        `CREATE TABLE orders (id bigint PRIMARY KEY`,
			wantDetail: "cannot be parsed as PostgreSQL DDL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := New().LintPostgresSchema(map[string]string{"orders": tt.ddl})
			var unlintable *UnlintableTableError
			require.ErrorAs(t, err, &unlintable)
			assert.Equal(t, "orders", unlintable.Table)
			assert.Contains(t, unlintable.Detail, tt.wantDetail)
			assert.Nil(t, results)
		})
	}
}

// lintOnePostgresEntry lints a single pulled table entry with the default
// config, keyed the way the pull response keys it.
func lintOnePostgresEntry(t *testing.T, ddl string) ([]Result, error) {
	t.Helper()
	return New().LintPostgresSchema(map[string]string{"entry": ddl})
}
