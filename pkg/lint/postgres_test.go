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
			name: "a user type named like a built-in keeps its qualifier and is not allowed",
			ddl:  `CREATE TABLE orders (id custom.bigint PRIMARY KEY);`,
			want: []Result{{
				Table: "orders", Column: "id", Linter: "primary_key", Severity: "warning",
				Message: `Primary key column "id" in table "orders" uses "custom.bigint"; allowed types: bigint, uuid`,
			}},
		},
		{
			name: "an array of an allowed type is not that type",
			ddl:  `CREATE TABLE orders (id bigint[] PRIMARY KEY);`,
			want: []Result{{
				Table: "orders", Column: "id", Linter: "primary_key", Severity: "warning",
				Message: `Primary key column "id" in table "orders" uses "bigint[]"; allowed types: bigint, uuid`,
			}},
		},
		{
			name: "fixed-width character key reads in its SQL spelling",
			ddl:  `CREATE TABLE orders (id char(36) PRIMARY KEY);`,
			want: []Result{{
				Table: "orders", Column: "id", Linter: "primary_key", Severity: "warning",
				Message: `Primary key column "id" in table "orders" uses "character(36)"; allowed types: bigint, uuid`,
			}},
		},
		{
			name: "timestamp precision sits after the first word",
			ddl:  `CREATE TABLE orders (id timestamp(3) with time zone PRIMARY KEY);`,
			want: []Result{{
				Table: "orders", Column: "id", Linter: "primary_key", Severity: "warning",
				Message: `Primary key column "id" in table "orders" uses "timestamp(3) with time zone"; allowed types: bigint, uuid`,
			}},
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
// CREATE INDEX statements. Indexes that serve distinct queries produce no
// findings: a plain index on a column that another key carries only in its
// trailing position is not a prefix of that key, and two composite keys over
// the same columns in different orders serve different lookups.
func TestLintPostgresSchema_DistinctIndexesPass(t *testing.T) {
	results, err := lintOnePostgresEntry(t, `CREATE TABLE orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL, sku text, shipped_at timestamptz);
CREATE INDEX orders_sku_idx ON orders (sku);
CREATE UNIQUE INDEX orders_customer_sku_idx ON orders (customer_id, sku);
CREATE INDEX orders_shipped_customer_idx ON orders (shipped_at, customer_id);
CREATE INDEX orders_customer_shipped_idx ON orders (customer_id, shipped_at);`)
	require.NoError(t, err)
	assert.Empty(t, results)
}

// The redundant index rule mirrors Spirit's btree semantics: a plain index is
// redundant when its key parts are a leading prefix of another key (including
// the primary key), a duplicate pair reports both members, a UNIQUE key is
// redundant only to a UNIQUE key or the primary key over exactly the same
// columns, and the primary key itself is never reported. Messages match
// Spirit's wording so a mixed-fleet response reads alike.
func TestLintPostgresSchema_RedundantIndexes(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
		want []string
	}{
		{
			name: "prefix of a wider index",
			ddl: `CREATE TABLE t (id bigint PRIMARY KEY, a bigint, b bigint);
CREATE INDEX k_a ON t (a);
CREATE INDEX k_ab ON t (a, b);`,
			want: []string{`Index "k_a" on column "a" is redundant - covered by index "k_ab" on columns ("a", "b")`},
		},
		{
			name: "duplicate pair reports both",
			ddl: `CREATE TABLE t (id bigint PRIMARY KEY, a bigint);
CREATE INDEX k_a1 ON t (a);
CREATE INDEX k_a2 ON t (a);`,
			want: []string{
				`Index "k_a1" on column "a" is a duplicate of index "k_a2"`,
				`Index "k_a2" on column "a" is a duplicate of index "k_a1"`,
			},
		},
		{
			name: "duplicate of the primary key",
			ddl: `CREATE TABLE t (id bigint PRIMARY KEY);
CREATE INDEX t_id_idx ON t (id);`,
			want: []string{`Index "t_id_idx" on column "id" is a duplicate of the PRIMARY KEY`},
		},
		{
			name: "prefix of a composite primary key",
			ddl: `CREATE TABLE t (tenant_id bigint, id bigint, PRIMARY KEY (tenant_id, id));
CREATE INDEX t_tenant_idx ON t (tenant_id);`,
			want: []string{`Index "t_tenant_idx" on column "tenant_id" is redundant - covered by PRIMARY KEY on columns ("tenant_id", "id")`},
		},
		{
			name: "unique constraint duplicated by a unique index",
			ddl: `CREATE TABLE t (id bigint PRIMARY KEY, email text, CONSTRAINT t_email_key UNIQUE (email));
CREATE UNIQUE INDEX t_email_uidx ON t (email);`,
			want: []string{
				`Index "t_email_key" on column "email" is a duplicate of index "t_email_uidx"`,
				`Index "t_email_uidx" on column "email" is a duplicate of index "t_email_key"`,
			},
		},
		{
			name: "unique key is not redundant to a wider plain index",
			ddl: `CREATE TABLE t (id bigint PRIMARY KEY, a bigint, b bigint);
CREATE UNIQUE INDEX u_a ON t (a);
CREATE INDEX k_ab ON t (a, b);`,
			want: nil,
		},
		{
			name: "unnamed inline unique constraint is described by its definition",
			ddl: `CREATE TABLE t (id bigint PRIMARY KEY, code text UNIQUE);
CREATE INDEX t_code_idx ON t (code);`,
			want: []string{`Index "t_code_idx" on column "code" is a duplicate of index "UNIQUE (code)"`},
		},
		{
			name: "sort direction is part of the key",
			ddl: `CREATE TABLE t (id bigint PRIMARY KEY, a bigint, b bigint);
CREATE INDEX k_a_desc ON t (a DESC);
CREATE INDEX k_ab ON t (a, b);`,
			want: nil,
		},
		{
			name: "partial, expression, covering, and non-btree indexes do not take part",
			ddl: `CREATE TABLE t (id bigint PRIMARY KEY, a bigint, b bigint, doc jsonb);
CREATE INDEX k_a_partial ON t (a) WHERE b IS NOT NULL;
CREATE INDEX k_a_lower ON t ((a + 1));
CREATE INDEX k_a_include ON t (a) INCLUDE (b);
CREATE INDEX k_doc ON t USING gin (doc);
CREATE INDEX k_ab ON t (a, b);`,
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := lintOnePostgresEntry(t, tt.ddl)
			require.NoError(t, err)
			var messages []string
			for _, r := range results {
				assert.Equal(t, "t", r.Table)
				assert.Empty(t, r.Column)
				assert.Equal(t, "redundant_indexes", r.Linter)
				assert.Equal(t, "warning", r.Severity)
				messages = append(messages, r.Message)
			}
			assert.Equal(t, tt.want, messages)
		})
	}
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
// error naming the table, rather than producing a partial result. The entry
// must be a create set — the CREATE TABLE first, then only CREATE INDEX
// statements on that same relation — so an index that precedes its table or
// names another relation is refused, not skipped, and the cause is worded in
// the statement parser's vocabulary.
func TestLintPostgresSchema_UnlintableEntries(t *testing.T) {
	tests := []struct {
		name       string
		ddl        string
		wantDetail string
	}{
		{
			name:       "ALTER TABLE",
			ddl:        `ALTER TABLE orders ADD COLUMN note text;`,
			wantDetail: "is a ALTER TABLE statement, expected CREATE TABLE",
		},
		{
			name:       "DROP TABLE alongside the table",
			ddl:        `CREATE TABLE orders (id bigint PRIMARY KEY); DROP TABLE legacy;`,
			wantDetail: "statement 2 is DROP TABLE; a multi-statement DDL script must be a CREATE TABLE followed only by CREATE INDEX statements on that table",
		},
		{
			name:       "two CREATE TABLE statements",
			ddl:        `CREATE TABLE a (id bigint PRIMARY KEY); CREATE TABLE b (id bigint PRIMARY KEY);`,
			wantDetail: "statement 2 is CREATE TABLE; a multi-statement DDL script must be a CREATE TABLE followed only by CREATE INDEX statements on that table",
		},
		{
			name:       "only an index",
			ddl:        `CREATE INDEX orders_id_idx ON orders (id);`,
			wantDetail: "is a CREATE INDEX statement, expected CREATE TABLE",
		},
		{
			name:       "index before its table",
			ddl:        `CREATE INDEX orders_id_idx ON orders (id); CREATE TABLE orders (id bigint PRIMARY KEY);`,
			wantDetail: "statement 1 is CREATE INDEX; a multi-statement DDL script must start with CREATE TABLE",
		},
		{
			name:       "index on another relation",
			ddl:        `CREATE TABLE orders (id bigint PRIMARY KEY); CREATE INDEX customers_id_idx ON customers (id);`,
			wantDetail: `statement 2 creates an index on table "customers", not CREATE TABLE target "orders"`,
		},
		{
			name:       "index qualified differently from its table",
			ddl:        `CREATE TABLE orders (id bigint PRIMARY KEY); CREATE INDEX orders_id_idx ON app.orders (id);`,
			wantDetail: `statement 2 creates an index on "app"."orders" while CREATE TABLE targets "orders"`,
		},
		{
			name:       "unparsable",
			ddl:        `CREATE TABLE orders (id bigint PRIMARY KEY`,
			wantDetail: "is not a create set: split DDL script: failed to parse SQL statements",
		},
		{
			name:       "empty",
			ddl:        "",
			wantDetail: "is not a create set: DDL script contains no statements",
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
