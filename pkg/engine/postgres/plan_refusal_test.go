package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/schema"
)

// A declared schema the PostgreSQL engine will not plan as written is reported
// as a plan refusal, not as a failure of the engine, and keeps its message word
// for word: the message tells whoever edits the schema files or the config what
// to change. Each case is a refusal the engine reaches before it reads the
// target, so no pool is needed.
func TestPlanRefusalsAreTyped(t *testing.T) {
	tests := []struct {
		name    string
		refuse  func() error
		wantErr string
	}{
		{
			name: "drop_tables on a PostgreSQL target",
			refuse: func() error {
				_, err := New().Plan(t.Context(), &engine.PlanRequest{
					Database:    "orders_db",
					Credentials: &engine.Credentials{DSN: "postgres://user:pass@127.0.0.1:1/orders_db"},
					DropTables:  []string{"orders_old"},
				})
				return err
			},
			wantErr: `plan PostgreSQL database "orders_db": drop_tables is supported only on MySQL, and this is a PostgreSQL target. Remove drop_tables from schemabot.yaml`,
		},
		{
			name: "a table declared by two schema files",
			refuse: func() error {
				_, err := planSchemas(t.Context(), nil, planRequestWithFiles(map[string]string{
					"users.sql":         "CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL)",
					"users_profile.sql": "CREATE TABLE users (id bigint PRIMARY KEY, display_name text)",
				}), DefaultNativeSafeTableSizeLimitBytes, "")
				return err
			},
			wantErr: `plan PostgreSQL namespace "public": table "users" is declared by both schema files "users.sql" and "users_profile.sql". Declare each table in exactly one schema file`,
		},
		{
			name: "an enum type declared by two schema files",
			refuse: func() error {
				_, err := planSchemas(t.Context(), nil, planRequestWithFiles(map[string]string{
					"status.sql":     "CREATE TYPE status AS ENUM ('new', 'done')",
					"status_two.sql": "CREATE TYPE status AS ENUM ('new', 'done', 'late')",
				}), DefaultNativeSafeTableSizeLimitBytes, "")
				return err
			},
			wantErr: `plan PostgreSQL namespace "public": enum type "status" is declared by both schema files "status.sql" and "status_two.sql". Declare each type in exactly one schema file`,
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

func planRequestWithFiles(files map[string]string) *engine.PlanRequest {
	return &engine.PlanRequest{
		Database:    "orders_db",
		SchemaFiles: schema.SchemaFiles{"public": {Files: files}},
	}
}

// pg-sprite names each way its planner declines a declared schema. Those
// declines are plan refusals, whatever context the engine wraps them in, and
// the text is the planner's, behind the file it came from. A failure that is
// neither a decline nor a missing privilege, such as a lost connection, stays
// an ordinary error.
func TestPlannerRefusalTypesOnlyPlannerDeclines(t *testing.T) {
	declines := []error{
		statement.ErrMultipleCreateTables,
		statement.ErrDisallowedStatement,
		statement.ErrDesiredPartitions,
		statement.ErrEnumTypeAlone,
		statement.ErrEnumLabel,
		schemadiff.ErrUnsupportedChange,
		schemadiff.ErrReferencedTableNotFound,
		schemadiff.ErrDesiredPartitionOf,
		schemadiff.ErrDesiredInherits,
		schemadiff.ErrDesiredConstraintMerged,
		schemadiff.ErrExpressionUnsettled,
		executor.ErrPolicyRecursion,
	}
	for _, decline := range declines {
		t.Run(decline.Error(), func(t *testing.T) {
			cause := fmt.Errorf(`plan PostgreSQL table "users": %w`, decline)
			err := plannerRefusal("public", "users.sql", cause)
			assert.True(t, engine.IsPlanRefused(err))
			assert.ErrorIs(t, err, decline)
			assert.EqualError(t, err, `plan PostgreSQL schema in "public"/"users.sql": `+cause.Error())
		})
	}

	t.Run("a lost connection is not a refusal", func(t *testing.T) {
		lost := errors.New("write tcp 127.0.0.1:5432: connection reset by peer")
		err := plannerRefusal("public", "users.sql", lost)
		assert.False(t, engine.IsPlanRefused(err))
		assert.ErrorIs(t, err, lost)
		assert.EqualError(t, err, `plan PostgreSQL schema in "public"/"users.sql": `+lost.Error())
	})
}

// A privilege the planning role lacks fails every plan of the file the same
// way until someone grants it, so it is a refusal that ends in the grant, not
// an internal error to retry. When pg-sprite checked the privilege, the
// refusal names its exact GRANT and the catalog check that verifies it; when
// only the server refused, it carries the server's message, which names the
// object, and says to grant that privilege. The remedy leads, so a renderer
// that clamps the text cuts the file context, not the GRANT.
func TestPlannerRefusalNamesTheGrantAPlanNeeds(t *testing.T) {
	t.Run("a privilege pg-sprite checked", func(t *testing.T) {
		privilege := &preflight.PrivilegeError{
			Tier:  preflight.TierConnect,
			Check: "has_schema_privilege(sb_planner, billing, 'USAGE')",
			Grant: `GRANT USAGE ON SCHEMA "billing" TO "sb_planner"`,
		}
		err := plannerRefusal("billing", "invoices.sql", fmt.Errorf(`plan PostgreSQL table "invoices": %w`, privilege))
		assert.True(t, engine.IsPlanRefused(err))
		assert.ErrorIs(t, err, privilege)
		assert.EqualError(t, err, `grant SchemaBot's role the privilege planning needs, then re-plan: GRANT USAGE ON SCHEMA "billing" TO "sb_planner" `+
			`(file "billing"/"invoices.sql"; verified by: has_schema_privilege(sb_planner, billing, 'USAGE'))`)
	})

	t.Run("a hint follows the grant", func(t *testing.T) {
		privilege := &preflight.PrivilegeError{
			Tier:  preflight.TierConnect,
			Check: "has_schema_privilege(sb_planner, billing, 'USAGE')",
			Grant: `GRANT USAGE ON SCHEMA "billing" TO "sb_planner"`,
			Hint:  "the schema's owner can grant it",
		}
		err := plannerRefusal("billing", "invoices.sql", privilege)
		assert.True(t, engine.IsPlanRefused(err))
		assert.EqualError(t, err, `grant SchemaBot's role the privilege planning needs, then re-plan: GRANT USAGE ON SCHEMA "billing" TO "sb_planner"; the schema's owner can grant it `+
			`(file "billing"/"invoices.sql"; verified by: has_schema_privilege(sb_planner, billing, 'USAGE'))`)
	})

	t.Run("a privilege only the server checked", func(t *testing.T) {
		denied := &pgconn.PgError{Severity: "ERROR", Code: "42501", Message: "permission denied for schema billing"}
		err := plannerRefusal("billing", "invoices.sql", fmt.Errorf("look up table facts billing.invoices: %w", denied))
		assert.True(t, engine.IsPlanRefused(err))
		assert.ErrorIs(t, err, denied)
		assert.EqualError(t, err, `grant SchemaBot's role the privilege this error names, then re-plan: permission denied for schema billing (SQLSTATE 42501; file "billing"/"invoices.sql")`)
	})

	t.Run("a referenced table the role cannot read", func(t *testing.T) {
		denied := &pgconn.PgError{Severity: "ERROR", Code: "42501", Message: "permission denied for table customers"}
		cause := fmt.Errorf(`plan PostgreSQL table "orders": %w`, fmt.Errorf("%w: public.customers: %w", schemadiff.ErrReferencedTableNotReadable, denied))
		err := plannerRefusal("public", "orders.sql", cause)
		assert.True(t, engine.IsPlanRefused(err))
		assert.ErrorIs(t, err, schemadiff.ErrReferencedTableNotReadable)
		assert.EqualError(t, err, `grant SchemaBot's role SELECT on the table a foreign key references, then re-plan: permission denied for table customers (SQLSTATE 42501; file "public"/"orders.sql")`)
	})

	t.Run("a referenced table the role cannot reference", func(t *testing.T) {
		cause := fmt.Errorf(`plan PostgreSQL table "orders": %w`, fmt.Errorf("%w: public.customers(id)", schemadiff.ErrReferencedTableNotReferenceable))
		err := plannerRefusal("public", "orders.sql", cause)
		assert.True(t, engine.IsPlanRefused(err))
		assert.ErrorIs(t, err, schemadiff.ErrReferencedTableNotReferenceable)
		assert.EqualError(t, err, `grant SchemaBot's role REFERENCES on the table a foreign key references, then re-plan: `+
			`planning role lacks REFERENCES on a table a foreign key references: public.customers(id) (file "public"/"orders.sql")`)
	})

	t.Run("a connection refused for a privilege shows the server's message alone", func(t *testing.T) {
		denied := &pgconn.PgError{Severity: "FATAL", Code: "42501", Message: "permission denied for database app"}
		err := plannerRefusal("billing", "invoices.sql", fmt.Errorf("failed to connect to `user=sb_planner database=app`: 10.0.0.7:5432 (db.internal.example): %w", denied))
		assert.True(t, engine.IsPlanRefused(err))
		assert.ErrorIs(t, err, denied)
		assert.EqualError(t, err, `grant SchemaBot's role the privilege this error names, then re-plan: permission denied for database app (SQLSTATE 42501; file "billing"/"invoices.sql")`)
	})

	t.Run("the grant survives the inline clamp at the longest names", func(t *testing.T) {
		schemaName := strings.Repeat("s", 63)
		filename := strings.Repeat("f", 251) + ".sql"
		privilege := &preflight.PrivilegeError{
			Tier:  preflight.TierConnect,
			Check: fmt.Sprintf("has_schema_privilege(sb_planner, %s, 'USAGE')", schemaName),
			Grant: fmt.Sprintf(`GRANT USAGE ON SCHEMA "%s" TO "sb_planner"`, schemaName),
		}
		rendered := presentation.InlineError(plannerRefusal(schemaName, filename, privilege).Error())
		assert.Contains(t, rendered, "then re-plan: "+privilege.Grant, "the remedy leads, so the clamp cuts only the file context")
	})

	t.Run("another server error is not a refusal", func(t *testing.T) {
		gone := &pgconn.PgError{Severity: "FATAL", Code: "57P01", Message: "terminating connection due to administrator command"}
		err := plannerRefusal("billing", "invoices.sql", gone)
		assert.False(t, engine.IsPlanRefused(err))
		assert.ErrorIs(t, err, gone)
	})
}

// A schema file the desired-file parser does not admit is refused before any
// database is read, and the refusal is typed like a planner decline, because
// the remedy is the same: change the file. That holds for every error the
// parse returns, a schema-qualified name and a syntax error included, so none
// of them reads as an engine failure to plan again.
func TestPlanRefusesFileThePlannerDoesNotAdmit(t *testing.T) {
	refused := map[string]struct {
		sql      string
		sentinel error
		wantErr  string
	}{
		"two tables in one file": {
			sql:      "CREATE TABLE a (id bigint PRIMARY KEY); CREATE TABLE b (id bigint PRIMARY KEY)",
			sentinel: statement.ErrMultipleCreateTables,
			wantErr:  "desired schema must contain exactly one CREATE TABLE",
		},
		"a change statement in it": {
			sql:      "DROP TABLE a",
			sentinel: statement.ErrDisallowedStatement,
			wantErr:  "statement kind not allowed in a desired schema: DROP TABLE a",
		},
		"a schema-qualified table name": {
			sql:      `CREATE TABLE "public"."a" (id bigint PRIMARY KEY)`,
			sentinel: statement.ErrQualifiedName,
			wantErr:  "desired schema statements must use unqualified names: public.a",
		},
		"a foreign key to a schema-qualified table": {
			sql:      `CREATE TABLE a (id bigint PRIMARY KEY, b_id bigint REFERENCES "public"."b"("id"))`,
			sentinel: statement.ErrQualifiedName,
			wantErr:  "desired schema statements must use unqualified names",
		},
		"a syntax error": {
			sql:      "CREATE TABLE a (id bigint PRIMARY KEY",
			sentinel: statement.ErrParseDesired,
			wantErr:  "syntax error at end of input",
		},
	}
	for name, tc := range refused {
		t.Run(name, func(t *testing.T) {
			_, err := planSchemas(t.Context(), nil, planRequestWithFiles(map[string]string{"a.sql": tc.sql}), DefaultNativeSafeTableSizeLimitBytes, "")
			require.Error(t, err)
			assert.True(t, engine.IsPlanRefused(err), "the refusal must be typed")
			assert.ErrorIs(t, err, tc.sentinel)
			assert.Contains(t, err.Error(), `plan PostgreSQL schema in "public"/"a.sql": `)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// A syntax error is about one line of one file, so the refusal locates it
// there for a renderer to point at, with the parser's own words and the line
// it stopped on.
func TestPlanRefusalLocatesASyntaxError(t *testing.T) {
	sql := "CREATE TABLE a (\n  id bigint PRIMARY KEY,\n  name text text\n)"
	_, err := planSchemas(t.Context(), nil, planRequestWithFiles(map[string]string{"a.sql": sql}), DefaultNativeSafeTableSizeLimitBytes, "")
	require.Error(t, err)
	var refusal *engine.PlanRefusedError
	require.ErrorAs(t, err, &refusal)
	require.Len(t, refusal.Problems, 1)
	problem := refusal.Problems[0]
	assert.Equal(t, "public/a.sql", problem.File)
	assert.Equal(t, 3, problem.Line)
	assert.Equal(t, `The statement has a syntax error at column 13: syntax error at or near "text". Correct it in this file.`, problem.Message)
	assert.Equal(t, "  name text text", problem.Statement)
}

// A refusal that is not a syntax error names no line, so it lists no problem.
func TestPlanRefusalOfAQualifiedNameListsNoProblem(t *testing.T) {
	_, err := planSchemas(t.Context(), nil, planRequestWithFiles(map[string]string{"a.sql": "CREATE TABLE public.a (id bigint PRIMARY KEY)"}), DefaultNativeSafeTableSizeLimitBytes, "")
	var refusal *engine.PlanRefusedError
	require.ErrorAs(t, err, &refusal)
	assert.Empty(t, refusal.Problems)
}
