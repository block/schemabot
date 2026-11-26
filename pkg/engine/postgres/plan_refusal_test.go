package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/pg-sprite/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
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
// the wrapped text is unchanged. A failure that is not a decline, such as a
// lost connection or a missing privilege, stays an ordinary error.
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
			wrapped := fmt.Errorf(`plan PostgreSQL schema in "public"/"users.sql": plan PostgreSQL table "users": %w`, decline)
			err := plannerRefusal(wrapped)
			assert.True(t, engine.IsPlanRefused(err))
			assert.ErrorIs(t, err, decline)
			assert.EqualError(t, err, wrapped.Error())
		})
	}

	for _, missing := range []error{schemadiff.ErrReferencedTableNotReadable, schemadiff.ErrReferencedTableNotReferenceable} {
		t.Run("a missing privilege is not a refusal: "+missing.Error(), func(t *testing.T) {
			wrapped := fmt.Errorf("plan PostgreSQL table %q: %w", "users", missing)
			err := plannerRefusal(wrapped)
			assert.False(t, engine.IsPlanRefused(err))
			assert.Equal(t, wrapped, err)
		})
	}

	t.Run("a lost connection is not a refusal", func(t *testing.T) {
		lost := errors.New("write tcp 127.0.0.1:5432: connection reset by peer")
		err := plannerRefusal(lost)
		assert.False(t, engine.IsPlanRefused(err))
		assert.Equal(t, lost, err)
	})
}

// A schema file the desired-file parser does not admit is refused before any
// database is read, and the refusal is typed like a planner decline, because
// the remedy is the same: change the file. A file that does not parse at all
// is an input error, not a refusal, and stays an ordinary error.
func TestPlanRefusesFileThePlannerDoesNotAdmit(t *testing.T) {
	refused := map[string]struct {
		sql     string
		wantErr string
	}{
		"two tables in one file": {
			sql:     "CREATE TABLE a (id bigint PRIMARY KEY); CREATE TABLE b (id bigint PRIMARY KEY)",
			wantErr: "desired schema must contain exactly one CREATE TABLE",
		},
		"a change statement in it": {
			sql:     "DROP TABLE a",
			wantErr: "statement kind not allowed in a desired schema: DROP TABLE a",
		},
	}
	for name, tc := range refused {
		t.Run(name, func(t *testing.T) {
			_, err := planSchemas(t.Context(), nil, planRequestWithFiles(map[string]string{"a.sql": tc.sql}), DefaultNativeSafeTableSizeLimitBytes, "")
			require.Error(t, err)
			assert.True(t, engine.IsPlanRefused(err), "the refusal must be typed")
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}

	t.Run("a syntax error is not a refusal", func(t *testing.T) {
		_, err := planSchemas(t.Context(), nil, planRequestWithFiles(map[string]string{"a.sql": "CREATE TABLE a (id bigint PRIMARY KEY"}), DefaultNativeSafeTableSizeLimitBytes, "")
		require.Error(t, err)
		assert.False(t, engine.IsPlanRefused(err))
	})
}
