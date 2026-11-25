package postgres

import (
	"testing"

	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/router"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

const reviewedValidationSQL = "ALTER TABLE public.orders VALIDATE CONSTRAINT orders_user_id_fkey"

func reviewedUserKey() schemadiff.ConstraintValidation {
	return schemadiff.ConstraintValidation{Constraint: "orders_user_id_fkey", Def: "FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID"}
}

// The plan records the live NOT VALID constraint on the step that validates
// it in place, and the apply reads that record back. The validation step of a
// constraint the same plan adds carries none.
func TestTableChangesRecordsTheReviewedValidation(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	statement := func(validates *schemadiff.ConstraintValidation) pgplan.Statement {
		return pgplan.Statement{
			SQL:         reviewedValidationSQL,
			Route:       planner.RouteNative,
			Backend:     router.BackendNative,
			Disposition: router.DispositionExecute,
			ExecSQL:     []string{reviewedValidationSQL},
			Validates:   validates,
		}
	}
	plan := func(st pgplan.Statement) engine.TableChange {
		report := pgplan.NewReport(pgplan.SourceDiff)
		report.Table = "orders"
		report.Statements = []pgplan.Statement{st}
		changes, _, _, err := tableChanges(report, nil, parser)
		require.NoError(t, err)
		require.Len(t, changes, 1)
		return changes[0]
	}

	key := reviewedUserKey()
	recorded := plan(statement(&key))
	got, err := reviewedValidationFromMetadata(recorded.Metadata)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, key, *got)

	added := plan(statement(nil))
	assert.Empty(t, added.Metadata, "a constraint the plan adds is validated without a record")
}

// An apply refuses a validation record it cannot read, and carries a readable
// one to the executor.
func TestValidateOptimisticApplyReadsTheReviewedValidation(t *testing.T) {
	request := func(metadata map[string]string) *engine.ApplyRequest {
		return &engine.ApplyRequest{
			Database: "app",
			Changes: []engine.SchemaChange{{Namespace: "public", TableChanges: []engine.TableChange{{
				Table: "orders", DDL: reviewedValidationSQL, Metadata: metadata,
			}}}},
			Credentials: &engine.Credentials{DSN: "postgres://localhost/app"},
		}
	}

	_, err := validateOptimisticApply(request(map[string]string{reviewedValidationMetadataKey: "{"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `apply PostgreSQL table "orders": the plan's record of the constraint this statement validates is unreadable; re-plan and apply again`)

	_, err = validateOptimisticApply(request(map[string]string{reviewedValidationMetadataKey: `{"Constraint":"orders_user_id_fkey"}`}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the plan's record of the constraint this statement validates is unreadable")

	key := reviewedUserKey()
	metadata, err := stepValidationMetadata(pgplan.Statement{Validates: &key}, reviewedValidationSQL)
	require.NoError(t, err)
	change, err := validateOptimisticApply(request(metadata))
	require.NoError(t, err)
	require.NotNil(t, change.validates)
	assert.Equal(t, key, *change.validates)

	change, err = validateOptimisticApply(request(nil))
	require.NoError(t, err)
	assert.Nil(t, change.validates, "a validation the plan did not record runs without one")
}
