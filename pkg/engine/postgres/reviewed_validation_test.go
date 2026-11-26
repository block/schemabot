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

// The plan records the constraint a VALIDATE CONSTRAINT step validates, and
// the apply reads that record back. A validation step the planner left
// unrecorded is blocked, since the apply could not check it.
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

	unrecorded := plan(statement(nil))
	assert.Empty(t, unrecorded.Metadata)
	assert.Equal(t, engine.ExecutionModeBlocked, unrecorded.ExecutionMode)
	assert.Equal(t, `statement for table "orders" validates a constraint the planner did not record, so the apply cannot check that it is still the constraint this plan reviewed; make the change outside SchemaBot`, unrecorded.ModeReason)
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
	metadata, unrecorded, err := stepValidationMetadata(pgplan.Statement{Validates: &key}, reviewedValidationSQL)
	require.NoError(t, err)
	require.False(t, unrecorded)
	change, err := validateOptimisticApply(request(metadata))
	require.NoError(t, err)
	require.NotNil(t, change.validates)
	assert.Equal(t, key, *change.validates)

	_, err = validateOptimisticApply(request(nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `apply PostgreSQL table "orders": the plan has no record of the constraint this statement validates; re-plan and apply again`)
}
