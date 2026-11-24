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

const reviewedDropSQL = "ALTER TABLE public.users DROP COLUMN legacy"

func reviewedLegacyColumn() schemadiff.DroppedObject {
	return schemadiff.DroppedObject{Kind: schemadiff.DroppedColumn, Name: "legacy", Definition: "attnum 3 text"}
}

func TestReviewedDropMetadataRoundTrips(t *testing.T) {
	metadata, err := reviewedDropMetadata(reviewedLegacyColumn())
	require.NoError(t, err)
	require.Contains(t, metadata, reviewedDropMetadataKey)

	got, err := reviewedDropFromMetadata(metadata)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, reviewedLegacyColumn(), *got)

	none, err := reviewedDropFromMetadata(map[string]string{"other": "value"})
	require.NoError(t, err)
	assert.Nil(t, none, "a change with no record decodes to none")

	_, err = reviewedDropFromMetadata(map[string]string{reviewedDropMetadataKey: "{"})
	require.ErrorContains(t, err, "decode the reviewed drop")
	_, err = reviewedDropFromMetadata(map[string]string{reviewedDropMetadataKey: `{"kind":"column","name":"legacy"}`})
	require.ErrorContains(t, err, "does not name the object's kind, name, and definition")
}

// The plan records the reviewed object on the one rendered step that drops
// it, and blocks a dropping step it has no record for, because the apply
// would refuse that step.
func TestTableChangesRecordsTheReviewedDrop(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	statement := func(drops *schemadiff.DroppedObject) pgplan.Statement {
		return pgplan.Statement{
			SQL:         reviewedDropSQL,
			Route:       planner.RouteNative,
			Backend:     router.BackendNative,
			Disposition: router.DispositionExecute,
			ExecSQL:     []string{reviewedDropSQL},
			Destructive: true,
			Drops:       drops,
		}
	}
	plan := func(st pgplan.Statement) []engine.TableChange {
		report := pgplan.NewReport(pgplan.SourceDiff)
		report.Table = "users"
		report.Statements = []pgplan.Statement{st, {
			SQL:         "ALTER TABLE public.users ADD COLUMN email text",
			Route:       planner.RouteNative,
			Backend:     router.BackendNative,
			Disposition: router.DispositionExecute,
			ExecSQL:     []string{"ALTER TABLE public.users ADD COLUMN email text"},
		}}
		changes, _, _, err := tableChanges(report, nil, parser)
		require.NoError(t, err)
		require.Len(t, changes, 2)
		return changes
	}

	dropped := reviewedLegacyColumn()
	recorded := plan(statement(&dropped))
	got, err := reviewedDropFromMetadata(recorded[0].Metadata)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, dropped, *got)
	assert.NotEqual(t, engine.ExecutionModeBlocked, recorded[0].ExecutionMode)
	assert.Empty(t, recorded[1].Metadata, "a step that drops nothing carries no record")

	unrecorded := plan(statement(nil))
	assert.Empty(t, unrecorded[0].Metadata)
	assert.Equal(t, engine.ExecutionModeBlocked, unrecorded[0].ExecutionMode)
	assert.Contains(t, unrecorded[0].ModeReason, `drops an object the planner did not record`)
	assert.NotEqual(t, engine.ExecutionModeBlocked, unrecorded[1].ExecutionMode)
}

// An apply admits a statement that drops a live object only when the plan
// recorded that object, so the drop can be checked against what was reviewed.
func TestValidateOptimisticApplyRequiresTheReviewedDrop(t *testing.T) {
	request := func(metadata map[string]string) *engine.ApplyRequest {
		return &engine.ApplyRequest{
			Database: "app",
			Changes: []engine.SchemaChange{{Namespace: "public", TableChanges: []engine.TableChange{{
				Table: "users", DDL: reviewedDropSQL, Metadata: metadata,
			}}}},
			Credentials: &engine.Credentials{DSN: "postgres://localhost/app"},
		}
	}

	_, err := validateOptimisticApply(request(nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `apply PostgreSQL table "users": the plan does not record the object this statement drops`)
	assert.Contains(t, err.Error(), "re-plan and apply again")

	_, err = validateOptimisticApply(request(map[string]string{reviewedDropMetadataKey: "{"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the plan's record of the object this statement drops is unreadable; re-plan and apply again")

	metadata, err := reviewedDropMetadata(reviewedLegacyColumn())
	require.NoError(t, err)
	change, err := validateOptimisticApply(request(metadata))
	require.NoError(t, err)
	require.NotNil(t, change.dropped)
	assert.Equal(t, reviewedLegacyColumn(), *change.dropped)

	change, err = validateOptimisticApply(&engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{Namespace: "public", TableChanges: []engine.TableChange{{
			Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text",
		}}}},
		Credentials: &engine.Credentials{DSN: "postgres://localhost/app"},
	})
	require.NoError(t, err)
	assert.Nil(t, change.dropped, "a statement that drops nothing needs no record")
}
