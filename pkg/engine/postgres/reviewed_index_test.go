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

const reviewedAdoptionSQL = "ALTER TABLE public.orders ADD CONSTRAINT orders_email_key UNIQUE USING INDEX orders_email_key_new"

func reviewedEmailIndex() schemadiff.IndexAdoption {
	return schemadiff.IndexAdoption{Index: "orders_email_key_new", Def: "CREATE UNIQUE INDEX orders_email_key_new ON public.orders USING btree (email)"}
}

// The plan records the index an ADD CONSTRAINT ... USING INDEX step takes
// over, and the apply reads that record back. An adoption the planner left
// unrecorded is blocked, since the apply could not check it.
func TestTableChangesRecordsTheReviewedAdoption(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	plan := func(adopts *schemadiff.IndexAdoption) engine.TableChange {
		report := pgplan.NewReport(pgplan.SourceDiff)
		report.Table = "orders"
		report.Statements = []pgplan.Statement{{
			SQL:         reviewedAdoptionSQL,
			Route:       planner.RouteNative,
			Backend:     router.BackendNative,
			Disposition: router.DispositionExecute,
			ExecSQL:     []string{reviewedAdoptionSQL},
			Adopts:      adopts,
		}}
		changes, _, _, err := tableChanges(report, parser)
		require.NoError(t, err)
		require.Len(t, changes, 1)
		return changes[0]
	}

	index := reviewedEmailIndex()
	recorded := plan(&index)
	assert.Empty(t, recorded.ExecutionMode, recorded.ModeReason)
	got, err := reviewedAdoptionForApply("app", recorded, reviewedAdoptionSQL)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, index, *got)

	unrecorded := plan(nil)
	assert.Empty(t, unrecorded.Metadata)
	assert.Equal(t, engine.ExecutionModeBlocked, unrecorded.ExecutionMode)
	assert.Equal(t, `statement for table "orders" makes a constraint take over an index the planner did not record, so the apply cannot check that it is still the index this plan reviewed; make the change outside SchemaBot`, unrecorded.ModeReason)

	other := schemadiff.IndexAdoption{Index: "orders_other_idx", Def: index.Def}
	mismatched := plan(&other)
	assert.Equal(t, engine.ExecutionModeBlocked, mismatched.ExecutionMode, "a record of another index does not cover this adoption")
}

// An apply refuses an adoption record it cannot read, or none at all, and
// carries a readable one to the executor. A statement that takes over no
// index needs no record.
func TestValidateOptimisticApplyReadsTheReviewedAdoption(t *testing.T) {
	request := func(statement string, metadata map[string]string) *engine.ApplyRequest {
		return &engine.ApplyRequest{
			Database: "app",
			Changes: []engine.SchemaChange{{Namespace: "public", TableChanges: []engine.TableChange{{
				Table: "orders", DDL: statement, Metadata: metadata,
			}}}},
			Credentials: &engine.Credentials{DSN: "postgres://localhost/app"},
		}
	}

	_, err := validateOptimisticApply(request(reviewedAdoptionSQL, nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `apply PostgreSQL table "orders": the plan has no record of the index this statement's constraint takes over; re-plan and apply again`)

	_, err = validateOptimisticApply(request(reviewedAdoptionSQL, map[string]string{reviewedAdoptionMetadataKey: `{"Index":"orders_other_idx","Def":"x"}`}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the plan's record of the index this statement's constraint takes over is unreadable")

	index := reviewedEmailIndex()
	metadata, unrecorded, err := stepAdoptionMetadata(pgplan.Statement{Adopts: &index}, reviewedAdoptionSQL)
	require.NoError(t, err)
	require.False(t, unrecorded)
	change, err := validateOptimisticApply(request(reviewedAdoptionSQL, metadata))
	require.NoError(t, err)
	require.NotNil(t, change.adopts)
	assert.Equal(t, index, *change.adopts)

	plain, err := validateOptimisticApply(request("ALTER TABLE public.orders ADD CONSTRAINT orders_email_key UNIQUE (email)", nil))
	require.NoError(t, err)
	assert.Nil(t, plain.adopts)
}

// A replacement record is read only on the drop of an index, and must name
// the replacement and its definition.
func TestIndexReplacementForApplyReadsOnlyAnIndexDrop(t *testing.T) {
	droppedIndex := &schemadiff.DroppedObject{Kind: schemadiff.DroppedIndex, Name: "orders_email_key", Definition: "CREATE UNIQUE INDEX orders_email_key ON public.orders USING btree (email)"}
	change := func(raw string) engine.TableChange {
		return engine.TableChange{Table: "orders", Metadata: map[string]string{indexReplacementMetadataKey: raw}}
	}

	none, err := indexReplacementForApply("app", engine.TableChange{Table: "orders"}, droppedIndex)
	require.NoError(t, err)
	assert.Nil(t, none, "a drop of an index the desired schema no longer has carries no replacement")

	_, err = indexReplacementForApply("app", change(`{"index":"orders_email_key_new","def":"x"}`), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `apply PostgreSQL table "orders": the plan records a replacement index on a statement that drops no index; re-plan and apply again`)

	_, err = indexReplacementForApply("app", change(`{"index":"orders_email_key_new"}`), droppedIndex)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `apply PostgreSQL table "orders": the plan's record of the index that replaces "orders_email_key" is unreadable; re-plan and apply again`)

	got, err := indexReplacementForApply("app", change(`{"index":"orders_email_key_new","def":"CREATE UNIQUE INDEX orders_email_key_new ON public.orders USING btree (lower(email))"}`), droppedIndex)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "orders_email_key_new", got.Index)
}
