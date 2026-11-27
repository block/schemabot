package postgres

import (
	"testing"

	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIndexDropForApply pins the one index drop the apply runs: a DROP INDEX
// CONCURRENTLY of one index in the target's schema, whose reviewed record
// names that index. Every other DROP INDEX is refused before work is queued,
// with a remedy, and a statement that is not an index drop passes through.
func TestIndexDropForApply(t *testing.T) {
	reviewed := &schemadiff.DroppedObject{
		Kind:       schemadiff.DroppedIndex,
		Name:       "orders_status_idx",
		Definition: "CREATE INDEX orders_status_idx ON public.orders USING btree (status)",
	}

	indexDrop, err := indexDropForApply("app", "public", "orders", "DROP INDEX CONCURRENTLY public.orders_status_idx", reviewed)
	require.NoError(t, err)
	assert.True(t, indexDrop)

	indexDrop, err = indexDropForApply("app", "public", "orders", "ALTER TABLE public.orders ADD COLUMN note text", nil)
	require.NoError(t, err)
	assert.False(t, indexDrop, "a statement that is not an index drop is left to the other paths")

	refusals := []struct {
		name     string
		sql      string
		reviewed *schemadiff.DroppedObject
		want     string
	}{
		{name: "blocking drop", sql: "DROP INDEX public.orders_status_idx", reviewed: reviewed,
			want: `apply PostgreSQL table "orders": SchemaBot drops an index only with DROP INDEX CONCURRENTLY of that one index; re-plan and apply again`},
		{name: "cascading drop", sql: "DROP INDEX CONCURRENTLY public.orders_status_idx CASCADE", reviewed: reviewed,
			want: `apply PostgreSQL table "orders": SchemaBot drops an index only with DROP INDEX CONCURRENTLY of that one index; re-plan and apply again`},
		{name: "another schema", sql: "DROP INDEX CONCURRENTLY billing.orders_status_idx", reviewed: reviewed,
			want: `apply PostgreSQL table "orders": the planned index drop names schema "billing" but the apply targets schema "public"; the target's schema route changed after planning, so plan again`},
		{name: "no record", sql: "DROP INDEX CONCURRENTLY public.orders_status_idx", reviewed: nil,
			want: `apply PostgreSQL table "orders": the plan's record of the index this statement drops does not name index "orders_status_idx"; re-plan and apply again`},
		{name: "record of another index", sql: "DROP INDEX CONCURRENTLY public.orders_status_idx",
			reviewed: &schemadiff.DroppedObject{Kind: schemadiff.DroppedIndex, Name: "orders_placed_idx", Definition: reviewed.Definition},
			want:     `apply PostgreSQL table "orders": the plan's record of the index this statement drops does not name index "orders_status_idx"; re-plan and apply again`},
		{name: "record of a constraint", sql: "DROP INDEX CONCURRENTLY public.orders_status_idx",
			reviewed: &schemadiff.DroppedObject{Kind: schemadiff.DroppedConstraint, Name: "orders_status_idx", Definition: "UNIQUE (status)"},
			want:     `apply PostgreSQL table "orders": the plan's record of the index this statement drops does not name index "orders_status_idx"; re-plan and apply again`},
	}
	for _, tt := range refusals {
		t.Run(tt.name, func(t *testing.T) {
			indexDrop, err := indexDropForApply("app", "public", "orders", tt.sql, tt.reviewed)
			require.Error(t, err)
			assert.False(t, indexDrop)
			assert.Equal(t, tt.want, err.Error())
		})
	}
}

// TestIndexDropLimitFitsInsideTheApplyCeiling proves the server's timer ends
// a slow index drop before the optimistic apply ceiling cancels the apply,
// so the operator sees the drop's own budget failure.
func TestIndexDropLimitFitsInsideTheApplyCeiling(t *testing.T) {
	assert.Positive(t, indexDropLimit)
	assert.Less(t, indexDropLimit, optimisticApplyCeiling)
}
