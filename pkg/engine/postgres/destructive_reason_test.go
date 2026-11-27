package postgres

import (
	"testing"

	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ui"
)

// droppedReasonSentences is the unsafe reason each kind of dropped object
// renders for an object named "note" on "users".
var droppedReasonSentences = map[schemadiff.DroppedKind]string{
	schemadiff.DroppedColumn:     `statement drops column "note" from table "users": its values are deleted, and reverting the schema file adds the column back empty`,
	schemadiff.DroppedNotNull:    `statement lets column "note" of table "users" hold NULL: once NULLs are written, making it NOT NULL again means fixing those rows first`,
	schemadiff.DroppedConstraint: `statement drops constraint "note" from table "users": the rule it enforces stops applying, and rows written after the drop are not checked against it`,
	schemadiff.DroppedIndex:      `statement drops index "note" from table "users": queries that use it lose it, and putting it back means building it again`,
}

// droppedStatement is a destructive statement that drops the named object of
// the given kind.
func droppedStatement(kind schemadiff.DroppedKind, name string, dependents ...string) pgplan.Statement {
	return pgplan.Statement{Destructive: true, Drops: &schemadiff.DroppedObject{Kind: kind, Name: name, Dependents: dependents}}
}

// Every kind of object pg-sprite can drop renders a reason that names the
// object and says what the drop costs, as one finding. The set comes from
// pg-sprite, so a kind added upstream fails here by name.
func TestDestructiveReasonCoversEveryDroppedKind(t *testing.T) {
	kinds := schemadiff.DroppedKinds()
	assert.Len(t, droppedReasonSentences, len(kinds), "golden table carries a kind pg-sprite no longer reports")
	for _, kind := range kinds {
		t.Run(string(kind), func(t *testing.T) {
			want, ok := droppedReasonSentences[kind]
			require.True(t, ok, "no reason for dropped kind %q", kind)
			got := destructiveReason(droppedStatement(kind, "note"), "users", false)
			assert.Equal(t, want, got)
			assert.Equal(t, []string{want}, ui.LintReasons(got), "the reason renders as one finding")
		})
	}
}

// A schema file that renames a column plans a drop of the old column and an
// add of the new one. The drop's reason says the values are deleted, names
// what goes with the column, and gives the steps that rename it without
// losing them.
func TestDestructiveReasonDroppedColumnBesideAnAddedColumn(t *testing.T) {
	got := destructiveReason(droppedStatement(schemadiff.DroppedColumn, "note", "index users_note_idx: CREATE INDEX users_note_idx ON public.users USING btree (note)"), "users", true)
	assert.Equal(t, `statement drops column "note" from table "users": its values are deleted, and reverting the schema file adds the column back empty. `+
		`It also drops index users_note_idx. `+
		`This plan also adds a column to the table, and SchemaBot does not rename columns. To rename one, add the new column, copy the values into it, and drop the old column in a later change`, got)
	assert.Len(t, ui.LintReasons(got), 1, "the reason renders as one finding")
}

// An identifier that holds the finding separator does not split the reason.
func TestDestructiveReasonNeutralizesTheFindingSeparator(t *testing.T) {
	got := destructiveReason(droppedStatement(schemadiff.DroppedIndex, "a; b"), "users", false)
	assert.Equal(t, `statement drops index "a, b" from table "users": queries that use it lose it, and putting it back means building it again`, got)
}

// The drop that completes a replacement names the object it replaces by its
// kind and the index that takes over, for an index and for a constraint.
func TestDestructiveReasonReplacement(t *testing.T) {
	index := droppedStatement(schemadiff.DroppedIndex, "users_email_key")
	index.ReplacedBy = "users_email_key_new"
	assert.Equal(t, `statement replaces index "users_email_key" on table "users" with the definition built as "users_email_key_new": the old index is dropped only while that index is valid, and from then on its old definition no longer applies`,
		destructiveReason(index, "users", false))

	constraint := droppedStatement(schemadiff.DroppedConstraint, "users_email_key")
	constraint.ReplacedBy = "users_email_key_new"
	assert.Equal(t, `statement replaces constraint "users_email_key" on table "users" with the definition built as "users_email_key_new": the old constraint is dropped only while that index is valid, and from then on its old definition no longer applies`,
		destructiveReason(constraint, "users", false))
}

// A statement that is not destructive has no reason, and one that records no
// dropped object keeps the general sentence.
func TestDestructiveReasonWithoutADroppedObject(t *testing.T) {
	assert.Empty(t, destructiveReason(pgplan.Statement{}, "users", false))
	assert.Equal(t, `statement removes live structure from table "users"`, destructiveReason(pgplan.Statement{Destructive: true}, "users", false))
}
