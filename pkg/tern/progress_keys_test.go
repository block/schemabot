package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

func TestStatementIndexForTaskUsesNamespace(t *testing.T) {
	progress := indexEngineTableProgress([]engine.TableProgress{
		{
			Namespace: "commerce_sharded",
			Table:     "orders",
			Progress:  25,
		},
		{
			Namespace: "commerce_sharded_006",
			Table:     "orders",
			Progress:  100,
		},
	})

	tp, ok := progress.ForTask(&storage.Task{
		Namespace: "commerce_sharded_006",
		TableName: "orders",
	})
	require.True(t, ok)
	require.Equal(t, "commerce_sharded_006", tp.Namespace)
	require.Equal(t, 100, tp.Progress)
}

func TestStatementIndexForTaskRequiresNamespaceMatch(t *testing.T) {
	progress := indexEngineTableProgress([]engine.TableProgress{
		{
			Table:    "users",
			Progress: 50,
		},
	})

	tp, ok := progress.ForTask(&storage.Task{
		Namespace: "app",
		TableName: "users",
	})
	require.False(t, ok)
	require.Nil(t, tp)
}

// A table can carry several statements in one apply, each its own task. When
// the progress entries name their statement, every task resolves to the entry
// for its own statement rather than all of them sharing the last one reported.
func TestStatementIndexForTaskResolvesEachStatementToItsOwnTask(t *testing.T) {
	const (
		createDDL = "CREATE TABLE public.users (id bigint PRIMARY KEY)"
		indexDDL  = "CREATE INDEX users_email_idx ON public.users (email)"
	)
	progress := IndexProtoTableProgress([]*ternv1.TableProgress{
		{Namespace: "app", TableName: "users", Ddl: createDDL, Status: "completed", PercentComplete: 100},
		{Namespace: "app", TableName: "users", Ddl: indexDDL, Status: "running", PercentComplete: 40},
	})

	createTP, ok := progress.ForTask(&storage.Task{Namespace: "app", TableName: "users", DDL: createDDL})
	require.True(t, ok)
	assert.Equal(t, "completed", createTP.Status)
	assert.Equal(t, int32(100), createTP.PercentComplete)

	indexTP, ok := progress.ForTask(&storage.Task{Namespace: "app", TableName: "users", DDL: indexDDL})
	require.True(t, ok)
	assert.Equal(t, "running", indexTP.Status)
	assert.Equal(t, int32(40), indexTP.PercentComplete)
}

func TestStatementIndexForTaskMatchesStatementIgnoringSurroundingWhitespace(t *testing.T) {
	progress := indexEngineTableProgress([]engine.TableProgress{
		{Namespace: "app", Table: "users", DDL: "ALTER TABLE users ADD COLUMN email text\n", Progress: 70},
	})

	tp, ok := progress.ForTask(&storage.Task{
		Namespace: "app",
		TableName: "users",
		DDL:       "  ALTER TABLE users ADD COLUMN email text",
	})
	require.True(t, ok)
	assert.Equal(t, 70, tp.Progress)
}

// Entries without DDL describe the table as a whole and resolve to every task
// on that table, whether or not the task carries its own statement.
func TestStatementIndexForTaskFallsBackToTableEntry(t *testing.T) {
	progress := indexEngineTableProgress([]engine.TableProgress{
		{Namespace: "app", Table: "users", Progress: 60},
	})

	tp, ok := progress.ForTask(&storage.Task{
		Namespace: "app",
		TableName: "users",
		DDL:       "ALTER TABLE users ADD COLUMN email text",
	})
	require.True(t, ok)
	assert.Equal(t, 60, tp.Progress)

	tp, ok = progress.ForTask(&storage.Task{Namespace: "app", TableName: "users"})
	require.True(t, ok)
	assert.Equal(t, 60, tp.Progress)
}

// An engine that runs a table's statements as one change reports the combined
// text as the entry's DDL, which matches no single task's statement. With one
// entry on the table there is nothing else that entry could describe, so every
// task on the table resolves to it.
func TestStatementIndexForTaskFallsBackToSoleTableEntryWhenStatementUnmatched(t *testing.T) {
	progress := indexEngineTableProgress([]engine.TableProgress{
		{
			Namespace: "app",
			Table:     "users",
			DDL:       "ALTER TABLE users ADD COLUMN email text; ALTER TABLE users ADD COLUMN name text",
			Progress:  90,
		},
	})

	for _, statement := range []string{
		"ALTER TABLE users ADD COLUMN email text",
		"ALTER TABLE users ADD COLUMN name text",
	} {
		tp, ok := progress.ForTask(&storage.Task{Namespace: "app", TableName: "users", DDL: statement})
		require.True(t, ok, statement)
		assert.Equal(t, 90, tp.Progress, statement)
	}
}

// Once a table has several entries, a task whose statement matches none of
// them is a miss: handing back a sibling statement's entry would report that
// sibling's progress and terminal state as this task's.
func TestStatementIndexForTaskMissesWhenStatementUnmatchedAmongSeveralEntries(t *testing.T) {
	progress := IndexProtoTableProgress([]*ternv1.TableProgress{
		{Namespace: "app", TableName: "users", Ddl: "CREATE TABLE public.users (id bigint PRIMARY KEY)", Status: "completed"},
		{Namespace: "app", TableName: "users", Ddl: "CREATE INDEX users_email_idx ON public.users (email)", Status: "running"},
	})

	tp, ok := progress.ForTask(&storage.Task{
		Namespace: "app",
		TableName: "users",
		DDL:       "CREATE INDEX users_name_idx ON public.users (name)",
	})
	require.False(t, ok)
	require.Nil(t, tp)

	// A task without a statement asks about the table as a whole and still
	// resolves to the table's entry.
	tp, ok = progress.ForTask(&storage.Task{Namespace: "app", TableName: "users"})
	require.True(t, ok)
	assert.Equal(t, "running", tp.Status)
}

func TestStatementIndexTreatsWhitespaceOnlyDDLAsTableEntry(t *testing.T) {
	progress := indexEngineTableProgress([]engine.TableProgress{
		{Namespace: "app", Table: "users", DDL: "  \n", Progress: 15},
	})

	tp, ok := progress.ForTask(&storage.Task{Namespace: "app", TableName: "users"})
	require.True(t, ok)
	assert.Equal(t, 15, tp.Progress)
}

func TestStatementIndexZeroValueResolvesNothing(t *testing.T) {
	var progress StatementIndex[engine.TableProgress]

	tp, ok := progress.ForTask(&storage.Task{Namespace: "app", TableName: "users", DDL: "x"})
	require.False(t, ok)
	require.Nil(t, tp)
}

func TestIndexProtoTableProgressSkipsNilEntries(t *testing.T) {
	progress := IndexProtoTableProgress([]*ternv1.TableProgress{
		nil,
		{Namespace: "app", TableName: "users", PercentComplete: 5},
	})

	tp, ok := progress.ForTask(&storage.Task{Namespace: "app", TableName: "users"})
	require.True(t, ok)
	assert.Equal(t, int32(5), tp.PercentComplete)
}
