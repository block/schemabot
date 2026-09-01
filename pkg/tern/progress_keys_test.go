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

func TestStatementIndexForTaskFallsBackToTableWhenStatementUnmatched(t *testing.T) {
	progress := indexEngineTableProgress([]engine.TableProgress{
		{Namespace: "app", Table: "users", DDL: "ALTER TABLE users ADD COLUMN email text", Progress: 90},
	})

	tp, ok := progress.ForTask(&storage.Task{
		Namespace: "app",
		TableName: "users",
		DDL:       "ALTER TABLE users ADD COLUMN name text",
	})
	require.True(t, ok)
	assert.Equal(t, 90, tp.Progress)
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
