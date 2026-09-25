package tern

import (
	"log/slog"
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
	}, nil)

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
	}, nil)

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

// A deployment reports each statement as its own engine emitted it — qualified
// with that deployment's physical schema — while the stored task carries the
// reviewed text from the deployment that planned. With several statements on
// one table, only a canonical index can tell which entry is which; matching
// the text would miss both and leave the tasks unaccounted for.
func TestStatementIndexCanonicalMatchesDeploymentRenderingOfReviewedStatement(t *testing.T) {
	const (
		reviewedCreate = `CREATE TABLE "app-region-a".recall (id bigint NOT NULL, consumer_uuid text, CONSTRAINT recall_pkey PRIMARY KEY (id))`
		reviewedIndex  = `CREATE INDEX idx_recall_consumer_uuid ON "app-region-a".recall USING btree (consumer_uuid)`
		renderedCreate = `CREATE TABLE "app-region-b".recall (id bigint NOT NULL, consumer_uuid text, CONSTRAINT recall_pkey PRIMARY KEY (id))`
		renderedIndex  = `CREATE INDEX idx_recall_consumer_uuid ON "app-region-b".recall USING btree (consumer_uuid)`
	)
	canon, err := StatementCanonicalizerForDatabaseType("postgres", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	remote := []*ternv1.TableProgress{
		{Namespace: "app", TableName: "recall", Ddl: renderedCreate, Status: "completed", PercentComplete: 100},
		{Namespace: "app", TableName: "recall", Ddl: renderedIndex, Status: "running", PercentComplete: 40},
	}
	createTask := &storage.Task{Namespace: "app", TableName: "recall", DDL: reviewedCreate}
	indexTask := &storage.Task{Namespace: "app", TableName: "recall", DDL: reviewedIndex}

	byText := IndexProtoTableProgress(remote, nil)
	_, ok := byText.ForTask(createTask)
	require.False(t, ok)
	_, ok = byText.ForTask(indexTask)
	require.False(t, ok)

	byCanon := IndexProtoTableProgress(remote, canon)
	createTP, ok := byCanon.ForTask(createTask)
	require.True(t, ok)
	assert.Equal(t, "completed", createTP.Status)
	indexTP, ok := byCanon.ForTask(indexTask)
	require.True(t, ok)
	assert.Equal(t, "running", indexTP.Status)
	assert.Equal(t, int32(40), indexTP.PercentComplete)
}

// A canonical index still refuses a statement that is a different change,
// not a different spelling: the sibling index on another column must not be
// handed this task's progress.
func TestStatementIndexCanonicalStillMissesDifferentStatement(t *testing.T) {
	canon, err := StatementCanonicalizerForDatabaseType("postgres", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	progress := IndexProtoTableProgress([]*ternv1.TableProgress{
		{Namespace: "app", TableName: "recall", Ddl: `CREATE TABLE "app-region-b".recall (id bigint PRIMARY KEY)`, Status: "completed"},
		{Namespace: "app", TableName: "recall", Ddl: `CREATE INDEX idx_recall_agency ON "app-region-b".recall (agency_id)`, Status: "running"},
	}, canon)

	tp, ok := progress.ForTask(&storage.Task{
		Namespace: "app",
		TableName: "recall",
		DDL:       `CREATE INDEX idx_recall_consumer ON "app-region-a".recall (consumer_uuid)`,
	})
	require.False(t, ok)
	require.Nil(t, tp)
}

func TestStatementCanonicalizerForDatabaseType(t *testing.T) {
	t.Run("blank stays blank so it still means the table as a whole", func(t *testing.T) {
		canon, err := StatementCanonicalizerForDatabaseType("postgres", slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		assert.Empty(t, canon("  \n"))
	})
	t.Run("text the parser rejects keys by its trimmed form and says so once", func(t *testing.T) {
		// Keying by text is the degraded path this canonicalizer exists to
		// avoid, so each statement it happens to is named in the log — once,
		// since the memo answers every later request for the same text.
		var records []capturedLog
		canon, err := StatementCanonicalizerForDatabaseType("mysql", slog.New(captureHandler{records: &records}))
		require.NoError(t, err)
		assert.Equal(t, "this is not sql", canon("  this is not sql "))
		assert.Equal(t, "this is not sql", canon("this is not sql"))
		assert.Equal(t, "this is not sql", canon("  this is not sql "))

		require.Len(t, records, 1)
		assert.Equal(t, slog.LevelWarn, records[0].level)
		assert.Equal(t, "statement is matched by its text because the dialect parser rejected it", records[0].msg)
		assert.Equal(t, "mysql", records[0].attrs["database_type"])
		assert.Equal(t, "this is not sql", records[0].attrs["ddl"])
		require.Contains(t, records[0].attrs, "error")
		assert.Error(t, records[0].attrs["error"].(error))
	})
	t.Run("text the parser accepts is not logged", func(t *testing.T) {
		var records []capturedLog
		canon, err := StatementCanonicalizerForDatabaseType("postgres", slog.New(captureHandler{records: &records}))
		require.NoError(t, err)
		assert.NotEmpty(t, canon(`CREATE TABLE "app-region-a".recall (id bigint PRIMARY KEY)`))
		assert.Empty(t, records)
	})
	t.Run("unregistered database type is an error", func(t *testing.T) {
		_, err := StatementCanonicalizerForDatabaseType("", slog.New(slog.DiscardHandler))
		require.Error(t, err)
	})
}

func TestIndexProtoTableProgressSkipsNilEntries(t *testing.T) {
	progress := IndexProtoTableProgress([]*ternv1.TableProgress{
		nil,
		{Namespace: "app", TableName: "users", PercentComplete: 5},
	}, nil)

	tp, ok := progress.ForTask(&storage.Task{Namespace: "app", TableName: "users"})
	require.True(t, ok)
	assert.Equal(t, int32(5), tp.PercentComplete)
}
