package tern

import (
	"strings"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

const progressTableKeySep = "\x00"

func progressTableKey(namespace, table string) string {
	return namespace + progressTableKeySep + table
}

func progressStatementKey(namespace, table, ddl string) string {
	return progressTableKey(namespace, table) + progressTableKeySep + strings.TrimSpace(ddl)
}

// StatementIndex matches entries that describe schema-change work on a table —
// stored tasks, an engine's progress, a remote data plane's progress — across
// the boundary between them.
//
// A task is one statement, and a table can carry several statements in one
// apply, so an entry that names its DDL is matched by that same statement on
// that (namespace, table). An entry without DDL describes the table as a whole
// and matches every statement on it. The two planes mint task identifiers
// independently, so the statement text is the only identity that travels
// between them intact.
type StatementIndex[T any] struct {
	byStatement map[string]*T
	byTable     map[string]*T
}

// NewStatementIndex returns an empty index sized for about size entries.
func NewStatementIndex[T any](size int) StatementIndex[T] {
	return StatementIndex[T]{
		byStatement: make(map[string]*T, size),
		byTable:     make(map[string]*T, size),
	}
}

// Add records an entry for the table and, when ddl is not blank, for that
// statement on the table. Must be called on an index from NewStatementIndex.
func (ix StatementIndex[T]) Add(namespace, table, ddl string, entry *T) {
	if strings.TrimSpace(ddl) != "" {
		ix.byStatement[progressStatementKey(namespace, table, ddl)] = entry
	}
	ix.byTable[progressTableKey(namespace, table)] = entry
}

// Lookup returns the entry for the statement on the table when ddl is not
// blank and one was added for it, otherwise the entry for the table.
func (ix StatementIndex[T]) Lookup(namespace, table, ddl string) (*T, bool) {
	if strings.TrimSpace(ddl) != "" {
		if entry, ok := ix.byStatement[progressStatementKey(namespace, table, ddl)]; ok {
			return entry, true
		}
	}
	entry, ok := ix.byTable[progressTableKey(namespace, table)]
	return entry, ok
}

// ForTask returns the entry describing the task: its own statement's entry
// when one was recorded, otherwise the entry for its table.
func (ix StatementIndex[T]) ForTask(task *storage.Task) (*T, bool) {
	return ix.Lookup(task.Namespace, task.TableName, task.DDL)
}

func indexEngineTableProgress(tables []engine.TableProgress) StatementIndex[engine.TableProgress] {
	index := NewStatementIndex[engine.TableProgress](len(tables))
	for i := range tables {
		tp := &tables[i]
		index.Add(tp.Namespace, tp.Table, tp.DDL, tp)
	}
	return index
}

// IndexProtoTableProgress indexes the per-table entries of a data plane's
// progress response for lookup by stored task.
func IndexProtoTableProgress(tables []*ternv1.TableProgress) StatementIndex[ternv1.TableProgress] {
	index := NewStatementIndex[ternv1.TableProgress](len(tables))
	for _, tp := range tables {
		if tp == nil {
			continue
		}
		index.Add(tp.Namespace, tp.TableName, tp.Ddl, tp)
	}
	return index
}
