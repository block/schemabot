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

// StatementIndex matches entries that describe schema change work on a table —
// stored tasks, an engine's progress, a remote data plane's progress — across
// the boundary between them.
//
// A task is one statement, and a table can carry several statements in one
// apply, so an entry that names its DDL is matched by that same statement on
// that (namespace, table). The two planes mint task identifiers independently,
// so the statement text is the only identity that travels between them intact.
//
// A lookup that names a statement but finds no entry for it falls back to the
// table's entry only while that entry is unambiguous: a table with a single
// entry can only be describing this statement's work — whether the entry
// omits its DDL, or an engine that runs a table's statements as one change
// reports the combined text. Once a table has several entries, a statement
// that matches none of them is unaccounted for, and the lookup reports a miss
// rather than hand back a sibling statement's progress.
type StatementIndex[T any] struct {
	byStatement map[string]*T
	byTable     map[string]tableEntries[T]
}

// tableEntries is the last entry recorded for a table and how many entries the
// table has received in total.
type tableEntries[T any] struct {
	last  *T
	count int
}

// NewStatementIndex returns an empty index sized for about size entries.
func NewStatementIndex[T any](size int) StatementIndex[T] {
	return StatementIndex[T]{
		byStatement: make(map[string]*T, size),
		byTable:     make(map[string]tableEntries[T], size),
	}
}

// Add records an entry for the table and, when ddl is not blank, for that
// statement on the table. Must be called on an index from NewStatementIndex.
func (ix StatementIndex[T]) Add(namespace, table, ddl string, entry *T) {
	if strings.TrimSpace(ddl) != "" {
		ix.byStatement[progressStatementKey(namespace, table, ddl)] = entry
	}
	tableKey := progressTableKey(namespace, table)
	ix.byTable[tableKey] = tableEntries[T]{last: entry, count: ix.byTable[tableKey].count + 1}
}

// Lookup returns the entry for the statement on the table when ddl is not
// blank and one was added for it. Otherwise it returns the table's entry when
// the table has exactly one, or when ddl is blank and the caller is asking
// about the table as a whole. A statement that matches none of a table's
// several entries is a miss.
func (ix StatementIndex[T]) Lookup(namespace, table, ddl string) (*T, bool) {
	hasStatement := strings.TrimSpace(ddl) != ""
	if hasStatement {
		if entry, ok := ix.byStatement[progressStatementKey(namespace, table, ddl)]; ok {
			return entry, true
		}
	}
	entries, ok := ix.byTable[progressTableKey(namespace, table)]
	if !ok {
		return nil, false
	}
	if hasStatement && entries.count > 1 {
		return nil, false
	}
	return entries.last, true
}

// ForTask returns the entry describing the task: its own statement's entry
// when one was recorded, otherwise the table's entry while it is unambiguous.
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
