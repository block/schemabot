package tern

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
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
//
// Across the plane boundary the two sides spell the same statement
// differently: a deployment stores and reports the text its own engine
// emitted, qualified with its own physical schema, while the control plane's
// task rows carry the reviewed text (RV-1). An index built with a
// StatementCanonicalizer keys statements by their canonical form so the two
// spellings meet; an index without one keys by the trimmed text.
type StatementIndex[T any] struct {
	byStatement map[string]*T
	byTable     map[string]tableEntries[T]
	canon       StatementCanonicalizer
}

// tableEntries is the last entry recorded for a table and how many entries the
// table has received in total.
type tableEntries[T any] struct {
	last  *T
	count int
}

// StatementCanonicalizer reduces a statement to the form two renderings of the
// same change share. It must return "" for blank input and a non-empty string
// otherwise, so blank DDL still means "the table as a whole".
type StatementCanonicalizer func(ddl string) string

// StatementCanonicalizerForDatabaseType returns the canonicalizer for a
// database type's dialect: the drift comparison's canonical form, which
// strips the physical schema qualifier and normalizes spelling, so a
// deployment's rendering of a reviewed statement keys the same as the
// reviewed text. Text the dialect's parser rejects keys by its trimmed form
// instead — the drift comparison has already refused such text before any
// apply, so at this point it can only belong to work that never ran. An
// unregistered database type is an error.
//
// Canonicalizing is a full parse and deparse, and one sync pass or progress
// request asks for the same statement several times — once indexing it, once
// looking it up, once comparing renderings — so the returned canonicalizer
// remembers every answer for its lifetime. It is therefore scoped to a single
// pass and not safe for concurrent use: build one per call site, never share
// one across goroutines or keep one alive across passes.
func StatementCanonicalizerForDatabaseType(databaseType string) (StatementCanonicalizer, error) {
	parser, err := ddl.ParserForDialect(schema.DialectForDatabaseType(databaseType))
	if err != nil {
		return nil, fmt.Errorf("statement canonicalizer for database type %q: %w", databaseType, err)
	}
	memo := map[string]string{}
	return func(raw string) string {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return ""
		}
		if canonical, ok := memo[raw]; ok {
			return canonical
		}
		canonical, err := canonicalDDLForDrift(parser, raw)
		if err != nil {
			canonical = raw
		}
		memo[raw] = canonical
		return canonical
	}, nil
}

// NewStatementIndex returns an empty index sized for about size entries that
// keys statements by their trimmed text.
func NewStatementIndex[T any](size int) StatementIndex[T] {
	return NewCanonicalStatementIndex[T](size, nil)
}

// NewCanonicalStatementIndex returns an empty index sized for about size
// entries that keys statements by canon; a nil canon keys by trimmed text.
func NewCanonicalStatementIndex[T any](size int, canon StatementCanonicalizer) StatementIndex[T] {
	return StatementIndex[T]{
		byStatement: make(map[string]*T, size),
		byTable:     make(map[string]tableEntries[T], size),
		canon:       canon,
	}
}

func (ix StatementIndex[T]) statementKey(ddl string) string {
	if ix.canon != nil {
		return ix.canon(ddl)
	}
	return strings.TrimSpace(ddl)
}

// Add records an entry for the table and, when ddl is not blank, for that
// statement on the table. Must be called on an index from NewStatementIndex
// or NewCanonicalStatementIndex.
func (ix StatementIndex[T]) Add(namespace, table, ddl string, entry *T) {
	if key := ix.statementKey(ddl); key != "" {
		ix.byStatement[progressStatementKey(namespace, table, key)] = entry
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
	key := ix.statementKey(ddl)
	hasStatement := key != ""
	if hasStatement {
		if entry, ok := ix.byStatement[progressStatementKey(namespace, table, key)]; ok {
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
// progress response for lookup by stored task, keying statements by canon
// (see NewCanonicalStatementIndex).
func IndexProtoTableProgress(tables []*ternv1.TableProgress, canon StatementCanonicalizer) StatementIndex[ternv1.TableProgress] {
	index := NewCanonicalStatementIndex[ternv1.TableProgress](len(tables), canon)
	for _, tp := range tables {
		if tp == nil {
			continue
		}
		index.Add(tp.Namespace, tp.TableName, tp.Ddl, tp)
	}
	return index
}
