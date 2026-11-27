package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/progress"
	"github.com/block/pg-sprite/pkg/schemadiff"
	pgstatement "github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// indexDropLimit bounds one DROP INDEX CONCURRENTLY, waits included. The drop
// reads no rows and rewrites nothing, so its time goes to waiting out the
// transactions open on the table, and while it waits it blocks no reads or
// writes. It is bounded inside the optimistic apply ceiling, with headroom
// for the session and catalog reads around the drop, so the server's timer
// ends a slow drop and its typed budget error reaches the operator before the
// ceiling cancels the apply.
const indexDropLimit = optimisticApplyCeiling - indexDropHeadroom

// indexDropHeadroom is the part of the optimistic apply ceiling kept for the
// work around a concurrent index drop.
const indexDropHeadroom = time.Minute

// indexDropForApply admits a planned index drop. pg-sprite runs one shape:
// DROP INDEX CONCURRENTLY of one index in the target's schema, checked against
// the definition the plan recorded. Any other DROP INDEX is refused before
// work is queued. A blocking drop never reaches here from a plan, which blocks
// the one the planner writes for a partitioned table's index. It returns
// whether the statement is an index drop.
func indexDropForApply(database, namespace, table, sql string, dropped *schemadiff.DroppedObject) (bool, error) {
	statement, err := pgstatement.ParseOne(sql)
	if err != nil {
		slog.Warn("PostgreSQL apply admission could not classify a planned index drop", "database", database, "table", table, "error", err)
		return false, fmt.Errorf("apply PostgreSQL table %q: planned statement could not be classified", table)
	}
	if statement.Kind() != pgstatement.KindDropIndex {
		return false, nil
	}
	concurrent, err := concurrentIndexDropStatement(sql)
	if err != nil {
		slog.Warn("PostgreSQL apply admission could not classify a planned index drop", "database", database, "table", table, "error", err)
		return false, fmt.Errorf("apply PostgreSQL table %q: planned statement could not be classified", table)
	}
	if !concurrent {
		slog.Warn("PostgreSQL apply admission refused an index drop that is not one DROP INDEX CONCURRENTLY",
			"database", database, "namespace", namespace, "table", table)
		return false, fmt.Errorf("apply PostgreSQL table %q: SchemaBot drops an index only with DROP INDEX CONCURRENTLY of that one index; re-plan and apply again", table)
	}
	index := statement.IndexRelation()
	if index.Schema != namespace {
		slog.Warn("PostgreSQL apply admission refused an index drop in another schema",
			"database", database, "namespace", namespace, "table", table, "index_schema", index.Schema, "index", index.Name)
		return false, fmt.Errorf("apply PostgreSQL table %q: the planned index drop names schema %q but the apply targets schema %q; the target's schema route changed after planning, so plan again", table, index.Schema, namespace)
	}
	if dropped == nil || dropped.Kind != schemadiff.DroppedIndex || dropped.Name != index.Name {
		slog.Warn("PostgreSQL apply admission refused an index drop whose reviewed record does not name the index",
			"database", database, "namespace", namespace, "table", table, "index", index.Name)
		return false, fmt.Errorf("apply PostgreSQL table %q: the plan's record of the index this statement drops does not name index %q; re-plan and apply again", table, index.Name)
	}
	return true, nil
}

// executeIndexDrop drops the reviewed index through pg-sprite's concurrent
// index drop. Right before the drop, pg-sprite checks the index is still a
// valid plain index of the table with the definition the plan recorded and is
// not the table's replica identity, and drops nothing when it is not. The
// drop of a changed index's old definition also checks that the index the
// plan built to replace it is still valid with the definition it built. The
// table's size does not bound the drop, which scans no rows.
func executeIndexDrop(ctx context.Context, pool *pgxpool.Pool, change nativeApply, tracker *progress.Tracker, logger *slog.Logger) (err error) {
	if tracker == nil {
		return fmt.Errorf("drop reviewed index %q on PostgreSQL table %q: progress tracker is required", change.dropped.Name, change.table)
	}
	tracker.Start(1, progress.OperationOptimistic)
	defer func() { tracker.Finish(err) }()
	budget := executor.ConcurrentBudget{Overall: indexDropLimit}
	var dropped executor.DroppedIndex
	if change.replacement != nil {
		dropped, err = executor.ExecuteReplacedIndexDropConcurrently(ctx, pool, change.namespace, change.table, change.dropped.Name, change.dropped.Definition,
			*change.replacement, budget)
	} else {
		dropped, err = executor.ExecuteDropIndexConcurrently(ctx, pool, change.namespace, change.table, change.dropped.Name, change.dropped.Definition, budget)
	}
	if err != nil {
		return fmt.Errorf("drop reviewed index %q on PostgreSQL table %q.%q: %w", change.dropped.Name, change.namespace, change.table,
			nameInterruptedIndexDrop(err, change.namespace, change.dropped.Name, indexDropLimit))
	}
	logger.Info("PostgreSQL index dropped concurrently",
		"database", change.database,
		"namespace", change.namespace,
		"table", change.table,
		"index", change.dropped.Name,
		"elapsed", dropped.Duration)
	return nil
}

// indexDropInterruptedError reports that a concurrent index drop was
// cancelled before it finished: by its own limit firing, by the apply's
// context ending, or by a cancel from outside SchemaBot. A cancelled DROP
// INDEX CONCURRENTLY can leave the index in place and invalid. A plan never
// drops an invalid index, so the operator finishes that drop by hand.
type indexDropInterruptedError struct {
	schema string
	index  string
	// limit is the drop's time limit; timedOut reports that the limit is
	// what cancelled it.
	limit    time.Duration
	timedOut bool
	err      error
}

func (e *indexDropInterruptedError) Error() string {
	if e.timedOut {
		return fmt.Sprintf("the concurrent drop of index %q ran past %s and was cancelled", e.index, e.limit)
	}
	return fmt.Sprintf("the concurrent drop of index %q was cancelled before it finished", e.index)
}

func (e *indexDropInterruptedError) Unwrap() error { return e.err }

// finishDropStatement is the statement that finishes the drop by hand, with
// the index schema-qualified and quoted as SQL identifiers so it runs as
// shown whatever the operator's search_path.
func (e *indexDropInterruptedError) finishDropStatement() string {
	return "DROP INDEX CONCURRENTLY " + pgx.Identifier{e.schema, e.index}.Sanitize()
}

// nameInterruptedIndexDrop wraps a drop failure that cancelled the drop
// mid-flight, so the refusal names the index and how to finish the drop.
// Every other failure passes through unchanged. The drop runs with
// lock_timeout disabled, so only the statement cause of a budget error is
// the limit's.
func nameInterruptedIndexDrop(err error, schema, index string, limit time.Duration) error {
	var budgetErr *executor.BudgetError
	switch {
	case errors.As(err, &budgetErr) && budgetErr.Cause == executor.CauseStatement:
		return &indexDropInterruptedError{schema: schema, index: index, limit: limit, timedOut: true, err: err}
	case errors.Is(err, executor.ErrCancelledByCaller), errors.Is(err, executor.ErrCancelledExternally):
		return &indexDropInterruptedError{schema: schema, index: index, limit: limit, err: err}
	default:
		return err
	}
}
