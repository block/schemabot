package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/progress"
	"github.com/block/pg-sprite/pkg/schemadiff"
	pgstatement "github.com/block/pg-sprite/pkg/statement"
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
// table's size does not bound the drop, which scans no rows.
func executeIndexDrop(ctx context.Context, pool *pgxpool.Pool, change nativeApply, tracker *progress.Tracker, logger *slog.Logger) (err error) {
	if tracker == nil {
		return fmt.Errorf("drop reviewed index %q on PostgreSQL table %q: progress tracker is required", change.dropped.Name, change.table)
	}
	tracker.Start(1, progress.OperationOptimistic)
	defer func() { tracker.Finish(err) }()
	dropped, err := executor.ExecuteDropIndexConcurrently(ctx, pool, change.namespace, change.table, change.dropped.Name, change.dropped.Definition,
		executor.ConcurrentBudget{Overall: indexDropLimit})
	if err != nil {
		return fmt.Errorf("drop reviewed index %q on PostgreSQL table %q.%q: %w", change.dropped.Name, change.namespace, change.table, err)
	}
	logger.Info("PostgreSQL index dropped concurrently",
		"database", change.database,
		"namespace", change.namespace,
		"table", change.table,
		"index", change.dropped.Name,
		"elapsed", dropped.Duration)
	return nil
}
