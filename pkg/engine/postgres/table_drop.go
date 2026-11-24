package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/progress"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/schema"
)

// reviewedTableDropMetadataKey is the engine.TableChange.Metadata key under
// which the plan records the definition of a table it drops, rendered by
// schemadiff.DroppedTableDefinition. The apply hands it to pg-sprite, which
// reads the table again under its lock and drops nothing when the table no
// longer has that definition.
const reviewedTableDropMetadataKey = "postgres_reviewed_table_drop"

// tableDropPlan is the outcome of planning the drops of a namespace's
// undeclared tables: the tables to drop, in the order the apply must drop
// them, each with its reviewed definition, and the tables the plan cannot
// drop, each with the reason.
type tableDropPlan struct {
	order       []string
	definitions map[string]string
	blocked     map[string]string
}

// planTableDrops decides which undeclared tables the plan drops and in which
// order, from each table's model read at plan time.
//
// A table that belongs to a partition or inheritance hierarchy is blocked: its
// drop would reach tables, or remove rows of a table, the plan does not show.
// A table that a foreign key of a table that stays references is blocked,
// since PostgreSQL refuses the drop without CASCADE, and SchemaBot never
// cascades. Every other table is dropped, and a table whose referencing
// tables are dropped too is dropped after them, so by the time the apply
// reaches it no foreign key references it. Its definition leaves out those
// references for the same reason: it is the table the apply will find.
// Tables whose foreign keys form a cycle have no such order and are blocked.
func planTableDrops(namespace string, models map[string]schemadiff.Model) tableDropPlan {
	plan := tableDropPlan{definitions: map[string]string{}, blocked: map[string]string{}}
	for name, m := range models {
		if inTableHierarchy(m) {
			plan.blocked[name] = hierarchyDropReason(namespace, name)
		}
	}
	for {
		blockKeptReferences(namespace, models, plan.blocked)
		order, cycle := dropOrder(models, plan.blocked)
		if len(cycle) == 0 {
			plan.order = order
			break
		}
		for _, name := range cycle {
			plan.blocked[name] = cycleDropReason(namespace, name)
		}
	}
	for _, name := range plan.order {
		m := models[name]
		m.ReferencedBy = slices.DeleteFunc(slices.Clone(m.ReferencedBy), func(ref string) bool {
			_, dropped := droppedReferrer(ref, models, plan.blocked)
			return dropped
		})
		plan.definitions[name] = schemadiff.DroppedTableDefinition(m)
	}
	return plan
}

// inTableHierarchy reports whether the table is partitioned, a partition, an
// inheritance parent, or an inheritance child.
func inTableHierarchy(m schemadiff.Model) bool {
	return m.PartitionKey != "" || m.IsPartition || len(m.InheritsParents) > 0 || len(m.InheritanceChildren) > 0
}

// blockKeptReferences blocks every table still to be dropped that a foreign
// key of a kept table references, until no such table remains: blocking a
// table keeps it, which can in turn keep a table it references.
func blockKeptReferences(namespace string, models map[string]schemadiff.Model, blocked map[string]string) {
	for changed := true; changed; {
		changed = false
		for _, name := range sortedKeys(models) {
			if _, ok := blocked[name]; ok {
				continue
			}
			var kept []string
			for _, ref := range models[name].ReferencedBy {
				// Introspection names a referencing table without its
				// schema, so a name seen twice belongs to two tables in
				// different schemas, and at most one of them is dropped.
				if _, dropped := droppedReferrer(ref, models, blocked); !dropped || countOf(models[name].ReferencedBy, ref) > 1 {
					kept = append(kept, ref)
				}
			}
			if len(kept) > 0 {
				blocked[name] = keptReferenceDropReason(namespace, name, kept)
				changed = true
			}
		}
	}
}

func countOf(values []string, value string) int {
	n := 0
	for _, v := range values {
		if v == value {
			n++
		}
	}
	return n
}

// droppedReferrer resolves an incoming foreign key, as introspection names it
// ("<table>.<constraint>"), to a table the plan drops that owns a foreign key
// constraint of that name. A reference it cannot resolve that way belongs to
// a table that stays.
func droppedReferrer(ref string, models map[string]schemadiff.Model, blocked map[string]string) (string, bool) {
	for name, m := range models {
		if _, ok := blocked[name]; ok {
			continue
		}
		for _, con := range m.Constraints {
			if strings.HasPrefix(con.Def, "FOREIGN KEY") && name+"."+con.Name == ref {
				return name, true
			}
		}
	}
	return "", false
}

// dropOrder orders the tables to drop so each comes after every dropped table
// that references it, choosing by name among tables that are ready so the
// order is stable. Tables left over sit on a cycle of foreign keys.
// Introspection leaves a table's foreign key to itself out of ReferencedBy,
// so an entry that resolves to the table itself names a same-named table in
// another schema, and the table stays on a cycle of one.
func dropOrder(models map[string]schemadiff.Model, blocked map[string]string) (order, cycle []string) {
	waiting := map[string]int{}
	referencedBy := map[string][]string{}
	for _, name := range sortedKeys(models) {
		if _, ok := blocked[name]; ok {
			continue
		}
		waiting[name] += 0
		for _, ref := range models[name].ReferencedBy {
			referrer, _ := droppedReferrer(ref, models, blocked)
			waiting[name]++
			referencedBy[referrer] = append(referencedBy[referrer], name)
		}
	}
	for len(waiting) > 0 {
		var ready []string
		for name, n := range waiting {
			if n == 0 {
				ready = append(ready, name)
			}
		}
		if len(ready) == 0 {
			return order, sortedKeys(waiting)
		}
		slices.Sort(ready)
		next := ready[0]
		order = append(order, next)
		delete(waiting, next)
		for _, referenced := range referencedBy[next] {
			waiting[referenced]--
		}
	}
	return order, nil
}

func undeclaredTablePreamble(namespace, table string) string {
	return fmt.Sprintf("table %q exists on the target but no schema file in namespace %q declares it", table, namespace)
}

func hierarchyDropReason(namespace, table string) string {
	return undeclaredTablePreamble(namespace, table) +
		"; SchemaBot does not drop a partitioned table or a table in an inheritance hierarchy, because the drop reaches tables the plan does not show" +
		" — declare the table in a schema file, or drop the table tree through a separately reviewed process"
}

func keptReferenceDropReason(namespace, table string, refs []string) string {
	return undeclaredTablePreamble(namespace, table) +
		fmt.Sprintf("; foreign key constraint(s) %s of tables that stay reference it, so PostgreSQL refuses to drop it", strings.Join(quoteAll(refs), ", ")) +
		" — remove those constraints or their tables' schema files too and plan again, or declare the table in a schema file"
}

func cycleDropReason(namespace, table string) string {
	return undeclaredTablePreamble(namespace, table) +
		"; its foreign keys form a cycle with other tables the plan drops, so no drop order satisfies PostgreSQL" +
		" — remove one foreign key in the cycle and plan again, or declare the table in a schema file"
}

// tableDropForApply admits a planned DROP TABLE. The statement must be
// exactly the drop the plan writes for the change's table in the apply's
// namespace, and the change must carry the definition the plan reviewed, so
// the apply drops only that table and only while it still has that
// definition. ok is false when the planned DDL is anything else.
func tableDropForApply(database, namespace string, tc engine.TableChange, statements []string) (change nativeApply, ok bool, err error) {
	if len(statements) != 1 {
		return nativeApply{}, false, nil
	}
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	if err != nil {
		return nativeApply{}, false, fmt.Errorf("select PostgreSQL statement parser: %w", err)
	}
	operation, _, err := parser.Classify(statements[0])
	if err != nil {
		return nativeApply{}, false, fmt.Errorf("classify the planned statement for PostgreSQL table %q: %w", tc.Table, err)
	}
	if operation != ddl.StatementDropTable {
		return nativeApply{}, false, nil
	}
	want := parser.Canonicalize("DROP TABLE " + pgx.Identifier{namespace, tc.Table}.Sanitize())
	if parser.Canonicalize(statements[0]) != want {
		slog.Warn("PostgreSQL apply admission refused a DROP TABLE that is not the plan's drop of its table",
			"database", database, "namespace", namespace, "table", tc.Table)
		return nativeApply{}, false, fmt.Errorf("apply PostgreSQL table %q: the planned DROP TABLE does not drop exactly this table in schema %q; re-plan and apply again", tc.Table, namespace)
	}
	definition := tc.Metadata[reviewedTableDropMetadataKey]
	if definition == "" {
		slog.Warn("PostgreSQL apply admission refused a table drop the plan did not record",
			"database", database, "namespace", namespace, "table", tc.Table)
		return nativeApply{}, false, fmt.Errorf("apply PostgreSQL table %q: the plan does not record the table this statement drops, so the apply cannot check it is still the table that was reviewed; re-plan and apply again", tc.Table)
	}
	return nativeApply{database: database, namespace: namespace, table: tc.Table, sql: tc.DDL, steps: 1, droppedTable: definition}, true, nil
}

// executeTableDrop drops one reviewed table through pg-sprite, under the same
// lock and statement limits as every other optimistic statement. A table that
// is already gone has reached the state the plan asked for, so it is skipped
// with a warning, as the MySQL engine skips one: a resumed apply meets the
// tables its earlier attempt dropped, and anything else that removed the
// table is worth an operator's attention but not a failed apply.
func executeTableDrop(ctx context.Context, pool *pgxpool.Pool, change nativeApply, tracker *progress.Tracker, logger *slog.Logger) (err error) {
	if tracker == nil {
		return fmt.Errorf("drop PostgreSQL table %q: progress tracker is required", change.table)
	}
	tracker.Start(1, progress.OperationOptimistic)
	defer func() { tracker.Finish(err) }()
	err = executor.ExecuteDropTable(ctx, pool, change.namespace, change.table, change.droppedTable, executor.Budget{
		LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
	}, executor.DefaultRetryPolicy())
	if errors.Is(err, executor.ErrTableNotFound) {
		logger.Warn("DROP TABLE target is already absent; nothing to drop",
			"database", change.database, "namespace", change.namespace, "table", change.table)
		metrics.RecordDropTableAlreadyAbsent(ctx, change.database)
		return nil
	}
	if err != nil {
		return fmt.Errorf("drop PostgreSQL table %q.%q: %w", change.namespace, change.table, err)
	}
	return nil
}
