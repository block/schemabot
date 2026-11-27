package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
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
// drop, each with its independent causes, the first of which says why the
// plan drops the table at all.
type tableDropPlan struct {
	order       []string
	definitions map[string]string
	blocked     map[string][]string
}

// planTableDrops decides which undeclared tables the plan drops and in which
// order, from each table's model read at plan time and the reasons the
// target already gives to refuse a table's drop, such as a view that reads
// it or a role that cannot drop it.
//
// A table the target refuses to drop is blocked. So is a table that belongs
// to a partition or inheritance hierarchy: its drop would reach tables, or
// remove rows of a table, the plan does not show. A table that a foreign key
// of a table that stays references is blocked, since PostgreSQL refuses the
// drop without CASCADE, and SchemaBot never cascades. Every other table is
// dropped, and a table whose referencing tables are dropped too is dropped
// after them, so by the time the apply reaches it no foreign key references
// it. Its definition leaves out those references for the same reason: it is
// the table the apply will find. Tables on a cycle of foreign keys have no
// such order and are blocked, and so, in turn, are the tables they
// reference. A table that a foreign key in another namespace references is
// blocked, since drops in different namespaces have no guaranteed order.
func planTableDrops(namespace string, models map[string]schemadiff.Model, refused map[string][]string) tableDropPlan {
	blocked := map[string][]string{}
	for name, m := range models {
		blocked[name] = slices.Clone(refused[name])
		if inTableHierarchy(m) {
			blocked[name] = append(blocked[name], hierarchyDropReason)
		}
		if len(blocked[name]) == 0 {
			delete(blocked, name)
		}
	}
	plan := tableDropPlan{definitions: map[string]string{}, blocked: map[string][]string{}}
	for {
		blockKeptReferences(namespace, models, blocked)
		order, cycle := dropOrder(namespace, models, blocked)
		if len(cycle) == 0 {
			plan.order = order
			break
		}
		for _, name := range cycle {
			blocked[name] = []string{cycleDropReason}
		}
	}
	for name, reasons := range blocked {
		causes := slices.Clone(reasons)
		causes[0] = undeclaredTablePreamble(namespace, name) + "; " + causes[0]
		plan.blocked[name] = causes
	}
	for _, name := range plan.order {
		// Every table that references a table in the order is dropped
		// before it, so the apply finds no incoming foreign key.
		m := models[name]
		m.ReferencedBy = nil
		m.IncomingForeignKeys = nil
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
// key of a kept table, or of a table in another namespace, references, until
// no such table remains: blocking a table keeps it, which can in turn keep a
// table it references.
func blockKeptReferences(namespace string, models map[string]schemadiff.Model, blocked map[string][]string) {
	for changed := true; changed; {
		changed = false
		for _, name := range sortedKeys(models) {
			if _, ok := blocked[name]; ok {
				continue
			}
			var kept, otherNamespaces []string
			for _, fk := range models[name].IncomingForeignKeys {
				if fk.Schema != namespace {
					otherNamespaces = append(otherNamespaces, foreignKeyName(namespace, fk))
					continue
				}
				if _, dropped := droppedReferrer(namespace, fk, models, blocked); !dropped {
					kept = append(kept, foreignKeyName(namespace, fk))
				}
			}
			var reasons []string
			if len(kept) > 0 {
				reasons = append(reasons, keptReferenceDropReason(kept))
			}
			if len(otherNamespaces) > 0 {
				reasons = append(reasons, otherNamespaceReferenceDropReason(otherNamespaces))
			}
			if len(reasons) > 0 {
				blocked[name] = reasons
				changed = true
			}
		}
	}
}

// droppedReferrer resolves an incoming foreign key to the table the plan
// drops that holds it: a table in the plan's namespace, not blocked, with a
// foreign key constraint of that name. A foreign key it cannot resolve that
// way belongs to a table that stays or to a table in another schema, even
// one with the same name.
func droppedReferrer(namespace string, fk schemadiff.IncomingForeignKey, models map[string]schemadiff.Model, blocked map[string][]string) (string, bool) {
	if fk.Schema != namespace {
		return "", false
	}
	if _, ok := blocked[fk.Table]; ok {
		return "", false
	}
	m, ok := models[fk.Table]
	if !ok {
		return "", false
	}
	for _, con := range m.Constraints {
		if strings.HasPrefix(con.Def, "FOREIGN KEY") && con.Name == fk.Constraint {
			return fk.Table, true
		}
	}
	return "", false
}

// foreignKeyName names an incoming foreign key as "table.constraint", with
// the schema in front when the table is in another schema.
func foreignKeyName(namespace string, fk schemadiff.IncomingForeignKey) string {
	if fk.Schema == namespace {
		return fk.Table + "." + fk.Constraint
	}
	return fk.Schema + "." + fk.Table + "." + fk.Constraint
}

// dropOrder orders the tables to drop so each comes after every dropped table
// that references it, choosing by name among tables that are ready so the
// order is stable. When no table is ready, it returns the tables that sit on
// a cycle of foreign keys; the tables those reference wait on the cycle
// without being on it. The caller has blocked every table a kept table
// references, so each incoming foreign key of a table here resolves to a
// dropped table.
func dropOrder(namespace string, models map[string]schemadiff.Model, blocked map[string][]string) (order, cycle []string) {
	waiting := map[string]int{}
	referencedBy := map[string][]string{}
	for _, name := range sortedKeys(models) {
		if _, ok := blocked[name]; ok {
			continue
		}
		waiting[name] += 0
		for _, fk := range models[name].IncomingForeignKeys {
			referrer, _ := droppedReferrer(namespace, fk, models, blocked)
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
			return order, onACycle(waiting, referencedBy)
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

// onACycle returns the waiting tables that reach themselves through the
// foreign keys between waiting tables. When no waiting table is ready, at
// least one cycle exists: each waiting table is referenced by another
// waiting table.
func onACycle(waiting map[string]int, references map[string][]string) []string {
	var members []string
	for _, start := range sortedKeys(waiting) {
		seen := map[string]bool{}
		next := slices.Clone(references[start])
		for len(next) > 0 {
			table := next[len(next)-1]
			next = next[:len(next)-1]
			if _, ok := waiting[table]; !ok || seen[table] {
				continue
			}
			if table == start {
				members = append(members, start)
				break
			}
			seen[table] = true
			next = append(next, references[table]...)
		}
	}
	return members
}

func undeclaredTablePreamble(namespace, table string) string {
	return fmt.Sprintf("table %q exists on the target but no schema file in namespace %q declares it", table, namespace)
}

const hierarchyDropReason = "SchemaBot does not drop a partitioned table or a table in an inheritance hierarchy, because the drop reaches tables the plan does not show" +
	" — declare the table in a schema file, or drop the table tree through a separately reviewed process"

func keptReferenceDropReason(refs []string) string {
	return fmt.Sprintf("foreign key constraint(s) %s of tables that stay reference it, so PostgreSQL refuses to drop it", strings.Join(quoteAll(refs), ", ")) +
		" — remove those constraints or their tables' schema files too and plan again, or declare the table in a schema file"
}

func otherNamespaceReferenceDropReason(refs []string) string {
	return fmt.Sprintf("foreign key constraint(s) %s in other namespaces reference it, and drops in different namespaces have no guaranteed order", strings.Join(quoteAll(refs), ", ")) +
		" — drop those constraints or their tables first, then plan again, or declare the table in a schema file"
}

const cycleDropReason = "its foreign keys form a cycle with other tables the plan drops, so no drop order satisfies PostgreSQL" +
	" — remove one foreign key in the cycle and plan again, or declare the table in a schema file"

func dependentsDropReason(dependents []string) string {
	verb := "depend"
	if len(dependents) == 1 {
		verb = "depends"
	}
	return fmt.Sprintf("%s %s on it, so PostgreSQL refuses to drop it", strings.Join(quoteAll(dependents), ", "), verb) +
		" — drop or change those objects and plan again, or declare the table in a schema file"
}

func privilegeDropReason(err *preflight.PrivilegeError) string {
	return "the engine role cannot drop it, because only the table's owner can drop a table — " + privilegeRemedy(err)
}

// privilegeRemedy is the statement that provisions the access a privilege
// refusal names, with the refusal's hint. The catalog check that failed is
// how the engine proved the access missing, not something the reader runs,
// so it is left out.
func privilegeRemedy(err *preflight.PrivilegeError) string {
	remedy := "provision with: " + err.Grant
	if err.Hint != "" {
		remedy += "; " + err.Hint
	}
	return remedy
}

// tableDropRefusals reads the reasons the target already gives to refuse
// the drop of a table: objects that depend on it, and a role that cannot
// drop it. The server's own refusal at execution stays the backstop for a
// reason that appears after the plan.
func tableDropRefusals(ctx context.Context, pool *pgxpool.Pool, namespace, table string) ([]string, error) {
	var reasons []string
	dependents, err := preflight.ReadTableDropDependents(ctx, pool, namespace, table)
	if err != nil {
		return nil, fmt.Errorf("read the objects that depend on table %q: %w", table, err)
	}
	if len(dependents) > 0 {
		reasons = append(reasons, dependentsDropReason(dependents))
	}
	_, err = preflight.CheckPrivileges(ctx, pool, namespace, table, preflight.Requirement{Tier: tableDropTier})
	if privilegeErr, ok := errors.AsType[*preflight.PrivilegeError](err); ok {
		reasons = append(reasons, privilegeDropReason(privilegeErr))
	} else if err != nil {
		return nil, fmt.Errorf("check the privileges to drop table %q: %w", table, err)
	}
	return reasons, nil
}

// tableDropTier is the access a DROP TABLE needs: PostgreSQL lets only the
// table's owner drop it, and the engine role acts as the owner through
// inheritable membership in the owning role.
const tableDropTier = preflight.TierAlterInPlace

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
	// A table that is gone fails the check as not found; the drop below
	// then reads its absence under its own transaction and skips it.
	if _, err := preflight.CheckPrivileges(ctx, pool, change.namespace, change.table, preflight.Requirement{Tier: tableDropTier}); err != nil && !errors.Is(err, preflight.ErrTableNotFound) {
		return fmt.Errorf("check privileges to drop PostgreSQL table %q.%q: %w", change.namespace, change.table, err)
	}
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
