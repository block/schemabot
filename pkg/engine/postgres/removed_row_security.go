package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

// A file without row security clauses leaves the table's row security alone,
// so a file written before its table's row security was managed keeps
// working. The same rule would turn the removal of clauses SchemaBot applied,
// such as a revert of the change that added them, into a plan that changes
// nothing while the table keeps every policy. A table whose clauses the files
// last applied carried, and whose live row security is still there, is
// refused instead, naming the two ways to say what the file means.

// removedRowSecurityGate refuses, within one namespace of a plan, a file
// that no longer declares row security the files last applied declared. The
// apply history is read only for a namespace with a table that has live row
// security and a file that declares none, so a plan that cannot hit the
// refusal never pays for the history.
type removedRowSecurityGate struct {
	database  string
	namespace string
	pool      *pgxpool.Pool
	applied   engine.AppliedSchemaFiles

	// live are the namespace's tables with row security on the target; nil
	// until first read.
	live map[string]bool
	// lastApplied are the tables whose file declared row security in the
	// files last applied; nil until first read.
	lastApplied map[string]bool
}

func newRemovedRowSecurityGate(database, namespace string, pool *pgxpool.Pool, applied engine.AppliedSchemaFiles) *removedRowSecurityGate {
	return &removedRowSecurityGate{database: database, namespace: namespace, pool: pool, applied: applied}
}

// check refuses table's file, which declares no row security, when the
// files last applied declared the table's row security and the live table
// still has some. A table with none left, or not there yet, has nothing a
// plan would leave behind.
func (g *removedRowSecurityGate) check(ctx context.Context, table string) error {
	if g.applied == nil {
		return nil
	}
	if g.live == nil {
		live, err := tablesWithRowSecurity(ctx, g.pool, g.namespace)
		if err != nil {
			return err
		}
		g.live = live
	}
	if !g.live[table] {
		return nil
	}
	if g.lastApplied == nil {
		files, err := g.applied(ctx, g.namespace)
		if err != nil {
			return fmt.Errorf("read the schema files last applied to namespace %q: %w", g.namespace, err)
		}
		g.lastApplied = rowSecurityLastApplied(g.database, g.namespace, files)
	}
	if !g.lastApplied[table] {
		return nil
	}
	return refuseRemovedRowSecurity(ctx, g.pool, g.namespace, table)
}

// tablesWithRowSecurityQuery lists the tables of a schema that have row
// level security enabled or forced, or any policy.
const tablesWithRowSecurityQuery = `
SELECT c.relname
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
WHERE n.nspname OPERATOR(pg_catalog.=) $1
  AND (c.relrowsecurity OR c.relforcerowsecurity
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_policy AS p WHERE p.polrelid OPERATOR(pg_catalog.=) c.oid))`

// tablesWithRowSecurity returns the tables of namespace with row security
// on the target.
func tablesWithRowSecurity(ctx context.Context, pool *pgxpool.Pool, namespace string) (map[string]bool, error) {
	rows, err := pool.Query(ctx, tablesWithRowSecurityQuery, namespace)
	if err != nil {
		return nil, fmt.Errorf("list the tables with row security in namespace %q: %w", namespace, err)
	}
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("read the tables with row security in namespace %q: %w", namespace, err)
	}
	live := make(map[string]bool, len(tables))
	for _, table := range tables {
		live[table] = true
	}
	return live, nil
}

// rowSecurityLastApplied returns the tables of namespace whose schema file,
// in the files last applied, declared row security. A file that declared
// only DISABLE ROW LEVEL SECURITY counts too: its author managed the table's
// row security, so a later file that drops the declaration while the table
// has row security again is just as ambiguous. A stored file that no
// longer parses is logged and skipped: it was admitted when it was applied,
// and refusing every later plan over it would leave no plan that could
// replace it.
func rowSecurityLastApplied(database, namespace string, applied *schema.Namespace) map[string]bool {
	tables := make(map[string]bool)
	if applied == nil {
		return tables
	}
	for _, filename := range sortedKeys(applied.Files) {
		file, err := statement.ParseDesiredFile(applied.Files[filename])
		if err != nil {
			slog.Warn("a schema file last applied no longer parses; the plan cannot tell whether it removed row security that file declared",
				"database", database, "namespace", namespace, "file", filename, "error", err)
			continue
		}
		if rowSecurity, ok := file.RowSecurity(); ok {
			tables[rowSecurity.Table()] = true
		}
	}
	return tables
}

// refuseRemovedRowSecurity reads table's live row security and refuses the
// file that no longer declares it.
func refuseRemovedRowSecurity(ctx context.Context, pool *pgxpool.Pool, namespace, table string) error {
	live, err := schemadiff.Introspect(ctx, pool, namespace, table)
	if errors.Is(err, schemadiff.ErrTableNotFound) {
		slog.Debug("table with row security was dropped while the plan read it; nothing to refuse",
			"namespace", namespace, "table", table)
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the row security of table %q.%q: %w", namespace, table, err)
	}
	if !hasRowSecurity(live.RowSecurity) {
		slog.Debug("table's row security was removed while the plan read it; nothing to refuse",
			"namespace", namespace, "table", table)
		return nil
	}
	return &removedRowSecurityError{table: table, live: live.RowSecurity}
}

func hasRowSecurity(rs schemadiff.RowSecurity) bool {
	return rs.Enabled || rs.Forced || len(rs.Policies) > 0
}

// removedRowSecurityError refuses a file whose row security clauses were
// removed while the table kept its row security.
type removedRowSecurityError struct {
	table string
	live  schemadiff.RowSecurity
}

func (e *removedRowSecurityError) Error() string {
	return fmt.Sprintf("add ALTER TABLE %s DISABLE ROW LEVEL SECURITY; to %s on table %q, or declare the row security the table keeps. "+
		"The schema file SchemaBot last applied declared this table's row security, and a file without row security clauses leaves it as it is",
		pgx.Identifier{e.table}.Sanitize(), removalOf(e.live), e.table)
}

// removalOf names what the removal does to a table's row security, such as
// `turn off row level security and drop policy "read all"`.
func removalOf(rs schemadiff.RowSecurity) string {
	var parts []string
	if rs.Enabled || rs.Forced {
		parts = append(parts, "turn off row level security")
	}
	if len(rs.Policies) > 0 {
		names := make([]string, len(rs.Policies))
		for i, policy := range rs.Policies {
			names[i] = pgx.Identifier{policy.Name}.Sanitize()
		}
		noun := "policy"
		if len(names) > 1 {
			noun = "policies"
		}
		parts = append(parts, "drop "+noun+" "+strings.Join(names, ", "))
	}
	return strings.Join(parts, " and ")
}
