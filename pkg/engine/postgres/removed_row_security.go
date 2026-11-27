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

	"github.com/block/schemabot/pkg/schema"
)

// A file without row security clauses leaves the table's row security alone,
// so a file written before its table's row security was managed keeps
// working. The same rule would turn the removal of clauses SchemaBot applied,
// such as a revert of the change that added them, into a plan that changes
// nothing while the table keeps every policy. A table whose clauses the files
// last applied carried, and whose live row security is still there, is
// refused instead, naming the two ways to say what the file means.

// rowSecurityLastApplied returns the tables of namespace whose schema file,
// in the files last applied, declared row security. A stored file that no
// longer parses is logged and skipped: it was admitted when it was applied,
// and refusing every later plan over it would leave no plan that could
// replace it.
func rowSecurityLastApplied(database, namespace string, applied *schema.Namespace) map[string]bool {
	if applied == nil {
		return nil
	}
	tables := make(map[string]bool)
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

// refuseRemovedRowSecurity refuses a file that no longer declares the row
// security of table when the files last applied did and the live table still
// has some. A table with none left, or not there yet, has nothing a plan
// would leave behind.
func refuseRemovedRowSecurity(ctx context.Context, pool *pgxpool.Pool, namespace, table string) error {
	live, err := schemadiff.Introspect(ctx, pool, namespace, table)
	if errors.Is(err, schemadiff.ErrTableNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the row security of table %q.%q: %w", namespace, table, err)
	}
	if !hasRowSecurity(live.RowSecurity) {
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
	return fmt.Sprintf("table %q no longer declares row security, but the schema files SchemaBot last applied did, and the table still has %s. "+
		"A file without row security clauses leaves them in place, so this plan would not remove them. "+
		"To remove them, add ALTER TABLE %s DISABLE ROW LEVEL SECURITY; to the file. "+
		"To keep them, put the file's row security clauses back",
		e.table, describeRowSecurity(e.live), pgx.Identifier{e.table}.Sanitize())
}

// describeRowSecurity names what a table's row security holds, such as
// `row level security enabled and policy "read all"`.
func describeRowSecurity(rs schemadiff.RowSecurity) string {
	var parts []string
	switch {
	case rs.Enabled && rs.Forced:
		parts = append(parts, "row level security enabled and forced")
	case rs.Enabled:
		parts = append(parts, "row level security enabled")
	case rs.Forced:
		parts = append(parts, "row level security forced")
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
		parts = append(parts, noun+" "+strings.Join(names, ", "))
	}
	return strings.Join(parts, " and ")
}
