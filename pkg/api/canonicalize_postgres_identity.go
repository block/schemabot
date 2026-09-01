package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// postgresIdentityKeyColumns names, per storage table, the columns that hold
// identity strings — repository full names, database names, database types,
// environments, deployments, and lock owners. These are the cross-dialect
// row-identity keys that storage.CanonicalKey folds at every write boundary;
// this map drives the one-time fold of rows written before those boundaries
// folded. The parity test against the embedded schema files keeps the map in
// lockstep with the schema.
//
// Lock owner strings ("org/repo#42") are folded here even though the write
// boundaries never re-fold them: new owners are canonical by construction
// (derived from the folded repository), but owners stored by earlier
// releases can embed the unfolded spelling, which the folded lookup can
// never match again. CLI lock owners ("cli:user@host") fold with them; a
// caller whose spelling differs only by case can release its lock through
// force-release.
//
// Deliberately absent: lease owners and observer owners are pod identities,
// not row-identity keys; plan identifiers, apply identifiers, SHAs, GitHub
// node IDs, and delivery IDs are opaque or case-significant values.
var postgresIdentityKeyColumns = map[string][]string{
	"applies":            {"database_name", "database_type", "deployment", "environment", "repository"},
	"apply_operations":   {"deployment"},
	"apply_target_locks": {"database_name", "database_type", "deployment", "environment"},
	"checks":             {"database_name", "database_type", "environment", "repository"},
	"locks":              {"database_name", "database_type", "owner", "repository"},
	"plan_comments":      {"database_name", "database_type", "environment_scope", "repository"},
	"plans":              {"database_name", "database_type", "deployment", "environment", "repository"},
	"tasks":              {"database_name", "database_type", "environment", "repository"},
	"webhook_events":     {"repository"},
}

// postgresErrUniqueViolation is PostgreSQL's SQLSTATE for a unique-index
// violation, the error a fold collision raises.
const postgresErrUniqueViolation = "23505"

// CanonicalizePostgresIdentityKeys folds every stored identity string on
// SchemaBot's PostgreSQL storage tables to its canonical lowercase spelling.
// MySQL's accent- and case-insensitive storage collation forgives spelling
// drift, but PostgreSQL compares bytes: rows written by releases that did
// not fold identity strings at the write boundaries are invisible to the
// folded lookups — locks that cannot be released, checks that duplicate
// instead of updating. Run the fold once when upgrading a PostgreSQL storage
// database to a release that folds identity strings; it only rewrites rows
// whose spelling is not already canonical, so rerunning it is safe.
//
// The fold rewrites spelling only, so updated_at timestamps are left
// untouched — lease-expiry and staleness heuristics keep their meaning.
//
// When two rows differ only by case (for example a lock on "Foo" and one on
// "foo"), the fold would collapse them into one unique key; the affected
// table's fold fails with the violated constraint named, and the operator
// resolves the duplicate rows by hand before rerunning. Earlier tables'
// folds remain applied — each table folds in its own implicit transaction —
// which the rerun tolerates.
func CanonicalizePostgresIdentityKeys(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	tables, _, err := readEmbeddedPostgresSchemaFiles()
	if err != nil {
		return fmt.Errorf("read embedded PostgreSQL storage schema: %w", err)
	}
	missing, err := missingPostgresTables(ctx, db, tables)
	if err != nil {
		return fmt.Errorf("check target for storage tables: %w", err)
	}
	if len(missing) == len(tables) {
		return fmt.Errorf("none of the %d storage tables exist in the target database; it does not look like SchemaBot's storage database", len(tables))
	}
	missingSet := make(map[string]bool, len(missing))
	for _, table := range missing {
		missingSet[table] = true
	}

	foldTables := make([]string, 0, len(postgresIdentityKeyColumns))
	for table := range postgresIdentityKeyColumns {
		foldTables = append(foldTables, table)
	}
	sort.Strings(foldTables)

	folded := int64(0)
	for _, table := range foldTables {
		if missingSet[table] {
			logger.Warn("storage table does not exist; its identity keys were not folded — rerun the canonicalization after the release that creates it bootstraps",
				"table", table)
			continue
		}
		rows, err := foldPostgresIdentityKeys(ctx, db, table, postgresIdentityKeyColumns[table])
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == postgresErrUniqueViolation {
				return fmt.Errorf("fold identity keys on storage table %q: rows that differ only by case collide on unique index %q; resolve the duplicate rows by hand, then rerun the canonicalization: %w",
					table, pgErr.ConstraintName, err)
			}
			return fmt.Errorf("fold identity keys on storage table %q: %w", table, err)
		}
		folded += rows
		if rows > 0 {
			logger.Info("folded stored identity keys to canonical lowercase",
				"table", table, "rows", rows)
		} else {
			logger.Debug("stored identity keys already canonical",
				"table", table)
		}
	}
	logger.Info("identity key canonicalization summary",
		"tables", len(foldTables), "rows_folded", folded)
	return nil
}

// foldPostgresIdentityKeys lowercases the given identity columns on every row
// of one table whose spelling is not already canonical, returning the number
// of rows rewritten.
func foldPostgresIdentityKeys(ctx context.Context, db *sql.DB, table string, columns []string) (int64, error) {
	sets := make([]string, len(columns))
	predicates := make([]string, len(columns))
	for i, column := range columns {
		quoted := quotePostgresIdentifier(column)
		sets[i] = fmt.Sprintf("%s = lower(%s)", quoted, quoted)
		predicates[i] = fmt.Sprintf("%s <> lower(%s)", quoted, quoted)
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		quotePostgresIdentifier(table), strings.Join(sets, ", "), strings.Join(predicates, " OR "))
	result, err := db.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count folded rows: %w", err)
	}
	return rows, nil
}
