package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
)

// The pull's catalog reads carry the same pg_catalog qualification as
// schemadiff.ListManagedTables: a search_path that lists a user schema first
// must not let a decoy relation or operator hand the pull a wrong baseline.
const listPostgresSchemas = `
SELECT nspname
FROM pg_catalog.pg_namespace
ORDER BY nspname`

const postgresSchemaExists = `
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace
               WHERE nspname OPERATOR(pg_catalog.=) $1)`

const inspectPostgresTableInheritance = `
SELECT
  EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid OPERATOR(pg_catalog.=) c.oid)
    AND NOT c.relispartition
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
WHERE n.nspname OPERATOR(pg_catalog.=) $1 AND c.relname OPERATOR(pg_catalog.=) $2`

// PullSchema exports tables that the PostgreSQL declarative format can
// represent. A table's own foreign keys and storage parameters are written
// into its file. A user trigger or a table or column comment is left out of
// the file and left in place: no plan or apply reads or changes it, so the
// table is still managed. It refuses tables with table inheritance, a table
// referenced by a foreign key that the pull does not write, as well as shapes
// rejected by the renderer.
// By default one refused table refuses the whole pull, so a caller never
// mistakes a partial baseline for the namespace. A request that sets
// skip_unrepresentable_tables gets the rest of the namespace instead, with
// each refused table named in skipped_tables. PostgreSQL currently supports
// only basic catalog detail.
func (e *Engine) PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("pull PostgreSQL schema: request is required")
	}
	if req.GetCatalogDetail() != ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_BASIC {
		return nil, fmt.Errorf("pull PostgreSQL database %q: catalog detail %q is unsupported; use basic", e.pullDatabase, req.GetCatalogDetail())
	}
	if e.pullCredentials == nil || e.pullCredentials.DSN == "" {
		return nil, fmt.Errorf("pull PostgreSQL database %q: DSN credentials are required", e.pullDatabase)
	}
	pool, closePool, err := openTargetPool(ctx, e.pullCredentials)
	if err != nil {
		return nil, fmt.Errorf("pull PostgreSQL database %q: %w", e.pullDatabase, err)
	}
	defer closePool()

	namespaces, err := pullNamespaces(ctx, pool, req.GetNamespace())
	if err != nil {
		return nil, fmt.Errorf("discover PostgreSQL namespaces for database %q: %w", e.pullDatabase, err)
	}
	response := &ternv1.PullSchemaResponse{
		Database: req.GetDatabase(), Type: e.Name(), Environment: req.GetEnvironment(),
		Namespaces: make(map[string]*ternv1.PulledNamespace, len(namespaces)),
	}
	if response.Database == "" {
		response.Database = e.pullDatabase
	}
	var renderErrors []error
	skip := req.GetSkipUnrepresentableTables()
	for _, namespace := range namespaces {
		// The pull renders the same table set the plan holds schema files
		// accountable for, so a pulled baseline declares exactly what a later
		// plan would otherwise report as undeclared. Partitions and
		// extension-owned tables have no file of their own and are left out.
		tables, tableErrors, err := renderPostgresTables(ctx, pool, e.pullDatabase, namespace, pulledBaseline)
		if err != nil {
			return nil, fmt.Errorf("pull PostgreSQL database %q: %w", e.pullDatabase, err)
		}
		pulled := &ternv1.PulledNamespace{Tables: tables}
		if skip {
			skipped, err := skippedTables(tableErrors)
			if err != nil {
				return nil, fmt.Errorf("pull PostgreSQL database %q: %w", e.pullDatabase, err)
			}
			for _, table := range skipped {
				slog.Info("PostgreSQL pull skipped a table its schema file cannot describe",
					"database", e.pullDatabase,
					"namespace", namespace,
					"table", table.GetTable(),
					"reason", table.GetReason())
			}
			pulled.SkippedTables = skipped
		} else {
			renderErrors = append(renderErrors, tableErrors...)
		}
		response.Namespaces[namespace] = pulled
		response.TableCount += int32(len(pulled.Tables))
	}
	if err := errors.Join(renderErrors...); err != nil {
		return nil, fmt.Errorf("pull PostgreSQL database %q refused incomplete baseline: %w", e.pullDatabase, err)
	}
	return response, nil
}

// baselinePolicy says which live tables a rendered baseline must carry and
// which it must refuse, beyond what pg-sprite's renderer refuses on its own.
// The two baselines SchemaBot renders answer those questions differently
// because they are read by different parties.
type baselinePolicy struct {
	// includeRowSecurity writes a table's row security settings and policies
	// after its CREATE TABLE. The renderer writes all of them or refuses the
	// table, so a baseline never carries part of a table's access rules.
	includeRowSecurity bool
	// declareRowSecurity names the tables whose row security the baseline
	// writes even when the live table has none, as an explicit DISABLE and
	// NO FORCE with no policies. A table-only file leaves row security
	// unmanaged, so an original for a table the forward plan first secured
	// must say "none" to take the security back off on rollback.
	declareRowSecurity map[string]bool
	// includeForeignKeys writes each table's own foreign keys, and refuses a
	// referenced table whose incoming foreign key lives on a table the
	// baseline does not write, since that key would be in no file.
	includeForeignKeys bool
	// refuseNotValidForeignKeys refuses a table holding a foreign key added
	// NOT VALID. The renderer writes every key as a validated one, so a
	// baseline carrying it would plan a VALIDATE CONSTRAINT the forward apply
	// never ran, and existing rows that break the key would fail it.
	refuseNotValidForeignKeys bool
	// refuseInheritance refuses a child of classic table inheritance,
	// which the declarative format cannot express, even though the renderer
	// would render its columns and indexes. A trigger or a table or column comment
	// is not refused: neither the differ nor any apply reads or changes it, so
	// the file is complete for everything a plan can do to the table.
	refuseInheritance bool
	// skipUndeclaredArchiveTables leaves archive-named tables out only when
	// no forward schema file declares them. Declared archives are managed.
	skipUndeclaredArchiveTables bool
	declaredTables              map[string]bool
	ignoredTables               engine.IgnoredTables
	// unwrittenKeyRemedy follows an incoming foreign key on a table in this
	// schema that has no file of its own, and says what the reader of this
	// baseline can do about it.
	unwrittenKeyRemedy string
}

// pulledBaseline becomes the owner's declared schema, so a table the format
// would describe incompletely is refused, and every table the plan would hold
// a file accountable for is present, archive tables included. A table's
// triggers and comments stay on the live table, outside the file.
var pulledBaseline = baselinePolicy{
	refuseInheritance:  true,
	includeRowSecurity: true,
	includeForeignKeys: true,
	unwrittenKeyRemedy: "on a table the pull cannot write, such as one an extension owns; remove the key",
}

// rollbackBaseline is read only by a rollback re-plan, which manages the
// same table set the forward plan did. Objects the differ cannot see are
// left in place by any apply and by any rollback, so they cost the
// namespace nothing. Ignored tables and undeclared archives sit outside
// management, so their shapes are not the baseline's concern. Foreign keys
// are carried, so a namespace that has them keeps its rollback: a key the
// forward plan did not touch diffs to nothing, and a key it added comes
// back as a drop, which the re-plan shows blocked like any foreign key drop.
// A key added NOT VALID cannot be carried as it is, so a namespace holding
// one has no rollback baseline (RV-7). Row security is carried the same way:
// settings and policies the forward plan did not touch diff to nothing, and
// one it changed comes back as the reverse change, which the re-plan admits
// or refuses like any row security change. A table whose forward file
// declares row security is written with its row security even when it has
// none, so a rollback of the first ENABLE or the first policy removes it
// rather than leaving the new access rules in place. Every other table
// without row security keeps a table-only file.
func rollbackBaseline(declared, rowSecurityDeclared map[string]bool, ignored engine.IgnoredTables) baselinePolicy {
	return baselinePolicy{
		includeRowSecurity:          true,
		declareRowSecurity:          rowSecurityDeclared,
		includeForeignKeys:          true,
		refuseNotValidForeignKeys:   true,
		skipUndeclaredArchiveTables: true,
		declaredTables:              declared,
		ignoredTables:               ignored,
		unwrittenKeyRemedy:          "on a table without a schema file, such as one an extension owns or one the repository ignores; declare that table in a schema file or remove the key",
	}
}

// exclusionReason names the rule that leaves a table out of the baseline,
// using the same reason strings a plan reports for its exempt tables, or
// returns "" when the baseline carries the table.
func (p baselinePolicy) exclusionReason(table string) string {
	if p.ignoredTables.Withholds(table) {
		return engine.ExemptReasonIgnoreTables
	}
	if p.skipUndeclaredArchiveTables && !p.declaredTables[table] && engine.IsArchiveTable(table) {
		return engine.ExemptReasonArchiveNaming
	}
	return ""
}

// baselineIntrospectionConcurrency caps how many tables a baseline render
// introspects at once. Each introspection is one read-only transaction of
// catalog queries against the target, so the render's wall time otherwise
// grows with the namespace one round trip at a time; a small cap recovers
// most of that without turning a large namespace into a burst of catalog
// load the target did not size for.
const baselineIntrospectionConcurrency = 8

// baselineIntrospectionLimit bounds the render's concurrency by the pool as
// well as the cap: an introspection holds one pooled connection for its whole
// transaction, so more goroutines than connections would only queue on
// acquire. MaxConns honors pool_max_conns when the DSN sets it; otherwise
// pgxpool defaults it from the process CPU count.
func baselineIntrospectionLimit(pool *pgxpool.Pool) int {
	return max(1, min(baselineIntrospectionConcurrency, int(pool.Config().MaxConns)))
}

// tableRefusal is one table a baseline cannot carry. Its error text names
// the schema and table for a refused pull; its reason is the short phrase a
// skipped table is reported with.
type tableRefusal struct {
	namespace string
	table     string
	// detail completes the refusal's error text after the table name.
	detail string
	// reason says what the table carries that its schema file cannot
	// describe.
	reason string
	cause  error
}

func (r *tableRefusal) Error() string {
	return fmt.Sprintf("schema %q table %q: %s", r.namespace, r.table, r.detail)
}

func (r *tableRefusal) Unwrap() error { return r.cause }

// skippedTables turns a namespace's refusals into the tables a skipping pull
// reports, in name order. Every refusal a render records is a tableRefusal,
// so any other error is a broken contract and refuses the pull rather than
// dropping a table without a reason.
func skippedTables(refusals []error) ([]*ternv1.SkippedTable, error) {
	skipped := make([]*ternv1.SkippedTable, 0, len(refusals))
	for _, err := range refusals {
		var refusal *tableRefusal
		if !errors.As(err, &refusal) {
			return nil, fmt.Errorf("a table refusal has no table to skip: %w", err)
		}
		skipped = append(skipped, &ternv1.SkippedTable{Table: refusal.table, Reason: refusal.reason})
	}
	slices.SortFunc(skipped, func(a, b *ternv1.SkippedTable) int { return strings.Compare(a.GetTable(), b.GetTable()) })
	return skipped, nil
}

// renderRefusalReason is the reason a table the renderer refused is skipped
// with: the verdict's own wording, which says what the table carries without
// repeating its name, or the render error itself when it carries no verdict.
func renderRefusalReason(err error) string {
	if verdict, ok := schemadiff.RenderRefusal(err); ok {
		return verdict.Error()
	}
	return err.Error()
}

// renderedTable is one table's outcome from a baseline render: its canonical
// content, or the reason the baseline cannot carry it. The render records the
// latter and carries on, so a namespace's full list of refused tables reaches
// the caller in one pass.
type renderedTable struct {
	content   string
	renderErr error
	// incoming are the foreign keys on other tables that reference this
	// one. Each lives in its referencing table's file, so a baseline that
	// writes this table must write those tables too.
	incoming []schemadiff.IncomingForeignKey
}

// renderPostgresTables uses pg-sprite's managed-table enumeration and
// canonical renderer for every PostgreSQL baseline SchemaBot captures. Tables
// are introspected concurrently, bounded by baselineIntrospectionLimit, and
// the result is assembled in listing order so the rendered set and the
// refusals read the same regardless of which introspection finished first.
//
// A per-table refusal — objects the format cannot carry, a shape the
// renderer refuses — is recorded against that table and the render carries
// on. Any failure to read the catalog — a table the listing named that
// introspection cannot find or resolve, a query that fails, a cancelled or
// expired context — is the caller's outcome rather than one table's: it ends
// the whole render with an error, and cancels the introspections still in
// flight, instead of being recorded as a per-table refusal and carried on
// past.
func renderPostgresTables(ctx context.Context, pool *pgxpool.Pool, database, namespace string, policy baselinePolicy) (map[string]string, []error, error) {
	tables, err := schemadiff.ListManagedTables(ctx, pool, namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("list PostgreSQL tables in schema %q: %w", namespace, err)
	}
	managedTables := make([]string, 0, len(tables))
	for _, table := range tables {
		if reason := policy.exclusionReason(table); reason != "" {
			slog.Debug("PostgreSQL table is outside management and left out of the rendered baseline",
				"database", database,
				"namespace", namespace,
				"table", table,
				"reason", reason)
			continue
		}
		managedTables = append(managedTables, table)
	}
	results := make([]*renderedTable, len(managedTables))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(baselineIntrospectionLimit(pool))
	for i, table := range managedTables {
		g.Go(func() error {
			result, err := renderPostgresTable(gctx, pool, namespace, table, policy)
			if err != nil {
				return err
			}
			results[i] = result
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("render PostgreSQL tables in schema %q: %w", namespace, err)
	}
	rendered := make(map[string]string, len(managedTables))
	var renderErrors []error
	for i, table := range managedTables {
		result := results[i]
		switch {
		case result == nil:
			return nil, nil, fmt.Errorf("render PostgreSQL table %q in schema %q produced no result", table, namespace)
		case result.renderErr != nil:
			renderErrors = append(renderErrors, result.renderErr)
		default:
			rendered[table] = result.content
		}
	}
	if policy.includeForeignKeys {
		written := make(map[string]bool, len(managedTables))
		for _, table := range managedTables {
			written[table] = true
		}
		for i, table := range managedTables {
			if _, ok := rendered[table]; !ok {
				continue
			}
			unwritten, err := unwrittenIncomingForeignKeys(ctx, pool, namespace, results[i].incoming, written, policy.unwrittenKeyRemedy)
			if err != nil {
				return nil, nil, err
			}
			if len(unwritten) > 0 {
				delete(rendered, table)
				detail := fmt.Sprintf("%s: %s", errUnwrittenReferencingTable, strings.Join(unwritten, ", "))
				renderErrors = append(renderErrors, &tableRefusal{namespace: namespace, table: table, detail: detail, reason: detail, cause: errUnwrittenReferencingTable})
			}
		}
	}
	return rendered, renderErrors, nil
}

// errUnwrittenReferencingTable refuses a referenced table whose incoming
// foreign key lives on a table the pull does not write. The key is declared
// only in the referencing table's file, so a baseline without that file
// would not carry the relationship.
var errUnwrittenReferencingTable = errors.New("referenced by a foreign key on a table without a schema file")

// unwrittenIncomingForeignKeys describes every incoming foreign key that lives
// outside the pulled files, with what the operator can do about it: a key on
// a table in another schema, which a desired file cannot reference; a key on
// a partition, which belongs on the partitioned parent; or a key on another
// table in this schema that has no file of its own, followed by the
// baseline's remedy for it. It fails only when the catalog cannot say
// whether the referencing table is a partition.
func unwrittenIncomingForeignKeys(ctx context.Context, pool *pgxpool.Pool, namespace string, incoming []schemadiff.IncomingForeignKey, written map[string]bool, remedy string) ([]string, error) {
	var unwritten []string
	for _, fk := range incoming {
		name := foreignKeyName(namespace, fk)
		switch {
		case fk.Schema != namespace:
			unwritten = append(unwritten, name+" (in another schema; a schema file cannot reference it)")
		case written[fk.Table]:
			// The key is declared in its referencing table's file, which this pull writes.
		default:
			partition, err := isPostgresPartition(ctx, pool, fk.Schema, fk.Table)
			if err != nil {
				return nil, err
			}
			if partition {
				unwritten = append(unwritten, name+" (on a partition; declare the key on the partitioned parent)")
			} else {
				unwritten = append(unwritten, name+" ("+remedy+")")
			}
		}
	}
	return unwritten, nil
}

const postgresTableIsPartition = `
SELECT c.relispartition
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
WHERE n.nspname OPERATOR(pg_catalog.=) $1 AND c.relname OPERATOR(pg_catalog.=) $2`

// isPostgresPartition reports whether the table is a partition of another.
func isPostgresPartition(ctx context.Context, pool *pgxpool.Pool, schema, table string) (bool, error) {
	var partition bool
	if err := pool.QueryRow(ctx, postgresTableIsPartition, schema, table).Scan(&partition); err != nil {
		return false, fmt.Errorf("check whether PostgreSQL table %q in schema %q is a partition: %w", table, schema, err)
	}
	return partition, nil
}

// renderPostgresTable renders one table for a baseline. A refusal the
// baseline must report per table — objects the format cannot carry, a shape
// the renderer refuses — comes back in the result. Every other failure comes
// back as the error and ends the whole render: a table the listing named
// that introspection cannot read is not a shape of that table, it is a
// catalog the render could not read consistently, and a baseline assembled
// past it would misreport a connection failure or a concurrent schema change
// as a refusal of one table.
func renderPostgresTable(ctx context.Context, pool *pgxpool.Pool, namespace, table string, policy baselinePolicy) (*renderedTable, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if policy.refuseInheritance {
		inherits, err := inheritsFromAParent(ctx, pool, namespace, table)
		if err != nil {
			return nil, err
		}
		if inherits {
			return &renderedTable{renderErr: inheritanceRefusal(namespace, table)}, nil
		}
	}
	model, err := schemadiff.Introspect(ctx, pool, namespace, table)
	if err != nil {
		return nil, fmt.Errorf("introspect schema %q table %q: %w", namespace, table, err)
	}
	if policy.refuseNotValidForeignKeys {
		if key, ok := notValidForeignKey(model); ok {
			return &renderedTable{renderErr: &tableRefusal{
				namespace: namespace, table: table,
				detail: fmt.Sprintf("%s: %s", errNotValidForeignKey, key), reason: errNotValidForeignKey.Error(), cause: errNotValidForeignKey,
			}}, nil
		}
	}
	opts := schemadiff.RenderOptions{ForeignKeys: policy.includeForeignKeys, RowSecurity: policy.declareRowSecurity[table]}
	content, err := schemadiff.RenderWith(model, opts)
	if policy.includeRowSecurity && errors.Is(err, schemadiff.ErrUnrenderableRowSecurity) {
		opts.RowSecurity = true
		content, err = schemadiff.RenderWith(model, opts)
	}
	if err != nil {
		return &renderedTable{renderErr: &tableRefusal{
			namespace: namespace, table: table,
			detail: "render: " + err.Error(), reason: renderRefusalReason(err), cause: err,
		}}, nil
	}
	return &renderedTable{content: content, incoming: model.IncomingForeignKeys}, nil
}

// errNotValidForeignKey refuses a table holding a foreign key added NOT
// VALID from a baseline that must reproduce the key exactly.
var errNotValidForeignKey = errors.New("holds a foreign key added NOT VALID, which a schema file cannot declare")

// notValidForeignKey names the first foreign key on the table that was added
// NOT VALID and not yet validated.
func notValidForeignKey(model schemadiff.Model) (string, bool) {
	for _, c := range model.Constraints {
		if c.ForeignKey && c.NotValid {
			return c.Name, true
		}
	}
	return "", false
}

// pullNamespaces discovers every non-reserved schema by default. Callers use
// the requested namespace as the precise lever for selecting a single schema.
func pullNamespaces(ctx context.Context, pool *pgxpool.Pool, requested string) ([]string, error) {
	if requested != "" {
		if schema.IsReservedPullNamespaceForDialect(schema.DialectPostgres, requested) {
			return nil, fmt.Errorf("schema %q is reserved and cannot be pulled", requested)
		}
		var exists bool
		if err := pool.QueryRow(ctx, postgresSchemaExists, requested).Scan(&exists); err != nil {
			return nil, fmt.Errorf("check PostgreSQL schema %q exists: %w", requested, err)
		}
		if !exists {
			return nil, fmt.Errorf("schema %q does not exist", requested)
		}
		return []string{requested}, nil
	}
	rows, err := pool.Query(ctx, listPostgresSchemas)
	if err != nil {
		return nil, fmt.Errorf("query PostgreSQL schemas: %w", err)
	}
	defer rows.Close()
	var namespaces []string
	for rows.Next() {
		var namespace string
		if err := rows.Scan(&namespace); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL schema name: %w", err)
		}
		if !schema.IsReservedPullNamespaceForDialect(schema.DialectPostgres, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL schemas: %w", err)
	}
	return namespaces, nil
}

// inheritsFromAParent reports whether the table is a child of classic table
// inheritance. A partition also has a pg_inherits row, and it is not one.
func inheritsFromAParent(ctx context.Context, pool *pgxpool.Pool, namespace, table string) (bool, error) {
	var inherits bool
	if err := pool.QueryRow(ctx, inspectPostgresTableInheritance, namespace, table).Scan(&inherits); err != nil {
		return false, fmt.Errorf("inspect PostgreSQL schema %q table %q for table inheritance: %w", namespace, table, err)
	}
	return inherits, nil
}

// inheritanceRefusal names the table inheritance the declarative format cannot
// carry, so a refused pull tells the operator what to move or drop instead of
// hiding it behind a rendered table that looks complete.
func inheritanceRefusal(namespace, table string) *tableRefusal {
	return &tableRefusal{
		namespace: namespace, table: table,
		detail: "carries objects the declarative format does not represent: table inheritance",
		reason: "schema files cannot describe its table inheritance",
	}
}
