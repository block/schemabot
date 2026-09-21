package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/schemadiff"
	spirittable "github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/postgresconn"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/targetauth"
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

const inspectUnmodeledPostgresTableObjects = `
SELECT
  EXISTS (SELECT 1 FROM pg_catalog.pg_trigger
          WHERE tgrelid OPERATOR(pg_catalog.=) c.oid AND NOT tgisinternal),
  c.relrowsecurity OR c.relforcerowsecurity,
  EXISTS (SELECT 1 FROM pg_catalog.pg_policy WHERE polrelid OPERATOR(pg_catalog.=) c.oid),
  EXISTS (SELECT 1 FROM pg_catalog.pg_description
          WHERE objoid OPERATOR(pg_catalog.=) c.oid AND objsubid OPERATOR(pg_catalog.>=) 0),
  COALESCE(pg_catalog.cardinality(c.reloptions), 0) OPERATOR(pg_catalog.>) 0,
  EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid OPERATOR(pg_catalog.=) c.oid)
    AND NOT c.relispartition
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
WHERE n.nspname OPERATOR(pg_catalog.=) $1 AND c.relname OPERATOR(pg_catalog.=) $2`

type unmodeledTableObjects struct {
	trigger     bool
	rowSecurity bool
	policy      bool
	comment     bool
	reloptions  bool
	inheritance bool
}

// PullSchema exports tables that the PostgreSQL declarative format can
// represent. It refuses tables carrying user triggers, row-level security,
// policies, comments, non-default relation options, or table inheritance, as
// well as shapes rejected by the renderer. PostgreSQL currently supports only
// basic catalog detail.
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
	caPath, err := caCertPath(e.pullCredentials)
	if err != nil {
		return nil, fmt.Errorf("pull PostgreSQL database %q: %w", e.pullDatabase, err)
	}
	validationOpts, err := validationRootCAs(caPath)
	if err != nil {
		return nil, fmt.Errorf("pull PostgreSQL database %q: %w", e.pullDatabase, err)
	}
	// Validate the SchemaBot-managed connection path, including its transport
	// policy, before adapting the same normalized DSN to pg-sprite's pool API.
	db, err := postgresconn.Open(e.pullCredentials.DSN, validationOpts...)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL database %q for schema pull: %w", e.pullDatabase, err)
	}
	defer utils.CloseAndLog(db)
	// Open only parsed the DSN; this ping is the first dial, so it is where the
	// target can refuse the session and the only error worth classifying.
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping PostgreSQL database %q for schema pull: %w", e.pullDatabase, targetauth.Wrap(err))
	}
	poolCfg, err := spritePoolConfig(e.pullCredentials.DSN, caPath)
	if err != nil {
		return nil, fmt.Errorf("pull PostgreSQL database %q: %w", e.pullDatabase, err)
	}
	pool, err := dbconn.NewPool(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("open pg-sprite pool for PostgreSQL database %q schema pull: %w", e.pullDatabase, err)
	}
	defer pool.Close()

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
	for _, namespace := range namespaces {
		// The pull renders the same table set the plan holds schema files
		// accountable for, so a pulled baseline declares exactly what a later
		// plan would otherwise report as undeclared. Partitions and
		// extension-owned tables have no file of their own and are left out.
		tables, tableErrors, err := renderPostgresTables(ctx, pool, namespace, pulledBaseline)
		if err != nil {
			return nil, fmt.Errorf("pull PostgreSQL database %q: %w", e.pullDatabase, err)
		}
		pulled := &ternv1.PulledNamespace{Tables: tables}
		renderErrors = append(renderErrors, tableErrors...)
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
	// refuseUnmodeledObjects refuses a table that carries objects the
	// declarative format does not represent — a trigger, a policy, a comment
	// — even though the renderer would happily render its columns and
	// indexes without them.
	refuseUnmodeledObjects bool
	// skipArchiveTables leaves archive-named tables out of the baseline, the
	// same tables the plan exempts from the undeclared-table verdict.
	skipArchiveTables bool
}

// pulledBaseline becomes the owner's declared schema, so a table the format
// would describe incompletely is refused rather than written down without
// its trigger or comment, and every table the plan would hold a file
// accountable for is present, archive tables included.
var pulledBaseline = baselinePolicy{refuseUnmodeledObjects: true}

// rollbackBaseline is read only by a rollback re-plan, which manages the
// same table set the forward plan did. Objects the differ cannot see are
// left in place by any apply and by any rollback, so they cost the
// namespace nothing; an archive table sits outside management on both
// plans, so its shape — renderable or not — is not the baseline's concern.
var rollbackBaseline = baselinePolicy{skipArchiveTables: true}

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

// renderedTable is one table's outcome from a baseline render: its canonical
// content, or the reason the baseline cannot carry it. The render records the
// latter and carries on, so a namespace's full list of refused tables reaches
// the caller in one pass.
type renderedTable struct {
	content   string
	renderErr error
}

// renderPostgresTables uses pg-sprite's managed-table enumeration and
// canonical renderer for every PostgreSQL baseline SchemaBot captures. Tables
// are introspected concurrently, bounded by baselineIntrospectionLimit, and
// the result is assembled in listing order so the rendered set and the
// refusals read the same regardless of which introspection finished first.
//
// A per-table refusal — objects the format cannot carry, a shape the
// renderer refuses — is recorded against that table and the render carries
// on. A failure to read the catalog at all, and a cancelled or expired
// context, are the caller's outcome rather than one table's: either ends the
// whole render with an error instead of being recorded as a per-table
// failure and carried on past.
func renderPostgresTables(ctx context.Context, pool *pgxpool.Pool, namespace string, policy baselinePolicy) (map[string]string, []error, error) {
	tables, err := schemadiff.ListManagedTables(ctx, pool, namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("list PostgreSQL tables in schema %q: %w", namespace, err)
	}
	managedTables := make([]string, 0, len(tables))
	for _, table := range tables {
		if policy.skipArchiveTables && spirittable.IsArchiveTable(table) {
			slog.Debug("PostgreSQL archive table is outside management and left out of the rendered baseline",
				"namespace", namespace,
				"table", table)
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
	return rendered, renderErrors, nil
}

// renderPostgresTable renders one table for a baseline. A refusal the
// baseline must report per table comes back in the result; an error that
// ends the whole render — a catalog read failure or a cancelled or expired
// context — comes back as the error.
func renderPostgresTable(ctx context.Context, pool *pgxpool.Pool, namespace, table string, policy baselinePolicy) (*renderedTable, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if policy.refuseUnmodeledObjects {
		objects, err := pullUnmodeledTableObjects(ctx, pool, namespace, table)
		if err != nil {
			return nil, err
		}
		if err := unmodeledTableObjectsError(namespace, table, objects); err != nil {
			return &renderedTable{renderErr: err}, nil
		}
	}
	model, err := schemadiff.Introspect(ctx, pool, namespace, table)
	if err != nil {
		if isContextError(err) {
			return nil, fmt.Errorf("introspect schema %q table %q: %w", namespace, table, err)
		}
		return &renderedTable{renderErr: fmt.Errorf("schema %q table %q: introspect: %w", namespace, table, err)}, nil
	}
	content, err := schemadiff.Render(model)
	if err != nil {
		return &renderedTable{renderErr: fmt.Errorf("schema %q table %q: render: %w", namespace, table, err)}, nil
	}
	return &renderedTable{content: content}, nil
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

func pullUnmodeledTableObjects(ctx context.Context, pool *pgxpool.Pool, namespace, table string) (unmodeledTableObjects, error) {
	var objects unmodeledTableObjects
	err := pool.QueryRow(ctx, inspectUnmodeledPostgresTableObjects, namespace, table).Scan(
		&objects.trigger,
		&objects.rowSecurity,
		&objects.policy,
		&objects.comment,
		&objects.reloptions,
		&objects.inheritance,
	)
	if err != nil {
		return unmodeledTableObjects{}, fmt.Errorf("inspect PostgreSQL schema %q table %q for unmodeled objects: %w", namespace, table, err)
	}
	return objects, nil
}

// unmodeledTableObjectsError names every object kind on the table that the
// declarative format cannot carry, so a refused pull tells the operator what
// to move or drop instead of hiding the objects behind a rendered table that
// looks complete.
func unmodeledTableObjectsError(namespace, table string, objects unmodeledTableObjects) error {
	present := []struct {
		kind    string
		present bool
	}{
		{"trigger", objects.trigger},
		{"row-level security", objects.rowSecurity},
		{"policy", objects.policy},
		{"comment", objects.comment},
		{"relation options", objects.reloptions},
		{"table inheritance", objects.inheritance},
	}
	var kinds []string
	for _, candidate := range present {
		if candidate.present {
			kinds = append(kinds, candidate.kind)
		}
	}
	if len(kinds) == 0 {
		return nil
	}
	return fmt.Errorf("schema %q table %q carries objects the declarative format does not represent: %s", namespace, table, strings.Join(kinds, ", "))
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
