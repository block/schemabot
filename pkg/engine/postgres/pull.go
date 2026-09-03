package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/spirit/pkg/utils"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/postgresconn"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
)

const listPostgresSchemas = `
SELECT nspname
FROM pg_namespace
ORDER BY nspname`

const postgresSchemaExists = `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`

const listPostgresTables = `
SELECT c.relname
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = $1
  AND c.relkind IN ('r', 'p')
  AND c.relispartition = false
ORDER BY c.relname`

// PullSchema exports only complete, round-trippable namespaces. If any table
// cannot be represented by the declarative format, the whole request fails so
// callers never persist a partial baseline that misrepresents the live schema.
func (e *Engine) PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("pull PostgreSQL schema: request is required")
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
	db, err := postgresconn.Open(e.pullCredentials.DSN, validationOpts...)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL database %q for schema pull: %w", e.pullDatabase, err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping PostgreSQL database %q for schema pull: %w", e.pullDatabase, err)
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
		tables, err := pullTables(ctx, pool, namespace)
		if err != nil {
			return nil, fmt.Errorf("list PostgreSQL tables in schema %q: %w", namespace, err)
		}
		pulled := &ternv1.PulledNamespace{Tables: make(map[string]string, len(tables))}
		for _, table := range tables {
			model, err := schemadiff.Introspect(ctx, pool, namespace, table)
			if err != nil {
				renderErrors = append(renderErrors, fmt.Errorf("schema %q table %q: introspect: %w", namespace, table, err))
				continue
			}
			content, err := schemadiff.Render(model)
			if err != nil {
				renderErrors = append(renderErrors, fmt.Errorf("schema %q table %q: render: %w", namespace, table, err))
				continue
			}
			pulled.Tables[table] = content
		}
		response.Namespaces[namespace] = pulled
		response.TableCount += int32(len(pulled.Tables))
	}
	if err := errors.Join(renderErrors...); err != nil {
		return nil, fmt.Errorf("pull PostgreSQL database %q refused incomplete baseline: %w", e.pullDatabase, err)
	}
	return response, nil
}

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

func pullTables(ctx context.Context, pool *pgxpool.Pool, namespace string) ([]string, error) {
	rows, err := pool.Query(ctx, listPostgresTables, namespace)
	if err != nil {
		return nil, fmt.Errorf("query PostgreSQL tables in schema %q: %w", namespace, err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL table in schema %q: %w", namespace, err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL tables in schema %q: %w", namespace, err)
	}
	return tables, nil
}
