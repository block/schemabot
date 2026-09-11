package serve

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
)

// storageSchemaAdapter answers the storage-schema RPCs for the instance it was
// built on: it reads that instance's own storage database, with that
// instance's own embedded schema files, and converges it by running that
// instance's own startup bootstrap.
//
// The bindings are what make the answer trustworthy, so they are fixed at
// construction and nothing on the wire can move them. There is no target in
// the request, so no caller — not even the control plane — can point this at a
// different database. The one thing a caller may influence is whether
// destructive statements run, and that only ever widens what the local config
// already allows (see effectiveAllowDestructive).
type storageSchemaAdapter struct {
	// resolveDSN re-resolves the storage DSN per call rather than capturing a
	// string, so a credential rotated since startup is picked up the same way
	// the storage pool picks it up.
	resolveDSN func() (string, error)
	dialect    schema.Dialect
	version    string
	// configAllowsDestructive is the deployment's standing storage policy
	// (storage.allow_destructive_schema_changes). A boot converges under it, so
	// an operator convergence must too — otherwise "apply is what a boot does"
	// would stop being true on exactly the deployments that opted in.
	configAllowsDestructive bool
	// postgresStatementTimeout is the budget the bootstrap runs its catalog
	// reads under, so a diff cannot succeed under a budget the convergence
	// would then fail on, or the reverse. It carries the PostgreSQL
	// statement-budget convention: zero disables the budget explicitly,
	// negative leaves the platform's ambient value alone.
	postgresStatementTimeout time.Duration
	logger                   *slog.Logger
}

// storageSchemaService builds the adapter for this server's own storage. It is
// registered on the gRPC endpoint so a control plane can reach the storage of
// a data plane an operator's workstation cannot dial directly.
func (s *Server) storageSchemaService() *storageSchemaAdapter {
	return &storageSchemaAdapter{
		resolveDSN:               s.cfg.StorageDSN,
		dialect:                  s.dialect,
		version:                  s.version,
		configAllowsDestructive:  s.cfg.Storage.AllowDestructiveSchemaChanges,
		postgresStatementTimeout: s.cfg.Postgres.StatementTimeoutOrDefault(),
		logger:                   s.logger,
	}
}

func (a *storageSchemaAdapter) StorageSchemaDiff(ctx context.Context, req *ternv1.StorageSchemaDiffRequest) (*ternv1.StorageSchemaDiffResponse, error) {
	dsn, opts, err := a.target(req.GetAllowDestructive())
	if err != nil {
		return nil, err
	}
	diffCtx, cancel := context.WithTimeout(ctx, api.StorageSchemaDiffTimeout)
	defer cancel()

	report, err := api.DiffStorageSchema(diffCtx, dsn, a.logger, opts...)
	if err != nil {
		return nil, fmt.Errorf("diff storage schema (dialect %s): %w", a.dialect, err)
	}
	report.Version = a.version
	return &ternv1.StorageSchemaDiffResponse{Report: api.StorageSchemaReportProto(report)}, nil
}

func (a *storageSchemaAdapter) StorageSchemaApply(ctx context.Context, req *ternv1.StorageSchemaApplyRequest) (*ternv1.StorageSchemaApplyResponse, error) {
	dsn, opts, err := a.target(req.GetAllowDestructive())
	if err != nil {
		return nil, err
	}
	// No timeout of its own: the convergence is the startup bootstrap, which
	// bounds itself with EnsureSchemaTimeout, and wrapping a shorter deadline
	// around it would cancel a legitimate online DDL mid-copy. The caller's
	// context still cuts it short if the control plane hangs up.
	planned, remaining, err := api.ApplyStorageSchema(ctx, dsn, a.logger, opts...)
	if err != nil {
		return nil, fmt.Errorf("converge storage schema (dialect %s): %w", a.dialect, err)
	}
	planned.Version = a.version
	remaining.Version = a.version
	a.logger.InfoContext(ctx, "storage schema convergence answered",
		"dialect", a.dialect,
		"database", remaining.Database,
		"caller", req.GetCaller(),
		"planned_count", len(planned.Outstanding),
		"remaining_count", len(remaining.Outstanding),
		"refused_count", len(remaining.Destructive),
		"manual_count", len(remaining.Manual),
	)
	return &ternv1.StorageSchemaApplyResponse{
		Planned:   api.StorageSchemaReportProto(planned),
		Remaining: api.StorageSchemaReportProto(remaining),
	}, nil
}

// target resolves the storage DSN and the bootstrap options for one call.
func (a *storageSchemaAdapter) target(requestAllowsDestructive bool) (string, []api.EnsureSchemaOption, error) {
	dsn, err := a.resolveDSN()
	if err != nil {
		return "", nil, fmt.Errorf("resolve storage DSN for dialect %s: %w", a.dialect, err)
	}
	if dsn == "" {
		return "", nil, fmt.Errorf("storage DSN not configured for dialect %s; the storage schema surface has no database to read", a.dialect)
	}
	return dsn, []api.EnsureSchemaOption{
		api.WithDialect(a.dialect),
		api.WithAllowDestructiveSchemaChanges(a.effectiveAllowDestructive(requestAllowsDestructive)),
		api.WithPostgresStatementTimeout(a.postgresStatementTimeout),
	}, nil
}

// effectiveAllowDestructive is the local policy widened by an explicit
// per-request opt-in, never narrowed by its absence.
//
// The two directions are not symmetric and the asymmetry is the point. A
// deployment that configured allow_destructive_schema_changes has already made
// the decision for every boot; a convergence that ignored it would run less
// than the next boot runs, so "apply is what a boot does" — the property that
// makes this usable as a pre-deploy convergence step — would quietly stop
// holding there. In the other direction, a request opting in is exactly the
// explicit operator consent AV-9 asks for before surplus storage state is
// destroyed, arriving through a command an admin had to issue rather than
// through a config file nobody re-read.
func (a *storageSchemaAdapter) effectiveAllowDestructive(requestAllowsDestructive bool) bool {
	return a.configAllowsDestructive || requestAllowsDestructive
}
